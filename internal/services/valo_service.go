package services

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"valo-engine/config"
	"valo-engine/internal/models"

	_ "github.com/lib/pq"
	"github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

// Account statuses stored in valo_accounts.status.
// whatsmeow auto-reconnects on plain Disconnected; LOGGED_OUT / TEMP_BANNED
// come from PermanentDisconnect events where it will NOT reconnect.
const (
	StatusConnected    = "CONNECTED"
	StatusDisconnected = "DISCONNECTED"
	StatusLoggedOut    = "LOGGED_OUT"
	StatusTempBanned   = "TEMP_BANNED"
)

// Sends via the public API are spaced per account to avoid spam flags.
// ponytail: blocking pacing — swap for a real queue if API callers need instant responses.
var (
	sendMinGap = 4 * time.Second
	sendJitter = 6 * time.Second
)

// sendPacer serializes and spaces outbound sends for one account.
// ponytail: sleeps while holding the lock (that's what serializes sends); global per-account only.
type sendPacer struct {
	mu   sync.Mutex
	next time.Time
}

type ValoService struct {
	db                 *gorm.DB
	container          *sqlstore.Container
	clients            map[string]*whatsmeow.Client
	clientsMutex       sync.RWMutex
	pacers             map[string]*sendPacer
	pacersMutex        sync.Mutex
	grupiaAPIURL       string
	grupiaWebhookKey   string
	ariaWhatsAppNumber string
	whatsAppProxyURL   string
}

func NewValoService(db *gorm.DB, cfg *config.Config) *ValoService {
	sslMode := "disable"
	if cfg.DBSSLMode == "true" || cfg.DBSSLMode == "require" {
		sslMode = "require"
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBName, sslMode)
	if cfg.DBPassword != "" {
		dsn = fmt.Sprintf("%s password=%s", dsn, cfg.DBPassword)
	}

	dbLog := waLog.Stdout("Database", "WARN", true)
	container, err := sqlstore.New(context.Background(), "postgres", dsn, dbLog)
	if err != nil {
		panic("failed to initialize whatsmeow sqlstore: " + err.Error())
	}
	if err := container.Upgrade(context.Background()); err != nil {
		panic("failed to upgrade whatsmeow database schema: " + err.Error())
	}

	svc := &ValoService{
		db:                 db,
		container:          container,
		clients:            make(map[string]*whatsmeow.Client),
		pacers:             make(map[string]*sendPacer),
		grupiaAPIURL:       cfg.GrupiaAPIURL,
		grupiaWebhookKey:   cfg.GrupiaWebhookKey,
		ariaWhatsAppNumber: cfg.AriaWhatsAppNumber,
		whatsAppProxyURL:   cfg.WhatsAppProxyURL,
	}

	svc.initExistingClients()
	return svc
}

func normalizePhoneNumber(number string) string {
	cleaned := strings.TrimSpace(number)
	cleaned = strings.TrimPrefix(cleaned, "+")
	cleaned = strings.ReplaceAll(cleaned, "-", "")
	cleaned = strings.ReplaceAll(cleaned, " ", "")
	if strings.HasPrefix(cleaned, "0") {
		cleaned = "62" + cleaned[1:]
	}
	return cleaned
}

func (s *ValoService) initExistingClients() {
	var accounts []models.ValoAccount
	s.db.Find(&accounts)

	devices, err := s.container.GetAllDevices(context.Background())
	if err != nil {
		log.Printf("[Valo] Failed to get devices from store: %v", err)
		return
	}

	for _, account := range accounts {
		normAccountNum := normalizePhoneNumber(account.PhoneNumber)
		found := false
		for _, device := range devices {
			if device.ID != nil {
				normDeviceUser := normalizePhoneNumber(device.ID.User)
				if normDeviceUser == normAccountNum || strings.HasPrefix(device.ID.String(), normAccountNum) {
					// Connect every account that still has a session; the event
					// handler writes the real status (Connected/LoggedOut/etc).
					s.startClient(device, account.PhoneNumber)
					found = true
					break
				}
			}
		}
		// DB says connected but the session is gone (e.g. logged out while we
		// were down) — pairing is dead, needs re-add.
		if !found && account.Status == StatusConnected {
			s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", account.PhoneNumber).Update("status", StatusLoggedOut)
		}
	}
}

func (s *ValoService) createAndRegisterClient(deviceStore *store.Device, phoneNumber string) *whatsmeow.Client {
	clientLog := waLog.Stdout("Client", "WARN", true)
	client := whatsmeow.NewClient(deviceStore, clientLog)

	// Per-account proxy overrides the global WHATSAPP_PROXY_URL so accounts
	// don't all share one exit IP (shared IPs are a spam flag).
	proxyURL := s.whatsAppProxyURL
	var account models.ValoAccount
	if err := s.db.Where("phone_number = ?", phoneNumber).Select("proxy_url").First(&account).Error; err == nil && account.ProxyURL != "" {
		proxyURL = account.ProxyURL
	}
	if proxyURL != "" {
		if err := client.SetProxyAddress(proxyURL); err != nil {
			log.Printf("[Valo] Invalid proxy URL for %s: %v", phoneNumber, err)
		} else {
			log.Printf("[Valo] Using proxy %s for %s", proxyURL, phoneNumber)
		}
	}

	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Connected:
			log.Printf("[Valo] Account connected: %s", phoneNumber)
			s.updateStatus(phoneNumber, StatusConnected)
		case *events.Disconnected:
			// Transient: whatsmeow auto-reconnects (EnableAutoReconnect).
			log.Printf("[Valo] Account disconnected (will auto-reconnect): %s", phoneNumber)
			s.updateStatus(phoneNumber, StatusDisconnected)
		case *events.LoggedOut:
			// Device removed from the phone / banned for an unknown reason.
			// Session is dead — delete the store so it must be re-paired.
			log.Printf("[Valo] Account logged out (%v): %s — device store deleted, re-pair required", v.Reason, phoneNumber)
			s.removeClient(phoneNumber, client)
			if client.Store != nil {
				_ = client.Store.Delete(context.Background())
			}
			s.updateStatus(phoneNumber, StatusLoggedOut)
		case *events.TemporaryBan:
			log.Printf("[Valo] Account temporarily banned (%v, expires after %s): %s — stop sending on this number until expiry", v.Code, v.Expire, phoneNumber)
			s.removeClient(phoneNumber, client)
			s.updateStatus(phoneNumber, StatusTempBanned)
		case *events.StreamReplaced:
			// Same device got paired elsewhere — this connection is obsolete.
			log.Printf("[Valo] Stream replaced (device paired elsewhere): %s", phoneNumber)
			s.removeClient(phoneNumber, client)
			s.updateStatus(phoneNumber, StatusLoggedOut)
		case *events.ClientOutdated:
			log.Printf("[Valo] whatsmeow client outdated for %s — upgrade the whatsmeow dependency and redeploy", phoneNumber)
			s.removeClient(phoneNumber, client)
			s.updateStatus(phoneNumber, StatusDisconnected)
		case *events.CATRefreshError:
			log.Printf("[Valo] CAT refresh failed for %s: %v", phoneNumber, v.Error)
			s.removeClient(phoneNumber, client)
			s.updateStatus(phoneNumber, StatusDisconnected)
		case *events.ConnectFailure:
			log.Printf("[Valo] Connect failure (%v %s): %s", v.Reason, v.Message, phoneNumber)
			s.removeClient(phoneNumber, client)
			s.updateStatus(phoneNumber, StatusDisconnected)
		case *events.Message:
			if v.Info.IsFromMe {
				break
			}

			// Quietly skip messages received on accounts other than dedicated ARIA_WHATSAPP_NUMBER
			if s.ariaWhatsAppNumber == "" {
				break
			}

			normClient := normalizePhoneNumber(phoneNumber)
			normAria := normalizePhoneNumber(s.ariaWhatsAppNumber)
			if normClient != normAria {
				break
			}

			senderStr := v.Info.Sender.String()
			log.Printf("[Valo] Incoming message event on Aria account %s from %s", phoneNumber, senderStr)

			if v.Info.IsGroup {
				log.Printf("[Valo] Message from %s is a group message, skipping", senderStr)
				break
			}

			senderID := v.Info.Sender.User

			if v.Info.Sender.Server == waTypes.HiddenUserServer {
				// 1. Check if SenderAlt contains the phone number JID
				if !v.Info.SenderAlt.IsEmpty() && v.Info.SenderAlt.User != "" {
					senderID = v.Info.SenderAlt.User
					log.Printf("[Valo] Resolved LID %s to Phone Number %s via SenderAlt", v.Info.Sender.User, senderID)
				} else {
					// 2. Query whatsmeow SQLStore LID table
					pn, err := client.Store.LIDs.GetPNForLID(context.Background(), v.Info.Sender.ToNonAD())
					if err == nil && !pn.IsEmpty() && pn.User != "" {
						senderID = pn.User
						log.Printf("[Valo] Resolved LID %s to Phone Number %s via LIDs Store", v.Info.Sender.User, senderID)
					} else {
						log.Printf("[Valo] Warning: Could not resolve LID %s to Phone Number. Forwarding LID JID.", v.Info.Sender.User)
						senderID = v.Info.Sender.ToNonAD().String()
					}
				}
			}

			// Mark the message as read — humans do this, bots that never do are a flag.
			_ = client.MarkRead(context.Background(), []waTypes.MessageID{v.Info.ID}, time.Now(), v.Info.Chat, v.Info.Sender)

			text := v.Message.GetConversation()
			if text == "" {
				if ext := v.Message.GetExtendedTextMessage(); ext != nil {
					text = ext.GetText()
				} else if img := v.Message.GetImageMessage(); img != nil {
					text = img.GetCaption()
				} else if doc := v.Message.GetDocumentMessage(); doc != nil {
					text = doc.GetCaption()
				} else if vid := v.Message.GetVideoMessage(); vid != nil {
					text = vid.GetCaption()
				}
			}

			// Supported media (PDF/gambar) is downloaded and forwarded to Grupia
			// alongside the caption. Unsupported media types keep the old
			// text-only behavior.
			media, mediaSupported := s.extractSupportedMedia(client, v.Message)
			if (text == "" && len(media) == 0) || (!mediaSupported && text == "") {
				log.Printf("[Valo] Non-text or unsupported media message received from %s, sending auto-reply notice", senderID)
				// One combined message — sending the same two-message template to
				// many users is a classic SentTooManySameMessage spam trigger.
				notice := "⚠️ Aria di WhatsApp dapat memproses teks, gambar (JPEG/PNG/WebP), dan dokumen PDF maksimal 5MB. Untuk berkas rekam medis berukuran besar, silakan gunakan website Casemix Pintar: https://casemixpintar.id"
				go func(acc, target, msg string) {
					if err := s.sendNow(acc, target, msg); err != nil {
						log.Printf("[Valo] Failed to send media auto-reply notice to %s: %v", target, err)
					}
				}(phoneNumber, senderID, notice)
				break
			}

			log.Printf("[Aria-WA] Forwarding message from %s to Grupia API: %q (media: %d)", senderID, text, len(media))
			go s.forwardToGrupia(phoneNumber, senderID, text, media)
		}
	})

	return client
}

func (s *ValoService) startClient(deviceStore *store.Device, phoneNumber string) {
	client := s.createAndRegisterClient(deviceStore, phoneNumber)

	err := client.Connect()
	if err != nil {
		log.Printf("Failed to connect client %s: %v\n", phoneNumber, err)
		return
	}

	s.clientsMutex.Lock()
	s.clients[phoneNumber] = client
	s.clientsMutex.Unlock()
}

// updateStatus writes the account status; the row may not exist yet mid-pairing.
func (s *ValoService) updateStatus(phoneNumber, status string) {
	if err := s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("status", status).Error; err != nil {
		log.Printf("[Valo] Failed to update status for %s to %s: %v", phoneNumber, status, err)
	}
}

// removeClient drops the client from the live map and disconnects it.
func (s *ValoService) removeClient(phoneNumber string, client *whatsmeow.Client) {
	s.clientsMutex.Lock()
	if c, ok := s.clients[phoneNumber]; ok && c == client {
		delete(s.clients, phoneNumber)
	}
	s.clientsMutex.Unlock()
	client.Disconnect()
}

// SyncStatuses reconciles DB statuses with the live clients. Events can be
// missed (crash, restart), so status is also derived from
// client.IsConnected()/IsLoggedIn() on demand.
func (s *ValoService) SyncStatuses() {
	var accounts []models.ValoAccount
	s.db.Find(&accounts)

	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()

	for _, account := range accounts {
		client, ok := s.clients[account.PhoneNumber]
		if ok {
			status := StatusDisconnected
			if client.IsConnected() && client.IsLoggedIn() {
				status = StatusConnected
			}
			if account.Status != status && account.Status != StatusTempBanned {
				s.updateStatus(account.PhoneNumber, status)
			}
		} else if account.Status == StatusConnected {
			s.updateStatus(account.PhoneNumber, StatusDisconnected)
		}
	}
}

// resolveClient picks the sending client: explicit sender number, else the
// default account. No random fallback — blasting through an arbitrary account
// (e.g. the Aria chatbot number) is what gets numbers spam-flagged.
func (s *ValoService) resolveClient(senderNumber string) (*whatsmeow.Client, string, error) {
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()

	if senderNumber != "" {
		if client, ok := s.clients[senderNumber]; ok {
			return client, senderNumber, nil
		}
		return nil, "", fmt.Errorf("sender account %s is not connected", senderNumber)
	}

	var defAccount models.ValoAccount
	if err := s.db.Where("is_default = ? AND status = ?", true, StatusConnected).First(&defAccount).Error; err == nil {
		if client, ok := s.clients[defAccount.PhoneNumber]; ok {
			return client, defAccount.PhoneNumber, nil
		}
	}
	return nil, "", fmt.Errorf("no sender_number given and no connected default account set")
}

func (s *ValoService) getPacer(account string) *sendPacer {
	s.pacersMutex.Lock()
	defer s.pacersMutex.Unlock()
	if p, ok := s.pacers[account]; ok {
		return p
	}
	p := &sendPacer{}
	s.pacers[account] = p
	return p
}

// pace blocks until this account is allowed to send again, then reserves the
// next slot (min gap + jitter). Sleeping while holding the lock is what
// serializes concurrent sends for the same account.
func (s *ValoService) pace(account string) {
	p := s.getPacer(account)
	p.mu.Lock()
	defer p.mu.Unlock()
	if wait := time.Until(p.next); wait > 0 {
		time.Sleep(wait)
	}
	gap := sendMinGap
	if sendJitter > 0 {
		gap += time.Duration(rand.Int63n(int64(sendJitter)))
	}
	p.next = time.Now().Add(gap)
}

// sendNow sends a text message without pacing — used for conversational Aria
// replies, which must stay fast. Blasts via the public API go through pace().
func (s *ValoService) sendNow(senderNumber, to, message string) error {
	client, _, err := s.resolveClient(senderNumber)
	if err != nil {
		return err
	}

	if !strings.Contains(to, "@") {
		to = to + "@s.whatsapp.net"
	}
	targetJID, err := waTypes.ParseJID(to)
	if err != nil {
		return err
	}

	_, err = client.SendMessage(context.Background(), targetJID, &waE2E.Message{
		Conversation: proto.String(message),
	})
	return err
}

func (s *ValoService) getOrCreateFreshClient(phoneNumber string) (*whatsmeow.Client, error) {
	phoneNumber = normalizePhoneNumber(phoneNumber)
	s.clientsMutex.Lock()
	defer s.clientsMutex.Unlock()

	if existingClient, exists := s.clients[phoneNumber]; exists {
		if existingClient.IsConnected() && existingClient.IsLoggedIn() {
			return nil, fmt.Errorf("already logged in")
		}
		// Clean up old / stale / disconnected / deleted client
		existingClient.Disconnect()
		if existingClient.Store != nil {
			_ = existingClient.Store.Delete(context.Background())
		}
		delete(s.clients, phoneNumber)
	}

	// Create new account record if doesn't exist
	var account models.ValoAccount
	res := s.db.Where("phone_number = ?", phoneNumber).First(&account)
	if res.Error != nil {
		account = models.ValoAccount{
			PhoneNumber: phoneNumber,
			Status:      StatusDisconnected,
		}
		s.db.Create(&account)
	}

	deviceStore := s.container.NewDevice()
	client := s.createAndRegisterClient(deviceStore, phoneNumber)
	s.clients[phoneNumber] = client

	return client, nil
}

func (s *ValoService) GetQR(phoneNumber string) (string, error) {
	client, err := s.getOrCreateFreshClient(phoneNumber)
	if err != nil {
		return "", err
	}

	qrChan, err := client.GetQRChannel(context.Background())
	if err != nil {
		return "", err
	}
	err = client.Connect()
	if err != nil {
		return "", err
	}

	select {
	case qrEvent := <-qrChan:
		if qrEvent.Event == "code" {
			png, err := qrcode.Encode(qrEvent.Code, qrcode.Medium, 256)
			if err != nil {
				return "", err
			}
			base64QR := "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)

			go func() {
				for evt := range qrChan {
					if evt.Event == "success" {
						s.updateStatus(phoneNumber, StatusConnected)
					}
				}
			}()

			return base64QR, nil
		}
	case <-time.After(15 * time.Second):
		return "", fmt.Errorf("timeout waiting for QR")
	}

	return "", fmt.Errorf("failed to get QR")
}

func (s *ValoService) GetPairingCode(phoneNumber string) (string, error) {
	client, err := s.getOrCreateFreshClient(phoneNumber)
	if err != nil {
		return "", err
	}

	qrChan, err := client.GetQRChannel(context.Background())
	if err != nil {
		return "", err
	}
	err = client.Connect()
	if err != nil {
		return "", err
	}

	select {
	case qrEvent := <-qrChan:
		if qrEvent.Event == "code" || qrEvent.Event == "success" {
			code, err := client.PairPhone(context.Background(), phoneNumber, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
			if err != nil {
				return "", fmt.Errorf("failed to get pairing code: %w", err)
			}

			go func() {
				for evt := range qrChan {
					if evt.Event == "success" {
						s.updateStatus(phoneNumber, StatusConnected)
					}
				}
			}()

			return code, nil
		}
	case <-time.After(15 * time.Second):
		return "", fmt.Errorf("timeout waiting for connection")
	}

	return "", fmt.Errorf("failed to get pairing code")
}

func (s *ValoService) SendMessage(senderNumber string, to string, message string, imageUrl string, imageBase64 string, caption string) error {
	client, account, err := s.resolveClient(senderNumber)
	if err != nil {
		return err
	}
	// Public API sends are paced per account (min gap + jitter) to avoid
	// burst-triggered spam flags. Conversational Aria replies use sendNow.
	s.pace(account)

	if !strings.Contains(to, "@") {
		to = to + "@s.whatsapp.net"
	}
	targetJID, err := waTypes.ParseJID(to)
	if err != nil {
		return err
	}

	var msg *waE2E.Message

	if imageUrl != "" || imageBase64 != "" {
		var imgBytes []byte
		var err error

		if imageUrl != "" {
			// Download image with 10s timeout
			httpClient := &http.Client{Timeout: 10 * time.Second}
			resp, err := httpClient.Get(imageUrl)
			if err != nil {
				return fmt.Errorf("failed to download image: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to download image, status code: %d", resp.StatusCode)
			}

			imgBytes, err = io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("failed to read downloaded image data: %w", err)
			}
		} else {
			// Decode base64 image
			rawStr := imageBase64
			if idx := strings.Index(rawStr, ","); idx != -1 {
				rawStr = rawStr[idx+1:]
			}
			imgBytes, err = base64.StdEncoding.DecodeString(rawStr)
			if err != nil {
				return fmt.Errorf("failed to decode base64 image: %w", err)
			}
		}

		// Size check (Max 5MB)
		if len(imgBytes) > 5*1024*1024 {
			return fmt.Errorf("image size exceeds 5MB limit")
		}

		// Mimetype check
		contentType := http.DetectContentType(imgBytes)
		allowedTypes := map[string]bool{
			"image/jpeg": true,
			"image/png":  true,
			"image/webp": true,
			"image/gif":  true,
		}
		if !allowedTypes[contentType] {
			return fmt.Errorf("unsupported image format: %s. Allowed formats: JPEG, PNG, WEBP, GIF", contentType)
		}

		// Upload image to WhatsApp
		uploadResp, err := client.Upload(context.Background(), imgBytes, whatsmeow.MediaImage)
		if err != nil {
			return fmt.Errorf("failed to upload image to WhatsApp: %w", err)
		}

		// Determine caption (fallback to message if caption is empty)
		finalCaption := caption
		if finalCaption == "" {
			finalCaption = message
		}

		// Build ImageMessage
		imageMsg := &waE2E.ImageMessage{
			URL:           &uploadResp.URL,
			DirectPath:    &uploadResp.DirectPath,
			MediaKey:      uploadResp.MediaKey,
			FileEncSHA256: uploadResp.FileEncSHA256,
			FileSHA256:    uploadResp.FileSHA256,
			FileLength:    &uploadResp.FileLength,
			Mimetype:      proto.String(contentType),
		}

		if finalCaption != "" {
			imageMsg.Caption = proto.String(finalCaption)
		}

		msg = &waE2E.Message{
			ImageMessage: imageMsg,
		}
	} else {
		// Standard text message
		msg = &waE2E.Message{
			Conversation: proto.String(message),
		}
	}

	_, err = client.SendMessage(context.Background(), targetJID, msg)
	return err
}

func (s *ValoService) Logout(phoneNumber string) error {
	phoneNumber = normalizePhoneNumber(phoneNumber)
	s.clientsMutex.Lock()
	client, exists := s.clients[phoneNumber]
	if exists {
		delete(s.clients, phoneNumber)
	}
	s.clientsMutex.Unlock()

	if exists && client != nil {
		_ = client.Logout(context.Background())
		client.Disconnect()
		if client.Store != nil {
			_ = client.Store.Delete(context.Background())
		}
	} else {
		// No live client (e.g. after a restart) — still clean up the stored
		// session so device rows don't orphan in the whatsmeow tables.
		devices, err := s.container.GetAllDevices(context.Background())
		if err == nil {
			for _, device := range devices {
				if device.ID != nil && normalizePhoneNumber(device.ID.User) == phoneNumber {
					_ = device.Delete(context.Background())
				}
			}
		}
	}

	s.updateStatus(phoneNumber, StatusDisconnected)
	return nil
}

// forwardMedia is one media item forwarded to the Grupia webhook.
type forwardMedia struct {
	MimeType string `json:"mime_type"`
	Filename string `json:"filename"`
	Base64   string `json:"base64"`
}

var valoSupportedMediaMimes = map[string]bool{
	"application/pdf": true,
	"image/jpeg":      true,
	"image/png":       true,
	"image/webp":      true,
}

// Must match Grupia's AriaMaxAttachmentSize (5MB raw) — the product cap for
// chat attachments. Grupia inlines the file into one Gemini request; base64
// expands 4/3×, so keeping well under Gemini's ~20MB request limit.
const valoMaxForwardMediaSize = 5 * 1024 * 1024

// normalizeWAMime lowercases a WhatsApp mimetype and strips parameters
// (e.g. "application/PDF; name=x.pdf" → "application/pdf").
func normalizeWAMime(mime string) string {
	mime = strings.ToLower(strings.TrimSpace(mime))
	if idx := strings.Index(mime, ";"); idx != -1 {
		mime = mime[:idx]
	}
	return mime
}

// extractSupportedMedia downloads supported media (images, PDF documents)
// from an incoming WhatsApp message. Returns (media, supported): supported
// reports whether the media type is one we handle — when false the caller
// falls back to text-only forwarding (or the notice when there is no text).
func (s *ValoService) extractSupportedMedia(client *whatsmeow.Client, msg *waE2E.Message) ([]forwardMedia, bool) {
	if img := msg.GetImageMessage(); img != nil {
		mime := normalizeWAMime(img.GetMimetype())
		if mime == "" {
			mime = "image/jpeg" // WhatsApp re-encodes images to JPEG by default
		}
		ext := ".jpg"
		switch mime {
		case "image/png":
			ext = ".png"
		case "image/webp":
			ext = ".webp"
		}
		return s.downloadAsForwardMedia(client, img, mime, fmt.Sprintf("gambar_wa_%d%s", time.Now().Unix(), ext), img.GetFileLength())
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		mime := normalizeWAMime(doc.GetMimetype())
		if mime == "" {
			mime = "application/pdf"
		}
		if mime != "application/pdf" {
			return nil, false // non-PDF documents are not supported downstream
		}
		name := doc.GetFileName()
		if name == "" {
			name = fmt.Sprintf("dokumen_wa_%d.pdf", time.Now().Unix())
		}
		return s.downloadAsForwardMedia(client, doc, mime, name, doc.GetFileLength())
	}
	if msg.GetVideoMessage() != nil || msg.GetAudioMessage() != nil || msg.GetStickerMessage() != nil {
		return nil, false
	}
	return nil, true // no media at all — plain text message
}

func (s *ValoService) downloadAsForwardMedia(client *whatsmeow.Client, dl whatsmeow.DownloadableMessage, mime, filename string, fileLength uint64) ([]forwardMedia, bool) {
	if !valoSupportedMediaMimes[mime] {
		return nil, false
	}
	if fileLength > valoMaxForwardMediaSize {
		log.Printf("[Valo] Skipping media %s: %d bytes exceeds %d limit", filename, fileLength, valoMaxForwardMediaSize)
		return nil, false
	}
	data, err := client.Download(context.Background(), dl)
	if err != nil {
		// Download failure degrades to text-only forwarding, not a hard error.
		log.Printf("[Valo] Failed to download media %s: %v", filename, err)
		return nil, true
	}
	log.Printf("[Valo] Downloaded media %s (%s, %d bytes)", filename, mime, len(data))
	return []forwardMedia{{MimeType: mime, Filename: filename, Base64: base64.StdEncoding.EncodeToString(data)}}, true
}

// forwardToGrupia sends an incoming WhatsApp message (text + optional media)
// to Grupia API for Aria processing and sends the AI reply back to the sender.
func (s *ValoService) forwardToGrupia(senderAccount, fromNumber, text string, media []forwardMedia) {
	if s.grupiaAPIURL == "" {
		log.Println("[Aria-WA] GRUPIA_API_URL not configured, skipping forward")
		return
	}

	// Prepare target JID for typing presence indicator
	targetJID := waTypes.NewJID(fromNumber, waTypes.DefaultUserServer)
	if strings.Contains(fromNumber, "@") {
		if j, err := waTypes.ParseJID(fromNumber); err == nil {
			targetJID = j
		}
	}

	s.clientsMutex.RLock()
	client, exists := s.clients[senderAccount]
	s.clientsMutex.RUnlock()

	// Send typing presence ("ketik...") to WhatsApp chat while Grupia AI processes
	if exists && client != nil && client.IsConnected() {
		_ = client.SendChatPresence(context.Background(), targetJID, waTypes.ChatPresenceComposing, waTypes.ChatPresenceMediaText)
		defer func() {
			_ = client.SendChatPresence(context.Background(), targetJID, waTypes.ChatPresencePaused, waTypes.ChatPresenceMediaText)
		}()
	}

	payload := struct {
		From          string         `json:"from"`
		Message       string         `json:"message"`
		SenderAccount string         `json:"sender_account"`
		Media         []forwardMedia `json:"media,omitempty"`
	}{
		From:          fromNumber,
		Message:       text,
		SenderAccount: senderAccount,
		Media:         media,
	}
	bodyData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[Aria-WA] Failed to marshal payload: %v", err)
		return
	}

	req, err := http.NewRequest("POST", s.grupiaAPIURL+"/api/aria-whatsapp/webhook/incoming", bytes.NewBuffer(bodyData))
	if err != nil {
		log.Printf("[Aria-WA] Failed to create request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if s.grupiaWebhookKey != "" {
		req.Header.Set("X-Valo-Webhook-Key", s.grupiaWebhookKey)
	}

	httpClient := &http.Client{Timeout: 120 * time.Second} // Aria can take time to respond
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("[Aria-WA] Failed to forward to Grupia API: %v", err)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[Aria-WA] Failed to read Grupia API response: %v", err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Aria-WA] Grupia API returned status %d: %s", resp.StatusCode, string(respBody))
		return
	}

	var webhookResp struct {
		Reply          string `json:"reply"`
		DocumentBase64 string `json:"document_base64,omitempty"`
		DocumentName   string `json:"document_name,omitempty"`
	}
	if err := json.Unmarshal(respBody, &webhookResp); err != nil {
		log.Printf("[Aria-WA] Failed to parse Grupia API response: %v", err)
		return
	}

	if webhookResp.Reply != "" {
		// Send the AI text reply back to the WhatsApp sender
		if err := s.sendNow(senderAccount, fromNumber, webhookResp.Reply); err != nil {
			log.Printf("[Aria-WA] Failed to send reply to %s: %v", fromNumber, err)
		}
	}

	if webhookResp.DocumentBase64 != "" {
		docBytes, err := base64.StdEncoding.DecodeString(webhookResp.DocumentBase64)
		if err == nil && len(docBytes) > 0 {
			docName := webhookResp.DocumentName
			if docName == "" {
				docName = "Laporan_Export.xlsx"
			}
			log.Printf("[Aria-WA] Sending document attachment %s (%d bytes) to %s", docName, len(docBytes), fromNumber)
			if err := s.SendDocument(senderAccount, fromNumber, docBytes, docName, ""); err != nil {
				log.Printf("[Aria-WA] Failed to send document attachment to %s: %v", fromNumber, err)
			}
		}
	}
}

// SendDocument sends a document attachment (e.g. Excel spreadsheet) via WhatsApp
func (s *ValoService) SendDocument(senderNumber string, to string, docBytes []byte, fileName string, caption string) error {
	client, _, err := s.resolveClient(senderNumber)
	if err != nil {
		return err
	}

	if !strings.Contains(to, "@") {
		to = to + "@s.whatsapp.net"
	}
	targetJID, err := waTypes.ParseJID(to)
	if err != nil {
		return err
	}

	uploadResp, err := client.Upload(context.Background(), docBytes, whatsmeow.MediaDocument)
	if err != nil {
		return fmt.Errorf("failed to upload document to WhatsApp: %w", err)
	}

	docMsg := &waE2E.DocumentMessage{
		URL:           &uploadResp.URL,
		DirectPath:    &uploadResp.DirectPath,
		MediaKey:      uploadResp.MediaKey,
		FileEncSHA256: uploadResp.FileEncSHA256,
		FileSHA256:    uploadResp.FileSHA256,
		FileLength:    &uploadResp.FileLength,
		Mimetype:      proto.String("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
		FileName:      proto.String(fileName),
	}
	if caption != "" {
		docMsg.Caption = proto.String(caption)
	}

	msg := &waE2E.Message{
		DocumentMessage: docMsg,
	}

	_, err = client.SendMessage(context.Background(), targetJID, msg)
	return err
}
