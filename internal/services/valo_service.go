package services

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

type ValoService struct {
	db                 *gorm.DB
	container          *sqlstore.Container
	clients            map[string]*whatsmeow.Client
	clientsMutex       sync.RWMutex
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
	s.db.Where("status = ?", "CONNECTED").Find(&accounts)

	devices, err := s.container.GetAllDevices(context.Background())
	if err != nil {
		log.Printf("[Valo] Failed to get devices from store: %v", err)
		return
	}

	for _, account := range accounts {
		normAccountNum := normalizePhoneNumber(account.PhoneNumber)
		for _, device := range devices {
			if device.ID != nil {
				normDeviceUser := normalizePhoneNumber(device.ID.User)
				if normDeviceUser == normAccountNum || strings.HasPrefix(device.ID.String(), account.PhoneNumber) {
					s.startClient(device, account.PhoneNumber)
					break
				}
			}
		}
	}
}

func (s *ValoService) createAndRegisterClient(deviceStore *store.Device, phoneNumber string) *whatsmeow.Client {
	clientLog := waLog.Stdout("Client", "WARN", true)
	client := whatsmeow.NewClient(deviceStore, clientLog)

	if s.whatsAppProxyURL != "" {
		if err := client.SetProxyAddress(s.whatsAppProxyURL); err != nil {
			log.Printf("[Valo] Invalid WHATSAPP_PROXY_URL for %s: %v", phoneNumber, err)
		} else {
			log.Printf("[Valo] Using proxy for %s", phoneNumber)
		}
	}

	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Connected:
			log.Printf("[Valo] Account connected: %s", phoneNumber)
			s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("status", "CONNECTED")
		case *events.Disconnected:
			log.Printf("[Valo] Account disconnected: %s", phoneNumber)
			s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("status", "DISCONNECTED")
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

			if text == "" {
				log.Printf("[Valo] Non-text or empty media message received from %s, sending auto-reply notice", senderID)
				notice := "⚠️ Saat ini Aria di WhatsApp hanya dapat memproses pesan berupa teks. Untuk mengunggah dan menganalisis berkas rekam medis/PDF, silakan kunjungi website Casemix Pintar."
				linkMsg := "https://casemixpintar.id"
				go func(acc, target, msg1, msg2 string) {
					if err := s.SendMessage(acc, target, msg1, "", "", ""); err != nil {
						log.Printf("[Valo] Failed to send media auto-reply notice to %s: %v", target, err)
					} else {
						time.Sleep(500 * time.Millisecond)
						if err := s.SendMessage(acc, target, msg2, "", "", ""); err != nil {
							log.Printf("[Valo] Failed to send media auto-reply link to %s: %v", target, err)
						}
					}
				}(phoneNumber, senderID, notice, linkMsg)
				break
			}

			log.Printf("[Aria-WA] Forwarding message from %s to Grupia API: %q", senderID, text)
			go s.forwardToGrupia(phoneNumber, senderID, text)
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

func (s *ValoService) getOrCreateFreshClient(phoneNumber string) (*whatsmeow.Client, error) {
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
			Status:      "DISCONNECTED",
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

	qrChan, _ := client.GetQRChannel(context.Background())
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
						s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("status", "CONNECTED")
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

	qrChan, _ := client.GetQRChannel(context.Background())
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
						s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("status", "CONNECTED")
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
	s.clientsMutex.RLock()
	var client *whatsmeow.Client

	if senderNumber != "" {
		client = s.clients[senderNumber]
	} else {
		var defAccount models.ValoAccount
		if err := s.db.Where("is_default = ? AND status = ?", true, "CONNECTED").First(&defAccount).Error; err == nil {
			client = s.clients[defAccount.PhoneNumber]
		}

		if client == nil {
			for _, c := range s.clients {
				if c.IsConnected() && c.IsLoggedIn() {
					client = c
					break
				}
			}
		}
	}
	s.clientsMutex.RUnlock()

	if client == nil {
		return fmt.Errorf("no connected client available")
	}

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
	}

	s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("status", "DISCONNECTED")
	return nil
}

// forwardToGrupia sends an incoming WhatsApp message to Grupia API for Aria processing
// and sends the AI reply back to the sender.
func (s *ValoService) forwardToGrupia(senderAccount, fromNumber, text string) {
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

	payload := map[string]string{
		"from":           fromNumber,
		"message":        text,
		"sender_account": senderAccount,
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
		if err := s.SendMessage(senderAccount, fromNumber, webhookResp.Reply, "", "", ""); err != nil {
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
	s.clientsMutex.RLock()
	var client *whatsmeow.Client

	if senderNumber != "" {
		client = s.clients[senderNumber]
	} else {
		var defAccount models.ValoAccount
		if err := s.db.Where("is_default = ? AND status = ?", true, "CONNECTED").First(&defAccount).Error; err == nil {
			client = s.clients[defAccount.PhoneNumber]
		}
		if client == nil {
			for _, c := range s.clients {
				if c.IsConnected() && c.IsLoggedIn() {
					client = c
					break
				}
			}
		}
	}
	s.clientsMutex.RUnlock()

	if client == nil {
		return fmt.Errorf("no connected client available")
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
