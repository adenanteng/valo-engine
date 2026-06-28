package services

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
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
	db           *gorm.DB
	container    *sqlstore.Container
	clients      map[string]*whatsmeow.Client
	clientsMutex sync.RWMutex
}

func NewValoService(db *gorm.DB) *ValoService {
	cfg, _ := config.LoadConfig()

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
		db:        db,
		container: container,
		clients:   make(map[string]*whatsmeow.Client),
	}

	svc.initExistingClients()
	return svc
}

func (s *ValoService) initExistingClients() {
	var accounts []models.ValoAccount
	s.db.Where("status = ?", "CONNECTED").Find(&accounts)

	for _, account := range accounts {
		devices, err := s.container.GetAllDevices(context.Background())
		if err != nil {
			continue
		}

		for _, device := range devices {
			if device.ID != nil {
				jid := device.ID.String()
				if strings.HasPrefix(jid, account.PhoneNumber) {
					s.startClient(device, account.PhoneNumber)
					break
				}
			}
		}
	}
}

func (s *ValoService) startClient(deviceStore *store.Device, phoneNumber string) {
	clientLog := waLog.Stdout("Client", "WARN", true)
	client := whatsmeow.NewClient(deviceStore, clientLog)

	client.AddEventHandler(func(evt interface{}) {
		switch evt.(type) {
		case *events.Connected:
			s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("status", "CONNECTED")
		case *events.Disconnected:
			s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("status", "DISCONNECTED")
		}
	})

	err := client.Connect()
	if err != nil {
		fmt.Printf("Failed to connect client %s: %v\n", phoneNumber, err)
		return
	}

	s.clientsMutex.Lock()
	s.clients[phoneNumber] = client
	s.clientsMutex.Unlock()
}

func (s *ValoService) GetQR(phoneNumber string) (string, error) {
	s.clientsMutex.RLock()
	client, exists := s.clients[phoneNumber]
	s.clientsMutex.RUnlock()

	if exists && client.IsConnected() && client.IsLoggedIn() {
		return "", fmt.Errorf("already logged in")
	}

	// Create new account if doesn't exist
	var account models.ValoAccount
	res := s.db.Where("phone_number = ?", phoneNumber).First(&account)
	if res.Error != nil {
		account = models.ValoAccount{
			PhoneNumber: phoneNumber,
			Status:      "DISCONNECTED",
		}
		s.db.Create(&account)
	}

	if !exists {
		deviceStore := s.container.NewDevice()
		client = whatsmeow.NewClient(deviceStore, waLog.Stdout("Client", "WARN", true))

		s.clientsMutex.Lock()
		s.clients[phoneNumber] = client
		s.clientsMutex.Unlock()
	}

	qrChan, _ := client.GetQRChannel(context.Background())
	err := client.Connect()
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
	s.clientsMutex.RLock()
	client, exists := s.clients[phoneNumber]
	s.clientsMutex.RUnlock()

	if !exists {
		return fmt.Errorf("client not found")
	}

	err := client.Logout(context.Background())
	if err != nil {
		return err
	}

	s.clientsMutex.Lock()
	delete(s.clients, phoneNumber)
	s.clientsMutex.Unlock()

	s.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("status", "DISCONNECTED")
	return nil
}
