package controllers

import (
	"crypto/rand"
	"encoding/hex"

	"valo-engine/internal/models"
	"valo-engine/internal/services"

	"github.com/gofiber/fiber/v3"
	"gorm.io/gorm"
)

type AddAccountRequest struct {
	PhoneNumber string `json:"phone_number" example:"628123456789"`
}

type AddAccountResponse struct {
	QrCode  string `json:"qr_code" example:"data:image/png;base64,iVBORw0KGgoAAA..."`
	Message string `json:"message" example:"Scan this QR code with WhatsApp"`
}

type GenerateApiKeyRequest struct {
	AppName string `json:"app_name" example:"Aplikasi Antrean"`
}

type ValoAccountsResponse struct {
	Data []models.ValoAccount `json:"data"`
}

type ValoApiKeysResponse struct {
	Data []models.ValoApiKey `json:"data"`
}

type ValoApiKeyResponse struct {
	Data models.ValoApiKey `json:"data"`
}

type MessageResponse struct {
	Message string `json:"message" example:"Success"`
}

type ErrorResponse struct {
	Error string `json:"error" example:"error description"`
}

type ValoAdminController struct {
	db          *gorm.DB
	valoService *services.ValoService
}

func NewValoAdminController(db *gorm.DB, valoService *services.ValoService) *ValoAdminController {
	return &ValoAdminController{
		db:          db,
		valoService: valoService,
	}
}

// ListAccounts lists all WhatsApp accounts
// @Summary List WhatsApp accounts
// @Description Retrieve a list of all WhatsApp accounts connected to Valo engine
// @Tags Valo Engine
// @Produce json
// @Success 200 {object} ValoAccountsResponse "Success"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Security BearerAuth
// @Router /valo/accounts [get]
func (c *ValoAdminController) ListAccounts(ctx fiber.Ctx) error {
	var accounts []models.ValoAccount
	c.db.Find(&accounts)
	return ctx.JSON(fiber.Map{"data": accounts})
}

// AddAccount initiates WhatsApp pairing and gets QR
// @Summary Add WhatsApp account / Get QR
// @Description Initiate pairing for a WhatsApp phone number and return the QR Code
// @Tags Valo Engine
// @Accept json
// @Produce json
// @Param request body AddAccountRequest true "Add Account Request"
// @Success 200 {object} AddAccountResponse "Success"
// @Failure 400 {object} ErrorResponse "Bad Request"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 500 {object} ErrorResponse "Internal Server Error"
// @Security BearerAuth
// @Router /valo/accounts/add [post]
func (c *ValoAdminController) AddAccount(ctx fiber.Ctx) error {
	var req AddAccountRequest
	if err := ctx.Bind().JSON(&req); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	qr, err := c.valoService.GetQR(req.PhoneNumber)
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return ctx.JSON(fiber.Map{"qr_code": qr, "message": "Scan this QR code with WhatsApp"})
}

// LogoutAccount logs out a WhatsApp account
// @Summary Logout WhatsApp account
// @Description Log out a WhatsApp account and terminate its active session
// @Tags Valo Engine
// @Produce json
// @Param phone_number path string true "Phone Number"
// @Success 200 {object} MessageResponse "Success"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 500 {object} ErrorResponse "Internal Server Error"
// @Security BearerAuth
// @Router /valo/accounts/{phone_number}/logout [post]
func (c *ValoAdminController) LogoutAccount(ctx fiber.Ctx) error {
	phoneNumber := ctx.Params("phone_number")
	if err := c.valoService.Logout(phoneNumber); err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return ctx.JSON(fiber.Map{"message": "Logged out successfully"})
}

// SetDefaultAccount sets a WhatsApp account as default
// @Summary Set default WhatsApp account
// @Description Set a WhatsApp account as the default fallback for sending messages
// @Tags Valo Engine
// @Produce json
// @Param phone_number path string true "Phone Number"
// @Success 200 {object} MessageResponse "Success"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Security BearerAuth
// @Router /valo/accounts/{phone_number}/default [post]
func (c *ValoAdminController) SetDefaultAccount(ctx fiber.Ctx) error {
	phoneNumber := ctx.Params("phone_number")

	c.db.Model(&models.ValoAccount{}).Where("1 = 1").Update("is_default", false)
	c.db.Model(&models.ValoAccount{}).Where("phone_number = ?", phoneNumber).Update("is_default", true)

	return ctx.JSON(fiber.Map{"message": "Default account updated"})
}

// GenerateApiKey generates a new API key
// @Summary Generate API Key
// @Description Generate a new API Key for third-party applications to access public endpoints
// @Tags Valo Engine
// @Accept json
// @Produce json
// @Param request body GenerateApiKeyRequest true "Generate API Key Request"
// @Success 200 {object} ValoApiKeyResponse "Success"
// @Failure 400 {object} ErrorResponse "Bad Request"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Security BearerAuth
// @Router /valo/apikeys [post]
func (c *ValoAdminController) GenerateApiKey(ctx fiber.Ctx) error {
	var req GenerateApiKeyRequest
	if err := ctx.Bind().JSON(&req); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	bytes := make([]byte, 32)
	rand.Read(bytes)
	key := hex.EncodeToString(bytes)

	apiKey := models.ValoApiKey{
		AppName: req.AppName,
		Key:     key,
	}

	c.db.Create(&apiKey)
	return ctx.JSON(fiber.Map{"data": apiKey})
}

// ListApiKeys lists all API keys
// @Summary List API Keys
// @Description Get all API keys generated for third-party integration
// @Tags Valo Engine
// @Produce json
// @Success 200 {object} ValoApiKeysResponse "Success"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Security BearerAuth
// @Router /valo/apikeys [get]
func (c *ValoAdminController) ListApiKeys(ctx fiber.Ctx) error {
	var keys []models.ValoApiKey
	c.db.Find(&keys)
	return ctx.JSON(fiber.Map{"data": keys})
}

// DeleteApiKey deletes an API key (soft delete)
// @Summary Delete API Key
// @Description Soft-delete an API key from the database by ID
// @Tags Valo Engine
// @Produce json
// @Param id path string true "API Key ID"
// @Success 200 {object} MessageResponse "Success"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 404 {object} ErrorResponse "Not Found"
// @Security BearerAuth
// @Router /valo/apikeys/{id} [delete]
func (c *ValoAdminController) DeleteApiKey(ctx fiber.Ctx) error {
	id := ctx.Params("id")

	var apiKey models.ValoApiKey
	if err := c.db.Where("id = ?", id).First(&apiKey).Error; err != nil {
		return ctx.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "API Key not found"})
	}

	if err := c.db.Delete(&apiKey).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return ctx.JSON(fiber.Map{"message": "API Key deleted successfully"})
}
