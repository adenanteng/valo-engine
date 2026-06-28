package controllers

import (
	"strings"

	"valo-engine/internal/services"

	"github.com/gofiber/fiber/v3"
)

type SendMessageRequest struct {
	To           string `json:"to" example:"628123456789"`
	Message      string `json:"message" example:"Halo, ini adalah pesan uji coba."`
	SenderNumber string `json:"sender_number" example:"628987654321"`
	ImageUrl     string `json:"image_url" example:"https://example.com/image.jpg"`
	ImageBase64  string `json:"image_base64" example:"data:image/png;base64,iVBORw0KGgoAAA..."`
	Caption      string `json:"caption" example:"Ini adalah caption foto"`
}

type ValoPublicController struct {
	valoService *services.ValoService
}

func NewValoPublicController(valoService *services.ValoService) *ValoPublicController {
	return &ValoPublicController{
		valoService: valoService,
	}
}

// SendMessage sends a WhatsApp message using a specific or default bot account
// @Summary Send WhatsApp message
// @Description Send a WhatsApp message (text and/or photo) to a recipient. Authentication via X-Valo-Key header is required.
// @Tags Valo Engine
// @Accept json
// @Produce json
// @Param request body SendMessageRequest true "Send Message Request"
// @Success 200 {object} MessageResponse "Success"
// @Failure 400 {object} ErrorResponse "Bad Request"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 500 {object} ErrorResponse "Internal Server Error"
// @Security ValoApiKey
// @Router /valo/messages/send [post]
func (c *ValoPublicController) SendMessage(ctx fiber.Ctx) error {
	var req SendMessageRequest
	if err := ctx.Bind().JSON(&req); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	if req.To == "" {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "to is required"})
	}

	if req.Message == "" && req.ImageUrl == "" && req.ImageBase64 == "" {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "at least one of message, image_url, or image_base64 is required"})
	}

	err := c.valoService.SendMessage(req.SenderNumber, req.To, req.Message, req.ImageUrl, req.ImageBase64, req.Caption)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "image") || strings.Contains(errMsg, "download") || strings.Contains(errMsg, "decode") || strings.Contains(errMsg, "limit") || strings.Contains(errMsg, "format") || strings.Contains(errMsg, "size") {
			return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": errMsg})
		}
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": errMsg})
	}

	return ctx.JSON(fiber.Map{"message": "Message sent successfully"})
}
