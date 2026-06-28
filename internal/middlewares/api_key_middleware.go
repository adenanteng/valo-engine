package middlewares

import (
	"valo-engine/internal/models"

	"github.com/gofiber/fiber/v3"
	"gorm.io/gorm"
)

func ApiKeyMiddleware(db *gorm.DB) fiber.Handler {
	return func(c fiber.Ctx) error {
		apiKey := c.Get("X-Valo-Key")
		if apiKey == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Missing X-Valo-Key header",
			})
		}

		var keyRecord models.ValoApiKey
		if err := db.Where("key = ? AND is_active = ?", apiKey, true).First(&keyRecord).Error; err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Invalid or inactive API Key",
			})
		}

		c.Locals("valo_app_name", keyRecord.AppName)
		return c.Next()
	}
}
