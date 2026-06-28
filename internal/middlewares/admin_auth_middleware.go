package middlewares

import (
	"strings"

	"github.com/gofiber/fiber/v3"
)

// AdminAuthMiddleware validates the master admin key for admin routes
func AdminAuthMiddleware(adminKey string) fiber.Handler {
	return func(c fiber.Ctx) error {
		if adminKey == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Admin access disabled: VALO_ADMIN_KEY is not configured on the server",
			})
		}

		clientKey := c.Get("X-Valo-Admin-Key")
		if clientKey == "" {
			authHeader := c.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				clientKey = strings.TrimPrefix(authHeader, "Bearer ")
			}
		}

		if clientKey == "" || clientKey != adminKey {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Invalid or missing admin key. Provide X-Valo-Admin-Key or Authorization: Bearer <key>",
			})
		}

		return c.Next()
	}
}
