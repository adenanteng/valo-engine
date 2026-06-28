package routes

import (
	"valo-engine/internal/controllers"
	"valo-engine/internal/middlewares"
	"valo-engine/internal/services"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	httpSwagger "github.com/swaggo/http-swagger"
	"gorm.io/gorm"
)

// RegisterRoutes sets up all routes for Valo Engine standalone microservice
func RegisterRoutes(app *fiber.App, db *gorm.DB, valoService *services.ValoService, adminKey string) {
	adminController := controllers.NewValoAdminController(db, valoService)
	publicController := controllers.NewValoPublicController(valoService)

	// Swagger UI
	app.Get("/swagger/*", adaptor.HTTPHandler(httpSwagger.WrapHandler))
	app.Get("/api/swagger/*", adaptor.HTTPHandler(httpSwagger.WrapHandler))

	// Health check
	healthHandler := func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok", "service": "valo-engine"})
	}
	app.Get("/health", healthHandler)
	app.Get("/api/health", healthHandler)

	// Setup groups for /valo and /api/valo
	setupValoRoutes := func(router fiber.Router) {
		valoGroup := router.Group("/valo")

		// Accounts routes (Protected by AdminAuthMiddleware)
		accountsGroup := valoGroup.Group("/accounts", middlewares.AdminAuthMiddleware(adminKey))
		accountsGroup.Get("", adminController.ListAccounts)
		accountsGroup.Post("/add", adminController.AddAccount)
		accountsGroup.Post("/:phone_number/logout", adminController.LogoutAccount)
		accountsGroup.Post("/:phone_number/default", adminController.SetDefaultAccount)

		// API Keys routes (Protected by AdminAuthMiddleware)
		apikeysGroup := valoGroup.Group("/apikeys", middlewares.AdminAuthMiddleware(adminKey))
		apikeysGroup.Get("", adminController.ListApiKeys)
		apikeysGroup.Post("", adminController.GenerateApiKey)
		apikeysGroup.Delete("/:id", adminController.DeleteApiKey)

		// Public external route (protected by API Key)
		publicGroup := valoGroup.Group("/messages")
		publicGroup.Use(middlewares.ApiKeyMiddleware(db))
		publicGroup.Post("/send", publicController.SendMessage)
	}

	setupValoRoutes(app)
	setupValoRoutes(app.Group("/api"))
}
