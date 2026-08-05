package main

import (
	"log"

	"valo-engine/config"
	_ "valo-engine/docs"
	"valo-engine/internal/models"
	"valo-engine/internal/services"
	"valo-engine/routes"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/gofiber/fiber/v3/middleware/recover"
)

// @title Valo Engine API
// @version 1.0
// @description Decoupled standalone microservice for Valo Engine (WhatsApp Bot & Messaging).
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.email support@valoengine.io

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:8083
// @BasePath /

// @securityDefinitions.apikey ValoApiKey
// @in header
// @name X-Valo-Key
// @description Enter valid API key for Valo Engine public messaging endpoints

// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
// @description Enter Bearer token for Valo Engine admin endpoints

// @securityDefinitions.apikey ValoAdminKey
// @in header
// @name X-Valo-Admin-Key
// @description Enter Master Admin Key for Valo Engine admin endpoints
func main() {
	// 1. Initialize configuration and database
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	db := config.InitDB(cfg)

	// Auto-migrate tables
	if err := db.AutoMigrate(
		&models.ValoAccount{},
		&models.ValoApiKey{},
	); err != nil {
		log.Printf("Warning: AutoMigrate encountered error: %v", err)
	}

	// 2. Initialize Valo Service
	valoService := services.NewValoService(db, cfg)

	// Log Aria WhatsApp status
	if cfg.AriaWhatsAppNumber != "" {
		log.Printf("🤖 Aria WhatsApp chatbot enabled for number: %s", cfg.AriaWhatsAppNumber)
	} else {
		log.Println("ℹ️  Aria WhatsApp chatbot disabled (ARIA_WHATSAPP_NUMBER not set)")
	}

	// 3. Initialize Fiber App
	app := fiber.New(fiber.Config{
		AppName: "Valo Engine",
	})

	// Middleware
	app.Use(logger.New())
	app.Use(recover.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"*"},
		AllowHeaders: []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Valo-Key", "X-Valo-Admin-Key"},
		AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
	}))

	// 4. Register Routes
	routes.RegisterRoutes(app, db, valoService, cfg.ValoAdminKey)

	// 5. Start Server
	port := cfg.Port
	if port == "" {
		port = "8083"
	}

	log.Printf("🚀 Valo Engine starting on port %s...", port)
	if err := app.Listen(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
