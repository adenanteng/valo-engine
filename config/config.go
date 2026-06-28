package config

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Config holds all application configuration
type Config struct {
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string
	Port         string
	Timezone     string
	ValoAdminKey string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	return &Config{
		DBHost:       getEnv("DATABASE_HOST", "localhost"),
		DBPort:       getEnv("DATABASE_PORT", "5432"),
		DBUser:       getEnv("DATABASE_USERNAME", "postgres"),
		DBPassword:   getEnv("DATABASE_PASSWORD", ""),
		DBName:       getEnv("DATABASE_NAME", "valo_engine"),
		DBSSLMode:    getEnv("DATABASE_SSL", "disable"),
		Port:         getEnv("PORT", "8083"),
		Timezone:     getEnv("TIMEZONE", "Asia/Jakarta"),
		ValoAdminKey: getEnv("VALO_ADMIN_KEY", ""),
	}, nil
}

// InitDB initializes the PostgreSQL database connection
func InitDB(cfg *Config) *gorm.DB {
	sslMode := "disable"
	if cfg.DBSSLMode == "true" || cfg.DBSSLMode == "require" {
		sslMode = "require"
	}

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s dbname=%s sslmode=%s TimeZone=Asia/Jakarta",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBName, sslMode,
	)
	if cfg.DBPassword != "" {
		dsn = fmt.Sprintf("%s password=%s", dsn, cfg.DBPassword)
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	log.Printf("Connected to PostgreSQL database: %s@%s:%s/%s", cfg.DBUser, cfg.DBHost, cfg.DBPort, cfg.DBName)

	// Ensure uuid-ossp extension exists for uuid_generate_v4()
	if err := db.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`).Error; err != nil {
		log.Printf("Warning: Failed to create uuid-ossp extension: %v", err)
	}

	return db
}

func getEnv(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}

// GetEnv is exported version for use in other packages
func GetEnv(key, defaultValue string) string {
	return getEnv(key, defaultValue)
}

// GetTimezoneLocation returns the timezone location
func GetTimezoneLocation(tz string) *time.Location {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		log.Printf("Warning: Failed to load timezone %s, using UTC: %v", tz, err)
		return time.UTC
	}
	return loc
}
