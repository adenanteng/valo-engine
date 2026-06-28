package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ValoAccount represents a managed WhatsApp bot account.
type ValoAccount struct {
	ID          uuid.UUID      `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	PhoneNumber string         `gorm:"type:varchar(50);uniqueIndex;not null" json:"phone_number"` // e.g., 6281234567890
	IsDefault   bool           `gorm:"default:false" json:"is_default"`
	Status      string         `gorm:"type:varchar(20);default:'DISCONNECTED'" json:"status"` // DISCONNECTED, CONNECTED
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"`
}

// ValoApiKey represents an API Key given to 3rd party apps to use Valo Engine
type ValoApiKey struct {
	ID        uuid.UUID      `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	AppName   string         `gorm:"type:varchar(100);not null" json:"app_name"`
	Key       string         `gorm:"type:varchar(100);uniqueIndex;not null" json:"key"` // Pre-generated API Key
	IsActive  bool           `gorm:"default:true" json:"is_active"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}
