package sql

import (
	"encoding/json"
	"time"
)

type SandboxClass string
type BillingStatus string
type UsageSessionStatus string

const (
	LINUX_VM                 SandboxClass = "linux-vm"
	CONTAINER                SandboxClass = "container"
	ANDROID                  SandboxClass = "android"
	WINDOWS                  SandboxClass = "windows"
	UNKNOWN_DEFAULT_OPEN_API SandboxClass = "11184809"
)

const (
	BILLING_ACTIVE    BillingStatus = "ACTIVE"
	BILLING_COMPLETED BillingStatus = "COMPLETED"
)

const (
	SESSION_ACTIVE   UsageSessionStatus = "SESSION_ACTIVE"
	SESSION_FINISHED UsageSessionStatus = "SESSION_FINISHED"
)

const (
	TABLE_USAGE_SESSIONS = "usage_sessions"
	TABLE_OUTBOX_EVENTS  = "outbox_events"
)

type UsageSessions struct {
	ID             string `json:"id" column:"id"`
	SandboxID      string `json:"sandboxId" gorm:"column:sandboxId"`
	OrganizationID string `json:"organizationId" gorm:"column:organizationId"`

	StartAt time.Time  `json:"startAt" gorm:"column:startAt"`
	EndAt   *time.Time `json:"endAt" gorm:"column:endAt"`

	Status UsageSessionStatus `json:"status" column:"status"`

	LastBilledAt    *time.Time    `db:"lastBilledAt"`
	BillingStatus   BillingStatus `json:"billingStatus" column:"billingStatus"`
	BillingSequence int64         `db:"billingSequence"`

	CPU    *float64 `json:"cpu" gorm:"column:cpu"`
	GPU    *float64 `json:"gpu" gorm:"column:gpu"`
	RamGB  *float64 `json:"ramGB" gorm:"column:ramGB"`
	DiskGB *float64 `json:"diskGB" gorm:"column:diskGB"`
	Region string   `json:"region" gorm:"column:region"`

	SandboxClass SandboxClass `json:"sandboxClass" gorm:"column:sandboxClass;default:container"`

	RecordedAt time.Time `json:"recordedAt" gorm:"column:recordedAt;default:CURRENT_TIMESTAMP"`
}

type OutboxEvent struct {
	ID        int64  `db:"id"`
	EventID   string `db:"event_id"` // unique — BillingChunkCreated.EventID
	EventType string `db:"event_type"`

	SessionID string `db:"session_id"`
	Sequence  int64  `db:"sequence"` // outbox sequence number

	Payload json.RawMessage `db:"payload"`

	CreatedAt   time.Time  `db:"created_at"`
	PublishedAt *time.Time `db:"published_at"`

	RetryCount int     `db:"retry_count"`
	LastError  *string `db:"last_error"`
}

type ProcessedBillingEvent struct {
	ID            int64     `db:"id"`
	EventID       string    `db:"event_id"`
	SessionID     string    `db:"session_id"`
	Sequence      int64     `db:"sequence"` // outbox sequence number
	TransactionID string    `db:"transaction_id"`
	ProcessedAt   time.Time `db:"processed_at"`
}

type ProcessedOutboxEvent struct {
	ID          int64     `db:"id"`
	EventID     string    `db:"event_id"`
	SessionID   string    `db:"session_id"`
	Sequence    int64     `db:"sequence"` // outbox sequence number
	ProcessedAt time.Time `db:"processed_at"`
}
