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
	ID             string `json:"id" db:"id"`
	SandboxID      string `json:"sandboxId" db:"sandbox_id"`
	OrganizationID string `json:"organizationId" db:"organization_id"`

	StartAt time.Time  `json:"startAt" db:"start_at"`
	EndAt   *time.Time `json:"endAt" db:"end_at"`

	Status UsageSessionStatus `json:"status" column:"status"`

	LastBilledAt    *time.Time    `db:"last_billed_at"`
	BillingStatus   BillingStatus `json:"billingStatus" db:"billing_status"`
	BillingSequence int64         `db:"billingSequence"`

	CPU    *float64 `json:"cpu" db:"cpu"`
	GPU    *float64 `json:"gpu" db:"gpu"`
	RamGB  *float64 `json:"ramGB" db:"ram_gb"`
	DiskGB *float64 `json:"diskGB" db:"disk_gb"`
	Region string   `json:"region" db:"region"`

	SandboxClass SandboxClass `json:"sandboxClass" db:"sandbox_class"`

	RecordedAt time.Time `json:"recordedAt" db:"recorded_at"`
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
