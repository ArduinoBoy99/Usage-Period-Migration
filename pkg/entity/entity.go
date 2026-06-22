package entity

import "time"

// BillingChunkPayload represents the payload structure for billing chunk events
type BillingChunkPayload struct {
	EventID        string    `json:"event_id"`
	SessionID      string    `json:"session_id"`
	Sequence       int64     `json:"sequence"`
	From           time.Time `json:"from"`
	To             time.Time `json:"to"`
	CPU            *float64  `json:"cpu,omitempty"`
	GPU            *float64  `json:"gpu,omitempty"`
	RamGB          *float64  `json:"ram_gb,omitempty"`
	DiskGB         *float64  `json:"disk_gb,omitempty"`
	Region         string    `json:"region"`
	SandboxClass   string    `json:"sandbox_class"`
	OrganizationID string    `json:"organization_id"`
}
