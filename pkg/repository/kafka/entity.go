package kafka

import "time"

type BillingChunkCreated struct {
	EventID string `json:"event_id"`

	SessionID      string `json:"session_id"`
	SandboxID      string `json:"sandbox_id"`
	OrganizationID string `json:"organization_id"`

	Sequence int64 `json:"sequence"`

	From time.Time `json:"from"`
	To   time.Time `json:"to"`

	CPU    *float64 `json:"cpu,omitempty"`
	GPU    *float64 `json:"gpu,omitempty"`
	RAMGB  *float64 `json:"ram_gb,omitempty"`
	DiskGB *float64 `json:"disk_gb,omitempty"`

	Region       string `json:"region"`
	RegionType   string `json:"region_type"`
	SandboxClass string `json:"sandbox_class"`
}
