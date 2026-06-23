package events

import (
	"testing"
	"time"

	"usage-period-migration/pkg/repository/kafka"
)

func TestValidateBillingChunk(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name      string
		chunk     *kafka.BillingChunkCreated
		wantError bool
	}{
		{
			name: "valid chunk passes validation",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "evt-123",
				SessionID:      "sess-456",
				SandboxID:      "sb-789",
				OrganizationID: "org-001",
				Sequence:       1,
				From:           now,
				To:             now.Add(1 * time.Hour),
				Region:         "us-west",
				SandboxClass:   "standard",
			},
			wantError: false,
		},
		{
			name: "chunk with empty event ID fails",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "",
				SessionID:      "sess-456",
				SandboxID:      "sb-789",
				OrganizationID: "org-001",
				Sequence:       1,
				From:           now,
				To:             now.Add(1 * time.Hour),
			},
			wantError: true,
		},
		{
			name: "chunk with invalid time range fails",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "evt-123",
				SessionID:      "sess-456",
				SandboxID:      "sb-789",
				OrganizationID: "org-001",
				Sequence:       1,
				From:           now.Add(1 * time.Hour),
				To:             now,
				Region:         "us-west",
				SandboxClass:   "standard",
			},
			wantError: true,
		},
	}

	s := &outboxPublisherService{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.validateBillingChunk(tt.chunk)
			if (err != nil) != tt.wantError {
				t.Errorf("validateBillingChunk() error = %v, wantErr %v", err, tt.wantError)
			}
		})
	}
}
