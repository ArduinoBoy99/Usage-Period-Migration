package billing

import (
	"log/slog"
	"testing"
	"time"

	"usage-period-migration/pkg/repository/kafka"
)

func TestBuildMetronomePayload(t *testing.T) {
	tests := []struct {
		name      string
		event     *kafka.BillingChunkCreated
		wantError bool
		validate  func(*MetronomePayload) bool
	}{
		{
			name: "valid event builds payload",
			event: &kafka.BillingChunkCreated{
				EventID:        "evt-123",
				SessionID:      "sess-456",
				Sequence:       1,
				OrganizationID: "org-789",
				From:           time.Now(),
				To:             time.Now().Add(1 * time.Hour),
			},
			wantError: false,
			validate: func(p *MetronomePayload) bool {
				return p.TransactionID == "sess-456:1" && p.CustomerID == "org-789"
			},
		},
	}

	s := &billingService{
		logger: slog.Default(),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := s.BuildMetronomePayload(tt.event)
			if (err != nil) != tt.wantError {
				t.Errorf("BuildMetronomePayload() error = %v, wantErr %v", err, tt.wantError)
			}
			if err == nil && !tt.validate(payload) {
				t.Errorf("BuildMetronomePayload() payload validation failed")
			}
		})
	}
}
