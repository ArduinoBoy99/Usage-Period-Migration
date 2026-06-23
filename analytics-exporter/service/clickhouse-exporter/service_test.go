package clickhouse_exporter

import (
	"context"
	"testing"
	"time"
)

func TestExportToClickhouse(t *testing.T) {
	tests := []struct {
		name    string
		event   *BillingProcessedEvent
		wantErr bool
	}{
		{
			name: "valid event exports successfully",
			event: &BillingProcessedEvent{
				EventID:        "event-123",
				SessionID:      "session-456",
				Sequence:       1,
				ProcessedAt:    time.Now(),
				MetronomeID:    "metro-789",
				OrganizationID: "org-001",
			},
			wantErr: false,
		},
		{
			name:    "nil event returns error",
			event:   nil,
			wantErr: true,
		},
	}

	s := &analyticsService{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.ExportToClickhouse(context.Background(), tt.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExportToClickhouse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
