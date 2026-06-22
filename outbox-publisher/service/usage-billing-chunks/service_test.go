package usage_billing_chunks

import (
	"encoding/json"
	"testing"
	"time"

	"usage-period-migration/pkg/repository/kafka"
	"usage-period-migration/pkg/repository/sql"

	"github.com/google/uuid"
)

// TestValidateBillingChunk tests the validation logic
func TestValidateBillingChunk(t *testing.T) {
	service := &outboxPublisherService{}

	tests := []struct {
		name    string
		chunk   *kafka.BillingChunkCreated
		wantErr bool
	}{
		{
			name: "valid outbox chunk",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "event-123",
				SessionID:      "session-456",
				SandboxID:      "sandbox-789",
				OrganizationID: "org-abc",
				Sequence:       1,
				From:           time.Now().Add(-1 * time.Hour),
				To:             time.Now(),
				Region:         "us-east-1",
				RegionType:     "cloud",
				SandboxClass:   "container",
			},
			wantErr: false,
		},
		{
			name: "missing event_id",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "",
				SessionID:      "session-456",
				SandboxID:      "sandbox-789",
				OrganizationID: "org-abc",
				Sequence:       1,
				From:           time.Now().Add(-1 * time.Hour),
				To:             time.Now(),
				Region:         "us-east-1",
				SandboxClass:   "container",
			},
			wantErr: true,
		},
		{
			name: "missing session_id",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "event-123",
				SessionID:      "",
				SandboxID:      "sandbox-789",
				OrganizationID: "org-abc",
				Sequence:       1,
				From:           time.Now().Add(-1 * time.Hour),
				To:             time.Now(),
				Region:         "us-east-1",
				SandboxClass:   "container",
			},
			wantErr: true,
		},
		{
			name: "missing sandbox_id",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "event-123",
				SessionID:      "session-456",
				SandboxID:      "",
				OrganizationID: "org-abc",
				Sequence:       1,
				From:           time.Now().Add(-1 * time.Hour),
				To:             time.Now(),
				Region:         "us-east-1",
				SandboxClass:   "container",
			},
			wantErr: true,
		},
		{
			name: "invalid sequence (zero)",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "event-123",
				SessionID:      "session-456",
				SandboxID:      "sandbox-789",
				OrganizationID: "org-abc",
				Sequence:       0,
				From:           time.Now().Add(-1 * time.Hour),
				To:             time.Now(),
				Region:         "us-east-1",
				SandboxClass:   "container",
			},
			wantErr: true,
		},
		{
			name: "invalid time range (to before from)",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "event-123",
				SessionID:      "session-456",
				SandboxID:      "sandbox-789",
				OrganizationID: "org-abc",
				Sequence:       1,
				From:           time.Now(),
				To:             time.Now().Add(-1 * time.Hour),
				Region:         "us-east-1",
				SandboxClass:   "container",
			},
			wantErr: true,
		},
		{
			name: "missing region",
			chunk: &kafka.BillingChunkCreated{
				EventID:        "event-123",
				SessionID:      "session-456",
				SandboxID:      "sandbox-789",
				OrganizationID: "org-abc",
				Sequence:       1,
				From:           time.Now().Add(-1 * time.Hour),
				To:             time.Now(),
				Region:         "",
				SandboxClass:   "container",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := service.validateBillingChunk(tt.chunk)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateBillingChunk() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestOutboxEventParsing tests parsing outbox events
func TestOutboxEventParsing(t *testing.T) {
	// Create a test outbox chunk
	chunk := kafka.BillingChunkCreated{
		EventID:        "event-123",
		SessionID:      "session-456",
		SandboxID:      "sandbox-789",
		OrganizationID: "org-abc",
		Sequence:       5,
		From:           time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC),
		To:             time.Date(2026, 6, 21, 11, 0, 0, 0, time.UTC),
		CPU:            float64Ptr(2.0),
		GPU:            float64Ptr(1.0),
		RAMGB:          float64Ptr(8.0),
		DiskGB:         float64Ptr(50.0),
		Region:         "us-east-1",
		RegionType:     "cloud",
		SandboxClass:   "container",
	}

	// Marshal to JSON
	payload, err := json.Marshal(chunk)
	if err != nil {
		t.Fatalf("Failed to marshal outbox chunk: %v", err)
	}

	// Create outbox event
	outboxEvent := &sql.OutboxEvent{
		ID:         uuid.New(),
		EventID:    chunk.EventID,
		EventType:  "billing_chunk_created",
		SessionID:  chunk.SessionID,
		Sequence:   chunk.Sequence,
		Payload:    payload,
		CreatedAt:  time.Now(),
		RetryCount: 0,
		LastError:  nil,
	}

	// Parse back
	var parsedChunk kafka.BillingChunkCreated
	if err := json.Unmarshal(outboxEvent.Payload, &parsedChunk); err != nil {
		t.Fatalf("Failed to unmarshal outbox chunk: %v", err)
	}

	// Verify fields
	if parsedChunk.EventID != chunk.EventID {
		t.Errorf("EventID mismatch: got %s, want %s", parsedChunk.EventID, chunk.EventID)
	}
	if parsedChunk.SessionID != chunk.SessionID {
		t.Errorf("SessionID mismatch: got %s, want %s", parsedChunk.SessionID, chunk.SessionID)
	}
	if parsedChunk.SandboxID != chunk.SandboxID {
		t.Errorf("SandboxID mismatch: got %s, want %s", parsedChunk.SandboxID, chunk.SandboxID)
	}
	if parsedChunk.Sequence != chunk.Sequence {
		t.Errorf("Sequence mismatch: got %d, want %d", parsedChunk.Sequence, chunk.Sequence)
	}
	if parsedChunk.CPU == nil || *parsedChunk.CPU != 2.0 {
		t.Errorf("CPU mismatch")
	}
	if parsedChunk.Region != chunk.Region {
		t.Errorf("Region mismatch: got %s, want %s", parsedChunk.Region, chunk.Region)
	}
}

// Benchmark validation performance
func BenchmarkValidateBillingChunk(b *testing.B) {
	service := &outboxPublisherService{}
	chunk := &kafka.BillingChunkCreated{
		EventID:        "event-123",
		SessionID:      "session-456",
		SandboxID:      "sandbox-789",
		OrganizationID: "org-abc",
		Sequence:       1,
		From:           time.Now().Add(-1 * time.Hour),
		To:             time.Now(),
		Region:         "us-east-1",
		RegionType:     "cloud",
		SandboxClass:   "container",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = service.validateBillingChunk(chunk)
	}
}

// Helper function
func float64Ptr(f float64) *float64 {
	return &f
}

// Mock implementations for integration testing would go here
// For example:
// - Mock PostgresDB
// - Mock KafkaConnector
// - Test full ProcessSingleEvent flow
