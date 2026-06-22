package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	entity "usage-period-migration/pkg/entity"
	repo "usage-period-migration/pkg/repository/sql"

	"github.com/google/uuid"
)

const (
	EventTypeBillingChunk = "usage-billing-chunks"
	ScanIntervalMinutes   = 1
	BatchSize             = 1000
)

// Service defines the interface for the Outbox service
type Service interface {
	// StartPeriodicScanner starts the periodic scanner that runs every minute
	StartPeriodicScanner(ctx context.Context) error

	// ScanAndCreateOutboxEvents scans for unbilled sessions and creates outbox events
	ScanAndCreateOutboxEvents(ctx context.Context) error

	// CreateOutboxEvent creates an outbox billing chunk event for a session
	CreateOutboxEvent(ctx context.Context, session *repo.UsageSessions) error
}

type outboxService struct {
	db *repo.PostgresDB
}

// NewService creates a new Outbox service
func NewService(db *repo.PostgresDB) Service {
	return &outboxService{
		db: db,
	}
}

// StartPeriodicScanner starts the periodic scanner that runs every minute
func (s *outboxService) StartPeriodicScanner(ctx context.Context) error {
	log.Println("Starting periodic outbox scanner...")

	ticker := time.NewTicker(ScanIntervalMinutes * time.Minute)
	defer ticker.Stop()

	// Run immediately on start
	if err := s.ScanAndCreateOutboxEvents(ctx); err != nil {
		log.Printf("Error in initial scan: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping periodic outbox scanner...")
			return ctx.Err()
		case <-ticker.C:
			log.Println("Running periodic scan for unbilled sessions...")
			if err := s.ScanAndCreateOutboxEvents(ctx); err != nil {
				log.Printf("Error scanning unbilled sessions: %v", err)
				// Continue running despite errors
			}
		}
	}
}

// ScanAndCreateOutboxEvents scans for unbilled sessions and creates outbox events
func (s *outboxService) ScanAndCreateOutboxEvents(ctx context.Context) error {
	startTime := time.Now()

	totalCount, err := s.db.CountUnbilledUsageSessions(ctx)
	if err != nil {
		return fmt.Errorf("failed to count unbilled sessions: %w", err)
	}

	if totalCount == 0 {
		log.Println("No unbilled sessions found")
		return nil
	}

	log.Printf("Found %d unbilled sessions to process", totalCount)

	var totalProcessed int
	lastID := ""

	for {
		sessions, err := s.db.GetUnbilledUsageSessionsAfterCursor(ctx, lastID, BatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch unbilled sessions: %w", err)
		}

		if len(sessions) == 0 {
			break
		}

		// Process each session and create outbox events
		for _, session := range sessions {
			if err := s.CreateOutboxEvent(ctx, session); err != nil {
				log.Printf("Error creating billing chunk event for session %s: %v", session.ID, err)
				// Continue processing other sessions
				continue
			}
			lastID = session.ID
			totalProcessed++
		}

		log.Printf("Processed %d/%d sessions", totalProcessed, totalCount)
	}

	duration := time.Since(startTime)
	log.Printf("Completed processing %d sessions in %v", totalProcessed, duration)

	return nil
}

// CreateOutboxEvent creates an outbox event for a session
func (s *outboxService) CreateOutboxEvent(ctx context.Context, session *repo.UsageSessions) error {
	// Begin transaction
	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Calculate billing period
	from := session.LastBilledAt
	to := time.Now()

	// If session has ended, and we haven't billed until the end, bill until endAt
	if session.EndAt != nil && session.EndAt.Before(to) {
		to = *session.EndAt
	}

	// Increment billing sequence
	newSequence := session.BillingSequence + 1

	// Generate unique event ID
	eventID := uuid.New().String()

	// Create payload
	payload := entity.BillingChunkPayload{
		EventID:        eventID,
		SessionID:      session.ID,
		Sequence:       newSequence,
		From:           *from,
		To:             to,
		CPU:            session.CPU,
		GPU:            session.GPU,
		RamGB:          session.RamGB,
		DiskGB:         session.DiskGB,
		Region:         session.Region,
		SandboxClass:   string(session.SandboxClass),
		OrganizationID: session.OrganizationID,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Create outbox event
	outboxEvent := &repo.OutboxEvent{
		ID:          uuid.New(),
		EventID:     eventID,
		EventType:   EventTypeBillingChunk,
		SessionID:   session.ID,
		Sequence:    newSequence,
		Payload:     payloadBytes,
		CreatedAt:   time.Now(),
		PublishedAt: nil,
		RetryCount:  0,
		LastError:   nil,
	}

	// Insert outbox event
	if err := s.db.InsertOutboxEventTx(ctx, tx, outboxEvent); err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	// Update session's lastBilledAt and billingSequence
	updateQuery := `
		UPDATE usage_sessions
		SET "lastBilledAt" = $2,
		    "billingSequence" = $3
		WHERE id = $1
	`

	_, err = tx.ExecContext(ctx, updateQuery, session.ID, to, newSequence)
	if err != nil {
		return fmt.Errorf("failed to update session: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Created billing chunk event for session %s (sequence: %d, period: %s to %s)",
		session.ID, newSequence, from.Format(time.RFC3339), to.Format(time.RFC3339))

	return nil
}
