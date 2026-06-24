package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
	repo "usage-period-migration/pkg/repository/sql"

	"github.com/google/uuid"
)

const (
	EventTypeBillingChunk = "billing_chunk_created"
	ScanIntervalMinutes   = 1
	BatchSize             = 1000
	DuplicateBatchSize    = 15
)

// Service defines the interface for the Outbox service
type Service interface {
	// StartPeriodicScanner starts the periodic scanner that runs every minute
	StartPeriodicScanner(ctx context.Context) error

	// ScanAndCreateOutboxEvents scans for unbilled sessions and creates outbox events
	ScanAndCreateOutboxEvents(ctx context.Context) error

	// CreateOutboxEvent creates an outbox billing chunk event for a session
	CreateOutboxEvent(ctx context.Context, session *repo.UsageSessions) error

	// StartDuplicateInjector periodically re-publishes already-sent events to demo idempotency
	StartDuplicateInjector(ctx context.Context, interval time.Duration) error
}

// OutboxRepository defines the interface for outbox repository operations
type OutboxRepository interface {
	CountUnbilledUsageSessions(ctx context.Context, interval time.Duration) (int64, error)
	GetUnbilledUsageSessionsAfterCursor(ctx context.Context, lastID string, limit int, interval time.Duration) ([]*repo.UsageSessions, error)
	InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event *repo.OutboxEvent) error
	BeginTx(ctx context.Context) (*sql.Tx, error)
	ResetRandomPublishedOutboxEvents(ctx context.Context, eventType string, limit int) (int64, error)
}

type outboxService struct {
	db       OutboxRepository
	logger   *slog.Logger
	interval time.Duration
}

// NewService creates a new Outbox service
func NewService(db OutboxRepository, logger *slog.Logger, interval time.Duration) Service {
	return &outboxService{
		db:       db,
		logger:   logger,
		interval: interval,
	}
}

// StartPeriodicScanner starts the periodic scanner that runs every minute
func (s *outboxService) StartPeriodicScanner(ctx context.Context) error {
	s.logger.Info("Starting periodic outbox scanner")

	ticker := time.NewTicker(ScanIntervalMinutes * time.Minute)
	defer ticker.Stop()

	// Run immediately on start
	if err := s.ScanAndCreateOutboxEvents(ctx); err != nil {
		s.logger.Error("Error in initial scan", slog.Any("error", err))
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping periodic outbox scanner")
			return ctx.Err()
		case <-ticker.C:
			s.logger.Info("Running periodic scan for unbilled sessions")
			if err := s.ScanAndCreateOutboxEvents(ctx); err != nil {
				s.logger.Error("Error scanning unbilled sessions", slog.Any("error", err))
			}
		}
	}
}

// ScanAndCreateOutboxEvents scans for unbilled sessions and creates outbox events
func (s *outboxService) ScanAndCreateOutboxEvents(ctx context.Context) error {
	startTime := time.Now()

	totalCount, err := s.db.CountUnbilledUsageSessions(ctx, s.interval)
	if err != nil {
		return fmt.Errorf("failed to count unbilled sessions: %w", err)
	}

	if totalCount == 0 {
		s.logger.Info("No unbilled sessions found")
		return nil
	}

	s.logger.Info("Found unbilled sessions to process", slog.Int64("count", totalCount))

	var totalProcessed int
	lastID := ""

	for {
		sessions, err := s.db.GetUnbilledUsageSessionsAfterCursor(ctx, lastID, BatchSize, s.interval)
		if err != nil {
			return fmt.Errorf("failed to fetch unbilled sessions: %w", err)
		}

		if len(sessions) == 0 {
			break
		}

		// Process each session and create outbox events
		for _, session := range sessions {
			if err := s.CreateOutboxEvent(ctx, session); err != nil {
				s.logger.Error("Error creating billing chunk event for session",
					slog.String("session_id", session.ID),
					slog.Any("error", err))
				// Continue processing other sessions
				continue
			}
			lastID = session.ID
			totalProcessed++
		}

		s.logger.Info("Processing progress",
			slog.Int("processed", totalProcessed),
			slog.Int64("total", totalCount))
	}

	duration := time.Since(startTime)
	s.logger.Info("Completed processing sessions",
		slog.Int("count", totalProcessed),
		slog.Duration("duration", duration))

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
	from := session.StartAt
	if session.LastBilledAt != nil {
		from = *session.LastBilledAt
	}

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
	payload := repo.BillingChunkCreated{
		EventID:        eventID,
		SessionID:      session.ID,
		SandboxID:      session.SandboxID,
		Sequence:       newSequence,
		From:           from,
		To:             to,
		CPU:            session.CPU,
		GPU:            session.GPU,
		RAMGB:          session.RamGB,
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

	// Update session's last_billed_at and billing_sequence
	updateQuery := `
		UPDATE usage_sessions
		SET last_billed_at = $2,
			billing_sequence = $3
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

	s.logger.Info("Created billing chunk event for session",
		slog.String("session_id", session.ID),
		slog.Int64("sequence", newSequence),
		slog.String("period", fmt.Sprintf("%s to %s", from.Format(time.RFC3339), to.Format(time.RFC3339))))

	return nil
}

// StartDuplicateInjector periodically resets a small batch of already-published events
// back to unpublished, so the outbox-publisher re-emits them. This demonstrates that
// downstream consumers handle duplicate events idempotently (dedupe by event_id).
func (s *outboxService) StartDuplicateInjector(ctx context.Context, interval time.Duration) error {
	s.logger.Info("Starting outbox duplicate injector",
		slog.Duration("interval", interval),
		slog.Int("batch_size", DuplicateBatchSize))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping outbox duplicate injector")
			return ctx.Err()
		case <-ticker.C:
			n, err := s.db.ResetRandomPublishedOutboxEvents(ctx, EventTypeBillingChunk, DuplicateBatchSize)
			if err != nil {
				s.logger.Error("Failed to inject duplicate events", slog.Any("error", err))
				continue
			}
			s.logger.Info("Injected duplicate events for idempotency demo",
				slog.Int64("count", n))
		}
	}
}
