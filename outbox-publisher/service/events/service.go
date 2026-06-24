package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"usage-period-migration/pkg/repository/kafka"
	"usage-period-migration/pkg/repository/sql"
)

const (
	postgresBatchSize     int    = 1000
	eventTypeBillingChunk string = "billing_chunk_created"
)

// OutboxPublisherService defines pure leasing worker interface
type OutboxPublisherService interface {
	StartWorkers(ctx context.Context, numWorkers int, pollInterval time.Duration, batchSize int)
}

type outboxPublisherService struct {
	db            *sql.PostgresDB
	kafkaProducer *kafka.KafkaConnector
	logger        *slog.Logger
}

func NewOutboxPublisherService(db *sql.PostgresDB, kafkaProducer *kafka.KafkaConnector, logger *slog.Logger) OutboxPublisherService {
	return &outboxPublisherService{
		db:            db,
		kafkaProducer: kafkaProducer,
		logger:        logger,
	}
}

func (s *outboxPublisherService) validateBillingChunk(chunk *kafka.BillingChunkCreated) error {
	if chunk.EventID == "" || chunk.SessionID == "" || chunk.SandboxID == "" {
		return fmt.Errorf("required fields missing")
	}
	if !chunk.To.After(chunk.From) {
		return fmt.Errorf("invalid time range: To (%s) must be after From (%s)",
			chunk.To.Format(time.RFC3339), chunk.From.Format(time.RFC3339))
	}
	return nil
}

// StartWorkers spawns N independent leasing workers
func (s *outboxPublisherService) StartWorkers(
	ctx context.Context,
	numWorkers int,
	pollInterval time.Duration,
	batchSize int,
) {
	s.logger.Info("Starting leasing workers",
		slog.Int("num_workers", numWorkers),
		slog.Int("batch_size", batchSize),
		slog.Duration("poll_interval", pollInterval))

	for i := 0; i < numWorkers; i++ {
		go s.workerLoop(ctx, i, batchSize, pollInterval)
	}
}

// workerLoop: infinite lease → process → commit cycle
func (s *outboxPublisherService) workerLoop(
	ctx context.Context,
	workerID int,
	batchSize int,
	pollInterval time.Duration,
) {
	s.logger.Info("Worker started", slog.Int("worker_id", workerID))

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Worker stopping", slog.Int("worker_id", workerID))
			return
		default:
		}

		processed, err := s.processOnce(ctx, workerID, batchSize)
		if err != nil {
			s.logger.Error("Worker error",
				slog.Int("worker_id", workerID),
				slog.Any("error", err))
		}

		if processed == 0 {
			time.Sleep(pollInterval)
		}
	}
}

// processOnce: single lease-process-commit cycle
func (s *outboxPublisherService) processOnce(
	ctx context.Context,
	workerID int,
	batchSize int,
) (int, error) {
	// Lease rows (FOR UPDATE SKIP LOCKED handled by DB)
	events, tx, err := s.db.GetUnpublishedOutboxEvents(ctx, eventTypeBillingChunk, batchSize)
	if err != nil {
		return 0, fmt.Errorf("worker %d lease failed: %w", workerID, err)
	}
	defer tx.Rollback()

	if len(events) == 0 {
		return 0, nil
	}

	s.logger.Info("Worker leased events",
		slog.Int("worker_id", workerID),
		slog.Int("event_count", len(events)))

	successIDs := make([]int64, 0, len(events))
	failureCount := 0

	for _, event := range events {
		// Idempotency check
		ok, err := s.db.CheckIfProcessed(ctx, tx, event.EventID)
		if err != nil {
			failureCount++
			continue
		}
		if !ok {
			// Already processed by a prior run — mark published in-tx so it isn't re-leased.
			successIDs = append(successIDs, event.ID)
			continue // Already processed
		}

		// Validate and publish
		var eventPayload sql.BillingChunkCreated

		if err := json.Unmarshal(event.Payload, &eventPayload); err != nil {
			s.logger.Warn("Invalid payload json",
				slog.String("event_id", event.EventID),
				slog.String("payload", string(event.Payload)),
				slog.String("error", err.Error()))
			continue
		}

		kafkaPayload := BillingChunkToKafkaPayload(eventPayload)

		s.logger.Info(
			"decoded chunk",
			slog.String("event_id", kafkaPayload.EventID),
			slog.String("session_id", kafkaPayload.SessionID),
			slog.String("sandbox_id", kafkaPayload.SandboxID),
		)

		if err := s.validateBillingChunk(kafkaPayload); err != nil {
			s.logger.Error("Worker validation error",
				slog.Int("worker_id", workerID),
				slog.Any("error", err))
			failureCount++
			continue
		}
		if err := s.kafkaProducer.ProduceBillingChunk(ctx, kafkaPayload); err != nil {
			s.logger.Error("Worker publish error",
				slog.Int("worker_id", workerID),
				slog.Any("error", err))
			failureCount++
			continue
		}

		// mark processed for idempotency, atomically with the publish commit
		if err := s.db.InsertProcessedOutboxEventTx(ctx, tx, &sql.ProcessedOutboxEvent{
			EventID:     event.EventID,
			SessionID:   event.SessionID,
			Sequence:    event.Sequence,
			ProcessedAt: time.Now(),
		}); err != nil {
			s.logger.Error("Worker mark-processed error",
				slog.Int("worker_id", workerID),
				slog.String("event_id", event.EventID),
				slog.Any("error", err))
			return len(successIDs), fmt.Errorf("worker %d mark processed failed: %w", workerID, err)
		}

		successIDs = append(successIDs, event.ID)
	}

	// Batch mark published
	if len(successIDs) > 0 {
		for _, chunk := range chunkIDs(successIDs, postgresBatchSize) {
			if err := s.db.BatchMarkOutboxEventsPublishedTx(ctx, tx, chunk); err != nil {
				return 0, fmt.Errorf("worker %d mark failed: %w", workerID, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("worker %d commit failed: %w", workerID, err)
	}

	s.logger.Info("Worker processing complete",
		slog.Int("worker_id", workerID),
		slog.Int("success_count", len(successIDs)),
		slog.Int("failure_count", failureCount))
	return len(successIDs), nil
}

func chunkIDs(ids []int64, size int) [][]int64 {
	var chunks [][]int64
	for size < len(ids) {
		ids, chunks = ids[size:], append(chunks, ids[:size])
	}
	return append(chunks, ids)
}

func BillingChunkToKafkaPayload(created sql.BillingChunkCreated) *kafka.BillingChunkCreated {
	return &kafka.BillingChunkCreated{
		EventID:        created.EventID,
		SessionID:      created.SessionID,
		SandboxID:      created.SandboxID,
		OrganizationID: created.OrganizationID,
		Sequence:       created.Sequence,
		From:           created.From,
		To:             created.To,
		CPU:            created.CPU,
		GPU:            created.GPU,
		RAMGB:          created.RAMGB,
		DiskGB:         created.DiskGB,
		Region:         created.Region,
		RegionType:     created.RegionType,
		SandboxClass:   created.SandboxClass,
	}
}
