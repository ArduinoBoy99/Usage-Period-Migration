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

var logger *slog.Logger

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
}

func NewOutboxPublisherService(db *sql.PostgresDB, kafkaProducer *kafka.KafkaConnector) OutboxPublisherService {
	return &outboxPublisherService{db: db, kafkaProducer: kafkaProducer}
}

func (s *outboxPublisherService) validateBillingChunk(chunk *kafka.BillingChunkCreated) error {
	if chunk.EventID == "" || chunk.SessionID == "" || chunk.SandboxID == "" || chunk.OrganizationID == "" {
		return fmt.Errorf("required fields missing")
	}
	if chunk.Sequence <= 0 || chunk.From.IsZero() || chunk.To.IsZero() {
		return fmt.Errorf("invalid sequence or timestamps")
	}
	if chunk.To.Before(chunk.From) || chunk.Region == "" || chunk.SandboxClass == "" {
		return fmt.Errorf("invalid time range or missing region/class")
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
	logger.Info("Starting leasing workers",
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
	logger.Info("Worker started", slog.Int("worker_id", workerID))

	for {
		select {
		case <-ctx.Done():
			logger.Info("Worker stopping", slog.Int("worker_id", workerID))
			return
		default:
		}

		processed, err := s.processOnce(ctx, workerID, batchSize)
		if err != nil {
			logger.Error("Worker error",
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

	logger.Info("Worker leased events",
		slog.Int("worker_id", workerID),
		slog.Int("event_count", len(events)))

	successIDs := make([]int64, 0, len(events))
	failureCount := 0

	for _, event := range events {
		// Idempotency check
		ok, err := s.db.MarkIfNotProcessed(ctx, event.EventID)
		if err != nil {
			failureCount++
			continue
		}
		if !ok {
			continue // Already processed
		}

		// Validate and publish
		var billingChunk kafka.BillingChunkCreated
		if err := json.Unmarshal(event.Payload, &billingChunk); err != nil {
			failureCount++
			continue
		}
		if err := s.validateBillingChunk(&billingChunk); err != nil {
			failureCount++
			continue
		}
		if err := s.kafkaProducer.ProduceBillingChunk(ctx, &billingChunk); err != nil {
			logger.Error("Worker publish error",
				slog.Int("worker_id", workerID),
				slog.Any("error", err))
			failureCount++
			continue
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

	logger.Info("Worker processing complete",
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
