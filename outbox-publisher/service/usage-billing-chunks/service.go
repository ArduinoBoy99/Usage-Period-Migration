package usage_billing_chunks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"usage-period-migration/pkg/repository/kafka"
	"usage-period-migration/pkg/repository/sql"

	"github.com/google/uuid"
)

const (
	postgresBatchSize     int    = 1000
	eventTypeBillingChunk string = "billing_chunk_created"
)

// OutboxPublisherService defines the interface for publishing outbox events to Kafka
type OutboxPublisherService interface {
	// ProcessUnpublishedEvents reads unpublished outbox events and publishes them to Kafka
	ProcessUnpublishedEvents(ctx context.Context, batchSize int, numWorkers int) (int, error)

	// ProcessSingleEvent processes a single outbox event
	ProcessSingleEvent(ctx context.Context, event *sql.OutboxEvent) error

	// StartPolling starts continuous polling for unpublished events
	StartPolling(ctx context.Context, interval time.Duration, batchSize int, numWorkers int) error
}

// PublisherStats holds statistics about the publisher
type PublisherStats struct {
	TotalProcessed int64
	TotalFailed    int64
	LastProcessed  time.Time
	LastError      error
}

// outboxPublisherService implements OutboxPublisherService
type outboxPublisherService struct {
	db            *sql.PostgresDB
	kafkaProducer *kafka.KafkaConnector
}

// workerResult carries the outcome of a single event processing
type workerResult struct {
	eventID uuid.UUID
	err     error
}

// NewOutboxPublisherService creates a new outbox publisher service
func NewOutboxPublisherService(db *sql.PostgresDB, kafkaProducer *kafka.KafkaConnector) OutboxPublisherService {
	return &outboxPublisherService{
		db:            db,
		kafkaProducer: kafkaProducer,
	}
}

// validateBillingChunk validates the outbox chunk data
func (s *outboxPublisherService) validateBillingChunk(chunk *kafka.BillingChunkCreated) error {
	if chunk.EventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if chunk.SessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	if chunk.SandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	if chunk.OrganizationID == "" {
		return fmt.Errorf("organization_id is required")
	}
	if chunk.Sequence <= 0 {
		return fmt.Errorf("sequence must be greater than 0")
	}
	if chunk.From.IsZero() {
		return fmt.Errorf("from time is required")
	}
	if chunk.To.IsZero() {
		return fmt.Errorf("to time is required")
	}
	if chunk.To.Before(chunk.From) {
		return fmt.Errorf("to time must be after from time")
	}
	if chunk.Region == "" {
		return fmt.Errorf("region is required")
	}
	if chunk.SandboxClass == "" {
		return fmt.Errorf("sandbox_class is required")
	}
	return nil
}

// StartPolling starts continuous polling for unpublished events
func (s *outboxPublisherService) StartPolling(ctx context.Context, interval time.Duration, batchSize int, numWorkers int) error {
	log.Printf("Starting polling (interval: %s, batch: %d, workers: %d)", interval, batchSize, numWorkers)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if _, err := s.ProcessUnpublishedEvents(ctx, batchSize, numWorkers); err != nil {
		log.Printf("Error on startup: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if processed, err := s.ProcessUnpublishedEvents(ctx, batchSize, numWorkers); err != nil {
				log.Printf("Error processing events: %v", err)
			} else if processed > 0 {
				log.Printf("Cycle complete: %d processed", processed)
			}
		}
	}
}

// ProcessSingleEvent processes a single outbox event
func (s *outboxPublisherService) ProcessSingleEvent(ctx context.Context, event *sql.OutboxEvent) error {
	// Parse payload to BillingChunkCreated
	var billingChunk kafka.BillingChunkCreated
	if err := json.Unmarshal(event.Payload, &billingChunk); err != nil {
		return fmt.Errorf("failed to unmarshal outbox chunk: %w", err)
	}

	// Validate the outbox chunk
	if err := s.validateBillingChunk(&billingChunk); err != nil {
		return fmt.Errorf("invalid outbox chunk: %w", err)
	}

	// Publish to Kafka
	if err := s.kafkaProducer.ProduceBillingChunk(ctx, &billingChunk); err != nil {
		// Increment retry count on failure
		event.RetryCount++
		errorMsg := err.Error()
		event.LastError = &errorMsg

		// Update error in database
		if updateErr := s.db.UpdateOutboxEvent(ctx, event); updateErr != nil {
			log.Printf("Failed to update retry count for event %s: %v", event.ID, updateErr)
		}

		return fmt.Errorf("failed to publish to kafka: %w", err)
	}

	// Mark event as published
	if err := s.db.MarkOutboxEventPublished(ctx, event.ID); err != nil {
		return fmt.Errorf("failed to mark event as published: %w", err)
	}

	log.Printf("Successfully published event %s (outbox chunk: %s)", event.ID, billingChunk.EventID)

	return nil
}

// ProcessingWorker processes events received from a channel
// ProcessingWorker sends workerResult
func (s *outboxPublisherService) ProcessingWorker(ctx context.Context, id int, jobs <-chan *sql.OutboxEvent, results chan<- workerResult) {
	log.Printf("Worker %d started", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d stopping", id)
			return
		case event, ok := <-jobs:
			if !ok {
				log.Printf("Worker %d finished", id)
				return
			}

			err := s.publishToKafka(ctx, event) // Only Kafka publish, no DB mark
			results <- workerResult{eventID: event.ID, err: err}
		}
	}
}

// publishToKafka publishes a single event to Kafka WITHOUT marking it in the DB
func (s *outboxPublisherService) publishToKafka(ctx context.Context, event *sql.OutboxEvent) error {

	var billingChunk kafka.BillingChunkCreated
	if err := json.Unmarshal(event.Payload, &billingChunk); err != nil {
		return fmt.Errorf("failed to unmarshal outbox chunk: %w", err)
	}

	if err := s.validateBillingChunk(&billingChunk); err != nil {
		return fmt.Errorf("invalid outbox chunk: %w", err)
	}

	if err := s.kafkaProducer.ProduceBillingChunk(ctx, &billingChunk); err != nil {
		event.RetryCount++
		errMsg := err.Error()
		event.LastError = &errMsg
		if updateErr := s.db.UpdateOutboxEvent(ctx, event); updateErr != nil {
			log.Printf("Failed to update retry count for event %s: %v", event.ID, updateErr)
		}
		return fmt.Errorf("failed to publish to kafka: %w", err)
	}

	return nil
}

// ProcessUnpublishedEvents fans out Kafka publishing to workers,
// then batch-marks all successful events published in one DB roundtrip per 1000
func (s *outboxPublisherService) ProcessUnpublishedEvents(ctx context.Context, batchSize int, numWorkers int) (int, error) {
	events, tx, err := s.db.GetUnpublishedOutboxEvents(ctx, eventTypeBillingChunk, batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to get unpublished events: %w", err)
	}
	defer tx.Rollback()

	if len(events) == 0 {
		return 0, nil
	}

	log.Printf("Processing %d events with %d workers", len(events), numWorkers)

	jobs := make(chan *sql.OutboxEvent, len(events))
	results := make(chan workerResult, len(events))

	// Start workers
	for i := range numWorkers {
		go s.ProcessingWorker(ctx, i, jobs, results)
	}

	// Enqueue and close
	for _, event := range events {
		jobs <- event
	}
	close(jobs)

	// Collect results — separate successful IDs from failures
	var successIDs []uuid.UUID
	failureCount := 0

	for range len(events) {
		res := <-results
		if res.err != nil {
			failureCount++
		} else {
			successIDs = append(successIDs, res.eventID)
		}
	}

	// Single bulk DB update for all successes, chunked at 1000
	for _, chunk := range chunkIDs(successIDs, postgresBatchSize) {
		if err := s.db.BatchMarkOutboxEventsPublishedTx(ctx, tx, chunk); err != nil {
			return 0, fmt.Errorf("failed to batch mark events published: %w", err)
		}
	}

	tx.Commit()

	successCount := len(successIDs)

	log.Printf("Done — success: %d, failed: %d", successCount, failureCount)

	return successCount, nil
}

func chunkIDs(ids []uuid.UUID, size int) [][]uuid.UUID {
	var chunks [][]uuid.UUID
	for size < len(ids) {
		ids, chunks = ids[size:], append(chunks, ids[:size])
	}
	return append(chunks, ids)
}
