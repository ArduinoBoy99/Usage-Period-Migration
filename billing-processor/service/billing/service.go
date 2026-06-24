package billing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	kafkaRepo "usage-period-migration/pkg/repository/kafka"
	"usage-period-migration/pkg/repository/sql"
)

const (
	// Kafka configuration
	TopicUsageBillingChunks = "events"
	TopicBillingProcessed   = "billing-processed"
	ConsumerGroup           = "billing-processors"
)

// Service defines the interface for the Billing Processor service
type Service interface {
	// StartConsumer starts consuming billing chunk events from Kafka
	StartConsumer(ctx context.Context) error

	// ProcessBillingChunk processes a single billing chunk event
	ProcessBillingChunk(ctx context.Context, event *kafkaRepo.BillingChunkCreated) error

	// CheckIdempotency checks if event was already processed
	CheckIdempotency(ctx context.Context, eventID string) (bool, error)

	// BuildMetronomePayload builds the Metronome API payload
	BuildMetronomePayload(event *kafkaRepo.BillingChunkCreated) (*MetronomePayload, error)

	// SendToMetronome sends billing data to Metronome (simulated)
	SendToMetronome(ctx context.Context, payload *MetronomePayload) error

	// PublishBillingProcessed publishes billing processed event to Kafka
	PublishBillingProcessed(ctx context.Context, event *BillingProcessedEvent) error
}

// MetronomePayload represents the payload sent to Metronome
type MetronomePayload struct {
	TransactionID string                 `json:"transaction_id"` // session_id:sequence
	CustomerID    string                 `json:"customer_id"`    // organization_id
	EventType     string                 `json:"event_type"`
	Timestamp     time.Time              `json:"timestamp"`
	Properties    map[string]interface{} `json:"properties"`
}

// BillingProcessedEvent represents the event published after successful billing
type BillingProcessedEvent struct {
	EventID        string    `json:"event_id"`
	SessionID      string    `json:"session_id"`
	Sequence       int64     `json:"sequence"`
	ProcessedAt    time.Time `json:"processed_at"`
	MetronomeID    string    `json:"metronome_id,omitempty"`
	OrganizationID string    `json:"organization_id"`
}

type billingService struct {
	db             *sql.PostgresDB
	kafkaConnector *kafkaRepo.KafkaConnector
	consumer       *kafka.Reader
	producer       *kafka.Writer
	logger         *slog.Logger
}

// NewService creates a new Billing Processor service
func NewService(db *sql.PostgresDB, kafkaConnector *kafkaRepo.KafkaConnector, logger *slog.Logger) Service {
	return &billingService{
		db:             db,
		kafkaConnector: kafkaConnector,
		logger:         logger,
	}
}

// StartConsumer starts consuming billing chunk events from Kafka
func (s *billingService) StartConsumer(ctx context.Context) error {
	s.logger.Info("Starting Kafka consumer for billing chunks")

	// Get Kafka brokers from environment or use default
	brokers := []string{"kafka:9092"}

	// Create Kafka reader (consumer)
	s.consumer = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          TopicUsageBillingChunks,
		GroupID:        ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset, // Start from beginning for testing
		MaxWait:        500 * time.Millisecond,
	})
	defer s.consumer.Close()

	// Create Kafka writer (producer) for billing-processed events
	s.producer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        TopicBillingProcessed,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  3,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		Compression:  kafka.Snappy,
	}
	defer s.producer.Close()

	s.logger.Info("Kafka consumer started",
		slog.String("topic", TopicUsageBillingChunks),
		slog.String("consumer_group", ConsumerGroup))

	// Consume messages
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping Kafka consumer")
			return ctx.Err()
		default:
			// Read message with timeout
			readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			msg, err := s.consumer.ReadMessage(readCtx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					continue
				}
				s.logger.Error("Error reading message", slog.Any("error", err))
				continue
			}

			// Process message
			if err := s.processMessage(ctx, msg); err != nil {
				s.logger.Error("Error processing message", slog.Any("error", err))
				// Continue processing other messages
			}
		}
	}
}

// processMessage processes a single Kafka message
func (s *billingService) processMessage(ctx context.Context, msg kafka.Message) error {
	s.logger.Info("Received message",
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
		slog.String("key", string(msg.Key)))

	// Step 1: Parse the event
	var event kafkaRepo.BillingChunkCreated
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal billing chunk event: %w", err)
	}

	s.logger.Info("Processing billing chunk",
		slog.String("event_id", event.EventID),
		slog.String("session_id", event.SessionID),
		slog.Int64("sequence", event.Sequence))

	// Process the billing chunk
	if err := s.ProcessBillingChunk(ctx, &event); err != nil {
		return fmt.Errorf("failed to process billing chunk: %w", err)
	}

	s.logger.Info("Successfully processed billing chunk",
		slog.String("event_id", event.EventID))

	return nil
}

// ProcessBillingChunk processes a single billing chunk event (Steps 2-5)
func (s *billingService) ProcessBillingChunk(ctx context.Context, event *kafkaRepo.BillingChunkCreated) error {
	// Step 2: Idempotency Check (dedupe by event_id)
	s.logger.Info("Idempotency check for event",
		slog.String("step", "2"),
		slog.String("event_id", event.EventID))

	processed, err := s.CheckIdempotency(ctx, event.EventID)
	if err != nil {
		return fmt.Errorf("idempotency check failed: %w", err)
	}

	if processed {
		s.logger.Info("Event already processed, skipping",
			slog.String("event_id", event.EventID))
		return nil
	}

	// Step 3: Build Metronome Payload
	s.logger.Info("Building Metronome payload",
		slog.String("step", "3"),
		slog.String("event_id", event.EventID))

	metronomePayload, err := s.BuildMetronomePayload(event)
	if err != nil {
		return fmt.Errorf("failed to build Metronome payload: %w", err)
	}

	// Step 4: Send to Metronome
	s.logger.Info("Sending to Metronome",
		slog.String("step", "4"),
		slog.String("transaction_id", metronomePayload.TransactionID))

	if err := s.SendToMetronome(ctx, metronomePayload); err != nil {
		return fmt.Errorf("failed to send to Metronome: %w", err)
	}

	// Step 5: Publish BillingProcessed Event
	s.logger.Info("Publishing billing processed event",
		slog.String("step", "5"),
		slog.String("event_id", event.EventID))

	billingProcessedEvent := &BillingProcessedEvent{
		EventID:        event.EventID,
		SessionID:      event.SessionID,
		Sequence:       event.Sequence,
		ProcessedAt:    time.Now(),
		MetronomeID:    metronomePayload.TransactionID,
		OrganizationID: event.OrganizationID,
	}

	if err := s.PublishBillingProcessed(ctx, billingProcessedEvent); err != nil {
		return fmt.Errorf("failed to publish billing processed event: %w", err)
	}

	// Record as processed in database
	processedEvent := &sql.ProcessedBillingEvent{
		EventID:     event.EventID,
		ProcessedAt: time.Now(),
	}

	if err := s.db.InsertProcessedBillingEvent(ctx, processedEvent); err != nil {
		s.logger.Warn("Failed to record processed event",
			slog.Any("error", err))
		// Don't fail the entire process if we can't record it
	}

	s.logger.Info("Successfully completed billing",
		slog.String("event_id", event.EventID))
	return nil
}

// CheckIdempotency checks if event was already processed (Step 2)
func (s *billingService) CheckIdempotency(ctx context.Context, eventID string) (bool, error) {
	exists, err := s.db.ProcessedBillingEventExists(ctx, eventID)
	if err != nil {
		return false, fmt.Errorf("failed to check processed event: %w", err)
	}

	if exists {
		s.logger.Info("Idempotency: Event already processed",
			slog.String("event_id", eventID))
	} else {
		s.logger.Info("Idempotency: Event is new",
			slog.String("event_id", eventID))
	}

	return exists, nil
}

// BuildMetronomePayload builds the Metronome API payload (Step 3)
func (s *billingService) BuildMetronomePayload(event *kafkaRepo.BillingChunkCreated) (*MetronomePayload, error) {
	// transaction_id = session_id:sequence (as shown in diagram)
	transactionID := fmt.Sprintf("%s:%d", event.SessionID, event.Sequence)

	// Calculate usage duration in hours
	duration := event.To.Sub(event.From)
	durationHours := duration.Hours()

	// Build properties map with all resource metrics
	properties := map[string]interface{}{
		"session_id":     event.SessionID,
		"sandbox_id":     event.SandboxID,
		"sequence":       event.Sequence,
		"from":           event.From.Format(time.RFC3339),
		"to":             event.To.Format(time.RFC3339),
		"duration_hours": durationHours,
		"region":         event.Region,
		"sandbox_class":  event.SandboxClass,
	}

	// Add resource metrics if present
	if event.CPU != nil {
		properties["cpu"] = *event.CPU
	}
	if event.GPU != nil {
		properties["gpu"] = *event.GPU
	}
	if event.RAMGB != nil {
		properties["ram_gb"] = *event.RAMGB
	}
	if event.DiskGB != nil {
		properties["disk_gb"] = *event.DiskGB
	}

	payload := &MetronomePayload{
		TransactionID: transactionID,
		CustomerID:    event.OrganizationID,
		EventType:     "usage_billing_chunk",
		Timestamp:     event.To,
		Properties:    properties,
	}

	s.logger.Info("Built Metronome payload",
		slog.String("transaction_id", transactionID),
		slog.String("customer_id", event.OrganizationID),
		slog.Float64("duration_hours", durationHours))

	return payload, nil
}

// SendToMetronome sends billing data to Metronome (Step 4 - Simulated)
func (s *billingService) SendToMetronome(ctx context.Context, payload *MetronomePayload) error {
	// In a real implementation, this would make an HTTP request to Metronome API

	payloadJSON, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal Metronome payload: %w", err)
	}

	s.logger.Info("Sending to Metronome (simulated)",
		slog.String("payload", string(payloadJSON)))

	// Simulate API call delay
	time.Sleep(100 * time.Millisecond)

	// Metronome provides idempotent ingestion via transaction_id
	s.logger.Info("Metronome accepted transaction",
		slog.String("transaction_id", payload.TransactionID))

	return nil
}

// PublishBillingProcessed publishes billing processed event to Kafka (Step 5)
func (s *billingService) PublishBillingProcessed(ctx context.Context, event *BillingProcessedEvent) error {
	// Marshal event to JSON
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal billing processed event: %w", err)
	}

	// Create Kafka message
	// Key by session_id for ordering per session
	msg := kafka.Message{
		Key:   []byte(event.SessionID),
		Value: payload,
		Time:  time.Now(),
	}

	// Write message to Kafka
	if err := s.producer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	s.logger.Info("Published billing-processed event",
		slog.String("event_id", event.EventID),
		slog.String("session_id", event.SessionID))

	return nil
}

// GetProcessingStats returns processing statistics
func (s *billingService) GetProcessingStats(ctx context.Context) (map[string]interface{}, error) {
	totalProcessed, err := s.db.CountProcessedBillingEvents(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get processed count: %w", err)
	}

	stats := map[string]interface{}{
		"total_processed": totalProcessed,
		"consumer_group":  ConsumerGroup,
		"topic":           TopicUsageBillingChunks,
	}

	return stats, nil
}
