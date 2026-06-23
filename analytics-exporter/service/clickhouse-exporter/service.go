package clickhouse_exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkaRepo "usage-period-migration/pkg/repository/kafka"

	"github.com/segmentio/kafka-go"
)

var logger *slog.Logger

const (
	TopicBillingProcessed = "billing-processed"
	ConsumerGroup         = "analytics-exporters"
)

// Service defines the interface for the Analytics Exporter service
type Service interface {
	// StartConsumer starts consuming billing-processed events from Kafka
	StartConsumer(ctx context.Context) error

	// ExportToClickhouse exports billing event to Clickhouse (simulated to stdout)
	ExportToClickhouse(ctx context.Context, event *BillingProcessedEvent) error
}

// BillingProcessedEvent represents the billing processed event from Kafka
type BillingProcessedEvent struct {
	EventID        string    `json:"event_id"`
	SessionID      string    `json:"session_id"`
	Sequence       int64     `json:"sequence"`
	ProcessedAt    time.Time `json:"processed_at"`
	MetronomeID    string    `json:"metronome_id,omitempty"`
	OrganizationID string    `json:"organization_id"`
}

type analyticsService struct {
	kafkaConnector *kafkaRepo.KafkaConnector
	consumer       *kafka.Reader
}

// NewService creates a new Analytics Exporter service
func NewService(kafkaConnector *kafkaRepo.KafkaConnector) Service {
	return &analyticsService{
		kafkaConnector: kafkaConnector,
	}
}

// StartConsumer starts consuming billing-processed events from Kafka
func (s *analyticsService) StartConsumer(ctx context.Context) error {
	logger.Info("Starting Kafka consumer for billing-processed events...")

	brokers := []string{"localhost:9092"}

	s.consumer = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          TopicBillingProcessed,
		GroupID:        ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
		MaxWait:        500 * time.Millisecond,
	})
	defer s.consumer.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := s.consumer.ReadMessage(ctx)
			if err != nil {
				return fmt.Errorf("failed to read message: %w", err)
			}

			var event BillingProcessedEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				logger.Error("failed to unmarshal event", slog.Any("error", err))
				continue
			}

			if err := s.ExportToClickhouse(ctx, &event); err != nil {
				logger.Error("failed to export event", slog.Any("error", err))
				continue
			}
		}
	}
}

// ExportToClickhouse exports billing event to Clickhouse (simulated to stdout)
func (s *analyticsService) ExportToClickhouse(ctx context.Context, event *BillingProcessedEvent) error {
	if event == nil {
		return fmt.Errorf("event cannot be nil")
	}

	// Simulate Clickhouse export by writing to stdout
	output, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	fmt.Printf("[CLICKHOUSE EXPORT] %s\n", output)
	return nil
}
