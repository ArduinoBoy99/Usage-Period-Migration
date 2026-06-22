package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	// TopicUsageBillingChunks is the topic name for outbox chunk events
	TopicUsageBillingChunks = "usage-outbox-chunks"
)

// Config holds Kafka configuration
type Config struct {
	Brokers           []string      // List of Kafka broker addresses
	Topic             string        // Default topic name
	ConsumerGroup     string        // Consumer group ID
	MaxAttempts       int           // Maximum number of retry attempts
	MinBytes          int           // Minimum bytes to fetch per request (default: 1)
	MaxBytes          int           // Maximum bytes to fetch per request (default: 10MB)
	CommitInterval    time.Duration // How often to commit offsets (default: 1s)
	SessionTimeout    time.Duration // Consumer session timeout (default: 10s)
	HeartbeatInterval time.Duration // Heartbeat interval (default: 3s)
}

// KafkaConnector manages Kafka producer and consumer connections
type KafkaConnector struct {
	config Config
	writer *kafka.Writer
	reader *kafka.Reader
}

// NewKafkaConnector creates a new Kafka connector
func NewKafkaConnector(cfg Config) (*KafkaConnector, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	// Set defaults
	if cfg.Topic == "" {
		cfg.Topic = TopicUsageBillingChunks
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "usage-outbox-processor"
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.MinBytes == 0 {
		cfg.MinBytes = 1
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 10e6 // 10MB
	}
	if cfg.CommitInterval == 0 {
		cfg.CommitInterval = time.Second
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 10 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 3 * time.Second
	}

	// Create writer (producer)
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  cfg.MaxAttempts,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireAll, // Wait for all replicas
		Async:        false,            // Synchronous writes for reliability
	}

	return &KafkaConnector{
		config: cfg,
		writer: writer,
		reader: nil, // Reader created when StartConsumer is called
	}, nil
}

// ============================================================================
// Producer Methods
// ============================================================================

// ProduceBillingChunk sends a BillingChunkCreated event to Kafka
func (k *KafkaConnector) ProduceBillingChunk(ctx context.Context, chunk *BillingChunkCreated) error {
	// Marshal to JSON
	payload, err := json.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox chunk: %w", err)
	}

	// Create Kafka message
	msg := kafka.Message{
		Key:   []byte(chunk.SandboxID), // Use sandbox ID as partition key
		Value: payload,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("billing_chunk_created")},
			{Key: "event_id", Value: []byte(chunk.EventID)},
			{Key: "organization_id", Value: []byte(chunk.OrganizationID)},
		},
		Time: time.Now(),
	}

	// Send message
	if err := k.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

// ProduceBillingChunkBatch sends multiple BillingChunkCreated events to Kafka in a batch
func (k *KafkaConnector) ProduceBillingChunkBatch(ctx context.Context, chunks []*BillingChunkCreated) error {
	if len(chunks) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(chunks))

	for _, chunk := range chunks {
		payload, err := json.Marshal(chunk)
		if err != nil {
			return fmt.Errorf("failed to marshal outbox chunk %s: %w", chunk.EventID, err)
		}

		msg := kafka.Message{
			Key:   []byte(chunk.SandboxID),
			Value: payload,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte("billing_chunk_created")},
				{Key: "event_id", Value: []byte(chunk.EventID)},
				{Key: "organization_id", Value: []byte(chunk.OrganizationID)},
			},
			Time: time.Now(),
		}

		messages = append(messages, msg)
	}

	// Send batch
	if err := k.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("failed to write batch messages to kafka: %w", err)
	}

	return nil
}

// ProduceMessage sends a generic message to Kafka
func (k *KafkaConnector) ProduceMessage(ctx context.Context, key string, value []byte, headers map[string]string) error {
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	msg := kafka.Message{
		Key:     []byte(key),
		Value:   value,
		Headers: kafkaHeaders,
		Time:    time.Now(),
	}

	if err := k.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

// ============================================================================
// Consumer Methods
// ============================================================================

// MessageHandler is a function that processes a Kafka message
type MessageHandler func(ctx context.Context, message *kafka.Message) error

// StartConsumer initializes and starts the Kafka consumer
func (k *KafkaConnector) StartConsumer(ctx context.Context) error {
	if k.reader != nil {
		return fmt.Errorf("consumer already started")
	}

	// Create reader (consumer)
	k.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:           k.config.Brokers,
		Topic:             k.config.Topic,
		GroupID:           k.config.ConsumerGroup,
		MinBytes:          k.config.MinBytes,
		MaxBytes:          k.config.MaxBytes,
		CommitInterval:    k.config.CommitInterval,
		SessionTimeout:    k.config.SessionTimeout,
		HeartbeatInterval: k.config.HeartbeatInterval,
		StartOffset:       kafka.LastOffset, // Start from latest offset for new consumers
		MaxAttempts:       k.config.MaxAttempts,
		ReadBackoffMin:    100 * time.Millisecond,
		ReadBackoffMax:    1 * time.Second,
	})

	return nil
}

// ConsumeBillingChunks consumes BillingChunkCreated messages and processes them with the provided handler
func (k *KafkaConnector) ConsumeBillingChunks(ctx context.Context, handler func(context.Context, *BillingChunkCreated) error) error {
	if k.reader == nil {
		if err := k.StartConsumer(ctx); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Fetch message
			msg, err := k.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded {
					return err
				}
				fmt.Printf("Error fetching message: %v\n", err)
				continue
			}

			// Parse message
			var chunk BillingChunkCreated
			if err := json.Unmarshal(msg.Value, &chunk); err != nil {
				fmt.Printf("Error unmarshaling message: %v, Raw: %s\n", err, string(msg.Value))
				// Commit even if parsing fails to avoid infinite loop
				if commitErr := k.reader.CommitMessages(ctx, msg); commitErr != nil {
					fmt.Printf("Error committing failed message: %v\n", commitErr)
				}
				continue
			}

			// Process message with handler
			if err := handler(ctx, &chunk); err != nil {
				fmt.Printf("Error processing outbox chunk %s: %v\n", chunk.EventID, err)
				// Don't commit - message will be reprocessed
				// In production, you might want to implement a retry mechanism or dead letter queue
				continue
			}

			// Commit message after successful processing
			if err := k.reader.CommitMessages(ctx, msg); err != nil {
				fmt.Printf("Error committing message: %v\n", err)
				// Message will be reprocessed on next read
				continue
			}

			fmt.Printf("Successfully processed and committed outbox chunk %s (session: %s, sequence: %d)\n",
				chunk.EventID, chunk.SessionID, chunk.Sequence)
		}
	}
}

// ConsumeMessages consumes generic messages with a custom handler
func (k *KafkaConnector) ConsumeMessages(ctx context.Context, handler MessageHandler) error {
	if k.reader == nil {
		if err := k.StartConsumer(ctx); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Fetch message
			msg, err := k.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded {
					return err
				}
				fmt.Printf("Error fetching message: %v\n", err)
				continue
			}

			// Process with handler
			if err := handler(ctx, &msg); err != nil {
				fmt.Printf("Error processing message: %v\n", err)
				// Don't commit on error
				continue
			}

			// Commit after successful processing
			if err := k.reader.CommitMessages(ctx, msg); err != nil {
				fmt.Printf("Error committing message: %v\n", err)
				continue
			}
		}
	}
}

// ReadMessage reads a single message without committing (manual commit required)
func (k *KafkaConnector) ReadMessage(ctx context.Context) (*kafka.Message, error) {
	if k.reader == nil {
		if err := k.StartConsumer(ctx); err != nil {
			return nil, err
		}
	}

	msg, err := k.reader.FetchMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch message: %w", err)
	}

	return &msg, nil
}

// CommitMessage commits a message after processing
func (k *KafkaConnector) CommitMessage(ctx context.Context, msg *kafka.Message) error {
	if k.reader == nil {
		return fmt.Errorf("consumer not initialized")
	}

	if err := k.reader.CommitMessages(ctx, *msg); err != nil {
		return fmt.Errorf("failed to commit message: %w", err)
	}

	return nil
}

// ============================================================================
// Utility Methods
// ============================================================================

// Close closes the Kafka connections
func (k *KafkaConnector) Close() error {
	var errs []error

	if k.writer != nil {
		if err := k.writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close writer: %w", err))
		}
	}

	if k.reader != nil {
		if err := k.reader.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close reader: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing kafka connector: %v", errs)
	}

	return nil
}

// GetStats returns statistics about the producer
func (k *KafkaConnector) GetStats() kafka.WriterStats {
	if k.writer == nil {
		return kafka.WriterStats{}
	}
	return k.writer.Stats()
}

// GetReaderStats returns statistics about the consumer
func (k *KafkaConnector) GetReaderStats() kafka.ReaderStats {
	if k.reader == nil {
		return kafka.ReaderStats{}
	}
	return k.reader.Stats()
}
