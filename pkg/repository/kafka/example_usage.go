package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// ExampleProducer demonstrates how to use the Kafka producer
func ExampleProducer() {
	// Create Kafka connector
	connector, err := NewKafkaConnector(Config{
		Brokers: []string{"kafka:9092", "localhost:9093", "localhost:9094"},
		Topic:   TopicUsageBillingChunks,
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka connector: %v", err)
	}
	defer connector.Close()

	ctx := context.Background()

	// Example 1: Produce a single outbox chunk
	chunk := &BillingChunkCreated{
		EventID:        "event-12345",
		SessionID:      "session-abc",
		SandboxID:      "sandbox-xyz",
		OrganizationID: "org-123",
		Sequence:       1,
		From:           time.Now().Add(-1 * time.Hour),
		To:             time.Now(),
		CPU:            float64Ptr(2.0),
		GPU:            float64Ptr(1.0),
		RAMGB:          float64Ptr(8.0),
		DiskGB:         float64Ptr(50.0),
		Region:         "us-east-1",
		RegionType:     "cloud",
		SandboxClass:   "container",
	}

	if err := connector.ProduceBillingChunk(ctx, chunk); err != nil {
		log.Fatalf("Failed to produce outbox chunk: %v", err)
	}

	fmt.Println("Successfully produced outbox chunk")

	// Example 2: Produce multiple chunks in a batch
	chunks := []*BillingChunkCreated{
		{
			EventID:        "event-001",
			SessionID:      "session-001",
			SandboxID:      "sandbox-001",
			OrganizationID: "org-123",
			Sequence:       1,
			From:           time.Now().Add(-2 * time.Hour),
			To:             time.Now().Add(-1 * time.Hour),
			CPU:            float64Ptr(4.0),
			Region:         "us-east-1",
			RegionType:     "cloud",
			SandboxClass:   "linux-vm",
		},
		{
			EventID:        "event-002",
			SessionID:      "session-002",
			SandboxID:      "sandbox-002",
			OrganizationID: "org-456",
			Sequence:       1,
			From:           time.Now().Add(-1 * time.Hour),
			To:             time.Now(),
			CPU:            float64Ptr(2.0),
			Region:         "eu-west-1",
			RegionType:     "cloud",
			SandboxClass:   "container",
		},
	}

	if err := connector.ProduceBillingChunkBatch(ctx, chunks); err != nil {
		log.Fatalf("Failed to produce batch: %v", err)
	}

	fmt.Printf("Successfully produced %d outbox chunks\n", len(chunks))

	// Print stats
	stats := connector.GetStats()
	fmt.Printf("Producer stats - Messages: %d, Bytes: %d, Errors: %d\n",
		stats.Messages, stats.Bytes, stats.Errors)
}

// ExampleConsumer demonstrates how to use the Kafka consumer
func ExampleConsumer() {
	// Create Kafka connector
	connector, err := NewKafkaConnector(Config{
		Brokers:       []string{"kafka:9092", "localhost:9093", "localhost:9094"},
		Topic:         TopicUsageBillingChunks,
		ConsumerGroup: "outbox-processor-group",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka connector: %v", err)
	}
	defer connector.Close()

	ctx := context.Background()

	// Define message handler
	handler := func(ctx context.Context, chunk *BillingChunkCreated) error {
		fmt.Printf("Processing outbox chunk:\n")
		fmt.Printf("  Event ID: %s\n", chunk.EventID)
		fmt.Printf("  Session ID: %s\n", chunk.SessionID)
		fmt.Printf("  Organization ID: %s\n", chunk.OrganizationID)
		fmt.Printf("  Sequence: %d\n", chunk.Sequence)
		fmt.Printf("  From: %s\n", chunk.From)
		fmt.Printf("  To: %s\n", chunk.To)
		fmt.Printf("  Duration: %s\n", chunk.To.Sub(chunk.From))

		// Your business logic here
		// e.g., send to outbox API, update database, etc.

		// Simulate processing
		time.Sleep(100 * time.Millisecond)

		// Return nil to commit the message
		// Return error to retry (message won't be committed)
		return nil
	}

	// Start consuming (blocks until context is cancelled)
	fmt.Println("Starting consumer...")
	if err := connector.ConsumeBillingChunks(ctx, handler); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}

// ExampleConsumerWithManualCommit demonstrates manual message commit
func ExampleConsumerWithManualCommit() {
	connector, err := NewKafkaConnector(Config{
		Brokers:       []string{"kafka:9092"},
		Topic:         TopicUsageBillingChunks,
		ConsumerGroup: "manual-commit-group",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka connector: %v", err)
	}
	defer connector.Close()

	ctx := context.Background()

	// Start consumer
	if err := connector.StartConsumer(ctx); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	// Read and process messages manually
	for i := 0; i < 10; i++ {
		// Read message (not committed yet)
		msg, err := connector.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Parse message
		var chunk BillingChunkCreated
		if err := json.Unmarshal(msg.Value, &chunk); err != nil {
			log.Printf("Error parsing message: %v", err)
			// Skip this message
			connector.CommitMessage(ctx, msg)
			continue
		}

		fmt.Printf("Read message: Event ID: %s\n", chunk.EventID)

		// Process message
		// Your business logic here...

		// Manually commit after successful processing
		if err := connector.CommitMessage(ctx, msg); err != nil {
			log.Printf("Error committing message: %v", err)
		} else {
			fmt.Printf("Committed message: %s\n", chunk.EventID)
		}
	}
}

// ExampleIntegratedWorkflow demonstrates the complete workflow
func ExampleIntegratedWorkflow() {
	// Producer side - simulates outbox service
	go func() {
		producerConnector, err := NewKafkaConnector(Config{
			Brokers: []string{"kafka:9092"},
			Topic:   TopicUsageBillingChunks,
		})
		if err != nil {
			log.Printf("Producer error: %v", err)
			return
		}
		defer producerConnector.Close()

		ctx := context.Background()

		// Simulate producing outbox chunks every 5 seconds
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		sequence := int64(1)
		for range ticker.C {
			chunk := &BillingChunkCreated{
				EventID:        fmt.Sprintf("event-%d", time.Now().Unix()),
				SessionID:      "session-ongoing",
				SandboxID:      "sandbox-active",
				OrganizationID: "org-123",
				Sequence:       sequence,
				From:           time.Now().Add(-5 * time.Second),
				To:             time.Now(),
				CPU:            float64Ptr(2.0),
				Region:         "us-east-1",
				RegionType:     "cloud",
				SandboxClass:   "container",
			}

			if err := producerConnector.ProduceBillingChunk(ctx, chunk); err != nil {
				log.Printf("Failed to produce chunk: %v", err)
			} else {
				fmt.Printf("Produced chunk with sequence %d\n", sequence)
			}

			sequence++
		}
	}()

	// Consumer side - simulates outbox processor
	consumerConnector, err := NewKafkaConnector(Config{
		Brokers:       []string{"kafka:9092"},
		Topic:         TopicUsageBillingChunks,
		ConsumerGroup: "outbox-processor",
	})
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
	defer consumerConnector.Close()

	ctx := context.Background()

	handler := func(ctx context.Context, chunk *BillingChunkCreated) error {
		// Process the outbox chunk
		fmt.Printf("Processing chunk: Session=%s, Sequence=%d, Duration=%s\n",
			chunk.SessionID, chunk.Sequence, chunk.To.Sub(chunk.From))

		// Here you would:
		// 1. Send to outbox API (Metronome, Stripe, etc.)
		// 2. Update database
		// 3. Send notifications

		return nil
	}

	// Start consuming
	if err := consumerConnector.ConsumeBillingChunks(ctx, handler); err != nil {
		log.Fatalf("Consumer failed: %v", err)
	}
}

// Helper function
func float64Ptr(f float64) *float64 {
	return &f
}
