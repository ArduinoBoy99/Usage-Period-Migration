package main

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

// This example demonstrates the complete Outbox Pattern workflow:
// 1. Create a outbox chunk in the outbox_events table (within a transaction)
// 2. The outbox publisher service reads it
// 3. Publishes to Kafka
// 4. Marks it as published

func main() {
	ctx := context.Background()

	// Initialize database
	db, err := sql.NewPostgresql(sql.Config{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "password",
		DBName:   "usage_db",
		SSLMode:  "disable",
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	fmt.Println("=== Outbox Pattern Workflow Example ===\n")

	// ========================================================================
	// Step 1: Create a usage session outbox chunk event in outbox
	// ========================================================================
	fmt.Println("Step 1: Creating outbox chunk event in outbox...")

	// Create outbox chunk
	billingChunk := kafka.BillingChunkCreated{
		EventID:        uuid.New().String(),
		SessionID:      "session-abc-123",
		SandboxID:      "sandbox-xyz-456",
		OrganizationID: "org-company-789",
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

	// Marshal to JSON
	payload, err := json.Marshal(billingChunk)
	if err != nil {
		log.Fatalf("Failed to marshal outbox chunk: %v", err)
	}

	// Create outbox event
	outboxEvent := &sql.OutboxEvent{
		ID:         uuid.New(),
		EventID:    billingChunk.EventID,
		EventType:  "billing_chunk_created",
		SessionID:  billingChunk.SessionID,
		Sequence:   billingChunk.Sequence,
		Payload:    payload,
		CreatedAt:  time.Now(),
		RetryCount: 0,
		LastError:  nil,
	}

	// Begin transaction
	tx, err := db.BeginTx(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert into outbox (within transaction)
	if err := db.InsertOutboxEvent(ctx, outboxEvent); err != nil {
		tx.Rollback()
		log.Fatalf("Failed to insert outbox event: %v", err)
	}

	// In a real scenario, you would also update the usage_session here
	// For example:
	// db.UpdateUsageSessionBillingStatus(ctx, tx, sessionID, BILLING_ACTIVE, time.Now(), 1)

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	fmt.Printf("✓ Created outbox event: %s\n", outboxEvent.ID)
	fmt.Printf("  - Event Type: %s\n", outboxEvent.EventType)
	fmt.Printf("  - Session ID: %s\n", billingChunk.SessionID)
	fmt.Printf("  - Sandbox ID: %s\n", billingChunk.SandboxID)
	fmt.Printf("  - Sequence: %d\n\n", billingChunk.Sequence)

	// ========================================================================
	// Step 2: The Outbox Publisher Service processes this event
	// ========================================================================
	fmt.Println("Step 2: Outbox Publisher Service would now:")
	fmt.Println("  1. Query: SELECT * FROM outbox_events WHERE published_at IS NULL")
	fmt.Println("  2. Parse the payload")
	fmt.Println("  3. Publish to Kafka topic 'usage-outbox-chunks'")
	fmt.Println("  4. Update: SET published_at = NOW() WHERE id = '<event_id>'")
	fmt.Println()

	// ========================================================================
	// Step 3: Manual verification - check unpublished events
	// ========================================================================
	fmt.Println("Step 3: Checking unpublished events...")

	unpublishedEvents, tx, err := db.GetUnpublishedOutboxEvents(ctx, "billing_chunk_created", 10)
	if err != nil {
		log.Fatalf("Failed to get unpublished events: %v", err)
	}
	defer tx.Rollback()

	fmt.Printf("✓ Found %d unpublished event(s)\n\n", len(unpublishedEvents))

	for i, event := range unpublishedEvents {
		var chunk kafka.BillingChunkCreated
		if err := json.Unmarshal(event.Payload, &chunk); err != nil {
			continue
		}

		fmt.Printf("Event #%d:\n", i+1)
		fmt.Printf("  ID: %s\n", event.ID)
		fmt.Printf("  Type: %s\n", event.EventType)
		fmt.Printf("  Sandbox: %s\n", chunk.SandboxID)
		fmt.Printf("  Session: %s\n", chunk.SessionID)
		fmt.Printf("  Sequence: %d\n", chunk.Sequence)
		fmt.Printf("  Duration: %s\n", chunk.To.Sub(chunk.From))
		fmt.Printf("  Created: %s\n", event.CreatedAt.Format(time.RFC3339))
		fmt.Println()
	}

	// ========================================================================
	// Step 4: Simulate publishing (what the outbox service does)
	// ========================================================================
	fmt.Println("Step 4: To run the actual Outbox Publisher Service:")
	fmt.Println()
	fmt.Println("  $ cd outbox-publisher/cmd")
	fmt.Println("  $ go run main.go")
	fmt.Println()
	fmt.Println("Or with environment variables:")
	fmt.Println()
	fmt.Println("  $ export DB_HOST=localhost")
	fmt.Println("  $ export DB_PORT=5432")
	fmt.Println("  $ export KAFKA_BROKERS=localhost:9092")
	fmt.Println("  $ go run outbox-publisher/cmd/main.go")
	fmt.Println()

	// ========================================================================
	// Optional: Clean up for demo purposes
	// ========================================================================
	fmt.Println("=== Example Complete ===")
	fmt.Println()
	fmt.Println("The outbox event is now ready to be published by the service.")
	fmt.Println("Run the outbox-publisher service to see it in action!")
}

func float64Ptr(f float64) *float64 {
	return &f
}
