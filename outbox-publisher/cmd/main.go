package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"usage-period-migration/outbox-publisher/service/usage-billing-chunks"
	"usage-period-migration/pkg/repository/kafka"
	"usage-period-migration/pkg/repository/sql"
)

type Application struct {
	Run             bool
	shutdownChannel chan bool
	cSignal         chan os.Signal

	rootCtx    context.Context
	rootCancel context.CancelFunc

	db               *sql.PostgresDB
	kafkaConnector   *kafka.KafkaConnector
	publisherService usage_billing_chunks.OutboxPublisherService
}

func (app *Application) initDatabase() error {
	// Get database configuration from environment variables
	dbConfig := sql.Config{
		Host:     getEnv("DB_HOST", "localhost"),
		Port:     getEnvAsInt("DB_PORT", 5432),
		User:     getEnv("DB_USER", "postgres"),
		Password: getEnv("DB_PASSWORD", "password"),
		DBName:   getEnv("DB_NAME", "usage_db"),
		SSLMode:  getEnv("DB_SSL_MODE", "disable"),
	}

	log.Printf("Connecting to database: %s@%s:%d/%s", dbConfig.User, dbConfig.Host, dbConfig.Port, dbConfig.DBName)

	db, err := sql.NewPostgresql(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	app.db = db
	log.Println("Database connection established")
	return nil
}

func (app *Application) initKafka() error {
	// Get Kafka configuration from environment variables
	brokers := getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"})

	kafkaConfig := kafka.Config{
		Brokers:       brokers,
		Topic:         kafka.TopicUsageBillingChunks,
		ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "usage-outbox-processor"),
		MaxAttempts:   getEnvAsInt("KAFKA_MAX_ATTEMPTS", 3),
	}

	log.Printf("Connecting to Kafka brokers: %v", brokers)

	kafkaConnector, err := kafka.NewKafkaConnector(kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize kafka: %w", err)
	}

	app.kafkaConnector = kafkaConnector
	log.Println("Kafka connection established")
	return nil
}

func (app *Application) initServices() {
	// Create outbox publisher service
	app.publisherService = usage_billing_chunks.NewOutboxPublisherService(app.db, app.kafkaConnector)
	log.Println("Outbox publisher service initialized")
}

func (app *Application) start() error {
	log.Println("Starting application...")

	app.rootCtx, app.rootCancel = context.WithCancel(context.Background())

	// Initialize database
	if err := app.initDatabase(); err != nil {
		return fmt.Errorf("database initialization failed: %w", err)
	}

	// Initialize Kafka
	if err := app.initKafka(); err != nil {
		return fmt.Errorf("kafka initialization failed: %w", err)
	}

	// Initialize services
	app.initServices()

	// Start polling for unpublished events
	pollingInterval := time.Duration(getEnvAsInt("POLLING_INTERVAL_SECONDS", 5)) * time.Second
	batchSize := getEnvAsInt("BATCH_SIZE", 1000)
	numWorkers := getEnvAsInt("NUM_WORKERS", 16)

	go func() {
		if err := app.publisherService.StartPolling(app.rootCtx, pollingInterval, batchSize, numWorkers); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("Polling stopped with error: %v", err)
			}
		}
	}()

	app.Run = true
	log.Println("Application started successfully")

	return nil
}

func (app *Application) shutdown() {
	log.Println("Shutting down application...")

	// Cancel root context
	if app.rootCancel != nil {
		app.rootCancel()
	}

	// Close Kafka connection
	if app.kafkaConnector != nil {
		if err := app.kafkaConnector.Close(); err != nil {
			log.Printf("Error closing Kafka connection: %v", err)
		} else {
			log.Println("Kafka connection closed")
		}
	}

	// Close database connection
	if app.db != nil {
		if err := app.db.Close(); err != nil {
			log.Printf("Error closing database connection: %v", err)
		} else {
			log.Println("Database connection closed")
		}
	}

	log.Println("Application shutdown complete")
}

func (app *Application) waitForShutdown() {
	<-app.shutdownChannel
	app.shutdown()
}

func main() {
	log.Println("=== Outbox Publisher Service ===")
	log.Println("Starting application...")

	app := &Application{
		Run:             false,
		shutdownChannel: make(chan bool),
		cSignal:         make(chan os.Signal, 1),
	}

	// Setup signal handler for graceful shutdown
	signal.Notify(app.cSignal, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-app.cSignal
		log.Printf("Received signal: %v", sig)
		app.Run = false
		app.shutdownChannel <- true
	}()

	// Start application
	if err := app.start(); err != nil {
		log.Fatalf("Failed to start application: %v", err)
	}

	// Wait for shutdown signal
	app.waitForShutdown()
}

// Helper functions for environment variables
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	var value int
	if _, err := fmt.Sscanf(valueStr, "%d", &value); err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsSlice(key string, defaultValue []string) []string {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	// Simple split by comma - can be enhanced for more complex parsing
	var result []string
	var current string
	for _, char := range valueStr {
		if char == ',' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}
