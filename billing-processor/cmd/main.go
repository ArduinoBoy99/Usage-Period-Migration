package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"usage-period-migration/pkg/repository/kafka"
	"usage-period-migration/pkg/repository/sql"
)

type Application struct {
	Run             bool
	shutdownChannel chan bool
	cSignal         chan os.Signal

	rootCtx    context.Context
	rootCancel context.CancelFunc

	db              *sql.PostgresDB
	kafkaConnector  *kafka.KafkaConnector
	consumerService strings

	wg sync.WaitGroup
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

func (app *Application) initServices() {

}

func (app *Application) start() error {
	log.Println("Starting application...")

	app.rootCtx, app.rootCancel = context.WithCancel(context.Background())

	// Initialize database
	if err := app.initDatabase(); err != nil {
		return fmt.Errorf("database initialization failed: %w", err)
	}

	// Initialize services
	app.initServices()

	app.Run = true
	return nil
}

func (app *Application) shutdown() {
	log.Println("Shutting down application...")

	// Cancel root context to stop all goroutines
	if app.rootCancel != nil {
		app.rootCancel()
	}

	// Wait for all goroutines to finish
	log.Println("Waiting for goroutines to finish...")
	app.wg.Wait()

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
	log.Println("=== Billing Processor Service ===")
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
