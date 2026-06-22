package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"usage-period-migration/pkg/repository/sql"
	"usage-period-migration/usage-service/service/outbox"
	"usage-period-migration/usage-service/service/sessions"
)

type Application struct {
	Run             bool
	shutdownChannel chan bool
	cSignal         chan os.Signal

	rootCtx    context.Context
	rootCancel context.CancelFunc

	db              *sql.PostgresDB
	sessionsService sessions.Service
	outboxService   outbox.Service

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
	// Create sessions service
	app.sessionsService = sessions.NewService(app.db)
	log.Println("Sessions service initialized")

	// Create outbox service
	app.outboxService = outbox.NewService(app.db)
	log.Println("Outbox service initialized")
}

func (app *Application) startPeriodicSessionCreator() {
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		log.Println("Starting periodic session creator (every 40 seconds)...")

		ticker := time.NewTicker(40 * time.Second)
		defer ticker.Stop()

		// Run immediately on start
		app.bulkCreateSessions()

		for {
			select {
			case <-app.rootCtx.Done():
				log.Println("Stopping periodic session creator...")
				return
			case <-ticker.C:
				app.bulkCreateSessions()
			}
		}
	}()
}

func (app *Application) bulkCreateSessions() {
	log.Println("=== Bulk Creating Sessions ===")

	// Get configuration from environment or use defaults
	count := getEnvAsInt("SESSION_CREATE_COUNT", 10)

	if err := app.sessionsService.GenerateBulkActiveSessions(app.rootCtx, count); err != nil {
		log.Printf("Error creating bulk sessions: %v", err)
	}
}

func (app *Application) startPeriodicSessionFinisher() {
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		log.Println("Starting periodic session finisher (every 2 minutes)...")

		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()

		// Wait a bit before first run to allow some sessions to be created
		time.Sleep(1 * time.Minute)

		for {
			select {
			case <-app.rootCtx.Done():
				log.Println("Stopping periodic session finisher...")
				return
			case <-ticker.C:
				app.finishActiveSessions()
			}
		}
	}()
}

func (app *Application) finishActiveSessions() {
	log.Println("=== Finishing Active Sessions ===")

	// Get configuration from environment or use defaults
	batchSize := getEnvAsInt("SESSION_FINISH_BATCH_SIZE", 50)

	count, err := app.sessionsService.FinishActiveSessions(app.rootCtx, batchSize)
	if err != nil {
		log.Printf("Error finishing sessions: %v", err)
		return
	}

	log.Printf("Successfully finished %d sessions", count)
}

func (app *Application) startOutboxScanner() {
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		log.Println("Starting outbox scanner (every 1 minute)...")

		// The outbox service has its own periodic scanner
		if err := app.outboxService.StartPeriodicScanner(app.rootCtx); err != nil {
			if err != context.Canceled {
				log.Printf("Outbox scanner stopped with error: %v", err)
			} else {
				log.Println("Outbox scanner stopped gracefully")
			}
		}
	}()
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

	// Start periodic tasks
	app.startPeriodicSessionCreator()
	app.startPeriodicSessionFinisher()
	app.startOutboxScanner()

	app.Run = true
	log.Println("Application started successfully")
	log.Println("")
	log.Println("=== Usage Service Simulation Running ===")
	log.Println("- Creating sessions every 40 seconds")
	log.Println("- Finishing sessions every 2 minutes")
	log.Println("- Scanning for outbox events every 1 minute")
	log.Println("")

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
	log.Println("=== Usage Service ===")
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
