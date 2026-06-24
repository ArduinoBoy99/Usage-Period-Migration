package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"usage-period-migration/pkg/config"
	"usage-period-migration/pkg/repository/sql"
	"usage-period-migration/usage-service/service/outbox"
	"usage-period-migration/usage-service/service/sessions"
)

var logger *slog.Logger

const (
	test_outbox_interval time.Duration = time.Duration(time.Minute * 2) // 2 minute for testing
	outbox_interval      time.Duration = time.Duration(time.Hour * 1)   // 1 minute for production
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
	dbConfig := sql.Config{
		Host:     config.GetEnv("DB_HOST", "localhost"),
		Port:     config.GetEnvAsInt("DB_PORT", 5432),
		User:     config.GetEnv("DB_USER", "postgres"),
		Password: config.GetEnv("DB_PASSWORD", "password"),
		DBName:   config.GetEnv("DB_NAME", "usage_db"),
		SSLMode:  config.GetEnv("DB_SSL_MODE", "disable"),
	}

	logger.Info("Connecting to database",
		slog.String("user", dbConfig.User),
		slog.String("host", dbConfig.Host),
		slog.Int("port", dbConfig.Port),
		slog.String("database", dbConfig.DBName))

	db, err := sql.NewPostgresql(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	app.db = db
	logger.Info("Database connection established")

	return nil
}

func (app *Application) initServices() {
	app.sessionsService = sessions.NewService(app.db)
	logger.Info("Sessions service initialized")

	app.outboxService = outbox.NewService(app.db, logger, test_outbox_interval)
	logger.Info("Outbox service initialized")
}

func (app *Application) startPeriodicSessionCreator() {
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		logger.Info("Starting periodic session creator", slog.String("interval", "every 40 seconds"))

		ticker := time.NewTicker(40 * time.Second)
		defer ticker.Stop()

		app.bulkCreateSessions()

		for {
			select {
			case <-app.rootCtx.Done():
				logger.Info("Stopping periodic session creator")
				return
			case <-ticker.C:
				app.bulkCreateSessions()
			}
		}
	}()
}

func (app *Application) bulkCreateSessions() {
	logger.Info("Bulk Creating Sessions")

	count := config.GetEnvAsInt("SESSION_CREATE_COUNT", 10)

	if err := app.sessionsService.GenerateBulkActiveSessions(app.rootCtx, count); err != nil {
		logger.Error("Error creating bulk sessions",
			slog.Int("count", count),
			slog.Any("error", err))
	}
}

func (app *Application) startPeriodicSessionFinisher() {
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		logger.Info("Starting periodic session finisher", slog.String("interval", "every 2 minutes"))

		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()

		time.Sleep(1 * time.Minute)

		for {
			select {
			case <-app.rootCtx.Done():
				logger.Info("Stopping periodic session finisher")
				return
			case <-ticker.C:
				app.finishActiveSessions()
			}
		}
	}()
}

func (app *Application) finishActiveSessions() {
	logger.Info("Finishing Active Sessions")

	batchSize := config.GetEnvAsInt("SESSION_FINISH_BATCH_SIZE", 50)

	count, err := app.sessionsService.FinishActiveSessions(app.rootCtx, batchSize)
	if err != nil {
		logger.Error("Error finishing sessions",
			slog.Int("batch_size", batchSize),
			slog.Any("error", err))
		return
	}

	logger.Info("Successfully finished sessions", slog.Int("count", count))
}

func (app *Application) startOutboxScanner() {
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		logger.Info("Starting outbox scanner", slog.String("interval", "every 1 minute"))

		if err := app.outboxService.StartPeriodicScanner(app.rootCtx); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error("Outbox scanner stopped with error", slog.Any("error", err))
			} else {
				logger.Info("Outbox scanner stopped gracefully")
			}
		}
	}()
}

func (app *Application) start() error {
	logger.Info("Starting application")

	app.rootCtx, app.rootCancel = context.WithCancel(context.Background())

	if err := app.initDatabase(); err != nil {
		return fmt.Errorf("database initialization failed: %w", err)
	}

	app.initServices()

	app.startPeriodicSessionCreator()
	app.startPeriodicSessionFinisher()
	app.startOutboxScanner()

	app.Run = true
	logger.Info("Application started successfully")
	logger.Info("Usage Service Simulation Running",
		slog.String("session_creation", "every 40 seconds"),
		slog.String("session_finishing", "every 2 minutes"),
		slog.String("outbox_scanning", "every 1 minute"))

	return nil
}

func (app *Application) shutdown() {
	logger.Info("Shutting down application")

	if app.rootCancel != nil {
		app.rootCancel()
	}

	logger.Info("Waiting for goroutines to finish")
	app.wg.Wait()

	if app.db != nil {
		if err := app.db.Close(); err != nil {
			logger.Error("Error closing database connection", slog.Any("error", err))
		} else {
			logger.Info("Database connection closed")
		}
	}

	logger.Info("Application shutdown complete")
}

func (app *Application) waitForShutdown() {
	<-app.shutdownChannel
	app.shutdown()
}

func main() {
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("Usage Service starting")

	app := &Application{
		Run:             false,
		shutdownChannel: make(chan bool),
		cSignal:         make(chan os.Signal, 1),
	}

	signal.Notify(app.cSignal, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-app.cSignal
		logger.Info("Received signal", slog.Any("signal", sig))
		app.Run = false
		app.shutdownChannel <- true
	}()

	if err := app.start(); err != nil {
		logger.Error("Failed to start application", slog.Any("error", err))
		os.Exit(1)
	}

	app.waitForShutdown()
}
