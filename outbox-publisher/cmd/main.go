package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
	"usage-period-migration/pkg/config"

	"usage-period-migration/outbox-publisher/service/events"
	"usage-period-migration/pkg/repository/kafka"
	"usage-period-migration/pkg/repository/sql"
)

var logger *slog.Logger

type Application struct {
	Run             bool
	shutdownChannel chan bool
	cSignal         chan os.Signal

	rootCtx    context.Context
	rootCancel context.CancelFunc

	db               *sql.PostgresDB
	kafkaConnector   *kafka.KafkaConnector
	publisherService events.OutboxPublisherService
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

func (app *Application) initKafka() error {
	brokers := config.GetEnvAsSlice("KAFKA_BROKERS", []string{"kafka:9092"})

	kafkaConfig := kafka.Config{
		Brokers:       brokers,
		Topic:         kafka.TopicUsageBillingChunks,
		ConsumerGroup: config.GetEnv("KAFKA_CONSUMER_GROUP", "usage-outbox-processor"),
		MaxAttempts:   config.GetEnvAsInt("KAFKA_MAX_ATTEMPTS", 3),
	}

	logger.Info("Connecting to Kafka brokers", slog.Any("brokers", brokers))

	kafkaConnector, err := kafka.NewKafkaConnector(kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize kafka: %w", err)
	}

	app.kafkaConnector = kafkaConnector
	logger.Info("Kafka connection established")
	return nil
}

func (app *Application) initServices() {
	app.publisherService = events.NewOutboxPublisherService(app.db, app.kafkaConnector, logger)
	logger.Info("Outbox publisher service initialized")
}

func (app *Application) startLeasingWorkers() {
	pollInterval := time.Duration(config.GetEnvAsInt("POLL_INTERVAL_MS", 500)) * time.Millisecond
	batchSize := config.GetEnvAsInt("BATCH_SIZE", 500)
	numWorkers := config.GetEnvAsInt("NUM_WORKERS", runtime.NumCPU()*2)

	logger.Info("Starting leasing workers",
		slog.Int("num_workers", numWorkers),
		slog.Int("batch_size", batchSize),
		slog.Duration("poll_interval", pollInterval))
	app.publisherService.StartWorkers(app.rootCtx, numWorkers, pollInterval, batchSize)
}

func (app *Application) start() error {
	logger.Info("Starting application")

	app.rootCtx, app.rootCancel = context.WithCancel(context.Background())

	if err := app.initDatabase(); err != nil {
		return fmt.Errorf("database init failed: %w", err)
	}
	if err := app.initKafka(); err != nil {
		return fmt.Errorf("kafka init failed: %w", err)
	}

	app.initServices()
	app.startLeasingWorkers()

	app.Run = true
	logger.Info("Outbox Publisher Running",
		slog.String("model", "FOR UPDATE SKIP LOCKED"),
		slog.String("concurrency", "Horizontal via worker replicas"),
		slog.String("idempotency", "processed_outbox_events table"))
	return nil
}

func (app *Application) shutdown() {
	logger.Info("Shutting down application")

	if app.rootCancel != nil {
		app.rootCancel()
	}

	if app.kafkaConnector != nil {
		if err := app.kafkaConnector.Close(); err != nil {
			logger.Error("Error closing Kafka connection", slog.Any("error", err))
		} else {
			logger.Info("Kafka connection closed")
		}
	}

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

	logger.Info("Outbox Publisher Service starting")

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
