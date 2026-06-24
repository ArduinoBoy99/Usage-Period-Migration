package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"usage-period-migration/pkg/config"

	"usage-period-migration/billing-processor/service/billing"
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

	db             *sql.PostgresDB
	kafkaConnector *kafka.KafkaConnector
	billingService billing.Service

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

func (app *Application) initKafka() error {
	brokersStr := config.GetEnv("KAFKA_BROKERS", "kafka:9092")
	brokers := strings.Split(brokersStr, ",")

	kafkaConfig := kafka.Config{
		Brokers:           brokers,
		Topic:             config.GetEnv("KAFKA_TOPIC", "events"),
		ConsumerGroup:     config.GetEnv("KAFKA_CONSUMER_GROUP", "billing-processors"),
		MaxAttempts:       config.GetEnvAsInt("KAFKA_MAX_ATTEMPTS", 3),
		MinBytes:          config.GetEnvAsInt("KAFKA_MIN_BYTES", 1),
		MaxBytes:          config.GetEnvAsInt("KAFKA_MAX_BYTES", 10485760),
		CommitInterval:    time.Duration(config.GetEnvAsInt("KAFKA_COMMIT_INTERVAL_MS", 1000)) * time.Millisecond,
		SessionTimeout:    time.Duration(config.GetEnvAsInt("KAFKA_SESSION_TIMEOUT_MS", 10000)) * time.Millisecond,
		HeartbeatInterval: time.Duration(config.GetEnvAsInt("KAFKA_HEARTBEAT_INTERVAL_MS", 3000)) * time.Millisecond,
	}

	logger.Info("Connecting to Kafka brokers", slog.Any("brokers", brokers))
	logger.Info("Kafka configuration",
		slog.String("topic", kafkaConfig.Topic),
		slog.String("consumer_group", kafkaConfig.ConsumerGroup))

	connector, err := kafka.NewKafkaConnector(kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize Kafka connector: %w", err)
	}

	app.kafkaConnector = connector
	logger.Info("Kafka connector initialized")
	return nil
}

func (app *Application) initServices() {
	app.billingService = billing.NewService(app.db, app.kafkaConnector, logger)
	logger.Info("Billing service initialized")
}

func (app *Application) startBillingConsumer() {
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		logger.Info("Starting billing consumer")

		if err := app.billingService.StartConsumer(app.rootCtx); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error("Billing consumer stopped with error", slog.Any("error", err))
			} else {
				logger.Info("Billing consumer stopped gracefully")
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

	if err := app.initKafka(); err != nil {
		return fmt.Errorf("kafka initialization failed: %w", err)
	}

	app.initServices()
	app.startBillingConsumer()

	app.Run = true
	logger.Info("Application started successfully")
	logger.Info("Billing Processor Running",
		slog.String("consumer_group", config.GetEnv("KAFKA_CONSUMER_GROUP", "billing-processors")),
		slog.String("topic", config.GetEnv("KAFKA_TOPIC", "events")))
	logger.Info("Processing billing chunks with 5-step flow")
	logger.Info("  1. Read Kafka event")
	logger.Info("  2. Idempotency check (dedupe by event_id)")
	logger.Info("  3. Build Metronome payload")
	logger.Info("  4. Send to Metronome")
	logger.Info("  5. Publish BillingProcessed event")

	return nil
}

func (app *Application) shutdown() {
	logger.Info("Shutting down application")

	if app.rootCancel != nil {
		app.rootCancel()
	}

	logger.Info("Waiting for goroutines to finish")
	app.wg.Wait()

	if app.kafkaConnector != nil {
		if err := app.kafkaConnector.Close(); err != nil {
			logger.Error("Error closing Kafka connector", slog.Any("error", err))
		} else {
			logger.Info("Kafka connector closed")
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

	logger.Info("Billing Processor Service starting")

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
