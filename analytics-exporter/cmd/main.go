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
	"usage-period-migration/analytics-exporter/service/clickhouse-exporter"
	"usage-period-migration/pkg/config"

	"usage-period-migration/pkg/repository/kafka"
)

var logger *slog.Logger

type Application struct {
	Run             bool
	shutdownChannel chan bool
	cSignal         chan os.Signal

	rootCtx    context.Context
	rootCancel context.CancelFunc

	kafkaConnector   *kafka.KafkaConnector
	analyticsService clickhouse_exporter.Service

	wg sync.WaitGroup
}

func (app *Application) initKafka() error {
	brokersStr := config.GetEnv("KAFKA_BROKERS", "kafka:9092")
	brokers := strings.Split(brokersStr, ",")

	kafkaConfig := kafka.Config{
		Brokers:           brokers,
		Topic:             config.GetEnv("KAFKA_TOPIC", "billing-processed"),
		ConsumerGroup:     config.GetEnv("KAFKA_CONSUMER_GROUP", "analytics-exporters"),
		MaxAttempts:       config.GetEnvAsInt("KAFKA_MAX_ATTEMPTS", 3),
		MinBytes:          config.GetEnvAsInt("KAFKA_MIN_BYTES", 1),
		MaxBytes:          config.GetEnvAsInt("KAFKA_MAX_BYTES", 10485760),
		CommitInterval:    time.Duration(config.GetEnvAsInt("KAFKA_COMMIT_INTERVAL_MS", 1000)) * time.Millisecond,
		SessionTimeout:    time.Duration(config.GetEnvAsInt("KAFKA_SESSION_TIMEOUT_MS", 10000)) * time.Millisecond,
		HeartbeatInterval: time.Duration(config.GetEnvAsInt("KAFKA_HEARTBEAT_INTERVAL_MS", 3000)) * time.Millisecond,
	}

	logger.Info("Connecting to Kafka brokers", slog.Any("brokers", brokers))
	logger.Info("Kafka configuration", slog.String("topic", kafkaConfig.Topic), slog.String("consumer_group", kafkaConfig.ConsumerGroup))

	connector, err := kafka.NewKafkaConnector(kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize Kafka connector: %w", err)
	}

	app.kafkaConnector = connector
	logger.Info("Kafka connector initialized")
	return nil
}

func (app *Application) initServices() {
	app.analyticsService = clickhouse_exporter.NewService(app.kafkaConnector, logger)
	logger.Info("Analytics exporter service initialized")
}

func (app *Application) startAnalyticsConsumer() {
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		logger.Info("Starting analytics consumer")

		if err := app.analyticsService.StartConsumer(app.rootCtx); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error("Analytics consumer stopped with error", slog.Any("error", err))
			} else {
				logger.Info("Analytics consumer stopped gracefully")
			}
		}
	}()
}

func (app *Application) start() error {
	logger.Info("Starting application")

	app.rootCtx, app.rootCancel = context.WithCancel(context.Background())

	if err := app.initKafka(); err != nil {
		return fmt.Errorf("kafka initialization failed: %w", err)
	}

	app.initServices()
	app.startAnalyticsConsumer()

	app.Run = true
	logger.Info("Application started successfully")
	logger.Info("Analytics Exporter Running",
		slog.String("consumer_group", config.GetEnv("KAFKA_CONSUMER_GROUP", "analytics-exporters")),
		slog.String("topic", config.GetEnv("KAFKA_TOPIC", "billing-processed")),
		slog.String("flow", "Read billing-processed events from Kafka -> Export to Clickhouse (simulated)"),
	)

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

	logger.Info("Application shutdown complete")
}

func (app *Application) waitForShutdown() {
	<-app.shutdownChannel
	app.shutdown()
}

func main() {
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("Analytics Exporter Service starting")

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
