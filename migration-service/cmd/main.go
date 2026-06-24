package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"
	"usage-period-migration/pkg/config"

	"usage-period-migration/pkg/repository/sql"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	log.Println("Migration service starting...")

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
		log.Fatal("failed to initialize database: %w", err)
	}

	defer db.Close()

	// optional retry loop (important in docker)
	for i := 0; i < 10; i++ {
		err = db.Ping()
		if err == nil {
			break
		}
		log.Println("waiting for DB...")
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatal("database not ready")
	}

	if err := db.InitializeSchema(context.Background()); err != nil {
		logger.Error("Failed to initialize database schema", slog.Any("error", err))
		os.Exit(1)
	}

	log.Println("migrations completed successfully")
}
