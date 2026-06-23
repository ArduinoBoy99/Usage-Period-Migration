package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// PostgresDB wraps the database connection and provides methods for data access
type PostgresDB struct {
	db *sql.DB
}

// Config holds database configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// NewPostgresql creates a new PostgreSQL database connection
func NewPostgresql(cfg Config) (*PostgresDB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName, cfg.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return &PostgresDB{db: db}, nil
}

// Close closes the database connection
func (p *PostgresDB) Close() error {
	return p.db.Close()
}

// DB returns the underlying database connection
func (p *PostgresDB) DB() *sql.DB {
	return p.db
}

// BeginTx starts a new transaction
func (p *PostgresDB) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return p.db.BeginTx(ctx, nil)
}

// InitializeSchema creates all required tables with indexes if they don't exist
func (p *PostgresDB) InitializeSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS usage_sessions (
		id VARCHAR(255) PRIMARY KEY,
		sandboxId VARCHAR(255) NOT NULL,
		organizationId VARCHAR(255) NOT NULL,
		startAt TIMESTAMP NOT NULL,
		endAt TIMESTAMP,
		status VARCHAR(50) NOT NULL,
		lastBilledAt TIMESTAMP,
		billingStatus VARCHAR(50) NOT NULL,
		billingSequence BIGINT NOT NULL DEFAULT 0,
		cpu FLOAT8,
		gpu FLOAT8,
		ramGB FLOAT8,
		diskGB FLOAT8,
		region VARCHAR(100) NOT NULL,
		sandboxClass VARCHAR(50) NOT NULL DEFAULT 'container',
		recordedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_usage_sessions_last_billed_at ON usage_sessions(lastBilledAt);
	CREATE INDEX IF NOT EXISTS idx_usage_sessions_end_at ON usage_sessions(endAt);
	CREATE INDEX IF NOT EXISTS idx_usage_sessions_status ON usage_sessions(status);

	CREATE TABLE IF NOT EXISTS outbox_events (
		id BIGSERIAL PRIMARY KEY,
		event_id VARCHAR(255) NOT NULL UNIQUE,
		event_type VARCHAR(100) NOT NULL,
		session_id VARCHAR(255) NOT NULL,
		sequence BIGINT NOT NULL,
		payload JSONB,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		published_at TIMESTAMP,
		retry_count INT NOT NULL DEFAULT 0,
		last_error TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_outbox_events_event_id ON outbox_events(event_id);
	CREATE INDEX IF NOT EXISTS idx_outbox_events_published_at ON outbox_events(published_at);

	CREATE TABLE IF NOT EXISTS processed_billing_events (
		id BIGSERIAL PRIMARY KEY,
		event_id VARCHAR(255) NOT NULL UNIQUE,
		session_id VARCHAR(255) NOT NULL,
		sequence BIGINT NOT NULL,
		transaction_id VARCHAR(255) NOT NULL,
		processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_processed_billing_events_event_id ON processed_billing_events(event_id);

	CREATE TABLE IF NOT EXISTS processed_outbox_events (
		id BIGSERIAL PRIMARY KEY,
		event_id VARCHAR(255) NOT NULL UNIQUE,
		session_id VARCHAR(255) NOT NULL,
		sequence BIGINT NOT NULL,
		processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_processed_outbox_events_event_id ON processed_outbox_events(event_id);
	`

	_, err := p.db.ExecContext(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	return nil
}
