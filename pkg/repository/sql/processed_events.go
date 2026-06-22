package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ============================================================================
// ProcessedEvent CRUD Operations
// ============================================================================

// GetProcessedEventByEventID retrieves a processed event by event_id
func (p *PostgresDB) GetProcessedEventByEventID(ctx context.Context, eventID string) (*ProcessedEvent, error) {
	query := `
		SELECT id, event_id, session_id, sequence, processed_at
		FROM processed_events
		WHERE event_id = $1
	`

	event := &ProcessedEvent{}
	err := p.db.QueryRowContext(ctx, query, eventID).Scan(
		&event.ID,
		&event.EventID,
		&event.SessionID,
		&event.Sequence,
		&event.TransactionID,
		&event.ProcessedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("processed event not found: %s", eventID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get processed event: %w", err)
	}

	return event, nil
}

// GetProcessedEventByID retrieves a processed event by ID
func (p *PostgresDB) GetProcessedEventByID(ctx context.Context, id uuid.UUID) (*ProcessedEvent, error) {
	query := `
		SELECT id, event_id, session_id, sequence, transaction_id, processed_at
		FROM processed_events
		WHERE id = $1
	`

	event := &ProcessedEvent{}
	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&event.ID,
		&event.EventID,
		&event.SessionID,
		&event.Sequence,
		&event.TransactionID,
		&event.ProcessedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("processed event not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get processed event: %w", err)
	}

	return event, nil
}

// InsertProcessedEvent inserts a new processed event
func (p *PostgresDB) InsertProcessedEvent(ctx context.Context, event *ProcessedEvent) error {
	query := `
		INSERT INTO processed_events (
			id, event_id, session_id, sequence, transaction_id, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := p.db.ExecContext(ctx, query,
		event.ID,
		event.EventID,
		event.SessionID,
		event.Sequence,
		event.TransactionID,
		event.ProcessedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to insert processed event: %w", err)
	}

	return nil
}

// InsertProcessedEventTx inserts a new processed event within a transaction
func (p *PostgresDB) InsertProcessedEventTx(ctx context.Context, tx *sql.Tx, event *ProcessedEvent) error {
	query := `
		INSERT INTO processed_events (
			id, event_id, session_id, sequence, transaction_id, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := tx.ExecContext(ctx, query,
		event.ID,
		event.EventID,
		event.SessionID,
		event.Sequence,
		event.TransactionID,
		event.ProcessedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to insert processed event: %w", err)
	}

	return nil
}

// ProcessedEventExists checks if a processed event exists by event_id
func (p *PostgresDB) ProcessedEventExists(ctx context.Context, eventID string) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM processed_events WHERE event_id = $1
		)
	`

	var exists bool
	err := p.db.QueryRowContext(ctx, query, eventID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check processed event existence: %w", err)
	}

	return exists, nil
}

// ProcessedEventExistsTx checks if a processed event exists by event_id within a transaction
func (p *PostgresDB) ProcessedEventExistsTx(ctx context.Context, tx *sql.Tx, eventID string) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM processed_events WHERE event_id = $1
		)
	`

	var exists bool
	err := tx.QueryRowContext(ctx, query, eventID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check processed event existence: %w", err)
	}

	return exists, nil
}

// DeleteProcessedEvent deletes a processed event by ID
func (p *PostgresDB) DeleteProcessedEvent(ctx context.Context, id uuid.UUID) error {
	query := `
		DELETE FROM processed_events
		WHERE id = $1
	`

	result, err := p.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete processed event: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("processed event not found: %s", id)
	}

	return nil
}

// GetProcessedEventsByTimeRange retrieves processed events within a time range
func (p *PostgresDB) GetProcessedEventsByTimeRange(ctx context.Context, from, to time.Time, limit int) ([]*ProcessedEvent, error) {
	query := `
		SELECT id, event_id, session_id, sequence, transaction_id, processed_at
		FROM processed_events
		WHERE processed_at >= $1 AND processed_at <= $2
		ORDER BY processed_at DESC
		LIMIT $3
	`

	rows, err := p.db.QueryContext(ctx, query, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query processed events: %w", err)
	}
	defer rows.Close()

	var events []*ProcessedEvent
	for rows.Next() {
		event := &ProcessedEvent{}
		err := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.SessionID,
			&event.Sequence,
			&event.TransactionID,
			&event.ProcessedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan processed event: %w", err)
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating processed events: %w", err)
	}

	return events, nil
}

// CountProcessedEvents returns the total count of processed events
func (p *PostgresDB) CountProcessedEvents(ctx context.Context) (int64, error) {
	query := `
		SELECT COUNT(*)
		FROM processed_events
	`

	var count int64
	err := p.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count processed events: %w", err)
	}

	return count, nil
}
