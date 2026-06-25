package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ============================================================================
// OutboxEvent CRUD Operations
// ============================================================================

// GetOutboxEvent retrieves an outbox event by ID
func (p *PostgresDB) GetOutboxEvent(ctx context.Context, id int64) (*OutboxEvent, error) {
	query := `
		SELECT id, event_id, event_type, session_id, sequence, payload,
		       created_at, published_at, retry_count, last_error
		FROM outbox_events
		WHERE id = $1
	`

	event := &OutboxEvent{}
	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&event.ID,
		&event.EventID,
		&event.EventType,
		&event.SessionID,
		&event.Sequence,
		&event.Payload,
		&event.CreatedAt,
		&event.PublishedAt,
		&event.RetryCount,
		&event.LastError,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("outbox event not found: %d", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get outbox event: %w", err)
	}

	return event, nil
}

// GetUnpublishedOutboxEvents retrieves all unpublished outbox events filtered by event type,
// within a transaction using FOR UPDATE SKIP LOCKED for safe concurrent processing.
func (p *PostgresDB) GetUnpublishedOutboxEvents(ctx context.Context, eventType string, limit int) ([]OutboxEvent, *sql.Tx, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	query := `
		SELECT id, event_id, event_type, session_id, sequence, payload,
		       created_at, published_at, retry_count, last_error
		FROM outbox_events
		WHERE published_at IS NULL
		  AND event_type = $1
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, query, eventType, limit)
	if err != nil {
		tx.Rollback()
		return nil, nil, fmt.Errorf("failed to query outbox events: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		event := OutboxEvent{}
		err := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.EventType,
			&event.SessionID,
			&event.Sequence,
			&event.Payload,
			&event.CreatedAt,
			&event.PublishedAt,
			&event.RetryCount,
			&event.LastError,
		)
		if err != nil {
			tx.Rollback()
			return nil, nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		tx.Rollback()
		return nil, nil, fmt.Errorf("error iterating outbox events: %w", err)
	}

	return events, tx, nil
}

// InsertOutboxEvent inserts a new outbox event
func (p *PostgresDB) InsertOutboxEvent(ctx context.Context, event *OutboxEvent) error {
	query := `
		INSERT INTO outbox_events (
			event_id, event_type, session_id, sequence, payload,
			created_at, published_at, retry_count, last_error
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err := p.db.ExecContext(ctx, query,
		event.EventID,
		event.EventType,
		event.SessionID,
		event.Sequence,
		event.Payload,
		event.CreatedAt,
		event.PublishedAt,
		event.RetryCount,
		event.LastError,
	)

	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	return nil
}

// InsertOutboxEventTx inserts a new outbox event within a transaction
func (p *PostgresDB) InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event *OutboxEvent) error {
	query := `
		INSERT INTO outbox_events (
			 event_id, event_type, session_id, sequence, payload,
			created_at, published_at, retry_count, last_error
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err := tx.ExecContext(ctx, query,
		event.EventID,
		event.EventType,
		event.SessionID,
		event.Sequence,
		event.Payload,
		event.CreatedAt,
		event.PublishedAt,
		event.RetryCount,
		event.LastError,
	)

	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	return nil
}

// UpdateOutboxEvent updates retry_count and last_error on failure
func (p *PostgresDB) UpdateOutboxEvent(ctx context.Context, event *OutboxEvent) error {
	query := `
		UPDATE outbox_events
		SET retry_count = $2, last_error = $3
		WHERE id = $1
	`

	result, err := p.db.ExecContext(ctx, query,
		event.ID,
		event.RetryCount,
		event.LastError,
	)

	if err != nil {
		return fmt.Errorf("failed to update outbox event: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("outbox event not found: %d", event.ID)
	}

	return nil
}

// MarkOutboxEventPublished marks an outbox event as published
func (p *PostgresDB) MarkOutboxEventPublished(ctx context.Context, id int64) error {
	query := `
		UPDATE outbox_events
		SET published_at = $2
		WHERE id = $1
	`

	now := time.Now()
	result, err := p.db.ExecContext(ctx, query, id, now)
	if err != nil {
		return fmt.Errorf("failed to mark outbox event as published: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("outbox event not found: %d", id)
	}

	return nil
}

// BatchMarkOutboxEventsPublishedTx marks multiple outbox events as published in a single query within a transaction
func (p *PostgresDB) BatchMarkOutboxEventsPublishedTx(ctx context.Context, tx *sql.Tx, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids)+1)

	now := time.Now()
	args[0] = now

	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id
	}

	query := fmt.Sprintf(`
		UPDATE outbox_events
		SET published_at = $1
		WHERE id IN (%s)
	`, strings.Join(placeholders, ", "))

	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to mark outbox events as published: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if int(rowsAffected) != len(ids) {
		return fmt.Errorf("expected to update %d events, updated %d", len(ids), rowsAffected)
	}

	return nil
}

// CheckIfProcessed checks if an event was already processed and marks it if not.
// Returns true if successfully marked (not yet processed), false if already processed.
func (p *PostgresDB) CheckIfProcessed(ctx context.Context, tx *sql.Tx, eventID string) (bool, error) {
	// Use the processed_events table for idempotency tracking
	exists, err := p.ProcessedOutboxEventExistsTx(ctx, tx, eventID)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil // Already processed
	}

	// Not yet processed - this is checked again at insert time atomically
	return true, nil
}

// ResetRandomPublishedOutboxEvents marks up to `limit` already-published events as
// unpublished so they get re-published. Used to demonstrate idempotency via duplicates.
// Returns the number of rows reset.
func (p *PostgresDB) ResetRandomPublishedOutboxEvents(ctx context.Context, eventType string, limit int) (int64, error) {
	query := `
		UPDATE outbox_events
		SET published_at = NULL
		WHERE id IN (
			SELECT id FROM outbox_events
			WHERE published_at IS NOT NULL
			  AND event_type = $1
			ORDER BY random()
			LIMIT $2
		)
	`

	result, err := p.db.ExecContext(ctx, query, eventType, limit)
	if err != nil {
		return 0, fmt.Errorf("failed to reset published outbox events: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// GetPublishedOutboxEvents fetches up to `limit` already-published events of a type.
// Read-only; used by the replayer to re-emit duplicates and exercise consumer idempotency.
func (p *PostgresDB) GetPublishedOutboxEvents(ctx context.Context, eventType string, limit int) ([]OutboxEvent, error) {
	query := `
		SELECT id, event_id, event_type, session_id, sequence, payload,
		       created_at, published_at, retry_count, last_error
		FROM outbox_events
		WHERE published_at IS NOT NULL
		  AND event_type = $1
		ORDER BY random()
		LIMIT $2
	`

	rows, err := p.db.QueryContext(ctx, query, eventType, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query published outbox events: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		event := OutboxEvent{}
		if err := rows.Scan(
			&event.ID, &event.EventID, &event.EventType, &event.SessionID,
			&event.Sequence, &event.Payload, &event.CreatedAt, &event.PublishedAt,
			&event.RetryCount, &event.LastError,
		); err != nil {
			return nil, fmt.Errorf("failed to scan published outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating published outbox events: %w", err)
	}
	return events, nil
}
