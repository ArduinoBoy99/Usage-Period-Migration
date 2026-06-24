package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ============================================================================
// UsageSessions CRUD Operations
// ============================================================================

// CountUnbilledUsageSessions returns the count of usage sessions that need outbox
func (p *PostgresDB) CountUnbilledUsageSessions(ctx context.Context, interval time.Duration) (int64, error) {
	cutoff := time.Now().Add(-interval)

	query := `
		SELECT COUNT(*)
		FROM usage_sessions
		WHERE status = $1
		  AND last_billed_at < $2
	`

	var count int64
	err := p.db.QueryRowContext(ctx, query, SESSION_ACTIVE, cutoff).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count unbilled usage sessions: %w", err)
	}

	return count, nil
}

// CountActiveSessions returns the count of active sessions
func (p *PostgresDB) CountActiveSessions(ctx context.Context) (int64, error) {
	query := `
		SELECT COUNT(*)
		FROM usage_sessions
		WHERE status = $1
	`

	var count int64
	err := p.db.QueryRowContext(ctx, query, SESSION_ACTIVE).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count active sessions: %w", err)
	}

	return count, nil
}

// GetActiveSessionIDs retrieves IDs of active sessions
func (p *PostgresDB) GetActiveSessionIDs(ctx context.Context, limit int) ([]string, error) {
	query := `
		SELECT id
		FROM usage_sessions
		WHERE status = $1
		ORDER BY start_at ASC
		LIMIT $2
	`

	rows, err := p.db.QueryContext(ctx, query, SESSION_ACTIVE, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query active session IDs: %w", err)
	}
	defer rows.Close()

	var sessionIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan session ID: %w", err)
		}
		sessionIDs = append(sessionIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating session IDs: %w", err)
	}

	return sessionIDs, nil
}

// GetUnbilledUsageSessions retrieves usage sessions that need outbox
// (last_billed_at is over 1 hour ago and status is ACTIVE)
func (p *PostgresDB) GetUnbilledUsageSessions(ctx context.Context, limit int, interval time.Duration) ([]*UsageSessions, error) {
	cutoff := time.Now().Add(-interval)

	query := `
		SELECT id, sandbox_id, organization_id, start_at, end_at, 
		       last_billed_at, status, billing_sequence, 
		       cpu, gpu, ram_gb, disk_gb, 
		       region, sandbox_class, recorded_at, billing_status
		FROM usage_sessions
		WHERE status = $1
		  AND last_billed_at < NOW() - $3
		ORDER BY last_billed_at ASC
		LIMIT $2
	`

	rows, err := p.db.QueryContext(ctx, query, SESSION_ACTIVE, limit, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to query unbilled usage sessions: %w", err)
	}
	defer rows.Close()

	var sessions []*UsageSessions
	for rows.Next() {
		session := &UsageSessions{}
		err := rows.Scan(
			&session.ID,
			&session.SandboxID,
			&session.OrganizationID,
			&session.StartAt,
			&session.EndAt,
			&session.LastBilledAt,
			&session.Status,
			&session.BillingSequence,
			&session.CPU,
			&session.GPU,
			&session.RamGB,
			&session.DiskGB,
			&session.Region,
			&session.SandboxClass,
			&session.RecordedAt,
			&session.BillingStatus,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan usage session: %w", err)
		}
		sessions = append(sessions, session)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating usage sessions: %w", err)
	}

	return sessions, nil
}

// GetUnbilledUsageSessionsAfterCursor retrieves unbilled sessions after a cursor (last processed ID)
func (p *PostgresDB) GetUnbilledUsageSessionsAfterCursor(ctx context.Context, lastID string, limit int, interval time.Duration) ([]*UsageSessions, error) {
	cutoff := time.Now().Add(-interval)

	query := `
		SELECT id, sandbox_id, organization_id, start_at, end_at, 
		       last_billed_at, status, billing_sequence, 
		       cpu, gpu, ram_gb, disk_gb, 
		       region, sandbox_class, recorded_at, billing_status
		FROM usage_sessions
		WHERE status = $1
		  AND last_billed_at < NOW() - INTERVAL $4
		  AND ($2 = '' OR id > $2)
		ORDER BY id ASC
		LIMIT $3
	`

	rows, err := p.db.QueryContext(ctx, query, SESSION_ACTIVE, lastID, limit, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to query unbilled usage sessions: %w", err)
	}
	defer rows.Close()

	var sessions []*UsageSessions
	for rows.Next() {
		session := &UsageSessions{}
		err := rows.Scan(
			&session.ID,
			&session.SandboxID,
			&session.OrganizationID,
			&session.StartAt,
			&session.EndAt,
			&session.LastBilledAt,
			&session.Status,
			&session.BillingSequence,
			&session.CPU,
			&session.GPU,
			&session.RamGB,
			&session.DiskGB,
			&session.Region,
			&session.SandboxClass,
			&session.RecordedAt,
			&session.BillingStatus,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan usage session: %w", err)
		}
		sessions = append(sessions, session)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating usage sessions: %w", err)
	}

	return sessions, nil
}

// InsertUsageSession inserts a new usage session
func (p *PostgresDB) InsertUsageSession(ctx context.Context, session *UsageSessions) error {
	query := `
		INSERT INTO usage_sessions (
			id, sandbox_id, organization_id, start_at, end_at, 
			last_billed_at, billing_sequence, billing_status, status, cpu, gpu, ram_gb, disk_gb, 
			region, sandbox_class, recorded_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
	`

	_, err := p.db.ExecContext(ctx, query,
		session.ID,
		session.SandboxID,
		session.OrganizationID,
		session.StartAt,
		session.EndAt,
		session.LastBilledAt,
		session.BillingSequence,
		session.BillingStatus,
		session.Status,
		session.CPU,
		session.GPU,
		session.RamGB,
		session.DiskGB,
		session.Region,
		session.SandboxClass,
		session.RecordedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to insert usage session: %w", err)
	}

	return nil
}

// UpdateUsageSessionComplete updates a usage session when outbox is complete
// Updates: last_billed_at, billing_sequence, status to SESSION_FINISHED, and end_at
func (p *PostgresDB) UpdateUsageSessionComplete(
	ctx context.Context,
	tx *sql.Tx,
	sessionID string,
	lastBilledAt time.Time,
	billingSequence int64,
	endAt time.Time,
) error {
	query := `
		UPDATE usage_sessions
		SET last_billed_at = $2,
		    billing_sequence = $3,
		    status = $4,
		    end_at = $5,
		    billing_status = $6
		WHERE id = $1
	`

	var result sql.Result
	var err error

	if tx != nil {
		result, err = tx.ExecContext(ctx, query,
			sessionID,
			lastBilledAt,
			billingSequence,
			SESSION_FINISHED,
			endAt,
			BILLING_COMPLETED,
		)
	} else {
		result, err = p.db.ExecContext(ctx, query,
			sessionID,
			lastBilledAt,
			billingSequence,
			SESSION_FINISHED,
			endAt,
			BILLING_COMPLETED,
		)
	}

	if err != nil {
		return fmt.Errorf("failed to update usage session complete: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("usage session not found: %s", sessionID)
	}

	return nil
}

// GetUsageSessionByID retrieves a usage session by ID
func (p *PostgresDB) GetUsageSessionByID(ctx context.Context, sessionID string) (*UsageSessions, error) {
	query := `
		SELECT id, sandbox_id, organization_id, start_at, end_at, 
		       last_billed_at, status, billing_status, billing_sequence, 
		       cpu, gpu, ram_gb, disk_gb, 
		       region, sandbox_class, recorded_at
		FROM usage_sessions
		WHERE id = $1
	`

	session := &UsageSessions{}
	err := p.db.QueryRowContext(ctx, query, sessionID).Scan(
		&session.ID,
		&session.SandboxID,
		&session.OrganizationID,
		&session.StartAt,
		&session.EndAt,
		&session.LastBilledAt,
		&session.Status,
		&session.BillingStatus,
		&session.BillingSequence,
		&session.CPU,
		&session.GPU,
		&session.RamGB,
		&session.DiskGB,
		&session.Region,
		&session.SandboxClass,
		&session.RecordedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("usage session not found: %s", sessionID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get usage session: %w", err)
	}

	return session, nil
}
