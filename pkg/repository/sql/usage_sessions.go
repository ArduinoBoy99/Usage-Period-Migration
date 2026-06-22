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
func (p *PostgresDB) CountUnbilledUsageSessions(ctx context.Context) (int64, error) {
	query := `
		SELECT COUNT(*)
		FROM usage_sessions
		WHERE "status" = $1
		  AND "lastBilledAt" < NOW() - INTERVAL '1 hour'
	`

	var count int64
	err := p.db.QueryRowContext(ctx, query, SESSION_ACTIVE).Scan(&count)
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
		WHERE "status" = $1
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
		WHERE "status" = $1
		ORDER BY "startAt" ASC
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
// (lastBilledAt is over 1 hour ago and billingStatus is ACTIVE)
func (p *PostgresDB) GetUnbilledUsageSessions(ctx context.Context, limit int) ([]*UsageSessions, error) {
	query := `
		SELECT id, "sandboxId", "organizationId", "startAt", "endAt", 
		       "lastBilledAt", "status", "billingSequence", 
		       cpu, gpu, "ramGB", "diskGB", 
		       region, "sandboxClass", "recordedAt", "sentAt", "metronomeSentAt"
		FROM usage_sessions
		WHERE "billingStatus" = $1
		  AND "lastBilledAt" < NOW() - INTERVAL '1 hour'
		ORDER BY "lastBilledAt" ASC
		LIMIT $2
	`

	rows, err := p.db.QueryContext(ctx, query, SESSION_ACTIVE, limit)
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
func (p *PostgresDB) GetUnbilledUsageSessionsAfterCursor(ctx context.Context, lastID string, limit int) ([]*UsageSessions, error) {
	query := `
		SELECT id, "sandboxId", "organizationId", "startAt", "endAt", 
		       "lastBilledAt", "status", "billingSequence", 
		       cpu, gpu, "ramGB", "diskGB", 
		       region, "sandboxClass", "recordedAt"
		FROM usage_sessions
		WHERE "billingStatus" = $1
		  AND "lastBilledAt" < NOW() - INTERVAL '1 hour'
		  AND ($2 = '' OR id > $2)
		ORDER BY id ASC
		LIMIT $3
	`

	rows, err := p.db.QueryContext(ctx, query, SESSION_ACTIVE, lastID, limit)
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
			id, "sandboxId", "organizationId", "startAt", "endAt", 
			"lastBilledAt", "billingSequence", "billingStatus", "status", cpu, gpu, "ramGB", "diskGB", 
			region, "sandboxClass", "recordedAt"
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
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
// Updates: lastBilledAt, billingSequence, status to SESSION_FINISHED, and endAt
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
		SET "lastBilledAt" = $2,
		    "billingSequence" = $3,
		    "status" = $4,
		    "endAt" = $5,
		    "billingStatus" = $6
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
		SELECT id, "sandboxId", "organizationId", "startAt", "endAt", 
		       "lastBilledAt", "status", "billingStatus", "billingSequence", 
		       cpu, gpu, "ramGB", "diskGB", 
		       region, "sandboxClass", "recordedAt"
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
