package sessions

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
	repo "usage-period-migration/pkg/repository/sql"

	"github.com/google/uuid"
)

// Service defines the interface for the Sessions service
type Service interface {
	// CreateSession creates a new active usage session
	CreateSession(ctx context.Context, orgID string, sandboxID string, config SessionConfig) error

	// CreateSessionWithResourceSpecs creates a session with specific resource configurations
	CreateSessionWithResourceSpecs(ctx context.Context, orgID string, sandboxID string, cpu, gpu, ramGB, diskGB float64, region string, class repo.SandboxClass) error

	// FinishSession marks a session as finished
	FinishSession(ctx context.Context, sessionID string) error

	// FinishActiveSessions counts active sessions and finishes them in batches
	FinishActiveSessions(ctx context.Context, batchSize int) (int, error)

	// GenerateBulkActiveSessions generates multiple active sessions for testing
	GenerateBulkActiveSessions(ctx context.Context, count int) error
}

// SessionConfig holds configuration for session creation
type SessionConfig struct {
	CPU          *float64
	GPU          *float64
	RamGB        *float64
	DiskGB       *float64
	Region       string
	SandboxClass repo.SandboxClass
	StartAt      *time.Time
	Status       repo.UsageSessionStatus
}

// RealisticSessionConfig holds configuration for realistic session generation
type RealisticSessionConfig struct {
	OrganizationIDs  []string
	MinDurationHours int
	MaxDurationHours int
	ActiveRatio      float64 // Ratio of active vs finished sessions (0.0-1.0)
}

// Repository defines the interface for session repository operations
type Repository interface {
	InsertUsageSession(ctx context.Context, session *repo.UsageSessions) error
	GetUsageSessionByID(ctx context.Context, sessionID string) (*repo.UsageSessions, error)
	BeginTx(ctx context.Context) (*sql.Tx, error)
	UpdateUsageSessionComplete(ctx context.Context, tx *sql.Tx, sessionID string, finishedAt time.Time, newSequence int64, lastBilledAt time.Time) error
	InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event *repo.OutboxEvent) error
	CountActiveSessions(ctx context.Context) (int64, error)
	GetActiveSessionIDs(ctx context.Context, batchSize int) ([]string, error)
}

// Change the service struct:
type sessionService struct {
	db Repository // Change from *repo.PostgresDB
}

// NewService creates a new Sessions service
func NewService(db Repository) Service {
	return &sessionService{
		db: db,
	}
}

// CreateSession creates a new active usage session
func (s *sessionService) CreateSession(ctx context.Context, orgID string, sandboxID string, config SessionConfig) error {
	sessionID := uuid.New().String()
	now := time.Now()

	startAt := now
	if config.StartAt != nil {
		startAt = *config.StartAt
	}

	status := repo.SESSION_ACTIVE
	if config.Status != "" {
		status = config.Status
	}

	// Set default values if not provided
	cpu := config.CPU
	gpu := config.GPU
	ramGB := config.RamGB
	diskGB := config.DiskGB
	region := config.Region
	sandboxClass := config.SandboxClass

	if cpu == nil {
		defaultCPU := 2.0
		cpu = &defaultCPU
	}
	if gpu == nil {
		defaultGPU := 0.0
		gpu = &defaultGPU
	}
	if ramGB == nil {
		defaultRam := 4.0
		ramGB = &defaultRam
	}
	if diskGB == nil {
		defaultDisk := 20.0
		diskGB = &defaultDisk
	}
	if region == "" {
		region = "us-east-1"
	}
	if sandboxClass == "" {
		sandboxClass = repo.CONTAINER
	}

	session := &repo.UsageSessions{
		ID:              sessionID,
		SandboxID:       sandboxID,
		OrganizationID:  orgID,
		StartAt:         startAt,
		EndAt:           nil,
		Status:          status,
		LastBilledAt:    &startAt,
		BillingStatus:   repo.BILLING_ACTIVE,
		BillingSequence: 0,
		CPU:             cpu,
		GPU:             gpu,
		RamGB:           ramGB,
		DiskGB:          diskGB,
		Region:          region,
		SandboxClass:    sandboxClass,
		RecordedAt:      now,
	}

	if err := s.db.InsertUsageSession(ctx, session); err != nil {
		return fmt.Errorf("failed to insert usage session: %w", err)
	}

	fmt.Printf("Created session: %s for org: %s, sandbox: %s\n", sessionID, orgID, sandboxID)
	return nil
}

// CreateSessionWithResourceSpecs creates a session with specific resource configurations
func (s *sessionService) CreateSessionWithResourceSpecs(ctx context.Context, orgID string, sandboxID string, cpu, gpu, ramGB, diskGB float64, region string, class repo.SandboxClass) error {
	config := SessionConfig{
		CPU:          &cpu,
		GPU:          &gpu,
		RamGB:        &ramGB,
		DiskGB:       &diskGB,
		Region:       region,
		SandboxClass: class,
	}

	return s.CreateSession(ctx, orgID, sandboxID, config)
}

// FinishSession marks a session as finished and creates an outbox event
func (s *sessionService) FinishSession(ctx context.Context, sessionID string) error {
	// Fetch the session first
	session, err := s.db.GetUsageSessionByID(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("failed to get session: %w", err)
	}

	now := time.Now()

	// Begin transaction
	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Increment billing sequence
	newSequence := session.BillingSequence + 1

	err = s.db.UpdateUsageSessionComplete(ctx, tx, sessionID, now, newSequence, now)
	if err != nil {
		return fmt.Errorf("failed to update session: %w", err)
	}

	// Generate unique event ID
	eventID := uuid.New().String()

	from := time.Time{}

	if session.LastBilledAt != nil {
		from = *session.LastBilledAt
	}

	// Create payload
	payload := repo.BillingChunkCreated{
		EventID:        eventID,
		SessionID:      session.ID,
		SandboxID:      session.SandboxID,
		Sequence:       newSequence,
		From:           from,
		To:             now,
		CPU:            session.CPU,
		GPU:            session.GPU,
		RAMGB:          session.RamGB,
		DiskGB:         session.DiskGB,
		Region:         session.Region,
		SandboxClass:   string(session.SandboxClass),
		OrganizationID: session.OrganizationID,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal billing chunk: %w", err)
	}

	// Create outbox event
	outboxEvent := &repo.OutboxEvent{
		EventID:     eventID,
		EventType:   "billing_chunk_created",
		SessionID:   session.ID,
		Sequence:    newSequence,
		Payload:     payloadBytes,
		CreatedAt:   time.Now(),
		PublishedAt: nil,
		RetryCount:  0,
		LastError:   nil,
	}

	// Insert outbox event
	if err := s.db.InsertOutboxEventTx(ctx, tx, outboxEvent); err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("Finished session: %s and created outbox event (sequence: %d)\n", sessionID, newSequence)
	return nil
}

// GenerateBulkActiveSessions generates multiple active sessions for testing
func (s *sessionService) GenerateBulkActiveSessions(ctx context.Context, count int) error {
	fmt.Printf("Generating %d active sessions...\n", count)

	for i := 0; i < count; i++ {
		orgID := fmt.Sprintf("org-%d", rand.Intn(100))
		sandboxID := uuid.New().String()

		// Randomize start time (between 2 hours ago and 10 hours ago)
		hoursAgo := rand.Intn(8) + 2
		startAt := time.Now().Add(time.Duration(-hoursAgo) * time.Hour)

		// Randomize resources
		cpu := float64(rand.Intn(8) + 1)
		gpu := float64(rand.Intn(2))
		ramGB := float64(rand.Intn(32) + 2)
		diskGB := float64(rand.Intn(100) + 10)

		// Randomize region
		regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}
		region := regions[rand.Intn(len(regions))]

		// Randomize sandbox class
		classes := []repo.SandboxClass{repo.CONTAINER, repo.LINUX_VM, repo.WINDOWS, repo.ANDROID}
		class := classes[rand.Intn(len(classes))]

		config := SessionConfig{
			CPU:          &cpu,
			GPU:          &gpu,
			RamGB:        &ramGB,
			DiskGB:       &diskGB,
			Region:       region,
			SandboxClass: class,
			StartAt:      &startAt,
			Status:       repo.SESSION_ACTIVE,
		}

		if err := s.CreateSession(ctx, orgID, sandboxID, config); err != nil {
			return fmt.Errorf("failed to create session %d: %w", i, err)
		}

		if (i+1)%100 == 0 {
			fmt.Printf("Generated %d/%d sessions\n", i+1, count)
		}
	}

	fmt.Printf("Successfully generated %d active sessions\n", count)
	return nil
}

// FinishActiveSessions counts active sessions and finishes them in batches
func (s *sessionService) FinishActiveSessions(ctx context.Context, batchSize int) (int, error) {
	// Count total active sessions
	totalCount, err := s.db.CountActiveSessions(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to count active sessions: %w", err)
	}

	if totalCount == 0 {
		fmt.Println("No active sessions to finish")
		return 0, nil
	}

	fmt.Printf("Found %d active sessions to finish\n", totalCount)

	totalFinished := 0
	totalFailed := 0

	// Process in batches
	for {
		// Get batch of active session IDs
		sessionIDs, err := s.db.GetActiveSessionIDs(ctx, batchSize)
		if err != nil {
			return totalFinished, fmt.Errorf("failed to get active session IDs: %w", err)
		}

		if len(sessionIDs) == 0 {
			break
		}

		// Finish each session
		for _, sessionID := range sessionIDs {
			if err := s.FinishSession(ctx, sessionID); err != nil {
				fmt.Printf("Error finishing session %s: %v\n", sessionID, err)
				totalFailed++
				continue
			}
			totalFinished++
		}

		fmt.Printf("Finished %d/%d sessions (failed: %d)\n", totalFinished, totalCount, totalFailed)

		// If we got fewer than batchSize, we're done
		if len(sessionIDs) < batchSize {
			break
		}
	}

	fmt.Printf("Completed finishing sessions: %d successful, %d failed\n", totalFinished, totalFailed)
	return totalFinished, nil
}
