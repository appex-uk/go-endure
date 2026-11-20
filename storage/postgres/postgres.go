// Package postgres provides a PostgreSQL-backed go-endure storage adapter.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/appex-uk/go-endure/storage"
)

//go:embed schema.sql
var schemaSQL string

// Store persists workflows in PostgreSQL.
type Store struct {
	pool   *pgxpool.Pool
	ownsDB bool
}

var _ storage.Storage = (*Store)(nil)

// New wraps pool in a PostgreSQL adapter.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Open opens a pool from config and verifies connectivity.
func Open(ctx context.Context, config storage.PostgreSQLConfig) (*Store, error) {
	if strings.TrimSpace(config.DSN) == "" {
		return nil, fmt.Errorf("%w: PostgreSQL DSN is required", storage.ErrInvalidConfig)
	}
	pool, err := pgxpool.New(ctx, config.DSN)
	if err != nil {
		return nil, fmt.Errorf("endure: open PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("endure: connect to PostgreSQL: %w", err)
	}
	return &Store{pool: pool, ownsDB: true}, nil
}

// Close closes an adapter-opened pool. It is a no-op for pools supplied to New.
func (s *Store) Close() error {
	if s == nil || s.pool == nil || !s.ownsDB {
		return nil
	}
	s.pool.Close()
	return nil
}

// Migrate creates the endure tables if they do not exist. It is safe to call concurrently.
func (s *Store) Migrate(ctx context.Context) error {
	if s.pool == nil {
		return fmt.Errorf("%w: PostgreSQL pool is nil", storage.ErrInvalidConfig)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("endure: begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('endure_migrate'))`); err != nil {
		return fmt.Errorf("endure: migration lock: %w", err)
	}
	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("endure: apply schema: %w", err)
	}
	return tx.Commit(ctx)
}

// CreateWorkflow creates an idempotent workflow record.
func (s *Store) CreateWorkflow(ctx context.Context, workflowID, name string, input []byte) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO endure_workflow (id, name, input, status)
		VALUES ($1, $2, $3, 'queued') ON CONFLICT (id) DO NOTHING`,
		workflowID, name, input)
	if err != nil {
		return err
	}
	var existing string
	if err := s.pool.QueryRow(ctx, `SELECT name FROM endure_workflow WHERE id = $1`, workflowID).
		Scan(&existing); err != nil {
		return err
	}
	if existing != name {
		return storage.ErrNameMismatch
	}
	return nil
}

// ClaimWorkflows atomically claims due workflows using SKIP LOCKED.
func (s *Store) ClaimWorkflows(ctx context.Context, request storage.ClaimRequest) ([]storage.Claim, error) {
	rows, err := s.pool.Query(ctx, `
UPDATE endure_workflow SET
    status = 'running', worker_id = $1, lease_token = gen_random_uuid(),
    lease_expires_at = now() + make_interval(secs => $2::float8),
    attempt = attempt + 1, waiting_on = NULL, updated_at = now()
WHERE id IN (
    SELECT id FROM endure_workflow
    WHERE name = ANY($3)
      AND ((status = 'queued' AND run_at <= now())
        OR (status = 'running' AND lease_expires_at < now()))
    ORDER BY run_at
    FOR UPDATE SKIP LOCKED
    LIMIT $4)
RETURNING id, lease_token::text, name, input, attempt`,
		request.WorkerID, request.LeaseFor.Seconds(), request.Names, request.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var claims []storage.Claim
	for rows.Next() {
		var claim storage.Claim
		if err := rows.Scan(&claim.ID, &claim.Token, &claim.Name, &claim.Input, &claim.Attempt); err != nil {
			return nil, err
		}
		claims = append(claims, claim)
	}
	return claims, rows.Err()
}

// RenewLease extends a current lease.
func (s *Store) RenewLease(ctx context.Context, lease storage.Lease, leaseFor time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE endure_workflow SET lease_expires_at = now() + make_interval(secs => $3::float8)
WHERE id = $1 AND lease_token::text = $2 AND status = 'running'`,
		lease.ID, lease.Token, leaseFor.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return storage.ErrLeaseLost
	}
	return nil
}

// GetStep returns a step checkpoint or nil when it does not exist.
func (s *Store) GetStep(ctx context.Context, workflowID, name string) (*storage.StepRecord, error) {
	var record storage.StepRecord
	var errMsg *string
	err := s.pool.QueryRow(ctx, `
SELECT status, output, error, attempts, not_before
FROM endure_step
WHERE workflow_id = $1 AND name = $2`, workflowID, name).
		Scan(&record.Status, &record.Output, &errMsg, &record.Attempts, &record.NotBefore)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if errMsg != nil {
		record.Error = *errMsg
	}
	return &record, nil
}

// WriteStep atomically upserts a fenced step checkpoint.
func (s *Store) WriteStep(ctx context.Context, lease storage.Lease, write storage.StepWrite) error {
	return s.fenced(ctx, lease, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
INSERT INTO endure_step (workflow_id, name, status, attempts, output, error, not_before, completed_at)
VALUES ($1, $2, $3::text, $4, $5, $6, $7, CASE WHEN $3::text IN ('completed', 'failed') THEN now() END)
ON CONFLICT (workflow_id, name) DO UPDATE SET
    status = EXCLUDED.status, attempts = EXCLUDED.attempts, output = EXCLUDED.output,
    error = EXCLUDED.error, not_before = EXCLUDED.not_before, completed_at = EXCLUDED.completed_at`,
			lease.ID, write.Name, write.Status, write.Attempts, write.Output, nullableString(write.Error), write.NotBefore)
		return err
	})
}

// GetSignal returns a buffered signal when present.
func (s *Store) GetSignal(ctx context.Context, workflowID, name string) ([]byte, bool, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `
SELECT payload FROM endure_signal WHERE workflow_id = $1 AND name = $2`, workflowID, name).
		Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	return payload, err == nil, err
}

// SignalWorkflow stores the first signal for name and wakes a matching parked workflow.
func (s *Store) SignalWorkflow(ctx context.Context, workflowID, name string, payload []byte) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var one int
	err = tx.QueryRow(ctx, `SELECT 1 FROM endure_workflow WHERE id = $1 FOR NO KEY UPDATE`, workflowID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNotFound
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO endure_signal (workflow_id, name, payload)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING`, workflowID, name, payload); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
UPDATE endure_workflow
SET status = 'queued', run_at = now(), waiting_on = NULL, updated_at = now()
WHERE id = $1 AND status = 'waiting' AND waiting_on = $2`, workflowID, name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// FinishWorkflow atomically stores a fenced terminal outcome.
func (s *Store) FinishWorkflow(ctx context.Context, lease storage.Lease, status storage.WorkflowStatus, output []byte, errMsg string) error {
	return s.fenced(ctx, lease, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
UPDATE endure_workflow
SET status = $2, output = $3, error = $4,
    lease_token = NULL, lease_expires_at = NULL, worker_id = NULL, updated_at = now()
WHERE id = $1`, lease.ID, status, output, nullableString(errMsg))
		return err
	})
}

// RequeueWorkflow releases a fenced workflow until runAt.
func (s *Store) RequeueWorkflow(ctx context.Context, lease storage.Lease, runAt time.Time) error {
	return s.fenced(ctx, lease, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
UPDATE endure_workflow
SET status = 'queued', run_at = $2,
    lease_token = NULL, lease_expires_at = NULL, worker_id = NULL, updated_at = now()
WHERE id = $1`, lease.ID, runAt)
		return err
	})
}

// WaitForSignal parks or immediately requeues a fenced workflow based on buffered signals.
func (s *Store) WaitForSignal(ctx context.Context, lease storage.Lease, name string) error {
	return s.fenced(ctx, lease, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
UPDATE endure_workflow SET
    status = CASE WHEN EXISTS (SELECT 1 FROM endure_signal WHERE workflow_id = $1 AND name = $2)
                  THEN 'queued' ELSE 'waiting' END,
    run_at = now(),
    waiting_on = CASE WHEN EXISTS (SELECT 1 FROM endure_signal WHERE workflow_id = $1 AND name = $2)
                      THEN NULL ELSE $2 END,
    lease_token = NULL, lease_expires_at = NULL, worker_id = NULL, updated_at = now()
WHERE id = $1`, lease.ID, name)
		return err
	})
}

// GetWorkflow returns a workflow result.
func (s *Store) GetWorkflow(ctx context.Context, workflowID string) (*storage.WorkflowRecord, error) {
	var record storage.WorkflowRecord
	var errMsg *string
	err := s.pool.QueryRow(ctx, `SELECT status, output, error FROM endure_workflow WHERE id = $1`, workflowID).
		Scan(&record.Status, &record.Output, &errMsg)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if errMsg != nil {
		record.Error = *errMsg
	}
	return &record, nil
}

func (s *Store) fenced(ctx context.Context, lease storage.Lease, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var one int
	err = tx.QueryRow(ctx, `
SELECT 1 FROM endure_workflow
WHERE id = $1 AND lease_token::text = $2 AND status = 'running'
FOR NO KEY UPDATE`, lease.ID, lease.Token).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
