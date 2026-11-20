// Package sqlite provides a SQLite-backed go-endure storage adapter.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"github.com/appex-uk/go-endure/storage"
)

const rollbackTimeout = 5 * time.Second

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS endure_workflow (
		id               TEXT PRIMARY KEY,
		name             TEXT NOT NULL,
		input            BLOB NOT NULL,
		status           TEXT NOT NULL CHECK (status IN ('queued', 'running', 'waiting', 'completed', 'failed')),
		output           BLOB,
		error            TEXT,
		run_at           INTEGER NOT NULL,
		waiting_on       TEXT,
		worker_id        TEXT,
		lease_token      TEXT,
		lease_expires_at INTEGER,
		attempt          INTEGER NOT NULL DEFAULT 0,
		created_at       INTEGER NOT NULL,
		updated_at       INTEGER NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS endure_workflow_queued_idx
		ON endure_workflow (run_at) WHERE status = 'queued'`,
	`CREATE INDEX IF NOT EXISTS endure_workflow_lease_idx
		ON endure_workflow (lease_expires_at) WHERE status = 'running'`,
	`CREATE TABLE IF NOT EXISTS endure_step (
		workflow_id  TEXT NOT NULL REFERENCES endure_workflow (id) ON DELETE CASCADE,
		name         TEXT NOT NULL,
		status       TEXT NOT NULL CHECK (status IN ('running', 'retrying', 'completed', 'failed')),
		output       BLOB,
		error        TEXT,
		attempts     INTEGER NOT NULL DEFAULT 0,
		not_before   INTEGER,
		started_at   INTEGER NOT NULL,
		completed_at INTEGER,
		PRIMARY KEY (workflow_id, name)
	)`,
	`CREATE TABLE IF NOT EXISTS endure_signal (
		workflow_id TEXT NOT NULL REFERENCES endure_workflow (id) ON DELETE CASCADE,
		name        TEXT NOT NULL,
		payload     BLOB NOT NULL,
		created_at  INTEGER NOT NULL,
		PRIMARY KEY (workflow_id, name)
	)`,
}

// Store persists workflows in SQLite. It serializes access through one database connection,
// while BEGIN IMMEDIATE provides atomic claims and fencing across multiple processes.
type Store struct {
	db     *sql.DB
	ownsDB bool
}

var _ storage.Storage = (*Store)(nil)

// New wraps db in a SQLite adapter. The adapter limits this handle to one connection because
// SQLite uses per-connection PRAGMA configuration and serializes writes.
func New(db *sql.DB) *Store {
	if db != nil {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	return &Store{db: db}
}

// Open opens path and creates its parent directory when needed.
func Open(ctx context.Context, config storage.SQLiteConfig) (*Store, error) {
	if strings.TrimSpace(config.Path) == "" {
		return nil, fmt.Errorf("%w: SQLite path is required", storage.ErrInvalidConfig)
	}
	if dir := sqliteDirectory(config.Path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("endure: create SQLite directory: %w", err)
		}
	}
	if err := createSQLiteFile(config.Path); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", config.Path)
	if err != nil {
		return nil, fmt.Errorf("endure: open SQLite: %w", err)
	}
	store := New(db)
	store.ownsDB = true
	if err := store.configure(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// Close closes an adapter-opened database. It is a no-op for databases supplied to New.
func (s *Store) Close() error {
	if s == nil || s.db == nil || !s.ownsDB {
		return nil
	}
	return s.db.Close()
}

// Migrate creates the SQLite schema. It is safe to call concurrently.
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.configure(ctx); err != nil {
		return err
	}
	return s.withImmediateTransaction(ctx, func(conn *sql.Conn) error {
		for _, statement := range schemaStatements {
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("endure: apply SQLite schema: %w", err)
			}
		}
		return nil
	})
}

// CreateWorkflow creates an idempotent workflow record.
func (s *Store) CreateWorkflow(ctx context.Context, workflowID, name string, input []byte) error {
	return s.withImmediateTransaction(ctx, func(conn *sql.Conn) error {
		now := time.Now().UnixNano()
		_, err := conn.ExecContext(ctx, `
INSERT INTO endure_workflow (id, name, input, status, run_at, created_at, updated_at)
VALUES (?, ?, ?, 'queued', ?, ?, ?)
ON CONFLICT (id) DO NOTHING`,
			workflowID, name, input, now, now, now)
		if err != nil {
			return err
		}
		var existing string
		if err := conn.QueryRowContext(ctx, `SELECT name FROM endure_workflow WHERE id = ?`, workflowID).
			Scan(&existing); err != nil {
			return err
		}
		if existing != name {
			return storage.ErrNameMismatch
		}
		return nil
	})
}

// ClaimWorkflows atomically claims due workflows and returns their inputs.
func (s *Store) ClaimWorkflows(ctx context.Context, request storage.ClaimRequest) ([]storage.Claim, error) {
	if request.Limit <= 0 || len(request.Names) == 0 {
		return nil, nil
	}
	type candidate struct {
		id      string
		name    string
		input   []byte
		attempt int
	}

	var claims []storage.Claim
	err := s.withImmediateTransaction(ctx, func(conn *sql.Conn) error {
		placeholders, args := namePlaceholders(request.Names)
		claimCutoff := time.Now().UnixNano()
		args = append(args, claimCutoff, claimCutoff, request.Limit)
		rows, err := conn.QueryContext(ctx, fmt.Sprintf(`
SELECT id, name, input, attempt
FROM endure_workflow
WHERE name IN (%s)
  AND ((status = 'queued' AND run_at <= ?)
    OR (status = 'running' AND lease_expires_at <= ?))
ORDER BY run_at, id
LIMIT ?`, placeholders), args...)
		if err != nil {
			return err
		}
		var candidates []candidate
		for rows.Next() {
			var item candidate
			if err := rows.Scan(&item.id, &item.name, &item.input, &item.attempt); err != nil {
				_ = rows.Close()
				return err
			}
			candidates = append(candidates, item)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}

		now := time.Now()
		expiresAt := now.Add(request.LeaseFor).UnixNano()
		for _, item := range candidates {
			token := uuid.NewString()
			result, err := conn.ExecContext(ctx, `
UPDATE endure_workflow
SET status = 'running', worker_id = ?, lease_token = ?, lease_expires_at = ?,
    attempt = attempt + 1, waiting_on = NULL, updated_at = ?
WHERE id = ?`, request.WorkerID, token, expiresAt, now.UnixNano(), item.id)
			if err != nil {
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if affected != 1 {
				return storage.ErrLeaseLost
			}
			claims = append(claims, storage.Claim{
				Lease:   storage.Lease{ID: item.id, Token: token},
				Name:    item.name,
				Input:   cloneBytes(item.input),
				Attempt: item.attempt + 1,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}

// RenewLease extends a current lease.
func (s *Store) RenewLease(ctx context.Context, lease storage.Lease, leaseFor time.Duration) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE endure_workflow
SET lease_expires_at = ?, updated_at = ?
WHERE id = ? AND lease_token = ? AND status = 'running'`,
		time.Now().Add(leaseFor).UnixNano(), time.Now().UnixNano(), lease.ID, lease.Token)
	if err != nil {
		return err
	}
	return requireAffected(result)
}

// GetStep returns a step checkpoint or nil when it does not exist.
func (s *Store) GetStep(ctx context.Context, workflowID, name string) (*storage.StepRecord, error) {
	var (
		record     storage.StepRecord
		output     []byte
		errMessage sql.NullString
		notBefore  sql.NullInt64
	)
	err := s.db.QueryRowContext(ctx, `
SELECT status, output, error, attempts, not_before
FROM endure_step
WHERE workflow_id = ? AND name = ?`, workflowID, name).
		Scan(&record.Status, &output, &errMessage, &record.Attempts, &notBefore)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	record.Output = cloneBytes(output)
	record.Error = errMessage.String
	record.NotBefore = timeFromNullable(notBefore)
	return &record, nil
}

// WriteStep atomically upserts a fenced step checkpoint.
func (s *Store) WriteStep(ctx context.Context, lease storage.Lease, write storage.StepWrite) error {
	return s.withImmediateTransaction(ctx, func(conn *sql.Conn) error {
		if err := verifyLease(ctx, conn, lease); err != nil {
			return err
		}
		now := time.Now().UnixNano()
		completedAt := any(nil)
		if write.Status == storage.StepCompleted || write.Status == storage.StepFailed {
			completedAt = now
		}
		_, err := conn.ExecContext(ctx, `
INSERT INTO endure_step (
	workflow_id, name, status, attempts, output, error, not_before, started_at, completed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (workflow_id, name) DO UPDATE SET
	status = excluded.status,
	attempts = excluded.attempts,
	output = excluded.output,
	error = excluded.error,
	not_before = excluded.not_before,
	completed_at = excluded.completed_at`,
			lease.ID, write.Name, write.Status, write.Attempts, write.Output,
			nullableString(write.Error), unixNanoNullable(write.NotBefore), now, completedAt)
		return err
	})
}

// GetSignal returns a buffered signal when present.
func (s *Store) GetSignal(ctx context.Context, workflowID, name string) ([]byte, bool, error) {
	var payload []byte
	err := s.db.QueryRowContext(ctx, `
SELECT payload FROM endure_signal WHERE workflow_id = ? AND name = ?`, workflowID, name).
		Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	return cloneBytes(payload), err == nil, err
}

// SignalWorkflow stores the first signal for name and wakes a matching parked workflow.
func (s *Store) SignalWorkflow(ctx context.Context, workflowID, name string, payload []byte) error {
	return s.withImmediateTransaction(ctx, func(conn *sql.Conn) error {
		var one int
		err := conn.QueryRowContext(ctx, `SELECT 1 FROM endure_workflow WHERE id = ?`, workflowID).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrNotFound
		}
		if err != nil {
			return err
		}
		now := time.Now().UnixNano()
		if _, err := conn.ExecContext(ctx, `
INSERT INTO endure_signal (workflow_id, name, payload, created_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (workflow_id, name) DO NOTHING`, workflowID, name, payload, now); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `
UPDATE endure_workflow
SET status = 'queued', run_at = ?, waiting_on = NULL, updated_at = ?
WHERE id = ? AND status = 'waiting' AND waiting_on = ?`, now, now, workflowID, name)
		return err
	})
}

// FinishWorkflow atomically stores a fenced terminal outcome.
func (s *Store) FinishWorkflow(ctx context.Context, lease storage.Lease, status storage.WorkflowStatus, output []byte, errMsg string) error {
	return s.withImmediateTransaction(ctx, func(conn *sql.Conn) error {
		if err := verifyLease(ctx, conn, lease); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `
UPDATE endure_workflow
SET status = ?, output = ?, error = ?, lease_token = NULL, lease_expires_at = NULL,
	worker_id = NULL, updated_at = ?
WHERE id = ?`, status, output, nullableString(errMsg), time.Now().UnixNano(), lease.ID)
		return err
	})
}

// RequeueWorkflow releases a fenced workflow until runAt.
func (s *Store) RequeueWorkflow(ctx context.Context, lease storage.Lease, runAt time.Time) error {
	return s.withImmediateTransaction(ctx, func(conn *sql.Conn) error {
		if err := verifyLease(ctx, conn, lease); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `
UPDATE endure_workflow
SET status = 'queued', run_at = ?, lease_token = NULL, lease_expires_at = NULL,
	worker_id = NULL, updated_at = ?
WHERE id = ?`, runAt.UnixNano(), time.Now().UnixNano(), lease.ID)
		return err
	})
}

// WaitForSignal parks or immediately requeues a fenced workflow based on buffered signals.
func (s *Store) WaitForSignal(ctx context.Context, lease storage.Lease, name string) error {
	return s.withImmediateTransaction(ctx, func(conn *sql.Conn) error {
		if err := verifyLease(ctx, conn, lease); err != nil {
			return err
		}
		var exists bool
		if err := conn.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1 FROM endure_signal WHERE workflow_id = ? AND name = ?
)`, lease.ID, name).Scan(&exists); err != nil {
			return err
		}
		now := time.Now().UnixNano()
		status := storage.WorkflowWaiting
		waitingOn := any(name)
		if exists {
			status = storage.WorkflowQueued
			waitingOn = nil
		}
		_, err := conn.ExecContext(ctx, `
UPDATE endure_workflow
SET status = ?, run_at = ?, waiting_on = ?, lease_token = NULL,
	lease_expires_at = NULL, worker_id = NULL, updated_at = ?
WHERE id = ?`, status, now, waitingOn, now, lease.ID)
		return err
	})
}

// GetWorkflow returns a workflow result.
func (s *Store) GetWorkflow(ctx context.Context, workflowID string) (*storage.WorkflowRecord, error) {
	var (
		record     storage.WorkflowRecord
		output     []byte
		errMessage sql.NullString
	)
	err := s.db.QueryRowContext(ctx, `
SELECT status, output, error FROM endure_workflow WHERE id = ?`, workflowID).
		Scan(&record.Status, &output, &errMessage)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	record.Output = cloneBytes(output)
	record.Error = errMessage.String
	return &record, nil
}

func (s *Store) configure(ctx context.Context) error {
	if s.db == nil {
		return fmt.Errorf("%w: SQLite database is nil", storage.ErrInvalidConfig)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("endure: connect to SQLite: %w", err)
	}
	defer func() { _ = conn.Close() }()
	for _, statement := range []string{
		`PRAGMA foreign_keys = ON`,
		`PRAGMA busy_timeout = 5000`,
	} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("endure: configure SQLite: %w", err)
		}
	}
	return conn.PingContext(ctx)
}

func (s *Store) withImmediateTransaction(ctx context.Context, fn func(*sql.Conn) error) (err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("endure: begin SQLite transaction: %w", err)
	}

	defer func() {
		if err == nil {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()
		if _, rollbackErr := conn.ExecContext(rollbackCtx, `ROLLBACK`); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("endure: rollback SQLite transaction: %w", rollbackErr))
		}
	}()
	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("endure: commit SQLite transaction: %w", err)
	}
	return nil
}

func verifyLease(ctx context.Context, conn *sql.Conn, lease storage.Lease) error {
	var one int
	err := conn.QueryRowContext(ctx, `
SELECT 1 FROM endure_workflow
WHERE id = ? AND lease_token = ? AND status = 'running'`, lease.ID, lease.Token).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrLeaseLost
	}
	return err
}

func requireAffected(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return storage.ErrLeaseLost
	}
	return nil
}

func namePlaceholders(names []string) (string, []any) {
	args := make([]any, len(names))
	placeholders := strings.Repeat("?,", len(names))
	for i, name := range names {
		args[i] = name
	}
	return strings.TrimSuffix(placeholders, ","), args
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func unixNanoNullable(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UnixNano()
}

func timeFromNullable(value sql.NullInt64) *time.Time {
	if !value.Valid {
		return nil
	}
	result := time.Unix(0, value.Int64).UTC()
	return &result
}

func sqliteDirectory(path string) string {
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return ""
	}
	dir := filepath.Dir(path)
	if dir == "." {
		return ""
	}
	return dir
}

func createSQLiteFile(path string) error {
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("endure: create SQLite database: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("endure: close SQLite database file: %w", err)
	}
	return nil
}

func cloneBytes(input []byte) []byte {
	return append([]byte(nil), input...)
}
