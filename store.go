package endure

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type store struct{ pool *pgxpool.Pool }

// lease identifies one claim of a workflow; every write is fenced by its token.
type lease struct {
	id    string
	token string
}

type claimed struct {
	lease
	name    string
	input   []byte
	attempt int
}

type stepRow struct {
	status    string
	output    []byte
	errMsg    string
	attempts  int
	notBefore *time.Time
}

const (
	stepRunning   = "running"
	stepRetrying  = "retrying"
	stepCompleted = "completed"
	stepFailed    = "failed"
)

// fenced runs fn in a transaction that holds the workflow row lock and only if the lease is still ours.
func (s *store) fenced(ctx context.Context, l lease, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var one int
	err = tx.QueryRow(ctx,
		`SELECT 1 FROM endure_workflow WHERE id = $1 AND lease_token = $2::uuid AND status = 'running' FOR NO KEY UPDATE`,
		l.id, l.token).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *store) create(ctx context.Context, id, name string, input []byte) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO endure_workflow (id, name, input, status) VALUES ($1, $2, $3, 'queued') ON CONFLICT (id) DO NOTHING`,
		id, name, input)
	if err != nil {
		return err
	}
	var existing string
	if err := s.pool.QueryRow(ctx, `SELECT name FROM endure_workflow WHERE id = $1`, id).Scan(&existing); err != nil {
		return err
	}
	if existing != name {
		return ErrNameMismatch
	}
	return nil
}

func (s *store) claim(ctx context.Context, workerID string, leaseFor time.Duration, names []string, limit int) ([]claimed, error) {
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
		workerID, leaseFor.Seconds(), names, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []claimed
	for rows.Next() {
		var c claimed
		if err := rows.Scan(&c.id, &c.token, &c.name, &c.input, &c.attempt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *store) renew(ctx context.Context, l lease, leaseFor time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE endure_workflow SET lease_expires_at = now() + make_interval(secs => $3::float8)
WHERE id = $1 AND lease_token = $2::uuid AND status = 'running'`,
		l.id, l.token, leaseFor.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *store) getStep(ctx context.Context, workflowID, name string) (*stepRow, error) {
	var r stepRow
	var errMsg *string
	err := s.pool.QueryRow(ctx,
		`SELECT status, output, error, attempts, not_before FROM endure_step WHERE workflow_id = $1 AND name = $2`,
		workflowID, name).Scan(&r.status, &r.output, &errMsg, &r.attempts, &r.notBefore)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if errMsg != nil {
		r.errMsg = *errMsg
	}
	return &r, nil
}

func (s *store) writeStep(ctx context.Context, l lease, name, status string, attempts int, output []byte, errMsg string, notBefore *time.Time) error {
	var errArg *string
	if errMsg != "" {
		errArg = &errMsg
	}
	return s.fenced(ctx, l, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
INSERT INTO endure_step (workflow_id, name, status, attempts, output, error, not_before, completed_at)
VALUES ($1, $2, $3::text, $4, $5, $6, $7, CASE WHEN $3::text IN ('completed', 'failed') THEN now() END)
ON CONFLICT (workflow_id, name) DO UPDATE SET
    status = EXCLUDED.status, attempts = EXCLUDED.attempts, output = EXCLUDED.output,
    error = EXCLUDED.error, not_before = EXCLUDED.not_before, completed_at = EXCLUDED.completed_at`,
			l.id, name, status, attempts, output, errArg, notBefore)
		return err
	})
}

func (s *store) getSignal(ctx context.Context, workflowID, name string) ([]byte, bool, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx,
		`SELECT payload FROM endure_signal WHERE workflow_id = $1 AND name = $2`, workflowID, name).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	return payload, err == nil, err
}

// signal stores the payload and wakes the workflow if it is waiting on name.
// The row lock orders it against waitFor so a wakeup can never be lost.
func (s *store) signal(ctx context.Context, workflowID, name string, payload []byte) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM endure_workflow WHERE id = $1 FOR NO KEY UPDATE`, workflowID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO endure_signal (workflow_id, name, payload) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		workflowID, name, payload); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
UPDATE endure_workflow SET status = 'queued', run_at = now(), waiting_on = NULL, updated_at = now()
WHERE id = $1 AND status = 'waiting' AND waiting_on = $2`, workflowID, name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *store) finish(ctx context.Context, l lease, status string, output []byte, errMsg string) error {
	var errArg *string
	if errMsg != "" {
		errArg = &errMsg
	}
	return s.fenced(ctx, l, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
UPDATE endure_workflow SET status = $2, output = $3, error = $4,
    lease_token = NULL, lease_expires_at = NULL, worker_id = NULL, updated_at = now()
WHERE id = $1`, l.id, status, output, errArg)
		return err
	})
}

func (s *store) requeue(ctx context.Context, l lease, runAt time.Time) error {
	return s.fenced(ctx, l, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
UPDATE endure_workflow SET status = 'queued', run_at = $2,
    lease_token = NULL, lease_expires_at = NULL, worker_id = NULL, updated_at = now()
WHERE id = $1`, l.id, runAt)
		return err
	})
}

// waitFor parks the workflow until a signal arrives, or requeues immediately if it already has.
func (s *store) waitFor(ctx context.Context, l lease, name string) error {
	return s.fenced(ctx, l, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
UPDATE endure_workflow SET
    status = CASE WHEN EXISTS (SELECT 1 FROM endure_signal WHERE workflow_id = $1 AND name = $2)
                  THEN 'queued' ELSE 'waiting' END,
    run_at = now(),
    waiting_on = CASE WHEN EXISTS (SELECT 1 FROM endure_signal WHERE workflow_id = $1 AND name = $2)
                      THEN NULL ELSE $2 END,
    lease_token = NULL, lease_expires_at = NULL, worker_id = NULL, updated_at = now()
WHERE id = $1`, l.id, name)
		return err
	})
}

type workflowRow struct {
	status string
	output []byte
	errMsg string
}

func (s *store) getWorkflow(ctx context.Context, id string) (*workflowRow, error) {
	var r workflowRow
	var errMsg *string
	err := s.pool.QueryRow(ctx, `SELECT status, output, error FROM endure_workflow WHERE id = $1`, id).
		Scan(&r.status, &r.output, &errMsg)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if errMsg != nil {
		r.errMsg = *errMsg
	}
	return &r, nil
}
