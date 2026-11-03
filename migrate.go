package endure

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// Migrate creates the endure tables if they do not exist. It is safe to call concurrently.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
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
