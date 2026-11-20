package endure

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/appex-uk/go-endure/storage"
	"github.com/appex-uk/go-endure/storage/postgres"
)

// Migrate creates the PostgreSQL tables if they do not exist. It is safe to call concurrently.
// Use MigrateStorage for another built-in or custom backend.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return MigrateStorage(ctx, postgres.New(pool))
}

// MigrateStorage initializes any built-in or custom Storage adapter.
func MigrateStorage(ctx context.Context, backend storage.Storage) error {
	if backend == nil {
		return storage.ErrInvalidConfig
	}
	return backend.Migrate(ctx)
}
