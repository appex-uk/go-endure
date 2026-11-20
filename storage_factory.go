package endure

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/appex-uk/go-endure/storage"
	"github.com/appex-uk/go-endure/storage/file"
	"github.com/appex-uk/go-endure/storage/postgres"
	"github.com/appex-uk/go-endure/storage/sqlite"
)

// Storage is the persistence adapter contract implemented by built-in and custom stores.
type Storage = storage.Storage

// StorageConfig selects and configures a built-in storage backend.
type StorageConfig = storage.Config

// StorageBackend identifies a built-in storage backend.
type StorageBackend = storage.Backend

// PostgreSQLStorageConfig configures the PostgreSQL storage adapter.
type PostgreSQLStorageConfig = storage.PostgreSQLConfig

// SQLiteStorageConfig configures the SQLite storage adapter.
type SQLiteStorageConfig = storage.SQLiteConfig

// FileStorageConfig configures the filesystem storage adapter.
type FileStorageConfig = storage.FileConfig

const (
	StoragePostgres   = storage.BackendPostgres
	StoragePostgreSQL = storage.BackendPostgres
	StorageSQLite     = storage.BackendSQLite
	StorageFile       = storage.BackendFile
	StorageFileSystem = storage.BackendFile
)

// OpenStorage opens a built-in storage adapter. It does not migrate unless AutoMigrate is set.
// The returned adapter may implement io.Closer when it owns a database connection.
func OpenStorage(ctx context.Context, config StorageConfig) (Storage, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	var (
		backend Storage
		err     error
	)
	switch config.Backend.Normalize() {
	case storage.BackendPostgres:
		backend, err = postgres.Open(ctx, config.PostgreSQL)
	case storage.BackendSQLite:
		backend, err = sqlite.Open(ctx, config.SQLite)
	case storage.BackendFile:
		backend, err = file.Open(config.File)
	default:
		return nil, fmt.Errorf("%w: %q", storage.ErrUnsupportedBackend, config.Backend)
	}
	if err != nil {
		return nil, err
	}
	if config.AutoMigrate {
		if err := backend.Migrate(ctx); err != nil {
			migrateErr := fmt.Errorf("endure: migrate storage: %w", err)
			if closeErr := closeStorage(backend); closeErr != nil {
				return nil, errors.Join(migrateErr, fmt.Errorf("endure: close storage after migration failure: %w", closeErr))
			}
			return nil, migrateErr
		}
	}
	return backend, nil
}

func closeStorage(backend Storage) error {
	closer, ok := backend.(io.Closer)
	if !ok {
		return nil
	}
	return closer.Close()
}
