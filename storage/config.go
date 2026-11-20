package storage

import (
	"fmt"
	"strings"
)

// Backend identifies a built-in storage adapter.
type Backend string

const (
	BackendPostgres   Backend = "postgres"
	BackendPostgreSQL Backend = BackendPostgres
	BackendSQLite     Backend = "sqlite"
	BackendFile       Backend = "file"
	BackendFileSystem Backend = BackendFile
)

// PostgreSQLConfig configures the PostgreSQL adapter.
type PostgreSQLConfig struct {
	DSN string `json:"dsn" yaml:"dsn" koanf:"dsn"`
}

// SQLiteConfig configures the SQLite adapter.
type SQLiteConfig struct {
	Path string `json:"path" yaml:"path" koanf:"path"`
}

// FileConfig configures the filesystem adapter. Path is a directory managed by the adapter.
type FileConfig struct {
	Path string `json:"path" yaml:"path" koanf:"path"`
}

// Config selects and configures one of the built-in storage adapters.
type Config struct {
	Backend     Backend          `json:"backend" yaml:"backend" koanf:"backend"`
	PostgreSQL  PostgreSQLConfig `json:"postgresql" yaml:"postgresql" koanf:"postgresql"`
	SQLite      SQLiteConfig     `json:"sqlite" yaml:"sqlite" koanf:"sqlite"`
	File        FileConfig       `json:"file" yaml:"file" koanf:"file"`
	AutoMigrate bool             `json:"auto-migrate" yaml:"auto-migrate" koanf:"auto-migrate"`
}

// Normalize accepts common aliases and returns the canonical backend name.
func (b Backend) Normalize() Backend {
	switch strings.ToLower(strings.TrimSpace(string(b))) {
	case "postgres", "postgresql":
		return BackendPostgres
	case "sqlite", "sqlite3":
		return BackendSQLite
	case "file", "filesystem", "file-system":
		return BackendFile
	default:
		return Backend(strings.ToLower(strings.TrimSpace(string(b))))
	}
}

// Validate checks that the selected built-in backend has the required configuration.
func (c Config) Validate() error {
	switch c.Backend.Normalize() {
	case "":
		return fmt.Errorf("%w: backend is required", ErrInvalidConfig)
	case BackendPostgres:
		if strings.TrimSpace(c.PostgreSQL.DSN) == "" {
			return fmt.Errorf("%w: PostgreSQL DSN is required", ErrInvalidConfig)
		}
	case BackendSQLite:
		if strings.TrimSpace(c.SQLite.Path) == "" {
			return fmt.Errorf("%w: SQLite path is required", ErrInvalidConfig)
		}
	case BackendFile:
		if strings.TrimSpace(c.File.Path) == "" {
			return fmt.Errorf("%w: file path is required", ErrInvalidConfig)
		}
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedBackend, c.Backend)
	}
	return nil
}
