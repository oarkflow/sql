package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Config controls how the storage layer is initialized.
type Config struct {
	Path          string
	EncryptionKey string
}

// Store wraps a SQLite backed persistence layer for the control plane.
type Store struct {
	db     *sql.DB
	secret *secretCipher
}

// New creates a storage instance, ensuring the database is migrated.
func New(cfg Config) (*Store, error) {
	if cfg.Path == "" {
		cfg.Path = "data/app.db"
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	dsn := fmt.Sprintf("%s?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)", cfg.Path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	db.SetConnMaxLifetime(time.Minute * 5)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	secret, err := newSecretCipher(cfg.EncryptionKey)
	if err != nil {
		db.Close()
		return nil, err
	}

	store := &Store{db: db, secret: secret}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

// Close releases all database resources.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS credentials (
            key TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            description TEXT,
            data BLOB NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        );`,
		`CREATE TABLE IF NOT EXISTS integrations (
            name TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            require_auth INTEGER NOT NULL DEFAULT 0,
            credential_key TEXT,
            config TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            FOREIGN KEY (credential_key) REFERENCES credentials(key) ON DELETE SET NULL
        );`,
		`CREATE TABLE IF NOT EXISTS saved_queries (
			id TEXT PRIMARY KEY,
			name TEXT,
			integration TEXT,
			query TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS query_history (
			id TEXT PRIMARY KEY,
			query_id TEXT,
			integration TEXT,
			query TEXT NOT NULL,
			row_count INTEGER NOT NULL DEFAULT 0,
			execution_time REAL NOT NULL DEFAULT 0,
			success INTEGER NOT NULL DEFAULT 0,
			error TEXT,
			created_at DATETIME NOT NULL,
			FOREIGN KEY (query_id) REFERENCES saved_queries(id) ON DELETE SET NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_query_history_created_at ON query_history(created_at DESC);`,
	}

	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("run migration: %w", err)
		}
	}

	if _, err := s.db.ExecContext(ctx, `ALTER TABLE saved_queries ADD COLUMN integration TEXT`); err != nil {
		errMsg := strings.ToLower(err.Error())
		if !strings.Contains(errMsg, "duplicate column name") {
			return fmt.Errorf("run migration: %w", err)
		}
	}
	return nil
}
