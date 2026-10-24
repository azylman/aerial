package db

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	sqliteTestInitHook func(string) (*sql.DB, error)
	sqlOpen            = sql.Open
	dbPingContext      = func(ctx context.Context, db *sql.DB) error {
		return db.PingContext(ctx)
	}
)

// New establishes a database connection pool using the DatabaseURL from cfg.
func New(cfg *config.Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("db: config cannot be nil")
	}
	cur := cfg.Current()
	if cur == nil {
		return nil, fmt.Errorf("db: config snapshot is nil")
	}
	if strings.TrimSpace(cur.DatabaseURL) == "" {
		return nil, fmt.Errorf("db: database URL cannot be empty")
	}
	return Open(cur.DatabaseURL)
}

// Open establishes a database connection pool from dsn.
func Open(dsn string) (*sql.DB, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, fmt.Errorf("database_url is required (PostgreSQL DSN expected)")
	}
	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		if sqliteTestInitHook != nil {
			return sqliteTestInitHook(dsn)
		}
		return nil, fmt.Errorf("unsupported database DSN scheme in %q: only postgres:// and postgresql:// are supported in production", dsn)
	}
	db, err := sqlOpen("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := dbPingContext(ctx, db); err != nil {
		closeWarn(db, "database on ping failure")
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}
	metrics.RegisterDBStats(db)
	return db, nil
}

func closeWarn(closer io.Closer, name string) {
	if closer != nil {
		if err := closer.Close(); err != nil {
			log.Printf("[DB] Warning closing %s: %v", name, err)
		}
	}
}
