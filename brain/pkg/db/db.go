package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	sqliteTestInitHook func(string) (*sql.DB, error)
)

// RegisterSQLiteTestHook allows test suites to hook in-memory SQLite initialization
// without linking modernc.org/sqlite into the production binary.
func RegisterSQLiteTestHook(fn func(string) (*sql.DB, error)) {
	sqliteTestInitHook = fn
}

func isPostgres(database DBTX) (isPg bool) {
	if database == nil {
		return false
	}
	defer func() {
		if r := recover(); r != nil {
			isPg = false
		}
	}()
	type driverGetter interface {
		Driver() driver.Driver
	}
	if dg, ok := database.(driverGetter); ok && dg.Driver() != nil {
		driverType := fmt.Sprintf("%T", dg.Driver())
		return strings.Contains(driverType, "stdlib") || strings.Contains(driverType, "pgx")
	}
	return false
}

var (
	postgresMaxAttempts = 10
	postgresRetryBase   = 500 * time.Millisecond
)

// New establishes a database connection pool using the DatabaseURL from cfg.
// In production, cfg.DatabaseURL must be a valid PostgreSQL connection string starting with
// postgres:// or postgresql://.
func New(cfg *config.Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("db: config cannot be nil")
	}
	cur := cfg.Current()
	if cur == nil {
		return nil, fmt.Errorf("db: config snapshot is nil")
	}
	trimmed := strings.TrimSpace(cur.DatabaseURL)
	if trimmed == "" {
		return nil, fmt.Errorf("db: database URL cannot be empty")
	}
	if sqliteTestInitHook != nil && (trimmed == ":memory:" || strings.HasPrefix(trimmed, "file:") || strings.Contains(trimmed, ".db") || strings.HasPrefix(trimmed, "sqlite://")) {
		return sqliteTestInitHook(trimmed)
	}
	if !strings.HasPrefix(trimmed, "postgres://") && !strings.HasPrefix(trimmed, "postgresql://") {
		return nil, fmt.Errorf("db: unsupported database scheme in %q: only postgres:// or postgresql:// supported", trimmed)
	}
	return initDB(trimmed)
}

// InitDB is a compatibility wrapper for New and existing tests during migration.
func InitDB(dsnOrCfg any) (*sql.DB, error) {
	switch v := dsnOrCfg.(type) {
	case *config.Config:
		return New(v)
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil, fmt.Errorf("db: connection string cannot be empty")
		}
		if sqliteTestInitHook != nil && (trimmed == ":memory:" || strings.HasPrefix(trimmed, "file:") || strings.Contains(trimmed, ".db") || strings.HasPrefix(trimmed, "sqlite://")) {
			return sqliteTestInitHook(trimmed)
		}
		return initDB(trimmed)
	default:
		return nil, fmt.Errorf("db: unsupported InitDB argument type: %T", dsnOrCfg)
	}
}

func initDB(dsn string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return nil, fmt.Errorf("db: connection string cannot be empty")
	}

	if !strings.HasPrefix(trimmed, "postgres://") && !strings.HasPrefix(trimmed, "postgresql://") {
		return nil, fmt.Errorf("db: unsupported database scheme in %q: only postgres:// or postgresql:// supported", trimmed)
	}

	if _, err := pgx.ParseConfig(trimmed); err != nil {
		return nil, fmt.Errorf("db: invalid postgres connection string: %w", err)
	}

	var database *sql.DB
	var err error

	// 1. Connection retry loop with exponential backoff for containerized startup
	for attempt := 1; attempt <= postgresMaxAttempts; attempt++ {
		database, err = sql.Open("pgx", trimmed)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			pingErr := database.PingContext(ctx)
			cancel()
			if pingErr == nil {
				break
			}
			err = pingErr
			closeWarn(database, "database")
		}
		log.Printf("[DB] Waiting for PostgreSQL (attempt %d/%d): %v", attempt, postgresMaxAttempts, err)
		time.Sleep(time.Duration(attempt) * postgresRetryBase)
	}
	if err != nil {
		return nil, fmt.Errorf("could not connect to PostgreSQL after retries: %w", err)
	}

	// 2. Tune connection pool
	database.SetMaxOpenConns(25)
	database.SetMaxIdleConns(10)
	database.SetConnMaxLifetime(5 * time.Minute)
	database.SetConnMaxIdleTime(2 * time.Minute)

	// 3. Serialize schema creation using PostgreSQL advisory lock
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := initSchemaPostgres(ctx, database); err != nil {
		closeWarn(database, "database")
		return nil, err
	}

	log.Printf("[DB] PostgreSQL initialized successfully with pgvector at %s", trimmed)
	metrics.RegisterDBStats(database)
	return database, nil
}

func closeWarn(closer io.Closer, name string) {
	if closer != nil {
		if err := closer.Close(); err != nil {
			log.Printf("[DB] Warning closing %s: %v", name, err)
		}
	}
}

func rollbackWarn(tx *sql.Tx, name string) {
	if tx != nil {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Printf("[DB] Warning rolling back %s: %v", name, err)
		}
	}
}
