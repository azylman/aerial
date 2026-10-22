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

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	sqliteTestInitHook  func(string) (*sql.DB, error)
	postgresMaxAttempts = 10
	postgresRetryBase   = 500 * time.Millisecond
)

func isPostgres(database DBTX) (isPg bool) {
	if database == nil {
		return false
	}
	defer func() {
		if r := recover(); r != nil {
			isPg = false
		}
	}()
	type driverGetter interface{ Driver() driver.Driver }
	if dg, ok := database.(driverGetter); ok && dg.Driver() != nil {
		dt := fmt.Sprintf("%T", dg.Driver())
		return strings.Contains(dt, "stdlib") || strings.Contains(dt, "pgx")
	}
	return false
}

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
	return InitDB(cur.DatabaseURL)
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
		if !strings.HasPrefix(trimmed, "postgres://") && !strings.HasPrefix(trimmed, "postgresql://") {
			return nil, fmt.Errorf("db: unsupported database scheme in %q: only postgres:// or postgresql:// supported", trimmed)
		}
		return initDB(trimmed)
	default:
		return nil, fmt.Errorf("db: unsupported InitDB argument type: %T", dsnOrCfg)
	}
}

func initDB(dsn string) (*sql.DB, error) {
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return nil, fmt.Errorf("db: invalid postgres connection string: %w", err)
	}
	var database *sql.DB
	var err error
	for attempt := 1; attempt <= postgresMaxAttempts; attempt++ {
		if database, err = sql.Open("pgx", dsn); err == nil {
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
	database.SetMaxOpenConns(25)
	database.SetMaxIdleConns(10)
	database.SetConnMaxLifetime(5 * time.Minute)
	database.SetConnMaxIdleTime(2 * time.Minute)
	log.Printf("[DB] PostgreSQL initialized successfully with pgvector at %s", dsn)
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
