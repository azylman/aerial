package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

func isPostgres(database DBTX) bool {
	if database == nil {
		return false
	}
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
func New(cfg *config.Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("db: config cannot be nil")
	}
	cur := cfg.Current()
	if cur == nil {
		return nil, fmt.Errorf("db: config snapshot is nil")
	}
	return initDB(cur.DatabaseURL)
}

// InitDB is a compatibility wrapper for New and existing tests during migration.
func InitDB(dsnOrCfg any) (*sql.DB, error) {
	switch v := dsnOrCfg.(type) {
	case *config.Config:
		return New(v)
	case string:
		return initDB(v)
	default:
		return nil, fmt.Errorf("db: unsupported InitDB argument type: %T", dsnOrCfg)
	}
}

func initDB(dsn string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return nil, fmt.Errorf("db: connection string cannot be empty")
	}

	// Normalize sqlite:// prefix to plain path
	trimmed = strings.TrimPrefix(trimmed, "sqlite://")

	isPg := strings.HasPrefix(trimmed, "postgres://") || strings.HasPrefix(trimmed, "postgresql://")
	if !isPg {
		if strings.Contains(trimmed, "://") && !strings.HasPrefix(trimmed, "file://") {
			return nil, fmt.Errorf("db: unsupported database scheme in %q", trimmed)
		}
	}

	if isPg {
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
				_ = database.Close()
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
			_ = database.Close()
			return nil, err
		}

		log.Printf("[DB] PostgreSQL initialized successfully with pgvector at %s", trimmed)
		metrics.RegisterDBStats(database)
		return database, nil
	}

	// SQLite fallback for in-memory unit tests and local runs
	if trimmed != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(trimmed), 0755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
	}

	sqliteDSN := trimmed
	if trimmed != ":memory:" && !strings.Contains(trimmed, "_pragma") {
		if strings.Contains(trimmed, "?") {
			sqliteDSN += "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		} else {
			sqliteDSN += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		}
	}

	database, err := sql.Open("sqlite", sqliteDSN)
	if err != nil {
		return nil, err
	}

	if trimmed == ":memory:" || strings.Contains(trimmed, "mode=memory") {
		database.SetMaxOpenConns(1)
	}

	if err := initSchemaSQLite(database); err != nil {
		_ = database.Close()
		return nil, err
	}

	log.Printf("[DB] SQLite initialized successfully at %s", trimmed)
	metrics.RegisterDBStats(database)
	return database, nil
}
