package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/metrics"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

// GetDBPath returns the active database connection string from environment variables.
func GetDBPath() string {
	if testURL := os.Getenv("TEST_DATABASE_URL"); testURL != "" {
		return testURL
	}
	if envDSN := os.Getenv("DATABASE_URL"); envDSN != "" {
		return envDSN
	}
	dbUser := os.Getenv("POSTGRES_USER")
	if dbUser == "" {
		dbUser = "aerial"
	}
	dbPass := os.Getenv("POSTGRES_PASSWORD")
	if dbPass == "" {
		dbPass = "aerial_secure_pass"
	}
	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "postgres"
	}
	dbPort := os.Getenv("POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("POSTGRES_DB")
	if dbName == "" {
		dbName = "aerial"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)
}

func isPostgres(database *sql.DB) bool {
	if database == nil {
		return false
	}
	driverType := fmt.Sprintf("%T", database.Driver())
	return strings.Contains(driverType, "stdlib") || strings.Contains(driverType, "pgx")
}

var (
	postgresMaxAttempts = 10
	postgresRetryBase   = 500 * time.Millisecond
)

// InitDB establishes the database connection pool, applies schema migrations, and registers metrics.
func InitDB(dsn string) (*sql.DB, error) {
	if dsn == "" {
		dsn = GetDBPath()
	}

	isPg := strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")

	if isPg {
		var database *sql.DB
		var err error

		// 1. Connection retry loop with exponential backoff for containerized startup
		for attempt := 1; attempt <= postgresMaxAttempts; attempt++ {
			database, err = sql.Open("pgx", dsn)
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

		log.Printf("[DB] PostgreSQL initialized successfully with pgvector at %s", dsn)
		metrics.RegisterDBStats(database)
		return database, nil
	}

	// SQLite fallback for in-memory unit tests and local runs
	if dsn != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(dsn), 0755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
	}

	sqliteDSN := dsn
	if dsn != ":memory:" && !strings.Contains(dsn, "_pragma") {
		if strings.Contains(dsn, "?") {
			sqliteDSN += "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		} else {
			sqliteDSN += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		}
	}

	database, err := sql.Open("sqlite", sqliteDSN)
	if err != nil {
		return nil, err
	}

	if dsn == ":memory:" || strings.Contains(dsn, "mode=memory") {
		database.SetMaxOpenConns(1)
	}

	if err := initSchemaSQLite(database); err != nil {
		_ = database.Close()
		return nil, err
	}

	log.Printf("[DB] SQLite initialized successfully at %s", dsn)
	metrics.RegisterDBStats(database)
	return database, nil
}
