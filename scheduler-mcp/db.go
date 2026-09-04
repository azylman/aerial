package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

type CronSchedule struct {
	ID          string    `json:"id"`
	TargetID    string    `json:"channel_id"`
	TitlePrefix string    `json:"title_prefix"`
	CronExpr    string    `json:"cron_expression"`
	Prompt      string    `json:"prompt"`
	Timezone    string    `json:"timezone"`
	NextRunAt   time.Time `json:"next_run_at"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
}

type OneShotSchedule struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
}

func GetDBPath() string {
	if testURL := os.Getenv("TEST_DATABASE_URL"); testURL != "" {
		return testURL
	}
	if envURL := os.Getenv("DATABASE_URL"); envURL != "" {
		return envURL
	}
	if envPath := os.Getenv("DB_PATH"); envPath != "" {
		return envPath
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

func rebindQuery(query string, isPg bool) string {
	if !isPg {
		return query
	}
	var b strings.Builder
	paramIdx := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteString(fmt.Sprintf("$%d", paramIdx))
			paramIdx++
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

func InitDB(dsn string) (*sql.DB, error) {
	isPg := strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")

	if isPg {
		var database *sql.DB
		var err error

		maxAttempts := 10
		backoff := 500 * time.Millisecond
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			database, err = sql.Open("pgx", dsn)
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				err = database.PingContext(ctx)
				cancel()
				if err == nil {
					break
				}
				_ = database.Close()
			}
			log.Printf("[Scheduler DB] Waiting for PostgreSQL (attempt %d/%d): %v", attempt, maxAttempts, err)
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * 1.5)
			if backoff > 5*time.Second {
				backoff = 5*time.Second
			}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to connect to PostgreSQL after %d attempts: %w", maxAttempts, err)
		}

		database.SetMaxOpenConns(10)
		database.SetMaxIdleConns(5)
		database.SetConnMaxLifetime(30 * time.Minute)
		database.SetConnMaxIdleTime(5 * time.Minute)

		schema := `
		CREATE TABLE IF NOT EXISTS cron_schedules (
			id TEXT PRIMARY KEY,
			target_id TEXT NOT NULL,
			title_prefix TEXT NOT NULL DEFAULT '',
			cron_expr TEXT NOT NULL,
			prompt TEXT NOT NULL,
			timezone TEXT NOT NULL DEFAULT 'UTC',
			next_run_at TIMESTAMPTZ NOT NULL,
			enabled BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMPTZ NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);

		CREATE TABLE IF NOT EXISTS one_shot_schedules (
			id TEXT PRIMARY KEY,
			thread_id TEXT NOT NULL,
			prompt TEXT NOT NULL,
			run_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);
		`
		if _, err := database.Exec(schema); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("failed to execute postgres schema: %w", err)
		}

		log.Printf("[Scheduler DB] PostgreSQL initialized successfully at %s", dsn)
		return database, nil
	}

	// SQLite fallback
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

	pragmas := `
	PRAGMA journal_mode = WAL;
	PRAGMA busy_timeout = 5000;
	PRAGMA synchronous = NORMAL;
	`
	if _, err := database.Exec(pragmas); err != nil {
		log.Printf("Warning: failed to execute PRAGMAs: %v", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS cron_schedules (
		id TEXT PRIMARY KEY,
		target_id TEXT NOT NULL,
		title_prefix TEXT NOT NULL DEFAULT '',
		cron_expr TEXT NOT NULL,
		prompt TEXT NOT NULL,
		timezone TEXT NOT NULL DEFAULT 'UTC',
		next_run_at TIMESTAMP NOT NULL,
		enabled BOOLEAN NOT NULL DEFAULT TRUE,
		created_at TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);

	CREATE TABLE IF NOT EXISTS one_shot_schedules (
		id TEXT PRIMARY KEY,
		thread_id TEXT NOT NULL,
		prompt TEXT NOT NULL,
		run_at TIMESTAMP NOT NULL,
		created_at TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);
	`
	if _, err := database.Exec(schema); err != nil {
		_ = database.Close()
		return nil, err
	}

	// Safe column migrations on existing tables
	_, _ = database.Exec(`ALTER TABLE cron_schedules ADD COLUMN title_prefix TEXT NOT NULL DEFAULT '';`)
	_, _ = database.Exec(`ALTER TABLE cron_schedules ADD COLUMN timezone TEXT NOT NULL DEFAULT 'UTC';`)

	log.Printf("[Scheduler DB] SQLite database initialized at %s", dsn)
	return database, nil
}

func InsertCronSchedule(database *sql.DB, c CronSchedule) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	if c.Timezone == "" {
		c.Timezone = GetDefaultTimezone()
	}
	query := `
	INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	query = rebindQuery(query, isPostgres(database))
	_, err := database.Exec(query, c.ID, c.TargetID, c.TitlePrefix, c.CronExpr, c.Prompt, c.Timezone, c.NextRunAt, c.Enabled, c.CreatedAt)
	return err
}

func InsertOneShotSchedule(database *sql.DB, s OneShotSchedule) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	query := `
	INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at)
	VALUES (?, ?, ?, ?, ?)
	`
	query = rebindQuery(query, isPostgres(database))
	_, err := database.Exec(query, s.ID, s.ThreadID, s.Prompt, s.RunAt, s.CreatedAt)
	return err
}

func ListCronSchedules(database *sql.DB, targetID string) ([]CronSchedule, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	query := `
	SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at
	FROM cron_schedules
	WHERE enabled = TRUE
	`
	var rows *sql.Rows
	var err error
	if targetID != "" {
		query += " AND target_id = ? ORDER BY created_at ASC"
		query = rebindQuery(query, isPostgres(database))
		rows, err = database.Query(query, targetID)
	} else {
		query += " ORDER BY created_at ASC"
		query = rebindQuery(query, isPostgres(database))
		rows, err = database.Query(query)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []CronSchedule
	for rows.Next() {
		var c CronSchedule
		if err := rows.Scan(&c.ID, &c.TargetID, &c.TitlePrefix, &c.CronExpr, &c.Prompt, &c.Timezone, &c.NextRunAt, &c.Enabled, &c.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, c)
	}
	return results, nil
}

func ListOneShotSchedules(database *sql.DB, targetID string) ([]OneShotSchedule, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	query := `
	SELECT id, thread_id, prompt, run_at, created_at
	FROM one_shot_schedules
	`
	var rows *sql.Rows
	var err error
	if targetID != "" {
		query += " WHERE thread_id = ? ORDER BY run_at ASC"
		query = rebindQuery(query, isPostgres(database))
		rows, err = database.Query(query, targetID)
	} else {
		query += " ORDER BY run_at ASC"
		query = rebindQuery(query, isPostgres(database))
		rows, err = database.Query(query)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []OneShotSchedule
	for rows.Next() {
		var s OneShotSchedule
		if err := rows.Scan(&s.ID, &s.ThreadID, &s.Prompt, &s.RunAt, &s.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, s)
	}
	return results, nil
}

func DeleteSchedule(database *sql.DB, scheduleID string) (bool, error) {
	if database == nil {
		return false, fmt.Errorf("database is nil")
	}
	queryCron := rebindQuery("DELETE FROM cron_schedules WHERE id = ?", isPostgres(database))
	resCron, err := database.Exec(queryCron, scheduleID)
	if err != nil {
		return false, err
	}
	cronRows, _ := resCron.RowsAffected()

	queryOneShot := rebindQuery("DELETE FROM one_shot_schedules WHERE id = ?", isPostgres(database))
	resOneShot, err := database.Exec(queryOneShot, scheduleID)
	if err != nil {
		return false, err
	}
	oneShotRows, _ := resOneShot.RowsAffected()

	return (cronRows > 0 || oneShotRows > 0), nil
}
