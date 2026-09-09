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
	Effort      string    `json:"effort,omitempty"`
}

type OneShotSchedule struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
}

const migrationLockID = 849201948201

// NewDB initializes the database connection using the provided configuration.
func NewDB(cfg *Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("db: config cannot be nil")
	}
	return initDB(cfg.DatabaseURL)
}

// InitDB is a compatibility wrapper for NewDB.
func InitDB(cfg *Config) (*sql.DB, error) {
	return NewDB(cfg)
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

var (
	postgresMaxAttempts = 10
	postgresRetryBase   = 500 * time.Millisecond
)

func initDB(dsn string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return nil, fmt.Errorf("db: connection string cannot be empty")
	}

	trimmed = strings.TrimPrefix(trimmed, "sqlite://")

	isPg := strings.HasPrefix(trimmed, "postgres://") || strings.HasPrefix(trimmed, "postgresql://")
	if !isPg && strings.Contains(trimmed, "://") && !strings.HasPrefix(trimmed, "file://") {
		return nil, fmt.Errorf("db: unsupported database scheme in %q", trimmed)
	}

	if isPg {
		var database *sql.DB
		var err error

		maxAttempts := postgresMaxAttempts
		backoff := postgresRetryBase
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			database, err = sql.Open("pgx", trimmed)
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
				backoff = 5 * time.Second
			}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to connect to PostgreSQL after %d attempts: %w", maxAttempts, err)
		}

		database.SetMaxOpenConns(10)
		database.SetMaxIdleConns(5)
		database.SetConnMaxLifetime(30 * time.Minute)
		database.SetConnMaxIdleTime(5 * time.Minute)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn, err := database.Conn(ctx)
		if err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("failed to acquire connection for migrations: %w", err)
		}
		defer func() { _ = conn.Close() }()

		if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1);", migrationLockID); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("failed to acquire migration advisory lock: %w", err)
		}
		defer func() {
			_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1);", migrationLockID)
		}()

		schema := `
		CREATE TABLE IF NOT EXISTS cron_schedules (
			id TEXT PRIMARY KEY,
			target_id TEXT NOT NULL,
			title_prefix TEXT NOT NULL DEFAULT '',
			cron_expr TEXT NOT NULL,
			prompt TEXT NOT NULL,
			timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
			next_run_at TIMESTAMPTZ NOT NULL,
			enabled BOOLEAN NOT NULL DEFAULT TRUE,
			effort TEXT NOT NULL DEFAULT 'high',
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);

		CREATE TABLE IF NOT EXISTS one_shot_schedules (
			id TEXT PRIMARY KEY,
			thread_id TEXT NOT NULL,
			prompt TEXT NOT NULL,
			run_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);
		`
		if _, err := conn.ExecContext(ctx, schema); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("failed to execute postgres schema: %w", err)
		}

		_, _ = conn.ExecContext(ctx, "ALTER TABLE cron_schedules ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'high';")

		log.Printf("[Scheduler DB] PostgreSQL initialized successfully at %s", trimmed)
		return database, nil
	}

	// SQLite fallback
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
		timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
		next_run_at TIMESTAMP NOT NULL,
		enabled BOOLEAN NOT NULL DEFAULT TRUE,
		effort TEXT NOT NULL DEFAULT 'high',
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);

	CREATE TABLE IF NOT EXISTS one_shot_schedules (
		id TEXT PRIMARY KEY,
		thread_id TEXT NOT NULL,
		prompt TEXT NOT NULL,
		run_at TIMESTAMP NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);
	`
	if _, err := database.Exec(schema); err != nil {
		_ = database.Close()
		return nil, err
	}

	// Safe column migrations on existing tables
	_, _ = database.Exec(`ALTER TABLE cron_schedules ADD COLUMN title_prefix TEXT NOT NULL DEFAULT '';`)
	_, _ = database.Exec(`ALTER TABLE cron_schedules ADD COLUMN timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles';`)
	_, _ = database.Exec(`ALTER TABLE cron_schedules ADD COLUMN effort TEXT NOT NULL DEFAULT 'high';`)

	log.Printf("[Scheduler DB] SQLite database initialized at %s", trimmed)
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
		c.Timezone = "America/Los_Angeles"
	}
	if strings.ToLower(strings.TrimSpace(c.Effort)) != "low" {
		c.Effort = "high"
	} else {
		c.Effort = "low"
	}
	query := `
	INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	query = rebindQuery(query, isPostgres(database))
	_, err := database.Exec(query, c.ID, c.TargetID, c.TitlePrefix, c.CronExpr, c.Prompt, c.Timezone, c.NextRunAt, c.Enabled, c.CreatedAt, c.Effort)
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
	SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, COALESCE(effort, 'high')
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
		if err := rows.Scan(&c.ID, &c.TargetID, &c.TitlePrefix, &c.CronExpr, &c.Prompt, &c.Timezone, &c.NextRunAt, &c.Enabled, &c.CreatedAt, &c.Effort); err != nil {
			return nil, err
		}
		results = append(results, c)
	}
	return results, nil
}

func UpdateCronSchedule(database *sql.DB, id string, effort *string, cronExpr *string, prompt *string, titlePrefix *string, timezone *string, nextRunAt *time.Time) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("schedule_id cannot be empty")
	}

	var sets []string
	var args []interface{}

	if effort != nil {
		eff := strings.ToLower(strings.TrimSpace(*effort))
		if eff != "low" {
			eff = "high"
		}
		sets = append(sets, "effort = ?")
		args = append(args, eff)
	}
	if cronExpr != nil && strings.TrimSpace(*cronExpr) != "" {
		sets = append(sets, "cron_expr = ?")
		args = append(args, strings.TrimSpace(*cronExpr))
	}
	if prompt != nil && strings.TrimSpace(*prompt) != "" {
		sets = append(sets, "prompt = ?")
		args = append(args, strings.TrimSpace(*prompt))
	}
	if titlePrefix != nil {
		sets = append(sets, "title_prefix = ?")
		args = append(args, strings.TrimSpace(*titlePrefix))
	}
	if timezone != nil && strings.TrimSpace(*timezone) != "" {
		sets = append(sets, "timezone = ?")
		args = append(args, strings.TrimSpace(*timezone))
	}
	if nextRunAt != nil && !nextRunAt.IsZero() {
		sets = append(sets, "next_run_at = ?")
		args = append(args, *nextRunAt)
	}

	if len(sets) == 0 {
		return nil
	}

	query := fmt.Sprintf("UPDATE cron_schedules SET %s WHERE id = ?", strings.Join(sets, ", "))
	args = append(args, id)
	query = rebindQuery(query, isPostgres(database))

	res, err := database.Exec(query, args...)
	if err != nil {
		return err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("cron schedule %q not found", id)
	}
	return nil
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
