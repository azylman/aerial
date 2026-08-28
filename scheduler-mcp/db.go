package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	if envPath := os.Getenv("DB_PATH"); envPath != "" {
		return envPath
	}
	if _, err := os.Stat("/data"); err == nil {
		return "/data/aerial.db"
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "./aerial.db"
	}
	return filepath.Join(homeDir, ".gemini", "aerial.db")
}

func InitDB(dbPath string) (*sql.DB, error) {
	if dbPath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
	}

	dsn := dbPath
	if dbPath != ":memory:" && !strings.Contains(dbPath, "_pragma") {
		if strings.Contains(dbPath, "?") {
			dsn += "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		} else {
			dsn += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		}
	}

	database, err := sql.Open("sqlite", dsn)
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
		return nil, err
	}

	// Safe column migrations on existing tables
	_, _ = database.Exec(`ALTER TABLE cron_schedules ADD COLUMN title_prefix TEXT NOT NULL DEFAULT '';`)
	_, _ = database.Exec(`ALTER TABLE cron_schedules ADD COLUMN timezone TEXT NOT NULL DEFAULT 'UTC';`)

	log.Printf("Scheduler MCP SQLite database initialized at %s", dbPath)
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
		c.Timezone = "America/New_York"
	}
	query := `
	INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
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
		rows, err = database.Query(query, targetID)
	} else {
		query += " ORDER BY created_at ASC"
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
		rows, err = database.Query(query, targetID)
	} else {
		query += " ORDER BY run_at ASC"
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
	resCron, err := database.Exec("DELETE FROM cron_schedules WHERE id = ?", scheduleID)
	if err != nil {
		return false, err
	}
	cronRows, _ := resCron.RowsAffected()

	resOneShot, err := database.Exec("DELETE FROM one_shot_schedules WHERE id = ?", scheduleID)
	if err != nil {
		return false, err
	}
	oneShotRows, _ := resOneShot.RowsAffected()

	return (cronRows > 0 || oneShotRows > 0), nil
}
