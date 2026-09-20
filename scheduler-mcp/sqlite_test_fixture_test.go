package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

func initTestSQLite(trimmed string) (*sql.DB, error) {
	trimmed = strings.TrimPrefix(trimmed, "sqlite://")
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
		closeWarn(database, "database on schema error")
		return nil, err
	}

	// Safe column migrations on existing tables
	for _, alterStmt := range []string{
		`ALTER TABLE cron_schedules ADD COLUMN title_prefix TEXT NOT NULL DEFAULT '';`,
		`ALTER TABLE cron_schedules ADD COLUMN timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles';`,
		`ALTER TABLE cron_schedules ADD COLUMN effort TEXT NOT NULL DEFAULT 'high';`,
	} {
		if _, alterErr := database.Exec(alterStmt); alterErr != nil {
			log.Printf("[Scheduler DB] Column migration notice (safe to ignore if exists): %v", alterErr)
		}
	}

	log.Printf("[Scheduler DB] SQLite database initialized at %s", trimmed)
	return database, nil
}

func init() {
	RegisterSQLiteTestHook(initTestSQLite)
}
