package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const migrationLockID = 849201948201

const postgresSchema = `
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS messages (
	id TEXT PRIMARY KEY,
	row_id BIGSERIAL UNIQUE,
	thread_id TEXT NOT NULL DEFAULT '',
	guild_id TEXT NOT NULL DEFAULT '',
	author_id TEXT NOT NULL DEFAULT '',
	author_name TEXT NOT NULL DEFAULT '',
	content TEXT NOT NULL DEFAULT '',
	summary TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT 'PENDING',
	retry_count INTEGER NOT NULL DEFAULT 0,
	restart_count INTEGER NOT NULL DEFAULT 0,
	effort TEXT NOT NULL DEFAULT '',
	error_message TEXT,
	response_text TEXT,
	schedule_run_id TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
	thread_id TEXT PRIMARY KEY,
	internal_session_id TEXT NOT NULL DEFAULT '',
	turn_count INTEGER NOT NULL DEFAULT 0,
	last_extracted_rowid BIGINT NOT NULL DEFAULT 0,
	fact_extracted_at TIMESTAMPTZ,
	summary TEXT NOT NULL DEFAULT '',
	last_summarized_message_id TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS one_shot_schedules (
	id TEXT PRIMARY KEY,
	thread_id TEXT NOT NULL,
	prompt TEXT NOT NULL,
	run_at TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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

CREATE TABLE IF NOT EXISTS schedule_runs (
	id TEXT PRIMARY KEY,
	schedule_id TEXT NOT NULL,
	schedule_type TEXT NOT NULL,
	message_id TEXT NOT NULL DEFAULT '',
	target_id TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	title TEXT NOT NULL DEFAULT '',
	prompt TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'enqueued',
	started_at TIMESTAMPTZ NOT NULL,
	completed_at TIMESTAMPTZ,
	duration_ms BIGINT DEFAULT 0,
	effort TEXT NOT NULL DEFAULT 'high',
	model TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS facts (
	id BIGSERIAL PRIMARY KEY,
	category TEXT NOT NULL DEFAULT 'general',
	fact_text TEXT NOT NULL,
	importance REAL NOT NULL DEFAULT 1.0,
	thread_id TEXT NOT NULL DEFAULT '',
	embedding vector(384),
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_messages_thread_status ON messages(thread_id, status);
CREATE INDEX IF NOT EXISTS idx_messages_thread_row_id ON messages(thread_id, row_id);
CREATE INDEX IF NOT EXISTS idx_messages_status_created_at ON messages(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_sessions_fact_extracted ON sessions(last_extracted_rowid, fact_extracted_at);
CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);
CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_started ON schedule_runs(schedule_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_status_started ON schedule_runs(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_message_id ON schedule_runs(message_id);
CREATE INDEX IF NOT EXISTS idx_facts_thread_id ON facts(thread_id);
CREATE INDEX IF NOT EXISTS idx_facts_category ON facts(category);
CREATE INDEX IF NOT EXISTS idx_facts_created_at ON facts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_importance_created_at ON facts(importance DESC, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_embedding_hnsw ON facts USING hnsw (embedding vector_cosine_ops);
`

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS messages (
	id TEXT PRIMARY KEY,
	row_id INTEGER,
	thread_id TEXT NOT NULL DEFAULT '',
	guild_id TEXT NOT NULL DEFAULT '',
	author_id TEXT NOT NULL DEFAULT '',
	author_name TEXT NOT NULL DEFAULT '',
	content TEXT NOT NULL DEFAULT '',
	summary TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT 'PENDING',
	retry_count INTEGER NOT NULL DEFAULT 0,
	restart_count INTEGER NOT NULL DEFAULT 0,
	effort TEXT NOT NULL DEFAULT '',
	error_message TEXT,
	response_text TEXT,
	schedule_run_id TEXT NOT NULL DEFAULT '',
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER IF NOT EXISTS trg_messages_row_id AFTER INSERT ON messages WHEN new.row_id IS NULL OR new.row_id = 0 BEGIN
	UPDATE messages SET row_id = (SELECT COALESCE(MAX(row_id), 0) + 1 FROM messages) WHERE id = new.id;
END;

CREATE TABLE IF NOT EXISTS sessions (
	thread_id TEXT PRIMARY KEY,
	internal_session_id TEXT NOT NULL DEFAULT '',
	turn_count INTEGER NOT NULL DEFAULT 0,
	last_extracted_rowid INTEGER NOT NULL DEFAULT 0,
	fact_extracted_at DATETIME,
	summary TEXT NOT NULL DEFAULT '',
	last_summarized_message_id TEXT NOT NULL DEFAULT '',
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS one_shot_schedules (
	id TEXT PRIMARY KEY,
	thread_id TEXT NOT NULL,
	prompt TEXT NOT NULL,
	run_at DATETIME NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cron_schedules (
	id TEXT PRIMARY KEY,
	target_id TEXT NOT NULL,
	title_prefix TEXT NOT NULL DEFAULT '',
	cron_expr TEXT NOT NULL,
	prompt TEXT NOT NULL,
	timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
	next_run_at DATETIME NOT NULL,
	enabled BOOLEAN NOT NULL DEFAULT 1,
	effort TEXT NOT NULL DEFAULT 'high',
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS schedule_runs (
	id TEXT PRIMARY KEY,
	schedule_id TEXT NOT NULL,
	schedule_type TEXT NOT NULL,
	message_id TEXT NOT NULL DEFAULT '',
	target_id TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	title TEXT NOT NULL DEFAULT '',
	prompt TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'enqueued',
	started_at DATETIME NOT NULL,
	completed_at DATETIME,
	duration_ms INTEGER DEFAULT 0,
	effort TEXT NOT NULL DEFAULT 'high',
	model TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS facts (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	category TEXT NOT NULL DEFAULT 'general',
	fact_text TEXT NOT NULL,
	importance REAL NOT NULL DEFAULT 1.0,
	thread_id TEXT NOT NULL DEFAULT '',
	embedding BLOB,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_messages_thread_status ON messages(thread_id, status);
CREATE INDEX IF NOT EXISTS idx_messages_status_created_at ON messages(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_sessions_fact_extracted ON sessions(last_extracted_rowid, fact_extracted_at);
CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);
CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_started ON schedule_runs(schedule_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_status_started ON schedule_runs(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_message_id ON schedule_runs(message_id);
CREATE INDEX IF NOT EXISTS idx_facts_thread_id ON facts(thread_id);
CREATE INDEX IF NOT EXISTS idx_facts_category ON facts(category);
CREATE INDEX IF NOT EXISTS idx_facts_created_at ON facts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_importance_created_at ON facts(importance DESC, created_at DESC);
`

func initSchemaPostgres(ctx context.Context, database *sql.DB) error {
	conn, err := database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire db connection for migrations: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1);", migrationLockID); err != nil {
		return fmt.Errorf("failed to acquire migration advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1);", migrationLockID)
	}()

	if _, err := conn.ExecContext(ctx, postgresSchema); err != nil {
		return fmt.Errorf("failed to run postgres migrations: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "ALTER TABLE messages ADD COLUMN IF NOT EXISTS restart_count INTEGER NOT NULL DEFAULT 0;"); err != nil {
		return fmt.Errorf("failed to add restart_count column to messages: %w", err)
	}

	_, _ = conn.ExecContext(ctx, "ALTER TABLE messages ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT ''")
	_, _ = conn.ExecContext(ctx, "ALTER TABLE cron_schedules ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'high'")
	_, _ = conn.ExecContext(ctx, "ALTER TABLE schedule_runs ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'high'")
	_, _ = conn.ExecContext(ctx, "ALTER TABLE schedule_runs ADD COLUMN IF NOT EXISTS model TEXT NOT NULL DEFAULT ''")

	_, _ = conn.ExecContext(ctx, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS summary TEXT NOT NULL DEFAULT ''")
	_, _ = conn.ExecContext(ctx, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS last_summarized_message_id TEXT NOT NULL DEFAULT ''")

	// Idempotent sequence resynchronization in case of manual data restoration
	_, _ = conn.ExecContext(ctx, `
		SELECT setval(pg_get_serial_sequence('facts', 'id'), COALESCE((SELECT MAX(id) FROM facts), 1), (SELECT COUNT(*) > 0 FROM facts));
		SELECT setval(pg_get_serial_sequence('messages', 'row_id'), COALESCE((SELECT MAX(row_id) FROM messages), 1), (SELECT COUNT(*) > 0 FROM messages));
	`)
	return nil
}

func initSchemaSQLite(database *sql.DB) error {
	pragmas := `
	PRAGMA journal_mode = WAL;
	PRAGMA busy_timeout = 5000;
	PRAGMA synchronous = NORMAL;
	`
	if _, err := database.Exec(pragmas); err != nil {
		return fmt.Errorf("failed to set sqlite pragmas: %w", err)
	}

	if _, err := database.Exec(sqliteSchema); err != nil {
		return fmt.Errorf("failed to run sqlite migrations: %w", err)
	}

	if _, err := database.Exec("ALTER TABLE messages ADD COLUMN restart_count INTEGER NOT NULL DEFAULT 0;"); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("failed to add restart_count column to messages: %w", err)
		}
	}

	_, _ = database.Exec("ALTER TABLE messages ADD COLUMN effort TEXT NOT NULL DEFAULT ''")
	_, _ = database.Exec("ALTER TABLE cron_schedules ADD COLUMN effort TEXT NOT NULL DEFAULT 'high'")
	_, _ = database.Exec("ALTER TABLE schedule_runs ADD COLUMN effort TEXT NOT NULL DEFAULT 'high'")
	_, _ = database.Exec("ALTER TABLE schedule_runs ADD COLUMN model TEXT NOT NULL DEFAULT ''")

	_, _ = database.Exec("ALTER TABLE sessions ADD COLUMN summary TEXT NOT NULL DEFAULT ''")
	_, _ = database.Exec("ALTER TABLE sessions ADD COLUMN last_summarized_message_id TEXT NOT NULL DEFAULT ''")
	return nil
}
