package db

import (
	"context"
	"database/sql"
)

// WARNING: Any indexes on columns added via downstream ALTER TABLE migrations (e.g. idx_facts_last_reinforced_at, idx_facts_fts)
// must NOT be defined here in postgresSchema. Doing so breaks migrations on existing databases where the table exists
// but the column has not yet been added. Define such indexes in initSchemaPostgres after the ALTER TABLE statements.
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
	metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
	thread_id TEXT PRIMARY KEY,
	internal_session_id TEXT NOT NULL DEFAULT '',
	previous_session_id TEXT NOT NULL DEFAULT '',
	turn_count INTEGER NOT NULL DEFAULT 0,
	last_extracted_rowid BIGINT NOT NULL DEFAULT 0,
	fact_extracted_at TIMESTAMPTZ,
	summary TEXT NOT NULL DEFAULT '',
	last_summarized_message_id TEXT NOT NULL DEFAULT '',
	active_tasks TEXT NOT NULL DEFAULT '[]',
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS one_shot_schedules (
	id TEXT PRIMARY KEY,
	thread_id TEXT NOT NULL,
	prompt TEXT NOT NULL,
	run_at TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	effort TEXT NOT NULL DEFAULT 'low'
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
	effort TEXT NOT NULL DEFAULT 'low',
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
	effort TEXT NOT NULL DEFAULT 'low',
	model TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS facts (
	id BIGSERIAL PRIMARY KEY,
	category TEXT NOT NULL DEFAULT 'general',
	fact_text TEXT NOT NULL,
	importance REAL NOT NULL DEFAULT 1.0,
	embedding vector(384),
	fts_tokens tsvector GENERATED ALWAYS AS (to_tsvector('simple', fact_text)) STORED,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	last_reinforced_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	last_decayed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	reinforce_count INTEGER NOT NULL DEFAULT 1
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
CREATE INDEX IF NOT EXISTS idx_facts_category ON facts(category);
CREATE INDEX IF NOT EXISTS idx_facts_created_at ON facts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_importance_created_at ON facts(importance DESC, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_embedding_hnsw ON facts USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS session_summaries (
	session_id TEXT PRIMARY KEY,
	thread_id TEXT NOT NULL DEFAULT '',
	summary TEXT NOT NULL DEFAULT '',
	fts_tokens tsvector GENERATED ALWAYS AS (to_tsvector('simple', summary)) STORED,
	embedding vector(384),
	last_indexed_step INT NOT NULL DEFAULT -1,
	last_mtime TIMESTAMPTZ,
	summary_step_watermark INT NOT NULL DEFAULT -1,
	is_settled BOOLEAN NOT NULL DEFAULT FALSE,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transcript_steps (
	session_id TEXT NOT NULL REFERENCES session_summaries(session_id) ON DELETE CASCADE,
	step_index INT NOT NULL,
	step_type TEXT NOT NULL DEFAULT '',
	tool_name TEXT NOT NULL DEFAULT '',
	content TEXT NOT NULL DEFAULT '',
	fts_tokens tsvector GENERATED ALWAYS AS (to_tsvector('simple', LEFT(content, 50000))) STORED,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (session_id, step_index)
);

CREATE INDEX IF NOT EXISTS idx_session_summaries_fts ON session_summaries USING GIN(fts_tokens);
CREATE INDEX IF NOT EXISTS idx_session_summaries_mtime ON session_summaries(last_mtime);
CREATE INDEX IF NOT EXISTS idx_session_summaries_thread ON session_summaries(thread_id);
CREATE INDEX IF NOT EXISTS idx_session_summaries_embedding ON session_summaries USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_transcript_steps_fts ON transcript_steps USING GIN(fts_tokens);
CREATE INDEX IF NOT EXISTS idx_transcript_steps_tool ON transcript_steps(tool_name);
`


func initSchemaPostgres(ctx context.Context, database *sql.DB) error {
	return RunMigrations(ctx, database)
}

