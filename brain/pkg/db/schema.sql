-- Aerial Brain PostgreSQL Schema
-- Declarative Schema Managed by Ariga Atlas
-- Note: 'vector' extension is managed by base database / migrations

-- Messages Table
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

CREATE INDEX IF NOT EXISTS idx_messages_thread_status ON messages(thread_id, status);
CREATE INDEX IF NOT EXISTS idx_messages_thread_row_id ON messages(thread_id, row_id);
CREATE INDEX IF NOT EXISTS idx_messages_status_created_at ON messages(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_messages_row_id ON messages(row_id);

-- Sessions Table
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

CREATE INDEX IF NOT EXISTS idx_sessions_fact_extracted ON sessions(last_extracted_rowid, fact_extracted_at);

-- One-Shot Schedules Table
CREATE TABLE IF NOT EXISTS one_shot_schedules (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    prompt TEXT NOT NULL,
    run_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effort TEXT NOT NULL DEFAULT 'low'
);

CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);

-- Cron Schedules Table
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

CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);

-- Schedule Runs Table
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

CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_started ON schedule_runs(schedule_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_status_started ON schedule_runs(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_message_id ON schedule_runs(message_id);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_id ON schedule_runs(schedule_id);

-- Facts Table
CREATE TABLE IF NOT EXISTS facts (
    id BIGSERIAL PRIMARY KEY,
    category TEXT NOT NULL DEFAULT 'general',
    fact_text TEXT NOT NULL,
    importance REAL NOT NULL DEFAULT 1.0,
    embedding vector(384),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_reinforced_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_decayed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reinforce_count INTEGER NOT NULL DEFAULT 1,
    fts_tokens tsvector GENERATED ALWAYS AS (to_tsvector('simple', fact_text)) STORED
);

CREATE INDEX IF NOT EXISTS idx_facts_category ON facts(category);
CREATE INDEX IF NOT EXISTS idx_facts_created_at ON facts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_importance_created_at ON facts(importance DESC, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_last_reinforced_at ON facts(last_reinforced_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_embedding_hnsw ON facts USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_facts_fts ON facts USING gin(fts_tokens);

-- Session Summaries Table
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

CREATE INDEX IF NOT EXISTS idx_session_summaries_fts ON session_summaries USING GIN(fts_tokens);
CREATE INDEX IF NOT EXISTS idx_session_summaries_mtime ON session_summaries(last_mtime);
CREATE INDEX IF NOT EXISTS idx_session_summaries_thread ON session_summaries(thread_id);
CREATE INDEX IF NOT EXISTS idx_session_summaries_embedding ON session_summaries USING hnsw (embedding vector_cosine_ops);

-- Transcript Steps Table
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

CREATE INDEX IF NOT EXISTS idx_transcript_steps_fts ON transcript_steps USING GIN(fts_tokens);
CREATE INDEX IF NOT EXISTS idx_transcript_steps_tool ON transcript_steps(tool_name);

-- Pull Request and Deployment Registry Table
CREATE TABLE IF NOT EXISTS pr_registry (
    id BIGSERIAL PRIMARY KEY,
    repo TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    branch TEXT NOT NULL,
    head_sha TEXT NOT NULL,
    merge_sha TEXT,
    target_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    title TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_pr_registry_repo_pr UNIQUE (repo, pr_number)
);

CREATE INDEX IF NOT EXISTS idx_pr_registry_head_sha ON pr_registry (repo, head_sha);
CREATE INDEX IF NOT EXISTS idx_pr_registry_merge_sha ON pr_registry (repo, merge_sha);
CREATE INDEX IF NOT EXISTS idx_pr_registry_status ON pr_registry (status);
CREATE INDEX IF NOT EXISTS idx_pr_registry_target_id ON pr_registry (target_id);

-- River Background Queue Tables
CREATE TABLE IF NOT EXISTS river_migration (
    line TEXT NOT NULL,
    version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT line_length CHECK (char_length(line) > 0 AND char_length(line) < 128),
    CONSTRAINT version_gte_1 CHECK (version >= 1),
    PRIMARY KEY (line, version)
);

DO $$ BEGIN
    CREATE TYPE river_job_state AS ENUM(
        'available',
        'cancelled',
        'completed',
        'discarded',
        'pending',
        'retryable',
        'running',
        'scheduled'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS river_job (
    id BIGSERIAL PRIMARY KEY,
    state river_job_state NOT NULL DEFAULT 'available',
    attempt SMALLINT NOT NULL DEFAULT 0,
    max_attempts SMALLINT NOT NULL DEFAULT 25,
    attempted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finalized_at TIMESTAMPTZ,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    priority SMALLINT NOT NULL DEFAULT 1,
    args JSONB NOT NULL,
    attempted_by TEXT[],
    errors JSONB[],
    kind TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    queue TEXT NOT NULL DEFAULT 'default',
    tags VARCHAR(255)[] NOT NULL DEFAULT '{}',
    unique_key BYTEA,
    unique_states BIT(8),
    CONSTRAINT finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
        (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
    ),
    CONSTRAINT max_attempts_is_positive CHECK (max_attempts > 0),
    CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    CONSTRAINT kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
);

CREATE OR REPLACE FUNCTION river_job_state_in_bitmask(bitmask BIT(8), state river_job_state)
RETURNS boolean
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE state
        WHEN 'available' THEN get_bit(bitmask, 7)
        WHEN 'cancelled' THEN get_bit(bitmask, 6)
        WHEN 'completed' THEN get_bit(bitmask, 5)
        WHEN 'discarded' THEN get_bit(bitmask, 4)
        WHEN 'pending'   THEN get_bit(bitmask, 3)
        WHEN 'retryable' THEN get_bit(bitmask, 2)
        WHEN 'running'   THEN get_bit(bitmask, 1)
        WHEN 'scheduled' THEN get_bit(bitmask, 0)
        ELSE 0
    END = 1;
$$;

CREATE INDEX IF NOT EXISTS river_job_kind ON river_job USING btree(kind);
CREATE INDEX IF NOT EXISTS river_job_state_and_finalized_at_index ON river_job USING btree(state, finalized_at) WHERE finalized_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS river_job_prioritized_fetching_index ON river_job USING btree(state, queue, priority, scheduled_at, id);
CREATE INDEX IF NOT EXISTS river_job_args_index ON river_job USING GIN(args);
CREATE INDEX IF NOT EXISTS river_job_metadata_index ON river_job USING GIN(metadata);
CREATE UNIQUE INDEX IF NOT EXISTS river_job_unique_idx ON river_job (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND river_job_state_in_bitmask(unique_states, state);

CREATE UNLOGGED TABLE IF NOT EXISTS river_leader (
    elected_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    leader_id TEXT NOT NULL,
    name TEXT PRIMARY KEY DEFAULT 'default',
    CONSTRAINT name_length CHECK (name = 'default'),
    CONSTRAINT leader_id_length CHECK (char_length(leader_id) > 0 AND char_length(leader_id) < 128)
);

CREATE TABLE IF NOT EXISTS river_queue (
    name TEXT PRIMARY KEY NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    paused_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS river_notification (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload TEXT NOT NULL,
    topic TEXT NOT NULL,
    CONSTRAINT topic_length CHECK (length(topic) > 0 AND length(topic) < 128)
);

CREATE INDEX IF NOT EXISTS river_notification_created_at_idx ON river_notification (created_at);
CREATE INDEX IF NOT EXISTS river_notification_topic_id_idx ON river_notification (topic, id);


