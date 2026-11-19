CREATE TABLE IF NOT EXISTS cron_schedules (
	id TEXT PRIMARY KEY,
	target_id TEXT NOT NULL,
	title_prefix TEXT NOT NULL DEFAULT '',
	cron_expr TEXT NOT NULL,
	prompt TEXT NOT NULL,
	timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
	next_run_at TIMESTAMP NOT NULL,
	enabled BOOLEAN NOT NULL DEFAULT TRUE,
	effort TEXT NOT NULL DEFAULT 'low',
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);

CREATE TABLE IF NOT EXISTS one_shot_schedules (
	id TEXT PRIMARY KEY,
	thread_id TEXT NOT NULL,
	prompt TEXT NOT NULL,
	run_at TIMESTAMP NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	effort TEXT NOT NULL DEFAULT 'low'
);
CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);

CREATE TABLE IF NOT EXISTS heartbeat_schedules (
	id TEXT PRIMARY KEY,
	target_id TEXT NOT NULL,
	title_prefix TEXT NOT NULL DEFAULT '',
	cron_expr TEXT NOT NULL,
	prompt TEXT NOT NULL,
	timezone TEXT NOT NULL DEFAULT 'UTC',
	effort TEXT NOT NULL DEFAULT 'low',
	next_run_at TIMESTAMP NOT NULL,
	enabled BOOLEAN NOT NULL DEFAULT TRUE,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	last_completed_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_heartbeat_schedules_next_run ON heartbeat_schedules(enabled, next_run_at);

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
	started_at TIMESTAMP NOT NULL,
	completed_at TIMESTAMP,
	duration_ms INTEGER DEFAULT 0,
	effort TEXT NOT NULL DEFAULT 'low',
	model TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS event_triggers (
	id TEXT PRIMARY KEY,
	channel_id TEXT NOT NULL,
	source TEXT NOT NULL,
	target TEXT NOT NULL,
	event_type TEXT NOT NULL,
	prompt TEXT NOT NULL,
	effort TEXT NOT NULL DEFAULT 'medium',
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	persistent BOOLEAN NOT NULL DEFAULT FALSE,
	updated_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_event_triggers_lookup ON event_triggers (source, target, event_type);
CREATE INDEX IF NOT EXISTS idx_event_triggers_channel ON event_triggers (channel_id, created_at ASC);

-- HARO_SQLITE_MIGRATIONS_BELOW
ALTER TABLE cron_schedules ADD COLUMN title_prefix TEXT NOT NULL DEFAULT '';
ALTER TABLE cron_schedules ADD COLUMN timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles';
ALTER TABLE cron_schedules ADD COLUMN effort TEXT NOT NULL DEFAULT 'low';
ALTER TABLE one_shot_schedules ADD COLUMN effort TEXT NOT NULL DEFAULT 'low';
ALTER TABLE heartbeat_schedules ADD COLUMN last_completed_at TIMESTAMP;
ALTER TABLE event_triggers ADD COLUMN persistent BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE event_triggers ADD COLUMN updated_at TIMESTAMP;
