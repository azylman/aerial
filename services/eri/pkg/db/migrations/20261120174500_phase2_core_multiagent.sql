-- Modify "messages" table
ALTER TABLE "messages" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
ALTER TABLE "messages" DROP CONSTRAINT "messages_pkey", ADD CONSTRAINT "messages_pkey" PRIMARY KEY ("agent", "id");
ALTER TABLE "messages" DROP CONSTRAINT "messages_row_id_key";
ALTER TABLE "messages" ADD CONSTRAINT "uq_messages_agent_row_id" UNIQUE ("agent", "row_id");
DROP INDEX "idx_messages_row_id";
DROP INDEX "idx_messages_status_created_at";
DROP INDEX "idx_messages_thread_row_id";
CREATE INDEX "idx_messages_agent_status_created_at" ON "messages" ("agent", "status", "created_at" ASC);
CREATE INDEX "idx_messages_agent_thread_row_id" ON "messages" ("agent", "thread_id", "row_id");
CREATE INDEX "idx_messages_agent_row_id" ON "messages" ("agent", "row_id");

-- Modify "sessions" table
ALTER TABLE "sessions" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
ALTER TABLE "sessions" DROP CONSTRAINT "sessions_pkey", ADD CONSTRAINT "sessions_pkey" PRIMARY KEY ("agent", "session_id");

-- Modify "dispatches" table
ALTER TABLE "dispatches" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
ALTER TABLE "dispatches" DROP CONSTRAINT "dispatches_row_id_key";
ALTER TABLE "dispatches" ADD CONSTRAINT "uq_dispatches_agent_row_id" UNIQUE ("agent", "row_id");
DROP INDEX "idx_dispatches_status_created_at";
DROP INDEX "idx_dispatches_session_row_id";
CREATE INDEX "idx_dispatches_agent_status_created_at" ON "dispatches" ("agent", "status", "created_at" ASC);
CREATE INDEX "idx_dispatches_agent_session_row_id" ON "dispatches" ("agent", "session_id", "row_id");

-- Modify "one_shot_schedules" table
ALTER TABLE "one_shot_schedules" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
DROP INDEX "idx_one_shot_schedules_run_at";
CREATE INDEX "idx_one_shot_schedules_agent_run_at" ON "one_shot_schedules" ("agent", "run_at");

-- Modify "cron_schedules" table
ALTER TABLE "cron_schedules" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
DROP INDEX "idx_cron_schedules_next_run_at";
CREATE INDEX "idx_cron_schedules_agent_next_run" ON "cron_schedules" ("agent", "enabled", "next_run_at");

-- Modify "heartbeat_schedules" table
ALTER TABLE "heartbeat_schedules" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
DROP INDEX "idx_heartbeat_schedules_next_run";
CREATE INDEX "idx_heartbeat_schedules_agent_next_run" ON "heartbeat_schedules" ("agent", "enabled", "next_run_at");

-- Modify "schedule_runs" table
ALTER TABLE "schedule_runs" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
CREATE INDEX "idx_schedule_runs_agent_schedule_started" ON "schedule_runs" ("agent", "schedule_id", "started_at" DESC);
CREATE INDEX "idx_schedule_runs_agent_status_started" ON "schedule_runs" ("agent", "status", "started_at" DESC);

-- Modify "event_triggers" table
ALTER TABLE "event_triggers" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
DROP INDEX "idx_event_triggers_lookup";
DROP INDEX "idx_event_triggers_channel";
CREATE INDEX "idx_event_triggers_agent_lookup" ON "event_triggers" ("agent", "source", "target", "event_type");
CREATE INDEX "idx_event_triggers_agent_channel" ON "event_triggers" ("agent", "channel_id", "created_at" ASC);

-- Modify "vanadis_builds" table
ALTER TABLE "vanadis_builds" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
CREATE INDEX "idx_vanadis_builds_agent_status" ON "vanadis_builds" ("agent", "status");

-- Modify "pr_registry" table
ALTER TABLE "pr_registry" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
CREATE INDEX "idx_pr_registry_agent_status" ON "pr_registry" ("agent", "status");
