-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create "cron_schedules" table
CREATE TABLE "public"."cron_schedules" (
  "id" text NOT NULL,
  "target_id" text NOT NULL,
  "title_prefix" text NOT NULL DEFAULT '',
  "cron_expr" text NOT NULL,
  "prompt" text NOT NULL,
  "timezone" text NOT NULL DEFAULT 'America/Los_Angeles',
  "next_run_at" timestamptz NOT NULL,
  "enabled" boolean NOT NULL DEFAULT true,
  "effort" text NOT NULL DEFAULT 'low',
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("id")
);
-- Create index "idx_cron_schedules_next_run_at" to table: "cron_schedules"
CREATE INDEX "idx_cron_schedules_next_run_at" ON "public"."cron_schedules" ("enabled", "next_run_at");
-- Create "facts" table
CREATE TABLE "public"."facts" (
  "id" bigserial NOT NULL,
  "category" text NOT NULL DEFAULT 'general',
  "fact_text" text NOT NULL,
  "importance" real NOT NULL DEFAULT 1.0,
  "embedding" public.vector(384) NULL,
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "last_reinforced_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "last_decayed_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "reinforce_count" integer NOT NULL DEFAULT 1,
  "fts_tokens" tsvector NULL GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, fact_text)) STORED,
  PRIMARY KEY ("id")
);
-- Create index "idx_facts_category" to table: "facts"
CREATE INDEX "idx_facts_category" ON "public"."facts" ("category");
-- Create index "idx_facts_created_at" to table: "facts"
CREATE INDEX "idx_facts_created_at" ON "public"."facts" ("created_at" DESC);
-- Create index "idx_facts_embedding_hnsw" to table: "facts"
CREATE INDEX "idx_facts_embedding_hnsw" ON "public"."facts" USING HNSW ("embedding" public.vector_cosine_ops);
-- Create index "idx_facts_fts" to table: "facts"
CREATE INDEX "idx_facts_fts" ON "public"."facts" USING GIN ("fts_tokens");
-- Create index "idx_facts_importance_created_at" to table: "facts"
CREATE INDEX "idx_facts_importance_created_at" ON "public"."facts" ("importance" DESC, "created_at" DESC);
-- Create index "idx_facts_last_reinforced_at" to table: "facts"
CREATE INDEX "idx_facts_last_reinforced_at" ON "public"."facts" ("last_reinforced_at" DESC);
-- Create "messages" table
CREATE TABLE "public"."messages" (
  "id" text NOT NULL,
  "row_id" bigserial NOT NULL,
  "thread_id" text NOT NULL DEFAULT '',
  "guild_id" text NOT NULL DEFAULT '',
  "author_id" text NOT NULL DEFAULT '',
  "author_name" text NOT NULL DEFAULT '',
  "content" text NOT NULL DEFAULT '',
  "summary" text NOT NULL DEFAULT '',
  "status" text NOT NULL DEFAULT 'PENDING',
  "retry_count" integer NOT NULL DEFAULT 0,
  "restart_count" integer NOT NULL DEFAULT 0,
  "effort" text NOT NULL DEFAULT '',
  "error_message" text NULL,
  "response_text" text NULL,
  "schedule_run_id" text NOT NULL DEFAULT '',
  "metadata" jsonb NOT NULL DEFAULT '{}',
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("id"),
  CONSTRAINT "messages_row_id_key" UNIQUE ("row_id")
);
-- Create index "idx_messages_row_id" to table: "messages"
CREATE INDEX "idx_messages_row_id" ON "public"."messages" ("row_id");
-- Create index "idx_messages_status_created_at" to table: "messages"
CREATE INDEX "idx_messages_status_created_at" ON "public"."messages" ("status", "created_at");
-- Create index "idx_messages_thread_row_id" to table: "messages"
CREATE INDEX "idx_messages_thread_row_id" ON "public"."messages" ("thread_id", "row_id");
-- Create index "idx_messages_thread_status" to table: "messages"
CREATE INDEX "idx_messages_thread_status" ON "public"."messages" ("thread_id", "status");
-- Create "one_shot_schedules" table
CREATE TABLE "public"."one_shot_schedules" (
  "id" text NOT NULL,
  "thread_id" text NOT NULL,
  "prompt" text NOT NULL,
  "run_at" timestamptz NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "effort" text NOT NULL DEFAULT 'low',
  PRIMARY KEY ("id")
);
-- Create index "idx_one_shot_schedules_run_at" to table: "one_shot_schedules"
CREATE INDEX "idx_one_shot_schedules_run_at" ON "public"."one_shot_schedules" ("run_at");
-- Create "schedule_runs" table
CREATE TABLE "public"."schedule_runs" (
  "id" text NOT NULL,
  "schedule_id" text NOT NULL,
  "schedule_type" text NOT NULL,
  "message_id" text NOT NULL DEFAULT '',
  "target_id" text NOT NULL,
  "thread_id" text NOT NULL,
  "title" text NOT NULL DEFAULT '',
  "prompt" text NOT NULL,
  "status" text NOT NULL DEFAULT 'enqueued',
  "started_at" timestamptz NOT NULL,
  "completed_at" timestamptz NULL,
  "duration_ms" bigint NULL DEFAULT 0,
  "effort" text NOT NULL DEFAULT 'low',
  "model" text NOT NULL DEFAULT '',
  "error" text NOT NULL DEFAULT '',
  PRIMARY KEY ("id")
);
-- Create index "idx_schedule_runs_message_id" to table: "schedule_runs"
CREATE INDEX "idx_schedule_runs_message_id" ON "public"."schedule_runs" ("message_id");
-- Create index "idx_schedule_runs_schedule_id" to table: "schedule_runs"
CREATE INDEX "idx_schedule_runs_schedule_id" ON "public"."schedule_runs" ("schedule_id");
-- Create index "idx_schedule_runs_schedule_started" to table: "schedule_runs"
CREATE INDEX "idx_schedule_runs_schedule_started" ON "public"."schedule_runs" ("schedule_id", "started_at" DESC);
-- Create index "idx_schedule_runs_started_at" to table: "schedule_runs"
CREATE INDEX "idx_schedule_runs_started_at" ON "public"."schedule_runs" ("started_at" DESC);
-- Create index "idx_schedule_runs_status_started" to table: "schedule_runs"
CREATE INDEX "idx_schedule_runs_status_started" ON "public"."schedule_runs" ("status", "started_at" DESC);
-- Create "session_summaries" table
CREATE TABLE "public"."session_summaries" (
  "session_id" text NOT NULL,
  "thread_id" text NOT NULL DEFAULT '',
  "summary" text NOT NULL DEFAULT '',
  "fts_tokens" tsvector NULL GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, summary)) STORED,
  "embedding" public.vector(384) NULL,
  "last_indexed_step" integer NOT NULL DEFAULT -1,
  "last_mtime" timestamptz NULL,
  "summary_step_watermark" integer NOT NULL DEFAULT -1,
  "is_settled" boolean NOT NULL DEFAULT false,
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("session_id")
);
-- Create index "idx_session_summaries_embedding" to table: "session_summaries"
CREATE INDEX "idx_session_summaries_embedding" ON "public"."session_summaries" USING HNSW ("embedding" public.vector_cosine_ops);
-- Create index "idx_session_summaries_fts" to table: "session_summaries"
CREATE INDEX "idx_session_summaries_fts" ON "public"."session_summaries" USING GIN ("fts_tokens");
-- Create index "idx_session_summaries_mtime" to table: "session_summaries"
CREATE INDEX "idx_session_summaries_mtime" ON "public"."session_summaries" ("last_mtime");
-- Create index "idx_session_summaries_thread" to table: "session_summaries"
CREATE INDEX "idx_session_summaries_thread" ON "public"."session_summaries" ("thread_id");
-- Create "sessions" table
CREATE TABLE "public"."sessions" (
  "thread_id" text NOT NULL,
  "internal_session_id" text NOT NULL DEFAULT '',
  "previous_session_id" text NOT NULL DEFAULT '',
  "turn_count" integer NOT NULL DEFAULT 0,
  "last_extracted_rowid" bigint NOT NULL DEFAULT 0,
  "fact_extracted_at" timestamptz NULL,
  "summary" text NOT NULL DEFAULT '',
  "last_summarized_message_id" text NOT NULL DEFAULT '',
  "active_tasks" text NOT NULL DEFAULT '[]',
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("thread_id")
);
-- Create index "idx_sessions_fact_extracted" to table: "sessions"
CREATE INDEX "idx_sessions_fact_extracted" ON "public"."sessions" ("last_extracted_rowid", "fact_extracted_at");
-- Create "transcript_steps" table
CREATE TABLE "public"."transcript_steps" (
  "session_id" text NOT NULL,
  "step_index" integer NOT NULL,
  "step_type" text NOT NULL DEFAULT '',
  "tool_name" text NOT NULL DEFAULT '',
  "content" text NOT NULL DEFAULT '',
  "fts_tokens" tsvector NULL GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, "left"(content, 50000))) STORED,
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("session_id", "step_index"),
  CONSTRAINT "transcript_steps_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "public"."session_summaries" ("session_id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_transcript_steps_fts" to table: "transcript_steps"
CREATE INDEX "idx_transcript_steps_fts" ON "public"."transcript_steps" USING GIN ("fts_tokens");
-- Create index "idx_transcript_steps_tool" to table: "transcript_steps"
CREATE INDEX "idx_transcript_steps_tool" ON "public"."transcript_steps" ("tool_name");
