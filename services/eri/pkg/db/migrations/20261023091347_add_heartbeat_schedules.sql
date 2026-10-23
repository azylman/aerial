-- Create "heartbeat_schedules" table
CREATE TABLE "public"."heartbeat_schedules" (
  "id" text NOT NULL,
  "target_id" text NOT NULL,
  "title_prefix" text NOT NULL DEFAULT '',
  "cron_expr" text NOT NULL,
  "prompt" text NOT NULL,
  "timezone" text NOT NULL DEFAULT 'UTC',
  "effort" text NOT NULL DEFAULT 'low',
  "next_run_at" timestamptz NOT NULL,
  "enabled" boolean NOT NULL DEFAULT true,
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("id")
);
-- Create index "idx_heartbeat_schedules_next_run" to table: "heartbeat_schedules"
CREATE INDEX "idx_heartbeat_schedules_next_run" ON "public"."heartbeat_schedules" ("enabled", "next_run_at");
