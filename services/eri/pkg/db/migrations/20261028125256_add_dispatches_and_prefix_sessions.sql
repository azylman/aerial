-- Modify "messages" table
ALTER TABLE "public"."messages" ADD COLUMN "conversation_id" text NOT NULL DEFAULT '';
-- Rename columns on "sessions" and prefix numeric Discord snowflakes
ALTER TABLE "public"."sessions" RENAME COLUMN "thread_id" TO "session_id";
ALTER TABLE "public"."sessions" RENAME COLUMN "internal_session_id" TO "conversation_id";
ALTER TABLE "public"."sessions" RENAME COLUMN "previous_session_id" TO "previous_conversation_id";
UPDATE "public"."sessions" SET "session_id" = 'discord-' || "session_id" WHERE "session_id" ~ '^[0-9]+$';
-- Create "dispatches" table
CREATE TABLE "public"."dispatches" (
  "id" text NOT NULL,
  "row_id" bigserial NOT NULL,
  "source" text NOT NULL DEFAULT '',
  "session_id" text NOT NULL DEFAULT '',
  "conversation_id" text NOT NULL DEFAULT '',
  "prompt" text NOT NULL DEFAULT '',
  "summary" text NOT NULL DEFAULT '',
  "effort" text NOT NULL DEFAULT '',
  "model" text NOT NULL DEFAULT '',
  "status" text NOT NULL DEFAULT 'PENDING',
  "retry_count" integer NOT NULL DEFAULT 0,
  "restart_count" integer NOT NULL DEFAULT 0,
  "error_message" text NULL,
  "response_text" text NULL,
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("id"),
  CONSTRAINT "dispatches_row_id_key" UNIQUE ("row_id")
);
-- Create index "idx_dispatches_session_row_id" to table: "dispatches"
CREATE INDEX "idx_dispatches_session_row_id" ON "public"."dispatches" ("session_id", "row_id");
-- Create index "idx_dispatches_session_status" to table: "dispatches"
CREATE INDEX "idx_dispatches_session_status" ON "public"."dispatches" ("session_id", "status");
-- Create index "idx_dispatches_status_created_at" to table: "dispatches"
CREATE INDEX "idx_dispatches_status_created_at" ON "public"."dispatches" ("status", "created_at");
