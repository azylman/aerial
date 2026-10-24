-- Modify "messages" table
ALTER TABLE "public"."messages" ADD COLUMN "channel_id" text NOT NULL DEFAULT '', ADD COLUMN "channel_name" text NOT NULL DEFAULT '', ADD COLUMN "thread_name" text NOT NULL DEFAULT '', ADD COLUMN "author_global_name" text NOT NULL DEFAULT '', ADD COLUMN "author_bot" boolean NOT NULL DEFAULT false, ADD COLUMN "is_admin" boolean NOT NULL DEFAULT false, ADD COLUMN "mentions" text[] NOT NULL DEFAULT '{}', ADD COLUMN "mention_user_ids" text[] NOT NULL DEFAULT '{}', ADD COLUMN "mention_role_ids" text[] NOT NULL DEFAULT '{}', ADD COLUMN "replying_to_author" text NOT NULL DEFAULT '', ADD COLUMN "replying_to_content" text NOT NULL DEFAULT '', ADD COLUMN "attachments" text[] NOT NULL DEFAULT '{}', ADD COLUMN "model" text NOT NULL DEFAULT '', ADD COLUMN "silent" boolean NOT NULL DEFAULT false, ADD COLUMN "ephemeral" boolean NULL;
-- Backfill "messages" columns from "metadata"
UPDATE "public"."messages" SET
  "channel_id" = COALESCE("metadata"->>'channel_id', ''),
  "channel_name" = COALESCE("metadata"->>'channel_name', ''),
  "thread_name" = COALESCE("metadata"->>'thread_name', ''),
  "author_global_name" = COALESCE("metadata"->>'author_global_name', ''),
  "author_bot" = COALESCE(("metadata"->>'author_bot')::boolean, false),
  "is_admin" = COALESCE(("metadata"->>'is_admin')::boolean, false),
  "mentions" = CASE WHEN jsonb_typeof("metadata"->'mentions') = 'array' THEN ARRAY(SELECT jsonb_array_elements_text("metadata"->'mentions')) ELSE '{}'::text[] END,
  "mention_user_ids" = CASE WHEN jsonb_typeof("metadata"->'mention_user_ids') = 'array' THEN ARRAY(SELECT jsonb_array_elements_text("metadata"->'mention_user_ids')) ELSE '{}'::text[] END,
  "mention_role_ids" = CASE WHEN jsonb_typeof("metadata"->'mention_role_ids') = 'array' THEN ARRAY(SELECT jsonb_array_elements_text("metadata"->'mention_role_ids')) ELSE '{}'::text[] END,
  "replying_to_author" = COALESCE("metadata"->>'replying_to_author', ''),
  "replying_to_content" = COALESCE("metadata"->>'replying_to_content', ''),
  "attachments" = CASE WHEN jsonb_typeof("metadata"->'attachments') = 'array' THEN ARRAY(SELECT jsonb_array_elements_text("metadata"->'attachments')) ELSE '{}'::text[] END,
  "model" = COALESCE("metadata"->>'model', ''),
  "silent" = COALESCE(("metadata"->>'silent')::boolean, false),
  "ephemeral" = ("metadata"->>'ephemeral')::boolean
WHERE "metadata" IS NOT NULL AND "metadata" != '{}'::jsonb;
-- Drop "metadata" column from "messages"
ALTER TABLE "public"."messages" DROP COLUMN "metadata";
-- Create index "idx_messages_admin_completed" to table: "messages"
CREATE INDEX "idx_messages_admin_completed" ON "public"."messages" ("status", "is_admin", "updated_at");
-- Create "extraction_watermarks" table
CREATE TABLE "public"."extraction_watermarks" (
  "thread_id" text NOT NULL,
  "last_extracted_rowid" bigint NOT NULL DEFAULT 0,
  "fact_extracted_at" timestamptz NULL,
  "updated_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("thread_id")
);
-- Backfill "extraction_watermarks" from "sessions"
INSERT INTO "extraction_watermarks" ("thread_id", "last_extracted_rowid", "fact_extracted_at", "updated_at") SELECT "thread_id", COALESCE("last_extracted_rowid", 0), "fact_extracted_at", COALESCE("updated_at", CURRENT_TIMESTAMP) FROM "sessions" WHERE COALESCE("last_extracted_rowid", 0) > 0 OR "fact_extracted_at" IS NOT NULL ON CONFLICT ("thread_id") DO NOTHING;
-- Modify "sessions" table
ALTER TABLE "public"."sessions" DROP COLUMN "last_extracted_rowid", DROP COLUMN "fact_extracted_at", DROP COLUMN "active_tasks";
