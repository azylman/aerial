-- Modify "messages" table
ALTER TABLE "public"."messages" ADD COLUMN "duration_ms" bigint NOT NULL DEFAULT 0, ADD COLUMN "input_tokens" integer NOT NULL DEFAULT 0, ADD COLUMN "output_tokens" integer NOT NULL DEFAULT 0, ADD COLUMN "thinking_tokens" integer NOT NULL DEFAULT 0, ADD COLUMN "cache_read_tokens" integer NOT NULL DEFAULT 0, ADD COLUMN "total_tokens" integer NOT NULL DEFAULT 0;
-- Modify "dispatches" table
ALTER TABLE "public"."dispatches" ADD COLUMN "duration_ms" bigint NOT NULL DEFAULT 0, ADD COLUMN "input_tokens" integer NOT NULL DEFAULT 0, ADD COLUMN "output_tokens" integer NOT NULL DEFAULT 0, ADD COLUMN "thinking_tokens" integer NOT NULL DEFAULT 0, ADD COLUMN "cache_read_tokens" integer NOT NULL DEFAULT 0, ADD COLUMN "total_tokens" integer NOT NULL DEFAULT 0;
