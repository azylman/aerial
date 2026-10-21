-- Modify "pr_registry" table
ALTER TABLE "public"."pr_registry" ADD COLUMN "status_message_id" text NOT NULL DEFAULT '';
-- Modify "vanadis_builds" table
ALTER TABLE "public"."vanadis_builds" ADD COLUMN "status_message_id" text NOT NULL DEFAULT '';
-- Drop index "idx_vanadis_chunks_pr_number" from table: "vanadis_chunks"
DROP INDEX "public"."idx_vanadis_chunks_pr_number";
-- Modify "vanadis_chunks" table
ALTER TABLE "public"."vanadis_chunks" ADD COLUMN "repo" text NOT NULL DEFAULT 'azylman/aerial';
-- Create index "idx_vanadis_chunks_pr_number" to table: "vanadis_chunks"
CREATE INDEX "idx_vanadis_chunks_pr_number" ON "public"."vanadis_chunks" ("repo", "pr_number") WHERE (pr_number IS NOT NULL);
