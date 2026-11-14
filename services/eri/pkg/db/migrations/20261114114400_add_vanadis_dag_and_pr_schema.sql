-- Modify "vanadis_builds" table
ALTER TABLE "public"."vanadis_builds" ADD COLUMN "quiet" boolean NOT NULL DEFAULT false;
-- Drop index "idx_vanadis_chunks_pr_number" from table: "vanadis_chunks"
DROP INDEX "public"."idx_vanadis_chunks_pr_number";
-- Modify "vanadis_chunks" table
ALTER TABLE "public"."vanadis_chunks" ADD COLUMN "depends_on" text[] NOT NULL DEFAULT '{}', ADD COLUMN "ci_fail_count" integer NOT NULL DEFAULT 0, ADD COLUMN "ci_checks" jsonb NOT NULL DEFAULT '{}', ADD COLUMN "rollout_state" jsonb NOT NULL DEFAULT '{}', ADD COLUMN "banner" text NOT NULL DEFAULT '', ADD COLUMN "merge_awakeable_id" text NOT NULL DEFAULT '', ADD COLUMN "deploy_awakeable_id" text NOT NULL DEFAULT '';
-- Create index "idx_vanadis_chunks_repo_pr" to table: "vanadis_chunks"
CREATE INDEX "idx_vanadis_chunks_repo_pr" ON "public"."vanadis_chunks" ("repo", "pr_number") WHERE (pr_number IS NOT NULL);
