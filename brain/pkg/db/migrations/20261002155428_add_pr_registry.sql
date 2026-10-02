-- Create "pr_registry" table
CREATE TABLE "public"."pr_registry" (
  "id" bigserial NOT NULL,
  "repo" text NOT NULL,
  "pr_number" integer NOT NULL,
  "branch" text NOT NULL,
  "head_sha" text NOT NULL,
  "merge_sha" text NULL,
  "target_id" text NOT NULL,
  "status" text NOT NULL DEFAULT 'open',
  "title" text NOT NULL DEFAULT '',
  "metadata" jsonb NOT NULL DEFAULT '{}',
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("id"),
  CONSTRAINT "uq_pr_registry_repo_pr" UNIQUE ("repo", "pr_number")
);
-- Create index "idx_pr_registry_head_sha" to table: "pr_registry"
CREATE INDEX "idx_pr_registry_head_sha" ON "public"."pr_registry" ("repo", "head_sha");
-- Create index "idx_pr_registry_merge_sha" to table: "pr_registry"
CREATE INDEX "idx_pr_registry_merge_sha" ON "public"."pr_registry" ("repo", "merge_sha");
-- Create index "idx_pr_registry_status" to table: "pr_registry"
CREATE INDEX "idx_pr_registry_status" ON "public"."pr_registry" ("status");
-- Create index "idx_pr_registry_target_id" to table: "pr_registry"
CREATE INDEX "idx_pr_registry_target_id" ON "public"."pr_registry" ("target_id");
