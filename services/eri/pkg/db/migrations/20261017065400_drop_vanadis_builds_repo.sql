-- Drop index "idx_vanadis_builds_repo_status" from table: "vanadis_builds"
DROP INDEX "public"."idx_vanadis_builds_repo_status";
-- Modify "vanadis_builds" table
ALTER TABLE "public"."vanadis_builds" DROP COLUMN "repo";
-- Create index "idx_vanadis_builds_status" to table: "vanadis_builds"
CREATE INDEX "idx_vanadis_builds_status" ON "public"."vanadis_builds" ("status");
