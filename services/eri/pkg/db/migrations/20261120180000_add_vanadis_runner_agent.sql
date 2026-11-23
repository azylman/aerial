-- Modify "vanadis_builds" table
ALTER TABLE "vanadis_builds" ADD COLUMN "runner_agent" text NOT NULL DEFAULT 'aerial';
-- Create index "idx_vanadis_builds_runner_agent_status" to table: "vanadis_builds"
CREATE INDEX "idx_vanadis_builds_runner_agent_status" ON "vanadis_builds" ("runner_agent", "status");
