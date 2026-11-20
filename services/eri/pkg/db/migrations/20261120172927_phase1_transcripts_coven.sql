-- Rename a column from "stack" to "agent"
ALTER TABLE "extraction_watermarks" RENAME COLUMN "stack" TO "agent";
-- Modify "facts" table
ALTER TABLE "facts" ADD COLUMN "source_agent" text NOT NULL DEFAULT 'aerial';
-- Modify "procedural_candidates" table
ALTER TABLE "procedural_candidates" ADD COLUMN "agent" text NOT NULL DEFAULT 'aerial';
-- Create index "idx_procedural_candidates_agent_thread" to table: "procedural_candidates"
CREATE INDEX "idx_procedural_candidates_agent_thread" ON "procedural_candidates" ("agent", "thread_id");
-- Drop index "idx_session_summaries_stack_mtime" from table: "session_summaries"
DROP INDEX "idx_session_summaries_stack_mtime";
-- Rename a column from "stack" to "agent"
ALTER TABLE "session_summaries" RENAME COLUMN "stack" TO "agent";
-- Create index "idx_session_summaries_agent_mtime" to table: "session_summaries"
CREATE INDEX "idx_session_summaries_agent_mtime" ON "session_summaries" ("agent", "last_mtime");
-- Rename a column from "stack" to "agent"
ALTER TABLE "transcript_steps" RENAME COLUMN "stack" TO "agent";
-- Modify "transcript_steps" table
ALTER TABLE "transcript_steps" DROP CONSTRAINT "transcript_steps_stack_session_id_fkey", ADD CONSTRAINT "transcript_steps_agent_session_id_fkey" FOREIGN KEY ("agent", "session_id") REFERENCES "session_summaries" ("agent", "session_id") ON UPDATE NO ACTION ON DELETE CASCADE;
