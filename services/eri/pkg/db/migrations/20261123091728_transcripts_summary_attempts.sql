-- Modify "session_summaries" table
ALTER TABLE "session_summaries" ADD COLUMN "summary_attempts" integer NOT NULL DEFAULT 0;
