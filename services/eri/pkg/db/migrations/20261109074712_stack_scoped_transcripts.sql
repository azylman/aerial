-- Drop the "transcript_steps" foreign key first: it depends on "session_summaries_pkey", which is replaced below
ALTER TABLE "public"."transcript_steps" DROP CONSTRAINT "transcript_steps_session_id_fkey";
-- Modify "session_summaries" table (existing rows adopt this database's name as their stack so no stack re-syncs)
ALTER TABLE "public"."session_summaries" DROP CONSTRAINT "session_summaries_pkey", ADD COLUMN "stack" text NOT NULL DEFAULT current_database(), ADD COLUMN "engine" text NOT NULL DEFAULT '', ADD PRIMARY KEY ("stack", "session_id");
ALTER TABLE "public"."session_summaries" ALTER COLUMN "stack" DROP DEFAULT;
-- Create index "idx_session_summaries_stack_mtime" to table: "session_summaries"
CREATE INDEX "idx_session_summaries_stack_mtime" ON "public"."session_summaries" ("stack", "last_mtime");
-- Modify "transcript_steps" table
ALTER TABLE "public"."transcript_steps" DROP CONSTRAINT "transcript_steps_pkey", ADD COLUMN "stack" text NOT NULL DEFAULT current_database(), ADD COLUMN "tool_kind" text NOT NULL DEFAULT '', ADD COLUMN "tool_call_id" text NOT NULL DEFAULT '', ADD PRIMARY KEY ("stack", "session_id", "step_index"), ADD CONSTRAINT "transcript_steps_stack_session_id_fkey" FOREIGN KEY ("stack", "session_id") REFERENCES "public"."session_summaries" ("stack", "session_id") ON UPDATE NO ACTION ON DELETE CASCADE;
ALTER TABLE "public"."transcript_steps" ALTER COLUMN "stack" DROP DEFAULT;
-- Create index "idx_transcript_steps_tool_kind" to table: "transcript_steps"
CREATE INDEX "idx_transcript_steps_tool_kind" ON "public"."transcript_steps" ("tool_kind");
