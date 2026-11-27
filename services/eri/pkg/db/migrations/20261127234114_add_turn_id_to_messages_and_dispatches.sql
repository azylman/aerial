-- Modify "dispatches" table
ALTER TABLE "dispatches" ADD COLUMN "turn_id" text NOT NULL DEFAULT '';
-- Create index "idx_dispatches_agent_turn_id" to table: "dispatches"
CREATE INDEX "idx_dispatches_agent_turn_id" ON "dispatches" ("agent", "turn_id") WHERE (turn_id <> ''::text);
-- Modify "messages" table
ALTER TABLE "messages" ADD COLUMN "turn_id" text NOT NULL DEFAULT '';
-- Create index "idx_messages_agent_turn_id" to table: "messages"
CREATE INDEX "idx_messages_agent_turn_id" ON "messages" ("agent", "turn_id") WHERE (turn_id <> ''::text);
