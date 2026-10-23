-- Create "event_triggers" table
CREATE TABLE "public"."event_triggers" (
  "id" text NOT NULL,
  "channel_id" text NOT NULL,
  "source" text NOT NULL,
  "target" text NOT NULL,
  "event_type" text NOT NULL,
  "prompt" text NOT NULL,
  "effort" text NOT NULL DEFAULT 'medium',
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("id")
);
-- Create index "idx_event_triggers_lookup" to table: "event_triggers"
CREATE INDEX "idx_event_triggers_lookup" ON "public"."event_triggers" ("source", "target", "event_type");
-- Create index "idx_event_triggers_channel" to table: "event_triggers"
CREATE INDEX "idx_event_triggers_channel" ON "public"."event_triggers" ("channel_id", "created_at" ASC);
