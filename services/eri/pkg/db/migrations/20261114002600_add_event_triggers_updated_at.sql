-- Modify "event_triggers" table
ALTER TABLE "public"."event_triggers" ADD COLUMN "updated_at" timestamptz NULL;
