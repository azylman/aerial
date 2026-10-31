-- Modify "event_triggers" table
ALTER TABLE "public"."event_triggers" ADD COLUMN "persistent" boolean NOT NULL DEFAULT false;
