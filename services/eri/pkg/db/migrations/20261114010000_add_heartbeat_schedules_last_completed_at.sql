-- Modify "heartbeat_schedules" table
ALTER TABLE "public"."heartbeat_schedules" ADD COLUMN "last_completed_at" timestamptz NULL;
