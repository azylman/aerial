-- Modify "dispatches" table
ALTER TABLE "public"."dispatches" ADD COLUMN "traceparent" text NOT NULL DEFAULT '';
-- Modify "messages" table
ALTER TABLE "public"."messages" ADD COLUMN "traceparent" text NOT NULL DEFAULT '';
