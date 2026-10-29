-- Modify "vanadis_chunks" table
ALTER TABLE "public"."vanadis_chunks" ADD COLUMN "completed_at" timestamptz NULL;
UPDATE "public"."vanadis_chunks" SET "completed_at" = "updated_at" WHERE "status" IN ('merged', 'completed', 'failed', 'cancelled') AND "completed_at" IS NULL;
