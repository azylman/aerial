-- Modify "dispatches" table
ALTER TABLE "public"."dispatches" ADD COLUMN "target_id" text NOT NULL DEFAULT '', ADD COLUMN "vanadis_chunk_id" text NOT NULL DEFAULT '';
