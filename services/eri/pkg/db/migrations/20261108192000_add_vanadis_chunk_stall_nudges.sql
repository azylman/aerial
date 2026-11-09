-- Modify "vanadis_chunks" table
ALTER TABLE "public"."vanadis_chunks" ADD COLUMN "stall_nudges" integer NOT NULL DEFAULT 0;
