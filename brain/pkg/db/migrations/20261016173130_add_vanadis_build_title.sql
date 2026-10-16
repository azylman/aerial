-- Modify "vanadis_builds" table
ALTER TABLE "public"."vanadis_builds" ADD COLUMN "title" text NOT NULL DEFAULT '';
