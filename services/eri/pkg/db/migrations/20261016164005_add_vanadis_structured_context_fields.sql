-- Modify "vanadis_builds" table
ALTER TABLE "public"."vanadis_builds" ADD COLUMN "shared_context" text NOT NULL DEFAULT '';
-- Modify "vanadis_chunks" table
ALTER TABLE "public"."vanadis_chunks" ADD COLUMN "target_symbols" text[] NOT NULL DEFAULT '{}', ADD COLUMN "effort" text NOT NULL DEFAULT 'low', ADD COLUMN "acceptance_criteria" text[] NOT NULL DEFAULT '{}';
