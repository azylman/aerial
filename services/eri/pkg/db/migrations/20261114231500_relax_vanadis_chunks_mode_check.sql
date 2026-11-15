-- Modify "vanadis_chunks" table
ALTER TABLE "public"."vanadis_chunks" DROP CONSTRAINT "vanadis_chunks_mode_check", ADD CONSTRAINT "vanadis_chunks_mode_check" CHECK (((mode = 'pr'::text) AND (repo <> ''::text)) OR ((mode = 'action'::text) AND (((repo = ''::text) AND (pr_number IS NULL)) OR ((repo <> ''::text) AND (pr_number IS NOT NULL)))));
