-- Modify "vanadis_chunks" table
ALTER TABLE "public"."vanadis_chunks" ALTER COLUMN "repo" SET DEFAULT '', ADD COLUMN "mode" text NOT NULL DEFAULT 'pr', ADD CONSTRAINT "vanadis_chunks_mode_check" CHECK ((mode = 'pr' AND repo <> '') OR (mode = 'action' AND repo = '' AND pr_number IS NULL));
