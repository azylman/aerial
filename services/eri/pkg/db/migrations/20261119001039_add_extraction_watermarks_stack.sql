-- Modify "extraction_watermarks" table
ALTER TABLE "public"."extraction_watermarks" ADD COLUMN "stack" text NOT NULL DEFAULT 'aerial', ADD CONSTRAINT "uq_extraction_watermarks_stack_thread" UNIQUE ("stack", "thread_id");
