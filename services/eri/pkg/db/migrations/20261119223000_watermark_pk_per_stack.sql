-- Modify "extraction_watermarks" table
ALTER TABLE "public"."extraction_watermarks" DROP CONSTRAINT "extraction_watermarks_pkey", DROP CONSTRAINT "uq_extraction_watermarks_stack_thread", ADD PRIMARY KEY ("stack", "thread_id");
