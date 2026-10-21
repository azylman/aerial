-- Drop index "idx_facts_created_at" from table: "facts"
DROP INDEX "public"."idx_facts_created_at";
-- Drop index "idx_facts_last_reinforced_at" from table: "facts"
DROP INDEX "public"."idx_facts_last_reinforced_at";
-- Modify "facts" table
ALTER TABLE "public"."facts" ADD CONSTRAINT "facts_fact_text_check" CHECK (btrim(fact_text) <> ''::text), DROP COLUMN "category", DROP COLUMN "importance", ALTER COLUMN "embedding" SET NOT NULL, DROP COLUMN "last_decayed_at";
-- Create index "idx_facts_activity" to table: "facts"
CREATE INDEX "idx_facts_activity" ON "public"."facts" ("reinforce_count" DESC, "last_reinforced_at" DESC);
