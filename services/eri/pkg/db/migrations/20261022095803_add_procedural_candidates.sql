-- Create "procedural_candidates" table
CREATE TABLE "public"."procedural_candidates" (
  "id" bigserial NOT NULL,
  "thread_id" text NOT NULL DEFAULT '',
  "domain" text NOT NULL DEFAULT 'discord',
  "loaded_skill" text NOT NULL DEFAULT '',
  "trigger_pattern" text NOT NULL,
  "steps_json" jsonb NOT NULL DEFAULT '[]',
  "avoid_json" jsonb NOT NULL DEFAULT '[]',
  "embedding" public.vector(384) NOT NULL,
  "status" text NOT NULL DEFAULT 'pending',
  "pr_number" integer NULL,
  "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("id"),
  CONSTRAINT "procedural_candidates_trigger_pattern_check" CHECK (btrim(trigger_pattern) <> ''::text)
);
-- Create index "idx_procedural_candidates_loaded_skill" to table: "procedural_candidates"
CREATE INDEX "idx_procedural_candidates_loaded_skill" ON "public"."procedural_candidates" ("status", "loaded_skill");
-- Create index "idx_procedural_candidates_status" to table: "procedural_candidates"
CREATE INDEX "idx_procedural_candidates_status" ON "public"."procedural_candidates" ("status", "created_at");
