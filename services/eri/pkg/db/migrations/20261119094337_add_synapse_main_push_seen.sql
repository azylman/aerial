-- Create "synapse_main_push_seen" table
CREATE TABLE "public"."synapse_main_push_seen" ("repo" text NOT NULL, "latest_main_sha" text NOT NULL DEFAULT '', "updated_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT "synapse_main_push_seen_pkey" PRIMARY KEY ("repo"));
