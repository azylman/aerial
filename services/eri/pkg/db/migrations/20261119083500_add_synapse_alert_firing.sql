-- Create "synapse_alert_firing" table
CREATE TABLE "public"."synapse_alert_firing" ("fingerprint" text NOT NULL, "labels" jsonb NOT NULL DEFAULT '{}'::jsonb, "first_seen_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, "last_seen_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT "synapse_alert_firing_pkey" PRIMARY KEY ("fingerprint"));
