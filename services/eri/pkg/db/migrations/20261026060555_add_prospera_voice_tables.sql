-- Create "voice_calls" table
CREATE TABLE "public"."voice_calls" (
  "call_id" text NOT NULL,
  "provider_call_sid" text NULL,
  "direction" text NOT NULL,
  "state" text NOT NULL,
  "end_reason" text NULL,
  "from_number" text NOT NULL,
  "to_number" text NOT NULL,
  "callback_of_call_id" text NULL,
  "discord_channel_id" text NULL,
  "discord_thread_id" text NULL,
  "brief" jsonb NOT NULL DEFAULT '{}',
  "owner_instructions" jsonb NOT NULL DEFAULT '[]',
  "transcript" jsonb NOT NULL DEFAULT '[]',
  "live_transcript_cursor" integer NOT NULL DEFAULT 0,
  "answered_by" text NULL,
  "voicemail_recording_url" text NULL,
  "report_status" text NOT NULL DEFAULT 'pending',
  "report_summary" text NULL,
  "started_at" timestamptz NOT NULL DEFAULT now(),
  "answered_at" timestamptz NULL,
  "ended_at" timestamptz NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("call_id"),
  CONSTRAINT "voice_calls_provider_call_sid_key" UNIQUE ("provider_call_sid"),
  CONSTRAINT "voice_calls_callback_of_call_id_fkey" FOREIGN KEY ("callback_of_call_id") REFERENCES "public"."voice_calls" ("call_id") ON UPDATE NO ACTION ON DELETE SET NULL,
  CONSTRAINT "voice_calls_direction_check" CHECK (direction = ANY (ARRAY['outbound'::text, 'inbound'::text])),
  CONSTRAINT "voice_calls_from_number_check" CHECK (from_number ~ '^\+[1-9][0-9]{1,14}$'::text),
  CONSTRAINT "voice_calls_report_status_check" CHECK (report_status = ANY (ARRAY['pending'::text, 'awaiting_recording'::text, 'processing'::text, 'delivered'::text, 'failed'::text])),
  CONSTRAINT "voice_calls_to_number_check" CHECK (to_number ~ '^\+[1-9][0-9]{1,14}$'::text)
);
-- Create index "idx_voice_calls_state" to table: "voice_calls"
CREATE INDEX "idx_voice_calls_state" ON "public"."voice_calls" ("state") WHERE (ended_at IS NULL);
-- Create index "idx_voice_calls_to_started" to table: "voice_calls"
CREATE INDEX "idx_voice_calls_to_started" ON "public"."voice_calls" ("to_number", "started_at" DESC) WHERE (direction = 'outbound'::text);
-- Create "voice_messages" table
CREATE TABLE "public"."voice_messages" (
  "message_sid" text NOT NULL,
  "direction" text NOT NULL,
  "from_number" text NOT NULL,
  "to_number" text NOT NULL,
  "body" text NOT NULL,
  "media_urls" jsonb NOT NULL DEFAULT '[]',
  "related_call_id" text NULL,
  "discord_delivered" boolean NOT NULL DEFAULT false,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("message_sid"),
  CONSTRAINT "voice_messages_related_call_id_fkey" FOREIGN KEY ("related_call_id") REFERENCES "public"."voice_calls" ("call_id") ON UPDATE NO ACTION ON DELETE SET NULL,
  CONSTRAINT "voice_messages_direction_check" CHECK (direction = ANY (ARRAY['outbound'::text, 'inbound'::text])),
  CONSTRAINT "voice_messages_from_number_check" CHECK (from_number ~ '^\+[1-9][0-9]{1,14}$'::text),
  CONSTRAINT "voice_messages_to_number_check" CHECK (to_number ~ '^\+[1-9][0-9]{1,14}$'::text)
);
-- Create index "idx_voice_messages_created" to table: "voice_messages"
CREATE INDEX "idx_voice_messages_created" ON "public"."voice_messages" ("created_at" DESC);
