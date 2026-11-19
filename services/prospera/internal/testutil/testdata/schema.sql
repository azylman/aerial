CREATE TABLE voice_calls (
	call_id TEXT PRIMARY KEY,
	provider_call_sid TEXT UNIQUE,
	direction TEXT NOT NULL,
	state TEXT NOT NULL,
	end_reason TEXT,
	from_number TEXT NOT NULL,
	to_number TEXT NOT NULL,
	callback_of_call_id TEXT,
	discord_channel_id TEXT,
	discord_thread_id TEXT,
	brief TEXT NOT NULL DEFAULT '{}',
	owner_instructions TEXT NOT NULL DEFAULT '[]',
	transcript TEXT NOT NULL DEFAULT '[]',
	live_transcript_cursor INTEGER NOT NULL DEFAULT 0,
	answered_by TEXT,
	voicemail_recording_url TEXT,
	report_status TEXT NOT NULL DEFAULT 'pending',
	report_summary TEXT,
	started_at TIMESTAMP NOT NULL,
	answered_at TIMESTAMP,
	ended_at TIMESTAMP,
	updated_at TIMESTAMP NOT NULL
);

CREATE TABLE voice_messages (
	message_sid TEXT PRIMARY KEY,
	direction TEXT NOT NULL,
	from_number TEXT NOT NULL,
	to_number TEXT NOT NULL,
	body TEXT NOT NULL,
	media_urls TEXT NOT NULL DEFAULT '[]',
	related_call_id TEXT,
	discord_delivered BOOLEAN NOT NULL DEFAULT FALSE,
	created_at TIMESTAMP NOT NULL
);
