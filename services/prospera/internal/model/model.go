package model

import "time"

// TranscriptTurn represents a single speaker turn in a call transcript.
type TranscriptTurn struct {
	Role      string    `json:"role"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
}

// OwnerInstruction captures mid-call guidance from the account owner.
type OwnerInstruction struct {
	Instruction string    `json:"instruction"`
	Mode        string    `json:"mode"`
	CreatedAt   time.Time `json:"created_at"`
}

// VoiceCall represents a persistent phone call record.
type VoiceCall struct {
	CallID                string             `json:"call_id"`
	ProviderCallSID       string             `json:"provider_call_sid,omitempty"`
	Direction             string             `json:"direction"`
	State                 string             `json:"state"`
	EndReason             string             `json:"end_reason,omitempty"`
	FromNumber            string             `json:"from_number"`
	ToNumber              string             `json:"to_number"`
	CallbackOfCallID      string             `json:"callback_of_call_id,omitempty"`
	DiscordChannelID      string             `json:"discord_channel_id,omitempty"`
	DiscordThreadID       string             `json:"discord_thread_id,omitempty"`
	Brief                 CallBrief          `json:"brief"`
	OwnerInstructions     []OwnerInstruction `json:"owner_instructions"`
	Transcript            []TranscriptTurn   `json:"transcript"`
	LiveTranscriptCursor  int                `json:"live_transcript_cursor"`
	AnsweredBy            string             `json:"answered_by,omitempty"`
	VoicemailRecordingURL string             `json:"voicemail_recording_url,omitempty"`
	ReportStatus          string             `json:"report_status"`
	ReportSummary         string             `json:"report_summary,omitempty"`
	StartedAt             time.Time          `json:"started_at"`
	AnsweredAt            *time.Time         `json:"answered_at,omitempty"`
	EndedAt               *time.Time         `json:"ended_at,omitempty"`
	UpdatedAt             time.Time          `json:"updated_at"`
}

// VoiceMessage represents an SMS or MMS message associated with Prospera.
type VoiceMessage struct {
	MessageSID       string    `json:"message_sid"`
	Direction        string    `json:"direction"`
	FromNumber       string    `json:"from_number"`
	ToNumber         string    `json:"to_number"`
	Body             string    `json:"body"`
	MediaURLs        []string  `json:"media_urls"`
	RelatedCallID    string    `json:"related_call_id,omitempty"`
	DiscordDelivered bool      `json:"discord_delivered"`
	CreatedAt        time.Time `json:"created_at"`
}
