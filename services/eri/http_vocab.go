package main

// JSON field names shared by the HTTP handlers' response bodies.
const (
	jsonKeyError      = "error"
	jsonKeyStatus     = "status"
	jsonKeyChannelID  = "channel_id"
	jsonKeySessionID  = "session_id"
	jsonKeyMessageID  = "message_id"
	jsonKeyDispatchID = "dispatch_id"
)

// Voice SSE event names and the error outcome label recorded in metrics.
const (
	sseEventStatus = "status"
	sseEventError  = "error"
	outcomeError   = "error"
)
