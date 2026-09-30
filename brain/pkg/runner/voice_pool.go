package runner

import "context"

// VoiceSession defines the interface for communicating with an active voice daemon.
type VoiceSession interface {
	Send(prompt string, turn *TurnContext) error
	SessionID() string
}

// VoiceProcessPool manages pinned daemons and session retrieval for voice interactions.
type VoiceProcessPool interface {
	GetOrCreateSession(ctx context.Context, targetKey string) (VoiceSession, error)
	Initialize(ctx context.Context) error
	Close() error
}
