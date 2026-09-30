package runner

import "context"

// AgentSession defines the interface for communicating with an active conversation session.
type AgentSession interface {
	Send(prompt string, turn *TurnContext) error
	SessionID() string
}

// AgentPool manages pinned daemons and session retrieval for conversational interactions.
type AgentPool interface {
	GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error)
	Initialize(ctx context.Context) error
	Close() error
}
