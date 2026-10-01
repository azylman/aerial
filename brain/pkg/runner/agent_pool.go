package runner

import "context"

// MemoryRetriever retrieves formatted memory context (e.g. <retrieved_memory>...</retrieved_memory>)
// for a given query text. Returns empty string if no memory is found or on error.
type MemoryRetriever func(ctx context.Context, query string) (string, error)

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
