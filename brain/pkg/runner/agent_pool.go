package runner

import (
	"context"
)

// MemoryRetriever retrieves formatted memory context (e.g. <retrieved_memory>...</retrieved_memory>)
// for a given query text. Returns empty string if no memory is found or on error.
type MemoryRetriever func(ctx context.Context, query string) (string, error)

// AmbientContextRetriever retrieves formatted ambient context (e.g. <ambient_context>...</ambient_context>).
// Returns empty string if disabled, empty, or on error.
type AmbientContextRetriever func(ctx context.Context) (string, error)

// SessionRecord provides persistent state queried during cold starts.
type SessionRecord struct {
	ActiveSessionID   string
	PreviousSessionID string
	TurnCount         int
}

// AgentSession defines the interface for communicating with an active conversation session.
type AgentSession interface {
	Send(prompt string, turn *TurnContext) error
	SessionID() string
}

// SessionLease represents an exclusive, caller-held lease over an active session.
// It serializes execution for the target and encapsulates lifecycle state and rotation.
type SessionLease interface {
	// Identity & Authoritative Lifecycle State
	SessionID() string
	PreviousSessionID() string
	IsCold() bool
	TurnCount() int

	// Execution
	Execute(ctx context.Context, turn *TurnContext) (*TurnResult, error)

	// CancelRotation instructs the lease to suppress session rotation on Release.
	// Used when external turn errors (such as API quota pauses) require preserving session state.
	CancelRotation()

	// Release terminates the turn lease. It is idempotent (sync.Once), evaluates rotation,
	// calls the injected persistence hook (OnSessionRotated), and releases the target lock.
	Release() error
}

// AgentPool manages pinned daemons and session retrieval for conversational interactions.
type AgentPool interface {
	GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error)
	Initialize(ctx context.Context) error
	Close() error
}

// LeasedAgentPool defines the interface for pools that manage sessions via exclusive leases.
type LeasedAgentPool interface {
	AgentPool
	AcquireLease(ctx context.Context, targetKey string) (SessionLease, error)
}

// SessionRotator is an optional interface implemented by pools that support explicit session rotation.
type SessionRotator interface {
	ShouldRotateSession(sess AgentSession) (bool, string)
	RotateSession(ctx context.Context, targetKey string) (AgentSession, error)
}
