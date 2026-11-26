package runner

import "context"

type (
	MemoryRetriever         func(ctx context.Context, query string) (string, error)
	AmbientContextRetriever func(ctx context.Context) (string, error)
)

type SessionRecord struct {
	ActiveSessionID   string
	PreviousSessionID string
	TurnCount         int
}

type AgentSession interface {
	Send(prompt string, turn *TurnContext) error
	SessionID() string
}

type SessionLease interface {
	SessionID() string
	PreviousSessionID() string
	IsCold() bool
	IsEphemeral() bool
	TurnCount() int
	MarkDirty()
	Execute(ctx context.Context, turn *TurnContext) (*TurnResult, error)
	Release() error
}

type AgentPool interface {
	AcquireLease(ctx context.Context, targetKey string) (SessionLease, error)
	Initialize(ctx context.Context) error
	Close() error
}
