package db

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// DBTX abstracts common SQL execution methods shared by *sql.DB and *sql.Tx.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

var ErrMessageAlreadyClaimed = errors.New("message already claimed")

// SessionActivityStats summarizes database-level activity timestamps for session lifecycle decisions.
type SessionActivityStats struct {
	InternalSessionID             string    `json:"internal_session_id"`
	TurnCount                     int       `json:"turn_count"`
	SessionUpdatedAt              time.Time `json:"session_updated_at"`
	CompletedTurns                int64     `json:"completed_turns"`
	LastMessageAt                 time.Time `json:"last_message_at"`
	LastCompletedMessageCreatedAt time.Time `json:"last_completed_message_created_at"`
}

// TurnTelemetry encapsulates execution timing and token breakdown for messages and dispatches.
type TurnTelemetry struct {
	DurationMs      int64 `json:"duration_ms,omitempty"`
	InputTokens     int   `json:"input_tokens,omitempty"`
	OutputTokens    int   `json:"output_tokens,omitempty"`
	ThinkingTokens  int   `json:"thinking_tokens,omitempty"`
	CacheReadTokens int   `json:"cache_read_tokens,omitempty"`
	TotalTokens     int   `json:"total_tokens,omitempty"`
}

// MessageStore handles message queue lifecycle and turn history.
type MessageStore interface {
	InsertMessage(ctx context.Context, msg Message) error
	UpdateMessageStatus(ctx context.Context, id, status, errorMsg string, telemetry ...TurnTelemetry) error
	UpdateMessageCompleted(ctx context.Context, id, responseText, conversationID string, telemetry ...TurnTelemetry) error
	IncrementMessageRetry(ctx context.Context, id, errorMsg string) error
	ResetMessageToPendingWithRestart(ctx context.Context, id, reason string) error
	GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error)
	GetMessage(ctx context.Context, id string) (*Message, error)
	MessageExists(ctx context.Context, id string) (bool, error)
	ClaimPendingMessage(ctx context.Context, id string) (bool, error)
	GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error)
	GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error)
	HasActiveThreadTurn(ctx context.Context, threadID string) (bool, error)
	GetActiveTasks(ctx context.Context, filter ...TasksFilter) ([]ActiveTask, error)
}

// DispatchStore handles headless background dispatch execution.
type DispatchStore interface {
	InsertDispatch(ctx context.Context, d Dispatch) error
	GetDispatch(ctx context.Context, id string) (*Dispatch, error)
	ClaimPendingDispatch(ctx context.Context, id string) (bool, error)
	UpdateDispatchStatus(ctx context.Context, id, status, errorMsg string, telemetry ...TurnTelemetry) error
	UpdateDispatchCompleted(ctx context.Context, id, responseText, conversationID string, telemetry ...TurnTelemetry) error
	IncrementDispatchRetry(ctx context.Context, id, errorMsg string) error
	ResetDispatchToPendingWithRestart(ctx context.Context, id, reason string) error
	GetPendingOrProcessingDispatches(ctx context.Context, limit int) ([]Dispatch, error)
	GetRecentSessionDispatches(ctx context.Context, sessionID string, limit int) ([]Dispatch, error)
	HasActiveDispatchTurn(ctx context.Context, sessionID, id string) (bool, error)
	CancelPendingDispatches(ctx context.Context, dispatchID, buildID, reason string) ([]string, error)
}

// SessionStore handles thread turn counts and session rotations.
type SessionStore interface {
	GetSessionID(ctx context.Context, threadID string) (string, error)
	GetPreviousSessionID(ctx context.Context, threadID string) (string, error)
	SaveSessionID(ctx context.Context, threadID, sessionID string) error
	IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error
	GetThreadSummary(ctx context.Context, threadID string) (summary string, lastSummarizedMsgID string, err error)
	SaveThreadSummary(ctx context.Context, threadID, summary, lastSummarizedMsgID string) error
	GetSessionActivityStats(ctx context.Context, threadID string) (*SessionActivityStats, error)
	FindUnrotatedSessions(ctx context.Context, minTurns int, maxAge ...time.Duration) ([]SessionInfo, error)
}

// SessionSummary encapsulates macro-level session summaries, embeddings, and sync metadata.
type SessionSummary struct {
	Stack                string    `json:"stack,omitempty"`
	SessionID            string    `json:"session_id"`
	Engine               string    `json:"engine,omitempty"`
	ThreadID             string    `json:"thread_id"`
	Summary              string    `json:"summary"`
	Embedding            []float32 `json:"embedding,omitempty"`
	LastIndexedStep      int       `json:"last_indexed_step"`
	LastMtime            time.Time `json:"last_mtime,omitempty"`
	SummaryStepWatermark int       `json:"summary_step_watermark"`
	IsSettled            bool      `json:"is_settled"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
	Score                float64   `json:"score,omitempty"`
}

// SessionSyncState tracks ingestion progress and modification times for disk-to-DB reconciliation.
type SessionSyncState struct {
	SessionID       string    `json:"session_id"`
	Engine          string    `json:"engine"`
	LastMtime       time.Time `json:"last_mtime"`
	LastIndexedStep int       `json:"last_indexed_step"`
	IsSettled       bool      `json:"is_settled"`
}

// TranscriptStep captures granular execution step details and tool receipts.
type TranscriptStep struct {
	Stack      string    `json:"stack,omitempty"`
	SessionID  string    `json:"session_id"`
	Engine     string    `json:"engine,omitempty"`
	StepIndex  int       `json:"step_index"`
	StepType   string    `json:"step_type"`
	ToolName   string    `json:"tool_name"`
	ToolKind   string    `json:"tool_kind,omitempty"`
	ToolCallID string    `json:"tool_call_id,omitempty"`
	Content    string    `json:"content"`
	CreatedAt  time.Time `json:"created_at"`
	RankScore  float64   `json:"rank_score,omitempty"`
}

// TranscriptStore handles persistence, reconciliation, and search for session summaries and transcript steps
// in a transcript index shared across stacks. Writes (sync states, upserts, inserts, backfills) are scoped to
// the store's own stack; reads take a stack filter where empty matches every stack.
type TranscriptStore interface {
	GetSessionSyncStates(ctx context.Context) (map[string]SessionSyncState, error)
	UpsertSessionSummary(ctx context.Context, summary SessionSummary) error
	SetSessionEngine(ctx context.Context, sessionID, engine string) error
	BatchInsertTranscriptSteps(ctx context.Context, steps []TranscriptStep) error
	BackfillToolKinds(ctx context.Context, mapper func(toolName string) string) (int64, error)
	SearchSessionSummaries(ctx context.Context, stack string, embedding []float32, queryText string, limit int, minScore float64) ([]SessionSummary, error)
	SearchTranscriptSteps(ctx context.Context, stack, queryText, sessionFilter, toolFilter string, limit int, since ...time.Time) ([]TranscriptStep, error)
	GetTranscriptStepRange(ctx context.Context, stack, sessionID string, startStep, endStep int) ([]TranscriptStep, error)
	GetTranscriptStats(ctx context.Context, stack string) ([]TranscriptStackStats, error)
}

// TranscriptStackStats counts one stack's indexed sessions and steps in the shared transcript index.
type TranscriptStackStats struct {
	Stack    string `json:"stack"`
	Sessions int    `json:"sessions"`
	Steps    int    `json:"steps"`
}

// Store unifies all repository capabilities under a single interface.
type Store interface {
	MessageStore
	DispatchStore
	SessionStore
	AcquireLeaderLock(ctx context.Context) (func(), error)
}
