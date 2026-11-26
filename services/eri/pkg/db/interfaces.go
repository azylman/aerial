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

// TurnTelemetry encapsulates execution timing and token breakdown for messages and dispatches.
type TurnTelemetry struct {
	DurationMs      int64 `json:"duration_ms,omitempty"`
	InputTokens     int   `json:"input_tokens,omitempty"`
	OutputTokens    int   `json:"output_tokens,omitempty"`
	ThinkingTokens  int   `json:"thinking_tokens,omitempty"`
	CacheReadTokens int   `json:"cache_read_tokens,omitempty"`
	TotalTokens     int   `json:"total_tokens,omitempty"`
}

// AttemptStart reports the result of BeginMessageAttempt / BeginDispatchAttempt.
// Reentry is true when the row's retry_count already reached the requested ordinal
// (the attempt started before a crash), in which case restart_count was incremented.
type AttemptStart struct {
	Reentry      bool `json:"reentry"`
	RetryCount   int  `json:"retry_count"`
	RestartCount int  `json:"restart_count"`
}

// ErrRecordNotFound is returned by by-ID record operations when the row does not exist.
var ErrRecordNotFound = errors.New("record not found")

// ErrSessionSuperseded is returned when RotateSessionID cannot match the expected prior session ID.
var ErrSessionSuperseded = errors.New("session rotation superseded: active session does not match expected baseline")

// MessageStore handles message queue lifecycle and turn history.
type MessageStore interface {
	InsertMessage(ctx context.Context, msg Message) error
	UpdateMessageStatus(ctx context.Context, id, status, errorMsg string, telemetry ...TurnTelemetry) error
	UpdateMessageCompleted(ctx context.Context, id, responseText, conversationID string, telemetry ...TurnTelemetry) error
	GetMessage(ctx context.Context, id string) (*Message, error)
	MessageExists(ctx context.Context, id string) (bool, error)
	ClaimPendingMessage(ctx context.Context, id string) (bool, error)
	GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error)
	GetActiveTasks(ctx context.Context, filter ...TasksFilter) ([]ActiveTask, error)
	GetMessagesByID(ctx context.Context, ids []string) ([]Message, error)
	FailMessagesIfActive(ctx context.Context, ids []string, reason string) ([]string, error)
	FailMessagesIfPending(ctx context.Context, ids []string, reason string) ([]string, error)
	SkipMessagesIfActive(ctx context.Context, ids []string, reason string) ([]string, error)
	RequeueProcessingMessages(ctx context.Context, ids []string, reason string) ([]string, error)
	BeginMessageAttempt(ctx context.Context, id string, ordinal int) (AttemptStart, error)
}

// DispatchStore handles headless background dispatch execution.
type DispatchStore interface {
	InsertDispatch(ctx context.Context, d Dispatch) (bool, error)
	GetDispatch(ctx context.Context, id string) (*Dispatch, error)
	ClaimPendingDispatch(ctx context.Context, id string) (bool, error)
	UpdateDispatchStatus(ctx context.Context, id, status, errorMsg string, telemetry ...TurnTelemetry) error
	UpdateDispatchCompleted(ctx context.Context, id, responseText, conversationID string, telemetry ...TurnTelemetry) error
	GetRecentSessionDispatches(ctx context.Context, sessionID string, limit int) ([]Dispatch, error)
	FailDispatchesIfActive(ctx context.Context, ids []string, reason string) ([]string, error)
	FailDispatchesIfPending(ctx context.Context, ids []string, reason string) ([]string, error)
	SkipDispatchesIfActive(ctx context.Context, ids []string, reason string) ([]string, error)
	BeginDispatchAttempt(ctx context.Context, id string, ordinal int) (AttemptStart, error)
	ListActiveDispatchSessionKeys(ctx context.Context, dispatchID, buildID string) ([]string, error)
}

// SessionStore handles thread turn counts and session rotations.
type SessionStore interface {
	GetSessionID(ctx context.Context, threadID string) (string, error)
	GetPreviousSessionID(ctx context.Context, threadID string) (string, error)
	SaveSessionID(ctx context.Context, threadID, sessionID string) error
	IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	RotateSessionID(ctx context.Context, sessionKey, newSessionID string, expectedOldSessionID ...string) error
	GetThreadSummary(ctx context.Context, threadID string) (summary string, lastSummarizedMsgID string, err error)
	SaveThreadSummary(ctx context.Context, threadID, summary, lastSummarizedMsgID string) error
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

// TranscriptStore handles persistence and reconciliation of known turn sessions in the transcript index.
type TranscriptStore interface {
	GetKnownTurnSessionIDs(ctx context.Context) (map[string]bool, error)
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
}
