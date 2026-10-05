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

var (
	ErrScheduleAlreadyConsumed = errors.New("schedule already consumed")
	ErrMessageAlreadyClaimed   = errors.New("message already claimed")
	ErrFactNotFound            = errors.New("fact not found")
)

// SessionActivityStats summarizes database-level activity timestamps for session lifecycle decisions.
type SessionActivityStats struct {
	InternalSessionID             string    `json:"internal_session_id"`
	TurnCount                     int       `json:"turn_count"`
	SessionUpdatedAt              time.Time `json:"session_updated_at"`
	CompletedTurns                int64     `json:"completed_turns"`
	LastMessageAt                 time.Time `json:"last_message_at"`
	LastCompletedMessageCreatedAt time.Time `json:"last_completed_message_created_at"`
}

// FactStore handles persistence and semantic search for conversation facts.
type FactStore interface {
	InsertFact(ctx context.Context, category, factText string, importance float64, embedding []float32) (int64, error)
	SearchSimilarFacts(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]Fact, error)
	GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error)
	GetAllFactsWithEmbeddings(ctx context.Context) ([]FactWithEmbedding, error)
	GetFactsByThreadWithEmbeddings(ctx context.Context, threadID string) ([]FactWithEmbedding, error)
	GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error)
	UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error
	UpdateConversationFactExtractedAt(ctx context.Context, threadID string) error
	FindDuplicateFact(ctx context.Context, embedding []float32, minSim float64) (*Fact, float64, error)
	ReinforceFact(ctx context.Context, id int64, newText string, newEmbedding []float32, boost float64) error
	DecayAndPruneFacts(ctx context.Context, decayStep float64, pruneFloor float64, pruneAgeDays int) (decayed int64, pruned int64, err error)
	GetFactsMissingEmbeddings(ctx context.Context, limit int) ([]Fact, error)
	UpdateFactEmbedding(ctx context.Context, id int64, embedding []float32) error
}

// ScheduleStore handles cron and one-shot schedule execution and telemetry.
type ScheduleStore interface {
	CreateOneShotSchedule(ctx context.Context, s OneShotSchedule) error
	GetDueOneShotSchedules(ctx context.Context) ([]OneShotSchedule, error)
	DeleteOneShotSchedule(ctx context.Context, id string) error
	InsertMessageAndConsumeOneShot(ctx context.Context, scheduleID string, msg Message) error
	GetAllOneShotSchedules(ctx context.Context, threadID string) ([]OneShotSchedule, error)

	CreateCronSchedule(ctx context.Context, c CronSchedule) error
	GetDueCronSchedules(ctx context.Context) ([]CronSchedule, error)
	GetAllCronSchedules(ctx context.Context, targetID string) ([]CronSchedule, error)
	DeleteCronSchedule(ctx context.Context, id string) error
	UpdateCronNextRun(ctx context.Context, id string, nextRunAt time.Time) error
	UpdateCronScheduleEffort(ctx context.Context, id, effort string) error

	CreateScheduleRun(ctx context.Context, run ScheduleRun) error
	UpdateScheduleRunStatus(ctx context.Context, params UpdateRunParams) error
	GetScheduleRunsPaginated(ctx context.Context, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error)
	GetScheduleSummaryMetrics(ctx context.Context) (ScheduleSummaryMetrics, error)
	ReconcileOrphanedScheduleRuns(ctx context.Context) (int64, error)
	PruneScheduleRuns(ctx context.Context, maxCount int, maxAge time.Duration) (int64, error)
}

// MessageStore handles message queue lifecycle and turn history.
type MessageStore interface {
	InsertMessage(ctx context.Context, msg Message) error
	UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error
	UpdateMessageCompleted(ctx context.Context, id, responseText string) error
	IncrementMessageRetry(ctx context.Context, id, errorMsg string) error
	IncrementMessageRestart(ctx context.Context, id, errorMsg string) error
	ResetMessageToPendingWithRestart(ctx context.Context, id, reason string) error
	GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error)
	GetMessage(ctx context.Context, id string) (*Message, error)
	MessageExists(ctx context.Context, id string) (bool, error)
	ClaimPendingMessage(ctx context.Context, id string) (bool, error)
	ClaimNextPendingMessage(ctx context.Context, workerID string) (*Message, error)
	GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error)
	GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error)
	GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error)
	GetActiveTasks(ctx context.Context) ([]ActiveTask, error)
}

// SessionStore handles thread turn counts and session rotations.
type SessionStore interface {
	GetSessionID(ctx context.Context, threadID string) (string, error)
	GetPreviousSessionID(ctx context.Context, threadID string) (string, error)
	SaveSessionID(ctx context.Context, threadID, sessionID string) error
	DeleteSessionID(ctx context.Context, threadID string) error
	IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error
	GetSessionInfo(ctx context.Context, threadID string) (*SessionInfo, error)
	GetThreadSummary(ctx context.Context, threadID string) (summary string, lastSummarizedMsgID string, err error)
	SaveThreadSummary(ctx context.Context, threadID, summary, lastSummarizedMsgID string) error
	GetSessionActivityStats(ctx context.Context, threadID string) (*SessionActivityStats, error)
	GetExternalConversationID(ctx context.Context, internalID string) (string, error)
	SaveConversationMapping(ctx context.Context, externalID, internalID string) error
	FindUnrotatedSessions(ctx context.Context, minTurns int) ([]SessionInfo, error)
}

// SessionSummary encapsulates macro-level session summaries, embeddings, and sync metadata.
type SessionSummary struct {
	SessionID            string    `json:"session_id"`
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
	LastMtime       time.Time `json:"last_mtime"`
	LastIndexedStep int       `json:"last_indexed_step"`
	IsSettled       bool      `json:"is_settled"`
}

// TranscriptStep captures granular execution step details and tool receipts.
type TranscriptStep struct {
	SessionID string    `json:"session_id"`
	StepIndex int       `json:"step_index"`
	StepType  string    `json:"step_type"`
	ToolName  string    `json:"tool_name"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
	RankScore float64   `json:"rank_score,omitempty"`
}

// TranscriptStore handles persistence, reconciliation, and search for session summaries and transcript steps.
type TranscriptStore interface {
	GetSessionSyncStates(ctx context.Context) (map[string]SessionSyncState, error)
	UpsertSessionSummary(ctx context.Context, summary SessionSummary) error
	BatchInsertTranscriptSteps(ctx context.Context, steps []TranscriptStep) error
	SearchSessionSummaries(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]SessionSummary, error)
	SearchTranscriptSteps(ctx context.Context, queryText, sessionFilter, toolFilter string, limit int) ([]TranscriptStep, error)
}

// PRRecord encapsulates a tracked Pull Request and deployment lifecycle state.
type PRRecord struct {
	ID        int64     `json:"id"`
	Repo      string    `json:"repo"`
	PRNumber  int       `json:"pr_number"`
	Branch    string    `json:"branch"`
	HeadSHA   string    `json:"head_sha"`
	MergeSHA  string    `json:"merge_sha,omitempty"`
	TargetID  string    `json:"target_id"`
	Status    string    `json:"status"`
	Title     string    `json:"title"`
	Metadata  string    `json:"metadata"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PRRegistryStore handles persistent tracking and atomic state transitions for PRs and deployments.
type PRRegistryStore interface {
	UpsertPR(ctx context.Context, record PRRecord) error
	GetPRByNumber(ctx context.Context, repo string, prNumber int) (*PRRecord, error)
	GetPRByHeadSHA(ctx context.Context, repo string, headSHA string) (*PRRecord, error)
	GetPRByMergeSHA(ctx context.Context, repo string, mergeSHA string) (*PRRecord, error)
	UpdatePRMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) error
	UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string) error
	AtomicTransitionPRStatus(ctx context.Context, repo string, prNumber int, toStatus, notStatus string) (*PRRecord, error)
	AtomicTransitionPRStatusByMergeSHA(ctx context.Context, repo string, mergeSHA string, toStatus, notStatus string) (*PRRecord, error)
}

// Store unifies all repository capabilities under a single interface.
type Store interface {
	FactStore
	ScheduleStore
	MessageStore
	SessionStore
	TranscriptStore
	PRRegistryStore
	WithTx(ctx context.Context, fn func(txStore Store) error) error
	Close() error
}
