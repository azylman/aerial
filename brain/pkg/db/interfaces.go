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

// FactStore handles persistence and semantic search for conversation facts.
type FactStore interface {
	InsertFact(ctx context.Context, category, factText string, importance float64, threadID string, embedding []float32) (int64, error)
	SearchSimilarFacts(ctx context.Context, embedding []float32, limit int, minScore float64, threadID string) ([]Fact, error)
	GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error)
	GetFactsByThreadWithEmbeddings(ctx context.Context, threadID string) ([]FactWithEmbedding, error)
	GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error)
	UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error
	UpdateConversationFactExtractedAt(ctx context.Context, threadID string) error
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
	GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error)
	GetMessage(ctx context.Context, id string) (*Message, error)
	MessageExists(ctx context.Context, id string) (bool, error)
	ClaimPendingMessage(ctx context.Context, id string) (bool, error)
	ClaimNextPendingMessage(ctx context.Context, workerID string) (*Message, error)
	GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error)
	GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error)
	GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error)
}

// SessionStore handles thread turn counts and session rotations.
type SessionStore interface {
	GetSessionID(ctx context.Context, threadID string) (string, error)
	SaveSessionID(ctx context.Context, threadID, sessionID string) error
	DeleteSessionID(ctx context.Context, threadID string) error
	IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error
}

// Store unifies all repository capabilities under a single interface.
type Store interface {
	FactStore
	ScheduleStore
	MessageStore
	SessionStore
	WithTx(ctx context.Context, fn func(txStore Store) error) error
	Close() error
}
