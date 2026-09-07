# Database Storage Abstraction & Hermetic Test Architecture Specification

**Date:** 2026-09-07  
**Target Repository:** `azylman/aerial` (`brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/scheduler`, `brain/pkg/queue`, `brain/pkg/session`)  
**Status:** Approved by Human Review (Alex) - Stage 4 Implementation  

---

## 1. Overview & Objectives

Higher-level packages in `brain` (`memory`, `scheduler`, `queue`, `session`) are being decoupled from concrete `*sql.DB` pointers and raw SQL helper functions in `brain/pkg/db`.

### Key Architecture Rules & Objectives:
1. **Consumer-Defined Interfaces**: Consumer packages define small, focused interfaces for their exact needs (`FactStore`, `ScheduleStore`, `MessageStore`, `SessionStore`) rather than inheriting a single monolithic 35-method interface.
2. **Dual Backend Implementations**:
   - `PostgresStore`: Native PostgreSQL driver utilizing `pgvector` HNSW vector indexes (`<=>` cosine distance), `BIGSERIAL` sequences, `FOR UPDATE SKIP LOCKED` queue claiming, and `pg_advisory_lock`.
   - `SQLiteStore` / `InMemoryStore`: Hermetic, sub-millisecond storage backends for unit testing with zero PostgreSQL network dependencies.
3. **Transaction Unit-of-Work (`WithTx`)**: Support cross-domain atomic transactions via a `WithTx(ctx, func(txStore Store) error) error` handle.
4. **Normalized Vector Math & Min-Heap Top-K**: Pre-normalize vectors ($||A|| = 1$) so Go cosine similarity in `SQLiteStore` simplifies to dot product ($A \cdot B$) and uses a `container/heap` min-heap of size `limit` ($O(N \log K)$).
5. **Contract Test Compliance (`dbtesting`)**: Shared test suite `TestStoreCompliance(t, factory)` validating both `PostgresStore` and `SQLiteStore` to guarantee zero behavioral divergence.

---

## 2. Interface Definitions (`brain/pkg/db/interfaces.go`)

```go
package db

import (
	"context"
	"errors"
	"time"
)

var (
	ErrScheduleAlreadyConsumed = errors.New("schedule already consumed")
	ErrMessageAlreadyClaimed   = errors.New("message already claimed")
	ErrFactNotFound            = errors.New("fact not found")
)

type FactStore interface {
	InsertFact(ctx context.Context, category, factText string, importance float64, threadID string, embedding []float32) (int64, error)
	SearchSimilarFacts(ctx context.Context, embedding []float32, limit int, minScore float64, threadID string) ([]Fact, error)
	GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error)
	GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error)
	UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error
	UpdateConversationFactExtractedAt(ctx context.Context, threadID string) error
}

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

type MessageStore interface {
	InsertMessage(ctx context.Context, msg Message) error
	UpdateMessageStatus(ctx context.Context, id string, status string, errorMsg string) error
	UpdateMessageCompleted(ctx context.Context, id string, responseText string) error
	IncrementMessageRetry(ctx context.Context, id string, errorMsg string) error
	GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]Message, error)
	GetMessage(ctx context.Context, id string) (*Message, error)
	MessageExists(ctx context.Context, id string) (bool, error)
	ClaimPendingMessage(ctx context.Context, id string) (bool, error)
	ClaimNextPendingMessage(ctx context.Context, workerID string) (*Message, error)
	GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error)
	GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]Message, error)
	GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error)
}

type SessionStore interface {
	GetSessionID(ctx context.Context, threadID string) (string, error)
	SaveSessionID(ctx context.Context, threadID, sessionID string) error
	DeleteSessionID(ctx context.Context, threadID string) error
	IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error)
	RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error
}

type Store interface {
	FactStore
	ScheduleStore
	MessageStore
	SessionStore
	WithTx(ctx context.Context, fn func(txStore Store) error) error
	Close() error
}
```

---

## 3. Implementation Steps

1. Create `interfaces.go` in `brain/pkg/db`.
2. Implement `PostgresStore` and `SQLiteStore` adhering to `Store` interface.
3. Add `NewTestStore(t testing.TB)` helper in `brain/pkg/db/test_helpers.go`.
4. Refactor `brain/pkg/memory`, `brain/pkg/scheduler`, `brain/pkg/queue`, `brain/pkg/session` to accept interface dependencies.
5. Run full test suite & coverage gating.
