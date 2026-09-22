# Retire Pre-Postgres *sql.DB Interface Shims Implementation Plan

**Goal:** Clean up legacy pre-Postgres `*sql.DB` dual-type branches and `any` untyped shims across `pkg/scheduler`, `pkg/memory`, and `pkg/queue`, enforcing strict, hermetic `db.Store` and `db.FactStore` interface contracts.

**Architecture:**
- `brain/pkg/scheduler`: Remove `db *sql.DB` struct field, unused `"database/sql"` import, and all `*sql.DB` type-switch fallback shims across `New`, `getStore`, `ProcessDueSchedules`, `Start`, `Run`, `NewScheduler`, `RunPruneRetention`, `RunPruneRetentionWithContext`, `RunFactExtraction`, `RunMemoryDecayWithContext`, and `RunMemoryDecay`. Change signatures to accept `db.Store` directly (or `db.FactStore` for memory decay).
- `brain/pkg/memory`: Replace `database any` with `factStore db.FactStore` (and `sessStore db.SessionStore` / `store db.Store`) across `RetrieveRelevantFacts`, `BackfillMissingEmbeddings`, `ExtractActiveConversationFacts`, `processThreadFacts`, and `loadThreadTranscript`. Strip all obsolete `switch v := database.(type) { case *sql.DB: ... }` blocks and remove unused `"database/sql"` import from `search.go` and `extractor.go`.
- `brain/pkg/queue`: Update `MemoryRetrieverFunc` signature in `pool.go` to accept `factStore db.FactStore` instead of `database any`. Update `worker.go` to pass `te.store()` directly. Update all ~37 test sites in `queue_test.go` defining inline anonymous `MemoryRetrieverFunc` mocks.
- Tests: Update test assertions and mock signatures in `pkg/scheduler`, `pkg/memory`, and `pkg/queue`.

**Tech Stack:** Go (1.24+), PostgreSQL / pgvector contracts

## Global Constraints
- Zero breaking changes to production runtime behavior (`brain/main.go` already passes `store = db.NewSQLStore(database)`).
- Preserve test isolation: table-driven tests with in-memory `db.Store` / `FakeStore` / `MemStore`.
- Pre-flight staged verification (`./scripts/verify.sh --staged`) must pass cleanly.

---

### Task 1: Refactor `brain/pkg/scheduler` to Strict `db.Store` Contracts

**Files:**
- Modify: `brain/pkg/scheduler/scheduler.go`
- Modify: `brain/pkg/scheduler/scheduler_test.go`

**Changes in `scheduler.go`:**
- Remove `"database/sql"` from imports.
- In `Scheduler` struct: Remove `db *sql.DB`.
- In `New`: Update signature to `func New(cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) (*Scheduler, error)`. Remove `database *sql.DB` and `switch v := dbOrStore.(type)`.
- In `(s *Scheduler) getStore()`: Return `s.store` directly without fallback.
- In top-level wrappers:
  - `ProcessDueSchedules(ctx context.Context, cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator) (int, error)`
  - `Start(ctx context.Context, cfg *config.Config, store db.Store, pool *queue.WorkerPool, threadCreator ThreadCreator, opts ...Option) (*Scheduler, error)`
  - `Run(ctx context.Context, cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) error`
  - `NewScheduler(cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) (*Scheduler, error)`
  - `RunPruneRetention(store db.Store)`
  - `RunPruneRetentionWithContext(ctx context.Context, store db.Store)`
  - `RunFactExtraction(ctx context.Context, store db.Store, client *memory.Client, llmFunc memory.LLMClientFunc)`
  - `RunMemoryDecay(factStore db.FactStore)`
  - `RunMemoryDecayWithContext(ctx context.Context, factStore db.FactStore)`

**Changes in `scheduler_test.go`:**
- In `TestScheduler_GetStore`: Remove `sDB := &Scheduler{db: dummyDB}` test case.
- In `TestNew_StoreAndWrappers`: Remove `s1.db != nil` assertion.

---

### Task 2: Refactor `brain/pkg/memory` to Strict `db.FactStore` & `db.SessionStore` Contracts

**Files:**
- Modify: `brain/pkg/memory/search.go`
- Modify: `brain/pkg/memory/extractor.go`
- Modify: `brain/pkg/memory/memory_test.go`

**Changes in `search.go`:**
- Remove `"database/sql"` from imports.
- In `RetrieveRelevantFacts`: Update signature to `func RetrieveRelevantFacts(ctx context.Context, factStore db.FactStore, client *Client, queryText string, maxFacts int) ([]db.Fact, error)`. Remove type switch on `database.(type)`.

**Changes in `extractor.go`:**
- Remove `"database/sql"` from imports.
- In `BackfillMissingEmbeddings`: Update signature to `func BackfillMissingEmbeddings(ctx context.Context, factStore db.FactStore, client *Client) (int, error)`. Remove `case *sql.DB:`.
- In `ExtractActiveConversationFacts`: Update signature to `func ExtractActiveConversationFacts(ctx context.Context, store db.Store, client *Client, llmFunc LLMClientFunc, activeHours int) error`. Remove `case *sql.DB:`.
- In `processThreadFacts`: Update signature to `(ctx context.Context, store db.Store, client *Client, llmFunc LLMClientFunc, threadID string) error`.
- In `loadThreadTranscript`: Update signature to `(sessStore db.SessionStore, client *Client, threadID string) (string, error)`.

**Changes in `memory_test.go`:**
- Update `TestMemory_RetrieveRelevantFacts_TypesAndErrors`: remove `nilDB *sql.DB` and `"not a database"` test cases; test `nil db.FactStore` typed handling.
- Update `TestMemory_AdditionalCoverage`: remove `nilDB *sql.DB` test case in `BackfillMissingEmbeddings`; test `nil db.FactStore`.

---

### Task 3: Update `brain/pkg/queue` `MemoryRetrieverFunc` Signature

**Files:**
- Modify: `brain/pkg/queue/pool.go`
- Modify: `brain/pkg/queue/worker.go`
- Modify: `brain/pkg/queue/queue_test.go`

**Changes:**
- In `pool.go`: Update `MemoryRetrieverFunc` definition:
  `type MemoryRetrieverFunc func(ctx context.Context, factStore db.FactStore, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error)`
- In `worker.go`: Replace `dbArg := any(te.store()) ...` with passing `te.store()` directly to `te.pool.cfg.MemoryRetrieverFunc`.
- In `queue_test.go`: Update all ~37 anonymous `MemoryRetrieverFunc` mock sites from `func(ctx context.Context, database any, ...)` to `func(ctx context.Context, factStore db.FactStore, ...)`.

---

### Task 4: Verify Full Test Suite & Staged Changes

**Files:**
- Run package unit tests: `pkg/scheduler`, `pkg/memory`, `pkg/queue`, `main.go`.
- Run `./scripts/verify.sh --staged`.
