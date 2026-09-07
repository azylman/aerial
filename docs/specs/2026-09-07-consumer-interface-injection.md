# Consumer Package Storage Interface Injection Specification

**Date:** 2026-09-07  
**Target Repository:** `azylman/aerial` (`brain/pkg/memory`, `brain/pkg/scheduler`, `brain/pkg/queue`, `brain/pkg/session`, `brain/main.go`)  
**Status:** Approved by Human Review (Alex) - Stage 4 Implementation  

---

## 1. Overview & Objectives

Following the creation of storage interfaces in `brain/pkg/db/interfaces.go` (`FactStore`, `ScheduleStore`, `MessageStore`, `SessionStore`), this specification details refactoring higher-level consumer packages to depend on these Go interface types instead of concrete `*sql.DB` pointers or raw helper calls.

### Objectives:
1. **Consumer Interface Injection**: Update `memory`, `scheduler`, `queue`, and `session` to accept interface dependencies (`db.FactStore`, `db.ScheduleStore`, `db.MessageStore`, `db.SessionStore`, `db.Store`).
2. **100% Backward Compatibility**: Keep existing constructors (`scheduler.New`, `queue.NewWorkerPool`, `db.InitDB`) working seamlessly for legacy `*sql.DB` callers by auto-wrapping `*sql.DB` with `db.NewSQLStore(rawDB)`.
3. **Transaction Safety (`DBExecutor`)**: Ensure `SQLStore.WithTx` wraps active `*sql.Tx` handles via a `DBExecutor` interface so queries inside `WithTx` execute inside the transaction.
4. **Typed Nil Guarding & Thread Safety**: Provide nil-guard helpers and ensure all test mocks use `sync.RWMutex` thread safety for `-race` verification.

---

## 2. Refactoring Details by Package

### 2.1 `brain/pkg/db` (`SQLStore` Transaction Fix)
- Refactor `SQLStore` to hold a `DBExecutor` interface (`QueryContext`, `ExecContext`, `QueryRowContext`, `QueryRowContext`) implemented by `*sql.DB` and `*sql.Tx`.
- In `WithTx(ctx, fn)`, instantiate `txStore := &SQLStore{exec: tx}` so operations inside `fn` run strictly inside the transaction.

### 2.2 `brain/pkg/memory`
- Update `RetrieveRelevantFacts(ctx context.Context, database any, client *Client, queryText string, maxFacts int)`: Accepts `db.FactStore` or `*sql.DB`.
- Update `BackfillMissingEmbeddings(ctx context.Context, database any, client *Client)`: Accepts `db.FactStore` or `*sql.DB`.
- Update `ExtractActiveConversationFacts(ctx context.Context, database any, client *Client, llmFunc LLMClientFunc, activeHours int)`: Accepts `db.FactStore` or `*sql.DB`.

### 2.3 `brain/pkg/scheduler`
- Update `Scheduler` struct:
```go
type Scheduler struct {
    cfg           *config.Config
    store         db.Store
    enqueuer      MessageEnqueuer
    threadCreator ThreadCreator
}
```
- Update `New(cfg *config.Config, dbOrStore any, enqueuer MessageEnqueuer, threadCreator ThreadCreator) *Scheduler`: Auto-wraps `*sql.DB` with `db.NewSQLStore(db)`.
- Add `NewWithStore(cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator) *Scheduler`.

### 2.4 `brain/pkg/queue`
- Update `WorkerPoolConfig`:
```go
type WorkerPoolConfig struct {
    Store               db.Store
    DB                  *sql.DB
    MemoryClient        *memory.Client
    MemoryRetrieverFunc MemoryRetrieverFunc
    // ...
}
```
- Auto-wrap `WorkerPoolConfig.DB` into `WorkerPoolConfig.Store` if `Store` is omitted.
- Update `MemoryRetrieverFunc`:
```go
type MemoryRetrieverFunc func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error)
```

---

## 3. Verification & Test Strategy

- Execute `make test` across all microservices (`brain`, `scheduler-mcp`, `discord-mcp`, `dashboard`).
- Run `go test -race ./...` to verify race safety.
- Verify `main.go` compiles cleanly with zero breaking signature changes.
