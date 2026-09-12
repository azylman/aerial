# Modular Architecture Refactor for `brain/pkg/queue` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decompose the 2,418-line monolithic `brain/pkg/queue/queue.go` file into cohesive, modular domain units (`channel_resolution.go`, `pool.go`, `burst.go`, `recovery.go`, and `worker.go`) while preserving 100% public API compatibility, zero regression in concurrency and recovery invariants, and keeping test statement coverage strictly $\ge 95.0\%$ at every task boundary.

**Architecture:** Split the monolithic `queue.go` within package `queue` so all types, variables, and methods maintain zero-overhead internal package visibility while cleanly separating distinct concerns:
1. `channel_resolution.go`: Channel caching, snowflake resolution, effective thread/parent channel lookup, bot role extraction, and ambient wake classification.
2. `pool.go`: `WorkerPool` struct definition, configuration, constructor injection, model overrides, and graceful drain/stop lifecycle.
3. `burst.go`: Inbound message enqueueing, thread-scoped worker dispatching, channel backpressure coordination (`activeEnqueuers`), and non-blocking burst draining.
4. `recovery.go`: Interrupted task startup recovery, orphaned schedule run reconciliation, and dual-boundary crash velocity heuristics.
5. `worker.go`: Pipeline orchestration for `processBurst` structured around a clean `turnExecution` context struct, encapsulating staleness validation, ambient filtering, prompt synthesis, and execution retries with quota lockout propagation.

**Tech Stack:** Go 1.24, SQLite (modernc.org/sqlite), PostgreSQL 16 (pgx/v5 & pgvector), DiscordGo, Prometheus client.

## Global Constraints
- Pure Package Invariant: All refactored files reside strictly in `package queue`—no cyclic dependencies, no breaking package-level type or signature changes.
- 100% Public Interface Compatibility: `WorkerPool`, `WorkerPoolConfig`, `New`, `NewWorkerPool`, `RecoverInterrupted`, `ResolveEffectiveChannel`, `IsNumericSnowflake`, `GetCachedChannel`, `InvalidateChannelCache`, `CacheDiscordChannel`, and `CoalesceBurstPrompt` retain identical signatures and semantics.
- Zero Test Flakes & Test Isolation: All unit tests execute hermetically using in-memory databases (`:memory:`) or wire-mocked PostgreSQL fixtures (`127.0.0.1:0`).
- Strict Coverage Ratchet Invariant: Statement coverage for `brain/pkg/queue` must remain $\ge 95.0\%$ (baseline: 95.2%), and monorepo backend coverage must remain $\ge 95.0\%$ across all tasks.
- Continuous Coverage Gate Enforcement: `./scripts/check-coverage.sh --service brain --check` must be run and verified at the completion of *every* task.
- Discord Markdown Invariant: Zero markdown tables in all alerts, notifications, and telemetry.

---

### Task 1: Extract Channel Resolution & Discord Channel Cache (`channel_resolution.go`)

**Files:**
- Create: `brain/pkg/queue/channel_resolution.go`
- Modify: `brain/pkg/queue/queue.go`
- Test: `brain/pkg/queue/queue_test.go`, `brain/pkg/queue/coverage_boost_test.go`

**Interfaces:**
- Produces:
  ```go
  type ChannelSnapshot struct {
      ID       string
      GuildID  string
      Name     string
      ParentID string
      IsThread bool
  }
  func CacheDiscordChannel(ch *discordgo.Channel)
  func InvalidateChannelCache(channelID string)
  func GetCachedChannel(channelID string) (ChannelSnapshot, bool)
  func IsNumericSnowflake(id string) bool
  func resolveChannelSnapshot(s *discordgo.Session, channelID string) (ChannelSnapshot, bool)
  func ResolveEffectiveChannel(s *discordgo.Session, channelID string) (effectiveID string, effectiveName string, isThread bool)
  func extractMessageBody(content string) string
  func ResolveBotRoleIDs(sess *discordgo.Session, guildID string, botUserID string) []string
  func isTier1Wake(m db.Message, botUserID string, botRoleIDs []string, wakeMode string) bool
  ```

- [ ] **Step 1: Create `channel_resolution.go`**
  Transfer channel caching primitives (`channelCacheMu`, `channelCache`, `restSingleFlight`), regex matchers (`aerialExclusionRegex`, `tier1KeywordRegex`), snapshot types (including mandatory `GuildID string`), and resolution functions from `queue.go` to `channel_resolution.go`.
- [ ] **Step 2: Remove moved symbols from `queue.go`**
  Clean up duplicate definitions in `queue.go` ensuring proper imports are maintained.
- [ ] **Step 3: Run package test suite to verify resolution logic**
  Run: `(cd brain && go test -v -run "TestResolve|TestChannel|TestSnowflake|TestTier1" ./pkg/queue/...)`
  Expected: PASS with 0 compilation errors.
- [ ] **Step 4: Verify statement coverage threshold**
  Run: `./scripts/check-coverage.sh --service brain --check`
  Expected: PASS ($\ge 95.0\%$).
- [ ] **Step 5: Commit**
  ```bash
  git add brain/pkg/queue/channel_resolution.go brain/pkg/queue/queue.go
  git commit -m "refactor(queue): extract channel resolution and cache into channel_resolution.go"
  ```

---

### Task 2: Extract Pool Lifecycle & Concurrency State (`pool.go`)

**Files:**
- Create: `brain/pkg/queue/pool.go`
- Modify: `brain/pkg/queue/queue.go`
- Test: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Produces:
  ```go
  const (
      DefaultMaxSessionTurns     = 15
      DefaultTimeoutMinutes      = 60
      DefaultMaxRestarts         = 3
      MaxMessageAbsoluteAge      = 2 * time.Hour
      ContinuationPromptTemplate = "Your previous execution timed out or was interrupted while working. Please inspect where you left off in the conversation transcript and continue the task to completion.\n\nOriginal user request:\n%s"
  )
  type MemoryRetrieverFunc func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error)
  type WorkerPoolConfig struct { ... }
  type threadWorkerState struct {
      ch              chan db.Message
      activeEnqueuers int
      inFlight        bool
  }
  type WorkerPool struct { ... }
  func New(appCfg *config.Config, cfg WorkerPoolConfig) *WorkerPool
  func NewWorkerPool(cfg WorkerPoolConfig) *WorkerPool
  func (p *WorkerPool) SetDiscordSession(s *discordgo.Session)
  func (p *WorkerPool) Classifier() *classifier.Classifier
  func (p *WorkerPool) getDiscordSession() *discordgo.Session
  func (p *WorkerPool) DiscordSession() *discordgo.Session
  func (p *WorkerPool) UpdateRuntimeConfig(model string)
  func (p *WorkerPool) GetRuntimeConfig() string
  func (p *WorkerPool) SessionManager() *session.Manager
  func (p *WorkerPool) Start()
  func (p *WorkerPool) StopWithTimeout(drainTimeout time.Duration)
  func (p *WorkerPool) Stop()
  ```

- [ ] **Step 1: Create `pool.go`**
  Move pool constants, config structs, `WorkerPool` type definition, constructor functions (`New`, `NewWorkerPool`), accessor methods, and lifecycle methods (`Start`, `StopWithTimeout`, `Stop`) from `queue.go` to `pool.go`. Move `sanitizeErrorText` to `pool.go` (or `worker.go`) so `New()` has immediate access.
  Preserve `StopWithTimeout` Stage 1 draining logic inspecting `state.inFlight`, Stage 2 cancellation, and Stage 3 detached DB sweep.
- [ ] **Step 2: Remove moved symbols from `queue.go`**
  Prune moved structs and methods from `queue.go`.
- [ ] **Step 3: Run pool lifecycle tests**
  Run: `(cd brain && go test -v -run "TestWorkerPool|TestPool|TestStop" ./pkg/queue/...)`
  Expected: PASS.
- [ ] **Step 4: Verify statement coverage threshold**
  Run: `./scripts/check-coverage.sh --service brain --check`
  Expected: PASS ($\ge 95.0\%$).
- [ ] **Step 5: Commit**
  ```bash
  git add brain/pkg/queue/pool.go brain/pkg/queue/queue.go
  git commit -m "refactor(queue): extract pool definition and lifecycle methods into pool.go"
  ```

---

### Task 3: Extract Thread Worker Inbound Dispatch & Burst Aggregation (`burst.go`)

**Files:**
- Create: `brain/pkg/queue/burst.go`
- Modify: `brain/pkg/queue/queue.go`
- Test: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Produces:
  ```go
  func (p *WorkerPool) Enqueue(msg db.Message)
  func (p *WorkerPool) runThreadWorker(threadID string, state *threadWorkerState)
  func CoalesceBurstPrompt(burst []db.Message) string
  ```

- [ ] **Step 1: Create `burst.go`**
  Move `Enqueue`, `runThreadWorker`, and `CoalesceBurstPrompt` from `queue.go` to `burst.go`.
  Preserve the exact worker coordination protocol verbatim:
  - 100-capacity channel `state.ch`.
  - Inbound fast-path send under `p.mu` and slow-path `state.activeEnqueuers` backpressure fallback.
  - Non-blocking `DrainLoop` collecting up to 5 messages.
  - Idle worker eviction timer (`idleTimeout = 30s`) checking `activeEnqueuers == 0 && len(state.ch) == 0`.
  - Setting `state.inFlight = true` under `p.mu` before invoking `p.processBurst(burst)` and `state.inFlight = false` after.
  - Zero synthetic debounce timers or burst tickers are introduced.
- [ ] **Step 2: Remove moved symbols from `queue.go`**
  Prune `Enqueue`, `runThreadWorker`, and `CoalesceBurstPrompt` from `queue.go`.
- [ ] **Step 3: Run burst aggregation unit tests**
  Run: `(cd brain && go test -v -run "TestEnqueue|TestBurst|TestDebounce|TestQueueBurstCoalescing" ./pkg/queue/...)`
  Expected: PASS.
- [ ] **Step 4: Verify statement coverage threshold**
  Run: `./scripts/check-coverage.sh --service brain --check`
  Expected: PASS ($\ge 95.0\%$).
- [ ] **Step 5: Commit**
  ```bash
  git add brain/pkg/queue/burst.go brain/pkg/queue/queue.go
  git commit -m "refactor(queue): extract thread worker and burst debouncing into burst.go"
  ```

---

### Task 4: Extract Interrupted Startup Recovery & Crash Heuristics (`recovery.go`)

**Files:**
- Create: `brain/pkg/queue/recovery.go`
- Modify: `brain/pkg/queue/queue.go`
- Test: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Produces:
  ```go
  const (
      MaxHardRestarts            = 5
      CrashLoopVelocityThreshold = 30 * time.Second
  )
  func RecoverInterrupted(database *sql.DB, pool *WorkerPool)
  ```

- [ ] **Step 1: Create `recovery.go`**
  Move `RecoverInterrupted`, orphaned schedule reconciliation invocations (`db.ReconcileOrphanedScheduleRuns`), poison pill detection heuristics, and crash loop velocity checks to `recovery.go`.
  Declare `MaxHardRestarts = 5` and `CrashLoopVelocityThreshold = 30 * time.Second` as package-level constants.
  Preserve the strict sequence: schedule reconciliation executes synchronously before querying pending/processing messages.
- [ ] **Step 2: Remove `RecoverInterrupted` from `queue.go`**
- [ ] **Step 3: Run recovery and poison pill tests**
  Run: `(cd brain && go test -v -run "TestRecoverInterrupted|TestPoisonPill|TestCrashLoop" ./pkg/queue/...)`
  Verify all 10 recovery test suites:
  - `TestRecoverInterrupted`
  - `TestRecoverInterruptedPoisonPill`
  - `TestRecoverInterrupted_ReconcilesOrphanedScheduleRuns`
  - `TestRecoverInterrupted_PreservesExecutionRetryBudget`
  - `TestRecoverInterrupted_RestartPoisonPill_Boundaries`
  - `TestRecoverInterrupted_LongRunningTask_DeploymentVelocityProtection`
  - `TestRecoverInterrupted_PoisonPill_HardCeiling`
  - `TestRecoverInterrupted_DiscordSessionDeliversPoisonNotice`
  - `TestRecoverInterrupted_CoverageEdgeCases`
  - `TestRecoverInterrupted_AllBranches`
  Expected: PASS.
- [ ] **Step 4: Verify statement coverage threshold**
  Run: `./scripts/check-coverage.sh --service brain --check`
  Expected: PASS ($\ge 95.0\%$).
- [ ] **Step 5: Commit**
  ```bash
  git add brain/pkg/queue/recovery.go brain/pkg/queue/queue.go
  git commit -m "refactor(queue): extract crash recovery and startup reconciliation into recovery.go"
  ```

---

### Task 5: Modularize `processBurst` into Worker Pipeline (`worker.go`) & Retire Monolith `queue.go`

**Files:**
- Create: `brain/pkg/queue/worker.go`
- Modify: `brain/pkg/queue/queue.go` (strip to package doc comments or remove)
- Test: `brain/pkg/queue/queue_test.go`, `brain/pkg/queue/coverage_boost_test.go`

**Interfaces:**
- Produces:
  ```go
  type wakeInfo struct {
      isWake    bool
      score     float64
      threshold float64
      reason    string
  }
  type turnExecution struct {
      pool             *WorkerPool
      burst            []db.Message
      threadID         string
      triggerType      string
      execStart        time.Time
      currentSessionID string
      turnCount        int
      policy           config.ChannelPolicy
      effectiveID      string
      effectiveName    string
      isThread         bool
      skipDiscord      bool
      wakeIdx          int
      wakeInfos        []wakeInfo
      trailingMsgs     []db.Message
      trailingInfos    []wakeInfo
      turnPrompt       string
      isQuotaPaused    bool
      statusUpdater    *StatusUpdater
      stopTyping       func()
  }
  func parseDBTime(val any) (time.Time, bool)
  func GetSessionLastActivity(database db.DBTX, threadID string, mgr ...*session.Manager) (time.Time, bool, error)
  func sanitizeErrorText(errStr string) string
  func isRateLimitError(errDetail string) bool
  var rateLimitKeywords = []string{ ... }
  func (p *WorkerPool) processBurst(burst []db.Message)
  ```

- Pipeline Methods on `*turnExecution`:
  ```go
  func (te *turnExecution) claimAndFilterStale() bool
  func (te *turnExecution) resolveTurnPolicy() bool
  func (te *turnExecution) evaluateAmbientWake() (shouldExit bool)
  func (te *turnExecution) handleTrailing()
  func (te *turnExecution) buildTurnPrompt()
  func (te *turnExecution) executeWithRetries()
  ```

- [ ] **Step 1: Create `worker.go` with `turnExecution` context structure**
  - Anchor `scopeLock.Lock()`, `defer scopeLock.Unlock()`, master panic `recover()` defer, and `defer te.handleTrailing()` at the top-level `processBurst` orchestrator.
  - Implement `te.claimAndFilterStale()`: claim pending messages via `Store.ClaimPendingMessage` and validate `MaxMessageAbsoluteAge` & `stalenessTTL` against session activity.
  - Implement `te.resolveTurnPolicy()`: resolve channel snapshot, identify `http-client` callers, evaluate ignored policy, and return `shouldExit == true` on ignored channels.
  - Implement `te.evaluateAmbientWake()`: tier-1 pre-scan, heuristic skipping, ambient burst classification (`Classifier.ClassifyBurst`), ambient telemetry recording, and trailing message partitioning.
  - Implement `te.handleTrailing()`: run in defer scope at the end of `processBurst`, correctly observing `te.isQuotaPaused` to suppress re-enqueueing on active quota lockouts.
  - Implement `te.buildTurnPrompt()`: cold-start thread history summarization (`SummarizeThreadHistory`), watermark caching, channel history lookback formatting, semantic memory retrieval (`MemoryRetrieverFunc`), and channel instructions injection (`LoadChannelInstructions`).
  - Implement `te.executeWithRetries()`: retry loop up to `maxAttempts`, quota lockout detection & one-shot scheduling (setting `te.isQuotaPaused = true`), runner execution with watchdog options, error classification, response parsing, media attachment sanitization, Discord delivery, turn token recording, session rotation, and graceful shutdown reset.
  - Ensure graceful shutdown invariant: whenever `p.ctx.Err() != nil`, all claimed in-flight messages are reset to `db.StatusPending` with `"interrupted by graceful deployment"`.
  - Relocate `rateLimitKeywords` array and `isRateLimitError` function.
- [ ] **Step 2: Clean up or retire `queue.go`**
  Retain `queue.go` containing only package documentation, or remove it entirely if domain files cover the whole package cleanly.
- [ ] **Step 3: Run comprehensive queue test suite**
  Run: `(cd brain && go test -v ./pkg/queue/...)`
  Expected: PASS with 100% of existing tests green.
- [ ] **Step 4: Verify statement coverage threshold**
  Run: `./scripts/check-coverage.sh --service brain --check`
  Expected: PASS ($\ge 95.0\%$).
- [ ] **Step 5: Commit**
  ```bash
  git add brain/pkg/queue/worker.go brain/pkg/queue/queue.go
  git commit -m "refactor(queue): decompose processBurst into pipeline stages in worker.go"
  ```

---

### Task 6: Monorepo Integration & Strict Coverage Verification

**Files:**
- Test: Full monorepo

- [ ] **Step 1: Execute package test suites with race detector**
  Run: `(cd brain && go test -race -cover ./pkg/queue/...)`
  Expected: PASS with 0 data races.
- [ ] **Step 2: Execute monorepo coverage verification**
  Run: `./scripts/check-coverage.sh --service brain --check`
  Expected: PASS, statement coverage $\ge 95.0\%$.
- [ ] **Step 3: Execute `./scripts/verify.sh`**
  Run: `./scripts/verify.sh`
  Expected: All builds, linters, and tests pass cleanly.
- [ ] **Step 4: Final validation commit**
  ```bash
  git commit --allow-empty -m "chore(queue): confirm modular architecture refactor passes 95% coverage gate"
  ```
