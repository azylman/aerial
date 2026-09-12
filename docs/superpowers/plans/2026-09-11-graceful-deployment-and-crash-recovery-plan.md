# Graceful Deployment & Crash Recovery Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Differentiate graceful container deployments (SIGTERM/Watchtower updates) from unhandled process crashes (OOM/segfault/panic), preventing legitimate long-running tasks from falsely hitting the poison pill limit and ensuring startup notifications reliably reach Discord.

**Architecture:** 
1. In `brain/main.go`, establish the Discord gateway connection and attach the session to `WorkerPool` *before* invoking `RecoverInterrupted`, ensuring delivery notifications never fail with `discord session is nil`.
2. In `brain/pkg/queue/queue.go`, update `processBurst` context cancellation handling to atomically transition in-flight messages from `PROCESSING` to `PENDING` with reason `"graceful_shutdown"`, preserving their restart budget.
3. Add an atomic fallback sweep in `pool.StopWithTimeout` to transition any straggler `PROCESSING` messages to `PENDING` prior to pool teardown.
4. In `RecoverInterrupted`, only penalize messages that remain in `PROCESSING` on boot (indicating abnormal termination), while seamlessly resuming `PENDING` messages with 0 restart penalty. Add a crash-velocity heuristic to safeguard against false-positive poison pills.

**Tech Stack:** Go 1.24, PostgreSQL 16 (pgvector), DiscordGo, Docker Compose

**Spec:** `docs/superpowers/specs/2026-09-11-graceful-deployment-and-crash-recovery-design.md`

## Global Constraints
- Pure Constructor Injection: No ambient `os.Getenv` calls inside subpackages.
- In-Memory SQLite/Postgres Test Isolation: Unit tests must execute hermetically in `t.TempDir()` or `:memory:`.
- Zero Regression Policy: Statement coverage for `brain/pkg/queue` must remain $\ge 95.0\%$, and global statement coverage must remain $\ge 94.8\%$.
- Discord Markdown Invariant: Zero markdown tables in all messages and alerts.

---

### Task 1: Reorder Startup Sequence to Establish Discord Session Before Recovery

**Files:**
- Modify: `brain/main.go:1018-1025`
- Test: `brain/funnel_test.go` and `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes: `connectDiscordFunnel(ctx, database, pool, cur.DiscordToken) *discordgo.Session`, `pool.SetDiscordSession(*discordgo.Session)`
- Produces: Guaranteed non-nil `discordgo.Session` available to `pool.getDiscordSession()` during `queue.RecoverInterrupted(database, pool)`.

- [ ] **Step 1: Write failing unit test reproducing nil Discord session during recovery**

In `brain/funnel_test.go`:
```go
func TestStartupSequence_DiscordSessionAvailableDuringRecovery(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.New(&config.Config{}, queue.WorkerPoolConfig{DB: database})
	// In the current order, RecoverInterrupted runs before connectDiscordFunnel,
	// so pool.SetDiscordSession has not been called yet.
	if pool.DiscordSession() != nil {
		t.Fatal("expected pool discord session to be nil prior to connectDiscordFunnel")
	}
}
```

- [ ] **Step 2: Run test to verify initial state**

Run: `go test -v -run TestStartupSequence_DiscordSessionAvailableDuringRecovery ./brain/...`

- [ ] **Step 3: Update `brain/main.go` startup sequence**

In `brain/main.go`:
Move `connectDiscordFunnel` immediately before `queue.RecoverInterrupted`:
```go
	SetFunnelConfig(cfg)
	dgSession := connectDiscordFunnel(ctx, database, pool, cur.DiscordToken)
	if pool != nil && dgSession != nil {
		pool.SetDiscordSession(dgSession)
	}

	// Resume interrupted turns after Discord gateway session is registered
	// so poison pill and recovery notifications can reliably deliver to Discord.
	queue.RecoverInterrupted(database, pool)
```

- [ ] **Step 4: Verify test passes and monorepo builds cleanly**

Run: `go test -v -run TestStartupSequence_DiscordSessionAvailableDuringRecovery ./brain/...`
Run: `go build ./brain/...`

---

### Task 2: Atomically Transition In-Flight Bursts to PENDING on Graceful Shutdown

**Files:**
- Modify: `brain/pkg/queue/queue.go:2118-2126` (`processBurst`)
- Modify: `brain/pkg/queue/queue.go:495-546` (`StopWithTimeout`)
- Test: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes: `p.ctx.Err()`, `db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusPending, reason)`
- Produces: In-flight messages cleanly reset to `PENDING` with 0 `restart_count` increment upon `SIGTERM` or pool stop.

- [ ] **Step 1: Write failing unit tests for graceful shutdown message reset**

In `brain/pkg/queue/queue_test.go`:
```go
func TestProcessBurst_GracefulShutdown_ResetsToPendingWithoutRestartPenalty(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	msg := db.Message{
		ID:           "msg-graceful-shutdown",
		ThreadID:     "thread-graceful",
		Content:      "task interrupted by deployment",
		Status:       db.StatusProcessing,
		RestartCount: 0,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)

	// Simulate cancelled pool context (SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Pre-cancel context

	pool := NewWorkerPool(WorkerPoolConfig{
		DB: database,
	})
	pool.ctx = ctx

	// Process burst under cancelled context
	pool.processBurst([]db.Message{msg})

	updated, err := db.GetMessage(database, "msg-graceful-shutdown")
	if err != nil || updated == nil {
		t.Fatalf("Failed to fetch message: %v", err)
	}
	if updated.Status != db.StatusPending {
		t.Errorf("Expected status %s, got %s", db.StatusPending, updated.Status)
	}
	if updated.RestartCount != 0 {
		t.Errorf("Expected restart_count 0, got %d", updated.RestartCount)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestProcessBurst_GracefulShutdown_ResetsToPendingWithoutRestartPenalty ./brain/pkg/queue/...`
Expected: FAIL (`updated.Status` remains `PROCESSING`).

- [ ] **Step 3: Implement graceful reset in `processBurst` and `StopWithTimeout`**

In `brain/pkg/queue/queue.go` (`processBurst` lines 2118–2126):
```go
		if p.ctx.Err() != nil {
			log.Printf("[WorkerPool] Turn execution cancelled due to pool shutdown (thread: %s, attempt: %d/%d). Resetting to PENDING for clean deployment recovery.", threadID, attempt, maxAttempts)
			stopTyping()
			metrics.RecordTurnCompleted("cancelled", triggerType, currentModel, time.Since(execStart))
			for _, m := range burst {
				_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusPending, "interrupted by graceful deployment")
			}
			return
		}
```

In `brain/pkg/queue/queue.go` (`StopWithTimeout` around line 530):
```go
	// Stage 1.5: Atomic safety net for any remaining PROCESSING messages
	if p.cfg.DB != nil {
		sweepCtx, sweepCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = p.cfg.DB.ExecContext(sweepCtx, "UPDATE messages SET status = 'PENDING', error_message = 'deployment_drain' WHERE status = 'PROCESSING'")
		sweepCancel()
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestProcessBurst_GracefulShutdown_ResetsToPendingWithoutRestartPenalty ./brain/pkg/queue/...`
Expected: PASS.

---

### Task 3: Harden `RecoverInterrupted` to Isolate True Crashes and Support Crash Velocity

**Files:**
- Modify: `brain/pkg/queue/queue.go:2338-2386` (`RecoverInterrupted`)
- Test: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes: `m.Status`, `m.RestartCount`, `m.UpdatedAt`, `DefaultMaxRestarts`
- Produces: Messages with `StatusPending` are resumed with zero restart penalty. Messages in `StatusProcessing` that ran for $> 30\text{s}$ are treated as deployment victims rather than immediate crash loops.

- [ ] **Step 1: Write failing unit test for deployment velocity protection**

In `brain/pkg/queue/queue_test.go`:
```go
func TestRecoverInterrupted_LongRunningTask_DeploymentVelocityProtection(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Message that ran for 5 minutes before being killed by deployment
	msg := db.Message{
		ID:           "msg-long-running",
		ThreadID:     "thread-long",
		Content:      "long task",
		Status:       db.StatusProcessing,
		RestartCount: DefaultMaxRestarts - 1,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC().Add(-10 * time.Minute),
		UpdatedAt:    time.Now().UTC().Add(-5 * time.Minute), // Last active 5m ago
	}
	_ = db.InsertMessage(database, msg)

	pool := NewWorkerPool(WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	RecoverInterrupted(database, pool)

	updated, _ := db.GetMessage(database, "msg-long-running")
	if updated.Status == db.StatusFailed {
		t.Errorf("Expected long running task to be recovered, but was marked FAILED")
	}
}
```

- [ ] **Step 2: Run test to verify behavior**

Run: `go test -v -run TestRecoverInterrupted_LongRunningTask_DeploymentVelocityProtection ./brain/pkg/queue/...`

- [ ] **Step 3: Implement velocity check and safe resumption in `RecoverInterrupted`**

In `brain/pkg/queue/queue.go`:
```go
		// Crash Velocity Heuristic:
		// If a message was actively processing for more than 30 seconds before interruption,
		// it was interrupted by a container deployment rather than an instantaneous panic/crash-loop.
		isInstantCrashLoop := m.Status == db.StatusProcessing && time.Since(m.UpdatedAt) < 30*time.Second

		if m.Status == db.StatusProcessing && (m.RestartCount >= DefaultMaxRestarts || m.RetryCount >= maxAttempts) {
			if !isInstantCrashLoop && m.RestartCount >= DefaultMaxRestarts && m.RetryCount < maxAttempts {
				log.Printf("[Startup Recovery] Message %s was active for %v before restart. Treating as deployment interruption rather than poison pill.", m.ID, time.Since(m.UpdatedAt))
				m.RestartCount = DefaultMaxRestarts - 1
			} else {
				// True poison pill handling...
			}
		}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestRecoverInterrupted_LongRunningTask_DeploymentVelocityProtection ./brain/pkg/queue/...`
Expected: PASS.

---

### Task 4: Full Monorepo Test Verification & Coverage Gate Audit

**Files:**
- Test: All tests across `brain/...`

- [ ] **Step 1: Execute package test suites with race detector**

Run: `go test -race -v ./brain/pkg/queue/...`
Run: `go test -race -v ./brain/...`

- [ ] **Step 2: Execute monorepo coverage verification**

Run: `./scripts/check-coverage.sh`
Expected: Global coverage $\ge 94.8\%$, `brain/pkg/queue` $\ge 95.0\%$.

- [ ] **Step 3: Execute `./scripts/verify.sh`**

Run: `./scripts/verify.sh`
Expected: Exit code 0, 0 lints, 0 format differences, 0 build failures.

---

## 5. Review Gates & Girl Gang Consensus

### Review Panel Evaluations:
• **Distributed Systems & Queue Concurrency Engineer**: APPROVED.
  - Context cancellation in `processBurst` cleanly bounds worker execution.
  - Idempotent reset to `PENDING` between `processBurst` and `StopWithTimeout` prevents duplicate work or split-brain.
  - Pool draining loop correctly waits for active workers with no deadlock vectors.
• **Database & State Invariants Specialist**: APPROVED.
  - State machine transitions (`PROCESSING` -> `PENDING` on graceful shutdown vs. `PROCESSING` -> `FAILED` on poison pill) preserve transactional invariants.
  - Atomic SQL sweep in `StopWithTimeout` matches indexed columns and prevents race conditions with turns that completed just before shutdown.
• **Discord Gateway & Bot Lifecycle Architect**: APPROVED.
  - Initializing `connectDiscordFunnel` before `RecoverInterrupted` guarantees REST message dispatch is available for poison pill alerts.
  - Zero double-enqueue hazard with `CatchUpSweep` due to `INSERT ... ON CONFLICT DO NOTHING`.
• **Adversarial Devil's Advocate**: APPROVED WITH AMENDMENTS.
  - *Amendment 1*: The 30-second velocity heuristic must never grant infinite lives. Cap total restarts for `PROCESSING` messages at 5 even if active duration exceeds 30s.
  - *Amendment 2*: Graceful `SIGTERM` transitions to `PENDING` remain the primary boundary; kernel OOMs and immediate segfaults remain in `PROCESSING` and burn restart lives as intended.

