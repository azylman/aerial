# Model-Bound Process Pools & Scheduled Low-Effort Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish model-bound `UnifiedProcessPool` instances across Aerial, route Discord low-effort turns to a dedicated pool, harden subprocess lifecycles against zombie leaks and accidental SIGKILLs, coordinate dual-pool session isolation, and enforce low effort as the universal default for all incoming scheduled cron routines and one-shot reminders.

**Architecture:** Refactor `UnifiedProcessPool` to bind to a single immutable model per pool instance, instantiating dual Discord pools (`discordPrimaryPool` with `cur.Model` and `discordLowEffortPool` with `cur.LowEffortModel`) alongside `voicePool`. `WorkerPool` dynamically dispatches turns based on resolved effort while isolating low-effort scheduled session pointers to avoid flapping primary conversation records. Schemas and scheduler tool APIs in `scheduler-mcp` and `brain/pkg/db` default all schedules to `"low"` effort.

**Tech Stack:** Go standard library (`sync`, `time`, `os`, `os/exec`, `io`, `bufio`, `encoding/json`, `database/sql`, `golang.org/x/sync/singleflight`), PostgreSQL, SQLite, Bash, PowerShell.

**Spec:** [`docs/superpowers/specs/2026-09-28-model-bound-process-pools-and-scheduled-low-effort.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-09-28-model-bound-process-pools-and-scheduled-low-effort.md)

## Global Constraints

- Pure Go standard library for concurrency, lifecycle, and process management (`sync`, `time`, `os`, `os/exec`, `io`, `bufio`, `encoding/json`, `database/sql`, `golang.org/x/sync/singleflight`).
- Strictly ZERO markdown tables across all artifacts, messages, commits, code comments, and summaries (bulleted lists only).
- Strictly ZERO swallowed errors. Every error path must be logged with structured context or propagated.
- Strictly ZERO host memory inspection (`/proc/meminfo` reading, string parsing, or memory pressure eviction).
- All unit tests must be hermetic and execute in-memory via `io.Pipe()`, mock spawners, or SQLite in-memory fixtures without spawning real OS processes or opening real network listeners in CI.
- Statement coverage floor of strictly >= 95.0% maintained across all modified packages (`brain/pkg/runner`, `brain/pkg/queue`, `brain/pkg/db`, `brain/pkg/scheduler`, `scheduler-mcp`, and root `brain`).
- Use git binary at `C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe` for git operations.
- All staged changes must pass fast pre-commit verification (`powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`).

---

### Task 1: Runner Subsystem - Model-Bound UnifiedProcessPool & Subprocess Lifecycle Hardening

**Files:**
- Modify: `brain/pkg/runner/unified_pool.go:15-185`
- Modify: `brain/pkg/runner/spawner.go:80-115`
- Modify: `brain/pkg/runner/streaming_daemon.go:280-320`
- Test: `brain/pkg/runner/unified_pool_test.go`
- Test: `brain/pkg/runner/spawner_test.go`

**Interfaces:**
- Consumes: `DaemonSpawner`, `DaemonConfig`, `StreamingDaemon`
- Produces: `PoolConfig.Model string`, `UnifiedProcessPool.Model() string`, lifecycle-bound spawner, zombie-reaped `StreamingDaemon.Close()`

- [ ] **Step 1: Write failing unit test for Model-bound UnifiedProcessPool and concurrent Close**

Add `TestUnifiedProcessPool_ModelBound` and `TestStreamingDaemon_CloseReapsChildProcess` in `brain/pkg/runner/unified_pool_test.go`:

```go
func TestUnifiedProcessPool_ModelBound(t *testing.T) {
	mock := NewMockSpawner()
	pool := NewUnifiedProcessPool(PoolConfig{
		Model: "gemini-3.8-flash-low",
	}, mock)
	defer pool.Close()

	if pool.Model() != "gemini-3.8-flash-low" {
		t.Fatalf("expected pool model gemini-3.8-flash-low, got %s", pool.Model())
	}

	ctx := context.Background()
	daemon, err := pool.GetOrCreate(ctx, "target-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if daemon == nil {
		t.Fatalf("expected non-nil daemon")
	}

	mock.mu.Lock()
	spawnCfg := mock.lastSpawnCfg
	mock.mu.Unlock()

	if spawnCfg.Model != "gemini-3.8-flash-low" {
		t.Fatalf("expected spawned daemon model gemini-3.8-flash-low, got %s", spawnCfg.Model)
	}
}

func TestUnifiedProcessPool_ModelFallback(t *testing.T) {
	mock := NewMockSpawner()
	pool := NewUnifiedProcessPool(PoolConfig{
		DefaultModel: "gemini-2.5-pro",
	}, mock)
	defer pool.Close()

	if pool.Model() != "gemini-2.5-pro" {
		t.Fatalf("expected fallback model gemini-2.5-pro, got %s", pool.Model())
	}
}

func TestStreamingDaemon_CloseReapsChildProcess(t *testing.T) {
	mockHandle := &mockReapProcessHandle{}
	daemon := &StreamingDaemon{
		handle: mockHandle,
		state:  StateReady,
	}
	if err := daemon.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !mockHandle.waitCalled {
		t.Fatalf("expected handle.Wait() to be called on Close")
	}
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test -C brain -v ./pkg/runner -run TestUnifiedProcessPool_ModelBound`
Expected: FAIL with `pool.Model undefined` or configuration mismatch.

- [ ] **Step 3: Implement Model-bound configuration, spawner context decoupling, and process reaping**

In `brain/pkg/runner/unified_pool.go`:
- Update `PoolConfig`:
  ```go
  type PoolConfig struct {
      AgyBin           string
      Model            string
      DefaultModel     string // Deprecated: use Model instead. Preserved for backward compatibility.
      Cwd              string
      Env              []string
      GeminiHomeDir    string
      PrewarmedTargets []string
  }
  ```
- In `NewUnifiedProcessPool`:
  ```go
  ctx, cancel := context.WithCancel(context.Background())
  resolvedModel := cfg.Model
  if resolvedModel == "" {
      resolvedModel = cfg.DefaultModel
  }
  cfg.Model = resolvedModel
  cfg.DefaultModel = resolvedModel
  p := &UnifiedProcessPool{
      cfg:     cfg,
      spawner: spawner,
      daemons: make(map[string]*StreamingDaemon),
      ctx:     ctx,
      cancel:  cancel,
  }
  ```
- Add `func (p *UnifiedProcessPool) Model() string { return p.cfg.Model }`.
- In `GetOrCreate`: eliminate `TargetModels` mapping and always set `daemonCfg.Model = p.cfg.Model`. Pass pool lifecycle context `p.ctx` to `StartStreamingDaemon(p.ctx, daemonCfg, p.spawner)`.
- In `Close()`: cancel `p.cancel()`, and close all daemons concurrently using `sync.WaitGroup` under mutex:
  ```go
  p.closeOnce.Do(func() {
      p.mu.Lock()
      p.closed = true
      toClose := p.daemons
      p.daemons = make(map[string]*StreamingDaemon)
      p.mu.Unlock()

      p.cancel()

      var wg sync.WaitGroup
      var errMu sync.Mutex
      var errs []error
      for target, d := range toClose {
          if d != nil {
              wg.Add(1)
              go func(tKey string, daemon *StreamingDaemon) {
                  defer wg.Done()
                  if err := daemon.Close(); err != nil {
                      errMu.Lock()
                      errs = append(errs, fmt.Errorf("error closing daemon %q: %w", tKey, err))
                      errMu.Unlock()
                  }
              }(target, d)
          }
      }
      wg.Wait()
      if len(errs) > 0 {
          p.closeErr = fmt.Errorf("errors closing pool daemons: %v", errs)
      }
  })
  ```

In `brain/pkg/runner/streaming_daemon.go`:
- Update `Close()` to reap the process handle cleanly:
  ```go
  if d.handle != nil {
      if err := d.handle.Kill(); err != nil {
          log.Printf("[StreamingDaemon] Warning: failed to kill process handle: %v", err)
          closeErr = err
      }
      if waitErr := d.handle.Wait(); waitErr != nil {
          log.Printf("[StreamingDaemon] Process wait finished with: %v", waitErr)
      }
  }
  ```

- [ ] **Step 4: Run tests to verify pass and statement coverage >= 95.0%**

Run: `go test -C brain -v -cover ./pkg/runner`
Expected: PASS with >= 95.0% coverage across `brain/pkg/runner`.

- [ ] **Step 5: Commit changes**

Run:
```bash
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/pkg/runner/
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(runner): bind UnifiedProcessPool to immutable model and harden process reaping"
```

---

### Task 2: Database Subsystem - Schemas, Migrations, and Low-Effort Defaults

**Files:**
- Modify: `brain/pkg/db/schema.go:50-180`
- Modify: `brain/pkg/db/schedules.go:10-520`
- Modify: `brain/pkg/db/sqlite_test_fixture_test.go:40-160`
- Test: `brain/pkg/db/schedules_test.go`
- Test: `brain/pkg/db/db_test.go`

**Interfaces:**
- Consumes: PostgreSQL / SQLite connection pool
- Produces: `OneShotSchedule.Effort string`, low-effort default queries using `COALESCE(NULLIF(effort, ''), 'low')`, non-nil error handling on nil database

- [ ] **Step 1: Write failing unit test for one-shot schedule effort and low-effort defaults**

Add `TestOneShotSchedule_EffortDefaultAndCoalesce` in `brain/pkg/db/schedules_test.go`:

```go
func TestOneShotSchedule_EffortDefaultAndCoalesce(t *testing.T) {
	db, err := setupTestDB(t)
	if err != nil {
		t.Fatalf("failed to setup test db: %v", err)
	}

	// 1. Unspecified effort defaults to "low"
	oneShot := OneShotSchedule{
		ID:       "oneshot-default",
		ThreadID: "thread-1",
		Prompt:   "hello test",
		RunAt:    time.Now().UTC().Add(-1 * time.Minute),
	}
	if err := CreateOneShotSchedule(db, oneShot); err != nil {
		t.Fatalf("failed to create one shot: %v", err)
	}

	due, err := GetDueOneShotSchedules(db)
	if err != nil {
		t.Fatalf("failed to get due: %v", err)
	}
	if len(due) == 0 {
		t.Fatalf("expected due schedule, got 0")
	}
	if due[0].Effort != "low" {
		t.Fatalf("expected default effort 'low', got %q", due[0].Effort)
	}

	// 2. Explicit high effort is preserved
	oneShotHigh := OneShotSchedule{
		ID:       "oneshot-high",
		ThreadID: "thread-2",
		Prompt:   "high reasoning",
		RunAt:    time.Now().UTC().Add(-1 * time.Minute),
		Effort:   "high",
	}
	if err := CreateOneShotSchedule(db, oneShotHigh); err != nil {
		t.Fatalf("failed to create high one shot: %v", err)
	}

	all, err := GetAllOneShotSchedules(db, "thread-2")
	if err != nil {
		t.Fatalf("failed to get all: %v", err)
	}
	if len(all) == 0 || all[0].Effort != "high" {
		t.Fatalf("expected explicit effort 'high', got %q", all[0].Effort)
	}

	// 3. Nil DB returns error
	if err := CreateOneShotSchedule(nil, oneShot); err == nil {
		t.Fatalf("expected error on nil database, got nil")
	}
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test -C brain -v ./pkg/db -run TestOneShotSchedule_EffortDefaultAndCoalesce`
Expected: FAIL with `oneShot.Effort undefined` or default mismatch.

- [ ] **Step 3: Implement database migrations, schema definitions, and query normalization**

In `brain/pkg/db/schema.go`:
- Update `one_shot_schedules` table in `postgresSchema`:
  ```sql
  CREATE TABLE IF NOT EXISTS one_shot_schedules (
  	id TEXT PRIMARY KEY,
  	thread_id TEXT NOT NULL,
  	prompt TEXT NOT NULL,
  	run_at TIMESTAMPTZ NOT NULL,
  	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  	effort TEXT NOT NULL DEFAULT 'low'
  );
  ```
- Update `cron_schedules` and `schedule_runs` table defaults in `postgresSchema` to `'low'`.
- In `initSchemaPostgres`, add migrations:
  ```go
  execNotice("ALTER TABLE one_shot_schedules ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'low'")
  execNotice("ALTER TABLE one_shot_schedules ALTER COLUMN effort SET DEFAULT 'low'")
  execNotice("ALTER TABLE cron_schedules ALTER COLUMN effort SET DEFAULT 'low'")
  execNotice("ALTER TABLE schedule_runs ALTER COLUMN effort SET DEFAULT 'low'")
  ```

In `brain/pkg/db/sqlite_test_fixture_test.go`:
- Update `sqliteSchema` table definitions for `one_shot_schedules`, `cron_schedules`, and `schedule_runs` with `effort ... DEFAULT 'low'`.
- In migration block, add:
  ```go
  _, _ = database.Exec("ALTER TABLE one_shot_schedules ADD COLUMN effort TEXT NOT NULL DEFAULT 'low'")
  ```

In `brain/pkg/db/schedules.go`:
- Add `Effort string` to `OneShotSchedule`.
- In `CreateOneShotSchedule`:
  - If `database == nil` -> `return fmt.Errorf("database is nil")`.
  - Normalize: `if strings.ToLower(strings.TrimSpace(s.Effort)) != "high" { s.Effort = "low" }`.
  - Update query: `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6)`.
- In `GetDueOneShotSchedules` and `GetAllOneShotSchedules`:
  - Update query: `SELECT id, thread_id, prompt, run_at, created_at, COALESCE(NULLIF(effort, ''), 'low') FROM one_shot_schedules...`.
  - Scan into `&s.Effort`.
- In `CreateCronSchedule`:
  - If `database == nil` -> `return fmt.Errorf("database is nil")`.
  - Normalize: `if strings.ToLower(strings.TrimSpace(c.Effort)) != "high" { c.Effort = "low" }`.
- In `GetDueCronSchedules` and `GetAllCronSchedules`:
  - Update query: use `COALESCE(NULLIF(effort, ''), 'low')`.
- In `CreateScheduleRun`:
  - If `database == nil` -> `return fmt.Errorf("database is nil")`.
  - Normalize: `if strings.ToLower(strings.TrimSpace(run.Effort)) != "high" { run.Effort = "low" }`.
- In `GetScheduleRunsPaginated`:
  - Update query: use `COALESCE(NULLIF(effort, ''), 'low')`.

- [ ] **Step 4: Run tests to verify pass and statement coverage >= 95.0%**

Run: `go test -C brain -v -cover ./pkg/db`
Expected: PASS with >= 95.0% coverage across `brain/pkg/db`.

- [ ] **Step 5: Commit changes**

Run:
```bash
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/pkg/db/
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(db): default all schedules to low effort and add one_shot_schedules effort column"
```

---

### Task 3: Scheduler MCP Subsystem - Low-Effort Defaults & SQLite/Postgres Persistence

**Files:**
- Modify: `scheduler-mcp/tools.go:10-180`
- Modify: `scheduler-mcp/db.go:30-220`
- Modify: `scheduler-mcp/sqlite_test_fixture_test.go:45-88`
- Test: `scheduler-mcp/tools_test.go`
- Test: `scheduler-mcp/server_test.go`

**Interfaces:**
- Consumes: MCP tool invocations for `schedule_once`, `schedule_recurring`, `update_cron_schedule`
- Produces: `schedule_once` with `Effort` parameter defaulting to `"low"`, `InsertOneShotSchedule` inserting effort

- [ ] **Step 1: Write failing unit test for scheduler-mcp schedule_once effort and default normalization**

Add `TestScheduleOnce_EffortDefaultAndExplicit` in `scheduler-mcp/tools_test.go`:

```go
func TestScheduleOnce_EffortDefaultAndExplicit(t *testing.T) {
	db, err := setupTestDB(t)
	if err != nil {
		t.Fatalf("failed to setup test db: %v", err)
	}

	// 1. Unspecified effort defaults to "low"
	out1, err := handleScheduleOnce(context.Background(), db, ScheduleOnceArgs{
		Prompt:         "remind me",
		RunAt:          time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339),
		ThreadID:       "thread-mcp-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out1.Effort != "low" {
		t.Fatalf("expected effort 'low', got %q", out1.Effort)
	}

	// 2. Explicit "high" effort is preserved
	out2, err := handleScheduleOnce(context.Background(), db, ScheduleOnceArgs{
		Prompt:         "remind high",
		RunAt:          time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339),
		ThreadID:       "thread-mcp-2",
		Effort:         "high",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out2.Effort != "high" {
		t.Fatalf("expected effort 'high', got %q", out2.Effort)
	}
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test -C scheduler-mcp -v -run TestScheduleOnce_EffortDefaultAndExplicit`
Expected: FAIL with `ScheduleOnceArgs.Effort undefined` or output mismatch.

- [ ] **Step 3: Implement scheduler-mcp tool schemas, arguments, and db methods**

In `scheduler-mcp/tools.go`:
- In `ScheduleOnceArgs`:
  ```go
  Effort string `json:"effort,omitempty" jsonschema:"Effort tier: 'low' (lightweight low-effort model) or 'high' (primary high-effort model). Defaults to 'low'."`
  ```
- In `ScheduleOnceOutput`:
  ```go
  Effort string `json:"effort"`
  ```
- In `handleScheduleOnce`:
  ```go
  effort := strings.ToLower(strings.TrimSpace(args.Effort))
  if effort != "high" {
      effort = "low"
  }
  sched := OneShotSchedule{
      // ...
      Effort: effort,
  }
  ```
- In `ScheduleRecurringArgs` and `handleScheduleRecurring`:
  ```go
  effort := strings.ToLower(strings.TrimSpace(args.Effort))
  if effort != "high" {
      effort = "low"
  }
  ```

In `scheduler-mcp/db.go`:
- Add `Effort string` to `OneShotSchedule`.
- In `InitDB` Postgres schema:
  - Add `effort TEXT NOT NULL DEFAULT 'low'` to `one_shot_schedules`.
  - Update `cron_schedules` table default to `'low'`.
  - Add Postgres migrations:
    ```go
    _, _ = conn.ExecContext(ctx, "ALTER TABLE one_shot_schedules ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'low';")
    _, _ = conn.ExecContext(ctx, "ALTER TABLE one_shot_schedules ALTER COLUMN effort SET DEFAULT 'low';")
    _, _ = conn.ExecContext(ctx, "ALTER TABLE cron_schedules ALTER COLUMN effort SET DEFAULT 'low';")
    ```
- In `InsertOneShotSchedule`:
  - Check nil database: `if database == nil { return fmt.Errorf("database is nil") }`.
  - Query: `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES (?, ?, ?, ?, ?, ?)`.
- In `ListOneShotSchedules`:
  - Query: `SELECT id, thread_id, prompt, run_at, created_at, COALESCE(NULLIF(effort, ''), 'low') FROM one_shot_schedules...`.
  - Scan into `&s.Effort`.

In `scheduler-mcp/sqlite_test_fixture_test.go`:
- Update `sqliteSchema` table definitions and add `ALTER TABLE one_shot_schedules ADD COLUMN effort TEXT NOT NULL DEFAULT 'low';`.

- [ ] **Step 4: Run tests to verify pass and statement coverage >= 95.0%**

Run: `go test -C scheduler-mcp -v -cover .`
Expected: PASS with >= 95.0% coverage across `scheduler-mcp`.

- [ ] **Step 5: Commit changes**

Run:
```bash
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add scheduler-mcp/
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(scheduler-mcp): default schedule_once and schedule_recurring to low effort"
```

---

### Task 4: Scheduler Core Subsystem - Event Dispatch & One-Shot Effort Propagation

**Files:**
- Modify: `brain/pkg/scheduler/pure.go:195-240`
- Modify: `brain/pkg/scheduler/scheduler.go:380-450`
- Test: `brain/pkg/scheduler/pure_test.go`
- Test: `brain/pkg/scheduler/scheduler_test.go`

**Interfaces:**
- Consumes: `db.OneShotSchedule`, `db.CronSchedule`
- Produces: `BuildOneShotScheduleRun` with `Effort`, `BuildOneShotMessage` with `Effort`

- [ ] **Step 1: Write failing unit test for one-shot pure constructors and scheduler event dispatch**

Add `TestBuildOneShot_EffortPropagation` in `brain/pkg/scheduler/pure_test.go`:

```go
func TestBuildOneShot_EffortPropagation(t *testing.T) {
	now := time.Now().UTC()
	oneShot := db.OneShotSchedule{
		ID:       "oneshot-1",
		ThreadID: "thread-1",
		Prompt:   "reminder test",
		RunAt:    now,
		Effort:   "low",
	}

	run := BuildOneShotScheduleRun("run-1", "msg-1", oneShot, now)
	if run.Effort != "low" {
		t.Fatalf("expected run.Effort == 'low', got %q", run.Effort)
	}

	msg := BuildOneShotMessage("msg-1", "run-1", oneShot, "[Reminder] reminder test", now)
	if msg.Effort != "low" {
		t.Fatalf("expected msg.Effort == 'low', got %q", msg.Effort)
	}
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test -C brain -v ./pkg/scheduler -run TestBuildOneShot_EffortPropagation`
Expected: FAIL with `run.Effort == ""` or `msg.Effort == ""`.

- [ ] **Step 3: Implement effort propagation in pure.go and scheduler.go**

In `brain/pkg/scheduler/pure.go`:
- In `BuildOneShotScheduleRun`:
  ```go
  return db.ScheduleRun{
      ID:           runID,
      ScheduleID:   oneShot.ID,
      ScheduleType: "one_shot",
      MessageID:    msgID,
      TargetID:     oneShot.ThreadID,
      ThreadID:     oneShot.ThreadID,
      Title:        "One-shot Reminder",
      Prompt:       oneShot.Prompt,
      Status:       "enqueued",
      StartedAt:    now,
      Effort:       oneShot.Effort,
  }
  ```
- In `BuildOneShotMessage`:
  ```go
  return db.Message{
      ID:            msgID,
      ThreadID:      oneShot.ThreadID,
      GuildID:       "scheduled",
      AuthorID:      "scheduler",
      AuthorName:    "Scheduler",
      Content:       oneShot.Prompt,
      Summary:       summary,
      Status:        db.StatusPending,
      ScheduleRunID: runID,
      CreatedAt:     now,
      UpdatedAt:     now,
      Effort:        oneShot.Effort,
  }
  ```

In `brain/pkg/scheduler/scheduler.go`:
- Verify due one-shot message consumption enqueues `msg` with `oneShot.Effort` set.

- [ ] **Step 4: Run tests to verify pass and statement coverage >= 95.0%**

Run: `go test -C brain -v -cover ./pkg/scheduler`
Expected: PASS with >= 95.0% coverage across `brain/pkg/scheduler`.

- [ ] **Step 5: Commit changes**

Run:
```bash
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/pkg/scheduler/
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(scheduler): propagate one-shot effort through schedule runs and enqueued messages"
```

---

### Task 5: Queue Subsystem - Dual-Pool Worker Configuration, Routing & Anti-Flapping

**Files:**
- Modify: `brain/pkg/queue/pool.go:30-650`
- Modify: `brain/pkg/queue/worker.go:1530-2120`
- Modify: `brain/pkg/queue/burst.go:120-145`
- Test: `brain/pkg/queue/unified_pool_wiring_test.go`
- Test: `brain/pkg/queue/worker_test.go`
- Test: `brain/pkg/queue/burst_test.go`

**Interfaces:**
- Consumes: `WorkerPoolConfig.LowEffortProcessPool`, `runner.UnifiedProcessPool`
- Produces: `WorkerPool.LowEffortProcessPool()`, effort-aware pool routing in `worker.go`, dual-pool yield-trap and burst eviction tracking, session anti-flapping

- [ ] **Step 1: Write failing unit test for dual-pool turn routing and burst eviction check**

Add `TestWorkerPool_DualPoolTurnRouting` in `brain/pkg/queue/unified_pool_wiring_test.go`:

```go
func TestWorkerPool_DualPoolTurnRouting(t *testing.T) {
	primarySpawner := runner.NewMockSpawner()
	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer primaryPool.Close()

	lowEffortSpawner := runner.NewMockSpawner()
	lowEffortPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowEffortSpawner)
	defer lowEffortPool.Close()

	wp, err := NewWorkerPool(WorkerPoolConfig{
		ProcessPool:          primaryPool,
		LowEffortProcessPool: lowEffortPool,
	})
	if err != nil {
		t.Fatalf("failed to create worker pool: %v", err)
	}
	defer wp.Stop()

	if wp.LowEffortProcessPool() != lowEffortPool {
		t.Fatalf("expected LowEffortProcessPool to match injected pool")
	}
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test -C brain -v ./pkg/queue -run TestWorkerPool_DualPoolTurnRouting`
Expected: FAIL with `LowEffortProcessPool undefined`.

- [ ] **Step 3: Implement WorkerPool configuration, MarkDirty, Stop, routing, and anti-flapping**

In `brain/pkg/queue/pool.go`:
- In `WorkerPoolConfig`, add `LowEffortProcessPool *runner.UnifiedProcessPool`.
- In `WorkerPool`, add field `lowEffortProcessPool *runner.UnifiedProcessPool` and accessor `func (p *WorkerPool) LowEffortProcessPool() *runner.UnifiedProcessPool`.
- In `MarkDirty()`: call `procPool.MarkDirty()`, `p.lowEffortProcessPool.MarkDirty()`, and `p.voiceProcessPool.MarkDirty()`.
- In `Stop()`: gracefully close `procPool`, `p.lowEffortProcessPool`, and `p.voiceProcessPool`.

In `brain/pkg/queue/burst.go`:
- In idle timer eviction loop (around line 129):
  ```go
  hasActiveTasks := false
  if p.processPool != nil {
      if d, ok := p.processPool.Get(threadID); ok && d != nil && d.TaskTracker().ActiveCount() > 0 {
          hasActiveTasks = true
      }
  }
  if !hasActiveTasks && p.lowEffortProcessPool != nil {
      if d, ok := p.lowEffortProcessPool.Get(threadID); ok && d != nil && d.TaskTracker().ActiveCount() > 0 {
          hasActiveTasks = true
      }
  }
  if hasActiveTasks {
      p.mu.Unlock()
      idleTimer.Reset(idleTimeout)
      continue
  }
  ```

In `brain/pkg/queue/worker.go`:
- Select active pool:
  ```go
  activePool := te.pool.processPool
  if isLowEffort && te.pool.lowEffortProcessPool != nil {
      activePool = te.pool.lowEffortProcessPool
  } else if isLowEffort && te.pool.lowEffortProcessPool == nil {
      log.Printf("[Worker] Notice: lowEffortProcessPool is nil, falling back to processPool for thread %s", te.threadID)
  }
  ```
- Spawn and execute daemon from `activePool`:
  ```go
  daemon, daemonErr := activePool.GetOrCreate(runCtx, te.threadID)
  ```
- Session Anti-Flapping:
  - If `isLowEffort && te.pool.lowEffortProcessPool != nil`, and `te.currentSessionID != ""` from prior interactive turns in `sessions` table, DO NOT call `te.saveSessionID(te.threadID, daemon.SessionID())` to overwrite the primary session. Record the session ID only in the turn's response and schedule run.
- Daemon Rotation:
  - Call `activePool.RotateDaemon(context.Background(), te.threadID, "")`.
- Quota pause auto-retry:
  - Preserve effort: `retryEffort := "high"; if isLowEffort { retryEffort = "low" }; oneShot.Effort = retryEffort`.
- Yield-trap detection:
  - Check `activePool` for active background tasks. If 0, check the alternate pool as fallback, and latch `trackerDaemon`.

- [ ] **Step 4: Run tests to verify pass and statement coverage >= 95.0%**

Run: `go test -C brain -v -cover ./pkg/queue`
Expected: PASS with >= 95.0% coverage across `brain/pkg/queue`.

- [ ] **Step 5: Commit changes**

Run:
```bash
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/pkg/queue/
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(queue): wire dual process pools, route low effort turns, and protect session pointers"
```

---

### Task 6: Application Entrypoint & Script Wiring - End-to-End Dual Pools & PR Script

**Files:**
- Modify: `brain/main.go:1070-1120`
- Modify: `scripts/aerial-pr.sh:65-95`
- Test: `brain/main_test.go`

**Interfaces:**
- Consumes: `runner.NewUnifiedProcessPool`, `WorkerPoolConfig`, `scripts/aerial-pr.sh`
- Produces: Dual Discord pools in `main.go`, explicit `"effort": "low"` in PR followup scheduler payload

- [ ] **Step 1: Write failing unit test for main pool wiring**

In `brain/main_test.go`, add `TestMain_DualProcessPoolWiring`:

```go
func TestMain_DualProcessPoolWiring(t *testing.T) {
	primarySpawner := runner.NewMockSpawner()
	lowEffortSpawner := runner.NewMockSpawner()

	p1 := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer p1.Close()
	p2 := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowEffortSpawner)
	defer p2.Close()

	if p1.Model() != "gemini-3.7-pro" || p2.Model() != "gemini-3.8-flash-low" {
		t.Fatalf("expected distinct models in dual pools")
	}
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test -C brain -v -run TestMain_DualProcessPoolWiring`
Expected: FAIL or verify baseline.

- [ ] **Step 3: Implement main.go pool construction, teardown, and scripts/aerial-pr.sh**

In `brain/main.go`:
- Construct `discordPrimaryPool`:
  ```go
  discordPrimaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
      GeminiHomeDir: discordHome,
      Model:         cur.Model,
      AgyBin:        cur.AgyBin,
      Cwd:           projectRoot,
      Env:           cleanEnv,
  }, defaultSpawner)
  ```
- Construct `discordLowEffortPool`:
  ```go
  discordLowEffortPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
      GeminiHomeDir:    discordHome,
      Model:            lowEffortModel,
      AgyBin:           cur.AgyBin,
      Cwd:              projectRoot,
      Env:              cleanEnv,
      PrewarmedTargets: []string{"ephemeral:classifier", "ephemeral:summarizer"},
  }, defaultSpawner)
  ```
- Pass `discordLowEffortPool` to classifier and summarizer.
- Inject both `ProcessPool: discordPrimaryPool` and `LowEffortProcessPool: discordLowEffortPool` into `WorkerPoolConfig`.
- Defer teardown of `discordLowEffortPool` alongside `discordPrimaryPool` and `voicePool`.

In `scripts/aerial-pr.sh` (`schedule_pr_followup`):
- Add `"effort": "low"` to the JSON payload sent to `schedule_once`.

- [ ] **Step 4: Run tests to verify pass and statement coverage >= 95.0%**

Run: `go test -C brain -v -cover .`
Expected: PASS with >= 95.0% coverage across root `brain`.

- [ ] **Step 5: Commit changes**

Run:
```bash
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/main.go brain/main_test.go scripts/aerial-pr.sh
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(main): construct and wire dual Discord process pools in main and scripts"
```

---

### Task 7: Comprehensive Regression & Statement Coverage Verification

**Files:**
- Test: All packages across repository

- [ ] **Step 1: Run comprehensive unit test suites with race detection**

Run:
```bash
go test -C brain -race -v ./pkg/runner
go test -C brain -race -v ./pkg/db
go test -C scheduler-mcp -race -v .
go test -C brain -race -v ./pkg/scheduler
go test -C brain -race -v ./pkg/queue
go test -C brain -race -v .
```
Expected: PASS with 0 race warnings.

- [ ] **Step 2: Verify statement coverage >= 95.0% floor**

Run:
```bash
go test -C brain -v -cover ./pkg/runner
go test -C brain -v -cover ./pkg/db
go test -C scheduler-mcp -v -cover .
go test -C brain -v -cover ./pkg/scheduler
go test -C brain -v -cover ./pkg/queue
go test -C brain -v -cover .
```
Expected: Statement coverage >= 95.0% across all target packages.

- [ ] **Step 3: Run fast pre-commit verification**

Run: `powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`
Expected: Pass cleanly.

- [ ] **Step 4: Commit any test adjustments or coverage boosters**

Run:
```bash
& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" status
```
If clean, proceed to Whole-Branch Review.
