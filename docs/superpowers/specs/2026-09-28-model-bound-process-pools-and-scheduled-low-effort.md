# Model-Bound Process Pools & Scheduled Low-Effort Execution Design

## 1. Executive Summary & Goals

This specification formalizes the architectural separation of process pools by model and establishes low effort as the universal default execution tier for all scheduled operations across Aerial.

Prior to this design, `UnifiedProcessPool` attempted to multiplex multiple models within a single pool via a static `TargetModels map[string]string` table while defaulting dynamic keys (such as Discord thread IDs) to `DefaultModel`. This caused all scheduled cron turns and one-shot reminders running in Discord threads to execute on the primary high-effort model (`gemini-3.7-pro`), dropping the worker's resolved `lowEffortModel` before process execution while falsely recording low-effort telemetry in Prometheus and the database. Furthermore, one-shot reminders (`schedule_once`) lacked an `effort` field entirely, and recurring crons (`schedule_recurring`) defaulted to high effort.

This design achieves five primary architectural goals:
- **Homogeneous Model-Bound Process Pools**: Every `UnifiedProcessPool` instance is bound to a single immutable model (via `PoolConfig.Model`), matching its bound `GeminiHomeDir`. Internal multi-model multiplexing (`TargetModels` and `DefaultModel`) is completely eliminated.
- **Dual Process Pool Routing in Discord**: `brain/main.go` instantiates dual process pools for Discord (`discordPrimaryPool` using `cur.Model` and `discordLowEffortPool` using `cur.LowEffortModel`). `WorkerPool` selectively routes incoming turns to the appropriate pool based on turn effort.
- **Subprocess Lifecycle & Zombie Reaping Hardening**: Daemon subprocess lifecycles are decoupled from transient caller request contexts, preventing accidental `SIGKILL` on turn completion. Process exit reaping via `handle.Wait()` is strictly mandated to eliminate zombie (`<defunct>`) processes.
- **Dual-Pool Session Isolation & Anti-Flapping**: Discord threads running scheduled low-effort turns maintain independent transcripts without corrupting or ping-ponging the primary conversation's `sessions` table record.
- **Universal Low-Effort Scheduled Default**: All scheduled operations—both recurring cron routines (`schedule_recurring`) and one-shot reminders/PR checks (`schedule_once`)—default to `"low"` effort across `scheduler-mcp`, SQLite/Postgres schemas, and event dispatch.

---

## 2. Architectural Overview & Pool Topology

### Process Pool Singletons

The system instantiates three model-bound singleton process pools in `brain/main.go`:
- **`discordPrimaryPool`**:
  - Runtime Home: `filepath.Join(dataDir, "runtimes", "discord")`
  - Model: `cur.Model` (e.g. `gemini-3.7-pro`)
  - Target Audience: User-interactive Discord threads and channels requiring high-effort reasoning.
- **`discordLowEffortPool`**:
  - Runtime Home: `filepath.Join(dataDir, "runtimes", "discord")`
  - Model: `cur.LowEffortModel` (e.g. `gemini-3.8-flash-low`)
  - Pre-warmed Targets: `[]string{"ephemeral:classifier", "ephemeral:summarizer"}`
  - Target Audience: Low-effort Discord turns, ambient classifier, thread history summarizer, and scheduled routines.
- **`voicePool`**:
  - Runtime Home: `filepath.Join(dataDir, "runtimes", "voice")`
  - Model: `cur.LowEffortModel` (e.g. `gemini-3.8-flash-low`)
  - Pre-warmed Targets: `[]string{"kiosk"}`
  - Target Audience: All voice kiosk interactions and voice session streams.

```
                           +----------------------------------------+
                           |           Incoming Requests            |
                           +----------------------------------------+
                                        |              |
                    Discord Interactive |              | Scheduled / Low-Effort / Voice
                                        v              v
               +----------------------------+   +-------------------------------+
               | WorkerPool (Standard)      |   | WorkerPool (Low Effort)       |
               +----------------------------+   +-------------------------------+
                            |                                  |
                            v                                  v
             +------------------------------+   +-------------------------------+
             | discordPrimaryPool           |   | discordLowEffortPool          |
             | - Home: runtimes/discord     |   | - Home: runtimes/discord      |
             | - Model: gemini-3.7-pro      |   | - Model: gemini-3.8-flash-low |
             +------------------------------+   +-------------------------------+
                                                               |
                                            Voice Turns        v
                                            +-----------------------------------+
                                            | voicePool                         |
                                            | - Home: runtimes/voice            |
                                            | - Model: gemini-3.8-flash-low     |
                                            +-----------------------------------+
```

---

## 3. Subsystem Detailed Specifications

### 3.1. Runner Subsystem (`brain/pkg/runner`)

#### Configuration Changes in `unified_pool.go`
- In `PoolConfig`:
  - Replace `DefaultModel string` with `Model string` (required).
  - Delete `TargetModels map[string]string`.
  - For backward compatibility during migration, if `Model` is empty, fallback to `DefaultModel`. If both are empty, return an error on `NewUnifiedProcessPool`.
- In `UnifiedProcessPool`:
  - Add pool lifecycle context `ctx context.Context` and `cancel context.CancelFunc` initialized in `NewUnifiedProcessPool` to bound long-lived daemon subprocesses.
  - Add method `Model() string` returning `p.cfg.Model`.
  - In `GetOrCreate(ctx context.Context, targetKey string)`:
    - Eliminate all `TargetModels` and `DefaultModel` resolution logic.
    - Set `daemonCfg.Model = p.cfg.Model`.
    - Every spawned `StreamingDaemon` is started with `p.cfg.Model`.
    - Pass pool lifecycle context `p.ctx` to `StartStreamingDaemon` for process lifetime bounding, while caller's `ctx` bounds only the startup handshake timeout.
  - In `Close()`:
    - Cancel pool lifecycle context `p.cancel()`.
    - Close daemons concurrently using `sync.WaitGroup` to avoid sequential head-of-line blocking on shutdown.

#### Subprocess Lifecycle & Spawner Hardening in `spawner.go`
- Context Decoupling in `DefaultDaemonSpawner.Spawn`:
  - `exec.CommandContext(ctx, ...)` must NOT be bound to a transient request or turn context.
  - Pass the pool's long-lived lifecycle context to `exec.CommandContext` (or manage explicit termination on pool shutdown), ensuring that when a single turn context cancels or times out, Go does not send `SIGKILL` to the persistent daemon subprocess.
  - Caller's handshake context strictly bounds the initial ready signal read loop in `StartStreamingDaemon`.
- Process Handle Reaping in `streaming_daemon.go`:
  - In `StreamingDaemon.Close()`:
    - After calling `d.handle.Kill()`, explicitly call `d.handle.Wait()` (or await completion in a supervised reaper goroutine) to reap child process exit status and avoid `<defunct>` zombie process accumulation in Linux containers.

#### Testing in `brain/pkg/runner`
- Update unit tests in `unified_pool_test.go` to construct pools with `Model: "model-name"`.
- Verify `p.Model()` returns the configured model.
- Verify daemons spawned under any `targetKey` inherit `p.cfg.Model`.
- Verify concurrent `Close()` terminates daemons cleanly without deadlocks.
- Verify zombie reaping on daemon termination.

---

### 3.2. Queue Subsystem (`brain/pkg/queue`)

#### WorkerPool Configuration in `pool.go`
- In `WorkerPoolConfig`:
  - Retain `ProcessPool *runner.UnifiedProcessPool` (represents the primary high-effort Discord pool).
  - Add `LowEffortProcessPool *runner.UnifiedProcessPool` (represents the low-effort Discord pool).
  - Retain `VoiceProcessPool *runner.UnifiedProcessPool` (represents the voice pool).
- In `WorkerPool`:
  - Add field `lowEffortProcessPool *runner.UnifiedProcessPool`.
  - Add accessor method `LowEffortProcessPool() *runner.UnifiedProcessPool`.
  - In `MarkDirty()`:
    - Notify `p.processPool.MarkDirty()`, `p.lowEffortProcessPool.MarkDirty()`, and `p.voiceProcessPool.MarkDirty()`.
  - In `StopWithTimeout(drainTimeout time.Duration)`:
    - Preserve queue draining in Stage 1 without prematurely closing pools.
  - In `Stop()`:
    - Gracefully close `p.processPool`, `p.lowEffortProcessPool`, and `p.voiceProcessPool`.

#### Turn Routing in `worker.go`
- Model and Effort Resolution:
  - Worker computes `isLowEffort` by inspecting burst messages for `strings.EqualFold(m.Effort, "low")`.
  - Sets `currentModel = lowEffortModel` if `isLowEffort`, otherwise `cur.Model`.
- Process Pool Selection:
  - Select active pool:
    ```go
    activePool := te.pool.processPool
    if isLowEffort && te.pool.lowEffortProcessPool != nil {
        activePool = te.pool.lowEffortProcessPool
    }
    ```
  - Fail-safe fallback: If `te.pool.lowEffortProcessPool` is nil (e.g. in legacy tests), log structured warning and fall back safely to `te.pool.processPool`.
- Daemon Acquisition and Execution:
  - `daemon, daemonErr := activePool.GetOrCreate(runCtx, te.threadID)`
  - Execute turn via `daemon.Send(promptToSend, turnCtx)`.
- Session ID Coordination & Anti-Flapping:
  - When executing on `lowEffortProcessPool` for a thread that has an existing primary session, the low-effort daemon maintains its own session ID and transcript locally.
  - To prevent database session ID flapping and race conditions with active interactive conversations, `te.saveSessionID(te.threadID, sessionID)` must NOT overwrite the primary `sessions` table record when running a low-effort scheduled turn on a thread with an existing primary session.
  - The low-effort session ID is recorded in message metadata and schedule run records.
- Error and Quota Rotation:
  - When rotation is triggered by quota pause or session reset:
    - Call `RotateDaemon` on `activePool.RotateDaemon(context.Background(), te.threadID, "")`.
- Quota Retry Effort Preservation:
  - In `worker.go` quota pause handling, when creating `db.OneShotSchedule` for an auto-retry, explicitly preserve the active turn's effort:
    ```go
    retryEffort := "high"
    if isLowEffort {
        retryEffort = "low"
    }
    oneShot := db.OneShotSchedule{
        ID:        oneShotID,
        ThreadID:  te.threadID,
        Prompt:    retryPrompt,
        RunAt:     runAt,
        CreatedAt: time.Now().UTC(),
        Effort:    retryEffort,
    }
    ```
- Yield Trap & Eviction Protection:
  - In `burst.go` (idle eviction loop), inspect BOTH pools before evicting thread workers:
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
  - In `worker.go` yield-trap detection:
    - Check `activePool` for active tasks.
    - If `activePool` has zero active tasks, check the alternate pool as fallback.
    - Latch the specific daemon that owns the active tasks (`trackerDaemon`) to ensure task status and logs resolve to the correct session.

---

### 3.3. Application Entrypoint (`brain/main.go`)

- Pool Construction:
  - Construct `discordPrimaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{ GeminiHomeDir: discordHome, Model: cur.Model, ... })`.
  - Construct `discordLowEffortPool := runner.NewUnifiedProcessPool(runner.PoolConfig{ GeminiHomeDir: discordHome, Model: lowEffortModel, PrewarmedTargets: []string{"ephemeral:classifier", "ephemeral:summarizer"}, ... })`.
  - Construct `voicePool := runner.NewUnifiedProcessPool(runner.PoolConfig{ GeminiHomeDir: voiceHome, Model: lowEffortModel, PrewarmedTargets: []string{"kiosk"}, ... })`.
- Component Wiring:
  - Pass `discordLowEffortPool` to `classifier.New(..., classifier.WithProcessPool(discordLowEffortPool))`.
  - Pass `discordLowEffortPool.EphemeralLLMFunc("ephemeral:summarizer")` to `scheduler.WithLLMFunc(...)`.
  - Inject both `ProcessPool: discordPrimaryPool` and `LowEffortProcessPool: discordLowEffortPool` into `WorkerPoolConfig`.
- Teardown:
  - Defer close calls on all three pools in reverse initialization order.

---

### 3.4. Scheduler MCP Service (`scheduler-mcp/`)

#### Tool Signatures and Defaults in `tools.go`
- `schedule_once`:
  - Add `Effort string` to `ScheduleOnceArgs` (`jsonschema:"Effort tier: 'low' (lightweight low-effort model) or 'high' (primary high-effort model). Defaults to 'low'."`).
  - Add `Effort string` to `ScheduleOnceOutput`.
  - Validation / Normalization:
    - Normalize: `effort := strings.ToLower(strings.TrimSpace(args.Effort))`
    - If `effort != "high"` -> default to `"low"`.
    - Persist `sched.Effort = effort`.
- `schedule_recurring`:
  - Update `ScheduleRecurringArgs` validation:
    - Normalize: `effort := strings.ToLower(strings.TrimSpace(args.Effort))`
    - If `effort != "high"` -> default to `"low"`.
    - Persist `sched.Effort = effort`.
- `update_cron_schedule`:
  - Retain ability to switch effort explicitly between `"low"` and `"high"`.
- `OneShotSchedule` Struct in `db.go`:
  - Add `Effort string` field.

#### Database Persistence in `scheduler-mcp/db.go`
- Schema Definition:
  - SQLite Schema:
    - In `InitDB` / `sqlite_test_fixture_test.go`:
      `CREATE TABLE IF NOT EXISTS one_shot_schedules (id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, prompt TEXT NOT NULL, run_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, effort TEXT NOT NULL DEFAULT 'low');`
      `CREATE TABLE IF NOT EXISTS cron_schedules (..., effort TEXT NOT NULL DEFAULT 'low', ...);`
  - Postgres Schema Migrations:
    - `ALTER TABLE one_shot_schedules ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'low';`
    - `ALTER TABLE one_shot_schedules ALTER COLUMN effort SET DEFAULT 'low';`
    - `ALTER TABLE cron_schedules ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'low';`
    - `ALTER TABLE cron_schedules ALTER COLUMN effort SET DEFAULT 'low';`
- Queries and Scans:
  - In `InsertOneShotSchedule`:
    - Insert `effort` column (`$6` / `?`).
    - If `sched.Effort == ""` or `sched.Effort != "high"`, normalize to `"low"`.
  - In `ListOneShotSchedules`:
    - Query and scan `COALESCE(NULLIF(effort, ''), 'low')`.

---

### 3.5. Database Schema & Storage Subsystem (`brain/pkg/db`)

#### Schema Migrations in `schema.go`
- In `one_shot_schedules` table definition:
  - Add `effort TEXT NOT NULL DEFAULT 'low'`.
- In `cron_schedules` table definition:
  - Change default from `'high'` to `'low'`: `effort TEXT NOT NULL DEFAULT 'low'`.
- In `schedule_runs` table definition:
  - Change default from `'high'` to `'low'`: `effort TEXT NOT NULL DEFAULT 'low'`.
- Postgres schema notices:
  - `ALTER TABLE one_shot_schedules ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'low'`
  - `ALTER TABLE one_shot_schedules ALTER COLUMN effort SET DEFAULT 'low'`
  - `ALTER TABLE cron_schedules ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'low'`
  - `ALTER TABLE cron_schedules ALTER COLUMN effort SET DEFAULT 'low'`
  - `ALTER TABLE schedule_runs ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'low'`
  - `ALTER TABLE schedule_runs ALTER COLUMN effort SET DEFAULT 'low'`

#### Query and Scan Updates in `schedules.go`
- Update `OneShotSchedule` struct:
  - Add `Effort string` field.
- Update `CreateOneShotSchedule`:
  - If `database == nil`, return `fmt.Errorf("database is nil")` (strictly zero swallowed errors).
  - Normalize: if `strings.ToLower(strings.TrimSpace(s.Effort)) != "high"`, set `s.Effort = "low"`.
  - Insert `effort` column (`$6`).
- Update `GetDueOneShotSchedules` and `GetAllOneShotSchedules`:
  - Select `COALESCE(NULLIF(effort, ''), 'low')` and scan into `s.Effort`.
- Update `CreateCronSchedule`:
  - If `database == nil`, return `fmt.Errorf("database is nil")`.
  - Normalize: if `strings.ToLower(strings.TrimSpace(c.Effort)) != "high"`, set `c.Effort = "low"`.
- Update `GetDueCronSchedules` and `GetAllCronSchedules`:
  - Select `COALESCE(NULLIF(effort, ''), 'low')` and scan into `c.Effort`.
- Update `CreateScheduleRun`:
  - If `database == nil`, return `fmt.Errorf("database is nil")`.
  - Normalize: if `strings.ToLower(strings.TrimSpace(run.Effort)) != "high"`, set `run.Effort = "low"`.
- Update `GetScheduleRunsPaginated`:
  - Select `COALESCE(NULLIF(effort, ''), 'low')` and scan into `r.Effort`.

#### SQLite Test Fixtures Updates
- In `brain/pkg/db/sqlite_test_fixture_test.go` and `scheduler-mcp/sqlite_test_fixture_test.go`:
  - Update `sqliteSchema` table definitions for `one_shot_schedules`, `cron_schedules`, and `schedule_runs` with `effort TEXT NOT NULL DEFAULT 'low'`.
  - In migration loops, add `ALTER TABLE one_shot_schedules ADD COLUMN effort TEXT NOT NULL DEFAULT 'low';` ignoring duplicate column errors.

---

### 3.6. Scheduler Event Dispatch (`brain/pkg/scheduler`)

#### Pure Constructor Updates in `pure.go`
- In `BuildOneShotScheduleRun`:
  - Set `Effort: oneShot.Effort`.
- In `BuildOneShotMessage`:
  - Set `Effort: oneShot.Effort`.
- In `BuildCronScheduleRun`:
  - Retain `Effort: cron.Effort` (which now defaults to `"low"`).
- In `BuildCronMessage`:
  - Retain `Effort: cron.Effort` (which now defaults to `"low"`).

---

### 3.7. Automated Scripts (`scripts/aerial-pr.sh`)

- In `scripts/aerial-pr.sh` (function `schedule_pr_followup`):
  - Pass `"effort": "low"` in the JSON payload to `schedule_once` for explicit clarity and defense-in-depth.

---

## 4. Invariants & Guardrails

- **Zero Markdown Tables**: All artifacts, commit messages, code comments, and summaries strictly use bulleted lists.
- **Zero Swallowed Errors**: Every error path in pool acquisition, database scan, spawner failure, and daemon lifecycle must be logged with structured context or propagated.
- **Coverage Floor**: Maintain strictly >= 95.0% statement coverage across all modified packages (`brain/pkg/runner`, `brain/pkg/queue`, `brain/pkg/db`, `brain/pkg/scheduler`, `scheduler-mcp`, and root `brain`).
- **Hermetic Testing**: All unit tests must use in-memory pipes, mock spawners, or SQLite test fixtures without spawning live OS processes or external network calls.
- **Fail-Safe Fallbacks**: If `LowEffortProcessPool` is nil in unit tests, `WorkerPool` falls back safely to `ProcessPool` with structured logging.
- **Clean Zombie Reaping**: All terminated process handles must be waited on to reclaim OS resources and avoid `<defunct>` process leaks.
