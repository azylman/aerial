# Model-Bound Process Pools & Scheduled Low-Effort Execution Design

## 1. Executive Summary & Goals

This specification formalizes the architectural separation of process pools by model and establishes low effort as the default execution tier for all scheduled operations across Aerial.

Prior to this design, `UnifiedProcessPool` attempted to multiplex multiple models within a single pool via a static `TargetModels map[string]string` table while defaulting dynamic keys (such as Discord thread IDs) to `DefaultModel`. This caused all scheduled cron turns and one-shot reminders running in Discord threads to execute on the primary high-effort model (`gemini-3.7-pro`), dropping the worker's resolved `lowEffortModel` before process execution while falsely recording low-effort telemetry in Prometheus and the database. Furthermore, one-shot reminders (`schedule_once`) lacked an `effort` field entirely, and recurring crons (`schedule_recurring`) defaulted to high effort.

This design achieves four primary goals:
- **Homogeneous Model-Bound Process Pools**: Every `UnifiedProcessPool` instance is bound to a single immutable model (via `PoolConfig.Model`), matching its bound `GeminiHomeDir`. Internal multi-model multiplexing (`TargetModels` and `DefaultModel`) is completely eliminated.
- **Dual Process Pool Routing in Discord**: `brain/main.go` instantiates dual process pools for Discord (`discordPrimaryPool` using `cur.Model` and `discordLowEffortPool` using `cur.LowEffortModel`). `WorkerPool` selectively routes incoming turns to the appropriate pool based on turn effort.
- **Universal Low-Effort Scheduled Default**: All scheduled operations—both recurring cron routines (`schedule_recurring`) and one-shot reminders/PR checks (`schedule_once`)—default to `"low"` effort across `scheduler-mcp`, SQLite/Postgres schemas, and event dispatch.
- **Zero Process Mutation**: Daemons in a pool never undergo process-level model restarts or argument mutation. If a thread requires a different effort tier, turns are routed to the corresponding pool cleanly.

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
               +---------------------------+   +-------------------------------+
               | WorkerPool (Standard)     |   | WorkerPool (Low Effort)       |
               +---------------------------+   +-------------------------------+
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
  - Delete `DefaultModel string`.
  - Delete `TargetModels map[string]string`.
  - Add `Model string` (required). If empty, return an error on `NewUnifiedProcessPool` or validation.
- In `UnifiedProcessPool`:
  - Add method `Model() string` returning `p.cfg.Model`.
  - In `GetOrCreate(ctx context.Context, targetKey string)`:
    - Eliminate all `TargetModels` and `DefaultModel` resolution logic.
    - Set `daemonCfg.Model = p.cfg.Model`.
    - Every spawned `StreamingDaemon` is started with `p.cfg.Model`.

#### Testing in `brain/pkg/runner`
- Update unit tests in `unified_pool_test.go` to construct pools with `Model: "model-name"` instead of `DefaultModel` / `TargetModels`.
- Verify `p.Model()` returns the configured model.
- Verify daemons spawned under any `targetKey` inherit `p.cfg.Model`.

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
  - In `Stop()` and `StopWithTimeout(timeout time.Duration)`:
    - Gracefully close `p.processPool`, `p.lowEffortProcessPool`, and `p.voiceProcessPool`.

#### Turn Routing in `worker.go`
- Model and Effort Resolution:
  - Worker computes `isLowEffort` as before by scanning `te.burst` for `strings.EqualFold(m.Effort, "low")`.
  - Sets `currentModel = lowEffortModel` if `isLowEffort`.
- Process Pool Selection:
  - If `isLowEffort && te.pool.lowEffortProcessPool != nil`:
    - `activePool = te.pool.lowEffortProcessPool`
  - Else:
    - `activePool = te.pool.processPool`
- Daemon Acquisition and Execution:
  - `daemon, daemonErr := activePool.GetOrCreate(runCtx, te.threadID)`
  - Turn is executed via `daemon.Send(promptToSend, turnCtx)`.
- Error and Quota Rotation:
  - If rotation is triggered by quota pause or session reset:
    - Rotate on `activePool.RotateDaemon(context.Background(), te.threadID, "")`.
- Yield Trap Detection:
  - Check active tasks on `activePool` first; fall back to checking `processPool` if needed to ensure no background task completion is missed.

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
- `update_cron_schedule`:
  - Retain ability to switch effort explicitly between `"low"` and `"high"`.
- `OneShotSchedule` Struct in `db.go`:
  - Add `Effort string` field.

#### Database Persistence in `scheduler-mcp/db.go`
- In `InitDB`:
  - SQLite: `CREATE TABLE IF NOT EXISTS one_shot_schedules (id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, prompt TEXT NOT NULL, run_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, effort TEXT NOT NULL DEFAULT 'low');`
  - Postgres: Add migration `ALTER TABLE one_shot_schedules ADD COLUMN IF NOT EXISTS effort TEXT NOT NULL DEFAULT 'low';`.
  - Update `cron_schedules` table default: `effort TEXT NOT NULL DEFAULT 'low'`.
- In `InsertOneShotSchedule`:
  - Insert `effort` column.
- In `ListOneShotSchedules`:
  - Query and scan `COALESCE(effort, 'low')`.

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
  - `ALTER TABLE cron_schedules ALTER COLUMN effort SET DEFAULT 'low'`
  - `ALTER TABLE schedule_runs ALTER COLUMN effort SET DEFAULT 'low'`

#### Query and Scan Updates in `schedules.go`
- Update `OneShotSchedule` struct:
  - Add `Effort string` field.
- Update `InsertOneShotSchedule`:
  - Insert `effort` column (`$6`).
- Update `GetDueOneShotSchedules` and `GetAllOneShotSchedules`:
  - Select `COALESCE(effort, 'low')` and scan into `s.Effort`.

---

### 3.6. Scheduler Event Dispatch (`brain/pkg/scheduler`)

#### Pure Constructor Updates in `pure.go`
- In `BuildOneShotMessage`:
  - Map `Effort: oneShot.Effort`.
- In `BuildOneShotScheduleRun`:
  - Map `Effort: oneShot.Effort`.
- In `BuildCronMessage`:
  - Retain `Effort: cron.Effort` (which now defaults to `"low"`).
- In `BuildCronScheduleRun`:
  - Retain `Effort: cron.Effort` (which now defaults to `"low"`).

---

### 3.7. Automated Scripts (`scripts/aerial-pr.sh`)

- In `scripts/aerial-pr.sh` (function `schedule_pr_followup`):
  - Pass `"effort": "low"` in the JSON payload to `schedule_once` for explicit clarity and defense-in-depth.

---

## 4. Invariants & Guardrails

- **Zero Markdown Tables**: All artifacts, commit messages, code comments, and summaries strictly use bulleted lists.
- **Zero Swallowed Errors**: Every error path in pool acquisition, database scan, and daemon lifecycle must be logged with structured context or propagated.
- **Coverage Floor**: Maintain strictly >= 95.0% statement coverage across all modified packages (`brain/pkg/runner`, `brain/pkg/queue`, `brain/pkg/db`, `brain/pkg/scheduler`, `scheduler-mcp`, and root `brain`).
- **Hermetic Testing**: All unit tests must use in-memory pipes, mock spawners, or SQLite test fixtures without spawning live OS processes or external network calls.
- **Fail-Safe Fallbacks**: If `LowEffortProcessPool` is nil in unit tests, `WorkerPool` falls back safely to `ProcessPool` with structured logging.
