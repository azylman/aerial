# Dashboard Active Schedules & Execution Telemetry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Active Schedules & Execution Telemetry subsystem across Aerial Brain and Permet HUD Dashboard, providing real-time visibility into recurring crons, one-shot reminders, drift-corrected countdowns, and historical run audit logs.

**Architecture:** A decoupled architecture where SQLite (`/data/aerial.db`) persists active configurations and execution history in `schedule_runs`. Aerial Brain evaluates schedules, manages execution hooks in the worker pool, and exposes cached REST APIs with token sanitization. The Go dashboard proxies endpoints to the frontend, where a declarative multi-tab router renders real-time Permet HUD telemetry.

**Tech Stack:** Go 1.22+ (`modernc.org/sqlite`, `robfig/cron/v3`, `net/http`), Vanilla ES6+ JavaScript, Cyberpunk Permet HUD CSS, SQLite WAL.

**Spec:** [`docs/superpowers/specs/2026-08-30-dashboard-active-schedules-design.md`](file:///share/aerial/docs/superpowers/specs/2026-08-30-dashboard-active-schedules-design.md)

## Global Constraints

- **Zero Plaintext Tokens**: Scrub sensitive keys (`GEMINI_API_KEY`, `DISCORD_BOT_TOKEN`, `GITHUB_PAT`, `HA_TOKEN`, `TOKEN`, `KEY`, `PASSWORD`, `SECRET`) before serialization.
- **Zero Stuck Running Invariant**: Startup crash recovery must reconcile orphaned `running` / `enqueued` runs to `failed`.
- **100% Generic & Domain-Agnostic**: No hardcoded personal names, user handles, or private IDs.
- **SQLite WAL Concurrency**: Preserve `PRAGMA busy_timeout = 5000;` and WAL mode across all transactions.

---

### Task 1: Database Storage & `schedule_runs` Model in Brain

**Files:**
- Modify: `brain/pkg/db/db.go`
- Modify: `brain/pkg/db/db_test.go`

**Interfaces:**
- Consumes: Existing SQLite connection `*sql.DB`
- Produces:
  - `type ScheduleRun struct { ID, ScheduleID, ScheduleType, MessageID, TargetID, ThreadID, Title, Prompt, Status string; StartedAt, CompletedAt *time.Time; DurationMs int64; Error string }`
  - `type UpdateRunParams struct { RunID, MessageID, Status string; CompletedAt time.Time; DurationMs int64; Error string }`
  - `type ScheduleSummaryMetrics struct { TotalActive, CronCount, OneShotCount, TotalRuns24h int; NextRunAt *time.Time; SuccessRate24h float64 }`
  - `CreateScheduleRun(db *sql.DB, run ScheduleRun) error`
  - `UpdateScheduleRunStatus(db *sql.DB, params UpdateRunParams) error`
  - `GetScheduleRunsPaginated(db *sql.DB, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error)`
  - `GetScheduleSummaryMetrics(db *sql.DB) (ScheduleSummaryMetrics, error)`
  - `ReconcileOrphanedScheduleRuns(db *sql.DB) (int64, error)`
  - `PruneScheduleRuns(db *sql.DB, maxCount int, maxAge time.Duration) (int64, error)`

- [ ] **Step 1: Write failing unit tests in `brain/pkg/db/db_test.go`**

```go
func TestScheduleRunsCRUD(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()

    now := time.Now().UTC()
    run := ScheduleRun{
        ID:           "run-test-1",
        ScheduleID:   "cron-123",
        ScheduleType: "cron",
        MessageID:    "msg-123",
        TargetID:     "chan-1",
        ThreadID:     "thread-1",
        Title:        "Morning Brief",
        Prompt:       "Check systems",
        Status:       "enqueued",
        StartedAt:    now,
    }

    if err := CreateScheduleRun(db, run); err != nil {
        t.Fatalf("CreateScheduleRun failed: %v", err)
    }

    // Update status to running then completed
    updateParams := UpdateRunParams{
        RunID:       "run-test-1",
        Status:      "completed",
        CompletedAt: now.Add(5 * time.Second),
        DurationMs:  5000,
        Error:       "",
    }
    if err := UpdateScheduleRunStatus(db, updateParams); err != nil {
        t.Fatalf("UpdateScheduleRunStatus failed: %v", err)
    }

    runs, total, err := GetScheduleRunsPaginated(db, 10, 0, "", "")
    if err != nil || total != 1 || len(runs) != 1 {
        t.Fatalf("GetScheduleRunsPaginated failed: %v (total=%d, len=%d)", err, total, len(runs))
    }
    if runs[0].Status != "completed" || runs[0].DurationMs != 5000 {
        t.Errorf("Unexpected run fields: %+v", runs[0])
    }
}

func TestReconcileOrphanedScheduleRuns(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()

    now := time.Now().UTC()
    _ = CreateScheduleRun(db, ScheduleRun{ID: "run-stuck-1", ScheduleID: "cron-1", ScheduleType: "cron", TargetID: "c1", ThreadID: "t1", Prompt: "p1", Status: "running", StartedAt: now})
    _ = CreateScheduleRun(db, ScheduleRun{ID: "run-stuck-2", ScheduleID: "cron-2", ScheduleType: "cron", TargetID: "c2", ThreadID: "t2", Prompt: "p2", Status: "enqueued", StartedAt: now})
    _ = CreateScheduleRun(db, ScheduleRun{ID: "run-ok-3", ScheduleID: "cron-3", ScheduleType: "cron", TargetID: "c3", ThreadID: "t3", Prompt: "p3", Status: "completed", StartedAt: now})

    reconciled, err := ReconcileOrphanedScheduleRuns(db)
    if err != nil {
        t.Fatalf("ReconcileOrphanedScheduleRuns failed: %v", err)
    }
    if reconciled != 2 {
        t.Errorf("Expected 2 reconciled runs, got %d", reconciled)
    }

    runs, _, _ := GetScheduleRunsPaginated(db, 10, 0, "", "")
    for _, r := range runs {
        if (r.ID == "run-stuck-1" || r.ID == "run-stuck-2") && r.Status != "failed" {
            t.Errorf("Run %s expected status failed, got %s", r.ID, r.Status)
        }
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cd /share/aerial/brain && go test -v -run "TestScheduleRuns|TestReconcileOrphaned" ./pkg/db/...`  
Expected: FAIL (compilation errors: undefined types/functions)

- [ ] **Step 3: Implement data structures, table schemas, and methods in `brain/pkg/db/db.go`**

Implement `ScheduleRun` struct, schema migration in `InitDB`, index creation, `CreateScheduleRun`, `UpdateScheduleRunStatus`, `GetScheduleRunsPaginated`, `GetScheduleSummaryMetrics`, `ReconcileOrphanedScheduleRuns`, and `PruneScheduleRuns`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /share/aerial/brain && go test -v ./pkg/db/...`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/db/db.go brain/pkg/db/db_test.go
git commit -m "feat(brain): add schedule_runs table, CRUD methods, and crash recovery"
```

---

### Task 2: Scheduler Trigger & Worker Pool Lifecycle Hooks

**Files:**
- Modify: `brain/pkg/scheduler/scheduler.go`
- Modify: `brain/pkg/scheduler/scheduler_test.go`
- Modify: `brain/pkg/queue/queue.go`
- Modify: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes: `CreateScheduleRun`, `UpdateScheduleRunStatus`, `ReconcileOrphanedScheduleRuns`, `PruneScheduleRuns`
- Produces: Execution state transitions: `enqueued` (on scheduler trigger) ➔ `running` (when worker claims message) ➔ `completed` / `failed` (turn finish)

- [ ] **Step 1: Write failing tests in `brain/pkg/scheduler/scheduler_test.go` and `brain/pkg/queue/queue_test.go`**

Test that:
1. When cron fires in `ProcessDueSchedules`, a `schedule_runs` entry with status `enqueued` is created and `msg.ScheduleRunID` is populated.
2. When worker pool processes message, status transitions to `running` with execution start time, then `completed` with duration.
3. Panics in worker pool are recovered and marked `failed`.
4. Startup `RecoverInterrupted` calls `ReconcileOrphanedScheduleRuns`.

- [ ] **Step 2: Run tests to verify failure**

Run: `cd /share/aerial/brain && go test -v ./pkg/scheduler/... ./pkg/queue/...`  
Expected: FAIL

- [ ] **Step 3: Implement scheduler and queue lifecycle hooks**

- In `brain/pkg/scheduler/scheduler.go`:
  - Create run record before enqueueing message in `ProcessDueSchedules`.
  - Pass `runID` into `msg.ScheduleRunID`.
  - In `InsertMessageAndConsumeOneShot`, check `RowsAffected()` on `DELETE` and rollback if 0.
  - Run `PruneScheduleRuns` in background once every 24 hours.
- In `brain/pkg/queue/queue.go`:
  - Update `schedule_runs` to `status = 'running'` and start timestamp when claimed.
  - Add `defer recover()` block in worker goroutine to mark run `failed` on panic.
  - On turn completion, record `completed` or `failed` with elapsed `duration_ms`.
  - In `RecoverInterrupted`, call `db.ReconcileOrphanedScheduleRuns(database)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /share/aerial/brain && go test -v ./pkg/scheduler/... ./pkg/queue/...`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/scheduler/scheduler.go brain/pkg/scheduler/scheduler_test.go brain/pkg/queue/queue.go brain/pkg/queue/queue_test.go
git commit -m "feat(brain): integrate schedule execution lifecycle hooks and crash recovery"
```

---

### Task 3: Brain REST Endpoints (`GET /schedules` & `GET /schedules/runs`) with Token Redaction & Caching

**Files:**
- Modify: `brain/main.go`
- Modify: `brain/main_test.go`

**Interfaces:**
- Consumes: `db.GetScheduleSummaryMetrics`, `db.GetAllCronSchedules`, `db.GetAllOneShotSchedules`, `db.GetScheduleRunsPaginated`
- Produces: HTTP endpoints `GET /schedules` and `GET /schedules/runs` with 5s memory cache and token sanitization.

- [ ] **Step 1: Write failing tests in `brain/main_test.go`**

```go
func TestSchedulesEndpoints(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()

    // Test /schedules JSON structure & token sanitization
    // Test /schedules/runs pagination and filtering
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cd /share/aerial/brain && go test -v -run TestSchedulesEndpoints .`  
Expected: FAIL

- [ ] **Step 3: Implement `handleSchedules` and `handleScheduleRuns` in `brain/main.go`**

Implement:
- `handleSchedules(database *sql.DB)`: Generates summary metrics, crons, one-shots with 5s TTL memory cache. Includes `cron_description` formatted in Go.
- `handleScheduleRuns(database *sql.DB)`: Returns paginated runs with query params `limit`, `offset`, `schedule_id`, `status`.
- Pass all prompts and errors through `SanitizeString(str)`.
- Register routes in `mux`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /share/aerial/brain && go test -v ./...`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/main.go brain/main_test.go
git commit -m "feat(brain): add /schedules and /schedules/runs REST endpoints with caching and token redaction"
```

---

### Task 4: Dashboard Backend Proxy Handlers & Fallbacks

**Files:**
- Modify: `dashboard/main.go`
- Modify: `dashboard/main_test.go`

**Interfaces:**
- Consumes: Brain REST endpoints
- Produces: Proxy handlers for `/api/schedules`, `/dashboard/api/schedules`, `/api/schedules/runs`, `/dashboard/api/schedules/runs` with fallback payload when Brain is unavailable.

- [ ] **Step 1: Write failing unit tests in `dashboard/main_test.go`**

Test proxy routing, parameter forwarding (`limit`, `offset`), and degraded fallback responses.

- [ ] **Step 2: Run tests to verify failure**

Run: `cd /share/aerial/dashboard && go test -v -run TestSchedulesProxy .`  
Expected: FAIL

- [ ] **Step 3: Implement proxy handlers in `dashboard/main.go`**

Implement `schedulesHandler(brainURL)` and `scheduleRunsHandler(brainURL)` with `brainHTTPClient` (4s timeout) and fallback JSON. Register routes in `mux`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /share/aerial/dashboard && go test -v ./...`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dashboard/main.go dashboard/main_test.go
git commit -m "feat(dashboard): add /api/schedules proxy endpoints with graceful error fallback"
```

---

### Task 5: Frontend HUD UI, Declarative Router & Live Drift-Corrected Tickers

**Files:**
- Modify: `dashboard/static/index.html`
- Modify: `dashboard/static/app.js`
- Modify: `dashboard/static/style.css`

**Interfaces:**
- Consumes: `/api/schedules`, `/api/schedules/runs`
- Produces: Gundam Aerial Permet HUD active schedules view, summary metrics, drift-corrected countdown engine, and execution audit feed.

- [ ] **Step 1: Update `dashboard/static/index.html`**

- Add `<button class="nav-btn" id="tab-schedules-btn" role="tab" aria-selected="false" aria-controls="schedules-view"><span class="btn-glow"></span><span class="btn-text">⏰ SCHEDULES</span></button>` in header nav.
- Add `<div id="schedules-view" class="view-panel" role="tabpanel" style="display: none;">` with:
  - Summary metric bar (Active Crons, Pending Once, Next Upcoming Trigger, 24h Execution Success Rate).
  - Search & filter toolbar with refresh button.
  - Active Schedules grid container (`#schedules-grid`).
  - Execution Run Audit feed container (`#runs-feed-container`).

- [ ] **Step 2: Implement declarative router, drift-corrected tickers & state in `dashboard/static/app.js`**

- Refactor `setupTabs()` to declarative `TABS` router.
- Implement `fetchSchedules()` and `fetchScheduleRuns()`.
- Implement `startSchedulesTicker()` with clock skew calculation against `system_time` and countdown clamping (`⚡ TRIGGERING...`).
- Implement pure-JS `formatCronExpression(expr, tz)` helper.
- Implement prompt drawer expansion and copy buttons.
- Implement stale-while-revalidate reconnection banner.

- [ ] **Step 3: Add responsive Cyberpunk Permet HUD styles in `dashboard/static/style.css`**

- Add schedule card styles with Permet neon borders and pulse badges.
- Add run audit feed table/card styles with status chips (⚡ `COMPLETED` green, 🔄 `RUNNING` pulsing magenta, 🚨 `FAILED` red).
- Add `@media` responsive breakpoints for desktop (>1024px), tablet (768px-1024px), and mobile (<768px).

- [ ] **Step 4: Verify static assets and run dashboard tests**

Run: `cd /share/aerial/dashboard && go test -v ./...`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dashboard/static/index.html dashboard/static/app.js dashboard/static/style.css
git commit -m "feat(dashboard): add active schedules HUD UI, multi-tab router, and drift-corrected countdown ticker"
```

---

### Task 6: Full System Verification & Integration Testing

**Files:**
- Full codebase across `/share/aerial/brain` and `/share/aerial/dashboard`

- [ ] **Step 1: Run full unit test suite in Brain**

Run: `cd /share/aerial/brain && go test -v ./...`  
Expected: ALL PASS

- [ ] **Step 2: Run full unit test suite in Dashboard**

Run: `cd /share/aerial/dashboard && go test -v ./...`  
Expected: ALL PASS

- [ ] **Step 3: Verify git hygiene and zero personal tokens**

Run: `git status && git diff origin/main`  
Verify no sensitive keys, private IDs, or uncommitted files.

- [ ] **Step 4: Final Integration Commit & Ready State**

```bash
git add .
git commit -m "chore: complete dashboard active schedules & telemetry implementation verification"
```
