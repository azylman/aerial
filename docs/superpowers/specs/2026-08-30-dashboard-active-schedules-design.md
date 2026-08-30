# Technical Design Specification: Dashboard Active Schedules & Execution Telemetry

- **Author**: Aerial
- **Target Repository**: `azylman/aerial` (`/share/aerial`)
- **Status**: Hardened & Approved
- **Date**: 2026-08-30

---

## 1. Executive Summary

Aerial operates an autonomous scheduling subsystem supporting recurring cron routines and one-shot reminders backed by SQLite (`/data/aerial.db`). While schedules can be configured dynamically via assistant MCP tools, there is currently no direct visibility into active schedules, upcoming execution timers, or historical run status on the Permet HUD Dashboard (`aerial-dashboard`).

This hardened technical design introduces the **Active Schedules & Execution Telemetry Subsystem** on the dashboard. It delivers full observability into active recurring crons, pending one-shot reminders, drift-corrected live countdown tickers, and an execution history audit log tracking run durations, outcomes, and generated Discord thread conversations.

---

## 2. Goals & Key Requirements

1. **Active Schedules Telemetry**:
   - Live visibility into all enabled `cron_schedules` and pending `one_shot_schedules`.
   - Real-time, drift-corrected countdown tickers for upcoming schedule triggers (`next_run_at` / `run_at`).
   - Human-readable breakdown of standard 5-field cron expressions (e.g. `0 9 * * 1-5` ➔ *"Weekdays (Mon–Fri) at 09:00"*).
   - Timezone badges, target channel/thread identifiers, and expandable prompt previews.

2. **Execution History & Run Audit Log**:
   - Introduce a persistent `schedule_runs` table in SQLite (`/data/aerial.db`).
   - Track every schedule execution: trigger time, actual execution start, completion time, duration in milliseconds, final status (`enqueued`, `running`, `completed`, `failed`), and sanitized error messages.
   - Deep-link completed runs directly to the created Discord conversation thread in `agentsview`.

3. **Decoupled Engine & Dashboard Architecture**:
   - `brain` service exposes clean REST endpoints: `GET /schedules` and `GET /schedules/runs` with 5s memory caching and token sanitization.
   - `dashboard` service proxies `/api/schedules` and `/api/schedules/runs` with resilient stale-while-revalidate fallback handling.
   - Frontend single-page application integrates a new **`⏰ SCHEDULES`** tab matching the Gundam Aerial Cyberpunk Permet HUD design system.

4. **Invariants Compliance & Security**:
   - **Zero Plaintext Tokens**: Prompt previews and error messages are scrubbed of sensitive keys/tokens before API serialization.
   - **Zero Stuck Running Invariant**: Startup reconciler automatically transitions orphaned `running` states to `failed` upon container restart.
   - **100% Generic & Domain-Agnostic**: Zero hardcoded personal usernames, channel IDs, or private business logic in source code.

---

## 3. Architecture & Topology

```
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                         PERMET HUD DASHBOARD                             │
   │   - Multi-Tab Router (Telemetry | Schedules | Memory Archive)            │
   │   - Active Schedules Grid  (Drift-Corrected Timers, Human-Readable Cron) │
   │   - Summary Metrics        (Active Crons, Pending Once, 24h Success)     │
   │   - Run Audit Feed         (Status Badges, Durations, Thread Links)      │
   └─────────────────────────────────────┬────────────────────────────────────┘
                                         │ HTTP GET /api/schedules
                                         │ HTTP GET /api/schedules/runs
                                         ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                      aerial-dashboard (Go Proxy)                         │
   └─────────────────────────────────────┬────────────────────────────────────┘
                                         │ HTTP Proxy to brainURL (Cached/Fallback)
                                         ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                       aerial-brain (Go Core Engine)                      │
   │   ┌───────────────────────────┐      ┌──────────────────────────────┐    │
   │   │  pkg/scheduler (Monitor)  │      │   pkg/queue (Worker Pool)    │    │
   │   │  - Evaluates due crons    │      │   - Claims message & sets    │    │
   │   │  - Creates 'enqueued' run │      │     'running' status         │    │
   │   │  - Daily retention prune  │      │   - Updates 'completed'      │    │
   │   └─────────────┬─────────────┘      │   - Startup Crash Recovery   │    │
   │                 │                    └──────────────┬───────────────┘    │
   └─────────────────┼───────────────────────────────────┼────────────────────┘
                     │                                   │
                     ▼                                   ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                         SQLite (/data/aerial.db)                         │
   │   - cron_schedules                                                       │
   │   - one_shot_schedules                                                   │
   │   - messages (with schedule_run_id column)                               │
   │   - schedule_runs (with optimized composite indexes)                     │
   └──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Hardened Component Specifications

### 4.1. Data Storage & Schema Migration (`brain/pkg/db`)

A new table `schedule_runs` is created in SQLite, and `messages` is updated with a `schedule_run_id` correlation column:

```sql
CREATE TABLE IF NOT EXISTS schedule_runs (
    id TEXT PRIMARY KEY,                       -- e.g. "run-550e8400-e29b-..."
    schedule_id TEXT NOT NULL,                 -- ID of cron or one-shot schedule
    schedule_type TEXT NOT NULL,               -- 'cron' | 'one_shot'
    message_id TEXT NOT NULL DEFAULT '',       -- Correlated db.Message ID
    target_id TEXT NOT NULL,                   -- Target Discord Channel or Thread ID
    thread_id TEXT NOT NULL,                   -- Resolved Discord Thread ID
    title TEXT NOT NULL DEFAULT '',            -- Thread title or schedule prefix
    prompt TEXT NOT NULL,                      -- Scheduled execution prompt
    status TEXT NOT NULL DEFAULT 'enqueued',   -- 'enqueued' | 'running' | 'completed' | 'failed'
    started_at DATETIME NOT NULL,
    completed_at DATETIME,
    duration_ms INTEGER DEFAULT 0,
    error TEXT NOT NULL DEFAULT ''
);

-- Optimized Indexes
CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_started ON schedule_runs(schedule_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_status_started ON schedule_runs(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_message_id ON schedule_runs(message_id);
```

#### Safe Column Migrations:
```sql
ALTER TABLE messages ADD COLUMN schedule_run_id TEXT NOT NULL DEFAULT '';
ALTER TABLE schedule_runs ADD COLUMN message_id TEXT NOT NULL DEFAULT '';
```

#### Retention Policy:
Pruning runs on startup and once every 24 hours during `scheduler.Run`:
- Retains the last 1,000 runs or runs within the last 30 days.

---

### 4.2. Brain Execution Engine Lifecycle & Crash Recovery

1. **Trigger & Registration (`pkg/scheduler/scheduler.go`)**:
   - Generates `runID := "run-" + uuid.New().String()`.
   - Inserts row into `schedule_runs` with `status = "enqueued"`, `started_at = time.Now().UTC()`.
   - Passes `runID` into `db.Message.ScheduleRunID`.
   - In `InsertMessageAndConsumeOneShot`, enforces transaction rollback if `DELETE` affected 0 rows (prevents cancelled one-shots from firing).

2. **Execution Hook & Accurate Duration (`pkg/queue/queue.go`)**:
   - In `WorkerPool.processMessage`, when message is claimed:
     - Updates `schedule_runs` to `status = "running"`, reset `started_at = time.Now().UTC()` to measure actual execution time without queue wait latency.
   - Wrapped in `defer recover()` to guarantee panics mark the run `status = "failed"` with panic trace.
   - On completion, updates `schedule_runs` to `status = "completed"` with `duration_ms` and `completed_at`.

3. **Startup Crash Recovery (`queue.RecoverInterrupted`)**:
   - Any `schedule_runs` with `status IN ('enqueued', 'running')` upon brain startup are automatically updated to `status = 'failed'`, `error = 'Interrupted by server restart'`.

---

### 4.3. REST API Contract & Security Redaction

#### A. Active Schedules Telemetry
- **Path**: `GET /schedules` (Brain) / `GET /api/schedules` (Dashboard)
- **Response Schema**:
```json
{
  "status": "ok",
  "system_time": "2026-08-30T17:25:00Z",
  "summary": {
    "total_active": 4,
    "cron_count": 3,
    "one_shot_count": 1,
    "next_run_at": "2026-08-30T18:00:00Z",
    "total_runs_24h": 14,
    "success_rate_24h": 100.0
  },
  "crons": [
    {
      "id": "cron-daily-brief",
      "channel_id": "1542423172400291873",
      "title_prefix": "Daily Morning Briefing",
      "cron_expr": "0 9 * * 1-5",
      "cron_description": "Weekdays (Mon–Fri) at 09:00",
      "prompt": "Summarize outstanding notifications and daily priorities...",
      "timezone": "America/Los_Angeles",
      "next_run_at": "2026-08-31T16:00:00Z",
      "enabled": true,
      "created_at": "2026-08-28T12:00:00Z"
    }
  ],
  "one_shots": [
    {
      "id": "once-reminder-123",
      "thread_id": "1543668253363150928",
      "prompt": "Check container memory in 15 minutes",
      "run_at": "2026-08-30T17:40:00Z",
      "created_at": "2026-08-30T17:25:00Z"
    }
  ]
}
```

#### B. Execution History Runs
- **Path**: `GET /schedules/runs?limit=50&offset=0` (Brain) / `GET /api/schedules/runs` (Dashboard)
- **Response Schema**:
```json
{
  "status": "ok",
  "total": 42,
  "limit": 50,
  "offset": 0,
  "runs": [
    {
      "id": "run-a1b2c3d4",
      "schedule_id": "cron-daily-brief",
      "schedule_type": "cron",
      "target_id": "1542423172400291873",
      "thread_id": "1543660000000000000",
      "title": "Daily Morning Briefing – Aug 29, 2026",
      "prompt": "Summarize outstanding notifications...",
      "status": "completed",
      "started_at": "2026-08-29T16:00:00Z",
      "completed_at": "2026-08-29T16:00:42Z",
      "duration_ms": 42150,
      "error": ""
    }
  ]
}
```

#### Token Sanitization:
All `prompt` and `error` outputs are sanitized with `SanitizeString(text)` against sensitive keys (`GEMINI_API_KEY`, `DISCORD_BOT_TOKEN`, `GITHUB_PAT`, `HA_TOKEN`, `TOKEN`, `KEY`, `PASSWORD`, `SECRET`).

---

### 4.4. Permet HUD Frontend Architecture (`index.html`, `app.js`, `style.css`)

1. **Multi-Tab Declarative Router (`app.js`)**:
   - Replaces binary if/else tab switcher with extensible `TABS` dictionary (`telemetry`, `schedules`, `memory`).
   - Dynamically starts/stops polling intervals upon tab entry/exit.

2. **Drift-Corrected Countdown Engine**:
   - Calculates `serverClockSkewMs = Date.parse(data.system_time) - Date.now()`.
   - Clamps countdowns:
     - `> 60s`: `⏱ in 2h 15m`
     - `1s to 59s`: `⏱ in 45s` (neon amber)
     - `0s to -15s`: `⚡ TRIGGERING...` (pulsing neon magenta)
     - `< -15s`: `⏱ OVERDUE (SYNCING...)`

3. **Stale-While-Revalidate Resilience**:
   - Retains existing schedule cards during network blips / Watchtower restarts with a non-blocking `⚡ RECONNECTING...` warning banner.

4. **Responsive Layout**:
   - Summary cards grid: 4 columns (>1024px) ➔ 2 columns (768px-1024px) ➔ 1 column (<768px).
   - Mobile-optimized run feed cards to prevent horizontal overflow.

---

## 5. Verification & Testing Plan

1. **Brain Unit Tests (`brain/pkg/db/db_test.go`, `brain/pkg/scheduler/scheduler_test.go`, `brain/main_test.go`)**:
   - `TestScheduleRunsCRUD`: Insertion, updates, pagination, status transitions.
   - `TestOrphanedRunRecovery`: Ensure crashed runs transition to `failed`.
   - `TestSchedulerExecutionTracking`: Verify trigger creates run, worker marks completed.
   - `TestTokenSanitization`: Verify prompts with API keys are redacted.
   - `TestSchedulesAPIEndpoints`: Verify `GET /schedules` and `GET /schedules/runs`.

2. **Dashboard Unit Tests (`dashboard/main_test.go`)**:
   - Verify proxy handlers, upstream error handling, and JSON schema compliance.

3. **Full Suite Verification**:
   - Run `go test ./...` in both `brain/` and `dashboard/`.
