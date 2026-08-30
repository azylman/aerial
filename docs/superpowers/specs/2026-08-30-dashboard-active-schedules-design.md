# Technical Design Specification: Dashboard Active Schedules & Execution Telemetry

- **Author**: Aerial
- **Target Repository**: `azylman/aerial` (`/share/aerial`)
- **Status**: Proposed (Under Review)
- **Date**: 2026-08-30

---

## 1. Executive Summary

Aerial operates an autonomous scheduling subsystem supporting recurring cron routines and one-shot reminders backed by SQLite (`/data/aerial.db`). While schedules can be configured dynamically via assistant MCP tools, there is currently no direct visibility into active schedules, upcoming execution timers, or historical run status on the Permet HUD Dashboard (`aerial-dashboard`).

This technical design introduces the **Active Schedules & Execution Telemetry Subsystem** on the dashboard. It delivers full observability into active recurring crons, pending one-shot reminders, live countdown timers, and an execution history audit log tracking run durations, outcomes, and generated Discord thread conversations.

---

## 2. Goals & Key Requirements

1. **Active Schedules Telemetry**:
   - Live visibility into all enabled `cron_schedules` and pending `one_shot_schedules`.
   - Real-time countdown tickers for upcoming schedule triggers (`next_run_at` / `run_at`).
   - Human-readable breakdown of standard 5-field cron expressions (e.g. `0 9 * * 1-5` ➔ *"Every weekday at 09:00 AM"*).
   - Timezone badges, target channel/thread identifiers, and expandable prompt previews.

2. **Execution History & Run Audit Log**:
   - Introduce a persistent `schedule_runs` table in SQLite (`/data/aerial.db`).
   - Track every schedule execution: trigger time, completion time, duration in milliseconds, final status (`running`, `completed`, `failed`), and error messages.
   - Deep-link completed runs directly to the created Discord conversation thread in `agentsview`.

3. **Decoupled Engine & Dashboard Architecture**:
   - `brain` service exposes clean REST endpoints: `GET /schedules` and `GET /schedules/runs`.
   - `dashboard` service proxies `/api/schedules` and `/api/schedules/runs` with resilient fallback handling.
   - Frontend single-page application integrates a new **`⏰ SCHEDULES`** tab matching the Gundam Aerial Cyberpunk Permet HUD design system.

4. **Invariants Compliance**:
   - 100% generic and domain-agnostic (no hardcoded personal usernames, IDs, or sensitive tokens).
   - Read-only observability on the dashboard; all schedule mutations continue to flow through Aerial / MCP tools.

---

## 3. Architecture & Data Flow

```
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                         PERMET HUD DASHBOARD                             │
   │   - Active Schedules Grid  (Countdown Tickers, Human-Readable Cron)     │
   │   - Summary Metrics        (Active Crons, Pending Once, 24h Success)    │
   │   - Run Audit Feed         (Status Badges, Durations, Thread Links)     │
   └─────────────────────────────────────┬────────────────────────────────────┘
                                         │ HTTP GET /api/schedules
                                         │ HTTP GET /api/schedules/runs
                                         ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                      aerial-dashboard (Go Proxy)                         │
   └─────────────────────────────────────┬────────────────────────────────────┘
                                         │ HTTP Proxy to brainURL
                                         ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                       aerial-brain (Go Core Engine)                      │
   │   ┌───────────────────────────┐      ┌──────────────────────────────┐    │
   │   │  pkg/scheduler (Monitor)  │      │   pkg/queue (Worker Pool)    │    │
   │   │  - Evaluates due crons    │      │   - Executes turn runner     │    │
   │   │  - Creates 'running' log  │      │   - Updates 'completed' log  │    │
   │   └─────────────┬─────────────┘      └──────────────┬───────────────┘    │
   └─────────────────┼───────────────────────────────────┼────────────────────┘
                     │                                   │
                     ▼                                   ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                         SQLite (/data/aerial.db)                         │
   │   - cron_schedules                                                       │
   │   - one_shot_schedules                                                   │
   │   - schedule_runs (NEW)                                                  │
   └──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Detailed Component Specifications

### 4.1. Data Storage & SQLite Migration (`brain/pkg/db`)

A new table `schedule_runs` will be added to the SQLite database schema with indexes optimized for chronological and schedule-specific queries:

```sql
CREATE TABLE IF NOT EXISTS schedule_runs (
    id TEXT PRIMARY KEY,             -- e.g. "run-550e8400-e29b-..."
    schedule_id TEXT NOT NULL,       -- ID of the cron or one-shot schedule
    schedule_type TEXT NOT NULL,     -- 'cron' or 'one_shot'
    target_id TEXT NOT NULL,         -- Discord Channel ID or Thread ID
    thread_id TEXT NOT NULL,         -- Created/Target Discord Thread ID
    title TEXT NOT NULL DEFAULT '',  -- Thread title or schedule prefix
    prompt TEXT NOT NULL,            -- Scheduled execution prompt
    status TEXT NOT NULL,            -- 'running' | 'completed' | 'failed'
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    duration_ms INTEGER DEFAULT 0,
    error TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_id ON schedule_runs(schedule_id);
```

#### Retention Policy:
To prevent unbounded database growth, `schedule_runs` will retain up to the last 1,000 runs or 30 days of execution history, with automated pruning executed during scheduler startup.

---

### 4.2. Brain Execution Engine Lifecycle (`brain/pkg/scheduler` & `brain/pkg/queue`)

1. **Scheduler Trigger Hook (`pkg/scheduler/scheduler.go`)**:
   - When a cron or one-shot schedule reaches its execution threshold, a new unique `run_id` (`run-<uuid>`) is generated.
   - An initial row is written to `schedule_runs` with `status = "running"`, `started_at = time.Now().UTC()`, and associated metadata.
   - The `run_id` is embedded in the enqueued `db.Message.Metadata["run_id"]`.

2. **Worker Pool Completion Hook (`pkg/queue/pool.go` / `pkg/runner`)**:
   - As the worker pool finishes processing the queued message, it checks for `run_id` in the message metadata.
   - Computes `duration_ms = int(time.Since(startedAt).Milliseconds())`.
   - If turn execution succeeded, updates `schedule_runs` to `status = "completed"`.
   - If turn execution returned an error or timed out, updates `schedule_runs` to `status = "failed"` with the error description.

---

### 4.3. REST API Contract

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

---

### 4.4. Permet HUD Frontend Architecture (`index.html`, `app.js`, `style.css`)

1. **Navigation Integration**:
   - Navigation button `<button class="nav-btn" id="tab-schedules-btn">` styled with Permet glowing hover effects.
   - Deep-linking support via hash `#schedules`.

2. **Summary Metric Cards**:
   - **ACTIVE CRONS**: Count of active recurring cron jobs.
   - **PENDING ONCE**: Count of un-fired one-shot reminders.
   - **NEXT UPCOMING**: Real-time countdown timer to the next scheduled trigger.
   - **24H SUCCESS RATE**: Percentage success rate with badge styling.

3. **Active Schedules Grid & Cards**:
   - Cards display schedule type badge (`CRON` vs `ONE-SHOT`), enabled status pulse, and human-readable cron expression breakdown.
   - Live ticker element calculating remaining time countdown every second (`⏱ in 1h 24m 10s`).
   - Prompt drawer with collapsible preview.

4. **Run Audit Log & Execution Feed**:
   - Clean tabular/feed layout with visual status chips (⚡ `COMPLETED`, 🔄 `RUNNING`, 🚨 `FAILED`).
   - Relative start timestamps ("5m ago") and formatted durations (`42.2s`).
   - Clickable link to Discord conversation thread view.

---

## 5. Testing & Quality Assurance Plan

1. **Brain Unit Tests**:
   - `brain/pkg/db/db_test.go`: Verify `schedule_runs` CRUD, queries, and pagination.
   - `brain/pkg/scheduler/scheduler_test.go`: Verify execution state recording when schedules trigger.
   - `brain/main_test.go`: Verify `/schedules` and `/schedules/runs` HTTP endpoints and response schemas.

2. **Dashboard Unit Tests**:
   - `dashboard/main_test.go`: Verify `/api/schedules` proxying, upstream error fallbacks, and JSON serialization.

3. **Full Suite Verification**:
   - Run `go test ./...` in both `/share/aerial/brain` and `/share/aerial/dashboard`.
   - Verify frontend asset validation and cross-browser responsiveness.

---

## 6. Security, Privacy & Invariant Checklist

- [x] **Zero Plaintext Tokens**: No tokens, API keys, or private webhook URLs exposed in responses or logs.
- [x] **100% Generic & Reusable**: No personal names or hardcoded user IDs in source code.
- [x] **Read-Only Dashboard**: All schedule modifications remain routed through Aerial assistant MCP tools.
- [x] **Database Resilience**: PRAGMA busy_timeout and WAL mode preserved across all queries.
