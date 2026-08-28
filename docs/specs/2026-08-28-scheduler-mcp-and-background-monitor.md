# Architectural Specification: Dedicated `scheduler-mcp` & Persistent Brain Monitor

**Date**: 2026-08-28  
**Status**: DRAFT (Ready for Human Review)  
**Authors**: Aerial Core Team  
**Scope**: `scheduler-mcp/`, `brain/pkg/scheduler/`, `brain/pkg/db/`, `brain/main.go`, `docker-compose.yml`, `SYSTEM.md`, `.agents/rules/`

---

## 1. Executive Summary & Motivation

### The Problem
When users request recurring schedules or one-shot reminders (e.g. *"Message me every Friday at 8 PM with a proposed meal plan"*), `agy` previously invoked its native in-memory `schedule` tool. This caused two fatal failures:
1. **Subprocess Hang & Timeout**: `agy` defaulted to `IsDaemon: false`, blocking the turn process indefinitely waiting for the recurring cron to finish, hitting the 15-minute timeout and causing cascading retries.
2. **Ephemeral Memory & Loss on Restart**: Even if `IsDaemon: true` were set, `agy` stores timers in ephemeral process memory. The moment the `agy` process exits or the container restarts, the schedule vanishes.

### The Solution
We separate the **Tool Interface** from the **Execution Engine**:
1. **`scheduler-mcp` (Dedicated Container)**: A standalone MCP microservice exposing typed tools (`schedule_recurring`, `schedule_once`, `list_schedules`, `cancel_schedule`) to `agy`. Tool calls validate parameters, write persistent records into SQLite (`/data/aerial.db`), and return instant JSON confirmations in ~2ms.
2. **`brain` Background Scheduler Monitor (Go Daemon)**: A lightweight 30-second monitor running inside `brain` that checks for due schedules in SQLite:
   - For **Recurring Schedules**: Dynamically creates a **brand-new Discord thread** in the parent channel (e.g. *"Weekly Meal Plan - Aug 28, 2026"*), inserts a `PENDING` turn into `messages`, enqueues to `WorkerPool`, and updates `next_run_at`.
   - For **One-Shot Reminders**: Inserts a `PENDING` turn targeting the designated `thread_id` into `messages`, enqueues to `WorkerPool`, and deletes the completed one-shot schedule.
   - All scheduled turns run through the exact same hardened queue, runner, retry backoff, and Discord delivery pipeline.

---

## 2. Microservice Architecture & Data Flow

```
========================================================================================
PHASE A: SCHEDULE CREATION (User Interaction Turn)
========================================================================================
[User on Discord]
       ? "Message me every Friday at 8 PM with a meal plan in this channel..."
       ?
[`brain` Ingest ??? WorkerPool ??? agy execution]
       ?
       ? (Model emits tool call)
[Tool: scheduler_schedule_recurring(channel_id, cron_expression, prompt, title_prefix)]
       ?
       ? (Streamable HTTP /mcp)
[Container: `aerial-scheduler-mcp` (Python or Go)]
       ?
       ??? Validates Cron expression (e.g. "0 20 * * 5")
       ??? Computes initial `next_run_at` timestamp
       ??? Inserts row into `/data/aerial.db` (`cron_schedules` table)
       ?
       ? (Returns JSON: {"status": "scheduled", "id": "...", "next_run_at": "..."})
[`agy` captures tool response ??? generates friendly Discord reply ??? exits in ~2s]

========================================================================================
PHASE B: SCHEDULE EXECUTION (Background Go Daemon in `brain`)
========================================================================================
[Brain Scheduler Monitor (Ticks every 30s in `brain/main.go`)]
       ?
       ?
[Queries SQLite: `GetDueCronSchedules()` & `GetDueOneShotSchedules()`]
       ?
       ???? CASE 1: Recurring Cron Due (e.g., Friday 8:00 PM)
       ?       ?
       ?       ??? 1. Creates NEW Discord Thread: `dg.ThreadStart(channel_id, "Weekly Meal Plan - Aug 28, 2026")`
       ?       ??? 2. Inserts message into `messages` table (ThreadID = newThread.ID, Status = 'PENDING')
       ?       ??? 3. Enqueues turn into `WorkerPool`
       ?       ??? 4. Updates `next_run_at` in `cron_schedules` for next occurrence
       ?
       ???? CASE 2: One-Shot Reminder Due
               ?
               ??? 1. Inserts message into `messages` table (ThreadID = target_id, Status = 'PENDING')
               ??? 2. Enqueues turn into `WorkerPool`
               ??? 3. Deletes row from `one_shot_schedules`
       ?
       ?
[WorkerPool runs prompt via `agy` ??? Captures stdout ??? Delivers to target/new Discord thread]
```

---

## 3. Database Schema (`/data/aerial.db`)

### A. `cron_schedules` Table
```sql
CREATE TABLE IF NOT EXISTS cron_schedules (
    id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL,          -- Discord Channel ID (where new threads will be spawned)
    title_prefix TEXT NOT NULL DEFAULT '', -- e.g. "Weekly Meal Plan"
    cron_expr TEXT NOT NULL,          -- e.g. "0 20 * * 5"
    prompt TEXT NOT NULL,             -- Prompt instructions for agy
    timezone TEXT NOT NULL DEFAULT 'UTC',
    next_run_at TIMESTAMP NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cron_schedules_due ON cron_schedules(enabled, next_run_at);
```

### B. `one_shot_schedules` Table
```sql
CREATE TABLE IF NOT EXISTS one_shot_schedules (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,          -- Discord Thread ID (where reminder will be delivered)
    prompt TEXT NOT NULL,             -- Reminder prompt instructions
    run_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_due ON one_shot_schedules(run_at);
```

---

## 4. `scheduler-mcp` Tools Specification

The `scheduler-mcp` service exposes standard Model Context Protocol tools over Streamable HTTP:

### 1. `schedule_recurring`
Registers a persistent recurring schedule.
* **Parameters**:
  * `channel_id` (string, required): Discord Channel ID where fresh threads will be created.
  * `cron_expression` (string, required): Standard 5-field cron expression (`min hour dom month dow`) or macros (`@daily`, `@weekly`, `@monthly`).
  * `prompt` (string, required): Instructions to execute on every occurrence.
  * `title_prefix` (string, optional): Title prefix for spawned threads (e.g. `"Weekly Meal Plan"`).
  * `timezone` (string, optional, default: `"America/New_York"` or `"UTC"`).
* **Return**:
  ```json
  {
    "status": "success",
    "schedule_id": "c1f7b8a2-...",
    "cron_expression": "0 20 * * 5",
    "next_run_at": "2026-09-04T20:00:00Z",
    "channel_id": "1542423172400291873",
    "message": "Recurring schedule created successfully."
  }
  ```

### 2. `schedule_once`
Registers a one-shot reminder.
* **Parameters**:
  * `target_id` (string, required): Target Discord thread ID or channel ID.
  * `run_at` (string, required): ISO 8601 timestamp (e.g. `2026-08-28T21:00:00Z`) or relative duration (`30m`, `2h`, `1d`).
  * `prompt` (string, required): Content/instructions of the reminder.
* **Return**:
  ```json
  {
    "status": "success",
    "schedule_id": "o4e9c1d3-...",
    "run_at": "2026-08-28T21:00:00Z",
    "message": "One-shot reminder scheduled successfully."
  }
  ```

### 3. `list_schedules`
Lists all active recurring schedules and pending reminders.
* **Parameters**:
  * `target_id` (string, optional): Filter by Channel/Thread ID.
* **Return**:
  ```json
  {
    "recurring": [
      {
        "id": "c1f7b8a2-...",
        "channel_id": "1542423172400291873",
        "title_prefix": "Weekly Meal Plan",
        "cron_expression": "0 20 * * 5",
        "next_run_at": "2026-09-04T20:00:00Z",
        "prompt": "..."
      }
    ],
    "one_shot": []
  }
  ```

### 4. `cancel_schedule`
Cancels and deletes an existing schedule.
* **Parameters**:
  * `schedule_id` (string, required): The ID of the schedule to cancel.
* **Return**:
  ```json
  {
    "status": "success",
    "schedule_id": "c1f7b8a2-...",
    "message": "Schedule cancelled successfully."
  }
  ```

---

## 5. `brain` Background Monitor Implementation (`pkg/scheduler`)

### Scheduler Engine
Inside `brain`, a new package `brain/pkg/scheduler` is introduced:
* **`Start(ctx context.Context, database *sql.DB, pool *queue.WorkerPool, dg *discordgo.Session)`**:
  - Runs a 30-second ticker (`time.NewTicker(30 * time.Second)`).
  - Evaluates `GetDueCronSchedules(database)` and `GetDueOneShotSchedules(database)`.
  - For each due cron:
    1. Generates a fresh thread title: `<title_prefix> - <date>` (e.g. `"Weekly Meal Plan - Aug 28, 2026"`).
    2. If `dg != nil`, calls `dg.ThreadStartComplex(c.TargetID, ...)` to create the thread in Discord.
    3. Persists a new `PENDING` message into SQLite targeting the new thread ID.
    4. Enqueues the message into `pool.Enqueue(msg)`.
    5. Calculates `next_run_at` using a robust cron evaluator (e.g. `robfig/cron/v3`) and updates `cron_schedules`.
  - For each due one-shot:
    1. Persists a new `PENDING` message into SQLite targeting `s.ThreadID`.
    2. Enqueues into `pool.Enqueue(msg)`.
    3. Calls `DeleteOneShotSchedule(database, s.ID)`.

---

## 6. System Instructions & Rules Invariant

In `SYSTEM.md` and `/share/aerial/.agents/rules/system_instructions.md`:
```markdown
## Scheduling & Recurring Reminders Invariant
- NEVER use the built-in native `schedule` tool (it is an ephemeral CLI tool that will hang the turn).
- ALWAYS use the persistent scheduler MCP tools:
  - `scheduler_schedule_recurring(channel_id, cron_expression, prompt, title_prefix)` for recurring weekly/daily routines (creates a fresh thread on each run).
  - `scheduler_schedule_once(target_id, run_at, prompt)` for one-time reminders in the current thread.
  - `scheduler_list_schedules()` and `scheduler_cancel_schedule(schedule_id)` to view and manage active schedules.
```

---

## 7. Container Orchestration & Docker Compose

In `docker-compose.yml`:
```yaml
  scheduler-mcp:
    build:
      context: ./scheduler-mcp
      dockerfile: Dockerfile
    container_name: aerial-scheduler-mcp
    restart: unless-stopped
    volumes:
      - aerial-data:/data
    networks:
      - aerial-net
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      timeout: 5s
      retries: 3
```

In `mcp_config.json`:
```json
{
  "mcpServers": {
    "scheduler": {
      "url": "http://scheduler-mcp:8080/mcp",
      "transport": "streamable-http"
    }
  }
}
```

---

## 8. Verification & Test Plan

1. **Unit Tests**:
   - `scheduler-mcp`: Test cron expression validation, ISO timestamp parsing, and SQLite CRUD operations.
   - `brain/pkg/scheduler`: Test cron next-run calculation, thread creation mocking, message enqueuing, and graceful ticker shutdown.
2. **Integration & Pre-Commit Build**:
   - `docker compose build scheduler-mcp`
   - `docker compose build brain`
3. **Live End-to-End Test**:
   - Send Discord request: *"Message me every Friday at 8 PM with a proposed meal plan..."*
   - Verify `agy` calls `scheduler_schedule_recurring`, receives instant response, responds to Discord, and exits in < 3 seconds.
   - Inspect SQLite `/data/aerial.db` to verify `cron_schedules` row.
   - Trigger a due test event and verify a brand-new Discord thread is created with the generated meal plan.
