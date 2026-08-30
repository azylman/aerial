# Technical Design Specification: Dashboard Live Agent Task Queue & Execution Monitor

- **Author**: Aerial
- **Target Repository**: `azylman/aerial` (`/share/aerial`)
- **Status**: Hardened & Multi-Agent Audit Approved (Grade A)
- **Date**: 2026-08-30

---

## 1. Executive Summary

Aerial coordinates autonomous AI task executions, user conversations, scheduled routines, and background jobs. Currently, the Permet HUD Dashboard (`aerial-dashboard`) displays cluster service health, active schedules, semantic memory archives, and container deployment pipelines. However, there is no direct, real-time observability at the top of the HUD into pending or actively executing agent tasks, their duration, or their live turn-by-turn execution traces in `agentsview`.

Following a comprehensive 4-agent architectural and devil's advocate audit (covering Backend Concurrency, Frontend UX, Security/Invariants, and Chaos Engineering), this hardened specification introduces the **Live Agent Task Queue & Execution Monitor** at the very top of the landing page telemetry view (above the deployment pipeline). It provides real-time visibility into all queued (`PENDING`) and executing (`PROCESSING`) agent tasks, drift-corrected live execution tickers, trigger source badges, prompt containment, and direct deep-linking into `agentsview` for inspecting live conversation and agent step trajectories.

---

## 2. Goals & Key Requirements

1. **Top-Level HUD Queue Section**:
   - Prominently positioned at the top of the Telemetry view (`#telemetry-view`), directly above the Live Deployment Pipeline.
   - Real-time rendering of all currently `PENDING` (queued) and `PROCESSING` (running) agent tasks with laser sweep animations.
   - Sleek Cyberpunk/Gundam Aerial idle state banner when zero tasks are active (`0 ACTIVE TASKS // QUEUE IDLE`).
   - Expandable queue containment to gracefully handle bursts of 10–50+ queued tasks without DOM layout blowouts.

2. **Agentsview Direct Deep-Linking & Session Allocation**:
   - Each running task card features an **`INSPECT IN AGENTSVIEW ↗`** interactive badge and clickable card action.
   - Deep-links directly to the Antigravity transcript session (`/conversations/?session=<session_id>`) in a new browser tab (`target="_blank" rel="noopener noreferrer"`).
   - Graceful pending state: if a task is newly enqueued and waiting for session allocation, displays `⏳ QUEUE ALLOCATING SESSION` until execution starts.

3. **Summary Bar Queue Metric**:
   - Dedicated `AGENT QUEUE` metric card added to the top `.summary-bar` (card #2) displaying live task status (e.g. `0 IDLE` in green or `1 RUNNING` in pulsating cyan).

4. **BFF Unified Telemetry Architecture**:
   - `brain` exposes a fast, cached `GET /tasks` endpoint backed by SQLite query `GetActiveTasks(db)` joining `messages` and `sessions` with an indexed read query.
   - `dashboard` aggregates active tasks asynchronously into the atomic `GET /api/status` telemetry endpoint with a 2s timeout and stale-while-revalidate fallback.
   - Zero browser-to-brain direct network exposure, with full graceful fallback if `brain` restarts.

5. **Security & Zero Token Leakage Invariants**:
   - Strict sanitization of all prompt snippets and metadata using token regex redaction (`SanitizeString`) **BEFORE** any length truncation.
   - Full client-side HTML escaping (`escapeHtml`) on all dynamic attributes, preventing any XSS vulnerability.
   - 100% generic, open-source compliant, and domain-agnostic.

---

## 3. Architecture & Data Flow

```
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                         PERMET HUD DASHBOARD                             │
   │   - Summary Bar: AGENT QUEUE Metric Card (0 IDLE / 1 RUNNING)            │
   │   - LIVE AGENT QUEUE SECTION (Above Deployment Pipeline)                 │
   │     • Task Cards with Trigger Badges (💬 Discord, ⏰ Cron, ⚡ API)       │
   │     • Drift-Corrected 1s Elapsed Tickers (⏱ 00:34s)                      │
   │     • Multi-Line Prompt Containment with Monospace Block                 │
   │     • Direct Action: [ 💬 INSPECT IN AGENTSVIEW ↗ ]                      │
   └─────────────────────────────────────┬────────────────────────────────────┘
                                         │ HTTP GET /api/status (every 5s)
                                         ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                      aerial-dashboard (Go BFF)                           │
   │   - Asynchronously queries GET http://aerial-brain:8080/tasks            │
   │   - Aggregates active tasks with Docker services and GHCR deployments    │
   │   - In-memory caching & graceful error fallback                          │
   └─────────────────────────────────────┬────────────────────────────────────┘
                                         │ HTTP GET /tasks (2s timeout, 1s TTL)
                                         ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                       aerial-brain (Go Core Engine)                      │
   │   ┌──────────────────────────────────────────────────────────────────┐   │
   │   │  GET /tasks Handler (Sanitization + 1s TTL Cache)                │   │
   │   └────────────────────────────────┬─────────────────────────────────┘   │
   │                                    │ SQLite Read Query                   │
   │                                    ▼                                     │
   │   ┌──────────────────────────────────────────────────────────────────┐   │
   │   │  SQLite (/data/aerial.db)                                        │   │
   │   │  - Index: messages(status, created_at)                           │   │
   │   │  - messages (status IN ('PENDING', 'PROCESSING'))                │   │
   │   │  - sessions (LEFT JOIN for internal_session_id mapping)          │   │
   │   └──────────────────────────────────────────────────────────────────┘   │
   └──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Hardened Component Specifications

### 4.1. Core Engine Backend (`brain/pkg/db/db.go` & `brain/main.go`)

#### 1. Data Models (`ActiveTask`)
```go
type ActiveTask struct {
	ID            string    `json:"id"`
	ThreadID      string    `json:"thread_id"`
	SessionID     string    `json:"session_id,omitempty"`
	AuthorName    string    `json:"author_name"`
	AuthorID      string    `json:"author_id"`
	Prompt        string    `json:"prompt"`
	Status        string    `json:"status"` // "PENDING", "PROCESSING"
	RetryCount    int       `json:"retry_count"`
	ScheduleRunID string    `json:"schedule_run_id,omitempty"`
	TriggerType   string    `json:"trigger_type"` // "discord", "cron", "reminder", "http"
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}
```

#### 2. Database Index & Query (`GetActiveTasks`)
In `brain/pkg/db/db.go`:
```sql
CREATE INDEX IF NOT EXISTS idx_messages_status_created ON messages(status, created_at);
```

```go
func InferTriggerType(authorID, scheduleRunID string) string {
	if authorID == "http-client" {
		return "http"
	}
	if scheduleRunID != "" {
		if strings.HasPrefix(scheduleRunID, "cron-") {
			return "cron"
		}
		return "reminder"
	}
	return "discord"
}

func GetActiveTasks(database *sql.DB) ([]ActiveTask, error) {
	query := `
	SELECT 
		m.id,
		m.thread_id,
		COALESCE(s.internal_session_id, '') AS session_id,
		m.author_name,
		m.author_id,
		m.content,
		m.status,
		m.retry_count,
		m.schedule_run_id,
		m.created_at,
		m.updated_at
	FROM messages m
	LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.status IN ('PENDING', 'PROCESSING')
	ORDER BY m.created_at ASC
	LIMIT 50;
	`
	rows, err := database.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []ActiveTask
	for rows.Next() {
		var t ActiveTask
		if err := rows.Scan(
			&t.ID,
			&t.ThreadID,
			&t.SessionID,
			&t.AuthorName,
			&t.AuthorID,
			&t.Prompt,
			&t.Status,
			&t.RetryCount,
			&t.ScheduleRunID,
			&t.CreatedAt,
			&t.UpdatedAt,
		); err != nil {
			return nil, err
		}
		t.TriggerType = InferTriggerType(t.AuthorID, t.ScheduleRunID)
		tasks = append(tasks, t)
	}
	return tasks, nil
}
```

#### 3. Brain HTTP Endpoint (`GET /tasks`)
In `brain/main.go`:
- Exposes `GET /tasks` with method validation (returns 405 for non-GET).
- Incorporates 1-second in-memory caching protected by `sync.RWMutex`.
- Sanitizes all prompts using `SanitizeString(prompt)` **prior** to truncation (max 500 characters).
- Returns JSON payload: `{ "status": "ok", "total": N, "tasks": [...] }`.

---

### 4.2. Dashboard BFF Gateway (`dashboard/main.go`)

- Updates `ClusterResponse` and introduces `ActiveTaskStatus`:
```go
type ActiveTaskStatus struct {
	ID          string    `json:"id"`
	ThreadID    string    `json:"thread_id"`
	SessionID   string    `json:"session_id,omitempty"`
	AuthorName  string    `json:"author_name"`
	Prompt      string    `json:"prompt"`
	Status      string    `json:"status"` // "PENDING", "PROCESSING"
	RetryCount  int       `json:"retry_count"`
	TriggerType string    `json:"trigger_type"`
	CreatedAt   time.Time `json:"created_at"`
	StartedAt   time.Time `json:"started_at"`
}

type ClusterResponse struct {
	SystemTime       time.Time          `json:"system_time"`
	ClusterStatus    string             `json:"cluster_status"`
	ActiveTasksCount int                `json:"active_tasks_count"`
	ActiveTasks      []ActiveTaskStatus `json:"active_tasks"`
	Services         []ServiceStatus    `json:"services"`
	Deployments      []DeploymentStatus `json:"deployments"`
}
```
- Fetches active tasks via `fetchActiveTasksFromBrain(ctx, brainBaseURL)` with a 2-second timeout.
- Graceful degradation: if `aerial-brain` is restarting or offline, returns `ActiveTasks: []` and `ActiveTasksCount: 0` without failing the cluster status response.

---

### 4.3. Frontend User Interface (`index.html`, `style.css`, `app.js`)

#### 1. Top Summary Bar (`index.html`)
Adds `AGENT QUEUE` as metric card #2:
```html
<div class="summary-card">
    <div class="card-glow"></div>
    <span class="label">AGENT QUEUE</span>
    <span class="value text-success" id="summary-tasks-val">0 IDLE</span>
    <span class="sub-label" id="summary-tasks-sub">REAL-TIME DISPATCH</span>
</div>
```

#### 2. Live Agent Execution Queue Section (`index.html`)
Placed immediately above `<section class="deployment-pipeline-section">`:
```html
<section class="active-tasks-section" aria-live="polite">
    <div class="section-title-bar">
        <div class="section-title">
            <span class="title-icon">⚡</span>
            <h2>LIVE AGENT EXECUTION QUEUE</h2>
        </div>
        <span class="section-badge" id="tasks-count-badge">0 IN PROGRESS</span>
    </div>
    <div class="active-tasks-container" id="active-tasks-container">
        <!-- Dynamically populated via app.js -->
    </div>
</section>
```

#### 3. Task Card Component Details (`app.js` & `style.css`)
- **Idle State**:
  ```html
  <div class="task-idle-card">
      <div class="idle-indicator">
          <span class="pulse-dot healthy"></span>
          <span>ALL WORKERS IDLE // NO PENDING TURNS</span>
      </div>
      <div>DISPATCH POLLING SQLITE QUEUE (1s)</div>
  </div>
  ```
- **Active Card Structure**:
  - Laser sweep border animation (`.task-card-laser`) on `PROCESSING` cards.
  - Header: Trigger source badge (`💬 DISCORD`, `⏰ CRON`, `⏱️ REMINDER`, `⚡ API`), author label, status pill (`⚡ RUNNING` / `⏳ QUEUED`), retry badge if `retry_count > 0`, and live elapsed ticker (`⏱ 00:15s` with `data-started`).
  - Body: Prompt snippet formatted in a 3-line clamped monospace cyber block (`-webkit-line-clamp: 3`).
  - Action Footer: Primary action button **`💬 INSPECT IN AGENTSVIEW ↗`** linking to `/conversations/?session=${encodeURIComponent(task.session_id)}` in `target="_blank" rel="noopener noreferrer"`.
  - Pending session state: If `session_id` is empty, renders disabled badge `⏳ QUEUE ALLOCATING SESSION`.

---

## 5. Verification & Testing Matrix

| Component | Test File | Target Scenario |
| :--- | :--- | :--- |
| Database Layer | `brain/pkg/db/db_test.go` | Test `GetActiveTasks()` with mixed states (`PENDING`, `PROCESSING`, `COMPLETED`), test `sessions` left join, index usage, and trigger type inference. |
| Brain API Layer | `brain/main_test.go` | Test `GET /tasks` endpoint status 200, JSON formatting, token redaction with `SanitizeString`, and 1s TTL caching. |
| Dashboard BFF | `dashboard/main_test.go` | Test `/api/status` BFF aggregation with brain active tasks, verify graceful timeout degradation, and verify metric counts. |
| Frontend Assets | Local verification | Verify HTML structure, CSS rules, drift-corrected ticker calculations, and zero XSS vulnerability. |
| Build & Lint | `go test -v ./...` | Unit tests pass cleanly across `brain` and `dashboard` with zero lint regressions. |

---
