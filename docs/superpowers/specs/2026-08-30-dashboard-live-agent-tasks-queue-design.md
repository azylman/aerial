# Technical Design Specification: Dashboard Live Agent Task Queue & Execution Monitor

- **Author**: Aerial
- **Target Repository**: `azylman/aerial` (`/share/aerial`)
- **Status**: Hardened & Approved
- **Date**: 2026-08-30

---

## 1. Executive Summary

Aerial coordinates autonomous AI task executions, user conversations, scheduled routines, and background jobs. Currently, the Permet HUD Dashboard (`aerial-dashboard`) displays cluster service health, active schedules, semantic memory archives, and container deployment pipelines. However, there is no direct, real-time observability at the top of the HUD into pending or actively executing agent tasks, their duration, or their live turn-by-turn execution traces in `agentsview`.

This specification introduces the **Live Agent Task Queue & Execution Monitor** at the very top of the landing page telemetry view (above the deployment pipeline). It provides real-time visibility into all queued (`PENDING`) and executing (`PROCESSING`) agent tasks, live drift-corrected elapsed execution tickers, trigger source badges, and direct one-click deep-linking into `agentsview` for inspecting live conversation and agent step trajectories.

---

## 2. Goals & Key Requirements

1. **Top-Level HUD Queue Section**:
   - Prominently positioned at the top of the Telemetry view (`#telemetry-view`), directly above the Live Deployment Pipeline.
   - Real-time rendering of all currently `PENDING` (queued) and `PROCESSING` (running) agent tasks.
   - Sleek Cyberpunk/Gundam Aerial idle state banner when zero tasks are active (`0 ACTIVE TASKS // QUEUE IDLE`).

2. **Agentsview Direct Deep-Linking**:
   - Each running task card features an **`INSPECT IN AGENTSVIEW ↗`** interactive badge and clickable card action.
   - Deep-links directly to the Antigravity transcript session (`/conversations/?session=<session_id>`) in a new browser tab (`target="_blank" rel="noopener"`).
   - Graceful pending state: if a task is newly enqueued and waiting for session allocation, displays `⏳ QUEUE ALLOCATING SESSION` until execution starts.

3. **Summary Bar Queue Metric**:
   - Dedicated `AGENT QUEUE` metric card added to the top `.summary-bar` displaying live task status (e.g. `0 IDLE` in green or `1 RUNNING` in pulsating cyan).

4. **BFF Unified Telemetry Architecture**:
   - `brain` exposes a fast, cached `GET /tasks` endpoint backed by SQLite query `GetActiveTasks(db)` joining `messages` and `sessions`.
   - `dashboard` aggregates active tasks directly into the atomic `GET /api/status` telemetry endpoint.
   - Zero browser-to-brain direct network exposure, with full graceful fallback if `brain` restarts.

5. **Security & Zero Token Leakage Invariants**:
   - Strict sanitization of all prompt snippets and metadata using token regex redaction before API delivery.
   - Full client-side HTML escaping (`escapeHtml`) preventing any XSS vulnerability.
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
   │     • Direct Action: [ 💬 INSPECT IN AGENTSVIEW ↗ ]                      │
   └─────────────────────────────────────┬────────────────────────────────────┘
                                         │ HTTP GET /api/status (every 5s)
                                         ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                      aerial-dashboard (Go BFF)                           │
   │   - Calls GET http://aerial-brain:8080/tasks                             │
   │   - Aggregates active tasks with Docker services and GHCR deployments    │
   │   - In-memory caching & graceful error fallback                          │
   └─────────────────────────────────────┬────────────────────────────────────┘
                                         │ HTTP GET /tasks (2s timeout)
                                         ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                       aerial-brain (Go Core Engine)                      │
   │   ┌──────────────────────────────────────────────────────────────────┐   │
   │   │  GET /tasks Handler (Sanitization + 1s TTL Cache)                │   │
   │   └────────────────────────────────┬─────────────────────────────────┘   │
   │                                    │ SQLite Query                        │
   │                                    ▼                                     │
   │   ┌──────────────────────────────────────────────────────────────────┐   │
   │   │  SQLite (/data/aerial.db)                                        │   │
   │   │  - messages (status IN ('PENDING', 'PROCESSING'))                │   │
   │   │  - sessions (LEFT JOIN for internal_session_id mapping)          │   │
   │   └──────────────────────────────────────────────────────────────────┘   │
   └──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Component Specifications

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

#### 2. Database Query (`GetActiveTasks`)
```go
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
	// Executes query, sanitizes prompt content, infers trigger_type, and returns slice
}
```

#### 3. Brain HTTP Endpoint (`GET /tasks`)
```go
func handleTasks(database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Validates GET method
		// Fetches active tasks with 1s cache
		// Redacts sensitive tokens from prompt snippets
		// Returns JSON { "status": "ok", "total": N, "tasks": [...] }
	}
}
```

---

### 4.2. Dashboard BFF Gateway (`dashboard/main.go`)

- Integrates `fetchActiveTasksFromBrain()` into the existing `/api/status` aggregation pipeline.
- Updates `ClusterResponse`:
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
- Graceful degradation: if `brain` does not respond within 2 seconds, returns `ActiveTasks: []` and `ActiveTasksCount: 0` without failing the cluster status response.

---

### 4.3. Frontend User Interface (`index.html`, `style.css`, `app.js`)

#### 1. Top Summary Bar
Adds the `AGENT QUEUE` metric card:
```html
<div class="summary-card">
    <div class="card-glow"></div>
    <span class="label">AGENT QUEUE</span>
    <span class="value text-success" id="summary-tasks-val">0 IDLE</span>
    <span class="sub-label" id="summary-tasks-sub">REAL-TIME DISPATCH</span>
</div>
```

#### 2. Live Agent Execution Queue Section
Inserted immediately above `<section class="deployment-pipeline-section">`:
```html
<section class="active-tasks-section">
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

#### 3. Task Card Component Details
- **Idle State**: Rendered when `active_tasks.length === 0`, displaying a sleek cybernetic status card.
- **Active Card (`.task-card`)**:
  - Laser sweep border animation (`.task-card-laser`) when `status === 'PROCESSING'`.
  - Header: Trigger source badge (`💬 DISCORD`, `⏰ CRON`, `⏱️ REMINDER`, `⚡ API`), author label, status pill (`⚡ RUNNING` / `⏳ QUEUED`), and live elapsed ticker (`⏱ 00:15s` with `data-started`).
  - Body: Prompt snippet with cybernetic block formatting.
  - Action Footer: Primary action button **`💬 INSPECT IN AGENTSVIEW ↗`** linking to `/conversations/?session=<session_id>`.

---

## 5. Verification & Testing Matrix

| Component | Test File | Target Scenario |
| :--- | :--- | :--- |
| Database Layer | `brain/pkg/db/db_test.go` | Query active messages with status `PENDING` / `PROCESSING`, verify `sessions` table join, and test limit handling. |
| Brain API Layer | `brain/main_test.go` | Verify `GET /tasks` returns 200 OK, JSON structure, token redaction, and error handling. |
| Dashboard BFF | `dashboard/main_test.go` | Verify `/api/status` merges brain active tasks, handles brain timeout gracefully, and sanitizes output. |
| End-to-End Build | Local `go test ./...` | Unit tests pass cleanly across `brain` and `dashboard` with zero lint regressions. |

---
