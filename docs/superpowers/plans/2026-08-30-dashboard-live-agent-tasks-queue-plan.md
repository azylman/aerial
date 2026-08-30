# Dashboard Live Agent Task Queue & Execution Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real-time Live Agent Execution Queue section to the top of the Permet HUD Dashboard (above the deployment pipeline) with drift-corrected elapsed timers, trigger source badges, prompt containment, summary bar metric integration, and direct deep-linking into Agentsview transcripts.

**Architecture:** 
1. `brain` adds a SQLite query `GetActiveTasks()` (indexed on `status, created_at`) joining `messages` and `sessions`, exposed via a 1s-cached, token-sanitized `GET /tasks` endpoint.
2. `dashboard` aggregates active tasks into `ClusterResponse` on `GET /api/status` with asynchronous timeout resilience and graceful fallback.
3. The frontend (`index.html`, `style.css`, `app.js`) renders a dedicated `AGENT QUEUE` metric card in the top summary bar and an animated Cyberpunk active task container with zero-flicker live elapsed tickers and secure `agentsview` deep-links.

**Tech Stack:** Go 1.24+, modernc.org/sqlite, HTML5/CSS3 (Cyberpunk/Gundam Aerial HUD), Vanilla JavaScript (ES6+), Agentsview.

**Spec:** [`docs/superpowers/specs/2026-08-30-dashboard-live-agent-tasks-queue-design.md`](file:///share/aerial/docs/superpowers/specs/2026-08-30-dashboard-live-agent-tasks-queue-design.md)

## Global Constraints

- **Zero Plaintext Tokens**: All prompt snippets must be sanitized with `SanitizeString` before any length truncation and before API serialization.
- **100% Generic & Domain-Agnostic**: No hardcoded personal names, Discord handles, or private user logic in any file in `/share/aerial`.
- **XSS Prevention**: All dynamic properties inserted into DOM innerHTML must pass through `escapeHtml()` and `encodeURIComponent()`.
- **No Direct Browser-to-Brain Exposure**: Browser clients talk strictly to `aerial-dashboard` via `aerial-proxy`; `aerial-dashboard` acts as the BFF.
- **Zero Stuck State Invariant**: Gracefully handle missing session IDs during the initial queue allocation window.

---

### Task 1: Database Layer — `ActiveTask` Model, Compound Index & `GetActiveTasks()`

**Files:**
- Modify: `brain/pkg/db/db.go`
- Test: `brain/pkg/db/db_test.go`

**Interfaces:**
- Consumes: SQLite `messages` and `sessions` tables
- Produces: `ActiveTask` struct, `InferTriggerType(authorID, scheduleRunID string) string`, `GetActiveTasks(database *sql.DB) ([]ActiveTask, error)`

- [ ] **Step 1: Write the failing unit test**

In `brain/pkg/db/db_test.go`, add `TestGetActiveTasks`:
```go
func TestGetActiveTasks(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	// Insert test messages with various statuses
	now := time.Now().UTC()
	msgs := []Message{
		{
			ID:          "msg-1",
			ThreadID:    "thread-1",
			AuthorID:    "user-123",
			AuthorName:  "Arcane",
			Content:     "Hello agent",
			Status:      StatusPending,
			CreatedAt:   now.Add(-2 * time.Minute),
			UpdatedAt:   now.Add(-2 * time.Minute),
		},
		{
			ID:            "msg-2",
			ThreadID:      "thread-2",
			AuthorID:      "scheduler",
			AuthorName:    "Scheduler",
			Content:       "Daily backup check",
			Status:        StatusProcessing,
			ScheduleRunID: "cron-abc-123",
			CreatedAt:     now.Add(-1 * time.Minute),
			UpdatedAt:     now.Add(-30 * time.Second),
		},
		{
			ID:          "msg-3",
			ThreadID:    "thread-3",
			AuthorID:    "http-client",
			AuthorName:  "HTTP Client",
			Content:     "API prompt",
			Status:      StatusCompleted,
			CreatedAt:   now.Add(-5 * time.Minute),
			UpdatedAt:   now.Add(-4 * time.Minute),
		},
	}

	for _, m := range msgs {
		if err := InsertMessage(database, m); err != nil {
			t.Fatalf("InsertMessage failed: %v", err)
		}
	}

	// Save session for thread-2
	if err := SaveSessionID(database, "thread-2", "sess-uuid-456"); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}

	tasks, err := GetActiveTasks(database)
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}

	if len(tasks) != 2 {
		t.Fatalf("expected 2 active tasks, got %d", len(tasks))
	}

	// Verify FIFO ordering by created_at ASC
	if tasks[0].ID != "msg-1" || tasks[0].Status != StatusPending || tasks[0].TriggerType != "discord" || tasks[0].SessionID != "" {
		t.Errorf("unexpected task[0]: %+v", tasks[0])
	}

	if tasks[1].ID != "msg-2" || tasks[1].Status != StatusProcessing || tasks[1].TriggerType != "cron" || tasks[1].SessionID != "sess-uuid-456" {
		t.Errorf("unexpected task[1]: %+v", tasks[1])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /share/aerial/brain && go test -v ./pkg/db -run TestGetActiveTasks`
Expected: FAIL (compilation error: `GetActiveTasks` undefined).

- [ ] **Step 3: Write minimal implementation in `brain/pkg/db/db.go`**

In `brain/pkg/db/db.go`:
1. Add compound index in `InitDB`:
```sql
CREATE INDEX IF NOT EXISTS idx_messages_status_created ON messages(status, created_at);
```
2. Define `ActiveTask` struct:
```go
type ActiveTask struct {
	ID            string    `json:"id"`
	ThreadID      string    `json:"thread_id"`
	SessionID     string    `json:"session_id,omitempty"`
	AuthorName    string    `json:"author_name"`
	AuthorID      string    `json:"author_id"`
	Prompt        string    `json:"prompt"`
	Status        string    `json:"status"`
	RetryCount    int       `json:"retry_count"`
	ScheduleRunID string    `json:"schedule_run_id,omitempty"`
	TriggerType   string    `json:"trigger_type"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}
```
3. Add `InferTriggerType` and `GetActiveTasks`:
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
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}

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
		return nil, fmt.Errorf("query active tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]ActiveTask, 0)
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
			return nil, fmt.Errorf("scan active task: %w", err)
		}
		t.TriggerType = InferTriggerType(t.AuthorID, t.ScheduleRunID)
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active tasks: %w", err)
	}
	return tasks, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /share/aerial/brain && go test -v ./pkg/db -run TestGetActiveTasks`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /share/aerial
git add brain/pkg/db/db.go brain/pkg/db/db_test.go
git commit -m "feat(brain): add GetActiveTasks query and compound index"
```

---

### Task 2: Brain HTTP API Layer — `GET /tasks` Endpoint with 1s TTL Cache & Token Sanitization

**Files:**
- Modify: `brain/main.go`
- Test: `brain/main_test.go`

**Interfaces:**
- Consumes: `db.GetActiveTasks()`, `SanitizeString()`
- Produces: `handleTasks(database *sql.DB) http.HandlerFunc`, registered at `/tasks`

- [ ] **Step 1: Write the failing unit test**

In `brain/main_test.go`, add `TestHandleTasks`:
```go
func TestHandleTasks(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	now := time.Now().UTC()
	_ = db.InsertMessage(database, db.Message{
		ID:         "msg-test-task",
		ThreadID:   "thread-1",
		AuthorID:   "user-1",
		AuthorName: "Tester",
		Content:    "Secret token ghp_123456789012345678901234567890123456 in prompt",
		Status:     db.StatusProcessing,
		CreatedAt:  now,
		UpdatedAt:  now,
	})

	handler := handleTasks(database)

	// Test GET request
	req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Status string          `json:"status"`
		Total  int             `json:"total"`
		Tasks  []db.ActiveTask `json:"tasks"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode json: %v", err)
	}

	if resp.Status != "ok" || resp.Total != 1 || len(resp.Tasks) != 1 {
		t.Fatalf("unexpected response payload: %+v", resp)
	}

	// Verify token was redacted
	if strings.Contains(resp.Tasks[0].Prompt, "ghp_123456") {
		t.Errorf("token was not redacted from prompt: %s", resp.Tasks[0].Prompt)
	}

	// Test Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/tasks", nil)
	rrPost := httptest.NewRecorder()
	handler.ServeHTTP(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rrPost.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /share/aerial/brain && go test -v -run TestHandleTasks`
Expected: FAIL (compilation error: `handleTasks` undefined).

- [ ] **Step 3: Implement `handleTasks` and register route in `brain/main.go`**

In `brain/main.go`:
1. Define tasks response types and cache:
```go
type TasksResponse struct {
	Status string          `json:"status"`
	Total  int             `json:"total"`
	Tasks  []db.ActiveTask `json:"tasks"`
}

type tasksCache struct {
	mu        sync.RWMutex
	tasks     []db.ActiveTask
	expiresAt time.Time
}

func handleTasks(database *sql.DB) http.HandlerFunc {
	cache := &tasksCache{}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Method not allowed"})
			return
		}

		now := time.Now().UTC()

		cache.mu.RLock()
		if now.Before(cache.expiresAt) && cache.tasks != nil {
			cached := cache.tasks
			cache.mu.RUnlock()
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(TasksResponse{
				Status: "ok",
				Total:  len(cached),
				Tasks:  cached,
			})
			return
		}
		cache.mu.RUnlock()

		cache.mu.Lock()
		defer cache.mu.Unlock()

		if now.Before(cache.expiresAt) && cache.tasks != nil {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(TasksResponse{
				Status: "ok",
				Total:  len(cache.tasks),
				Tasks:  cache.tasks,
			})
			return
		}

		rawTasks, err := db.GetActiveTasks(database)
		if err != nil {
			log.Printf("[HTTP] Error fetching active tasks: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch active tasks"})
			return
		}

		tasks := make([]db.ActiveTask, 0, len(rawTasks))
		for _, task := range rawTasks {
			sanitizedPrompt := SanitizeString(task.Prompt)
			runes := []rune(sanitizedPrompt)
			if len(runes) > 500 {
				sanitizedPrompt = string(runes[:500]) + "..."
			}
			task.Prompt = sanitizedPrompt
			task.AuthorName = SanitizeString(task.AuthorName)
			tasks = append(tasks, task)
		}

		cache.tasks = tasks
		cache.expiresAt = now.Add(1 * time.Second)

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(TasksResponse{
			Status: "ok",
			Total:  len(tasks),
			Tasks:  tasks,
		})
	}
}
```
2. Register in `main()`:
```go
mux.HandleFunc("/tasks", handleTasks(database))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /share/aerial/brain && go test -v -run TestHandleTasks`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /share/aerial
git add brain/main.go brain/main_test.go
git commit -m "feat(brain): add cached and sanitized GET /tasks endpoint"
```

---

### Task 3: Dashboard BFF Layer — Asynchronous / Cached Tasks Fetching & `ClusterResponse` Integration

**Files:**
- Modify: `dashboard/main.go`
- Test: `dashboard/main_test.go`

**Interfaces:**
- Consumes: `GET http://aerial-brain:8080/tasks`
- Produces: `ActiveTaskStatus` struct, `ClusterResponse.ActiveTasks`, `ClusterResponse.ActiveTasksCount`, `fetchActiveTasksFromBrain(ctx, brainURL)`

- [ ] **Step 1: Write the failing unit test**

In `dashboard/main_test.go`, add `TestStatusHandlerActiveTasks`:
```go
func TestStatusHandlerActiveTasks(t *testing.T) {
	// Mock brain server
	brainMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tasks" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"status": "ok",
				"total": 1,
				"tasks": [
					{
						"id": "task-abc",
						"thread_id": "thread-123",
						"session_id": "session-456",
						"author_name": "Arcane",
						"prompt": "Test execution prompt",
						"status": "PROCESSING",
						"retry_count": 0,
						"trigger_type": "discord",
						"created_at": "2026-08-30T12:00:00Z",
						"updated_at": "2026-08-30T12:00:10Z"
					}
				]
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer brainMock.Close()

	handler := statusHandler(brainMock.URL)
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var resp ClusterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}

	if resp.ActiveTasksCount != 1 || len(resp.ActiveTasks) != 1 {
		t.Fatalf("expected 1 active task, got %d: %+v", resp.ActiveTasksCount, resp.ActiveTasks)
	}

	if resp.ActiveTasks[0].ID != "task-abc" || resp.ActiveTasks[0].SessionID != "session-456" || resp.ActiveTasks[0].TriggerType != "discord" {
		t.Errorf("unexpected task contents: %+v", resp.ActiveTasks[0])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /share/aerial/dashboard && go test -v -run TestStatusHandlerActiveTasks`
Expected: FAIL.

- [ ] **Step 3: Implement `ActiveTaskStatus` and `fetchActiveTasksFromBrain` in `dashboard/main.go`**

In `dashboard/main.go`:
1. Define struct types:
```go
type ActiveTaskStatus struct {
	ID          string    `json:"id"`
	ThreadID    string    `json:"thread_id"`
	SessionID   string    `json:"session_id,omitempty"`
	AuthorName  string    `json:"author_name"`
	Prompt      string    `json:"prompt"`
	Status      string    `json:"status"`
	RetryCount  int       `json:"retry_count"`
	TriggerType string    `json:"trigger_type"`
	CreatedAt   time.Time `json:"created_at"`
	StartedAt   time.Time `json:"started_at"`
}

type BrainTasksAPIResponse struct {
	Status string `json:"status"`
	Total  int    `json:"total"`
	Tasks  []struct {
		ID            string    `json:"id"`
		ThreadID      string    `json:"thread_id"`
		SessionID     string    `json:"session_id"`
		AuthorName    string    `json:"author_name"`
		Prompt        string    `json:"prompt"`
		Status        string    `json:"status"`
		RetryCount    int       `json:"retry_count"`
		TriggerType   string    `json:"trigger_type"`
		CreatedAt     time.Time `json:"created_at"`
		UpdatedAt     time.Time `json:"updated_at"`
	} `json:"tasks"`
}
```
2. Update `ClusterResponse`:
```go
type ClusterResponse struct {
	SystemTime       time.Time          `json:"system_time"`
	ClusterStatus    string             `json:"cluster_status"`
	ActiveTasksCount int                `json:"active_tasks_count"`
	ActiveTasks      []ActiveTaskStatus `json:"active_tasks"`
	Services         []ServiceStatus    `json:"services"`
	Deployments      []DeploymentStatus `json:"deployments"`
}
```
3. Implement `fetchActiveTasksFromBrain`:
```go
func fetchActiveTasksFromBrain(ctx context.Context, brainURL string) ([]ActiveTaskStatus, error) {
	if brainURL == "" {
		return []ActiveTaskStatus{}, nil
	}

	reqURL := strings.TrimRight(brainURL, "/") + "/tasks"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return []ActiveTaskStatus{}, err
	}

	resp, err := brainHTTPClient.Do(req)
	if err != nil {
		return []ActiveTaskStatus{}, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return []ActiveTaskStatus{}, fmt.Errorf("brain returned HTTP %d", resp.StatusCode)
	}

	var apiResp BrainTasksAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return []ActiveTaskStatus{}, err
	}

	tasks := make([]ActiveTaskStatus, 0, len(apiResp.Tasks))
	for _, t := range apiResp.Tasks {
		startedAt := t.UpdatedAt
		if t.Status == "PENDING" || startedAt.IsZero() {
			startedAt = t.CreatedAt
		}
		tasks = append(tasks, ActiveTaskStatus{
			ID:          t.ID,
			ThreadID:    t.ThreadID,
			SessionID:   t.SessionID,
			AuthorName:  t.AuthorName,
			Prompt:      t.Prompt,
			Status:      t.Status,
			RetryCount:  t.RetryCount,
			TriggerType: t.TriggerType,
			CreatedAt:   t.CreatedAt,
			StartedAt:   startedAt,
		})
	}
	return tasks, nil
}
```
4. Update `statusHandler` signature and execution:
```go
func statusHandler(brainURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Concurrent fetch of Docker status, deployments, and active tasks
		// Gracefully degrade ActiveTasks to empty slice on error
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /share/aerial/dashboard && go test -v ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /share/aerial
git add dashboard/main.go dashboard/main_test.go
git commit -m "feat(dashboard): aggregate active tasks into /api/status response"
```

---

### Task 4: Frontend HTML Structure & Cyberpunk Styling

**Files:**
- Modify: `dashboard/static/index.html`
- Modify: `dashboard/static/style.css`

**Interfaces:**
- Consumes: Permet HUD CSS classes
- Produces: `#summary-tasks-val`, `#summary-tasks-sub`, `#tasks-count-badge`, `#active-tasks-container`, `.active-tasks-section`, `.task-card`, `.task-idle-card`, `.task-inspect-btn`, `.task-prompt-box`

- [ ] **Step 1: Update `.summary-bar` in `dashboard/static/index.html`**

Insert the `AGENT QUEUE` metric card as Card #2 (right after `CLUSTER STATUS`):
```html
<div class="summary-card">
    <div class="card-glow"></div>
    <span class="label">AGENT QUEUE</span>
    <span class="value text-success" id="summary-tasks-val">0 IDLE</span>
    <span class="sub-label" id="summary-tasks-sub">REAL-TIME DISPATCH</span>
</div>
```

- [ ] **Step 2: Add `<section class="active-tasks-section">` in `dashboard/static/index.html`**

Insert immediately above `<section class="deployment-pipeline-section">`:
```html
<!-- LIVE AGENT EXECUTION QUEUE SECTION -->
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

- [ ] **Step 3: Add CSS classes in `dashboard/static/style.css`**

Add styling rules for `.active-tasks-section`, `.active-tasks-container`, `.task-card`, `.status-processing`, `.status-pending`, `.task-card-laser`, `.task-prompt-box`, `.task-inspect-btn`, `.task-idle-card`:
```css
/* --- LIVE AGENT TASK QUEUE --- */
.active-tasks-section {
    margin-bottom: 2.5rem;
    padding-bottom: 1.5rem;
    border-bottom: 1px dashed rgba(0, 242, 254, 0.15);
}

.active-tasks-container {
    display: flex;
    flex-direction: column;
    gap: 1rem;
}

.task-card {
    background: var(--card-bg);
    border: 1px solid rgba(0, 242, 254, 0.25);
    border-radius: 12px;
    padding: 1.25rem 1.5rem;
    backdrop-filter: var(--glass-backdrop);
    box-shadow: 0 0 20px rgba(0, 242, 254, 0.08);
    position: relative;
    overflow: hidden;
    transition: all 0.25s ease;
}

.task-card.status-processing {
    border-color: var(--neon-cyan);
    box-shadow: 0 0 25px rgba(0, 242, 254, 0.18);
}

.task-card.status-pending {
    border-color: rgba(255, 183, 0, 0.3);
    box-shadow: 0 0 15px rgba(255, 183, 0, 0.08);
}

.task-card-laser {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--neon-cyan), var(--neon-purple), transparent);
    animation: laser-sweep 2.5s infinite linear;
}

.task-card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
}

.task-meta-left {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    flex-wrap: wrap;
}

.task-trigger-badge {
    font-size: 0.75rem;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 4px;
    background: rgba(0, 242, 254, 0.1);
    color: var(--neon-cyan);
    border: 1px solid rgba(0, 242, 254, 0.3);
    letter-spacing: 0.5px;
}

.task-author {
    font-size: 0.85rem;
    font-weight: 700;
    color: #fff;
    font-family: var(--font-heading);
}

.task-prompt-box {
    background: rgba(0, 0, 0, 0.4);
    border-left: 3px solid var(--neon-cyan);
    padding: 0.75rem 1rem;
    margin: 0.75rem 0;
    font-family: var(--font-mono);
    font-size: 0.82rem;
    line-height: 1.45;
    color: var(--text-primary);
    border-radius: 0 6px 6px 0;
    max-height: 4.8rem;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 3;
    -webkit-box-orient: vertical;
    word-break: break-word;
    overflow-wrap: anywhere;
}

.task-card-footer {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.75rem;
    margin-top: 0.75rem;
    padding-top: 0.75rem;
    border-top: 1px solid rgba(255, 255, 255, 0.06);
}

.task-inspect-btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 0.45rem 1rem;
    border-radius: 6px;
    font-size: 0.8rem;
    font-weight: 700;
    text-decoration: none;
    transition: all 0.2s ease;
}

.task-inspect-btn.active {
    background: rgba(0, 242, 254, 0.12);
    border: 1px solid var(--neon-cyan);
    color: var(--neon-cyan);
}

.task-inspect-btn.active:hover {
    background: rgba(0, 242, 254, 0.25);
    color: #fff;
    box-shadow: 0 0 15px rgba(0, 242, 254, 0.4);
}

.task-inspect-btn.disabled {
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.1);
    color: var(--text-dim);
    cursor: not-allowed;
}

.task-idle-card {
    background: var(--card-bg);
    border: 1px solid var(--card-border);
    border-radius: 12px;
    padding: 1.5rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    backdrop-filter: var(--glass-backdrop);
    color: var(--text-muted);
    font-size: 0.85rem;
    letter-spacing: 1px;
}
```

- [ ] **Step 4: Commit**

```bash
cd /share/aerial
git add dashboard/static/index.html dashboard/static/style.css
git commit -m "feat(dashboard): add active tasks HUD markup and cyberpunk styles"
```

---

### Task 5: Frontend Dynamic Rendering, Zero-Flicker Timers & Agentsview Deep-Linking

**Files:**
- Modify: `dashboard/static/app.js`

**Interfaces:**
- Consumes: `data.active_tasks`, `data.active_tasks_count` from `GET /api/status`
- Produces: `renderActiveTasks(tasks)`, `formatElapsedTicker(seconds)`, updated `fetchStatus()`

- [ ] **Step 1: Implement `formatElapsedTicker` and timer helper functions**

In `dashboard/static/app.js`:
```javascript
function formatElapsedTicker(seconds) {
    if (seconds == null || isNaN(seconds) || seconds < 0) return '⏱ 00:00s';
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);

    if (hrs > 0) {
        return `⏱ ${String(hrs).padStart(2, '0')}:${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
    }
    return `⏱ ${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}s`;
}
```

- [ ] **Step 2: Implement `renderActiveTasks(tasks)`**

In `dashboard/static/app.js`:
```javascript
function renderActiveTasks(tasks) {
    const container = document.getElementById('active-tasks-container');
    const badge = document.getElementById('tasks-count-badge');
    const summaryVal = document.getElementById('summary-tasks-val');
    const summarySub = document.getElementById('summary-tasks-sub');

    const activeList = tasks || [];
    const runningCount = activeList.filter(t => t.status === 'PROCESSING').length;
    const pendingCount = activeList.filter(t => t.status === 'PENDING').length;
    const totalCount = activeList.length;

    // Update Summary Bar
    if (summaryVal) {
        if (runningCount > 0) {
            summaryVal.textContent = `${runningCount} RUNNING`;
            summaryVal.className = 'value text-cyan';
        } else if (pendingCount > 0) {
            summaryVal.textContent = `${pendingCount} QUEUED`;
            summaryVal.className = 'value text-warning';
        } else {
            summaryVal.textContent = '0 IDLE';
            summaryVal.className = 'value text-success';
        }
    }
    if (summarySub) {
        summarySub.textContent = totalCount > 0 ? `${totalCount} ACTIVE IN QUEUE` : 'REAL-TIME DISPATCH';
    }

    // Update Section Badge
    if (badge) {
        if (runningCount > 0) {
            badge.textContent = `⚡ ${runningCount} RUNNING` + (pendingCount > 0 ? ` (+${pendingCount} QUEUED)` : '');
            badge.className = 'section-badge active';
        } else if (pendingCount > 0) {
            badge.textContent = `⏳ ${pendingCount} IN QUEUE`;
            badge.className = 'section-badge building';
        } else {
            badge.textContent = '0 IN PROGRESS';
            badge.className = 'section-badge';
        }
    }

    if (!container) return;

    if (activeList.length === 0) {
        container.innerHTML = `
            <div class="task-idle-card">
                <div class="idle-indicator">
                    <span class="pulse-dot healthy"></span>
                    <span>ALL WORKERS IDLE // NO PENDING TURNS</span>
                </div>
                <div>DISPATCH POLLING SQLITE QUEUE (1s)</div>
            </div>
        `;
        return;
    }

    const now = Date.now();
    container.innerHTML = activeList.map(task => {
        const isProcessing = task.status === 'PROCESSING';
        const isPending = task.status === 'PENDING';
        const statusClass = isProcessing ? 'status-processing' : 'status-pending';
        const statusBadge = isProcessing ? '⚡ RUNNING' : '⏳ QUEUED';

        const authorSafe = escapeHtml(task.author_name || 'System');
        const promptSafe = escapeHtml(task.prompt || '');
        const triggerSafe = escapeHtml((task.trigger_type || 'task').toUpperCase());
        const threadSafe = escapeHtml(task.thread_id || '');

        let timerHTML = '';
        if (isProcessing && task.started_at) {
            const startedMs = new Date(task.started_at).getTime();
            const initialSec = !isNaN(startedMs) ? Math.max(0, Math.floor((now - startedMs) / 1000)) : 0;
            const formattedTime = formatElapsedTicker(initialSec);
            timerHTML = `
                <div class="deploy-timer-badge">
                    <span class="pulse-indicator"></span>
                    <span class="timer-text" data-started="${escapeHtml(task.started_at)}">${formattedTime}</span>
                </div>
            `;
        }

        const retryHTML = task.retry_count > 0 
            ? `<span class="matrix-chip chip-failed" title="Retry Attempt">🔄 RETRY #${task.retry_count}</span>` 
            : '';

        const inspectHTML = task.session_id 
            ? `<a href="/conversations/?session=${encodeURIComponent(task.session_id)}" target="_blank" rel="noopener noreferrer" class="task-inspect-btn active">💬 INSPECT IN AGENTSVIEW ↗</a>`
            : `<span class="task-inspect-btn disabled">⏳ QUEUE ALLOCATING SESSION</span>`;

        return `
            <div class="task-card ${statusClass}">
                ${isProcessing ? '<div class="task-card-laser"></div>' : ''}
                <div class="task-card-header">
                    <div class="task-meta-left">
                        <span class="task-trigger-badge">${triggerSafe}</span>
                        <span class="task-author">${authorSafe}</span>
                        ${retryHTML}
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        ${timerHTML}
                        <span class="deploy-stage-badge ${isProcessing ? 'active' : 'building'}">${statusBadge}</span>
                    </div>
                </div>
                <div class="task-prompt-box">
                    &gt; ${promptSafe}
                </div>
                <div class="task-card-footer">
                    <span style="font-size: 0.75rem; color: var(--text-dim); font-family: var(--font-mono);">THREAD: ${threadSafe || 'N/A'}</span>
                    ${inspectHTML}
                </div>
            </div>
        `;
    }).join('');
}
```

- [ ] **Step 3: Wire `renderActiveTasks` into `fetchStatus()` in `dashboard/static/app.js`**

Inside `fetchStatus()`:
```javascript
// --- RENDER LIVE AGENT EXECUTION QUEUE ---
renderActiveTasks(data.active_tasks);
```

- [ ] **Step 4: Commit**

```bash
cd /share/aerial
git add dashboard/static/app.js
git commit -m "feat(dashboard): implement live task rendering with zero-flicker timers"
```

---

### Task 6: Full Verification, Test Suite & Self-Review

**Files:**
- Test: `brain/...`, `dashboard/...`

- [ ] **Step 1: Run all unit tests in `brain`**

Run: `cd /share/aerial/brain && go test -v ./...`
Expected: ALL PASS.

- [ ] **Step 2: Run all unit tests in `dashboard`**

Run: `cd /share/aerial/dashboard && go test -v ./...`
Expected: ALL PASS.

- [ ] **Step 3: Verify git status and clean working tree**

Run: `cd /share/aerial && git status`
Expected: Clean working tree.

- [ ] **Step 4: Commit any final test adjustments**

```bash
cd /share/aerial
git commit -m "test(all): complete test suite pass for live agent queue HUD" --allow-empty
```

---
