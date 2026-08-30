# Technical Specification: Active Tasks Human-Friendly Summaries & Agentsview Deep-Links

## 1. Problem Statement & Motivation
Users inspecting the Permet HUD Dashboard reported two issues with the Live Agent Execution Queue:
1. **Broken Agentsview Deep-Links**: Clicking "INSPECT IN AGENTSVIEW ↗" routed to `/conversations/?session=<session_id>`, which redirected to the root `/sessions?date_from=...` instead of directly loading the target session transcript at `/sessions/<session_id>`.
2. **Raw XML Boilerplate in Task Cards**: The task cards displayed raw unparsed prompt strings (`<USER_REQUEST>\nHere's a message someone sent you from Discord...`) rather than a concise, human-readable summary of the work.

## 2. Architectural Design

### 2.1 Database & Persistence Layer (`brain/pkg/db`)
1. **Schema Migration**:
   - Add `summary TEXT NOT NULL DEFAULT ''` to the `messages` table in `InitDB`.
2. **Data Model**:
   - Update `Message` struct with `Summary string \`json:"summary"\``.
   - Update `ActiveTask` struct with `Summary string \`json:"summary"\``.
3. **Summary Extraction Helper (`CleanTaskSummary`)**:
   - Strips XML tags (`<USER_REQUEST>`, `<ADDITIONAL_METADATA>`, etc.).
   - Extracts Discord `content: <text>` field if present in structured message wrappers.
   - Cleans Markdown headings, excess whitespace, and newlines.
   - Truncates to at most 140 runes on word boundaries.
4. **Active Tasks Query (`GetActiveTasks`)**:
   - Selects `COALESCE(m.summary, '') AS summary`.
   - Computes fallback summary if `summary` column is empty for legacy rows.

### 2.2 Ingestion Points
1. **Discord Funnel (`brain/funnel.go`)**:
   - Sets `msg.Summary = db.CleanTaskSummary(m.Content)` using the user's raw message text before prompt envelope wrapping.
2. **Scheduler Monitor (`brain/pkg/scheduler/scheduler.go`)**:
   - Recurring Crons: Sets `msg.Summary = fmt.Sprintf("[%s] %s", c.TitlePrefix, db.CleanTaskSummary(c.Prompt))`.
   - One-Shot Reminders: Sets `msg.Summary = fmt.Sprintf("[Reminder] %s", db.CleanTaskSummary(s.Prompt))`.
3. **HTTP Prompt Endpoint (`brain/main.go`)**:
   - Sets `msg.Summary = db.CleanTaskSummary(req.Prompt)`.

### 2.3 API & BFF Layer (`brain/main.go` & `dashboard/main.go`)
1. `GET /tasks` on `aerial-brain` sanitizes and serializes `summary`.
2. `GET /api/status` on `aerial-dashboard` aggregates `ActiveTaskStatus.Summary`.

### 2.4 Frontend Layer (`dashboard/static/app.js`)
1. **Agentsview Deep-Link**:
   - Update URL format to `/sessions/${encodeURIComponent(task.session_id)}`.
2. **Task Card Rendering**:
   - Display `task.summary` (fallback to `task.prompt`) inside `.task-prompt-box`.

## 3. Invariants & Security
- **Zero Plaintext Tokens**: Summaries pass through `SanitizeString` before storage and before API response serialization.
- **100% Generic & Domain-Agnostic**: No hardcoded personal names in tests or code in `/share/aerial`.
- **XSS Prevention**: Summaries pass through `escapeHtml()` in `app.js`.
