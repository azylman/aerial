# Telemetry Triage Skill, Progressive Capacity Backoff, and Session Transcript Tool Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide an overarching telemetry triage runbook for all failures and performance questions, scale capacity blip backoff progressively to 30s/60s, and eliminate the Groundhog Day retry loop by compacting on-disk tool actions during session rotation.

**Architecture:** Update `.agents/skills/discord/incident-triage/SKILL.md` with dynamic component discovery and metric-first hierarchy; update `brain/pkg/queue/worker.go` capacity retry backoff to 30s/60s; implement `ExtractTranscriptToolSummary` in `brain/pkg/session/session.go` and wire it into worker session rotation.

**Tech Stack:** Go standard library (`os`, `path/filepath`, `strings`, `time`, `encoding/json`, `fmt`), VictoriaMetrics MCP, OpenObserve MCP, Docker MCP.

**Spec:** `docs/superpowers/specs/2026-09-28-telemetry-triage-and-capacity-compaction.md`

## Global Constraints

- Pure Go standard library for concurrency, session parsing, and backoff timing.
- Strictly ZERO markdown tables across all artifacts, messages, commits, code comments, and summaries (bulleted lists only).
- Strictly ZERO swallowed errors. Every error path must be logged with structured context or propagated.
- Strictly ZERO host memory inspection (`/proc/meminfo` reading, string parsing, or memory pressure eviction).
- Maintain statement coverage floor of strictly >= 95.0% across `brain/pkg/session` and `brain/pkg/queue`.
- All staged changes must pass fast pre-commit verification (`powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`).
- Always address Alex directly.

---

### Task 1: Core Skill Overhaul - Generic Telemetry Triage Runbook

**Files:**
- Modify: `.agents/skills/discord/incident-triage/SKILL.md`
- Test: `brain/pkg/env/skills_test.go` (existing skill provisioning tests)

**Interfaces:**
- Consumes: `docker-mcp:list_containers`, `victoriametrics:query_range` / `query`, `openobserve:SearchSQL`, `docker-mcp:fetch_container_logs`.
- Produces: `.agents/skills/discord/incident-triage/SKILL.md` (updated runbook specification).

- [ ] **Step 1: Update YAML frontmatter with broadened trigger**
  - Update `description` to trigger on:
    - Any task failure, unhandled error, or crashed job across any service.
    - Questions regarding performance, latency, durations, voice TTFR, or timings (e.g. "how long did X take", "why was voice slow", "check latency").
    - Discord message dropouts, misrouted turns, or silent replies.
  - Enforce the Two-Repository Boundary: strictly generic core runbook, zero hardcoded non-core service names.

- [ ] **Step 2: Add Phase 1: Dynamic Component Discovery**
  - Instruct the agent to inspect the user's prompt for mentioned features, clients, or sidecars.
  - If a non-core feature or external client is referenced (e.g. "kiosk", "dashboard", "lights"):
    - Query `docker-mcp:list_containers` to discover active container names.
    - Map the user's intent to actual container names and dynamically bind `<target_services>`.

- [ ] **Step 3: Restructure Telemetry Hierarchy (VictoriaMetrics -> OpenObserve -> Docker)**
  - **Phase 2: Universal Metric Triage (VictoriaMetrics MCP)**:
    - State that VictoriaMetrics is the cluster-wide TSDB containing metrics from all containers.
    - Query capability metrics (`aerial_brain_*` like `aerial_brain_voice_ttfr_duration_seconds`) or cAdvisor container metrics (`container_cpu_*`, `container_memory_*`).
    - Use metrics to pinpoint the exact microsecond event timestamp and duration in <1s (~200 tokens).
  - **Phase 3: Scoped Log Forensics (OpenObserve MCP)**:
    - Query OpenObserve stream `docker_logs` parameterized by service: `WHERE service IN ('brain', '<target_services>')`.
    - Apply a 2–5 minute time window centered on the event timestamp.
    - Default to `LIMIT 100` (~5,000 tokens) to capture complete lifecycles without token bloat. If noisy, filter by `level IN ('warn', 'error')` or message keywords.
  - **Phase 4: Raw Docker Logs (Docker MCP)**:
    - Strictly a last-resort fallback if OpenObserve or Vector is offline. Never dump unbounded logs.

- [ ] **Step 4: Verify skill file validation test passes**
  - Run: `go test -C brain -v ./pkg/env -run TestProvisioner_LinkTargetSkills`
  - Expected: PASS (verifies non-empty `SKILL.md` and symlinking).

- [ ] **Step 5: Commit changes**
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add .agents/skills/discord/incident-triage/SKILL.md`
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(skills): generalize incident-triage with dynamic discovery and metric-first hierarchy"`

---

### Task 2: Queue Subsystem - Progressive Capacity Backoff Delay

**Files:**
- Modify: `brain/pkg/queue/worker.go:1802-1807`
- Test: `brain/pkg/queue/worker_test.go`

**Interfaces:**
- Consumes: `attempt int`, `resetDur time.Duration`, `te.threadID string`.
- Produces: Progressive minimum delay floor of `attempt * 30s` during capacity throttle retries.

- [ ] **Step 1: Write failing unit test for progressive capacity backoff**
  - In `brain/pkg/queue/worker_test.go`:
    - Add `TestWorkerPool_ProgressiveCapacityBackoff`:
      - Verify that on Attempt 1, minimum backoff floor is 30 seconds.
      - Verify that on Attempt 2, minimum backoff floor is 60 seconds.
      - Verify that if `resetDur` exceeds the floor (e.g. 75s), `delay` uses `resetDur`.

- [ ] **Step 2: Run test to verify RED failure**
  - Run: `go test -C brain -v ./pkg/queue -run TestWorkerPool_ProgressiveCapacityBackoff`
  - Expected: FAIL (currently clamps to 2 seconds).

- [ ] **Step 3: Implement progressive backoff calculation in `worker.go`**
  - In `brain/pkg/queue/worker.go:1802-1807`:
    ```go
    minFloor := time.Duration(attempt) * 30 * time.Second
    delay := resetDur
    if delay < minFloor {
        delay = minFloor
    }
    delay += time.Duration(rand.Intn(3000)) * time.Millisecond
    ```

- [ ] **Step 4: Run test to verify GREEN pass and statement coverage >= 95.0%**
  - Run: `go test -C brain -v -cover ./pkg/queue -run TestWorkerPool_ProgressiveCapacityBackoff`
  - Expected: PASS.

- [ ] **Step 5: Commit changes**
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/pkg/queue/worker.go brain/pkg/queue/worker_test.go`
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(queue): scale capacity blip backoff delay progressively to 30s and 60s"`

---

### Task 3: Session Subsystem - Transcript Tool Action Compaction

**Files:**
- Modify: `brain/pkg/session/session.go`
- Test: `brain/pkg/session/session_test.go`

**Interfaces:**
- Produces: `func (m *Manager) ExtractTranscriptToolActions(convID string) string`
- Returns: Structured `<PREVIOUS_TURN_ACTIONS>` block or empty string if no tools were executed.

- [ ] **Step 1: Write failing unit test for `ExtractTranscriptToolActions`**
  - In `brain/pkg/session/session_test.go`:
    - Add `TestManager_ExtractTranscriptToolActions`:
      - Case 1: Empty transcript -> returns `""`.
      - Case 2: Transcript with user input and subsequent `PLANNER_RESPONSE` (`tool_calls: [{"name":"run_command","args":{"CommandLine":"git status"}}]`) followed by `GENERIC` tool result -> returns structured `<PREVIOUS_TURN_ACTIONS>` containing tool name and snippet.
      - Case 3: Transcript with multiple tool calls across turns -> only extracts tool calls from the latest active turn (after the last non-ambient `USER_INPUT`).
      - Case 4: Output length clamped to 2,000 characters to prevent summary bloat.

- [ ] **Step 2: Run test to verify RED failure**
  - Run: `go test -C brain -v ./pkg/session -run TestManager_ExtractTranscriptToolActions`
  - Expected: FAIL (`ExtractTranscriptToolActions` undefined).

- [ ] **Step 3: Implement `ExtractTranscriptToolActions` in `session.go`**
  - In `brain/pkg/session/session.go`:
    - Parse `transcript_full.jsonl` / `transcript.jsonl` for `convID`.
    - Find the line index of the last non-ambient `USER_INPUT`.
    - Collect tool names, command lines, and tool outputs from subsequent `PLANNER_RESPONSE` and `GENERIC` lines.
    - Security / Prompt Injection Invariant: Sanitize all extracted command lines and tool outputs using `sanitizer.SanitizePromptTags` (or escaping `</PREVIOUS_TURN_ACTIONS>`) to prevent breakout injection from log or file outputs.
    - Format into generic action block:
      ```markdown
      <PREVIOUS_TURN_ACTIONS>
      The previous attempt in this thread executed the following actions before session rotation:
      - Action: <tool_name> | Command/Args: <command_line> | Status: <status> | Result: <snippet>
      Do NOT repeat these exact actions. Use these results to proceed with the request or synthesize the final answer.
      </PREVIOUS_TURN_ACTIONS>
      ```
    - Clamp total snippet length to 2,000 characters.

- [ ] **Step 4: Run tests to verify GREEN pass and statement coverage >= 95.0%**
  - Run: `go test -C brain -v -cover ./pkg/session`
  - Expected: PASS with coverage >= 95.0%.

- [ ] **Step 5: Commit changes**
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/pkg/session/session.go brain/pkg/session/session_test.go`
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(session): extract completed tool calls and findings from session transcript"`

---

### Task 4: Queue Subsystem - Wire Tool Compaction into Session Rotation & Prompt

**Files:**
- Modify: `brain/pkg/queue/worker.go`
- Test: `brain/pkg/queue/worker_test.go`

**Interfaces:**
- Consumes: `session.Manager.ExtractTranscriptToolActions`, `te.currentSessionID`.
- Produces: `turnPrompt` enriched with `<PREVIOUS_TURN_ACTIONS>` across retry attempts after session rotation.

- [ ] **Step 1: Write failing unit test for tool summary injection on rotated retry**
  - In `brain/pkg/queue/worker_test.go`:
    - Add `TestWorkerPool_SessionRotationInjectsToolActions`:
      - Mock a turn that executes tools, writes to transcript, and then hits a quota pause exceeding safety guardrails (`pauseSteps >= DefaultMaxSessionSteps`).
      - Verify that session is rotated to cold state, but Attempt 2's prepared prompt contains `<PREVIOUS_TURN_ACTIONS>` with the tool findings from Attempt 1.
      - Verify context retention: verify that if Attempt 2 subsequently retries locally, `<PREVIOUS_TURN_ACTIONS>` is retained and not prematurely cleared.

- [ ] **Step 2: Run test to verify RED failure**
  - Run: `go test -C brain -v ./pkg/queue -run TestWorkerPool_SessionRotationInjectsToolActions`
  - Expected: FAIL.

- [ ] **Step 3: Implement tool compaction latching in `worker.go`**
  - Add `previousTurnActions string` field to `turnExecution` struct.
  - In `worker.go` quota pause rotation (around line 1765) and transient error rotation (around line 2440):
    - Before calling `te.rotateSessionID(te.threadID, "")` and clearing `te.currentSessionID`:
      ```go
      if te.currentSessionID != "" && te.pool != nil && te.pool.sessionMgr != nil {
          if toolActions := te.pool.sessionMgr.ExtractTranscriptToolActions(te.currentSessionID); toolActions != "" {
              // If actions trace is lengthy, condense with low-effort model via ephemeral:summarizer; otherwise preserve raw block
              te.previousTurnActions = te.condenseTurnActions(toolActions)
              log.Printf("[WorkerPool] Preserved %d bytes of turn actions for rotated session %s", len(te.previousTurnActions), te.currentSessionID)
          }
      }
      ```
  - Implement helper `condenseTurnActions(rawActions string) string`:
    - If `len(rawActions) <= 1500`, return `rawActions` directly (zero LLM overhead).
    - If `len(rawActions) > 1500`, run a single fast condensation call through `ephemeral:summarizer` bound to `cur.LowEffortModel`.
    - Zero Swallowed Errors Invariant: If condensation fails or times out, log structured warning `log.Printf("[WorkerPool] Warning: failed to condense turn actions with low-effort model: %v. Falling back to clamped raw actions.", err)` and return the clamped `rawActions`.
  - In `worker.go:1320` (`preparePrompt`):
    - If `te.previousTurnActions != ""` and prompt does not already contain it:
      ```go
      prompt = prompt + "\n\n" + te.previousTurnActions
      // Do NOT clear te.previousTurnActions here to ensure local transient retries retain the compacted context.
      // Lifecycle is naturally collected when turnExecution ends.
      ```
  - Verify Discord typing persistence: confirm `te.stopTyping()` is not called during `time.After(delay)` so the typing heartbeat pulses continuously through the 30s/60s sleep.

- [ ] **Step 4: Run tests to verify GREEN pass and statement coverage >= 95.0%**
  - Run: `go test -C brain -v -cover ./pkg/queue`
  - Expected: PASS with coverage >= 95.0%.

- [ ] **Step 5: Commit changes**
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/pkg/queue/worker.go brain/pkg/queue/worker_test.go`
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(queue): inject transcript tool actions into prompt on session rotation"`

---

### Task 5: Comprehensive Regression & Statement Coverage Verification

**Files:**
- Verify across: `.agents/skills/discord/incident-triage`, `brain/pkg/session`, `brain/pkg/queue`, `brain/pkg/env`.

- [ ] **Step 1: Run race detection across modified packages**
  - Run: `go test -C brain -race ./pkg/session ./pkg/queue ./pkg/env`
  - Expected: PASS with zero race warnings.

- [ ] **Step 2: Run statement coverage verification**
  - Run: `go test -C brain -cover ./pkg/session` (Verify >= 95.0%)
  - Run: `go test -C brain -cover ./pkg/queue` (Verify >= 95.0%)
  - Run: `go test -C brain -cover ./pkg/env` (Verify >= 95.0%)

- [ ] **Step 3: Run fast pre-commit verification**
  - Run: `powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`
  - Expected: All fast lint and verification checks pass.

- [ ] **Step 4: Commit any remaining test boosters if needed**
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "test: verify statement coverage and race safety across telemetry triage and compaction"`
