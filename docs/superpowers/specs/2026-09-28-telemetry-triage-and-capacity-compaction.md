# Architectural Specification: Telemetry Triage Skill, Progressive Capacity Backoff, and Session Transcript Tool Compaction

- **Date**: 2026-09-28
- **Author**: Alex Zylman & Antigravity
- **Status**: Approved Design
- **Target Subsystems**: `.agents/skills/discord/incident-triage`, `brain/pkg/session`, `brain/pkg/queue`

---

## 1. Overview & Problem Statement

Recent operational analysis revealed three systemic vulnerabilities during complex diagnostic queries:
- **Unguided Forensic Exploration**: When asked about system performance or latency (e.g. kiosk voice timing), the agent lacked an active skill and defaulted to dumping raw Docker container logs via Docker MCP. This burned tens of thousands of tokens, bloated the context window, and led to a 167-step runaway loop.
- **Premature Capacity Blip Retry (The 2-Second Trap)**: When Gemini returned a 429 capacity throttle, the worker clamped the backoff floor to 2 seconds. Because Gemini rate limits operate on a rolling 60-second window, retrying 2 seconds later hit the exact same saturated bucket.
- **The Groundhog Day Retry Loop**: When a session exceeded safety thresholds during a quota pause, the worker wiped `te.currentSessionID = ""`. This started the next retry attempt in a completely blank session with 0 context, causing the agent to repeat the exact same tool actions from step 1.

This specification addresses all three root causes without force-killing turns or violating core repository boundaries.

---

## 2. Subsystem 1: Generic Telemetry Triage Skill (`incident-triage`)

### 2.1 Scope & Two-Repository Boundary
- The skill lives in `.agents/skills/discord/incident-triage/SKILL.md` within the `aerial` repository.
- It is strictly generic and core-contained. It MUST NOT hardcode names of non-core client containers, overlays, or private environments.
- All containers on the host (core and non-core) ship logs to OpenObserve (`docker_logs`) and metrics to VictoriaMetrics (via cAdvisor and Prometheus).

### 2.2 Broadened Activation Triggers
The YAML frontmatter description is updated to trigger on:
- Any task failure, error, or unhandled exception (not just ones with explicit Discord links).
- Performance, latency, and duration inquiries across any service (e.g. voice TTFR, API latencies, queue delays).
- Discord message dropouts, routing anomalies, or silent turns.

### 2.3 Strict Telemetry Tool Hierarchy
The operational protocol enforces a mandatory order of operations:
- **Phase 1: Dynamic Component Discovery**:
  - If the user refers to an external client, component, or feature, the agent queries `docker-mcp:list_containers` to map the user's intent to actual active container names.
  - Resolves target container names dynamically into `<target_services>`.
- **Phase 2: Universal Metric Triage (VictoriaMetrics MCP)**:
  - VictoriaMetrics is the cluster-wide time-series database.
  - Queries TSDB metrics first: capability metrics (`aerial_brain_*` such as `aerial_brain_voice_ttfr_duration_seconds`), application Prometheus endpoints, or cAdvisor metrics (`container_cpu_*`, `container_memory_*`).
  - Pinpoints the exact microsecond timestamp of the event and its duration in <1 second (~200 tokens).
- **Phase 3: Scoped Log Forensics (OpenObserve MCP)**:
  - Queries OpenObserve `docker_logs` parameterized by service: `WHERE service IN ('brain', '<target_services>')`.
  - Strictly time-bounded to a 2–5 minute window around the metric event (`_timestamp >= <start> AND _timestamp <= <end>`).
  - Log limit set to 100 rows (`LIMIT 100`) by default (~5,000 tokens), preventing token bloat. If noisy, filter by `level IN ('warn', 'error')` or message keywords.
- **Phase 4: Raw Docker Logs (Docker MCP)**:
  - Strictly a last-resort fallback when OpenObserve or Vector is unreachable. Never dump unbounded log streams into the context.

---

## 3. Subsystem 2: Progressive Capacity Backoff Delay

### 3.1 Mechanism in `brain/pkg/queue/worker.go`
When `isCapacity` is true (transient 429 quota exhaustion or model capacity surge):
- Replace the 2-second floor with a progressive backoff floor tied to the retry attempt:
  - `minFloor := time.Duration(attempt) * 30 * time.Second`
  - Attempt 1: 30 seconds minimum.
  - Attempt 2: 60 seconds minimum.
- Retain jitter: `delay += time.Duration(rand.Intn(3000)) * time.Millisecond`.
- If Gemini explicitly supplies a larger reset duration (`resetDur > minFloor`), honor the larger duration (`delay = resetDur`).

### 3.2 Impact
- 30 seconds allows the rolling 60-second Gemini token bucket to shed half of its accumulated tokens, providing sufficient capacity for a compacted retry.
- 60 seconds on Attempt 2 guarantees a full rolling window drain for deeper quota recovery.
- Maintains the Discord typing heartbeat without locking the global queue.

---

## 4. Subsystem 3: Session Rotation with Transcript Tool Compaction

### 4.1 The Compaction Concept
Instead of wiping `te.currentSessionID = ""` to a completely blank session on quota/step rotation:
- Inspect the dying session's `transcript.jsonl` on disk.
- Extract the sequence of executed tool calls, command lines, and tool outputs from the current turn.
- Condense the actions into a compact summary using the pre-warmed **low-effort model** (`cur.LowEffortModel` via `discordLowEffortPool` / `ephemeral:summarizer`), or deterministic formatting if brief.
- Format these into a structured `<PREVIOUS_TURN_ACTIONS>` block (generic across all task types: coding, investigation, testing, deployment).
- Inject this block into the prompt of the newly rotated session.

### 4.2 API Additions in `brain/pkg/session`
Add to `session.Manager`:
```go
// ExtractTranscriptToolActions scans the transcript for the specified conversation ID
// and extracts completed tool calls and outputs from the latest turn into a structured action block.
func (m *Manager) ExtractTranscriptToolActions(convID string) string
```
- Scans backwards from the end of `transcript.jsonl` / `transcript_full.jsonl` up to the latest non-ambient `USER_INPUT`.
- Collects:
  - `PLANNER_RESPONSE` entries with non-empty `tool_calls` (tool name and command/parameters).
  - Subsequent `GENERIC` tool output entries (exit code, output snippet up to 500 characters, error text).
- Formats output:
  ```markdown
  <PREVIOUS_TURN_ACTIONS>
  The previous attempt in this thread executed the following actions before session rotation:
  - Action: <tool_name> | Command/Args: <command_line> | Status: <status> | Result: <snippet>
  Do NOT repeat these exact actions. Use these results to proceed with the request or synthesize the final answer.
  </PREVIOUS_TURN_ACTIONS>
  ```

### 4.3 Wiring into `brain/pkg/queue/worker.go`
In `executeWithRetries`:
- Before wiping `te.currentSessionID = ""` during quota pause rotation or step ceiling rotation:
  ```go
  if te.currentSessionID != "" && te.pool != nil && te.pool.sessionMgr != nil {
      if toolSum := te.pool.sessionMgr.ExtractTranscriptToolActions(te.currentSessionID); toolSum != "" {
          // If lengthy, condense via te.pool.lowEffortProcessPool using LowEffortModel; otherwise use raw structured block
          te.previousTurnActions = te.condenseTurnActions(toolSum)
      }
  }
  ```
- In `preparePrompt`:
  - If `te.previousTurnActions != ""`, append it to `turnPrompt`.
- Clear `te.previousTurnActions` once successfully consumed by a turn.

---

## 5. Global Constraints & Invariants

- **Zero Markdown Tables**: Strictly bulleted lists only across all documentation, skill text, commit messages, and PR descriptions.
- **Zero Swallowed Errors**: Every error path must be logged with structured context or propagated.
- **Zero Host Memory Inspection**: No `/proc/meminfo` reading or memory pressure eviction heuristics.
- **Statement Coverage Floor**: Strictly >= 95.0% statement coverage maintained across `brain/pkg/session` and `brain/pkg/queue`.
- **Pure Go Standard Library**: No external dependencies beyond the existing standard library and test fixtures.
