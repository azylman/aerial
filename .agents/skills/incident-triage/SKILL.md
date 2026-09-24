---
name: incident-triage
description: Forensic runbook and root-cause triage workflow whenever asked to investigate why an Aerial turn, Discord message, or automated task failed, duplicated, timed out, misrouted, or was unexpectedly silenced (e.g. "what happened here: <discord-link>", "why did you reply there", "why didn't you respond"). Strictly relies on generic core stack components (PostgreSQL, OpenObserve, Docker MCP, VictoriaMetrics MCP, Discord API).
---

# Aerial Incident Triage & Forensic Runbook

This skill defines the standardized operational protocol for diagnosing runtime anomalies, dropped turns, duplicate messages, gateway routing errors, and container crashes across Aerial's core execution stack.

---

## 1. Principles & Boundaries

- **Zero-Guessing Invariant**: Never guess why an action occurred or failed. Every conclusion MUST be backed by immutable telemetry: database records, structured log entries, or TSDB metrics.
- **Two-Repository Boundary**: This skill is strictly generic and core-contained. It relies **exclusively** on core components (`aerial-brain`, `aerial-postgres`, `aerial-openobserve`, `aerial-victoriametrics`, `aerial-vector`, Docker daemon, Discord API). It MUST NEVER query user-config overlays (`ha-mcp`, `google`, `ubereats`), custom sidecars, or private channel IDs.
- **Handoff Contract**: `incident-triage` is Phase 0 (Forensics & Root Cause Identification). Once the root-cause failure mode and offending code path are isolated, hand off immediately to:
  - `systematic-debugging`: For reproducing the bug with a minimal failing test.
  - `self-improvement`: For executing the tiered PR workflow and deploying the fix.

---

## 2. When to Use

Activate this skill when:
- The user provides a Discord message link and asks *"what happened here?"*, *"why did you say that?"*, or *"why didn't you respond?"*.
- The user reports duplicate responses, unexpected thread creations, or messages posted to the root channel instead of a thread.
- A scheduled cron or background task failed or produced unexpected output.
- Systems recovered from an unexpected restart, crash, or token burn spike.

---

## 3. The 5-Phase Diagnostic Protocol

### Phase 1: URL & Identifier Extraction
Extract the target coordinates from the user's message or link:
1. **Discord Message Link Structure**:
   `https://discord.com/channels/<guild_id>/<channel_id>/<message_id>`
2. **Derive Message Timestamp (Snowflake Math)**:
   Discord snowflake IDs encode the generation timestamp in milliseconds:
   ```python
   timestamp_ms = (int(message_id) >> 22) + 1420070400000
   ```
   Use this timestamp `T` to define an exact query window: `[T - 30s, T + 60s]`.

### Phase 2: Core Database Telemetry (`aerial-postgres`)
Query the production database using `DATABASE_URL` (`postgres://aerial:...@postgres:5432/aerial`):

```sql
-- 1. Inspect target message record:
SELECT id, thread_id, author_id, author_name, status, retry_count, 
       error_message, substring(response_text, 1, 300) as response_preview,
       created_at, updated_at, metadata
FROM messages 
WHERE id = '<message_id>';

-- 2. If message exists, inspect its session state:
SELECT thread_id, turn_count, internal_session_id, updated_at, 
       substring(summary, 1, 300) as summary_preview
FROM sessions 
WHERE thread_id = '<thread_id>';

-- 3. Check for recent schedule runs if incident involved an automation:
SELECT id, schedule_id, status, error, started_at, completed_at
FROM schedule_runs 
ORDER BY started_at DESC LIMIT 5;
```

**Diagnostic Branching**:
- **Row does not exist in `messages`**: The event was either dropped before DB ingestion (Gateway disconnect, permission error) or intentionally silenced by the classifier. Proceed directly to **Phase 3 (Gateway & Classifier Logs)**.
- **Status is `FAILED`**: Inspect `error_message` and retry count. Proceed to **Phase 3** to extract the stack trace.
- **Status is `COMPLETED` but response was empty or incorrect**: Check for `agy` print-mode drain or context truncation.
- **Multiple rows with similar timestamps**: Check for restart replay or deduplication bypass.

### Phase 3: Distributed Observability & Log Forensics (`openobserve`)
Query OpenObserve stream `docker_logs` (or Vector container stdout/stderr) scoped to the `[T - 30s, T + 60s]` window:

1. **Classifier & Gateway Evaluation**:
   ```sql
   SELECT timestamp, log.level, log.message, log.score, log.tier 
   FROM docker_logs 
   WHERE container_name = 'aerial-brain' 
     AND (log.message LIKE '%classifier%' OR log.message LIKE '%triage%')
     AND _timestamp >= <T_minus_30s> AND _timestamp <= <T_plus_60s>
   ORDER BY _timestamp ASC;
   ```
   *Look for*: Was the message evaluated? Did it score below the ambient threshold? Was it classified as `Tier.SILENT`?

2. **Error & Stack Trace Extraction**:
   ```sql
   SELECT timestamp, log.level, log.message, log.error, log.stack 
   FROM docker_logs 
   WHERE container_name = 'aerial-brain' 
     AND log.level IN ('error', 'fatal', 'panic')
     AND _timestamp >= <T_minus_30s> AND _timestamp <= <T_plus_60s>
   ORDER BY _timestamp ASC;
   ```

3. **Gateway Thread Deduplication & Discord API Errors**:
   *Look for*: Discord API error `160004` (*"Thread already exists"*), rate limits (HTTP 429), or missing channel permissions (HTTP 403).

### Phase 4: Container & Runtime Health (`docker-mcp`, `victoriametrics-mcp`)
Check whether the host execution environment suffered an outage or crash:
1. **Container Uptime & Restarts**:
   Inspect `aerial-brain` and supporting containers via Docker:
   - Check `RestartCount` and `State.StartedAt`.
   - Check if `OOMKilled == true` or exit code was `137` (SIGKILL / Out of Memory).
2. **TSDB Telemetry Spikes**:
   Query VictoriaMetrics for CPU saturation, RSS memory spikes, or goroutine leaks around `T`:
   - `container_memory_rss{container="aerial-brain"}`
   - `container_cpu_usage_seconds_total{container="aerial-brain"}`

### Phase 5: Root-Cause Synthesis & Failure Taxonomy Mapping
Match findings against the **Core Failure Taxonomy**:

1. **Classifier Misfire / Ambient Silence**: Message was received by the gateway but scored below wake threshold (`Tier.SILENT`).
2. **Gateway / Thread Routing Mismatch**: Discord API error `160004` caused thread creation retry failures, or turn was erroneously delivered to the root channel due to missing thread context.
3. **Session Rotation / Ceiling Overflow**: Thread reached maximum turn count (e.g. 20 turns) and either failed to rotate cleanly or dropped prior context.
4. **Harness Print-Mode Drain**: Active execution was terminated prematurely because conversational text was emitted during background task execution in `agy -p`.
5. **Container Crash / OOMKill**: Process died mid-turn due to memory limits (exit 137) or panic, leaving the message in `PENDING` or `PROCESSING`.
6. **Replay / Idempotency Storm**: Server restarted with pending unacknowledged tasks, triggering duplicate message processing.

---

## 4. Post-Mortem Delivery Format

Deliver the post-mortem directly to the user in clean Markdown (strictly < 1,800 characters, no markdown tables per Invariant 5):

```markdown
**BLUF**: [One-sentence bottom-line conclusion stating exact failure mode].

### 🔍 Forensic Findings
- **Target Event**: Message `<message_id>` in `<channel/thread>` at `<timestamp>`.
- **Database State**: Status `<status>`, retry count `<N>`, error `<error_summary>`.
- **Telemetry & Traces**: [Key log excerpt, classifier score, or container exit code].
- **Failure Taxonomy**: `[Failure Mode Category]`.

### 🛠️ Root Cause & Remediation
- **Why it happened**: [Clear technical explanation of code/runtime failure].
- **Next Step**: [Failing test in `brain/pkg/...` via `systematic-debugging` or operational fix via `self-improvement`].
```

---

## 5. Red Flags & Anti-Patterns

- **NEVER** guess or speculate without running database queries or checking OpenObserve logs.
- **NEVER** blame Discord API delivery until verifying Vector/OpenObserve received the gateway event.
- **NEVER** touch production code without reproducing the failure via a failing unit test in `brain/pkg/...` first.
