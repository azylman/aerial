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
- **Silent Multi-Step Execution**: Perform all diagnostic queries silently without intermediate play-by-play chatter to prevent `agy -p` print-mode drain.
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
   Discord snowflake IDs encode the generation timestamp in milliseconds. OpenObserve requires integer **microseconds**:
   ```python
   timestamp_ms = (int(message_id) >> 22) + 1420070400000
   timestamp_us = timestamp_ms * 1000
   ```
   Use `timestamp_us` to define the OpenObserve search window:
   - `start_time`: `timestamp_us - 30_000_000` (30s prior)
   - `end_time`: `timestamp_us + 60_000_000` (60s after)

### Phase 2: Core Database Telemetry (`aerial-postgres`)
Execute queries against the PostgreSQL container using `-x` (vertical output, prevents table generation):

```bash
docker exec -i aerial-postgres psql -U aerial -d aerial -x -c "
SELECT id, thread_id, author_id, author_name, status, retry_count, restart_count,
       error_message, LEFT(response_text, 160) as resp_snippet,
       created_at, updated_at
FROM messages 
WHERE id = '<message_id>';
"
```

#### Fast-Path Diagnostic Decision Tree
Check the returned row for immediate short-circuits:
- **`error_message LIKE '[AMBIENT score=%'`**: Message was evaluated by the classifier, scored below wake threshold, and was deliberately dropped as ambient chatter. Fast-path complete.
- **`error_message LIKE 'poison pill: exceeded restart limit%'`**: Message repeatedly crashed the execution runner, tripping restart limit protection. Fast-path complete.
- **`error_message LIKE '[EXHAUSTED_PRE_TURN_RETRIES]'`**: Channel pre-turn webhook failed repeatedly, aborting the turn. Fast-path complete.
- **`status = 'PENDING'` and `created_at < NOW() - INTERVAL '30 minutes'`**: Queue starvation or deadlocked worker pool. Fast-path complete.
- **Row does not exist (0 rows)**: Proceed to **Pre-DB Ingestion Gap Protocol** below.
- **`status = 'FAILED'` or ambiguous**: Proceed to **Phase 3** for log forensics.

#### Pre-DB Ingestion Gap Protocol (0 Rows in Postgres)
If the target message ID does not exist in `messages`:
1. **Bot Author Check**: Did another bot post the message? The Discord gateway funnel automatically ignores non-human bot messages unless explicitly configured.
2. **Channel Permissions**: Check if Aerial lacks `VIEW_CHANNEL` or `READ_MESSAGE_HISTORY` in that channel via `discord-mcp`.
3. **Gateway Disconnects**: Search OpenObserve for gateway dropouts around `timestamp_us`:
   ```sql
   SELECT _timestamp, level, message 
   FROM docker_logs 
   WHERE service = 'brain' 
     AND (message LIKE '%gateway%' OR message LIKE '%disconnect%')
     AND _timestamp >= <start_time> AND _timestamp <= <end_time>
   LIMIT 10;
   ```

### Phase 3: Distributed Observability & Log Forensics (`openobserve`)
Query OpenObserve stream `docker_logs` (Vector indexes the runner under `service = 'brain'`):

1. **Classifier & Gateway Evaluation**:
   ```sql
   SELECT _timestamp, level, message 
   FROM docker_logs 
   WHERE service = 'brain' 
     AND (message LIKE '%classifier%' OR message LIKE '%triage%' OR message LIKE '%<message_id>%')
     AND _timestamp >= <start_time> AND _timestamp <= <end_time>
   ORDER BY _timestamp ASC 
   LIMIT 25;
   ```

2. **Errors & Panics**:
   ```sql
   SELECT _timestamp, level, message 
   FROM docker_logs 
   WHERE service = 'brain' 
     AND level IN ('error', 'fatal', 'panic')
     AND _timestamp >= <start_time> AND _timestamp <= <end_time>
   ORDER BY _timestamp ASC 
   LIMIT 25;
   ```

### Phase 4: Container & Runtime Health (`docker-mcp`, `victoriametrics-mcp`)
Check if the host container environment experienced a crash or resource exhaustion:
1. **Docker Container State**:
   Inspect `aerial-brain` container via `docker-mcp`: check `RestartCount`, `ExitCode`, and whether `OOMKilled == true`.
2. **VictoriaMetrics cAdvisor Telemetry**:
   Target cAdvisor metric name (`name="aerial-brain"`):
   - Memory RSS: `container_memory_rss{name="aerial-brain"}`
   - CPU Rate: `rate(container_cpu_usage_seconds_total{name="aerial-brain"}[1m])`
   - OOM Events: `container_oom_events_total{name="aerial-brain"}`

### Phase 5: Root-Cause Synthesis & Failure Taxonomy Mapping
Map findings to the **8-Point Core Failure Taxonomy**:

1. **Classifier Misfire / Ambient Silence**: Message received by gateway but scored below ambient wake threshold (`Tier.SILENT`).
2. **Gateway / Thread Routing Mismatch**: Discord API error `160004` caused thread creation retry failures, or turn was erroneously delivered to the root channel due to missing thread context.
3. **Session Rotation / Ceiling Overflow**: Thread reached maximum turn count (e.g. 20 turns) and either failed to rotate cleanly or dropped prior context.
4. **Harness Print-Mode Drain**: Active execution was terminated prematurely because conversational text was emitted during background task execution in `agy -p`.
5. **Container Crash / OOMKill**: Process died mid-turn due to memory limits (exit code 137) or runtime panic, leaving the message in `PENDING` or `PROCESSING`.
6. **Replay / Idempotency Storm**: Server restarted with pending unacknowledged tasks, triggering duplicate message processing.
7. **Discord API Delivery Error**: Turn completed successfully in `agy` and `messages.response_text` was populated, but Discord API returned 403 Forbidden (50001/50013 Missing Permissions), 404 (10008 Unknown Message), or 50083/50084 (Thread Archived/Locked).
8. **Channel Lifecycle Webhook Gate**: An `on_wake` or `pre_turn` HTTP interceptor returned an explicit drop decision or timed out (`timeout_ms`).

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
- **NEVER** use `log.*` column prefixes in OpenObserve (Vector promotes fields directly to top-level).
- **NEVER** pass milliseconds to OpenObserve `_timestamp` (must be converted to microseconds: `ms * 1000`).
- **NEVER** touch production code without reproducing the failure via a failing unit test in `brain/pkg/...` first.
