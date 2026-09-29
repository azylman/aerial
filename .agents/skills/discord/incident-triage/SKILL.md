---
name: incident-triage
description: >-
  Forensic runbook and telemetry triage workflow whenever asked to investigate why an Aerial turn, task, service, or workflow failed, duplicated, timed out, misrouted, or was unexpectedly silenced (e.g. "what happened here", "why did you reply there", "why didn't you respond"), or when asked about system performance, latency, durations, voice TTFR, or timings (e.g. "how long did this take", "investigate latency", "why was voice slow"). Strictly relies on generic core stack telemetry (VictoriaMetrics MCP, OpenObserve MCP, PostgreSQL, Docker MCP, Discord API).
---

# Aerial Incident Triage & Forensic Runbook

This skill defines the standardized operational protocol for diagnosing runtime anomalies, task failures, performance bottlenecks, dropped turns, duplicate messages, gateway routing errors, and container crashes across Aerial's core execution stack.

---

## 1. Principles & Boundaries

- **Zero-Guessing Invariant**: Never guess why an action occurred or failed. Every conclusion MUST be backed by immutable telemetry: TSDB metrics, structured log entries, or database records.
- **Two-Repository Boundary**: This skill is strictly generic and core-contained. It relies exclusively on generic core stack components (`aerial-brain`, `aerial-victoriametrics`, `aerial-openobserve`, `aerial-postgres`, `aerial-vector`, Docker daemon, Discord API). It MUST NEVER hardcode non-core service names, user-config overlays (`ha-mcp`, `google`, `ubereats`), custom sidecars, or private channel IDs. Services outside the core stack are discovered dynamically at runtime.
- **Metric-First Hierarchy**: Always query metrics first via VictoriaMetrics to pinpoint exact microsecond timestamps and duration boundaries before querying logs. Never jump directly to dumping raw container logs via Docker MCP unless observability services are offline.
- **Strictly Zero Host Memory Inspection**: Never inspect host memory (`/proc/meminfo`, system memory heuristics). Container memory is observed strictly through cAdvisor metrics (`container_memory_rss`) in VictoriaMetrics.
- **Silent Multi-Step Execution**: Perform all diagnostic queries silently without intermediate play-by-play chatter to prevent `agy -p` print-mode drain.
- **Handoff Contract**: `incident-triage` is Phase 0 (Forensics & Root Cause Identification). Once the root-cause failure mode and offending code path are isolated, if code changes are needed in `aerial`, hand off immediately to:
  - `systematic-debugging`: For reproducing the bug with a minimal failing test.
  - `self-improvement`: For executing the tiered PR workflow and deploying the fix.

---

## 2. When to Use

Activate this skill when:
- The user asks why an Aerial turn, task, service, or workflow failed, timed out, crashed, or produced an unhandled error.
- The user asks about system performance, latency, durations, voice TTFR, or timings (e.g. "how long did this take", "investigate latency", "why was voice slow", "check response time").
- The user provides a Discord message link and asks "what happened here?", "why did you reply there?", or "why didn't you respond?".
- The user reports duplicate responses, unexpected thread creations, or messages posted to the root channel instead of a thread.
- Systems recovered from an unexpected restart, crash, or token burn spike.

---

## 3. The 5-Phase Diagnostic Protocol

### Phase 1: Dynamic Component Discovery

When an investigation involves non-core features, external clients, or user-space services (e.g. "kiosk", "dashboard", "smart home", "audio client"):
- **Dynamic Service Resolution**:
  - Inspect the user's prompt for mentioned features, clients, or sidecars.
  - Query `docker-mcp:list_containers` to inspect active container names on the host.
  - Match user intent to the discovered container names and dynamically bind `<target_services>`.
  - Strictly DO NOT hardcode non-core container names into this runbook or prompt templates.
- **Coordinate & Snowflake Extraction (Discord Context)**:
  - If a Discord message link is provided (`https://discord.com/channels/<guild_id>/<channel_id>/<message_id>`), derive the microsecond timestamp via snowflake math:
    - `timestamp_ms = (int(message_id) >> 22) + 1420070400000`
    - `timestamp_us = timestamp_ms * 1000`
  - For Discord turns, check `aerial-postgres` messages table using vertical output:
    ```bash
    docker exec -i aerial-postgres psql -U aerial -d aerial -x -c "
    SELECT id, thread_id, author_id, author_name, status, retry_count, restart_count,
           error_message, LEFT(response_text, 160) as resp_snippet,
           created_at, updated_at
    FROM messages 
    WHERE id = '<message_id>';
    "
    ```
  - Check for fast-path short-circuits:
    - `error_message LIKE '[AMBIENT score=%]'`: Message scored below ambient wake threshold and was intentionally dropped.
    - `error_message LIKE 'poison pill: exceeded restart limit%'`: Message repeatedly crashed runner.
    - `error_message LIKE '[EXHAUSTED_PRE_TURN_RETRIES]'`: Pre-turn webhook failed repeatedly.
    - `status = 'PENDING'` and `created_at < NOW() - INTERVAL '30 minutes'`: Queue starvation or deadlocked worker pool.

### Phase 2: Universal Metric Triage (VictoriaMetrics MCP)

VictoriaMetrics is the cluster-wide time-series database (TSDB) collecting metrics from all containers, including Prometheus application endpoints and cAdvisor container metrics.

- **Query Metrics FIRST**: Always query VictoriaMetrics before inspecting log streams. Metrics isolate exact microsecond event timestamps and anomaly durations in <1 second (~200 tokens), preventing context bloat.
- **Latency & Performance Metrics**:
  - Voice TTFR duration: `aerial_brain_voice_ttfr_duration_seconds` (or histogram quantiles).
  - Turn and tool execution durations: `rate(aerial_brain_turn_duration_seconds_sum[5m]) / rate(aerial_brain_turn_duration_seconds_count[5m])`.
  - API request latencies and queue wait durations.
- **cAdvisor Container Resource Metrics**:
  - Query container resource metrics for `aerial-brain` or dynamically discovered `<target_services>`:
    - Memory RSS: `container_memory_rss{name="<container_name>"}`
    - CPU Rate: `rate(container_cpu_usage_seconds_total{name="<container_name>"}[1m])`
    - OOM Events: `container_oom_events_total{name="<container_name>"}`
- **Establish Event Time Window**:
  - Use metric spikes, latency jumps, or error increments to pinpoint the event timestamp `t_event`.
  - Center a 2 to 5 minute time window around `t_event` for log correlation: `t_start = t_event - 2m` and `t_end = t_event + 3m`.

### Phase 3: Scoped Log Forensics (OpenObserve MCP)

Once the event timestamp `t_event` and `<target_services>` are pinpointed from metrics:

- **Targeted Log Query**:
  - Query OpenObserve stream `docker_logs` parameterized by service:
    ```sql
    SELECT _timestamp, service, level, message 
    FROM docker_logs 
    WHERE service IN ('brain', '<target_services>')
      AND _timestamp >= <start_time_us> AND _timestamp <= <end_time_us>
    ORDER BY _timestamp ASC 
    LIMIT 100;
    ```
- **Context Protection (LIMIT 100)**:
  - Default strictly to `LIMIT 100` (~5,000 tokens) to capture complete lifecycles without token bloat.
  - If the log stream is noisy, narrow the query by filtering on severity or keywords:
    - `AND level IN ('warn', 'error', 'fatal', 'panic')`
    - `AND (message LIKE '%error%' OR message LIKE '%timeout%' OR message LIKE '%disconnect%')`
- **Tracing Across Service Boundaries**:
  - Correlate timestamps across `brain` and `<target_services>` to determine whether latency or failure originated in `aerial-brain` or the external component.

### Phase 4: Raw Docker Logs (Docker MCP)

- **Strict Last-Resort Fallback**:
  - Query raw container logs via `docker-mcp:fetch_container_logs` ONLY if OpenObserve or Vector is unreachable, crashed, or experiencing ingestion delays.
  - NEVER dump unbounded log streams into the context window.
  - Always specify strict line limits (`tail: 50` or `tail: 100`) and target specific containers.

### Phase 5: Root-Cause Synthesis & Failure Taxonomy Mapping

Map findings to the **8-Point Core Failure Taxonomy**:

1. **Classifier Misfire / Ambient Silence**: Message received by gateway but scored below ambient wake threshold (`Tier.SILENT`).
2. **Gateway / Thread Routing Mismatch**: Discord API error `160004` caused thread creation retry failures, or turn was erroneously delivered to the root channel due to missing thread context.
3. **Session Rotation / Ceiling Overflow**: Thread reached maximum turn count (e.g. 20 turns) or quota pause steps, causing session rotation.
4. **Harness Print-Mode Drain**: Active execution was terminated prematurely because conversational text was emitted during background task execution in `agy -p`.
5. **Container Crash / OOMKill**: Process died mid-turn due to container memory limits (exit code 137) or runtime panic, leaving the message in `PENDING` or `PROCESSING`.
6. **Replay / Idempotency Storm**: Server restarted with pending unacknowledged tasks, triggering duplicate message processing.
7. **Discord API Delivery Error**: Turn completed successfully in `agy` and `messages.response_text` was populated, but Discord API returned 403 Forbidden (50001/50013 Missing Permissions), 404 (10008 Unknown Message), or 50083/50084 (Thread Archived/Locked).
8. **Channel Lifecycle Webhook Gate**: An `on_wake` or `pre_turn` HTTP interceptor returned an explicit drop decision or timed out (`timeout_ms`).

---

## 4. Post-Mortem Delivery Format

Deliver the post-mortem directly to the user in clean Markdown (strictly < 1,800 characters, bulleted lists only, no markdown tables):

```markdown
**BLUF**: [One-sentence bottom-line conclusion stating exact failure mode or latency source].

### 🔍 Forensic Findings
- **Target Event**: [Message ID, service, or operation at <timestamp>].
- **Metrics Observed**: [Key VictoriaMetrics data: TTFR, latency duration, CPU/memory spike, OOM count].
- **Telemetry & Traces**: [Key OpenObserve log excerpt or database status].
- **Failure Taxonomy**: [Failure Mode Category or Performance Root Cause].

### 🛠️ Root Cause & Remediation
- **Why it happened**: [Clear technical explanation of code/runtime failure or bottleneck].
- **Next Step**: [Failing test in `brain/pkg/...` via `systematic-debugging` or operational fix via `self-improvement`].
```

---

## 5. Red Flags & Anti-Patterns

- **NEVER** dump raw Docker logs via Docker MCP without querying VictoriaMetrics and OpenObserve first.
- **NEVER** guess or speculate without running metric queries or checking OpenObserve logs.
- **NEVER** hardcode non-core service names in skill files or prompts; use `docker-mcp:list_containers` for dynamic discovery.
- **NEVER** inspect host memory directly (`/proc/meminfo`); inspect container cAdvisor metrics via VictoriaMetrics.
- **NEVER** use `log.*` column prefixes in OpenObserve (Vector promotes fields directly to top-level).
- **NEVER** pass milliseconds to OpenObserve `_timestamp` (must be converted to microseconds: `ms * 1000`).
- **NEVER** touch production code without reproducing the failure via a failing unit test in `brain/pkg/...` first.
