# Core Incident Triage Skill Specification

## Overview
The `incident-triage` core skill provides a standardized, domain-agnostic operational runbook for diagnosing runtime anomalies, dropped turns, duplicate messages, gateway routing errors, and container crashes across Aerial's core execution stack.

It operates strictly on the generic core stack (`azylman/aerial`) and adheres to the **Two-Repository Separation of Concerns**, with zero dependencies on user-specific overlays or private channels.

---

## 1. Architectural Boundaries & Precedence

### Separation of Concerns
- **Core-Only Scope**: Relies exclusively on core engine microservices and storage primitives:
  - **`aerial-postgres`**: Relational persistence (`messages`, `sessions`, `schedule_runs`).
  - **`aerial-openobserve`**: Structured container log ingestion (`docker_logs`).
  - **`aerial-victoriametrics`**: Host and container telemetry TSDB.
  - **`docker-mcp`**: Docker socket inspection for container restarts and health status.
  - **`discord-mcp`**: Discord API inspection for snowflake and channel metadata.
- **Prohibited Overlays**: Must never reference or require private user MCPs (`ha-mcp`, `google`, `ubereats`), user credentials, or custom bot sidecars.

### Handoff Pipeline
`incident-triage` functions as **Phase 0 (Forensic Investigation)**:
1. User provides an incident trigger (e.g. Discord URL or error description).
2. `incident-triage` extracts identifiers, queries telemetry across Postgres, OpenObserve, and VictoriaMetrics, and isolates the root cause into a standardized failure category.
3. Hand off to `systematic-debugging` to construct a minimal failing Go test in `brain/pkg/...`.
4. Hand off to `self-improvement` to implement, verify, and deploy the fix via the scratch PR workflow.

---

## 2. Snowflake Math & Query Bounding
To avoid costly unbounded log sweeps in OpenObserve, snowflake IDs are converted into millisecond timestamps to establish a precise temporal bounding window:

$$\text{timestamp\_ms} = (\text{snowflake} \gg 22) + 1420070400000$$

The query window is dynamically bounded to $[T - 30\text{s}, T + 60\text{s}]$, capturing the gateway event arrival, classifier evaluation, execution, and potential retry attempts.

---

## 3. Core Failure Taxonomy

| Failure Mode | Telemetry Signature | Typical Root Cause |
|---|---|---|
| **Classifier Misfire / Ambient Silence** | No entry in `messages` or `Tier.SILENT` in `aerial-brain` logs | Message failed ambient score threshold or wake rule |
| **Gateway / Thread Routing Mismatch** | Discord API error `160004` or thread ID matching root channel | Missing thread context or race in thread creation |
| **Session Ceiling Overflow** | `sessions.turn_count >= 20` or session rotation error | Context window saturation or rotation failure |
| **Harness Print-Mode Drain** | `messages.status = 'COMPLETED'` with empty `response_text` | Intermediate prose emitted during background task execution in `agy -p` |
| **Container Crash / OOMKill** | `docker inspect` exit code 137 or sudden TSDB drop | Memory limit exhaustion or unhandled panic |
| **Replay / Idempotency Storm** | Duplicate rows in `messages` with identical timestamps | Server restart re-processing `PENDING` queue items |

---

## 4. Verification & Testing

The skill is verified through:
1. Skill linkage verification in `brain/pkg/env/env_test.go` ensuring `.agents/skills/incident-triage` is dynamically discovered and symlinked to `~/.gemini/config/skills/incident-triage` on boot.
2. Static verification via `./scripts/verify.sh --staged`.
