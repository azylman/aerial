# Chunk 17/25 Verification Report: Post-Cutover Separate Images and Consumers Boundary Gate

Build: build-1795131906-b2b196f5 — Migrate Eri To Restate Graceful Draining
Chunk: c10-post-cutover-boundary-gate (Verify Post-Cutover Separate Images And Consumers)
Dispatch: vanadis-c10-post-cutover-boundary-gate-turn-0
Date: 2026-11-20

## Verdict: PASS

All boundary requirements verified and live gates confirmed.

## 1. Workload Verification & Separate Images

- **Stateless HTTP Deployments (`apps/v1 Deployment`)**:
  - `Deployment/eri`: Image `ghcr.io/azylman/aerial-eri:9716a934f1c6eb9772554806ceaeabe7533f57aa`, `readyReplicas: 1`, containerPort: `8080`, readinessProbe HTTP `/healthz`.
  - `Deployment/eri-miorine`: Image `ghcr.io/azylman/aerial-eri:9716a934f1c6eb9772554806ceaeabe7533f57aa`, `readyReplicas: 1`, containerPort: `8080`, readinessProbe HTTP `/healthz`.
  - `Deployment/eri-suletta`: Image `ghcr.io/azylman/aerial-eri:9716a934f1c6eb9772554806ceaeabe7533f57aa`, `readyReplicas: 1`, containerPort: `8080`, readinessProbe HTTP `/healthz`.

- **Restate Worker Deployments (`restate.dev/v1beta1 RestateDeployment`)**:
  - `RestateDeployment/eri-restate`: Image `ghcr.io/azylman/aerial-eri-restate:9716a934f1c6eb9772554806ceaeabe7533f57aa`, `readyReplicas: 1`, containerPorts: `9080` (Restate HTTP/2) + `4028` (Health/MCP/Metrics).
  - `RestateDeployment/eri-miorine-restate`: Image `ghcr.io/azylman/aerial-eri-restate:9716a934f1c6eb9772554806ceaeabe7533f57aa`, `readyReplicas: 1`, containerPorts: `9080` + `4028`.
  - `RestateDeployment/eri-suletta-restate`: Image `ghcr.io/azylman/aerial-eri-restate:9716a934f1c6eb9772554806ceaeabe7533f57aa`, `readyReplicas: 1`, containerPorts: `9080` + `4028`.

- **Restate Service Registrations & Timeouts**:
  Admin endpoint `http://restate.aerial:9070/services` confirms:
  - `ThreadSession`: `inactivity_timeout=1h`, `abort_timeout=1h`
  - `ThreadSession-miorine`: `inactivity_timeout=1h`, `abort_timeout=1h`
  - `ThreadSession-suletta`: `inactivity_timeout=1h`, `abort_timeout=1h`
  - `DispatchSession`: `inactivity_timeout=1h`, `abort_timeout=1h`
  - `DispatchSession-miorine`: `inactivity_timeout=1h`, `abort_timeout=1h`
  - `DispatchSession-suletta`: `inactivity_timeout=1h`, `abort_timeout=1h`
  - `VoiceSession`: `inactivity_timeout=1h`, `abort_timeout=1h`
  - `VoiceSession-miorine`: `inactivity_timeout=1h`, `abort_timeout=1h`
  - `VoiceSession-suletta`: `inactivity_timeout=1h`, `abort_timeout=1h`

## 2. NATS JetStream Consumer Cleanup

- Successfully deleted retired durable consumers across all three accounts on stream `AERIAL_JOBS`:
  - `aerial`: deleted `eri-prompt`, `eri-dispatch`, `eri-discord-message`
  - `miorine`: deleted `eri-prompt`, `eri-dispatch`, `eri-discord-message`
  - `suletta`: deleted `eri-prompt`, `eri-dispatch`, `eri-discord-message`
- Zero active subscriptions and zero pending messages verified via `http://nats.aerial:8222/jsz?consumers=true`.

## 3. Static Codebase Boundary Invariants

- `services/eri/Dockerfile`: Minimal `alpine:3.24` image running `/app/eri` on port 8080.
- `services/eri-restate/Dockerfile`: Ubuntu 24.04 image building `/app/eri-restate` on ports 9080 and 4028.
- Zero `ERI_MODE` environment variable switches across code, configs, and scripts.
- Zero `registerEriJobConsumers` in `services/eri`.
- Zero `/discord/widget` routes in `services/eri`.
- Zero `checkUnrotatedSessions` in `services/eri`.
- Zero `ResendOnStartup` in `services/eri`.
- VictoriaLogs query confirms zero runtime error logs for `eri`, `eri-restate`, `vanadis-restate`, `haro`, and `coven`.
