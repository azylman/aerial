# Synapse Pre-Cutover Readiness Verification Gate (syn-12-pre-cutover-gate)

## Summary
Pre-cutover operational readiness verification for Synapse prior to thin-main router cutover (Chunk 13: `syn-13-thin-main-cutover`). All cluster readiness probes, health endpoints, Prometheus telemetry gauges, and runtime stream logs across live instances have been verified and passed without regressions.

## Cluster Pod Readiness
Kubernetes pod readiness probes for `app.kubernetes.io/name` across default namespace:
- `synapse` (`synapse-6c7c8c7c87-nv8z9`): Status `Running`, Ready `2/2`
- `synapse-miorine` (`synapse-miorine-695d44bd9b-nwfck`): Status `Running`, Ready `2/2`
- `synapse-suletta` (`synapse-suletta-779797b485-5d28z`): Status `Running`, Ready `2/2`

## Endpoint Health & Telemetry Verification
Probed `/healthz` and `/metrics` on all live instances:
1. `synapse` (`http://synapse.aerial:4020`):
   - Health: HTTP 200 OK (`{"service":"synapse","status":"ok"}`)
   - Gateway Latency: `synapse_discord_gateway_latency_seconds ~0.082s`
   - Queue Depth: `synapse_queue_depth` reporting 0 in-flight / 0 pending across all queues
   - JetStream / River Job Ingestion: Active and processing (`synapse_jobs_enqueued_total`)
2. `synapse-miorine` (`http://synapse-miorine.aerial:4020`):
   - Health: HTTP 200 OK (`{"service":"synapse","status":"ok"}`)
   - Gateway Latency: `synapse_discord_gateway_latency_seconds ~0.086s`
   - Queue Depth: Nominal
3. `synapse-suletta` (`http://synapse-suletta.aerial:4020`):
   - Health: HTTP 200 OK (`{"service":"synapse","status":"ok"}`)
   - Gateway Latency: `synapse_discord_gateway_latency_seconds ~0.087s`
   - Queue Depth: Nominal

## Log Telemetry & Operational Health
- VictoriaLogs and active container streams inspected: Zero panics, zero unexpected crash-loops, and clean dispatch processing across GitHub webhooks, River/JetStream tasks, and Flux deployment syncs.
- Monorepo Statement Coverage: `services/synapse` stands at 95.99% (5,148 / 5,363 statements) exceeding the strict 95.0% floor.
