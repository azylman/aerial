# Live Rollout Verification: Eri Mid-Turn Watchdog & Remediated SHA Routing

## Overview
Post-deployment verification for build build-1794987722-02650e53 covering:
- Chunk 1: Eri Mid-Turn Transcript Watchdog Rescue
- Chunk 2: Vanadis-Restate Remediated SHA CI Failure Tracking

## Verification Checks
1. Pod Status:
   - eri (3/3 Running, 0 restarts)
   - vanadis-restate (3/3 Running, 0 restarts)
2. Health Probes:
   - http://eri.default.svc.cluster.local:8080/healthz: HTTP 200 {"status":"ok"}
   - http://vanadis-restate-health.default.svc.cluster.local:4026/healthz: HTTP 200 {"status":"ok"}
3. Telemetry & Logs:
   - VictoriaLogs query over last 10m confirms 0 error/fatal/warn logs on both containers.
