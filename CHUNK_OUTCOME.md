# Chunk c2-baseline-gate: NATS client baseline (report only)

Build: build-1795020758-65004387. Taken 2026-11-18 ~10:08 AM PT, read-only. No code changes.

## Result: PASS

- Every NATS client pod runs image 2fe3eb3 (the c1 merge, PR #2006): eri, haro, coven, prospera, synapse, vanadis, vanadis-nats, vanadis-restate across aerial, miorine and suletta. Fresh rollout, all Running.
- /connz?auth=1 shows nats.go 1.56.0 on all new clients: aerial 7, miorine 5, suletta 5.
- One extra connection per account (aerial, miorine, suletta) is the retained previous-revision vanadis-restate worker (nats.go 1.55.0, 1 subscription). Restate keeps it for in-flight invocations. It reconnects fine on a broker restart, since it is not an auth-error case.
- NATS server: nats-0, 7h25m uptime, 0 restarts, nats:2-alpine (v2.15.3).

## JetStream baseline (per account; all consumers pending 0, ack-pending 0, redelivered 0)

- aerial: AERIAL_EVENTS 756 msgs, 6 consumers (vanadis-dispatch, vanadis-pr-events, coven-transcript-synced, haro-dispatch, haro-events, eri-transcript-sync). AERIAL_COALESCED 0 msgs, 2 consumers (vanadis-widget, coven-extract). AERIAL_JOBS 0 msgs, 11 consumers (synapse x4, prospera x3, haro-oneshot, eri x3).
- miorine: AERIAL_EVENTS 544 msgs, 5 consumers. AERIAL_COALESCED 0 msgs, 1 consumer. AERIAL_JOBS 0 msgs, 8 consumers.
- suletta: AERIAL_EVENTS 569 msgs, 5 consumers. AERIAL_COALESCED 0 msgs, 1 consumer. AERIAL_JOBS 0 msgs, 8 consumers.

## Log and CI checks

- VictoriaLogs: no client "connection closed" errors in the last 10 minutes.
- No Aerial-stack PR is mid-CI. Only open PRs: mirrormere #1021 (external fork, CI Gate queued after other jobs already failed), mirrormere #1015, aerial-config #735 and #709, aerial #1427 (stale). None are Vanadis-driven.
