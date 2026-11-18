# Chunk 11/11 Verification Report: Final Shared Infrastructure Boundaries

Build: build-1794989311-fa940fcc — Consolidate Shared NATS Restate Infrastructure
Chunk: chunk-10-final-boundary-verification (read-only gate, no code changes)
Dispatch: vanadis-chunk-10-final-boundary-verification-turn-0
Date: 2026-11-18

## Verdict: PASS with one non-blocking residual finding

All live and static boundary gates passed. Verified read-only: no JetStream
messages published, no synthetic builds/PRs/events injected, no credentials
read or printed, at most zero agent credentials mounted anywhere (probes used
unauthenticated TCP, wrong passwords, and the public NATS monitoring port).

## Live evidence (observed 2026-11-18 ~10:11 UTC via in-cluster kubernetes-mcp)

1. Shared NATS ready: StatefulSet `nats` 1/1 in `default`, pod `nats-0`
   1/1 Running, server v2.15.3, uptime 29m, 13 live client connections,
   JetStream enabled, 3 accounts, 9 streams, 35 consumers, 518 messages.
2. Shared Restate ready: exactly one `RestateCluster/restate`, status True /
   "reconciled successfully"; backing StatefulSet `restate` 1/1 in
   namespace `restate`.
3. Agent stacks ready: eri, haro, synapse, vanadis Deployments for aerial,
   miorine, and suletta all Running (3/3, 2/2, 2/2, 1/1 respectively), plus
   per-agent vanadis-nats and vanadis-restate dispatchers Running. Only
   non-ready pods cluster-wide are unrelated leftovers (atlas-dev-cleanup
   Init:Error x3, postgres backup/restore debug pods).
4. Restate deployment list scoped-only: 13 registrations collapse to exactly
   three service sets — `Build`/`BuildRouter` (aerial legacy default, by
   design with AGENT_NAME=aerial), `Build-miorine`/`BuildRouter-miorine`,
   `Build-suletta`/`BuildRouter-suletta`. Extra URIs are stale ReplicaSet
   registrations of the same service names, none serving traffic.
5. Auth rejection (raw NATS protocol probes): unauthenticated CONNECT ->
   `-ERR 'Authorization Violation'`; wrong password for user `aerial` ->
   rejected; wrong password for user `suletta` -> rejected.
6. Per-account isolation: `/connz?auth=1` shows live connections split
   across accounts only (aerial x3, miorine x5, suletta x5); `/jsz` shows
   each account owns a separate copy of AERIAL_EVENTS / AERIAL_JOBS /
   AERIAL_COALESCED with divergent message counts (aerial 182/1/0, miorine
   137/0/0, suletta 198/0/0) — separate storage per account.

## Static evidence (config repos)

- `no_auth_user` absent from all manifests in both repos (aerial cut done).
- Zero plaintext NATS credentials: no bcrypt hashes, no credentialed
  `nats://user:pass@` literals; all client URLs use secret expansion only
  (`$(NATS_*_PASSWORD)` / `${NATS_*_PASSWORD}`).
- Exactly one NATS StatefulSet (`nats` + `nats-jetstream-data` PVC) and one
  `RestateCluster` (`restate`) declared; no per-agent StatefulSet, PVC, or
  RestateCluster in either repo.

## Reclaimed (post-cutover deletion confirmed)

- 2 legacy per-agent NATS StatefulSets + JetStream storage.
- 2 legacy per-agent RestateClusters (namespaces `restate-miorine` and
  `restate-suletta` hold zero pods).

## Residual finding (non-blocking, left untouched — read-only chunk)

Namespaces `restate-miorine` and `restate-suletta` each retain one Bound 10Gi
orphaned PVC (`storage-restate-0`, no pods reference them). Recommend a
follow-up cleanup deleting the two PVCs and namespaces after confirming no PV
reclaim need. No action taken here.

## Files changed by this chunk

None. Report-only verification chunk.
