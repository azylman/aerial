# Chunk 11/14 Soak Gate Report: Confirm Clean Post-Cutover Soak

Build: build-1795109791-2ea91b63 — Port Synapse Lifecycle To Restate
Chunk: chunk-07gate-soak-gate (read-only gate, no code changes)
Dispatch: vanadis-chunk-07gate-soak-gate-turn-0
Date: 2026-11-19

## Verdict: CONDITIONAL HOLD — static probes pass, live deploy-cycle observation blocked

No code changes made (read-only chunk; legacy-tracker deletion forbidden
until this gate passes, and it stays forbidden after this report).

## Static probes (all PASS, observed 2026-11-19 in this dispatch turn)

1. Single journal-or-mirror core: `services/synapse/internal/router/restate_cutover.go`
   `routeEvent` sends authoritative XOR shadow; the two paths never
   double-fire. `routeCheckOutcome` journals nothing for unfixable payloads
   (pending/neutral, PR-less, SHA-less) so no redelivery loop.
2. Stale-SHA drops present in every handler family (`handlers_checks.go`,
   `handlers_deploy.go`, `handlers_repo.go` return `DispositionDropStale`);
   terminal states absorb within reopen epoch; reopened PRs start a new
   epoch so absorbing never bricks a reopen.
3. Effect keys generation-scoped: `EffectKey{repo, PR, headSHA,
   gateGeneration, kind}` (`contracts.go`); only `RecordKustomizationRequests`
   (`state.go`) bumps the generation / clears keys. Readiness retry mints
   no generation. No bare PR-key dedup.
4. Unmanaged collaborator PRs stay inside PRObject (`Routed` always false
   for unmanaged; `MarkCollaboratorAlerted` path in `handlers_checks.go`)
   and the synapse invoker has no BuildRouter send path (`restateclient`).
5. No bare HTTP inside handlers: the single `http.NewRequestWithContext`
   is the sanctioned Restate ingress transport in `restateclient/client.go`.
   No `os.Getenv` in touched packages; `restate_authoritative` comes from
   YAML only and is hot-reloadable without restart (`config.go`
   `RestartRequired` exclusion).
6. Tests green in this turn:
   - `synapse`: `go test ./internal/router/ ./internal/metrics/ ./internal/config/` → ok
   - `synapse router` + `vanadis-restate prstate` with `-race` → ok
   - `go vet` on touched packages → clean.

## Live probes (BLOCKED — environment, not code)

- Cluster API unreachable from this dispatch environment: every `kubectl`
  call fails at server discovery (`couldn't get current server API group
  list ... NotFound`). No pod status, no metric counters
  (`authoritative_sent` / `authoritative_error` /
  `RestateShadowDivergenceTotal`), no log scan, and no deploy-cycle
  observation were possible here.
- Soak criteria requiring live evidence remain unverified: ≥1 full deploy
  cycle observed, shadow/fallback activation count zero, duplicate-effect
  count zero, zero error-log anomalies, no legacy-tracker fallback
  activations.

## Required next step

Re-run the live half of this gate from an environment with cluster API
access (or re-dispatch this chunk there): confirm
`RestateShadowDivergenceTotal == 0`, `authoritative_error == 0`, exactly
one `authoritative_sent` per event with no duplicate user-visible effects
across one full deploy cycle, then pass. Until then chunk 12 (tracker
deletion) must not proceed.
