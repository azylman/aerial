# PR lifecycle parity goldens

Each JSON file is one PR lifecycle. Two tests read it from separate Go
modules, so both effect owners must land on the same outcome:

- `services/synapse/internal/router/restate_parity_test.go` drives the
  legacy router over `synapse` (webhooks, alerts, watcher observations,
  register calls). It uses a stateful fake registry, a fake GitHub API and a
  fake dispatcher.
- `services/vanadis-restate/internal/prstate/parity_test.go` replays
  `restate` (the events synapse sends with `effects_owner=restate`) through
  the real PRObject/RepoObject handlers on an in-memory Restate.

Both tests assert the same `effects` multiset and the same final `rows`.

## Fields

- `effects`: every side effect as `kind` plus the generation-free match key
  (`repo#pr@sha:kind[:job=..][:check=..]`). The shadow gates join
  `[synapse-effect]` and `[prstate-shadow]` lines on this same key. Facts are
  `pr_fact_<event>`. Repo effects (`github_auto_update`, `flux_git_push`,
  `image_promote`, `flux_reconcile` with no PR) use PR 0.
- `rows`: the final pr_registry rows, written out by hand. Metadata leaves
  out empty values because prstate projects them as absent keys.
- `restate`: the wire contract synapse must send once it owns the event as
  restate. Every lifecycle fact rides the event that owns it, so PRObject
  decides whether the fact fires.
- `github_routes`: canned GitHub API bodies for the legacy side.

## Contract notes the goldens pin

- A deploy alert or sync names only the merge commit (`head_sha` empty).
  The same goes for a squash-merge push, which carries the pushed commit as
  both `head_sha` and `merge_sha`. PRObject judges these against the stored
  merge SHA and never against the PR head. A deploy alert that lands before
  `pull_request.closed` therefore still publishes `deploy_success`.
- A watcher-tracked rollout publishes `deploy_success` once per PR, when
  its last Kustomization completes. Before that, each completion publishes
  `rollout_updated`. In order, the restate watcher path sends:
  1. `HandleRolloutProgress` for each completed Kustomization, carrying its
     ready jobs, the Kustomization and the `rollout_updated` fact.
  2. `HandleDeploySuccess`, carrying the `deploy_success` fact.
- A direct alert or sync deploy leaves the row status alone. Legacy never
  writes it on those paths.
- Deploy-failure exhaustion writes no banner. The error rides the
  `deploy_failed` fact.
- Deploy-phase events carry the main branch, which never replaces the PR
  branch on the row.
