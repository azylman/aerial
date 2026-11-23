# PR lifecycle parity goldens

Each JSON file is one PR lifecycle. Two tests read it from separate Go
modules:

- `services/synapse/internal/router/restate_edge_test.go`
  (`TestRestateOwnedLifecycleEdge`) drives synapse over `synapse` (webhooks,
  alerts, watcher observations, register calls) and asserts it sends the
  `restate` stream and performs no effect itself.
- `services/vanadis-restate/internal/prstate/parity_test.go` replays
  `restate` through the real PRObject/RepoObject handlers on an in-memory
  Restate and asserts the `effects` multiset and the final `rows`.

Synapse has no registry and no legacy pipeline; the `effects`, `rows` and
`fact_rollout_states` fields are hand-derived from the lifecycle the legacy
pipeline used to run, so prstate is still proven against it.

## Fields

- `effects`: every side effect as `kind` plus the generation-free match key
  (`repo#pr@sha:kind[:job=..][:check=..]`). Facts are
  `pr_fact_<event>`. Repo effects (`github_auto_update`, `flux_git_push`,
  `image_promote`, `flux_reconcile` with no PR) use PR 0.
- `fact_rollout_states`: the `rollout_state` every published fact carries,
  keyed by the fact's match key and compared as a multiset. prstate renders
  it from committed PRObject state (`BuildPRRolloutState`) when the fact
  carries none. Build VO stores a chunk's rollout state only from these
  facts. `../rollout_state/render.json` pins the render per row.
- `rows`: the final pr_registry rows, written out by hand. Metadata leaves
  out empty values because prstate projects them as absent keys.
- `restate`: the wire contract synapse sends. Every lifecycle fact rides the event that owns it, so PRObject
  decides whether the fact fires.
- `github_routes`: canned GitHub API bodies (kept for reference; no test
  reads them since the legacy synapse harness was deleted).

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
- A direct alert or sync deploy leaves the row status alone.
- Deploy-failure exhaustion writes no banner. The error rides the
  `deploy_failed` fact.
- Deploy-phase events carry the main branch, which never replaces the PR
  branch on the row.
- The synapse edge sends the open-PR auto-update cascade trigger
  (`RepoObject.HandleAutoUpdate`) itself only for triggers it can judge
  without state: a main push, a CI failure on a known PR, and a gate or
  RoboRev success. A recovered check_run success instead carries the policy
  on `HandleCIRecovered` (`auto_update`). PRObject forwards the trigger only
  when the recovery applies, because only PRObject knows the PR was
  CI-failed.
- `HandleSynchronized` resolves a stored conflict itself. The synapse edge
  cannot know the row was in conflict, so it never sends a follow-up
  `HandleConflictResolved`.
- Every send the synapse edge makes appears in `restate`, including
  state-only ones such as `HandleCheckPassed` and the `HandlePushBySHA`
  envelope for a push with no resolved PR. The synapse
  `TestRestateOwnedLifecycleEdge` test drives each golden through the real
  accept points. It asserts that the sends match this stream handler for
  handler and that synapse performs no effect.
