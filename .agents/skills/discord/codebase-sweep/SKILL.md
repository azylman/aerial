---
name: codebase-sweep
description: >-
  Use when planning or running a repo-wide cleanup sweep (dead code removal, test-suite compaction, or consolidating duplicated logic across services and repos), or when asked how much code can come out, why the codebase grew, or to repeat a previous sweep.
---

# Codebase Cleanup Sweep

A sweep removes code that never runs, tests that add no signal, and logic copied between services, without changing live behavior or dropping any package below its coverage floor. The planner finds and classifies everything up front. Chunks only execute a reviewed list.

## 1. Measure growth first
- Net LOC per repo and per service since the last sweep (`git log --numstat` bucketed by week; count tests separately from prod code).
- Find hotspots: services with the most growth, the highest test share, and the largest single test files.
- Find the previous sweep's PRs and use them as precedent for chunk size and shape.
- Report the numbers before proposing anything. The user decides whether a sweep is worth it.

## 2. Build the inventory before writing any plan
Work in fresh clones of every repo at current main, under one scratch dir, e.g. `/data/scratch/sweep/{repo...}`. Write a shared `BRIEF.md` (goal, hard rules, inputs, classification, item line format), then fan out one read-only subagent per area: dead code per repo or service group, test compaction per repo, duplicated logic across repos. Each one writes `inventory/<area>.md` and returns only totals plus its top items.

Hard rules for inventory agents:
- Read-only. Never commit, push, or touch the live cluster.
- **Run every Go command under `hermetic_go` (source `scripts/lib/hermetic-go-env.sh`) or `env -i` with only toolchain/OS allowlist vars set.** The agent's pod environment carries real endpoints (`ERI_HOOK_ENDPOINT`, `CONFIG_PATH`, `POSTGRES_*`, `AGENT_NAME`). A non-hermetic test will happily post to live services or boot a real daemon. Any test that changes behavior under the real env goes on the bug list.
- Use bullet lines in the inventory, never tables.

### 2a. Dead code signals
- **Production coverage** (`production_code_coverage`, populated by `services/coverage-ingest`; query dormant rows via `scripts/coverage-query.sh [service] [days_dormant]` or `psql -c "SELECT ..."` which connect automatically via the local `/var/run/postgresql` proxy without needing `POSTGRES_URL` or `/secrets/POSTGRES_PASSWORD`). It is sticky: once a function executes it keeps `last_executed_at`, so `NULL` means it never ran since `first_seen_at`.
  - A few days of data covers everything except failure recovery. There are no weekly jobs unless the user says otherwise.
  - A partial % means some branches never ran. Data is per function, so dead branches inside live functions still need a static read.
  - **"Stopped running" is a second signal.** Group functions by `last_executed_at`. A big cluster that last ran on one date usually marks a feature switch-over (e.g. the day a new UI shell became the default). That cluster is legacy code.
- **Static analysis across all stack languages**:
  - **Go**: `deadcode` per module, with and without `-test` (reachable only from tests shows up without it), and `golangci-lint --enable-only unused`.
  - **TypeScript / UI**: `knip --production` with prod-only entry points (default knip configs list test files as entries, which hides test-only exports).
  - **Rust (`senses` in `aerial-sidecars`) & Python**: `cargo clippy --all-targets -- -W dead_code -W unused` for Rust crates, and unused symbol/script checks for Python tooling.
  - **Schema, K8s & Observability**: Cross-check `services/eri/pkg/db/schema.sql` tables and columns against single-service ownership in Go/Rust/Python code, check `k8s/*.yaml` across `aerial` and `aerial-config` for orphaned manifests/ConfigMaps/service keys, and check Grafana dashboards/vmalert rules for metrics no service emits.
- **Know the blind spots and list them in the inventory:**
  - Binaries built with `-cover` but no `-coverpkg` only instrument their own module. In `aerial`'s single root `go.mod`, Bazel instruments `pkg/` packages via `--instrumentation_filter`, but multi-module or non-Bazel repos still show code from another module as 0%.
  - Shared library modules (`pkg/`) in multi-module or non-Bazel repos usually have no rows at all; use static evidence across every consuming module in every repo.
  - Binaries that never flush coverage (CLIs, hooks) have no data.
  - RTA-based `deadcode` treats every exported method of a type that reaches reflection (fmt, json, templates) as live. Run `deadcode -whylive` on prod-0% exported methods to find the ones reachable only through reflection.
  - Deadcode-gate anchors (`_ = x.Method`, `var _ = []any{...}`, `keepLive(...)`) deliberately hide dead code from CI. Remove them temporarily, re-run, then restore.
  - `services/coverage-ingest` auto-prunes retired services via `PruneServices`—check manually for unpruned service rows.
- **False-positive checklist** before calling anything dead: Restate `Reflect` registers all exported methods of a service struct, MCP tool registration, HTTP route tables, `go:embed`, `init()`, build tags, interfaces satisfied implicitly, generated code, and callers in other repos (k8s args, scripts, config keys).

Classify each item as:
- `[delete]`: 0% in prod and unreachable or test-only reachable. Also anything static analysis flags in code with no coverage data (scripts, tools).
- `[review-recovery]`: 0% in prod, reachable, and it handles failure, retry or crash recovery. Keep by default and give a one-line description of the failure.
- `[review-feature]`: 0% in prod, reachable, and not failure handling. A feature nobody uses. Name who would call it. **These are product decisions for the user, not cleanup.**

Item line: `- [class] path: Symbol (~N LOC, +M test LOC) — evidence: prod 0% since <date> / deadcode / no refs — note`

### 2b. Test compaction signals
- Per-package headroom: covered statements above the floor (`./scripts/verify.sh` or `bazel coverage //...` via `coverage-gate.awk`). Most packages sit right on the floor, so compaction usually means merging tests into tables, not deleting them. Test table structs are hard-capped at `<=16` fields in `scripts/code-health`, so compaction must never merge heterogeneous setup modes into >16-field Swiss-army structs.
- **Per-test coverage**: build each package's test binary once (`go test -c -cover -covermode=set`) and run every top-level `Test*` alone with `-test.coverprofile`. Check that the union reproduces package coverage. Then compute tests with zero unique statements, and a greedy set of tests removable together with zero statement loss (remove the largest first). This is only a candidate list: hand-check that each test's assertions exist elsewhere, and keep concurrency, ordering, idempotency and drain tests.
- Rule violations that compaction must fix anyway (enforced by `scripts/code-health`): closure fields in tables, test table structs exceeding 16 fields, mode-switch union tables, sequential literal `t.Run` blocks, catch-all `*Coverage*` / `*_extra_test.go` tests, test-only hooks on prod structs.
- Mechanical wins: multi-line table rows that fit on one line under gofmt, duplicated fakes across packages (one shared fake package per boundary), duplicated hermetic TestMain harnesses.
- Tests for code on the delete list: count them with the dead code, not as compaction.

### 2c. Duplicated logic signals
- **Built-in AST & struct clone detector (`scripts/code-health`)**: `scripts/code-health` already detects cross-package Go function clones (3-gram token shingles on functions ≥20 lines at ≥0.80 Jaccard / ≥0.85 edit similarity) and struct field clones (≥5 fields at ≥0.85 similarity) within a single repo. Start there before writing custom scanners.
- **Cross-repo & short-function scans**: For clones spanning `aerial`, `aerial-sidecars`, and `mirrormere` (or functions of 8–19 lines below the `code-health` floor), use:
  - `jscpd` for exact copy-paste clones across repos.
  - An exact-function index: hash every top-level func body after stripping comments, whitespace and package qualifiers, then group matches across directories and repos.
  - A near-function matcher: token-level difflib ratio ≥0.80 for funcs of 8+ lines in different directories or repos.
- Targeted greps for the usual suspects: Restate ingress clients, agent-name and service-name helpers, DB open helpers, k8s auth middleware, GitHub/Discord/MCP clients, coverage flush, retry and backoff.
- **Record drift.** Copies that already behave differently (substring vs exact match, different fallbacks or timeouts) are bugs. The plan must pick one behavior and say why.
- Note module boundaries (`internal/` can't cross modules; sharing needs a `pkg/` package and possibly a replace directive), Bazel single-module instrumentation (`aerial`'s root `go.mod` vs multi-module sidecars needing static cross-repo checks), and the per-stack rule: share code via packages, never via cross-service SQL.

### 2d. Verify the inventory's claims about system state
Inventory agents read code, not live state. Before reporting that a build is stuck, a feature flag was never enabled, or a replacement path superseded a legacy one, verify live state directly: call `get_build` for Vanadis build status, check `git log -n` for what actually merged, and inspect the live `/share/aerial-config` mount or Kubernetes manifests to confirm which path is active in production. Never schedule a legacy path for deletion until you verify its replacement is both deployed and enabled in live config.

## 3. Report and get decisions
Lead with what can come out safely (delete, compaction, duplication, with LOC each). Then list only the calls the user has to make (whole services, features, legacy paths, VRAM-holding models). Then blind spots, bugs found along the way, and any side effects the inventory caused. Record the user's answers in `inventory/DECISIONS.md`. It overrides every inventory file, and every chunk reads it first.

## 4. Turn it into staged builds
**REQUIRED SUB-SKILL:** Load `self-improvement` and its `references/vanadis-build-authoring.md`. Sweeps are Tier 3: Girl Gang plan review, then human approval before enqueue. Split builds where risk changes:
1. **Feature cutovers and removals the user approved** (e.g. switching on a dark replacement, then deleting the legacy path). Own build, with live soak gates that write evidence files. Each destructive chunk re-proves the previous gate before deleting anything.
2. **Dead code purge**, parallel chunks per service group and repo, plus a live verify chunk per deploy group. Close coverage blind spots here so the next sweep has data, and drop any dead schema tables/columns via declarative `schema.sql` + Atlas migrations (`scripts/atlas-diff.sh`).
3. **Shared helpers**: new `pkg/` packages first, then one adoption chunk per module in parallel, then cross-repo adoption.
4. **Test compaction** last, so nobody compacts tests for code that is about to go. Per package, ordered by headroom, re-measuring coverage in each chunk.

Pitfalls the review panel caught last time:
- Clones go stale within hours on a busy day. Re-fetch every repo right before writing the plan, and re-derive config-repo items.
- An action gate completes only when its worker calls `complete_chunk`, so a gate that finds HOLD must not call it: it calls `fail_chunk` (a retryable pause) instead. Gates keep resumable evidence files, wait for real traffic in checkpointed batches, and time-box each wait to 45 min per turn.
- A config key removal unblocks when the code PR merges, not when it deploys. Put a live-check chunk between them, or hot reload flips behavior while the old image is still running.
- Moving tests off an in-memory SQL dialect onto fakes loses real query coverage. Only do it alongside a real hermetic SQL path (`pkg/pgtest`), and migrate legacy `github.com/lib/pq` / custom `sql.Register` mock drivers directly onto `pkg/pgutil` (`OpenSQL` + `WithSearchPath` or `NewPool`) paired with `pkg/pgtest`.
- New shared packages with no callers trip the deadcode gate. Land each one with its first adopter.
- Extracting a shared wire struct into `pkg/` or a type alias (`type A = pkg.A`) can cascade into cross-package struct-clone violations if parent DTOs referencing that field also share ≥85% field similarity. Run `go run ./scripts/code-health -root . -baseline .code-health-baseline.json` across the full repo after extracting any shared struct so transitive DTO clones are extracted in the same chunk.
- Shrinking or deleting grandfathered files and functions leaves slack in `.code-health-baseline.json`. Ratchet the baseline down (`go run ./scripts/code-health -root . -baseline .code-health-baseline.json -update`) in cleanup chunks so removed bloat cannot grow back, and bump `CODE_HEALTH_REF` / `CODE_HEALTH_BIN` / `CODE_HEALTH_PKG` pins in downstream repo `scripts/verify.sh` files to match upstream `aerial/scripts/code-health`.

Chunk contract: point at inventory sections instead of copying item lists, act only on `[delete]` items plus DECISIONS.md promotions, re-check every item against current main, ratchet `.code-health-baseline.json` when grandfathered files shrink or disappear, skip still-live items and list them in the PR, and never add catch-all coverage tests.

## 5. Evaluate the results
When the last build completes, write `inventory/RESULTS.md` and report:
- Net LOC removed per repo, prod vs test, against the inventory estimate. Use the same `--numstat` method as step 1, and diff from the commit before the first sweep PR to the commit after the last.
- Coverage floors: every package still ≥95%, plus each package's headroom before and after.
- Code-health ratchets: reduction in `.code-health-baseline.json` grandfathered file and function entries across repos.
- Prod-0% function count per service, before and after (from `production_code_coverage`).
- Skipped items and why (still live, false positive, coverage). A high skip rate in one area means its inventory method needs fixing.
- Incidents and regressions traced to sweep PRs (VictoriaLogs error patterns, reverted PRs, CI or RoboRev remediation rounds per chunk).
- Blind spots closed, and which remain.
- Lessons: update this skill with any new false-positive class, blind spot or tooling that worked.
