---
name: codebase-sweep
description: >-
  Use when planning or running a repo-wide cleanup sweep: dead code removal, test-suite compaction, or consolidating duplicated logic across services and repos, or when the user asks how much code can come out, why the codebase grew, or to repeat a previous sweep. Covers building the inventory from production coverage plus static analysis, classifying items, surfacing product decisions, turning the inventory into staged Vanadis builds, and evaluating the results afterwards.
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
- **Run every test under `env -i` with only PATH, HOME and GOPATH-style vars set.** The agent's pod environment carries real endpoints (`ERI_HOOK_ENDPOINT`, `CONFIG_PATH`, `POSTGRES_*`, `AGENT_NAME`). A non-hermetic test will happily post to live services or boot a real daemon. Any test that changes behavior under the real env goes on the bug list.
- Use bullet lines in the inventory, never tables.

### 2a. Dead code signals
- **Production coverage** (the per-function coverage table that coverage-ingest fills). It is sticky: once a function executes it keeps `last_executed_at`, so empty means it never ran since `first_seen`.
  - A few days of data covers everything except failure recovery. There are no weekly jobs unless the user says otherwise.
  - A partial % means some branches never ran. Data is per function, so dead branches inside live functions still need a static read.
  - **"Stopped running" is a second signal.** Group functions by `last_executed_at`. A big cluster that last ran on one date usually marks a feature switch-over (e.g. the day a new UI shell became the default). That cluster is legacy code.
- **Static analysis**: `deadcode` per module, with and without `-test` (reachable only from tests shows up without it), `golangci-lint --enable-only unused`, and `knip --production` with prod-only entry points for TS. Default knip configs list test files as entries, which hides test-only exports.
- **Know the blind spots and list them in the inventory:**
  - Binaries built with `-cover` but no `-coverpkg` only instrument their own module. Code from another module running inside them shows as 0%.
  - Shared library modules (`pkg/`) usually have no rows at all. Use static evidence across every consuming module in every repo.
  - Binaries that never flush coverage (CLIs, hooks) have no data.
  - RTA-based `deadcode` treats every exported method of a type that reaches reflection (fmt, json, templates) as live. Run `deadcode -whylive` on prod-0% exported methods to find the ones reachable only through reflection.
  - Deadcode-gate anchors (`_ = x.Method`, `var _ = []any{...}`, `keepLive(...)`) deliberately hide dead code from CI. Remove them temporarily, re-run, then restore.
  - Coverage rows for services that are no longer collected. Ignore them and add a pruning item.
- **False-positive checklist** before calling anything dead: Restate `Reflect` registers all exported methods of a service struct, MCP tool registration, HTTP route tables, `go:embed`, `init()`, build tags, interfaces satisfied implicitly, generated code, and callers in other repos (k8s args, scripts, config keys).

Classify each item as:
- `[delete]`: 0% in prod and unreachable or test-only reachable. Also anything static analysis flags in code with no coverage data (scripts, tools).
- `[review-recovery]`: 0% in prod, reachable, and it handles failure, retry or crash recovery. Keep by default and give a one-line description of the failure.
- `[review-feature]`: 0% in prod, reachable, and not failure handling. A feature nobody uses. Name who would call it. **These are product decisions for the user, not cleanup.**

Item line: `- [class] path: Symbol (~N LOC, +M test LOC) — evidence: prod 0% since <date> / deadcode / no refs — note`

### 2b. Test compaction signals
- Per-package headroom: covered statements above the floor (`scripts/check-coverage.sh`). Most packages sit right on the floor, so compaction usually means merging tests into tables, not deleting them.
- **Per-test coverage**: build each package's test binary once (`go test -c -cover -covermode=set`) and run every top-level `Test*` alone with `-test.coverprofile`. Check that the union reproduces package coverage. Then compute tests with zero unique statements, and a greedy set of tests removable together with zero statement loss (remove the largest first). This is only a candidate list: hand-check that each test's assertions exist elsewhere, and keep concurrency, ordering, idempotency and drain tests.
- Rule violations that compaction must fix anyway: closure fields in tables, mode-switch union tables, sequential literal `t.Run` blocks, catch-all `*Coverage*` tests, test-only hooks on prod structs.
- Mechanical wins: multi-line table rows that fit on one line under gofmt, duplicated fakes across packages (one shared fake package per boundary), duplicated hermetic TestMain harnesses.
- Tests for code on the delete list: count them with the dead code, not as compaction.

### 2c. Duplicated logic signals
- `jscpd` for exact clones (exact copy-paste is usually rare).
- An exact-function index: hash every top-level func body after stripping comments, whitespace and package qualifiers, then group matches across directories.
- A near-function matcher: token-level difflib ratio ≥0.80 for funcs of 8+ lines in different directories.
- Targeted greps for the usual suspects: Restate ingress clients, agent-name and service-name helpers, DB open helpers, k8s auth middleware, GitHub/Discord clients, coverage flush, retry and backoff.
- **Record drift.** Copies that already behave differently (substring vs exact match, different fallbacks or timeouts) are bugs. The plan must pick one behavior and say why.
- Note module boundaries (`internal/` can't cross modules; sharing needs a `pkg/` package and possibly a replace directive), and the per-stack rule: share code via packages, never via cross-service SQL.

### 2d. Verify the inventory's claims about system state
Inventory agents read code, not live state. Before you report something like "build X is stuck", "flag Y was never switched on", or "this replaced that", check it yourself: `get_build` for build status, `git log -n` for what merged, and the live config mount for flags. Prior sweep lesson: an inventory said a port's build was blocked on its soak gate. In fact the build had completed, and the legacy trackers had already been deleted without the new path ever being switched on.

## 3. Report and get decisions
Lead with what can come out safely (delete, compaction, duplication, with LOC each). Then list only the calls the user has to make (whole services, features, legacy paths, VRAM-holding models). Then blind spots, bugs found along the way, and any side effects the inventory caused. Record the user's answers in `inventory/DECISIONS.md`. It overrides every inventory file, and every chunk reads it first.

## 4. Turn it into staged builds
Load the self-improvement skill and the Vanadis build authoring reference. Sweeps are Tier 3: Girl Gang plan review, then human approval before enqueue. Split builds where risk changes:
1. **Feature cutovers and removals the user approved** (e.g. switching on a dark replacement, then deleting the legacy path). Own build, with live soak gates that write evidence files. Each destructive chunk re-proves the previous gate before deleting anything.
2. **Dead code purge**, parallel chunks per service group and repo, plus a live verify chunk per deploy group. Also close coverage blind spots here so the next sweep has data.
3. **Shared helpers**: new `pkg/` packages first, then one adoption chunk per module in parallel, then cross-repo adoption.
4. **Test compaction** last, so nobody compacts tests for code that is about to go. Per package, ordered by headroom, re-measuring coverage in each chunk.

Pitfalls the review panel caught last time:
- Clones go stale within hours on a busy day. Re-fetch every repo right before writing the plan, and re-derive config-repo items.
- An action gate that ends its turn on HOLD still counts as completed. Gates keep resumable evidence files, wait for real traffic in checkpointed batches, and call `fail_chunk` (a retryable pause) when blocked.
- A config key removal unblocks when the code PR merges, not when it deploys. Put a live-check chunk between them, or hot reload flips behavior while the old image is still running.
- Moving tests off an in-memory SQL dialect onto fakes loses real query coverage. Only do it alongside a real hermetic SQL path.
- New shared packages with no callers trip the deadcode gate. Land each one with its first adopter.

Chunk contract: point at inventory sections instead of copying item lists, act only on `[delete]` items plus DECISIONS.md promotions, re-check every item against current main, skip still-live items and list them in the PR, and never add catch-all coverage tests.

## 5. Evaluate the results
When the last build completes, write `inventory/RESULTS.md` and report:
- Net LOC removed per repo, prod vs test, against the inventory estimate. Use the same `--numstat` method as step 1, and diff from the commit before the first sweep PR to the commit after the last.
- Coverage floors: every package still ≥95%, plus each package's headroom before and after.
- Prod-0% function count per service, before and after (from the coverage table).
- Skipped items and why (still live, false positive, coverage). A high skip rate in one area means its inventory method needs fixing.
- Incidents and regressions traced to sweep PRs (VictoriaLogs error patterns, reverted PRs, CI or RoboRev remediation rounds per chunk).
- Blind spots closed, and which remain.
- Lessons: update this skill with any new false-positive class, blind spot or tooling that worked.
