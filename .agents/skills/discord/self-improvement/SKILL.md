---
name: self-improvement
description: Tiered engineering workflow (Tiers 0-3), Girl Gang multi-agent review orchestration, Subagent-Driven Development (SDD), Dynamic Scope Re-Tiering, and CI/RoboRev remediation across Aerial repositories. Load for Tier 1-3 planning and reviews, Vanadis builds, schema migrations, or CI/RoboRev failure wakes.
---

# Aerial Self-Improvement & Tiered Engineering Workflow

Universal coding, testing, scratch workspace (`scripts/aerial-pr.sh init/submit`), and repository topology invariants are always loaded via `rules/discord/04-engineering.md` and `rules/discord/06-architecture.md`. This skill defines the **Tiered Review & Orchestration Workflow**, **Scope Escalation Gates**, and **CI / RoboRev Remediation Playbook**.

## Specialized Reference Runbooks (Load Only When Needed)
- **Vanadis Build Plans (`scripts/aerial-build.sh enqueue`)**: Read [`references/vanadis-build-authoring.md`](references/vanadis-build-authoring.md) before authoring a single-chunk or multi-chunk build JSON.
- **Declarative Database Schema Migrations (`schema.sql` / Ariga Atlas)**: Read [`references/atlas-migrations.md`](references/atlas-migrations.md) whenever modifying database schemas.

---

## 1. Complexity Tiers & Review Matrix

Scale review rigor and subagent orchestration dynamically by blast radius:

- **Tier 0 (≤ 5 LOC, single-line changes, config tweaks, doc typos)**:
  - **Review Overhead**: None. Execute inline, verify with `./scripts/verify.sh --staged`, and submit via `scripts/aerial-pr.sh submit`.
  - **Negative Scope Blacklist**: Strictly forbidden for SQL/database schemas, security/auth primitives, concurrency/mutex logic, Nomad/Docker topology, or core queue/runner loops (auto-escalates to Tier 1+).

- **Tier 1 (< 50 LOC, targeted bugfixes & test additions)**:
  - **Review Overhead**: None (plan and diff review subagents bypassed).
  - **Execution**: Routed through Vanadis single-chunk builds (`scripts/aerial-build.sh enqueue <plan.json>`) by default to keep the primary channel daemon and root transcript featherweight. Verify via targeted package unit tests and `./scripts/verify.sh --staged`.

- **Tier 2 (50–200 LOC, multi-PR/cross-repo rollouts, standard features & multi-package refactors)**:
  - **Plan Review**: The Girl Gang review panel audits the plan in a single isolated subagent across 4 disciplines (~45s).
  - **Execution**:
    - **Multi-PR / Cross-Repo Plans (≥ 2 PRs)**: Enqueue as a single multi-chunk Vanadis build via `scripts/aerial-build.sh enqueue <plan.json>` (see [`references/vanadis-build-authoring.md`](references/vanadis-build-authoring.md)).
    - **Single-PR Plans (1 PR in 1 repo)**: Continuous autonomous execution inline or via scoped implementation subagents, followed by Consolidated Devil's Advocate diff review (~30s) before `scripts/aerial-pr.sh submit`.

- **Tier 3 (> 200 LOC, core architecture, core queue/runner logic, database migrations, breaking changes)**:
  - **Triggers**:
    - Cumulative diff **> 200 LOC** (additions + deletions).
    - Touches to **core queue logic** (`services/eri/pkg/queue`, `WorkerPool`, worker turn loops, burst scheduling).
    - Touches to **core runner logic and turn resolution** (`services/eri/pkg/runner`, streaming daemons, process pools, turn sinks/resolvers).
    - Database schema migrations (`schema.sql`, SQL migrations) or service topology changes (`nomad/jobs/*.nomad`, Dockerfiles).
    - Cross-cutting architectural overhauls or breaking API changes.
  - **Plan Review**: The Girl Gang audits the plan in an isolated subagent across 4 disciplines (~45s).
  - **Human Checkpoint**: **MANDATORY HUMAN REVIEW CHECKPOINT (STOP)**—synthesize panel findings and wait for explicit user approval before touching code or enqueueing a Vanadis build.
  - **Execution**: Once approved, enqueue multi-PR/multi-chunk plans via `scripts/aerial-build.sh enqueue <plan.json>` (default), or execute single-PR changes with a full 4-discipline Girl Gang diff audit before `scripts/aerial-pr.sh submit`.

### Review Panel Composition & Subagent Execution Invariants
- **The Girl Gang (4 Disciplines)**: 3 domain specialists dynamically tailored to the change (e.g., Systems/Concurrency, Architecture, Security, Data Integrity, or PWA/Frontend when user-facing UI is touched) + 1 mandatory Adversarial Devil's Advocate.
- **Single Consolidated Review Subagent**: Always run The Girl Gang inside a **single `research` subagent** (`role: "TheGirlGangReviewer"`, `TypeName: "research"`). Never spawn multiple parallel review subagents or emit intermediate waiting prose while a subagent is running. Once parallel work is complete, stop calling tools with empty text so `agy` suspends until the reactive `<SYSTEM_MESSAGE>` wakeup arrives (never busy-poll `manage_subagents list` or read a running subagent's `transcript.jsonl` to check status).

---

## 2. Execution Stages & Dynamic Scope Re-Tiering

### Stage 1: Specification & Execution Routing (Vanadis Build vs. Single-PR)
1. **Multi-PR / Cross-Repo / Sequential Plans (≥ 2 PRs — Default to Vanadis)**:
   - Whenever a rollout requires ≥ 2 PRs (across `aerial`, `aerial-config`, `mirrormere`, or `aerial-sidecars`, including Two-Step Feature Rule changes, or multiple sequential chunks in one repo), load [`references/vanadis-build-authoring.md`](references/vanadis-build-authoring.md), batch-read the target files once up front, and design a single multi-chunk Vanadis build plan (`plan.json` with per-chunk `"repo"` fields). Never propose or execute manual turn-by-turn *"Step 1 PR now, then wait for deploy to run Step 2"* loops.
2. **Single-PR Plans (1 PR in 1 repo)**:
   - Initialize the scratch workspace via `/share/aerial/scripts/aerial-pr.sh init [repo]` and decompose Tier 2/3 work into discrete, self-contained tasks sized to **~15 minutes** (1 failing test + minimal implementation + local verification + atomic commit).

### Stage 2–3: Plan Audit & Tier 3 Checkpoint
- Run the Tier 2/3 Girl Gang plan review subagent and fold valid objections into the plan.
- For **Tier 3**, **STOP** and present the plan and review synthesis to the user for explicit approval before writing code or enqueueing the Vanadis build. (Tiers 0–2 proceed immediately to Stage 4.)

### Stage 4: Enqueue Vanadis Build (≥ 2 PRs) or Autonomous Implementation (Single PR)
- **If ≥ 2 PRs / Multi-Chunk**: Enqueue the build directly via `/share/aerial/scripts/aerial-build.sh enqueue <plan.json>` and terminate the turn immediately (`"turn_action": "end_turn"`).
- **If Single PR**: Follow strict TDD (write failing test -> confirm RED -> write minimal code -> verify GREEN with `go test -race` -> refactor), dispatching scoped subagents per ~15-minute task if needed.

### Stage 5: Dynamic Scope Reconciliation & Post-Implementation Re-Tiering Gate
*MANDATORY: Never submit a PR based solely on upfront tier estimates.*
1. Run `git diff --stat` (or `git diff HEAD~1..HEAD --stat`) immediately after implementation.
2. Reconcile actual diff metrics against the initial tier:
   - **Tier 0 -> Tier 1/2**: Diff > 5 LOC escalates to Tier 1 (> 50 LOC to Tier 2).
   - **Tier 1 -> Tier 2/3**: Diff > 50 LOC escalates to Tier 2 (requires Devil's Advocate diff audit).
   - **Auto-Escalation to Tier 3**: If cumulative diff exceeds **200 LOC** OR touches **core queue logic** (`pkg/queue`), **core runner logic** (`pkg/runner`), or **database schemas**:
     - Run the full 4-discipline **Girl Gang** diff review subagent.
     - **MANDATORY HUMAN ESCALATION CHECKPOINT (STOP BEFORE AUTO-MERGE)**: Do NOT submit with auto-merge armed. Pause, report the scope escalation and diff metrics to Discord with the panel's review synthesis, and await explicit user authorization.

### Stage 6–7: Pre-Flight Verification & Asynchronous PR Submission
1. Run `./scripts/verify.sh --staged` and targeted package tests (`go test -race ./pkg/...`). If coverage is below 95%, run `./scripts/check-coverage.sh --service <svc> --check --gaps` and fold missing branch cases into canonical test tables in a single pass.
2. Audit `git diff` holistically against the objective (per final escalated tier).
3. Write `PR_DESCRIPTION.md` in the scratch root and submit:
   ```bash
   /share/aerial/scripts/aerial-pr.sh submit <scratch_dir> "feat(scope): concise summary"
   ```
4. End the turn immediately upon submission (`"turn_action": "end_turn"`).

---

## 3. Event-Driven CI Remediation & RoboRev Disagreement Playbook

When `synapse` wakes Brain on a CI check failure or `roborev` review failure, Aerial **must proactively remediate the PR** rather than merely reporting the failure:

### 3.1 Deterministic CI Check Failures (Tests, Lint, Coverage, Build)
1. Inspect the pre-extracted failure log in the wake prompt (or fetch check details if needed).
2. Initialize a scratch workspace on the PR branch:
   ```bash
   /share/aerial/scripts/aerial-pr.sh init <repo> --branch <branch>
   cd <scratch_dir>
   ```
3. Fix the root cause locally, run `./scripts/verify.sh --staged` and targeted package tests to confirm exit code 0, and submit the updated PR branch via `/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> "fix(scope): summary"` (never run bare `git push`, which lacks ephemeral `GITHUB_PAT` auth headers). Native auto-merge remains armed and will merge once CI turns green.

### 3.2 Independent RoboRev Evaluation & Disagreement Protocol
Distinguish deterministic CI checks (authoritative) from `roborev` AI reviews (advisory second opinion with read-only repo tools, lacking Discord thread intent, multi-chunk Vanadis build plans, and cross-repo context):
- **Signal Calibration**:
  - *High Trust (Mechanical & Objective)*: Duplicate code blocks, dead code, repo invariant violations (closure fields in table tests, `time.Sleep`, imperative DDL), unquoted shell/JQ vars, unchecked errors, and concrete logic/concurrency bugs. Fix these locally and push.
  - *Verify First (Subjective Design & Unverified Caller Claims)*: Subjective `design` abstractions, multi-chunk Vanadis boundary claims, or unverified caller assumptions. Verify against the codebase before acting.
- **Resolution Paths**:
  - **True Positive**: Fix the defect in a scratch checkout (`aerial-pr.sh init <repo> --branch <branch>`), run `./scripts/verify.sh --staged`, and update the PR via `/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> "fix(scope): summary"`.
  - **Context Gap**: Add a concise inline code comment explaining the non-obvious cross-file invariant and submit via `aerial-pr.sh submit <scratch_dir> "docs(scope): clarify invariant"`. Update `.roborev.toml` (`review_guidelines`) only if the gap reflects a general, repo-wide architectural pattern (never narrow one-off exceptions).
  - **False Positive / Architectural Disagreement**: Reject the suggestion and record a commit-SHA-bound technical rebuttal to pass the gate and clear the PR failure banner:
    ```bash
    /share/aerial/scripts/aerial-pr.sh --repo <repo> override-roborev <pr_num> '<technical_rebuttal>'
    ```

### 3.3 Post-Merge Confirmation Standard
- On PR merge and deployment confirmation, deliver responses in plain prose strictly capped at **two sentences max** (no markdown bullet lists, tables, or forward-looking checklists for nominal states; lift the cap only for unrecoverable failures requiring diagnostic logs).
