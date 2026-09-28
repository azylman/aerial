# Core Engineering, Testing & Deployment Invariants

1. **Physical Immutability & Ephemeral Workspaces**:
   - **Kernel Read-Only Invariant**: `/share/aerial-config` and `/share/aerial` are mounted strictly **read-only (`:ro`)** into `aerial-brain`. Any direct file writes or local git operations targeting `/share/aerial-config` or `/share/aerial` will fail with `EROFS: Read-only file system`.
   - **Ephemeral Scratch Workspaces**: All configuration, persona, skill, and engine updates must be authored in isolated scratch clones initialized via `scripts/aerial-config-pr.sh init` or `scripts/aerial-pr.sh init`, submitted asynchronously per Continuous Deployment invariants, and verified prior to commit.

2. **Continuous Deployment & Engineering Invariant**:
   - Whenever asked to modify, enhance, or fix the core engine, Aerial MUST invoke and follow the `self-improvement` skill (`.agents/skills/discord/self-improvement/SKILL.md`).
   - **Asynchronous PR Submission (`scripts/aerial-pr.sh submit`)**: Code modifications must be submitted asynchronously from ephemeral scratch workspaces. Fast pre-flight verification (`scripts/verify.sh --staged`) runs locally in <1s, pushes the branch, enables native auto-merge, and schedules a one-shot follow-up check via `scheduler-mcp`. On scheduled wake-up, Aerial verifies green CI, completes squash-merge via `scripts/aerial-pr.sh merge <pr_num>`, and reports deployment status in plain prose strictly capped at two sentences max.
   - **Mandatory PR Descriptions**: Descriptions are strictly mandatory via workspace `PR_DESCRIPTION.md` or `--body-file` (Inverted Pyramid format, zero markdown tables). Titles must be sanitized to prevent literal `\n` pollution in GitHub titles.
   - **Zero-Bypass Verification**: Under NO circumstance commit or push unverified changes; fresh verification evidence (`scripts/verify.sh --staged`) must be obtained prior to commit. Comprehensive monorepo sweeps and coverage gating are offloaded to GitHub Actions CI.

3. **Core Software Engineering & Hermetic Testing Invariants**:
   - **Production Database**: Aerial runs exclusively on PostgreSQL 16 with `pgvector` (`aerial-postgres`) for production persistence (messages, sessions, schedules, facts, embeddings, and Grafana).
   - **Hermetic In-Memory Test Fixtures**: All storage and database contract tests MUST use airgapped, in-memory database handles or `t.TempDir()` isolated files strictly for fast, hermetic unit testing. Unit tests MUST NEVER write to shared host database paths, `/data`, or `/share`.
   - **Subprocess & Runner Airgapping**: Live agent runner execution (`runner.RunAgy`), real `agy` binaries, and live shell subprocesses must NEVER execute during test runs. Production supplies `runner.RunAgy`; tests supply mock runner functions guarded by `isTestEnvironment()`.
   - **Pure Constructor Injection & Atomic Snapshots**: Packages MUST require explicitly passed dependencies in constructors; subpackage workers read dynamic configuration JIT via `cfg.Current()` snapshots and never cache scalar config fields in long-lived struct fields.
   - **Functional Core, Imperative Shell**: Factor business logic into pure, deterministic functions tested via fast, table-driven unit tests (< 1ms). Reserve mock runner and subprocess orchestration strictly for concurrency plumbing. Detailed testing guidelines are codified in `self-improvement` skill.
   - **Zero Arbitrary Sleeps**: Arbitrary sleeps (`time.Sleep`) are strictly prohibited in tests and production plumbing. Asynchronous coordination must use event-driven signaling, condition variables, channel selects, or injected delay overrides.
   - **External Boundary Abstraction & Test Isolation**: Abstract external boundaries (git, Docker sockets, Discord REST, database) behind interfaces with full intra-package test parallelism (`t.Parallel()`).
   - **Single-Pass Coverage Audit Invariant**: Committing or pushing exploratory single-statement micro-tests to remote CI is strictly prohibited. When statement coverage falls below the required threshold, developers and agents must audit uncovered blocks locally (via scripts/test-linux.ps1 or scripts/check-coverage.sh --gaps) and satisfy the deficit in a single verification pass prior to commit.

4. **Multi-Agent Review Panel & Tiered Engineering Workflow**:
   - Code changes follow the Tiered Engineering Workflow dynamically scaled across four complexity tiers (Tier 0 through Tier 3) canonically detailed in the `self-improvement` skill (`.agents/skills/discord/self-improvement/SKILL.md`):
     - **Tier 0 (≤ 5 LOC, single-line changes, config/doc tweaks)**: Zero review subagents; direct implementation in the root thread with automated pre-flight verification. Negative scope blacklist: strictly forbidden for SQL/database schemas, security/auth, concurrency/mutex logic, Docker topology, or core runner loops (auto-escalates to Tier 1+).
     - **Tier 1 (< 50 LOC, targeted bugfixes & tests)**: Inline root-thread execution with zero review subagents; verified via targeted package unit tests and `./scripts/verify.sh --staged`.
     - **Tier 2 (50–200 LOC, standard features & refactors)**: The Girl Gang review panel audits the plan in an isolated subagent (~45s), followed by autonomous execution and a consolidated Devil's Advocate diff audit before opening the PR (~30s).
     - **Tier 3 (> 200 LOC, core architecture, schema migrations, breaking changes)**: The Girl Gang audits the plan in an isolated subagent. **MANDATORY HUMAN REVIEW CHECKPOINT (STOP)**: synthesize findings and wait for explicit user approval before touching code. Diff review panel audits before merge (~45s).
   - **Review Panel Invariant**: For Tiers 2 & 3, review panels (The Girl Gang) consist of exactly **4 reviewers / disciplines**: 3 domain specialists dynamically tailored to the change plus 1 mandatory Adversarial Devil's Advocate (PWA/Frontend included only when user-facing UI changes are involved), executed within a **single consolidated review subagent** (e.g. `role: "TheGirlGangReviewer"`, `TypeName: "research"`) to prevent print-mode drain and quadratic token compounding.
   - **Continuous Autonomous Execution**: For Tiers 0–2 (and approved Tier 3), execution is continuous without stopping between tasks; pre-PR diff review is consolidated before submission rather than after every micro-task.
   - **Subagent Model Tier & Cost Discipline Invariant**:
     - When delegating tasks to subagents via `invoke_subagent`, the primary agent must explicitly set the `Model` argument based on the cognitive role of the task:
       - **Low-Effort / Fast Tier (`Model: "flash"` or `Model: "flash_lite"`)**: Mandatory for all mechanical implementation, coding from task briefs/specs, unit test authoring, table-driven fixture expansion, lint/formatting remediation, and single-package refactors.
       - **Inherited / Standard Tier (`Model: "inherit"`)**: Reserved for cross-package integration debugging and multi-file dependency reconciliation.
       - **High-Effort / Reasoning Tier (`Model: "pro"`)**: Strictly reserved for architectural design reviews (The Girl Gang), whole-branch diff audits, and fix-loop escalations (rounds 4+).
     - **Zero-Inherit Default for Implementers**: Never omit the `Model` argument or leave it as `Model: "inherit"` when spawning implementation subagents. Inheriting the parent's high-effort reasoning model for transcription and test generation is treated as an architectural defect.

5. **Host-Native Tooling & Container Cleanliness Invariants**:
   - **Host-Native Execution**: When planning or executing builds, tests, lints, or script validations (`go test`, `node --test`, `golangci-lint run`, `./scripts/verify.sh`), always invoke the installed binaries directly in the workspace shell (`run_command`). Never wrap standard unit test commands in `docker run`.
   - **Ephemeral Container Cleanup**: External containers strictly required for tests (e.g. pgvector) must implement deterministic cleanup (`defer`, `t.Cleanup()`, or shell traps) with unique timestamped names.
   - **BuildKit**: Always execute image builds with Docker BuildKit enabled (`DOCKER_BUILDKIT=1`).

6. **Subprocess Signal Safety & Test Mocking Invariant**:
   - **Zero Raw Signal Broadcasts**: Unit tests must never execute raw POSIX signal broadcasts (`syscall.Kill` with negative or arbitrary PIDs) against the host environment.
   - **Dependency-Injected Mocking**: Subprocess signals, group termination, and error branches must be tested using managed dependency-injected mock functions (`killProcessGroupWith`, `terminateProcessGroupWith`) or dedicated child subprocesses.
