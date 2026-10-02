---
name: self-improvement
description: Mandatory skill whenever Aerial must inspect, modify, enhance, debug, author PRs, or refactor code across ANY repository (aerial, aerial-config, mirrormere, aerial-sidecars, etc.), author scratch PRs, OR pull upstream git releases and trigger container deployment syncs. DO NOT use for managing Home Assistant devices, user tasks, calendar events, recipes, or day-to-day conversation.
---

# Aerial Self-Improvement & Continuous Engineering Workflow

This skill defines how Aerial safely manages its own lifecycle across two operational paths:
1. **Path A (Operational Maintenance & Rollout)**: Fast-path upstream syncs, container reconciliations, and deployment status checks.
2. **Path B (Autonomous Self-Engineering)**: Multi-agent tiered review, TDD implementation, and scratch PR automation across all repositories.

---

## 1. Intent Triage Matrix (Decision Gate)

Before running any commands or dispatching review subagents, determine which operational path applies:

• **Path A: Operational Maintenance & Container Rollout (Zero Code Changes)**
  - **Triggers**: *"Update Aerial"*, *"Update yourself"*, *"Pull latest code / git sync"*, *"Reconcile containers"*, *"Check deployment status"*.
  - **Target Action**: Pull upstream images/git and reconcile runtime containers via Hangar daemon.
  - **Subagent Review Overhead**: **None (Bypassed)**. Execute Hangar endpoints directly in root turn.
  - **Execution Reference**: Proceed directly to **Section 2**.

• **Path B: Autonomous Self-Engineering (Code & Configuration Changes Across Any Repository)**
  - **Triggers**: *"Fix bug in X"*, *"Add feature / MCP service"*, *"Modify skill or prompt"*, *"Edit config.yaml / AGENTS.md"*, or any coding/refactoring task in ANY repository (`aerial`, `aerial-config`, `mirrormere`, `aerial-sidecars`, etc.).
  - **Target Action**: Implement, test, verify, and submit code or configuration changes across ANY repository via universal scratch PR workflows.
  - **Subagent Review Overhead**: **Tiered (Tiers 0–3)**. Scaled review panels, TDD, and scratch PR automation.
  - **Execution Reference**: Proceed directly to **Sections 3–5**.

---

## 2. Path A: Operational Maintenance & Container Rollout Runbook

When instructed to update Aerial, pull latest code, or deploy system updates without modifying code:

### Step 1: Trigger Fast-Path Git Sync & Reconciliation
Because `/share/aerial` and `/share/aerial-config` are mounted read-only (`:ro`), Aerial triggers the host Hangar daemon to pull upstream changes and reconcile containers out-of-band:
```bash
# Trigger GitOps repository sync on Hangar:
curl -s -f -X POST http://aerial-hangar:8080/sync

# Reconcile container topology (if services require recreation):
curl -s -f -X POST http://aerial-hangar:8080/reconcile
```

### Step 2: Track Deployment & Container Rollout
Check active deployment and container swap progress:
```bash
/share/aerial/scripts/aerial-pr.sh deploy-status main
```
- **Response Format Invariant**: Report deployment status in plain prose strictly capped at 2 sentences max (zero markdown bullet lists, tables, or forward-looking checklists).
- If deployment is ongoing, reschedule a 2-minute follow-up check via `scheduler-mcp` (`schedule_once`).

---

## 3. Path B: Universal Multi-Repository Architecture & Target Selection

Code and configuration changes across all repositories are governed by ephemeral scratch isolation, kernel `:ro` immutability, and strict separation of concerns.

### Repository Target Selection Decision Matrix

Before initializing a workspace, determine which repository owns the change based on intent and scope:

• **Target 1: User Configuration Repository (`azylman/aerial-config`)**
  - **Initialize Command**: `scripts/aerial-pr.sh init aerial-config`
  - **Scope & Contents**:
    - **Persona, Identity & Tone**: User guidelines, personal preferences, nicknames, household members, communication style (`rules/{common,discord,voice}/*.md`).
    - **Channel Instructions**: Channel-specific constraints and operating guidelines (`channels/<channel-name>.md`).
    - **Runtime Options & Toggles**: Non-secret user options (`config.yaml`: LLM models, timezones, channel modes, admin user lists, enabled MCP servers).
    - **Custom Operational Skills**: Private domain runbooks and workflows (`custom-skills/`: Home Assistant devices, calendar routines, recipes, personal tasks).
    - **Custom Telemetry Scrapes**: Prometheus scrape configurations (`victoriametrics/*.yml`: Home Assistant metrics, network endpoints).
    - **User Documentation**: Dynamic Docsify documentation portal (`docs/`).
    - **Host Container Overrides**: User-defined sidecars or extra local MCP servers (`docker-compose.override.yml`).

• **Target 2: Core Engine Monorepo (`azylman/aerial`)**
  - **Initialize Command**: `scripts/aerial-pr.sh init aerial` (or `scripts/aerial-pr.sh init`)
  - **Scope & Contents**:
    - **Core Backend Engine**: Go execution engine, runner loops, process pools, session management, database migrations (`brain/pkg/...`, `brain/main.go`).
    - **Built-in MCP Microservices**: Monorepo Go microservices (`discord-mcp/`, `dashboard/`, `sidecars/hangar/`).
    - **Base System Rules**: Immutable system invariants, architecture constraints, security rules (`rules/{common,discord,voice}/*.md`).
    - **Built-in System Skills**: Core engineering skills baked into image (`.agents/skills/discord/{self-improvement,incident-triage}`).
    - **Docker Infrastructure**: Base Docker Compose topology and service Dockerfiles (`docker-compose.yml`, `Dockerfile.*`).
    - **Monorepo Scripts**: Core CI, verification, and PR automation (`scripts/verify.*`, `scripts/aerial-pr.sh`).
  - **Strict Engine Invariants**:
    - **100% Generic & Domain-Agnostic**: All code, prompts, and schemas must remain reusable for any user.
    - **Zero Personal Data Invariant**: NEVER commit real names, Discord handles, usernames, family members, home addresses, private device/entity IDs, or user-specific business logic into `aerial`.
    - **Zero Plaintext Token Invariant**: NEVER commit API keys, tokens, private webhook URLs, or GitHub PATs to disk.

• **Target 3: Display Kiosks & Peripherals (`azylman/mirrormere`)**
  - **Initialize Command**: `scripts/aerial-pr.sh init mirrormere`
  - **Scope & Contents**: Smart display kiosk UI, frontend dashboard widgets, display hardware integration, touch controls, kiosk voice client.

• **Target 4: Auxiliary Sidecars (`azylman/aerial-sidecars`)**
  - **Initialize Command**: `scripts/aerial-pr.sh init aerial-sidecars`
  - **Scope & Contents**: Standalone auxiliary daemon services, hardware bridges, and companion utilities.

### The Two-Step Feature Rule (Engine Capability vs. Configuration)
When a feature introduces a new engine capability that requires user configuration (e.g. adding a new configuration field like `ignore_bots`):
- **Step 1 (Engine Capability)**: Implement the schema field, parsing, and runtime behavior in `azylman/aerial` (`brain/pkg/config/types.go` and `brain/pkg/...`), submitted and merged first.
- **Step 2 (Configuration Value)**: Enable or set the desired configuration value in `azylman/aerial-config` (`config.yaml`), submitted after engine support is deployed.

### Ephemeral Scratch Workspaces
Because host/container repositories are mounted read-only (`:ro`), all changes across ALL repositories must be authored in isolated scratch clones initialized via `scripts/aerial-pr.sh init [repo]`. Never bypass this with manual clones or raw GitHub MCP tools.

---

## 4. The Tiered Engineering & Review Workflow

Whenever undertaking feature development, architectural changes, bug fixes, or system modifications, Aerial follows a **Tiered Engineering Workflow** scaled dynamically by the complexity and blast radius of the change.

### Complexity Tiers & Review Matrix

Code review and execution are structured dynamically across four complexity tiers:

• **Tier 0 (≤ 5 LOC, single-line changes, config tweaks, doc typos)**:
  - **Review Overhead**: Zero review. Both plan review and diff review subagents are completely bypassed. Direct implementation with automated pre-flight syntax/YAML verification.
  - **Negative Scope Blacklist**: Strictly forbidden from Tier 0: SQL/database schemas, security/auth primitives, concurrency/mutex logic, Docker topology, or core runner loops (any touch auto-escalates to Tier 1+).

• **Tier 1 (< 50 LOC, targeted bugfixes & test additions)**:
  - **Review Overhead**: Zero review overhead. Plan and diff review subagents are completely bypassed to eliminate delegation overhead and premature yield chatter.
  - **Execution**: Autonomous continuous execution (no mandatory stop). Simple single-file changes execute directly inline; multi-step fixes or test expansions can be delegated to a scoped subagent to keep the root transcript featherweight. Verified via targeted package unit tests and local pre-flight verification (`./scripts/verify.sh --staged`).

• **Tier 2 (50–200 LOC, standard features & multi-package refactors)**:
  - **Plan Review**: The Girl Gang review panel audits the plan in an isolated subagent across 4 disciplines (~45s).
  - **Execution**: Autonomous execution (no mandatory stop; directly incorporates plan feedback). Zero mid-task pauses during coding.
  - **Diff Review**: Consolidated Devil's Advocate audits the unified `git diff` via isolated review subagent before opening the PR (~30s).

• **Tier 3 (> 200 LOC, core architecture, core queue/runner logic, database migrations, breaking changes)**:
  - **Scope & Explicit Examples**:
    - Any changes totaling **> 200 LOC** (additions + deletions).
    - Touches to **core queue logic** (`brain/pkg/queue`, `WorkerPool`, worker turn loops, queue partitioning, burst scheduling).
    - Touches to **core runner logic and turn resolution** (`brain/pkg/runner`, streaming daemons, process pools, turn sinks, turn resolvers, error recovery cascades).
    - Database schema migrations or ORM persistence routing (`pkg/session`, database models, SQL migrations).
    - Service topology expansions or Docker infrastructure refactors (`docker-compose.yml`, multi-arch sidecar pipelines).
    - Any cross-cutting architectural unification or public API breaking change.
  - **Plan Review**: The Girl Gang review panel audits the plan in an isolated subagent across 4 disciplines (~45s).
  - **Human Checkpoint**: **MANDATORY HUMAN REVIEW CHECKPOINT (STOP)**: synthesize panel findings and wait for explicit user approval before touching code.
  - **Execution**: Continuous implementation once approved (zero mid-task pauses).
  - **Diff Review**: The Girl Gang review panel audits the unified `git diff` via isolated subagent before merge (~45s).

### Review Panel Composition & Subagent Execution Invariants

• **Review Panel Invariant**: For Tiers 2 & 3, review panels (The Girl Gang) consist of exactly **4 reviewers / disciplines**:
  - **3 relevant Domain Specialists** dynamically tailored to the specific change (e.g. Systems/Concurrency, Architecture, Security, Data Integrity, or PWA/Frontend when user-facing UI is touched). PWA/Frontend is NOT mandated for every change, but included only when relevant.
  - **1 mandatory Adversarial Devil's Advocate** focused on failure mode analysis, edge cases, and architectural risk.

• **Consolidated Review Subagent Execution (Token Isolation & Atomic Gathering Invariant)**:
  - In headless Discord bot turns (`agy -p`), review panels (The Girl Gang) MUST execute within a **single consolidated review subagent** (e.g. `role: "TheGirlGangReviewer"`, `TypeName: "research"`).
  - Dispatching multiple independent review subagents is strictly prohibited because asynchronous completion times tempt the root agent into emitting intermediate waiting chatter to Discord, which triggers `agy`'s print-mode drain and kills in-flight subagents.
  - Running reviews inline in the root thread is prohibited for Tier 2 and Tier 3 because multi-expert critiques dump thousands of tokens into `transcript.jsonl`, compounding quadratically across subsequent tool calls.
  - The consolidated review subagent audits across all 4 disciplines in its own isolated context and returns a single, structured synthesis back to the root agent. The root agent MUST NOT emit conversational text to Discord while the review subagent is in flight.

• **Continuous Autonomous Execution & Review Panel Timing**:
  - **Zero Human Check-In Stalls**: For Tiers 0–2 (and approved Tier 3), execution is continuous without stopping to prompt the user for permission between individual tasks.
  - **Consolidated Pre-PR Review**: Code review panel audits are performed on the consolidated pre-PR diff before submission, rather than pausing to run full review panels after every micro-task.

• **Orchestration & Proactive Subagent-Driven Development (SDD) Invariant**:
  - For Tier 2+ features, refactors, and complex multi-task implementations, the root agent acts primarily as an **orchestrator** rather than a monolithic implementer.
  - **Proactive Subagent-Per-Task Execution**: Instead of executing long, monolithic tool loops in the root thread until quota or context limits are exhausted, the orchestrator proactively delegates each discrete implementation task to a dedicated, scoped subagent by default.
  - **Token Isolation & Transcript Health**: Offloading implementation, test cycles, and lint remediation to isolated subagents keeps the root transcript featherweight (<50 steps), preventing quadratic token compounding across tool calls and eliminating the risk of runaway root-thread loops.
  - **Tier 0 & Simple Tier 1**: Simple, single-step Tier 0 or Tier 1 changes (≤ 1–2 tool operations) may execute directly inline. If a Tier 1 fix expands into iterative debugging or multi-file remediation, delegate to an isolated subagent immediately.

• **Subagent Model Allocation Matrix**:
  - **Implementation Subagents (Low-Effort Tier)**: When dispatching subagents to write code or tests per an implementation plan, always specify `Model: "flash"` (or `"flash_lite"`). Implementing code from an approved plan is transcription and verification, not open-ended reasoning. Using high-effort models for routine coding burns quota and increases turn latency.
  - **Review Subagents (High-Effort Tier)**: Consolidated review panels (The Girl Gang) and final pre-PR diff reviewers must use `Model: "pro"` or `Model: "inherit"` to ensure rigorous critique and edge-case discovery.
  - **Fix-Loop Escalation**: Use `Model: "flash"` for initial fix attempts (rounds 1–3); escalate to `Model: "pro"` only if an implementer remains stuck on round 4 or 5.

---

### Universal Workflow Stages

```
Stage 1: Implementation Plan (Scoped per GEMINI.md Section 8 classification tiers)
   │
   ▼
Stage 2: Tiered Plan Review (The Review Gate)
   │     • Follow classification tiers in GEMINI.md Section 8
   │     • Remediate all plan objections immediately
   │
   ▼
Stage 3: Human Review Checkpoint (Tier 3 ONLY per GEMINI.md Section 8)
   │     • Tiers 0–2: Bypassed autonomously (flow directly into Stage 4)
   │     • Tier 3: MANDATORY STOP — wait for explicit human approval before touching code
   │
   ▼
Stage 4: Autonomous Continuous Implementation (TDD)
   │     • Implement tasks continuously in flow per GEMINI.md Section 8
   │
   ▼
Stage 5: Dynamic Scope Reconciliation & Post-Implementation Re-Tiering Gate
   │     • Calculate actual git diff LOC (`git diff --stat`) & touched packages
   │     • If diff expanded past initial tier (e.g. >200 LOC or core queue/runner logic), escalate tier immediately
   │     • Tier 3 Escalation: MANDATORY HUMAN STOP before auto-merge / submission
   │     • Ensure review panel rigor matches final escalated blast radius
   │
   ▼
Stage 6: Pre-Flight Verification & Pre-PR Diff Audit
   │     • Execute local verification runner (./scripts/verify.sh --staged)
   │     • Audit diff per final escalated tier
   │     • ZERO-BYPASS INVARIANT: --no-verify strictly forbidden
   │
   ▼
Stage 7: Commit, Push & Asynchronous PR Deployment
         • Fast-path static pre-commit hook (< 1s)
         • scripts/aerial-pr.sh submit (PR creation; auto-merge restricted if escalated to Tier 3)
         • Report PR link, diff summary, and verification evidence directly
```

---

### Stage 1: Brainstorming & Architectural Specification
1. **Explore Intent & Scope**:
   - Clarify scope, system constraints, persistence schemas, concurrency boundaries, and failure modes before writing code.
2. **Initialize Workspace**:
   - Workspaces are initialized into ephemeral scratch directories via `scripts/aerial-pr.sh init [repo]` (e.g. `scripts/aerial-pr.sh init mirrormere`, `scripts/aerial-pr.sh init aerial-config`, or `scripts/aerial-pr.sh init` for core engine). Never use wrapper scripts or manual clones.
3. **Draft Implementation Plan & Task Sizing (~15-Minute Granularity)**:
   - Scope and author `implementation_plan.md` per the requirements of your classification tier in `GEMINI.md` Section 8 (omitted for Tier 0, lightweight task list for Tier 1, formal specification for Tiers 2/3).
   - **Task Right-Sizing (~15-Minute Target Length)**:
     - Decompose the implementation plan into discrete, self-contained tasks sized to approximately **15 minutes of execution length** (typically: 1 failing test + minimal code to pass + local verification + atomic commit).
     - Avoid monolithic, sprawling tasks as well as microscopic trivialities.
     - **Session Rotation Synergy**: Sizing tasks to ~15 minutes creates natural turn boundaries between tasks. This allows the root orchestrator or process pool (`UnifiedProcessPool`) clean opportunities to rotate sessions (`ShouldRotate` evaluated when `InflightCount() == 0`) and compact context between tasks without risking context rot or token exhaustion.

---

### Stage 2: Tiered Plan Review
Before modifying source code, Aerial MUST audit the plan according to the classification tiers in `GEMINI.md` (Section 8):
- Review overhead, panel composition, and subagent delegation are governed strictly by your classification tiers in `GEMINI.md` Section 8.
- Remediate all valid architectural objections directly in `implementation_plan.md` before proceeding.

---

### Stage 3: Human Review Checkpoint (Tier 3 ONLY)
- **Tiers 0–2 Tasks**: **Bypassed autonomously**. If the user requested an implementation/fix, Aerial directly incorporates review feedback and transitions immediately to Stage 4 without asking for permission.
- **Tier 3 Tasks**: **MANDATORY STOP**. For database schema migrations, service topology changes, breaking API updates, core queue/runner execution overhauls, or high-blast-radius changes, present the synthesized panel findings and **STOP execution to obtain explicit user approval before touching code** per `GEMINI.md` Section 8.

---

### Stage 4: Autonomous Continuous Implementation (TDD)
1. **Subagent-Driven Development (SDD) & Modular Task Execution**:
   - **Orchestrator Workflow**: For Tier 2+ features, refactors, and multi-task implementations, the root agent drives implementation sequentially through the planned ~15-minute tasks:
     - For each task, dispatch a dedicated, scoped implementation subagent equipped with the task specification, acceptance criteria, and hermetic testing instructions.
     - The subagent follows strict TDD: writes the failing test first, runs it to confirm RED, writes minimal code to pass, verifies GREEN, checks test coverage and lint, and creates an atomic commit.
     - The subagent reports back a concise execution summary and verification evidence.
     - The root orchestrator inspects the subagent's report, confirms task completion, and advances to the next task.
   - **Turn-Boundary Session Rotation**: Because tasks are right-sized to ~15 minutes, each subagent completes within a small, isolated context (<15–20 steps). Between tasks, when no subagents or turns are in flight, the root agent or process pool can safely rotate sessions or compact memory without interrupting active operations.
   - **TDD Rigor**: All new functionality and bug fixes must follow Test-Driven Development (tests first, then implementation), verified with race detection (`-race`).
2. **Coding & Architectural Review Standards**:
   - **No Change Detectors & Hand-Derived Literals**: Tests must verify observable behavior, never internal constants, private struct fields, or exact log wording. Expected values must be hand-derived literals or static fixtures—never computed using the same builder or helper logic under test.
   - **Defense-in-Depth Validation**: When designing or validating input handling, validate across all layers (API entry point, domain business logic, environment guards) so invalid states become structurally impossible to reach.
   - **Test Double & Production Purity**: Never assert on a mock's internal calls; assert real component outcomes. Mock only external boundaries (slow I/O, network), never domain business logic. Production structs carry production methods only—keep test-only reset or lifecycle hooks in test utilities.
   - **Scope Discipline & File Health**: Speculative abstractions and unrequested "nice-to-haves" are flagged as architectural defects ("Extra") on equal footing with missed requirements ("Missing"). Block changes that significantly bloat already large files; decompose cohesive units behind narrow interface boundaries.
3. **Functional Core, Imperative Shell (Decoupling Logic from Concurrency & I/O)**:
   - When designing new features or bugfixes, business logic, prompt formatting, state transitions, session key calculation, and decision rules MUST be factored into pure, deterministic functions (e.g. `AssembleTurnPrompt`, `PlanBurstExecution`, `DetermineSessionAction`, `CalculateBackoffDelay`) that accept values and return values.
   - **Table-Driven Unit Tests as Primary Vehicle**: Unit tests for business logic permutations and edge cases MUST target these pure functions directly using fast, table-driven tests (< 1ms) with zero chance of channel deadlocks, race conditions, or timing flakiness.
   - **Reserve Mock Runner / Subprocess Orchestration for Plumbing Only**: Tests that instantiate mock subprocesses (`createMockAgyScript`), mock runners (`RunnerFunc`), goroutine worker pools, channel select loops, and context timeouts MUST be strictly limited to verifying concurrency plumbing (e.g., watchdog timeouts, process group kills, startup catch-up sweeps). Never test business logic variants via end-to-end mock runner pipelines.
   - **Zero Arbitrary Sleeps Invariant (`time.Sleep` Elimination)**: Arbitrary sleeps (`time.Sleep`) are strictly prohibited in tests and production plumbing. Asynchronous coordination must use event-driven signaling, condition variables, channel selects, or injected delay overrides (`RetryDelayOverride`) to guarantee fast, deterministic execution without timing flakes.
   - **Hermetic In-Memory Test Fixtures**: All storage and database contract tests MUST use airgapped, in-memory database handles or `t.TempDir()` isolated files strictly for fast, hermetic unit testing. Unit tests MUST NEVER write to shared host database paths, `/data`, or `/share`.
   - **Interface Abstraction at External Boundaries**: Abstract external system boundaries (git commands, Docker sockets, Discord REST, database drivers) behind interfaces (e.g. `GitExecutor`) so packages can be tested hermetically with in-memory mocks without disk or subprocess overhead.
4. **Orchestration & Workflow Standards**:
   - Strictly adhere to the Tiered Engineering Workflow, Orchestration Invariants, and Subagent Model Allocation Matrix detailed above (mandatory `Model: "flash"` or `Model: "flash_lite"` for all implementer subagent dispatches).
5. **Declarative Database Schema Migrations Runbook (Atlas)**:
   - **Step 1: Declarative Schema Definition (`schema.sql`)**:
     - Modify the repository's canonical schema file (`brain/pkg/db/schema.sql` for `aerial`, `db/schema.sql` for `aerial-sidecars`).
     - Never hand-write migration files or imperative DDL (`ALTER TABLE`, `DROP TABLE`) in Go/Python/TypeScript application code. All DDL is mechanically blocked by `scripts/verify.sh`.
   - **Step 2: Generate Migration Diff (`scripts/atlas-diff.sh <migration_name>`)**:
     - Run `./scripts/atlas-diff.sh <migration_name>` (e.g., `./scripts/atlas-diff.sh add_effort_column`).
     - The script automatically provisions an ephemeral PostgreSQL + pgvector container, calculates the declarative schema diff, writes the versioned migration SQL file, updates `atlas.sum`, and deterministically cleans up the container.
   - **Step 3: Verify Integrity & Code Hygiene (`./scripts/verify.sh --staged`)**:
     - Staged pre-flight checks automatically run `check_no_imperative_ddl` to ensure no raw DDL leaked into application code, and validate migration directory checksums via `atlas migrate validate`.
   - **Step 4: Hermetic Tests & Nomad Batch Migration Execution**:
     - Run target database tests (`go test -v ./brain/pkg/db/...`). Ensure application code does not embed migration runners on startup, and that schema provisioning cluster-wide is handled via the colocated Nomad batch job (`nomad/jobs/migrate.nomad`).

---

### Stage 5: Dynamic Scope Reconciliation & Post-Implementation Re-Tiering Gate
*MANDATORY GATE: Never open a PR based solely on upfront tier estimations. Always reconcile against actual diff metrics.*

1. **Calculate Actual Staged Metrics**:
   - Immediately upon concluding implementation (before diff audit and PR creation), run `git diff --stat` (or `git diff --cached --stat` / `git diff HEAD~1..HEAD --stat`) to inspect the true cumulative line count and touched packages.
2. **Evaluate Scope Expansion & Trigger Re-Tiering**:
   - Compare the actual diff statistics against the initial classification tier:
     - **Tier 0 Escalation**: If a task started as Tier 0 (expected ≤5 LOC) but expanded past 5 LOC, it auto-escalates to Tier 1 (or Tier 2 if >50 LOC).
     - **Tier 1 Escalation**: If a task started as Tier 1 (expected <50 LOC) but expanded past 50 LOC, it auto-escalates to Tier 2 (or Tier 3 if >200 LOC or touching core queue/runner logic).
     - **Tier 2 to Tier 3 Escalation**: If a task started as Tier 2 (expected 50–200 LOC) but:
       - Cumulative diff exceeds **200 LOC** (additions + deletions), OR
       - Touched files include **core queue logic** (`pkg/queue`, `WorkerPool`, worker loops), **core runner logic** (`pkg/runner`, streaming daemons, turn sinks, turn resolvers), or schema migrations,
       - **IT IMMEDIATELY AUTO-ESCALATES TO TIER 3**.
3. **Enforce Escalated Review Rigor**:
   - **Retroactive Plan & Architecture Audit**: If the initial plan review was bypassed (e.g. started as Tier 1) or used a narrower scope, dispatch The Girl Gang to audit the implementation architecture.
   - **Diff Review Panel Escalation**: If escalated to Tier 3, the diff review MUST be performed by the full **Girl Gang** panel (4 disciplines), not just a single Devil's Advocate.
   - **MANDATORY HUMAN ESCALATION CHECKPOINT (STOP BEFORE AUTO-MERGE)**:
     - **If a task escalates to Tier 3 during or after implementation, the agent MUST NOT open the PR with auto-merge armed without explicit human authorization!**
     - The agent MUST pause and report the scope escalation to Discord: synthesize the panel's review findings, summarize the unexpected diff growth and why it expanded, and provide the PR / diff for explicit user review. Auto-merge must NOT be armed without user authorization when scope has escalated to Tier 3.

---

### Stage 6: Pre-Flight Verification & Pre-PR Diff Audit
*MANDATORY: Never commit or push unverified code.*

1. **Local Pre-Flight Verification Runner (`--staged`)**:
   Always execute targeted local verification before committing:
   - **Linux / Container**:
     ```bash
     ./scripts/verify.sh --staged
     ```
   - **Windows Host**:
     ```powershell
     powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged
     ```
   - Run targeted package tests (`go test -v ./pkg/...`).

2. **Pre-PR Diff Audit**:
   - Audit the unified `git diff` against the approved plan strictly according to your final escalated classification tier. Resolve any identified regressions before commit.

3. **ZERO-BYPASS INVARIANT**:
   - Fresh verification evidence must exist in the turn transcript prior to commit.
   - If a check fails, fix the code/tests, re-run `./scripts/verify.sh --staged` until exit code is 0, and proceed.

4. **Single-Pass Coverage Audit & Deficit Resolution Runbook**:
   - **The Anti-Pattern (Micro-Pushes)**: Never push exploratory single-statement tests to remote CI. Pushing incremental fixes burns 3 minutes per CI cycle and compounds turn latency. Always resolve coverage deficits locally in a single unified pass.
   - **Unified Linux Coverage Execution Across Platforms**:
     - Both Linux environments (Aerial's in-container runtime, GitHub Actions CI) and Windows development hosts (via Docker Desktop in `scripts/test-linux.ps1`) execute the identical `golang:1.24` Linux test suite with identical statement denominators.
     - The universal hard floor across all Go packages is **`>= 95.0%`**.
     - **On Linux / Container**:
       ```bash
       ./scripts/check-coverage.sh --service brain --check --gaps
       ```
     - **On Windows Workstation**:
       ```powershell
       powershell -ExecutionPolicy Bypass -File scripts/test-linux.ps1 -Service brain -Check -Gaps
       ```
     - Both commands execute the Linux test engine and output the exact statement deficit and top 5 uncovered functions directly in the terminal.
   - **The 4-Step Single-Pass Resolution Process**:
     - **Step 1: Calculate the Exact Deficit**: Compute statements needed: required delta equals the ceiling of total statements multiplied by 0.95 minus covered statements.
     - **Step 2: Audit High-Yield Uncovered Blocks**: Identify the top 2-3 functions with the largest clusters of uncovered statements (e.g. error handling branches, handshake negotiation, retry loops).
     - **Step 3: Batch-Implement Tests in a Single Pass**: Write table-driven test cases covering those branches to satisfy at least 2x the deficit.
     - **Step 4: Re-Verify Locally**: Run local verification to confirm all packages meet or exceed 95.0% before staging changes.

---

### Stage 7: Commit, Push & Continuous Deployment
Follow the automated scratch PR workflows detailed in **Section 5**. If the task escalated to Tier 3, auto-merge must NOT be armed without explicit user authorization.

---

## 5. Asynchronous Scratch PR Automation (Universal Workflow Across All Repositories)

All code and configuration changes across all repositories (`aerial`, `aerial-config`, `mirrormere`, `aerial-sidecars`, etc.) must follow the unified scratch PR workflow via `scripts/aerial-pr.sh`:

### 5.1 Universal Multi-Repository PR Workflow (`scripts/aerial-pr.sh`)
1. **Initialize Scratch Workspace**:
   ```bash
   # Syntax: /share/aerial/scripts/aerial-pr.sh init [repo]
   /share/aerial/scripts/aerial-pr.sh init mirrormere       # Peripheral / display kiosk
   /share/aerial/scripts/aerial-pr.sh init aerial-config   # User configuration & rules
   /share/aerial/scripts/aerial-pr.sh init aerial          # Core monorepo (or simply `init`)
   ```
2. **Implement & Author PR Description**:
   - Write tests and code following TDD.
   - Run local pre-flight checks (`./scripts/verify.sh --staged` if present in repo).
   - Author mandatory `PR_DESCRIPTION.md` in the scratch root.
3. **Submit Asynchronously**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh submit <scratch_dir> "feat(module): description"
   ```
   - Auto-detects target repository and owner from the scratch git remote.
   - Pushes branch, arms native GitHub auto-merge (`SQUASH`), and schedules a one-shot follow-up check via `scheduler-mcp`.
   - **TURN TERMINATION INVARIANT**: The submit command instructs the agent to end the turn immediately (`"turn_action": "end_turn"`). Synchronous foreground polling (`sleep` loops, repeated check inspection) and bypassing `aerial-pr.sh` via raw GitHub MCP tools (`create_pull_request`, `merge_pull_request`) are strictly prohibited.
4. **Scheduled Wake-Up: PR Verification & Proactive Failure Remediation**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh --repo <repo> check <pr_num>
   ```
   - **Nominal State (Auto-Merged)**: When CI passes, GitHub automatically merges the PR without manual intervention. Aerial verifies deployment and confirms status in plain prose strictly capped at 2 sentences max.
   - **Pending State**: If checks are still running, inform the user and reschedule a follow-up check via `scheduler-mcp` (`schedule_once`).
   - **Failure State (PROACTIVE FAILURE REMEDIATION)**: If CI fails, Aerial **must proactively fix the failure**:
     1. Inspect failing GitHub check runs and diagnostic logs.
     2. Checkout the PR branch in an ephemeral scratch workspace:
        ```bash
        /share/aerial/scripts/aerial-pr.sh init <repo>
        git checkout <branch>
        ```
     3. Diagnose and resolve the issue (unit test failures, lint errors, coverage deficits, compilation issues).
     4. Verify locally (`./scripts/verify.sh --staged` or local package tests).
     5. Commit and push directly to the PR branch (`git push origin <branch>`).
     6. Quietly reschedule a follow-up check via `scheduler-mcp` (`schedule_once`).
     7. Auto-merge remains armed and will merge once CI turns green.
     8. Never report a failure to the user without attempting remediation, unless an unrecoverable architectural conflict exists.

### 5.2 PR Submission, Description & Status Reporting Standards
- **Mandatory PR Descriptions**: Pull Request descriptions are strictly mandatory under all circumstances. Submissions without a description will fail fast with exit code 1. Authors must provide a description via `PR_DESCRIPTION.md` in the scratch root (the recommended path for agents; automatically consumed and deleted before staging) or `--body-file <path>`.
- **Title Hygiene**: PR titles are automatically sanitized (first non-empty line, stripped of `\r\n`, clamped to 256 characters) to prevent literal `\n` pollution in GitHub titles.
- **Inverted Pyramid Format**: PR descriptions should follow the Inverted Pyramid format (concise summary first, key changes as bulleted key-value lists, verification evidence last) with zero markdown tables.
- **Two-Sentence Plain Prose Confirmation**: On PR merge and deployment confirmation, responses must be delivered in plain prose strictly capped at two sentences max, omitting markdown bullet lists and forward-looking checklists for nominal and ongoing states. The sentence limit is lifted only for unrecoverable failures to provide full diagnostic logs and failure context.

---

## 6. Operational Invariants

All engineering operations must strictly adhere to the canonical invariants defined in `GEMINI.md` ("Core Invariants & Operational Rules"):
• **Scheduling Invariant (Invariant 4)**: Persistent reminders and PR follow-ups exclusively via `scheduler-mcp`. The built-in ephemeral CLI `schedule` tool is strictly prohibited (causes print-mode drain and premature termination).
• **Asynchronous PR Workflow (Invariant 6)**: Exclusively asynchronous submission via `scripts/aerial-pr.sh` across ALL repositories with automated follow-up scheduling and armed native auto-merge. Mandatory `PR_DESCRIPTION.md`. Zero-bypass pre-flight verification (`./scripts/verify.sh --staged`). Zero raw MCP PR tools; zero foreground CI polling loops (`sleep` loops). On scheduled wake-up, proactively remediate any failing builds. Response confirmations strictly capped at 2 sentences max in plain prose.
• **Hermetic Testing & Environment Boundaries (Invariants 7 & 15)**: Host-native test execution; zero arbitrary `time.Sleep`; zero in-container `docker compose` mutations.

