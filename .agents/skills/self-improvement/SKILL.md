---
name: self-improvement
description: Mandatory skill whenever Aerial must inspect, modify, enhance, debug, or refactor its own Go codebase, system skills, or configuration repos (aerial / aerial-config), author scratch PRs, OR pull upstream git releases and trigger Hangar container deployment syncs. DO NOT use for managing Home Assistant devices, user tasks, calendar events, recipes, or external domain operations.
---

# Aerial Self-Improvement & Continuous Engineering Workflow

This skill defines how Aerial safely manages its own lifecycle across two operational paths:
1. **Path A (Operational Maintenance & Rollout)**: Fast-path upstream syncs, container reconciliations, and deployment status checks.
2. **Path B (Autonomous Self-Engineering)**: Multi-agent tiered review, TDD implementation, and scratch PR automation across the dual-repository architecture.

---

## 1. Intent Triage Matrix (Decision Gate)

Before running any commands or dispatching review subagents, determine which operational path applies:

• **Path A: Operational Maintenance & Container Rollout (Zero Code Changes)**
  - **Triggers**: *"Update Aerial"*, *"Update yourself"*, *"Pull latest code / git sync"*, *"Reconcile containers"*, *"Check deployment status"*.
  - **Target Action**: Pull upstream images/git and reconcile runtime containers via Hangar daemon.
  - **Subagent Review Overhead**: **None (Bypassed)**. Execute Hangar endpoints directly in root turn.
  - **Execution Reference**: Proceed directly to **Section 2**.

• **Path B: Autonomous Self-Engineering (Code & Configuration Changes)**
  - **Triggers**: *"Fix bug in X"*, *"Add feature / MCP service"*, *"Modify skill or prompt"*, *"Edit config.yaml / AGENTS.md"*.
  - **Target Action**: Implement, test, verify, and submit code or configuration changes via scratch PR workflows.
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

## 3. Path B: Dual-Repository Architecture & Ephemeral Workspaces

Code and configuration changes are governed by the two-repository separation and kernel `:ro` immutability defined in `GEMINI.md` ("Decoupled Configuration & Repository Separation"):
- **Core Engine (`azylman/aerial`)**: Generic execution engine, base skills, and Docker topology. Zero personal data; zero plaintext tokens.
- **User Configuration (`azylman/aerial-config`)**: Private options, persona overrides, and domain skills.
- **Ephemeral Scratch Workspaces**: Because `/share/aerial` and `/share/aerial-config` are mounted read-only (`:ro`), all changes must be authored in isolated scratch clones initialized via `scripts/aerial-pr.sh init` or `scripts/aerial-config-pr.sh init`.

---

## 4. The Tiered Engineering & Review Workflow

Whenever undertaking feature development, architectural changes, bug fixes, or system modifications, Aerial follows a **Tiered Engineering Workflow** scaled dynamically by the complexity and blast radius of the change.

### Complexity Tiers & Review Matrix

Code review and execution are structured dynamically across four complexity tiers:

• **Tier 0 (≤ 5 LOC, single-line changes, config tweaks, doc typos)**:
  - **Review Overhead**: Zero review. Both plan review and diff review subagents are completely bypassed. Direct implementation in the root thread with automated pre-flight syntax/YAML verification.
  - **Negative Scope Blacklist**: Strictly forbidden from Tier 0: SQL/database schemas, security/auth primitives, concurrency/mutex logic, Docker topology, or core runner loops (any touch auto-escalates to Tier 1+).

• **Tier 1 (< 50 LOC, targeted bugfixes & test additions)**:
  - **Review Overhead**: Inline root-thread execution. Plan and diff review subagents are completely bypassed to eliminate delegation overhead and premature yield chatter.
  - **Execution**: Autonomous continuous execution (no mandatory stop) directly in the root thread, verified via targeted package unit tests and local pre-flight verification (`./scripts/verify.sh --staged`).

• **Tier 2 (50–200 LOC, standard features & multi-package refactors)**:
  - **Plan Review**: The Girl Gang review panel audits the plan in an isolated subagent across 4 disciplines (~45s).
  - **Execution**: Autonomous execution (no mandatory stop; directly incorporates plan feedback). Zero mid-task pauses during coding.
  - **Diff Review**: Consolidated Devil's Advocate audits the unified `git diff` via isolated review subagent before opening the PR (~30s).

• **Tier 3 (> 200 LOC, core architecture, database migrations, breaking changes)**:
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

• **Orchestration & Subagent Delegation Invariant**:
  - For Tier 2+ multi-file edits, test triage, and lint remediation loops exceeding tool budget thresholds, the primary agent acts as an orchestrator, delegating heavy tool loops to scoped, isolated subagents (for implementation/execution only).
  - Tier 0 and Tier 1 changes execute directly inline in the root thread. Root transcript must remain featherweight (<50 steps) to prevent quadratic token compounding.

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
Stage 5: Pre-Flight Verification & Pre-PR Diff Audit
   │     • Execute local verification runner (./scripts/verify.sh --staged)
   │     • Audit diff per GEMINI.md Section 8 classification tiers
   │     • ZERO-BYPASS INVARIANT: --no-verify strictly forbidden
   │
   ▼
Stage 6: Commit, Push & Asynchronous PR Deployment
         • Fast-path static pre-commit hook (< 1s)
         • scripts/aerial-pr.sh submit (instant PR creation, scheduled follow-up via scheduler-mcp)
         • Report PR link, diff summary, and verification evidence directly
```

---

### Stage 1: Brainstorming & Architectural Specification
1. **Explore Intent & Scope**:
   - Clarify scope, system constraints, persistence schemas, concurrency boundaries, and failure modes before writing code.
2. **Initialize Workspace**:
   - Workspaces are initialized into ephemeral scratch directories via `scripts/aerial-pr.sh init` or `scripts/aerial-config-pr.sh init`.
3. **Draft Implementation Plan**:
   - Scope and author `implementation_plan.md` per the requirements of your classification tier in `GEMINI.md` Section 8 (omitted for Tier 0, lightweight task list for Tier 1, formal specification for Tiers 2/3).

---

### Stage 2: Tiered Plan Review
Before modifying source code, Aerial MUST audit the plan according to the classification tiers in `GEMINI.md` (Section 8):
- Review overhead, panel composition, and subagent delegation are governed strictly by your classification tiers in `GEMINI.md` Section 8.
- Remediate all valid architectural objections directly in `implementation_plan.md` before proceeding.

---

### Stage 3: Human Review Checkpoint (Tier 3 ONLY)
- **Tiers 0–2 Tasks**: **Bypassed autonomously**. If the user requested an implementation/fix, Aerial directly incorporates review feedback and transitions immediately to Stage 4 without asking for permission.
- **Tier 3 Tasks**: **MANDATORY STOP**. For database schema migrations, service topology changes, breaking API updates, or high-blast-radius changes, present the synthesized panel findings and **STOP execution to obtain explicit user approval before touching code** per `GEMINI.md` Section 8.

---

### Stage 4: Autonomous Continuous Implementation (TDD)
1. **Modular Task Execution (TDD)**:
   - Break implementation into discrete, sequential components/tasks.
   - Implement following Test-Driven Development (write tests first, then implementation).
   - Verify task unit tests pass with race detection (`-race`).
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
   - Strictly adhere to the Tiered Engineering Workflow and Orchestration Invariants detailed above.

---

### Stage 5: Pre-Flight Verification & Pre-PR Diff Audit
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
   - Audit the unified `git diff` against the approved plan strictly according to your classification tiers in `GEMINI.md` Section 8. Resolve any identified regressions before commit.

3. **ZERO-BYPASS INVARIANT**:
   - Fresh verification evidence must exist in the turn transcript prior to commit.
   - If a check fails, fix the code/tests, re-run `./scripts/verify.sh --staged` until exit code is 0, and proceed.

---

### Stage 6: Commit, Push & Continuous Deployment
Follow the automated scratch PR workflows detailed in **Section 5**.

---

## 5. Asynchronous Scratch PR Automation

All code and configuration changes must follow the automated scratch PR workflow per `GEMINI.md` Invariant 6:

### 5.1 Core Engine PR Workflow (`scripts/aerial-pr.sh`)
1. **Initialize Scratch Workspace**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh init
   ```
2. **Implement & Author PR Description**:
   - Write tests and code following TDD.
   - Author mandatory `PR_DESCRIPTION.md` in the scratch root.
3. **Submit Asynchronously**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh submit <scratch_dir> "feat(module): description"
   ```
4. **Verify & Merge on Scheduled Wake-up**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh merge <pr_num>
   ```

### 5.2 Configuration PR Workflow (`scripts/aerial-config-pr.sh`)
1. **Initialize Scratch Workspace**:
   ```bash
   /share/aerial/scripts/aerial-config-pr.sh init
   ```
2. **Implement & Author PR Description**:
   - Update YAML, prompts, or custom skills.
   - Author mandatory `PR_DESCRIPTION.md` in the scratch root.
3. **Submit Asynchronously**:
   ```bash
   /share/aerial/scripts/aerial-config-pr.sh submit <scratch_dir> "chore(config): description"
   ```
4. **Verify & Merge on Scheduled Wake-up**:
   ```bash
   /share/aerial/scripts/aerial-config-pr.sh merge <pr_num>
   ```

### 5.3 PR Submission, Description & Status Reporting Standards
- **Mandatory PR Descriptions**: Pull Request descriptions are strictly mandatory under all circumstances. Submissions without a description will fail fast with exit code 1. Authors must provide a description via `PR_DESCRIPTION.md` in the scratch root (the recommended path for agents; automatically consumed and deleted before staging) or `--body-file <path>`.
- **Title Hygiene**: PR titles are automatically sanitized (first non-empty line, stripped of `\r\n`, clamped to 256 characters) to prevent literal `\n` pollution in GitHub titles.
- **Inverted Pyramid Format**: PR descriptions should follow the Inverted Pyramid format (concise summary first, key changes as bulleted key-value lists, verification evidence last) with zero markdown tables.
- **Two-Sentence Plain Prose Confirmation**: On PR merge and deployment confirmation, responses must be delivered in plain prose strictly capped at two sentences max, omitting markdown bullet lists and forward-looking checklists for nominal and ongoing states. The sentence limit is lifted only for failures to provide full diagnostic logs and failure context.

---

## 6. Operational Invariants

All engineering operations must strictly adhere to the canonical invariants defined in `GEMINI.md` ("Core Invariants & Operational Rules"):
• **Scheduling Invariant (Invariant 4)**: Persistent reminders and PR follow-ups exclusively via `scheduler-mcp`. The built-in ephemeral CLI `schedule` tool is strictly prohibited (causes print-mode drain and premature termination).
• **Asynchronous PR Workflow (Invariant 6)**: Exclusively asynchronous submission with automated follow-up scheduling. Mandatory `PR_DESCRIPTION.md`. Zero-bypass pre-flight verification (`./scripts/verify.sh --staged`). Response confirmations strictly capped at 2 sentences max in plain prose.
• **Hermetic Testing & Environment Boundaries (Invariants 7 & 15)**: Host-native test execution; zero arbitrary `time.Sleep`; zero in-container `docker compose` mutations.

