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
Use your classification tiers defined in `GEMINI.md` Section 8 (**Tier 0 through Tier 3**) to evaluate task scope and govern execution:
• **Single Source of Truth**: All tier thresholds, review panel compositions, human approval gates (Tier 3 stop), and orchestration invariants are canonically defined in `GEMINI.md` Section 8.
• **Negative Scope Blacklist**: Strictly forbidden from Tier 0: SQL/database schemas, security/auth primitives, concurrency/mutex logic, Docker topology, or core runner loops (auto-escalates to Tier 1+ per `GEMINI.md`).

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
2. **Orchestration & Workflow Standards**:
   - Strictly adhere to the Tiered Engineering Workflow and Orchestration Invariants in `GEMINI.md` (Section 8).

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

---

## 6. Operational Invariants

All engineering operations must strictly adhere to the canonical invariants defined in `GEMINI.md` ("Core Invariants & Operational Rules"):
• **Scheduling Invariant (Invariant 4)**: Persistent reminders and PR follow-ups exclusively via `scheduler-mcp`. The built-in ephemeral CLI `schedule` tool is strictly prohibited (causes print-mode drain and premature termination).
• **Asynchronous PR Workflow (Invariant 6)**: Exclusively asynchronous submission with automated follow-up scheduling. Mandatory `PR_DESCRIPTION.md`. Zero-bypass pre-flight verification (`./scripts/verify.sh --staged`). Response confirmations strictly capped at 2 sentences max in plain prose.
• **Hermetic Testing & Environment Boundaries (Invariants 7 & 15)**: Host-native test execution; zero arbitrary `time.Sleep`; zero in-container `docker compose` mutations.

