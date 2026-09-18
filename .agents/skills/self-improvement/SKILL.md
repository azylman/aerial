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

## 3. Path B: Two-Repository Separation of Concerns & Scratch PR Gate

When undertaking code or configuration changes, Aerial cleanly separates generic system code from private user configuration:

### 3.1 Core Engine Repository (`azylman/aerial` at `/share/aerial`)
- **Scope**: Generic Go execution engine (`brain/`), built-in MCP microservices (`scheduler-mcp`, `discord-mcp`, `docker-mcp`, `github-mcp`), base system skills, Docker topology, and core architecture docs.
- **Strict Invariants**:
  - **100% Generic & Domain-Agnostic**: All prompts, code, error handlers, and schemas must remain completely generic and reusable for any user.
  - **Zero Personal Data Invariant**: **NEVER** commit real names, Discord handles, usernames, family members, home addresses/locations, private device/entity IDs, or user-specific business logic into this repository.
  - **Zero Plaintext Secrets Invariant**: NEVER commit API keys, tokens, private webhook URLs, or GitHub PATs to disk.
- **Deployment Flow**: GitHub Actions builds and publishes images to GHCR; Hangar on the host reconciles updated containers out-of-band.

### 3.2 User Configuration Repository (`azylman/aerial-config` at `/share/aerial-config`)
- **Scope**: User options (`config.yaml`), persona overrides & user identity/aliases (`AGENTS.md`), private smart home/domain workflows (`custom-skills/`), sidecar containers (`docker-compose.override.yml`), and host environment secrets (`.env`).
- **Strict Invariants**: User identity, personal aliases, private credentials. Filesystem is mounted read-only (`:ro`) inside the container.
- **Deployment Flow**: In-process `fsnotify` watcher hot-reloads configuration and skills dynamically within milliseconds; Hangar pulls git updates.

### 3.3 Read-Only Mount Invariant
**NEVER** run `git pull`, `git checkout`, `git commit`, or edit files in `/share/aerial` or `/share/aerial-config` directly. All changes must be authored in an ephemeral scratch workspace via `scripts/aerial-pr.sh init` or `scripts/aerial-config-pr.sh init`.

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

All code and configuration changes must follow the automated PR workflow:

### 5.1 Core Engine PR Workflow (`scripts/aerial-pr.sh`)
1. **Initialize Scratch Workspace**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh init
   ```
   Clones `azylman/aerial:main` into an isolated scratch directory (`/data/scratch/aerial-code-scratch.XXXXXX`) on an ephemeral branch.
2. **Implement & Author PR Description**:
   - Write tests and code following TDD.
   - Author `PR_DESCRIPTION.md` in the scratch root (mandatory; submissions fail without a description).
3. **Submit Asynchronously**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh submit <scratch_dir> "feat(module): description"
   ```
   - Automatically runs fast pre-flight verification (`./scripts/verify.sh --staged`).
   - Pushes branch, creates PR with auto-merge, and schedules follow-up check via `scheduler-mcp`.
   - Cleans up scratch directory immediately. **Do not poll CI in foreground; end active execution turn.**
4. **Verify & Merge on Scheduled Wake-up**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh merge <pr_num>
   ```
   - If CI is still pending (exit code 2), quietly reschedule follow-up check.
   - Upon green merge, confirms merge and tracks container swap to completion in max 2 sentences.

### 5.2 Configuration PR Workflow (`scripts/aerial-config-pr.sh`)
1. **Initialize Scratch Workspace**:
   ```bash
   /share/aerial/scripts/aerial-config-pr.sh init
   ```
2. **Implement & Author PR Description**:
   - Update YAML, prompts, or custom skills.
   - Author `PR_DESCRIPTION.md` in the scratch root.
3. **Submit Asynchronously**:
   ```bash
   /share/aerial/scripts/aerial-config-pr.sh submit <scratch_dir> "chore(config): description"
   ```
   - Automatically validates YAML syntax and schema.
   - Pushes branch, creates PR, and schedules follow-up check via `scheduler-mcp`.
4. **Verify & Merge on Scheduled Wake-up**:
   ```bash
   /share/aerial/scripts/aerial-config-pr.sh merge <pr_num>
   ```
   - Hangar syncs repository and in-process watcher hot-reloads changes without container downtime.

---

## 6. Operational Invariants

• **Container Execution Invariant**: NEVER execute `docker compose up`, `docker compose build`, or `docker restart` from inside the container. All image builds occur in GitHub Actions CI, and container swaps are handled by Hangar.
• **Scheduling Invariant**: Never schedule manual follow-up timers after calling `submit`; `aerial-pr.sh submit` automatically registers the follow-up reminder via `scheduler-mcp`.
• **Response Format Invariant**: Merge and deployment confirmations must be reported in plain prose strictly capped at 2 sentences max (no markdown checklists, tables, or forward-looking promises).
• **ZERO-BYPASS INVARIANT**: Verification checks (`./scripts/verify.sh --staged`) are strictly blocking; `--no-verify` is forbidden.
