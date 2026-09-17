---
name: self-improvement
description: Use this skill whenever Aerial needs to modify, enhance, debug, or refactor its own codebase, skills, or system configuration, commit changes, pull updates, or deploy updates via CI/CD.
---

# Aerial Self-Improvement & Continuous Engineering Workflow

This skill defines the mandatory, rigorous development workflow Aerial must follow whenever designing, implementing, refactoring, or modifying its own codebase, skills, configurations, or Docker container stack.

---

## 1. Two-Repository Separation of Concerns & Scope Gate

Before making any changes, Aerial MUST determine the target repository:

### 1.1 Core Engine Repository (`azylman/aerial` at `/share/aerial`)
- **Scope**: Generic Go execution engine (`brain/`), built-in MCP microservices (`scheduler-mcp`, `discord-mcp`, `docker-mcp`, `github-mcp`), base system skills, Docker topology, and core architecture docs.
- **Strict Invariants**:
  - **100% Generic & Domain-Agnostic**: All prompts, code, error handlers, and schemas must remain completely generic and reusable for any user.
  - **Zero Personal Data Invariant**: **NEVER** commit real names, Discord handles, usernames, family members, home addresses/locations, private device/entity IDs, or user-specific business logic into this repository.
  - **Zero Plaintext Secrets Invariant**: NEVER commit API keys, tokens, private webhook URLs, or GitHub PATs to disk.
- **Deploy Path**: Commit and push to `azylman/aerial:main`. Hangar synchronizes code and reconciles container updates out-of-band.

### 1.2 User Configuration Repository (e.g. `azylman/aerial-config` at `/share/aerial-config`)
- **Scope**: User options (`config.yaml`), persona overrides & user identity/aliases (`AGENTS.md`), private smart home/domain workflows (`custom-skills/`), sidecar containers (`docker-compose.override.yml`), and host environment secrets (`.env`).
- **Deploy Path**: Commit and push to the user's private configuration repository. Hot-reloaded automatically in-process.

---

## 2. The Tiered Engineering & Review Workflow

Whenever undertaking feature development, architectural changes, bug fixes, or system modifications, Aerial follows a **Tiered Engineering Workflow** scaled dynamically by the complexity and blast radius of the change.

### The Four Complexity Tiers

- **Tier 0: Micro-Changes & Config Tweaks (≤ 5 LOC, Single-Line Changes)**:
  - **Scope**: Single-line changes, configuration updates (`config.yaml`, environment overrides), prompt string adjustments, documentation/typo fixes.
  - **Negative Scope & Blacklist (Auto-Escalate to Tier 1+)**: Strictly forbidden for SQL/database schemas, security/auth primitives, concurrency/mutex logic, Docker topology, or core runner loops. Touching any blacklisted domain immediately escalates to Tier 1+.
  - **Plan Review**: **None (Bypassed)**. Zero subagent review overhead.
  - **Human Gate**: Autonomous execution (no mandatory stop).
  - **Coding**: Direct continuous implementation.
  - **Pre-PR Gate**: Automated syntax/schema checks (`./scripts/verify.sh --staged` or YAML validation). Zero subagent diff review. Hard blocking invariant (zero `--no-verify`). Mandatory PR description still enforced. If diff > 5 LOC or verification fails, auto-escalate to Tier 1.

- **Tier 1: Targeted Fixes, Bugfixes & Tests (5–50 LOC, Single Package)**:
  - **Scope**: Bug fixes, parameter/config tweaks, isolated test additions, pure function adjustments.
  - **Plan Review**: Solo Adversarial Systems Critic / Devil's Advocate (~30s).
  - **Human Gate**: Autonomous execution (no mandatory stop).
  - **Coding**: Continuous TDD implementation (zero mid-task subagent pauses).
  - **Pre-PR Gate**: Fast local pre-flight verification (`./scripts/verify.sh --staged` + package tests).

- **Tier 2: Standard Features & Multi-Package Enhancements (50–200 LOC, 1–3 Packages)**:
  - **Scope**: New features, multi-package refactors, MCP API additions, runner/queue plumbing enhancements.
  - **Plan Review**: Full 4-Expert Review Panel (3 Domain Specialists + 1 Devil's Advocate, ~45s).
  - **Human Gate**: Autonomous execution (no mandatory stop; directly incorporates plan feedback).
  - **Coding**: Continuous TDD implementation (zero mid-task subagent pauses).
  - **Pre-PR Gate**: Solo Devil's Advocate audits the complete consolidated `git diff` (~30s).

- **Tier 3: Core Architecture, Database Schemas & Breaking Changes (> 200 LOC, Cross-Service)**:
  - **Scope**: SQLite/PostgreSQL schema migrations, Docker service topology changes, breaking API/protocol updates, core security boundaries.
  - **Plan Review**: Full 4-Expert Review Panel (3 Domain Specialists + 1 Devil's Advocate, ~45s).
  - **Human Gate**: **MANDATORY HUMAN REVIEW CHECKPOINT (STOP)**. Synthesize panel findings, trade-offs, and consensus, and pause execution until Alex explicitly approves.
  - **Coding**: Continuous modular implementation once approved (zero mid-task subagent pauses).
  - **Pre-PR Gate**: Full 4-Expert Review Panel audits the complete consolidated `git diff` before merge (~45s).

---

### Universal Workflow Stages

```
Stage 1: Implementation Plan (Bypassed for Tier 0; Lightweight for Tier 1; Formal for Tier 2; Full RFC for Tier 3)
   │
   ▼
Stage 2: Tiered Plan Review (The Review Gate)
   │     • Tier 0: Bypassed (Zero subagents dispatched)
   │     • Tier 1: Solo Devil's Advocate Critic (~30s)
   │     • Tier 2 & 3: Full 4-Expert Review Panel (3 Domain Specialists + 1 Devil's Advocate, ~45s)
   │     • Remediate all plan objections immediately
   │
   ▼
Stage 3: Human Review Checkpoint (Tier 3 ONLY)
   │     • Tier 0, Tier 1 & Tier 2: Bypassed autonomously (flow directly into Stage 4)
   │     • Tier 3: MANDATORY STOP — wait for explicit human approval before touching code
   │
   ▼
Stage 4: Autonomous Continuous Implementation (TDD)
   │     • Implement tasks continuously in flow
   │     • STRICT PROHIBITION: Zero mid-task pauses or per-task subagent audits
   │
   ▼
Stage 5: Pre-Flight Verification & Pre-PR Diff Audit
   │     • Execute local verification runner (./scripts/verify.sh --staged)
   │     • Tier 0: Direct syntax & automated pre-flight checks (Zero subagent diff review)
   │     • Tier 1: Direct verification & targeted package tests
   │     • Tier 2: Consolidated Devil's Advocate audit of unified git diff (~30s)
   │     • Tier 3: Consolidated Full 4-Expert Panel audit of unified git diff (~45s)
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
2. **Sync Workspace**:
   ```bash
   git pull --rebase origin main
   ```
3. **Draft Implementation Plan**:
   - Tier 0: Omitted (proceed directly to implementation).
   - Tier 1: Lightweight task list in `implementation_plan.md`.
   - Tier 2/3: Full specification in `implementation_plan.md` defining exact schemas, state machines, component interactions, and API signatures.

---

### Stage 2: Tiered Plan Review
Before modifying source code, Aerial MUST audit the plan according to the task's complexity tier:
- **Tier 0**: **Bypassed completely**. Zero subagent review overhead.
- **Tier 1**: Concurrently dispatch **exactly one subagent**: the **Adversarial Systems Critic / Devil's Advocate** to attack edge cases, regression risks, error handling, and invariant compliance (~30s).
- **Tier 2 & Tier 3**: Concurrently dispatch the **4-expert review panel** via `invoke_subagent` (~45s):
  1. **Three Domain Specialists**: Tailored to the task (e.g. Concurrency Engineer, Systems Architect, Platform Specialist).
  2. **One Dedicated Adversarial Systems Critic / Devil's Advocate**: Challenging assumptions, race conditions, memory leaks, and invariants.

**Action**:
- Collect and synthesize audit findings.
- Remediate all valid architectural objections directly in `implementation_plan.md` before proceeding.

---

### Stage 3: Human Review Checkpoint (Tier 3 ONLY)
- **Tier 0, Tier 1 & Tier 2 Tasks**: **Bypassed autonomously**. If the user requested an implementation/fix, Aerial directly incorporates review feedback and transitions immediately to Stage 4 without asking for permission.
- **Tier 3 Tasks**: **MANDATORY STOP**. For database schema migrations, service topology changes, breaking API updates, or high-blast-radius changes, present the synthesized panel findings and **STOP execution to obtain explicit user approval before touching code**.

---

### Stage 4: Autonomous Continuous Implementation (TDD)
1. **Modular Task Execution (TDD)**:
   - Break implementation into discrete, sequential components/tasks.
   - Implement following Test-Driven Development (write tests first, then implementation).
   - Verify task unit tests pass with race detection (`-race`).
2. **STRICT PROHIBITION: Zero Mid-Task Subagent Halts**:
   - **Under NO circumstance should Aerial halt execution between individual tasks to spawn subagents.**
   - Per-task subagent reviews are strictly eliminated. Implementation must proceed continuously from Task 1 to completion.
   - Code review is deferred exclusively to Stage 5 on the consolidated diff.

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
   - **Tier 0**: Direct syntax/schema verification (Zero subagent diff review).
   - **Tier 1**: Direct local verification.
   - **Tier 2**: Dispatch a single **Devil's Advocate Critic** subagent to audit the complete, unified `git diff` against the approved plan (~30s). Resolve any identified regressions before commit.
   - **Tier 3**: Dispatch the **full 4-expert panel** to audit the complete, unified `git diff` (~45s).

3. **ZERO-BYPASS INVARIANT**:
   - Fresh verification evidence must exist in the turn transcript prior to commit.
   - If a check fails, fix the code/tests, re-run `./scripts/verify.sh --staged` until exit code is 0, and proceed.

---

### Stage 6: Commit, Push & Continuous Deployment

1. **Review Diffs & Status**:
   ```bash
   git status && git diff
   ```
2. **Commit with Conventional Messages**:
   ```bash
   git add -A && git commit -m "feat(module): clear description of changes"
   ```
   *The fast-path pre-commit hook verifies staged syntax and BOM hygiene automatically in < 1s.*

3. **Push to Remote & Branch Protection Awareness**:
   - When pushing directly to `main`:
     ```bash
     git push origin main
     ```
     *Pushes proceed immediately without pre-push hook latency; GitHub Actions CI validates the build out-of-band.*
   - When branch protection is active on `main`:
     ```bash
     git checkout -b fix/<topic>
     git push origin fix/<topic>
     ```
     Open a Pull Request via GitHub MCP (`create_pull_request`), enable auto-merge (`gh pr merge --auto --squash`), and let CI verify and merge asynchronously into `main`.

4. **Continuous Deployment Invariant**:
   - **DO NOT run `docker compose up`, `docker compose build`, or `docker restart` from inside the container.**
   - Pushing/merging to `origin/main` triggers GitHub Actions CI (`docker-publish.yml`) to build and publish container images to GitHub Container Registry (`ghcr.io`).
   - Hangar on the host automatically detects the new image and performs an out-of-band container swap without interrupting execution or causing downtime.
