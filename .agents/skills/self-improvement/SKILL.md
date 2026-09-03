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
- **Deploy Path**: Commit and push to `azylman/aerial:main`. Watchtower builds & deploys container updates out-of-band.

### 1.2 User Configuration Repository (e.g. `azylman/aerial-config` at `/share/aerial-config`)
- **Scope**: User options (`config.yaml`), persona overrides & user identity/aliases (`AGENTS.md`), private smart home/domain workflows (`custom-skills/`), sidecar containers (`docker-compose.override.yml`), and host environment secrets (`.env`).
- **Deploy Path**: Commit and push to the user's private configuration repository. Hot-reloaded automatically in-process.

---

## 2. The 6-Stage Multi-Agent Engineering Workflow

Whenever undertaking feature development, architectural changes, bug fixes, or system modifications, Aerial must follow this strict workflow:

```
Stage 1: Brainstorming & Formal Implementation Plan
   │
   ▼
Stage 2: The 4-Expert Review Panel — Plan Audit
   │     • 3 Domain Specialists tailored to the problem
   │     • 1 Dedicated Adversarial Systems Critic / Devil's Advocate
   │
   ▼
Stage 3: Human Review Checkpoint (Mandatory Gate)
   │     • Synthesize expert panel findings, trade-offs, and consensus
   │     • STOP and obtain explicit user approval before touching code
   │
   ▼
Stage 4: Implementation with Per-Task Expert Review
   │     • Break plan into discrete, modular tasks (TDD)
   │     • For EACH task: consult the 4-expert panel to audit code & tests
   │     • Verify invariants, race safety, and error paths before next task
   │
   ▼
Stage 5: Pre-Flight Verification Gate (The New Path)
   │     • Execute monorepo verification runner (verify.sh / verify.ps1)
   │     • ZERO-BYPASS INVARIANT: --no-verify strictly forbidden
   │
   ▼
Stage 6: Commit, Push & Continuous Deployment
         • Fast-path pre-commit & comprehensive pre-push hooks
         • Automated PR auto-merge (if branch protection active)
         • Watchtower out-of-band rolling deployment on main (60s)
```

---

### Stage 1: Brainstorming & Architectural Specification
1. **Explore Intent & Scope**:
   - Activate the `brainstorming` skill.
   - Clarify scope, system constraints, persistence schemas, concurrency boundaries, and failure modes before writing code.
2. **Sync Workspace**:
   ```bash
   git pull --rebase origin main
   ```
3. **Draft Formal Implementation Plan**:
   - Write a complete design document in `implementation_plan.md` (or `docs/specs/YYYY-MM-DD-<feature-name>.md`).
   - Define exact schemas, state machines, component interactions, and API signatures.

---

### Stage 2: The 4-Expert Review Panel — Plan Audit
Before modifying source code, Aerial MUST assemble and consult a dynamic review panel of **four independent expert subagents**:
1. **Three Domain Specialists**: Dynamically chosen and tailored to the technical requirements of the task (e.g. CI/CD & DevOps Specialist, Distributed Systems & Concurrency Engineer, Discord Gateway & UX Specialist, Git Tooling Specialist, etc.).
2. **One Dedicated Adversarial Systems Critic / Devil's Advocate**: Tasked specifically with aggressively challenging assumptions, probing edge cases, failure modes, race conditions, memory leaks, and personal data leakage.

**Action**:
- Concurrently dispatch all four subagents using `invoke_subagent`.
- Collect and synthesize their structured audit reports and verdicts.

---

### Stage 3: Human Review Checkpoint (Mandatory Gate)
Present a structured synthesis of the expert panel's audit to the human user:
1. **Consensus & Contentions**: Highlight where the domain specialists and Devil's Advocate agreed and where they clashed.
2. **Key Decisions & Trade-Offs**: Outline architectural trade-offs, risk mitigations, and plan remediations.
3. **MANDATORY STOP**: Wait for explicit human approval before writing or modifying any implementation code.

---

### Stage 4: Implementation with Per-Task Expert Review
1. **Modular Task Execution (TDD)**:
   - Break implementation into discrete, sequential components/tasks.
   - Implement following Test-Driven Development (write tests first, then implementation).
   - Verify task unit tests pass with race detection (`-race`).
2. **Per-Task Expert Review Gate**:
   - **For EACH task completed**, consult the 4-expert panel (or dispatch a dedicated Devil's Advocate subagent from the panel) to audit the task's code changes.
   - Inspect:
     - Strict adherence to repository invariants (zero personal data, zero plaintext secrets).
     - Concurrency safety, lock lifecycles, and deadlocks.
     - Error path coverage and defensive nil guards.
   - **Remediation**: Resolve all identified P0/P1 bugs and audit objections before proceeding to subsequent tasks.

---

### Stage 5: Pre-Flight Verification Gate (The New Path)
*MANDATORY: Never commit or push unverified code.*

1. **Execute Pre-Flight Verification Runner**:
   Always execute the monorepo verification runner before committing:
   - **Linux / Container**:
     ```bash
     ./scripts/verify.sh
     ```
   - **Windows Host**:
     ```powershell
     powershell -ExecutionPolicy Bypass -File scripts/verify.ps1
     ```
   - **Container Fallback**: If local Go or Node tools are not installed in `PATH`, the runner automatically delegates to deterministic Docker containers (`golangci/golangci-lint:v1.59.1`, `golang:1.22`, `node:20`).

2. **Full CI Parity Verification**:
   The runner validates all microservices (`brain`, `scheduler-mcp`, `discord-mcp`, `dashboard`):
   - **Static Analysis & Linting**: `golangci-lint run ./...`
   - **Unit Test Suites**: `go test -v ./...`
   - **Frontend & Documentation Syntax**: `node --check dashboard/static/app.js` and `docs-service/...`
   - **Frontend Unit Tests**: `node --test dashboard/app.test.js`

3. **ZERO-BYPASS INVARIANT**:
   - **Under NO CIRCUMSTANCE is an agent permitted to use `git commit --no-verify`, `git commit -n`, or `git push --no-verify`.**
   - Any attempt to bypass pre-commit or pre-push verification is classified as a Critical System Invariant Violation.
   - If a hook or test fails:
     1. Read the hook failure log from stderr.
     2. Fix the reported code violations, linter errors, or failing unit tests in the source files.
     3. Re-run `./scripts/verify.sh` until exit code is 0.
     4. Retry the commit.

---

### Stage 6: Commit, Push & Continuous Deployment

1. **Review Diffs & Status**:
   ```bash
   git status && git diff
   ```
2. **Commit with Conventional Messages (WITHOUT `--no-verify`)**:
   ```bash
   git add -A && git commit -m "feat(module): clear description of changes"
   ```
   *The fast-path pre-commit hook verifies staged changes automatically.*

3. **Push to Remote & Branch Protection Awareness**:
   - When pushing directly to `main`:
     ```bash
     git push origin main
     ```
     *The pre-push hook runs full monorepo verification before egress.*
   - When branch protection is active on `main`:
     ```bash
     git checkout -b fix/<topic>
     git push origin fix/<topic>
     ```
     Open a Pull Request via GitHub MCP (`create_pull_request`), enable auto-merge (`gh pr merge --auto --squash`), and let CI verify and merge asynchronously into `main`.

4. **Continuous Deployment Invariant**:
   - **DO NOT run `docker compose up`, `docker compose build`, or `docker restart` from inside the container.**
   - Pushing/merging to `origin/main` triggers GitHub Actions CI (`docker-publish.yml`) to build and publish container images to GitHub Container Registry (`ghcr.io`).
   - Watchtower on the host automatically detects the new image and performs an out-of-band container swap within 60 seconds without interrupting execution or causing downtime.
