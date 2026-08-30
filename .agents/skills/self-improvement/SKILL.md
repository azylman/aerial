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
  - **Zero Personal Data Invariant**: **NEVER** commit user names (e.g., "Alex"), user handles (e.g., "Arcane", "arcane103"), family members, personal home locations, private device/entity IDs, or user-specific business logic into this repository.
  - **Zero Plaintext Secrets Invariant**: NEVER commit API keys, tokens, private webhook URLs, or GitHub PATs to disk.
- **Deploy Path**: Commit and push to `azylman/aerial:main`. Watchtower builds & deploys container updates out-of-band.

### 1.2 User Configuration Repository (e.g. `azylman/aerial-config` at `/share/aerial-config`)
- **Scope**: User options (`config.yaml`), persona overrides & user identity/aliases (`AGENTS.md`), private smart home/domain workflows (`custom-skills/`), sidecar containers (`docker-compose.override.yml`), and host environment secrets (`.env`).
- **Deploy Path**: Commit and push to the user's private configuration repository. Hot-reloaded automatically in-process.

---

## 2. The 9-Step Engineering Workflow

Whenever undertaking feature development, architectural changes, bug fixes, or system modifications, Aerial must follow this strict workflow:

```
Step 1: Brainstorm & Design Spec (docs/specs/...)
   │
   ▼
Step 2: Write Initial Code (Modular Packages)
   │
   ▼
Step 3: Senior Code Review (Concurrency, Leaks, DB Safety, Repo Boundary)
   │
   ▼
Step 4: Adversarial Critique / Devil's Advocate (Edge Cases, Personal Data Leakage)
   │
   ▼
Step 5: Human Review Checkpoint (Synthesize 3-way findings & get approval)
   │
   ▼
Step 6: Implement Approved Refinements
   │
   ▼
Step 7: Comprehensive Unit Tests & Complexity Comparison
   │
   ▼
Step 8: Pre-Commit Test Verification Gate
   │
   ▼
Step 9: Commit, Push & Continuous Deployment
```

---

### Step 1: Brainstorming & Architectural Specification
1. **Explore Intent & Architecture**:
   - Activate the `brainstorming` skill.
   - Clarify scope, system constraints, persistence schemas, concurrency boundaries, and failure modes before writing code.
2. **Sync Workspace**:
   ```bash
   cd /share/aerial && git pull --rebase origin main
   ```
3. **Draft Formal Design Spec**:
   - Write a complete specification in `docs/specs/YYYY-MM-DD-<feature-name>.md`.
   - Define exact schemas, state machines, component interactions, and API signatures.

---

### Step 2: Implementation (Modular Architecture)
1. Write the initial implementation code cleanly across modular packages with single responsibilities.
2. Follow strict isolation:
   - Separate database persistence (`pkg/db`), message delivery (`pkg/delivery`), runner execution (`pkg/runner`), and queue scheduling (`pkg/queue`).
3. **DO NOT COMMIT OR PUSH TO GIT YET**. All work remains local for review and comparison.

---

### Step 3: Senior Code Review
Conduct a thorough, deep technical code review inspecting:
- **Repository Boundary & Generic Invariant**: Ensure no user-specific names (e.g. "Alex"), Discord handles (e.g. "Arcane"), family members, private devices, or home locations are hardcoded in code, comments, or prompts for `/share/aerial`.
- **Concurrency & Goroutine Lifecycle**: Mutex lock scope, race conditions, channel deadlocks, and worker idle teardown.
- **Database Safety**: SQLite WAL mode (`PRAGMA journal_mode = WAL;`), busy timeout (`PRAGMA busy_timeout = 5000;`), and connection pool constraints.
- **Subprocess Management**: Context cancellation, zombie process prevention, stream buffer capture, and exit code handling.
- **Error Handling**: Complete error path coverage and operational logging.

---

### Step 4: Adversarial Critique & Devil's Advocate
Aggressively challenge the design and code. Specifically analyze and argue why the implementation could fail:
- **Personal Data / Logic Leakage**: Did any private user business logic, personal aliases, or private secrets leak into the public core engine repository?
- **Cascading Failures (Apology Cascades)**: Are we calling an external service (e.g. an LLM API) to report that the external service is down?
- **Poison-Pill Death Spirals**: Will an invalid message or crash loop restart infinitely on boot recovery?
- **Burst Traffic & Backpressure**: What happens if 50 messages arrive in 10 seconds? Is there stale backlog accumulation or unbounded channel buffering?
- **Data Mangling**: Does message splitting cut through Markdown code blocks or JSON formatting?
- **Session Corruption**: Could transient errors falsely trigger session deletion?

---

### Step 5: Human Review Checkpoint (Mandatory Gate)
Present a structured 3-way synthesis to the human user:
1. **Plan vs. Reviewer vs. Devil's Advocate**: Highlight where the initial plan, code reviewer, and devil's advocate agree and where they clash.
2. **Explicit Decisions Required**: Outline architectural trade-offs (e.g. static fallback vs dynamic generation, poison-pill thresholds, schema changes).
3. **STOP and wait for human approval** before making final code refinements.

---

### Step 6: Implement Approved Refinements
1. Incorporate all decisions and resolutions approved during the Human Review Checkpoint.
2. Fix identified P0/P1 bugs, race conditions, error misclassifications, and locking issues.

---

### Step 7: Comprehensive Unit Tests & Complexity Comparison
1. **Unit Test Coverage**:
   - Write comprehensive unit tests in `*_test.go` covering clean success paths, transient errors, corruption recovery, poison-pill quarantine, and boundary conditions.
2. **Codebase & Complexity Comparison**:
   - Compare Lines of Code (LOC) for production vs test suites.
   - Compare algorithmic time and space complexity against the legacy implementation.

---

### Step 8: Pre-Commit Test Verification (Zero-Failure Gate)
*MANDATORY: Never commit or push unverified code.*

1. **Execute Unit Tests**:
   - For Go services (`brain`, `scheduler-mcp`, `discord-mcp`):
     ```bash
     cd /share/aerial/<service> && go test -v ./...
     ```
2. **Zero-Failure Rule**:
   - If tests fail, **DO NOT COMMIT OR PUSH**.
   - Read error logs, fix violations, and re-run until all tests pass with exit code 0.

---

### Step 9: Commit, Push & Continuous Deployment

1. **Review Diffs & Status**:
   ```bash
   cd /share/aerial && git status && git diff
   ```
2. **Commit with Conventional Messages**:
   ```bash
   cd /share/aerial && git add -A && git commit -m "feat(module): clear description of changes"
   ```
3. **Push to Remote**:
   ```bash
   cd /share/aerial && git push origin main
   ```
4. **Continuous Deployment Invariant**:
   - **DO NOT run `docker compose up`, `docker compose build`, or `docker restart` from inside the container.**
   - Pushing to `origin/main` triggers the GitHub Actions CI pipeline to build and publish the image to GitHub Container Registry (`ghcr.io`).
   - Watchtower on the host automatically detects the new image and performs an out-of-band container swap within 60 seconds without interrupting execution or causing downtime.
