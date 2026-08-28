---
name: self-improvement
description: Use this skill whenever Aerial needs to modify, enhance, debug, or refactor its own codebase, skills, or system configuration, commit changes, pull updates, or rebuild and redeploy its Docker containers.
---

# Aerial Self-Improvement & Continuous Engineering Workflow

This skill defines the mandatory, rigorous development workflow Aerial must follow whenever designing, implementing, refactoring, or modifying its own codebase, skills, configurations, or Docker container stack.

---

## 1. Project Directory & Workspace Layout

The root repository workspace is located at `/share/aerial`.

```text
/share/aerial/
??? brain/               # Go backend (Discord gateway funnel, queue worker pool, agy runner, SQLite memory)
??? discord-mcp/         # Discord MCP server (Streamable HTTP /mcp)
??? docker-mcp/          # Docker socket MCP proxy (supergateway + mcp/docker)
??? github-mcp/          # GitHub Copilot MCP proxy
??? .agents/skills/      # Built-in skills tracked in Git (baked into brain image)
??? skills/              # User custom runtime skills (ignored by Git)
??? docs/specs/          # Architectural specifications and design documents
??? docker-compose.yml   # Multi-container orchestration
??? GEMINI.md            # Topology and agent instructions
```

---

## 2. The 7-Step Engineering Workflow

Whenever undertaking feature development, architectural changes, bug fixes, or system modifications, Aerial must follow this strict 7-step process:

```
Step 1: Brainstorm & Design Spec (docs/specs/...)
   ?
   ?
Step 2: Write Initial Code (Modular Packages)
   ?
   ?
Step 3: Senior Code Review (Concurrency, Leaks, DB Safety)
   ?
   ?
Step 4: Adversarial Critique / Devil's Advocate (Edge Cases, Cascades)
   ?
   ?
Step 5: Human Review Checkpoint (Synthesize 3-way findings & get approval)
   ?
   ?
Step 6: Implement Approved Refinements
   ?
   ?
Step 7: Comprehensive Unit Tests & Complexity Comparison
   ?
   ?
Step 8: Pre-Commit Docker Build Verification Gate
   ?
   ?
Step 9: Commit, Push, Deploy & Health Verification
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
- **Concurrency & Goroutine Lifecycle**: Mutex lock scope, race conditions, channel deadlocks, and worker idle teardown.
- **Database Safety**: SQLite WAL mode (`PRAGMA journal_mode = WAL;`), busy timeout (`PRAGMA busy_timeout = 5000;`), and connection pool constraints.
- **Subprocess Management**: Context cancellation, zombie process prevention, stream buffer capture, and exit code handling.
- **Error Handling**: Complete error path coverage and operational logging.

---

### Step 4: Adversarial Critique & Devil's Advocate
Aggressively challenge the design and code. Specifically analyze and argue why the implementation could fail:
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

### Step 8: Pre-Commit Docker Build Verification (Zero-Failure Gate)
*MANDATORY: Never commit or push unverified code.*

1. **Execute Docker Container Build**:
   ```bash
   docker compose -f /share/aerial/docker-compose.yml build brain
   ```
   - Verifies `golangci-lint run ./...`
   - Verifies `go test ./...`
   - Verifies compilation `go build`
2. **Zero-Failure Rule**:
   - If linter, tests, or compilation fail with any non-zero exit code, **DO NOT COMMIT OR PUSH**.
   - Read error logs, fix violations, and re-run until the build succeeds with exit code 0.

---

### Step 9: Commit, Push, Deploy & Post-Deployment Health Check

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
4. **Deploy & Restart**:
   - **For Non-Brain Services** (`discord-mcp`, `docker-mcp`, `github-mcp`):
     ```bash
     docker compose -f /share/aerial/docker-compose.yml up -d --no-build <service_name>
     ```
   - **For `brain` (Self-Restart)**:
     Post explanation to Discord, then trigger restart in background:
     ```bash
     (sleep 2 && docker compose -f /share/aerial/docker-compose.yml up -d --no-build brain) &
     ```
5. **Post-Deployment Verification**:
   ```bash
   docker compose -f /share/aerial/docker-compose.yml ps
   docker compose -f /share/aerial/docker-compose.yml logs --tail 30 brain
   ```
6. **Automated Rollback**:
   If the container crashes or fails health checks:
   ```bash
   git revert --no-edit HEAD && git push origin main
   docker compose -f /share/aerial/docker-compose.yml up -d --build brain
   ```