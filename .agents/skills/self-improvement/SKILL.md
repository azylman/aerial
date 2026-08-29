---
name: self-improvement
description: Use this skill whenever Aerial needs to modify, enhance, debug, or refactor its own codebase, skills, or system configuration, commit changes, pull updates, or deploy updates via CI/CD.
---

# Aerial Self-Improvement & Continuous Engineering Workflow

This skill defines the mandatory, rigorous development workflow Aerial must follow whenever designing, implementing, refactoring, or modifying its own codebase, skills, configurations, or Docker container stack.

---

## 1. Project Directory & Workspace Layout

The root repository workspace is located at `/share/aerial`.

```text
/share/aerial/
â”œâ”€â”€ brain/               # Go backend (Discord gateway funnel, queue worker pool, agy runner, SQLite memory)
â”œâ”€â”€ discord-mcp/         # Discord MCP server (Streamable HTTP /mcp)
â”œâ”€â”€ docker-mcp/          # Docker socket MCP proxy (supergateway + mcp/docker)
â”œâ”€â”€ github-mcp/          # GitHub Copilot MCP proxy
â”œâ”€â”€ scheduler-mcp/       # Persistent SQLite task scheduler MCP
â”œâ”€â”€ .agents/skills/      # Built-in skills tracked in Git (baked into brain image)
â”œâ”€â”€ skills/              # User custom runtime skills (ignored by Git)
â”œâ”€â”€ docs/specs/          # Architectural specifications and design documents
â”œâ”€â”€ docker-compose.yml   # Multi-container orchestration
â””â”€â”€ GEMINI.md            # Topology and agent instructions
```

---

## 2. The 9-Step Engineering Workflow

Whenever undertaking feature development, architectural changes, bug fixes, or system modifications, Aerial must follow this strict workflow:

```
Step 1: Brainstorm & Design Spec (docs/specs/...)
   â”‚
   â–¼
Step 2: Write Initial Code (Modular Packages)
   â”‚
   â–¼
Step 3: Senior Code Review (Concurrency, Leaks, DB Safety)
   â”‚
   â–¼
Step 4: Adversarial Critique / Devil's Advocate (Edge Cases, Cascades)
   â”‚
   â–¼
Step 5: Human Review Checkpoint (Synthesize 3-way findings & get approval)
   â”‚
   â–¼
Step 6: Implement Approved Refinements
   â”‚
   â–¼
Step 7: Comprehensive Unit Tests & Complexity Comparison
   â”‚
   â–¼
Step 8: Pre-Commit Test Verification Gate
   â”‚
   â–¼
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
