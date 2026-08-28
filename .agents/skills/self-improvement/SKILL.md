---
name: self-improvement
description: Use this skill whenever Aerial needs to modify, enhance, debug, or refactor its own codebase, skills, or system configuration, commit changes, pull updates, or rebuild and redeploy its Docker containers.
---

# Aerial Self-Improvement & Continuous Deployment Runbook

This skill outlines the strict workflow Aerial must follow when modifying its own architecture, codebase, skills, or Docker compose stack.

---

## 1. Project Directory & Workspace Layout

The root repository workspace is located at `/share/aerial`.

```text
/share/aerial/
??? brain/               # Go backend (Discord gateway funnel, agy runner, SQLite memory)
??? discord-mcp/         # Discord MCP server (Streamable HTTP /mcp)
??? docker-mcp/          # Docker socket MCP proxy (supergateway + mcp/docker)
??? github-mcp/          # GitHub Copilot MCP proxy
??? .agents/skills/      # Built-in skills tracked in Git (baked into brain image)
??? skills/              # User custom runtime skills (ignored by Git)
??? docker-compose.yml   # Multi-container orchestration
??? GEMINI.md            # Topology and agent instructions
```

---

## 2. The 5-Phase Self-Improvement Workflow

### Phase 1: Sync & Research
1. **Brainstorming & Design**:
   Before modifying code, run `superpowers:brainstorming` to explore intent and design architecture.
2. **Sync Workspace**:
   **ALWAYS** pull latest upstream commits before making edits:
   ```bash
   cd /share/aerial && git pull --rebase origin main
   ```
3. **Inspect Workspace**:
   Inspect existing code and dependencies under `/share/aerial`.

---

### Phase 2: Edit & Pre-Commit Build Verification (MANDATORY GATE)
1. Make code edits, configuration changes, or skill additions.
2. **VERIFY CONTAINER BUILD BEFORE COMMITTING**:
   You MUST execute a full Docker build of the modified service(s) to run linters, unit tests, and compilation:
   ```bash
   docker compose -f /share/aerial/docker-compose.yml build <service_name>
   ```
   - For `brain`: `docker compose -f /share/aerial/docker-compose.yml build brain` (runs `golangci-lint`, `go test ./...`, and `go build`).
   - For compose/config changes: `docker compose -f /share/aerial/docker-compose.yml config`
3. **ZERO-FAILURE GATE**:
   - If the build, linter, or tests exit with a non-zero code, **DO NOT COMMIT OR PUSH**.
   - Read the error output, fix the code/lint violations, and re-run `docker compose build <service>` until it exits cleanly with code 0.
   - If the task cannot be completed cleanly, rollback uncommitted changes with `git restore .`.

---

### Phase 3: Commit & Push Verified Changes
*Only execute this phase after Phase 2 has passed with a 100% successful build.*

1. Review changes:
   ```bash
   cd /share/aerial && git status && git diff
   ```
2. Commit with conventional commit messages:
   ```bash
   cd /share/aerial && git add -A && git commit -m "feat(module): clear description of change"
   ```
3. Push to upstream repository:
   ```bash
   cd /share/aerial && git push origin main
   ```

---

### Phase 4: Deploy & Restart

#### A. Updating Non-Brain Services (`discord-mcp`, `docker-mcp`, `github-mcp`)
Deploy the pre-built image:
```bash
docker compose -f /share/aerial/docker-compose.yml up -d --no-build <service_name>
```

#### B. Updating `brain` Itself (Self-Restart)
Because recreating `aerial-brain` restarts the runner:
1. Ensure the image was already built in Phase 2 (`docker compose build brain`).
2. Post your complete response back to Discord explaining the changes and announcing the restart.
3. Trigger the restart in the background with a delay buffer:
   ```bash
   (sleep 2 && docker compose -f /share/aerial/docker-compose.yml up -d --no-build brain) &
   ```

---

### Phase 5: Post-Deployment Verification & Rollback
1. **Health Verification**:
   ```bash
   docker compose -f /share/aerial/docker-compose.yml ps
   docker compose -f /share/aerial/docker-compose.yml logs --tail 30 <service_name>
   ```
2. **Automated Rollback Safeguard**:
   If the container fails to start, crashes, or is unhealthy after deployment:
   ```bash
   git log -n 1 --oneline
   git revert --no-edit HEAD
   git push origin main
   docker compose -f /share/aerial/docker-compose.yml up -d --build <service_name>
   ```