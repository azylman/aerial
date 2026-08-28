---
name: self-improvement
description: Use this skill whenever Aerial needs to modify, enhance, debug, or refactor its own codebase, commit changes, pull updates, or rebuild and redeploy its Docker containers.
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
   Before running any code or making modifications, **ALWAYS** run the `superpowers:brainstorming` skill to explore user intent, requirements, and design architecture.
2. **Ensure Workspace is Up-to-Date**:
   Before reading files or making changes, **ALWAYS** sync the local workspace with the remote upstream repository to avoid working on stale code:
   ```bash
   cd /share/aerial && git pull --rebase origin main
   ```
3. **Inspect Current Codebase**:
   Locate the relevant source files under `/share/aerial`, inspect the latest code, and understand existing patterns before making modifications.

### Phase 2: Edit & Verify Syntax
1. Make surgical edits to code, Dockerfiles, or configs.
2. Verify syntax and compilation before attempting deployment:
   - For Go (`brain/`): `cd /share/aerial/brain && CGO_ENABLED=0 go vet ./...`
   - For Docker Compose: `docker compose -f /share/aerial/docker-compose.yml config`

### Phase 3: Commit & Push Changes
1. Check changed files:
   ```bash
   cd /share/aerial && git status && git diff
   ```
2. Commit with conventional commit messages:
   ```bash
   cd /share/aerial && git add -A && git commit -m "feat(module): clear description of change"
   ```
3. **MANDATORY**: Push to upstream GitHub repository immediately after committing:
   ```bash
   cd /share/aerial && git push origin main
   ```
   *Do NOT proceed to Phase 4 until `git push` has succeeded and working tree / branch status is clean.*

### Phase 4: Rebuild & Redeploy

#### A. Updating Non-Brain Services (`discord-mcp`, `docker-mcp`, `github-mcp`)
Rebuild and recreate the target container without interrupting `brain`:
```bash
docker compose -f /share/aerial/docker-compose.yml up -d --build <service_name>
```
Verify container health:
```bash
docker compose -f /share/aerial/docker-compose.yml ps
```

#### B. Updating `brain` Itself (Self-Rebuild)
Because recreating `aerial-brain` replaces the currently running process:
1. **Pre-build image while alive**:
   ```bash
   docker compose -f /share/aerial/docker-compose.yml build brain
   ```
2. **Post Discord Reply**:
   Send your complete response back to Discord explaining the changes made and notifying the user that `brain` is restarting.
3. **Trigger Restart with Buffer**:
   ```bash
   (sleep 2 && docker compose -f /share/aerial/docker-compose.yml up -d --no-build brain) &
   ```

---

## 3. Post-Deployment Verification
After any microservice deployment:
- Check running containers: `docker compose -f /share/aerial/docker-compose.yml ps`
- Check container logs for errors: `docker compose -f /share/aerial/docker-compose.yml logs --tail 25 <service_name>`