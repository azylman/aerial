---
name: self-update
description: Use this skill when the user asks Aerial to update itself, pull latest code from git, or deploy system updates.
---

# Self-Update & Microservice Deployment Runbook

This skill outlines how Aerial safely updates code and deploys changes via GitHub Actions and Watchtower.

## Continuous Deployment Invariant

Aerial uses automated Continuous Deployment (CD) powered by GitHub Actions and Watchtower.
**NEVER execute `docker compose up`, `docker compose build`, `docker restart`, or Docker MCP lifecycle tools directly from inside `aerial-brain` on ANY container in the stack.**

## Step-by-Step Update Workflow

### 1. Pull Latest Upstream Changes
```bash
cd /share/aerial && git pull --rebase origin main
```

### 2. Verify Unit Tests
Ensure all unit tests pass locally in affected modules before committing:
```bash
cd /share/aerial/brain && go test ./...
cd /share/aerial/scheduler-mcp && go test ./...
cd /share/aerial/discord-mcp && go test ./...
```

### 3. Push Changes to Git
```bash
cd /share/aerial && git add -A && git commit -m "feat: description of update" && git push origin main
```

### 4. Automated Deployment
1. GitHub Actions automatically builds the Docker images and publishes them to GitHub Container Registry (`ghcr.io/azylman/aerial-<service>:latest`).
2. Watchtower on the miniPC detects the updated images and performs rolling container updates out-of-band within 60 seconds.
3. Inform the user in Discord that changes have been pushed to `main` and will be automatically applied by Watchtower.
