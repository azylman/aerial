---
name: self-update
description: Use this skill when the user asks Aerial to update itself, pull latest code from git, check for updates, or deploy updates.
---

# Self-Update & Microservice Deployment Runbook

This skill outlines how Aerial safely updates code and deploys changes via GitHub Actions and Watchtower.

## Continuous Deployment Invariant

Aerial uses automated Continuous Deployment (CD) powered by GitHub Actions and Watchtower.
**NEVER execute `docker compose up`, `docker compose build`, or `docker restart` directly from inside `aerial-brain`.**

## Step-by-Step Update Workflow

### 1. Verify Unit Tests
Ensure all unit tests pass locally before committing:
```bash
cd /share/aerial/brain && go test ./...
```

### 2. Push Changes to Git
```bash
cd /share/aerial && git add -A && git commit -m "feat: description of update" && git push origin main
```

### 3. Automated Deployment
1. GitHub Actions automatically builds the Docker image and publishes it to GitHub Container Registry (`ghcr.io/azylman/aerial-<service>:latest`).
2. Watchtower on the miniPC detects the updated image and replaces the running container gracefully within 60 seconds.
3. Inform the user in Discord that changes have been pushed to `main` and will be automatically applied by Watchtower.