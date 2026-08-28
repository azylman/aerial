---
name: self-update
description: Use this skill when the user asks Aerial to update itself, pull latest code from git, rebuild containers, or check for repository updates.
---

# Self-Update & Microservice Deployment Runbook

This skill outlines how Aerial safely updates code and redeploys its Docker services.

## Working Directory
The root project directory is `/share/aerial`. Always operate from this directory or pass `-f /share/aerial/docker-compose.yml`.

## Step-by-Step Update Workflow

### 1. Pull Latest Changes
Run git pull in `/share/aerial`:
```bash
cd /share/aerial && git pull
```
Inspect what changed:
```bash
cd /share/aerial && git log -n 1 --stat
```

### 2. Updating Non-Brain Microservices
If changes affect `discord-mcp`, `docker-mcp`, `github-mcp`, or `agentsview`:
```bash
docker compose -f /share/aerial/docker-compose.yml up -d --build <service_name>
```
These rebuild with zero interruption to `brain`. Report completion directly.

### 3. Updating Brain Itself
If changes affect `brain/` or `docker-compose.yml`:
1. **Compile First**:
   ```bash
   docker compose -f /share/aerial/docker-compose.yml build brain
   ```
2. **Send Discord Response**:
   Post your response to Discord summarizing the updates pulled and letting the user know the brain is restarting to apply the changes.
3. **Trigger Container Restart**:
   Trigger the restart with a brief 2-second buffer so your Discord message packet finishes sending:
   ```bash
   (sleep 2 && docker compose -f /share/aerial/docker-compose.yml up -d --no-build brain) &
   ```
   The new `brain` will boot up and reconnect to Discord automatically.