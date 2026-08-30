---
name: self-update
description: Use this skill when the user asks Aerial to update itself, pull latest code from git, or deploy system updates.
---

# Self-Update & Microservice Deployment Runbook

This skill outlines how Aerial safely updates code, applies user configurations, and deploys changes across the dual-repository architecture.

## 1. Two-Repository Separation of Concerns

Aerial separates generic system code from private user configuration:

### Repository Matrix

| Attribute | Core Engine (`/share/aerial`) | User Configuration (`/share/aerial-config`) |
| :--- | :--- | :--- |
| **Repository** | `azylman/aerial` (Public) | `azylman/aerial-config` (Private) |
| **Contents** | Brain engine, MCPs, base skills, compose topology | `config.yaml`, `AGENTS.md`, `custom-skills/`, compose overrides |
| **Invariants** | 100% generic, zero personal data/names, zero secrets | User persona, personal identity, private runbooks |
| **Deployment** | GitHub Actions -> GHCR -> Watchtower (60s rolling update) | In-process `fsnotify` watcher + GitSync (immediate hot-reload) |

---

## 2. Core Engine Update Workflow (`/share/aerial`)

Use this workflow when modifying the core execution brain, built-in MCPs, or Docker stack:

### Step 1: Pull Latest Upstream Changes
```bash
cd /share/aerial && git pull --rebase origin main
```

### Step 2: Verify Strict Invariants
- **Zero Personal Data Check**: Confirm NO user names (e.g. "Alex"), Discord handles (e.g. "Arcane"), private locations, or personal logic are included in code, comments, or prompts.
- **Zero Plaintext Secrets**: Confirm no API keys or tokens are written to disk.

### Step 3: Run Full Unit Test Verification
```bash
cd /share/aerial/brain && go test -v ./...
cd /share/aerial/scheduler-mcp && go test -v ./...
cd /share/aerial/discord-mcp && go test -v ./...
```

### Step 4: Push to Remote
```bash
cd /share/aerial && git add -A && git commit -m "feat(module): description of update"
git push origin main
```

### Step 5: Automated CD & Watchtower Invariant
- **NEVER execute `docker compose up`, `docker compose build`, or `docker restart` from inside the container.**
- GitHub Actions automatically builds and publishes GHCR images.
- Watchtower on the host automatically updates containers out-of-band within 60 seconds.

---

## 3. User Configuration Update Workflow (`/share/aerial-config`)

Use this workflow when adjusting user persona, user identity aliases, private skills, or config options:

### Step 1: Pull Latest Upstream Changes
```bash
cd /share/aerial-config && git pull --rebase origin main
```

### Step 2: Edit & Verify
- Modify `config.yaml`, `AGENTS.md`, `custom-skills/`, or `docker-compose.override.yml`.

### Step 3: Commit and Push
```bash
cd /share/aerial-config && git add -A && git commit -m "feat(persona): description of change"
git push origin main
```

### Step 4: In-Process Hot-Reload
- The built-in file watcher dynamically reloads rules, skills, and configuration within milliseconds without container restarts or downtime.
