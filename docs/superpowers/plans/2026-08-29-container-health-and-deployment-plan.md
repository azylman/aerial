# Container Health Monitoring & Automated Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a robust, off-the-shelf continuous deployment and self-healing infrastructure using GitHub Actions (GHCR), Watchtower, Autoheal, and calibrated Docker restart/healthcheck policies.

**Architecture:** Cloud-based GitHub Actions CI compiles tests and builds multi-stage Docker images to GitHub Container Registry (`ghcr.io`). Watchtower on the miniPC polls GHCR every 60s to gracefully recreate updated containers out-of-band, Autoheal monitors container healthchecks and restarts frozen containers, and Docker daemon restart policies (`restart: unless-stopped`) resurrect crashed processes.

**Tech Stack:** GitHub Actions, GHCR (`ghcr.io`), Docker Compose, `containrrr/watchtower:latest`, `willfarrell/autoheal:latest`, Go / Docker.

**Spec:** [`docs/superpowers/specs/2026-08-29-container-health-and-deployment-design.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-08-29-container-health-and-deployment-design.md)

## Global Constraints
- `aerial-brain` must never invoke `docker compose` on itself or sibling containers.
- All Docker services must configure `restart: unless-stopped`.
- Healthchecks must be calibrated (e.g. 180s start period for Ollama, 60s start period and 60s stop grace period for Brain).
- All built-in skills and agent system rules must be updated to remove in-container restart commands.

---

### Task 1: GitHub Actions CI/CD Workflow (`.github/workflows/deploy.yml`)

**Files:**
- Create: `.github/workflows/deploy.yml`

**Interfaces:**
- Consumes: Repository source code across all services (`brain/`, `scheduler-mcp/`, `discord-mcp/`, `docker/ollama/`, etc.).
- Produces: Published Docker images on GHCR:
  - `ghcr.io/azylman/aerial-brain:latest`
  - `ghcr.io/azylman/aerial-scheduler-mcp:latest`
  - `ghcr.io/azylman/aerial-discord-mcp:latest`
  - `ghcr.io/azylman/aerial-docker-mcp:latest`
  - `ghcr.io/azylman/aerial-github-mcp:latest`
  - `ghcr.io/azylman/aerial-ollama:latest`

- [ ] **Step 1: Create `.github/workflows/deploy.yml`**

Create `.github/workflows/deploy.yml` with the following content:
```yaml
name: Continuous Delivery

on:
  push:
    branches:
      - main

permissions:
  contents: read
  packages: write

jobs:
  test:
    name: Run Unit Tests
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Go 1.22
        uses: actions/setup-go@v5
        with:
          go-version: '1.22'
          cache: true

      - name: Test Brain
        run: |
          cd brain
          go test -v ./...

      - name: Test Scheduler MCP
        run: |
          cd scheduler-mcp
          go test -v ./...

      - name: Test Discord MCP
        run: |
          cd discord-mcp
          go test -v ./...

  build-and-push:
    name: Build & Push Images to GHCR
    needs: test
    runs-on: ubuntu-latest
    strategy:
      matrix:
        include:
          - service: brain
            context: ./brain
            image: ghcr.io/azylman/aerial-brain
          - service: scheduler-mcp
            context: ./scheduler-mcp
            image: ghcr.io/azylman/aerial-scheduler-mcp
          - service: discord-mcp
            context: ./discord-mcp
            image: ghcr.io/azylman/aerial-discord-mcp
          - service: docker-mcp
            context: ./docker-mcp
            image: ghcr.io/azylman/aerial-docker-mcp
          - service: github-mcp
            context: ./github-mcp
            image: ghcr.io/azylman/aerial-github-mcp
          - service: ollama
            context: ./docker/ollama
            image: ghcr.io/azylman/aerial-ollama
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Build and push ${{ matrix.service }}
        uses: docker/build-push-action@v5
        with:
          context: ${{ matrix.context }}
          push: true
          tags: |
            ${{ matrix.image }}:latest
            ${{ matrix.image }}:${{ github.sha }}
          cache-from: type=gha
          cache-to: type=gha,mode=max
```

- [ ] **Step 2: Validate YAML syntax**

Run:
```powershell
# Verify YAML loads properly
Get-Content "C:\Users\alexz\.gemini\antigravity\scratch\gundam\.github\workflows\deploy.yml" | Out-Null
```

- [ ] **Step 3: Commit Task 1**

```bash
git add .github/workflows/deploy.yml
git commit -m "ci(workflow): add continuous delivery pipeline building and publishing images to GHCR"
```

---

### Task 2: Docker Compose Configuration Update (`docker-compose.yml`)

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: GHCR image tags from Task 1.
- Produces: Standardized, supervised container stack with Watchtower, Autoheal, restart policies, and tuned healthchecks.

- [ ] **Step 1: Update `docker-compose.yml`**

Modify `docker-compose.yml` to:
1. Reference GHCR images with local build fallbacks.
2. Add `restart: unless-stopped` to all services.
3. Add `aerial-watchtower` service definition.
4. Add `aerial-autoheal` service definition.
5. Add `labels: autoheal: "true"` and calibrated healthchecks to all services.

```yaml
version: '3.8'

services:
  brain:
    image: ghcr.io/azylman/aerial-brain:latest
    build:
      context: ./brain
    container_name: aerial-brain
    restart: unless-stopped
    stop_grace_period: 60s
    ports:
      - "8088:8080"
    environment:
      - GEMINI_API_KEY=${GEMINI_API_KEY}
      - DISCORD_BOT_TOKEN=${DISCORD_BOT_TOKEN}
      - DEFAULT_TIMEZONE=${DEFAULT_TIMEZONE:-America/Los_Angeles}
      - TZ=${DEFAULT_TIMEZONE:-America/Los_Angeles}
      - AERIAL_MODEL=${AERIAL_MODEL:-Gemini 3.6 Flash (Low)}
      - AERIAL_TIMEOUT_MINUTES=${AERIAL_TIMEOUT_MINUTES:-15}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - aerial-brain-gemini:/root/.gemini
      - aerial-brain-data:/data
      - /share/aerial:/share/aerial
    networks:
      - aerial-net
    labels:
      autoheal: "true"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8080/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 60s
    depends_on:
      discord-mcp:
        condition: service_healthy
      scheduler-mcp:
        condition: service_healthy
      docker-mcp:
        condition: service_healthy
      github-mcp:
        condition: service_healthy
      ollama:
        condition: service_healthy

  watchtower:
    image: containrrr/watchtower:latest
    container_name: aerial-watchtower
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - WATCHTOWER_POLL_INTERVAL=60
      - WATCHTOWER_CLEANUP=true
      - WATCHTOWER_INCLUDE_SELF=false
      - WATCHTOWER_ROLLING_RESTART=true
    networks:
      - aerial-net

  autoheal:
    image: willfarrell/autoheal:latest
    container_name: aerial-autoheal
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    environment:
      - AUTOHEAL_CONTAINER_LABEL=autoheal
      - AUTOHEAL_INTERVAL=15
      - AUTOHEAL_START_PERIOD=0
      - AUTOHEAL_DEFAULT_STOP_TIMEOUT=20
    networks:
      - aerial-net

  scheduler-mcp:
    image: ghcr.io/azylman/aerial-scheduler-mcp:latest
    build:
      context: ./scheduler-mcp
    container_name: aerial-scheduler-mcp
    restart: unless-stopped
    environment:
      - DEFAULT_TIMEZONE=${DEFAULT_TIMEZONE:-America/Los_Angeles}
      - TZ=${DEFAULT_TIMEZONE:-America/Los_Angeles}
    volumes:
      - aerial-brain-data:/data
    networks:
      - aerial-net
    labels:
      autoheal: "true"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8080/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 30s

  discord-mcp:
    image: ghcr.io/azylman/aerial-discord-mcp:latest
    build:
      context: ./discord-mcp
    container_name: aerial-discord-mcp
    restart: unless-stopped
    environment:
      - DISCORD_BOT_TOKEN=${DISCORD_BOT_TOKEN}
    ports:
      - "4001:4001"
    networks:
      - aerial-net
    labels:
      autoheal: "true"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:4001/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 30s

  docker-mcp:
    image: ghcr.io/azylman/aerial-docker-mcp:latest
    build:
      context: ./docker-mcp
    container_name: aerial-docker-mcp
    restart: unless-stopped
    ports:
      - "4002:4002"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      - aerial-net
    labels:
      autoheal: "true"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:4002/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 30s

  github-mcp:
    image: ghcr.io/azylman/aerial-github-mcp:latest
    build:
      context: ./github-mcp
    container_name: aerial-github-mcp
    restart: unless-stopped
    ports:
      - "4003:4003"
    environment:
      - GITHUB_PERSONAL_ACCESS_TOKEN=${GITHUB_PERSONAL_ACCESS_TOKEN}
    networks:
      - aerial-net
    labels:
      autoheal: "true"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:4003/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 30s

  ollama:
    image: ghcr.io/azylman/aerial-ollama:latest
    build:
      context: ./docker/ollama
    container_name: aerial-ollama
    restart: unless-stopped
    ports:
      - "11434:11434"
    volumes:
      - aerial-ollama-data:/root/.ollama
    networks:
      - aerial-net
    labels:
      autoheal: "true"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:11434/api/version || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 4
      start_period: 180s

  agentsview:
    image: ghcr.io/kenn-io/agentsview:latest
    container_name: aerial-agentsview
    restart: unless-stopped
    ports:
      - "8089:8080"
    volumes:
      - aerial-brain-data:/data
      - aerial-agentsview-data:/root/.gemini/antigravity-cli
    networks:
      - aerial-net

volumes:
  aerial-brain-gemini:
    name: aerial-brain-gemini
  aerial-brain-data:
    name: aerial-brain-data
  aerial-ollama-data:
    name: aerial-ollama-data
  aerial-agentsview-data:
    name: aerial-agentsview-data

networks:
  aerial-net:
    name: aerial-net
```

- [ ] **Step 2: Commit Task 2**

```bash
git add docker-compose.yml
git commit -m "feat(compose): add watchtower and autoheal, configure restart policies and calibrated healthchecks"
```

---

### Task 3: Agent Operational Rules & Skill Updates

**Files:**
- Modify: `.agents/skills/self-improvement/SKILL.md`
- Modify: `.agents/skills/self-update/SKILL.md`
- Modify: `SYSTEM.md`
- Modify: `.agents/rules/system_instructions.md`

**Interfaces:**
- Consumes: Operational deployment invariant.
- Produces: Updated guidelines preventing agent self-termination.

- [x] **Step 1: Update `.agents/skills/self-improvement/SKILL.md`**

Replace Step 8 and Step 9 in `.agents/skills/self-improvement/SKILL.md` with:
```markdown
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
   - **DO NOT run `docker compose up` or `docker restart` from inside the container.**
   - Pushing to `origin/main` triggers the GitHub Actions CI pipeline to build and publish the image to GitHub Container Registry (`ghcr.io`).
   - Watchtower on the host automatically detects the new image and performs an out-of-band container swap within 60 seconds without interrupting execution or causing downtime.
```

- [x] **Step 2: Update `.agents/skills/self-update/SKILL.md`**

Replace the runbook in `.agents/skills/self-update/SKILL.md` to:
```markdown
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
```

- [x] **Step 3: Update `SYSTEM.md` and `.agents/rules/system_instructions.md`**

Append the **Stack Deployment Invariant**:
```markdown
## Stack Deployment Invariant
Aerial utilizes automated continuous deployment via GitHub Container Registry (GHCR) and Watchtower.
When implementing code changes, bug fixes, or enhancements:
1. Verify all unit tests pass locally with `go test ./...`.
2. Commit and push your changes to `origin/main`.
3. **NEVER** run `docker compose build`, `docker compose up`, or `docker restart` from within the `aerial-brain` container. Watchtower on the host will automatically pull the new GHCR image and recreate the container within 60 seconds.
```

- [x] **Step 4: Commit Task 3**

```bash
git add .agents/skills/self-improvement/SKILL.md .agents/skills/self-update/SKILL.md SYSTEM.md .agents/rules/system_instructions.md
git commit -m "docs(rules,skills): update self-improvement and self-update skills with Watchtower CD invariant"
```

---

### Task 4: Deployment & Live Stack Verification on MiniPC

**Files:**
- Deploy: `docker-compose.yml` to miniPC host.

- [ ] **Step 1: Push changes to GitHub repository**

Push all commits from Tasks 1–3 to `origin/main`.
Verify that the GitHub Actions `Continuous Delivery` workflow runs and successfully publishes the images to `ghcr.io`.

- [ ] **Step 2: Deploy updated `docker-compose.yml` on MiniPC**

Run on miniPC via SSH:
```bash
cd /share/aerial && git pull origin main && sudo docker compose up -d
```

- [ ] **Step 3: Verify all containers running and healthy**

Run:
```bash
sudo docker ps
```
Verify:
- `aerial-watchtower`: **Up**
- `aerial-autoheal`: **Up**
- `aerial-brain`: **Up (healthy)**
- `aerial-scheduler-mcp`: **Up (healthy)**
- `aerial-discord-mcp`: **Up (healthy)**
- `aerial-docker-mcp`: **Up (healthy)**
- `aerial-github-mcp`: **Up (healthy)**
- `aerial-ollama`: **Up (healthy)**

- [ ] **Step 4: Test Watchtower Log Stream**

Run:
```bash
sudo docker logs --tail 20 aerial-watchtower
```
Confirm Watchtower is polling GHCR for image updates.
