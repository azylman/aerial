# Container Health Monitoring & Automated Deployment Specification

## 1. Problem Statement & Background

### 1.1 Context
Aerial runs as a set of containerized services on a miniPC host (Home Assistant OS / Docker). The core orchestrator (`aerial-brain`) is an autonomous agent daemon that frequently modifies its own repository, dependencies, and configuration.

### 1.2 The Failure Mode (Root Cause)
Previously, Aerial's built-in skills (`.agents/skills/self-improvement/SKILL.md` and `.agents/skills/self-update/SKILL.md`) instructed the agent to apply code updates by executing:
```bash
(sleep 2 && docker compose -f /share/aerial/docker-compose.yml up -d --no-build brain) &
```
directly from *inside* its own running container via a mounted `/var/run/docker.sock`. 

When Docker Compose reached the stage of recreating `aerial-brain`:
1. It sent `SIGTERM` to the active `aerial-brain` container.
2. Stopping the container immediately terminated the background subshell and child `docker compose` process executing inside it.
3. The termination occurred **before** Docker could issue the `docker start` commands for the newly created replacement container.
4. The entire stack was left in an unstarted `Created` state, taking the Discord bot and all background automation offline until manual SSH intervention.

### 1.3 Goals & Success Criteria
- **Zero In-Container Rebuilds**: `aerial-brain` must never invoke `docker compose` on itself or its siblings.
- **Off-the-Shelf Architecture**: Utilize standard, established open-source tools with zero custom deployment scripts or bespoke orchestration code.
- **Continuous Deployment**: Pushing code to `main` on GitHub triggers an automated CI build in the cloud (GHCR), which is automatically pulled and applied on the miniPC within 60 seconds.
- **Automated Self-Healing**: Containers that crash are instantly resurrected by the Docker Engine (`restart: unless-stopped`), and containers that hang/deadlock are automatically detected and restarted by `autoheal`.
- **Zero Downtime on Broken Builds**: Broken builds fail in CI on GitHub and never get tagged/pushed to GHCR, ensuring the production miniPC continues running the last known good (LKG) release uninterrupted.

---

## 2. Architecture & Component Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. CLOUD CI/CD BUILD PLANE (GitHub Actions)                                 │
│                                                                             │
│  Git Push to `main` ──► GitHub Actions Workflow (.github/workflows/deploy.yml)
│                          ├─► Runs unit tests (`go test ./...`)              │
│                          ├─► Builds multi-stage Docker images               │
│                          └─► Publishes to GitHub Container Registry (GHCR)  │
│                              • ghcr.io/azylman/aerial-brain:latest          │
│                              • ghcr.io/azylman/aerial-scheduler-mcp:latest  │
│                              • ghcr.io/azylman/aerial-discord-mcp:latest    │
│                              • ghcr.io/azylman/aerial-docker-mcp:latest     │
│                              • ghcr.io/azylman/aerial-github-mcp:latest     │
│                              • ghcr.io/azylman/aerial-ollama:latest         │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼ Pulls pre-built image layers
┌─────────────────────────────────────────────────────────────────────────────┐
│ 2. MINIPC PRODUCTION STACK (docker-compose.yml)                             │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ SUPERVISION & LIFECYCLE (Off-the-Shelf)                               │  │
│  │                                                                       │  │
│  │ [containrrr/watchtower] ──► Checks GHCR every 60s via Docker API      │  │
│  │                             - Gracefully updates running containers   │  │
│  │                             - Automatically cleans old image layers   │  │
│  │                             - Ignores self (WATCHTOWER_INCLUDE_SELF=0)│  │
│  │                                                                       │  │
│  │ [willfarrell/autoheal]  ──► Polls Docker HEALTHCHECK every 15s        │  │
│  │                             - Restarts unhealthy containers (opt-in)  │  │
│  │                             - Respects calibrated start periods       │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                      │                                      │
│                                      ▼ Supervises                           │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ APPLICATION WORKLOADS                                                 │  │
│  │                                                                       │  │
│  │ [aerial-brain]          • restart: unless-stopped                     │  │
│  │                         • stop_grace_period: 60s                      │  │
│  │                         • Healthcheck: start_period 60s, interval 30s │  │
│  │                                                                       │  │
│  │ [aerial-ollama]         • restart: unless-stopped                     │  │
│  │                         • Healthcheck: start_period 180s, interval 30s│  │
│  │                                                                       │  │
│  │ [aerial-*-mcp servers]  • restart: unless-stopped                     │  │
│  │                         • Healthcheck: start_period 30s, interval 15s │  │
│  │                                                                       │  │
│  │ [aerial-agentsview]     • restart: unless-stopped                     │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Detailed Component Specifications

### 3.1 GitHub Actions Workflow (`.github/workflows/deploy.yml`)
- **Trigger**: `push` to `main` branch.
- **Permissions**: `packages: write`, `contents: read`.
- **Jobs**:
  1. **`test`**: Runs Go and Node unit test suites. If any test fails, image building is aborted.
  2. **`build-and-push`**:
     - Uses `docker/setup-buildx-action` and `docker/login-action` against `ghcr.io`.
     - Builds each service with Docker layer caching (`type=gha`).
     - Tags and pushes `ghcr.io/azylman/aerial-<service>:latest` and `ghcr.io/azylman/aerial-<service>:${{ github.sha }}`.

### 3.2 Watchtower Deployment Supervisor (`containrrr/watchtower`)
- **Service Name**: `aerial-watchtower`
- **Image**: `containrrr/watchtower:latest`
- **Restart Policy**: `restart: unless-stopped`
- **Volumes**:
  - `/var/run/docker.sock:/var/run/docker.sock`
  - `/root/.docker/config.json:/config.json:ro` (for GHCR authentication if private)
- **Environment Variables**:
  - `WATCHTOWER_POLL_INTERVAL=60`: Polling frequency in seconds.
  - `WATCHTOWER_CLEANUP=true`: Automatically prune old dangling images after replacement.
  - `WATCHTOWER_INCLUDE_SELF=false`: Watchtower never attempts to stop or replace itself.
  - `WATCHTOWER_ROLLING_RESTART=true`: Restarts containers sequentially rather than all at once.

### 3.3 Autoheal Health Watchdog (`willfarrell/autoheal`)
- **Service Name**: `aerial-autoheal`
- **Image**: `willfarrell/autoheal:latest`
- **Restart Policy**: `restart: unless-stopped`
- **Volumes**:
  - `/var/run/docker.sock:/var/run/docker.sock:ro`
- **Environment Variables**:
  - `AUTOHEAL_CONTAINER_LABEL=autoheal`: Evaluates only containers explicitly labeled `autoheal: "true"`.
  - `AUTOHEAL_INTERVAL=15`: Health check inspection interval (seconds).
  - `AUTOHEAL_START_PERIOD=0`: Defers startup grace period to individual container definitions.
  - `AUTOHEAL_DEFAULT_STOP_TIMEOUT=20`: Graceful stop window before SIGKILL.

### 3.4 Container Healthchecks & Tuning

| Container | Healthcheck Command | Interval | Timeout | Retries | Start Period | Stop Grace Period |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`aerial-brain`** | `curl -fsS http://localhost:8080/health \|\| exit 1` | `30s` | `10s` | `5` (150s tolerance) | `60s` | `60s` |
| **`aerial-ollama`** | `curl -fsS http://localhost:11434/api/version \|\| exit 1` | `30s` | `10s` | `4` | `180s` | `30s` |
| **`aerial-scheduler-mcp`** | `curl -fsS http://localhost:8080/health \|\| exit 1` | `15s` | `5s` | `3` | `30s` | `10s` |
| **`aerial-discord-mcp`** | `curl -fsS http://localhost:4001/health \|\| exit 1` | `15s` | `5s` | `3` | `30s` | `10s` |
| **`aerial-docker-mcp`** | `curl -fsS http://localhost:4002/health \|\| exit 1` | `15s` | `5s` | `3` | `30s` | `10s` |
| **`aerial-github-mcp`** | `curl -fsS http://localhost:4003/health \|\| exit 1` | `15s` | `5s` | `3` | `30s` | `10s` |

---

## 4. Agent Operational Rules & Skill Updates

Update all operational documentation and built-in agent skills to eliminate in-container `docker compose` execution:

1. **`.agents/skills/self-improvement/SKILL.md`**:
   - Replace Step 9 with the GHCR + Watchtower continuous delivery workflow.
   - Remove all instructions referencing `docker compose up -d brain` or `(sleep 2 && docker compose up)`.
2. **`.agents/skills/self-update/SKILL.md`**:
   - Update runbook: Self-update consists solely of verifying tests and pushing commits to `main`. Watchtower executes the container swap.
3. **`SYSTEM.md` & `.agents/rules/system_instructions.md`**:
   - Add the strict **Stack Deployment Invariant**:
     > **Stack Deployment Invariant**:
     > Aerial utilizes automated continuous deployment via GitHub Container Registry (GHCR) and Watchtower.
     > When implementing code changes, bug fixes, or enhancements:
     > 1. Verify all unit tests pass locally with `go test ./...`.
     > 2. Commit and push your changes to `origin/main`.
     > 3. **NEVER** run `docker compose build`, `docker compose up`, or `docker restart` from within the `aerial-brain` container. Watchtower on the host will automatically pull the new GHCR image and recreate the container within 60 seconds.

---

## 5. Verification & Test Plan

### 5.1 Automated CI Verification
- Push commit with passing unit tests -> Verify GitHub Actions builds and pushes all tagged images to `ghcr.io/azylman/aerial-*`.
- Push intentional test failure -> Verify GitHub Actions halts immediately and does **not** push images to GHCR.

### 5.2 Live Deployment Verification
1. Verify `aerial-watchtower` and `aerial-autoheal` are running and healthy on the miniPC.
2. Push a test commit updating a non-breaking version string or log output in `aerial-brain`.
3. Monitor Watchtower logs (`docker logs -f aerial-watchtower`):
   - Confirm Watchtower detects new image on GHCR.
   - Confirm Watchtower stops the old container, creates the new container, and starts it.
   - Confirm old dangling image layers are automatically pruned.
4. Verify Discord Gateway reconnects cleanly and recovers SQLite message state.

### 5.3 Health & Recovery Verification
1. Simulate a container crash: Run `docker kill --signal=SIGKILL aerial-scheduler-mcp` -> Confirm Docker daemon resurrects it immediately (`restart: unless-stopped`).
2. Simulate a container freeze: Block the health endpoint or pause the process -> Confirm `aerial-autoheal` detects `unhealthy` state and triggers a graceful restart.
