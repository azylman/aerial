# Aerial

An autonomous personal AI assistant system running natively on Docker, named after Gundam Aerial.

Aerial provides a multi-agent, tool-enabled AI assistant accessible via Discord and HTTP API, with persistent multi-turn PostgreSQL & pgvector memory, full-stack observability with VictoriaMetrics and Grafana, deep Prometheus telemetry instrumentation, declarative GitOps Docker Compose reconciliation, GitHub operations, host Docker infrastructure inspection, and an extensible architecture for custom skills, MCP tools, and sidecar containers.

---

## 1. System Architecture & Topology

Aerial uses a decoupled **Two-Repository Architecture**:
- **Engine Repo (`azylman/aerial`)**: Core Go backend (`aerial-brain`), MCP microservices, observability telemetry stack, and Docker Compose topology.
- **User Config Repo (e.g. `your-username/your-aerial-config`)**: Private user configuration (`config.yaml`), persona guidelines (`AGENTS.md`), channel rules (`channels/`), custom telemetry scrapes (`victoriametrics/`), and custom skills (`custom-skills/`). Starter template available at [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example).

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                               Discord Gateway                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │ Realtime Gateway Events & Mentions
                                       │ Continuous Typing Indicator Refresh
┌─────────────────────────────────────────────────────────────────────────────┐
│                                Aerial Brain                                 │
│  • In-process Discord Funnel & Gateway Worker (30m TTL / Dedup Recovery)    │
│  • Headless Antigravity Agent Engine (agy)                                  │
│  • Fast Ambient Relevance Classifier (Gemini 3.8 Flash Low)                 │
│  • Read-Only Kernel Mounts (/share/aerial-config:ro, /share/aerial:ro)      │
│  • Recursive File Watcher (fsnotify) with Hot-Reloading & LKGC Fallback     │
│  • PostgreSQL 16 Multi-Turn Thread Memory & Atomic CAS Task State           │
│  • Semantic Memory Native pgvector RAG (HNSW Cosine ops / 384-dim)          │
│  • Deep Prometheus Telemetry Instrumentation (:8080/metrics)                │
│  • Empty Response Suppression (Purged [NO_REPLY] Sentinel)                  │
└─────────────────────────────────────────────────────────────────────────────┘
               │                       │                      │
┌───────────────────────────┐ ┌───────────────────────┐ ┌─────────────────────┐
│       aerial-gitsync      │ │       docker-mcp      │ │     github-mcp      │
│  (Port 8080: Sidecar :rw) │ │  (Port 4002: In-Image)│ │(Port 4003: In-Image)│
│  • Declarative GitOps     │ └───────────────────────┘ └─────────────────────┘
│    Compose Reconciler     │          │                      │
│  • Singleflight Git Sync  │  Host Docker Socket       GitHub API (PAT)
│  • Prometheus (:8080)     │      (/var/run/docker.sock)
└───────────────────────────┘          │                      │
               │                       │                      │
┌───────────────────────────┐ ┌───────────────────────┐ ┌─────────────────────┐
│       scheduler-mcp       │ │     aerial-ollama     │ │     discord-mcp     │
│  (Port 8080: PostgreSQL)  │ │ (Port 11434: Embed)   │ │ (Port 4001: Outbound│
└───────────────────────────┘ └───────────────────────┘ └─────────────────────┘
               │                       │                      │
┌───────────────────────────┐ ┌───────────────────────┐ ┌─────────────────────┐
│      aerial-postgres      │ │   aerial-watchtower   │ │    aerial-proxy     │
│   (Port 5432: pgvector)   │ │ (GHCR CD Supervisor)  │ │ (Port 8089: Edge)   │
└───────────────────────────┘ └───────────────────────┘ └──────────┬──────────┘
               │                       │                           │
┌──────────────────────────────────────────────────────────┐       ├─ / -> 302 Redirect to /dashboard/
│           Full Observability & Telemetry Stack           │       ├─ /dashboard/ -> Dashboard HUD
│  • cAdvisor (Container Metrics :8080)                    │       ├─ /docs/ -> Docsify Living Docs
│  • Node Exporter (Host System Metrics :9100)             │       ├─ /conversations/ -> Agentsview
│  • PostgreSQL Exporter (Database Pool & Stats :9187)     │       └─ /grafana/ -> Grafana Telemetry
│  • VictoriaMetrics TSDB (:8428 with modular scrape.d/)   │
│  • Grafana Cyberpunk Dashboards (:3000 / Postgres store) │
│  • aerial-autoheal (Container Healthcheck Supervisor)    │
└──────────────────────────────────────────────────────────┘
```

---

## 2. Extensibility Guide

Aerial is designed to be easily extended across four distinct layers:

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  4 WAYS TO EXTEND AERIAL                                    │
├─────────────────────────────┬──────────────────────────┬────────────────────┬───────────────┤
│ 1. CONFIG & PERSONA         │ 2. CUSTOM SKILLS         │ 3. MCP SERVERS     │ 4. CONTAINERS │
│ config.yaml & AGENTS.md     │ Runbooks in config repo  │ Connecting APIs    │ Extra sidecars│
└─────────────────────────────┴──────────────────────────┴────────────────────┴───────────────┘
```

---

### Layer 1: Configuration & Persona (`config.yaml` & `AGENTS.md`)

User configuration and persona rules live in your private configuration repository (see [aerial-config-example](https://github.com/azylman/aerial-config-example)):

1. **`config.yaml`** (Agent Options, Channel Policies, & MCP Tools):
   ```yaml
   model: "Gemini 3.7 Flash (High)"
   timeout_minutes: 15
   timezone: "America/Los_Angeles"
   system_channel: "aerial-dev"

   # Administrator Allowlist
   # Numeric Discord Snowflake IDs authorized to perform system-level operations
   admin_users:
     - "123456789012345678"

   # Channel Policies & Interaction Modes
   channels:
     # Default fallback (Required)
     default:
       mode: "threads"           # "threads" | "channel" | "ignore"
       typing_indicator: "always" # "always" | "on_mention" | "never"
       ignore_bots: true
       ambient_wake_threshold: 0.80

     # In-Channel Direct Interaction with Classifier (wake_mode: classifier)
     general:
       mode: "channel"
       wake_mode: "classifier"   # "classifier" | "mention" | "all"
       typing_indicator: "on_mention"
       ignore_bots: true
       ambient_wake_threshold: 0.80
       ambient_wake_prompt: "Determine whether the target message is relevant to Aerial and warrants Aerial waking up and responding, based on the recent channel context."
       max_session_turns: 50

     # Mention-Only Channel (listens ambiently, responds ONLY on explicit @mention or direct reply)
     lounge:
       mode: "channel"
       wake_mode: "mention"
       typing_indicator: "on_mention"
       ignore_bots: false
       max_session_turns: 50

     # Custom ambient prompt per channel
     dev-alerts:
       mode: "channel"
       wake_mode: "classifier"
       typing_indicator: "on_mention"
       ignore_bots: false # Allow bot alerts
       ambient_wake_threshold: 0.70
       ambient_wake_prompt: "Only wake up and respond if the user is asking about Kubernetes deployments, CI/CD pipeline failures, or production outages."

     # Ignore specific noisy channels
     memes:
       mode: "ignore"

   git_sync:
     enabled: true
     interval: "60s"
     config_repo_url: "https://github.com/your-username/your-aerial-config.git"
     repositories:
       - "/share/aerial-config"
       - "/share/aerial"

   memory:
     fact_extraction:
       enabled: true
       interval: "6h"
   ```
   - **Interaction Modes (`mode`)**:
     - `threads`: Direct messages or mentions spawn and route to a Discord thread (default).
     - `channel`: Messages are evaluated directly in-channel without spawning threads.
     - `ignore` (or `disabled`): Channel is completely ignored (no messages evaluated, no startup sweeps).
   - **Wake Sensitivity Modes (`wake_mode`)**:
     - `mention` (or `mentions`, `direct`): Aerial responds strictly to explicit user pings (`@Aerial`) and direct replies. Keyword triggers and LLM classification are bypassed with zero token cost. Crucially, ambient channel chatter is silently appended into `transcript.jsonl` so Aerial retains complete conversational lookback when subsequently pinged.
     - `classifier` (or `ambient`): Tier 1 wakes on direct mentions, replies, or keywords (`aerial`, `gundam`); Tier 2 ambient messages are scored (0.0 to 1.0) by `Gemini 3.8 Flash (Low)` against `ambient_wake_prompt` using recent channel context.
     - `all` (or `always`): Responds to every incoming message (default inside active threads).
   - **Typing Indicator Dynamics (`typing_indicator`)**:
     - `always`: Sends typing indicator on every evaluated turn (default for `threads`).
     - `on_mention`: Sends typing indicator only when the bot is directly mentioned or replied to (default for `channel`). Prevents channel chatter distraction during ambient evaluation.
     - `never`: Completely suppresses typing indicators.
   - **Empty Response Suppression**: Agent turns with empty or whitespace-only stdout silently skip Discord message delivery without error, eliminating fragile `[NO_REPLY]` prompt sentinels.
   - **Server Whitelisting (Default-Deny)**: Set `channels.default.mode: "ignore"` to ignore the entire server by default, responding only in explicitly declared channels.
   - **Hot-Reloading & LKGC**: Changes to `config.yaml` are detected instantly via `fsnotify` and reconfigured in-memory without restarting the daemon. If invalid YAML is saved, Aerial retains the **Last Known Good Configuration (LKGC)** in memory and posts a diagnostic alert to `#aerial-dev`.

2. **`AGENTS.md`** (Persona & Tone Overrides):
   Define custom persona rules, tone guidelines, or private operational context. Instructions in `AGENTS.md` take priority over base `GEMINI.md` rules.

3. **Per-Channel Instructions (`channels/<channel-name>.md`)**:
   Define dedicated guidelines and operational personas tailored to specific Discord channels:
   - **Convention Auto-Discovery**: Place Markdown files in `channels/<channel-name>.md` within your configuration repository (e.g., `/share/aerial-config/channels/general.md` or `/share/aerial-config/channels/lounge.md`). Aerial automatically discovers and injects them dynamically without requiring explicit path configuration in `config.yaml`.
   - **Thread Inheritance**: Conversations in Discord threads automatically resolve and apply instructions from their parent channel (`channels/<parent-channel-name>.md`).
   - **Normalized Lookups**: Channel names are normalized case-insensitively, strip leading `#`, and interoperate between spaces and hyphens (e.g., `#Dev Chat` resolves `dev-chat.md` or `dev chat.md`).
   - **Prompt Injection & Safety**: Instructions are framed inside `<CHANNEL_INSTRUCTIONS>` prior to `<USER_REQUEST>`, escaped against XML delimiter breakouts, capped at 64KB, and defended against directory traversal.

---

### Layer 2: Adding Custom Skills

Skills use **Progressive Disclosure**—Aerial only loads skill titles and descriptions into context, reading full runbooks on-demand when relevant.

#### A. User Custom Skills (`custom-skills/` in your config repo)
Place custom skill directories inside `custom-skills/` in your private configuration repository:
```text
custom-skills/
└── weather-alerts/
    └── SKILL.md
```
`aerial-brain` automatically discovers custom skills in `/share/aerial-config/custom-skills` and symlinks them into `/root/.gemini/skills/` with highest priority. Orphaned or dead symlinks are automatically swept when skills are renamed or removed.

#### B. Built-in Skills (`azylman/aerial/.agents/skills/`)
Core system skills (such as `self-improvement` and `self-update`) are baked into the `brain` image during build.

#### Skill File Structure (`SKILL.md`)
```markdown
---
name: weather-alerts
description: "Check regional weather forecasts and send alert summaries via Discord."
---

# Weather Alerts Runbook

## Steps
1. Query weather API using MCP tools.
2. Format forecast summary.
```

---

### Layer 3: Adding Custom MCP Servers

Aerial connects to external Model Context Protocol (MCP) servers over Streamable HTTP / SSE:

#### Built-in Tool Autodiscovery
By default, Aerial automatically mounts:
- **`discord`** (`http://discord-mcp:4001/mcp`)
- **`docker`** (`http://docker-mcp:4002/mcp` - Native in-image execution over host `/var/run/docker.sock`)
- **`github`** (`http://github-mcp:4003/mcp` - Native in-image execution with PAT auth)
- **`scheduler`** (`http://scheduler-mcp:8080/mcp` - PostgreSQL-backed cron & reminder manager)

#### Custom MCP Servers (`config.yaml`)
Define additional MCP tools directly in your `config.yaml`:
```yaml
mcp_servers:
  brave-search:
    serverUrl: "http://brave-mcp:4005/mcp"
  custom-remote-api:
    serverUrl: "https://mcp.example.com/mcp"
    headers:
      Authorization: "Bearer ${CUSTOM_API_KEY}"
```
Environment variables `${VAR}` are interpolated dynamically at runtime from your host `.env`.

---

### Layer 4: Adding Custom Containers (`docker-compose.override.yml`)

You can add extra services or MCP containers to the `aerial-net` bridge network by placing `docker-compose.override.yml` in your private configuration repository. Docker Compose natively includes and merges this file on the host via the top-level `include:` directive in `docker-compose.yml`:

```yaml
services:
  brave-mcp:
    image: mcp/brave-search
    container_name: aerial-brave-mcp
    restart: unless-stopped
    environment:
      - BRAVE_API_KEY=${BRAVE_API_KEY}
    networks:
      - aerial-net
```

---

## 3. Observability & Telemetry Stack

Aerial includes an enterprise-grade, out-of-the-box observability matrix with single-node VictoriaMetrics TSDB, PostgreSQL-backed Grafana dashboards, deep Go Prometheus instrumentation, and dynamic modular scrape configurations.

```text
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                                 TELEMETRY PIPELINE                                    │
├────────────────────────────┬─────────────────────────────┬────────────────────────────┤
│ METRIC EXPORTERS           │ TIME-SERIES TSDB            │ VISUALIZATION & HUD        │
│ • aerial-brain (:8080)     │                             │                            │
│ • aerial-gitsync (:8080)   │ aerial-victoriametrics      │ aerial-grafana (:3000)     │
│ • postgres-exporter (:9187)│ (:8428)                     │ • Anonymous Admin HUD      │
│ • cadvisor (:8080)         │ • 5-Year Retention          │ • Cyberpunk Permet Theme   │
│ • node-exporter (:9100)    │ • Dynamic 15s Scrape Engine │ • Core Telemetry Dashboard │
│ • Modular scrape.d/*.yml   │ • Token Interpolation       │ • Postgres & Docker Views  │
└────────────────────────────┴─────────────────────────────┴────────────────────────────┘
```

### 1. VictoriaMetrics TSDB (`aerial-victoriametrics`)
- **Engine**: Single-node VictoriaMetrics (`v1.101.0`) running with 5-year retention (`-retentionPeriod=5y`) and 15s scrape interval.
- **Dynamic Modular Scrapes**: VictoriaMetrics automatically discovers and live-reloads scrape configurations mounted from `/share/aerial-config/victoriametrics/*.yml` every 15 seconds without container restarts (`-promscrape.configCheckInterval=15s`).
- **Token Interpolation**: Automatically expands environment variables (e.g. `%{HA_METRICS_TOKEN}`) in custom scrape configs.

### 2. Pre-Provisioned Grafana Dashboards (`http://localhost:8089/grafana/`)
- **Single-Click Anonymous Admin Access**: Instant dashboard access without login friction.
- **Persistent Backend**: Dashboards and user settings persist directly in PostgreSQL 16 (`GF_DATABASE_TYPE=postgres`).
- **Pre-Provisioned Dashboard Suite**:
  - **`⚡ Aerial Brain & GitSync Operations` (`core-telemetry.json`)**: Live turn execution latency (p50/p90/p95/p99), token usage, active worker pool depth, CAS task states, runner error taxonomy, Discord gateway ping, classifier triage decisions, Ollama vector search durations, and GitSync reconcile runs.
  - **`🐘 PostgreSQL Overview` (`postgres-overview.json`)**: Active backends, connection pool state, buffer cache hit ratio (>99%), commits/rollbacks, tuple read/write velocity, and lock contention.
  - **`🐳 Docker Containers Overview` (`docker-overview.json`)**: Per-container CPU %, working set memory curves, network RX/TX, and CFS CPU throttling periods.
  - **`🖥️ Host System & Hardware Overview` (`host-system-overview.json`)**: Host CPU load breakdown, thermal sensors per core, RAM utilization, root disk space, and load averages.

### 3. Deep Go Prometheus Metrics Instrumentation
- **`aerial-brain`** (Exposed on internal `:8080/metrics` / host `:8088/metrics`):
  - Queue & Workers: `aerial_brain_active_workers`, `aerial_brain_queue_depth`, `aerial_brain_interrupted_turns_recovered_total`.
  - Turns & Runner: `aerial_brain_turns_total`, `aerial_brain_turn_duration_seconds`, `aerial_brain_runner_executions_total`, `aerial_brain_runner_duration_seconds`, `aerial_brain_runner_errors_total`.
  - Classifier & Funnel: `aerial_brain_classifier_duration_seconds`, `aerial_brain_classifier_decisions_total`, `aerial_brain_classifier_confidence_score`, `aerial_brain_discord_events_total`, `aerial_brain_discord_gateway_latency_seconds`.
  - Memory & Embeddings: `aerial_brain_memory_operations_total`, `aerial_brain_memory_search_duration_seconds`, `aerial_brain_embeddings_generated_total`, `aerial_brain_facts_extracted_total`.
  - Database Connection Pool: `aerial_brain_db_query_duration_seconds`, `aerial_brain_db_open_connections`, `aerial_brain_db_in_use_connections`, `aerial_brain_db_idle_connections`, `aerial_brain_db_wait_count_total`.
- **`aerial-gitsync`** (Exposed on internal `:8080/metrics`):
  - `aerial_gitsync_pulls_total`, `aerial_gitsync_pull_duration_seconds`, `aerial_gitsync_sync_requests_total`, `aerial_gitsync_reconciliations_total`, `aerial_gitsync_compose_duration_seconds`, `aerial_gitsync_last_sync_timestamp_seconds`.

---

## 4. Continuous Deployment, GitOps & Self-Improvement

Aerial uses an automated GitOps deployment and configuration pipeline:
1. **GitHub Actions Matrix Builds**: Triggers dynamic matrix builds only for modified microservices and publishes them to GitHub Container Registry (`ghcr.io/azylman/aerial-*`).
2. **Watchtower Supervisor**: Polls GHCR every 60 seconds out-of-band and performs zero-downtime rolling container updates across core services.
3. **Dedicated GitSync Sidecar (`aerial-gitsync`)**: A dedicated background daemon holding read-write mounts on `/share/aerial-config` and `/share/aerial`, pulling updates via singleflight fast-forward syncs every 60s and exposing a `POST /sync` trigger.
4. **Declarative GitOps Compose Reconciler**: Whenever repository updates are synchronized (periodically or via webhook), `aerial-gitsync` automatically pre-flight validates and executes `docker compose up -d` with strict timeouts and token sanitization, reconciling any topology changes declaratively without manual container intervention.
5. **Physical Immutability & Two-Phase PR Workflow**: The `aerial-brain` execution container mounts repositories strictly **read-only (`:ro`)**. When making configuration, persona, or skill adjustments, Aerial uses `scripts/aerial-config-pr.sh` to clone into an ephemeral `/dev/shm` scratch directory, run pre-flight syntax checks, push a branch, open a GitHub PR, verify CI, squash merge into `main`, and trigger fast-path sync via `aerial-gitsync`.
6. **Core Engine Self-Improvement**: For changes to the Go monorepo (`azylman/aerial`), Aerial uses `.agents/skills/self-improvement/SKILL.md` to run local tests and static analysis (`./scripts/verify.sh`), commit, push a branch, and open a PR with required CI status checks.

---

## 5. Component Modules

| Service | Port | Description |
| :--- | :--- | :--- |
| **`aerial-postgres`** | `5432` (Host `127.0.0.1:5432`) | PostgreSQL 16 relational database with `pgvector` extension for state, CAS task queues, vector memory, and Grafana storage. |
| **`aerial-brain`** | `8080` (Host `8088`) | Go execution daemon running `agy`, PostgreSQL memory, Discord funnel, Prometheus metrics (`:8080/metrics`), and file watcher. Mounted `:ro`. |
| **`aerial-gitsync`** | `8080` (Internal) | Dedicated GitSync sidecar daemon managing automated repository synchronization, `/sync` webhooks, Prometheus metrics, and declarative GitOps compose reconciliation. Mounted `:rw`. |
| **`aerial-scheduler-mcp`**| `8080` (Internal) | PostgreSQL-backed cron and one-shot reminder management server over HTTP MCP. |
| **`aerial-discord-mcp`** | `4001` (Host `4001`) | Outbound MCP server providing Discord messaging, thread creation, and channel tools. |
| **`aerial-docker-mcp`** | `4002` (Host `4002`) | Native in-image Docker MCP stdio server with `supergateway` translation proxy over `/var/run/docker.sock`. |
| **`aerial-github-mcp`** | `4003` (Host `4003`) | Native in-image GitHub MCP stdio server with `supergateway` translation proxy and PAT authentication. |
| **`aerial-ollama`** | `11434` (Host `11434`) | Local LLM and embedding server for vector memory retrieval (`all-minilm:latest` / 384-dim). |
| **`aerial-agentsview`** | `8080` (via proxy) | Web UI for visualizing agent transcripts, session history, and execution timelines. |
| **`aerial-dashboard`** | `8080` (via proxy) | Status microservice serving Cyberpunk status HUD. |
| **`aerial-docs`** | `80` (via proxy) | Documentation engine rendering Markdown and Mermaid diagrams from config repo via Docsify. |
| **`aerial-proxy`** | `8089` (Host `8089`) | Edge reverse proxy routing traffic for `/` (302 to HUD), `/dashboard/`, `/docs/`, `/conversations/`, and `/grafana/`. |
| **`aerial-cadvisor`** | `8080` (Internal) | cAdvisor container metrics collector gathering per-container CPU, memory, network, and disk telemetry. |
| **`aerial-node-exporter`**| `9100` (Internal) | Node Exporter host telemetry gathering host CPU loads, memory, storage, thermals, and network metrics. |
| **`aerial-postgres-exporter`**| `9187` (Internal) | PostgreSQL database metrics exporter gathering connection pools, locks, query stats, and buffer metrics. |
| **`aerial-victoriametrics`**| `8428` (Internal) | VictoriaMetrics single-node TSDB scraping Prometheus metrics from all exporters with 5-year retention and dynamic `scrape.d/` config. |
| **`aerial-grafana`** | `3000` (via proxy) | Grafana visual dashboards serving system HUD & container metrics with PostgreSQL persistent backend and pre-provisioned dashboards. |
| **`aerial-watchtower`** | - | Out-of-band continuous deployment supervisor polling GHCR every 60 seconds. |
| **`aerial-autoheal`** | - | Health supervisor probing container healthchecks every 15s and auto-restarting unhealthy containers. |

---

## 6. Quickstart Setup

### Prerequisites
- Docker Engine 24+ & Docker Compose v2+
- Google Account for OAuth authentication (recommended to avoid API key rate limits and 503 overload errors) OR Gemini API Key (from [Google AI Studio](https://aistudio.google.com/))
- Discord Bot Token (with Message Content and Server Members intents enabled)
- GitHub Personal Access Token (for private configuration repository synchronization)

### Step 1: Create Your Private Configuration Repository
1. Create a private repository on GitHub (e.g. `your-username/my-aerial-config`).
2. Copy or fork the template files from [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example) into your private repository.
3. Customize `config.yaml` and `AGENTS.md` as desired.

### Step 2: Clone Aerial Engine
```bash
git clone https://github.com/azylman/aerial.git
cd aerial
```

### Step 3: Configure Environment Variables
```bash
cp .env.example .env
```
Edit `.env` and configure your credentials and config repo URL:
```ini
# Recommended: Leave GEMINI_API_KEY commented out for Google OAuth authentication!
# GEMINI_API_KEY=your_gemini_api_key_here

DISCORD_BOT_TOKEN=your_discord_bot_token_here
GITHUB_PAT=your_github_personal_access_token_here

# Private Configuration Repository URL
AERIAL_CONFIG_REPO_URL=https://github.com/your-username/my-aerial-config.git
```

### Step 4: Launch Stack
```bash
docker compose up -d
```
On boot, `aerial-brain` and `aerial-gitsync` will automatically adopt or clone your private repository into `/share/aerial-config` using `GITHUB_PAT` and load your `config.yaml` settings.

### Step 5: Authenticate via Google OAuth (Recommended)
If using OAuth (with `GEMINI_API_KEY` commented out):
1. Run the interactive `agy` CLI inside the running container:
   ```bash
   docker exec -it aerial-brain agy
   ```
2. Copy the displayed Google OAuth login URL into your web browser and sign in.
3. Paste the authorization code back into the terminal prompt and hit Enter.
4. Press `Ctrl+C` to exit once authenticated. Aerial will store the OAuth session token in `/data` and automatically refresh access tokens in the background!

### Step 6: Verify Health
```bash
docker compose ps
docker compose logs -f brain
```

---

## 7. Operational Commands & Endpoints

| Action / Service | URL / Command |
| :--- | :--- |
| Status Dashboard (HUD) | `http://localhost:8089/dashboard/` (or `http://localhost:8089/`) |
| Documentation (Docsify) | `http://localhost:8089/docs/` |
| Agent Transcripts (Agentsview) | `http://localhost:8089/conversations/` |
| System Telemetry (Grafana) | `http://localhost:8089/grafana/` |
| Brain Prometheus Metrics | `http://localhost:8088/metrics` |
| Brain Healthcheck | `http://localhost:8088/health` |
| Start all services | `docker compose up -d` |
| Stop all services | `docker compose down` |
| View live logs | `docker compose logs -f` |
| Trigger GitSync & GitOps Reconcile | `docker exec aerial-brain curl -s -X POST http://aerial-gitsync:8080/sync` |
| Restart single service | `docker compose restart brain` |
| Update images & rebuild | `docker compose build && docker compose up -d` |

---

## 8. Security & Best Practices

- **Multi-User Security & Admin Privilege Enforcement**: In shared or multi-user channels, messages from users are automatically checked against `admin_users` in `config.yaml`. Only authorized admins (`is_admin: true`) can modify system instructions (`GEMINI.md`, `AGENTS.md`), edit system configuration (`config.yaml`), manage Docker containers, or alter cron schedules.
- **Fail-Closed Default-Deny Server Containment**: Set `channels.default.mode: "ignore"` to contain Aerial exclusively to allowlisted channels on shared Discord servers.
- **Discord Funnel Hardening**: Thread ID deduplication recovery resolves Discord error 160004 race conditions seamlessly, and message staleness TTL is set to 30 minutes to prevent dropped messages during deployment bursts.
- **Zero Plaintext Tokens**: GitHub PATs and database secrets are passed in-memory ephemerally and never written to `.git/config` on disk.
- **Automated Token Redaction**: All subprocess logs, errors, and GitOps reconcile streams pass through multi-pattern token sanitizers to redact sensitive credentials.
- **Never commit `.env`**: Secrets and tokens are strictly ignored by `.gitignore`.
- **Restricted File Permissions**: Run `chmod 600 .env` on the host to protect credentials.
- **Isolated Bridge Network**: All container-to-container traffic operates on the private `aerial-net` bridge network.
