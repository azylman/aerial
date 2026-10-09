# Aerial

An autonomous AI agent and configurable homelab operations platform engineered for heterogeneous multi-node clusters running HashiCorp Nomad.

Aerial is built from the ground up around end-to-end configurability across both the agent and the platform. While Aerial herself is tailored through pluggable execution backends (such as `agy-cli` or Gemini CLI), domain-specific skills, and modular persona rules, the underlying platform is equally extensible—orchestrating user-defined containers, declarative Nomad sidecars, and persistent scheduled pipelines. While interfacing with her through Discord, Aerial actively commands compute across storage servers, local GPU inference nodes, and touch kiosks—executing complex multi-turn workflows, authoring and deploying her own code, and operating your homelab fleet.

**Observability, CI/CD, reliability, and extensibility are first-class invariants.** Deployments are fully automated through event-driven push GitOps that continuously reconciles Nomad jobs and proactively self-heals failing CI builds or deployment rollbacks, backed by a turnkey telemetry matrix featuring VictoriaMetrics TSDB, Vector log streaming to OpenObserve, and pre-provisioned Grafana dashboards. The system guarantees operational resilience through dynamic CoreDNS service discovery, centralized secret governance via Infisical, read-only container mounts, and Last Known Good Configuration (LKGC) fallbacks, while staying fully extensible via custom skills, declarative Nomad sidecar jobs, and Streamable HTTP MCP microservices.

---

## Tech Stack

Built in **Go (1.27)** and orchestrated as a resilient bare-metal homelab mesh:

- **Cluster Orchestration**: **HashiCorp Nomad** — Heterogeneous bare-metal cluster managing distributed daemons, batch tasks, and GPU workloads.
- **Service Discovery & Routing**: **CoreDNS** (internal mesh DNS) + **Nginx** (reverse proxy & edge ingress).
- **Persistence & Vector Memory**: **PostgreSQL 18 + pgvector** — Multi-turn conversation state, relational persistence, and hybrid RRF search (dense HNSW embeddings + sparse FTS).
- **Schema Migrations**: **Ariga Atlas** — Declarative schema migrations, automated diffing, and checksum validation via Nomad batch jobs.
- **Queuing & Idempotent Ingestion**: **River** — Transactional, PostgreSQL-backed Go job queue powering strictly idempotent webhook processing and event dispatch.
- **Secrets Management**: **Infisical + Redis** — Centralized secret governance with dynamic runtime injection and zero plaintext disk persistence.
- **Observability & Telemetry**:
  - **Metrics**: **VictoriaMetrics** (TSDB) + **Grafana** (telemetry HUDs) + Prometheus Exporters (**cAdvisor**, **Node Exporter**)
  - **Logs**: **Vector** (collection & transforms) + **OpenObserve** (structured SQL log analytics)
- **Agent Protocols & Extensibility**: **Model Context Protocol (MCP)** — Streamable HTTP microservices providing modular tool boundaries across the platform.
- **Edge AI & Speech**: **Faster-Whisper (CUDA)** + **Kokoro-82M TTS** over Wyoming protocol, with quantized **ModernBERT** ambient triage.

---

## 1. System Architecture & Topology

Aerial separates generic platform orchestration from private homelab state using a decoupled **Two-Repository Architecture**:
- **Engine Repo (`azylman/aerial`)**: Autonomous agent execution core (`aerial-brain`), declarative Nomad cluster jobs (`nomad/jobs/*.nomad`), turnkey telemetry stack (VictoriaMetrics, Vector, Grafana), PostgreSQL 18 persistence, CoreDNS service discovery, and outbound MCP microservices.
- **User Config Repo (e.g. `your-username/your-aerial-config`)**: Declarative user sidecar jobs (`jobs/*.nomad`), custom skills and automation runbooks (`custom-skills/`), modular telemetry scrapes (`victoriametrics/`), platform options (`config.yaml`), persona guidelines (`rules/`), and Discord channel policies (`channels/`). Starter template available at [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example).

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  MULTIPLEXED INGRESS LAYER                                  │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Real-time Voice (Kiosk/Orin) │ HTTP REST & Event Webhooks   │ Discord Gateway Funnel        │
│ • Whisper ASR (GPU Accel)    │ • /prompt (Prompt Injection) │ • Realtime Gateway Events     │
│ • Wyoming Protocol (:10300)  │ • /voice/ask (Voice Pipeline)│ • Laya System-1 Classifier    │
│ • WebRTC & Audio Streaming   │ • /discord/message & Webhooks│ • Continuous Typing Pulses    │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                        Aerial Brain                                         │
│  • Autonomous Subagent Orchestrator & SDD Planning Engine                                   │
│  • Self-Healing GitOps Worker (Proactive CI/CD Remediation)                                 │
│  • Configurable Multi-Protocol Execution Core: agy-cli, Gemini CLI, etc.                    │
│  • Laya INT8 ModernBERT-large Ambient Classifier (/v1/systemone)                            │
│  • PostgreSQL 18 Multi-Turn Thread Memory & Atomic CAS Task State                           │
│  • Semantic Memory Hybrid RRF (dense pgvector HNSW + sparse FTS lexical search)             │
│  • Deep Prometheus Telemetry Instrumentation (:8080/metrics)                                │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                       HASHICORP NOMAD 3-NODE HETEROGENEOUS CLUSTER                          │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Core Server (quiet-zero)     │ GPU Worker (calibarn)        │ Touch Kiosk (cockpit)         │
│ • aerial-brain (Agent Core)  │ • orin-voice (Whisper GPU)   │ • kiosk-client (ALSA / ear)   │
│ • aerial-postgres (pgvector) │ • Kokoro-82M TTS synthesis   │ • WebRTC stream & Chromium UI │
│ • coredns (*.aerial, *.lan)  │ • Wyoming protocol (:10300)  │ • Voice fingerprinter sidecar │
│ • infisical & redis (Secrets)│ • Host volume whisper_cache  │ • wake-word detection         │
│ • webhooks-router & hangar   │ • High-bandwidth speech pipe │ • Real-time user touch HUD    │
│ • User Sidecars (*.nomad)    │ • CUDA / Tensor acceleration │ • Local audio capture         │
│ • Persistent Cron Pipelines  │ • Edge AI speech engine      │ • Ambient room microphone     │
│ • Full Telemetry Matrix      │ • Host GPU worker offload    │ • Wayland display kiosk       │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                          AUTONOMOUS TOOL RUNTIME (MCP) & TELEMETRY                          │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Nomad Cluster MCP (:4005)    │ Database Scheduler (:8080)   │ VictoriaMetrics TSDB (:8428)  │
│ GitHub Operations MCP (:4003)│ Infisical Secrets (:4006)    │ OpenObserve Telemetry (:5080) │
│ Discord REST MCP (:4001)     │ Host Docker MCP (:4002)      │ Grafana Cyberpunk HUD (:3000) │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
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
   model: "gemini-3.8-flash-high"
   low_effort_model: "gemini-3.8-flash-low"
   timezone: "America/Los_Angeles"
   system_channel: "aerial-dev"

   # Administrator Allowlist
   # Numeric Discord Snowflake IDs or usernames authorized to perform system operations
   admin_users:
     - "123456789012345678"

   # Channel Policies, Interaction Modes & Webhook Interceptors
   channels:
     # Default fallback (Required)
     default:
       mode: "threads"           # "threads" | "channel" | "ignore"
       ignore_bots: true
       ambient_wake_threshold: 0.80

     # In-Channel Direct Interaction with Classifier and Lifecycle Webhooks
     general:
       mode: "channel"
       wake_mode: "classifier"   # "classifier" | "mention" | "all"
       ignore_bots: true
       ambient_wake_threshold: 0.80
       # Optional Channel Lifecycle Webhook Interceptors
       hooks:
         on_wake:
           url: "http://sidecar-service:8000/hooks/wake"
           timeout_ms: 2000
           on_timeout: "classify"
         pre_turn:
           url: "http://sidecar-service:8000/hooks/pre-turn"
           timeout_ms: 5000
           on_timeout: "retry"
         post_turn:
           url: "http://sidecar-service:8000/hooks/post-turn"
           timeout_ms: 3000
           on_timeout: "proceed"

     # Mention-Only Channel (listens ambiently, responds ONLY on explicit @mention or direct reply)
     lounge:
       mode: "channel"
       wake_mode: "mention"
       ignore_bots: false

     # Custom ambient threshold per channel
     dev-alerts:
       mode: "channel"
       wake_mode: "classifier"
       ignore_bots: false # Allow bot alerts
       ambient_wake_threshold: 0.70

     # Ignore specific noisy channels
     memes:
       mode: "ignore"
   ```
   - **Interaction Modes (`mode`)**:
     - `threads`: Direct messages or mentions spawn and route to a Discord thread (default).
     - `channel`: Messages are evaluated directly in-channel without spawning threads.
     - `ignore` (or `disabled`): Channel is completely ignored (no messages evaluated, no startup sweeps).
   - **Wake Sensitivity Modes (`wake_mode`)**:
     - `mention` (or `mentions`, `direct`): Aerial responds strictly to explicit user pings (`@Aerial`) and direct replies. Keyword triggers and LLM classification are bypassed with zero token cost. Ambient channel chatter is silently appended into `transcript.jsonl` so Aerial retains complete conversational lookback when subsequently pinged.
     - `classifier` (or `ambient`): Tier 1 wakes strictly on direct mentions and direct replies; Tier 2 ambient messages are scored (0.0 to 1.0) locally by the in-cluster Laya System-1 INT8 ModernBERT-large classifier (`/v1/systemone`) evaluating recent channel context and thread history with zero external API latency or token cost. Plaintext keywords do not trigger Tier 1 wakes.
     - `all` (or `always`): Responds to every incoming message (default inside active threads).
   - **Channel Lifecycle Webhook Interceptors (`hooks:`)**:
     - Generic harness extension points allowing user services or sidecars to programmatically modify turn behavior:
       - `on_wake`: Intercepts raw messages to override wake decisions (`wake`, `drop`, `classify`).
       - `pre_turn`: Execution gating (`proceed`, `drop`, `retry`) and dynamic operational context injection (`injected_context` wrapped in `<COORDINATION_CONTEXT>`).
       - `post_turn`: Captures turn outcome, error messages, duration, and LLM token usage.
   - **Substantive Response Enforcement**: Agent turns must always produce substantive output; unrecovered empty stdout or missing responses trigger automatic retry and failure handling without swallowed turns.
   - **Server Whitelisting (Default-Deny)**: Set `channels.default.mode: "ignore"` to ignore the entire server by default, responding only in explicitly declared channels.
   - **Hot-Reloading & LKGC**: Changes to `config.yaml` trigger zero-downtime hot-reloads via Nomad template signaling (`SIGHUP`) and are reconfigured in-memory without restarting the daemon. If invalid YAML is saved, Aerial retains the **Last Known Good Configuration (LKGC)** in memory and posts a diagnostic alert to `#aerial-dev`.

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
Core system skills (such as `self-improvement`) are baked into the `brain` image during build.

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

Aerial connects to external Model Context Protocol (MCP) servers over Streamable HTTP / SSE. Service hostnames resolve dynamically across cluster nodes via CoreDNS (`*.aerial`, `*.lan`):

#### Built-in Tool Autodiscovery
By default, Aerial automatically mounts:
- **`nomad`** (`http://nomad-mcp:4005/mcp` - Nomad cluster orchestration, job lifecycles & allocation diagnostics)
- **`discord`** (`http://discord-mcp:4001/mcp` - Outbound Discord messaging & thread tools)
- **`github`** (`http://github-mcp:4003/mcp` - GitHub repository, PR, and issue operations with PAT auth)
- **`scheduler`** (`http://scheduler-mcp:8080/mcp` - PostgreSQL-backed cron & reminder manager)
- **`infisical`** (`http://infisical-mcp:4006/mcp` - Secret inspection and rotation management)
- **`victoriametrics`** (`http://victoriametrics-mcp:4004/mcp` - Streamable HTTP TSDB metric querying & alert rule inspection)
- **`openobserve`** (`http://openobserve:5080/openobserve/api/default/mcp` - Native telemetry, log exploration, and SQL search)
- **`docker`** (`http://docker-mcp:4002/mcp` - Host Docker daemon operations)


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
Environment variables `${VAR}` are interpolated dynamically at runtime from Infisical secrets and Nomad variables.

---

### Layer 4: Adding Custom Nomad Jobs (`jobs/*.nomad`)

You can run user-defined sidecars, background workers, or hardware bridges across the cluster by placing declarative Nomad job specifications in `jobs/` within your private configuration repository (`azylman/aerial-config`). Nomad natively schedules these jobs across cluster nodes using hardware constraints:

```hcl
job "custom-worker" {
  datacenters = ["dc1"]
  type        = "service"

  # Target specific cluster node class (e.g. quiet-zero, calibarn, cockpit)
  constraint {
    attribute = "${node.class}"
    operator  = "="
    value     = "quiet-zero"
  }

  group "worker" {
    count = 1

    network {
      mode = "host"
      port "http" {
        to = 8080
      }
    }

    task "worker" {
      driver = "docker"

      config {
        image        = "ghcr.io/your-username/custom-worker:latest"
        network_mode = "host"
      }
    }
  }
}
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
│ • aerial-hangar (:8080)    │ aerial-victoriametrics      │ aerial-grafana (:3000)     │
│ • postgres-exporter (:9187)│ (:8428)                     │ • Anonymous Admin HUD      │
│ • cadvisor (:8080)         │ • 5-Year Retention          │ • Cyberpunk Permet Theme   │
│ • node-exporter (:9100)    │ • Dynamic 15s Scrape Engine │ • Core Telemetry Dashboard │
│ • Modular scrape.d/*.yml   │ • Token Interpolation       │ • Postgres & Docker Views  │
└────────────────────────────┴─────────────────────────────┴────────────────────────────┘
```

### 1. VictoriaMetrics TSDB (`aerial-victoriametrics`)
- **Engine**: Single-node VictoriaMetrics (`v1.153.0`) running with 5-year retention (`-retentionPeriod=5y`) and 15s scrape interval.
- **Dynamic Modular Scrapes**: VictoriaMetrics automatically discovers and live-reloads scrape configurations mounted from `/share/aerial-config/victoriametrics/*.yml` every 15 seconds without container restarts (`-promscrape.configCheckInterval=15s`).
- **Token Interpolation**: Automatically expands environment variables (e.g. `%{HA_METRICS_TOKEN}`) in custom scrape configs.

### 2. Pre-Provisioned Grafana Dashboards (`http://localhost:8089/grafana/`)
- **Single-Click Anonymous Admin Access**: Instant dashboard access without login friction.
- **Persistent Backend**: Dashboards and user settings persist directly in PostgreSQL 18 (`GF_DATABASE_TYPE=postgres`).
- **Pre-Provisioned Dashboard Suite**:
  - **`⚡ Aerial Brain & Hangar Operations` (`core-telemetry.json`)**: Live turn execution latency (p50/p90/p95/p99), token usage, active worker pool depth, CAS task states, runner error taxonomy, Discord gateway ping, classifier triage decisions, Ollama vector search durations, GitSync and Hangar reconcile runs.
  - **`🐘 PostgreSQL Overview` (`postgres-overview.json`)**: Active backends, connection pool state, buffer cache hit ratio (>99%), commits/rollbacks, tuple read/write velocity, and lock contention.
  - **`🐳 Docker Containers Overview` (`docker-overview.json`)**: Per-container CPU %, working set memory curves, network RX/TX, and CFS CPU throttling periods.
  - **`🖥️ Host System & Hardware Overview` (`host-system-overview.json`)**: Host CPU load breakdown, thermal sensors per core, RAM utilization, root disk space, and load averages.
  - **`🪙 Token Telemetry & Channel Breakdown` (`token-usage.json`)**: Real-time and cumulative token consumption partitioned by Discord channel, 24-hour and 7-day velocity, burn rates, and channel share leaderboards.

### 3. Deep Go Prometheus Metrics Instrumentation
- **`aerial-brain`** (Exposed on internal `:8080/metrics` / host `:8088/metrics`):
  - Queue & Workers: `aerial_brain_active_workers`, `aerial_brain_queue_depth`, `aerial_brain_interrupted_turns_recovered_total`.
  - Turns & Runner: `aerial_brain_turns_total`, `aerial_brain_turn_duration_seconds`, `aerial_brain_runner_executions_total`, `aerial_brain_runner_duration_seconds`, `aerial_brain_runner_errors_total`, `aerial_brain_yield_trap_total`.
  - Classifier & Funnel: `aerial_brain_classifier_duration_seconds`, `aerial_brain_classifier_decisions_total`, `aerial_brain_classifier_confidence_score`, `aerial_brain_discord_events_total`, `aerial_brain_discord_gateway_latency_seconds`.
  - Memory & Embeddings: `aerial_brain_memory_operations_total`, `aerial_brain_memory_search_duration_seconds`, `aerial_brain_embeddings_generated_total`, `aerial_brain_facts_extracted_total`.
  - Database Connection Pool: `aerial_brain_db_query_duration_seconds`, `aerial_brain_db_open_connections`, `aerial_brain_db_in_use_connections`, `aerial_brain_db_idle_connections`, `aerial_brain_db_wait_count_total`.
- **`aerial-hangar`** (Exposed on internal `:8080/metrics`):
  - Git-related: `aerial_gitsync_pulls_total`, `aerial_gitsync_pull_duration_seconds`, `aerial_gitsync_sync_requests_total`, `aerial_gitsync_last_sync_timestamp_seconds`.
  - Reconcile & OCI: `aerial_hangar_reconciliations_total`, `aerial_hangar_compose_duration_seconds`, `aerial_hangar_registry_manifest_requests_total`, `aerial_hangar_registry_manifest_duration_seconds`, `aerial_hangar_rollbacks_total`, `aerial_hangar_image_quarantines_total`, `aerial_hangar_active_quarantines`, `aerial_hangar_build_info`.

---

## 4. Continuous Deployment, GitOps & Self-Improvement

Aerial uses an automated, event-driven GitOps push continuous deployment pipeline:
1. **GitHub Actions Matrix Builds**: Triggers dynamic matrix builds for modified microservices and publishes them to GitHub Container Registry (`ghcr.io/azylman/aerial-*`).
2. **Event-Driven Push GitOps Router (`webhooks-router`)**: Receives authenticated GitHub push webhooks through Cloudflare tunnels, immediately triggering Hangar GitOps sync and Nomad job reconciliation without 60s pull polling delay.
3. **Dedicated Hangar Sidecar (`aerial-hangar`)**: A dedicated background daemon holding read-write mounts on `/share/aerial-config` and `/share/aerial`, dispatching declarative Nomad job updates, managing snapshot rollbacks, image quarantine, and exposing `/sync` and `/reconcile` triggers.
4. **Physical Immutability & Asynchronous PR Workflow**: The `aerial-brain` execution container mounts repositories strictly **read-only (`:ro`)**. When making code, configuration, persona, or peripheral adjustments across any repository, Aerial uses `scripts/aerial-pr.sh init [repo]` to initialize an ephemeral scratch directory, runs pre-flight syntax and verification checks (`./scripts/verify.sh --staged`), and submits asynchronously via `scripts/aerial-pr.sh submit` with native GitHub auto-merge (`SQUASH`) enabled and registered in PostgreSQL `pr_registry`.
5. **Event-Driven Continuous Delivery & Proactive CI Self-Healing**: Once submitted, turns terminate immediately with zero foreground CI polling. When CI turns green, GitHub automatically merges the PR and Hangar deploys the Nomad jobs, posting confirmation directly to the registered Discord thread. If CI checks fail, `webhooks-router` wakes Brain with a proactive remediation prompt to checkout the branch, resolve the failure locally, verify, and push directly to the PR branch.

---

## 5. Component Modules

| Service | Port | Description |
| :--- | :--- | :--- |
| **`aerial-postgres`** | `5432` (Host `127.0.0.1:5432`) | Dedicated PostgreSQL 18 relational database with `pgvector` extension for production state, CAS task queues, Hybrid RRF semantic memory (dense vector + sparse FTS), PR registry, schedules, and Grafana storage. Production runs exclusively on PostgreSQL. |
| **`aerial-brain`** | `8080` (Host `8088`) | Multi-protocol Go execution daemon running `agy`, PostgreSQL memory, multiplexed Discord, Voice (`/voice/ask`), and HTTP (`/prompt`) ingress, Prometheus metrics (`:8080/metrics`), and SIGHUP configuration hot-reloading. Mounted `:ro`. |
| **`aerial-hangar`** | `8087` (Host `8087`) | Dedicated Hangar sidecar daemon managing automated repository synchronization, push GitOps reconciliation for Nomad jobs, image update detection, snapshot rollbacks, and Prometheus metrics. Mounted `:rw`. |
| **`coredns`** | `53` (Host `53/udp`) | Dynamic Nomad service discovery daemon rendering internal DNS records (`*.aerial`, `*.lan`) directly from `nomadServices`. |
| **`infisical`** | Dynamic (via `infisical.aerial`) | Centralized secret management and automated rotation backed by Redis, dynamically syncing secrets into Nomad variables (`nomadVar`). |
| **`webhooks-router`** | `4020` (Host `4020`) | Event-driven webhook dispatcher routing GitHub push webhooks to Hangar and Infisical secret changes to Nomad variables. |
| **`nomad-mcp`** | `4005` (Host `4005`) | Native Streamable HTTP MCP server for Nomad cluster orchestration, job lifecycles, and allocation diagnostics. |
| **`infisical-mcp`** | `4006` (Host `4006`) | Native Streamable HTTP MCP server for Infisical secret management and rotation. |
| **`aerial-scheduler-mcp`**| `8080` (Internal) | PostgreSQL-backed cron and one-shot reminder management server over HTTP MCP. |
| **`aerial-discord-mcp`** | `4001` (Host `4001`) | Outbound MCP server providing Discord messaging, thread creation, and channel tools. |
| **`aerial-github-mcp`** | `4003` (Host `4003`) | Native in-image GitHub MCP server with PAT authentication for PR, issue, and code operations. |
| **`aerial-victoriametrics-mcp`**| `4004` (Host `127.0.0.1:4044`) | VictoriaMetrics MCP server exposing metrics querying and alert inspection over Streamable HTTP. |
| **`aerial-ollama`** | `11434` (Host `11434`) | Local LLM and embedding server for vector memory retrieval (`all-minilm:latest` / 384-dim). |
| **`aerial-agentsview`** | `8080` (via proxy) | Web UI for visualizing agent transcripts, session history, and execution timelines. |
| **`aerial-dashboard`** | `8080` (via proxy) | Status microservice serving Cyberpunk status HUD. |
| **`aerial-docs`** | `80` (via proxy) | Documentation engine rendering Markdown and Mermaid diagrams from config repo via Docsify. |
| **`aerial-proxy`** | `8089` (Host `8089`) | Edge reverse proxy routing traffic for `/` (302 to HUD), `/dashboard/`, `/docs/`, `/agentsview/`, and `/grafana/`. |
| **`aerial-cadvisor`** | `8080` (Internal) | cAdvisor container metrics collector gathering per-container CPU, memory, network, and disk telemetry. |
| **`aerial-node-exporter`**| `9100` (Internal) | Node Exporter host telemetry gathering host CPU loads, memory, storage, thermals, and network metrics. |
| **`aerial-postgres-exporter`**| `9187` (Internal) | PostgreSQL database metrics exporter gathering connection pools, locks, query stats, and buffer metrics. |
| **`aerial-victoriametrics`**| Dynamic (via `victoriametrics.aerial`) | VictoriaMetrics single-node TSDB scraping Prometheus metrics from all exporters with 5-year retention and dynamic `scrape.d/` config. |
| **`aerial-grafana`** | `3000` (via proxy) | Grafana visual dashboards serving system HUD & container metrics with PostgreSQL persistent backend and pre-provisioned dashboards. |
| **`docker-mcp`** | `4002` (Host `4002`) | Auxiliary host Docker MCP inspection service over `/var/run/docker.sock`. |

---

## 6. Quickstart Setup

### Prerequisites
- HashiCorp Nomad 1.8+ & Docker Engine 24+ (Docker task driver enabled)
- Google Account for OAuth authentication (recommended to avoid API key rate limits) OR Gemini API Key
- Discord Bot Token (with Message Content and Server Members intents enabled)
- GitHub Personal Access Token (for private configuration repository synchronization)

### Step 1: Create Your Private Configuration Repository
1. Create a private repository on GitHub (e.g. `your-username/my-aerial-config`).
2. Copy or fork the template files from [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example) into your private repository.
3. Customize `config.yaml` and persona rules as desired.

### Step 2: Clone Aerial Engine
```bash
git clone https://github.com/azylman/aerial.git
cd aerial
```

### Step 3: Configure Infisical Secrets & Nomad Variables
Secrets and credentials are managed through Infisical and synced automatically to Nomad variables (`nomad/jobs/shared` and `nomad/jobs/brain`):
```bash
# Example secrets managed in Infisical:
# DISCORD_BOT_TOKEN=...
# GITHUB_PAT=...
# AERIAL_CONFIG_REPO_URL=https://github.com/your-username/my-aerial-config.git
```

### Step 4: Deploy Nomad Stack
Core jobs are defined in `nomad/jobs/*.nomad` and deployed via Nomad:
```bash
# Run database migrations batch job:
nomad job run nomad/jobs/migrate.nomad

# Run core services:
nomad job run nomad/jobs/coredns.nomad
nomad job run nomad/jobs/postgres.nomad
nomad job run nomad/jobs/brain.nomad
nomad job run nomad/jobs/hangar.nomad
nomad job run nomad/jobs/proxy.nomad
```
On boot, `aerial-brain` and `aerial-hangar` adopt or clone your private configuration repository into `/share/aerial-config` and synchronize settings.

### Step 5: Authenticate via Google OAuth (Recommended)
By default, Aerial runs `agy` in Google OAuth / subscription mode:
1. Run the interactive `agy` CLI inside the brain container or shell:
   ```bash
   docker exec -it aerial-brain agy
   ```
2. Copy the displayed Google OAuth login URL into your web browser and sign in.
3. Paste the authorization code back into the terminal prompt and hit Enter.
4. Press `Ctrl+C` to exit once authenticated. Aerial stores the OAuth session token in `/data` and automatically refreshes access tokens in the background!

### Step 6: Verify Health
```bash
nomad job status
nomad alloc logs -f $(nomad job status brain | grep -m1 running | awk '{print $1}')
```

---

## 7. Operational Commands & Endpoints

| Action / Service | URL / Command |
| :--- | :--- |
| Status Dashboard (HUD) | `http://localhost:8089/dashboard/` (or `http://localhost:8089/`) |
| Documentation (Docsify) | `http://localhost:8089/docs/` |
| Agent Transcripts (Agentsview) | `http://localhost:8089/agentsview/` |
| System Telemetry (Grafana) | `http://localhost:8089/grafana/` |
| Brain Prometheus Metrics | `http://localhost:8088/metrics` |
| Brain Healthcheck | `http://localhost:8088/health` |
| List all cluster jobs | `nomad job status` |
| View job allocations | `nomad job status <job_name>` |
| View live allocation logs | `nomad alloc logs -f <alloc_id>` |
| Stop or restart a job | `nomad job stop <job_name>` / `nomad job restart <job_name>` |
| Trigger Hangar Sync & Push CD | `curl -s -X POST http://hangar:8087/sync` |
| Trigger Immediate GitOps Reconcile | `curl -s -X POST http://hangar:8087/reconcile` |

---

## 8. Security & Best Practices

- **Multi-User Security & Admin Privilege Enforcement**: In shared or multi-user channels, messages from users are automatically checked against `admin_users` in `config.yaml`. Only authorized admins (`is_admin: true`) can modify system instructions, edit system configuration (`config.yaml`), manage Nomad jobs, or alter cron schedules.
- **Fail-Closed Default-Deny Server Containment**: Set `channels.default.mode: "ignore"` to contain Aerial exclusively to allowlisted channels on shared Discord servers.
- **Discord Funnel Hardening**: Thread ID deduplication recovery resolves Discord error 160004 race conditions seamlessly, and message staleness TTL is set to 30 minutes to prevent dropped messages during deployment bursts.
- **Zero Plaintext Tokens**: GitHub PATs and database secrets are passed in-memory ephemerally and never written to `.git/config` on disk.
- **Automated Token Redaction**: All subprocess logs, errors, and GitOps reconcile streams pass through multi-pattern token sanitizers to redact sensitive credentials.
- **Infisical Secret Governance**: Secrets are centralized in Infisical and injected dynamically into Nomad variables (`nomadVar`), eliminating plaintext `.env` files on disk.
- **CoreDNS Mesh Isolation**: Internal microservices communicate securely across cluster nodes via dynamic CoreDNS (`*.aerial`, `*.lan`), keeping traffic private to the cluster network.
