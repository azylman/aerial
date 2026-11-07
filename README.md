# Aerial

An autonomous AI agent and configurable homelab operations platform engineered for heterogeneous multi-node Kubernetes clusters running k3s.

Aerial is built from the ground up around end-to-end configurability across both the agent and the platform. While Aerial herself is tailored through pluggable execution backends (such as `agy-cli` or Gemini CLI), domain-specific skills, and modular persona rules, the underlying platform is equally extensible—orchestrating user-defined containers, declarative Kubernetes workloads, and persistent scheduled pipelines. While interfacing with her through Discord or Voice, Aerial actively commands compute across storage servers, local GPU inference nodes, and touch kiosks—executing complex multi-turn workflows, authoring and deploying her own code, and operating your homelab fleet.

**Observability, CI/CD, reliability, and extensibility are first-class invariants.** Deployments are fully automated through event-driven push GitOps with Flux CD and `synapse` that continuously reconciles Kubernetes Kustomizations and proactively self-heals failing CI builds or deployment rollbacks, backed by a turnkey telemetry matrix featuring VictoriaMetrics TSDB, `vmalert`, Fluent Bit log streaming to OpenObserve, and pre-provisioned Grafana dashboards. The system guarantees operational resilience through dynamic CoreDNS service discovery, centralized secret governance via Infisical and Valkey, read-only container mounts, and Last Known Good Configuration (LKGC) fallbacks, while staying fully extensible via custom skills, declarative Kubernetes manifests, and Streamable HTTP MCP microservices.

---

## Tech Stack

Built in **Go (1.27)** and orchestrated as a resilient bare-metal homelab mesh:

- **Cluster Orchestration**: **Kubernetes (k3s)** — Heterogeneous 3-node bare-metal cluster running NixOS 26.05 provisioned via Colmena (`nixos/flake.nix`) with Spegel P2P OCI registry, managing distributed daemons, batch jobs, GPU workloads, and automated rolling deployments.
- **GitOps & Continuous Delivery**: **Flux CD (`flux-system`)** — Declarative GitOps reconciling `./k8s` Kustomizations from `azylman/aerial` (`aerial-core`) and `azylman/aerial-config` (`aerial-config`), driven by `synapse` webhook dispatch.
- **Service Discovery & Ingress**: **NGINX Gateway Fabric** (`lan-gateway` on `:80`/`:443` and `edge-gateway` on `:4021`) + **CoreDNS** rewriting `*.aerial` and `*.lan` internal DNS to `*.default.svc.cluster.local`.
- **Persistence & Vector Memory**: **CloudNativePG (PostgreSQL 18 + pgvector)** — 2-instance high-availability cluster for multi-turn conversation state, strict 1-to-1 single-service table ownership, native `:9187` Prometheus metrics, and hybrid RRF semantic memory (`coven`).
- **Schema Migrations**: **Ariga Atlas** — Declarative schema migrations, automated diffing, and checksum validation via Kubernetes batch jobs.
- **Idempotent, Guaranteed Event Handling**: **Webhooks + River Queue** — Transactional PostgreSQL-backed Go job queue powering strictly idempotent Discord ingress (`discord_ingress`, `discord_interaction`), webhook processing, event dispatch (`synapse`), and multi-chunk build orchestration (`vanadis`).
- **Secrets Management**: **Infisical + Valkey 9** — Centralized secret governance syncing secrets into Kubernetes Secrets (`shared-secrets`) with zero plaintext disk persistence.
- **Observability & Telemetry**:
  - **Metrics**: **VictoriaMetrics** (TSDB `:8428`) + **`vmalert`** (`:8880`) + **Grafana** (telemetry HUDs `:3000`) + Prometheus Exporters (**Kubelet / cAdvisor**, **Node Exporter**, **CloudNativePG**)
  - **Logs**: **Fluent Bit** (DaemonSet collection & transforms) + **OpenObserve** (structured SQL log analytics `:5080`)
- **Agent Protocols & Extensibility**: **Model Context Protocol (MCP)** — Streamable HTTP microservices providing modular tool boundaries across the platform.
- **Edge AI & Speech**: **NeMo Parakeet TDT 0.6B v2 STT** + **Kokoro-82M TTS** over Wyoming protocol (`senses` on GPU `:8880`/`:10300`), **BAAI/bge-small-en-v1.5** vector embeddings (`senses`), and local quantized GGUF ambient triage (`pjev` `/v1/systemone` `:8000`).

---

## 1. System Architecture & Topology

Aerial separates generic platform orchestration from private homelab state using a decoupled **Two-Repository Architecture**:
- **Engine Repo (`azylman/aerial`)**: Autonomous agent execution core (`eri`), scheduling & event watch orchestrator (`haro`), semantic memory & skill synthesis (`coven`), multi-chunk build orchestrator (`vanadis`), Discord Gateway & webhook router (`synapse`), shared Go packages (`pkg/`), declarative Kubernetes manifests (`k8s/*.yaml`), turnkey telemetry stack (VictoriaMetrics, OpenObserve, Grafana), PostgreSQL 18 persistence, CoreDNS service discovery, and outbound MCP microservices.
- **User Config Repo (e.g. `your-username/your-aerial-config`)**: Declarative user Kubernetes manifests (`k8s/*.yaml`), custom skills and automation runbooks (`custom-skills/`), modular telemetry scrapes (`victoriametrics/`), platform options (`config.yaml`), modular persona and runtime rules (`rules/`), and Discord channel policies (`channels/`). Starter template available at [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example).

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  MULTIPLEXED INGRESS LAYER                                  │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Real-time Voice (Cockpit)    │ HTTP REST & Event Webhooks   │ Discord Gateway Funnel        │
│ • Parakeet TDT STT (senses)  │ • /prompt (Prompt Injection) │ • synapse Gateway WebSocket   │
│ • Kokoro-82M TTS (senses)    │ • /voice/ask (Voice Pipeline)│ • Catch-up Sweeps & River Q   │
│ • Wyoming Protocol (:10300)  │ • /discord/message Outbound  │ • POST /discord/ingress       │
│ • WebRTC & Audio Streaming   │ • Cloudflare Tunnel Webhooks │ • pjev System-1 Classifier    │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  eri (Execution Core) :8080                                 │
│  • Autonomous Subagent Orchestrator & SDD Planning Engine                                   │
│  • Self-Healing GitOps Worker (Proactive CI/CD & RoboRev Remediation)                       │
│  • Configurable Multi-Protocol Execution Core: agy-cli, Gemini CLI, etc.                    │
│  • pjev GGUF System-1 Ambient Classifier (/v1/systemone)                                    │
│  • PostgreSQL 18 Multi-Turn Thread Memory & Atomic CAS Task State                           │
│  • Deep Prometheus Telemetry Instrumentation (:8080/metrics)                                │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                       KUBERNETES (k3s) 3-NODE HETEROGENEOUS CLUSTER                         │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ GPU Control-Plane (calibarn) │ Storage Node (quiet-zero)    │ Touch Kiosk (cockpit)         │
│ • eri, vanadis, coven, haro  │ • NixOS 26.05 k3s agent      │ • kiosk-display (Cage Wayland)│
│ • synapse, dashboard, docs   │ • NVMe persistent storage    │ • kiosk-ear (ALSA capture)    │
│ • homepage, agentsview       │ • CloudNativePG standby      │ • kiosk-remote & go2rtc       │
│ • CloudNativePG primary      │ • Compute & storage tier     │ • kiosk-cast-watcher          │
│ • valkey, infisical          │ • Standard worker workloads  │ • Display power CronJobs      │
│ • victoriametrics & vmalert  │ • docket-store (Vikunja)     │ • Local touch HUD & microphone│
│ • senses & pjev (NVIDIA GPU) │                              │ • role=kiosk:NoSchedule       │
│ • prospera & CI runners      │                              │                               │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                          AUTONOMOUS TOOL RUNTIME (MCP) & TELEMETRY                          │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Haro Scheduler MCP (:4005)   │ Coven Memory MCP (:4008)     │ VictoriaMetrics TSDB (:8428)  │
│ Kubernetes MCP (:8080)       │ Infisical Secrets (:8080)    │ vmalert Recording/Alerts:8880 │
│ GitHub Operations MCP (:8080)│ Discord REST MCP (:8080)     │ OpenObserve Telemetry (:5080) │
│ Kernel Browser MCP (:8080)   │ VictoriaMetrics MCP (:8080)  │ Grafana Cyberpunk HUD (:3000) │
│ Prospera Phone Agent (:8080) │ Vanadis Build Queue (:4025)  │ Fluent Bit Log DaemonSet      │
│ Docket Issue Tracker (:8080) │                              │                               │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
```

---

## 2. Extensibility Guide

Aerial is designed to be easily extended across four distinct layers:

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  4 WAYS TO EXTEND AERIAL                                    │
├─────────────────────────────┬──────────────────────────┬────────────────────┬───────────────┤
│ 1. CONFIG & PERSONA         │ 2. CUSTOM SKILLS         │ 3. MCP SERVERS     │ 4. WORKLOADS  │
│ config.yaml & rules/        │ Runbooks in config repo  │ Connecting APIs    │ Extra k8s apps│
└─────────────────────────────┴──────────────────────────┴────────────────────┴───────────────┘
```

---

### Layer 1: Configuration & Persona (`config.yaml` & `rules/`)

User configuration and persona rules live in your private configuration repository (see [aerial-config-example](https://github.com/azylman/aerial-config-example)):

1. **`config.yaml`** (Agent Options, Channel Policies, & MCP Tools):
   ```yaml
   model: "gemini-3.8-flash-high"
   medium_effort_model: "gemini-3.8-flash-medium"
   low_effort_model: "gemini-3.8-flash-low"
   timezone: "America/Los_Angeles"
   system_channel: "aerial-dev"

   # Administrator Allowlist
   # Numeric Discord Snowflake IDs or usernames authorized to perform system operations
   admin_users:
     - "123456789012345678"

   # Channel Policies, Effort Tiers, Interaction Modes & Webhook Interceptors
   channels:
     # Default fallback (Required)
     default:
       mode: "threads"           # "threads" | "channel" | "ignore"
       effort: "high"            # "high" | "medium" | "low"
       ignore_bots: true
       ambient_wake_threshold: 0.80

     # In-Channel Direct Interaction with Classifier and Lifecycle Webhooks
     general:
       mode: "channel"
       effort: "medium"
       wake_mode: "classifier"   # "classifier" | "mention" | "all"
       ignore_bots: true
       ambient_wake_threshold: 0.80
       # Optional Channel Lifecycle Webhook Interceptors
       hooks:
         on_wake:
           url: "http://sidecar-service.aerial/hooks/wake"
           timeout_ms: 2000
           on_timeout: "classify"
         pre_turn:
           url: "http://sidecar-service.aerial/hooks/pre-turn"
           timeout_ms: 5000
           on_timeout: "retry"
         post_turn:
           url: "http://sidecar-service.aerial/hooks/post-turn"
           timeout_ms: 3000
           on_timeout: "proceed"

     # Mention-Only Channel (listens ambiently, responds ONLY on explicit @mention or direct reply)
     lounge:
       mode: "channel"
       effort: "low"
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
   - **Effort Tiers (`effort`)**:
     - `high` (default): Uses primary `model`.
     - `medium`: Routes turns in the channel to `medium_effort_model`.
     - `low`: Routes turns in the channel to `low_effort_model`.
   - **Wake Sensitivity Modes (`wake_mode`)**:
     - `mention` (or `mentions`, `direct`): Aerial responds strictly to explicit user pings (`@Aerial`) and direct replies. Keyword triggers and LLM classification are bypassed with zero token cost. Ambient channel chatter is silently appended into `transcript.jsonl` so Aerial retains complete conversational lookback when subsequently pinged.
     - `classifier` (or `ambient`): Tier 1 wakes strictly on direct mentions and direct replies; Tier 2 ambient messages are scored (0.0 to 1.0) locally by the in-cluster `pjev` System-1 classifier (`/v1/systemone`) evaluating recent channel context and thread history with zero external API latency or token cost. Plaintext keywords do not trigger Tier 1 wakes.
     - `all` (or `always`): Responds to every incoming message (default inside active threads).
   - **Channel Lifecycle Webhook Interceptors (`hooks:`)**:
     - Generic harness extension points allowing user services or sidecars to programmatically modify turn behavior:
       - `on_wake`: Intercepts raw messages to override wake decisions (`wake`, `drop`, `classify`).
       - `pre_turn`: Execution gating (`proceed`, `drop`, `retry`) and dynamic operational context injection (`injected_context` wrapped in `<COORDINATION_CONTEXT>`).
       - `post_turn`: Captures turn outcome, error messages, duration, and LLM token usage.
   - **Substantive Response Enforcement**: Agent turns must always produce substantive output; unrecovered empty stdout or missing responses trigger automatic retry and failure handling without swallowed turns.
   - **Server Whitelisting (Default-Deny)**: Set `channels.default.mode: "ignore"` to ignore the entire server by default, responding only in explicitly declared channels.
   - **Hot-Reloading & LKGC**: Changes to `config.yaml` (synced via `git-sync`) trigger zero-downtime hot-reloads via `SIGHUP` signaling and are reconfigured in-memory without restarting the daemon. If invalid YAML is saved, Aerial retains the **Last Known Good Configuration (LKGC)** in memory and posts a diagnostic alert to `#aerial-dev`.

2. **Modular Rules (`rules/`)** (Persona, Identity & Runtime Overrides):
   Define custom persona rules, tone guidelines, user identity, and runtime preferences under `rules/{common,persona,discord,voice}/` in your config repository. User rules in `aerial-config/rules/` take precedence over base engine rules in `aerial/rules/` and are automatically provisioned into target runtime directories (`discord`, `voice`, `ephemeral`).

3. **Per-Channel Instructions (`channels/<channel-name>.md`)**:
   Define dedicated guidelines and operational personas tailored to specific Discord channels:
   - **Convention Auto-Discovery**: Place Markdown files in `channels/<channel-name>.md` within your configuration repository (e.g., `/share/aerial-config/channels/general.md` or `/share/aerial-config/channels/lounge.md`). `eri` automatically discovers and injects them dynamically without requiring explicit path configuration in `config.yaml`.
   - **Thread Inheritance**: Conversations in Discord threads automatically resolve and apply instructions from their parent channel (`channels/<parent-channel-name>.md`).
   - **Normalized Lookups**: Channel names are normalized case-insensitively, strip leading `#`, and interoperate between spaces and hyphens (e.g., `#Dev Chat` resolves `dev-chat.md` or `dev chat.md`).
   - **Prompt Injection & Safety**: Instructions are framed inside `<CHANNEL_INSTRUCTIONS>` prior to `<USER_REQUEST>`, escaped against XML delimiter breakouts, capped at 64KB, and defended against directory traversal.

---

### Layer 2: Adding Custom Skills

Skills use **Progressive Disclosure**—Aerial only loads skill titles and descriptions into context, reading full runbooks on-demand when relevant.

#### A. User Custom Skills (`custom-skills/` in your config repo)
Place custom skill directories inside `custom-skills/{common,discord,voice}/` in your private configuration repository:
```text
custom-skills/
└── common/
    └── weather-alerts/
        └── SKILL.md
```
`eri` automatically discovers custom skills in `/share/aerial-config/custom-skills` and symlinks them into each runtime's `.gemini/config/skills/` directory with highest priority. Orphaned or dead symlinks are automatically swept when skills are renamed or removed.

#### B. Built-in Skills (`azylman/aerial/.agents/skills/`)
Core system skills (such as `self-improvement`) are baked into the `eri` image during build.

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

`eri` connects to external Model Context Protocol (MCP) servers over Streamable HTTP / SSE. Service hostnames resolve dynamically across cluster nodes via CoreDNS (`*.aerial`, `*.lan`):

#### Built-in Tool Autodiscovery
By default, `eri` automatically mounts built-in MCP servers across runtimes:
- **`scheduler`** (`http://haro.aerial/mcp` - Persistent PostgreSQL cron, one-shot, heartbeat & event-watch orchestrator)
- **`memory`** (`http://coven.aerial/mcp` - Semantic memory search, fact storage & ACT-R recall)
- **`vanadis`** (`http://vanadis.aerial/mcp` - Autonomous multi-chunk build queue orchestrator)
- **`discord`** (`http://discord-mcp.aerial/mcp` - Outbound Discord messaging & thread tools)
- **`github`** (`http://github-mcp.aerial/mcp` - GitHub repository, PR, and issue operations with PAT auth)
- **`openobserve`** (`http://openobserve.aerial:5080/openobserve/api/default/mcp` - Native telemetry, log exploration, and SQL search)
- **`docket`** (`http://docket-mcp.aerial/mcp` - Issue tracking, DAG dependencies, and hybrid RRF task search backed by Vikunja and PostgreSQL 18)

Additional cluster MCP microservices (`kubernetes-mcp` on `http://kubernetes-mcp.aerial/mcp`, `infisical-mcp` on `http://infisical-mcp.aerial/mcp`, `victoriametrics-mcp` on `http://victoriametrics-mcp.aerial/mcp`, and `kernel-mcp` on `http://kernel-mcp.aerial/mcp`) can be enabled per target runtime in `config.yaml`.

#### Custom MCP Servers (`config.yaml`)
Define additional MCP tools partitioned by target runtime (`common`, `discord`, `voice`, `ephemeral`) in your `config.yaml`:
```yaml
mcp_servers:
  common:
    brave-search:
      serverUrl: "http://brave-mcp.aerial/mcp"
  discord:
    kubernetes:
      serverUrl: "http://kubernetes-mcp.aerial/mcp"
    custom-remote-api:
      serverUrl: "https://mcp.example.com/mcp"
      headers:
        Authorization: "Bearer ${CUSTOM_API_KEY}"
```
Environment variables `${VAR}` are interpolated dynamically at runtime from Infisical secrets and Kubernetes Secrets.

---

### Layer 4: Adding Custom Kubernetes Manifests (`k8s/*.yaml`)

You can run user-defined sidecars, background workers, or hardware bridges across the cluster by placing declarative Kubernetes resource manifests and Kustomizations in `k8s/` within your private configuration repository (`azylman/aerial-config`). Flux CD continuously reconciles these manifests into the cluster:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: custom-worker
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: custom-worker
  template:
    metadata:
      labels:
        app: custom-worker
    spec:
      containers:
        - name: worker
          image: ghcr.io/your-username/custom-worker:latest
          ports:
            - containerPort: 8080
              name: http
---
apiVersion: v1
kind: Service
metadata:
  name: custom-worker
  namespace: default
spec:
  selector:
    app: custom-worker
  ports:
    - name: http
      port: 8080
      targetPort: http
```

---

## 3. Observability & Telemetry Stack

Aerial includes an enterprise-grade, out-of-the-box observability matrix with single-node VictoriaMetrics TSDB, `vmalert`, SQLite-backed Grafana dashboards, deep Go Prometheus instrumentation, and dynamic modular scrape configurations.

```text
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                                 TELEMETRY PIPELINE                                    │
├────────────────────────────┬─────────────────────────────┬────────────────────────────┤
│ METRIC EXPORTERS           │ TIME-SERIES TSDB & ALERTS   │ VISUALIZATION & HUD        │
│ • eri (:8080)              │                             │                            │
│ • synapse (:4020)          │ victoriametrics (:8428)     │ grafana (:3000)            │
│ • haro (:4005)             │ • 5-Year Retention          │ • Anonymous Admin HUD      │
│ • coven (:4008)            │ • Dynamic 15s Scrape Engine │ • Cyberpunk Permet Theme   │
│ • vanadis (:4025)          │ • Pod & cAdvisor Scrapes    │ • Core Telemetry Dashboard │
│ • CloudNativePG (:9187)    │ vmalert (:8880)             │ • SQLite Storage Backend   │
│ • Kubelet / cAdvisor       │ • MAD Anomaly Detection     │ • openobserve (:5080)      │
│ • node-exporter (:9100)    │ • Alert Rules Evaluation    │   Structured SQL Logs      │
└────────────────────────────┴─────────────────────────────┴────────────────────────────┘
```

### 1. VictoriaMetrics TSDB & vmalert (`victoriametrics`, `vmalert`)
- **Engine**: Single-node VictoriaMetrics running with 5-year retention (`-retentionPeriod=5y`) and 15s scrape interval, scraping cluster pods, Kubelet, and cAdvisor.
- **Dynamic Modular Scrapes**: VictoriaMetrics automatically discovers and live-reloads scrape configurations mounted from `/share/aerial-config/victoriametrics/*.yml` every 15 seconds without container restarts (`-promscrape.configCheckInterval=15s`).
- **Anomaly Detection & Recording Rules (`vmalert`)**: Evaluates Median Absolute Deviation (MAD) anomaly rules and alert definitions on port `:8880`.

### 2. Pre-Provisioned Grafana Dashboards (`services/grafana/dashboards/`)
- **Single-Click Anonymous Admin Access**: Instant dashboard access without login friction.
- **Persistent Backend**: Dashboards and user settings persist in SQLite (`GF_DATABASE_TYPE=sqlite3`) with dashboards provisioned from JSON definitions.
- **Pre-Provisioned Dashboard Suite**:
  - **`⚡ Aerial Core & Operations` (`core-telemetry.json`)**: Live turn execution latency (p50/p90/p95/p99), token usage, active worker pool depth, CAS task states, runner error taxonomy, Discord gateway latency (`synapse_discord_gateway_latency_seconds`), River queue metrics, classifier triage decisions, and GitSync reconcile runs.
  - **`🐘 PostgreSQL Overview` (`postgres-overview.json`)**: CloudNativePG active backends, connection pool state, buffer cache hit ratio (>99%), commits/rollbacks, tuple read/write velocity, and lock contention.
  - **`🐳 Cluster Pods & Containers Overview` (`docker-overview.json`)**: Per-container CPU %, working set memory curves, network RX/TX, and CFS CPU throttling periods via cAdvisor.
  - **`🖥️ Host System & Hardware Overview` (`host-system-overview.json`)**: Host CPU load breakdown, thermal sensors per core, RAM utilization, root disk space, and load averages from Node Exporter.
  - **`🪙 Token Telemetry & Channel Breakdown` (`token-usage.json`)**: Real-time and cumulative token consumption partitioned by Discord channel, 24-hour and 7-day velocity, burn rates, and channel share leaderboards.

### 3. Deep Go Prometheus Metrics Instrumentation
- **`eri`** (Exposed on `:8080/metrics`):
  - Queue & Workers: `aerial_brain_active_workers`, `aerial_brain_queue_depth`, `aerial_brain_interrupted_turns_recovered_total`, `aerial_brain_daemons_active`, `aerial_brain_daemon_spawns_total`, `aerial_brain_daemon_prunes_total`.
  - Turns & Runner: `aerial_brain_turns_total`, `aerial_brain_turn_duration_seconds`, `aerial_brain_runner_executions_total`, `aerial_brain_runner_duration_seconds`, `aerial_brain_runner_errors_total`, `aerial_brain_yield_trap_total`.
  - Classifier & Funnel: `aerial_brain_classifier_duration_seconds`, `aerial_brain_classifier_decisions_total`, `aerial_brain_classifier_confidence_score`, `aerial_brain_discord_events_total`.
  - Memory & Embeddings: `aerial_brain_memory_operations_total`, `aerial_brain_memory_search_duration_seconds`, `aerial_brain_embeddings_generated_total`, `aerial_brain_embedding_duration_seconds`.
  - Database Connection Pool: `aerial_brain_db_query_duration_seconds`, `aerial_brain_db_open_connections`, `aerial_brain_db_in_use_connections`, `aerial_brain_db_idle_connections`, `aerial_brain_db_wait_count_total`.
- **`synapse`** (Exposed on `:4020/metrics`):
  - Discord Gateway: `synapse_discord_gateway_latency_seconds`, `synapse_discord_events_received_total`.
  - River Queue: `synapse_queue_depth`, `synapse_jobs_completed_total`, `synapse_jobs_errors_total`.

---

## 4. Continuous Deployment, GitOps & Self-Improvement

Aerial uses an automated, event-driven GitOps continuous deployment pipeline:
1. **GitHub Actions Matrix Builds**: Triggers dynamic matrix builds for modified microservices (including dependents when shared `pkg/` changes) and publishes them to GitHub Container Registry (`ghcr.io/azylman/aerial-*`).
2. **Event-Driven Push GitOps Router (`synapse`)**: Receives authenticated GitHub push and CI webhooks through Cloudflare tunnels and `edge-gateway`, updates `ConfigMap/aerial-image-tags` in `flux-system` on image promotions, immediately triggers Flux CD reconciliation (`FLUX_RECEIVER_URL`), notifies `eri`, and advances `vanadis` build chunks.
3. **Flux CD & Kubernetes git-sync**: Flux CD reconciles `./k8s` Kustomizations for core services and user configurations, substituting pinned image tags via `postBuild.substituteFrom` without git commit churn. Dedicated Kubernetes `git-sync` sidecar initContainers (`registry.k8s.io/git-sync/git-sync:v4.8.0`) continuously sync `/share/aerial:ro` and `/share/aerial-config:ro` for `eri`.
4. **Physical Immutability & Asynchronous PR Workflow**: The `eri` execution pod mounts repositories strictly **read-only (`:ro`)**. When making code, configuration, persona, or peripheral adjustments across any repository, Aerial uses `scripts/aerial-pr.sh init [repo]` to initialize an ephemeral scratch directory, runs pre-flight syntax and verification checks (`./scripts/verify.sh --staged`), and submits asynchronously via `scripts/aerial-pr.sh submit` with native GitHub auto-merge (`SQUASH`) enabled and registered in PostgreSQL `pr_registry`.
5. **Event-Driven Continuous Delivery & Proactive CI Self-Healing**: Once submitted, turns terminate immediately with zero foreground CI polling. When CI turns green, GitHub automatically merges the PR and Flux CD deploys the updated manifests, posting confirmation directly to the registered Discord thread. If CI checks or `roborev` fail, `synapse` wakes `eri` with a proactive remediation prompt to checkout the branch, resolve the failure locally, verify, and push directly to the PR branch.

---

## 5. Component Modules

| Service | Port | Description |
| :--- | :--- | :--- |
| **`postgres`** | `5432` | CloudNativePG 2-instance PostgreSQL 18 + `pgvector` cluster with native `:9187` Prometheus metrics for production state, CAS task queues, semantic memory, PR registry, schedules, and Vanadis build queues. |
| **`eri`** | `8080` | Multi-protocol Go execution daemon running `agy`, PostgreSQL session memory, multiplexed Discord ingress from `synapse`, Voice (`/voice/ask`), HTTP (`/prompt`) ingress, System-1 triage via `pjev`, Prometheus metrics (`:8080/metrics`), and `SIGHUP` configuration hot-reloading. Mounted `:ro`. |
| **`synapse`** | `4020` | Ingress gateway and event-driven router owning Discord Gateway WebSocket, startup catch-up sweeps, River queues (`discord_ingress`, `discord_interaction`), and GitHub/Flux webhook dispatch. |
| **`haro`** | `4005` | Persistent scheduling, heartbeat, and event-watch orchestration service (`http://haro.aerial/mcp`) evaluating cron, one-shot, and event triggers and dispatching due turns to `eri`. |
| **`coven`** | `4008` | Semantic fact extraction, hybrid RRF memory search, ACT-R memory decay, and procedural skill synthesis MCP service (`http://coven.aerial/mcp`). |
| **`vanadis`** | `4025` | Autonomous multi-chunk build queue orchestrator backed by PostgreSQL and River with JIT symbol line-slice resolution. |
| **`prospera`** | `8080` | Autonomous Twilio media-streaming voice and telephone call agent. |
| **`coredns`** | `53` | Kubernetes CoreDNS daemon rewriting internal DNS queries (`*.aerial`, `*.lan` -> `*.default.svc.cluster.local`). |
| **`infisical`** | `8080` | Centralized secret management and automated rotation backed by Valkey 9 (`:6379`), dynamically syncing secrets into Kubernetes Secrets (`shared-secrets`). |
| **`valkey`** | `6379` | In-memory key-value cache backing Infisical session governance. |
| **`kubernetes-mcp`** | `8080` | Native Streamable HTTP MCP server for Kubernetes cluster orchestration, resource lifecycles, and pod diagnostics. |
| **`infisical-mcp`** | `8080` | Native Streamable HTTP MCP server for Infisical secret management and rotation. |
| **`discord-mcp`** | `8080` | Outbound Streamable HTTP MCP server providing Discord messaging, thread creation, and channel tools. |
| **`github-mcp`** | `8080` | Native Streamable HTTP GitHub MCP server with PAT authentication for PR, issue, and code operations. |
| **`victoriametrics-mcp`**| `8080` | VictoriaMetrics MCP server exposing metrics querying and alert inspection over Streamable HTTP. |
| **`kernel-mcp`** | `8080` | Headless browser automation and web interaction microservice. |
| **`docket-mcp`** | `8080` | Native Streamable HTTP MCP server (`http://docket-mcp.aerial/mcp`) for issue tracking, DAG triage, and hybrid RRF task search backed by Vikunja and PostgreSQL 18. |
| **`docket-store`** | `3456` | Vikunja issue tracking API service scheduled on `quiet-zero` backed by PostgreSQL 18 (`postgres:5432/docket`). |
| **`openobserve`** | `5080` | Cloud-native log analytics and Streamable HTTP MCP server (`/openobserve/api/default/mcp`) for structured log exploration and SQL search. |
| **`senses`** | `8880` / `10300` | Unified Rust CUDA ONNX Runtime inference service serving NeMo Parakeet TDT 0.6B v2 STT, Kokoro-82M TTS, BAAI/bge-small embeddings, and speaker verification. |
| **`pjev`** | `8000` | Local CUDA GGUF LLM inference service serving `/v1/systemone` ambient triage. |
| **`homepage`** | `3000` | Root landing portal and service discovery HUD. |
| **`dashboard`** | `8080` | Web status HUD rendering live queue state, schedules, memory, and turn health. |
| **`agentsview`** | `8080` | Web UI for visualizing agent transcripts, session history, and execution timelines. |
| **`docs`** | `8080` | Living documentation portal rendering Markdown and Mermaid diagrams via Docsify. |
| **`lan-gateway`** | `80` / `443` | NGINX Gateway Fabric ingress controller routing dynamic internal services and web dashboards. |
| **`edge-gateway`** | `4021` | Ingress gateway routing external Cloudflare tunnel webhooks into the local cluster mesh. |
| **`fluent-bit`** | DaemonSet | High-performance log collector shipping container stdout/stderr to OpenObserve. |
| **`node-exporter`** | `9100` | Node Exporter host telemetry gathering host CPU loads, memory, storage, thermals, and network metrics. |
| **`victoriametrics`** | `8428` | VictoriaMetrics single-node TSDB scraping Prometheus metrics from all pods and nodes with 5-year retention and dynamic `scrape.d/` config. |
| **`vmalert`** | `8880` | Prometheus-compatible alerting engine evaluating anomaly detection and metric threshold rules. |
| **`grafana`** | `3000` | Grafana visual dashboards serving system HUD & container metrics with SQLite backend and pre-provisioned dashboards. |
| **`github-runner`** | Replicas | Scale-to-zero Actions Runner Controller (ARC) `AutoscalingRunnerSet` HelmReleases executing CI matrices and automated RoboRev code reviews. |

---

## 6. Quickstart Setup

### Prerequisites
- Kubernetes cluster (k3s on NixOS 26.05 recommended) & Flux CD CLI
- Google Account for OAuth authentication (recommended to avoid API key rate limits) OR Gemini API Key
- Discord Bot Token (with Message Content and Server Members intents enabled)
- GitHub Personal Access Token (for private configuration repository synchronization)

### Step 1: Create Your Private Configuration Repository
1. Create a private repository on GitHub (e.g. `your-username/my-aerial-config`).
2. Copy or fork the template files from [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example) into your private repository.
3. Customize `config.yaml` and persona rules in `rules/` as desired.

### Step 2: Clone Aerial Engine
```bash
git clone https://github.com/azylman/aerial.git
cd aerial
```

### Step 3: Configure Infisical Secrets & Kubernetes Secrets
Secrets and credentials can be managed through Infisical and synced automatically to Kubernetes Secrets, or initialized directly via `kubectl` when cold-booting a cluster:
```bash
# Create shared secrets namespace and credentials:
kubectl create secret generic shared-secrets \
  --from-literal=DISCORD_BOT_TOKEN="your-discord-bot-token" \
  --from-literal=GITHUB_PAT="your-github-pat" \
  --from-literal=POSTGRES_PASSWORD="securepassword"
```

### Step 4: Deploy Kubernetes Stack via Flux CD / Kustomize
Core manifests are defined in `k8s/*.yaml` and reconciled by Flux CD:
```bash
# Apply Kustomization manifests directly or let Flux reconcile:
kubectl apply -k k8s/
```
On boot, `eri` mounts `/share/aerial` and `/share/aerial-config` populated continuously by `git-sync` initContainers and sidecars.

### Step 5: Authenticate via Google OAuth (Recommended)
By default, `eri` runs `agy` in Google OAuth / subscription mode:
1. Run the interactive `agy` CLI inside the `eri` pod shell:
   ```bash
   kubectl exec -it deployment/eri -c eri -- agy
   ```
2. Copy the displayed Google OAuth login URL into your web browser and sign in.
3. Paste the authorization code back into the terminal prompt and hit Enter.
4. Press `Ctrl+C` to exit once authenticated. `eri` stores the OAuth session token in `/data` and automatically refreshes access tokens in the background.

### Step 6: Verify Health
```bash
kubectl get pods -o wide
kubectl logs -f deployment/eri -c eri
```

---

## 7. Operational Commands & Endpoints

| Action / Service | URL / Command |
| :--- | :--- |
| Root Landing Portal (`homepage`) | `http://homepage.aerial/` |
| Status Dashboard (`dashboard`) | `http://dashboard.aerial/` |
| Documentation (`docs`) | `http://docs.aerial/` |
| Agent Transcripts (`agentsview`) | `http://agentsview.aerial/` |
| System Telemetry (`grafana`) | `http://grafana.aerial/` |
| `eri` Prometheus Metrics | `http://eri.aerial:8080/metrics` |
| `eri` Healthcheck | `http://eri.aerial:8080/health` |
| `synapse` Healthcheck | `http://synapse.aerial:4020/health` |
| List all cluster pods | `kubectl get pods` |
| View pod logs | `kubectl logs -f <pod-name>` |
| Reconcile Flux CD Kustomization | `flux reconcile kustomization aerial-core` |
| Check Flux CD status | `flux get kustomizations` |

---

## 8. Security & Best Practices

- **Multi-User Security & Admin Privilege Enforcement**: In shared or multi-user channels, messages from users are automatically checked against `admin_users` in `config.yaml`. Only authorized admins (`is_admin: true`) can modify system instructions, edit system configuration (`config.yaml`), manage Kubernetes workloads, or alter cron schedules.
- **Fail-Closed Default-Deny Server Containment**: Set `channels.default.mode: "ignore"` to contain Aerial exclusively to allowlisted channels on shared Discord servers.
- **Discord Funnel Hardening**: River transactional queue buffering on `synapse` guarantees message delivery across deployment restarts, thread ID deduplication recovery resolves Discord error 160004 race conditions seamlessly, and message staleness TTL is set to 30 minutes to prevent dropped messages during deployment bursts.
- **Zero Plaintext Tokens**: GitHub PATs and database secrets are passed in-memory ephemerally and never written to `.git/config` on disk.
- **Automated Token Redaction**: All subprocess logs, errors, and GitOps reconcile streams pass through multi-pattern token sanitizers (`pkg/sanitizer`) to redact sensitive credentials.
- **Infisical Secret Governance**: Secrets are centralized in Infisical and injected dynamically into Kubernetes Secrets (`shared-secrets`), eliminating plaintext `.env` files on disk.
- **CoreDNS Mesh Isolation**: Internal microservices communicate securely across cluster nodes via dynamic CoreDNS (`*.aerial`, `*.lan` -> `*.default.svc.cluster.local`), keeping traffic private to the cluster network.
