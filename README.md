# Aerial

A tightly integrated **self-hosted application platform** for your homelab, **autonomous software factory**, and **AI agent** written in Go and running on Kubernetes (`k3s`)—running out-of-the-box on a single node or scaling across multi-node clusters with specialized hardware roles (such as GPU inference, NVMe storage, and touch kiosks).

Where standard GitOps homelabs stop at cluster infrastructure, standalone coding orchestrators lack a deployment target, and chat assistants lack durable infrastructure, Aerial unifies all three into a closed loop—so **your agent can build, ship, and operate stateful applications on your homelab autonomously**:

1. **Self-Hosted Application Platform**: Goes beyond basic container hosting to give your homelab a declarative Kubernetes compute fabric and edge routing (**k3s**, **NGINX Gateway Fabric**, **CoreDNS**, **Infisical** secret governance), stateful distributed primitives (**Restate** durable execution, **NATS JetStream** event streaming, and **CloudNativePG PostgreSQL 18 + `pgvector`**), and batteries-included observability (**VictoriaMetrics**, **`vmalert`**, **VictoriaLogs**, **VictoriaTraces**, and **Grafana**).
2. **Autonomous Software Factory**: Runs a lights-out, spec-to-production delivery loop (*DAG Backlog → Multi-Agent Plan & Review → Durable Multi-PR Rollout → In-Cluster CI & LLM Review → Self-Healing Flux GitOps*) built directly on the platform's primitives—combining dependency-aware DAG issue tracking backed by **Vikunja**, multi-agent architecture and review panels, durable multi-PR build pipelines on **Restate** and **NATS JetStream**, scale-to-zero in-cluster **GitHub Actions Runner Controller (ARC)** runners with **RoboRev** automated code review, and event-driven **Flux CD** push GitOps with proactive CI self-healing.
3. **Multimodal AI Agent**: Connects your self-improving assistant directly to your homelab and software factory in the channels you already use—running a configurable multi-engine agent execution core (`agy`, Claude Code, and Codex), real-time Discord, voice, and **Twilio** PSTN telephony pipelines, persistent cron/heartbeat/event-watch scheduling, procedural skill synthesis and hybrid RRF semantic memory, and modular Streamable HTTP **Model Context Protocol (MCP)** tools.

---

## Tech Stack

Built in **Go (1.27)** and orchestrated as a resilient Kubernetes platform that scales from a single vanilla node to heterogeneous multi-node clusters:

### 1. Self-Hosted Application Platform
- **Cluster Orchestration**: **Kubernetes (`k3s`)** — Runs out-of-the-box on a single node or scales across multi-node clusters (with optional NixOS Colmena provisioning and Spegel P2P OCI registry), managing distributed daemons, batch jobs, specialized hardware nodes (such as GPU inference or touch kiosks), and rolling deployments.
- **Service Discovery & Ingress**: **NGINX Gateway Fabric** + **CoreDNS** — Declarative Kubernetes Gateway API routing for LAN and external edge webhook traffic paired with internal cluster DNS resolution.
- **Relational & Vector Persistence**: **CloudNativePG (PostgreSQL 18 + `pgvector`)** — High-availability PostgreSQL cluster with strict single-service table ownership, vector similarity indexing, and native Prometheus metrics.
- **Durable Workflow Execution**: **Restate Durable Execution** — Kubernetes Restate Operator managing stateful cluster instances backed by RocksDB for fault-tolerant, exactly-once workflow and virtual object state machines.
- **Event Streaming & Messaging**: **NATS JetStream** — High-performance durable event bus and message broker powering idempotent ingress, edge webhook fan-out, and asynchronous job queues.
- **Secrets Governance**: **Infisical + Valkey 9 + External Secrets Operator** — Centralized secret management and automated rotation syncing scoped per-service Kubernetes Secrets with zero plaintext disk persistence.
- **Observability & Telemetry**:
  - **Metrics**: **VictoriaMetrics** (TSDB) + **`vmalert`** + **Grafana** (dashboards) + Prometheus Exporters (**Kubelet / cAdvisor**, **Node Exporter**, **CloudNativePG**)
  - **Logs**: **Fluent Bit** (DaemonSet collection & transforms) + **VictoriaLogs** (LogsQL log analytics, 60-day retention)
  - **Traces**: **VictoriaTraces** (OTLP distributed trace store, 30-day retention)

### 2. Autonomous Software Factory
- **Issue & Dependency-Aware Backlog Tracking**: **Vikunja** — DAG task graph and backlog engine backed by PostgreSQL 18 for tracking multi-stage epics, dependencies, and deferred discoveries.
- **Durable Multi-PR Build Pipelines**: **Restate + NATS JetStream** — Stateful DAG build orchestration coordinating multi-chunk, cross-repository Pull Request rollouts and event-driven state transitions.
- **In-Cluster CI & Automated Code Review**: **GitHub Actions Runner Controller (ARC)** + **RoboRev** — Scale-to-zero self-hosted Kubernetes runner sets paired with automated LLM code review gates on every Pull Request.
- **Declarative Schema Migrations**: **Ariga Atlas** — Declarative SQL schema management, automated migration diffing, and checksum verification via Kubernetes jobs.
- **Self-Healing Push GitOps**: **Flux CD** — Event-driven GitOps controllers reconciling Kubernetes Kustomizations continuously on webhook push and image promotion events, paired with automated CI failure remediation.

### 3. Multimodal AI Agent
- **Configurable Agent Execution Engines**: **`agy`, Claude Code, and Codex** — Multi-engine execution core supporting subagent review panels, Subagent-Driven Development (SDD), and effort-tiered model routing.
- **Multimodal Communication Pipelines**: **Discord Gateway/REST, Real-Time Voice, and Twilio PSTN** — Bi-directional chat, low-latency voice audio pipelines (with optional STT/TTS sidecars), and autonomous outbound/inbound PSTN phone call and SMS orchestration.
- **Persistent Scheduling & Event Watches**: **Cron, Heartbeat & Event-Watch Engine** — PostgreSQL-backed scheduler evaluating recurring cron jobs, silent edge-triggered heartbeats, one-shot timers, and deterministic system event watches.
- **Semantic Memory & Procedural Skills**: **Hybrid RRF (`pgvector` + FTS) & ACT-R Memory** — Reciprocal Rank Fusion semantic fact retrieval, temporal decay scoring, and automatic procedural skill synthesis from completed workflows.
- **Browser Automation, Credentials & Agentic Commerce**: **Kernel Browser + 1Password CLI + Stripe Link (`@stripe/link-cli`)** — Headful CDP browser control via Kernel, scoped vault credential retrieval via 1Password CLI, and one-time virtual payment tokens via Stripe Link.
- **Defense-in-Depth Agent Guardrails**: **Read-Only Volume Mounts (`:ro`), Child-Process Secret Scrubbing & Pre-Tool Interceptors** — Live repositories, OAuth credentials, and secret bundles are mounted read-only at the container boundary to force changes through isolated scratch clones and reviewed Pull Requests; database credentials, GitHub PATs, Discord tokens, and API keys are stripped from the agent CLI's environment before spawning turns so the agent acts strictly through typed MCP servers; compiled Go pre-tool hooks inspect every `agy`, Claude Code, and Codex tool call before execution to block read-only or live-SSH mutations, scan staged git diffs for leaked secrets or private IPs, prevent foreground polling loops, and bound filesystem and telemetry queries; and all outbound messages and log streams pass through exact and derived-encoding secret redaction.
- **Session Observability**: **AgentsView** — Web transcript, session history, and execution trace inspector backed by PostgreSQL.

---

## 1. System Architecture & Topology

Aerial separates generic platform, software factory, and agent orchestration from private user state using a decoupled **Two-Repository Architecture**:
- **Engine Repo (`azylman/aerial`)**: Autonomous agent execution core (`eri`), scheduling & event watch orchestrator (`haro`), semantic memory & skill synthesis (`coven`), multi-chunk build orchestrator (`vanadis`), durable build DAG engine (`vanadis-restate`), issue & DAG backlog tracker (`docket-mcp` & `docket-store`), NATS JetStream broker (`nats`), Restate cluster orchestrator (`restate`), Discord Gateway & edge fact publisher (`synapse`), scale-to-zero CI & RoboRev runners (`github-runner`), shared Go packages (`pkg/` including `pkg/pubsub`), declarative Kubernetes manifests (`k8s/*.yaml`), turnkey telemetry stack (VictoriaMetrics, VictoriaLogs, VictoriaTraces, Grafana), PostgreSQL 18 persistence, CoreDNS service discovery, and outbound MCP microservices.
- **User Config Repo (e.g. `your-username/your-aerial-config`)**: Declarative user Kubernetes manifests and optional hardware sidecars (`k8s/*.yaml`), custom skills and automation runbooks (`custom-skills/`), custom Grafana dashboards (`services/grafana/dashboards/`), platform options (`config.yaml`), modular persona and runtime rules (`rules/`), and Discord channel policies (`channels/`). Starter template available at [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example).

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  MULTIPLEXED INGRESS LAYER                                  │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Voice & Telephony Ingress    │ HTTP REST & Event Webhooks   │ Discord Gateway Funnel        │
│ • /voice/ask Voice Pipeline  │ • /prompt (Prompt Injection) │ • synapse Gateway WebSocket   │
│ • Prospera PSTN Agent        │ • /discord/message Outbound  │ • Catch-up Sweeps & JetStream │
│ • Optional STT/TTS Sidecars  │ • /discord/widget Components │ • POST /discord/ingress       │
│ • Low-Latency Audio Routing  │ • Edge Gateway Webhooks      │ • Ambient Wake Classification │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    eri (Execution Core)                                     │
│  • Autonomous Subagent Orchestrator & SDD Planning Engine                                   │
│  • Self-Healing GitOps Worker (Proactive CI/CD & RoboRev Remediation)                       │
│  • Configurable Multi-Engine Execution Core: agy, Claude Code, Codex                        │
│  • Two-Tier Ambient Wake Classifier (Built-in LLM & Optional Local Sidecar Hook)            │
│  • PostgreSQL 18 Multi-Turn Thread Memory & Atomic CAS Task State                           │
│  • Deep Prometheus Telemetry Instrumentation (/metrics)                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│               SELF-HOSTED APPLICATION PLATFORM & SOFTWARE FACTORY (k3s)                     │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Platform Runtime & Primitives│ Autonomous Software Factory  │ Agent Core & Web HUDs         │
│ • k3s (1 to N nodes)         │ • vanadis Build API          │ • eri, haro, coven, prospera  │
│ • lan-gateway & edge-gateway │ • vanadis-restate DAG Worker │ • homepage, console, docs     │
│ • restate (RestateCluster)   │ • docket-mcp & docket-store  │ • agentsview Session HUD      │
│ • nats (NATS JetStream)      │ • synapse GitOps Fact Router │ • Optional GPU / Kiosk Nodes  │
│ • CloudNativePG Postgres 18  │ • github-runner (ARC/RoboRev)│   via aerial-config Overlays  │
│ • infisical, valkey, coredns │ • flux-system GitOps Engine  │                               │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                          AUTONOMOUS TOOL RUNTIME (MCP) & TELEMETRY                          │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Haro Scheduler MCP           │ Coven Memory MCP             │ VictoriaMetrics TSDB          │
│ Kubernetes MCP               │ Infisical Secrets            │ vmalert Recording/Alerts      │
│ GitHub Operations MCP        │ Discord REST MCP             │ VictoriaLogs                  │
│ Kernel Browser MCP           │ VictoriaMetrics MCP          │ VictoriaTraces OTLP           │
│ Prospera Phone Agent         │ Vanadis Build Queue          │ Grafana Cyberpunk HUD         │
│ Docket Issue Tracker         │ VictoriaLogs/Traces MCP      │ Fluent Bit Log DaemonSet      │
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
     - `classifier` (or `ambient`): Tier 1 wakes strictly on direct mentions and direct replies; Tier 2 ambient messages are scored (0.0 to 1.0) against recent channel context and thread history (supporting an optional in-cluster GPU classifier sidecar over `/v1/systemone` with built-in LLM fallback). Plaintext keywords do not trigger Tier 1 wakes.
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
- **`victorialogs`** (`http://victorialogs-mcp.aerial/mcp` - LogsQL log search, stats, and field discovery)
- **`victoriatraces`** (`http://victoriatraces-mcp.aerial/mcp` - Trace search, span timings, and service dependencies)
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
```

---

## 3. Observability & Telemetry Stack

Aerial includes a batteries-included, out-of-the-box observability matrix with single-node VictoriaMetrics TSDB, `vmalert`, SQLite-backed Grafana dashboards, deep Go Prometheus instrumentation, and dynamic modular scrape configurations.

```text
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                                 TELEMETRY PIPELINE                                    │
├────────────────────────────┬─────────────────────────────┬────────────────────────────┤
│ METRIC EXPORTERS           │ TIME-SERIES TSDB & ALERTS   │ VISUALIZATION & HUD        │
│ • eri                      │                             │                            │
│ • synapse                  │ victoriametrics             │ grafana                    │
│ • haro                     │ • 5-Year Retention          │ • Anonymous Admin HUD      │
│ • coven                    │ • Dynamic 15s Scrape Engine │ • Cyberpunk Permet Theme   │
│ • vanadis                  │ • Pod & cAdvisor Scrapes    │ • Core Telemetry Dashboard │
│ • CloudNativePG            │ vmalert                     │ • SQLite Storage Backend   │
│ • Kubelet / cAdvisor       │ • MAD Anomaly Detection     │ • victorialogs             │
│ • node-exporter            │ • Alert Rules Evaluation    │   LogsQL Logs (LAN-only)   │
└────────────────────────────┴─────────────────────────────┴────────────────────────────┘
```

### 1. VictoriaMetrics TSDB & vmalert (`victoriametrics`, `vmalert`)
- **Engine**: Single-node VictoriaMetrics running with 5-year retention (`-retentionPeriod=5y`) and 15s scrape interval, scraping cluster pods, Kubelet, and cAdvisor.
- **Declarative Operator Scrapes (`vm-operator`)**: VictoriaMetrics Operator dynamically reconciles `VMServiceScrape` and `VMStaticScrape` CRDs across cluster namespaces (`k8s/observability-and-mcp.yaml`) without manual config file mounts or container restarts.
- **Anomaly Detection & Recording Rules (`vmalert`)**: Evaluates Median Absolute Deviation (MAD) anomaly rules and alert definitions.

### 2. Pre-Provisioned Grafana Dashboards (`services/grafana/dashboards/`)
- **Single-Click Anonymous Admin Access**: Instant dashboard access without login friction.
- **Persistent Backend**: Dashboards and user settings persist in SQLite (`GF_DATABASE_TYPE=sqlite3`) with dashboards provisioned from JSON definitions.
- **Pre-Provisioned Dashboard Suite**:
  - **`⚡ Aerial Core & Operations` (`core-telemetry.json`)**: Live turn execution latency (p50/p90/p95/p99), token usage, active worker pool depth, CAS task states, runner error taxonomy, Discord gateway latency (`synapse_discord_gateway_latency_seconds`), JetStream queue metrics, classifier triage decisions, and GitSync reconcile runs.
  - **`🐘 PostgreSQL Overview` (`postgres-overview.json`)**: CloudNativePG active backends, connection pool state, buffer cache hit ratio (>99%), commits/rollbacks, tuple read/write velocity, and lock contention.
  - **`🐳 Cluster Pods & Containers Overview` (`docker-overview.json`)**: Per-container CPU %, working set memory curves, network RX/TX, and CFS CPU throttling periods via cAdvisor.
  - **`🖥️ Host System & Hardware Overview` (`host-system-overview.json`)**: Host CPU load breakdown, thermal sensors per core, RAM utilization, root disk space, and load averages from Node Exporter.
  - **`🪙 Token Telemetry & Channel Breakdown` (`token-usage.json`)**: Real-time and cumulative token consumption partitioned by Discord channel, 24-hour and 7-day velocity, burn rates, and channel share leaderboards.

### 3. Deep Go Prometheus Metrics Instrumentation
- **`eri`** (Exposed on `/metrics`):
  - Queue & Workers: `aerial_brain_active_workers`, `aerial_brain_queue_depth`, `aerial_brain_interrupted_turns_recovered_total`, `aerial_brain_daemons_active`, `aerial_brain_daemon_spawns_total`, `aerial_brain_daemon_prunes_total`.
  - Turns & Runner: `aerial_brain_turns_total`, `aerial_brain_turn_duration_seconds`, `aerial_brain_runner_executions_total`, `aerial_brain_runner_duration_seconds`, `aerial_brain_runner_errors_total`, `aerial_brain_yield_trap_total`.
  - Classifier & Funnel: `aerial_brain_classifier_duration_seconds`, `aerial_brain_classifier_decisions_total`, `aerial_brain_classifier_confidence_score`, `aerial_brain_discord_events_total`.
  - Memory & Embeddings: `aerial_brain_memory_operations_total`, `aerial_brain_memory_search_duration_seconds`, `aerial_brain_embeddings_generated_total`, `aerial_brain_embedding_duration_seconds`.
  - Database Connection Pool: `aerial_brain_db_query_duration_seconds`, `aerial_brain_db_open_connections`, `aerial_brain_db_in_use_connections`, `aerial_brain_db_idle_connections`, `aerial_brain_db_wait_count_total`.
- **`synapse`** (Exposed on `/metrics`):
  - Discord Gateway: `synapse_discord_gateway_latency_seconds`, `synapse_discord_events_received_total`.
  - JetStream Queue: `synapse_queue_depth`, `synapse_jobs_processed_total`, `synapse_job_retries_total`.

---

## 4. Autonomous Software Factory & GitOps Delivery

Aerial includes a complete, self-hosted software factory built on top of its platform primitives (`Restate`, `NATS JetStream`, and `PostgreSQL 18`):
1. **Backlog & Multi-Agent Architecture (`docket-mcp`, `eri`)**: Features and tasks are tracked as dependency graphs in **Docket** (`docket-mcp` + Vikunja), planned through Subagent-Driven Development (SDD), and audited by parallel multi-agent review panels before execution.
2. **Durable Multi-PR Build Pipelines (`vanadis`, `vanadis-restate`)**: Multi-PR and cross-repository rollouts are enqueued into **Vanadis**, where **`vanadis-restate`** (`BuildRouter` and `Build` Virtual Object on **Restate**) orchestrates ordered DAG chunks and standalone PRs over **NATS JetStream** (`aerial.pr.>`).
3. **Physical Immutability & Ephemeral Scratch Workspaces**: The `eri` execution pod mounts repositories strictly **read-only (`:ro`)**. Code, configuration, and manifest changes are authored in isolated scratch clones (`scripts/aerial-pr.sh init [repo]`), verified locally (`./scripts/verify.sh --staged`), and submitted asynchronously (`scripts/aerial-pr.sh submit` or `scripts/aerial-build.sh enqueue`) with GitHub auto-merge (`SQUASH`).
4. **In-Cluster CI & Automated Code Review (`github-runner`, `RoboRev`)**: Scale-to-zero Actions Runner Controller (`arc-controller`) runner sets execute dynamic CI build matrices, publish container images to GHCR (`ghcr.io/azylman/aerial-*`), and run automated **RoboRev** LLM code reviews on every PR.
5. **Event-Driven Push GitOps & Proactive Self-Healing (`synapse`, `Flux CD`)**: **`synapse`** ingests authenticated GitHub and CI webhooks via `edge-gateway`, updates `ConfigMap/aerial-image-tags` in `flux-system`, triggers immediate **Flux CD** Kustomization reconciliation, and publishes PR/CI/deployment facts onto **NATS JetStream** so **Vanadis** and **Restate** advance dependent DAG chunks. If CI checks or **RoboRev** fail, `synapse` wakes `eri` to proactively remediate the branch and push a verified fix.

---

## 5. Component Modules

| Service | Description |
| :--- | :--- |
| **`postgres`** | CloudNativePG PostgreSQL 18 + `pgvector` cluster with native Prometheus metrics for production state, CAS task queues, semantic memory, PR registry, schedules, and Vanadis build queues. |
| **`eri`** | Multi-protocol Go execution daemon running `agy`, PostgreSQL session memory, multiplexed Discord ingress from `synapse`, Voice (`/voice/ask`), HTTP (`/prompt`) ingress, two-tier ambient triage, Prometheus metrics (`/metrics`), and `SIGHUP` configuration hot-reloading. Mounted `:ro`. |
| **`synapse`** | Ingress gateway and event-driven router owning Discord Gateway WebSocket, startup catch-up sweeps, NATS JetStream queues (`discord_ingress`, `discord_interaction`), and GitHub/Flux webhook dispatch. |
| **`haro`** | Persistent scheduling, heartbeat, and event-watch orchestration service (`http://haro.aerial/mcp`) evaluating cron, one-shot, and event triggers and dispatching due turns to `eri`. |
| **`coven`** | Semantic fact extraction, hybrid RRF memory search, ACT-R memory decay, and procedural skill synthesis MCP service (`http://coven.aerial/mcp`). |
| **`vanadis`** | Multi-chunk build queue orchestrator serving HTTP/MCP APIs, JIT symbol line-slice resolution, and NATS JetStream consumers. |
| **`vanadis-restate`** | Restate durable execution SDK worker (`RestateDeployment`) hosting the `Build` Virtual Object and `BuildRouter` service with health/metrics endpoints. |
| **`nats`** | NATS 2 JetStream broker StatefulSet backing durable cluster streams (`AERIAL_EVENTS`, `AERIAL_JOBS`, `AERIAL_COALESCED`, `VISION_JOBS`). |
| **`restate`** | Kubernetes Restate Operator managing `RestateCluster` instances with RocksDB persistence. |
| **`prospera`** | Autonomous Twilio media-streaming voice and telephone call agent. |
| **`coredns`** | Kubernetes CoreDNS daemon rewriting internal DNS queries (`*.aerial`, `*.lan` -> `*.default.svc.cluster.local`). |
| **`infisical`** | Centralized secret management and automated rotation backed by Valkey 9, dynamically syncing secrets into Kubernetes Secrets (`shared-secrets`). |
| **`valkey`** | In-memory key-value cache backing Infisical session governance. |
| **`kubernetes-mcp`** | Native Streamable HTTP MCP server for Kubernetes cluster orchestration, resource lifecycles, and pod diagnostics. |
| **`infisical-mcp`** | Native Streamable HTTP MCP server for Infisical secret management and rotation. |
| **`discord-mcp`** | Outbound Streamable HTTP MCP server providing Discord messaging, thread creation, and channel tools. |
| **`github-mcp`** | Native Streamable HTTP GitHub MCP server with PAT authentication for PR, issue, and code operations. |
| **`victoriametrics-mcp`**| VictoriaMetrics MCP server exposing metrics querying and alert inspection over Streamable HTTP. |
| **`kernel-mcp`** | Headless browser automation and web interaction microservice. |
| **`docket-mcp`** | Native Streamable HTTP MCP server (`http://docket-mcp.aerial/mcp`) for issue tracking, DAG triage, and hybrid RRF task search backed by Vikunja and PostgreSQL 18. |
| **`docket-store`** | Vikunja issue tracking API service backed by PostgreSQL 18 (`postgres/docket`). |
| **`victorialogs`** | VictoriaLogs log store (60-day retention) ingesting Fluent Bit container and journald logs, queried with LogsQL. LAN-only; never exposed on the edge gateway. |
| **`victoriatraces`** | VictoriaTraces OTLP trace store (30-day retention) receiving gateway and service spans. LAN-only; never exposed on the edge gateway. |
| **`victorialogs-mcp`** / **`victoriatraces-mcp`** | Streamable HTTP MCP servers for LogsQL log search and trace lookup. |
| **`homepage`** | Root landing portal and service discovery HUD. |
| **`console`** | Web status HUD rendering live queue state, schedules, memory, and turn health. |
| **`agentsview`** | Web UI for visualizing agent transcripts, session history, and execution timelines. |
| **`docs`** | Living documentation portal rendering Markdown and Mermaid diagrams via Docsify. |
| **`lan-gateway`** | NGINX Gateway Fabric ingress controller routing dynamic internal services and web dashboards. |
| **`edge-gateway`** | Ingress gateway routing external Cloudflare tunnel webhooks into the local cluster mesh. |
| **`fluent-bit`** | High-performance log collector shipping container stdout/stderr and journald logs to VictoriaLogs. |
| **`node-exporter`** | Node Exporter host telemetry gathering host CPU loads, memory, storage, thermals, and network metrics. |
| **`victoriametrics`** | VictoriaMetrics single-node TSDB scraping Prometheus metrics from all pods and nodes with 5-year retention and declarative VMServiceScrape / VMStaticScrape discovery via vm-operator. |
| **`vmalert`** | Prometheus-compatible alerting engine evaluating anomaly detection and metric threshold rules. |
| **`grafana`** | Grafana visual dashboards serving system HUD & container metrics with SQLite backend and pre-provisioned dashboards. |
| **`github-runner`** | Scale-to-zero Actions Runner Controller (ARC) `AutoscalingRunnerSet` HelmReleases executing CI matrices and automated RoboRev code reviews. |

---

## 6. Turnkey Single-Node k3s Quickstart

Aerial is designed to boot out-of-the-box on a single vanilla k3s node with no custom node labels, hardware affinity, or external dependencies.

### Step 1: Install Single-Node k3s & Prepare Storage
Install vanilla k3s with Traefik disabled (NGINX Gateway Fabric provides ingress routing) and create the hostPath directory for persistent agent data:

```bash
# 1. Install single-node k3s without Traefik
curl -sfL https://get.k3s.io | sh -s - --disable traefik

# 2. Configure kubeconfig access
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config
export KUBECONFIG=~/.kube/config

# 3. Create hostPath directory for eri & agentsview agent memory and session data
sudo install -d -o 1000 -g 1000 /var/lib/aerial/brain-data
```

### Step 2: Install Cluster Operators
Aerial relies on standard Kubernetes operators for persistence, secrets, and ingress (note that `restate-operator`, `vm-operator`, `reloader`, and `arc-controller` are deployed declaratively via Flux CD `HelmRelease` manifests in `k8s/operators-and-storage.yaml`):

```bash
# 1. CloudNativePG (PostgreSQL 18 + pgvector operator)
kubectl apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.28/releases/cnpg-1.28.4.yaml

# 2. External Secrets Operator (Infisical secret synchronization)
helm repo add external-secrets https://charts.external-secrets.io
helm repo update
helm upgrade --install external-secrets external-secrets/external-secrets \
  --namespace external-secrets --create-namespace

# 3. NGINX Gateway Fabric (Gateway API implementation)
kubectl kustomize "https://github.com/nginx/nginx-gateway-fabric/config/crd/gateway-api/standard?ref=v2.4.2" | kubectl apply -f -
helm upgrade --install ngf oci://ghcr.io/nginx/charts/nginx-gateway-fabric \
  --namespace nginx-gateway --create-namespace
```

### Step 3: Fork Configuration & Bootstrap Secrets
1. Fork [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example) to create your private `aerial-config` repository (e.g. `your-username/my-aerial-config`).
2. Customize `config.yaml` with your Discord administrator ID (`admin_users`) and persona instructions.
3. Bootstrap cluster secrets. Pre-create the initial Kubernetes secrets so that CloudNativePG and microservices boot without waiting for Infisical sync:

```bash
# Set your core tokens and generate initial service credentials
export DISCORD_BOT_TOKEN="your-discord-bot-token"
export GITHUB_PAT="your-github-pat"
export POSTGRES_PASSWORD="$(openssl rand -hex 16)"
export AUTH_SECRET="$(openssl rand -hex 16)"
export ENCRYPTION_KEY="$(openssl rand -hex 16)"

# 1. CloudNativePG PostgreSQL credentials
kubectl create secret generic postgres-aerial-role \
  --type=kubernetes.io/basic-auth \
  --from-literal=username=aerial \
  --from-literal=password="$POSTGRES_PASSWORD"

kubectl create secret generic postgres-secret \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD"

# 2. Git-sync and ingress credentials
kubectl create secret generic git-sync-secret \
  --from-literal=GITHUB_PAT="$GITHUB_PAT"

kubectl create secret generic synapse-secret \
  --from-literal=DISCORD_BOT_TOKEN="$DISCORD_BOT_TOKEN" \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=GITHUB_PAT="$GITHUB_PAT"

kubectl create secret generic coven-secret \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=GITHUB_PAT="$GITHUB_PAT"

kubectl create secret generic eri-secret \
  --from-literal=DISCORD_BOT_TOKEN="$DISCORD_BOT_TOKEN" \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=GITHUB_PAT="$GITHUB_PAT" \
  --from-literal=GEMINI_HARNESS_API_KEY=""

kubectl create secret generic discord-mcp-secret \
  --from-literal=DISCORD_BOT_TOKEN="$DISCORD_BOT_TOKEN" \
  --from-literal=DISCORD_TOKEN="$DISCORD_BOT_TOKEN"

kubectl create secret generic vanadis-secret \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=GITHUB_PAT="$GITHUB_PAT"

# 3. Observability, runner, and MCP service secrets
kubectl create secret generic victoriametrics-secret \
  --from-literal=HA_METRICS_TOKEN=""

kubectl create secret generic prospera-secret \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=GEMINI_HARNESS_API_KEY="" \
  --from-literal=TWILIO_AUTH_TOKEN=""

kubectl create secret generic roborev-runner-secret \
  --from-literal=GITHUB_PAT="$GITHUB_PAT" \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD"

# 4. Infisical bootstrap (if using Infisical for secret governance)
kubectl create secret generic infisical-secret \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=AUTH_SECRET="$AUTH_SECRET" \
  --from-literal=ENCRYPTION_KEY="$ENCRYPTION_KEY"

kubectl create secret generic infisical-universal-auth \
  --from-literal=INFISICAL_CLIENT_ID="" \
  --from-literal=INFISICAL_CLIENT_SECRET=""

kubectl create secret generic infisical-mcp-secret \
  --from-literal=INFISICAL_CLIENT_ID="" \
  --from-literal=INFISICAL_CLIENT_SECRET=""

kubectl create secret generic shared-secrets \
  --from-literal=DISCORD_BOT_TOKEN="$DISCORD_BOT_TOKEN" \
  --from-literal=GITHUB_PAT="$GITHUB_PAT" \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD"
```

### Step 4: Deploy Kubernetes Stack via Flux CD
Deploy the stack using Flux CD to reconcile `azylman/aerial` (`aerial-core`) and your private configuration repository (`aerial-config`):

```bash
# 1. Install Flux CLI and controllers
curl -s https://fluxcd.io/install.sh | sudo bash
flux install

# 2. Configure Git authentication for your private configuration repository
flux create secret git github-auth \
  --namespace flux-system \
  --url=https://github.com/your-username/my-aerial-config.git \
  --username=git \
  --password="$GITHUB_PAT"

# 3. Apply your private config repository's Flux manifests from a local clone
# (In your fork's k8s/ directory, Kustomization/aerial-core sets:
#    postBuild:
#      substitute:
#        AERIAL_CONFIG_REPO_URL: "https://github.com/your-username/my-aerial-config.git")
git clone https://github.com/your-username/my-aerial-config.git
kubectl apply -k my-aerial-config/k8s
```

### Step 5: Authenticate via Google OAuth (Recommended)
By default, `eri` runs `agy` in Google OAuth / subscription mode:
1. Run the interactive `agy` CLI inside the `eri` pod shell:
   ```bash
   kubectl exec -it deployment/eri -c eri -- agy
   ```
2. Copy the displayed Google OAuth login URL into your web browser and sign in.
3. Paste the authorization code back into the terminal prompt and press Enter.
4. Press `Ctrl+C` to exit once authenticated. `eri` stores the OAuth session token in `/data` and automatically refreshes access tokens in the background.

### Step 6: Verify Pod Readiness & Service Health
Verify all core services are running and passing readiness healthchecks:

```bash
# Check pod rollout status
kubectl get pods -o wide

# Check core microservice health endpoints
kubectl exec deployment/eri -c eri -- curl -sf http://localhost/health
kubectl exec deployment/haro -- wget -qO- http://localhost/health
kubectl exec deployment/coven -- wget -qO- http://localhost/health
kubectl exec deployment/vanadis -- curl -sf http://localhost/health
kubectl exec deployment/synapse -- curl -sf http://localhost/health
kubectl exec deployment/vanadis-restate -- wget -qO- http://localhost/healthz
kubectl exec statefulset/nats -- wget -qO- http://localhost/healthz?js-enabled-only=true
kubectl exec deployment/console -- wget -qO- http://localhost/health
```

Once running, access the web dashboards directly via your node's IP or hostname:
- Root Landing Portal: `http://<node-ip>/`
- Operator Console: `http://<node-ip>/console/`
- System Telemetry (Grafana): `http://<node-ip>/grafana/`
- Logs and Traces: Grafana Explore (VictoriaLogs and VictoriaTraces datasources)
- Agent Transcripts (AgentsView): `http://<node-ip>/agentsview/`
- Living Documentation (Docsify): `http://<node-ip>/docs/`

---

## 7. Multi-Node Clustering & High-Availability Overrides

While `azylman/aerial`'s base `k8s/*.yaml` manifests are completely single-node compatible and host-agnostic, multi-node homelab clusters layer site-specific overrides declaratively in `azylman/aerial-config/k8s/cluster-overrides.yaml` using Flux Kustomization `spec.patches`.

### How Cluster Overrides Work
1. **Separation of Base & Site Overrides**: Base manifests in `aerial/k8s/` define minimal, portable single-node defaults (single-instance PostgreSQL, no `nodeSelector` constraints, HTTP-only Gateway routes).
2. **Declarative Patching via Flux**: In your private `aerial-config` repository, `Kustomization/aerial-core` applies patches targeting specific resources to layer multi-node and high-availability features without forking base manifests:
   - **PostgreSQL High Availability**: Scaling `Cluster/postgres` to `spec.instances: 2` with dedicated `affinity` and anti-affinity rules across storage nodes.
   - **Hardware & Node Pinning**: Attaching `nodeSelector` and taint/toleration rules to scale across specialized node roles (e.g. scheduling optional GPU inference sidecars on GPU nodes, database storage on NVMe nodes, or display workloads on dedicated touch kiosks).
   - **Custom TLS & Ingress**: Patching `Gateway/lan-gateway` with Let's Encrypt `spec.listeners` for HTTPS and private domain certificates, along with custom domain `HTTPRoute` rules.
   - **Storage Mounts**: Adding SMB or NFS CSI volumes (e.g. `nas-share`) for persistent shared storage across worker nodes.
   - **Resource Adoption & Zero-Downtime Handoff**: Moved cluster resources annotate `metadata.annotations["kustomize.toolkit.fluxcd.io/prune"]: "disabled"` to allow safe ownership transitions between GitOps Kustomizations.

---

## 8. Operational Commands & Endpoints

| Action / Service | URL / Command |
| :--- | :--- |
| Root Landing Portal (`homepage`) | `http://<node-ip>/` or `http://homepage.aerial/` |
| Operator Console (`console`) | `http://<node-ip>/console/` or `http://console.aerial/` |
| Documentation (`docs`) | `http://<node-ip>/docs/` or `http://docs.aerial/` |
| Agent Transcripts (`agentsview`) | `http://<node-ip>/agentsview/` or `http://agentsview.aerial/` |
| System Telemetry (`grafana`) | `http://<node-ip>/grafana/` or `http://grafana.aerial/` |
| Logs and Traces (`victorialogs`, `victoriatraces`) | Grafana Explore, or LAN-only vmui at `http://victorialogs.aerial/select/vmui/` and `http://victoriatraces.aerial/select/vmui/` |
| `eri` Prometheus Metrics | `http://eri.aerial/metrics` |
| `eri` Healthcheck | `http://eri.aerial/health` |
| `synapse` Healthcheck | `http://synapse.aerial/health` |
| `haro` Healthcheck | `http://haro.aerial/health` |
| `coven` Healthcheck | `http://coven.aerial/health` |
| `vanadis` Healthcheck | `http://vanadis.aerial/health` |
| `vanadis-restate` Healthcheck | `http://vanadis-restate.aerial/healthz` |
| `nats` JetStream Healthcheck | `http://nats.aerial/healthz?js-enabled-only=true` |
| List all cluster pods | `kubectl get pods -o wide` |
| View pod logs | `kubectl logs -f <pod-name>` |
| Reconcile Flux CD Kustomization | `flux reconcile kustomization aerial-core` |
| Check Flux CD status | `flux get kustomizations` |

---

## 9. Security & Best Practices

- **Multi-User Security & Admin Privilege Enforcement**: In shared or multi-user channels, messages from users are automatically checked against `admin_users` in `config.yaml`. Only authorized admins (`is_admin: true`) can modify system instructions, edit system configuration (`config.yaml`), manage Kubernetes workloads, or alter cron schedules.
- **Fail-Closed Default-Deny Server Containment**: Set `channels.default.mode: "ignore"` to contain Aerial exclusively to allowlisted channels on shared Discord servers.
- **Discord Funnel Hardening**: NATS JetStream durable queue buffering on `synapse` guarantees message delivery across deployment restarts, thread ID deduplication recovery resolves Discord error 160004 race conditions seamlessly, and message staleness TTL is set to 30 minutes to prevent dropped messages during deployment bursts.
- **Zero Plaintext Tokens**: GitHub PATs and database secrets are passed in-memory ephemerally and never written to `.git/config` on disk.
- **Automated Token Redaction**: All subprocess logs, errors, and GitOps reconcile streams pass through multi-pattern token sanitizers (`pkg/sanitizer`) to redact sensitive credentials.
- **Infisical Secret Governance**: Secrets are centralized in Infisical and injected dynamically into Kubernetes Secrets (`shared-secrets`), eliminating plaintext `.env` files on disk.
- **CoreDNS Mesh Isolation**: Internal microservices communicate securely across cluster nodes via dynamic CoreDNS (`*.aerial`, `*.lan` -> `*.default.svc.cluster.local`), keeping traffic private to the cluster network.
