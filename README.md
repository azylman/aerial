# Aerial

A tightly integrated **self-hosted application platform** for your homelab, **autonomous software factory**, and **AI agent** written in Go and running on Kubernetes (`k3s`)—running out-of-the-box on a single node or scaling across multi-node clusters with specialized hardware roles (such as GPU inference, NVMe storage, and touch kiosks).

Where standard GitOps homelabs stop at cluster infrastructure, standalone coding orchestrators lack a deployment target, and chat assistants lack durable infrastructure, Aerial unifies all three into a closed loop—so **your agent can build, ship, and operate stateful applications on your homelab autonomously**:

1. **Self-Hosted Application Platform**: Goes beyond basic container hosting to give your homelab a declarative Kubernetes compute fabric and edge routing (**k3s**, **NGINX Gateway Fabric**, **CoreDNS**, **Infisical** secret governance), stateful distributed primitives (**Restate** durable execution, **NATS JetStream** event streaming, and **CloudNativePG PostgreSQL 18 + `pgvector`**), and batteries-included observability (**VictoriaMetrics**, **`vmalert`**, **VictoriaLogs**, **VictoriaTraces**, and **Grafana**).
2. **Autonomous Software Factory**: Runs a lights-out, spec-to-production delivery loop (*DAG Backlog → Multi-Agent Plan & Review → Durable Multi-PR Rollout → In-Cluster CI & LLM Review → Self-Healing Flux GitOps*) built directly on the platform's primitives—combining dependency-aware DAG issue tracking backed by **Vikunja**, multi-agent architecture and review panels, durable multi-PR build pipelines on **Restate** and **NATS JetStream**, scale-to-zero in-cluster **GitHub Actions Runner Controller (ARC)** runners with **RoboRev** automated code review, and event-driven **Flux CD** push GitOps with proactive CI self-healing.
3. **Multimodal AI Agent**: Connects your self-improving assistant directly to your homelab and software factory in the channels you already use—running a configurable multi-engine agent execution core (`agy`, Claude Code, and Codex), real-time Discord, voice, and **Twilio** PSTN telephony pipelines, persistent cron/heartbeat/event-watch scheduling, and procedural skill synthesis with hybrid RRF semantic memory, while letting you customize it to your needs by writing custom **Model Context Protocol (MCP)** connectors for the software you use and custom skills for your own workflows.

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
- **Custom Connectors & Workflow Skills**: **Streamable HTTP MCP Servers & Custom Skills (`SKILL.md`)** — Easily customized to your needs by writing custom MCP connectors for the software and APIs you use and custom progressive-disclosure skills for your own workflows.
- **Browser Automation, Credentials & Agentic Commerce**: **Kernel Browser + 1Password CLI + Stripe Link (`@stripe/link-cli`)** — Headful CDP browser control via Kernel, scoped vault credential retrieval via 1Password CLI, and one-time virtual payment tokens via Stripe Link.
- **Defense-in-Depth Agent Guardrails**: **Read-Only Volume Mounts (`:ro`), Child-Process Secret Scrubbing & Pre-Tool Interceptors** — Live repositories, OAuth credentials, and secret bundles are mounted read-only at the container boundary to force changes through isolated scratch clones and reviewed Pull Requests; database credentials, GitHub PATs, Discord tokens, and API keys are stripped from the agent CLI's environment before spawning turns so the agent acts strictly through typed MCP servers; compiled Go pre-tool hooks inspect every `agy`, Claude Code, and Codex tool call before execution to block read-only or live-SSH mutations, scan staged git diffs for leaked secrets or private IPs, prevent foreground polling loops, and bound filesystem and telemetry queries; and all outbound messages and log streams pass through exact and derived-encoding secret redaction.
- **Session Observability**: **AgentsView** — Web transcript, session history, and execution trace inspector backed by PostgreSQL.

---

## 1. System Architecture & Two-Repository GitOps

Aerial cleanly separates reusable platform, software factory, and agent infrastructure from your personal configuration using a decoupled **Two-Repository GitOps Architecture**:

- **Engine Repository (`azylman/aerial`)**: Generic, domain-agnostic foundation containing the Go microservices, MCP servers, base Kubernetes manifests, pre-provisioned Grafana dashboards, built-in engineering skills, and verification scripts.
- **User Configuration Repository (e.g. `your-username/your-aerial-config`)**: Private overlay repository holding your `config.yaml`, persona and runtime `rules/`, per-channel instructions (`channels/`), custom workflow skills (`custom-skills/`), and site-specific Kubernetes Kustomization patches (`k8s/`). Starter template available at [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example).

```text
azylman/aerial (Engine Repo)              your-aerial-config (Private Overlay Repo)
├── k8s/           Base k3s manifests     ├── config.yaml      Models, channels & MCP servers
├── services/      Core Go microservices  ├── rules/           Persona & runtime rules
├── mcp/           Streamable HTTP MCPs   ├── channels/        Per-channel markdown policies
├── pkg/           Shared Go libraries    ├── custom-skills/   User SKILL.md runbooks
├── rules/         Base engine invariants ├── services/        Service configs & dashboards
└── .agents/skills/ Built-in agent skills └── k8s/             Flux patches & extra workloads
```

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
│  • Configurable Multi-Engine Execution Core: agy, Claude Code, Codex                        │
│  • Autonomous Subagent Orchestrator & SDD Planning Engine                                   │
│  • Self-Healing GitOps Worker (Proactive CI/CD & RoboRev Remediation)                       │
│  • Read-Only Mounts (:ro), Child-Process Secret Scrubbing & Pre-Tool Hook Guardrails        │
│  • Two-Tier Ambient Wake Classifier & PostgreSQL 18 Multi-Turn Thread Memory                │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│               SELF-HOSTED APPLICATION PLATFORM & SOFTWARE FACTORY (k3s)                     │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Platform Runtime & Primitives│ Autonomous Software Factory  │ Agent Core & Web Surfaces     │
│ • k3s (1 to N nodes)         │ • vanadis Build API          │ • eri, haro, coven, prospera  │
│ • lan-gateway & edge-gateway │ • vanadis-restate DAG Worker │ • homepage, console, docs     │
│ • restate (RestateCluster)   │ • docket-mcp & docket-store  │ • agentsview Session HUD      │
│ • nats (NATS JetStream)      │ • synapse GitOps Fact Router │ • headlamp & infisical UIs    │
│ • CloudNativePG Postgres 18  │ • github-runner (ARC/RoboRev)│ • Optional GPU / Kiosk Nodes  │
│ • infisical, valkey, coredns │ • flux-system GitOps Engine  │   via aerial-config Overlays  │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                          AUTONOMOUS TOOL RUNTIME (MCP) & TELEMETRY                          │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Haro Scheduler MCP           │ Coven Memory MCP             │ VictoriaMetrics TSDB          │
│ Kubernetes MCP               │ Infisical Secrets MCP        │ vmalert Anomaly/Alert Rules   │
│ GitHub Operations MCP        │ Discord REST MCP             │ VictoriaLogs (LogsQL)         │
│ Kernel Browser MCP           │ VictoriaMetrics MCP          │ VictoriaTraces (OTLP)         │
│ Prospera Phone Agent         │ Vanadis Build Queue MCP      │ Grafana & Headlamp Dashboards │
│ Docket Issue Tracker MCP     │ VictoriaLogs/Traces MCP      │ Fluent Bit Log DaemonSet      │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
```

---

## 2. Closed-Loop Software Factory Workflow

Where standalone coding agent orchestrators run in local `tmux` sessions without a deployment target, Aerial runs a self-hosted, lights-out delivery pipeline directly on its Kubernetes platform primitives (**Restate**, **NATS JetStream**, and **PostgreSQL 18**):

```text
┌────────────────────┐    ┌────────────────────┐    ┌────────────────────┐    ┌────────────────────┐    ┌────────────────────┐
│  1. DAG BACKLOG    │    │ 2. PLAN & SCRATCH  │    │ 3. DURABLE BUILD   │    │ 4. CI & ROBOREV    │    │  5. PUSH GITOPS    │
│  Vikunja + Docket  ├───►│ Ephemeral Clone &  ├───►│  Restate + NATS    ├───►│ Scale-to-Zero ARC  ├───►│  Flux CD Reconcile │
│  Ready Queue & RRF │    │ Multi-Agent Review │    │ Multi-PR DAG Queue │    │ + LLM Review Gate  │    │  & Live Rollout    │
└────────────────────┘    └────────────────────┘    └────────────────────┘    └─────────┬──────────┘    └────────────────────┘
                                    ▲                                                   │
                                    └────────── Proactive Self-Healing Wake ────────────┘
                                                 (on CI or RoboRev failure)
```

1. **Dependency-Aware DAG Backlog (`docket-mcp` + Vikunja)**: Features, multi-stage epics, and deferred discoveries are tracked as a directed acyclic graph (DAG) in **Vikunja** backed by PostgreSQL 18 (`pgvector` + full-text hybrid RRF search), exposing an unblocked `ready` queue so the agent never loses context across sessions.
2. **Physical Immutability & Ephemeral Scratch Clones**: Live repositories (`/share/aerial`, `/share/aerial-config`) are mounted strictly **read-only (`:ro`)** inside the agent container. All code, schema, and manifest changes are authored in isolated scratch clones (`scripts/aerial-pr.sh init [repo]`), audited by parallel multi-agent review panels, and verified locally (`./scripts/verify.sh --staged`) before submission.
3. **Durable Multi-PR Build Pipelines (`vanadis` + `vanadis-restate`)**: Multi-PR and cross-repository rollouts (such as shipping an upstream engine feature before enabling downstream config) are enqueued as a single DAG build (`scripts/aerial-build.sh enqueue`). **Restate** durable Virtual Objects (`Build`, `BuildRouter`) and **NATS JetStream** (`aerial.pr.>`) coordinate ordered chunk execution and GitHub auto-merge (`SQUASH`).
4. **In-Cluster CI & Automated Code Review (`github-runner` + RoboRev)**: Scale-to-zero **GitHub Actions Runner Controller (ARC)** runner sets execute clean-room test/coverage matrices, publish container images to GHCR, and run automated **RoboRev** LLM code reviews on every Pull Request.
5. **Event-Driven Push GitOps & Self-Healing (`synapse` + Flux CD)**: Authenticated GitHub push and CI webhooks flow through `edge-gateway` to **`synapse`**, which updates `ConfigMap/aerial-image-tags` in `flux-system`, triggers immediate **Flux CD** Kustomization reconciliation, and publishes completion facts onto **NATS JetStream** to advance dependent build chunks. If CI checks or **RoboRev** fail, `synapse` automatically wakes the agent with the failure context to remediate the branch.

---

## 3. Built-In Web Interfaces (Glass-Box Operations)

Unlike black-box background daemons where state is hidden inside CLI logs or local files, Aerial pairs every layer of the platform, software factory, and agent with a built-in web interface routed through `lan-gateway`:

### 1. Platform & Infrastructure Views
- **Root Service Portal (`homepage`)** (`http://<node-ip>/`) — Unified landing dashboard with live service health monitors and status widgets, merging base defaults with user overrides from `aerial-config`.
- **Kubernetes & Flux GitOps Explorer (`headlamp`)** (`http://headlamp.aerial/`) — Read-only Kubernetes dashboard preloaded with `headlamp-plugin-flux` for inspecting pods, workloads, events, and **Flux CD** Kustomization/HelmRelease rollouts.
- **Secrets Governance UI (`infisical`)** (`http://infisical.aerial/`) — Web vault for managing environment secrets, machine identities, and automated rotation synced into Kubernetes via External Secrets Operator.
- **Restate Durable Execution UI (`restate`)** (`http://<node-ip>/restate/`) — Built-in **Restate** admin console for inspecting registered services (`Build`, `BuildRouter`), durable Virtual Object state in RocksDB, in-flight invocations, and execution journals.
- **Metrics, Logs & Traces (`grafana`, `victorialogs`, `victoriatraces`)** (`http://<node-ip>/grafana/`) — Single-click anonymous-admin **Grafana** with pre-provisioned system, PostgreSQL, container, and token dashboards, plus LAN-only `vmui` consoles at `http://victorialogs.aerial/select/vmui/` and `http://victoriatraces.aerial/select/vmui/`.

### 2. Software Factory Views
- **DAG Backlog & Issue Board (`docket-store` / Vikunja)** (`http://docket-store.aerial/`) — Visual Kanban, list, Gantt, and dependency board for browsing and managing the software factory's task graph.

### 3. AI Agent & Documentation Views
- **Operator Console (`console`)** (`http://<node-ip>/console/`) — Real-time mission control HUD rendering active agent turns, Vanadis multi-PR build progress, persistent schedules, and semantic memory facts.
- **Session & Trace Inspector (`agentsview`)** (`http://<node-ip>/agentsview/`) — Full multi-turn conversation transcripts, subagent execution trees, and tool call timelines backed by PostgreSQL.
- **Living Documentation Portal (`docs`)** (`http://<node-ip>/docs/`) — Docsify web portal rendering architectural specifications, Mermaid diagrams, and operational runbooks.
- **Headful Browser Viewer (`kernel-browser`)** — Live CDP browser session inspector when headful browser automation is deployed.

---

## 4. Customizing Aerial & Documentation

All user customization lives in your private config repository (start from [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example)):

| Goal | What's Covered | Start Here |
| :--- | :--- | :--- |
| **Configure models, channels & persona** | Effort-tiered models, Discord channel wake policies, admin allowlists, and persona rules | [`config.yaml` & `rules/`](https://github.com/azylman/aerial-config-example) |
| **Add custom workflow skills** | On-demand `SKILL.md` runbooks compatible with the `agentskills.io` standard | [`custom-skills/`](https://github.com/azylman/aerial-config-example/tree/main/custom-skills) |
| **Connect custom MCP servers** | Registering internal or remote Streamable HTTP MCP tools and secrets | [`mcp_servers` in `config.yaml`](https://github.com/azylman/aerial-config-example/blob/main/config.yaml) |
| **Intercept or enrich channel turns** | Lifecycle webhooks (`on_wake`, `pre_turn`, `post_turn`) for custom sidecars | [Channel Lifecycle Webhooks](docs/specs/2026-09-22-channel-lifecycle-webhooks.md) |
| **Deploy extra apps or scale nodes** | Adding Kubernetes workloads, HA Postgres, TLS, and GPU/kiosk node pinning | [`k8s/` Overlays](https://github.com/azylman/aerial-config-example/tree/main/k8s) |

---

## 5. Observability & Telemetry Stack

Aerial ships with a unified metrics, logs, and distributed tracing pipeline backed by **VictoriaMetrics**, **VictoriaLogs**, **VictoriaTraces**, and **Grafana**:

```text
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                                 TELEMETRY PIPELINE                                    │
├────────────────────────────┬─────────────────────────────┬────────────────────────────┤
│ METRIC, LOG & SPAN SOURCES │ STORAGE & ANOMALY ENGINE    │ VISUALIZATION & EXPLORERS  │
│ • Go Services (/metrics &  │ victoriametrics             │ grafana                    │
│   OTLP gRPC Spans)         │ • 5-Year Retention          │ • Anonymous Admin HUD      │
│ • NGINX Gateways (OTLP)    │ • Dynamic 15s VMScrape CRDs │ • Cyberpunk Permet Theme   │
│ • Restate (OTLP Spans)     │ vmalert                     │ • PostgreSQL Backend       │
│ • CloudNativePG Exporter   │ • MAD Anomaly & Alert Rules │ victorialogs & traces      │
│ • Kubelet / cAdvisor       │ victorialogs (60d LogsQL)   │ • Grafana Explore +        │
│ • node-exporter & fluent-bit│ victoriatraces (30d OTLP)  │   LAN-only vmui Consoles   │
└────────────────────────────┴─────────────────────────────┴────────────────────────────┘
```

- **Declarative Metrics & Anomaly Detection (`victoriametrics` + `vmalert`)**: Single-node VictoriaMetrics (5-year retention, 15s scrape interval) orchestrated by `vm-operator` (`VMServiceScrape`, `VMStaticScrape`, and `VMPodScrape` CRDs), paired with `vmalert` evaluating Median Absolute Deviation (MAD) anomaly scores and ingestion health alerts.
- **Secret-Scrubbed Log Analytics (`fluent-bit` + `victorialogs`)**: Fluent Bit DaemonSets collect container stdout/stderr and host `journald` logs, scrub exact and JSON-escaped Infisical secret values in-flight via Lua filters, and ship structured streams into VictoriaLogs (60-day retention, queried via LogsQL and `victorialogs-mcp`).
- **End-to-End Distributed Tracing (`victoriatraces`)**: NGINX gateways, Restate, and Go microservices export OpenTelemetry (OTLP) spans into VictoriaTraces (30-day retention), linked directly to VictoriaLogs by `trace_id` inside Grafana and queryable by the agent via `victoriatraces-mcp`.
- **Pre-Provisioned Grafana Dashboards (`services/grafana/dashboards/`)**:
  - **`⚡ Aerial Core & Operations` (`core-telemetry.json`)**: Turn latency percentiles (p50–p99), token velocity, active worker pools, CAS task states, Discord gateway latency, JetStream queues, and ambient classifier decisions.
  - **`🐘 PostgreSQL Overview` (`postgres-overview.json`)**: CloudNativePG active connections, buffer cache hit ratios, transaction velocity, and lock contention.
  - **`🐳 Cluster Pods & Containers Overview` (`docker-overview.json`)**: Per-container CPU, working set memory, network RX/TX, and CPU throttling via cAdvisor.
  - **`🖥️ Host System & Hardware Overview` (`host-system-overview.json`)**: Node CPU load, core thermals, memory, and disk utilization from Node Exporter.
  - **`🪙 Token Telemetry & Channel Breakdown` (`token-usage.json`)**: Real-time and cumulative token consumption partitioned by Discord channel.

---

## 6. Turnkey Single-Node k3s Quickstart & Multi-Node Scaling

Aerial boots out-of-the-box on a single vanilla `k3s` node with no custom node labels or hardware affinity required, and scales cleanly to multi-node clusters via declarative Flux patches.

### Step 1: Install Single-Node k3s & Prepare Storage
Install vanilla `k3s` with Traefik disabled (**NGINX Gateway Fabric** handles ingress routing) and create the hostPath directory for persistent agent session data:

```bash
# 1. Install single-node k3s without Traefik
curl -sfL https://get.k3s.io | sh -s - --disable traefik

# 2. Configure kubeconfig access
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config
export KUBECONFIG=~/.kube/config

# 3. Create hostPath directory for eri & agentsview session data
sudo install -d -o 1000 -g 1000 /var/lib/aerial/brain-data
```

### Step 2: Install Cluster Operators
Install the cluster operators for PostgreSQL, external secrets, and Gateway API routing (`restate-operator`, `vm-operator`, `reloader`, and `arc-controller` are deployed automatically via Flux CD `HelmRelease` manifests in `k8s/operators-and-storage.yaml`):

```bash
# 1. CloudNativePG (PostgreSQL 18 + pgvector operator). Install it before Flux so its
#    CRDs exist on first reconcile; the `cnpg` HelmRelease in k8s/operators-and-storage.yaml
#    then adopts this release and owns its values.
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm upgrade --install cnpg cnpg/cloudnative-pg --version 0.29.1 \
  --namespace cnpg-system --create-namespace

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
2. Customize `config.yaml` with your Discord administrator ID (`admin_users`) and persona rules.
3. Pre-create the initial Kubernetes secrets so CloudNativePG and core services boot immediately:

<details>
<summary><strong>Bootstrap Kubernetes Secrets (click to expand)</strong></summary>

```bash
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

# 2. Git-sync and core service credentials
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

</details>

### Step 4: Deploy Stack via Flux CD
Install **Flux CD** and apply your private configuration repository's Kustomizations:

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

# 3. Apply your private config repository's Flux manifests
git clone https://github.com/your-username/my-aerial-config.git
kubectl apply -k my-aerial-config/k8s
```

### Step 5: Authenticate Agent CLI & Verify Rollout
1. Authenticate your preferred execution engine (for example, `agy` via Google OAuth):
   ```bash
   kubectl exec -it deployment/eri -c eri -- agy
   ```
2. Verify all pods and Flux Kustomizations are healthy:
   ```bash
   kubectl get pods -o wide
   flux get kustomizations
   ```

### Scaling to Multi-Node Clusters (`aerial-config` Overlays)
Base manifests in `aerial/k8s/` use portable single-node defaults (single-instance PostgreSQL, no `nodeSelector` constraints, HTTP-only routes). To scale across a multi-node homelab without forking base manifests, layer Flux Kustomization `spec.patches` in `aerial-config/k8s/cluster-overrides.yaml`:
- **PostgreSQL High Availability**: Patch `Cluster/postgres` to `spec.instances: 2` with node anti-affinity across NVMe storage nodes.
- **Hardware & Role Pinning**: Attach `nodeSelector` and taint/toleration rules to schedule GPU inference sidecars on GPU nodes, databases on storage nodes, and display workloads on touch kiosks.
- **TLS & Custom Domains**: Patch `Gateway/lan-gateway` with Let's Encrypt HTTPS listeners and custom domain `HTTPRoute` rules.
- **Shared NAS Mounts**: Attach SMB or NFS CSI volumes across worker nodes.
