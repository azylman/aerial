# Aerial

A tightly integrated **self-hosted application platform** for your homelab, **autonomous software factory**, and **AI agent** written in Go and running on Kubernetes (`k3s`)—running out-of-the-box on a single node or scaling across multi-node clusters with specialized hardware roles (such as GPU inference, NVMe storage, and touch kiosks).

Where standard GitOps homelabs stop at cluster infrastructure, standalone coding orchestrators lack a deployment target, and chat assistants lack durable infrastructure, Aerial unifies all three into a closed loop—so **your agent can build, ship, and operate stateful applications on your homelab autonomously**:

1. **Self-Hosted Application Platform**: Gives your homelab a declarative Kubernetes compute fabric, LAN and edge gateway routing, centralized secret management, durable workflow state machines, persistent event streaming, relational and vector storage, and unified metrics, logs, and distributed tracing.
2. **Autonomous Software Factory**: Runs a lights-out, spec-to-production delivery loop (*DAG Backlog → Multi-Agent Plan & Review → Durable Multi-PR Rollout → In-Cluster CI & LLM Review → Self-Healing Flux GitOps*) directly on the platform's primitives—taking features from dependency-tracked issues through isolated scratch Pull Requests, scale-to-zero in-cluster CI and automated code review, and push-driven GitOps rollouts with proactive CI failure remediation.
3. **Multimodal AI Agent**: Connects your assistant directly to your homelab and software factory over Discord, real-time voice, and phone/SMS—backed by persistent cron, heartbeat, and event-watch scheduling, hybrid semantic memory, and extensible **Model Context Protocol (MCP)** connectors and custom `SKILL.md` workflows.

---

## Tech Stack

| Layer | Technologies & Role |
| :--- | :--- |
| **Language & Cluster Runtime** | **Go 1.27** microservices on **Kubernetes (`k3s`)** (single-node out of the box or multi-node with optional **NixOS Colmena** provisioning and **Spegel** P2P OCI registry caching) |
| **Networking & Ingress** | **NGINX Gateway Fabric** (Kubernetes Gateway API for LAN apps and external edge webhooks) and **CoreDNS** (internal service discovery) |
| **Databases & State** | **CloudNativePG** (**PostgreSQL 18** + **`pgvector`** for relational state, vector embeddings, and hybrid RRF search) and **Valkey 9** (caching) |
| **Workflows & Event Bus** | **Restate** (RocksDB-backed durable execution for workflow and virtual object state machines) and **NATS JetStream** (durable event streams, webhook fan-out, and work-queue job consumers) |
| **Secrets Governance** | **Infisical** + **External Secrets Operator** (centralized vault syncing scoped Kubernetes Secrets in-memory with zero plaintext disk persistence) |
| **Observability** | **VictoriaMetrics** (TSDB) + **`vmalert`** (MAD anomaly and alert rules), **Fluent Bit** + **VictoriaLogs** (LogsQL log analytics, 60-day retention), **VictoriaTraces** (OTLP distributed tracing, 30-day retention), **Grafana**, **Headlamp** (Kubernetes/Flux UI), and **Prometheus Exporters** (**Kubelet / cAdvisor**, **Node Exporter**, **CloudNativePG**) |
| **Software Factory & GitOps** | **Vikunja** (DAG issue and backlog graph), **GitHub Actions Runner Controller (ARC)** (scale-to-zero in-cluster CI runners), **RoboRev** (automated LLM code review gate), **Ariga Atlas** (declarative SQL schema migrations), and **Flux CD** (webhook-driven push GitOps) |
| **Agent Core & Memory** | Configurable CLI execution engines (**`agy`**, **Claude Code**, **Codex**), **Hybrid RRF (`pgvector` + FTS) & ACT-R Memory** (semantic fact retrieval, temporal decay, and procedural skill synthesis), **Streamable HTTP MCP Servers**, **`SKILL.md` Workflows** (`agentskills.io`), and **AgentsView** (web transcript and trace inspector) |
| **Multimodal I/O & Browser Automation** | **Discord Gateway/REST**, **Real-Time Voice** (with optional STT/TTS sidecars), **Twilio PSTN** (inbound/outbound voice calls and SMS), **Kernel Browser** (headful CDP browser control), **1Password CLI** (scoped vault credential retrieval), and **Stripe Link** (`@stripe/link-cli` one-time virtual payment tokens) |

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

### Defense-in-Depth Agent Guardrails

- **Read-Only Repository & Credential Mounts (`:ro`)**: Live repositories (`/share/aerial`, `/share/aerial-config`), OAuth credentials, and secret bundles are mounted read-only at the container boundary, forcing all changes through isolated scratch clones and reviewed Pull Requests.
- **Child-Process Secret Scrubbing**: Database passwords, GitHub PATs, Discord tokens, and API keys are stripped from the child process environment before spawning `agy`, Claude Code, or Codex so the agent acts strictly through typed MCP servers.
- **Compiled Pre-Tool Hook Interceptors**: Go pre-tool hooks inspect every agent tool call before execution to block read-only or live-SSH mutations, scan staged git diffs for leaked secrets or private IPs, prevent foreground polling loops, and bound filesystem and telemetry queries.
- **Outbound & Log Secret Redaction**: Outbound Discord messages and log streams pass through exact and derived-encoding (`base64`, URL, hex, JSON-escaped) secret redaction.

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

### Step 1: Install Single-Node k3s
Install vanilla `k3s` with Traefik disabled (**NGINX Gateway Fabric** handles ingress routing) and user-accessible kubeconfig:

```bash
curl -sfL https://get.k3s.io | sh -s - --disable traefik --write-kubeconfig ~/.kube/config --write-kubeconfig-mode 600
```

### Step 2: Fork Configuration, Bootstrap Database Secrets & Deploy via Flux CD
1. Fork [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example) to create your private `aerial-config` repository (e.g. `your-username/my-aerial-config`).
2. Customize `config.yaml` with your Discord administrator ID (`admin_users`) and persona rules.
3. Pre-create only `postgres-aerial-role` and `infisical-secret`:

```bash
export GITHUB_PAT="your-github-pat"
export POSTGRES_PASSWORD="$(openssl rand -hex 16)"
export AUTH_SECRET="$(openssl rand -hex 16)"
export ENCRYPTION_KEY="$(openssl rand -hex 16)"

# 1. CloudNativePG PostgreSQL credentials
kubectl create secret generic postgres-aerial-role \
  --type=kubernetes.io/basic-auth \
  --from-literal=username=aerial \
  --from-literal=password="$POSTGRES_PASSWORD"

# 2. Infisical database and encryption credentials
kubectl create secret generic infisical-secret \
  --from-literal=AUTH_SECRET="$AUTH_SECRET" \
  --from-literal=ENCRYPTION_KEY="$ENCRYPTION_KEY"
```

4. Install Flux CLI and bootstrap your private configuration repository:

```bash
curl -s https://fluxcd.io/install.sh | sudo bash
GITHUB_TOKEN="$GITHUB_PAT" flux bootstrap github \
  --owner=your-username \
  --repository=my-aerial-config \
  --branch=main \
  --path=k8s \
  --personal
```

### Step 3: Connect Infisical & Populate Application Secrets
Once Flux boots Infisical, create `infisical-universal-auth` (`INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`) and write only `DISCORD_BOT_TOKEN` and `GITHUB_PAT` to Infisical:

```bash
# 1. External Secrets Operator Universal Auth credentials
kubectl create secret generic infisical-universal-auth \
  --from-literal=INFISICAL_CLIENT_ID="your-infisical-client-id" \
  --from-literal=INFISICAL_CLIENT_SECRET="your-infisical-client-secret"

# 2. Populate application secrets in Infisical
infisical secrets set \
  DISCORD_BOT_TOKEN="your-discord-bot-token" \
  GITHUB_PAT="$GITHUB_PAT"
```

### Step 4: Authenticate Agent CLI & Verify Rollout
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
