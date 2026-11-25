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
| **Databases & State** | **CloudNativePG** (**PostgreSQL 19** + **`pgvector`** for relational state, vector embeddings, and hybrid RRF search) and **Valkey 9** (caching) |
| **Workflows & Event Bus** | **Restate** (RocksDB-backed durable execution for workflow and virtual object state machines) and **NATS JetStream** (durable event streams, webhook fan-out, and work-queue job consumers) |
| **Secrets Governance** | **Infisical** + **External Secrets Operator** (centralized vault syncing scoped Kubernetes Secrets in-memory with zero plaintext disk persistence) |
| **Observability** | **VictoriaMetrics** (TSDB) + **`vmalert`** (MAD anomaly and alert rules), **Fluent Bit** + **VictoriaLogs** (LogsQL log analytics, 60-day retention), **VictoriaTraces** (OTLP distributed tracing, 30-day retention), **Grafana**, **Headlamp** (Kubernetes/Flux UI), and **Prometheus Exporters** (**Kubelet / cAdvisor**, **Node Exporter**, **CloudNativePG**) |
| **Software Factory & GitOps** | **Vikunja** (DAG issue and backlog graph), **GitHub Actions Runner Controller (ARC)** (scale-to-zero in-cluster CI runners), **RoboRev** (automated LLM code review gate), **Ariga Atlas** (declarative SQL schema migrations), and **Flux CD** (webhook-driven push GitOps) |
| **Agent Core & Memory** | Configurable CLI execution engines (**`agy`**, **Claude Code**, **Codex**, **Muse**), **Hybrid RRF (`pgvector` + FTS) & ACT-R Memory** (semantic fact retrieval, temporal decay, and procedural skill synthesis), **Streamable HTTP MCP Servers**, **`SKILL.md` Workflows** (`agentskills.io`), and **AgentsView** (web transcript and trace inspector) |
| **Multimodal I/O & Browser Automation** | **Discord Gateway/REST**, **Real-Time Voice** (with optional STT/TTS sidecars), **Twilio PSTN** (inbound/outbound voice calls and SMS), **Kernel Browser** (headful CDP browser control), **1Password CLI** (scoped vault credential retrieval), and **Stripe Link** (`@stripe/link-cli` one-time virtual payment tokens) |

---

## Single-Node k3s Quickstart

Aerial boots out-of-the-box on a single vanilla `k3s` node with no custom node labels or hardware affinity required, and scales cleanly to multi-node clusters via declarative Flux patches.

### Step 1: Install Single-Node k3s
Install vanilla `k3s` with Traefik disabled (**NGINX Gateway Fabric** handles ingress routing) and user-accessible kubeconfig:

```bash
curl -sfL https://get.k3s.io | sh -s - --disable traefik --write-kubeconfig ~/.kube/config --write-kubeconfig-mode 600
```

### Step 2: Fork Configuration & Bootstrap via Flux CD
1. Fork [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example) to create your private `aerial-config` repository (e.g. `your-username/my-aerial-config`).
2. Customize `config.yaml` with your Discord administrator ID (`admin_users`) and persona rules.
3. Install the Flux CLI and bootstrap your private configuration repository:

```bash
export GITHUB_PAT="your-github-pat"

curl -s https://fluxcd.io/install.sh | sudo bash
GITHUB_TOKEN="$GITHUB_PAT" flux bootstrap github \
  --owner=your-username \
  --repository=my-aerial-config \
  --branch=main \
  --path=k8s \
  --personal
```

### Step 3: Bootstrap Infisical & Populate Application Secrets
Once Flux boots Infisical, initialize the instance from the CLI (`infisical bootstrap` creates the admin account, organization, and Instance Admin Machine Identity and returns a **Token Auth** access token), store `INFISICAL_TOKEN` for **External Secrets Operator**, and populate your external application secrets in project `aerial-8-jn8` (`default` environment). External Secrets Operator automatically generates database, Infisical, VictoriaMetrics, NATS, and GitHub webhook (`GITHUB_WEBHOOK_SECRET`) credentials and syncs them into Infisical via `PushSecret`:

```bash
# 1. Bootstrap Infisical and store the returned Machine Identity Token Auth token
export INFISICAL_TOKEN="$(infisical bootstrap \
  --domain http://<node-ip> \
  --email admin@example.com \
  --password 'your-admin-password' \
  --organization aerial \
  | jq -r '.identity.credentials.token')"

kubectl create secret generic infisical-universal-auth \
  --from-literal=INFISICAL_TOKEN="$INFISICAL_TOKEN"

# 2. Populate external application secrets in Infisical (project slug aerial-8-jn8, default environment)
infisical secrets set --env=default \
  DISCORD_BOT_TOKEN="your-discord-bot-token" \
  GITHUB_PAT="$GITHUB_PAT"
```

### Step 4: Authenticate Agent CLI & Verify Rollout
1. Authenticate your preferred execution engine (for example, `agy` via Google OAuth) in the execution worker pod:
   ```bash
   kubectl exec -it $(kubectl get pod -l app.kubernetes.io/name=eri-restate -o name | head -n1) -c eri-restate -- agy
   ```
2. Verify all pods and Flux Kustomizations are healthy:
   ```bash
   kubectl get pods -o wide
   flux get kustomizations
   ```

### Scaling to Multi-Node Clusters (`aerial-config` Overlays)
Base manifests in `aerial/k8s/` use portable single-node defaults (single-instance PostgreSQL, no `nodeSelector` constraints, HTTP-only routes). To scale across a multi-node homelab without forking base manifests, layer Flux Kustomization `spec.patches` in `aerial-config/k8s/cluster-overrides.yaml`:
- **PostgreSQL High Availability**: Patch `Cluster/postgres-19` (fronted by `Service/postgres`) to `spec.instances: 2` with node anti-affinity across NVMe storage nodes.
- **Hardware & Role Pinning**: Attach `nodeSelector` and taint/toleration rules to schedule GPU inference sidecars on GPU nodes, databases on storage nodes, and display workloads on touch kiosks.
- **TLS & Custom Domains**: Patch `Gateway/lan-gateway` with Let's Encrypt HTTPS listeners and custom domain `HTTPRoute` rules.
- **Shared NAS Mounts**: Attach SMB or NFS CSI volumes across worker nodes.

---

## System Architecture & Two-Repository GitOps

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
│ • Optional STT/TTS Sidecars  │ • Edge Gateway Webhooks      │ • POST /internal/discord/     │
│ • Low-Latency Audio Routing  │                              │   message & interaction       │
│                              │                              │ • Ambient Wake Classification │
└──────────────────────────────┴──────────────────────────────┴───────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                       eri & eri-restate (Execution Core & Worker)                           │
│  • eri (:8080): Stateless distroless HTTP ingress front door                                │
│  • eri-restate (:9080/:4028): Multi-engine worker (agy, Claude Code, Codex, Muse)           │
│  • Durable Restate sessions (ThreadSession, DispatchSession, TurnRunner, VoiceSession)      │
│  • Autonomous Subagent Orchestrator & SDD Planning Engine                                   │
│  • Self-Healing GitOps Worker (Proactive CI/CD & RoboRev Remediation)                       │
│  • Read-Only Mounts (:ro), Child-Process Secret Scrubbing & Pre-Tool Hook Guardrails        │
│  • Two-Tier Ambient Wake Classifier & PostgreSQL 19 Multi-Turn Thread Memory                │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│               SELF-HOSTED APPLICATION PLATFORM & SOFTWARE FACTORY (k3s)                     │
├──────────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│ Platform Runtime & Primitives│ Autonomous Software Factory  │ Agent Core & Web Surfaces     │
│ • k3s (1 to N nodes)         │ • vanadis Build API (:4025)  │ • eri (:8080) & eri-restate   │
│ • lan-gateway & edge-gateway │ • vanadis-restate (:9080)    │ • haro, coven, prospera       │
│ • restate (RestateCluster)   │ • vanadis-nats bridge (:4027)│ • transcripts (:4030)         │
│ • nats (NATS JetStream)      │ • docket-mcp & docket-store  │ • coverage-ingest             │
│ • CloudNativePG Postgres 19  │ • synapse GitOps Fact Router │ • homepage, console, docs     │
│ • infisical, valkey, coredns │ • github-runner (ARC/RoboRev)│ • agentsview Session HUD      │
│ • otel-collector (:4317)     │ • flux-system GitOps Engine  │ • headlamp & infisical UIs    │
│ • bazel-remote cache         │                              │ • Optional GPU / Kiosk Nodes  │
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

- **Read-Only Repository & Credential Mounts (`:ro`)**: Live repositories (`/share/aerial`, `/share/aerial-config`), OAuth credentials, and secret bundles are mounted read-only at the container boundary, forcing all changes through isolated scratch clones (`scripts/aerial-pr.sh init`) and reviewed Pull Requests.
- **Child-Process Secret Scrubbing**: Database passwords, GitHub PATs, Discord tokens, and API keys are stripped from the child process environment before spawning `agy`, Claude Code, Codex, or Muse so the agent acts strictly through typed MCP servers.
- **Compiled Pre-Tool Hook Interceptors**: Go pre-tool hooks inspect every agent tool call before execution to block read-only or live-SSH mutations, scan staged git diffs for leaked secrets or private IPs, prevent foreground polling loops, and bound filesystem and telemetry queries.
- **Outbound & Log Secret Redaction**: Outbound Discord messages and log streams pass through exact and derived-encoding (`base64`, URL, hex, JSON-escaped) secret redaction.

---

## Closed-Loop Software Factory Workflow

Where standalone coding agent orchestrators run in local `tmux` sessions without a deployment target, Aerial runs a self-hosted, lights-out delivery pipeline directly on its Kubernetes platform primitives (**Restate**, **NATS JetStream**, and **PostgreSQL 19**):

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

1. **Dependency-Aware DAG Backlog (`docket-mcp` + Vikunja)**: Features, multi-stage epics, and deferred discoveries are tracked as a directed acyclic graph (DAG) in **Vikunja** backed by PostgreSQL 19 (`pgvector` + full-text hybrid RRF search), exposing an unblocked `ready` queue so the agent never loses context across sessions.
2. **Ephemeral Scratch Clones & Multi-Agent Review**: Code, schema, and manifest changes are authored in isolated scratch clones (`scripts/aerial-pr.sh init [repo]`), audited by parallel multi-agent review panels, and verified locally (`./scripts/verify.sh --staged`) before submission.
3. **Durable Multi-PR Build Pipelines (`vanadis` + `vanadis-restate`)**: Multi-PR and cross-repository rollouts (such as shipping an upstream engine feature before enabling downstream config) are enqueued as a single DAG build (`scripts/aerial-build.sh enqueue`). **Restate** durable Virtual Objects (`Build`, `BuildRouter`) and **NATS JetStream** (`aerial.pr.>`) coordinate ordered chunk execution and GitHub auto-merge (`SQUASH`).
4. **In-Cluster CI & Automated Code Review (`github-runner` + RoboRev)**: Scale-to-zero **GitHub Actions Runner Controller (ARC)** runner sets execute clean-room test/coverage matrices, publish container images to GHCR, and run automated **RoboRev** LLM code reviews on every Pull Request.
5. **Event-Driven Push GitOps & Self-Healing (`synapse` + Flux CD + `vanadis-restate`)**: Authenticated GitHub push and CI webhooks flow through `edge-gateway` to **`synapse`**, which forwards events to Restate `PRObject` and `RepoObject` on **`vanadis-restate`**. In turn, `vanadis-restate` updates `ConfigMap/aerial-image-tags` in `flux-system`, triggers immediate **Flux CD** Kustomization reconciliation, publishes `aerial.pr.>` facts onto **NATS JetStream** to advance dependent build chunks, and, if CI checks or **RoboRev** fail, wakes `eri-restate` with the failure context to remediate the branch.

---

## Built-In Web Interfaces (Glass-Box Operations)

Unlike black-box background daemons where state is hidden inside CLI logs or local files, Aerial pairs every layer of the platform, software factory, and agent with a built-in web interface. Core subpath views are routed out-of-the-box on `lan-gateway` (`http://<node-ip>/...`), while standalone root-path apps resolve in-cluster via CoreDNS (`*.aerial`) and can be exposed on your LAN via `HTTPRoute` hostnames or `kubectl port-forward`:

| Interface | Route / Service | Purpose |
| :--- | :--- | :--- |
| **Root Service Portal (`homepage`)** | `http://<node-ip>/` | Unified landing dashboard with live service health monitors and status widgets |
| **Operator Console (`console`)** | `http://<node-ip>/console/` | Real-time mission control HUD for active agent turns, Vanadis multi-PR builds, schedules, and memory facts |
| **Session & Trace Inspector (`agentsview`)** | `http://<node-ip>/agentsview/` | Full multi-turn conversation transcripts, subagent execution trees, and tool call timelines |
| **Living Documentation Portal (`docs`)** | `http://<node-ip>/docs/` | Docsify web portal rendering architectural specifications, Mermaid diagrams, and operational runbooks |
| **Restate Durable Execution UI (`restate`)** | `http://<node-ip>/restate/` | Built-in Restate admin console for `Build` / `BuildRouter` services, RocksDB virtual object state, and execution journals |
| **Metrics, Logs & Traces (`grafana`, `victorialogs`, `victoriatraces`)** | `http://<node-ip>/grafana/` & `Service/victoria*` (`/select/vmui/`) | Anonymous-admin Grafana with pre-provisioned system/Postgres/container/token dashboards, plus LogsQL and OTLP trace `vmui` consoles |
| **DAG Backlog & Issue Board (`docket-store` / Vikunja)** | `Service/docket-store` | Visual Kanban, list, Gantt, and dependency board for browsing and managing the software factory's task graph |
| **Kubernetes & Flux Explorer (`headlamp`)** | `Service/headlamp` | Read-only Kubernetes dashboard preloaded with `headlamp-plugin-flux` for pods, events, and Flux GitOps rollouts |
| **Secrets Governance UI (`infisical`)** | `Service/infisical` | Web vault for environment secrets, machine identities, and External Secrets Operator sync status |

---

## Customizing Aerial & Documentation

All user customization lives in your private config repository (start from [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example)):

| Goal | What's Covered | Start Here |
| :--- | :--- | :--- |
| **Configure models, channels & persona** | Effort-tiered models, Discord channel wake policies, admin allowlists, and persona rules | [`config.yaml` & `rules/`](https://github.com/azylman/aerial-config-example) |
| **Add custom workflow skills** | On-demand `SKILL.md` runbooks compatible with the `agentskills.io` standard | [`custom-skills/`](https://github.com/azylman/aerial-config-example/tree/main/custom-skills) |
| **Connect custom MCP servers** | Registering internal or remote Streamable HTTP MCP tools and secrets | [`mcp_servers` in `config.yaml`](https://github.com/azylman/aerial-config-example/blob/main/config.yaml) |
| **Intercept or enrich channel turns** | Lifecycle webhooks (`on_wake`, `pre_turn`, `post_turn`) for custom sidecars | [Channel Lifecycle Webhooks](rules/discord/02-channels.md) |
| **Deploy extra apps or scale nodes** | Adding Kubernetes workloads, HA Postgres, TLS, and GPU/kiosk node pinning | [`k8s/` Overlays](https://github.com/azylman/aerial-config-example/tree/main/k8s) |
