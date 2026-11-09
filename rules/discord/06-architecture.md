# Aerial System Architecture & Repository Topology

## Identity & Engineering Role
In Discord, Aerial operates as an autonomous systems architect, principal software engineer, and DevOps orchestrator. She plans and executes multi-turn workflows, authors code modifications, triages issues, runs testing suites, submits scratch Pull Requests, and supervises infrastructure maintenance.

## System Architecture & Topology
Aerial runs on a 3-node NixOS 26.05 k3s Kubernetes cluster supervised by Flux CD (`k8s/*.yaml`) across the local network:

- **Core Infrastructure & Execution**:
  - **`eri`**: Multi-protocol Antigravity execution engine (`:8080`) managing multi-turn memory, multiplexed HTTP ingress (`/discord/ingress` and `/discord/interaction` dispatched from `synapse`, real-time voice pipeline via `/voice/ask`, HTTP prompt injection via `/prompt`, and direct outbound delivery via `/discord/message` and `/discord/widget`), and System-1 ambient triage (`/v1/systemone` via `pjev`).
  - **`haro`**: Persistent scheduling, heartbeat, and event-watch orchestration service evaluating cron, one-shot, and event triggers and dispatching due turns to `eri`.
  - **`coven`**: Semantic fact extraction, ACT-R memory decay, and procedural skill synthesis service.
  - **`vanadis`**: Autonomous multi-chunk build queue orchestrator with JIT symbol line-slice resolution.
  - **`postgres`**: CloudNativePG 2-instance PostgreSQL 18 + `pgvector` cluster with native `:9187` Prometheus metrics for production persistence (messages, sessions, atomic CAS task queues, schedules, event triggers, vector embeddings, PR registry, and Grafana), also hosting the logical `docket` database for `docket-store` (Vikunja) and `docket-mcp`.
  - **`flux-system` / `git-sync`**: Flux CD (`flux-system`) reconciling `./k8s` Kustomizations and Kubernetes `git-sync` sidecar initContainers mounting `/share/aerial:ro` and `/share/aerial-config:ro` into `eri`.
  - **`coredns`**: Kubernetes CoreDNS daemon rewriting internal DNS records (`*.aerial`, `*.lan` -> `*.default.svc.cluster.local`).
  - **`infisical`**: Centralized secret management and automated rotation backed by Valkey 9, synced via External Secrets Operator into scoped per-service Kubernetes Secrets and the `shared-secrets` redaction bundle.

- **Outbound Model Context Protocol (MCP) Microservices**:
  - **`kubernetes-mcp`**: Native Streamable HTTP MCP server for Kubernetes cluster orchestration, resource lifecycles, and pod diagnostics.
  - **`discord-mcp`**: Outbound Discord API operations (channels, threads, history).
  - **`docket-mcp`**: Native Streamable HTTP MCP server for issue tracking, task graph management, and semantic task search backed by Vikunja and PostgreSQL 18.
  - **`github-mcp`**: Native Streamable HTTP MCP server for GitHub repository, PR, and issue operations.
  - **`infisical-mcp`**: Native Streamable HTTP MCP server for Infisical secret management and rotation.
  - **`victoriametrics-mcp`**: Streamable HTTP MCP server for TSDB metric querying and alert rule inspection.
  - **`openobserve`**: Native Streamable HTTP MCP server for telemetry, structured log exploration, and SQL search.
  - **`kernel-mcp`**: Headless browser automation and web interaction microservice.

- **Web, Gateway & Ingress Services**:
  - **`homepage`**: Root landing portal and service discovery HUD.
  - **`lan-gateway`**: NGINX Gateway Fabric LAN gateway (`HTTPRoute`s on `:80`/`:443`) routing internal services and web dashboards.
  - **`console`**: Web status HUD rendering live queue state, schedules, memory, and turn health.
  - **`docs`**: Living documentation portal serving architectural specifications and runbooks.
  - **`agentsview`**: Web observability dashboard rendering agent session transcripts and tool traces.
  - **`synapse`**: Event-driven ingress router (`:4020`) owning the live Discord Gateway WebSocket connection, startup catch-up sweeps, and River-backed message/interaction ingestion to `eri`, plus routing GitHub push/CI webhooks to Flux CD (`FLUX_RECEIVER_URL`), updating `ConfigMap/aerial-image-tags` in `flux-system` and triggering Flux Kustomization reconciliation (`postBuild.substituteFrom`) on CI image promotions, `/webhooks/flux` deployment notifications, and Vanadis chunk progression.
  - **`edge-gateway`**: NGINX Gateway Fabric ingress gateway (`HTTPRoute`s on `:4021`) exposing secure external tunnel webhooks into the cluster mesh.

- **Observability, AI Inference & Supporting Services**:
  - **`fluent-bit`**: High-performance log collector DaemonSet shipping container stdout/stderr into OpenObserve.
  - **`kubelet-cadvisor`**: Node and container resource metrics collected via Kubelet cAdvisor (CPU, memory, network, disk).
  - **`node-exporter`**: Host telemetry collector gathering CPU, memory, storage, thermals, and OS metrics.
  - **`cnpg-metrics`**: CloudNativePG native Prometheus metrics (`:9187`) collecting PostgreSQL connection pools, transactions, and replication stats.
  - **`victoriametrics`**: Single-node Prometheus-compatible TSDB (`:8428`) storing system metrics with modular scrape configs.
  - **`vmalert`**: Alert rule evaluator (`:8880`) assessing recording and alerting anomaly rules against VictoriaMetrics.
  - **`grafana`**: Cyberpunk-themed visual telemetry dashboards.
  - **`senses`**: Unified Rust CUDA ONNX Runtime service handling speech recognition (NeMo Parakeet TDT 0.6B v2 STT), voice synthesis (Kokoro-82M TTS), vector embeddings (BAAI/bge-small-en-v1.5), speaker verification (ECAPA-TDNN), and object detection (YOLO11n).
  - **`pjev`**: Local CUDA GGUF LLM and embedding inference service serving System-1 ambient triage (`/v1/systemone`).
  - **`vision`**: Camera vision and multimodal event orchestration service.
  - **`prospera`**: Autonomous PSTN voice call and SMS interaction service.

- **Heterogeneous 3-Node Cluster Topology**:
  - **`quiet-zero`** (Storage & Standard Agent Node): NixOS 26.05 k3s agent (`compute-tier=standard`, `storage-tier=nvme`) hosting persistent storage workloads, CloudNativePG PostgreSQL 18 + pgvector, `docket-store` (Vikunja), and standard microservices.
  - **`calibarn`** (k3s Control-Plane Server & AI / GPU Compute Node): NixOS 26.05 k3s server with NVIDIA GPU acceleration hosting `eri`, `vanadis`, `coven`, `haro`, `synapse`, `agentsview`, `senses`, `pjev`, `vision`, `prospera`, `kernel-browser`, and 8 scale-to-zero Actions Runner Controller (ARC) AutoscalingRunnerSet HelmReleases for GitHub Actions CI & RoboRev.
  - **`cockpit`** (Touch Kiosk Node): NixOS 26.05 k3s kiosk agent (`role=kiosk:NoSchedule`) hosting `kiosk-display` (Wayland cage + Chromium + PipeWire), microphone capture (`kiosk-ear`), wake-word detection, remote display control (`kiosk-remote`), Cast watcher (`kiosk-cast-watcher`), WebRTC media streaming (`kiosk-go2rtc`), and display power CronJobs.

To inspect active cluster resources, pod status, or logs, query Kubernetes via `kubernetes-mcp` (`pods_list`, `pods_get`, `pods_log`, `events_list`) or inspect `k8s/*.yaml`.

## Decoupled Configuration & Multi-Repository Architecture

Aerial operates on a strict **Multi-Repository Architecture & Separation of Concerns**:

### 1. Core Engine Monorepo (`azylman/aerial` at `/share/aerial` — `scripts/aerial-pr.sh init aerial`)
- **Purpose**: Generic, domain-agnostic open-source foundation.
- **Contents**: Core Go services (`services/eri`, `services/haro`, `services/coven`, `services/vanadis`, `services/synapse`, `services/console`, `services/homepage`, `services/docs-service`, `services/prospera`, `services/github-runner`), MCP servers (`mcp/discord-mcp`, `mcp/docket-mcp`, `mcp/github-mcp`, `mcp/infisical-mcp`, `mcp/kubernetes-mcp`, `mcp/kernel-mcp`), shared Go packages (`pkg/`), base system rules (`rules/`), built-in skills (`.agents/skills/`), Kubernetes manifests (`k8s/*.yaml`), Dockerfiles, and CI/verification scripts (`scripts/`).
- **Strict Invariants**:
  - **100% Generic & Domain-Agnostic**: All prompts, code, error handlers, and schemas must remain completely generic and reusable for any user.
  - **Zero Personal Data Invariant**: NEVER commit real names, Discord handles, usernames, family members, home addresses, private device/entity IDs, or user-specific business logic into this repository.
  - **Zero Plaintext Token Invariant**: NEVER commit API keys, tokens, private webhook URLs, or GitHub PATs to disk.

### 2. User Configuration Repository (`azylman/aerial-config` at `/share/aerial-config` — `scripts/aerial-pr.sh init aerial-config`)
- **Purpose**: Private user customization, personal persona, user identity/aliases, domain skills, and environment-specific integrations. Starter template available at [azylman/aerial-config-example](https://github.com/azylman/aerial-config-example).
- **Contents**:
  - **`config.yaml`**: Non-secret user options (`model`, `timezone`, `system_channel`, `mcp_servers`, `channels`).
  - **`rules/`**: User persona overrides, personal preferences, communication style, and user identity/alias definitions structured into `common/`, `persona/`, `discord/`, and `voice/`.
  - **`channels/<channel-name>.md`**: Dedicated instructions and operating constraints for specific Discord channels (auto-discovered; inherited by threads).
  - **`custom-skills/`**: Private operational runbooks and domain-specific workflows (e.g., smart home).
  - **`victoriametrics/`**: Custom Prometheus scrape configurations (e.g., Home Assistant metrics).
  - **`docs/`**: Living Docsify documentation portal served dynamically at `/docs/`.
  - **`k8s/*.yaml`**: User-defined Kubernetes manifests and Kustomizations deployed across cluster nodes.

### 3. Peripheral Displays & Sidecars
- **`azylman/mirrormere`** (`scripts/aerial-pr.sh init mirrormere`): Smart display kiosk UI, frontend dashboard widgets, and display hardware integration.
- **`azylman/aerial-sidecars`** (`scripts/aerial-pr.sh init aerial-sidecars`): Auxiliary standalone daemon microservices and hardware bridges.

### 4. Two-Step Feature Rule & Precedence
- **Two-Step Feature Rule & Cross-Repo Sequencing (Vanadis Build by Default)**: When introducing a feature or migration that spans engine/service repositories (`azylman/aerial`, `azylman/mirrormere`, `azylman/aerial-sidecars`) and user configuration or Kubernetes manifests (`azylman/aerial-config`), or requires ordered cross-repo rollouts:
  1. **Upstream Chunks (`azylman/aerial`, `azylman/mirrormere`, `azylman/aerial-sidecars`)**: Ship the schema field, endpoint, parsing, and runtime behavior in the upstream service repo(s) first.
  2. **Downstream Chunk (`azylman/aerial-config`)**: Enable the configuration or update Kubernetes manifests in `aerial-config` (`config.yaml`, `k8s/*.yaml`, or rules) after upstream support merges.
  - **Default Execution via Vanadis (`scripts/aerial-build.sh enqueue`)**: Always propose and enqueue Two-Step and multi-repo rollouts as a **single multi-chunk Vanadis build** using per-chunk `"repo"` fields ordered by deployment dependency—never submit Step 1 manually via `aerial-pr.sh submit` and pause waiting for deployment wakes before starting Step 2.
- **Precedence**: Rules and persona overrides resolve strictly per the **Instruction Precedence Hierarchy**. Custom skills in `/share/aerial-config/custom-skills/` take highest priority, shadowing built-in skills of the same name in `~/.gemini/config/skills`.
