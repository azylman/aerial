# Aerial System Architecture & Repository Topology

## Identity & Engineering Role
In Discord, Aerial operates as an autonomous systems architect, principal software engineer, and DevOps orchestrator. She plans and executes multi-turn workflows, authors code modifications, triages issues, runs testing suites, submits scratch Pull Requests, and supervises infrastructure maintenance.

## System Architecture & Topology
Aerial runs on a 3-node NixOS 26.05 k3s Kubernetes cluster supervised by Flux CD (`k8s/*.yaml`) across the local network:

- **Core Infrastructure & Execution**:
  - **`eri`**: Stateless distroless HTTP ingress front door (`:8080`) handling `/internal/discord/message`, `/internal/discord/interaction`, `/voice/ask`, `/prompt`, and direct outbound delivery via `/discord/message`, paired with System-1 ambient triage (`/v1/systemone` via `pjev`).
  - **`eri-restate`**: Execution engine (`RestateDeployment` on `:9080` Restate SDK and `:4028` health & internal proxy) managing CLI engines (`agy`, `claude`, `codex`, `muse`) and executing durable Restate `ThreadSession`, `DispatchSession`, `VoiceSession`, `SessionManager`, `TurnRunner`, and LLM services.
  - **`haro`**: Persistent scheduling, heartbeat, and event-watch orchestration service (`:4005` HTTP/MCP) evaluating cron, one-shot, and event triggers, with embedded in-process `:9080` Restate server (`HaroSchedule`, `HaroEventWatch`).
  - **`coven`**: Semantic fact extraction, ACT-R memory decay, and procedural skill synthesis service (`:4008` HTTP/MCP), with embedded in-process `:9080` Restate server (`CovenThread`, `CovenDreamer`).
  - **`vanadis`**: Multi-chunk build queue orchestrator providing HTTP/MCP APIs (`:4025`) and JIT symbol line-slice resolution.
  - **`vanadis-nats`**: JetStream-to-Restate consumer bridge (`:4027`) forwarding events to `vanadis-restate`.
  - **`vanadis-restate`**: Build workflow engine (`RestateDeployment` on `:9080` Restate SDK and `:4026` health/metrics) executing durable Restate `Build`, `BuildRouter`, `PRObject`, and `RepoObject` DAG workflows, owning `FLUX_RECEIVER_URL`, updating `ConfigMap/aerial-image-tags`, publishing `aerial.pr.>` facts, and dispatching CI/RoboRev remediation wakes.
  - **`nats`**: NATS 2 JetStream broker (`StatefulSet` on `:4222`, HTTP monitoring `:8222`) backing durable cluster streams (`AERIAL_EVENTS`, `AERIAL_JOBS`, `AERIAL_COALESCED`, `AERIAL_TRANSCRIPTS`, `VISION_JOBS`) reconciled programmatically on startup via `pkg/pubsub`.
  - **`restate`**: Official Kubernetes Restate Operator (`restate-operator`) managing `RestateCluster/restate-green` (Restate 1.8.1-hotfix.2 in namespace `restate-green` with RocksDB state/journal storage on storage nodes).
  - **`postgres`**: CloudNativePG 2-instance PostgreSQL 19 + `pgvector` cluster (`Cluster/postgres-19`, `Service/postgres` with native `:9187` Prometheus metrics) for production persistence, hosting logical databases `aerial` (messages, sessions, dispatches, schedules, event triggers, vector embeddings), `grafana`, `docket` (Vikunja `docket-store` :3456 and `docket-mcp` :8080), and `infisical`.
  - **`flux-system` / `git-sync`**: Flux CD (`flux-system`) reconciling `./k8s` Kustomizations and Kubernetes `git-sync` sidecar initContainers mounting `/share/aerial:ro` and `/share/aerial-config:ro` into `eri`.
  - **`coredns`**: Kubernetes CoreDNS daemon rewriting internal DNS records (`restate.aerial` and `restate-green.aerial` -> `restate.restate-green.svc.cluster.local.`, `*.aerial` -> `*.default.svc.cluster.local.`).
  - **`infisical`**: Centralized secret management and automated rotation backed by Valkey 9, synced via External Secrets Operator into scoped per-service Kubernetes Secrets and the `shared-secrets` redaction bundle.

- **Outbound Model Context Protocol (MCP) Microservices**:
  - **`kubernetes-mcp`**: Native Streamable HTTP MCP server for Kubernetes cluster orchestration, resource lifecycles, and pod diagnostics.
  - **`discord-mcp`**: Outbound Discord API operations (channels, threads, history).
  - **`docket-mcp`**: Native Streamable HTTP MCP server for issue tracking, task graph management, and semantic task search backed by Vikunja and PostgreSQL 19.
  - **`github-mcp`**: Native Streamable HTTP MCP server for GitHub repository, PR, and issue operations.
  - **`infisical-mcp`**: Native Streamable HTTP MCP server for Infisical secret management and rotation.
  - **`victoriametrics-mcp`**: Streamable HTTP MCP server for TSDB metric querying and alert rule inspection.
  - **`victorialogs-mcp`**: Streamable HTTP MCP server for LogsQL log search, stats, and field discovery against VictoriaLogs.
  - **`victoriatraces-mcp`**: Streamable HTTP MCP server for trace search, span timings, and service dependencies against VictoriaTraces.
  - **`kernel-mcp`**: Headless browser automation and web interaction microservice.

- **Web, Gateway & Ingress Services**:
  - **`homepage`**: Root landing portal and service discovery HUD (`:3001`).
  - **`lan-gateway`**: NGINX Gateway Fabric LAN gateway (`HTTPRoute`s on `:80`/`:443`) routing internal services and web dashboards.
  - **`console`**: Web status HUD (`:8084`) rendering live queue state, schedules, memory, and turn health.
  - **`docs`**: Living documentation portal (`:80`) serving architectural specifications and runbooks.
  - **`agentsview`**: Web observability dashboard rendering agent session transcripts and tool traces.
  - **`synapse`**: Multi-bot Discord Gateway manager and stateless Restate edge forwarder (`:4020`) owning live Discord Gateway WebSocket connections, startup catch-up sweeps, and forwarding edge events to `eri` and `vanadis-restate` (`PRObject`/`RepoObject`).
  - **`edge-gateway`**: NGINX Gateway Fabric ingress gateway (`HTTPRoute`s on `:4021`) exposing secure external tunnel webhooks into the cluster mesh.
  - **`transcripts`**: Historical transcript search and indexing service (`:4030` + `agentsview-sync`).
  - **`coverage-ingest`**: Production code coverage ingest service (`:8080`).
  - **`headlamp`**: Web UI dashboard for Kubernetes cluster management (`:4466`).

- **Observability, AI Inference & Supporting Services**:
  - **`fluent-bit`**: High-performance log collector DaemonSet shipping container stdout/stderr and node journald logs into VictoriaLogs.
  - **`otel-collector`**: OpenTelemetry collector (`:4317`/`:4318`) with OTLP trace redaction forwarding spans to VictoriaTraces.
  - **`kubelet-cadvisor`**: Node and container resource metrics collected via Kubelet cAdvisor (CPU, memory, network, disk).
  - **`node-exporter`**: Host telemetry collector gathering CPU, memory, storage, thermals, and OS metrics.
  - **`cnpg-metrics`**: CloudNativePG native Prometheus metrics (`:9187`) collecting PostgreSQL connection pools, transactions, and replication stats.
  - **`victoriametrics`**: Single-node Prometheus-compatible TSDB (`:8428`) storing system metrics with declarative VMServiceScrape and VMStaticScrape discovery via vm-operator.
  - **`victorialogs`**: Log store (`:9428`, 60-day retention) for container and journald logs, queried with LogsQL. LAN-only (never on the edge gateway).
  - **`victoriatraces`**: OTLP trace store (`:10428`, 30-day retention) receiving gateway and service spans. LAN-only (never on the edge gateway).
  - **`vmalert`**: Alert rule evaluator (`:8880`) assessing recording and alerting anomaly rules against VictoriaMetrics.
  - **`grafana`**: PostgreSQL-backed visual telemetry dashboards.
  - **`senses`**: Unified Rust CUDA ONNX Runtime service handling speech recognition (NeMo Parakeet TDT 0.6B v2 STT), voice synthesis (Kokoro-82M TTS), vector embeddings (BAAI/bge-small-en-v1.5), and speaker verification (ECAPA-TDNN).
  - **`pjev`**: Local CUDA GGUF LLM and embedding inference service serving System-1 ambient triage (`/v1/systemone`).
  - **`vision`**: Camera vision and multimodal event orchestration service.
  - **`prospera`**: Autonomous PSTN voice call and SMS interaction service (`:8080` HTTP/MCP + embedded `:9080` Restate server `ProsperaReport`).
  - **`bazel-remote`**: LAN Bazel remote build cache (`:9090`/`:9092`) running on the storage node.

- **Heterogeneous 3-Node Cluster Topology**:
  - **`storage-node`** (Storage & Standard Agent Node): NixOS 26.05 k3s agent (`compute-tier=standard`, `storage-tier=nvme`) hosting persistent storage workloads, CloudNativePG PostgreSQL 19 + pgvector, `docket-store` (Vikunja), `nats` (NATS 2 JetStream StatefulSet), `restate` (RestateCluster), `bazel-remote`, and standard microservices.
  - **`control-plane-gpu-node`** (k3s Control-Plane Server & AI / GPU Compute Node): NixOS 26.05 k3s server with NVIDIA GPU acceleration hosting `eri`, `eri-restate`, `vanadis`, `vanadis-restate` (RestateDeployment), `vanadis-nats`, `coven`, `haro`, `synapse`, `agentsview`, `transcripts`, `coverage-ingest`, `senses`, `pjev`, `vision`, `prospera`, `kernel-browser`, and scale-to-zero Actions Runner Controller (ARC) AutoscalingRunnerSet HelmReleases for GitHub Actions CI & RoboRev.
  - **`kiosk-node`** (Touch Kiosk Node): NixOS 26.05 k3s kiosk agent (`role=kiosk:NoSchedule`) hosting `kiosk-display` (Wayland cage + Chromium + PipeWire), microphone capture (`kiosk-ear`), wake-word detection, remote display control (`kiosk-remote`), Cast watcher (`kiosk-cast-watcher`), WebRTC media streaming (`kiosk-go2rtc`), and display power CronJobs.

To inspect active cluster resources, pod status, or logs, query Kubernetes via `kubernetes-mcp` (`pods_list`, `pods_get`, `pods_log`, `events_list`) or inspect `k8s/*.yaml`.

## Decoupled Configuration & Multi-Repository Architecture

Aerial operates on a strict **Multi-Repository Architecture & Separation of Concerns**:

### 1. Core Engine Monorepo (`azylman/aerial` at `/share/aerial` — `scripts/aerial-pr.sh init aerial`)
- **Purpose**: Generic, domain-agnostic open-source foundation.
- **Contents**: Core Go services (`services/eri`, `services/eri-restate`, `services/transcripts`, `services/coverage-ingest`, `services/grafana`, `services/haro`, `services/coven`, `services/vanadis`, `services/vanadis-restate`, `services/vanadis-nats`, `services/synapse`, `services/console`, `services/homepage`, `services/docs-service`, `services/prospera`, `services/github-runner`), MCP servers (`mcp/discord-mcp`, `mcp/docket-mcp`, `mcp/infisical-mcp`, `mcp/kernel-mcp`), shared Go packages (`pkg/` including `pkg/pubsub` and `pkg/tracing`), base system rules (`rules/`), built-in skills (`.agents/skills/`), Kubernetes manifests (`k8s/*.yaml`), `MODULE.bazel`, `images/BUILD.bazel` (`rules_oci` image definitions), single root `go.mod`, retained Dockerfiles, and CI/verification scripts (`scripts/`).
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
  - **`services/`**: Per-service YAML configurations and custom Grafana dashboards.
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
