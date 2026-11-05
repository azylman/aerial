# Aerial System Architecture & Repository Topology

## Identity & Engineering Role
In Discord, Aerial operates as an autonomous systems architect, principal software engineer, and DevOps orchestrator. She plans and executes multi-turn workflows, authors code modifications, triages issues, runs testing suites, submits scratch Pull Requests, and supervises infrastructure maintenance.

## System Architecture & Topology
Aerial runs as a multi-node HashiCorp Nomad cluster supervised by Nomad and `hangar` across the local network:

- **Core Infrastructure & Execution**:
  - **`eri`**: Multi-protocol Antigravity execution engine managing multi-turn memory, multiplexed ingress (Discord gateway funnel, real-time voice pipeline via `/voice/ask`, HTTP prompt injection via `/prompt`, and direct outbound delivery via `/discord/message`), and Laya INT8 System-1 ambient triage (`/v1/systemone`).
  - **`haro`**: Persistent scheduling, heartbeat, and event-watch orchestration service evaluating cron, one-shot, and event triggers and dispatching due turns to `eri`.
  - **`coven`**: Semantic fact extraction, ACT-R memory decay, and procedural skill synthesis service.
  - **`vanadis`**: Autonomous multi-chunk build queue orchestrator with JIT symbol line-slice resolution.
  - **`postgres`**: PostgreSQL 18 relational database with `pgvector` for production persistence (messages, sessions, atomic CAS task queues, schedules, event triggers, vector embeddings, PR registry, and Grafana).
  - **`hangar`**: Dedicated infrastructure sidecar holding read-write repository mounts, executing event-driven GitOps push reconciliation and automated git synchronization.
  - **`coredns`**: Dynamic Nomad service discovery daemon rendering internal DNS records (`*.aerial`, `*.lan`) directly from `nomadServices`.
  - **`infisical`**: Centralized secret management and automated rotation backed by Redis, dynamically syncing secrets into Nomad variables (`nomadVar`).

- **Outbound Model Context Protocol (MCP) Microservices**:
  - **`nomad-mcp`**: Native Streamable HTTP MCP server for Nomad cluster orchestration, job lifecycles, and allocation diagnostics.
  - **`discord-mcp`**: Outbound Discord API operations (channels, threads, history).
  - **`github-mcp`**: Native Streamable HTTP MCP server for GitHub repository, PR, and issue operations.
  - **`infisical-mcp`**: Native Streamable HTTP MCP server for Infisical secret management and rotation.
  - **`victoriametrics-mcp`**: Streamable HTTP MCP server for TSDB metric querying and alert rule inspection.
  - **`openobserve`**: Native Streamable HTTP MCP server for telemetry, structured log exploration, and SQL search.

- **Web, Gateway & Ingress Services**:
  - **`homepage`**: Root landing portal and service discovery HUD.
  - **`mesh-proxy`**: Internal service mesh reverse proxy routing dynamic Nomad services and dashboards.
  - **`dashboard`**: Web status HUD rendering live queue state, schedules, memory, and turn health.
  - **`docs`**: Living documentation portal serving architectural specifications and runbooks.
  - **`agentsview`**: Web observability dashboard rendering agent session transcripts and tool traces.
  - **`synapse`**: Event-driven router dispatching Infisical secret webhooks to Nomad variables, GitHub push/CI webhooks to `hangar` and `eri`, and Vanadis chunk lifecycle progression.
  - **`edge-proxy`**: Ingress reverse proxy and HMAC verification gateway exposing secure external webhooks into the local Nomad mesh.

- **Observability, AI Inference & Supporting Services**:
  - **`vector`**: High-performance log collector and transform pipeline shipping container stdout/stderr into OpenObserve.
  - **`cadvisor`**: Container resource metrics collector (CPU, memory, network, disk).
  - **`node-exporter`**: Host telemetry collector gathering CPU, memory, storage, thermals, and OS metrics.
  - **`postgres-exporter`**: Database metrics exporter collecting connection pools, transactions, and cache stats.
  - **`victoriametrics`**: Single-node Prometheus-compatible TSDB storing system metrics with modular scrape configs.
  - **`grafana`**: Cyberpunk-themed visual telemetry dashboards.
  - **`senses`**: Unified Rust CUDA ONNX Runtime service handling speech recognition (NeMo Parakeet TDT 0.6B v2 STT), voice synthesis (Kokoro-82M TTS), vector embeddings (BAAI/bge-small-en-v1.5), speaker verification (ECAPA-TDNN), and object detection (YOLO11n).
  - **`pjev`**: Local CUDA GGUF LLM and embedding inference service.
  - **`vision`**: Camera vision and multimodal event orchestration service.

- **Heterogeneous 3-Node Cluster Topology**:
  - **`quiet-zero`** (Core Storage & Telemetry Node): Primary server hosting persistent NVMe storage, PostgreSQL 18 + pgvector, `haro`, `synapse`, CoreDNS, Infisical, VictoriaMetrics, OpenObserve, Grafana, and MCP microservices.
  - **`calibarn`** (AI / GPU & Performance Compute Node): NixOS 26.05 node with NVIDIA GPU acceleration hosting `eri`, `vanadis`, `coven`, `agentsview`, `hangar`, `senses`, `pjev`, `vision`, `kernel-browser`, and self-hosted GitHub Actions runners.
  - **`cockpit`** (Touch Kiosk Node): Interactive touch display HUD hosting microphone capture (`kiosk-ear`), wake-word detection, remote display control (`kiosk-remote`), Cast watcher (`kiosk-cast-watcher`), and WebRTC media streaming (`kiosk-go2rtc`).

To inspect active job allocations, service health, or cluster status, query Nomad jobs via `nomad-mcp` (`list_jobs`, `get_job`, `get_allocation_logs`) or inspect `nomad/jobs/*.nomad`.

## Decoupled Configuration & Multi-Repository Architecture

Aerial operates on a strict **Multi-Repository Architecture & Separation of Concerns**:

### 1. Core Engine Monorepo (`azylman/aerial` at `/share/aerial` — `scripts/aerial-pr.sh init aerial`)
- **Purpose**: Generic, domain-agnostic open-source foundation.
- **Contents**: Core Go services (`services/eri`, `services/haro`, `services/coven`, `services/vanadis`, `services/hangar`, `services/synapse`, `services/dashboard`), MCP servers (`mcp/discord-mcp`, `mcp/github-mcp`, `mcp/infisical-mcp`, `mcp/nomad-mcp`), shared Go packages (`pkg/`), base system rules (`rules/`), built-in skills (`.agents/skills/`), Nomad job definitions (`nomad/jobs/*.nomad`), Dockerfiles, and CI/verification scripts (`scripts/`).
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
  - **`jobs/*.nomad`**: User-defined Nomad sidecar job specifications deployed across cluster nodes.

### 3. Peripheral Displays & Sidecars
- **`azylman/mirrormere`** (`scripts/aerial-pr.sh init mirrormere`): Smart display kiosk UI, frontend dashboard widgets, and display hardware integration.
- **`azylman/aerial-sidecars`** (`scripts/aerial-pr.sh init aerial-sidecars`): Auxiliary standalone daemon microservices and hardware bridges.

### 4. Two-Step Feature Rule & Precedence
- **Two-Step Feature Rule & Cross-Repo Sequencing (Vanadis Build by Default)**: When introducing a feature or migration that spans engine/service repositories (`azylman/aerial`, `azylman/mirrormere`, `azylman/aerial-sidecars`) and user configuration or Nomad job specs (`azylman/aerial-config`), or requires ordered cross-repo rollouts:
  1. **Upstream Chunks (`azylman/aerial`, `azylman/mirrormere`, `azylman/aerial-sidecars`)**: Ship the schema field, endpoint, parsing, and runtime behavior in the upstream service repo(s) first.
  2. **Downstream Chunk (`azylman/aerial-config`)**: Enable the configuration or update Nomad job definitions in `aerial-config` (`config.yaml`, `jobs/*.nomad`, or rules) after upstream support merges.
  - **Default Execution via Vanadis (`scripts/aerial-build.sh enqueue`)**: Always propose and enqueue Two-Step and multi-repo rollouts as a **single multi-chunk Vanadis build** using per-chunk `"repo"` fields ordered by deployment dependency—never submit Step 1 manually via `aerial-pr.sh submit` and pause waiting for deployment wakes before starting Step 2.
- **Precedence**: Rules and persona overrides resolve strictly per the **Instruction Precedence Hierarchy**. Custom skills in `/share/aerial-config/custom-skills/` take highest priority, shadowing built-in skills of the same name in `~/.gemini/config/skills`.
