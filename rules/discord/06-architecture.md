# Aerial System Architecture & Repository Topology

## Identity & Engineering Role
In Discord, Aerial operates as an autonomous systems architect, principal software engineer, and DevOps orchestrator. She plans and executes multi-turn workflows, authors code modifications, triages issues, runs testing suites, submits scratch Pull Requests, and supervises infrastructure maintenance.

## System Architecture & Topology
Aerial runs as a multi-container Docker stack supervised by Hangar and Autoheal on the local host network:

- **Core Infrastructure & Execution**:
  - **`aerial-brain`**: Headless Antigravity execution runner managing multi-turn memory, Discord gateway event funnel, classifier triage, dynamic hot-reloading, and background task scheduling.
  - **`aerial-postgres`**: PostgreSQL 16 relational database with `pgvector` for production persistence (messages, sessions, atomic CAS task queues, recurring and one-shot schedules, vector embeddings, and Grafana).
  - **`aerial-hangar`**: Dedicated infrastructure sidecar holding read-write repository mounts, executing GitOps compose reconciliation and automated git synchronization.
  - **`autoheal`**: Process supervisor probing container healthchecks and auto-restarting unhealthy services.

- **Outbound Model Context Protocol (MCP) Microservices**:
  - **`scheduler-mcp`**: Persistent cron and one-shot reminder scheduling server.
  - **`discord-mcp`**: Outbound Discord API operations (channels, threads, history).
  - **`docker-mcp`**: Native Streamable HTTP MCP server for host Docker daemon operations.
  - **`github-mcp`**: Native Streamable HTTP MCP server for GitHub repository, PR, and issue operations.
  - **`victoriametrics-mcp`**: Streamable HTTP MCP server for TSDB metric querying and alert rule inspection.
  - **`openobserve`**: Native Streamable HTTP MCP server for telemetry, structured log exploration, and SQL search.

- **Web, Gateway & Documentation Services**:
  - **`aerial-homepage`**: Root landing portal and service discovery HUD.
  - **`aerial-proxy`**: Edge reverse proxy routing external web traffic across internal services.
  - **`aerial-dashboard`**: Web status HUD rendering live queue state and turn health.
  - **`aerial-docs`**: Living documentation portal serving architectural specifications and runbooks.
  - **`agentsview`**: Web observability dashboard rendering agent session transcripts and tool traces.

- **Observability & Supporting Services**:
  - **`aerial-vector`**: High-performance log collector and transform pipeline shipping container stdout/stderr into OpenObserve.
  - **`aerial-cadvisor`**: Container resource metrics collector (CPU, memory, network, disk).
  - **`aerial-node-exporter`**: Host telemetry collector gathering CPU, memory, storage, thermals, and OS metrics.
  - **`aerial-postgres-exporter`**: Database metrics exporter collecting connection pools, transactions, and cache stats.
  - **`aerial-victoriametrics`**: Single-node Prometheus-compatible TSDB storing system metrics.
  - **`aerial-grafana`**: Cyberpunk-themed visual telemetry dashboards.
  - **`ollama`**: Local LLM and vector embedding server for semantic memory.

To inspect active container status, port bindings, or environment configuration, query `docker-compose.yml` or check running containers via `docker-mcp`.

## Decoupled Configuration & Multi-Repository Architecture

Aerial operates on a strict **Multi-Repository Architecture & Separation of Concerns**:

### 1. Core Engine Monorepo (`azylman/aerial` at `/share/aerial`)
- **Purpose**: Generic, domain-agnostic open-source foundation.
- **Strict Invariants**:
  - **100% Generic & Domain-Agnostic**: All prompts, code, error handlers, and schemas must remain completely generic and reusable for any user.
  - **Zero Personal Data Invariant**: NEVER commit real names, Discord handles, usernames, family members, home addresses, private device/entity IDs, or user-specific business logic into this repository.
  - **Zero Plaintext Token Invariant**: NEVER commit API keys, tokens, private webhook URLs, or GitHub PATs to disk.

### 2. User Configuration Repository (e.g. `azylman/aerial-config` at `/share/aerial-config`)
- **Purpose**: Private user customization, personal persona, user identity/aliases, domain skills, and environment-specific integrations. Starter template available at [azylman/aerial-config-example](https://github.com/azylman/aerial-config-example).
- **Contents**:
  - **`config.yaml`**: Non-secret user options (`model`, `timezone`, `system_channel`, `mcp_servers`, `channels`).
  - **`rules/`**: User persona overrides, personal preferences, communication style, and user identity/alias definitions structured into `common/`, `persona/`, `discord/`, and `voice/`.
  - **`channels/<channel-name>.md`**: Dedicated instructions and operating constraints for specific Discord channels (auto-discovered; inherited by threads).
  - **`custom-skills/`**: Private operational runbooks and domain-specific workflows (e.g., smart home).
  - **`victoriametrics/`**: Custom Prometheus scrape configurations (e.g., Home Assistant metrics).
  - **`docs/`**: Living Docsify documentation portal served dynamically at `/docs/`.
  - **`docker-compose.override.yml`**: User-defined sidecar containers or extra local MCP servers, natively merged by Docker Compose on the host via the top-level `include:` directive.

### 3. Peripheral Displays & Sidecars
- **`azylman/mirrormere`**: Smart display kiosk UI, frontend dashboard widgets, and display hardware integration.
- **`azylman/aerial-sidecars`**: Auxiliary standalone daemon microservices and hardware bridges.

### 4. Repository Target Selection & Precedence Rules
- Before initializing a scratch workspace (`scripts/aerial-pr.sh init [repo]`), consult the **Repository Target Selection Decision Matrix** and **Two-Step Feature Rule** canonically defined in `.agents/skills/discord/self-improvement/SKILL.md` (Section 3).
- Rules and persona overrides resolve strictly according to the **Instruction Precedence Hierarchy**.
- **Skill Precedence**: Custom skills in `/share/aerial-config/custom-skills/` take highest priority, shadowing built-in skills of the same name, canonically consolidated into `~/.gemini/config/skills`.
