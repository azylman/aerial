# Aerial System Architecture & Repository Guidelines

## 1. System Overview & Component Topology
This repository (`azylman/aerial`) is the root orchestration repository for the Aerial AI Assistant system, defining the standalone Docker Compose multi-container topology:

```text
                               ┌──────────────────────────┐
                               │     Discord Gateway      │
                               └──────────────────────────┘
                                             │ Mentions & Threads
                                             ▼
                               ┌──────────────────────────┐
                               │       aerial-brain       │ ◄──► SQLite WAL (/data/aerial.db)
                               │  - Built-in Gateway      │
                               │  - Queue Worker Pool     │
                               │  - Background Scheduler  │
                               │  - Semantic Memory RAG   │
                               └──────────────────────────┘
                                             │
                       ┌─────────────────────┼─────────────────────┐
                       │                     │                     │
         ┌─────────────▼─────────┐ ┌─────────▼─────────┐ ┌─────────▼─────────┐
         │     scheduler-mcp     │ │    discord-mcp    │ │    docker-mcp     │
         │ (Port 8080: Schedule) │ │(Port 4001: Discord│ │(Port 4002: Docker)│
         └───────────────────────┘ └───────────────────┘ └───────────────────┘
                       │                     │                     │
         ┌─────────────▼─────────┐ ┌─────────▼─────────┐ ┌─────────▼─────────┐
         │      github-mcp       │ │    aerial-ollama  │ │ aerial-agentsview │
         │ (Port 4003: GitHub)   │ │(Port 11434: Embed)│ │(Port 8089: Web UI)│
         └───────────────────────┘ └───────────────────┘ └───────────────────┘
                       │                     │                     │
         ┌─────────────▼─────────┐ ┌─────────▼─────────┐                   │
         │   aerial-watchtower   │ │  aerial-autoheal  │ ──────────────────┘
         │ (GHCR CD Supervisor)  │ │ (Health Supervisor│
         └───────────────────────┘ └───────────────────┘
```

1. **Execution Brain & Built-in Funnel** (`brain/`):
   - Execution runner executing headless Antigravity CLI (`agy`).
   - Integrated Discord Gateway worker (`brain/funnel.go`) with continuous typing indicator refresh.
   - SQLite-backed conversation mapping and message queue state machine (`/data/aerial.db`).
   - Background scheduler monitor evaluating due crons and one-shots every 30s.
   - Semantic memory RAG querying embeddings from `aerial-ollama`.

2. **Outbound Model Context Protocol (MCP) Microservices**:
   - `scheduler-mcp` (`scheduler-mcp/`): Persistent task scheduling server on port 8080.
   - `discord-mcp` (`discord-mcp/`): Discord API tools on port 4001.
   - `docker-mcp` (`docker-mcp/`): Container diagnostics via `supergateway` and `mcp/docker` on port 4002.
   - `github-mcp` (`github-mcp/`): Repository operations via `supergateway` and `github-mcp-server` on port 4003.

3. **Supervision & Observability**:
   - `ollama` (`docker/ollama`): Local embeddings on port 11434.
   - `agentsview`: Antigravity transcript and session visualizer on port 8089.
   - `watchtower`: Polls GHCR every 60s for automatic rolling container updates.
   - `autoheal`: Probes container healthchecks every 15s and auto-restarts unhealthy services.

## 2. Invariants & Architectural Rules
- **Continuous Delivery Invariant**: Stack updates are deployed strictly by committing and pushing to `origin/main` (or merging via PR). Watchtower handles container recreations out-of-band. Never run `docker compose` inside containers.
- **Zero-Bypass Verification Invariant**: Pre-commit verification is mandatory. Always run `./scripts/verify.sh` (or `scripts/verify.ps1` on Windows) to verify all linters, unit tests, and frontend syntax with exit code 0. Under NO circumstances may an agent or developer use `git commit --no-verify`, `git commit -n`, or `git push --no-verify`.
- **Zero In-Image MCPs**: All MCP servers must run as standalone network endpoints on `aerial-net`.
- **Private Bridge Networking**: All inter-service communication happens over `aerial-net` using container DNS names (`http://brain:8080`, `http://scheduler-mcp:8080`, `http://discord-mcp:4001`).
- **Secrets Isolation**: API keys, bot tokens, and PATs are configured via `.env` files and referenced via environment variables in `docker-compose.yml`.
