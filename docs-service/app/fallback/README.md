# 🛸 Aerial Documentation Matrix

Welcome to **Aerial Docs**, your personal living documentation and architecture portal with native **Mermaid.js** diagram rendering!

> [!NOTE]
> **Dynamic User Documentation**: This portal dynamically serves Markdown (`.md`) files from your private configuration repository (`aerial-config/docs/`).

---

## ⚡ System Architecture Topology

```mermaid
flowchart TB
    subgraph Ingress ["🌐 Ingress & Edge Layer"]
        User(["Discord User / Gateway"])
        Browser(["Web Browser / Client"])
        Proxy["aerial-proxy (:8089)
Nginx Edge Gateway"]
    end

    subgraph Core ["🧠 Core Execution Daemon"]
        Brain["aerial-brain (:8080 -> host:8088)
• Headless agy Agent Runner
• In-Process Discord Funnel
• Fast Ambient Classifier
• Recursive File Watcher
• :ro Repositories Mount"]
    end

    subgraph Persistence ["💾 Persistence & Memory"]
        Postgres[("aerial-postgres (:5432)
• PostgreSQL 16 + pgvector
• Atomic CAS Task Queue
• Vector RAG (384-dim)
• Schedules & Grafana DB")]
        Ollama["aerial-ollama (:11434)
• all-minilm Embeddings"]
    end

    subgraph MCP ["🔌 Model Context Protocol (MCP)"]
        Scheduler["aerial-scheduler-mcp (:8080)
PostgreSQL Cron & Reminders"]
        Discord["aerial-discord-mcp (:4001)
Outbound Discord API"]
        DockerHost["aerial-docker-mcp (:4002)
Native in-image Docker MCP"]
        GitHub["aerial-github-mcp (:4003)
Native in-image GitHub MCP"]
    end

    subgraph WebServices ["📊 Web & Documentation Microservices"]
        Dashboard["aerial-dashboard (:8080)
Cyberpunk Status HUD"]
        Docs["aerial-docs (:80)
Docsify + Mermaid Engine"]
        Agentsview["aerial-agentsview (:8080)
Transcript Visualizer"]
    end

    subgraph GitOps ["🔄 GitOps & Continuous Deployment"]
        GitSync["aerial-gitsync (:8080)
• :rw Repository Mounts
• Singleflight Git Sync
• Declarative Compose Reconciler"]
        Watchtower["aerial-watchtower
GHCR Image Auto-Updater"]
        Autoheal["aerial-autoheal
Healthcheck Supervisor"]
    end

    subgraph Observability ["📈 Full-Stack Observability"]
        Victoria["aerial-victoriametrics (:8428)
Single-Node TSDB (5y Retention)
Modular scrape.d/*.yml"]
        Grafana["aerial-grafana (:3000)
PostgreSQL Backend
Cyberpunk Telemetry HUD"]
        CAdvisor["aerial-cadvisor (:8080)
Container Metrics"]
        NodeExp["aerial-node-exporter (:9100)
Host System Metrics"]
        PGExp["aerial-postgres-exporter (:9187)
Database Pool Metrics"]
    end

    %% Ingress routing
    User -->|Events & Mentions| Brain
    Browser -->|HTTP Requests| Proxy
    Proxy -->|/ -> 302 Redirect| Dashboard
    Proxy -->|/dashboard/| Dashboard
    Proxy -->|/docs/| Docs
    Proxy -->|/conversations/| Agentsview
    Proxy -->|/grafana/| Grafana

    %% Core interactions
    Brain -->|Atomic CAS / Sessions / Memory| Postgres
    Brain -->|Generate Embeddings| Ollama
    Brain -->|Tools Call| Scheduler
    Brain -->|Tools Call| Discord
    Brain -->|Tools Call| DockerHost
    Brain -->|Tools Call| GitHub
    Brain -->|/metrics| Victoria

    %% Persistence
    Scheduler --> Postgres
    Grafana -->|Dashboard Persistence| Postgres
    PGExp --> Postgres

    %% GitOps
    GitSync -->|docker compose up -d| Brain
    GitSync -->|/metrics| Victoria

    %% Observability Scrapes
    Victoria -->|Scrapes| CAdvisor
    Victoria -->|Scrapes| NodeExp
    Victoria -->|Scrapes| PGExp
    Victoria -->|Scrapes| Brain
    Victoria -->|Scrapes| GitSync
    Grafana -->|PromQL Queries| Victoria
```

---

## 📂 Recommended Configuration Directory Structure

In your `aerial-config` repository:

```text
aerial-config/
├── config.yaml               # Core agent & channel policies
├── AGENTS.md                 # Persona & communication guidelines
├── channels/                 # Per-channel operational instructions
│   └── lounge.md
├── victoriametrics/          # Modular Prometheus scrape configs
│   └── homeassistant.yml
└── docs/                     # Living Docsify documentation portal
    ├── README.md             # Docs homepage
    ├── _sidebar.md           # Sidebar navigation
    ├── architecture.md       # Multi-container topology
    ├── observability.md      # TSDB, metrics & Grafana
    ├── gitops.md             # Continuous delivery & PR workflow
    ├── channels.md           # Discord interaction & wake modes
    └── security.md           # Admin allowlist & kernel security
```

---

## 🔍 Features Included
- **Zero-Build Hot Reloading**: Any commit or sync to `aerial-config` is immediately live in your browser.
- **Mermaid.js Engine**: Sequence diagrams, flowcharts, and architecture maps rendered in real time.
- **Offline / Local First**: 100% vendored assets with zero external CDN dependencies.
- **Permet Dark Theme**: Seamlessly integrated with the Cyberpunk status HUD.
