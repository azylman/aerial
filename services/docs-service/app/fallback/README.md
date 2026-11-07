# 🛸 Aerial Documentation Matrix

Welcome to **Aerial Docs**, your personal living documentation and architecture portal with native **Mermaid.js** diagram rendering!

> [!NOTE]
> **Dynamic User Documentation**: This portal dynamically serves Markdown (`.md`) files from your private configuration repository (`aerial-config/docs/`).

---

## ⚡ System Architecture Topology

```mermaid
flowchart TB
    subgraph Ingress ["🌐 Ingress & Edge Layer"]
        DiscordUser(["Discord User / Gateway"])
        WebBrowser(["Web Browser / Client"])
        Synapse["synapse (:4020)
• Discord Gateway WebSocket & River Ingress
• GitHub, Flux & Infisical Webhooks"]
        LanGW["lan-gateway (:80 / :443)
NGINX Gateway Fabric"]
        EdgeGW["edge-gateway (:4021)
External Webhook Ingress"]
    end

    subgraph CoreAI ["🧠 Core Execution & AI Services"]
        Eri["eri (:8080)
• Antigravity Agent Runtime & Memory
• Voice & Prompt Execution Pipeline"]
        Haro["haro (:4005)
Persistent Scheduler & Event Watches"]
        Coven["coven (:4008)
Semantic Fact Extraction & Memory Decay"]
        Vanadis["vanadis (:4025)
Multi-Chunk Build Queue Orchestrator"]
        Pjev["pjev (:8000)
System-1 Ambient Triage (/v1/systemone)"]
        Senses["senses (:8880 / :10300)
STT, TTS, Embeddings & Wyoming"]
    end

    subgraph Persistence ["💾 Persistence & GitOps"]
        Postgres[("postgres (:5432 / :9187)
• CloudNativePG PostgreSQL 18 + pgvector
• River Task Queues & PR Registry
• Vector RAG & State Persistence")]
        Flux["flux-system
Flux CD k3s Reconciler (./k8s)"]
        GitSync["git-sync sidecars
Mounts /share/aerial:ro & /share/aerial-config:ro"]
    end

    subgraph WebServices ["📊 Web & Documentation Microservices"]
        Homepage["homepage (:8080)
Service Discovery HUD"]
        Dashboard["dashboard (:8080)
Live Cluster Status HUD"]
        Docs["docs (:80)
Docsify + Mermaid Engine"]
        Agentsview["agentsview (:8080)
Transcript Visualizer"]
    end

    subgraph Observability ["📈 Full-Stack Observability"]
        Victoria["victoriametrics (:8428)
Single-Node Prometheus TSDB"]
        VMAlert["vmalert (:8880)
Anomaly Alert Rule Evaluator"]
        Grafana["grafana (:3000)
Visual Telemetry Dashboards"]
        OpenObserve["openobserve (:5080)
Structured Log Analytics"]
        FluentBit["fluent-bit
Container Log Forwarder"]
        NodeExp["node-exporter (:9100)
Host System Metrics"]
        CAdvisor["kubelet-cadvisor (:10250)
Kubelet Container Metrics"]
    end

    %% Ingress flow
    DiscordUser -->|Gateway WebSocket| Synapse
    Synapse -->|POST /discord/ingress\nPOST /discord/interaction| Eri
    WebBrowser --> LanGW
    WebBrowser --> EdgeGW
    LanGW --> Homepage
    LanGW --> Dashboard
    LanGW --> Docs
    LanGW --> Agentsview
    LanGW --> Grafana
    LanGW --> OpenObserve

    %% Core interactions
    Eri --> Haro
    Eri --> Coven
    Eri --> Vanadis
    Eri --> Pjev
    Eri --> Senses
    Eri --> Postgres
    Haro --> Postgres
    Coven --> Postgres
    Vanadis --> Postgres
    Synapse --> Postgres

    %% GitOps & Persistence
    Flux --> GitSync

    %% Observability Scrapes & Logs
    Victoria -->|Scrapes| Postgres
    Victoria -->|Scrapes| NodeExp
    Victoria -->|Scrapes| CAdvisor
    Victoria -->|Scrapes| Eri
    Victoria -->|Scrapes| Synapse
    VMAlert --> Victoria
    Grafana -->|PromQL| Victoria
    FluentBit -->|Ships Logs| OpenObserve
```

---

## 📂 Recommended Configuration Directory Structure

In your `aerial-config` repository:

```text
aerial-config/
├── config.yaml               # Core agent, model & channel configuration
├── rules/                    # System persona overrides & preferences
│   ├── common/
│   ├── persona/
│   ├── discord/
│   └── voice/
├── channels/                 # Per-channel operational instructions
│   └── lounge.md
├── custom-skills/            # Private operational runbooks & domain skills
├── homepage/                 # Service discovery cards & bookmarks
├── k8s/                      # User-defined Kubernetes manifests & Kustomizations
├── services/                 # Dedicated service configuration files
│   └── dashboard/
├── victoriametrics/          # Modular Prometheus scrape configs
└── docs/                     # Living Docsify documentation portal
    ├── README.md             # Docs homepage
    ├── _sidebar.md           # Sidebar navigation
    ├── architecture.md       # Multi-node k3s cluster topology
    ├── observability.md      # TSDB, metrics & Grafana
    ├── gitops.md             # Continuous delivery & PR workflow
    ├── channels.md           # Discord interaction & wake modes
    └── security.md           # Admin allowlist & RBAC security
```

---

## 🔍 Features Included
- **Zero-Build Hot Reloading**: Any commit or sync to `aerial-config` is immediately live in your browser.
- **Mermaid.js Engine**: Sequence diagrams, flowcharts, and architecture maps rendered in real time.
- **Offline / Local First**: 100% vendored assets with zero external CDN dependencies.
- **Permet Dark Theme**: Seamlessly integrated with the Cyberpunk status HUD.
