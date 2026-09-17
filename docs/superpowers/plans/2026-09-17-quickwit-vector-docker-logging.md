# Implementation Plan: Docker Container Log Ingestion & Full-Text Search with Quickwit, Vector & Grafana

## 1. Overview & Objective
Enable fleet-wide Docker container log aggregation, normalization, and sub-second full-text search across all Aerial and user containers without incurring heavy JVM memory overhead. 

The architecture consists of:
- **Vector**: Lightweight log collection agent running in an Alpine container, scraping `/var/run/docker.sock` and container JSON logs, applying Vector Remap Language (VRL) for normalization and ANSI code stripping, and streaming batches to Quickwit.
- **Quickwit**: Distributed Rust-based log search engine running Tantivy, indexing container logs into the `docker-logs` index with tokenized full-text on message payloads and exact fast fields on metadata.
- **Grafana**: Pre-provisioned Elasticsearch-type datasource talking to Quickwit's `/api/v1/_elastic` endpoint and an out-of-the-box Cyberpunk HUD dashboard (`docker-logs.json`) providing instant search and metrics.

---

## 2. Architecture & Service Topology

```
┌─────────────────────────────────────────────────────────────┐
│ Docker Host & Container Fleet                               │
│  • stdout / stderr via /var/lib/docker/containers/*/*.log   │
│  • Container metadata via /var/run/docker.sock              │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ aerial-vector (timberio/vector:0.40.0-alpine)               │
│  • Source: docker_logs                                      │
│  • Transform: VRL (strip ANSI, detect level, clean schema)  │
│  • Sink: elasticsearch (POST /api/v1/_elastic/_bulk)        │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ aerial-quickwit (quickwit/quickwit:v0.8.2)                  │
│  • Storage: aerial-quickwit-data volume                     │
│  • Index: docker-logs (BM25 full-text + fast fields)        │
│  • API: REST & Elasticsearch bulk / search endpoint         │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ aerial-grafana (grafana/grafana:11.1.0)                     │
│  • Datasource: Quickwit (type: elasticsearch)               │
│  • Dashboard: ⚡ Docker Container Logs (docker-logs.json)   │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Configuration & Implementation Details

### 3.1 Docker Compose Services (`docker-compose.yml`)
- `quickwit`:
  - Image: `quickwit/quickwit:v0.8.2`
  - Container name: `aerial-quickwit`
  - Entrypoint: `["/bin/sh", "/quickwit/config/entrypoint.sh"]` (auto-initializes `docker-logs` index via CLI on startup)
  - Volumes: `aerial-quickwit-data:/quickwit/qwdata`, `${AERIAL_HOST_PROJECT_DIR}/quickwit:/quickwit/config:ro`
  - Network: `aerial-net`
  - Healthcheck: `quickwit index list --endpoint http://127.0.0.1:7280`
- `vector`:
  - Image: `timberio/vector:0.40.0-alpine`
  - Container name: `aerial-vector`
  - Volumes: `/var/run/docker.sock:ro`, `/var/lib/docker/containers:ro`, `aerial-vector-data:/var/lib/vector`, `${AERIAL_HOST_PROJECT_DIR}/vector/vector.yaml:/etc/vector/vector.yaml:ro`
  - Network: `aerial-net`
  - Healthcheck: `wget -q --spider http://127.0.0.1:8686/health`
  - Depends on: `quickwit: condition: service_healthy`
- `grafana`:
  - Added `quickwit: condition: service_healthy` to `depends_on`.

### 3.2 Quickwit Schema & Automation (`quickwit/`)
- `quickwit/quickwit.yaml`: Cluster configuration with data directory at `/quickwit/qwdata`.
- `quickwit/docker-logs.yaml`: Schema defining timestamp, container_name, service, stream, level, and message (tokenized full-text with positions for phrase searches).
- `quickwit/entrypoint.sh`: Signal-aware wrapper starting Quickwit in background, polling local REST API, and creating the `docker-logs` index if not already present.

### 3.3 Vector Pipeline (`vector/vector.yaml`)
- Docker source excludes `aerial-vector` to eliminate log loops.
- VRL remap program parses container name, compose service labels, strips ANSI color codes, infers log severity (error, warn, info, debug), and purges internal Docker metadata.
- Elasticsearch sink sends bulk index requests using `action: "create"` directly to `http://quickwit:7280/api/v1/_elastic`.

### 3.4 Grafana Provisioning (`grafana/`)
- `grafana/provisioning/datasources/quickwit.yml`: Pre-provisions an Elasticsearch-type datasource targeting `http://quickwit:7280/api/v1/_elastic` with index `docker-logs`.
- `grafana/dashboards/docker-logs.json`: Cyberpunk HUD dashboard with stat KPIs, ingestion rate time-series graph, and live searchable logs panel.

---

## 4. Complexity Tier & Review Gates
- **Complexity Tier**: **Tier 3** (Docker service topology expansion, new logging subsystem).
- **Review Panel (the girl gang)**: 
  - Stage 2: Architectural plan formulated and validated against Quickwit/Vector compatibility constraints.
  - Stage 3: Explicit user approval obtained ("This looks good").
  - Stage 4: Continuous TDD implementation in scratch workspace.
  - Stage 5: Verification runner validation (`./scripts/verify.sh --staged`).
