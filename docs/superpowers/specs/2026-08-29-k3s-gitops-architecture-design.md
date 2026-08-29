# Architectural Specification: Aerial K3s & GitOps Platform Architecture

**Date:** 2026-08-29  
**Status:** DRAFT / UNDER REVIEW  
**Author:** Antigravity Architect & Engineering Team  
**Target Platform:** Single-Node K3s on Home Assistant OS / Linux Host (`192.168.1.14`)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
In the legacy Docker Compose + Watchtower architecture:
1. **Host Disk vs. Container Desynchronization**: While container binaries are updated via GitHub Container Registry (GHCR) and Watchtower, user-level configurations (`SYSTEM.md`, `AGENTS.md`, `.env`, skills) live on the miniPC host disk (`/share/aerial`). Edits made remotely from a laptop or GitHub web UI leave the host disk stale, causing non-fast-forward Git rejections and split-brain configuration states.
2. **Orchestration Stagnation**: Watchtower only monitors image digests; it cannot dynamically apply changes to `docker-compose.yml` (e.g. adding microservices, updating environment variables, or remapping ports) without manual `docker compose up -d` execution on the host.
3. **Public vs. Private Entanglement**: Core public source code and private user customizations (API keys, home automation entity IDs, personal routines) are entangled in a single workspace.

### 1.2 The Solution: Two-Repository GitOps Architecture on K3s
Transition Aerial to a **Two-Repository GitOps model** powered by **K3s** (lightweight Kubernetes) and **FluxCD**:
1. **Public Engine Repository (`azylman/aerial`)**: Contains generic Go source code, container Dockerfiles, CI workflows, and base Kustomize manifests (`k8s/base/`).
2. **Private User Config Repository (`azylman/aerial-config`)**: Contains the user's personal `AGENTS.md`, encrypted secrets (`secrets.enc.yaml` via SOPS/Age), custom skills, and environment overlays.
3. **FluxCD on K3s**: Continuously synchronizes `aerial-config` into K3s every 60 seconds. In-cluster ConfigMaps, Secrets, and Deployments are updated automatically without host disk entanglement.

---

## 2. System Architecture & Topology

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. PUBLIC REPOSITORY: github.com/azylman/aerial                            │
│    • brain/, scheduler-mcp/, discord-mcp/, docker-mcp/, github-mcp/         │
│    • GitHub Actions CI: Tests & publishes ghcr.io/azylman/aerial-*:latest   │
│    • k8s/base/: Generic Kustomize manifests (Deployments, Services, Ingress)│
└─────────────────────────────────────────────────────────────────────────────┘
                                      ▲
                                      │ Kustomize Remote Base
┌─────────────────────────────────────────────────────────────────────────────┐
│ 2. PRIVATE REPOSITORY: github.com/azylman/aerial-config                     │
│    • AGENTS.md (Personal persona, habit tracking, user instructions)        │
│    • secrets.enc.yaml (SOPS/Age encrypted API keys, bot tokens, HA webhooks)│
│    • custom-skills/ (Private home automation skills)                        │
│    • kustomization.yaml (Overlays personal config on aerial//k8s/base)      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ FluxCD (GitOps Operator)
┌─────────────────────────────────────────────────────────────────────────────┐
│ 3. MINIPC HOST: K3s Cluster Container (rancher/k3s:v1.30.2-k3s1)            │
│    • Storage: Local-path-provisioner (/mnt/data/k3s/storage)                │
│    • Networking: Traefik Ingress Controller (:80 / :443)                    │
│    • Workloads (aerial namespace):                                          │
│      ├── aerial-brain (Go Daemon, Discord Gateway, Scheduler, Memory)       │
│      ├── aerial-scheduler-mcp (:8080 ClusterIP)                             │
│      ├── aerial-discord-mcp (:4001 ClusterIP)                               │
│      ├── aerial-docker-mcp (:4002 ClusterIP)                                │
│      ├── aerial-github-mcp (:4003 ClusterIP)                                │
│      ├── aerial-ollama (:11434 ClusterIP)                                   │
│      └── aerial-agentsview (:8080 ClusterIP -> Traefik / Ingress)           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Microservice Manifest Specifications (`k8s/base/`)

### 3.1 Namespace & Storage (`pvc.yaml`)
- **Namespace**: `aerial`
- **PVC 1: `aerial-data-pvc`**:
  - Storage Class: `local-path`
  - Access Mode: `ReadWriteOnce`
  - Capacity: `5Gi`
  - Mount Path: `/data` (Holds SQLite database `aerial.db` with WAL mode).
- **PVC 2: `aerial-ollama-pvc`**:
  - Storage Class: `local-path`
  - Access Mode: `ReadWriteOnce`
  - Capacity: `10Gi`
  - Mount Path: `/root/.ollama` (Holds vector embedding model weights `all-minilm`).

### 3.2 Brain Deployment (`apps/brain.yaml`)
- **Image**: `ghcr.io/azylman/aerial-brain:latest`
- **Replicas**: 1 (Singleton due to stateful Discord Gateway connection & SQLite writer).
- **Strategy**: `Recreate` (Ensures old pod terminates before new pod starts to prevent duplicate Discord gateway sessions).
- **Probes**:
  - `livenessProbe`: HTTP GET `/health` on port 8080 (interval 30s, timeout 5s, failureThreshold 3).
  - `readinessProbe`: HTTP GET `/health` on port 8080 (interval 10s, timeout 5s, failureThreshold 2).
- **Volume Mounts**:
  - `aerial-data-pvc` mounted at `/data`
  - `configMap: user-instructions` mounted at `/app/.agents/rules/system_instructions.md`
  - `configMap: custom-skills` mounted at `/app/skills`
  - `secretRef: aerial-secrets` injected as environment variables.
  - Host Docker socket `/var/run/docker.sock` mounted for container management.

### 3.3 MCP Deployments & ClusterIP Services
- **`scheduler-mcp`**:
  - Image: `ghcr.io/azylman/aerial-scheduler-mcp:latest`
  - Service: `aerial-scheduler-mcp` on port `8080`.
  - Probes: HTTP GET `/health` on `:8080`.
  - Volume: `aerial-data-pvc` mounted at `/data`.
- **`discord-mcp`**:
  - Image: `ghcr.io/azylman/aerial-discord-mcp:latest`
  - Service: `aerial-discord-mcp` on port `4001`.
  - Probes: HTTP GET `/health` on `:4001`.
- **`docker-mcp`**:
  - Image: `ghcr.io/azylman/aerial-docker-mcp:latest`
  - Service: `aerial-docker-mcp` on port `4002`.
  - Volume: `/var/run/docker.sock`.
- **`github-mcp`**:
  - Image: `ghcr.io/azylman/aerial-github-mcp:latest`
  - Service: `aerial-github-mcp` on port `4003`.
- **`ollama`**:
  - Image: `ghcr.io/azylman/aerial-ollama:latest`
  - Service: `aerial-ollama` on port `11434`.
  - Probes: HTTP GET `/api/version` on `:11434`.
  - Volume: `aerial-ollama-pvc` mounted at `/root/.ollama`.
- **`agentsview`**:
  - Image: `ghcr.io/kenn-io/agentsview:latest`
  - Service: `aerial-agentsview` on port `8080`.

### 3.4 Ingress Configuration (`ingress.yaml`)
- Uses built-in Traefik Ingress:
  - `aerial.zylman.com` $\rightarrow$ `aerial-brain:8080`
  - `agentsview.zylman.com` $\rightarrow$ `aerial-agentsview:8080`

---

## 4. Secret & Config Management

### 4.1 Secrets with Mozilla SOPS & Age
- Secrets are defined in `secrets.yaml` and encrypted using `sops --encrypt --age <age_public_key>` into `secrets.enc.yaml`.
- FluxCD natively decrypts SOPS encrypted files on K3s using the private Age key stored in the `flux-system` namespace.
- Plaintext secrets are NEVER committed to version control.

### 4.2 ConfigMaps for Prompt & Skills
- `AGENTS.md` and `SYSTEM.md` are compiled into Kubernetes ConfigMaps.
- When `AGENTS.md` is updated in `aerial-config`, Flux updates the ConfigMap in K3s.
- Pods consume ConfigMaps as mounted files or environment variables; updates apply live without Docker rebuilds.

---

## 5. Migration & Rollback Strategy

1. **Side-by-Side Validation**:
   - K3s runs alongside the existing Docker Compose stack on the miniPC during validation.
   - Compose services are paused (`sudo docker compose stop`) only when K3s is ready to claim Discord bot gateway and ports.
2. **Instant Rollback**:
   - If K3s exhibits unforeseen issues, stopping the K3s container and running `sudo docker compose start` restores the 100% functional Compose stack in <10 seconds.
