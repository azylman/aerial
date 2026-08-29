# K3s & GitOps Platform Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate Aerial to a production-grade, two-repository Kubernetes (K3s) + GitOps (FluxCD) architecture, eliminating all host-disk configuration desynchronization and establishing declarative infrastructure.

**Architecture:** Public core engine (`azylman/aerial`) provides container images and base Kustomize manifests (`k8s/base/`). Private user config (`azylman/aerial-config`) overlays encrypted secrets (SOPS/Age), `AGENTS.md`, and custom skills. K3s running on the miniPC runs FluxCD to automatically synchronize GitOps state every 60s with native health probes and Traefik ingress routing.

**Tech Stack:** K3s (`rancher/k3s:v1.30.2-k3s1`), FluxCD v2, Kustomize, Mozilla SOPS + Age, Traefik Ingress, Local-Path Storage, GitHub Actions, GHCR.

**Spec:** [`docs/superpowers/specs/2026-08-29-k3s-gitops-architecture-design.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-08-29-k3s-gitops-architecture-design.md)

## Global Constraints
- `aerial-brain` must run with `replicas: 1` and `strategy: Recreate` to prevent dual-gateway Discord collisions.
- Storage volumes for SQLite `/data` and Ollama models must use `local-path` persistent volume claims with `ReadWriteOnce`.
- Plaintext secrets must never be committed to Git; SOPS + Age encryption is mandatory for versioned secrets.
- Rollback path must be preserved: existing Docker Compose stack remains in a restorable state on the miniPC host during migration.

---

### Task 1: Base Kubernetes Manifests Scaffolding (`k8s/base/`)

**Files:**
- Create: `k8s/base/namespace.yaml`
- Create: `k8s/base/pvc.yaml`
- Create: `k8s/base/apps/brain.yaml`
- Create: `k8s/base/apps/scheduler-mcp.yaml`
- Create: `k8s/base/apps/discord-mcp.yaml`
- Create: `k8s/base/apps/docker-mcp.yaml`
- Create: `k8s/base/apps/github-mcp.yaml`
- Create: `k8s/base/apps/ollama.yaml`
- Create: `k8s/base/apps/agentsview.yaml`
- Create: `k8s/base/ingress.yaml`
- Create: `k8s/base/kustomization.yaml`

- [ ] **Step 1: Create namespace and persistent volume claims**
  Define `namespace.yaml` for `aerial` and `pvc.yaml` allocating `aerial-data-pvc` (5Gi) and `aerial-ollama-pvc` (10Gi).

- [ ] **Step 2: Create core microservice Deployment & Service manifests**
  Author modular YAML manifests in `k8s/base/apps/` with standardized label selectors, environment variable injections from `aerial-secrets`, and calibrated Liveness/Readiness probes.

- [ ] **Step 3: Create Traefik Ingress routing manifest**
  Define `k8s/base/ingress.yaml` routing `aerial.zylman.com` to `aerial-brain:8080` and `agentsview.zylman.com` to `aerial-agentsview:8080`.

- [ ] **Step 4: Create master base `kustomization.yaml`**
  Bundle all resources under `k8s/base/kustomization.yaml`. Validate manifest syntax locally using `kubectl kustomize k8s/base`.

- [ ] **Step 5: Commit Task 1**
  ```bash
  git add k8s/base/
  git commit -m "feat(k8s): scaffold base Kustomize manifests for Aerial microservices"
  ```

---

### Task 2: Private User Configuration Repository Structure (`aerial-config`)

**Files:**
- Create: `docs/k8s/aerial-config-template/kustomization.yaml`
- Create: `docs/k8s/aerial-config-template/AGENTS.md`
- Create: `docs/k8s/aerial-config-template/secrets.example.yaml`
- Create: `docs/k8s/aerial-config-template/flux-sync.yaml`

- [ ] **Step 1: Define private overlay `kustomization.yaml`**
  Reference `https://github.com/azylman/aerial//k8s/base?ref=main`, define `configMapGenerator` for `AGENTS.md` and custom skills, and configure environment patches.

- [ ] **Step 2: Define SOPS encryption schema and bootstrap script**
  Provide scripts for generating Age encryption keypairs (`age-keygen`) and encrypting `secrets.yaml` with SOPS.

- [ ] **Step 3: Define FluxCD GitRepository & Kustomization CRDs**
  Create `flux-sync.yaml` tracking the private Git repository with 60-second polling intervals.

- [ ] **Step 4: Commit Task 2**
  ```bash
  git add docs/k8s/
  git commit -m "docs(k8s): create aerial-config overlay template and SOPS secret runbook"
  ```

---

### Task 3: K3s Host Runtime & FluxCD Bootstrap on MiniPC

**Files:**
- Create: `docker/k3s/docker-compose.k3s.yml` (Supervisor runner for K3s on HAOS)
- Run: Bootstrap commands on miniPC via SSH (`192.168.1.14`).

- [ ] **Step 1: Deploy K3s Container on MiniPC**
  Run `rancher/k3s:v1.30.2-k3s1` with privileged access, host networking, and persistent volume at `/mnt/data/k3s`.

- [ ] **Step 2: Bootstrap Age Secret Key in Flux System**
  Inject Age private key into `flux-system` namespace on K3s for in-memory SOPS decryption.

- [ ] **Step 3: Bootstrap FluxCD Operator**
  Install FluxCD controllers (`source-controller`, `kustomize-controller`) pointing to `aerial-config`.

- [ ] **Step 4: Verify in-cluster reconciliation**
  Inspect `kubectl get pods -n aerial` and `flux get kustomizations`.

---

### Task 4: Live Verification, Cutover & Validation

**Files:**
- Update: Walkthrough and operational runbooks.

- [ ] **Step 1: Verify Pod Health & Probes**
  Verify all 7 pods (`brain`, `scheduler-mcp`, `discord-mcp`, `docker-mcp`, `github-mcp`, `ollama`, `agentsview`) are running and ready.

- [ ] **Step 2: Stop Legacy Docker Compose Stack**
  Gracefully pause Docker Compose (`sudo docker compose stop`) so K3s acquires Discord bot session without conflict.

- [ ] **Step 3: Test Dynamic ConfigMap Update**
  Commit a small prompt change in `aerial-config` $\rightarrow$ verify Flux updates the ConfigMap in <60s $\rightarrow$ verify Aerial reflects change live.

- [ ] **Step 4: Test Self-Improvement Code Push**
  Verify Aerial can push code changes $\rightarrow$ GHCR builds $\rightarrow$ Flux rolls new pod image.
