# Multi-Agent Cluster Core Architecture & RBAC Consolidation

**Date:** 2026-11-19  
**Status:** Proposed Design Specification  
**Scope:** Multi-agent service topology, database & queue consolidation, Kubernetes workload identity (`pkg/k8sauth`), L4 `NetworkPolicy` isolation, and 4-phase rollout across `azylman/aerial` and `azylman/aerial-config`.

---

## 1. Intent and Why

Aerial's cluster originally achieved multi-agent isolation across multiple agent identities (`aerial` plus configured sibling agents such as `<agent-a>`, `<agent-b>`) by cloning the entire control plane per agent: separate logical PostgreSQL databases (`aerial`, `<agent-a>`, `<agent-b>`), separate `db-migrate` jobs, separate NATS JetStream accounts with cross-account stream imports/exports, separate Restate clusters (`restate`, `restate-<agent-a>`, `restate-<agent-b>`), and ~12 duplicated pods per sibling agent (`synapse-*`, `vanadis-*`, `vanadis-restate-*`, `vanadis-nats-*`, `haro-*`, `coven-*`, `transcripts-*`).

That full-stack duplication created three compounding problems:

1. **Infrastructure & Operational Bloat:** Every new agent required ~12 pods, a dedicated Restate StatefulSet, a dedicated logical database and migration job, and hacky HTTP webhook fan-out (`webhook_forward_urls` in `synapse`) to tee GitHub and Flux CD webhooks across per-agent `synapse` instances.
2. **Coarse & Fragile Privilege Boundaries:** Sibling agents either received over-privileged database and cluster credentials or required ad-hoc environment/config overrides, while `uid=1000` bash tool execution inside an `eri` pod could still reach internal ClusterIP services (`infisical-mcp`, `restate`, peer `discord-mcp` instances) via raw `curl`.
3. **Fragmented Observability & Control Plane:** Inspecting or supervising builds, schedules, transcripts, or turn queues across agents required querying three separate databases and three separate control-plane endpoints.

This specification collapses the multi-agent architecture into a unified, single-database, single-bus, single-Restate control plane where **only `eri` (`Deployment/eri-<agent>` + `RestateDeployment/eri-<agent>-restate`) and `discord-mcp` remain per-agent** (cutting the per-agent pod footprint from ~12–13 pods to 3, with `Deployment/eri-<agent>` shrunk to a stateless ~30MB Alpine HTTP ingress/proxy), and **100% of caller authentication and authorization is driven declaratively by Kubernetes `ServiceAccount` tokens (`TokenReview`), static Kubernetes RBAC (`SubjectAccessReview`), and L4 `NetworkPolicy` isolation**—with zero hardcoded `caller_agent == "aerial"` checks anywhere in Go code.

---

## 2. Goals, Non-Goals, and Anti-Goals

### Goals
- **Standardize Terminology on `agent`:** Use `agent` (e.g., `agent` column, `caller_agent`, `target_agent`, `source_agent`) consistently across all schemas, Go packages, HTTP/MCP payloads, and YAML configurations, renaming legacy `stack` columns and config keys to `agent`.
- **Reduce Per-Agent Footprint to 3 Pods:** Consolidate `coven`, `transcripts`, `haro`, `vanadis`, `vanadis-nats`, `vanadis-restate`, `synapse`, and `db-migrate` into single multi-agent deployments, leaving only stateless HTTP ingress `Deployment/eri-<agent>` (`:8080`), agent runtime worker `RestateDeployment/eri-<agent>-restate` (`:9080` + `:4028`), and `Deployment/discord-mcp-<agent>` as per-agent workloads.
- **Unified Storage, Messaging, and Workflow State:** Collapse sibling logical PostgreSQL databases, extra NATS accounts, and extra Restate clusters into the single `aerial` PostgreSQL database, single NATS JetStream bus, and single `restate` cluster.
- **Declarative Kubernetes RBAC & Zero Hardcoded Supervisor Logic:** Authenticate and authorize all agent and service requests via projected bound Kubernetes `ServiceAccount` tokens (`TokenReview` + `SubjectAccessReview`) against two static cluster roles (`ClusterRole/aerial-agent` and `ClusterRole/aerial-supervisor`), plus native Bearer token passthrough on `kubernetes-mcp` (`require_oauth = true`).
- **Sibling Secret & Database Isolation:** Ensure sibling agent tool sandboxes (`uid=1000` inside `eri-<agent>-restate`) hold zero PostgreSQL credentials, zero Kubernetes `Secret` read or `pods/exec` permissions, zero `infisical-mcp` network access, zero direct `restate` ingress/admin network access, and zero ability to delete shared semantic memories (`memory_forget` removed from `coven` MCP).
- **Eliminate Webhook Fan-Out Hacks:** Consolidate `synapse` into a single multi-bot service that holds all configured Discord Gateway WebSocket sessions (`agent -> session`) and routes GitHub/Flux webhooks against one unified `pr_registry`, deleting `webhook_forward_urls`.

### Non-Goals
- **Merging `eri` or `eri-restate` Pods Across Agents:** Each agent keeps its own stateless `Deployment/eri-<agent>` (`:8080` HTTP ingress, wake-mode funnel, and GitHub credential/guard proxy) and `RestateDeployment/eri-<agent>-restate` (`:9080` Restate actor server + `:4028` internal/health server) so engine CLI state (`/data` host mounts), worker pools, and OS process namespaces remain physically isolated per agent while deployments drain gracefully between turns.
- **Merging `discord-mcp` Pods:** Upstream `discord-mcp` binds a single `DISCORD_TOKEN` per process and remains 1 pod per Discord bot application.
- **Pod-to-Pod mTLS (`PodCertificateRequest` KEP-4317):** Evaluated against Kubernetes v1.35–v1.37; projected bound `ServiceAccount` JWTs over HTTP `Authorization: Bearer` with L4 `NetworkPolicy` is the native fit for our MCP and HTTP control plane without adding an external X.509 CA signer controller.

### Anti-Goals (Failure Conditions Even If Technically Working)
- **Hardcoded Agent Names in Authorization Logic:** Any Go check of the form `if callerAgent == "aerial"` or `if isSupervisor(agent)` based on string literals is a design failure. Supervisor status must be determined solely by Kubernetes RBAC (`SubjectAccessReview`).
- **Per-Agent `ClusterRole` Cloning:** Adding a fourth agent must not require minting a bespoke `ClusterRole` with `resourceNames: ["<new-agent>"]`; self-ownership must derive from the verified `TokenReview` identity claim combined with the static `ClusterRole/aerial-agent`.
- **Config-Only Security Boundaries:** Removing an MCP server from a sibling's `config.yaml` without blocking direct `uid=1000` bash `curl` access via `NetworkPolicy` or `pkg/k8sauth` is unacceptable.

---

## 3. Constraints & Kubernetes Standards Alignment (v1.35 – v1.37)

1. **Physical Immutability & Config Separation:** Generic service code, `pkg/k8sauth`, and core Kubernetes manifests live in `azylman/aerial`; user-specific agent rosters (`<agent-a>`, `<agent-b>`), Discord channel mappings, and multi-bot configs live in `azylman/aerial-config`.
2. **Single-Service Table Ownership & Sandbox DB/NATS/Restate Isolation:** Every PostgreSQL table in `services/eri/pkg/db/schema.sql` is owned by its designated service. Each `eri-<agent>` and `eri-<agent>-restate` daemon (`PID 1`, running as `uid=0`) connects to the `aerial` PostgreSQL database solely for its own low-level turn queue tables (`messages`, `sessions`, and `dispatches`, scoped to `WHERE agent = $1`), and `eri-<agent>-restate` strips `POSTGRES_PASSWORD`, `DATABASE_URL`, `NATS_URL`, `RESTATE_INGRESS_URL`, `RESTATE_ADMIN_URL`, and all `*_SECRET`/`*_TOKEN`/`*_PASSWORD` variables in `FilterAgyBaseEnv` before spawning `agy`/`claude`/`codex`/`muse` as `uid=1000`. All agent tool and CLI interaction with `vanadis`, `haro`, `coven`, `transcripts`, and `synapse` happens strictly over HTTP/MCP.
3. **Kubernetes v1.35–v1.37 SIG Auth Compliance:**
   - **Bound `ServiceAccount` Tokens (`TokenRequest` GA / KEP-740 & KEP-6060):** All `eri`, `eri-restate`, and control-plane pods mount explicit projected `serviceAccountToken` volumes (`expirationSeconds: 3600`, rotated automatically by `kubelet` at 80% lifetime) with `automountServiceAccountToken: false` set on the namespace `default` `ServiceAccount`.
   - **In-Cluster `TokenReview` (`authentication.k8s.io/v1`):** Used instead of offline JWKS decoding so `kube-apiserver` verifies live pod and `ServiceAccount` binding (revoking tokens within the bounded 45-second cache window on pod deletion or `ServiceAccount` revocation rather than the 1-hour JWT expiration).
   - **Delegated `SubjectAccessReview` (`authorization.k8s.io/v1`):** Evaluated against virtual `apiGroups: ["aerial.io"]` resources. Aligns with Kubernetes v1.37's built-in admission exclusion for `TokenReview` and `SubjectAccessReview`.
   - **Fine-Grained Kubelet RBAC (KEP-2862, GA in v1.36):** `ClusterRole/aerial-k8s-reader` uses explicit `nodes/stats`, `nodes/log`, and `nodes/metrics` subresources rather than legacy `nodes/proxy`.

---

## 4. Approaches Considered

### Approach 1: Unified Single-Database + Single-NATS Bus + Single-Restate + Declarative K8s ServiceAccount RBAC (Selected)
Collapse all per-agent logical databases, per-agent `db-migrate` jobs, per-agent NATS accounts, and extra Restate clusters into the single `aerial` PostgreSQL database, single NATS JetStream bus, and single `restate` cluster. Partition multi-agent tables by an `agent TEXT NOT NULL DEFAULT 'aerial'` column, consolidate all Category 3 control-plane services (`haro`, `vanadis` suite, `synapse`, `transcripts`, `coven`) into single deployments, and enforce `read-all`, `write-own`, and supervisor permissions via projected Kubernetes `ServiceAccount` tokens (`TokenReview` + `SubjectAccessReview` in `pkg/k8sauth` and `require_oauth = true` in `kubernetes-mcp`).
- **Pros:** Eliminates redundant pods, extra Restate clusters, extra `db-migrate` jobs, cross-account NATS plumbing, and `webhook_forward_urls`; gives `console` a single database to query all agents; enforces privilege boundaries via standard CNCF/Kubernetes primitives.
- **Cons:** Requires a schema migration (`agent` column + composite primary keys on `messages`, `sessions`, `extraction_watermarks`, `session_summaries`, `transcript_steps`) and a one-time data backfill from sibling databases before retiring them.

### Approach 2: Per-Agent Databases for `eri` Turn Queues + Shared `aerial` Database for Control Plane (Rejected)
Move `vanadis`, `haro`, `synapse`, `transcripts`, and `coven` into the `aerial` database while keeping separate per-agent databases (`aerial`, `<agent-a>`, `<agent-b>`) for each `eri` pod's `messages`, `sessions`, and `dispatches` tables.
- **Pros:** Avoids adding `agent` scoping to `eri`'s internal SQL queries.
- **Cons:** Permanently retains 3 logical databases and 3 `db-migrate` jobs, forces `coven` and `console` (which renders live turn queues across all agents) to maintain multi-database connection pools, and complicates cross-agent transaction consistency.

### Approach 3: Keep `synapse` Per-Agent in Category 4 While Consolidating `vanadis` and `haro` (Rejected)
Keep 3 `synapse` pods (one per Discord bot token) and retain `webhook_forward_urls` while consolidating the rest of Category 3.
- **Pros:** Avoids refactoring `synapse` from a single-Discord-session struct to a multi-session manager.
- **Cons:** Keeps 3 duplicate pods watching the Kubernetes API and processing fanned-out GitHub/Flux webhooks, and preserves the brittle `webhook_forward_urls` HTTP proxy even after `pr_registry` and `vanadis` live in a single database.

**Decision:** **Approach 1** was selected with `synapse` consolidated into Category 3 and two-tier static Kubernetes RBAC (`aerial-agent` + `aerial-supervisor`).

---

## 5. Four-Tier Service Topology

### Category 1: Single Instance, Agent-Agnostic (Unchanged)
Shared infrastructure and stateless/shared MCP backends that operate identically regardless of which agent calls them:
- **Core Infrastructure:** `postgres` (CloudNativePG PostgreSQL 18), `nats` (NATS 2 JetStream), `restate` (single RestateCluster in `restate` namespace), `infisical` + `valkey`, `edge-gateway`, `lan-gateway`, `cloudflared`.
- **Observability & AI Inference:** `victoriametrics`, `victorialogs`, `victoriatraces`, `vmalert`, `grafana`, `fluent-bit`, `senses`, `pjev`, `vision`, `prospera`, `gatus`, `homepage`, `docs`.
- **Shared Collaborative MCPs:** `docket-mcp` (universally read/write across all agents), `github-mcp`, `kernel-mcp`, `victoriametrics-mcp`, `victorialogs-mcp`, `victoriatraces-mcp`.
- **Supervisor-Only MCP:** `infisical-mcp` (single instance, locked to primary `eri` and `eri-restate` / `eri-aerial` via `NetworkPolicy` and omitted from sibling `config.yaml`).

### Category 2: Single Instance, Agent-Aware
Services that already run as a single deployment and aggregate or enforce policy across agents:
- **`kubernetes-mcp`:** Single deployment configured with `require_oauth = true` so every MCP tool call passes the caller's `Authorization: Bearer <sa-jwt>` directly to `kube-apiserver`.
- **`console`:** Single web HUD reading all agents' turns, builds, schedules, and memory facts from the `aerial` database, exposing an agent filter and agent badges.
- **`agentsview` / `agentsview-sync` & `coverage-ingest`:** Single deployments aggregating session transcripts (including all agents' `/data` mounts and the shared `/var/lib/aerial/roborev-cache` RoboRev session cache) and Go coverage profiles across all pods (including `eri` `:8080` and `eri-restate` `:4028`).

### Category 3: Consolidated Multi-Agent Control Plane (Formerly Per-Agent)
Services collapsed from per-agent deployments into 1 shared multi-agent deployment:
- **`db-migrate`:** Single Ariga Atlas migration Job targeting the unified `aerial` database (per-sibling `db-migrate-<agent>` retired).
- **`coven`:** Single deployment indexing semantic facts and procedural skills across all agents' turn histories (`memory_forget` removal and `pkg/k8sauth` MCP RBAC in Phase 1; multi-agent extractor consolidation over unified `messages` and retirement of per-sibling `coven-<agent>` extractor deployments in Phase 2).
- **`transcripts`:** Single deployment indexing NATS `AERIAL_TRANSCRIPTS` step batches, running the background idle session summarizer, publishing `TranscriptsIndexedEvent` (`aerial.turn.transcripts_indexed`), and serving session summaries, transcript steps, watermarks, and thread trajectories across all agents (per-sibling `transcripts-<agent>` retired in Phase 1).
- **`haro`:** Single deployment evaluating one-shot, cron, heartbeat, and event triggers across all agents and dispatching due turns to the owning agent's Restate Virtual Objects (`ThreadSessionServiceName(agent)` and `DispatchSessionServiceName(agent)`) on `http://restate.aerial:8080` (per-sibling `haro-<agent>` retired).
- **`vanadis` Suite (`vanadis`, `vanadis-nats`, `vanadis-restate`):** Single HTTP/MCP API, single NATS bridge, and single RestateDeployment executing `Build`, `BuildRouter`, `PRObject`, and `RepoObject` Virtual Objects against the primary `restate` cluster, dispatching chunk turns and wake prompts via `RestateEriClient` (`DispatchSessionServiceName(agent)`, `ThreadSessionServiceName(agent)`, and `SessionManagerServiceName(agent)`), delivering serialized Components V2 build status widgets directly to Discord REST via `DiscordWidgetDeliverer`, and projecting `pr_registry` via `PRObject` (per-sibling `vanadis-*-<agent>` and extra Restate clusters retired).
- **`synapse`:** Single multi-bot deployment managing concurrent Discord Gateway WebSocket connections for all configured bots, forwarding inbound Discord messages and interactions to the owning bot's stateless `Deployment/eri*` `:8080` front door (`/internal/discord/message` and `/internal/discord/interaction`), and acting as a stateless PR/deploy edge forwarder that invokes `PRObject` and `RepoObject` on Restate (`http://restate.aerial:8080`) via `services/synapse/internal/restateclient` (per-sibling `synapse-<agent>` retired; `webhook_forward_urls` deleted).

### Category 4: Per-Agent Workloads (Strictly 3 Pods Per Agent)
Following the Restate graceful-draining split (`build-1795131906-b2b196f5`), **eri** is decomposed into two separate services and container images (matching `vanadis` + `vanadis-restate`, with **zero `ERI_MODE` env var switch**):
- **`eri` (`Deployment/eri`, plus sibling `Deployment/eri-<agent>` on `:8080`, image `ghcr.io/azylman/aerial-eri`):** Lightweight `alpine:3.24` stateless HTTP ingress and API server (`ca-certificates curl tzdata` + `/app/eri`, with zero `/data` or `/mnt/nas` mounts, zero CLI toolchains/binaries, zero local CLI process pools, zero `env.Provisioner`, and zero Restate server). Serves stateless HTTP/SSE ingress (`/internal/discord/message`, `/internal/discord/interaction`, `/prompt`, `/dispatch`, `/dispatch/cancel`, `/discord/message`, `/voice/ask`, `/tasks`, `/transcripts/*`, `/internal/llm`, `/quota`, `POST /-/reload`, `/healthz`, `/metrics`), routes turn execution to Restate (`RESTATE_INGRESS_URL: http://restate.aerial:8080`) via `RestateIngressClient` with strict at-least-once idempotent semantics, and serves `POST /internal/llm` by invoking the per-agent `LLM` Restate service (`Complete`) so each call is a durable invocation; callers may send an `Idempotency-Key` header naming the logical operation, and eri appends a content hash of prompt, model, effort and attachments so changed input never reuses a stale result. Thread-title summarization and Tier-2 classifier calls use the same service without a key. Delegates `/quota` (`GET /internal/quota` via `NewRemoteQuotaProvider`) over HTTP to `ERI_RESTATE_URL` (`http://eri-restate-health:4028` / `http://eri-<agent>-restate-health:4028`).
- **`eri-restate` (`RestateDeployment/eri-restate`, plus sibling `RestateDeployment/eri-<agent>-restate` on `:9080` + `:4028`, image `ghcr.io/azylman/aerial-eri-restate`):** Full Ubuntu 24.04 + CLI toolchains agent runtime worker pod. Owns each agent's isolated `/data` (`brain-data`, `ci-cache`, `ssh-state`) and `/mnt/nas` mounts, pod-local `/data/runtimes` `emptyDir` mount (and persistent `/data/brain/<rt>/.config` symlink), `ssh-bootstrap` initContainer, `env.Provisioner` (syncing rules/skills/hooks/MCP configs into `/data/runtimes` and binding the loopback MCP `TraceProxy` `/internal/mcp/*` and `/v1/logs` OTel log proxy on `HEALTH_PORT: 4028`, injecting the pod's `ServiceAccount` Bearer token), CLI process pools (`UnifiedProcessPool`, `InterchangeablePool`, `DynamicVoicePool`), outbound `DiscordSession`, NATS JetStream publisher `EventBus`, `Service/eri*-restate-health` on `:4028` (`/health`, `/healthz`, `/metrics`, `/debug/coverage/flush`, loopback `POST /-/reload`, `GET /internal/quota`), and `:9080` H2C cleartext HTTP/2 Restate actor server (`ThreadSessionServiceName(agent)`, `DispatchSessionServiceName(agent)`, `VoiceSessionServiceName(agent)`, unkeyed `SessionManagerServiceName(agent)`, and stateless `TurnRunnerServiceName(agent)`). Also serves the loopback-only PAT-injecting GitHub proxies (`/internal/github-api/`, `/internal/github-git/`) on `127.0.0.1:4028`, which scan every push and API write for registered secrets, so `aerial-pr.sh` reaches GitHub without depending on plain `eri`.
- **`discord-mcp` (`discord-mcp`, plus sibling `discord-mcp-<agent>`):** Bound 1:1 to each bot's `DISCORD_TOKEN`, network-isolated to its owning `eri-<agent>` and `eri-<agent>-restate` pods (plus primary `eri` and `eri-restate`).

---

## 6. Declarative Kubernetes Workload Identity & RBAC (`pkg/k8sauth`)

### 6.1 ServiceAccounts & Projected Bound Tokens
Every agent and control-plane service runs under a dedicated Kubernetes `ServiceAccount` in namespace `default`:
- **Agent ServiceAccounts:** `eri-aerial` (shared by primary `Deployment/eri` and `RestateDeployment/eri-restate`, replacing/aliasing legacy `eri`), and configured sibling `eri-<agent>` ServiceAccounts (e.g. `eri-<agent-a>`, `eri-<agent-b>`, shared by each sibling's `Deployment/eri-<agent>` and `RestateDeployment/eri-<agent>-restate`).
- **Control-Plane ServiceAccounts:** `synapse`, `vanadis`, `vanadis-restate`, `vanadis-nats`, `haro`, `coven`, `transcripts`, `console`.
- **Default ServiceAccount Hardening:** The `default` ServiceAccount in namespace `default` sets `automountServiceAccountToken: false`.
- **Explicit Token Projection:** Every `eri*`, `eri*-restate`, and control-plane pod mounts a projected `serviceAccountToken` volume at `/var/run/secrets/kubernetes.io/serviceaccount/token` with `expirationSeconds: 3600` (`kubelet` refreshes the token automatically before expiry).

### 6.2 Outbound Bearer Token Injection
Agents call internal services through two paths—MCP tool calls (routed through `eri-restate`'s loopback `TraceProxy` on `:4028`) and bash CLI scripts (`scripts/aerial-build.sh`, `scripts/aerial-pr.sh`, `transcript-search`) running inside `eri-restate`:
1. **`eri-restate` Loopback `TraceProxy` (`services/eri/pkg/mcp` bound on `:4028`):** On every proxied MCP HTTP request, `TraceProxy` reads the pod's projected token from `/var/run/secrets/kubernetes.io/serviceaccount/token` (cached in memory with a 60s stat/reload check so kubelet token rotation is picked up seamlessly) and sets `Authorization: Bearer <token>`.
2. **CLI Helper Scripts (`scripts/aerial-build.sh`, `scripts/aerial-pr.sh`, `transcript-search`):** When `/var/run/secrets/kubernetes.io/serviceaccount/token` exists on disk, the script attaches `-H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"` on all HTTP requests to `vanadis`, `synapse`, `haro`, and `transcripts`.
3. **Control-Plane & `eri` Inter-Service Calls (`eri`, `eri-restate`, `synapse`, `vanadis`, `vanadis-restate`, `haro`, `coven`, `console`):** Outbound HTTP clients to peer control-plane services wrap their `http.RoundTripper` with `k8sauth.NewServiceAccountTransport(baseTransport, tokenPath)` so internal calls carry their own `ServiceAccount` JWT.

### 6.3 Two-Tier Static RBAC Model with Claim-Based Self-Ownership
Only **two** virtual-resource `ClusterRole`s exist for agent authorization (never duplicated per agent):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: aerial-agent
rules:
  - apiGroups: ["aerial.io"]
    resources: ["builds", "schedules", "prs", "transcripts", "memories"]
    verbs: ["get", "list", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: aerial-supervisor
rules:
  - apiGroups: ["aerial.io"]
    resources:
      - "builds"
      - "builds/all"
      - "schedules"
      - "schedules/all"
      - "prs"
      - "prs/all"
      - "transcripts"
      - "transcripts/all"
      - "memories"
      - "memories/all"
    verbs: ["get", "list", "create", "update", "patch", "delete"]
```

- **`ClusterRoleBinding/aerial-agents`:** Binds `ClusterRole/aerial-agent` to `ServiceAccount/eri-aerial` and configured sibling `ServiceAccount/eri-<agent>` instances.
- **`ClusterRoleBinding/aerial-supervisors`:** Binds `ClusterRole/aerial-supervisor` to `ServiceAccount/eri-aerial` and internal control-plane ServiceAccounts (`synapse`, `vanadis`, `vanadis-restate`, `vanadis-nats`, `haro`, `coven`, `console`). Because `aerial-supervisor` explicitly grants both base resources (`builds`, `schedules`, `prs`, `transcripts`, `memories`) and `/all` subresources, supervisor principals can read any resource and manage cross-agent workflows, while agents bound only to `aerial-agent` remain restricted to self-owned mutations.
- **`ClusterRole/aerial-auth-delegator`:** Grants `create` on `authentication.k8s.io/tokenreviews` and `authorization.k8s.io/subjectaccessreviews` to all control-plane ServiceAccounts (`vanadis`, `vanadis-restate`, `vanadis-nats`, `haro`, `synapse`, `coven`, `transcripts`, and `console`) so every service enforcing `pkg/k8sauth` holds valid delegation permissions.

### 6.4 `pkg/k8sauth` Verification & Authorization Contract
A new shared package `pkg/k8sauth` in `pkg/` implements token verification, agent claim extraction, RBAC evaluation, and caching:

1. **Identity Extraction (`Authenticate(ctx, bearerToken) -> CallerIdentity`):**
   - Calls `authentication.k8s.io/v1` `TokenReview`.
   - Rejects unauthenticated tokens with `401 Unauthorized`.
   - Parses `status.user.username` (`system:serviceaccount:<ns>:<sa-name>`):
     - Uses generic configurable prefix matching and data-driven alias mapping (configured via service YAML / `config.yaml`, e.g. `agent_sa_prefix: "eri-"` and optional `sa_agent_aliases: {"eri": "aerial", "eri-aerial": "aerial"}`) rather than embedding agent string literals in Go code.
     - If `<sa-name>` matches an entry in `sa_agent_aliases`, `CallerAgent = sa_agent_aliases[saName]`.
     - Else if `<sa-name>` has prefix `agent_sa_prefix`, `CallerAgent = strings.TrimPrefix(saName, agent_sa_prefix)`.
     - Otherwise (internal control-plane service account such as `"synapse"` or `"vanadis-restate"`), `CallerAgent = saName` and `IsService = true`.
2. **Authorization (`Authorize(ctx, CallerIdentity, Resource, Verb, TargetAgent) -> error`) & Ownership Stamping:**
   - **Read Operations (`Verb` is `"get"` or `"list"`):** Evaluates `SubjectAccessReview` for `apiGroup: "aerial.io", resource: Resource, verb: Verb`. Both `aerial-agent` members and `aerial-supervisor` members pass because `aerial-supervisor` explicitly includes base resources (`builds`, `schedules`, `prs`, `transcripts`, `memories`).
   - **Write Operations (`Verb` in `"create"`, `"update"`, `"patch"`, `"delete"`):**
     - **Self-Owned Target (`CallerIdentity.CallerAgent == TargetAgent`):** Evaluates `SubjectAccessReview` for `apiGroup: "aerial.io", resource: Resource, verb: Verb`.
     - **Cross-Agent or Global Admin Target (`CallerIdentity.CallerAgent != TargetAgent` or `TargetAgent == "*"`):** Evaluates `SubjectAccessReview` for `apiGroup: "aerial.io", resource: Resource + "/all", verb: Verb`. Only principals bound to `ClusterRole/aerial-supervisor` pass; sibling agents holding only `ClusterRole/aerial-agent` receive `403 Forbidden`.
   - **Service Principal Ownership Stamping Contract (`CallerIdentity.IsService == true`):**
     - When control-plane services (`synapse`, `vanadis`, `vanadis-restate`, `vanadis-nats`, `haro`, `coven`) call resource creation/mutation endpoints (`POST /builds`, `POST /internal/pr/register`, schedule creation, `memory_store`), the caller **MUST** supply an explicit `TargetAgent` parameter identifying the owning agent.
     - Ownership is **never** defaulted to `CallerIdentity.CallerAgent` (which is the service name, e.g. `"synapse"` or `"vanadis"`).
     - If `TargetAgent` is omitted, empty, or not found in configured agent rosters, the request is immediately rejected with `400 Bad Request` (`"missing or unknown target agent for service principal"`).
     - Because `CallerIdentity.CallerAgent != TargetAgent`, Authorize evaluates `resource: Resource + "/all"`, which passes for supervisor services bound to `aerial-supervisor`.
   - **Agent Principal Ownership Stamping Contract (`CallerIdentity.IsService == false`):**
     - For `eri-<agent>` callers, `TargetAgent` defaults to `CallerIdentity.CallerAgent` (self-ownership).
     - If an agent explicitly specifies a different `TargetAgent`, Authorize evaluates `<resource>/all`, which is rejected with `403 Forbidden` for standard agents.
   - Zero agent names are hardcoded in `Authorize`: supervisor privileges are resolved strictly by asking `kube-apiserver` whether the caller's `user`/`groups`/`extra` hold the `<resource>/all` permission.

   **Authorization Outcome Matrix:**

   | Caller Principal | Bound ClusterRoles | Read (`get`/`list`) on `<resource>` | Self Write on `<resource>` (`caller == target`) | Cross-Agent / Supervised Write (`caller != target` or `target == "*"`) |
   | :--- | :--- | :--- | :--- | :--- |
   | **Sibling Agent** (`eri-<agent>`) | `aerial-agent` | **Allowed** (`resource: <resource>`) | **Allowed** (`resource: <resource>`) | **Denied (403)** (`resource: <resource>/all` denied) |
   | **Supervisor Service** (`synapse`, `vanadis`, `haro`, `coven`, `console`) | `aerial-supervisor` | **Allowed** (`resource: <resource>`) | **Allowed** (`resource: <resource>/all` on behalf of explicit target) | **Allowed** (`resource: <resource>/all` with mandatory explicit target) |
   | **Primary Operator** (`eri-aerial`) | `aerial-agent`, `aerial-supervisor` | **Allowed** (`resource: <resource>`) | **Allowed** (`resource: <resource>`) | **Allowed** (`resource: <resource>/all`) |

3. **Fail-Closed Keyed Cache & Stale-Authorization Interval:**
   - `pkg/k8sauth` caches positive `TokenReview` results keyed by `sha256(bearerToken)` and positive `SubjectAccessReview` decisions keyed by `(username, apiGroup, resource, verb)` for **45 seconds** (configurable, default `45s`, bounded to 1,024 LRU entries).
   - **Stale-Authorization Interval & Security Risk Decision:** To prevent `kube-apiserver` saturation during parallel multi-step MCP agent loops, positive decisions are cached for up to 45 seconds. The maximum window during which a revoked `ServiceAccount`, deleted pod, or altered RBAC role binding remains authenticated/authorized in memory is strictly bounded to 45 seconds. This trade-off is accepted as an operational compromise between API server request volume and revocation immediacy (dramatically tighter than standard 1-hour projected JWT token lifetimes).
   - **Fail-Closed Semantics:** Uncached tokens, expired cache entries, negative `TokenReview` outcomes, authorization denials, and `kube-apiserver` transport/timeout errors are **never cached** and immediately fail closed (`401 Unauthorized`, `403 Forbidden`, or `503 Service Unavailable`).
   - Health/metrics/coverage endpoints (`/health`, `/healthz`, `/metrics`, `/debug/coverage/flush`) and external webhook ingress (`/webhooks/github`, `/webhooks/flux`) bypass `pkg/k8sauth` (external webhooks use HMAC/token verification).

### 6.5 `kubernetes-mcp` Native Bearer Passthrough & K8s Resource RBAC
`containers/kubernetes-mcp-server` natively forwards the caller's HTTP `Authorization: Bearer <token>` header to `kube-apiserver`.
- **Configuration (`k8s/observability-and-mcp.yaml`):** Set `require_oauth = true` in `kubernetes-mcp`'s config TOML so unauthenticated requests fail closed and never fall back to the pod's own `ServiceAccount`.
- **Read-Only Cluster Visibility for All Agents (`ClusterRole/aerial-k8s-reader`):**
  - Bound to `ServiceAccount/eri-aerial` and configured sibling `ServiceAccount/eri-<agent>`.
  - Grants `get`, `list`, `watch` on standard core/apps/batch/networking/flux/restate resources (`pods`, `pods/log`, `services`, `deployments`, `statefulsets`, `daemonsets`, `replicasets`, `jobs`, `cronjobs`, `events`, `configmaps`, `persistentvolumeclaims`, `namespaces`, `nodes`, `nodes/stats`, `nodes/log`, `nodes/metrics`, `networkpolicies`, `httproutes`, `kustomizations`, `helmreleases`, `restatedeployments`, `restateclusters`).
  - **Explicitly excludes `secrets` and `pods/exec`**. When a sibling agent invokes `kubernetes-mcp` to read a `Secret` or exec into a pod, `kube-apiserver` rejects the call with a standard Kubernetes RBAC `Forbidden` error.
- **Supervisor Cluster Management (`ClusterRoleBinding` / `RoleBinding` for `eri-aerial`):**
  - Bind existing `Role/aerial-kubernetes-mcp-exec`, `Role/aerial-kubernetes-mcp-flux`, and cluster admin management privileges directly to `ServiceAccount/eri-aerial`.

---

## 7. L4 `NetworkPolicy` Hardening (Zero `uid=1000` Bash Bypass)

Because agents can run arbitrary shell commands (`curl`) as `uid=1000` inside `RestateDeployment/eri*-restate`, L4 `NetworkPolicy` manifests enforce strict pod-level segmentation across both `Deployment/eri*` (`:8080`) and `RestateDeployment/eri*-restate` (`:9080` + `:4028`) in addition to L7 `pkg/k8sauth`:

1. **`infisical-mcp-ingress-netpol` (`default` namespace):**
   - Allows ingress on `:8080` **only** from primary `eri` and `eri-restate` (`app.kubernetes.io/name in (eri, eri-restate)`) plus `vmsingle`/`gatus`/`coverage-ingest`. Sibling `eri-<agent>` and `eri-<agent>-restate` pods are blocked at L4 from connecting to `infisical-mcp`.
2. **`discord-mcp-*-ingress-netpol` (`default` namespace):**
   - `discord-mcp-ingress-netpol` allows ingress only from primary `eri` and `eri-restate` (`app.kubernetes.io/name in (eri, eri-restate)`).
   - Sibling `discord-mcp-<agent>-ingress-netpol` allows ingress only from its owning `app.kubernetes.io/name in (eri-<agent>, eri-<agent>-restate)` and primary `app.kubernetes.io/name in (eri, eri-restate)`.
3. **`restate-ingress-netpol` (`restate-green` / `restate` namespace):**
   - Allows ingress to `restate` (`:8080` ingress, `:9070` admin) from `vanadis`, `vanadis-restate`, `vanadis-nats`, `haro`, `synapse` (and sibling `vanadis-*-<agent>`, `haro-<agent>`, `synapse-<agent>` pods until Phases 2 and 3 retire them), `restate-operator`, `lan-gateway`, observability scrapers (`vmsingle`, `gatus`), stateless HTTP ingress `Deployment/eri` and `Deployment/eri-<agent>` (which run zero `uid=1000` LLM bash subshells and use `RestateIngressClient` on `:8080` for wake-funnel `ThreadSession.Enqueue`, `/prompt`, `/dispatch`, `/voice/ask`, and `SessionManager.CancelThread`), and primary `eri-restate`.
   - **Sibling `RestateDeployment/eri-<agent>-restate` Isolation from Restate Ingress/Admin:** During Phase 1 (while `eri-<agent>-restate` still runs `ReconcileRestateServiceTimeouts` against `:9070` and `FlushChunkActiveStepToRestate` against `:8080` `<BuildServiceName(agent)>/<buildID>/UpdateActiveStep/send`), `eri-<agent>-restate` is permitted on `restate-ingress-netpol`. In **Phase 2**, moving `ReconcileRestateServiceTimeouts` for all agents into unified `vanadis-restate` (or stateless `Deployment/eri-<agent>`) and routing `FlushChunkActiveStepToRestate` (`UpdateActiveStep`) through `POST http://vanadis.aerial:4025/builds/{id}/active-step` (authenticated by `pkg/k8sauth`) allows `restate-ingress-netpol` to remove sibling `eri-<agent>-restate` pods completely—guaranteeing that `uid=1000` bash subshells inside sibling agent worker pods cannot directly `curl` `restate:8080` or `:9070` to bypass `vanadis`, `haro`, or `synapse` RBAC.
4. **`eri-*-restate-ingress-netpol` & `eri-*-ingress-netpol` (`default` namespace — Restate `:9080` Actor Ingress, `:4028` Health/Internal Ingress & `:8080` Stateless Ingress):**
   - **`eri-restate-ingress-netpol` & `eri-<agent>-restate-ingress-netpol` (`:9080` + `:4028`):**
     - Allows TCP port `:9080` strictly from `namespaceSelector: kubernetes.io/metadata.name in (restate-green, restate, restate-operator)` so the Restate Server can invoke `ThreadSessionServiceName(agent)`, `DispatchSessionServiceName(agent)`, `VoiceSessionServiceName(agent)`, and `SessionManagerServiceName(agent)` over H2C HTTP/2.
     - Allows TCP port `:4028` strictly from its paired stateless `Deployment/eri` (`app.kubernetes.io/name: eri` for primary; `app.kubernetes.io/name: eri-<agent>` for each sibling, for `/internal/quota`) and telemetry/coverage scrapers (`victoriametrics`, `vmsingle`, `gatus`, `coverage-ingest`). No agent worker pod can reach another agent's `:4028` or `:9080` port.
   - **`eri-ingress-netpol` & `eri-<agent>-ingress-netpol` (`:8080` Stateless HTTP Front Door):**
     - **Immediate Peer-Agent `:8080` Block & Control-Plane Pruning (Phase 1):** Each `eri-<agent>-ingress-netpol` allows its own paired `eri-<agent>-restate` pod `synapse` / `synapse-<agent>` (for `/internal/discord/message` and `/internal/discord/interaction`), and telemetry/coverage scrapers (`victoriametrics`, `vmsingle`, `gatus`, `coverage-ingest`), while blocking all cross-agent `eri` / `eri-restate` pods so no agent can `curl` another agent's `:8080` `/prompt`, `/dispatch`, `/discord/message`, or `/internal/github-*` endpoints. Because `build-1795131906-b2b196f5` already cuts `haro` and `vanadis-restate` over to `http://restate.aerial:8080` and direct Discord REST widget delivery (deleting `POST /discord/widget`), `haro` and `vanadis` / `vanadis-restate` are also removed from `eri-*-ingress-netpol` immediately in Phase 1.
     - **Primary `eri` Stateless `/internal/llm` & `/discord/message` Ingress (Phase 1+):** Explicitly includes `coven` (`app.kubernetes.io/name: coven`) and `transcripts` (`app.kubernetes.io/name: transcripts`) on primary `eri-ingress-netpol` (`:8080`) so unified `coven` (`eri_llm_url` and `NotifyShortFriction` `POST /discord/message`) and `transcripts` session summarization can reach `http://eri.aerial` (which runs `/internal/llm` through the per-agent `LLM` Restate service), alongside `pjev`, `mirrormere-core`, `prospera`, `vision`, and scrapers.
     - **Phase 2 Extractor Selector Removal:** Retains sibling `coven-<agent>` (`role: "extractor"`, reading `/tasks` and `/internal/llm` until `messages` consolidates) in `eri-<agent>-ingress-netpol` during Phase 1, and removes `coven-<agent>` in Phase 2 once unified `coven` queries consolidated `messages` directly.

---

## 8. Unified Database Schema, NATS Subject Scoping & Data Migration

### 8.1 Per-Table Schema Changes (`services/eri/pkg/db/schema.sql`)
All multi-agent tables live in the single `aerial` PostgreSQL database and use the column name `agent TEXT NOT NULL DEFAULT 'aerial'`. Following the steady state of the per-stack indexers extraction rollout (`spl2`), `extraction_watermarks`, `session_summaries`, `transcript_steps`, `facts`, and `procedural_candidates` already store all agents' rows in the shared `aerial` database with composite primary keys `(stack, thread_id)`, `(stack, session_id)`, and `(stack, session_id, step_index)`. Schema changes are decoupled into two declarative migration steps across the rollout phases:

#### Phase 1 Telemetry & Extraction Migration (`001_phase1_transcripts_coven.sql`)
Shipped and applied in Phase 1 before deploying unified `transcripts` and `coven` MCP RBAC:
1. **`extraction_watermarks`:**
   - Rename column `stack` to `agent TEXT NOT NULL DEFAULT 'aerial'` on the existing composite primary key `PRIMARY KEY (agent, thread_id)`.
2. **`session_summaries` & `transcript_steps`:**
   - Rename column `stack` to `agent` (`PRIMARY KEY (agent, session_id)` and `PRIMARY KEY (agent, session_id, step_index)` with foreign key `(agent, session_id) REFERENCES session_summaries(agent, session_id) ON DELETE CASCADE`).
3. **`facts` & `procedural_candidates`:**
   - Add `source_agent TEXT NOT NULL DEFAULT 'aerial'` to `facts` (recording which agent stored or extracted the fact) and `agent TEXT NOT NULL DEFAULT 'aerial'` to `procedural_candidates`.

#### Phase 2 Core Multi-Agent Migration (`002_phase2_core_multiagent.sql`)
Shipped and applied in Phase 2 before the cross-database backfill and `eri` cutover:
1. **`messages`:**
   - Add `agent TEXT NOT NULL DEFAULT 'aerial'`.
   - Change primary key from `PRIMARY KEY (id)` to `PRIMARY KEY (agent, id)` so two agents present in the same Discord channel/thread can both ingest the same Discord message snowflake `id` without collision.
   - Replace the global unique constraint on `row_id` (`row_id BIGSERIAL UNIQUE`) with a composite unique constraint `CONSTRAINT uq_messages_agent_row_id UNIQUE (agent, row_id)`. This guarantees that historical `row_id` values from sibling databases do not collide during consolidation, keeping `extraction_watermarks.last_extracted_rowid` valid per `(agent, thread_id)`.
   - Add indexes: `idx_messages_agent_status_created_at ON messages(agent, status, created_at ASC)`, `idx_messages_agent_thread_row_id ON messages(agent, thread_id, row_id)`, and `idx_messages_agent_row_id ON messages(agent, row_id)`.
2. **`sessions`:**
   - Add `agent TEXT NOT NULL DEFAULT 'aerial'`.
   - Change primary key from `PRIMARY KEY (session_id)` to `PRIMARY KEY (agent, session_id)` so each agent maintains an independent conversation session (`conversation_id`, `turn_count`, `summary`) per Discord thread.
3. **`dispatches`:**
   - Add `agent TEXT NOT NULL DEFAULT 'aerial'`.
   - Keep `id TEXT PRIMARY KEY` (dispatch IDs are generated UUIDs/chunk keys).
   - Replace the global unique constraint on `row_id` (`row_id BIGSERIAL UNIQUE`) with a composite unique constraint `CONSTRAINT uq_dispatches_agent_row_id UNIQUE (agent, row_id)`.
   - Add indexes: `idx_dispatches_agent_status_created_at ON dispatches(agent, status, created_at ASC)` and `idx_dispatches_agent_session_row_id ON dispatches(agent, session_id, row_id)`.
4. **`one_shot_schedules`, `cron_schedules`, `heartbeat_schedules`, `schedule_runs`, `event_triggers`:**
   - Add `agent TEXT NOT NULL DEFAULT 'aerial'` and index `(agent, ...)` on lookup columns.
5. **`vanadis_builds`:**
   - Add `agent TEXT NOT NULL DEFAULT 'aerial'` and index `idx_vanadis_builds_agent_status ON vanadis_builds(agent, status)`. (`vanadis_chunks` inherits ownership via `build_id REFERENCES vanadis_builds(id) ON DELETE CASCADE`.)
6. **`pr_registry` (Read-Only Projection Written by `PRObject` in `vanadis-restate`):**
   - Add `agent TEXT NOT NULL DEFAULT 'aerial'` and index `idx_pr_registry_agent_status ON pr_registry(agent, status)`. Keep `UNIQUE (repo, pr_number)` as each PR is owned by the single agent that registered it (or the agent entry with `default_owner: true` in `synapse.yaml`, e.g. `'aerial'`, for external human/Dependabot PRs). Under the Restate PR lifecycle architecture (`services/vanadis-restate/internal/prstate`), `PRObject` is the sole ordered writer projecting `pr_registry` (including `agent`) via journaled `ProjectRegistry` steps, while `synapse` uses `pr_registry` strictly as a read-only projection and SHA lookup index.

### 8.2 Restate Virtual Object Routing, NATS JetStream Single-Account Consolidation & Authorization ACLs
Following the steady state of the Restate graceful-draining build (`build-1795131906-b2b196f5`, which splits stateless HTTP ingress `Deployment/eri*` on `:8080` from stateful Restate worker `RestateDeployment/eri*-restate` on `:9080` + `:4028` and purges `eri`'s legacy NATS job consumers `eri-prompt`, `eri-dispatch`, and `eri-discord-message` in `c09a-purge-eri-nats-consumers`) and the Restate Virtual Object alignment builds (`build-1795155723-4fa121f1` and `build-1795158435-e6cdf162`), **all turn and dispatch execution routes through Restate Virtual Objects on `http://restate.aerial:8080` with strict at-least-once semantics, while `eri*-restate` operates strictly as a NATS publisher** (and stateless `eri*` `:8080` subscribes only to Core NATS `voice.stream.<turn_id>` during active voice requests):
- **Per-Agent `eri-restate` Restate Virtual Objects & Unkeyed `SessionManager` (`services/eri-restate/internal/app/app.go`, `services/eri/pkg/queue/restate_actors.go`):** Each `RestateDeployment/eri*-restate` registers six per-agent Restate services on the single `restate` cluster over cleartext HTTP/2 (`SetUnencryptedHTTP2(true)` on `:9080`) with `inactivity_timeout: 1h` and `abort_timeout: 1h` reconciled on startup and `/discover`:
  - **Restate turn queue (the only turn queue):** Restate owns turn order, coalescing (bursts of up to 5 messages), one active turn per key, retries, the crash cap, delays, and cancellation. Postgres `messages`/`dispatches` rows are records only: PENDING when queued, PROCESSING when the turn starts, COMPLETED/FAILED when it ends. No schema change.
    - **Coordinators (`ThreadSessionServiceName(agent)` keyed by `thread_id`, `DispatchSessionServiceName(agent)` keyed by `session_id`; `ThreadSession`/`DispatchSession` for `aerial`, suffixed `-<agent>` for siblings):** state keys `pending` (FIFO ID list) and `active` (`TurnID`, `ItemIDs`, `RunnerInvocationID`, `Waiting`). Shared handlers: `Enqueue` (journals the row insert and validation inside `restate.Run`, returns terminal 400 on invalid requests), `CancelTurn` (thread) / `Cancel` (dispatch), and `Status`. Exclusive handlers (each holds the lock for milliseconds): `Append` (dedups IDs; the `SkipIfActive` guard only skips rows whose outcome is recorded by a `record-skipped` step), `OnTurnDone`, `Kick` (thread only), and `ClearPending`. `startNext` takes up to 5 pending IDs (1 for dispatch), derives a deterministic turn ID, `ServiceSend`s the runner, stores the invocation ID in `active`, and clears all state when idle. `OnTurnDone` is fenced on `active.TurnID`; outcome `reschedule` re-queues the IDs at the front, keeps `active` with `Waiting=true`, and self-sends a delayed `Kick`; every other outcome clears `active` and starts the next turn. Coordinators declare 1h inactivity and abort timeouts.
    - **`TurnRunner` (`TurnRunner` / `TurnRunner-<agent>`, stateless, `RunBurst` and `RunDispatch`):** steps `record-started` (claims PENDING rows; adopts own PROCESSING rows; skips terminal/missing), `prep` (policy, ambient classification, pre-turn hook, prompt), `run-<n>` (one agent attempt each, with `restate.Sleep` backoff between), `record-outcome`, `post-turn-hook`, `refresh-thread-summary` (bursts), `publish-dispatch-outcome` (dispatches), then `OnTurnDone`. Every `run-<n>` re-reads its rows first (all terminal: return the recorded outcome, never re-run the agent), then `BeginAttempt` per row. Success keeps Discord delivery and the COMPLETED write inside the same step. Declares 4h15m inactivity and 1h abort timeouts, covering the longest permitted turn.
    - **Crash cap and attempts:** a re-entered attempt bumps `restart_count`; at 3 the rows are FAILED "crash loop". `retry_count` is written only by `BeginAttempt` and the pre-turn requeue; attempts are capped at 3.
    - **Timeout outcome:** a per-attempt time limit (watchdog or the attempt's own deadline) is the distinct `timeout` outcome: failure notice, rows FAILED, no further attempt. A parent-context cancel is an aborted attempt: nothing is written and the step retries.
    - **Retries:** no custom retry policy or run bounds anywhere; steps retry under Restate's server default (exponential backoff, then pause). Store calls run under a 5s deadline and are classified: deterministic SQLSTATEs (classes 22, 23, 42, 0A, 2B/2D/25) and panics are terminal at once and take `fail-turn`; everything else retries, so transient DB outages are waited out. Long outages park runners as paused invocations, which keep their journal and are resumed by hand. Terminal code 409 is reserved for Restate cancellation; our own conflicts use 412.
    - **Cancel:** `CancelTurn` marks the snapshot's still-PENDING rows FAILED by ID, then sends `ClearPending` (exclusive), which removes the IDs and calls `CancelInvocation` on the runner when the active turn matches the snapshot (an active turn carrying later-appended IDs is not cancelled). The runner's `stop-turn` step kills the agent process on its pod, records "cancelled by operator", sends no failure notice, and reports `cancelled`; a late `OnTurnDone` is fenced. `SessionManager.CancelDispatch` fans a dispatch cancel out to the matching coordinator.
    - **Stuck-turn recovery:** a bare `/interrupt` is the recovery path. It is admin-only, routed through the interactions router, and answers with a deferred ephemeral reply that says whether a turn was running. If the runner is still non-completed after 10 minutes, admin-kill it, mark its input rows FAILED by ID, and call `ClearPending{CancelActive, TurnID}` on the key.
  - `VoiceSessionServiceName(agent)` (`VoiceSession` for `aerial`, `VoiceSession-<agent>` for sibling agents, keyed by `session_id`): exclusive `Ask` streaming `VoiceStreamEvent` over Core NATS `voice.stream.<turn_id>` (wrapping only 400 validation errors in `restate.ToTerminalError` so transient/drain errors retry at-least-once).
  - `SessionManagerServiceName(agent)` (`SessionManager` for `aerial`, `SessionManager-<agent>` for sibling agents, unkeyed Restate service): `CancelThread` and `CancelDispatch`, allowing stateless `eri` (`:8080` `/interrupt` and `/dispatch/cancel`) and `vanadis-restate` to preempt running invocations immediately without queueing behind an exclusive Virtual Object key lock.
- **Direct Discord Status Widget & Alert Delivery (`services/vanadis-restate/internal/widget/discord_sender.go`, `internal/build/active_step.go`):** `POST /discord/widget` on `eri` is deleted (`c09b-purge-eri-legacy-widgets`). `vanadis-restate` delivers Components V2 build status widgets and PR alerts directly to Discord REST via `DiscordWidgetDeliverer` (serializing per-channel sink/send/delete/edit operations via `channelLocks sync.Map` and running `BuildObject.UpdateActiveStep` as an exclusive `restate.ObjectContext` handler reusing `b.executor.SyncBuildWidget`, per PRs #2211 and #2212) using a multi-bot `agent -> *discordgo.Session` map (`DISCORD_TOKEN` / `DISCORD_TOKEN_<AGENT>`).
- **Shared Transcripts & Event Streams:** `AERIAL_TRANSCRIPTS` (`aerial.transcripts.steps.<agent>.>`) and `AERIAL_EVENTS` (`aerial.events.<agent>.>`) carry the `agent` identifier in the subject token on the single `aerial` NATS account. Under the `spl2` steady-state pipeline, each `eri*-restate` pod publishes `TranscriptStepBatch` messages to `aerial.transcripts.steps.<agent>.<session_id>`, unified `transcripts` indexes the steps and emits `TranscriptsIndexedEvent` (`aerial.turn.transcripts_indexed`) carrying `Agent`, and `coven` (`coven-transcripts-indexed`) routes extraction jobs to `aerial.coalesced.coven_extract.<agent>.<thread_id>` (`coven-extract`), retiring separate per-agent NATS accounts.
- **NATS User Authentication, Subject-Level Authorization ACLs & `uid=1000` Environment Hardening:**
  To guarantee sibling process isolation and prevent any workload (such as a `uid=1000` LLM shell inside an `eri*-restate` pod or a compromised sibling daemon) from spoofing transcript/extraction events on the shared bus:
  1. **Strip `NATS_URL`, `RESTATE_INGRESS_URL`, `RESTATE_ADMIN_URL` & Credentials from the `uid=1000` LLM Environment (`services/eri/pkg/runner/pure.go`):**
     - Add `"NATS_URL"`, `"RESTATE_INGRESS_URL"`, and `"RESTATE_ADMIN_URL"` to `exactSecretKeys` in `FilterAgyBaseEnv` so they are unconditionally stripped alongside `DATABASE_URL`, `POSTGRES_PASSWORD`, and all `*_PASSWORD` / `*_SECRET` / `*_TOKEN` environment variables before `eri*-restate` spawns the `uid=1000` LLM CLI.
     - Pass NATS credentials to the `PID 1` (`uid=0`) Go daemons (`eri*` and `eri*-restate`) separately via `NATS_USER` / `NATS_PASSWORD` (`nats.UserInfo`, or mounted at `/secrets/nats:ro` with `0400` owned by `uid=0`) so `NATS_URL` is a credential-free `nats://nats.aerial:4222` and `uid=1000` child processes have zero NATS credentials in environment variables or on disk.
  2. **Strictly Authenticated Connections & Per-Agent Users Inside the Single `aerial` NATS Account (`nats.conf`):**
     - Anonymous or unauthenticated access is completely disabled on NATS.
     - Because JetStream streams (`AERIAL_JOBS`, `AERIAL_EVENTS`, `AERIAL_TRANSCRIPTS`, `AERIAL_COALESCED`) belong to the NATS **account**, all cluster services and agents connect to the single `aerial` NATS account (`accounts.aerial`), avoiding cross-account stream imports/exports while authenticating as distinct users defined in `accounts.aerial.users` (`nats-server-config` `ExternalSecret` synced from Infisical):
       - **Control-Plane User (`user: "aerial"`):** Used by `synapse`, `vanadis`, `vanadis-nats`, `vanadis-restate`, `haro`, `coven`, and `transcripts` (each holding its scoped client secret). Granted full publish/subscribe across `aerial.>` and `$JS.API.>`.
       - **Per-Agent Users (`user: "eri-aerial"`, `user: "eri-<agent>"`):** Because `c09a-purge-eri-nats-consumers` deletes all NATS JetStream job consumers from `eri` and `eri-restate`, each agent's `eri-<agent>-restate` and `eri-<agent>` pods operate strictly as NATS publishers (plus Core NATS `voice.stream.>` pub/sub for voice SSE) and authenticate with their own dedicated NATS user and password (`NATS_ERI_<AGENT>_PASSWORD`) subject to broker-enforced `permissions` in `nats.conf`:
         - `subscribe`: Strictly restricted to Core NATS `voice.stream.>` and temporary JetStream/NATS publish-ack reply subjects `_INBOX.>`. Attempting to subscribe to any `aerial.>` or `$JS.>` subject is denied by the NATS broker.
         - `publish`: Strictly restricted to its own agent-scoped outbound subjects (`aerial.transcripts.steps.<agent>.>`, `aerial.events.<agent>.>`, `aerial.turn.<agent>.>`, `aerial.dispatch.<agent>.>`), `voice.stream.>`, and `_INBOX.>`. Because `transcripts` and `coven` (running as the control-plane `aerial` NATS user) emit `aerial.turn.transcripts_indexed` and `aerial.coalesced.coven_extract.<agent>.<thread_id>`, `eri-<agent>` holds zero publish permissions on `aerial.coalesced.>`, `aerial.jobs.>`, or `aerial.transcripts.steps.<other-agent>.>`.
  3. **Consumer-Side Subject-Token Identity Validation (`transcripts` & `coven`):**
     - Services consuming agent-attributed streams derive the owning agent directly from the broker-authenticated NATS subject token rather than trusting unverified payload fields:
       - `services/transcripts` extracts `<agent>` from `aerial.transcripts.steps.<agent>.>`, verifies that `<agent>` is a configured agent and matches `batch.Agent`, and rejects any mismatched or unconfigured payload before stamping `TranscriptsIndexedEvent.Agent = <agent>`.
       - `services/coven` validates `ev.Agent` on `TranscriptsIndexedEvent` (`aerial.turn.transcripts_indexed`) before publishing to `aerial.coalesced.coven_extract.<agent>.<thread_id>`, and extracts `<agent>` from `aerial.coalesced.coven_extract.<agent>.>` to verify it is a configured agent matching the job payload.

### 8.3 PostgreSQL Role, Process Sandbox Boundary & Aerial Direct Operator Access (Answered by Prior Session Decision)
Per prior engineering decisions (retiring per-stack DB role migration chunks) and the supervisor privilege requirement:
- **Process Sandbox Boundary (`uid=0` vs. `uid=1000`):** In the `spl2` and Restate graceful-draining steady state (`c09c-slim-eri-http-image`), `eri`'s legacy in-process transcript DB worker (`StartTranscriptSync`, `NewRoutedTranscriptStore`), startup resend scan (`RunStartupResend`, `ResendOnStartup`), leader lock (`startLeaderHandoff`), and unrotated-session DB poller (`checkUnrotatedSessions`, `FindUnrotatedSessions`) are all deleted, so each `eri-<agent>` and `eri-<agent>-restate` Go daemon process (`PID 1`, running as `uid=0`) queries only `messages`, `sessions`, and `dispatches`. Once consolidated in Phase 2, all `eri-<agent>` and `eri-<agent>-restate` daemons connect to the unified `aerial` PostgreSQL database using the `aerial` PostgreSQL role and scope all `messages`, `sessions`, and `dispatches` queries by `WHERE agent = $1`. `services/eri/pkg/runner/pure.go` (`FilterAgyBaseEnv`) strips `POSTGRES_PASSWORD`, `DATABASE_URL`, `NATS_URL`, `RESTATE_INGRESS_URL`, `RESTATE_ADMIN_URL`, and all `*_SECRET`/`*_TOKEN`/`*_PASSWORD` environment variables before `eri-<agent>-restate` spawns the `agy`/`claude`/`codex`/`muse` CLI as `uid=1000` (and `/proc/1/environ` and `/secrets` are `0400` owned by `uid=0`), guaranteeing that `uid=1000` bash subshells inside sibling agent workers (`eri-<agent>-restate`) have zero PostgreSQL or NATS credentials in memory or on disk.
- **Aerial Direct Database & Secret Access:** The primary `eri-aerial` agent (`Deployment/eri` + `RestateDeployment/eri-restate`) retains full direct database and secret access through its supervisor operator boundaries:
  1. `infisical-mcp` is accessible exclusively to primary `eri` and `eri-restate` (`infisical-mcp-ingress-netpol`), allowing Aerial to read/rotate `POSTGRES_PASSWORD` and cluster secrets directly.
  2. `kubernetes-mcp` with `require_oauth = true` grants `ServiceAccount/eri-aerial` full `Secret` read/write and `pods/exec` permissions (including exec into `postgres-1` and running direct `psql` connections from `eri-restate`).
- **Retirement of Redundant CNPG Roles & Database CRDs:** Retire redundant per-stack CloudNativePG managed roles in `k8s/cluster-overrides.yaml`, their associated `ExternalSecret` role secrets, and per-stack `Database` CRDs.

### 8.4 One-Time Cross-Database Backfill Migration & Reconciliation
Before retiring sibling logical PostgreSQL databases in Phase 2, an idempotent data migration copies existing rows from sibling databases into the consolidated `aerial` database:

1. **Backfill Conflict Targets, Serial-ID Remapping & Multi-Index Idempotency:**
   - Every backfill `INSERT` statement explicitly stamps `agent = '<source_agent>'` (or `source_agent = '<source_agent>'` on `facts`) and is designed for full re-run idempotency without dropping sibling rows on serial primary key collisions:
     - `messages`: `INSERT INTO messages (...) SELECT ... ON CONFLICT DO NOTHING;` (bare `ON CONFLICT DO NOTHING` safely handles both unique indexes: `PRIMARY KEY (agent, id)` and composite `UNIQUE (agent, row_id)` without raising unique-violation errors on re-runs, preserving historical `row_id` values so `extraction_watermarks.last_extracted_rowid` remains valid per `(agent, thread_id)`).
     - `dispatches`: `INSERT INTO dispatches (...) SELECT ... ON CONFLICT DO NOTHING;` (bare `ON CONFLICT DO NOTHING` safely handles both `PRIMARY KEY (id)` and composite `UNIQUE (agent, row_id)`).
     - `sessions`: `INSERT INTO sessions (...) SELECT ... ON CONFLICT (agent, session_id) DO NOTHING;`
     - `extraction_watermarks`: `INSERT INTO extraction_watermarks (thread_id, agent, last_extracted_rowid, fact_extracted_at, updated_at) SELECT thread_id, '<source_agent>', last_extracted_rowid, fact_extracted_at, updated_at FROM ... ON CONFLICT (agent, thread_id) DO NOTHING;`
     - `session_summaries`: `INSERT INTO session_summaries (...) SELECT ... ON CONFLICT (agent, session_id) DO NOTHING;`
     - `transcript_steps`: `INSERT INTO transcript_steps (...) SELECT ... ON CONFLICT (agent, session_id, step_index) DO NOTHING;`
     - `one_shot_schedules`, `cron_schedules`, `heartbeat_schedules`: `INSERT INTO ... SELECT ... ON CONFLICT (id) DO NOTHING;`
     - `schedule_runs`: `INSERT INTO schedule_runs (...) SELECT ... ON CONFLICT (id) DO NOTHING;`
     - `event_triggers`: `INSERT INTO event_triggers (...) SELECT ... ON CONFLICT (id) DO NOTHING;`
     - `vanadis_builds`: `INSERT INTO vanadis_builds (...) SELECT ... ON CONFLICT (id) DO NOTHING;`
     - `vanadis_chunks`: `INSERT INTO vanadis_chunks (...) SELECT ... ON CONFLICT (id) DO NOTHING;`
     - **Serial-PK Tables (`facts`, `procedural_candidates`, and `pr_registry` — Target ID Allocation without Cross-DB ID Collisions):** Because each sibling database independently allocated `BIGSERIAL id` values starting at `1` (and no foreign keys reference `facts.id`, `procedural_candidates.id`, or `pr_registry.id`), the backfill **omits the source `id` column** on insert so the target `aerial` database allocates fresh `BIGSERIAL id` values rather than colliding with `aerial`'s `id = 1..N`:
       - `facts`: `INSERT INTO facts (source_agent, fact_text, embedding, created_at, last_reinforced_at, reinforce_count) SELECT '<source_agent>', s.fact_text, s.embedding, s.created_at, s.last_reinforced_at, s.reinforce_count FROM source_facts s WHERE NOT EXISTS (SELECT 1 FROM facts t WHERE t.source_agent = '<source_agent>' AND t.fact_text = s.fact_text AND t.created_at = s.created_at);`
       - `procedural_candidates`: `INSERT INTO procedural_candidates (agent, thread_id, domain, loaded_skill, trigger_pattern, steps_json, avoid_json, embedding, status, pr_number, created_at, updated_at) SELECT '<source_agent>', s.thread_id, s.domain, s.loaded_skill, s.trigger_pattern, s.steps_json, s.avoid_json, s.embedding, s.status, s.pr_number, s.created_at, s.updated_at FROM source_procedural_candidates s WHERE NOT EXISTS (SELECT 1 FROM procedural_candidates t WHERE t.agent = '<source_agent>' AND t.thread_id = s.thread_id AND t.trigger_pattern = s.trigger_pattern AND t.created_at = s.created_at);`
       - `pr_registry`: Populated via a staged deterministic deduplication query (Section 8.4.2) that copies rows from all source sibling databases into a temporary staging table, selects the winning row per `(repo, pr_number)` ordered by disambiguation priority, and inserts into `pr_registry` omitting source `id` so target `id` `BIGSERIAL` values are generated without collisions.
   - A full re-run of each statement is exercised in testing to verify multi-index idempotency.
2. **Deterministic `pr_registry` Deduplication:**
   - Because legacy webhook fan-out forwarded GitHub/Flux webhooks to multiple sibling synapse instances prior to consolidation, duplicate `(repo, pr_number)` rows can exist across sibling databases with different owning agents.
   - Merging `pr_registry` executes an explicit two-step staging and deduplication workflow:
     1. Source rows from each sibling database are inserted into a temporary staging table:
        ```sql
        CREATE TEMP TABLE _staging_pr_registry (
            source_db TEXT NOT NULL,
            agent TEXT NOT NULL,
            repo TEXT NOT NULL,
            pr_number INTEGER NOT NULL,
            branch TEXT NOT NULL,
            head_sha TEXT NOT NULL,
            merge_sha TEXT,
            target_id TEXT NOT NULL,
            status TEXT NOT NULL,
            title TEXT NOT NULL,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            priority INT GENERATED ALWAYS AS (
                CASE
                    -- Priority 1: explicit agent trailer in metadata matching agent
                    WHEN metadata->>'trailer_agent' = agent THEN 1
                    -- Priority 2: branch name includes agent prefix or identifier
                    WHEN branch LIKE '%' || agent || '%' THEN 2
                    -- Priority 3: primary default owner agent ('aerial')
                    WHEN agent = 'aerial' THEN 3
                    -- Priority 4: remaining fallback
                    ELSE 4
                END
            ) STORED
        );
        ```
     2. Winning rows are inserted into `pr_registry` using `DISTINCT ON`:
        ```sql
        INSERT INTO pr_registry (agent, repo, pr_number, branch, head_sha, merge_sha, target_id, status, title, metadata, created_at, updated_at)
        SELECT DISTINCT ON (repo, pr_number)
            agent, repo, pr_number, branch, head_sha, merge_sha, target_id, status, title, metadata, created_at, updated_at
        FROM _staging_pr_registry
        ORDER BY repo, pr_number, priority ASC, updated_at DESC;
        ```
     3. Dropped duplicate rows are recorded in an audit table:
        ```sql
        INSERT INTO pr_registry_backfill_dropped (source_db, agent, repo, pr_number, branch, head_sha, dropped_at)
        SELECT s.source_db, s.agent, s.repo, s.pr_number, s.branch, s.head_sha, CURRENT_TIMESTAMP
        FROM _staging_pr_registry s
        WHERE NOT EXISTS (
            SELECT 1 FROM pr_registry t
            WHERE t.repo = s.repo AND t.pr_number = s.pr_number AND t.agent = s.agent AND t.head_sha = s.head_sha
        );
        ```
3. **Post-Backfill Sequence Resynchronization:**
   - After copying rows (preserving explicit `row_id` on `messages`/`dispatches` and allocating target `id`s on `facts`, `procedural_candidates`, and `pr_registry`), all serial sequences are synchronized using the 3-argument `setval(seq, val, is_called)` form so both empty tables (`is_called = false`, next value 1) and populated tables (`is_called = true`, next value max+1) behave correctly:
     ```sql
     SELECT setval(pg_get_serial_sequence('messages', 'row_id'), coalesce(max(row_id), 1), max(row_id) IS NOT NULL) FROM messages;
     SELECT setval(pg_get_serial_sequence('dispatches', 'row_id'), coalesce(max(row_id), 1), max(row_id) IS NOT NULL) FROM dispatches;
     SELECT setval(pg_get_serial_sequence('facts', 'id'), coalesce(max(id), 1), max(id) IS NOT NULL) FROM facts;
     SELECT setval(pg_get_serial_sequence('procedural_candidates', 'id'), coalesce(max(id), 1), max(id) IS NOT NULL) FROM procedural_candidates;
     SELECT setval(pg_get_serial_sequence('pr_registry', 'id'), coalesce(max(id), 1), max(id) IS NOT NULL) FROM pr_registry;
     ```
4. **Pre-Cutover Reconciliation & Watermark Verification:**
   - Before any sibling database is retired or decommissioned, an automated reconciliation check verifies:
     - **Exact Table Row Counts:** Every table receiving an `agent` or attribution column is enumerated and verified against source sibling databases:
       - For tables with column `agent` (`messages`, `sessions`, `dispatches`, `extraction_watermarks`, `session_summaries`, `transcript_steps`, `one_shot_schedules`, `cron_schedules`, `heartbeat_schedules`, `schedule_runs`, `event_triggers`, `vanadis_builds`, and `procedural_candidates`):
         `SELECT count(*) FROM <table> WHERE agent = '<agent>'` on the consolidated `aerial` database must equal `SELECT count(*) FROM <table>` on the source sibling database.
       - For `facts` (using column `source_agent`):
         `SELECT count(*) FROM facts WHERE source_agent = '<agent>'` on the consolidated `aerial` database must equal `SELECT count(*) FROM facts` on the source sibling database.
       - For `vanadis_chunks` (which inherits ownership via `build_id`):
         `SELECT count(*) FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE b.agent = '<agent>'` on the consolidated `aerial` database must equal `SELECT count(*) FROM vanadis_chunks` on the source sibling database.
     - **`pr_registry` Deduplication Verification:** `SELECT count(DISTINCT (repo, pr_number))` across all source sibling databases equals `SELECT count(*)` in the consolidated `pr_registry`, and the surviving row's `agent` satisfies the Section 8.4.2 disambiguation rule.
     - **Watermark Consistency:** For each thread in `extraction_watermarks`, `last_extracted_rowid <= max(row_id)` in `messages` for that `agent` and `thread_id`.
     - Zero unmigrated rows or broken foreign key references exist across any table.

---

## 9. Service-by-Service Consolidation Contracts

### 9.1 `coven` (Phase 1 MCP RBAC & Phase 2 Multi-Agent Extractor Consolidation)
- **MCP Tool Surface & Phase 1 Hardening (`services/coven/tools.go`, `server.go`):**
  - Remove `memory_forget` from the MCP tool registration and handler switch. Fact deletions are restricted strictly to direct PostgreSQL operations by Aerial.
  - `memory_search`: Open to all authenticated `aerial-agent` callers across the shared `facts` table.
  - `memory_store` & `POST /admin/extract`: For agent callers (`IsService == false`), extracts `caller_agent` via `pkg/k8sauth` and stamps `source_agent = caller_agent` on inserted/reinforced `facts` rows (ignoring any client-supplied agent string). For service callers (`IsService == true`), requires an explicit `TargetAgent` parameter in the tool payload (rejected with 400 Bad Request if missing or unknown).
  - Rename `stack` -> `agent` across `extraction_watermarks` and `procedural_candidates` queries (`001_phase1_transcripts_coven.sql`) and point `IndexerClient` (`services/coven/indexer_client.go`) at the unified `http://transcripts.aerial:4030` service.
  - During Phase 1, sibling `coven-<agent>` (`role: "extractor"`) deployments remain active so they can continue fetching completed admin turns (`GetUnextractedAdminTurns`, `GetMaxMessageRowID`, `GetThreadContextBeforeWatermark` in `services/coven/db_extraction.go`) from sibling `eri-<agent>` `/tasks` while sibling `messages` still reside in per-agent databases.
- **Phase 2 Multi-Agent Extractor Consolidation & Unified `messages` Turn Sourcing (`services/coven/config.go`, `db_extraction.go`, `extractor.go`, `indexed_handler.go`, `locks.go`):**
  - In Phase 2, once `002_phase2_core_multiagent.sql` and the Section 8.4 cross-database backfill consolidate all agents' `messages` and `sessions` into the unified `aerial` database (preserving `messages.row_id` per `(agent, row_id)` so every `extraction_watermarks(agent, thread_id)` row backfilled by `spl2-c19-backfill` remains 100% continuous), replace `fetchTasks` (`GET <eri_url>/tasks`) in `services/coven/db_extraction.go` with direct SQL queries against `messages` and `sessions` scoped by `WHERE agent = $1 AND thread_id = $2`, keeping `eri_url` in `coven.yaml` solely for primary `NotifyShortFriction` (`POST http://eri.aerial/discord/message`, per `c03b-coven-http-and-vmrule`).
  - Update the single `coven` deployment (`role: "all"`) to consume `TranscriptsIndexedEvent` (`aerial.turn.transcripts_indexed`) across all configured agents, publish and consume `CovenExtractJobArgs{Agent, ThreadID}` on `aerial.coalesced.coven_extract.<agent>.<thread_id>` (validating `<agent>` from the NATS subject token), serialize per-thread extraction via `ThreadLocker` on `ScopedThreadKey(agent, threadID)` and `withFactWriteLock` (`services/coven/locks.go`), fetch trajectory context via `IndexerClient.GetTrajectoryContext(ctx, agent, threadID)` (`GET http://transcripts.aerial:4030/transcripts/threads/{id}/trajectory?agent=<agent>`), and run stateless LLM extraction against `eri_llm_url: http://eri.aerial/internal/llm`.
  - Retire duplicate per-sibling `coven-<agent>` deployments and `postgres-coven-<agent>-role` secrets.

### 9.2 `transcripts` (Phase 1 — Unified Multi-Agent Indexer & Idle Summarizer)
- **Single Deployment:** Retire duplicate per-sibling `transcripts-<agent>` deployments and `postgres-transcripts-<agent>-role` secrets in Phase 1.
- **NATS Step-Batch Ingestion, `TranscriptsIndexedEvent` Emission & Idle Summarizer (`services/transcripts/ingest.go`, `store.go`, `summarizer.go`):**
  - Following the `spl2` and Restate graceful-draining steady state (where `eri`'s legacy in-process transcript DB worker, `NewRoutedTranscriptStore`, and `RunStartupResend` are all deleted, and each `eri*-restate` pod publishes per-turn `TranscriptStepBatch` messages via `EnqueueTranscriptPublish` on `aerial.transcripts.steps.<agent>.<session_id>` while `eri*` proxies `/transcripts/*` and `transcript-search` to `services/transcripts`), no hostPath mounts are needed on the `transcripts` pod.
  - Update `Ingester.HandleBatchMessage` and `Store.ApplyBatch` in `services/transcripts` to accept and persist any configured `batch.Agent` (renaming `stack` -> `agent` in `session_summaries` and `transcript_steps`), deriving and validating `<agent>` directly from the broker-authenticated NATS subject token (`aerial.transcripts.steps.<agent>.<session_id>`) to prevent payload spoofing, and emitting `TranscriptsIndexedEvent{Agent: <agent>, SessionID, ThreadID, ...}` on `aerial.turn.transcripts_indexed`.
  - Update the in-process background idle `Summarizer` (`services/transcripts/summarizer.go`, with idle debounce and legacy-row skipping from `build-1795130868-977aef73`) to sweep unsettled sessions across all agents in `session_summaries` using the single `eri_llm_url: http://eri.aerial/internal/llm` endpoint and `senses.aerial` embedder with zero per-agent URL map.
  - Point all `eri*`, `eri*-restate`, and `coven*` pods (`transcripts_url` / `IndexerURL`) at `http://transcripts.aerial:4030`.
- **Authorization (`pkg/k8sauth`):**
  - Existing read endpoints (`GET /search`, `GET /steps`, `GET /stats`, `GET /watermarks`, `GET /transcripts/threads/{id}/trajectory`): Require `aerial-agent` (`resource: "transcripts", verb: "get"` or `"list"`). Support optional `?agent=<name>` filter (with backward-compatible `?stack=<name>` alias, defaulting to all agents).

### 9.3 `haro` (Phase 2 — Multi-Agent Restate Ingress Dispatch)
- **Single Deployment:** Retire duplicate per-sibling `haro-<agent>` deployments.
- **100% Restate Ingress Dispatch (Zero `eri_url` Config — `services/haro/internal/dispatch/dispatch.go`, `ticker.go`):**
  - Following `c03a-haro-restate-dispatcher-dark` and `c08-cfg-stacks-restatedeployment`, `haro`'s `HTTPPromptDispatcher` already dispatches one-shot/cron schedules and heartbeats/event watches to `restate_url: "http://restate.aerial:8080"`.
  - Update `DispatchPrompt` and `Dispatch` in `services/haro/internal/dispatch/dispatch.go` to resolve the target service name dynamically per schedule/trigger row (`row.Agent`) rather than a single static `cfg.AgentName`—POSTing one-shot/cron prompts to `<restateURL>/<ThreadSessionServiceName(row.Agent)>/<channel_id>/Enqueue` and heartbeats/event watches to `<restateURL>/<DispatchSessionServiceName(row.Agent)>/<session_key>/Enqueue` with deterministic `Idempotency-Key` headers, and deleting `eri_url` from `haro.yaml`.
  - NATS event watches (`AERIAL_EVENTS`) match subjects across `aerial.events.>` and route to the trigger's `agent`.
- **Authorization (`pkg/k8sauth`):**
  - `list_schedules`, `get_schedule`, `list_event_watches`: Allowed for all `aerial-agent` callers (`resource: "schedules", verb: "get"/"list"`). Supports optional `agent` filter.
  - `schedule_once`, `schedule_recurring`, `schedule_heartbeat`, `watch_event`: For agent callers (`IsService == false`), defaults `agent = caller_agent`. For service callers (`IsService == true`, e.g. `synapse` or `vanadis`), requires an explicit `agent` parameter in the payload (rejected with 400 Bad Request if missing, empty, or unconfigured, never defaulting to the service name). If an agent specifies a different `agent`, `pkg/k8sauth` checks `resource: "schedules/all", verb: "create"`.
  - `update_cron_schedule`, `update_heartbeat`, `cancel_schedule`, `cancel_event_watch`: Loads the target row's `agent` and calls `k8sauth.Authorize(ctx, caller, "schedules", verb, row.Agent)`.

### 9.4 `vanadis`, `vanadis-nats`, and `vanadis-restate` (`Build`, `BuildRouter`, `PRObject`, `RepoObject` — Phase 2)
- **Single Suite & Single Restate Service Set:** Retire per-sibling `vanadis-*-<agent>` deployments (and their pre-consolidation `<Service>-<agent>` Restate service registrations from `build-1795158435-e6cdf162`) plus any extra Restate namespaces/clusters, leaving one unified `vanadis` suite registering canonical `Build`, `BuildRouter`, `PRObject`, and `RepoObject` Virtual Objects on `http://restate.aerial:8080` (with URL-path-escaped `buildID` ingress paths and non-terminal `statusFailed` recoverable pause semantics per `build-1795155723-4fa121f1` and `build-1795158435-e6cdf162`).
- **Build Ownership & Multi-Agent `RestateEriClient` / `DiscordWidgetDeliverer` (Zero `eri_url` Config):**
  - `vanadis_builds` stores `agent TEXT NOT NULL DEFAULT 'aerial'`.
  - `POST /builds` (enqueue): For agent callers (`IsService == false`), extracts `caller_agent` via `pkg/k8sauth` and defaults `build.Agent = caller_agent`. For service callers (`IsService == true`, e.g. `synapse`), requires an explicit `agent` parameter in the payload (rejected with 400 Bad Request if missing, empty, or unconfigured, never defaulting to the service name). If a caller requests a different `agent`, verifies `resource: "builds/all", verb: "create"`.
  - `GET /builds`, `GET /builds/{id}`, `GET /builds/{id}/chunks/{chunk_id}` (and MCP `get_build`, `get_chunk`): Allowed for any `aerial-agent` (`resource: "builds", verb: "get"/"list"`).
  - Build/chunk mutations (`POST /builds/{id}/cancel`, `POST /builds/{id}/active-step`, `append_chunk`, `update_chunk`, `remove_chunk`, `fail_chunk`, `retry_action_chunk`, `update_build`, chunk completion callbacks): All external/agent HTTP and MCP requests (including live chunk tool-step updates from `eri-restate`'s `FlushChunkActiveStepToRestate` in `services/eri/pkg/queue/vanadis.go`, which in Phase 2 switches from direct Restate `:8080` to `POST http://vanadis.aerial:4025/builds/{id}/active-step` with loud error logging and retry on failure) enter through the `vanadis` API pod, which loads `build.Agent` and enforces `k8sauth.Authorize(ctx, caller, "builds", "update", build.Agent)` (`write-own` or `builds/all` supervisor) before invoking Restate (`Build/<id>/UpdateActiveStep/send`, `Mutate`, etc.) over the `restate-ingress-netpol`-isolated internal mesh. `vanadis-restate` also runs `ReconcileRestateServiceTimeouts` (`inactivity_timeout: 1h`, `abort_timeout: 1h`) against `RESTATE_ADMIN_URL` for all registered `ThreadSession*`, `DispatchSession*`, and `VoiceSession*` deployments so sibling `eri-<agent>-restate` pods can be removed from `restate-ingress-netpol`.
  - **`PRObject` & `RepoObject` Multi-Agent PR Lifecycle Authority (`services/vanadis-restate/internal/prstate/`):**
    - `PRObject` (`key: repo#prNumber`) and `RepoObject` (`key: repo`) serve as the single ordered Restate K/V authority for PR and deploy lifecycle state across all agents, with PostgreSQL `pr_registry` demoted to a read-only projection and SHA lookup index written exclusively by `PRObject`'s journaled `ProjectRegistry` step (`ProjectionUpsertQuery`).
    - `PREvent`, `PRState`, and `ProjectionSnapshot` carry `Agent string` (`json:"agent"`), which `ProjectionUpsertQuery` writes to `pr_registry.agent`. On first touch for a pre-existing open PR (including sibling PRs migrated during the Phase 2 cross-database backfill), lazy hydration restores `PRState.Agent` from the `pr_registry` projection.
    - Journaled external effects fired by `PRObject` (Discord alerts, collaborator alerts, conflict/CI/deploy wakes) route to the owning `PRState.Agent` via `RestateEriClient` (`ThreadSessionServiceName(agent)` / `DispatchSessionServiceName(agent)`) and the multi-bot `DiscordWidgetDeliverer`, while build-owned PR transitions signal `BuildRouter.RoutePREvent` (including on `statusFailed` paused builds per `build-1795139844-868ab3ee`).
  - **Multi-Agent `RestateEriClient` & Multi-Bot `DiscordWidgetDeliverer` (`services/vanadis-restate/internal/ericlient/restate_client.go`, `internal/widget/discord_sender.go`):**
    - Following `c02a`, `c02b`, `c07b`, `c09d`, and `c09e`, `vanadis-restate` already uses `RestateEriClient` and `DiscordWidgetDeliverer` (with exclusive `BuildObject.UpdateActiveStep` reusing `b.executor.SyncBuildWidget` and per-channel mutex `channelLocks sync.Map` in `DeliverStatusWidget`). Update `RestateEriClient` to take the target `agent` (`build.Agent` or `PRState.Agent`) per call—invoking `DispatchSessionServiceName(agent)/<session_key>/Enqueue`, `SessionManagerServiceName(agent)/CancelDispatch` (or `DispatchSessionServiceName(agent)/vanadis-<chunkID>/Cancel`), and `ThreadSessionServiceName(agent)/<thread_id>/Enqueue` on `http://restate.aerial:8080`—and update `DiscordWidgetDeliverer` to hold an `agent -> *discordgo.Session` map initialized from `DISCORD_TOKEN` / `DISCORD_TOKEN_<AGENT>` so build widgets and alerts are posted under the owning agent's Discord bot identity.

### 9.5 `synapse` (Phase 3 — Stateless Restate Edge Invoker & Multi-Bot Discord Gateway)
- **Single Multi-Bot Deployment:** Retire per-sibling `synapse-<agent>` deployments; delete `webhook_forward_urls` and `services/synapse/internal/router/webhook_forward.go`.
- **Multi-Session Discord Gateway Manager & Stateless `Deployment/eri*` `:8080` Ingress (`services/synapse/`):**
  - `synapse.yaml` defines a `bots` map keyed by `agent` (e.g., `aerial`, `<agent-a>`, `<agent-b>`), where each entry configures:
    - `token_env`: Environment variable holding that bot's Discord token (e.g., `DISCORD_TOKEN`, `DISCORD_TOKEN_<AGENT>`, synced via External Secrets Operator into `synapse`'s scoped Kubernetes `Secret`).
    - `eri_url`: Base URL of that agent's stateless HTTP ingress `Deployment/eri*` on `:8080` (e.g., `http://eri.aerial`, `http://eri-<agent>.aerial`).
    - `system_channel_id`: Discord channel ID for that agent's system alerts.
    - `default_owner`: Boolean marking the primary agent (`aerial: true`) that receives unassigned human/Dependabot PR webhooks and cluster-level Flux alerts.
  - `synapse` opens one `discordgo.Session` per configured entry in `bots` and forwards inbound Discord Gateway events (`/internal/discord/message` and `/internal/discord/interaction`) to that bot's stateless `Deployment/eri*` (`bots.<agent>.eri_url`), which performs stateless wake-mode triage (`mention`/`classifier`/`all`), delegates Tier-2 classifier and `/quota` calls to `ERI_RESTATE_URL:4028`, and invokes `ThreadSessionServiceName(agent)` and `SessionManagerServiceName(agent)` on Restate (`http://restate.aerial:8080`).
- **Stateless `PRObject` / `RepoObject` Edge Invoker & `pkg/k8sauth` on PR Registration (`services/synapse/internal/restateclient/`):**
  - Under the Restate PR lifecycle architecture (`build-1795158435-e6cdf162`), `synapse` holds zero in-memory PR/deploy state trackers and treats `pr_registry` strictly as a read-only projection and SHA lookup index; all state transitions and `pr_registry` projection writes happen through unified `PRObject` and `RepoObject` on Restate (`http://restate.aerial:8080`, using explicit context-carried idempotency keys via `restateclient.WithIdempotencyKey` and never synthesizing coarse payload-derived `Idempotency-Key` headers).
  - `POST /internal/pr/register` and `POST /internal/pr/retry-readiness`: Replaces the single-stack `bot_id` conflict gate (`build-1795132956-7d42e377`) with `pkg/k8sauth` multi-agent authentication. For agent callers (`IsService == false`), resolves `agent = caller_agent` (and verifies any client-supplied `bot_id` matches `caller_agent` unless the caller holds `prs/all`). For service callers (`IsService == true`), requires an explicit `agent` parameter (rejected with 400 Bad Request if missing or unknown, never defaulting to the service name).
  - External PRs (unassigned human or Dependabot PRs received via GitHub webhooks) are automatically assigned `agent` matching the entry configured with `default_owner: true` in `synapse.yaml` (e.g. `'aerial'`), guaranteeing that `agent` always maps to a valid active agent rather than an undefined token.
  - If `(repo, pr_number)` is already present in the `pr_registry` projection under `existing.agent`, overwriting registration or retrying readiness requires `k8sauth.Authorize(ctx, caller, "prs", "update", existing.Agent)` before forwarding `PREvent` (carrying `Agent`) to `PRObject` via `restateclient.Client.SendPREvent`, preventing a sibling agent from hijacking another agent's PR.
  - When GitHub or Flux webhooks arrive at `/webhooks/github` or `/webhooks/flux`, `synapse` resolves the commit SHA or PR against the read-only `pr_registry` index (or parses the `<!-- aerial-pr: {"agent": "..."} -->` / legacy `bot_id` trailer across all configured `bots`) and invokes the corresponding `PRObject` or `RepoObject` handler via `services/synapse/internal/restateclient` with `Agent` populated on `PREvent`—with zero HTTP webhook forwarding and zero foreign-bot drop on configured sibling agents.

### 9.6 `console` (Phase 4)
- Reads `messages`, `sessions`, `dispatches`, `vanadis_builds`, `one_shot_schedules`, `cron_schedules`, `heartbeat_schedules`, `schedule_runs`, and `facts` from the single `aerial` database.
- Adds an agent selector pill bar (`All Agents`, `aerial`, plus configured agents) in the top header and renders agent badges on turn cards, build rows, schedule tables, and memory facts.

### 9.7 Category 2 Telemetry & Singleton Alignment (`coverage-ingest`, `agentsview-sync`, `pkg/tracing`)
- **`coverage-ingest` (`services/coverage-ingest/config.yaml`):** Prune retired per-agent pull targets as their Category 3 services consolidate in Phases 2 and 3, add `http://transcripts:4030/debug/coverage/flush`, and retain per-agent `eri` (`http://eri:8080`, `http://eri-<agent>:8080`) and `eri-restate` (`http://eri-restate-health:4028`, `http://eri-<agent>-restate-health:4028`) flush targets.
- **`agentsview-sync` (`k8s/agentsview-session-sources.yaml`):** Continues running as a single Category 2 deployment indexing `/data`, configured agent stack mounts, and the shared `/var/lib/aerial/roborev-cache` RoboRev session cache (`build-1795147339-9773d51c`) with per-agent attribution.
- **OpenTelemetry `pkg/tracing` (`tracing.yaml` / `k8s/cluster-overrides.yaml`):** Standardize the `tracing.yaml` config key from `stack` to `agent` (emitting resource attribute `aerial.agent`, with backward-compatible fallback for `stack` / `aerial.stack`), and tag spans in multi-agent Category 2/3 services with the target `aerial.agent` attribute.

---

## 10. Four-Phase Rollout Strategy

To uphold In-Flight Control-Plane Continuity and zero-downtime migration, rollout proceeds in four sequential Vanadis builds:

1. **Phase 1 — Privilege Boundaries, `pkg/k8sauth`, Scoped NATS Users/ACLs, `coven` MCP RBAC, and `transcripts` Unification:**
   - Implement `pkg/k8sauth` (`TokenReview` + `SubjectAccessReview` + 45s cache + `ServiceAccountTransport`).
   - Update `eri-restate` loopback `TraceProxy` (on `:4028`) and CLI scripts (`aerial-build.sh`, `aerial-pr.sh`, `transcript-search`) to inject the pod's projected `ServiceAccount` Bearer token, and add `"NATS_URL"`, `"RESTATE_INGRESS_URL"`, and `"RESTATE_ADMIN_URL"` to `exactSecretKeys` in `FilterAgyBaseEnv` (`services/eri/pkg/runner/pure.go`) while passing NATS credentials separately via `NATS_USER` / `NATS_PASSWORD` (`nats.UserInfo`).
   - Configure per-agent NATS publisher users (`eri-aerial`, `eri-<agent>`) and subject-level publish/subscribe `permissions` inside the single `aerial` NATS account (`nats.conf`), and cut all pods over to the single `aerial` NATS JetStream account.
   - Deploy Kubernetes RBAC manifests (`ClusterRole/aerial-agent`, `ClusterRole/aerial-supervisor`, `ClusterRole/aerial-k8s-reader`, `ClusterRole/aerial-auth-delegator`, ServiceAccounts `eri-aerial` and sibling `eri-<agent>` bound to both `Deployment/eri*` and `RestateDeployment/eri*-restate`) and Phase 1 L4 `NetworkPolicy` hardening (`infisical-mcp`, `discord-mcp-*`, `restate-ingress-netpol`, `eri-*-restate-ingress-netpol` `:9080`/`:4028`, and removing cross-agent `eri*`/`eri*-restate`, `haro`, and `vanadis-restate` from `eri-*-ingress-netpol` `:8080` allowing `coven` and `transcripts` to reach primary `eri:8080`, and retaining `coven-<agent>` until Phase 2).
   - Enable `require_oauth = true` on `kubernetes-mcp` and remove `infisical-mcp` from sibling `config.yaml` files.
   - Ship Phase 1 schema migration (`001_phase1_transcripts_coven.sql`) in `services/eri/pkg/db/schema.sql` (renaming `stack` -> `agent` on `extraction_watermarks`, `session_summaries`, `transcript_steps`, and adding `source_agent` to `facts` and `agent` to `procedural_candidates`).
   - Remove `memory_forget` from `coven`, wire `pkg/k8sauth` provenance stamping (`source_agent`) on `memory_store` and `POST /admin/extract`, unify `transcripts` into a single multi-agent deployment with subject-token `<agent>` validation on `aerial.transcripts.steps.<agent>.>` and `TranscriptsIndexedEvent` emission, point all `eri*`, `eri*-restate`, and `coven*` pods at `http://transcripts.aerial:4030`, and retire sibling `transcripts-<agent>` deployments.
2. **Phase 2 — Unified Database & Category 3 Control Plane (`coven` Extractor Consolidation, `haro`, and `vanadis` Suite including `PRObject` / `RepoObject`):**
   - Ship Phase 2 declarative `schema.sql` and Atlas migration (`002_phase2_core_multiagent.sql`) adding `agent` columns and lookup indexes across dispatches, schedules (`one_shot_schedules`, `cron_schedules`, `heartbeat_schedules`, `schedule_runs`, `event_triggers`), `vanadis_builds`, and `pr_registry`, composite unique constraints on `(agent, row_id)` on `messages` and `dispatches`, and composite primary keys on `messages` and `sessions`.
   - Update `eri` and `eri-restate` store queries to scope `messages`, `sessions`, and `dispatches` by `cfg.Agent`, route `FlushChunkActiveStepToRestate` (`UpdateActiveStep` in `services/eri/pkg/queue/vanadis.go`) through `POST http://vanadis.aerial:4025/builds/{id}/active-step` (authenticated by `pkg/k8sauth`), and move `ReconcileRestateServiceTimeouts` into `vanadis-restate` (or stateless `Deployment/eri*`) so sibling `RestateDeployment/eri-<agent>-restate` worker pods are removed from `restate-ingress-netpol`.
   - Update `coven` (`services/coven/db_extraction.go`) to query completed admin turns directly from unified `messages` and `sessions` (`WHERE agent = $1 AND thread_id = $2`), consume `TranscriptsIndexedEvent` -> `aerial.coalesced.coven_extract.<agent>.<thread_id>` across all agents in the single `coven` deployment, and retire sibling `coven-<agent>` extractor deployments.
   - Update `haro` (`HTTPPromptDispatcher`) and `vanadis` / `vanadis-nats` / `vanadis-restate` (`RestateEriClient`, multi-bot `DiscordWidgetDeliverer`, and canonical `Build`, `BuildRouter`, `PRObject`, and `RepoObject` Virtual Objects carrying `Agent` in `PREvent`, `PRState`, and `ProjectionSnapshot`) for multi-agent Restate Virtual Object routing (`ThreadSessionServiceName(agent)`, `DispatchSessionServiceName(agent)`, and `SessionManagerServiceName(agent)` on `http://restate.aerial:8080`, removing `eri_url`) and `pkg/k8sauth` enforcement (`builds`, `schedules`), then remove `coven-<agent>` from `eri-*-ingress-netpol`.
   - Run the Section 8.4 cross-database data backfill (including target `BIGSERIAL` ID allocation for `facts`, `procedural_candidates`, and `pr_registry`) and deterministic `pr_registry` deduplication, execute sequence resynchronization (`setval`), and complete the pre-cutover reconciliation check across all enumerated tables before pointing sibling `eri-<agent>` and `eri-<agent>-restate` pods at the `aerial` database and retiring sibling `coven-<agent>`, `haro-<agent>`, `vanadis-*-<agent>`, `db-migrate-<agent>`, and extra Restate clusters.
3. **Phase 3 — Multi-Bot `synapse` Consolidation (Stateless Restate Invoker for `PRObject` & `RepoObject` + Multi-Bot Discord Gateway):**
   - Refactor `synapse` to manage multiple concurrent Discord Gateway sessions (`bots` map in `synapse.yaml`), forward inbound Discord messages and interactions to each bot's stateless `Deployment/eri*` `:8080` front door (`bots.<agent>.eri_url`), populate `Agent` on `PREvent` calls to unified `PRObject` and `RepoObject` through `services/synapse/internal/restateclient`, enforce multi-agent `pkg/k8sauth` on `/internal/pr/register` and `/internal/pr/retry-readiness` (replacing the single-bot `bot_id` conflict gate), and delete `webhook_forward_urls` / `webhook_forward.go`.
   - Cut over `synapse` with all bot tokens mounted via ESO and retire sibling `synapse-<agent>` deployments.
4. **Phase 4 — Multi-Agent `console` HUD:**
   - Add `?agent=` query filtering and `agent` fields across `console` API handlers and React UI components, keeping the Catppuccin Mocha / Polar Slate dual-theme PWA aesthetic.

---

## 11. What Is Left to the Builder

- Exact Go struct and interface signatures inside `pkg/k8sauth` (provided `Authenticate` and `Authorize` enforce the two-role `TokenReview` + `SubjectAccessReview` contract and 45s fail-closed cache).
- Internal concurrency layout of `synapse`'s multi-bot session loop (e.g., whether `DiscordManager` wraps a map of `agent -> *BotSession` with shared or per-bot workers).
- Exact SQL index naming and migration script chunking for the Phase 2 cross-database backfill, provided all table primary keys and foreign keys match `services/eri/pkg/db/schema.sql`.
