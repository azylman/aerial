# Service Layout Standard

**Date:** 2026-11-21  
**Task:** 165 (Ratified Service Layout Standard & Internal Layer Architecture)  
**Status:** Ratified monorepo architectural standard for service package layout and layer boundaries  
**Scope:** Go services under `services/` and `mcp/`, shared libraries under `pkg/`  

---

## 1. Purpose and Reading Guide

This document defines, per application archetype, the canonical Go project layout and structural architecture for this repository. A developer or reviewer should be able to place any new file or new service without guessing, and conformance reviews cite the ratified sections here.

The repository is a multi-module monorepo:

- Each deployable Go binary lives in its own module: `services/<name>/go.mod` with module path `github.com/azylman/aerial/services/<name>`, or `mcp/<name>/go.mod` for MCP servers.
- Shared libraries live in one additional module: `pkg/go.mod` with module path `github.com/azylman/aerial/pkg`, consumed across services through `replace` directives.
- Every Go service Dockerfile builds the package at the module root with `go build .` (for example `services/haro/Dockerfile` runs `go build -cover ... -o /app/haro .` after `WORKDIR /src/services/haro`) or targets a thin entrypoint in `cmd/<binary>/` where multi-binary or entrypoint separation is used.

All Go services follow a standardized decomposition: single-binary service roots target a strict **3-file root ceiling** (`main.go`, `main_test.go`, and `goleak_test.go`), while all domain logic, persistence, and protocol routing are decomposed into layered packages under `internal/` governed by the `L0 -> L3` layer hierarchy.

---

## 2. Architectural Precedents & Standards

Three architectural standards govern this specification:

### 2.1 Go module layout (`go.dev/doc/modules/layout`)

The official Go documentation defines layout patterns based on program complexity:

- **Basic command.** Single-file or flat-file tools for small utilities.
- **Server project.** For long-running servers and background services, the Go standard places private service code under `internal/`, isolated from external modules. The compiler strictly prevents cross-module imports of `internal/`, ensuring modular boundaries and clear ownership.
- **Multiple commands.** Multi-binary modules organize entrypoints under `cmd/<binary>/main.go` with shared internal packages beside them.

While flat `package main` modules were historically used for minimal binaries, monorepo scale, test maintainability, and domain isolation require formal package separation. Services decompose domain concerns into `internal/` subpackages to eliminate circular dependencies and isolate testing boundaries.

### 2.2 Restate Go SDK (`docs.restate.dev/develop/go/services`)

Restate Virtual Objects, workflows, and plain services require durable execution guarantees:

- A struct holds exported handlers taking `restate.ObjectContext` (exclusive, read-write), `restate.ObjectSharedContext` (shared, read-only), `restate.WorkflowContext`, or `restate.Context`.
- `restate.Reflect(MyObject{})` dynamically reflects exported methods into RPC handlers reachable at `<RESTATE_INGRESS_URL>/<StructName>/<HandlerName>`.
- Reflected services configure explicit lifecycle timeouts: `restate.WithInactivityTimeout(...)` and `restate.WithAbortTimeout(...)` to prevent zombie executions.
- `server.NewRestate().Bind(restate.Reflect(...)).Start(ctx, "0.0.0.0:9080")` binds and serves reflected handlers.
- Non-deterministic operations (PostgreSQL queries, HTTP calls, NATS events, Discord webhooks, wall-clock time, random generation, and UUIDs) must be wrapped inside journaled execution steps (`restate.Run(ctx, func(rc restate.RunContext) (T, error) { ... }, restate.WithName("<step>"))`).
- Restate services isolate models (`internal/model`), persistence (`internal/store`), journaled domain execution (`internal/build` / `internal/workflow`), and server binding (`internal/app`).

### 2.3 NATS and JetStream (`docs.nats.io`)

NATS JetStream consumers provide at-least-once message processing:

- Stream subscriptions, consumer definitions, and shared reconnection lifecycles live in `pkg/pubsub`.
- Consumers run as durable subscriptions on designated cluster streams (`AERIAL_EVENTS`, `AERIAL_JOBS`, `AERIAL_COALESCED`, `VISION_JOBS`).
- Handlers translate stream events into idempotent domain actions or dispatch calls to Restate ingress before issuing `msg.Ack()`.
- Services with NATS consumers encapsulate consumer loops in dedicated packages (such as `internal/bridge` or `internal/consumer`) rather than entangling them in root `main.go`.

---

## 3. Core Mapping Rule & The `L0 -> L3` Layer Architecture

### 3.1 The 3-File Root `package main` Ceiling

Every Go service module targets a strict **3-file root ceiling** for `package main`:

1. `main.go`: Thin application entrypoint. Responsible strictly for environment/flag parsing, configuration loading, signal handling, dependency wiring, and invoking the application runner or HTTP server. Contains zero business logic, zero raw SQL, and zero protocol routing.
2. `main_test.go`: Top-level integration and lifecycle tests verifying wiring, flag parsing, graceful shutdown, and end-to-end component instantiation.
3. `goleak_test.go`: Goroutine leak verification (`uber-go/goleak`) ensuring no lingering goroutines or leaked timers survive test execution.

All additional code must be organized into domain-specific packages under `internal/`. Legacy services retaining flat files in `package main` are pending phased decomposition passes under Task #165.

### 3.2 The `L0 -> L3` Layer Ratchet

Subpackages under `internal/` must strictly respect the hierarchical layer model. Dependencies flow downward only (`L3 -> L2 -> L1 -> L0`). Upward imports and circular package dependencies are architectural violations prohibited by this specification.

```
+-------------------------------------------------------------------------+
| L3: Application Assembly, Ingress & Protocols                           |
| internal/server, internal/router, internal/tools, internal/app          |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
| L2: Domain Logic, Engines, Workflows & Consumer Pipelines               |
| internal/dispatch, internal/delivery, internal/ticker, internal/bridge, |
| internal/effects, internal/workflow, internal/flux, internal/mediastream|
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
| L1: Infrastructure, Persistence & Cluster Adapters                      |
| internal/store, internal/db, internal/kube, internal/metrics            |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
| L0: Leaf Domain Models & Configuration Types                            |
| internal/config, internal/model                                         |
+-------------------------------------------------------------------------+
```

#### Layer Responsibilities & Contracts:

- **L0 (Foundational Leaves / Zero-Dependency Domain Types & Config):**
  - **Packages:** `internal/config`, `internal/model`.
  - **Responsibilities:** Define declarative configuration schemas, domain structs, entity models, request/response DTOs, and error types.
  - **Import Constraints:** May import shared libraries (`pkg/*`) and standard library packages. **MUST NOT** import any sibling `internal/*` packages (`L1`, `L2`, or `L3`).
- **L1 (Infrastructure, Persistence & Cluster Adapters):**
  - **Packages:** `internal/store`, `internal/db`, `internal/kube`, `internal/metrics`.
  - **Responsibilities:** Own database connections, SQL query execution, transaction lifecycles, Kubernetes API clients, and Prometheus metric registry declarations. Store interfaces define query contracts that higher layers consume. Handlers call store methods; store never calls handlers.
  - **Import Constraints:** May import `L0` (`internal/config`, `internal/model`) and `pkg/*`. **MUST NOT** import `L2` or `L3` packages.
- **L2 (Domain Logic, Engines, Workflows & Pipelines):**
  - **Packages:** `internal/dispatch`, `internal/delivery`, `internal/ticker`, `internal/bridge`, `internal/effects`, `internal/workflow`, `internal/flux`, `internal/mediastream`, `internal/executor`, `internal/build`.
  - **Responsibilities:** Core business logic, durable state machine transitions, cron and heartbeat evaluation loops, event dispatching, telephony stream processing, and NATS JetStream consumer worker loops.
  - **Import Constraints:** May import `L0` and `L1` packages. **MUST NOT** import `L3` packages.
- **L3 (Application Boundary, Ingress & Protocol Assembly):**
  - **Packages:** `internal/server`, `internal/router`, `internal/tools`, `internal/app`.
  - **Responsibilities:** HTTP router setup and middleware registration, MCP tool declarations and call dispatching, Restate `server.NewRestate().Bind(...)` service reflection, and top-level application runner orchestration (`RunApp`).
  - **Import Constraints:** May import `L0`, `L1`, `L2`, and `pkg/*`.

---

## 4. Archetype A — HTTP API and Server Services

### 4.1 Canonical Directory Layout

HTTP API services expose HTTP endpoints, Kubernetes probes, metrics, and MCP tools. They structure their packages as follows:

```
services/<name>/
├── Dockerfile
├── go.mod
├── go.sum
├── main.go                     # Entrypoint (<= 3 root files)
├── main_test.go                # Lifecycle & integration tests
├── goleak_test.go              # Goroutine leak checks
└── internal/
    ├── config/                 # L0: YAML & environment configuration
    │   ├── config.go
    │   └── config_test.go
    ├── model/                  # L0: Pure domain types, DTOs
    │   ├── model.go
    │   └── model_test.go
    ├── store/                  # L1: Persistence, SQL queries, transactions
    │   ├── store.go
    │   └── store_test.go
    ├── dispatch/               # L2 (Optional): Orchestration & dispatching
    │   ├── dispatcher.go
    │   └── dispatcher_test.go
    ├── ticker/                 # L2 (Optional): Scheduled evaluation loops
    │   ├── ticker.go
    │   └── ticker_test.go
    ├── server/                 # L3: HTTP mux, route handlers, middleware
    │   ├── routes.go
    │   ├── handlers.go
    │   └── server_test.go
    └── tools/                  # L3 (Optional): MCP tool schemas & dispatch
        ├── tools.go
        └── tools_test.go
```

### 4.2 Package Responsibilities

- **`internal/config` (L0):** Parses YAML configuration from `aerial-config` and overrides from environment variables. Exposes validated configuration structures.
- **`internal/model` (L0):** Pure data models. Free of side effects and I/O.
- **`internal/store` (L1):** Owns database connection pools, SQL queries, and transactional helpers. Exposes interfaces for mocking in unit tests.
- **`internal/server` or `internal/router` (L3):** Constructs HTTP muxes (`Routes()` or `NewServer()`), mounts domain endpoints, and binds health, metrics, and coverage endpoints (`pkg/httpkit.HandleCoverageFlush`).
- **`internal/tools` (L3):** Implements MCP tools, parameter JSON schemas, and execution handlers.
- **Root `main.go`:** Wires configuration, store, and server; starts the HTTP listener; and manages OS signal shutdown.

### 4.3 Current Examples

- `services/haro`: Scheduler service with scheduler ticker (`internal/ticker`), event dispatcher (`internal/dispatch`), persistence (`internal/store`), MCP tools (`internal/tools`), and HTTP server (`internal/server`). Conforms to 3-file root ceiling.
- `services/prospera`: Telephony webhook service with Twilio/Gemini media pipelines (`internal/mediastream`, `internal/twilio`, `internal/audio`, `internal/delivery`), database store (`internal/store`), tools (`internal/tools`), and HTTP server (`internal/server`). Conforms to 3-file root ceiling.
- `services/vanadis`: Build orchestrator and MCP server with database store (`internal/store`), Restate client proxy (`internal/restate`), MCP tool definitions (`internal/mcp`), and HTTP server (`internal/server`). Conforms to 3-file root ceiling.
- `services/synapse`: Event ingress gateway with Kubernetes watcher (`internal/kube`), GitHub webhook router (`internal/github`), Discord events (`internal/discord`), and Flux CD webhook dispatcher (`internal/flux`). Conforms to 3-file root ceiling.
- Grandfathered services (`services/coven`, `services/console`, `services/coverage-ingest`): Tracked in `.code-health-baseline.json` pending decomposition passes.

---

## 5. Archetype B — NATS Consumer and Bridge Services

### 5.1 Canonical Directory Layout

NATS consumer and bridge services consume events from JetStream streams and translate them into downstream actions or Restate invocations:

```
services/<name>/
├── Dockerfile
├── go.mod
├── go.sum
├── main.go                     # Entrypoint (<= 3 root files)
├── main_test.go                # Lifecycle tests
├── goleak_test.go              # Goroutine leak checks
└── internal/
    ├── config/                 # L0: Configuration loading
    │   ├── config.go
    │   └── config_test.go
    ├── bridge/                 # L2: JetStream consumer & forwarder
    │   ├── consumer.go
    │   └── bridge_test.go
    └── app/                    # L3: Runner, health mux & graceful shutdown
        ├── app.go
        └── app_test.go
```

### 5.2 Package Responsibilities

- **`internal/bridge` or `internal/consumer` (L2):**
  - Manages durable JetStream consumer subscriptions via `pkg/pubsub`.
  - Constants for stream names (`AERIAL_EVENTS`, `AERIAL_JOBS`, `AERIAL_COALESCED`, `VISION_JOBS`) and subject patterns.
  - Translates incoming NATS messages into Restate ingress invocations using `pkg/restateingress` (or domain store calls).
  - Enforces at-least-once delivery semantics: issues `msg.Ack()` strictly after the durable call succeeds.
  - Traps and logs poison messages or returns transient errors for NATS redelivery.
- **`internal/app` (L3):**
  - Owns process lifecycle (`RunApp`), consumer start/stop, and mounts `pkg/httpkit.SetupHealthMux` on the admin port for `GET /health`, `GET /healthz`, `GET /metrics`, and `/debug/coverage/flush`.
- **Root `main.go`:** Thin entrypoint delegating directly to `app.RunApp`.

### 5.3 Current Examples

- `services/vanadis-nats`: Standalone JetStream-to-Restate bridge subscribing to `AERIAL_EVENTS`, translating build trigger events into Restate ingress calls via `pkg/restateingress`, and acknowledging messages upon Restate journal persistence. Root package conforms to the 3-file ceiling (`main.go`, `main_test.go`).
- `services/synapse`: Embeds an internal consumer in `internal/buildmeta` subscribing to build completion events from `AERIAL_JOBS` alongside its HTTP router.

---

## 6. Archetype C — MCP Servers (Go)

### 6.1 Canonical Directory Layout

Go-based Model Context Protocol servers expose tool definitions and execution over HTTP or stdio:

```
mcp/<name>/
├── Dockerfile
├── go.mod
├── go.sum
├── main.go                     # Entrypoint (<= 3 root files)
├── main_test.go                # Tool execution tests
├── goleak_test.go              # Goroutine leak checks
└── internal/
    ├── config/                 # L0: Upstream client credentials
    │   └── config.go
    ├── model/                  # L0: Tool parameter & result types
    │   └── model.go
    ├── tools/                  # L3: MCP tool declarations & handlers
    │   ├── search.go
    │   └── tools_test.go
    └── server/                 # L3: Stream transport & health
        └── server.go
```

### 6.2 Package Responsibilities

- **`internal/config` (L0):** Loads configuration and upstream connection credentials.
- **`internal/model` (L0):** Tool parameter schemas and structured response types.
- **`internal/tools` (L3):** Implements tool registration and execution against upstream services (e.g. Vikunja, Chrome DevTools Protocol).
- **`internal/server` (L3):** Exposes MCP streaming endpoints via `pkg/httpkit.ServeMCPStream` and standard health endpoints.
- **Root `main.go`:** Wires configuration, client connections, and tool handlers into the MCP server transport.

### 6.3 Current Examples

- `mcp/docket-mcp`: Go MCP server providing Vikunja issue and task management tools.
- `mcp/kernel-mcp`: Go MCP server providing headless browser automation tools.
- `mcp/discord-mcp` and `mcp/infisical-mcp`: Upstream Node/TypeScript implementations (out of scope for Go layout).

---

## 7. Archetype D — Restate Virtual Object and Workflow Services

### 7.1 Canonical Directory Layout

Restate services implement durable Virtual Objects, workflows, and plain RPC services using `github.com/restatedev/sdk-go`:

```
services/<name>/
├── Dockerfile
├── go.mod
├── go.sum
├── cmd/
│   └── <name>/
│       └── main.go             # Thin entrypoint (< 100 lines)
└── internal/
    ├── model/                  # L0: Frozen state types & DTOs
    │   ├── state.go
    │   └── requests.go
    ├── store/                  # L1: PostgreSQL persistence & queries
    │   ├── store.go
    │   └── store_test.go
    ├── executor/               # L2: Dispatcher & client invocation
    │   ├── executor.go
    │   └── executor_test.go
    ├── build/                  # L2: Virtual Object transition engine
    │   ├── object.go           # Handlers: Create, Mutate, Reconcile...
    │   ├── journal.go          # restate.Run helpers
    │   └── metrics.go
    ├── router/                 # L3: Plain Restate service handlers
    │   ├── router.go
    │   └── router_test.go
    └── app/                    # L3: Server binding & health admin mux
        ├── app.go              # Bind(restate.Reflect(...)) with timeouts
        └── app_test.go
```

### 7.2 Package Responsibilities & Wire Contract

- **`cmd/<name>/main.go`:** Minimal entrypoint holding only configuration loading, tracing setup, signal handling, and invocation of `app.RunApp`.
- **`internal/model` (L0):** Defines persistent state types (`BuildObjectState`, `VanadisBuild`, `VanadisChunk`) and request/response DTOs.
  - **State Key Invariant:** Keys passed to `restate.Get` and `restate.Set` (`state`, `launched`) and JSON field tags are frozen wire contracts.
- **`internal/store` (L1):** Implements SQL database queries and persistence routines called by journaled steps.
- **`internal/build` / `internal/workflow` (L2):**
  - Implements the durable Virtual Object struct (e.g. `BuildObject`).
  - **Durable Step Isolation:** Non-deterministic operations (DB queries, HTTP requests, UUID generation, random selection, clock reads) MUST be executed within a named `restate.Run(ctx, func(rc restate.RunContext) (T, error) { ... }, restate.WithName("<step>"))` block.
  - **Error Taxonomy:** Distinguishes terminal errors (`restate.ToTerminalError`) from retryable transient errors.
- **`internal/app` (L3):**
  - Restate service reflection and registration via `server.NewRestate()`.
  - **Mandatory Timeout Configuration:** Service reflection MUST configure explicit service timeouts via `restate.WithInactivityTimeout(...)` and `restate.WithAbortTimeout(...)` on `restate.Reflect(...)`:
    ```go
    server.NewRestate().
        Bind(restate.Reflect(
            buildObject,
            restate.WithInactivityTimeout(inactivityTimeout),
            restate.WithAbortTimeout(abortTimeout),
        )).
        Bind(restate.Reflect(
            routerService,
            restate.WithInactivityTimeout(inactivityTimeout),
            restate.WithAbortTimeout(abortTimeout),
        ))
    ```
  - **Admin & Health Port Separation:** Restate protocol runs on `:9080`, while health, Prometheus metrics, and coverage flush endpoints run on an isolated admin port (e.g. `:4026`) via `pkg/httpkit.SetupHealthMux`.
- **Ingress Callers:** External services invoking Restate handlers use `pkg/restateingress` (or a client package such as `internal/restateclient`) providing deterministic `Idempotency-Key` headers matched to the logical operation identity.

### 7.3 Frozen Contract (Non-Negotiable)

The following aspects of deployed Restate services represent immutable wire contracts:

- Restate service names returned by `ServiceName()` (`Build`, `BuildRouter`, `EriRestate`) and struct names passed to `restate.Reflect`.
- Exported handler method names on Virtual Objects and workflows.
- Serialized state types and JSON tags stored in Restate state storage.
- Step names passed to `restate.WithName` inside `restate.Run` (changing step names breaks journal replay).
- Prometheus metric counter names and label dimensions.

### 7.4 Current Examples

- `services/vanadis-restate`: Durable build queue Virtual Object (`Build`) and router service (`BuildRouter`). Structured under `cmd/vanadis-restate/main.go` and `internal/{model,store,executor,build,router,app,prompt,prflow,ericlient,prstate,deploygate,cilog,widget}`. Reflects services with explicit inactivity and abort timeouts.
- `services/eri-restate`: Restate execution workflow service structured under `cmd/eri-restate/main.go` and `internal/app`. Reflects handlers with explicit timeouts.

---

## 8. Archetype E — Multi-Binary Modules (the `eri` Exception)

`services/eri/` is the sole multi-binary module in the monorepo and constitutes a deliberate architectural exemption from the 3-file root ceiling and `internal/` layout:

- `services/eri/main.go` plus flat concern files (`handlers.go`, `funnel.go`, `proxies.go`, `pure.go`, `jetstream.go`, `http_vocab.go`) form the primary `eri` execution engine binary in `package main`.
- `services/eri/cmd/hook-guard/main.go` and `services/eri/cmd/transcript-search/main.go` are auxiliary binaries sharing supporting packages.
- `services/eri/pkg/<concern>/` holds supporting packages (`ambient`, `classifier`, `config`, `db`, `delivery`, `discord`, `env`, `guard`, `interactions`, `mcp`, `metrics`, `notifier`, `queue`, `runner`, `session`, `transcript`).
- Multi-binary helper imports share `services/eri/pkg/*` without duplicating code across separate modules.

---

## 9. Shared Libraries — Repo-Root `pkg/`

The `pkg/` module (`module github.com/azylman/aerial/pkg`) contains libraries shared across multiple service modules:

- `pkg/httpkit`: HTTP middleware, JSON writing, MCP streaming, coverage flushing (`HandleCoverageFlush`), and admin health mux (`SetupHealthMux`).
- `pkg/pubsub`: NATS JetStream client wrappers, stream and consumer reconciliation, and in-memory message bus for tests.
- `pkg/restateingress`: Client utilities for issuing idempotent HTTP calls to the Restate ingress.
- `pkg/tracing`: OpenTelemetry distributed trace configuration.
- `pkg/pgutil`: PostgreSQL connection pooling and DSN parsing.
- `pkg/ollama`, `pkg/sanitizer`: Inference client wrappers and credential/token redaction sanitizers.

**Rules:**
- `pkg/` packages must never import any `services/` or `mcp/` packages (`services/* -> pkg/*` is strictly unidirectional).
- Code moves to `pkg/` only when needed by two or more distinct service modules.

---

## 10. Structural Standards & Package Decomposition Triggers

To prevent architectural degradation and ensure long-term codebase health, structural limits and targets are defined:

1. **Target 3-File Root `package main` Ceiling:**  
   Single-binary Go service module roots target at most 3 Go files: `main.go`, `main_test.go`, and `goleak_test.go`. All other domain logic and routing are factored into `internal/<layer>` subpackages as services undergo scheduled decomposition passes.
2. **File Length Ceiling (`maxFileLines = 1500`):**  
   Enforced mechanically by `scripts/code-health`: no Go source file may exceed 1,500 lines. Files approaching this limit must be factored by domain concern.
3. **Nesting Depth Ceiling (`maxDepth = 5`):**  
   Enforced mechanically by `scripts/code-health`: functions must not exceed a control flow nesting depth of 5. Deep branches must be factored into pure helper functions.
4. **AST Clone & Similarity Ceilings:**  
   Enforced mechanically by `scripts/code-health`: rejects copy-pasted functions or struct types sharing >= 80% similarity (`maxCloneSimilarity = 0.80`, `maxCloneEditSimilarity = 0.85`, `maxStructCloneSimilarity = 0.85`). Shared logic must be factored into pure functions or shared libraries.
5. **Layer Boundary Hierarchy (`L0 -> L3`):**  
   Target architecture standard: dependencies must flow strictly downward (`L3 -> L2 -> L1 -> L0`). Circular package dependencies and upward imports are prohibited.
6. **Phased Task #165 Decomposition:**  
   Existing services retaining legacy flat files remain operational and are migrated incrementally into `internal/` packages across dedicated decomposition chunks under Task #165.

---

## 11. Cross-Cutting Engineering Rules

- **Declarative Configuration:** Non-secret settings are defined in `aerial-config` YAML files (`services/<name>/<name>.yaml` or `config.yaml`) and loaded in `internal/config`. Ad-hoc `os.Getenv` calls for non-secret operational settings are prohibited. Environment variables are reserved for secrets, connection URLs, and container ports.
- **Hermetic Testing:** Table-driven unit tests (`tests := []struct{...}`) with declarative expectations. Tests must not use `time.Sleep`, raw signals, or unmocked live daemon ports. Leak detection via `goleak` runs on all packages.
- **Single-Pass Coverage:** Target 95% statement coverage. Missing branches are folded into canonical test tables rather than appended to separate catch-all test files.
- **Docker Build Isolation:** Multi-stage Docker builds copy `pkg/go.mod` and service `go.mod` for layer caching, followed by `COPY pkg/` and `COPY services/<name>/`, compiling with `-trimpath -ldflags="-s -w"`. Services instrumented with coverage include `-cover -covermode=atomic`.

---

## 12. Repository Service Inventory

The current inventory of services and MCP servers, categorized by archetype and internal package layout:

- **`services/eri-restate`** (Archetype D — Restate Service):
  - Root Entrypoint: `cmd/eri-restate/main.go`
  - Internal Packages: `internal/app` (L3)
  - Responsibilities: Restate execution workflow service reflecting handlers with explicit inactivity and abort timeouts.

- **`services/haro`** (Archetype A — HTTP API & Scheduler Service):
  - Root Entrypoint: `main.go`, `main_test.go`, `goleak_test.go` (conforms to 3-file ceiling)
  - Internal Packages: `internal/config` (L0), `internal/model` (L0), `internal/store` (L1), `internal/metrics` (L1), `internal/ticker` (L2), `internal/dispatch` (L2), `internal/tools` (L3), `internal/server` (L3), `internal/testutil`
  - Responsibilities: Persistent scheduling, cron/heartbeat evaluation, event watches, and MCP tools.

- **`services/prospera`** (Archetype A — HTTP API & Telephony Service):
  - Root Entrypoint: `main.go`, `main_test.go`, `goleak_test.go` (conforms to 3-file ceiling)
  - Internal Packages: `internal/config` (L0), `internal/model` (L0), `internal/store` (L1), `internal/delivery` (L2), `internal/gemini` (L2), `internal/mediastream` (L2), `internal/audio` (L2), `internal/twilio` (L2), `internal/tools` (L3), `internal/server` (L3), `internal/testutil`
  - Responsibilities: Autonomous PSTN voice call and SMS interaction service with Twilio and Gemini Live pipelines.

- **`services/synapse`** (Archetype A + B — HTTP API & NATS Consumer):
  - Root Entrypoint: `main.go`, `main_test.go` (conforms to 3-file ceiling)
  - Internal Packages: `internal/config` (L0), `internal/model` (L0), `internal/kube` (L1), `internal/alerts` (L1), `internal/flux` (L2), `internal/ericlient` (L2), `internal/discord` (L2), `internal/github` (L2), `internal/restateclient` (L2), `internal/buildmeta` (L2), `internal/metrics` (L2), `internal/router` (L3)
  - Responsibilities: Edge ingestion, Discord Gateway WebSocket, GitHub push webhooks, Flux deployment notifications, and NATS metadata consumer.

- **`services/vanadis`** (Archetype A + B — HTTP API & MCP Service):
  - Root Entrypoint: `main.go`, `main_test.go`, `goleak_test.go` (conforms to 3-file ceiling)
  - Internal Packages: `internal/config` (L0), `internal/model` (L0), `internal/store` (L1), `internal/metrics` (L1), `internal/restate` (L2), `internal/mcp` (L3), `internal/server` (L3)
  - Responsibilities: Multi-chunk build queue HTTP API and MCP server with Restate proxy client.

- **`services/vanadis-nats`** (Archetype B — NATS Consumer & Bridge Service):
  - Root Entrypoint: `main.go`, `main_test.go` (conforms to 3-file ceiling)
  - Internal Packages: `internal/config` (L0), `internal/bridge` (L2), `internal/app` (L3)
  - Responsibilities: High-performance NATS JetStream consumer bridging build events to Restate ingress.

- **`services/vanadis-restate`** (Archetype D — Restate Virtual Object & Workflow Service):
  - Root Entrypoint: `cmd/vanadis-restate/main.go`
  - Internal Packages: `internal/model` (L0), `internal/store` (L1), `internal/executor` (L2), `internal/build` (L2), `internal/prompt` (L2), `internal/prflow` (L2), `internal/ericlient` (L2), `internal/prstate` (L2), `internal/deploygate` (L2), `internal/cilog` (L2), `internal/widget` (L2), `internal/router` (L3), `internal/app` (L3)
  - Responsibilities: Durable build DAG orchestrator implementing `Build` Virtual Object and `BuildRouter` plain service.

- **`services/eri`** (Archetype E — Multi-Binary Module Exception):
  - Root Entrypoint: `main.go`, `cmd/hook-guard/main.go`, `cmd/transcript-search/main.go`
  - Supporting Packages: `pkg/<concern>` (16 internal packages: `ambient`, `classifier`, `config`, `db`, `delivery`, `discord`, `env`, `guard`, `interactions`, `mcp`, `metrics`, `notifier`, `queue`, `runner`, `session`, `transcript`)
  - Responsibilities: Primary Antigravity execution engine; explicitly exempt from the single-binary 3-file root ceiling due to its multi-binary architecture and concern-file HTTP funnel.

- **`services/coven`** (Archetype A — Flat Layout Pending Decomposition):
  - Status: Legacy flat layout pending decomposition under Task #165.
  - Responsibilities: ACT-R memory decay, semantic fact extraction, and procedural skill synthesis.

- **`services/console`** (Archetype A — Flat Layout Pending Decomposition):
  - Status: Legacy flat layout pending decomposition under Task #165.
  - Responsibilities: System status HUD and web dashboard with embedded React frontend assets (`ui/`).

- **`services/coverage-ingest`** (Archetype A — Flat Layout Pending Decomposition):
  - Status: Legacy flat layout pending decomposition under Task #165.
  - Responsibilities: Cluster-wide test coverage ingest collector.

- **`services/transcripts`** (Archetype A — Flat Layout Pending Decomposition):
  - Status: Legacy flat layout pending decomposition under Task #165.
  - Responsibilities: Audio and turn transcript ingestion, vector embedding, and semantic search.

- **`mcp/docket-mcp`** (Archetype C — Flat Layout Pending Decomposition):
  - Status: Legacy flat layout pending decomposition under Task #165.
  - Responsibilities: MCP server for Vikunja task management and issue tracking.

- **`mcp/kernel-mcp`** (Archetype C — Flat Layout Pending Decomposition):
  - Status: Legacy flat layout pending decomposition under Task #165.
  - Responsibilities: MCP server for headless browser automation and CDP interactions.

- **`mcp/discord-mcp`**, **`mcp/infisical-mcp`** (External MCP Servers):
  - Status: Upstream Node / TypeScript implementations (out of scope for Go layout).

- **`services/docs-service`**, **`services/homepage`**, **`services/grafana`**, **`services/migrator`**, **`services/github-runner`** (Non-Go Services):
  - Status: Containerized auxiliary services without Go module layouts (out of scope for Go layout).

---

## 13. Migration Decisions & Ratification

The architecture reflects key ratified engineering decisions:

- **Ratification of the 3-File Root Ceiling & `internal/` Decomposition:**  
  The prior practice of allowing unbounded flat `package main` files across single-binary services has been superseded. Migrated services adhere to the 3-file root ceiling (`main.go`, `main_test.go`, `goleak_test.go`) and structure code into `internal/<layer>` packages. This guarantees compiler-enforced privacy, prevents accidental cyclic dependencies, and simplifies unit testing.
- **Adoption of the `L0 -> L3` Layer Ratchet:**  
  Dependencies must flow strictly downward. Low-level models and configuration (`L0`) remain leaf packages. Infrastructure (`L1`) owns persistence and cluster clients. Domain logic and consumers (`L2`) own business workflows. Application assembly (`L3`) wires routes, tools, and Restate reflection.
- **Restate Timeout Ratification:**  
  Reflected services bind handlers with explicit timeouts (`restate.WithInactivityTimeout` and `restate.WithAbortTimeout`) to prevent orphaned workflows and ensure deterministic failure handling.
- **Preservation of the `eri` Exception:**  
  `services/eri/` remains the documented multi-binary exception utilizing `services/eri/pkg/*` and concern files to avoid mass import rewrites across its auxiliary binaries.
- **Phased Service Migration:**  
  Remaining flat services (`services/coven`, `services/console`, `services/coverage-ingest`, `services/transcripts`, `mcp/*`) remain fully operational and are scheduled for incremental decomposition under Task #165.

---

## 14. Conformance Checklist

A service is compliant with this standard when:

- [ ] Root package main conforms to the **3-file root ceiling** (`main.go`, `main_test.go`, `goleak_test.go`), is scheduled for decomposition under Task #165, or is an explicit exception (`services/eri`).
- [ ] Subpackages live under `internal/` and conform to the **`L0 -> L3` layer hierarchy** with strictly downward-flowing imports.
- [ ] No file exceeds 1,500 lines and no function exceeds a nesting depth of 5.
- [ ] Persistence logic lives in `internal/store` or `internal/db`; handlers invoke store methods, never the reverse.
- [ ] Restate Virtual Objects and services wrap non-deterministic I/O in named `restate.Run` blocks and declare explicit inactivity and abort timeouts on reflection.
- [ ] NATS consumers encapsulate subscription loops in `internal/bridge` or `internal/consumer` and issue `msg.Ack()` only after durable processing succeeds.
- [ ] Shared cross-service logic resides in `pkg/*`; `pkg/*` never imports service modules.
- [ ] Health (`/health`, `/healthz`), metrics (`/metrics`), and coverage (`/debug/coverage/flush`) endpoints are mounted via `pkg/httpkit`.
- [ ] All tests are table-driven, hermetic, use literal assertions, and pass `./scripts/verify.sh --staged`.

---

## 15. Non-Goals

- No disruption to active deployment pipelines or container build boundaries (`go build .` remains supported at module root or targeted `cmd/*` entrypoints).
- No modification of wire-frozen Restate handler names, state keys, or JSON field tags.
- No cross-service SQL querying; database access remains strictly owned by single services.
- No introduction of ad-hoc environment variables for non-secret operational configuration.

---

## Sources

- Go module layout: https://go.dev/doc/modules/layout
- Restate Go SDK: https://docs.restate.dev/develop/go/services
- NATS JetStream: https://docs.nats.io/learn/jetstream/
