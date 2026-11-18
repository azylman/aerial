# Service Layout Standard

**Date:** 2026-11-17  
**Task:** 165 (Plan 1 — Layout Spec Only)  
**Status:** Proposed standard for per-service conformance plans  
**Scope:** Go services under `services/` and `mcp/`, shared libraries under `pkg/`  

---

## 1. Purpose and Reading Guide

This document defines, per application archetype, the community-standard Go project layout and how that standard maps onto this repository. A reviewer should be able to place any new file or any new service without guessing, and later conformance plans should be able to cite a section here instead of re-arguing layout from scratch.

This is a specification only. It does not rename files, move code, or change behavior. Migration, where recommended, happens in separate small plans (shared health helper, Restate extraction, and one conformance pass per service).

The repository is a multi-module monorepo:

- Each deployable Go binary lives in its own module: `services/<name>/go.mod` with module path `github.com/azylman/aerial/services/<name>`, or `mcp/<name>/go.mod` for MCP servers.
- Shared libraries live in one additional module: `pkg/go.mod` with module path `github.com/azylman/aerial/pkg`, consumed through `replace` directives.
- Every Go service Dockerfile builds the package at the module root with `go build .` (for example `services/haro/Dockerfile` runs `go build -cover ... -o /app/haro .` after `WORKDIR /src/services/haro`).

Any layout that breaks that `go build .` contract, the Docker `COPY pkg/` plus `COPY services/<name>/` layering, or the `GOCOVERDIR` coverage flush paths must justify itself explicitly. None of the archetypes below require such a break.

---

## 2. What the Community Standards Actually Say

Three sources govern this standard. They were read directly for this spec.

### 2.1 Go module layout (`go.dev/doc/modules/layout`)

The Go documentation does not mandate a single layout. It describes a progression:

- **Basic command.** The simplest program is a single file defining `func main`. Larger programs split code across multiple files that all declare `package main` in the module root. This flat `package main` shape is fully official, not a shortcut.
- **Command or package with supporting packages.** When code benefits from supporting packages, place them under `internal/`. The compiler enforces that only code inside the parent of `internal/` may import them, so internal APIs can change freely.
- **Multiple commands.** Put each program in its own directory. The common convention is a `cmd/` directory holding `cmd/<binary>/main.go` for each binary.
- **Server project.** Servers are usually self-contained binaries with no packages to export. The recommendation is to keep server logic in `internal/` and keep all commands together in `cmd/`, as in `cmd/api-server/main.go` plus `internal/auth/`, `internal/model/`, and similar packages.

The key judgment is therefore not "flat versus textbook." It is: *does this module contain one binary, several binaries, or importable packages?* The recommended layout follows from that answer.

### 2.2 Restate Go SDK (`docs.restate.dev/develop/go/services`)

Restate defines no directory standard. The documented shape is a single `package main` program:

- A struct holds exported handlers. For a Virtual Object, handlers take `restate.ObjectContext` (exclusive, read-write) or `restate.ObjectSharedContext` (shared, read-only). For a Workflow, a `Run` handler takes `restate.WorkflowContext` and must exist exactly once per workflow.
- `restate.Reflect(MyObject{})` turns exported methods into handlers; unexported methods are skipped. The service is reachable as `<RESTATE_INGRESS_URL>/<StructName>/<HandlerName>`.
- `server.NewRestate().Bind(restate.Reflect(...)).Start(ctx, "0.0.0.0:9080")` serves the bound services.

Two consequences matter for layout. First, the struct name and handler names are wire-visible and therefore frozen once deployed. Second, nothing in Restate requires `cmd/` or `internal/`; splitting handlers, state types, and journal helpers across files in the same `package main` is the idiomatic reading of the examples.

### 2.3 NATS and JetStream (`docs.nats.io`)

NATS publishes no NATS-specific directory standard. Official JetStream examples are ordinary Go programs: the consumer or handler lives in the same package as `func main`, durable consumer and stream setup happens through the JetStream API, and an HTTP health endpoint, when present, is an ordinary `net/http` server alongside. A NATS service therefore inherits the Go guidance for a basic command (Section 2.1) with no extra archetype rules.

---

## 3. Core Mapping Rule

Because every service here is its own module, the community layouts map as follows.

- A **single-binary module** maps to the Go "basic command" layout: flat `package main` at the module root (`services/<name>/main.go` plus sibling files). This is the community standard for this shape, not a deviation from it.
- The Go **"server project"** layout (`cmd/<binary>/main.go` thin entrypoint plus `internal/...`) is the community standard for a module that contains several binaries or importable packages. It is adopted only where that condition actually holds (Section 8, the `eri` exception, and Section 7.2, the `vanadis-restate` user-approved exception). Applying it to a single-binary module adds a directory level, changes every Dockerfile `WORKDIR` and `go build` target, and moves coverage package paths, for no compiler or clarity gain.
- **`internal/`** is adopted only when a package must be hidden from other modules, plus the single user-approved `vanadis-restate` exception (Section 7.2): `internal/model` (leaf shared types, imports no sibling `internal/` package), `internal/store` (owns `VanadisStore`, `PostgresVanadisStore`, and all its methods), `internal/executor` (owns `Executor` and `EriClient`, depends only on model/store contracts), `internal/build` (owns `BuildObject`, the transition engine, journal helpers, and metrics), `internal/router` (owns `BuildRouterService`, depends on the model/store/build-invoker contract without creating store or build cycles), and `internal/app` (owns `Config`, `LoadConfigFromEnv`, `RunApp` wiring, and the `loadConfigFn`/`runAppFn`/`exitFn`/`initTracingFn` seams), with `cmd/vanadis-restate/main.go` as a thin `package main` entrypoint holding only config load, tracing/signal setup, and app invocation. Inside any other single-binary `package main` module there is no cross-module importer to hide from, and code in `package main` cannot be imported at all, so `internal/` provides no additional privacy. Shared code that genuinely crosses services belongs in the repo-root `pkg/` module instead (Section 9).
- **`pkg/` at the repo root** (`pkg/httpkit`, `pkg/pubsub`, `pkg/tracing`, `pkg/pgutil`, `pkg/ollama`, `pkg/sanitizer`) is the deliberate exception to `internal/`: these packages are intended for import by many service modules, so they must stay exportable.

Later conformance plans must state which archetype they cite (Sections 4–8) and must not introduce `cmd/` or `internal/` inside a single-binary service without recording why the mapping rule above does not apply. The sole recorded exception is `vanadis-restate` (Section 7.2), approved by user supersession.

---

## 4. Archetype A — HTTP API and Server Services

### 4.1 Community standard

A self-contained HTTP server with no importable packages. Per Section 2.1 ("server project"), the general recommendation is `cmd/` plus `internal/` when the module also carries multiple binaries or non-Go assets. For a single-binary module, the basic-command flat layout is the standard form.

### 4.2 Our mapping (adopted)

Flat `package main` at `services/<name>/`:

- `main.go` — `func main`, config load, dependency wiring, server start, graceful shutdown. Keep it thin: wiring only, no business logic.
- `config.go` — `Config` struct, `LoadConfig` and `LoadConfigFromEnv`, YAML file schema when the service reads `services/<name>.yaml` from `aerial-config`.
- `server.go` — server struct (for example `RouterServer` in `services/synapse/`), `Routes()` or `SetupMux()` returning `http.Handler`, handler registration.
- One file per domain concern, named for the concern: `handlers.go`, `db.go`, `store.go`, `tools.go`, `metrics.go`, `pure.go`.
- `pure.go` — pure, deterministic helpers with no I/O, extracted so they can be tested with fast table-driven unit tests. Follow the local convention: same name for the test file (`pure_test.go`).
- Tests beside the code they cover: `<concern>_test.go` in the same package, plus `goleak_test.go` at the module root for goroutine-leak detection and `main_test.go` for wiring-level coverage.
- `Dockerfile` — multi-stage build that copies `pkg/` and `services/<name>/`, then `go build -cover -covermode=atomic -trimpath -ldflags="-s -w" -o /app/<name> .`

### 4.3 File placement rules

- New HTTP handlers go in `server.go` or a concern file (`handlers.go`, `<domain>.go`), never in `main.go`.
- New persistence goes in `db.go` or `store.go`. Raw SQL lives in the store file; handlers call store methods, never the reverse.
- New pure logic (parsing, classification, ranking, formatting) goes in `pure.go` or a concern file as a pure function first; wire it from the server file second.
- New configuration fields go in `config.go` and in the matching `aerial-config` YAML in a separate downstream change. Do not read operational settings from ad-hoc environment variables; environment variables are for secrets and for `PORT`, `CONFIG_PATH`, and connection URLs already established in `config.go`.
- Health and observability: every HTTP service exposes `GET /health` and `GET /healthz` for the Dockerfile `HEALTHCHECK` and Kubernetes probes, `GET /metrics` via `promhttp` where metrics exist, and `GET /debug/coverage/flush` via `pkg/httpkit.HandleCoverageFlush` when built with `-cover`. Mount these on the same mux as the domain routes unless the service needs a separate admin port (Section 7).

### 4.4 Current examples

- `services/haro/` — `main.go`, `config.go`, `server.go`, `db.go`, `ticker.go`, `tools.go`, `metrics.go`, `pure.go`. Archetype A with a scheduler loop.
- `services/coven/` — `main.go`, `config.go`, `server.go`, `db.go`, `extractor.go`, `scheduler.go`, `tools.go`, `metrics.go`. Archetype A.
- `services/synapse/` — `main.go`, `config.go`, `db.go`, `discord.go`, `github.go`, `flux.go`. Archetype A plus Archetype B consumers (Section 5).
- `services/vanadis/` — `main.go`, `chunks.go`, `dag.go`, `mcp.go`, `metrics.go`, `restate_proxy.go`. Archetype A plus a Restate client proxy; it is not itself a Restate service.
- `services/prospera/` — `main.go`, `config.go`, `server.go`, `store.go`, `tools.go`, `twilio.go`, `gemini_live.go`. Archetype A (voice/telephony webhooks).
- `services/console/` — `main.go`, `config.go`, `handlers.go`, `deployments.go`, `github.go`, `k8s.go`, plus `ui/` (React) and `static/`. Archetype A with a frontend asset pipeline; the `ui/` build in `services/console/Dockerfile` is part of this archetype, not a separate service.
- `services/coverage-ingest/` — `main.go`, `server.go`, `ingest.go`. Archetype A (coverage collector; deliberately not built with `-cover`).

---

## 5. Archetype B — NATS Consumer and Bridge Services

### 5.1 Community standard

No NATS-specific layout exists (Section 2.3). A NATS consumer is a basic Go command whose handlers run from JetStream subscriptions managed through `pkg/pubsub`.

### 5.2 Our mapping (adopted)

Flat `package main` at the module root, extending Archetype A:

- Consumer setup and subscription lifecycle live in the service's own files (`main.go` for a bridge, a concern file such as `build_metadata_consumer.go` in `services/synapse/` for an embedded consumer).
- Subject names, stream names (`AERIAL_EVENTS`, `AERIAL_JOBS`, `AERIAL_COALESCED`, `VISION_JOBS`), and durable consumer names are constants in the consumer file, never string literals scattered across handlers.
- Shared JetStream plumbing (stream reconciliation, consumer helpers, message types) lives in `pkg/pubsub` (`pkg/pubsub/pubsub.go`, `pkg/pubsub/memory.go`). Service files hold only service-specific subjects and handlers.
- At-least-once semantics: handlers must be idempotent or deduplicated, must ack only after durable work completes, and must surface poison messages through the existing error and metrics path rather than ack-and-drop.
- When a NATS service also serves HTTP (health, metrics, coverage flush), the HTTP surface follows Section 4.3 on a dedicated admin mux and port. A pure consumer with no HTTP surface still needs a health story: either the admin mux or an explicit note in the service README section of the conformance plan.

### 5.3 Current examples

- `services/vanadis-nats/` — single-file bridge (`main.go`, 415 lines) from NATS JetStream to the Restate ingress. Archetype B plus Archetype A admin surface (`SetupHealthMux`).
- `services/synapse/` — Archetype A service embedding JetStream consumers (`build_metadata_consumer.go`) through `pkg/pubsub`. Consumers stay in concern files inside the host service; do not split them into a new module.
- `services/eri/` — embeds JetStream publishing and consuming in `jetstream.go` through `pkg/pubsub`. Archetype E host (Section 8) with Archetype B concerns in concern files.
- `services/vanadis/` — Archetype A service consuming build events through `pkg/pubsub`. Same rule: consumer concern files inside the host module.

Standalone NATS bridges (one module whose only job is bridging) keep the whole bridge in `main.go` plus `main_test.go` until an extraction trigger fires (Section 10). Embedded consumers never get their own `go.mod`.

---

## 6. Archetype C — MCP Servers (Go)

### 6.1 Community standard

An MCP server is an HTTP (streamable) or stdio server. Layout follows the Go basic-command guidance; the MCP protocol adds handler-registration conventions, not directories.

### 6.2 Our mapping (adopted)

Flat `package main` at `mcp/<name>/`:

- `main.go` — MCP server construction, tool registration, transport start (`pkg/httpkit.ServeMCPStream` where the shared stream helper applies).
- One file per tool domain or upstream client: `search.go` and `vikunja.go` in `mcp/docket-mcp/`, `cdp.go` in `mcp/kernel-mcp/`.
- `pure.go` plus `pure_test.go` for pure helpers, `goleak_test.go` at the root, following the same test convention as Archetype A.
- `Dockerfile` — same multi-stage shape as Archetype A, building `mcp/<name>` with `go build .`

### 6.3 Current examples

- `mcp/docket-mcp/` — `main.go`, `pure.go`, `search.go`, `vikunja.go`. Archetype C.
- `mcp/kernel-mcp/` — `main.go`, `cdp.go`, `pure.go`. Archetype C.
- `mcp/discord-mcp/` and `mcp/infisical-mcp/` — upstream Node/TypeScript servers with Docker packaging only. Out of scope for Go layout; classified here so the inventory is complete.

---

## 7. Archetype D — Restate Virtual Object and Workflow Services

### 7.1 Community standard

Per Section 2.2: exported handlers on a struct, bound with `restate.Reflect`, served with `server.NewRestate()`. No directory standard. Struct and handler names are the public wire contract.

### 7.2 Our mapping (adopted)

`services/vanadis-restate/` follows the standard Go server-project layout by user supersession (the sole Section 3 exception besides `eri`); every other Restate service, if one is ever added, defaults to flat `package main` unless a future amendment says otherwise. The handler and state names below are frozen (Section 7.3), so this is a pure code move with no wire change:

- `cmd/vanadis-restate/main.go` — thin `package main` entrypoint holding only config load, tracing/signal setup, and app invocation. No business logic, no handlers, no state types.
- `internal/model` — leaf shared-types package importing no sibling `internal/` package: `BuildObjectState`, `VanadisBuild`, `VanadisChunk`, and every `*Request`/`*Response` type keep their exact names and JSON field tags. State keys (`state`, `launched`) and the strings passed to `restate.Set`/`restate.Get` are frozen.
- `internal/store` — owns `VanadisStore`, `PostgresVanadisStore`, and all its methods, including those today in `router.go` (moved verbatim with their tests). Raw SQL lives here; handlers call store methods, never the reverse.
- `internal/executor` — owns `Executor` and `EriClient`, depending only on model/store contracts.
- `internal/build` — owns the `BuildObject` Virtual Object (`ServiceName()` returns `Build`) with handlers `Create`, `UpdateBuild`, `Cancel`, `AppendChunks`, `UpdateChunk`, `RemoveChunk`, `FailChunk`, `AttachPR`, `Reconcile`, `Mutate`, `OnPREvent`, `OnDispatchOutcome`, `RetryActionChunk`, and `DeploymentDeadline`; plus the transition engine, journal helpers (`runJournaled`, `restate.Run` wrappers, `toTerminalErr`), and metrics. Every `restate.WithName` journal/step name is frozen. Never call non-deterministic APIs (wall clock, randomness, direct network or disk I/O) outside a journaled `restate.Run` step. Tests move with the production code into `package build` so unexported fields and helpers remain testable; `BuildObject.Create` keeps its exact signature and journal names.
- `internal/router` — owns the companion `BuildRouterService` (`ServiceName()` returns `BuildRouter`) with plain-service handlers `RoutePREvent`, `RouteDispatchOutcome`, `RouteAttachPR`, and `RouteFailChunk` taking `restate.Context`. Depends on the model/store/build-invoker contract without creating store or build cycles. Tests move with the production code into `package router`.
- `internal/app` — owns `Config`, `LoadConfigFromEnv`, and `RunApp` wiring (Restate server construction via `server.NewRestate().Bind(restate.Reflect(...))`, admin HTTP server start, shutdown), plus the `loadConfigFn`/`runAppFn`/`exitFn`/`initTracingFn` seams. Both services are bound in `RunApp`.
- Admin surface on a separate port: the health mux serves `GET /health`, `GET /healthz`, `GET /metrics` (callers pass `promhttp.Handler()`), and `GET` plus `POST /debug/coverage/flush` on the admin port (`4026` for `services/vanadis-restate/`, health-checked by its Dockerfile), while the Restate SDK serves handlers on `9080`. Decision A2 is adopted and frozen: `pkg/httpkit.SetupHealthMux(service string, metrics http.Handler)` is an error-free mux builder, `pkg/go.mod` gains no Prometheus import, and both `vanadis-nats` and `vanadis-restate` get identical health/metrics/coverage-flush behavior through it.
- Error taxonomy in `internal/build`: terminal versus retryable decisions go through `toTerminalErr` and the `recordReconcileError` metrics path (`reconcileErrorsTotal`, `clientErrorsTotal`). Handler labels (`labelCreate`, `labelUpdateBuild`, and the other `label*` constants) and the `vanadis_restate_*` counter names with the single `prometheus.Register` in `init` are part of the metrics contract and are frozen with the handler names.

### 7.3 Frozen contract (non-negotiable)

The following are deployed wire and state contracts. No conformance or extraction plan may rename them:

- Restate service names returned by `ServiceName()` (`Build` for `BuildObject`, `BuildRouter` for `BuildRouterService`) and the struct names seen by `restate.Reflect`.
- Every exported handler name: on `BuildObject`, `Create`, `UpdateBuild`, `Cancel`, `AppendChunks`, `UpdateChunk`, `RemoveChunk`, `FailChunk`, `AttachPR`, `Reconcile`, `Mutate`, `OnPREvent`, `OnDispatchOutcome`, `RetryActionChunk`, and `DeploymentDeadline`; on `BuildRouterService`, `RoutePREvent`, `RouteDispatchOutcome`, `RouteAttachPR`, and `RouteFailChunk`.
- Every state type and field (`BuildObjectState`, `VanadisBuild`, `VanadisChunk`) and every request/response type name and JSON tag.
- Metrics handler labels and the `vanadis_restate_*` counter names.

Extraction under Plan 3 is a pure code move: same names, same signatures, same journal/step strings, verified by the existing handler and journal tests moving with their production code into the same `internal/` package (so unexported fields and helpers remain testable) and passing with only package-path updates. Never duplicate unexported helpers; export through `model` or a named `internal/` helper only where a real cross-package need exists.

### 7.4 Current examples

- `services/vanadis-restate/` — the only Restate deployment today, binding two services (`Build` and `BuildRouter`). `main.go` is 1,470 lines and holds config, state types, most `BuildObject` handlers, journal helpers, the admin mux, and `main`. It is the one service that currently violates the "wiring only" rule for `main.go`, which is why it is being refactored into the Section 7.2 `cmd/` plus `internal/` layout (user supersession).
- `services/vanadis/` consumes Restate through `restate_proxy.go` as a client. Client proxies follow Archetype A placement (a concern file in the client service), not this archetype.

---

## 8. Archetype E — Multi-Binary Modules (the `eri` Exception)

### 8.1 Community standard

Per Section 2.1, a module with several programs keeps each program in its own directory, conventionally `cmd/<binary>/main.go`, with shared packages beside them. This is the "packages and commands in the same repository" and "server project" guidance.

### 8.2 Our mapping (adopted, single exception)

`services/eri/` is the sole multi-binary module and the sole exception to the flat standard:

- `services/eri/main.go` plus flat concern files (`handlers.go`, `funnel.go`, `proxies.go`, `pure.go`, `jetstream.go`, `http_vocab.go`) form the main `eri` binary in `package main`.
- `services/eri/cmd/hook-guard/main.go` and `services/eri/cmd/transcript-search/main.go` are auxiliary binaries. They follow the community `cmd/` convention exactly and must stay thin.
- `services/eri/pkg/<concern>/` holds the main binary's supporting packages (`ambient`, `classifier`, `config`, `db`, `delivery`, `discord`, `env`, `guard`, `interactions`, `mcp`, `metrics`, `notifier`, `queue`, `runner`, `session`, `transcript`). New supporting code for `eri` goes in a new or existing `pkg/<concern>` package, never in a new top-level directory and never in `internal/`.

Why `pkg/` and not `internal/` here: `cmd/hook-guard` and `cmd/transcript-search` import `services/eri/pkg/guard`, `pkg/config`, `pkg/metrics`, `pkg/session`, `pkg/db`, and `pkg/transcript` (see `services/eri/Dockerfile`, which builds the helpers from those packages before copying the full tree). All importers live inside the same module, so `internal/` would also work, but renaming `pkg/` to `internal/` would rewrite every import path, the Dockerfile's staged `COPY` lines, and coverage package paths across sixteen packages for a privacy property the module boundary already provides. The conformance decision is to keep `pkg/` and document it as a deliberate, frozen deviation. No other service may copy this shape without first becoming a genuine multi-binary module and recording the decision in an amendment to this spec.

### 8.3 Placement rules

- New `eri` domain logic goes in `services/eri/pkg/<concern>/`. The flat `package main` files are for HTTP wiring, Discord event handling, and startup only.
- New auxiliary binaries are not created casually. Prefer a flag or subcommand on an existing binary; a new `cmd/<name>/` requires the same justification as a new service.
- Database schema and migrations stay in `services/eri/pkg/db/` (`schema.sql`, `migrations/`). Declarative schema changes follow the Atlas migration rules; no imperative DDL in Go code.

---

## 9. Shared Libraries — Repo-Root `pkg/`

The `pkg/` module (`module github.com/azylman/aerial/pkg`) holds code shared by two or more service modules:

- `pkg/httpkit` — HTTP helpers: `HandleCoverageFlush`, `WriteJSON`, `WriteResponse`, `ServeMCPStream`, config-path resolution, and the reloadable-config handler (`NewReloadable`, `LoopbackOnly`).
- `pkg/pubsub` — JetStream bus abstraction, stream/consumer reconciliation, and the in-memory bus for tests.
- `pkg/tracing` — OpenTelemetry tracing setup.
- `pkg/pgutil` — PostgreSQL DSN and pool helpers.
- `pkg/ollama`, `pkg/sanitizer` — inference client helpers and log/secret sanitizers.

Rules:

- A helper moves to `pkg/` only when a second service needs it (see the extraction triggers in Section 10). The proven current case is the duplicated `SetupHealthMux` in `services/vanadis-nats/main.go` and `services/vanadis-restate/main.go` (same health, metrics, and coverage-flush mounting, differing only in the service label), which Plan 2 moves to `pkg/httpkit`.
- `pkg/` packages must not import any `services/` package. Dependency direction is always service → `pkg/`.
- `pkg/` APIs are consumed by many modules; changes there require running the tests of every consuming service, not just `pkg/`'s own tests.

---

## 10. Extraction Triggers — When Flat Files Must Split

Flat `package main` is the standard, but it has limits. Extract code into a new file in the same package (or, for `eri`, into `pkg/<concern>`) when any one of these three concrete triggers fires. `vanadis-restate` is exempt from this paragraph: by user supersession it extracts into the Section 7.2 `cmd/` plus `internal/` packages instead of same-package concern files:

1. **Size trigger.** A single file exceeds roughly 500 lines, or `main.go` exceeds roughly 400 lines of wiring plus logic. `services/vanadis-restate/main.go` at 1,470 lines is the standing example (now resolved via the Section 7.2 `internal/` split, not a same-package split); `services/eri/main.go` at 1,221 lines is mitigated by the `pkg/` split in Section 8.
2. **Reuse trigger.** The same logic appears in two files in one service, or in two services. The duplicated `SetupHealthMux` across `vanadis-nats` and `vanadis-restate` is the standing example and is handled by Plan 2 via decision A2 (`pkg/httpkit.SetupHealthMux`).
3. **Testability trigger.** Logic cannot be unit-tested without starting a server, a NATS connection, a Restate runtime, or a database. Extract the pure part into `pure.go` (or a concern file) and test it with a table-driven test; keep the I/O shell in the original file. In `vanadis-restate`, tests move with their production code into the same `internal/` package so unexported fields and helpers remain testable.

Extraction means a new file in the same package first. A new package (or a move to `pkg/`) requires trigger 2 across services, or, inside `eri`, a new concern boundary — except in `vanadis-restate`, where the Section 7.2 `internal/` packages are the approved target. Extraction never renames exported Restate handlers, state types, metrics labels, HTTP routes, or NATS subjects (Section 7.3).

Test conventions that apply at extraction time: one unified table (`tests := []struct{...}`) with one `for _, tc := range tests` loop per function under test, hand-derived literal expectations, mocks only at external boundaries (NATS, Restate runtime, database, HTTP upstreams), `t.Parallel()` where the package supports it, no `time.Sleep` synchronization, and a `goleak_test.go` at the module root.

---

## 11. Cross-Cutting Rules

- **Configuration.** Declarative YAML in `aerial-config` (`config.yaml`, `services/<name>/<name>.yaml`), loaded in `config.go` with environment overrides limited to secrets, `PORT`, `CONFIG_PATH`, and connection URLs. Workers read config through `cfg.Current()` snapshots where a reloadable config exists (`pkg/httpkit.NewReloadable`); do not cache config scalars at startup.
- **Health, metrics, coverage.** `GET /health` and `GET /healthz` return success only when the process can serve. `GET /metrics` uses `promhttp`. Coverage builds (`go build -cover`) mount `pkg/httpkit.HandleCoverageFlush` at `/debug/coverage/flush`; the handler writes counters with `coverage.WriteMetaDir` and `coverage.WriteCountersDir` so counters can be collected without restarting.
- **Docker.** Every Go service keeps the two-stage shape: copy `pkg/go.mod` and the service `go.mod` first for module caching, then `COPY pkg/` and the service tree, then `go build .` from the service directory. Layout changes that alter `COPY` paths must update the Dockerfile in the same change.
- **Naming.** Files are lowercase, named for their concern (`build_metadata_consumer.go`, not `consumer2.go`). Test files mirror source files. Package names are short lowercase words with no underscores.
- **Non-Go services.** `services/docs-service/` (nginx plus Docsify assets), `services/homepage/` (Node config preparation), `services/grafana/` (dashboards and provisioning), `services/migrator/` (Atlas image carrying `services/eri/pkg/db/migrations/`), and `services/github-runner/` (CI runner image) have no Go layout. They are listed in Section 12 for inventory completeness and are out of scope for conformance plans.

---

## 12. Current Service Classification

Every current deployable, classified by archetype. Conformance plans cite this section.

- `services/console/` — Archetype A (HTTP) with frontend assets (`ui/`, `static/`).
- `services/coven/` — Archetype A (HTTP).
- `services/coverage-ingest/` — Archetype A (HTTP, coverage collector).
- `services/docs-service/` — Non-Go (nginx/Docsify). Out of scope.
- `services/eri/` — Archetype E (multi-binary exception: main binary plus `cmd/hook-guard`, `cmd/transcript-search`, and `pkg/<concern>` packages), embedding Archetype B concerns.
- `services/github-runner/` — Non-Go (CI runner image). Out of scope.
- `services/grafana/` — Non-Go (dashboards/provisioning). Out of scope.
- `services/haro/` — Archetype A (HTTP) with scheduler loop.
- `services/homepage/` — Non-Go (Node). Out of scope.
- `services/migrator/` — Non-Go (Atlas; carries `services/eri/pkg/db/migrations/`). Out of scope.
- `services/prospera/` — Archetype A (HTTP, telephony webhooks).
- `services/synapse/` — Archetype A (HTTP) embedding Archetype B (JetStream consumers).
- `services/vanadis/` — Archetype A (HTTP) embedding Archetype B, plus Restate client proxy.
- `services/vanadis-nats/` — Archetype B (NATS bridge) with Archetype A admin mux.
- `services/vanadis-restate/` — Archetype D (Restate Virtual Object). Frozen contract per Section 7.3.
- `mcp/docket-mcp/` — Archetype C (MCP, Go).
- `mcp/kernel-mcp/` — Archetype C (MCP, Go).
- `mcp/discord-mcp/`, `mcp/infisical-mcp/` — Non-Go (upstream Node). Out of scope.
- `pkg/` (`httpkit`, `pubsub`, `tracing`, `pgutil`, `ollama`, `sanitizer`) — Shared libraries per Section 9.

---

## 13. Migration Decisions and Costs

Decisions recorded here are the ones later plans execute. Each states the community standard, the adopted mapping, and the cost of the alternative.

- **Single-binary HTTP, NATS-bridge, MCP, and Restate services: keep flat `package main`.** This adopts the Go basic-command standard (Section 2.1). The alternative — `cmd/<name>/main.go` plus `internal/` inside every `services/<name>/` module — would rewrite all Go Dockerfiles (`WORKDIR`, `COPY`, and `go build` target), shift coverage package paths, and touch every service's tests, for a privacy property that `package main` already provides. Cost of adopting the alternative: high, repo-wide. Benefit: none identified. Decision: do not adopt — except for the single `vanadis-restate` supersession below. Other single-binary services keep their existing layout unless a future amendment says otherwise.
- **`eri`: keep `services/eri/pkg/`, do not move to `internal/`.** The community server-project layout would name these packages `internal/`. Renaming would rewrite imports across sixteen packages, the staged `COPY` lines in `services/eri/Dockerfile`, and coverage paths, while both shapes give identical import restrictions inside one module. Cost: high, single-service. Benefit: cosmetic alignment only. Decision: keep `pkg/`, documented as frozen in Section 8. Revisit only if `eri` is ever split into separately versioned modules.
- **`vanadis-restate`: adopt the standard Go server-project layout (user supersession).** `cmd/vanadis-restate/main.go` becomes the thin `package main` entrypoint; production code moves into `internal/model`, `internal/store`, `internal/executor`, `internal/build`, `internal/router`, and `internal/app` per Section 7.2, with tests moving alongside their production code. Honest cost: a flat concern-file split inside `package main` (the Plan 3 shape this replaces) would be lower risk — fewer import rewrites, no package-cycle hazard, no Dockerfile `WORKDIR`/`go build` target change. What the `internal/` packages buy is compiler-enforced boundaries: `model` stays a leaf, `store`/`executor`/`build`/`router` dependencies flow one way, and future handlers cannot quietly reach across concerns. The frozen Restate contract (Section 7.3) and decision A2 (`pkg/httpkit.SetupHealthMux(service string, metrics http.Handler)`, error-free, no Prometheus import in `pkg/go.mod`) are unchanged by this move. Decision: adopt; this is the only approved `cmd/` plus `internal/` migration for a single-binary service.
- **Shared health mux: adopt `pkg/httpkit` helper (Plan 2).** The `SetupHealthMux` duplication between `vanadis-nats` and `vanadis-restate` is the reuse trigger (Section 10). Cost: low (one helper, two call sites passing `promhttp.Handler()` with identical `GET /health`, `GET /healthz`, `GET /metrics`, `GET`/`POST /debug/coverage/flush` behavior). Decision: adopt in Plan 2; other services keep their existing muxes unless the same duplication is proven.

No other layout migrations are approved by this spec. A future proposal that contradicts a decision here must amend this section with its cost analysis.

---

## 14. Conformance Checklist (for Later Plans)

A per-service conformance plan is complete when it can answer yes to each item, citing the section above:

- The service's archetype is named (Sections 4–8) and matches Section 12.
- `main.go` (or, for `vanadis-restate`, `cmd/vanadis-restate/main.go`) contains wiring only, or an extraction trigger from Section 10 is open and scheduled.
- Every new file follows the placement rules for its archetype; no business logic was added to the thin entrypoint.
- No `cmd/` or `internal/` directory was introduced inside a single-binary module (Sections 3, 13) — except the approved `vanadis-restate` Section 7.2 layout.
- Health, metrics, and coverage-flush mounts match Section 11 and the service Dockerfile `HEALTHCHECK`.
- Shared code used by two or more services lives in `pkg/`; `pkg/` does not import `services/` (Section 9).
- For Restate work: no handler, state, label, or metric name changed (Section 7.3), and journal/idempotency tests pass with their production code in the same `internal/` package (package-path updates only).
- `git diff` for the plan touches only the files the plan named, and `./scripts/verify.sh --staged` passes in the scratch clone before submit.

Most services are expected to close their conformance pass as a no-op (already flat, already wired correctly). A no-op close still records the archetype and the checklist answers in the plan so the audit trail exists.

---

## 15. Non-Goals

- No file renames, package moves, or code changes in this plan. This document is the only deliverable.
- No renaming of Restate handlers, state types, metrics labels, HTTP routes, or NATS subjects anywhere in the follow-up plans.
- No new services, no module splits, and no changes to `aerial-config` as part of the layout work. Config changes, if a conformance plan needs one, ship as a separate downstream change after the upstream code merges.
- No adoption of `cmd/` plus `internal/` for single-binary services (Section 13) — except the user-approved `vanadis-restate` migration into `cmd/vanadis-restate/main.go` plus `internal/model`, `internal/store`, `internal/executor`, `internal/build`, `internal/router`, and `internal/app` (Sections 7.2, 13), with the frozen Restate contract and decision A2 unchanged.

---

## Sources

- Go module layout: https://go.dev/doc/modules/layout
- Restate Go services: https://docs.restate.dev/develop/go/services
- NATS JetStream: https://docs.nats.io/learn/jetstream/
