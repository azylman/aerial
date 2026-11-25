---
name: go-engineering
description: Use when writing, reviewing, refactoring, testing, or debugging Go (.go) code, go.mod, Bazel go_library/go_test targets, PostgreSQL pgx/pgtest stores, or Restate SDK services across Aerial and sidecar repositories.
---

# Go Engineering, Testing & Durable Execution Standards

Synthesized from Aerial's core Go architecture invariants and [`samber/cc-skills-golang`](https://github.com/samber/cc-skills-golang) (`golang-code-style`, `golang-safety`, `golang-concurrency`, `golang-error-handling`, `golang-testing`, `golang-modernize`).

---

## 1. Canonical Go Service Package Layout (`L0 -> L3`)

- **Root `package main` Cap (`<= 3` Files)**:
  - Root `package main` in `services/<svc>` and `mcp/<srv>` is strictly capped at **3 files** (`main.go`, `main_test.go`, `goleak_test.go`) reserved for CLI flags, bootstrap, and dependency wiring only.
- **Strict Acyclic Layer Ordering (`L0 -> L3`)**:
  - Domain packages live under `internal/*` ordered strictly downward with zero upward or circular imports:
    - **`L0` (`internal/config`, `internal/model`)**: Pure domain types, request/response structs, and YAML configuration loaders. Zero sibling `internal/*` or `pgx` imports.
    - **`L1` (`internal/store`, outbound clients)**: PostgreSQL `pgxpool` repositories and external HTTP/NATS/Restate client adapters. Imports `L0` only.
    - **`L2` (`internal/mcp`, `internal/tools`, `internal/service`, `internal/ticker`)**: Business orchestration, MCP tool handlers, and background workers. Imports `L0` and `L1`.
    - **`L3` (`internal/server`, `internal/router`)**: HTTP/Streamable-HTTP transport routing, middleware, and lifecycle servers. Imports `L0`–`L2`.
- **Banned Grab-Bag Filenames**:
  - Horizontal grab-bag filenames (`pure.go`, `helpers.go`, `utils.go`, `common.go`, `types.go`, `frozen.go`, `*_part[0-9]*_test.go`) and catch-all test suffixes (`_extra_test.go`, `_more_test.go`, `_coverage_gaps_test.go`, `_additional_test.go`) are strictly prohibited by `scripts/code-health`. Name files after their concrete domain responsibility (`foo.go` paired with `foo_test.go`).

---

## 2. Code Style, Defensive Safety & Go 1.21–1.27 Idioms

- **Pure Constructor Injection & JIT Config**:
  - Pass explicit dependencies into constructors (`NewService(store, client, cfg)`).
  - Never cache mutable config scalars at startup; read dynamic snapshots JIT via `cfg.Current()`.
  - Never use `os.Getenv` for non-secret feature flags or operational settings (use declarative YAML in `aerial-config`).
- **Nil, Slice & Map Safety (`golang-safety`)**:
  - **Typed-Nil Interface Trap**: An interface holding `(*MyConcrete)(nil)` is **not** `== nil`. Always return an untyped `nil` explicitly when returning an interface type on disabled/absent paths.
  - **Explicit Slice & Map Initialization**: Initialize empty slices and maps returning over JSON APIs explicitly (`items := []Item{}`, `m := map[string]int{}`) so they serialize to `[]` and `{}` rather than `null`. Preallocate (`make([]T, 0, len(src))`) when capacity is known.
  - **Backing-Array Aliasing**: `append` mutates the shared backing array when capacity permits. When branching from a slice, force a fresh allocation with a full slice expression (`b := append(a[:len(a):len(a)], x)`) or `slices.Clone(a)`. Return `slices.Clone` / `maps.Clone` from exported state accessors.
  - **Numeric Narrowing**: Check bounds (`val > math.MaxInt32 || val < math.MinInt32`) before narrowing `int64`/`int` to `int32`/`uint16`.
  - **No `defer` in Loops**: `defer` executes at function exit, leaking descriptors across iterations. Extract the loop body into a helper function.
- **Control Flow & Function Hygiene (`golang-code-style`)**:
  - Handle errors and edge cases first with early returns; omit `else` blocks after `return`/`break`/`continue`.
  - Keep nesting depth `<= 5` (`scripts/code-health` ratchets AST depth).
  - Order parameters: `ctx context.Context` first, then domain inputs, then options/callbacks. Group `> 4` parameters into a typed request or options struct.
- **Modern Standard Library (`golang-modernize`, Go 1.21–1.27)**:
  - Use `any` over `interface{}`, `min`/`max` builtins, `for i := range n`, `slices` (`slices.SortFunc`, `slices.Contains`, `slices.Clone`), `maps` (`maps.Clone`, `maps.Keys`), `cmp.Or(val, fallback)`, `strings.Cut` / `strings.CutPrefix` / `strings.CutSuffix`, `sync.OnceValue` / `sync.OnceFunc`, `sync.WaitGroup.Go` (Go 1.25+), `math/rand/v2`, `os.Root` for untrusted relative paths, `t.Context()` in tests, and `b.Loop()` in benchmarks.

---

## 3. Error Handling & Structured Logging (`golang-error-handling`)

- **Never Discard Errors**: Every returned `error` must be checked (`scripts/code-health` enforces `blank-error-discard` and `golangci-lint` runs `errcheck`).
- **Wrap With Context**: Wrap internal errors with `fmt.Errorf("fetching build %s: %w", id, err)` (lowercase, no trailing punctuation). Use `%v` at external trust boundaries when internal cause chains should not be exposed.
- **Typed Inspection**: Use `errors.Is(err, ErrNotFound)` for sentinels, `errors.As(err, &target)` (or `errors.AsType[T](err)` in Go 1.26+) for custom types, and `errors.Join` for independent multi-errors. Never compare wrapped errors with `==` or bare type assertions.
- **Single-Handling Rule**: Either **log** an error OR **return** it—never both. Duplicate log-and-return pairs flood VictoriaLogs.
- **Low-Cardinality Structured Logging (`log/slog`)**: Keep the `slog` message string static and pass dynamic identifiers (`build_id`, `pr_number`, `err`) as structured key-value attributes.

---

## 4. Structured Concurrency & Lifecycle Safety (`golang-concurrency`)

- **Every Goroutine Has an Owner & Exit Path**:
  - Never spawn fire-and-forget goroutines without a cancellation context (`ctx.Done()`) and a wait barrier (`sync.WaitGroup`, `errgroup.WithContext`, or explicit `Close()` channel).
  - Every `select` in a long-running loop MUST include `case <-ctx.Done():`.
  - Never call `time.After` inside a loop (leaks timers until firing); allocate `timer := time.NewTimer(d)` once and `timer.Reset(d)`.
- **Channel & Lock Hygiene**:
  - Only the sender closes a channel. Declare directional channel parameters (`chan<- T`, `<-chan T`).
  - Keep `sync.Mutex` / `sync.RWMutex` critical sections short; never hold a lock across network, disk, or database I/O.
  - Use typed atomics (`atomic.Bool`, `atomic.Int64`, `atomic.Pointer[T]`) for lock-free flags and counters.
- **Goroutine Leak Detection**:
  - Include `goleak_test.go` (`goleak.VerifyTestMain(m)`) in service/package test suites to fail on leaked goroutines.

---

## 5. Hermetic Testing, `pkg/pgtest` & Homogeneous Table-Driven Tests

- **Homogeneous Table-Driven Tests (`<= 16` Fields, Zero Closure Fields)**:
  - Group test cases into `tests := []struct{...}` **only** when every row shares the exact same setup, call, and assertion shape.
  - Struct definitions inside test tables are hard-capped at **`<= 16` fields** (`st.Fields.NumFields()`).
  - **Banned Closure Fields**: Anonymous function fields (`run`, `check`, `extraCheck`, `setup`, `validate`, `validator`, `verify`, `lookup` `func(...)`) and one-hot mode-discriminator flags (`nilPool: true`, `switch tc.mode`) are blocked by `scripts/code-health`.
  - When a function has distinct lifecycle phases, fault modes, or setup shapes, split them into separate behavior-named `Test<Func>_<Behavior>` functions with compact tables and 1-line positional fixture constructors.
  - Never append catch-all `Test*Coverage*`, `Test*Extra*`, or `Test*Branches*` functions at EOF; fold missing branch cases into the canonical `Test<Func>_<Behavior>` table.
- **Hermetic Database Testing (`pkg/pgtest`)**:
  - Postgres-backed tests MUST use `pkg/pgtest`: `TestMain` runs `os.Exit(pgtest.Main(m))` (booting one throwaway PostgreSQL 19 + `pgvector` loaded from the real `schema.sql`), and each test opens an isolated cloned database via `pgtest.NewDSN(t)` with `t.Parallel()`.
  - Never use SQLite or fake SQL drivers for Postgres stores. Force driver failures hermetically by closing the pool handle, cancelling `ctx`, violating constraints, or sabotaging DDL on the test's own clone.
- **Zero Sleeps, Zero Live Subprocesses & Signal Safety**:
  - `time.Sleep` is banned in unit tests and production plumbing (`ruleTimeSleep`); use deterministic channels, condition variables, injected clock/delay hooks, or Go's `testing/synctest` (`synctest.Test`).
  - Never run live `agy` binaries (`runner.RunAgy`), live shell subprocesses, `http.DefaultClient` (`ruleHTTPDefaultClient`), or raw `syscall.Kill` (`ruleSyscallKill`) in unit tests. Use `t.TempDir()` (never write to `/data` or `/share`).
- **Verification & 95% Coverage Floor**:
  - Run `./scripts/verify.sh --staged` and targeted Bazel tests (`bazel test //<pkg>:all`).
  - Audit coverage gaps in a single pass via `bazel coverage --instrumentation_filter="^//<pkg>[:/]" //<pkg>:all` and inspect `coverage.dat` / `coverage-gate.awk` output to keep every package at `>= 95%` statement coverage.

---

## 6. Restate Durable Execution (`github.com/restatedev/sdk-go`)

When authoring or modifying Restate services, Virtual Objects, or workflows, load [`.agents/skills/discord/self-improvement/references/restate-workflows.md`](../self-improvement/references/restate-workflows.md) and enforce:
1. **`restate.Run` Isolation**: Wrap all non-deterministic operations and I/O (Postgres, HTTP, NATS, Discord, `time.Now()`, random/UUIDs) in `restate.Run(ctx, func(rc restate.RunContext) (T, error) { ... }, restate.WithName("<step>"))`. Pass strictly the inner `rc` to downstream calls and NEVER invoke outer `restate.Context` methods (`Get`, `Set`, `ObjectSend`, `Sleep`, or nested `Run`) inside `restate.Run`.
2. **Replay Determinism (`RT0016` Prevention)**: Never use raw goroutines, channels, `select`, or unsorted Go `map` iteration to emit Restate steps outside `restate.Run` (always `slices.Sort(keys)` first; use `restate.Wait` / `restate.WaitFirst`).
3. **Terminal vs. Retryable Errors**: Map 4xx/validation/not-found/conflict/fencing errors and deterministic Postgres SQLSTATEs (classes `22`, `23`, `42`, `0A`, `2B`, `2D`, `25`) to `restate.ToTerminalError(err, restate.WithErrorCode(...))`. Reserve `409` strictly for Restate cancellation; use `412` for conflicts, stale generation fencing, and interrupted `RunAsync` closures. Never set step retry caps (`WithMaxRetryAttempts`).
4. **Non-Blocking Exclusive Locks**: Never hold an exclusive `restate.ObjectContext` lock across sleeps or polling loops; return promptly and schedule delayed self-sends via `restate.ObjectSend(...).Send(req, restate.WithDelay(d))`.
5. **Deterministic `Idempotency-Key`**: Always send a 1:1 logical `Idempotency-Key` (including tick `dueAt` or `attemptOrdinal` when `SkipIfActive` can memoize early returns; stripping wall-clock publish timestamps), declare `restate.WithInactivityTimeout` / `restate.WithAbortTimeout` on reflected services, and use `context.WithoutCancel(ctx)` for post-commit finalization queries.
