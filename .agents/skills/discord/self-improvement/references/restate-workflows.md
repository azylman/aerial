# Restate Durable Workflows & Virtual Objects Runbook (`github.com/restatedev/sdk-go`)

This runbook establishes authoritative engineering guidelines, concurrency models, replay determinism rules, and error-handling patterns for authoring and maintaining Restate services, Virtual Objects, and durable workflows (`github.com/restatedev/sdk-go` v1.x) across `aerial` (such as `services/vanadis-restate` and `services/eri-restate`).

---

## 1. Service Type Decision Framework & Keying Strategy

Restate provides three core execution primitives. Choose the correct primitive based on statefulness, concurrency, and keying requirements:

| Component Type | Statefulness & Keying | Concurrency Model | Primary Use Cases |
| :--- | :--- | :--- | :--- |
| **`Service`** | Stateless, unkeyed | Fully concurrent invocations across handlers | Stateless API routers, event gateways, fan-out dispatchers (e.g. `BuildRouterService`, `SessionManagerService`). |
| **`Virtual Object`** | Keyed K/V state per object key | **Single-writer exclusive lock** per key by default (`ObjectContext`); **concurrent shared** access via `ObjectSharedContext` | State machines, entity lifecycles, session workers (e.g. `BuildObject` keyed by `build_id`, `ThreadSessionObject` keyed by `thread_id`). |
| **`Workflow`** | Keyed exactly-once run-to-completion workflow | Single `Run` execution with external signal and query handlers (`WorkflowContext`, `WorkflowSharedContext`) | Multi-step human-in-the-loop workflows, linear orchestrations with explicit completion promises. |

### Keying Strategy
- Virtual Objects and Workflows are partitioned by key: `restate.Key(ctx)`.
- Use natural, cluster-unique, deterministic identifiers (e.g. `build_id`, `thread_id`, or `session_key`).
- **Never use compound keys with unescaped delimiters**: If combining scopes, use a structured delimiter (e.g. `agent:thread_id`) and parse with strict validation to prevent cross-tenant key collision.

---

## 2. Journal Replay Determinism & `restate.Run` Context Isolation

### 2.1 The Replay Execution Model
Restate achieves fault tolerance by capturing execution progress into an append-only journal. When an invocation suspends (e.g. during an RPC call, sleep, or after a crash), Restate re-executes the handler from the beginning, matching executed steps against previous journal entries.

**Replay code outside `restate.Run` MUST be 100% deterministic.** If the replay path diverges from the journal (e.g. different step names, different step order, or missing steps), Restate raises a fatal journal mismatch error (`RT0016: Journal mismatch`) and aborts the invocation.

### 2.2 `restate.Run` Context Isolation & Shadowing Invariant
All non-deterministic operations and external side effects (database queries, HTTP requests, NATS publishing, Discord API calls, clock lookups, random generators, filesystem access) **must execute inside `restate.Run`**:

```go
res, err := restate.Run(ctx, func(rc restate.RunContext) (MyResult, error) {
    // rc is a restate.RunContext — an isolated context.Context for side effects.
    return client.DoExternalWork(rc, params)
}, restate.WithName("descriptive-step-name"))
```

#### Strict RunContext Invariants:
1. **Pass strictly the inner `rc` (`restate.RunContext`)**: Inside `restate.Run`, pass `rc` down to all internal function calls, database operations, and HTTP clients.
2. **NEVER call outer `restate.Context` operations inside `restate.Run`**:
   - Calling `restate.Get`, `restate.Set`, `restate.Clear`, `restate.ObjectSend`, `restate.Sleep`, or nesting another `restate.Run` inside a `restate.Run` callback is strictly prohibited and causes runtime panics or journal corruption.
   - Read or write Restate K/V state *before* or *after* `restate.Run`, never inside it.
3. **Always name steps with `restate.WithName("<name>")`**:
   - Explicit step names create readable traces and stabilize journal replay across minor code refactors.
   - Step names must remain stable across code versions.

### 2.3 Journaled Clocks, UUIDs, and Randomness
- **Never call `time.Now()` directly in replayable code**:
  - In replayable handler code outside `restate.Run`, calling `time.Now()` returns a different timestamp on each replay, corrupting logic.
  - To record a timestamp, run `time.Now().UTC()` inside a named `restate.Run` step, or use deterministic logical timestamps derived from the trigger event.
- **Never use unjournaled random or UUID generators**:
  - Use `restate.UUID(ctx)` or `restate.Rand(ctx)` for Restate-managed deterministic randomness.
  - For deterministic idempotency keys derived from invocation identity, use deterministic hashing:
    ```go
    id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(ctx.Request().ID + "/" + stepName))
    ```

### 2.4 Go Map Iteration Order Invariant
Go map iteration order is randomized by design.
- **NEVER iterate over a Go map to emit Restate journal steps** (such as `restate.Run`, `restate.Call`, `restate.ObjectSend`, or `restate.Set`).
- If iterating over a map, **always sort map keys first**:
  ```go
  keys := make([]string, 0, len(items))
  for k := range items {
      keys = append(keys, k)
  }
  slices.Sort(keys)
  for _, k := range keys {
      // Deterministic step execution order guaranteed
  }
  ```

### 2.5 Ban on Goroutines, Channels, and `select` in Replayable Handlers
- **Never spawn raw goroutines (`go func()`) or use Go channels and `select` to coordinate Restate operations outside `restate.Run`.**
- Restate cannot journal goroutine interleaving or channel multiplexing.
- To wait for multiple asynchronous operations, use `restate.Wait(ctx, ...)` or `restate.WaitFirst(ctx, ...)` on Restate `Selectable` objects (futures/promises).

### 2.6 Pointer vs. Value in `restate.Get`
- `restate.Get[*T](ctx, key)`: Returns a pointer `*T` (nil if the key is unset without unmarshaling an empty struct). Preferred for optional state models.
- `restate.Get[T](ctx, key)`: Returns a value `T` (zero value if unset).
- Use `restate.Void{}` as the request or response type for handlers that take or return empty payloads.

---

## 3. Terminal vs. Retriable Error Classification

Restate classifies errors into two categories:

### 3.1 Retriable (Transient) Errors
By default, any standard Go `error` returned by a handler or inside a `restate.Run` step is treated as **retriable**. Restate suspends the invocation and automatically retries the failed step with exponential backoff.
- Transient errors include PostgreSQL connection pool exhaustions, network blips, 502/503/504 HTTP responses, and external rate limits.
- Preserve these errors as standard Go errors so Restate guarantees eventual completion.

### 3.2 Terminal (Non-Retriable) Errors
Failures that will never succeed on retry must be wrapped in `restate.ToTerminalError`:
- Client validation failures (400 Bad Request, invalid JSON, missing required fields).
- Resource not found (404 Not Found).
- Unrecoverable state conflicts or stale generation fencing (409 Conflict).
- Precondition violations.

```go
func ToTerminalErr(err error) error {
    if err == nil {
        return nil
    }
    switch {
    case errors.Is(err, store.ErrInvalidBuildInput):
        return restate.ToTerminalError(err, restate.WithErrorCode(400))
    case errors.Is(err, store.ErrBuildNotFound), errors.Is(err, ErrChunkNotFound):
        return restate.ToTerminalError(err, restate.WithErrorCode(404))
    case errors.Is(err, store.ErrBuildConflict):
        return restate.ToTerminalError(err, restate.WithErrorCode(409))
    default:
        return err // retried automatically by Restate
    }
}
```

### 3.3 Terminal Errors Inside `restate.Run`
When `restate.Run` returns a `TerminalError`, Restate writes the terminal error to the journal and halts execution of that step immediately without retrying:
```go
out, err := restate.Run(ctx, func(rc restate.RunContext) (MyData, error) {
    data, err := fetch(rc)
    if errors.Is(err, ErrPermanentRejection) {
        return MyData{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
    }
    return data, err
}, restate.WithName("fetch-data"))
```

### 3.4 Separate Metrics for Client vs Reconcile Errors
Always track 4xx terminal client errors separately from 5xx retriable system errors in Prometheus:
```go
func RecordError(handler string, err error) {
    if err == nil {
        return
    }
    if isClientError(err) {
        clientErrorsTotal.WithLabelValues(handler).Inc()
        return
    }
    reconcileErrorsTotal.WithLabelValues(handler).Inc()
}
```

---

## 4. Exclusive Lock Hygiene, Deadlock Prevention & Shared vs. Exclusive Split

### 4.1 Exclusive Lock Invariant (`ObjectContext`)
A handler taking `restate.ObjectContext` holds an **exclusive lock** on that object key for the entire duration of the invocation. No other invocation targeting that same key can execute until the handler completes or suspends.

**CRITICAL RULE: NEVER hold an exclusive lock across sleeps or backoff loops.**
- Never call `time.Sleep(...)` inside a handler.
- Never call `restate.Sleep(ctx, ...)` while holding an exclusive lock if other handlers (such as `Cancel`, `AttachPR`, `UpdateActiveStep`, or metadata amendments) need to access the object.
- Sleeping inside an exclusive handler blocks all concurrent operations against that entity, causing request timeouts, deadlocks, and hung status widgets.

### 4.2 Non-Blocking Reconcile via Delayed Self-Sends (`ObjectSend` + `WithDelay`)
To implement polling, active dispatch monitoring, or backoff loops without holding the lock:
1. Perform the current state check and state transition.
2. If further work is required in the future, schedule a **delayed asynchronous self-send** using `restate.ObjectSend(...)`:
   ```go
   // Reconcile self-send releases the exclusive lock immediately.
   _ = restate.ObjectSend(ctx, serviceName, key, "Reconcile").Send(
       ReconcileRequest{Reason: "active dispatch recovery"},
       restate.WithDelay(5*time.Second),
   )
   return result, nil
   ```
3. Return promptly from the handler.
4. The lock is immediately released. When the delay elapses, Restate delivers the message and reacquires the lock for the next iteration. Other handlers can execute freely in the interim.

### 4.3 Capturing Transient Timer Delivery Errors
When dispatching delayed self-sends or event messages, capture or ignore delivery errors cleanly if the transition itself has already succeeded, preventing transient messaging failures from trapping the object in an indefinite backoff loop.

### 4.4 Concurrent Shared Handlers (`ObjectSharedContext`)
When an operation does not mutate core state machine transitions or only needs read-only access or atomic out-of-band updates, use `restate.ObjectSharedContext`:
- Multiple shared handlers can execute concurrently on the same key without queueing behind each other.
- Shared handlers do NOT block exclusive handlers from starting once current shared executions finish.
- Canonical use cases:
  - `UpdateActiveStep`: Live widget step updates without blocking build reconciliation.
  - `Enqueue` / `CancelTurn`: Inspecting active turns and issuing cancellation signals.
  - Query handlers returning status snapshots.

### 4.5 Canceling In-Flight Invocations (`restate.CancelInvocation`)
A shared handler can cancel an active, hung, or superseded exclusive invocation:
```go
func (t *ThreadSessionObject) CancelTurn(ctx restate.ObjectSharedContext, req CancelTurnRequest) (CancelTurnResponse, error) {
    activeInvID := getActiveInvocation(restate.Key(ctx))
    if activeInvID != "" {
        restate.CancelInvocation(ctx, activeInvID)
    }
    return CancelTurnResponse{Status: "cancelled"}, nil
}
```

### 4.6 Breaking Object-to-Object Exclusive Call Cycles
Two Virtual Objects calling each other synchronously (`restate.Object[T](ctx, ...).Call(...)`) will deadlock if they attempt to acquire exclusive locks on each other simultaneously.
- **Rule**: Break cycles using asynchronous messaging (`restate.ObjectSend(...)`), or route coordination through a stateless `Service` (such as `BuildRouterService`).

---

## 5. Two-Phase Side Effects & Generation Fencing

### 5.1 Two-Phase Side Effects Pattern
When modifying external systems (e.g. creating GitHub PRs, dispatching agent processes, or triggering deployments):
1. **Phase 1: State Transition & Intent Recording**: Commit the state transition and intent in PostgreSQL and Restate K/V state first inside `restate.Run`.
2. **Phase 2: Idempotent External Dispatch**: Execute the external action using an idempotency key.
3. If Phase 2 fails, the persistent intent recorded in Phase 1 ensures a subsequent reconcile pass recovers the exact same action without duplicating work.

### 5.2 Read-Only Verdict + Idempotent Publish-Ack
- First compute the next state transition as a pure function of current state and event inputs.
- Inside a journaled step, write the new state and emit the external notification.
- Never emit side effects before state changes are durably persisted.

### 5.3 Monotonic Generation & Dispatch ID Fencing
To prevent zombie workers or out-of-order delayed timer events from acting on obsolete state:
- Tag dispatches with monotonic fencing tokens: `(Revision, Generation)` or `ExpectedDispatchID`.
- When an outcome arrives (e.g. `OnDispatchOutcome` or webhook callback), verify that the incoming token matches the currently active dispatch.
- If the token is stale, log a warning and discard the outcome cleanly without mutating state.

---

## 6. K/V State Lifecycle & Vanadis Non-Terminal `failed` Invariant

### 6.1 Guarding K/V State Writes
- Never write K/V slots (`restate.Set`) on invalid input, validation failures, or no-op transitions.
- Ensure state writes only occur when a transition genuinely updates the model.

### 6.2 Vanadis Non-Terminal `failed` State Contract
In Vanadis (`services/vanadis-restate`), **`statusFailed` (`"failed"`) is a recoverable pause state, NEVER a terminal state.**

1. **Only `statusCompleted` (`"completed"`) and `statusCancelled` (`"cancelled"`) (plus `statusNotFound` on non-existent records) are terminal build states that may clear Virtual Object state (`rt.ClearState()` / `restate.ClearAll(ctx)`).**
2. **Never clear Restate state on `statusFailed`**:
   - A `failed` build retains its full Restate K/V state (`state`, `launched`, `deployment_deadlines`, `pending_deployment_starts`).
   - The build sets `TerminalNotified: true` in its state model so duplicate failure notifications to Discord are suppressed while paused.
3. **Recovery Paths for Paused Builds**:
   While paused in `failed`, a build remains addressable and can recover to `running` via:
   - PR head synchronization or reopen (`OnPREvent`).
   - PR attachment recovery (`AttachPR`).
   - Chunk retry (`RetryActionChunk` / `FailChunk` retry).
   - Deployment retry (`retry_deployment`).
   - Chunk amendments (`AppendChunks`, `UpdateChunk`).
   - Or it can be explicitly cancelled by the user (`Cancel`), which transitions it to `cancelled` and clears state.
4. **Guard Delayed Timer Handlers**:
   - Any delayed timer handler (e.g. deployment deadline check, reconcile timer) must inspect the current build state.
   - If the build is `failed`, `completed`, or `cancelled`, the timer handler must no-op cleanly without modifying state.

---

## 7. Multi-Agent Service Naming, Timeout Reconciliation, Versioning & Testing

### 7.1 Dynamic Multi-Agent Service Naming
In multi-agent deployments (e.g. Aerial, Suletta, Miorine running on the same cluster), avoid service name collisions by naming services deterministically per agent stack:
```go
func BuildServiceName(agent string) string {
    norm := strings.ToLower(strings.TrimSpace(agent))
    if norm == "" || norm == "aerial" {
        return "Build"
    }
    return "Build" + strings.Title(norm) // e.g. BuildSuletta, BuildMiorine
}
```

### 7.2 Startup Timeout Reconciliation
Restate assigns default inactivity and abort timeouts to services. Long-running agent tasks or builds require extended timeouts (e.g. 1 hour).
Reconcile service timeouts programmatically during startup via the Restate Admin API (`PATCH /services/<service_name>`):
```go
reqURL := fmt.Sprintf("%s/services/%s", adminURL, url.PathEscape(serviceName))
payload := []byte(`{"inactivity_timeout":"1h","abort_timeout":"1h"}`)
// Send HTTP PATCH to adminURL
```

### 7.3 Safe In-Flight Versioning (`RT0016` Avoidance)
Modifying the sequence, count, or type of `restate.Run`, `restate.Get`, `restate.Set`, or `restate.Call` in an existing handler will invalidate active journal histories, triggering `RT0016: Journal mismatch` on replay.
- Keep existing step names and order stable.
- If adding new steps, gate them behind a monotonic schema version or feature flag.
- For breaking state machine changes, register a new versioned service (e.g. `BuildV2`) or ensure active instances have completed before deploying.

### 7.4 Hermetic Testing Patterns
- **Unit Testing Handlers with Mock Runtimes**: Factor state access behind an interface (e.g. `objectRuntime`) so state transitions, validation logic, and error mapping can be tested hermetically with table-driven tests without requiring a live Restate daemon.
- **Restate SDK Mock Context**: Use `restate.WithMockContext` for testing handler logic with simulated journal state.
- **Disabling Invoker Inactivity Timeouts in Tests**: When running tests against an embedded or local Restate testcontainer, pass `RESTATE_WORKER__INVOKER__INACTIVITY_TIMEOUT=0m` to prevent spurious timeouts during test debugging.
