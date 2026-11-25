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

### 1.1 Dedicated `*-restate` Worker Service Separation Invariant
Restate Virtual Objects and durable workflows must run in a dedicated `*-restate` `RestateDeployment` service (e.g. `eri-restate`, `vanadis-restate`, `vision-restate`) separate from the public HTTP/UI/MCP server (`eri`, `vanadis`, `vision`).
- **Separation of Concerns**: The HTTP/UI/MCP server handles edge ingress, webhooks, user interfaces, and MCP tools statelessly. It validates incoming payloads and forwards events directly to Restate ingress (`http://restate.aerial:8080`) with deterministic `Idempotency-Key` headers.
- **Zero Pre-Ingress Postgres Tracking Writes**: The edge ingress service must never insert provisional or "pending" tracking rows into Postgres before posting to Restate ingress. All in-flight tracking, queue state, and workflow execution lifecycles live strictly in Restate K/V state and journals.
- **Postgres as Read-Model / Historical Archive**: Postgres rows are created or updated exclusively inside journaled `restate.Run` steps executed by the `*-restate` worker for querying, indexing, or archival purposes—never as the workflow's source of truth or concurrency coordinator.

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
- Preserve these errors as standard Go errors. Every service retries under the server's `default-retry-policy` (500ms initial, factor 2.0, 1m max interval, 70 attempts including the first): roughly an hour of retries, after which the invocation is **paused**, not failed. A paused invocation keeps its journal and needs a manual resume (admin API / `restate` CLI). The cluster sets no override; only test harnesses shorten it via `RESTATE_DEFAULT_RETRY_POLICY__*` env (`services/eri/pkg/restatetest/harness.go`).
- **No retry overrides**: never add `WithMaxRetryAttempts`, `WithMaxRetryDuration` or `WithInitialRetryInterval` on `restate.Run` steps, per-service or per-handler retry policies, deadline-to-terminal shortcuts, or custom idempotency retention. A capped step turns exhaustion into a `TerminalError`, converting a transient outage into a permanent failure. If retry-then-pause causes harm, change the cluster-wide default instead.

### 3.2 Terminal (Non-Retriable) Errors
Failures that will never succeed on retry must be wrapped in `restate.ToTerminalError`:
- Client validation failures (400 Bad Request, invalid JSON, missing required fields).
- Resource not found (404 Not Found).
- Unrecoverable state conflicts, stale-generation fencing, stale-state and precondition violations (412 Precondition Failed).
- **409 is reserved for Restate's own cancellation** (sdk-go surfaces a cancelled call/await as terminal 409). Never emit 409 yourself. A handler that awaits a call or `RunAsync` treats terminal 409 as "cancelled" (eri `isRestateCancel`); when re-wrapping a downstream terminal error that carries 409, remap it to 412 (eri `coordinatorError`). Constants: `CodeRestateCancelled` / `CodeQueueConflict` in `services/eri/pkg/queue/turn_contract.go`.

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
        return restate.ToTerminalError(err, restate.WithErrorCode(412))
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

### 3.4 Postgres Errors Inside `restate.Run`
Classify store errors explicitly; never let every DB error retry forever or fail at once.
- **Terminal at once (deterministic)**: SQLSTATE classes `22` (data exception / invalid input), `23` (integrity: unique, FK, check, not-null), `42` (syntax, undefined object, permission), `0A`, `2B`, `2D`, `25`.
- **Retryable (everything else)**: `08` connection, `40` serialization failure / deadlock (`40001`, `40P01`), `53` resources, `57` (query cancelled, admin shutdown), pool and context timeouts.
- Bound each store call with a short deadline (eri: `turnStepStoreTimeout = 5s`) so a hang becomes a retryable error instead of a stuck step.
- Reference: `classifyStoreError` in `services/eri/pkg/queue/turn_contract.go` (unexported; copy the pattern, there is no shared package). To map a specific owned unique constraint (`23505`) to a domain conflict, match the constraint name as vanadis-restate's `knownCreationConflict` does (`internal/store/build_create_get.go`).
- An idempotent insert uses `INSERT ... ON CONFLICT DO NOTHING` rather than surfacing `23505`.

### 3.5 Killed `RunAsync` Work
- Restate cancellation does **not** cancel a `RunAsync` closure's `RunContext`; only the await sees terminal 409. A stream drop does cancel it.
- A closure interrupted after the handler has moved past its await must return a **terminal non-409 error (412)**, never a retryable one. In sdk-go v1.2.0 a late retryable result aborts the current attempt, forcing a replay in which an unjournaled follow-up step (e.g. `stop-turn`) can run twice (observed in `services/eri/pkg/restatetest/cancel_spike_test.go`, see `errKilled`).
- Pattern: eri `attemptInterruptedError` (`turn_attempt.go`) and llmcall `stepError` (`ctx.Err() != nil` -> 412, `services/eri/pkg/llmcall/llmcall.go`).

### 3.6 Separate Metrics for Client vs Reconcile Errors
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

## 5. Two-Phase Side Effects, Idempotency Keys & Generation Fencing

### 5.1 Two-Phase Side Effects Pattern
When modifying external systems (e.g. creating GitHub PRs, dispatching agent processes, or triggering deployments):
1. **Phase 1: State Transition**: Record the transition in Restate K/V state (the source of truth) and the journal. Postgres rows are records written by journaled steps for list/get tools and cross-entity queries, not the workflow's state.
2. **Phase 2: Idempotent External Dispatch**: Execute the external action in a journaled step using a deterministic idempotency key.
3. If Phase 2 fails, Restate retries the step from the journal; the idempotency key keeps the retried action from duplicating work.

### 5.2 Read-Only Verdict + Idempotent Publish-Ack
- First compute the next state transition as a pure function of current state and event inputs.
- Inside a journaled step, write the new state and emit the external notification.
- Never emit side effects before state changes are durably persisted.

### 5.3 Monotonic Generation & Dispatch ID Fencing
To prevent zombie workers or out-of-order delayed timer events from acting on obsolete state:
- Tag dispatches with monotonic fencing tokens: `(Revision, Generation)` or `ExpectedDispatchID`.
- When an outcome arrives (e.g. `OnDispatchOutcome` or webhook callback), verify that the incoming token matches the currently active dispatch.
- If the token is stale, log a warning and discard the outcome cleanly without mutating state.

### 5.4 Idempotency Key Design: Logical Granularity & Failure Modes
When invoking Restate ingress over HTTP (`Idempotency-Key` header) or assigning downstream dispatch/message identifiers, the idempotency key must map 1:1 to the **logical identity of the intended operation**—neither coarser nor more specific.

#### Core Invariant: Never Omit or Randomize `Idempotency-Key` Per Attempt
- **Always send an `Idempotency-Key`** on HTTP calls to Restate ingress. Omitting the header or generating a fresh random UUID (`uuid.New()`) inside a retry loop or redelivery handler disables ingress deduplication: if an HTTP call times out after reaching Restate or an upstream event is redelivered, the retry launches a duplicate execution.
- Derive keys deterministically using canonical structured strings or `uuid.NewSHA1(uuid.NameSpaceOID, []byte(canonicalTuple))`.

#### Failure Mode 1: Under-Specificity & State-Dependent Early Returns (`SkipIfActive` Memoization)
Restate memoizes a handler's return value under its `Idempotency-Key` for the service's `idempotency_retention` window (default 24 hours)—**even when the handler takes a state-dependent no-op early return** (e.g., returning `skipped_active` because a session or worker is currently busy when `SkipIfActive: true`).
- **Symptom**: If the caller reuses a static or under-specified key across future attempts that are meant to re-evaluate whether the target is now idle (such as periodic heartbeat polls or a subsequent retry cycle on the same state snapshot), Restate short-circuits at ingress and replays the cached `skipped_active` response for 24 hours without invoking the handler.
- **Resolution**: Include the logical occurrence or attempt boundary in the key whenever distinct evaluations over time are valid:
  - **Scheduled ticks & periodic heartbeats**: Key on `(scheduleType, scheduleID, dueAt)` so transport retries of the *same* tick deduplicate, while the *next* scheduled tick gets a fresh key and re-evaluates live state.
  - **Bounded retry / remediation loops**: Include the monotonic attempt ordinal `(entityID, stateVersion, attemptOrdinal)` so attempt $k+1$ is not blocked by a memoized response from attempt $k$.

#### Failure Mode 2: Over-Specificity & Ephemeral Delivery Metadata
Including transport-layer noise or wall-clock publish timestamps in an idempotency key makes the key too specific, causing logically identical operations to bypass deduplication.
- **Wall-Clock Publish Timestamps (`OccurredAt: time.Now()`)**: Upstream event publishers often stamp `OccurredAt` at publish time. Hashing or appending `OccurredAt` into an idempotency key gives every webhook redelivery, NATS republish, or retry a brand-new key.
- **Per-Call Random UUIDs in Scheduled/Event Workers**: Generating `uuid.NewString()` inside a cron ticker or event-trigger dispatcher (instead of deriving the ID from the schedule's `dueAt` or the upstream `eventID`) generates a fresh key whenever a timed-out trigger is recovered and re-queued.
- **Fan-Out Noise on Single-Execution State Transitions**:
  - When an operation can only logically execute **once per state version** (because completing the operation produces a new state version, such as rebasing a branch to a new commit SHA), keying on anything beyond `(entityID, stateVersion)` allows duplicate notifications to trigger redundant runs.
  - When multiple concurrent signals in the same wave (e.g., parallel check failures on the same commit) should coalesce into a single ordinal remediation attempt $k$, including individual sub-event names or URLs in the key causes parallel executions instead of coalescing on `(entityID, stateVersion, attemptOrdinal)`.

#### Canonical Keying Recipes by Operation Shape

| Operation Shape | Canonical Key Tuple | Why |
| :--- | :--- | :--- |
| **External message / webhook ingress** | `(source, externalMessageID)` | Deduplicates upstream webhook or gateway retries of the exact same message. |
| **Scheduled cron / one-shot / heartbeat tick** | `(scheduleType, scheduleID, dueAtUTC)` | Deduplicates retries of the same due tick while giving future ticks a fresh key (preventing 24h `skipped_active` starvation). |
| **Event-watch trigger firing** | `(triggerID, upstreamEventID)` | Ensures recovered/redelivered triggers deduplicate on the exact upstream event firing. |
| **One-per-version transition** *(resolution produces a new version/SHA)* | `(entityID, stateVersion)` | At most one execution is logically valid per version; strips timestamps and duplicate detector notifications. |
| **Ordinal retry loop on a version** *(up to $N$ attempts per version)* | `(entityID, stateVersion, attemptOrdinal)` | Coalesces concurrent failure signals in wave $k$ into one attempt while giving attempt $k+1$ a distinct key. |
| **Content-addressed alert / wake** *(wake once per unique condition)* | `(entityID, sha256(canonicalPayload))` | Deduplicates repeated alerts for the same underlying failure payload by excluding wall-clock publish timestamps. |

#### Defense-in-Depth: Storage Idempotency & Context-Safe Finalization
1. **Pair Ingress Keys with Atomic Storage Guards**: Combine Restate `Idempotency-Key` headers with downstream database idempotency (`INSERT ... ON CONFLICT (id) DO NOTHING`) and idempotent async wake signals (`restate.ObjectSend`), ensuring at-least-once safety even across key rotation or retention expiry.
2. **Protect Post-Execution Finalization Checks from Stream Timeout Cancellation**: If a long-running invocation hits an ingress or stream `inactivity_timeout` right after committing its side effects and database state, the handler's `ctx` (`callerCtx`) will be canceled during post-turn cleanup. Any finalization check that queries storage using the canceled `ctx` will fail with `context canceled`, causing the handler to return a retriable error and triggering a duplicate replay storm. Always wrap post-commit finalization verification queries in `context.WithoutCancel(ctx)` with a bounded query timeout.

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
In multi-agent deployments (e.g. Aerial, AgentAlpha, AgentBeta running on the same cluster), avoid service name collisions by naming services deterministically per agent stack:
```go
func BuildServiceName(agent string) string {
    norm := strings.ToLower(strings.TrimSpace(agent))
    if norm == "" || norm == "aerial" {
        return "Build"
    }
    return "Build" + strings.ToUpper(norm[:1]) + norm[1:] // e.g. BuildAlpha, BuildBeta
}
```

### 7.2 Service Timeout Declaration & Startup Reconciliation
Restate assigns default inactivity and abort timeouts (1 minute) to newly discovered service revisions. Long-running agent tasks or builds require extended timeouts (e.g. 1 hour).
1. **Declare timeouts natively in `restate.Reflect` options**: Always pass `restate.WithInactivityTimeout(time.Hour)` and `restate.WithAbortTimeout(time.Hour)` directly when reflecting services and Virtual Objects so Restate service re-discovery on new revisions never resets timeouts back to the 1-minute default.
2. **Reconcile service timeouts programmatically during startup** via the Restate Admin API (`PATCH /services/<service_name>`):
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

#### Deployment Set Stability (`DeploymentInUse`)
- Normal rollouts are safe: the operator drains the old ReplicaSet (`drainDelaySeconds`, default 300s), in-flight invocations stay pinned to their old deployment until done, then it force-deletes the old Restate deployment.
- **Operator chart 2.8.2 (pinned in `k8s/operators/operators.yaml`)**: the `RestateDeployment` finalizer refuses deletion (`CleanupFailed` / `DeploymentInUse`) while that deployment is still the latest registration for any of its services. Deleting a whole `RestateDeployment`, or dropping a whole service from the new revision, leaves the object stuck terminating (2026-11-20 incident blocked Flux for every PR).
- Rules: keep the service set stable across revisions. To retire a service or `RestateDeployment`: confirm zero non-completed invocations via the admin `/query` on `sys_invocation`, get the admin user's explicit OK, then `DELETE /deployments/<id>?force=true` for its registrations before removing the manifest.
- restate-operator v3.0.0 changed this: only unfinished invocations hold a deletion. Re-check the deployed chart version before applying the 2.8.2 rule.

### 7.4 Hermetic Testing Patterns
- **Unit Testing Handlers with Mock Runtimes**: Factor state access behind an interface (e.g. `objectRuntime`) so state transitions, validation logic, and error mapping can be tested hermetically with table-driven tests without requiring a live Restate daemon.
- **Restate SDK Mock Context**: Use `restate.WithMockContext` for testing handler logic with simulated journal state.
- **Disabling Invoker Inactivity Timeouts in Tests**: When running tests against an embedded or local Restate testcontainer, pass `RESTATE_WORKER__INVOKER__INACTIVITY_TIMEOUT=0m` to prevent spurious timeouts during test debugging.

---

## 8. Anti-Patterns: What Not to Build When Restate Is Available

Each was hand-rolled in this repo and replaced; do not reintroduce them.

- **Status-flag claim queues in Postgres** (`UPDATE ... SET status='processing'` loops).
  - Why: a crash mid-claim strands rows (prospera call reports stuck in `processing`).
  - Instead: a Virtual Object per key queues invocations for you.
  - Example: eri's DB-claim queue -> `ThreadSession` / `DispatchSession` coordinators (legacy queries deleted in PR #2613); prospera -> `ProsperaReport` object (PR #2516).
- **Advisory locks / mutexes for per-key serialization**.
  - Why: busy-lock retries storm (Nak loops), and in-process mutexes do not span pods.
  - Instead: key a Virtual Object; its exclusive handler is the lock.
  - Example: coven advisory locks + `extractionMutex` -> `CovenThread` object (PR #2514).
- **Polling tickers and sweeps**.
  - Why: latency floor, wasted queries, races between pods.
  - Instead: delayed self-sends (`ObjectSend` + `WithDelay`, section 4.2).
  - Example: haro's ticker -> `HaroSchedule` / `HaroEventWatch` (PR #2515); coven's 30s dreaming ticker -> `CovenDreamer` (PR #2514).
- **Delete-before-send / advance-before-send**.
  - Why: a crash between the DB write and the send loses the run (haro deleted one-shots / triggers before the eri send).
  - Instead: send in a journaled step with a deterministic `Idempotency-Key`, then delete or advance in a later step.
  - Example: `HaroSchedule.Fire` (`dispatch-turn` -> `finalize-schedule`), `HaroEventWatch.Fire` (`dispatch-event` -> `consume-trigger`) (PR #2515).
- **In-memory state assuming one pod**.
  - Why: lost on restart, diverges across replicas.
  - Instead: object K/V state.
  - Example: Vanadis package-global `chunkActiveSteps` map -> per-chunk state on the `Build` object (PR #2535).
- **Cross-object row writes**.
  - Why: one object's step mutating another's rows bypasses its lock and contends on row locks.
  - Instead: `ObjectSend` to each owning object.
  - Example: Vanadis cross-build deploy sweep -> `Mutate` sends to each foreign `Build` object (PR #2534).
- **Replying "busy" and making the caller retry**.
  - Why: lease/backoff loops and memoized `skipped_active` responses (section 5.4).
  - Instead: enqueue into the Virtual Object and let it run when free.
  - Example: Vanadis dispatch deferral lease/backoff loop deleted; dispatches queue on `DispatchSession` (PR #2551).
- **Postgres as source of truth for workflow state**.
  - Why: two stores drift; replay cannot see Postgres.
  - Instead: Restate state and journal own the workflow; Postgres holds records written by journaled steps (section 5.1).
- **Pre-ingress Postgres tracking rows before calling Restate ingress**.
  - Why: writing "pending" or provisional tracking rows in an HTTP webhook or handler before posting to Restate ingress causes dual-write synchronization drift, database connection contention, and redundant coordination queries.
  - Instead: forward incoming payloads directly to Restate ingress with deterministic `Idempotency-Key` headers and track active/in-flight items directly in a Virtual Object's Restate K/V state.
