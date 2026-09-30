# Implementation Plan: Dynamic Voice Agent Pool Hot-Reloading

## Problem Statement
In `aerial-brain` (`brain/main.go`), `createVoiceProcessPool` constructs an `AgentPool` instance (`UnifiedProcessPool` or `GeminiAPIPool`) once at container startup. When configuration changes are merged into `azylman/aerial-config` (such as switching `voice.engine` from `agy` to `gemini_api`, changing `voice.model`, or updating MCP servers), the file watcher triggers `WorkerPool.MarkDirty()`. While `UnifiedProcessPool` rotates existing daemons, the `WorkerPool` retains its initial static `voiceProcessPool` instance. As a result, switching voice engines or updating voice models requires a full `aerial-brain` container restart.

## Proposed Architecture: `DynamicVoicePool`
Implement a concurrent, atomic `DynamicVoicePool` in `brain/pkg/runner` that wraps the underlying `AgentPool` and manages hot-reload transitions seamlessly in-process.

### Core Components & Interfaces
1. **`DynamicVoicePool` (`brain/pkg/runner/dynamic_pool.go`)**:
   - Implements `runner.AgentPool`:
     - `GetOrCreateSession(ctx, targetKey, sessionID) (AgentSession, error)`:
       - Obtains `currentPool` copy under brief `RLock()`, releases lock immediately, and invokes `pool.GetOrCreateSession(ctx, targetKey, sessionID)` on the local copy (preventing write starvation or deadlocks during pool swaps).
     - `Initialize(ctx) error`: Delegates to `currentPool.Initialize(ctx)`.
     - `Close() error`: Closes the current active pool and waits on `retiringWg` for retiring pools to finish closing.
   - Implements `interface{ MarkDirty() }`:
     - Uses a separate `reloadMu sync.Mutex` to serialize concurrent `MarkDirty` invocations (debouncing/coalescing rapid file watcher events).
     - Evaluates current config snapshot and computes a structural hash/fingerprint (engine, model, API key hash, MCP servers, system prompt).
     - **Unchanged Fingerprint**: Forwards `MarkDirty()` and `UpdatePrewarmedTargets()` to `currentPool` WITHOUT nuking session history.
     - **Changed Fingerprint**:
       1. Instantiates candidate `AgentPool` via factory outside of mutex locks.
       2. Initializes new candidate pool with a safe 30s timeout (`context.WithTimeout(context.Background(), 30*time.Second)`).
       3. If initialization fails: logs a warning, retains the Last Known Good Pool (LKGP), and emits an LKGP metric.
       4. If initialization succeeds: acquires write `Lock()` strictly for the atomic pointer swap (`p.currentPool = newPool`, `p.currentKey = newKey`).
       5. Tracks old pool retirement with `retiringWg.Add(1)` and closes it in a tracked background goroutine.
   - Implements `interface{ UpdatePrewarmedTargets([]string) }`:
     - Copies `currentPool` under `RLock()` and forwards to `currentPool`.
   - Implements `interface{ Model() string }`:
     - Returns active pool's model string.

2. **`GeminiAPIPool` Enhancements (`brain/pkg/runner/gemini_api_pool.go`)**:
   - Implement `MarkDirty()`: Rotates idle sessions while preserving active in-flight turns.
   - Implement `UpdatePrewarmedTargets(targets []string)`: Updates prewarmed target list in memory.

3. **Wiring in `brain/main.go`**:
   - Update `createVoiceProcessPool` to return `NewDynamicVoicePool(cfg, voiceHome, lowEffortModel, spawner)`.
   - On initial container startup, `Initialize()` bubbles up fatal errors if initial pool creation fails.
   - When the watcher fires `reloadConfig("Hot-Reload")`, `options.workerPool.MarkDirty()` calls `voiceProcessPool.MarkDirty()`, triggering in-process hot-reloading.

## Concurrency & Thread-Safety Invariants (Girl Gang Remediations Incorporated)
- **Zero Lock Contention**: `RLock()` is held strictly to capture the pointer. Turn execution runs unlocked on the captured instance.
- **Serialized Reloads**: `reloadMu` prevents stampedes and double-initializations during rapid file watcher storms.
- **Tracked Retirement**: `retiringWg sync.WaitGroup` tracks retiring background pool closures so container shutdown can cleanly wait for full resource cleanup.
- **LKGP Fallback**: If an updated config fails verification/initialization, the existing running pool remains fully operational.

## Hermetic Testing Strategy (TDD)
- **Unit Tests (`brain/pkg/runner/dynamic_pool_test.go`)**:
  - `TestDynamicVoicePool_Passthrough`: Verify sessions and initialization delegate transparently.
  - `TestDynamicVoicePool_MarkDirty_NoopWhenUnchanged`: Verify pool is not recreated when fingerprint matches and session history is preserved.
  - `TestDynamicVoicePool_MarkDirty_SwapsOnConfigChange`: Verify switching engines (e.g. MockPool1 to MockPool2) initializes new pool, updates active reference, and retires old pool.
  - `TestDynamicVoicePool_MarkDirty_RetainsLKGPOnFailure`: Verify failed initialization preserves existing pool.
  - `TestDynamicVoicePool_ConcurrentAccessDuringReload`: Race detector (`-race`) validation with 50 goroutines requesting sessions while reload fires.
  - `TestDynamicVoicePool_SerializedReloads`: Verify concurrent `MarkDirty` calls are safely serialized.
  - `TestDynamicVoicePool_GracefulShutdownDrainsRetiring`: Verify `Close()` waits for retiring pools.
- **Unit Tests (`brain/pkg/runner/gemini_api_pool_test.go`)**:
  - `TestGeminiAPIPool_MarkDirty`: Verify session history lifecycle on MarkDirty.

## Implementation Tasks (~15-Minute Sizing)
- **Task 1: GeminiAPIPool Lifecycle Methods** (1 failing test + add `MarkDirty` / `UpdatePrewarmedTargets` + verify).
- **Task 2: DynamicVoicePool Implementation & Tests** (TDD tests in `dynamic_pool_test.go` + implement `DynamicVoicePool` + verify with `-race`).
- **Task 3: Brain Main Wiring & Integration Test** (Wire in `brain/main.go` + integration test in `brain/pkg/queue` + verify).
- **Task 4: Pre-Flight Verification Sweep** (`./scripts/verify.sh --staged` + monorepo checks).
