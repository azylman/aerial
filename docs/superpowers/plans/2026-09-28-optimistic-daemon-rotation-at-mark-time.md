# Optimistic Daemon Rotation at Mark Time Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate turn latency and stale skills by optimistically recycling and re-warming idle daemons at `MarkDirty()` time and rotating busy daemons immediately upon turn completion.

**Architecture:** Extend `UnifiedProcessPool.MarkDirty()` to immediately evict idle daemons and asynchronously re-warm `PrewarmedTargets` (`kiosk`, `ephemeral:classifier`, `ephemeral:summarizer`) in the background. For in-flight daemons, preserve active execution, mark `dirty = true`, and hook turn completion to rotate the instant the turn finishes. Guard `GetOrCreate` against returning dirty daemons.

**Tech Stack:** Go standard library (`sync`, `time`, `os`, `context`).

**Spec:** `docs/superpowers/specs/2026-09-28-optimistic-daemon-rotation-at-mark-time.md`

## Global Constraints

- Pure Go standard library dependencies (`sync`, `time`, `os`, `context`, etc.).
- Strictly ZERO markdown tables across all artifacts, messages, commits, code comments, and summaries (bulleted lists only).
- Strictly ZERO swallowed errors. Every error path must be logged with structured context or propagated.
- Strictly ZERO host memory inspection (`/proc/meminfo` reading, string parsing, or memory pressure eviction).
- Maintain statement coverage floor of strictly `>= 95.0%` across `brain/pkg/runner`.
- All staged changes must pass fast pre-commit verification (`powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`).
- Always address Alex directly.

---

### Task 1: Runner Subsystem - StreamingDaemon Turn-Completion Dirty Hook & ShouldRotate

**Files:**
- Modify: `brain/pkg/runner/streaming_daemon.go`
- Modify: `brain/pkg/runner/unified_pool.go`
- Test: `brain/pkg/runner/streaming_daemon_test.go`
- Test: `brain/pkg/runner/unified_pool_test.go`

**Interfaces:**
- Consumes: `d.IsDirty()`, `d.InflightCount()`, `d.State()`.
- Produces: `d.onTurnFinished` callback in `StreamingDaemon`; `ShouldRotate` recognizing dirty state.

- [ ] **Step 1: Write failing unit test for ShouldRotate dirty detection and onTurnFinished hook**
  - In `brain/pkg/runner/unified_pool_test.go`:
    - Add test case to `TestUnifiedProcessPool_ShouldRotate` verifying that a dirty daemon with 0 in-flight turns returns `(true, "daemon marked dirty during in-flight turn")`, while a dirty daemon with >0 in-flight turns returns `(false, "")`.
  - In `brain/pkg/runner/streaming_daemon_test.go`:
    - Add test `TestStreamingDaemon_OnTurnFinishedCallback` verifying `onTurnFinished` callback executes when `hasMoreInflight == false` after `"result"` event.

- [ ] **Step 2: Run test to verify RED failure**
  - Run: `go test -C brain -v ./pkg/runner -run "TestUnifiedProcessPool_ShouldRotate|TestStreamingDaemon_OnTurnFinishedCallback"`
  - Expected: Failure due to missing `onTurnFinished` hook and `ShouldRotate` ignoring dirty flag.

- [ ] **Step 3: Implement onTurnFinished in StreamingDaemon and dirty check in ShouldRotate**
  - In `brain/pkg/runner/streaming_daemon.go`:
    - Add `onTurnFinished func(d *StreamingDaemon)` field to `StreamingDaemon`.
    - Add `SetOnTurnFinished(fn func(d *StreamingDaemon))` setter under mutex protection.
    - In `readLoop()` when processing `"result"` event: if `!hasMoreInflight`, retrieve `onTurnFinished` and invoke it in a non-blocking goroutine if non-nil.
  - In `brain/pkg/runner/unified_pool.go`:
    - In `ShouldRotate(d)`:
      - Add check: `if d.IsDirty() { return true, "daemon marked dirty during in-flight turn" }` immediately after `d.InflightCount() > 0` check.
    - In `GetOrCreate()` singleflight creation:
      - Set `daemon.SetOnTurnFinished` to evaluate `p.ShouldRotate(d)`: if true and targetKey is tracked, trigger asynchronous background rotation via `p.RotateDaemon(rotCtx, targetKey, "")` and pre-warm if in `p.cfg.PrewarmedTargets`.

- [ ] **Step 4: Run tests to verify GREEN pass and statement coverage >= 95.0%**
  - Run: `go test -C brain -v -cover ./pkg/runner`
  - Expected: PASS with coverage >= 95.0%.

- [ ] **Step 5: Commit changes**
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/pkg/runner/streaming_daemon.go brain/pkg/runner/unified_pool.go brain/pkg/runner/streaming_daemon_test.go brain/pkg/runner/unified_pool_test.go`
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(runner): hook daemon turn completion and evaluate dirty state in ShouldRotate"`

---

### Task 2: Runner Subsystem - Optimistic Rotation at MarkDirty() Time & GetOrCreate Guard

**Files:**
- Modify: `brain/pkg/runner/unified_pool.go`
- Test: `brain/pkg/runner/unified_pool_test.go`

**Interfaces:**
- Consumes: `p.cfg.PrewarmedTargets`, `d.InflightCount()`, `d.IsDirty()`.
- Produces: Optimistic background rotation of idle daemons at `MarkDirty()` call; clean rejection of dirty daemons in `GetOrCreate()`.

- [ ] **Step 1: Write failing unit tests for optimistic MarkDirty and GetOrCreate dirty guard**
  - In `brain/pkg/runner/unified_pool_test.go`:
    - Add `TestUnifiedProcessPool_OptimisticMarkDirty`:
      - Subtest 1: Idle pre-warmed daemon (`kiosk`) is immediately evicted and asynchronously replaced with a fresh daemon upon `MarkDirty()`.
      - Subtest 2: Idle non-prewarmed daemon is evicted and closed upon `MarkDirty()`.
      - Subtest 3: In-flight daemon (`InflightCount() > 0`) is not closed mid-flight, marked dirty, and rotated upon turn completion.
    - Add `TestUnifiedProcessPool_GetOrCreate_RejectsDirtyDaemon`:
      - Verify that if a dirty daemon is in `p.daemons` with 0 in-flight turns, `GetOrCreate` evicts it, closes it, and spawns a fresh daemon rather than returning the dirty instance.

- [ ] **Step 2: Run tests to verify RED failure**
  - Run: `go test -C brain -v ./pkg/runner -run "TestUnifiedProcessPool_OptimisticMarkDirty|TestUnifiedProcessPool_GetOrCreate_RejectsDirtyDaemon"`
  - Expected: Failures because `MarkDirty` does not evict idle daemons and `GetOrCreate` returns dirty daemons.

- [ ] **Step 3: Implement optimistic rotation in MarkDirty and dirty guard in GetOrCreate**
  - In `brain/pkg/runner/unified_pool.go`:
    - Overhaul `MarkDirty()`:
      - Lock `p.mu`.
      - If `p.closed`, unlock and return.
      - Create slices: `toClose []*StreamingDaemon`, `toPrewarm []string`.
      - Pre-warmed targets set lookup map from `p.cfg.PrewarmedTargets`.
      - Iterate over `p.daemons`:
        - If `d == nil`: continue.
        - If `d.InflightCount() == 0`:
          - Evict from map: `delete(p.daemons, target)`.
          - Append `d` to `toClose`.
          - If `prewarmedSet[target]`: append `target` to `toPrewarm`.
        - Else:
          - Daemon has in-flight turns: call `d.MarkDirty()`.
      - Unlock `p.mu`.
      - For each `daemon` in `toClose`:
        - Close asynchronously: `go func(d *StreamingDaemon) { _ = d.Close() }(daemon)`.
      - For each `target` in `toPrewarm`:
        - Re-warm asynchronously:
          ```go
          go func(tKey string) {
              initCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
              defer cancel()
              if _, err := p.GetOrCreate(initCtx, tKey); err != nil {
                  log.Printf("[UnifiedProcessPool] Warning: re-warming prewarmed target %q failed: %v", tKey, err)
              }
          }(target)
          ```
    - In `GetOrCreate()`:
      - Fast-path: `if d, exists := p.daemons[targetKey]; exists && d != nil && d.State() != StateClosed && !d.IsDirty()`
      - In singleflight closure:
        - If `d, exists := p.daemons[targetKey]; exists && d != nil`:
          - If `d.State() != StateClosed && !d.IsDirty()`: return `d`.
          - Else: delete `targetKey` from `p.daemons`, asynchronously close `d`, and proceed to spawn a fresh daemon.

- [ ] **Step 4: Run tests to verify GREEN pass and statement coverage >= 95.0%**
  - Run: `go test -C brain -v -cover ./pkg/runner`
  - Expected: PASS with coverage >= 95.0%.

- [ ] **Step 5: Commit changes**
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" add brain/pkg/runner/unified_pool.go brain/pkg/runner/unified_pool_test.go`
  - Command: `& "C:\Users\alexz\AppData\Local\Programs\MinGit\cmd\git.exe" commit -m "feat(runner): rotate idle daemons optimistically at mark time and guard GetOrCreate"`

---

### Task 3: Comprehensive Regression & Statement Coverage Verification

**Files:**
- Verify across: `brain/pkg/runner`, `brain/pkg/queue`, `brain/pkg/env`.

- [ ] **Step 1: Run race detection and unit test verification across modified packages**
  - Run: `go test -C brain -v ./pkg/runner ./pkg/queue ./pkg/env`
  - Expected: PASS with 0 failures.

- [ ] **Step 2: Run statement coverage verification**
  - Run: `go test -C brain -cover ./pkg/runner` (Verify >= 95.0%)
  - Run: `go test -C brain -cover ./pkg/queue` (Verify >= 95.0%)
  - Run: `go test -C brain -cover ./pkg/env` (Verify >= 95.0%)

- [ ] **Step 3: Run fast pre-commit verification**
  - Run: `powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`
  - Expected: All fast lint and verification checks pass.
