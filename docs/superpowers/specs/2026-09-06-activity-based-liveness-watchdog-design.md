# Activity-Based Liveness Watchdog & Queue Error Recovery Design

• **Status**: APPROVED (By User & The Girl Gang Panel)  
• **Date**: 2026-09-06  
• **Target Systems**: `azylman/aerial` (`brain/pkg/runner`, `brain/pkg/queue`, `brain/pkg/config`)  
• **Author**: Aerial & The Girl Gang Review Squad  

---

## 1. Executive Summary & Problem Statement

Currently, `aerial-brain` enforces a static 15-minute wall-clock execution deadline on every agent turn via `context.WithTimeout(..., 15*time.Minute)` in `WorkerPool.processBurst`. When executing large, multi-package tasks (such as refactoring monorepo test suites, executing build matrices, or running deep reasoning chains), the turn is abruptly killed by `context.DeadlineExceeded` even if the agent is actively making steady progress.

Furthermore, `runner.ClassifyError` treats all deadline/timeout errors as transient network glitches (`isTransient = true`), causing `WorkerPool` to blindly re-execute the exact same monolithic turn up to 3 times, locking up queue workers for 45 minutes before failing.

This design introduces an **Activity-Based Liveness Watchdog** that monitors active execution progress in real time, granting continuous execution as long as the agent is working while failing fast on genuine freezes without burning blind retries.

---

## 2. Goals & Non-Goals

### Goals
• **Activity-Based Sliding Lease**: Monitor agent progress using dual signals (stderr byte streams and `transcript.jsonl` mtime/size deltas). Execution continues indefinitely up to an absolute safety cap as long as activity occurs within a sliding 5-minute inactivity window.
• **Hard Safety Cap**: Enforce an absolute maximum turn duration (default: 60 minutes) to prevent runaway processes.
• **Real-Time Dynamic Session Latching**: Sniff session UUIDs on the fly during cold-start turns (Turn 1) to attach disk monitoring immediately without directory polling races.
• **CLI Flag Hardening**: Explicitly configure `--print-timeout` to match the 60-minute ceiling, preventing `agy` from falling back to its internal 5-minute default timeout.
• **Queue Short-Circuit on Inactivity Stalls**: Eliminate the blind 3-attempt retry loop on genuine watchdog inactivity kills.
• **Hermetic Testability**: Parameterize watchdog thresholds via `WatchdogOptions` to ensure unit tests execute deterministically in milliseconds.

### Non-Goals
• Building complex multi-state workflow orchestration engines in the runner harness.
• Modifying Antigravity CLI binary internals or changing the JSON output contract.

---

## 3. Architecture & Detailed Component Specification

```mermaid
flowchart TD
    A[WorkerPool.processBurst] --> B[RunAgyWithWatchdog]
    B --> C[exec.CommandContext agy --print-timeout 60m]
    B --> D[ActivityWriter: Stream Stderr & Sniff UUID]
    B --> E[Watchdog Ticker: Poll transcript.jsonl every 3s]
    
    D -- "Stderr bytes / Stream event" --> F[Reset lastActivity = time.Now()]
    E -- "transcript.jsonl mtime or size delta" --> F
    
    E -- "time.Since(lastActivity) > 5m" --> G[Cancel Child Context]
    E -- "time.Since(start) > 60m" --> G
    
    G --> H[SIGKILL -PGID to Process Tree]
    H --> I[Append [watchdog] diagnostic to stderr]
    I --> J[runner.ClassifyError]
    J --> K[isTransient = false, errDetail = inactivity_timeout]
    K --> L[WorkerPool: Break retry loop & Fail Fast]
```

### 3.1. Thread-Safe Activity Tracking (`brain/pkg/runner/runner.go`)

`ActivityWriter` implements a thread-safe `io.Writer` and buffer that captures raw stderr for post-run error parsing while recording real-time activity timestamps and sniffing conversation session UUIDs:

```go
type ActivityWriter struct {
    mu           sync.Mutex
    buf          bytes.Buffer
    lastActivity atomic.Int64 // UnixNano
    sessionID    atomic.Pointer[string]
}
```

• `Write([]byte)`: Atomically updates `lastActivity = time.Now().UnixNano()` and captures chunk bytes under `mu.Lock()`.
• **Cold-Start Stream Sniffing**: If `sessionID` is not yet latched, `Write` executes `reSessionStream.FindSubmatch(p)` (`Starting conversation update stream for <uuid>`) to dynamically store the active session UUID.
• `String()`: Thread-safely extracts buffered stderr.
• `LastActivity()`: Returns `time.Time` derived from atomic nanoseconds.
• `SessionID()`: Returns the dynamically discovered or initially provided session UUID.

---

### 3.2. Watchdog Options & Execution Lifecycle (`RunAgyWithWatchdog`)

```go
type WatchdogOptions struct {
    InactivityTimeout time.Duration
    MaxDuration       time.Duration
    PollInterval      time.Duration
}

func DefaultWatchdogOptions(timeoutMinutes int) WatchdogOptions {
    maxDur := 60 * time.Minute
    if timeoutMinutes > 0 {
        maxDur = time.Duration(timeoutMinutes) * time.Minute
    }
    return WatchdogOptions{
        InactivityTimeout: 5 * time.Minute,
        MaxDuration:       maxDur,
        PollInterval:      3 * time.Second,
    }
}
```

#### Execution Steps in `RunAgyWithWatchdog`:
1. **Child Context**: Creates `runCtx, runCancel := context.WithCancel(parentCtx)`.
2. **CLI Invocation**: Passes `--print-timeout` configured to `opts.MaxDuration` (e.g. `60m`), `--output-format json`, and `--dangerously-skip-permissions`.
3. **Pipes & Buffers**: Connects `cmd.Stdout = &outBuf` and `cmd.Stderr = actWriter`.
4. **Process Group Isolation**: Sets `Setpgid: true` in `SysProcAttr` with `cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }` and `cmd.WaitDelay = 3 * time.Second`.
5. **Watchdog Goroutine**:
   - Ticks every `opts.PollInterval` (default 3s).
   - Resolves transcript paths for active session ID across `/data/brain/<uuid>/.system_generated/logs/transcript.jsonl` and `/root/.gemini/antigravity-cli/brain/...`.
   - Compares `mtime` and `size` against previous recorded values; on delta, updates `actWriter.lastActivity`.
   - Evaluates:
     - `time.Since(actWriter.LastActivity()) > opts.InactivityTimeout` ➔ Inactivity timeout.
     - `time.Since(startTime) > opts.MaxDuration` ➔ Max duration cap exceeded.
   - On trigger: stores diagnostic reason in `atomic.Pointer[string]`, invokes `runCancel()`, and terminates.
6. **Clean Teardown**: `done := make(chan struct{})` guarantees the watchdog goroutine exits immediately when `cmd.Wait()` returns. If killed by watchdog, appends `\n[watchdog] <reason>` to stderr.

---

### 3.3. Error Classification Overhaul (`brain/pkg/runner/runner.go`)

In `ClassifyError`:
• **Pre-Emptive Watchdog Intercept**: Check for `[watchdog]` or `inactivity timeout` sentinel strings **before** general transient keyword checks.
• When matched:
  - `isFailure = true`
  - `isTransient = false`
  - `isSessionCorruption = false`
  - `errDetail = "inactivity watchdog timeout: no activity for 5m"`

---

### 3.4. Queue Retry Elimination (`brain/pkg/queue/queue.go`)

In `WorkerPool.processBurst`:
• Detect watchdog inactivity kills via error detail / classification.
• When an inactivity watchdog kill occurs:
  - Rotate session in database: `db.RotateSessionID(p.cfg.DB, threadID, "")`.
  - Mark turn as `StatusFailed` immediately.
  - Deliver a dedicated failure alert to Discord.
  - **`break` out of the retry loop** to prevent burning subsequent 5-minute attempts.

---

## 4. Error Handling & Edge Cases

• **Silent Long Tool Calls**: Liveness evaluates `max(lastStderrTime, lastTranscriptTime)`. If a tool writes output on completion to `transcript.jsonl`, the 5m clock resets.
• **Fast CLI Exits (Exit Code 1 / 127)**: Synchronous `done` channel ensures watchdog goroutine tears down cleanly without signaling dead PIDs.
• **Shutdown During Turn**: If parent context `p.ctx` cancels (container SIGTERM), `p.ctx.Err() != nil` suppresses error alerts and preserves message in `PROCESSING` state for `RecoverInterrupted` on restart.

---

## 5. Verification & Testing Strategy

• **`brain/pkg/runner/runner_test.go`**:
  - Test `ActivityWriter` thread-safety and real-time session discovery regex.
  - Test `RunAgyWithWatchdog` inactivity timeout trigger (configured with 50ms inactivity window).
  - Test `RunAgyWithWatchdog` active heartbeat pulse keeping long process alive.
  - Test `ClassifyError` distinguishing watchdog kills from transient network timeouts.
• **`brain/pkg/queue/queue_test.go`**:
  - Verify that an inactivity watchdog failure terminates on Attempt 1 without executing retries.
• **Full Monorepo Verification**:
  - Run `scripts/verify.sh` to validate formatting, linting, BOM checks, and `>= 90%` Go / Permet test coverage.
