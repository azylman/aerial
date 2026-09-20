# Implementation Plan: Purge Ephemeral Runner Suite & Fallback Toggles

## 1. Overview & Architectural Motivation
Now that persistent streaming daemons (`runner.Daemon` via `DaemonPool`, and `runner.UtilityDaemon`) are the standard, verified execution engine for all Discord threads, sessions, and ambient classification turns, the legacy ephemeral single-turn execution path is obsolete.
Previously, when persistent daemons were introduced in PR #320, the ephemeral process spawner (`RunAgyWithWatchdog`, `RunAgy`, `RunAgyWithOptions`, `DefaultWatchdogOptions`, `WatchdogOptions`, `activityTap`, `CmdRunner`, `defaultCmdRunner`, `execProcessRunner`, and `-p` print-mode CLI arguments) was retained behind a config flag (`WorkerPoolConfig.UsePersistentDaemons`) and fallback branches in `worker.go` (`te.pool.cfg.RunnerWithOptionsFunc` and `te.pool.cfg.RunnerFunc`).

The objective is to eliminate this ~1,400-line dead code path, remove `UsePersistentDaemons`, purge the ephemeral single-turn subprocess spawner from `runner.go` and `pure.go`, and streamline turn execution.

---

## 2. Scope & Changes

### 2.1 Brain Runner Package (`brain/pkg/runner/`)
- **`brain/pkg/runner/runner.go`**:
  - Remove `RunAgyWithWatchdog`, `RunAgy`, and `RunAgyWithOptions`.
  - Remove `DefaultWatchdogOptions` and `WatchdogOptions`.
  - Remove `CmdRunnerFunc`, `CmdRunner`, `defaultCmdRunner`, `execProcessRunner`, and `activityTap` (which were only used by `RunAgyWithWatchdog`).
  - Keep all shared utilities actively used by `daemon.go`, `utility_worker.go`, `worker.go`, etc.:
    - `IsValidUUID`
    - `sessionProbe`, `extractUUID`
    - `ActivityWriter`
    - `ParseAgyOutput`, `AgyResponse`, `AgyUsage`
    - `StepUpdateEvent`, `StepUpdateHandler`, `ResolvedType`, `ResolvedToolName`, `ResolvedCommandName`, `ExtractCommandName`, `tokenizeCommandLine`, `isPOSIXIdentifier`
    - `ClassifyError`, `IsInactivityTimeout`, `IsQuotaPause`, `ExtractQuotaResetDuration`, `IsYieldTrap`, `extractErrorDetail`, `containsFatalStderrError`
    - `ExtractSessionID`
- **`brain/pkg/runner/pure.go`**:
  - Remove dead watchdog types: `WatchdogAction`, `WatchdogStatusInput`, `WatchdogDecision`, `EvaluateWatchdogStatus`.
  - In `BuildAgyArgs`: Remove `-p` prompt appending and single-turn CLI args (`--print-timeout`, `--conversation` with `-p`); standardize on persistent streaming arguments.
- **`brain/pkg/runner/utility_daemon.go`**:
  - In `(d *UtilityDaemon) RunnerFunc()`: Remove the bypass to `RunAgyWithWatchdog` when `sessionID != ""`. The utility daemon directly executes via `d.Execute(ctx, prompt)`.
- **`brain/pkg/runner/runner_test.go` & `pure_test.go`**:
  - Remove tests for `RunAgyWithWatchdog`, `RunAgy`, `RunAgyWithOptions`, and `EvaluateWatchdogStatus`.
  - Retain and verify tests for `IsValidUUID`, `ParseAgyOutput`, `ClassifyError`, `ExtractCommandName`, `tokenizeCommandLine`, `ActivityWriter`, and `BuildAgyArgs`.

### 2.2 Brain Queue Package (`brain/pkg/queue/`)
- **`brain/pkg/queue/pool.go`**:
  - Remove `UsePersistentDaemons bool` from `WorkerPoolConfig`. Persistent daemons are the one and only execution architecture.
  - Simplify `New(...)`:
    - Remove `cfg.UsePersistentDaemons = true` assignment.
    - Remove conversion between `RunnerWithOptionsFunc` and `runner.WatchdogOptions`.
    - If a test passes `RunnerFunc` or `RunnerWithOptionsFunc`, retain the ability for test doubles to intercept turn execution for hermetic unit testing without relying on the ephemeral runner engine.
- **`brain/pkg/queue/worker.go`**:
  - In `executeWithRetries()` (line 1412):
    - Remove `&& te.pool.cfg.UsePersistentDaemons` check.
    - Production always routes through `te.pool.daemonPool.GetOrCreateDaemon(...)`.
    - Clean up the fallback `else if te.pool.cfg.RunnerWithOptionsFunc != nil` block that constructed `watchdogOpts := runner.DefaultWatchdogOptions(currentTimeout)`.
- **`brain/pkg/queue/worker_test.go`**:
  - Remove redundant `UsePersistentDaemons: true` assignments across test cases.
- **`brain/pkg/queue/coverage_boost_test.go`**:
  - Update tests asserting on `UsePersistentDaemons` or `RunnerWithOptionsFunc not configured`.

---

## 3. Invariants & Guardrails
- **PostgreSQL 16 & pgvector Invariant (Invariant 7)**: No database schema or driver changes; PostgreSQL remains exclusively utilized.
- **Continuous Deployment & Verification (Invariant 6)**: Must pass clean-room verification (`./scripts/verify.sh --staged`) and satisfy coverage floor (>= 95.0%).
- **Token Isolation & Subagent Execution**: Consolidated review subagent executes The Girl Gang review panel, and Devil's Advocate subagent audits the diff prior to PR submission.
- **Single-Message Plain Prose Output**: Final response to Discord must be concise plain prose (< 1,800 chars, no markdown tables, valid GitHub web links only).

---

## 4. Verification Plan
1. Local targeted tests:
   - `go test -v ./pkg/runner/...`
   - `go test -v ./pkg/queue/...`
2. Statement coverage floor verification:
   - Verify coverage in `pkg/runner` (>= 95.0%) and `pkg/queue` (>= 95.0%).
3. Clean-room monorepo verification:
   - `./scripts/verify.sh --staged`
