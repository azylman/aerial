# Implementation Plan: Phase 3 Error Remediation (`brain` Core Engines & Quarantine Elimination)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remediate all 92 remaining swallowed errors (`_ = ...`, unhandled stream closures, unchecked writes/encodes) across `brain` core execution engines (`brain/pkg/runner`, `brain/pkg/queue`, `brain/funnel.go`, `brain/main.go`). Completely delete the `errcheck` quarantine rules in `.golangci.yml`, achieving 100% monorepo enforcement of `errcheck.check-blank: true` with zero exceptions, while maintaining the uniform ≥ 95.0% statement test coverage floor across all 21 packages.

**Architecture:**
- **Tier A (Fatal Errors)**: Direct return on critical failures (I/O streaming failures, DB state inconsistency).
- **Tier B (Best-Effort / Warnings)**: Structured warning logs for secondary updates (session saves, message retry increments, telemetry status updates, Discord cache additions).
- **Tier C (Benign / Suppression)**: Filter and ignore expected conditions (`os.ErrNotExist`, `net.ErrClosed`, client disconnects `syscall.EPIPE`/`ECONNRESET`, process already dead `os.ErrProcessDone`).
- **HTTP Transport Pooling**: Reuse `drainAndClose`, `writeResponse`, and `writeJSON` patterns in `brain/main.go`.
- **Zero Quarantine**: Fully purge `issues.exclude-rules` for `errcheck` in root `.golangci.yml`.

**Tech Stack:** Go 1.24, `golangci-lint` v1.64.5+, `errcheck` (`check-blank: true`, `check-type-assertions: true`), standard library `net/http`, `io`, `os`, `syscall`, `database/sql`.

---

## Global Constraints
- Target files must reach 0 swallowed errors (`_ = ...`) in production code.
- Every touched Go package must maintain ≥ 95.0% statement test coverage floor:
  - `brain` (root): currently 95.0%
  - `brain/pkg/queue`: currently 95.0%
  - `brain/pkg/runner`: currently 95.5%
- Clean-room tests (`./scripts/verify.sh` and `./scripts/check-coverage.sh --check`) must pass with 100% success.
- Subagent review panel (the girl gang) must audit and approve changes before creating PR / merging.
- No markdown tables in user-facing Discord messages; GitHub web links only.

---

### Task 1: Remediate Swallowed Errors in `brain/pkg/runner` (22 Violations)

**Files:**
- Modify: `brain/pkg/runner/runner.go:575-822`
- Modify: `brain/pkg/runner/runner_posix.go:24-25`
- Modify: `brain/pkg/runner/runner_windows.go:16`
- Modify: `brain/pkg/runner/utility_worker.go:93-356`
- Test: `brain/pkg/runner/runner_test.go`
- Test: `brain/pkg/runner/utility_worker_test.go`

**Interfaces:**
- Consumes: `os/exec`, `io.Closer`, `syscall`, `encoding/json`
- Produces: `closeWarn(closer io.Closer, name string)`, robust stream writing, process kill error filtering (`os.ErrProcessDone`, `syscall.ESRCH`)

- [ ] **Step 1: Add `closeWarn` and safe kill helpers in `brain/pkg/runner`**
  - Add `closeWarn(closer io.Closer, name string)` in `utility_worker.go` to safely close pipes and log unexpected errors.
  - In `runner_posix.go` and `runner_windows.go`, filter `syscall.ESRCH` and `os.ErrProcessDone` when killing processes.

- [ ] **Step 2: Remediate 12 violations in `brain/pkg/runner/utility_worker.go`**
  - Lines 93, 98, 104, 145, 148, 205, 211, 212, 312, 315, 347, 350: replace unhandled pipe closures with `closeWarn`.
  - Line 356: inspect `w.cmd.Wait()` error, logging warning if non-nil and not already finished.

- [ ] **Step 3: Remediate 7 violations in `brain/pkg/runner/runner.go`**
  - Lines 575, 583, 623: handle `t.w.Write(line)` errors.
  - Line 604: log warning on `json.Unmarshal(raw.StepUpdate, &ev)` failure.
  - Lines 719, 822: log warning if `env.EnsureAgySettingsForHome` fails.
  - Line 763: safely inspect error from `session.LastActivityFromRoots`.

- [ ] **Step 4: Add unit tests in `runner_test.go` and `utility_worker_test.go`**
  - Exercise error branches for `closeWarn`, pipe write errors, and settings fallback to maintain statement coverage ≥ 95.0% (target: ≥ 95.5%).

- [ ] **Step 5: Verify and commit**
  ```bash
  go test -v -cover ./pkg/runner/...
  git add brain/pkg/runner/
  git commit -m "feat(lint): remediate swallowed errors in brain/pkg/runner"
  ```

---

### Task 2: Remediate Swallowed Errors in `brain/pkg/queue` (42 Violations)

**Files:**
- Modify: `brain/pkg/queue/worker.go:1115-1936`
- Modify: `brain/pkg/queue/pool.go:512, 519`
- Modify: `brain/pkg/queue/status_updater.go:319, 376, 397`
- Test: `brain/pkg/queue/queue_test.go`
- Test: `brain/pkg/queue/coverage_boost_test.go`

**Interfaces:**
- Consumes: `db.Store`, `discordgo.Session`
- Produces: checked state transitions, logged message retries, robust session rotation

- [ ] **Step 1: Remediate 3 violations in `status_updater.go`**
  - Lines 319, 376, 397: check `u.deleteFunc` error, logging warning if deletion fails.

- [ ] **Step 2: Remediate 2 violations in `pool.go`**
  - Lines 512, 519: inspect DB sweep errors in `sweepContext` and log warning if message status update fails.

- [ ] **Step 3: Remediate 37 violations in `worker.go`**
  - Line 1115: safely handle error from `te.getPreviousSessionID(te.threadID)`.
  - Lines 1271, 1274, 1276: inspect pause message delivery and status updates with warning logging.
  - Lines 1383, 1395, 1411, 1618: inspect `te.saveSessionID` error with warning logging.
  - Lines 1466, 1468, 1516, 1518, 1548, 1577, 1579, 1689, 1691, 1722, 1761, 1763, 1798, 1800, 1830, 1864, 1866, 1898, 1900, 1934, 1936: inspect `te.updateMessageStatus`, `te.updateScheduleRunStatus`, and `te.updateMessageCompleted` with warning logging.
  - Lines 1500, 1732, 1782, 1863: inspect `te.incrementMessageRetry` with warning logging.
  - Lines 1553, 1677, 1748, 1836: inspect `te.rotateSessionID` with warning logging.

- [ ] **Step 4: Add unit tests in `brain/pkg/queue`**
  - Add tests exercising the warning branches to guard the 95.0% statement coverage floor (target: ≥ 95.3%).

- [ ] **Step 5: Verify and commit**
  ```bash
  go test -v -cover ./pkg/queue/...
  git add brain/pkg/queue/
  git commit -m "feat(lint): remediate swallowed errors in brain/pkg/queue"
  ```

---

### Task 3: Remediate Swallowed Errors in `brain/funnel.go` & `brain/main.go` (28 Violations)

**Files:**
- Modify: `brain/funnel.go:54, 118, 236, 253, 478`
- Modify: `brain/main.go:56-974`
- Test: `brain/main_test.go`
- Test: `brain/funnel_test.go`

**Interfaces:**
- Consumes: `net/http`, `discordgo.Session`, `io.Closer`
- Produces: `writeResponse`, `writeJSON`, `drainAndClose`, safe Discord state cache updates

- [ ] **Step 1: Add HTTP write helpers in `brain/main.go`**
  - Introduce `writeResponse(w http.ResponseWriter, r *http.Request, data []byte)`
  - Introduce `writeJSON(w http.ResponseWriter, r *http.Request, status int, data interface{})`
  - Introduce `drainAndClose(body io.ReadCloser, name string)`

- [ ] **Step 2: Remediate 23 violations in `brain/main.go`**
  - Line 56: replace unhandled `r.Body.Close()` with `drainAndClose(r.Body, "webhook request body")`.
  - Lines 62, 105, 244, 253, 295, 302, 370, 395, 404, 413, 422, 470, 479, 511, 534, 557, 575, 588, 611: replace `json.NewEncoder(w).Encode(...)` with `writeJSON(w, r, status, payload)`.
  - Line 175: handle error from `os.Stat(tPath)`.
  - Lines 699, 704: replace `w.Write(...)` with `writeResponse(w, r, ...)`.
  - Line 802: check error from `delivery.SendSystemAlert` with warning log.
  - Line 883: check error from `InitializeBrainEnvironment(ctx, cfg)`.
  - Lines 934, 974: safely close `dgSession` and `fileWatcher` on shutdown with warning logs.

- [ ] **Step 3: Remediate 5 violations in `brain/funnel.go`**
  - Lines 54, 478: inspect `s.State.MemberAdd` error with warning log.
  - Lines 118, 236, 253: inspect `s.State.ChannelAdd` error with warning log.

- [ ] **Step 4: Add unit tests in `brain/main_test.go` and `brain/funnel_test.go`**
  - Exercise error branches for `writeJSON`, `writeResponse`, and Discord state caching to maintain `brain` statement coverage ≥ 95.0%.

- [ ] **Step 5: Verify and commit**
  ```bash
  go test -v -cover ./... (inside brain/)
  git add brain/main.go brain/funnel.go brain/main_test.go brain/funnel_test.go
  git commit -m "feat(lint): remediate swallowed errors in brain main and funnel"
  ```

---

### Task 4: Quarantine Elimination & Full Monorepo Verification

**Files:**
- Modify: `.golangci.yml:56-63`
- Test: All monorepo packages

- [ ] **Step 1: Delete `errcheck` quarantine from `.golangci.yml`**
  - Completely remove the `errcheck` entries under `issues.exclude-rules`:
  ```yaml
    # 4. Burndown Quarantine for errcheck (Phase 3: brain core engines)
    - path: ^brain/(main|funnel)\.go$
      linters:
        - errcheck
    - path: ^brain/pkg/(queue|runner)/
      linters:
        - errcheck
  ```
  - Result: 0 quarantine rules remain in `.golangci.yml`.

- [ ] **Step 2: Run multi-module `golangci-lint` across entire monorepo**
  - `golangci-lint run --path-prefix="brain/" --config ../.golangci.yml ./...` (inside `brain/`)
  - `golangci-lint run --path-prefix="dashboard/" --config ../.golangci.yml ./...` (inside `dashboard/`)
  - `golangci-lint run --path-prefix="discord-mcp/" --config ../.golangci.yml ./...` (inside `discord-mcp/`)
  - `golangci-lint run --path-prefix="scheduler-mcp/" --config ../.golangci.yml ./...` (inside `scheduler-mcp/`)
  - `golangci-lint run --path-prefix="sidecars/gitsync/" --config ../../.golangci.yml ./...` (inside `sidecars/gitsync/`)
  - Expected: 0 issues across the entire codebase!

- [ ] **Step 3: Run full `./scripts/verify.sh` and `./scripts/check-coverage.sh --check`**
  - Clean-room verification: 100% tests pass, global statement coverage ≥ 95.0%.

- [ ] **Step 4: Commit quarantine elimination**
  ```bash
  git add .golangci.yml
  git commit -m "feat(lint): eliminate errcheck quarantine and enforce check-blank globally"
  ```

- [ ] **Step 5: Dispatch Girl Gang Review Panel & Open PR**
  - Whole-branch review from `origin/main` to `HEAD`.
  - Push branch to origin and create PR.
