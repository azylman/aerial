# Implementation Plan: Phase 3 Error Remediation (`brain` Core Engines & Quarantine Elimination)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remediate all 144 remaining swallowed errors (`_ = ...`, unhandled stream closures, unchecked writes/encodes) across `brain` core execution engines (`brain/pkg/runner`: 24, `brain/pkg/queue`: 86, `brain/main.go` & `brain/funnel.go`: 34). Completely delete the `errcheck` quarantine rules in `.golangci.yml`, achieving 100% monorepo enforcement of `errcheck.check-blank: true` with zero exceptions, while maintaining the uniform ≥ 95.0% statement test coverage floor across all packages.

**Architecture:**
- **Tier A (Fatal Errors)**: Direct return on critical failures:
  - `brain/pkg/queue/history.go:433`: Safely check type assertion `s, ok := v.(string)` and return error if invalid.
  - Pipe write errors in `brain/pkg/runner/runner.go`.
- **Tier B (Best-Effort / Warnings)**: Structured warning logs for non-fatal status/lifecycle updates:
  - Worker state updates, retries, session saves, pause deliveries, and session rotations in `brain/pkg/queue/worker.go`.
  - Recovery resets in `brain/pkg/queue/recovery.go:92, 100`.
  - Deployment drain sweeps in `brain/pkg/queue/pool.go:512, 519`.
  - Webhook status responses in `brain/pkg/queue/webhook.go:216, 219`.
  - Discord State cache additions in `brain/funnel.go` and `brain/pkg/queue/channel_resolution.go`.
  - Message existence check in `brain/funnel.go:771`.
  - Process pipe closures via `closeWarn` in `brain/pkg/runner/utility_worker.go`.
- **Tier C (Benign / Suppression)**: Cleanly filter expected/benign error conditions without log noise:
  - Client disconnects (`syscall.EPIPE`, `syscall.ECONNRESET`, `net.ErrClosed`, context cancellation) in HTTP handlers.
  - Listener shutdown `net.ErrClosed` in `brain/main.go:994`.
  - Process kill errors (`os.ErrProcessDone`, `syscall.ESRCH`) in `runner_posix.go` and `runner_windows.go`.
- **HTTP Transport Pooling**: Reuse `drainAndClose`, `writeResponse`, and `writeJSON` patterns in `brain/main.go`.
- **Zero Quarantine**: Fully delete `issues.exclude-rules` for `errcheck` in root `.golangci.yml`.

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

### Task 1: Remediate Swallowed Errors in `brain/pkg/runner` (24 Violations)

**Files:**
- Modify: `brain/pkg/runner/runner.go:575-822` (8 violations)
- Modify: `brain/pkg/runner/runner_posix.go:24-25` (2 violations)
- Modify: `brain/pkg/runner/runner_windows.go:16` (1 violation)
- Modify: `brain/pkg/runner/utility_worker.go:93-356` (13 violations)
- Test: `brain/pkg/runner/runner_test.go`
- Test: `brain/pkg/runner/utility_worker_test.go`

**Interfaces:**
- Consumes: `os/exec`, `io.Closer`, `syscall`, `encoding/json`
- Produces: `closeWarn(closer io.Closer, name string)`, robust stream writing, process kill error filtering (`os.ErrProcessDone`, `syscall.ESRCH`)

- [ ] **Step 1: Add `closeWarn` and safe kill helpers in `brain/pkg/runner`**
  - Add `closeWarn(closer io.Closer, name string)` in `utility_worker.go` to safely close pipes and log unexpected errors.
  - In `runner_posix.go:24-25` and `runner_windows.go:16`, filter `syscall.ESRCH` and `os.ErrProcessDone` when killing processes.

- [ ] **Step 2: Remediate 13 violations in `brain/pkg/runner/utility_worker.go`**
  - Lines 93, 98, 104, 145, 148, 205, 211, 212, 312, 315, 347, 350: replace unhandled pipe closures with `closeWarn`.
  - Line 356: inspect `w.cmd.Wait()` error, logging warning if non-nil and not `os.ErrProcessDone` / `*exec.ExitError`.

- [ ] **Step 3: Remediate 8 violations in `brain/pkg/runner/runner.go`**
  - Lines 575, 583, 623: handle `t.w.Write(line)` errors.
  - Line 604: log warning on `json.Unmarshal(raw.StepUpdate, &ev)` failure.
  - Lines 719, 822: log warning if `env.EnsureAgySettingsForHome` fails.
  - Lines 748, 763: safely inspect error from `session.LastActivityFromRoots`.

- [ ] **Step 4: Add unit tests in `runner_test.go` and `utility_worker_test.go`**
  - Exercise error branches for `closeWarn`, pipe write errors, and settings fallback to maintain statement coverage ≥ 95.0% (target: ≥ 95.5%).

- [ ] **Step 5: Verify and commit**
  ```bash
  go test -v -cover ./pkg/runner/...
  git add brain/pkg/runner/
  git commit -m "feat(lint): remediate swallowed errors in brain/pkg/runner"
  ```

---

### Task 2: Remediate Swallowed Errors in `brain/pkg/queue` (86 Violations)

**Files:**
- Modify: `brain/pkg/queue/worker.go:100-1936` (74 violations)
- Modify: `brain/pkg/queue/channel_resolution.go:133, 281` (2 violations)
- Modify: `brain/pkg/queue/history.go:433` (1 violation)
- Modify: `brain/pkg/queue/pool.go:512, 519` (2 violations)
- Modify: `brain/pkg/queue/recovery.go:92, 100` (2 violations)
- Modify: `brain/pkg/queue/status_updater.go:319, 376, 397` (3 violations)
- Modify: `brain/pkg/queue/webhook.go:216, 219` (2 violations)
- Test: `brain/pkg/queue/queue_test.go`
- Test: `brain/pkg/queue/worker_test.go`
- Test: `brain/pkg/queue/coverage_boost_test.go`

**Interfaces:**
- Consumes: `db.Store`, `discordgo.Session`
- Produces: safe type assertion in `history.go`, checked state transitions, logged message retries, robust session rotation

- [ ] **Step 1: Remediate Tier A type assertion in `history.go:433`**
  - Replace unchecked `return v.(string), nil` with safe type assertion `s, ok := v.(string)` returning an informative error if `!ok`.

- [ ] **Step 2: Remediate auxiliary queue files (11 violations)**
  - `channel_resolution.go:133, 281`: inspect `s.State.ChannelAdd` and `sess.State.MemberAdd` with warning logging.
  - `pool.go:512, 519`: inspect DB sweep errors in `sweepContext` and log warning if message status update fails.
  - `recovery.go:92, 100`: inspect `store.UpdateMessageStatus` and `store.ResetMessageToPendingWithRestart` with warning logging.
  - `status_updater.go:319, 376, 397`: check `u.deleteFunc` error, logging warning if deletion fails.
  - `webhook.go:216, 219`: inspect response body closure and `io.ReadAll` error with warning logging.

- [ ] **Step 3: Remediate all 74 violations in `worker.go`**
  - Line 100: inspect `mgr[0].GetSessionLastActivity` error with warning logging.
  - Lines 347, 349, 468, 470, 515, 546: inspect early turn completion/failure DB updates with warning logging.
  - Lines 563, 685: inspect `te.getSessionID` error with warning logging.
  - Line 622: inspect `te.getRecentThreadMessages` error with warning logging.
  - Line 655: inspect graceful deployment status update.
  - Lines 667, 729: inspect `te.getSessionTurnCount` error with warning logging.
  - Lines 680, 742, 1553, 1677, 1748, 1836: inspect `te.rotateSessionID` with warning logging.
  - Line 692: inspect `te.pool.sessionMgr.EnsureSessionDir` with warning logging.
  - Lines 911, 912, 945, 947, 963, 965, 977: inspect pre-turn deferred/telemetry status updates with warning logging.
  - Line 1027: inspect `te.getThreadSummary` error with warning logging.
  - Line 1115: safely handle error from `te.getPreviousSessionID(te.threadID)`.
  - Line 1271: inspect pause message delivery error.
  - Lines 1274, 1276: inspect pause failure DB updates.
  - Lines 1383, 1395, 1411, 1618: inspect `te.saveSessionID` error with warning logging.
  - Lines 1466, 1468, 1516, 1518, 1548, 1577, 1579, 1689, 1691, 1722, 1761, 1763, 1798, 1800, 1830, 1864, 1866, 1898, 1900, 1934, 1936: inspect `te.updateMessageStatus`, `te.updateScheduleRunStatus`, and `te.updateMessageCompleted` with warning logging.
  - Lines 1500, 1732, 1782, 1863: inspect `te.incrementMessageRetry` with warning logging.

- [ ] **Step 4: Add unit tests in `brain/pkg/queue`**
  - Add tests exercising the warning branches and `history.go` type assertion error to guard the 95.0% statement coverage floor (target: ≥ 95.3%).

- [ ] **Step 5: Verify and commit**
  ```bash
  go test -v -cover ./pkg/queue/...
  git add brain/pkg/queue/
  git commit -m "feat(lint): remediate swallowed errors in brain/pkg/queue"
  ```

---

### Task 3: Remediate Swallowed Errors in `brain/funnel.go` & `brain/main.go` (34 Violations)

**Files:**
- Modify: `brain/funnel.go:54, 118, 236, 253, 478, 771` (6 violations)
- Modify: `brain/main.go:56-994` (28 violations)
- Test: `brain/main_test.go`
- Test: `brain/funnel_test.go`

**Interfaces:**
- Consumes: `net/http`, `discordgo.Session`, `io.Closer`
- Produces: `writeResponse`, `writeJSON`, `drainAndClose`, safe Discord state cache updates

- [ ] **Step 1: Add HTTP write helpers in `brain/main.go`**
  - Introduce `writeResponse(w http.ResponseWriter, r *http.Request, data []byte)`
  - Introduce `writeJSON(w http.ResponseWriter, r *http.Request, status int, data interface{})`
  - Introduce `drainAndClose(body io.ReadCloser, name string)`

- [ ] **Step 2: Remediate 28 violations in `brain/main.go`**
  - Line 56: replace unhandled `r.Body.Close()` with `drainAndClose(r.Body, "webhook request body")`.
  - Lines 62, 105, 244, 253, 295, 302, 370, 395, 404, 413, 422, 470, 479, 511, 534, 557, 575, 588, 611: replace `json.NewEncoder(w).Encode(...)` with `writeJSON(w, r, status, payload)`.
  - Line 175: handle error from `os.Stat(tPath)`.
  - Lines 699, 704: replace `w.Write(...)` with `writeResponse(w, r, ...)`.
  - Line 802: check error from `delivery.SendSystemAlert` with warning log.
  - Line 883: check error from `InitializeBrainEnvironment(ctx, cfg)`.
  - Lines 934, 974: safely close `dgSession` and `fileWatcher` on shutdown with warning logs.
  - Line 994: in `defer ln.Close()`, safely close listener filtering benign `net.ErrClosed`.

- [ ] **Step 3: Remediate 6 violations in `brain/funnel.go`**
  - Lines 54, 478: inspect `s.State.MemberAdd` error with warning log.
  - Lines 118, 236, 253: inspect `s.State.ChannelAdd` error with warning log.
  - Line 771: inspect `store.MessageExists(ctx, ...)` error with warning log.

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
