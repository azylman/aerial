# Implementation Plan: Phase 2 Error Remediation (`dashboard` & `brain` Foundation Libraries)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remediate all swallowed errors (`_ = ...`, unhandled `.Close()`, unchecked writes/encodes, discarded git returns) across `dashboard` (11 violations) and all 13 foundation packages in `brain/pkg/*` (`config`, `db`, `delivery`, `env`, `gitsync`, `memory`, `session`, `watcher`, plus verifying `classifier`, `metrics`, `notifier`, `sanitizer`, `scheduler`). Evict `dashboard` from `.golangci.yml` quarantine, narrow `brain` quarantine strictly to Phase 3 core engines (`brain/(main|funnel)\.go` and `brain/pkg/(queue|runner)/`), and maintain uniform ≥ 95.0% statement test coverage across all packages.

**Architecture:** Implement strict Tier A (Fatal returns), Tier B (Best-effort cleanups / warnings), and Tier C (Benign client disconnect / `sql.ErrTxDone` / `os.ErrNotExist` suppression) error handling. Implement `drainAndClose` in HTTP clients to preserve HTTP/1.x keep-alive connection reuse. Provide centralized `closeWarn` and `rollbackWarn` helpers in `db` to avoid uncovered branch traps. Enforce multi-module `golangci-lint` with `--path-prefix`.

**Tech Stack:** Go 1.24, `golangci-lint` v1.64.5+, `errcheck` with `check-blank: true` and `check-type-assertions: true`, standard library `net/http`, `io`, `os`, `database/sql`.

**Spec:** Monorepo `errcheck.check-blank: true` 4-phase rollout plan.

## Global Constraints
- Target packages must reach 0 swallowed errors (`_ = ...`) in production code.
- Every touched Go package must maintain ≥ 95.0% statement test coverage floor.
- Clean-room tests (`./scripts/verify.sh` and `./scripts/check-coverage.sh --check`) must pass with 100% success.
- Subagent review panel (the girl gang) must audit and approve changes before creating PR / merging.
- No markdown tables in user-facing Discord messages; GitHub web links only.

---

### Task 1: Remediate Swallowed Errors in `dashboard` & Evict from Quarantine

**Files:**
- Modify: `dashboard/main.go:445-1655`
- Test: `dashboard/main_test.go`
- Modify: `.golangci.yml:56-63`

**Interfaces:**
- Consumes: `net/http`, `io.Closer`, `encoding/json`
- Produces: `closeWarn(closer io.Closer, name string)`, `drainAndClose(body io.ReadCloser, name string)`, `isClientDisconnect(r *http.Request, err error) bool`, `writeResponse(w http.ResponseWriter, r *http.Request, data []byte)`, `writeJSON(w http.ResponseWriter, r *http.Request, status int, data interface{})`

- [ ] **Step 1: Write unit tests covering HTTP write error, client disconnect, close warning, and drainAndClose in `dashboard/main_test.go`**
```go
func TestDashboard_CloseWarnAndDisconnect(t *testing.T) {
	// 1. closeWarn with nil and error
	closeWarn(nil, "nil closer")
	closerErr := &mockCloser{err: errors.New("simulated close error")}
	closeWarn(closerErr, "err closer")

	// 2. drainAndClose with nil and error
	drainAndClose(nil, "nil body")
	drainAndClose(&mockReadCloser{Reader: strings.NewReader("hello"), closeErr: errors.New("simulated close error")}, "mock body")

	// 3. isClientDisconnect
	if isClientDisconnect(nil, nil) {
		t.Error("expected false for nil error")
	}
	if !isClientDisconnect(nil, syscall.EPIPE) {
		t.Error("expected true for EPIPE")
	}
	if !isClientDisconnect(nil, syscall.ECONNRESET) {
		t.Error("expected true for ECONNRESET")
	}
	if !isClientDisconnect(nil, net.ErrClosed) {
		t.Error("expected true for net.ErrClosed")
	}

	// 4. writeResponse & writeJSON error branch
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	mockW := &mockErrWriter{err: errors.New("write failure")}
	writeResponse(mockW, req, []byte("data"))
	writeJSON(mockW, req, http.StatusOK, map[string]string{"status": "ok"})
}
```

- [ ] **Step 2: Add helpers and remediate 11 violations in `dashboard/main.go`**
  - Add `closeWarn`, `drainAndClose`, `isClientDisconnect`, `writeResponse`, and `writeJSON` helpers.
  - In `pollWorkflowRuns` (lines 450-453): replace `_, _ = io.Copy(...)` and `_ = resp.Body.Close()` with `drainAndClose(resp.Body, "runs response body")`.
  - In `pollWorkflowJobs` (lines 555-558): replace with `drainAndClose(resp.Body, "jobs response body")`.
  - In `fetchDockerContainers` (lines 942-945): replace with `drainAndClose(resp.Body, "docker response body")`.
  - In `dashboardDataHandler` (line 1229): replace `_ = json.NewEncoder(w).Encode(resp)` with `writeJSON(w, r, http.StatusOK, resp)`.
  - In `factsHandler` (lines 1263, 1274-1275, 1281): replace json encodes with `writeJSON` and body drain/close with `drainAndClose(resp.Body, "brain facts response body")`.
  - In `healthHandler` (line 1505): replace `_, _ = w.Write([]byte("OK"))` with `writeResponse(w, r, []byte("OK"))`.
  - In `assetHandler` (line 1652): replace `_, _ = w.Write(asset.Data)` with `writeResponse(w, r, asset.Data)`.

- [ ] **Step 3: Evict `dashboard` from `.golangci.yml` quarantine**
Remove `- path: ^dashboard/` from `issues.exclude-rules`.

- [ ] **Step 4: Verify `dashboard` linter and test coverage**
Run: `golangci-lint run --path-prefix="dashboard/" --config .golangci.yml ./...`
Run: `go test -v -cover ./...` inside `dashboard/`
Expected: 0 linter errors, statement coverage ≥ 95.0%.

- [ ] **Step 5: Commit**
```bash
git add dashboard/ .golangci.yml
git commit -m "feat(lint): remediate swallowed errors in dashboard and evict from quarantine"
```

---

### Task 2: Remediate Swallowed Errors in `brain/pkg/config`, `brain/pkg/delivery`, `brain/pkg/env`, and `brain/pkg/gitsync`

**Files:**
- Modify: `brain/pkg/config/config.go:672, 1063, 1101-1113`
- Modify: `brain/pkg/delivery/delivery.go:256, 266`, `brain/pkg/delivery/media.go:439`
- Modify: `brain/pkg/env/provisioner.go:121-137`, `brain/pkg/env/rules.go:82-163`, `brain/pkg/env/skills.go:78-114`
- Modify: `brain/pkg/gitsync/gitsync.go:137, 169, 200, 211, 247`
- Test: `brain/pkg/config/config_test.go`
- Test: `brain/pkg/delivery/delivery_test.go`
- Test: `brain/pkg/env/provisioner_test.go`
- Test: `brain/pkg/gitsync/gitsync_test.go`

**Interfaces:**
- Consumes: `os`, `execGit`, `discordgo.Session`
- Produces: checked file ops, non-blocking typing indicators, checked branch tracking/safe.directory config

- [ ] **Step 1: Remediate 5 violations in `brain/pkg/config`**
  - Line 672: `if writeErr := writeAtomicFile(target, string(rawData)); writeErr != nil { log.Printf("[Config] Warning writing fallback: %v", writeErr) }`
  - Line 1063: `if closeErr := f.Close(); closeErr != nil { log.Printf("[Config] Warning closing file: %v", closeErr) }`
  - Line 1101, 1113: Safely check `rmErr := os.Remove(...)` filtering `os.ErrNotExist`.
  - Line 1105: `if closeErr := f.Close(); closeErr != nil { log.Printf(...) }`

- [ ] **Step 2: Remediate 3 violations in `brain/pkg/delivery`**
  - `delivery.go:256, 266`: `if err := s.ChannelTyping(channelID); err != nil { log.Printf("[Delivery] Warning sending channel typing: %v", err) }`
  - `media.go:439`: `defer func() { if err := f.Close(); err != nil { log.Printf("[Delivery] Warning closing media file: %v", err) } }()`

- [ ] **Step 3: Remediate 11 violations in `brain/pkg/env`**
  - `provisioner.go:122, 126, 135`: Handle `os.Remove(tmpName)`, `f.Close()`, and fallback `os.Remove(targetPath)`.
  - `rules.go:82, 151, 162`: Handle `p.writeAtomic` error logging on LKGC write and secondary rules sync; safely check `os.Remove(stale)` filtering `os.ErrNotExist`.
  - `skills.go:78, 80, 88, 89, 114`: Handle `os.Remove` and `os.Rename` with explicit error logging filtering `os.ErrNotExist`.

- [ ] **Step 4: Remediate ALL 5 violations in `brain/pkg/gitsync`**
  - Lines 137, 169, 247: Inspect `execGit(ctx, "", BuildSafeDirectoryArgs()...)` error with warning log.
  - Line 200: Inspect `execGit(ctx, repoPath, BuildBranchRenameArgs(targetBranch)...)` error with warning log.
  - Line 211: Inspect `execGit(ctx, repoPath, BuildBranchTrackArgs("origin", targetBranch)...)` error with warning log.

- [ ] **Step 5: Verify test coverage across config, delivery, env, gitsync**
Run: `go test -v -cover ./pkg/config/... ./pkg/delivery/... ./pkg/env/... ./pkg/gitsync/...` inside `brain/`
Expected: PASS with statement coverage ≥ 95.0% on every package.

- [ ] **Step 6: Commit**
```bash
git add brain/pkg/config/ brain/pkg/delivery/ brain/pkg/env/ brain/pkg/gitsync/
git commit -m "feat(lint): remediate swallowed errors in config, delivery, env, and gitsync"
```

---

### Task 3: Remediate Swallowed Errors in `brain/pkg/memory`, `brain/pkg/session`, and `brain/pkg/watcher`

**Files:**
- Modify: `brain/pkg/memory/extractor.go:196, 254, 255`, `brain/pkg/memory/ollama.go:234`
- Modify: `brain/pkg/session/session.go:436, 451, 656, 749, 788, 896`
- Modify: `brain/pkg/watcher/watcher.go:194, 222`
- Test: `brain/pkg/memory/memory_test.go`
- Test: `brain/pkg/session/session_test.go`
- Test: `brain/pkg/watcher/watcher_test.go`

**Interfaces:**
- Consumes: `factStore`, `fsnotify`, `io.Closer`
- Produces: robust error logging, safe resource closures, tested memory error branches

- [ ] **Step 1: Write unit tests for error paths in `brain/pkg/memory/memory_test.go`**
  - Add tests exercising watermark update warning branch and Ollama response body close warning branch to maintain `pkg/memory` statement coverage above the 95.0% floor (targeting ≥ 95.8%).

- [ ] **Step 2: Remediate 4 swallowed errors in `brain/pkg/memory`**
  - In `extractor.go:196, 254, 255`: Inspect `factStore.UpdateConversationFactWatermark` and `UpdateConversationFactExtractedAt` errors with warning logs.
  - In `ollama.go:234`: Replace `defer func() { _ = resp.Body.Close() }()` with inspected close warning.

- [ ] **Step 3: Remediate 6 swallowed errors in `brain/pkg/session`**
  - In `session.go:436, 451, 656, 749, 788`: Replace `_ = f.Close()` with inspected error warning closures.
  - In `session.go:896`: Inspect `os.RemoveAll(d)` error with warning log filtering `os.ErrNotExist`.
  - In `session_test.go`: Add test verifying close warning and cleanup edge cases to protect 95.3% statement coverage.

- [ ] **Step 4: Remediate 2 swallowed errors in `brain/pkg/watcher`**
  - In `watcher.go:194`: Replace `_ = w.Close()` with inspected error log.
  - In `watcher.go:222`: Inspect `_ = w.AddRecursive(event.Name)` error with warning log.

- [ ] **Step 5: Verify test coverage across memory, session, watcher**
Run: `go test -v -cover ./pkg/memory/... ./pkg/session/... ./pkg/watcher/...` inside `brain/`
Expected: PASS with statement coverage ≥ 95.0% on each package.

- [ ] **Step 6: Commit**
```bash
git add brain/pkg/memory/ brain/pkg/session/ brain/pkg/watcher/
git commit -m "feat(lint): remediate swallowed errors in memory, session, and watcher"
```

---

### Task 4: Remediate Swallowed Errors in `brain/pkg/db` & Narrow Quarantine

**Files:**
- Modify: `brain/pkg/db/db.go:123, 143`
- Modify: `brain/pkg/db/facts.go:226, 255, 300, 426, 470, 510, 612, 854, 865, 883, 894`
- Modify: `brain/pkg/db/messages.go:213, 363, 414`
- Modify: `brain/pkg/db/schedules.go:97, 153, 202, 250, 283, 497`
- Modify: `brain/pkg/db/schema.go:125, 131, 142-160`
- Modify: `brain/pkg/db/sessions.go:316, 359`
- Modify: `brain/pkg/db/sql_store.go:77`
- Modify: `brain/pkg/db/tasks.go:142`
- Test: `brain/pkg/db/db_test.go`
- Modify: `.golangci.yml:56-63`

**Interfaces:**
- Consumes: `database/sql`, `io.Closer`
- Produces: `closeWarn(closer io.Closer, name string)`, `rollbackWarn(tx *sql.Tx, name string)`

- [ ] **Step 1: Add `closeWarn` and `rollbackWarn` in `brain/pkg/db`**
```go
func closeWarn(closer io.Closer, name string) {
	if closer != nil {
		if err := closer.Close(); err != nil {
			log.Printf("[DB] Warning closing %s: %v", name, err)
		}
	}
}

func rollbackWarn(tx *sql.Tx, name string) {
	if tx != nil {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Printf("[DB] Warning rolling back %s: %v", name, err)
		}
	}
}
```

- [ ] **Step 2: Add unit tests in `brain/pkg/db/db_test.go` covering `closeWarn` and `rollbackWarn`**
Test `closeWarn` with nil and error, and `rollbackWarn` with nil, `sql.ErrTxDone` (suppressed), and genuine error.

- [ ] **Step 3: Remediate 40 violations in `brain/pkg/db`**
  - In `db.go:123, 143`: replace `_ = database.Close()` with `closeWarn`.
  - In `facts.go`, `messages.go`, `schedules.go`, `tasks.go`: replace all deferred `_ = rows.Close()` with `defer closeWarn(rows, "rows")`.
  - In `schedules.go:153`, `sql_store.go:77`: replace `defer func() { _ = tx.Rollback() }()` with `defer rollbackWarn(tx, "tx")`.
  - In `facts.go:510`, `sessions.go:316, 359`: safely inspect error return from `GetMaxMessageRowID` and `GetSessionID`.
  - In `facts.go:854, 865, 883, 894`: safely inspect `RowsAffected()`.
  - In `schema.go:125, 131, 142-160`: safely close migration connection with `closeWarn`, check advisory unlock, and log column migration notices.

- [ ] **Step 4: Narrow `.golangci.yml` quarantine block for `brain`**
Update `.golangci.yml` `issues.exclude-rules` to replace `path: ^brain/` with:
```yaml
    # 4. Burndown Quarantine for errcheck (Phase 3: brain core engines)
    - path: ^brain/(main|funnel)\.go$
      linters:
        - errcheck
    - path: ^brain/pkg/(queue|runner)/
      linters:
        - errcheck
```

- [ ] **Step 5: Verify all Foundation packages and linter**
Run: `golangci-lint run --path-prefix="brain/" --config ../.golangci.yml ./...` inside `brain/`
Expected: 0 errors across all 13 foundation packages (`classifier`, `config`, `db`, `delivery`, `env`, `gitsync`, `memory`, `metrics`, `notifier`, `sanitizer`, `scheduler`, `session`, `watcher`).

- [ ] **Step 6: Commit**
```bash
git add brain/pkg/db/ .golangci.yml
git commit -m "feat(lint): remediate errors in brain/pkg/db and narrow brain quarantine"
```

---

### Task 5: Full Monorepo Verification & Girl Gang Audit

**Files:**
- Audit: Entire git diff on `feat/lint-phase-2-dashboard-brain-foundation`

- [ ] **Step 1: Run full `./scripts/verify.sh`**
Verify syntax, clean-room Go tests, isolated linters, frontend tests, and hygiene pass with exit code 0.

- [ ] **Step 2: Run `./scripts/check-coverage.sh --check`**
Verify all packages satisfy the uniform ≥ 95.0% statement coverage floor.

- [ ] **Step 3: Dispatch Girl Gang Subagent Review Panel**
Adversarial Systems Critic and Go Code Reviewer audit the diff for Tier A/B/C invariants, error regression safety, and coverage margins.

- [ ] **Step 4: Push branch & create PR**
Push `feat/lint-phase-2-dashboard-brain-foundation` to origin and open PR.
