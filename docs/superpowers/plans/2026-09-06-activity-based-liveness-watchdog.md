# Activity-Based Liveness Watchdog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement an activity-based liveness watchdog in `aerial-brain` that tracks runner progress via live stderr streaming and transcript mtime/size deltas, eliminating false-positive timeout kills on long-running tasks and short-circuiting blind queue retries on genuine stalls.

**Architecture:** Replace static wall-clock `context.WithTimeout` with `RunAgyWithWatchdog` in `brain/pkg/runner`, which monitors a thread-safe `ActivityWriter` and polls `transcript.jsonl` every 3s. `ClassifyError` intercepts watchdog inactivity kills, and `WorkerPool.processBurst` in `brain/pkg/queue` breaks immediately from the retry loop on fatal watchdog stalls.

**Tech Stack:** Go 1.24, modernc SQLite / pgx Postgres, POSIX process groups (`syscall.SysProcAttr{Setpgid: true}`), Prometheus metrics.

**Spec:** `docs/superpowers/specs/2026-09-06-activity-based-liveness-watchdog-design.md`

## Global Constraints
• Pure Go standard library for watchdog & concurrency (`sync`, `sync/atomic`, `time`, `os`, `os/exec`, `syscall`, `io`).
• All code changes must pass `scripts/verify.sh` with 100% green tests and `>= 90.0%` statement coverage across all packages.
• Zero Markdown tables in conversational output; clean GitHub web URLs for links.
• Zero UTF-8 byte order marks (BOM) in source files.

---

### Task 1: Implement `ActivityWriter` with Dynamic Session Sniffing & Tests

**Files:**
- Modify: `brain/pkg/runner/runner.go`
- Modify: `brain/pkg/runner/runner_test.go`

**Interfaces:**
- Produces: `ActivityWriter`, `NewActivityWriter(initialSessionID string) *ActivityWriter`, `(w *ActivityWriter) Write(p []byte) (int, error)`, `(w *ActivityWriter) LastActivity() time.Time`, `(w *ActivityWriter) SessionID() string`, `(w *ActivityWriter) String() string`

- [ ] **Step 1: Write failing unit test for `ActivityWriter`**

In `brain/pkg/runner/runner_test.go`, add tests for `ActivityWriter` concurrency, atomic timestamps, and stream-based session discovery matching `reUpdateStream`:

```go
func TestActivityWriter_ThreadSafetyAndSessionDiscovery(t *testing.T) {
	w := NewActivityWriter("")
	if w.SessionID() != "" {
		t.Errorf("expected empty initial session ID, got %q", w.SessionID())
	}
	startNano := w.LastActivity().UnixNano()

	// Write session start log chunk
	chunk := []byte("INFO: Starting conversation update stream for 12345-abcd-6789\n")
	n, err := w.Write(chunk)
	if err != nil || n != len(chunk) {
		t.Fatalf("Write failed: n=%d, err=%v", n, err)
	}

	if w.SessionID() != "12345-abcd-6789" {
		t.Errorf("expected session ID 12345-abcd-6789, got %q", w.SessionID())
	}
	if w.LastActivity().UnixNano() < startNano {
		t.Errorf("expected lastActivity to advance")
	}
	if w.String() != string(chunk) {
		t.Errorf("expected buffered output %q, got %q", string(chunk), w.String())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestActivityWriter_ThreadSafetyAndSessionDiscovery ./brain/pkg/runner`
Expected: FAIL (types and methods undefined).

- [ ] **Step 3: Implement `ActivityWriter` in `runner.go`**

In `brain/pkg/runner/runner.go`:

```go
type ActivityWriter struct {
	mu           sync.Mutex
	buf          bytes.Buffer
	lastActivity atomic.Int64 // UnixNano
	sessionID    atomic.Pointer[string]
}

func NewActivityWriter(initialSessionID string) *ActivityWriter {
	w := &ActivityWriter{}
	w.lastActivity.Store(time.Now().UnixNano())
	if initialSessionID != "" {
		w.sessionID.Store(&initialSessionID)
	}
	return w
}

func (w *ActivityWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.lastActivity.Store(time.Now().UnixNano())

	w.mu.Lock()
	n, err := w.buf.Write(p)
	if w.sessionID.Load() == nil {
		if match := reUpdateStream.FindSubmatch(p); len(match) > 1 {
			sess := string(match[1])
			w.sessionID.Store(&sess)
		}
	}
	w.mu.Unlock()

	return n, err
}

func (w *ActivityWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *ActivityWriter) LastActivity() time.Time {
	return time.Unix(0, w.lastActivity.Load())
}

func (w *ActivityWriter) SessionID() string {
	ptr := w.sessionID.Load()
	if ptr == nil {
		return ""
	}
	return *ptr
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race -v -run TestActivityWriter_ThreadSafetyAndSessionDiscovery ./brain/pkg/runner`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/runner.go brain/pkg/runner/runner_test.go
git commit -m "feat(runner): implement thread-safe ActivityWriter with stream session discovery"
```

---

### Task 2: Implement `WatchdogOptions` and `RunAgyWithWatchdog`

**Files:**
- Modify: `brain/pkg/runner/runner.go`
- Modify: `brain/pkg/runner/runner_test.go`

**Interfaces:**
- Consumes: `ActivityWriter` from Task 1
- Produces: `WatchdogOptions`, `DefaultWatchdogOptions(timeoutMinutes int) WatchdogOptions`, `RunAgyWithWatchdog(ctx, agyBin, prompt, sessionID, apiKey, model string, opts WatchdogOptions) (stdout, stderr string, exitCode int, err error)`

- [ ] **Step 1: Write failing unit test for `RunAgyWithWatchdog`**

In `brain/pkg/runner/runner_test.go`:

```go
func TestRunAgyWithWatchdog_InactivityTimeout(t *testing.T) {
	ctx := context.Background()
	opts := WatchdogOptions{
		InactivityTimeout: 100 * time.Millisecond,
		MaxDuration:       2 * time.Second,
		PollInterval:      20 * time.Millisecond,
	}

	// Use sh command that sleeps silently longer than inactivity timeout
	stdout, stderr, exitCode, err := RunAgyWithWatchdog(
		ctx,
		"sh",
		"-c",
		"sleep 1",
		"",
		"",
		opts,
	)

	if exitCode == 0 {
		t.Errorf("expected non-zero exit code on inactivity timeout, got 0")
	}
	if !strings.Contains(stderr, "[watchdog]") || !strings.Contains(stderr, "inactivity timeout exceeded") {
		t.Errorf("expected [watchdog] inactivity timeout diagnosis in stderr, got: %s", stderr)
	}
	_ = stdout
	_ = err
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestRunAgyWithWatchdog_InactivityTimeout ./brain/pkg/runner`
Expected: FAIL (`RunAgyWithWatchdog` undefined).

- [ ] **Step 3: Implement `RunAgyWithWatchdog` and update `RunAgy`**

In `brain/pkg/runner/runner.go`:
Define `WatchdogOptions` and implement `RunAgyWithWatchdog`. Wire `RunAgy` to delegate to `RunAgyWithWatchdog` using `DefaultWatchdogOptions(timeoutMinutes)` to maintain backwards compatibility while adding the watchdog layer.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -v -run "TestRunAgy.*" ./brain/pkg/runner`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/runner.go brain/pkg/runner/runner_test.go
git commit -m "feat(runner): implement RunAgyWithWatchdog with dual-activity monitoring"
```

---

### Task 3: Overhaul `ClassifyError` for Watchdog Diagnoses

**Files:**
- Modify: `brain/pkg/runner/runner.go`
- Modify: `brain/pkg/runner/runner_test.go`

**Interfaces:**
- Produces: `IsInactivityTimeout(errDetail, stderr string) bool`, updated `ClassifyError(exitCode int, stdout, stderr string) (isFailure, isTransient, isSessionCorruption bool, errDetail string)`

- [ ] **Step 1: Write failing unit test for watchdog error classification**

In `brain/pkg/runner/runner_test.go`:

```go
func TestClassifyError_WatchdogInactivityNotTransient(t *testing.T) {
	stderr := "Starting conversation update stream for uuid-123\n[watchdog] inactivity timeout exceeded (5m without output or transcript update)"
	isFailure, isTransient, isSessionCorruption, errDetail := ClassifyError(-1, "", stderr)

	if !isFailure {
		t.Errorf("expected isFailure=true")
	}
	if isTransient {
		t.Errorf("expected isTransient=false for inactivity watchdog stall")
	}
	if isSessionCorruption {
		t.Errorf("expected isSessionCorruption=false")
	}
	if !strings.Contains(errDetail, "inactivity timeout exceeded") {
		t.Errorf("expected errDetail to contain inactivity timeout, got %q", errDetail)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestClassifyError_WatchdogInactivityNotTransient ./brain/pkg/runner`
Expected: FAIL (currently categorized as `isTransient=true` due to `"timeout"` keyword).

- [ ] **Step 3: Implement watchdog sentinel check in `ClassifyError`**

In `brain/pkg/runner/runner.go`:
Check `strings.Contains(combined, "[watchdog]")` and `strings.Contains(combined, "inactivity timeout exceeded")` **before** scanning `transientKeywords`. If found, set `isFailure = true`, `isTransient = false`, `isSessionCorruption = false`, and return cleanly.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -v -run "TestClassifyError.*" ./brain/pkg/runner`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/runner.go brain/pkg/runner/runner_test.go
git commit -m "fix(runner): classify watchdog inactivity timeouts as non-transient stalls"
```

---

### Task 4: Integrate Watchdog & Short-Circuit Queue Retries in `WorkerPool`

**Files:**
- Modify: `brain/pkg/queue/queue.go`
- Modify: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes: `RunAgyWithWatchdog`, `ClassifyError`, `IsInactivityTimeout` from Task 2 & 3
- Produces: `WorkerPool.processBurst` with hard ceiling `--print-timeout`, fatal stall short-circuiting, and metrics recording

- [ ] **Step 1: Write failing unit test in `queue_test.go` for watchdog failure single-attempt exit**

In `brain/pkg/queue/queue_test.go`:
Add a test verifying that when `RunnerFunc` returns a watchdog inactivity timeout error, `WorkerPool.processBurst` terminates on Attempt 1 and does not execute retry attempts 2 or 3.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestQueue_WatchdogInactivityFatalNoRetry ./brain/pkg/queue`
Expected: FAIL.

- [ ] **Step 3: Update `queue.go` to short-circuit on watchdog inactivity kills**

In `brain/pkg/queue/queue.go`:
When `errDetail` contains `[watchdog]` or inactivity timeout:
1. `db.RotateSessionID(p.cfg.DB, threadID, "")`
2. Update message status to `db.StatusFailed` with the watchdog diagnostic.
3. Send a clean inactivity alert to Discord.
4. `break` from the retry loop immediately.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race -v -run "TestQueue_.*" ./brain/pkg/queue`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/queue.go brain/pkg/queue/queue_test.go
git commit -m "feat(queue): short-circuit retry loop on fatal watchdog inactivity stalls"
```

---

### Task 5: Full Monorepo Verification & Coverage Gate

**Files:**
- Run: `scripts/verify.sh`
- Run: `scripts/check-coverage.sh`

- [ ] **Step 1: Run full verification suite**

Run: `bash scripts/verify.sh`
Expected: All formatting, linting, BOM checks, and Go unit test suites pass with statement coverage `>= 90.0%`.

- [ ] **Step 2: Commit any remaining cleanups**

```bash
git add -A
git commit -m "chore: verify monorepo tests and coverage thresholds"
```
