# Persistent Streaming Daemon Worker Pool & 24-Hour Inactivity Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a persistent streaming Antigravity daemon worker pool in `aerial-brain` with deterministic in-memory task tracking, unified 24-hour inactivity pruning, bounded safety ceilings (40 max daemons) with host memory-pressure LRU eviction, PostgreSQL crash-recovery task mirroring, and serialized turn mailboxes to eliminate the Option B yield trap and per-turn process spawning overhead.

**Architecture:** Replace per-turn `agy -p` process invocation with persistent streaming daemons (`agy --conversation <sessionID> --input-format stream-json --output-format stream-json`) managed by a `DaemonPool`. Daemons run with dedicated POSIX process groups (`syscall.SysProcAttr{Setpgid: true}`), serialize prompts via stdin using `{"event":"user","message":{"content":prompt}}\n`, frame stdout streams turn-by-turn on `event: "result"` while resetting per-turn buffers, deterministically track background tasks via an idempotent `TaskTracker` set, monitor background task logs on disk (`.system_generated/tasks/task-<N>.log`), deregister completed tasks from `TaskTracker`, and enqueue synthetic `TaskCompletionResumePrompt` turns through the thread worker queue to eliminate stdin write races and `scopeLock` deadlocks.

**Tech Stack:** Go 1.24, Linux POSIX process groups (`syscall.SysProcAttr{Setpgid: true}`), NDJSON streaming protocols, pgx / PostgreSQL 16, Prometheus metrics, Docsify.

**Spec:** `docs/superpowers/specs/2026-09-19-persistent-streaming-daemon-and-inactivity-tuning-design.md`

## Global Constraints
• Pure Go standard library for concurrency, process management, and streaming (`sync`, `time`, `os`, `os/exec`, `syscall`, `io`, `bufio`, `encoding/json`).
• Zero artificial concurrency bottlenecks; default bounded safety ceiling of 40 daemons (`max_concurrent_daemons`) with LRU eviction on memory pressure (< 15% available host RAM).
• Strict adherence to empirical `agy` NDJSON wire format (`{"event":"user","message":{"content":prompt}}\n`); no fictional `task_start`/`task_end` events.
• All code changes must pass `scripts/verify.sh` with 100% green tests and `>= 90.0%` statement coverage across all packages.
• Zero Markdown tables in conversational output; clean GitHub web URLs for links.
• Zero UTF-8 byte order marks (BOM) in source files.

---

### Task 1: Implement `TaskTracker` Concurrent Idempotent Task Set

**Files:**
- Modify: `brain/pkg/session/session.go` (define `TaskMetadata`)
- Create: `brain/pkg/runner/task_tracker.go`
- Create: `brain/pkg/runner/task_tracker_test.go`

**Interfaces:**
- Produces:
  ```go
  // in pkg/session/session.go:
  type TaskMetadata struct {
      TaskID      string    `json:"task_id"`
      ToolName    string    `json:"tool_name"`
      CommandLine string    `json:"command_line,omitempty"`
      StartedAt   time.Time `json:"started_at"`
  }

  // in pkg/runner/task_tracker.go:
  type TaskMetadata = session.TaskMetadata

  type TaskTracker struct {
      mu    sync.RWMutex
      tasks map[string]TaskMetadata
  }

  func NewTaskTracker() *TaskTracker
  func (t *TaskTracker) Add(meta TaskMetadata) bool
  func (t *TaskTracker) Remove(taskID string) bool
  func (t *TaskTracker) Has(taskID string) bool
  func (t *TaskTracker) ActiveCount() int
  func (t *TaskTracker) ActiveTasks() []TaskMetadata
  func (t *TaskTracker) PruneExpired(maxAge time.Duration) []string
  ```

- [ ] **Step 1: Write failing unit tests for `TaskTracker`**

Create `brain/pkg/runner/task_tracker_test.go`:
```go
package runner

import (
	"sync"
	"testing"
	"time"
)

func TestTaskTracker_IdempotentOperations(t *testing.T) {
	tracker := NewTaskTracker()
	if tracker.ActiveCount() != 0 {
		t.Fatalf("expected 0 active tasks, got %d", tracker.ActiveCount())
	}

	meta := TaskMetadata{
		TaskID:      "task-123",
		ToolName:    "run_command",
		CommandLine: "sleep 10",
		StartedAt:   time.Now(),
	}

	// First add should succeed
	if !tracker.Add(meta) {
		t.Errorf("expected true on initial Add")
	}
	if tracker.ActiveCount() != 1 {
		t.Errorf("expected 1 active task, got %d", tracker.ActiveCount())
	}
	if !tracker.Has("task-123") {
		t.Errorf("expected tracker to have task-123")
	}

	// Idempotent duplicate add should not duplicate or error
	if tracker.Add(meta) {
		t.Errorf("expected false on duplicate Add")
	}
	if tracker.ActiveCount() != 1 {
		t.Errorf("expected active count to remain 1, got %d", tracker.ActiveCount())
	}

	// Remove existing
	if !tracker.Remove("task-123") {
		t.Errorf("expected true on Remove of existing task")
	}
	if tracker.ActiveCount() != 0 {
		t.Errorf("expected 0 active tasks after remove, got %d", tracker.ActiveCount())
	}

	// Idempotent remove non-existent
	if tracker.Remove("task-123") {
		t.Errorf("expected false on Remove of already removed task")
	}
}

func TestTaskTracker_ConcurrentAccess(t *testing.T) {
	tracker := NewTaskTracker()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			taskID := "task-" + string(rune('a'+id))
			tracker.Add(TaskMetadata{TaskID: taskID, ToolName: "tool", StartedAt: time.Now()})
			_ = tracker.Has(taskID)
			_ = tracker.ActiveCount()
			_ = tracker.ActiveTasks()
			tracker.Remove(taskID)
		}(i)
	}

	wg.Wait()
	if tracker.ActiveCount() != 0 {
		t.Errorf("expected 0 tasks after concurrent add/remove, got %d", tracker.ActiveCount())
	}
}

func TestTaskTracker_PruneExpired(t *testing.T) {
	tracker := NewTaskTracker()
	now := time.Now()

	tracker.Add(TaskMetadata{TaskID: "fresh", ToolName: "tool", StartedAt: now})
	tracker.Add(TaskMetadata{TaskID: "stale", ToolName: "tool", StartedAt: now.Add(-3 * time.Hour)})

	pruned := tracker.PruneExpired(2 * time.Hour)
	if len(pruned) != 1 || pruned[0] != "stale" {
		t.Fatalf("expected ['stale'] pruned, got %v", pruned)
	}
	if tracker.Has("stale") {
		t.Errorf("expected stale task to be removed")
	}
	if !tracker.Has("fresh") {
		t.Errorf("expected fresh task to remain")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/runner -run TestTaskTracker`
Expected: FAIL (undefined `TaskTracker`, `NewTaskTracker`, etc.)

- [ ] **Step 3: Implement `TaskMetadata` in `session.go` and `TaskTracker` in `task_tracker.go`**

1. In `brain/pkg/session/session.go`, define `TaskMetadata`:
```go
// TaskMetadata contains execution details for a running background task.
type TaskMetadata struct {
	TaskID      string    `json:"task_id"`
	ToolName    string    `json:"tool_name"`
	CommandLine string    `json:"command_line,omitempty"`
	StartedAt   time.Time `json:"started_at"`
}
```

2. Create `brain/pkg/runner/task_tracker.go` aliasing `session.TaskMetadata`:
```go
package runner

import (
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/session"
)

// TaskMetadata aliases session.TaskMetadata to prevent circular dependencies between runner and session.
type TaskMetadata = session.TaskMetadata

// TaskTracker provides thread-safe, idempotent tracking of active background tasks.
type TaskTracker struct {
	mu    sync.RWMutex
	tasks map[string]TaskMetadata
}

// NewTaskTracker constructs an empty, initialized TaskTracker.
func NewTaskTracker() *TaskTracker {
	return &TaskTracker{
		tasks: make(map[string]TaskMetadata),
	}
}

// Add idempotently registers a background task. Returns true if newly added, false if already tracked.
func (t *TaskTracker) Add(meta TaskMetadata) bool {
	if meta.TaskID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.tasks[meta.TaskID]; exists {
		return false
	}
	if meta.StartedAt.IsZero() {
		meta.StartedAt = time.Now()
	}
	t.tasks[meta.TaskID] = meta
	return true
}

// Remove idempotently removes a background task. Returns true if existed and removed, false otherwise.
func (t *TaskTracker) Remove(taskID string) bool {
	if taskID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.tasks[taskID]; !exists {
		return false
	}
	delete(t.tasks, taskID)
	return true
}

// Has reports whether the specified task is currently tracked.
func (t *TaskTracker) Has(taskID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.tasks[taskID]
	return exists
}

// ActiveCount returns the current count of tracked background tasks.
func (t *TaskTracker) ActiveCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.tasks)
}

// ActiveTasks returns a snapshot slice of all currently tracked tasks.
func (t *TaskTracker) ActiveTasks() []TaskMetadata {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]TaskMetadata, 0, len(t.tasks))
	for _, meta := range t.tasks {
		out = append(out, meta)
	}
	return out
}

// PruneExpired removes tasks whose runtime has exceeded maxAge, returning the pruned task IDs.
func (t *TaskTracker) PruneExpired(maxAge time.Duration) []string {
	if maxAge <= 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	var pruned []string
	for id, meta := range t.tasks {
		if now.Sub(meta.StartedAt) > maxAge {
			pruned = append(pruned, id)
			delete(t.tasks, id)
		}
	}
	return pruned
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/runner -run TestTaskTracker`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/session/session.go brain/pkg/runner/task_tracker.go brain/pkg/runner/task_tracker_test.go
git commit -m "feat(runner,session): implement TaskMetadata in session and idempotent TaskTracker in runner"
```

---

### Task 2: Implement Persistent Streaming Daemon Runner & Wire Protocol

**Files:**
- Create: `brain/pkg/runner/daemon.go`
- Create: `brain/pkg/runner/daemon_test.go`

**Interfaces:**
- Consumes: `TaskTracker`, `TaskMetadata` from Task 1
- Produces:
  ```go
  type DaemonState string
  const (
      StateStarting     DaemonState = "STARTING"
      StateReady        DaemonState = "READY"
      StateExecuting    DaemonState = "EXECUTING"
      StateYieldWaiting DaemonState = "YIELD_WAITING"
      StateClosed       DaemonState = "CLOSED"
  )

  type DaemonConfig struct {
      SessionID      string
      ThreadID       string
      Model          string
      AgyBin         string
      Cwd            string
      Env            []string
      GeminiHomeDir  string
      Timeout        time.Duration
  }

  type TurnResult struct {
      Content       string
      Usage         AgyUsage
      ActiveTasks   []TaskMetadata
      IsYieldTrap   bool
      ExitCode      int
      Duration      time.Duration
  }

  type Daemon struct { ... }
  func StartDaemon(ctx context.Context, cfg DaemonConfig) (*Daemon, error)
  func (d *Daemon) ExecuteTurn(ctx context.Context, prompt string) (*TurnResult, error)
  func (d *Daemon) Close() error
  func (d *Daemon) State() DaemonState
  func (d *Daemon) SessionID() string
  func (d *Daemon) TaskTracker() *TaskTracker
  func (d *Daemon) SetDirty(dirty bool)
  func (d *Daemon) IsDirty() bool
  func (d *Daemon) LastUsed() time.Time
  ```

- [ ] **Step 1: Write failing unit tests for `Daemon`**

Create `brain/pkg/runner/daemon_test.go`:
```go
package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDaemon_LifecycleAndTurnExecution(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	script := `#!/bin/sh
echo '{"event":"init","init":{"tools":["run_command"]}}'
while IFS= read -r line; do
  echo '{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"tool","tool_name":"run_command","tool_info":{"parameters":{"CommandLine":"sleep 20"}}}}'
  echo '{"event":"step_update","step_update":{"step_index":2,"state":"DONE","step_type":"tool","tool_name":"run_command","tool_output":"Tool is running as a background task with task id: test-task-42"}}'
  echo '{"event":"step_update","step_update":{"step_index":3,"state":"DONE","step_type":"agent_response"}}'
  echo '{"event":"result","content":"Turn execution complete","usage":{"input_tokens":100,"output_tokens":50,"total_tokens":150}}'
done
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock agy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := DaemonConfig{
		SessionID: "sess-1234",
		ThreadID:  "thread-5678",
		AgyBin:    mockBin,
		Cwd:       tempDir,
		Timeout:   5 * time.Second,
	}

	daemon, err := StartDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer daemon.Close()

	if daemon.State() != StateReady {
		t.Errorf("expected state READY, got %s", daemon.State())
	}

	// Execute Turn 1
	res, err := daemon.ExecuteTurn(ctx, "Hello world turn 1")
	if err != nil {
		t.Fatalf("ExecuteTurn failed: %v", err)
	}
	if res.Content != "Turn execution complete" {
		t.Errorf("expected content 'Turn execution complete', got %q", res.Content)
	}
	if res.Usage.TotalTokens != 150 {
		t.Errorf("expected 150 total tokens, got %d", res.Usage.TotalTokens)
	}
	if !daemon.TaskTracker().Has("test-task-42") {
		t.Errorf("expected TaskTracker to track test-task-42 from step_update")
	}
	if !res.IsYieldTrap {
		t.Errorf("expected IsYieldTrap=true because test-task-42 was active")
	}

	// Execute Turn 2 (verifying persistent stream is still alive and per-turn buffer was reset)
	res2, err := daemon.ExecuteTurn(ctx, "Hello world turn 2")
	if err != nil {
		t.Fatalf("ExecuteTurn 2 failed: %v", err)
	}
	if res2.Content != "Turn execution complete" {
		t.Errorf("expected content 'Turn execution complete', got %q", res2.Content)
	}

	// Close cleanly
	if err := daemon.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if daemon.State() != StateClosed {
		t.Errorf("expected state CLOSED, got %s", daemon.State())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/runner -run TestDaemon`
Expected: FAIL (undefined `Daemon`, `StartDaemon`, etc.)

- [ ] **Step 3: Implement `Daemon` in `brain/pkg/runner/daemon.go`**

Create `brain/pkg/runner/daemon.go`:
```go
package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	reBackgroundTaskStarted = regexp.MustCompile(`(?i)Tool is running as a background task with task id:\s*([^\s\r\n]+)`)
)

type DaemonState string

const (
	StateStarting     DaemonState = "STARTING"
	StateReady        DaemonState = "READY"
	StateExecuting    DaemonState = "EXECUTING"
	StateYieldWaiting DaemonState = "YIELD_WAITING"
	StateClosed       DaemonState = "CLOSED"
)

type DaemonConfig struct {
	SessionID     string
	ThreadID      string
	Model         string
	AgyBin        string
	Cwd           string
	Env           []string
	GeminiHomeDir string
	Timeout       time.Duration
}

type TurnResult struct {
	Content     string
	Usage       AgyUsage
	ActiveTasks []TaskMetadata
	IsYieldTrap bool
	ExitCode    int
	Duration    time.Duration
}

type Daemon struct {
	cfg         DaemonConfig
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      *bufio.Reader
	stderrBuf   *ActivityWriter
	taskTracker *TaskTracker
	state       DaemonState
	mu          sync.Mutex
	dirty       bool
	lastUsed    time.Time
}

func StartDaemon(ctx context.Context, cfg DaemonConfig) (*Daemon, error) {
	if cfg.AgyBin == "" {
		cfg.AgyBin = "agy"
	}

	args := []string{
		"--input-format", "stream-json",
		"--output-format", "stream-json",
	}
	if cfg.SessionID != "" {
		args = append(args, "--conversation", cfg.SessionID)
	}
	if cfg.Model != "" {
		args = append(args, "--model", cfg.Model)
	}

	cmd := exec.Command(cfg.AgyBin, args...)
	if cfg.Cwd != "" {
		cmd.Dir = cfg.Cwd
	}
	cmd.Env = append(cmd.Environ(), cfg.Env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to open stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdinPipe.Close()
		return nil, fmt.Errorf("failed to open stdout pipe: %w", err)
	}

	stderrWriter := NewActivityWriter(cfg.SessionID)
	cmd.Stderr = stderrWriter

	if err := cmd.Start(); err != nil {
		_ = stdinPipe.Close()
		_ = stdoutPipe.Close()
		return nil, fmt.Errorf("failed to start agy daemon: %w", err)
	}

	d := &Daemon{
		cfg:         cfg,
		cmd:         cmd,
		stdin:       stdinPipe,
		stdout:      bufio.NewReader(stdoutPipe),
		stderrBuf:   stderrWriter,
		taskTracker: NewTaskTracker(),
		state:       StateStarting,
		lastUsed:    time.Now(),
	}

	d.mu.Lock()
	d.state = StateReady
	d.mu.Unlock()

	return d, nil
}

type streamInputPayload struct {
	Event   string             `json:"event"`
	Message streamInputMessage `json:"message"`
}

type streamInputMessage struct {
	Content string `json:"content"`
}

func (d *Daemon) ExecuteTurn(ctx context.Context, prompt string) (*TurnResult, error) {
	d.mu.Lock()
	if d.state == StateClosed {
		d.mu.Unlock()
		return nil, errors.New("cannot execute turn on closed daemon")
	}
	d.state = StateExecuting
	d.lastUsed = time.Now()
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		if d.state != StateClosed {
			if d.taskTracker.ActiveCount() > 0 {
				d.state = StateYieldWaiting
			} else {
				d.state = StateReady
			}
		}
		d.lastUsed = time.Now()
		d.mu.Unlock()
	}()

	start := time.Now()

	wireMsg := streamInputPayload{
		Event: "user",
		Message: streamInputMessage{
			Content: prompt,
		},
	}
	encoded, err := json.Marshal(wireMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal turn prompt: %w", err)
	}
	encoded = append(encoded, '\n')

	if _, err := d.stdin.Write(encoded); err != nil {
		return nil, fmt.Errorf("failed to write prompt to daemon stdin: %w", err)
	}

	var turnContent strings.Builder
	var turnUsage AgyUsage
	var lastToolName string
	var lastToolCmd string

	for {
		line, readErr := d.stdout.ReadString('\n')
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil, fmt.Errorf("daemon stdout closed unexpectedly (EOF): %s", d.stderrBuf.String())
			}
			return nil, fmt.Errorf("error reading daemon stream: %w", readErr)
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}

		event, _ := raw["event"].(string)
		switch event {
		case "step_update":
			stepUpdate, _ := raw["step_update"].(map[string]interface{})
			if stepUpdate != nil {
				toolName, _ := stepUpdate["tool_name"].(string)
				if toolName != "" {
					lastToolName = toolName
				}
				if toolInfo, ok := stepUpdate["tool_info"].(map[string]interface{}); ok {
					if params, ok := toolInfo["parameters"].(map[string]interface{}); ok {
						if cmd, ok := params["CommandLine"].(string); ok {
							lastToolCmd = cmd
						}
					}
				}

				if toolOut, ok := stepUpdate["tool_output"].(string); ok {
					if m := reBackgroundTaskStarted.FindStringSubmatch(toolOut); len(m) > 1 {
						taskID := m[1]
						d.taskTracker.Add(TaskMetadata{
							TaskID:      taskID,
							ToolName:    lastToolName,
							CommandLine: lastToolCmd,
							StartedAt:   time.Now(),
						})
					}
				}
			}

		case "result":
			if c, ok := raw["content"].(string); ok {
				turnContent.WriteString(c)
			}
			if u, ok := raw["usage"].(map[string]interface{}); ok {
				turnUsage = parseRawUsage(u)
			}
			activeTasks := d.taskTracker.ActiveTasks()
			isYield := len(activeTasks) > 0

			return &TurnResult{
				Content:     turnContent.String(),
				Usage:       turnUsage,
				ActiveTasks: activeTasks,
				IsYieldTrap: isYield,
				ExitCode:    0,
				Duration:    time.Since(start),
			}, nil
		}
	}
}

func parseRawUsage(u map[string]interface{}) AgyUsage {
	var usage AgyUsage
	if v, ok := u["input_tokens"].(float64); ok {
		usage.InputTokens = int(v)
	}
	if v, ok := u["output_tokens"].(float64); ok {
		usage.OutputTokens = int(v)
	}
	if v, ok := u["thinking_tokens"].(float64); ok {
		usage.ThinkingTokens = int(v)
	}
	if v, ok := u["cache_read_tokens"].(float64); ok {
		usage.CacheReadTokens = int(v)
	}
	if v, ok := u["total_tokens"].(float64); ok {
		usage.TotalTokens = int(v)
	}
	return usage
}

func (d *Daemon) Close() error {
	d.mu.Lock()
	if d.state == StateClosed {
		d.mu.Unlock()
		return nil
	}
	d.state = StateClosed
	d.mu.Unlock()

	if d.stdin != nil {
		_ = d.stdin.Close()
	}

	if d.cmd != nil && d.cmd.Process != nil {
		pid := d.cmd.Process.Pid
		_ = syscall.Kill(-pid, syscall.SIGTERM)

		done := make(chan error, 1)
		go func() {
			done <- d.cmd.Wait()
		}()

		select {
		case err := <-done:
			return err
		case <-time.After(3 * time.Second):
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			return <-done
		}
	}
	return nil
}

func (d *Daemon) State() DaemonState       { d.mu.Lock(); defer d.mu.Unlock(); return d.state }
func (d *Daemon) SessionID() string       { return d.cfg.SessionID }
func (d *Daemon) TaskTracker() *TaskTracker { return d.taskTracker }
func (d *Daemon) SetDirty(dirty bool)     { d.mu.Lock(); defer d.mu.Unlock(); d.dirty = dirty }
func (d *Daemon) IsDirty() bool           { d.mu.Lock(); defer d.mu.Unlock(); return d.dirty }
func (d *Daemon) LastUsed() time.Time     { d.mu.Lock(); defer d.mu.Unlock(); return d.lastUsed }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/runner -run TestDaemon`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/daemon.go brain/pkg/runner/daemon_test.go
git commit -m "feat(runner): implement persistent streaming Daemon and NDJSON wire protocol"
```

---

### Task 3: Extend Configuration, Prometheus Metrics & Dual Database/Session Task Persistence

**Files:**
- Modify: `brain/pkg/config/config.go`
- Modify: `brain/pkg/config/config_test.go`
- Modify: `brain/pkg/metrics/metrics.go`
- Modify: `brain/pkg/db/schema.go`
- Modify: `brain/pkg/db/sessions.go`
- Modify: `brain/pkg/db/db_test.go`
- Modify: `brain/pkg/session/session.go`
- Modify: `brain/pkg/session/session_test.go`

**Interfaces:**
- Produces:
  - `ConfigData.DaemonIdleTimeout time.Duration` (default `24 * time.Hour`)
  - `ConfigData.MaxConcurrentDaemons int` (default `40`)
  - `ConfigData.MaxBackgroundTaskDuration time.Duration` (default `2 * time.Hour`)
  - `metrics.DaemonsActive prometheus.Gauge`
  - `metrics.DaemonSpawnsTotal *prometheus.CounterVec`
  - `metrics.DaemonPrunesTotal *prometheus.CounterVec`
  - `metrics.ActiveTasksGauge *prometheus.GaugeVec`
  - `metrics.DaemonMemoryBytes prometheus.Gauge`
  - `metrics.TaskTimeoutsTotal prometheus.Counter`
  - `SaveActiveTasks(database DBTX, threadID string, tasks []session.TaskMetadata) error` (in `pkg/db`)
  - `GetActiveTasks(database DBTX, threadID string) ([]session.TaskMetadata, error)` (in `pkg/db`)
  - `(m *Manager) SaveActiveTasks(sessionID string, tasks []session.TaskMetadata) error` (in `pkg/session`)
  - `(m *Manager) GetActiveTasks(sessionID string) ([]session.TaskMetadata, error)` (in `pkg/session`)

- [ ] **Step 1: Write failing unit tests for config, DB active tasks, and session persistence**

In `brain/pkg/config/config_test.go`:
```go
func TestConfig_DaemonParameters(t *testing.T) {
	yamlContent := `
daemon_idle_timeout: 12h
max_concurrent_daemons: 25
max_background_task_duration: 90m
`
	cfg, err := LoadConfigFromBytes([]byte(yamlContent))
	if err != nil {
		t.Fatalf("LoadConfigFromBytes failed: %v", err)
	}

	data := cfg.Get()
	if data.DaemonIdleTimeout != 12*time.Hour {
		t.Errorf("expected 12h daemon idle timeout, got %v", data.DaemonIdleTimeout)
	}
	if data.MaxConcurrentDaemons != 25 {
		t.Errorf("expected 25 max concurrent daemons, got %d", data.MaxConcurrentDaemons)
	}
	if data.MaxBackgroundTaskDuration != 90*time.Minute {
		t.Errorf("expected 90m max background task duration, got %v", data.MaxBackgroundTaskDuration)
	}
}
```

In `brain/pkg/db/db_test.go`:
```go
func TestDB_ActiveTaskPersistence(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	_ = SaveSessionID(database, "thread-active-db", "sess-active-db")

	tasks := []session.TaskMetadata{
		{TaskID: "task-db-42", ToolName: "run_command", CommandLine: "sleep 20", StartedAt: time.Now()},
	}

	if err := SaveActiveTasks(database, "thread-active-db", tasks); err != nil {
		t.Fatalf("SaveActiveTasks failed: %v", err)
	}

	loaded, err := GetActiveTasks(database, "thread-active-db")
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}
	if len(loaded) != 1 || loaded[0].TaskID != "task-db-42" {
		t.Errorf("expected loaded task-db-42 from database, got %v", loaded)
	}
}
```

In `brain/pkg/session/session_test.go`:
```go
func TestSession_ActiveTaskPersistence(t *testing.T) {
	tempHome := t.TempDir()
	tempData := t.TempDir()
	mgr := New(tempHome, tempData)

	sessDir := filepath.Join(tempData, "brain", "sess-active")
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatalf("failed to create session dir: %v", err)
	}

	tasks := []TaskMetadata{
		{TaskID: "task-persist-1", ToolName: "run_command", CommandLine: "sleep 30", StartedAt: time.Now()},
	}

	if err := mgr.SaveActiveTasks("sess-active", tasks); err != nil {
		t.Fatalf("SaveActiveTasks failed: %v", err)
	}

	loaded, err := mgr.GetActiveTasks("sess-active")
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}
	if len(loaded) != 1 || loaded[0].TaskID != "task-persist-1" {
		t.Errorf("expected loaded task-persist-1, got %v", loaded)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -v ./brain/pkg/config -run TestConfig_DaemonParameters`
Run: `go test -v ./brain/pkg/db -run TestDB_ActiveTaskPersistence`
Run: `go test -v ./brain/pkg/session -run TestSession_ActiveTaskPersistence`
Expected: FAIL

- [ ] **Step 3: Implement Config, Metrics, DB Schema, and Dual Persistence**

1. In `brain/pkg/config/config.go`, add fields to `ConfigData` and `rawConfigHelper`:
```go
DaemonIdleTimeout          time.Duration `yaml:"daemon_idle_timeout"`
MaxConcurrentDaemons       int           `yaml:"max_concurrent_daemons"`
MaxBackgroundTaskDuration time.Duration `yaml:"max_background_task_duration"`
```
In `UnmarshalYAML`, default `DaemonIdleTimeout` to `24 * time.Hour`, `MaxConcurrentDaemons` to `40`, and `MaxBackgroundTaskDuration` to `2 * time.Hour`.

2. In `brain/pkg/metrics/metrics.go`, add:
```go
DaemonsActive = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "aerial_brain_daemons_active",
	Help: "Current count of active persistent agy daemons.",
})
DaemonSpawnsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "aerial_brain_daemon_spawns_total",
	Help: "Total number of agy daemon initializations.",
}, []string{"reason"})
DaemonPrunesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "aerial_brain_daemon_prunes_total",
	Help: "Total number of agy daemon prunings.",
}, []string{"reason"})
ActiveTasksGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "aerial_brain_active_tasks",
	Help: "Active background tasks per thread.",
}, []string{"thread_id"})
DaemonMemoryBytes = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "aerial_brain_daemon_memory_bytes",
	Help: "Aggregate RSS memory consumed by agy daemons.",
})
TaskTimeoutsTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "aerial_brain_task_timeouts_total",
	Help: "Total background tasks terminated by safety timeout.",
})
```

3. In `brain/pkg/db/schema.go`:
- In `postgresSchema`, add `active_tasks TEXT NOT NULL DEFAULT '[]'` to `sessions` table definition.
- In `initSchemaPostgres`, add live migration:
```go
execNotice("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS active_tasks TEXT NOT NULL DEFAULT '[]'")
```
- In `brain/pkg/db/sqlite_test_fixture_test.go`:
- In `sqliteSchema`, add `active_tasks TEXT NOT NULL DEFAULT '[]'` to `sessions` table definition.
- In `initSchemaSQLite`, add:
```go
_, _ = database.Exec("ALTER TABLE sessions ADD COLUMN active_tasks TEXT NOT NULL DEFAULT '[]'")
```

4. In `brain/pkg/db/sessions.go`, implement:
```go
// SaveActiveTasks mirrors active background task metadata into PostgreSQL for cold-boot recovery using atomic upsert.
func SaveActiveTasks(database DBTX, threadID string, tasks []session.TaskMetadata) error {
	if database == nil || threadID == "" {
		return nil
	}
	encoded, err := json.Marshal(tasks)
	if err != nil {
		return fmt.Errorf("failed to marshal active tasks: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	INSERT INTO sessions (thread_id, active_tasks, created_at, updated_at)
	VALUES ($1, $2, $3, $3)
	ON CONFLICT (thread_id) DO UPDATE SET
		active_tasks = EXCLUDED.active_tasks,
		updated_at = EXCLUDED.updated_at
	`
	now := time.Now().UTC()
	_, err = database.ExecContext(ctx, query, threadID, string(encoded), now)
	return err
}

// GetActiveTasks retrieves mirrored active background tasks from PostgreSQL upon cold boot.
func GetActiveTasks(database DBTX, threadID string) ([]session.TaskMetadata, error) {
	if database == nil || threadID == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var tasksJSON string
	err := database.QueryRowContext(ctx, "SELECT COALESCE(active_tasks, '[]') FROM sessions WHERE thread_id = $1", threadID).Scan(&tasksJSON)
	if err == sql.ErrNoRows || strings.TrimSpace(tasksJSON) == "" {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var tasks []session.TaskMetadata
	if err := json.Unmarshal([]byte(tasksJSON), &tasks); err != nil {
		return nil, fmt.Errorf("failed to unmarshal active tasks: %w", err)
	}
	return tasks, nil
}
```

5. In `brain/pkg/session/session.go`, implement `GetSessionDir`, `SaveActiveTasks`, and `GetActiveTasks`:
```go
// GetSessionDir returns the canonical directory path for a session ID.
func (m *Manager) GetSessionDir(sessionID string) (string, error) {
	if m == nil {
		return "", errors.New("session manager is nil")
	}
	cleanID := strings.TrimSpace(sessionID)
	if cleanID == "" || strings.ContainsAny(cleanID, `/\:`) || strings.Contains(cleanID, "..") {
		return "", fmt.Errorf("invalid session ID: %q", sessionID)
	}
	for _, dir := range m.getTargetDirs(cleanID) {
		if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
			return dir, nil
		}
	}
	baseDir := m.resolveBaseDir(cleanID)
	if baseDir == "" {
		return "", errors.New("no session roots configured")
	}
	return filepath.Join(baseDir, cleanID), nil
}

// SaveActiveTasks persists active tasks to the session's .system_generated directory.
func (m *Manager) SaveActiveTasks(sessionID string, tasks []TaskMetadata) error {
	sessDir, err := m.GetSessionDir(sessionID)
	if err != nil {
		return err
	}
	taskDir := filepath.Join(sessDir, ".system_generated")
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		return err
	}
	data, err := json.Marshal(tasks)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(taskDir, "active_tasks.json"), data, 0644)
}

// GetActiveTasks loads active tasks from the session's .system_generated directory.
func (m *Manager) GetActiveTasks(sessionID string) ([]TaskMetadata, error) {
	sessDir, err := m.GetSessionDir(sessionID)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(sessDir, ".system_generated", "active_tasks.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var tasks []TaskMetadata
	if err := json.Unmarshal(data, &tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -v ./brain/pkg/config -run TestConfig_DaemonParameters`
Run: `go test -v ./brain/pkg/db -run TestDB_ActiveTaskPersistence`
Run: `go test -v ./brain/pkg/session -run TestSession_ActiveTaskPersistence`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/config/config.go brain/pkg/config/config_test.go brain/pkg/metrics/metrics.go brain/pkg/db/schema.go brain/pkg/db/sessions.go brain/pkg/db/db_test.go brain/pkg/db/sqlite_test_fixture_test.go brain/pkg/session/session.go brain/pkg/session/session_test.go
git commit -m "feat(config,metrics,db,session): add daemon config, prometheus metrics, and dual database/session active task persistence"
```

---

### Task 4: Implement `DaemonPool` with Memory-Pressure LRU Eviction & Zombie Watchdog

**Files:**
- Create: `brain/pkg/queue/daemon_pool.go`
- Create: `brain/pkg/queue/daemon_pool_test.go`

**Interfaces:**
- Consumes: `runner.Daemon`, `runner.DaemonConfig`, `config.ConfigData`, `metrics`
- Produces:
  ```go
  type MemoryChecker func() (availablePercent float64, err error)

  type DaemonPool struct { ... }
  func NewDaemonPool(cfg *config.Config, agyBin string, env []string, cwd string) *DaemonPool
  func (p *DaemonPool) SetMemoryChecker(checker MemoryChecker)
  func (p *DaemonPool) GetOrCreateDaemon(ctx context.Context, threadID string, sessionID string, model string) (*runner.Daemon, error)
  func (p *DaemonPool) HasActiveTasks(threadID string) bool
  func (p *DaemonPool) ReleaseDaemon(threadID string)
  func (p *DaemonPool) PruneIdle(maxIdle time.Duration) int
  func (p *DaemonPool) EvictLRU() bool
  func (p *DaemonPool) MonitorZombieTasks(maxDuration time.Duration) int
  func (p *DaemonPool) MarkDirty()
  func (p *DaemonPool) Close() error
  ```

- [ ] **Step 1: Write failing unit tests for `DaemonPool`**

Create `brain/pkg/queue/daemon_pool_test.go`:
```go
package queue

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestDaemonPool_BoundedCeilingAndLRUEviction(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

	cfg := config.NewTestConfig(map[string]interface{}{
		"max_concurrent_daemons": 2,
		"daemon_idle_timeout":    "1s",
	})

	pool := NewDaemonPool(cfg, mockBin, nil, tempDir)
	defer pool.Close()

	ctx := context.Background()

	_, _ = pool.GetOrCreateDaemon(ctx, "thread-1", "sess-1", "model-a")
	time.Sleep(10 * time.Millisecond)
	_, _ = pool.GetOrCreateDaemon(ctx, "thread-2", "sess-2", "model-a")

	if pool.ActiveCount() != 2 {
		t.Errorf("expected 2 active daemons, got %d", pool.ActiveCount())
	}

	// Spawn 3: should trigger LRU eviction of thread-1
	_, _ = pool.GetOrCreateDaemon(ctx, "thread-3", "sess-3", "model-a")

	if pool.ActiveCount() > 2 {
		t.Errorf("expected count bounded by 2, got %d", pool.ActiveCount())
	}
	if pool.HasDaemon("thread-1") {
		t.Errorf("expected thread-1 to be evicted by LRU sweep")
	}
	if !pool.HasDaemon("thread-2") || !pool.HasDaemon("thread-3") {
		t.Errorf("expected thread-2 and thread-3 to remain in pool")
	}
}

func TestDaemonPool_MemoryPressureLRUEviction(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

	cfg := config.NewTestConfig(map[string]interface{}{
		"max_concurrent_daemons": 10,
	})
	pool := NewDaemonPool(cfg, mockBin, nil, tempDir)
	defer pool.Close()

	// Mock memory checker returning 10% available (triggering memory pressure eviction)
	pool.SetMemoryChecker(func() (float64, error) {
		return 0.10, nil
	})

	ctx := context.Background()
	_, _ = pool.GetOrCreateDaemon(ctx, "thread-mem-1", "sess-1", "model-a")

	evicted := pool.CheckMemoryPressureAndEvict()
	if !evicted {
		t.Errorf("expected eviction under 10%% memory headroom")
	}
	if pool.HasDaemon("thread-mem-1") {
		t.Errorf("expected thread-mem-1 to be evicted due to memory pressure")
	}
}

func TestDaemonPool_MonitorZombieTasks(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

	cfg := config.NewTestConfig(map[string]interface{}{
		"max_background_task_duration": "50ms",
	})
	pool := NewDaemonPool(cfg, mockBin, nil, tempDir)
	defer pool.Close()

	ctx := context.Background()
	d, _ := pool.GetOrCreateDaemon(ctx, "thread-zombie", "sess-zombie", "model-a")

	// Register task started 100ms ago
	d.TaskTracker().Add(runner.TaskMetadata{
		TaskID:    "zombie-task-1",
		ToolName:  "run_command",
		StartedAt: time.Now().Add(-100 * time.Millisecond),
	})

	pruned := pool.MonitorZombieTasks(50 * time.Millisecond)
	if pruned != 1 {
		t.Errorf("expected 1 zombie task pruned, got %d", pruned)
	}
	if d.TaskTracker().Has("zombie-task-1") {
		t.Errorf("expected zombie task to be dereferenced from TaskTracker")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/queue -run TestDaemonPool`
Expected: FAIL (undefined `DaemonPool`)

- [ ] **Step 3: Implement `DaemonPool` in `brain/pkg/queue/daemon_pool.go`**

Create `brain/pkg/queue/daemon_pool.go`:
```go
package queue

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
)

type MemoryChecker func() (availablePercent float64, err error)

func DefaultLinuxMemoryChecker() (float64, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 1.0, err
	}
	defer file.Close()

	var total, avail float64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				total, _ = strconv.ParseFloat(fields[1], 64)
			}
		} else if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				avail, _ = strconv.ParseFloat(fields[1], 64)
			}
		}
	}
	if total > 0 {
		return avail / total, nil
	}
	return 1.0, nil
}

type DaemonPool struct {
	cfg        *config.Config
	agyBin     string
	env        []string
	cwd        string
	daemons    map[string]*runner.Daemon
	mu         sync.RWMutex
	memChecker MemoryChecker
}

func NewDaemonPool(cfg *config.Config, agyBin string, env []string, cwd string) *DaemonPool {
	return &DaemonPool{
		cfg:        cfg,
		agyBin:     agyBin,
		env:        env,
		cwd:        cwd,
		daemons:    make(map[string]*runner.Daemon),
		memChecker: DefaultLinuxMemoryChecker,
	}
}

func (p *DaemonPool) SetMemoryChecker(checker MemoryChecker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.memChecker = checker
}

func (p *DaemonPool) ActiveCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.daemons)
}

func (p *DaemonPool) HasDaemon(threadID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.daemons[threadID]
	return ok
}

func (p *DaemonPool) GetOrCreateDaemon(ctx context.Context, threadID string, sessionID string, model string) (*runner.Daemon, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check existing
	if d, ok := p.daemons[threadID]; ok {
		if !d.IsDirty() && d.State() != runner.StateClosed {
			return d, nil
		}
		// Close dirty or dead daemon
		_ = d.Close()
		delete(p.daemons, threadID)
	}

	// Enforce concurrency ceiling via LRU eviction
	data := p.cfg.Get()
	ceiling := data.MaxConcurrentDaemons
	if ceiling <= 0 {
		ceiling = 40
	}

	if len(p.daemons) >= ceiling {
		if !p.evictOldestIdleLocked() {
			log.Printf("[DaemonPool] Warning: pool at capacity (%d) with all daemons active", len(p.daemons))
		}
	}

	// Spawn fresh daemon
	dCfg := runner.DaemonConfig{
		SessionID: sessionID,
		ThreadID:  threadID,
		Model:     model,
		AgyBin:    p.agyBin,
		Cwd:       p.cwd,
		Env:       p.env,
	}

	d, err := runner.StartDaemon(ctx, dCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to start persistent daemon: %w", err)
	}

	p.daemons[threadID] = d
	metrics.DaemonsActive.Set(float64(len(p.daemons)))
	metrics.DaemonSpawnsTotal.WithLabelValues("cold_start").Inc()

	return d, nil
}

func (p *DaemonPool) HasActiveTasks(threadID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if d, ok := p.daemons[threadID]; ok {
		return d.TaskTracker().ActiveCount() > 0
	}
	return false
}

func (p *DaemonPool) evictOldestIdleLocked() bool {
	var oldestThread string
	var oldestTime time.Time

	for thID, d := range p.daemons {
		if d.TaskTracker().ActiveCount() == 0 && d.State() == runner.StateReady {
			if oldestThread == "" || d.LastUsed().Before(oldestTime) {
				oldestThread = thID
				oldestTime = d.LastUsed()
			}
		}
	}

	if oldestThread != "" {
		d := p.daemons[oldestThread]
		_ = d.Close()
		delete(p.daemons, oldestThread)
		metrics.DaemonsActive.Set(float64(len(p.daemons)))
		metrics.DaemonPrunesTotal.WithLabelValues("lru_ceiling").Inc()
		return true
	}
	return false
}

func (p *DaemonPool) CheckMemoryPressureAndEvict() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.memChecker == nil {
		return false
	}
	avail, err := p.memChecker()
	if err != nil || avail >= 0.15 {
		return false
	}

	log.Printf("[DaemonPool] Host memory pressure detected (available: %.1f%% < 15.0%%). Running LRU eviction.", avail*100)
	evicted := p.evictOldestIdleLocked()
	if evicted {
		metrics.DaemonPrunesTotal.WithLabelValues("lru_memory").Inc()
	}
	return evicted
}

func (p *DaemonPool) PruneIdle(maxIdle time.Duration) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	pruned := 0
	for thID, d := range p.daemons {
		if d.TaskTracker().ActiveCount() == 0 && d.State() == runner.StateReady {
			if now.Sub(d.LastUsed()) > maxIdle {
				_ = d.Close()
				delete(p.daemons, thID)
				pruned++
				metrics.DaemonPrunesTotal.WithLabelValues("idle_timeout").Inc()
			}
		}
	}
	if pruned > 0 {
		metrics.DaemonsActive.Set(float64(len(p.daemons)))
	}
	return pruned
}

func (p *DaemonPool) MonitorZombieTasks(maxDuration time.Duration) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	terminated := 0
	for _, d := range p.daemons {
		expired := d.TaskTracker().PruneExpired(maxDuration)
		for _, taskID := range expired {
			terminated++
			metrics.TaskTimeoutsTotal.Inc()
			log.Printf("[DaemonPool] Terminated zombie background task %s exceeding %v duration ceiling", taskID, maxDuration)
		}
	}
	return terminated
}

func (p *DaemonPool) MarkDirty() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for thID, d := range p.daemons {
		if d.State() == runner.StateReady && d.TaskTracker().ActiveCount() == 0 {
			_ = d.Close()
			delete(p.daemons, thID)
			metrics.DaemonPrunesTotal.WithLabelValues("hot_reload").Inc()
		} else {
			d.SetDirty(true)
		}
	}
	metrics.DaemonsActive.Set(float64(len(p.daemons)))
}

func (p *DaemonPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for thID, d := range p.daemons {
		_ = d.Close()
		delete(p.daemons, thID)
	}
	metrics.DaemonsActive.Set(0)
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/queue -run TestDaemonPool`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/daemon_pool.go brain/pkg/queue/daemon_pool_test.go
git commit -m "feat(queue): implement DaemonPool with memory pressure LRU eviction and zombie watchdog"
```

---

### Task 5: Integrate Worker Pool, Serialized Turn Mailbox & Task Completion Resumption

**Files:**
- Modify: `brain/pkg/queue/pool.go`
- Modify: `brain/pkg/queue/burst.go`
- Modify: `brain/pkg/queue/worker.go`
- Modify: `brain/pkg/queue/worker_test.go`

**Interfaces:**
- Consumes: `DaemonPool`, `runner.Daemon`, `runner.TurnResult`
- Produces:
  ```go
  const TaskCompletionResumePrompt = "[SYSTEM NOTICE]: Background task %s has finished with exit code %d. Logs are available at %s. Continue your workflow to completion. Do NOT end the turn with waiting text."
  ```
  - 24-hour worker idle timeout lifecycle unified with `DefaultDaemonIdleTimeout`.
  - Serialized synthetic message injection into `state.ch` for background task completion wakes.
  - Background task completion watcher with explicit `d.TaskTracker().Remove(taskID)`.

- [ ] **Step 1: Write failing unit test for Yield Trap auto-resumption on persistent daemons**

In `brain/pkg/queue/worker_test.go`:
```go
func TestWorker_PersistentDaemonYieldTrapAutoResume(t *testing.T) {
	tempDir := t.TempDir()
	taskDir := filepath.Join(tempDir, ".system_generated", "tasks")
	_ = os.MkdirAll(taskDir, 0755)

	logFile := filepath.Join(taskDir, "task-test-1.log")
	_ = os.WriteFile(logFile, []byte("Task output\n"), 0644)

	exitCode, logPath, err := watchTaskCompletion(context.Background(), tempDir, "task-test-1")
	if err != nil {
		t.Fatalf("watchTaskCompletion failed: %v", err)
	}
	if exitCode != 0 {
		t.Errorf("expected exit code 0, got %d", exitCode)
	}
	if logPath != logFile {
		t.Errorf("expected log path %s, got %s", logFile, logPath)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/queue -run TestWorker_PersistentDaemonYieldTrapAutoResume`
Expected: FAIL (undefined `watchTaskCompletion`)

- [ ] **Step 3: Implement concrete task completion watcher and wire worker execution**

1. In `brain/pkg/queue/pool.go`:
Define the accurate completion prompt template:
```go
const TaskCompletionResumePrompt = "[SYSTEM NOTICE]: Background task %s has finished with exit code %d. Logs are available at %s. Continue your workflow to completion. Do NOT end the turn with waiting text."
```
Add `daemonPool *DaemonPool` to `WorkerPool`. In `NewWorkerPool`, initialize `p.daemonPool = NewDaemonPool(p.cfg, p.cfg.Get().AgyBin, os.Environ(), p.cfg.Get().DataDir)`. In `WorkerPool.Stop()`, call `p.daemonPool.Close()`.

2. In `brain/pkg/queue/burst.go`:
In `runThreadWorker`, set `idleTimeout` default to `p.cfg.Get().DaemonIdleTimeout` (`24 * time.Hour`).
When `idleTimer.C` fires:
```go
case <-idleTimer.C:
    p.mu.Lock()
    if len(state.ch) == 0 && state.activeEnqueuers == 0 {
        if p.daemonPool != nil && p.daemonPool.HasActiveTasks(threadID) {
            p.mu.Unlock()
            idleTimer.Reset(p.cfg.Get().DaemonIdleTimeout)
            continue
        }
        delete(p.threadChs, threadID)
        p.scopeLocks.Delete(threadID)
        p.mu.Unlock()
        return
    }
    p.mu.Unlock()
    idleTimer.Reset(idleTimeout)
```

3. In `brain/pkg/queue/worker.go`:
Add `watchTaskCompletion`:
```go
func watchTaskCompletion(ctx context.Context, sessionDir, taskID string) (int, string, error) {
	logPath := filepath.Join(sessionDir, ".system_generated", "tasks", taskID+".log")
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return -1, logPath, ctx.Err()
		case <-ticker.C:
			if _, err := os.Stat(logPath); err == nil {
				return 0, logPath, nil
			}
		}
	}
}
```

In `executeTurnWithRunner`:
- Acquire persistent daemon:
  `daemon, err := te.pool.daemonPool.GetOrCreateDaemon(ctx, te.threadID, te.currentSessionID, currentModel)`
- Execute turn via persistent stream:
  `turnRes, err := daemon.ExecuteTurn(ctx, promptToSend)`
- If `turnRes.IsYieldTrap`:
  - Suppress intermediate prose delivery to Discord.
  - In a background goroutine (or synchronous wait if bounded), watch `.system_generated/tasks/<taskID>.log`.
  - Upon task completion:
    `daemon.TaskTracker().Remove(taskID)`
    Enqueue synthetic message:
    ```go
    te.pool.Enqueue(db.Message{
        ID:        uuid.New().String(),
        ThreadID:  te.threadID,
        ChannelID: te.channelID,
        Content:   fmt.Sprintf(TaskCompletionResumePrompt, taskID, exitCode, logPath),
        Source:    "system_synthetic",
        CreatedAt: time.Now(),
    })
    ```
- If `!turnRes.IsYieldTrap`:
  - Deliver substantive content to Discord.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/queue -run TestWorker_PersistentDaemonYieldTrapAutoResume`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/pool.go brain/pkg/queue/burst.go brain/pkg/queue/worker.go brain/pkg/queue/worker_test.go
git commit -m "feat(queue): integrate persistent streaming daemon execution and serialized task completion resumption"
```

---

### Task 6: Full Verification, Lint & Test Coverage Suite

**Files:**
- Verify: all packages in `brain/pkg/...`

- [ ] **Step 1: Run comprehensive package tests**

Run: `go test -v ./brain/pkg/...`
Expected: All tests PASS

- [ ] **Step 2: Run check-coverage script**

Run: `scripts/check-coverage.sh`
Expected: `>= 90.0%` coverage across all packages

- [ ] **Step 3: Run verify script**

Run: `scripts/verify.sh --full`
Expected: 100% green verification (zero BOMs, zero lint errors, all tests pass)

- [ ] **Step 4: Commit and finalize**

```bash
git add -A
git commit -m "chore: complete verification suite for persistent streaming daemons"
```
