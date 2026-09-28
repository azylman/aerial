# Layered Streaming Process Pool Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor Gundam Brain from a synchronous request-response turn model to a decoupled, asynchronous NDJSON streaming architecture powered by a unified process pool across Discord, persistent Voice, and throwaway tasks, while completely deleting all legacy daemon pools and host memory inspection logic.

**Architecture:** Layer 1 (`StreamingDaemon` in `brain/pkg/runner`) wraps the persistent `agy` CLI (`stream-json`), continuously decoding NDJSON events from stdout and dispatching to an in-flight FIFO queue. Layer 2 (`UnifiedProcessPool` in `brain/pkg/runner`) manages pinned conversational daemons (Discord threads and persistent hardware voice daemons keyed by device ID, e.g. `"kiosk"`), pre-warms warm standby targets via `singleflight`, and internally enforces session rotation thresholds (turns, steps, transcript bytes, 24h idle TTL). Layer 3 (`TurnSink` in `brain/pkg/queue`) routes streaming events to transport-specific destinations (`DiscordTurnSink` for badge updates and 4-pass media extraction, `VoiceTurnSink` with a sliding replay ring buffer and re-binding for Wi-Fi recovery, and `ThrowawayTurnSink` for classifiers).

**Tech Stack:** Go 1.24, Linux POSIX process groups (`syscall.SysProcAttr{Setpgid: true}`), Windows job objects, NDJSON streaming, PostgreSQL 16, DiscordGo, Gorilla WebSocket.

**Spec:** [`docs/superpowers/specs/2026-09-28-streaming-process-pool-design.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-09-28-streaming-process-pool-design.md)

## Global Constraints
- Pure Go standard library for concurrency and process management (`sync`, `time`, `os`, `os/exec`, `io`, `bufio`, `encoding/json`).
- Strictly ZERO markdown tables across all artifacts, messages, commits, and summaries (bulleted lists only).
- Strictly ZERO swallowed errors. Every error path must be logged with context or propagated.
- Strictly ZERO host memory inspection (`/proc/meminfo` reading, string parsing, or memory pressure eviction).
- All unit tests must be hermetic and execute in-memory via `io.Pipe()` without spawning real OS processes or opening real network listeners in CI.
- Statement coverage floor of strictly >= 95.0% maintained across `brain/pkg/runner` and `brain/pkg/queue`.
- All staged changes must pass fast pre-commit verification (`powershell -File scripts/verify.ps1`).

---

### Task 1: Spawner Interfaces, ProcessHandle, and Mock Spawner

**Files:**
- Create: `brain/pkg/runner/spawner.go`
- Create: `brain/pkg/runner/spawner_test.go`

**Interfaces:**
- Produces:
  ```go
  type ProcessHandle interface {
      Pid() int
      Kill() error
      Wait() error
  }

  type DaemonSpawner interface {
      Spawn(ctx context.Context, cfg DaemonConfig) (stdin io.WriteCloser, stdout io.ReadCloser, stderr io.ReadCloser, handle ProcessHandle, err error)
  }

  type DefaultDaemonSpawner struct{}
  func (s *DefaultDaemonSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)

  type MockDaemonSpawner struct {
      SpawnFn func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
  }
  ```

- [ ] **Step 1: Write failing unit tests for `DaemonSpawner` and `MockDaemonSpawner`**

Create `brain/pkg/runner/spawner_test.go`:
```go
package runner

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestMockDaemonSpawner_CustomFunction(t *testing.T) {
	expectedErr := errors.New("spawn failed")
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, expectedErr
		},
	}

	_, _, _, _, err := mock.Spawn(context.Background(), DaemonConfig{SessionID: "test-sess"})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMockDaemonSpawner_DefaultPipeBehavior(t *testing.T) {
	mock := NewMockDaemonSpawner()
	stdin, stdout, stderr, handle, err := mock.Spawn(context.Background(), DaemonConfig{SessionID: "test-sess"})
	if err != nil {
		t.Fatalf("unexpected spawn error: %v", err)
	}
	defer stdin.Close()
	defer stdout.Close()
	defer stderr.Close()

	if handle.Pid() <= 0 {
		t.Errorf("expected positive pid, got %d", handle.Pid())
	}
	if err := handle.Kill(); err != nil {
		t.Errorf("unexpected kill error: %v", err)
	}
	if err := handle.Wait(); err != nil {
		t.Errorf("unexpected wait error: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/runner -run TestMockDaemonSpawner`
Expected: FAIL with undefined `MockDaemonSpawner` / `DaemonSpawner`

- [ ] **Step 3: Implement `DaemonSpawner` and `MockDaemonSpawner`**

Create `brain/pkg/runner/spawner.go`:
```go
package runner

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"
)

// ProcessHandle abstracts an operating system process.
type ProcessHandle interface {
	Pid() int
	Kill() error
	Wait() error
}

// DaemonSpawner abstracts spawning a daemon process.
type DaemonSpawner interface {
	Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
}

// OSProcessHandle wraps an *exec.Cmd process.
type OSProcessHandle struct {
	cmd *exec.Cmd
}

func (h *OSProcessHandle) Pid() int {
	if h.cmd == nil || h.cmd.Process == nil {
		return 0
	}
	return h.cmd.Process.Pid
}

func (h *OSProcessHandle) Kill() error {
	if h.cmd == nil || h.cmd.Process == nil {
		return nil
	}
	return KillProcessGroup(h.cmd)
}

func (h *OSProcessHandle) Wait() error {
	if h.cmd == nil {
		return nil
	}
	return h.cmd.Wait()
}

// DefaultDaemonSpawner launches an agy OS subprocess.
type DefaultDaemonSpawner struct{}

func (s *DefaultDaemonSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	args := BuildDaemonArgs(cfg)
	cmd := exec.CommandContext(ctx, cfg.AgyBin, args...)
	ConfigureProcessGroup(cmd)
	cmd.Dir = cfg.Cwd
	cmd.Env = cfg.Env

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, nil, nil, nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, nil, nil, nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, nil, nil, nil, fmt.Errorf("failed to start daemon process: %w", err)
	}

	return stdin, stdout, stderr, &OSProcessHandle{cmd: cmd}, nil
}

// MockProcessHandle simulates a running process in tests.
type MockProcessHandle struct {
	pid     int
	killErr error
	waitErr error
	mu      sync.Mutex
	killed  bool
	waited  bool
}

func (m *MockProcessHandle) Pid() int {
	return m.pid
}

func (m *MockProcessHandle) Kill() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.killed = true
	return m.killErr
}

func (m *MockProcessHandle) Wait() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waited = true
	return m.waitErr
}

// MockDaemonSpawner enables hermetic testing of streaming daemons.
type MockDaemonSpawner struct {
	SpawnFn func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
}

func NewMockDaemonSpawner() *MockDaemonSpawner {
	return &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()

			_ = inR.Close()
			_ = outW.Close()
			_ = errW.Close()

			handle := &MockProcessHandle{pid: 12345}
			return inW, outR, errR, handle, nil
		},
	}
}

func (m *MockDaemonSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	if m.SpawnFn != nil {
		return m.SpawnFn(ctx, cfg)
	}
	return NewMockDaemonSpawner().Spawn(ctx, cfg)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/runner -run TestMockDaemonSpawner`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/spawner.go brain/pkg/runner/spawner_test.go
git commit -m "feat(runner): introduce DaemonSpawner and MockDaemonSpawner interfaces"
```

---

### Task 2: StreamingDaemon NDJSON Event Stream Supervisor & Handshake

**Files:**
- Create: `brain/pkg/runner/streaming_daemon.go`
- Create: `brain/pkg/runner/streaming_daemon_test.go`

**Interfaces:**
- Produces:
  ```go
  type TurnSink interface {
      OnTurnStarted()
      OnThinking()
      OnToolCall(toolName, commandName string)
      OnTextDelta(delta string)
      OnResult(res *TurnResult)
      OnError(err error)
  }

  type TurnContext struct {
      TurnID    string
      Prompt    string
      Sink      TurnSink
      CreatedAt time.Time
  }

  type StreamingDaemon struct {
      // manages state, handshake, and stdout NDJSON event loop
  }

  func StartStreamingDaemon(ctx context.Context, cfg DaemonConfig, spawner DaemonSpawner) (*StreamingDaemon, error)
  func (d *StreamingDaemon) State() DaemonState
  func (d *StreamingDaemon) SessionID() string
  func (d *StreamingDaemon) Close() error
  ```

- [ ] **Step 1: Write failing unit test for `StartStreamingDaemon` handshake**

Create `brain/pkg/runner/streaming_daemon_test.go`:
```go
package runner

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestStreamingDaemon_HandshakeSuccess(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, _ := io.Pipe()

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 999}, nil
		},
	}

	go func() {
		defer outW.Close()
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000001",
		Timeout:   2 * time.Second,
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed to start streaming daemon: %v", err)
	}
	defer inR.Close()
	defer daemon.Close()

	if daemon.State() != StateReady {
		t.Errorf("expected state %s, got %s", StateReady, daemon.State())
	}
	if daemon.SessionID() != "00000000-0000-0000-0000-000000000001" {
		t.Errorf("expected latched session ID, got %s", daemon.SessionID())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/runner -run TestStreamingDaemon_HandshakeSuccess`
Expected: FAIL with undefined `StartStreamingDaemon`

- [ ] **Step 3: Implement `StreamingDaemon` and handshake protocol**

Create `brain/pkg/runner/streaming_daemon.go`:
```go
package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type TurnSink interface {
	OnTurnStarted()
	OnThinking()
	OnToolCall(toolName, commandName string)
	OnTextDelta(delta string)
	OnResult(res *TurnResult)
	OnError(err error)
}

type TurnContext struct {
	TurnID    string
	Prompt    string
	Sink      TurnSink
	CreatedAt time.Time
}

type streamInputMessage struct {
	Content string `json:"content"`
}

type streamInputPayload struct {
	Event   string             `json:"event"`
	Message streamInputMessage `json:"message"`
}

type StreamingDaemon struct {
	cfg         DaemonConfig
	spawner     DaemonSpawner
	handle      ProcessHandle
	stdin       io.WriteCloser
	stdout      io.ReadCloser
	stderr      *ActivityWriter
	taskTracker *TaskTracker

	mu          sync.RWMutex
	stdinMu     sync.Mutex
	state       DaemonState
	sessionID   string
	dirty       bool
	lastUsed    time.Time
	turnCount   int
	stepCount   int

	inflightMu  sync.Mutex
	inflight    []*TurnContext

	readerWg    sync.WaitGroup
	closeOnce   sync.Once
	closed      atomic.Bool
}

func StartStreamingDaemon(ctx context.Context, cfg DaemonConfig, spawner DaemonSpawner) (*StreamingDaemon, error) {
	if spawner == nil {
		spawner = &DefaultDaemonSpawner{}
	}

	stdin, stdout, stderr, handle, err := spawner.Spawn(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed spawning daemon: %w", err)
	}

	actWriter := NewActivityWriter(nil)
	go func() {
		defer stderr.Close()
		_, _ = io.Copy(actWriter, stderr)
	}()

	d := &StreamingDaemon{
		cfg:         cfg,
		spawner:     spawner,
		handle:      handle,
		stdin:       stdin,
		stdout:      stdout,
		stderr:      actWriter,
		taskTracker: NewTaskTracker(),
		state:       StateStarting,
		lastUsed:    time.Now(),
	}

	// Perform synchronous handshake with 5-second deadline
	handshakeDeadline := time.Now().Add(5 * time.Second)
	reader := bufio.NewReader(stdout)
	lineChan := make(chan string, 1)
	errChan := make(chan error, 1)

	go func() {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			errChan <- readErr
			return
		}
		lineChan <- line
	}()

	select {
	case <-time.After(time.Until(handshakeDeadline)):
		_ = d.Close()
		return nil, errors.New("daemon startup timed out waiting for init event")
	case readErr := <-errChan:
		_ = d.Close()
		return nil, fmt.Errorf("daemon stdout closed before init event: %w", readErr)
	case line := <-lineChan:
		initSessID, parseErr := ParseInitEvent(line)
		if parseErr != nil {
			_ = d.Close()
			return nil, fmt.Errorf("failed parsing init event (%q): %w", line, parseErr)
		}
		d.mu.Lock()
		d.sessionID = initSessID
		d.state = StateReady
		d.mu.Unlock()
		d.stderr.SetSessionID(initSessID)
	}

	d.readerWg.Add(1)
	go d.readStdoutLoop(reader)

	return d, nil
}

func (d *StreamingDaemon) State() DaemonState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state
}

func (d *StreamingDaemon) SessionID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.sessionID
}

func (d *StreamingDaemon) TurnCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.turnCount
}

func (d *StreamingDaemon) StepCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.stepCount
}

func (d *StreamingDaemon) LastUsed() time.Time {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastUsed
}

func (d *StreamingDaemon) TaskTracker() *TaskTracker {
	return d.taskTracker
}

func (d *StreamingDaemon) Close() error {
	var closeErr error
	d.closeOnce.Do(func() {
		d.closed.Store(true)
		d.mu.Lock()
		d.state = StateClosed
		d.mu.Unlock()

		if d.stdin != nil {
			_ = d.stdin.Close()
		}
		if d.stdout != nil {
			_ = d.stdout.Close()
		}
		if d.handle != nil {
			closeErr = d.handle.Kill()
		}
		d.readerWg.Wait()

		// Drain any remaining inflight turns with error
		d.inflightMu.Lock()
		remaining := d.inflight
		d.inflight = nil
		d.inflightMu.Unlock()

		for _, turn := range remaining {
			if turn.Sink != nil {
				turn.Sink.OnError(errors.New("daemon closed while turn was in-flight"))
			}
		}
	})
	return closeErr
}

func (d *StreamingDaemon) readStdoutLoop(r *bufio.Reader) {
	defer d.readerWg.Done()

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if !d.closed.Load() {
				d.mu.Lock()
				d.state = StateClosed
				d.mu.Unlock()

				d.inflightMu.Lock()
				remaining := d.inflight
				d.inflight = nil
				d.inflightMu.Unlock()

				for _, turn := range remaining {
					if turn.Sink != nil {
						turn.Sink.OnError(fmt.Errorf("daemon stdout unexpected EOF: %w", err))
					}
				}
			}
			return
		}

		d.dispatchNDJSONLine(line)
	}
}

func (d *StreamingDaemon) dispatchNDJSONLine(line string) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		log.Printf("[StreamingDaemon] Malformed NDJSON line: %v", err)
		return
	}

	event, _ := raw["event"].(string)

	d.inflightMu.Lock()
	var activeTurn *TurnContext
	if len(d.inflight) > 0 {
		activeTurn = d.inflight[0]
	}
	d.inflightMu.Unlock()

	if activeTurn == nil || activeTurn.Sink == nil {
		return
	}

	switch event {
	case "step_update":
		d.mu.Lock()
		d.stepCount++
		d.mu.Unlock()

		if toolCall, ok := raw["tool_call"].(map[string]any); ok {
			toolName, _ := toolCall["name"].(string)
			cmdName, _ := toolCall["command"].(string)
			activeTurn.Sink.OnToolCall(toolName, cmdName)
		}
		if delta, ok := raw["delta"].(string); ok && delta != "" {
			activeTurn.Sink.OnTextDelta(delta)
		}
		if _, ok := raw["thinking"]; ok {
			activeTurn.Sink.OnThinking()
		}

	case "result":
		d.inflightMu.Lock()
		if len(d.inflight) > 0 {
			activeTurn = d.inflight[0]
			d.inflight = d.inflight[1:]
		}
		d.inflightMu.Unlock()

		d.mu.Lock()
		d.turnCount++
		d.lastUsed = time.Now()
		if len(d.inflight) == 0 {
			if d.taskTracker.ActiveCount() > 0 {
				d.state = StateYieldWaiting
			} else {
				d.state = StateReady
			}
		}
		d.mu.Unlock()

		res := &TurnResult{
			ConversationID: d.SessionID(),
			Response:       extractResponseString(raw),
			Duration:       time.Since(activeTurn.CreatedAt),
		}
		activeTurn.Sink.OnResult(res)
	}
}

func extractResponseString(raw map[string]any) string {
	if resObj, ok := raw["result"].(map[string]any); ok {
		if resp, ok := resObj["response"].(string); ok {
			return resp
		}
	}
	return ""
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/runner -run TestStreamingDaemon_HandshakeSuccess`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/streaming_daemon.go brain/pkg/runner/streaming_daemon_test.go
git commit -m "feat(runner): implement StreamingDaemon handshake and stdout event reader loop"
```

---

### Task 3: StreamingDaemon In-Flight FIFO Pipelining & Stdin Concurrency

**Files:**
- Modify: `brain/pkg/runner/streaming_daemon.go`
- Test: `brain/pkg/runner/streaming_daemon_test.go`

**Interfaces:**
- Produces:
  ```go
  func (d *StreamingDaemon) Send(prompt string, turnCtx *TurnContext) error
  func (d *StreamingDaemon) InflightCount() int
  ```

- [ ] **Step 1: Write failing unit test for `Send` FIFO pipelining**

Add to `brain/pkg/runner/streaming_daemon_test.go`:
```go
type mockTurnSink struct {
	started   bool
	toolCalls []string
	deltas    []string
	result    *TurnResult
	err       error
	done      chan struct{}
}

func newMockTurnSink() *mockTurnSink {
	return &mockTurnSink{done: make(chan struct{})}
}

func (m *mockTurnSink) OnTurnStarted() { m.started = true }
func (m *mockTurnSink) OnThinking()    {}
func (m *mockTurnSink) OnToolCall(name, cmd string) {
	m.toolCalls = append(m.toolCalls, name+":"+cmd)
}
func (m *mockTurnSink) OnTextDelta(delta string) {
	m.deltas = append(m.deltas, delta)
}
func (m *mockTurnSink) OnResult(res *TurnResult) {
	m.result = res
	close(m.done)
}
func (m *mockTurnSink) OnError(err error) {
	m.err = err
	close(m.done)
}

func TestStreamingDaemon_SendFIFOPipelining(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, _ := io.Pipe()

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 888}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000002"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000002"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed to start daemon: %v", err)
	}
	defer inR.Close()
	defer daemon.Close()

	sink1 := newMockTurnSink()
	turn1 := &TurnContext{TurnID: "turn-1", Prompt: "first prompt", Sink: sink1, CreatedAt: time.Now()}
	sink2 := newMockTurnSink()
	turn2 := &TurnContext{TurnID: "turn-2", Prompt: "second prompt", Sink: sink2, CreatedAt: time.Now()}

	if err := daemon.Send(turn1.Prompt, turn1); err != nil {
		t.Fatalf("failed sending turn1: %v", err)
	}
	if err := daemon.Send(turn2.Prompt, turn2); err != nil {
		t.Fatalf("failed sending turn2: %v", err)
	}

	if daemon.InflightCount() != 2 {
		t.Fatalf("expected 2 inflight turns, got %d", daemon.InflightCount())
	}

	// Emit events for turn 1
	go func() {
		_, _ = outW.Write([]byte(`{"event":"step_update","delta":"hello "}` + "\n"))
		_, _ = outW.Write([]byte(`{"event":"result","result":{"response":"hello world"}}` + "\n"))

		// Emit events for turn 2
		_, _ = outW.Write([]byte(`{"event":"step_update","delta":"second "}` + "\n"))
		_, _ = outW.Write([]byte(`{"event":"result","result":{"response":"second response"}}` + "\n"))
	}()

	select {
	case <-sink1.done:
		if sink1.result == nil || sink1.result.Response != "hello world" {
			t.Errorf("turn 1 unexpected result: %+v", sink1.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("turn 1 timed out")
	}

	select {
	case <-sink2.done:
		if sink2.result == nil || sink2.result.Response != "second response" {
			t.Errorf("turn 2 unexpected result: %+v", sink2.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("turn 2 timed out")
	}

	if daemon.InflightCount() != 0 {
		t.Errorf("expected 0 inflight turns remaining, got %d", daemon.InflightCount())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/runner -run TestStreamingDaemon_SendFIFOPipelining`
Expected: FAIL with undefined `Send` / `InflightCount`

- [ ] **Step 3: Implement `Send` and FIFO queue management**

Add to `brain/pkg/runner/streaming_daemon.go`:
```go
func (d *StreamingDaemon) InflightCount() int {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	return len(d.inflight)
}

func (d *StreamingDaemon) Send(prompt string, turnCtx *TurnContext) error {
	d.stdinMu.Lock()
	defer d.stdinMu.Unlock()

	d.mu.RLock()
	if d.state == StateClosed || d.closed.Load() {
		d.mu.RUnlock()
		return errors.New("cannot send to closed streaming daemon")
	}
	d.mu.RUnlock()

	wireMsg := streamInputPayload{
		Event: "user",
		Message: streamInputMessage{
			Content: prompt,
		},
	}
	encoded, err := json.Marshal(wireMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal turn prompt: %w", err)
	}
	encoded = append(encoded, '\n')

	d.inflightMu.Lock()
	d.inflight = append(d.inflight, turnCtx)
	d.inflightMu.Unlock()

	d.mu.Lock()
	d.state = StateExecuting
	d.lastUsed = time.Now()
	d.mu.Unlock()

	if turnCtx.Sink != nil {
		turnCtx.Sink.OnTurnStarted()
	}

	if _, err := d.stdin.Write(encoded); err != nil {
		d.mu.Lock()
		d.dirty = true
		d.state = StateClosed
		d.mu.Unlock()
		return fmt.Errorf("failed writing prompt to daemon stdin: %w", err)
	}

	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/runner -run TestStreamingDaemon_SendFIFOPipelining`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/streaming_daemon.go brain/pkg/runner/streaming_daemon_test.go
git commit -m "feat(runner): implement Send with atomic stdin writes and FIFO queue pipelining"
```

---

### Task 4: Layer 2 UnifiedProcessPool Pre-Warming & Pinned Target Management

**Files:**
- Create: `brain/pkg/runner/unified_pool.go`
- Create: `brain/pkg/runner/unified_pool_test.go`

**Interfaces:**
- Produces:
  ```go
  type PoolConfig struct {
      PrewarmedTargets []string
      DefaultModel     string
      AgyBin           string
      Cwd              string
      Env              []string
      MaxIdle          time.Duration
  }

  type UnifiedProcessPool struct {
      // manages pinned daemons and singleflight pre-warming
  }

  func NewUnifiedProcessPool(cfg PoolConfig, spawner DaemonSpawner) *UnifiedProcessPool
  func (p *UnifiedProcessPool) Initialize(ctx context.Context) error
  func (p *UnifiedProcessPool) GetOrCreate(ctx context.Context, targetKey string) (*StreamingDaemon, error)
  func (p *UnifiedProcessPool) Close() error
  ```

- [ ] **Step 1: Write failing unit test for `UnifiedProcessPool` singleflight pre-warming**

Create `brain/pkg/runner/unified_pool_test.go`:
```go
package runner

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnifiedProcessPool_SingleflightPrewarming(t *testing.T) {
	var spawnCount atomic.Int32

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			spawnCount.Add(1)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000010"}` + "\n"))
			}()
			_ = inR.Close()

			return inW, outR, errR, &MockProcessHandle{pid: 100}, nil
		},
	}

	cfg := PoolConfig{
		PrewarmedTargets: []string{"kiosk"},
		DefaultModel:     "gemini-2.5-pro",
		MaxIdle:          24 * time.Hour,
	}

	pool := NewUnifiedProcessPool(cfg, mock)
	defer pool.Close()

	if err := pool.Initialize(context.Background()); err != nil {
		t.Fatalf("failed to initialize pool: %v", err)
	}

	// Concurrent callers requesting "kiosk" should join the same daemon, not re-spawn
	d1, err1 := pool.GetOrCreate(context.Background(), "kiosk")
	d2, err2 := pool.GetOrCreate(context.Background(), "kiosk")

	if err1 != nil || err2 != nil {
		t.Fatalf("errors getting kiosk daemon: %v, %v", err1, err2)
	}
	if d1 != d2 {
		t.Errorf("expected d1 and d2 to be identical pinned instance")
	}
	if spawnCount.Load() != 1 {
		t.Errorf("expected exactly 1 spawn for kiosk, got %d", spawnCount.Load())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/runner -run TestUnifiedProcessPool_SingleflightPrewarming`
Expected: FAIL with undefined `UnifiedProcessPool`

- [ ] **Step 3: Implement `UnifiedProcessPool` and pre-warming**

Create `brain/pkg/runner/unified_pool.go`:
```go
package runner

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type PoolConfig struct {
	PrewarmedTargets []string
	DefaultModel     string
	AgyBin           string
	Cwd              string
	Env              []string
	MaxIdle          time.Duration
}

type UnifiedProcessPool struct {
	cfg        PoolConfig
	spawner    DaemonSpawner
	daemons    map[string]*StreamingDaemon
	mu         sync.RWMutex
	sf         singleflight.Group
	closed     bool
	closeOnce  sync.Once
}

func NewUnifiedProcessPool(cfg PoolConfig, spawner DaemonSpawner) *UnifiedProcessPool {
	if spawner == nil {
		spawner = &DefaultDaemonSpawner{}
	}
	if cfg.MaxIdle <= 0 {
		cfg.MaxIdle = 24 * time.Hour
	}
	return &UnifiedProcessPool{
		cfg:     cfg,
		spawner: spawner,
		daemons: make(map[string]*StreamingDaemon),
	}
}

func (p *UnifiedProcessPool) Initialize(ctx context.Context) error {
	for _, target := range p.cfg.PrewarmedTargets {
		targetKey := target
		go func() {
			initCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := p.GetOrCreate(initCtx, targetKey); err != nil {
				log.Printf("[UnifiedProcessPool] Warning: pre-warming target %q failed: %v", targetKey, err)
			} else {
				log.Printf("[UnifiedProcessPool] Pre-warmed target %q successfully", targetKey)
			}
		}()
	}
	return nil
}

func (p *UnifiedProcessPool) GetOrCreate(ctx context.Context, targetKey string) (*StreamingDaemon, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("process pool is closed")
	}
	if d, exists := p.daemons[targetKey]; exists && d != nil && d.State() != StateClosed {
		p.mu.RUnlock()
		return d, nil
	}
	p.mu.RUnlock()

	res, err, _ := p.sf.Do(targetKey, func() (any, error) {
		p.mu.RLock()
		if d, exists := p.daemons[targetKey]; exists && d != nil && d.State() != StateClosed {
			p.mu.RUnlock()
			return d, nil
		}
		p.mu.RUnlock()

		daemonCfg := DaemonConfig{
			ThreadID: targetKey,
			Model:    p.cfg.DefaultModel,
			AgyBin:   p.cfg.AgyBin,
			Cwd:      p.cfg.Cwd,
			Env:      p.cfg.Env,
		}

		daemon, spawnErr := StartStreamingDaemon(ctx, daemonCfg, p.spawner)
		if spawnErr != nil {
			return nil, fmt.Errorf("failed to start daemon for target %q: %w", targetKey, spawnErr)
		}

		p.mu.Lock()
		p.daemons[targetKey] = daemon
		p.mu.Unlock()

		return daemon, nil
	})

	if err != nil {
		return nil, err
	}
	return res.(*StreamingDaemon), nil
}

func (p *UnifiedProcessPool) Close() error {
	var errs []error
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		toClose := p.daemons
		p.daemons = make(map[string]*StreamingDaemon)
		p.mu.Unlock()

		for target, d := range toClose {
			if err := d.Close(); err != nil {
				errs = append(errs, fmt.Errorf("error closing daemon %q: %w", target, err))
			}
		}
	})
	if len(errs) > 0 {
		return fmt.Errorf("errors closing pool daemons: %v", errs)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/runner -run TestUnifiedProcessPool_SingleflightPrewarming`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/unified_pool.go brain/pkg/runner/unified_pool_test.go
git commit -m "feat(runner): implement UnifiedProcessPool with singleflight pre-warming and target key mapping"
```

---

### Task 5: Pool Session Rotation Thresholds & Hardware Context Compaction

**Files:**
- Modify: `brain/pkg/runner/unified_pool.go`
- Test: `brain/pkg/runner/unified_pool_test.go`

**Interfaces:**
- Produces:
  ```go
  func (p *UnifiedProcessPool) ShouldRotate(d *StreamingDaemon) (bool, string)
  func (p *UnifiedProcessPool) RotateDaemon(ctx context.Context, targetKey string, summary string) (*StreamingDaemon, error)
  ```

- [ ] **Step 1: Write failing unit test for session rotation thresholds**

Add to `brain/pkg/runner/unified_pool_test.go`:
```go
func TestUnifiedProcessPool_ShouldRotate(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, nil)
	d := &StreamingDaemon{
		turnCount: 10,
		state:     StateReady,
	}

	shouldRotate, reason := pool.ShouldRotate(d)
	if !shouldRotate || reason == "" {
		t.Fatalf("expected rotation on turnCount >= 10, got %v (%s)", shouldRotate, reason)
	}

	d.turnCount = 2
	d.stepCount = 185
	shouldRotate, reason = pool.ShouldRotate(d)
	if !shouldRotate || reason == "" {
		t.Fatalf("expected rotation on stepCount >= 180, got %v (%s)", shouldRotate, reason)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/runner -run TestUnifiedProcessPool_ShouldRotate`
Expected: FAIL with undefined `ShouldRotate`

- [ ] **Step 3: Implement rotation evaluation and turn-boundary rotation**

Add to `brain/pkg/runner/unified_pool.go`:
```go
const (
	DefaultMaxSessionTurns   = 10
	DefaultMaxSessionSteps   = 180
	DefaultMaxTranscriptByte = 500 * 1024
)

func (p *UnifiedProcessPool) ShouldRotate(d *StreamingDaemon) (bool, string) {
	if d == nil {
		return false, ""
	}
	if d.InflightCount() > 0 {
		return false, "" // Never rotate during in-flight turn
	}
	if d.TurnCount() >= DefaultMaxSessionTurns {
		return true, fmt.Sprintf("turn count threshold exceeded (%d >= %d)", d.TurnCount(), DefaultMaxSessionTurns)
	}
	if d.StepCount() >= DefaultMaxSessionSteps {
		return true, fmt.Sprintf("step count threshold exceeded (%d >= %d)", d.StepCount(), DefaultMaxSessionSteps)
	}
	return false, ""
}

func (p *UnifiedProcessPool) RotateDaemon(ctx context.Context, targetKey string, summary string) (*StreamingDaemon, error) {
	p.mu.Lock()
	oldDaemon, exists := p.daemons[targetKey]
	delete(p.daemons, targetKey)
	p.mu.Unlock()

	if exists && oldDaemon != nil {
		_ = oldDaemon.Close()
	}

	newDaemon, err := p.GetOrCreate(ctx, targetKey)
	if err != nil {
		return nil, fmt.Errorf("failed creating rotated daemon for %q: %w", targetKey, err)
	}

	return newDaemon, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/runner -run TestUnifiedProcessPool_ShouldRotate`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/unified_pool.go brain/pkg/runner/unified_pool_test.go
git commit -m "feat(runner): implement pool session rotation thresholds and turn-boundary gating"
```

---

### Task 6: Layer 3 Transport Sinks: TurnSink Interface, DiscordTurnSink & ThrowawayTurnSink

**Files:**
- Create: `brain/pkg/queue/sink.go`
- Create: `brain/pkg/queue/sink_test.go`

**Interfaces:**
- Produces:
  ```go
  type DiscordMessageEditor interface {
      ChannelMessageEdit(channelID, messageID, content string) error
      ChannelMessageDelete(channelID, messageID string) error
  }

  type DiscordTurnSink struct {
      // manages intermediate badge, typing heartbeat, and result delivery
  }

  type ThrowawayTurnSink struct {
      // pushes final response to buffered channel
  }

  func NewThrowawayTurnSink() *ThrowawayTurnSink
  func (s *ThrowawayTurnSink) Result() (string, error)
  ```

- [ ] **Step 1: Write failing unit test for `ThrowawayTurnSink` and `DiscordTurnSink`**

Create `brain/pkg/queue/sink_test.go`:
```go
package queue

import (
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestThrowawayTurnSink_ResultDelivery(t *testing.T) {
	sink := NewThrowawayTurnSink()
	sink.OnTurnStarted()
	sink.OnTextDelta("some text")

	expected := &runner.TurnResult{Response: "classified label: casual"}
	go func() {
		time.Sleep(10 * time.Millisecond)
		sink.OnResult(expected)
	}()

	res, err := sink.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "classified label: casual" {
		t.Errorf("expected response %q, got %q", "classified label: casual", res)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/queue -run TestThrowawayTurnSink_ResultDelivery`
Expected: FAIL with undefined `ThrowawayTurnSink`

- [ ] **Step 3: Implement `sink.go` with `ThrowawayTurnSink` and `DiscordTurnSink`**

Create `brain/pkg/queue/sink.go`:
```go
package queue

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/runner"
)

type DiscordMessageEditor interface {
	ChannelMessageEdit(channelID, messageID, content string) error
	ChannelMessageDelete(channelID, messageID string) error
}

type ThrowawayTurnSink struct {
	resCh chan string
	errCh chan error
	once  sync.Once
}

func NewThrowawayTurnSink() *ThrowawayTurnSink {
	return &ThrowawayTurnSink{
		resCh: make(chan string, 1),
		errCh: make(chan error, 1),
	}
}

func (s *ThrowawayTurnSink) OnTurnStarted() {}
func (s *ThrowawayTurnSink) OnThinking()    {}
func (s *ThrowawayTurnSink) OnToolCall(toolName, commandName string) {}
func (s *ThrowawayTurnSink) OnTextDelta(delta string) {}

func (s *ThrowawayTurnSink) OnResult(res *runner.TurnResult) {
	s.once.Do(func() {
		if res != nil {
			s.resCh <- res.Response
		} else {
			s.resCh <- ""
		}
	})
}

func (s *ThrowawayTurnSink) OnError(err error) {
	s.once.Do(func() {
		s.errCh <- err
	})
}

func (s *ThrowawayTurnSink) Result() (string, error) {
	select {
	case res := <-s.resCh:
		return res, nil
	case err := <-s.errCh:
		return "", err
	}
}

type DiscordTurnSink struct {
	editor         DiscordMessageEditor
	channelID      string
	badgeMessageID string
	statusUpdater  *StatusUpdater
	stopTyping     func()
	onComplete     func(res *runner.TurnResult)
	onError        func(err error)

	mu       sync.Mutex
	response strings.Builder
}

func NewDiscordTurnSink(editor DiscordMessageEditor, chID, badgeMsgID string, onComplete func(res *runner.TurnResult), onError func(err error)) *DiscordTurnSink {
	return &DiscordTurnSink{
		editor:         editor,
		channelID:      chID,
		badgeMessageID: badgeMsgID,
		onComplete:     onComplete,
		onError:        onError,
	}
}

func (d *DiscordTurnSink) OnTurnStarted() {}
func (d *DiscordTurnSink) OnThinking()    {}

func (d *DiscordTurnSink) OnToolCall(toolName, commandName string) {
	if d.editor != nil && d.badgeMessageID != "" {
		_ = d.editor.ChannelMessageEdit(d.channelID, d.badgeMessageID, "⚙️ Executing "+toolName+"...")
	}
}

func (d *DiscordTurnSink) OnTextDelta(delta string) {
	d.mu.Lock()
	d.response.WriteString(delta)
	d.mu.Unlock()
}

func (d *DiscordTurnSink) OnResult(res *runner.TurnResult) {
	if d.stopTyping != nil {
		d.stopTyping()
	}
	if d.editor != nil && d.badgeMessageID != "" {
		_ = d.editor.ChannelMessageDelete(d.channelID, d.badgeMessageID)
	}
	if d.onComplete != nil {
		d.onComplete(res)
	}
}

func (d *DiscordTurnSink) OnError(err error) {
	if d.stopTyping != nil {
		d.stopTyping()
	}
	if d.editor != nil && d.badgeMessageID != "" {
		_ = d.editor.ChannelMessageDelete(d.channelID, d.badgeMessageID)
	}
	if d.onError != nil {
		d.onError(err)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/queue -run TestThrowawayTurnSink_ResultDelivery`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/sink.go brain/pkg/queue/sink_test.go
git commit -m "feat(queue): implement TurnSink, ThrowawayTurnSink and DiscordTurnSink"
```

---

### Task 7: VoiceTurnSink with Sliding Replay Ring Buffer & Re-Binding

**Files:**
- Modify: `brain/pkg/queue/sink.go`
- Test: `brain/pkg/queue/sink_test.go`

**Interfaces:**
- Produces:
  ```go
  type TokenChunk struct {
      Seq   uint64 `json:"seq"`
      Delta string `json:"delta"`
  }

  type WebSocketWriter interface {
      WriteJSON(v any) error
      Close() error
  }

  type VoiceTurnSink struct {
      // manages sliding token replay ring buffer, sequential streaming, and re-binding
  }

  func NewVoiceTurnSink(conn WebSocketWriter, deviceID string) *VoiceTurnSink
  func (v *VoiceTurnSink) Rebind(newConn WebSocketWriter, lastAckSeq uint64) error
  ```

- [ ] **Step 1: Write failing unit test for `VoiceTurnSink` replay buffer and re-binding**

Add to `brain/pkg/queue/sink_test.go`:
```go
type mockWSWriter struct {
	chunks []TokenChunk
	closed bool
}

func (m *mockWSWriter) WriteJSON(v any) error {
	if chunk, ok := v.(TokenChunk); ok {
		m.chunks = append(m.chunks, chunk)
	}
	return nil
}

func (m *mockWSWriter) Close() error {
	m.closed = true
	return nil
}

func TestVoiceTurnSink_RebindAndReplay(t *testing.T) {
	ws1 := &mockWSWriter{}
	sink := NewVoiceTurnSink(ws1, "kiosk")

	sink.OnTurnStarted()
	sink.OnTextDelta("turn ")
	sink.OnTextDelta("on ")
	sink.OnTextDelta("lights")

	if len(ws1.chunks) != 3 {
		t.Fatalf("expected 3 chunks sent on ws1, got %d", len(ws1.chunks))
	}

	// Simulate Wi-Fi drop: ws1 disconnected after receiving chunk 1 (seq=1)
	ws2 := &mockWSWriter{}
	if err := sink.Rebind(ws2, 1); err != nil {
		t.Fatalf("failed to rebind: %v", err)
	}

	// ws2 should receive replayed chunks 2 and 3
	if len(ws2.chunks) != 2 {
		t.Fatalf("expected 2 replayed chunks on ws2, got %d", len(ws2.chunks))
	}
	if ws2.chunks[0].Delta != "on " || ws2.chunks[1].Delta != "lights" {
		t.Errorf("unexpected replayed content: %+v", ws2.chunks)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/queue -run TestVoiceTurnSink_RebindAndReplay`
Expected: FAIL with undefined `VoiceTurnSink`

- [ ] **Step 3: Implement `VoiceTurnSink` with ring buffer**

Add to `brain/pkg/queue/sink.go`:
```go
import (
	"sync/atomic"
)

type TokenChunk struct {
	Seq   uint64 `json:"seq"`
	Delta string `json:"delta"`
}

type WebSocketWriter interface {
	WriteJSON(v any) error
	Close() error
}

type VoiceTurnSink struct {
	deviceID string
	conn     WebSocketWriter
	connMu   sync.Mutex
	seq      atomic.Uint64
	ringMu   sync.RWMutex
	ringBuf  []TokenChunk
	ringCap  int
}

func NewVoiceTurnSink(conn WebSocketWriter, deviceID string) *VoiceTurnSink {
	return &VoiceTurnSink{
		deviceID: deviceID,
		conn:     conn,
		ringBuf:  make([]TokenChunk, 0, 1024),
		ringCap:  1024,
	}
}

func (v *VoiceTurnSink) OnTurnStarted() {}
func (v *VoiceTurnSink) OnThinking()    {}
func (v *VoiceTurnSink) OnToolCall(toolName, commandName string) {}

func (v *VoiceTurnSink) OnTextDelta(delta string) {
	chunk := TokenChunk{
		Seq:   v.seq.Add(1),
		Delta: delta,
	}

	v.ringMu.Lock()
	if len(v.ringBuf) >= v.ringCap {
		v.ringBuf = v.ringBuf[1:]
	}
	v.ringBuf = append(v.ringBuf, chunk)
	v.ringMu.Unlock()

	v.connMu.Lock()
	defer v.connMu.Unlock()
	if v.conn != nil {
		_ = v.conn.WriteJSON(chunk)
	}
}

func (v *VoiceTurnSink) Rebind(newConn WebSocketWriter, lastAckSeq uint64) error {
	v.connMu.Lock()
	defer v.connMu.Unlock()

	v.conn = newConn

	v.ringMu.RLock()
	defer v.ringMu.RUnlock()

	for _, chunk := range v.ringBuf {
		if chunk.Seq > lastAckSeq {
			if err := v.conn.WriteJSON(chunk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *VoiceTurnSink) OnResult(res *runner.TurnResult) {
	v.connMu.Lock()
	defer v.connMu.Unlock()
	if v.conn != nil {
		_ = v.conn.WriteJSON(map[string]string{"type": "end_of_turn"})
	}
	v.ringMu.Lock()
	v.ringBuf = v.ringBuf[:0]
	v.ringMu.Unlock()
}

func (v *VoiceTurnSink) OnError(err error) {
	v.connMu.Lock()
	defer v.connMu.Unlock()
	if v.conn != nil {
		_ = v.conn.WriteJSON(map[string]string{"type": "error", "error": err.Error()})
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/queue -run TestVoiceTurnSink_RebindAndReplay`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/sink.go brain/pkg/queue/sink_test.go
git commit -m "feat(queue): implement VoiceTurnSink with sliding replay ring buffer and connection re-binding"
```

---

### Task 8: Centralized Quota Lockout Coordination & Gateway Fast Edge Rejection

**Files:**
- Create: `brain/pkg/runner/quota.go`
- Create: `brain/pkg/runner/quota_test.go`

**Interfaces:**
- Produces:
  ```go
  type QuotaCoordinator struct {
      // manages atomic quotaLockedUntil, quotaResumeCh, and jittered unblocking
  }

  func NewQuotaCoordinator() *QuotaCoordinator
  func (q *QuotaCoordinator) SetLockout(until time.Time)
  func (q *QuotaCoordinator) IsLocked() bool
  func (q *QuotaCoordinator) ResumeChannel() <-chan struct{}
  ```

- [ ] **Step 1: Write failing unit test for `QuotaCoordinator`**

Create `brain/pkg/runner/quota_test.go`:
```go
package runner

import (
	"testing"
	"time"
)

func TestQuotaCoordinator_LockoutAndResume(t *testing.T) {
	q := NewQuotaCoordinator()
	if q.IsLocked() {
		t.Errorf("expected not locked initially")
	}

	q.SetLockout(time.Now().Add(100 * time.Millisecond))
	if !q.IsLocked() {
		t.Errorf("expected locked after SetLockout")
	}

	resumeCh := q.ResumeChannel()
	select {
	case <-resumeCh:
		// Resumed
	case <-time.After(500 * time.Millisecond):
		t.Fatal("resumeCh did not fire within deadline")
	}

	if q.IsLocked() {
		t.Errorf("expected unlocked after expiration")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/runner -run TestQuotaCoordinator_LockoutAndResume`
Expected: FAIL with undefined `QuotaCoordinator`

- [ ] **Step 3: Implement `QuotaCoordinator`**

Create `brain/pkg/runner/quota.go`:
```go
package runner

import (
	"sync"
	"sync/atomic"
	"time"
)

type QuotaCoordinator struct {
	quotaLockedUntil atomic.Int64
	resumeCh         chan struct{}
	mu               sync.Mutex
	timer            *time.Timer
}

func NewQuotaCoordinator() *QuotaCoordinator {
	ch := make(chan struct{})
	close(ch)
	return &QuotaCoordinator{
		resumeCh: ch,
	}
}

func (q *QuotaCoordinator) IsLocked() bool {
	return time.Now().Unix() < q.quotaLockedUntil.Load()
}

func (q *QuotaCoordinator) ResumeChannel() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.resumeCh
}

func (q *QuotaCoordinator) SetLockout(until time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.quotaLockedUntil.Store(until.Unix())
	if q.timer != nil {
		q.timer.Stop()
	}

	q.resumeCh = make(chan struct{})
	remaining := time.Until(until)
	if remaining <= 0 {
		close(q.resumeCh)
		return
	}

	q.timer = time.AfterFunc(remaining, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		close(q.resumeCh)
	})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/runner -run TestQuotaCoordinator_LockoutAndResume`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/quota.go brain/pkg/runner/quota_test.go
git commit -m "feat(runner): implement QuotaCoordinator with broadcast resume channel"
```

---

### Task 9: Wire UnifiedProcessPool into WorkerPool and Main

**Files:**
- Modify: `brain/pkg/queue/pool.go`
- Modify: `brain/main.go`

**Interfaces:**
- Consumes: `UnifiedProcessPool` from Task 4, `DiscordTurnSink` & `VoiceTurnSink` from Task 6 & 7.
- Replaces legacy `daemonPool` and `voiceDaemonPool` with `processPool *runner.UnifiedProcessPool`.

- [ ] **Step 1: Write failing unit test for `WorkerPool` with `UnifiedProcessPool`**

Create `brain/pkg/queue/unified_pool_wiring_test.go`:
```go
package queue

import (
	"context"
	"testing"

	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestWorkerPool_UnifiedProcessPoolWiring(t *testing.T) {
	mockSpawner := runner.NewMockDaemonSpawner()
	uPool := runner.NewUnifiedProcessPool(runner.PoolConfig{}, mockSpawner)
	defer uPool.Close()

	pool := &WorkerPool{
		processPool: uPool,
	}

	if pool.ProcessPool() != uPool {
		t.Fatalf("expected pool.ProcessPool() to return injected UnifiedProcessPool")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/queue -run TestWorkerPool_UnifiedProcessPoolWiring`
Expected: FAIL with undefined `processPool` / `ProcessPool()`

- [ ] **Step 3: Update `brain/pkg/queue/pool.go` and `brain/main.go`**

In `brain/pkg/queue/pool.go`:
- Add field `processPool *runner.UnifiedProcessPool` to `WorkerPool`.
- Add method `ProcessPool() *runner.UnifiedProcessPool`.
- Remove legacy ticker executing `CheckMemoryPressureAndEvict()`.

In `brain/main.go`:
- Construct `unifiedPool := runner.NewUnifiedProcessPool(...)`.
- Pass to `WorkerPool`.
- Remove `utilityDaemon := runner.NewUtilityDaemon(...)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/queue -run TestWorkerPool_UnifiedProcessPoolWiring`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/pool.go brain/pkg/queue/unified_pool_wiring_test.go brain/main.go
git commit -m "refactor(queue): wire UnifiedProcessPool into WorkerPool and main"
```

---

### Task 10: Complete Deletion of Legacy Pools, Meminfo Checks & Verification

**Files:**
- Delete: `brain/pkg/queue/daemon_pool.go`
- Delete: `brain/pkg/queue/daemon_pool_test.go`
- Delete: `brain/pkg/runner/utility_daemon.go`
- Delete: `brain/pkg/runner/utility_daemon_test.go`
- Delete: `brain/pkg/runner/utility_worker.go`
- Delete: `brain/pkg/runner/utility_worker_test.go`
- Modify: `brain/pkg/queue/pool.go` (remove `daemonPool` and `voiceDaemonPool` fields)
- Modify: `brain/pkg/queue/coverage_boost_test.go` (remove `meminfoPath` tests)
- Modify: `brain/pkg/queue/voice_test.go` (update voice execution tests to use `processPool`)

- [ ] **Step 1: Delete legacy pool files**

Run commands to remove the 6 legacy files:
```bash
git rm brain/pkg/queue/daemon_pool.go brain/pkg/queue/daemon_pool_test.go
git rm brain/pkg/runner/utility_daemon.go brain/pkg/runner/utility_daemon_test.go
git rm brain/pkg/runner/utility_worker.go brain/pkg/runner/utility_worker_test.go
```

- [ ] **Step 2: Clean up legacy references in remaining test files**

Remove `meminfoPath` tests in `brain/pkg/queue/coverage_boost_test.go`.
Remove references to `p.daemonPool` and `p.voiceDaemonPool` in `brain/pkg/queue/pool.go` and `brain/pkg/queue/worker.go`.

- [ ] **Step 3: Run comprehensive verification script**

Run: `powershell -File scripts/verify.ps1`
Expected: ALL checks pass cleanly with `>= 95.0%` statement coverage.

- [ ] **Step 4: Commit**

```bash
git add -u
git commit -m "refactor: delete legacy DaemonPool, UtilityDaemon, and meminfo inspection"
```
