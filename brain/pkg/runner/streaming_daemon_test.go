package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockCompletedTool struct {
	ToolName  string
	MCPServer string
	Duration  time.Duration
	Status    string
}

type mockTurnSink struct {
	mu                   sync.Mutex
	started              bool
	thinking             int
	stepStarts           []int
	toolCalls            []string
	completedTools       []string
	completedToolDetails []mockCompletedTool
	activatedSkills      []string
	deltas               []string
	result               *TurnResult
	err                  error
	done                 chan struct{}
}

func newMockTurnSink() *mockTurnSink {
	return &mockTurnSink{done: make(chan struct{})}
}

func (s *mockTurnSink) OnTurnStarted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = true
}

func (s *mockTurnSink) OnThinking() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.thinking++
}

func (s *mockTurnSink) OnStepStarted(stepIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stepStarts = append(s.stepStarts, stepIndex)
}

func (s *mockTurnSink) OnToolCall(toolName, commandName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.toolCalls = append(s.toolCalls, toolName+":"+commandName)
}

func (s *mockTurnSink) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completedTools = append(s.completedTools, toolName+":"+mcpServer+":"+status)
	s.completedToolDetails = append(s.completedToolDetails, mockCompletedTool{
		ToolName:  toolName,
		MCPServer: mcpServer,
		Duration:  duration,
		Status:    status,
	})
}

func (s *mockTurnSink) OnSkillActivated(skillName, source string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activatedSkills = append(s.activatedSkills, skillName+":"+source)
}

func (s *mockTurnSink) OnTextDelta(delta string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deltas = append(s.deltas, delta)
}

func (s *mockTurnSink) OnResult(res *TurnResult) {
	s.mu.Lock()
	s.result = res
	if s.done != nil {
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	}
	s.mu.Unlock()
}

func (s *mockTurnSink) OnError(err error) {
	s.mu.Lock()
	s.err = err
	if s.done != nil {
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	}
	s.mu.Unlock()
}

func TestStreamingDaemon_HandshakeSuccess(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()

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
	defer closeQuietly(inR)
	defer closeQuietly(errW)
	defer daemon.Close()

	if daemon.State() != StateReady {
		t.Errorf("expected state %s, got %s", StateReady, daemon.State())
	}
	if daemon.SessionID() != "00000000-0000-0000-0000-000000000001" {
		t.Errorf("expected latched session ID, got %s", daemon.SessionID())
	}
	if daemon.TurnCount() != 0 {
		t.Errorf("expected 0 turns, got %d", daemon.TurnCount())
	}
	if daemon.StepCount() != 0 {
		t.Errorf("expected 0 steps, got %d", daemon.StepCount())
	}
	if daemon.TaskTracker() == nil {
		t.Error("expected non-nil TaskTracker")
	}
	if time.Since(daemon.LastUsed()) > 5*time.Second {
		t.Errorf("last used timestamp is too old: %v", daemon.LastUsed())
	}
}

func TestStreamingDaemon_HandshakeTimeout(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(outR)
	defer closeQuietly(outW)
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 888, killErr: errors.New("kill error on startup")}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	cfg := DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000001",
		Timeout:   50 * time.Millisecond,
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err == nil {
		if daemon != nil {
			_ = daemon.Close()
		}
		t.Fatal("expected handshake timeout error, got nil")
	}

	if !strings.Contains(err.Error(), "timed out waiting for init event") {
		t.Errorf("unexpected error message: %v", err)
	}

	mockHandle.mu.Lock()
	killed := mockHandle.killed
	mockHandle.mu.Unlock()
	if !killed {
		t.Error("expected process to be killed on handshake timeout")
	}
}

func TestStreamingDaemon_HandshakeStdoutEOF(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 777}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	// Close stdout immediately
	closeQuietly(outW)

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err == nil {
		if daemon != nil {
			_ = daemon.Close()
		}
		t.Fatal("expected stdout closed error, got nil")
	}

	if !strings.Contains(err.Error(), "daemon stdout closed before init event") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestStreamingDaemon_HandshakeMalformedInit(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{
			name: "invalid json",
			line: "not json at all\n",
		},
		{
			name: "missing session_id",
			line: `{"event":"init"}` + "\n",
		},
		{
			name: "not an init event",
			line: `{"event":"other","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n",
		},
		{
			name: "invalid uuid in init event",
			line: `{"event":"init","session_id":"not-a-valid-uuid"}` + "\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, errW := io.Pipe()
			defer closeQuietly(inR)
			defer closeQuietly(errW)

			mock := &MockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
					return inW, outR, errR, &MockProcessHandle{pid: 666}, nil
				},
			}

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(tc.line))
			}()

			cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
			daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
			if err == nil {
				if daemon != nil {
					_ = daemon.Close()
				}
				t.Fatal("expected error parsing init event, got nil")
			}

			if !strings.Contains(err.Error(), "failed parsing init event") {
				t.Errorf("expected 'failed parsing init event' in error, got: %v", err)
			}
		})
	}
}

func TestStreamingDaemon_SpawnerError(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("spawn binary not found")
		},
	}

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err == nil {
		if daemon != nil {
			_ = daemon.Close()
		}
		t.Fatal("expected spawn error, got nil")
	}

	if !strings.Contains(err.Error(), "failed spawning daemon") {
		t.Errorf("unexpected spawn error message: %v", err)
	}
}

func TestStreamingDaemon_ContextCancelledDuringHandshake(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(outR)
	defer closeQuietly(outW)
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 555}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(ctx, cfg, mock)
	if err == nil {
		if daemon != nil {
			_ = daemon.Close()
		}
		t.Fatal("expected cancellation error, got nil")
	}

	if !strings.Contains(err.Error(), "cancelled") {
		t.Errorf("expected cancellation error, got: %v", err)
	}
}

func TestStreamingDaemon_DispatchEvents(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 444}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	sink := &mockTurnSink{}
	turn := &TurnContext{
		TurnID:    "turn-1",
		Prompt:    "hello",
		Sink:      sink,
		CreatedAt: time.Now().Add(-100 * time.Millisecond),
	}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn)
	daemon.inflightMu.Unlock()

	// Write step_update events
	_, _ = outW.Write([]byte(`{"event":"step_update","thinking":true}` + "\n"))
	_, _ = outW.Write([]byte(`{"event":"step_update","delta":"chunk1"}` + "\n"))
	_, _ = outW.Write([]byte(`{"event":"step_update","tool_call":{"name":"bash","command":"ls -la"}}` + "\n"))
	_, _ = outW.Write([]byte(`{"event":"step_update","delta":"chunk2"}` + "\n"))

	// Write malformed line (should not crash reader loop)
	_, _ = outW.Write([]byte("invalid json line\n"))

	// Write result event
	_, _ = outW.Write([]byte(`{"event":"result","result":{"response":"completed answer"}}` + "\n"))

	// Wait for result to be received by sink
	deadline := time.Now().Add(2 * time.Second)
	for {
		sink.mu.Lock()
		res := sink.result
		sink.mu.Unlock()
		if res != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for result event")
		}
		time.Sleep(10 * time.Millisecond)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if sink.thinking != 1 {
		t.Errorf("expected 1 thinking event, got %d", sink.thinking)
	}
	if len(sink.deltas) != 2 || sink.deltas[0] != "chunk1" || sink.deltas[1] != "chunk2" {
		t.Errorf("unexpected deltas: %v", sink.deltas)
	}
	if len(sink.toolCalls) != 1 || sink.toolCalls[0] != "bash:ls -la" {
		t.Errorf("unexpected tool calls: %v", sink.toolCalls)
	}
	if sink.result == nil || sink.result.Response != "completed answer" {
		t.Errorf("unexpected result: %+v", sink.result)
	}
	if daemon.StepCount() != 4 {
		t.Errorf("expected 4 steps, got %d", daemon.StepCount())
	}
	if daemon.TurnCount() != 1 {
		t.Errorf("expected 1 turn, got %d", daemon.TurnCount())
	}
	if daemon.State() != StateReady {
		t.Errorf("expected StateReady, got %s", daemon.State())
	}
}

func TestStreamingDaemon_DispatchResultYieldWaiting(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 444}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	// Register an active task in task tracker
	daemon.TaskTracker().Add(TaskMetadata{
		TaskID:      "bg-1",
		CommandLine: "run long task",
		StartedAt:   time.Now(),
	})

	sink := &mockTurnSink{}
	turn := &TurnContext{
		TurnID:    "turn-1",
		Prompt:    "start background task",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn)
	daemon.inflightMu.Unlock()

	_, _ = outW.Write([]byte(`{"event":"result","response":"task started"}` + "\n"))

	deadline := time.Now().Add(2 * time.Second)
	for {
		sink.mu.Lock()
		res := sink.result
		sink.mu.Unlock()
		if res != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for result event")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if daemon.State() != StateYieldWaiting {
		t.Errorf("expected StateYieldWaiting when active background tasks exist, got %s", daemon.State())
	}
	if sink.result.Response != "task started" {
		t.Errorf("expected response 'task started', got %q", sink.result.Response)
	}
}

func TestStreamingDaemon_UnexpectedStdoutEOFDuringInflight(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 333}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	sink := &mockTurnSink{}
	turn := &TurnContext{
		TurnID:    "turn-eof",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn)
	daemon.inflightMu.Unlock()

	// Unexpectedly close stdout pipe
	closeQuietly(outW)

	deadline := time.Now().Add(2 * time.Second)
	for {
		sink.mu.Lock()
		sinkErr := sink.err
		sink.mu.Unlock()
		if sinkErr != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for EOF error callback on sink")
		}
		time.Sleep(10 * time.Millisecond)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if !strings.Contains(sink.err.Error(), "unexpected EOF") {
		t.Errorf("unexpected sink error: %v", sink.err)
	}
	if daemon.State() != StateClosed {
		t.Errorf("expected StateClosed on stdout EOF, got %s", daemon.State())
	}
}

func TestStreamingDaemon_CloseWhileInflight(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 222}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}

	sink := &mockTurnSink{}
	turn := &TurnContext{
		TurnID:    "turn-close",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn)
	daemon.inflightMu.Unlock()

	// Close daemon
	if err := daemon.Close(); err != nil {
		t.Fatalf("unexpected error closing daemon: %v", err)
	}
	// Calling Close() again should be idempotent
	if err := daemon.Close(); err != nil {
		t.Fatalf("idempotent close returned error: %v", err)
	}

	closeQuietly(outW)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.err == nil || !strings.Contains(sink.err.Error(), "daemon closed while turn was in-flight") {
		t.Errorf("expected 'daemon closed while turn was in-flight' error, got %v", sink.err)
	}
	if daemon.State() != StateClosed {
		t.Errorf("expected StateClosed, got %s", daemon.State())
	}

	mockHandle.mu.Lock()
	killed := mockHandle.killed
	mockHandle.mu.Unlock()
	if !killed {
		t.Error("expected process handle to be killed on Close")
	}
}

func TestCloseStreamQuietly(t *testing.T) {
	// Should not panic on nil
	closeStreamQuietly(nil, "nil-stream")

	// Should handle close error gracefully
	ec := &errCloser{err: errors.New("stream close failure")}
	closeStreamQuietly(ec, "err-stream")
}

func TestStreamingDaemon_CloseProcessKillError(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 111, killErr: errors.New("kill failed")}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}

	closeQuietly(outW)

	closeErr := daemon.Close()
	if closeErr == nil || !strings.Contains(closeErr.Error(), "kill failed") {
		t.Errorf("expected kill failed error from Close(), got %v", closeErr)
	}
}

func TestStreamingDaemon_NilSpawnerFallback(t *testing.T) {
	cfg := DaemonConfig{
		AgyBin:    "nonexistent-executable-binary-12345",
		SessionID: "00000000-0000-0000-0000-000000000001",
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, nil)
	if err == nil {
		if daemon != nil {
			_ = daemon.Close()
		}
		t.Fatal("expected spawn error with nil spawner and bogus binary, got nil")
	}
	if !strings.Contains(err.Error(), "failed spawning daemon") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestStreamingDaemon_DispatchEdgeCases(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 123}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	// 1. Dispatch with empty lines (no-op)
	daemon.dispatchNDJSONLine("")
	daemon.dispatchNDJSONLine("   \n")

	// 2. Dispatch with no active turns (no-op)
	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"orphan"}`)

	// 3. Dispatch with active turn but nil sink (no-op)
	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, &TurnContext{TurnID: "nil-sink", Sink: nil})
	daemon.inflightMu.Unlock()
	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"no-sink"}`)

	// Clear inflight
	daemon.inflightMu.Lock()
	daemon.inflight = nil
	daemon.inflightMu.Unlock()

	// 4. Dispatch unhandled event
	sink := &mockTurnSink{}
	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, &TurnContext{TurnID: "t-1", Sink: sink})
	daemon.inflightMu.Unlock()
	daemon.dispatchNDJSONLine(`{"event":"unknown_event_type"}`)

	// 5. Dispatch step_update with empty delta
	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":""}`)
	if len(sink.deltas) != 0 {
		t.Errorf("expected 0 deltas for empty string, got %d", len(sink.deltas))
	}

	// 5b. Dispatch tool start and completion without step_index
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","tool_name":"no_idx_tool"}}`)
	if len(sink.toolCalls) != 1 || sink.toolCalls[0] != "no_idx_tool:" {
		t.Errorf("expected tool start for no_idx_tool, got %+v", sink.toolCalls)
	}
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","tool_name":"no_idx_tool"}}`)
	if len(sink.completedToolDetails) != 1 || sink.completedToolDetails[0].ToolName != "no_idx_tool" {
		t.Errorf("expected tool completion for no_idx_tool, got %+v", sink.completedToolDetails)
	}



	// 6. Test extractResponseString with different shapes
	if res := extractResponseString(map[string]any{}); res != "" {
		t.Errorf("expected empty string for empty map, got %q", res)
	}
	if res := extractResponseString(map[string]any{"result": map[string]any{"response": "nested"}}); res != "nested" {
		t.Errorf("expected 'nested', got %q", res)
	}
	if res := extractResponseString(map[string]any{"response": "flat"}); res != "flat" {
		t.Errorf("expected 'flat', got %q", res)
	}
	if res := extractResponseString(map[string]any{"result": "not-a-map"}); res != "" {
		t.Errorf("expected empty string for non-map result, got %q", res)
	}
}

func TestStreamingDaemon_ResultAfterCloseDoesNotReopenState(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 456}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}

	sink := &mockTurnSink{}
	turn := &TurnContext{
		TurnID:    "turn-close-race",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn)
	daemon.inflightMu.Unlock()

	// Explicitly close the daemon
	if err := daemon.Close(); err != nil {
		t.Fatalf("failed to close daemon: %v", err)
	}
	closeQuietly(outW)

	if daemon.State() != StateClosed {
		t.Fatalf("expected StateClosed after Close(), got %s", daemon.State())
	}

	// Dispatch late-arriving result event
	daemon.dispatchNDJSONLine(`{"event":"result","result":{"response":"late result"}}`)

	// Daemon must remain StateClosed
	if daemon.State() != StateClosed {
		t.Errorf("daemon state regressed from StateClosed to %s on late result event", daemon.State())
	}
}

func TestStreamingDaemon_NilSinkCleanlyPopsOnResult(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 789}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	// Turn 1: nil sink (e.g. fire-and-forget or background turn)
	turn1 := &TurnContext{
		TurnID:    "turn-nil-sink",
		Sink:      nil,
		CreatedAt: time.Now(),
	}

	// Turn 2: active sink
	sink2 := &mockTurnSink{}
	turn2 := &TurnContext{
		TurnID:    "turn-with-sink",
		Sink:      sink2,
		CreatedAt: time.Now(),
	}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn1, turn2)
	daemon.inflightMu.Unlock()

	// Dispatch step_update for turn 1 with nil sink (should not panic or stall)
	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"nil-sink-delta"}`)

	// Dispatch result for turn 1 (must pop turn1 from queue even though sink is nil)
	daemon.dispatchNDJSONLine(`{"event":"result","result":{"response":"turn1 answer"}}`)

	daemon.inflightMu.Lock()
	remaining := len(daemon.inflight)
	var headTurnID string
	if remaining > 0 {
		headTurnID = daemon.inflight[0].TurnID
	}
	daemon.inflightMu.Unlock()

	if remaining != 1 {
		t.Fatalf("expected 1 turn remaining in inflight queue, got %d", remaining)
	}
	if headTurnID != "turn-with-sink" {
		t.Fatalf("expected head of queue to be turn-with-sink, got %s", headTurnID)
	}
	if daemon.TurnCount() != 1 {
		t.Errorf("expected turnCount to be 1, got %d", daemon.TurnCount())
	}

	// Now dispatch step_update and result for turn 2
	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"turn2-delta"}`)
	daemon.dispatchNDJSONLine(`{"event":"result","result":{"response":"turn2 answer"}}`)

	sink2.mu.Lock()
	defer sink2.mu.Unlock()

	if len(sink2.deltas) != 1 || sink2.deltas[0] != "turn2-delta" {
		t.Errorf("expected turn2 deltas, got %v", sink2.deltas)
	}
	if sink2.result == nil || sink2.result.Response != "turn2 answer" {
		t.Errorf("expected turn2 result, got %+v", sink2.result)
	}
	if daemon.TurnCount() != 2 {
		t.Errorf("expected turnCount to be 2, got %d", daemon.TurnCount())
	}
}

func TestStreamingDaemon_StderrClosedOnClose(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 321}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}

	// Write stderr data
	go func() {
		_, _ = errW.Write([]byte("some stderr log\n"))
		closeQuietly(errW)
	}()

	// Closing daemon should close stderrCloser cleanly without error
	if err := daemon.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
	closeQuietly(outW)
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

	go func() {
		_, _ = io.Copy(io.Discard, inR)
	}()

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

func TestStreamingDaemon_SendClosed(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, _ := io.Pipe()

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 888}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000003"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000003"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed to start daemon: %v", err)
	}
	defer inR.Close()

	if daemon.IsDirty() {
		t.Error("expected fresh daemon not to be dirty")
	}

	if err := daemon.Close(); err != nil {
		t.Fatalf("failed to close daemon: %v", err)
	}

	turn := &TurnContext{TurnID: "turn-closed", Prompt: "test"}
	err = daemon.Send(turn.Prompt, turn)
	if err == nil || !strings.Contains(err.Error(), "cannot send to closed streaming daemon") {
		t.Fatalf("expected 'cannot send to closed streaming daemon', got: %v", err)
	}
}

func TestStreamingDaemon_SendStdinWriteError(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, _ := io.Pipe()

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 888}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000004"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000004"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed to start daemon: %v", err)
	}
	defer daemon.Close()

	// Close stdin pipe so writing fails
	_ = inR.Close()
	_ = inW.Close()

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-write-err", Prompt: "fail", Sink: sink}
	err = daemon.Send(turn.Prompt, turn)
	if err == nil || !strings.Contains(err.Error(), "failed writing prompt to daemon stdin") {
		t.Fatalf("expected write error, got: %v", err)
	}

	if !daemon.IsDirty() {
		t.Error("expected daemon to be marked dirty after stdin write error")
	}
	if daemon.State() != StateClosed {
		t.Errorf("expected daemon state %s after write error, got %s", StateClosed, daemon.State())
	}
}

func TestStreamingDaemon_SendConcurrency(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, _ := io.Pipe()

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 888}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000005"}` + "\n"))
	}()

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000005"}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed to start daemon: %v", err)
	}
	defer inR.Close()
	defer daemon.Close()

	go func() {
		_, _ = io.Copy(io.Discard, inR)
	}()

	const concurrency = 20
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sink := newMockTurnSink()
			turn := &TurnContext{TurnID: fmt.Sprintf("turn-%d", idx), Sink: sink, CreatedAt: time.Now()}
			if sendErr := daemon.Send(fmt.Sprintf("prompt-%d", idx), turn); sendErr != nil {
				errCh <- sendErr
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for sendErr := range errCh {
		t.Errorf("concurrent send failed: %v", sendErr)
	}

	if daemon.InflightCount() != concurrency {
		t.Errorf("expected %d inflight turns, got %d", concurrency, daemon.InflightCount())
	}
}

func TestStreamingDaemon_StepUpdateNestedTextDeltaAndThinking(t *testing.T) {
	t.Parallel()
	daemon := &StreamingDaemon{
		sessionID: "test-nested-deltas",
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "t-1",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	daemon.inflight = append(daemon.inflight, turn)

	// Dispatch nested text_delta inside step_update
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","text_delta":"nested chunk 1"}}`)
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","delta":"nested chunk 2"}}`)
	daemon.dispatchNDJSONLine(`{"event":"step_update","text_delta":"root text_delta"}`)
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","thinking":"internal thought"}}`)

	if len(sink.deltas) != 3 {
		t.Fatalf("expected 3 deltas, got %d: %v", len(sink.deltas), sink.deltas)
	}
	if sink.deltas[0] != "nested chunk 1" || sink.deltas[1] != "nested chunk 2" || sink.deltas[2] != "root text_delta" {
		t.Errorf("unexpected deltas: %v", sink.deltas)
	}
	if sink.thinking != 1 {
		t.Errorf("expected 1 thinking event, got %d", sink.thinking)
	}
}

func TestStreamingDaemon_OnTurnFinishedCallback(t *testing.T) {
	daemon := &StreamingDaemon{
		sessionID: "test-on-turn-finished",
		state:     StateExecuting,
	}

	cbCalled := make(chan *StreamingDaemon, 1)
	daemon.SetOnTurnFinished(func(d *StreamingDaemon) {
		cbCalled <- d
	})

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "t-1",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	daemon.inflight = append(daemon.inflight, turn)

	// Dispatch result event
	daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"done"}}`)

	select {
	case d := <-cbCalled:
		if d != daemon {
			t.Errorf("expected callback with daemon instance %p, got %p", daemon, d)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for onTurnFinished callback")
	}

	// Also verify that if hasMoreInflight == true, callback is NOT invoked
	var secondCbCalled atomic.Bool
	daemon.SetOnTurnFinished(func(d *StreamingDaemon) {
		secondCbCalled.Store(true)
	})

	turn1 := &TurnContext{TurnID: "t-1", Sink: newMockTurnSink(), CreatedAt: time.Now()}
	turn2 := &TurnContext{TurnID: "t-2", Sink: newMockTurnSink(), CreatedAt: time.Now()}
	daemon.inflight = []*TurnContext{turn1, turn2}

	daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"done-1"}}`)
	time.Sleep(50 * time.Millisecond)
	if secondCbCalled.Load() {
		t.Error("expected onTurnFinished NOT to be called when inflight turns remain")
	}
}

func TestStreamingDaemon_ResultWithErrorStatusAndSubstantiveResponse(t *testing.T) {
	daemon := &StreamingDaemon{}

	// Case 1: Substantive response present despite status="ERROR" (e.g. agy internal transient retry)
	sinkWithResp := newMockTurnSink()
	turn1 := &TurnContext{
		TurnID:    "t-err-with-resp",
		Sink:      sinkWithResp,
		CreatedAt: time.Now(),
	}
	daemon.inflight = []*TurnContext{turn1}

	resultLineWithResp := `{"event":"result","result":{"conversation_id":"conv-1","status":"ERROR","error":"API error (attempt 2): RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 1s.","response":"Here is the full and complete answer to your query."}}`
	daemon.dispatchNDJSONLine(resultLineWithResp)

	select {
	case <-sinkWithResp.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for sink completion")
	}

	sinkWithResp.mu.Lock()
	if sinkWithResp.err != nil {
		t.Errorf("expected nil error when substantive response is present, got %v", sinkWithResp.err)
	}
	if sinkWithResp.result == nil {
		t.Fatal("expected non-nil result when substantive response is present")
	}
	if sinkWithResp.result.Response != "Here is the full and complete answer to your query." {
		t.Errorf("unexpected response: %q", sinkWithResp.result.Response)
	}
	sinkWithResp.mu.Unlock()

	// Case 2: Empty response with status="ERROR" should legitimately fail via OnError
	sinkEmptyResp := newMockTurnSink()
	turn2 := &TurnContext{
		TurnID:    "t-err-empty-resp",
		Sink:      sinkEmptyResp,
		CreatedAt: time.Now(),
	}
	daemon.inflight = []*TurnContext{turn2}

	resultLineEmptyResp := `{"event":"result","result":{"conversation_id":"conv-2","status":"ERROR","error":"hard model failure","response":""}}`
	daemon.dispatchNDJSONLine(resultLineEmptyResp)

	select {
	case <-sinkEmptyResp.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for sink completion")
	}

	sinkEmptyResp.mu.Lock()
	if sinkEmptyResp.err == nil || !strings.Contains(sinkEmptyResp.err.Error(), "hard model failure") {
		t.Errorf("expected hard model failure error when response is empty, got %v", sinkEmptyResp.err)
	}
	if sinkEmptyResp.result != nil {
		t.Errorf("expected nil result when response is empty, got %+v", sinkEmptyResp.result)
	}
	sinkEmptyResp.mu.Unlock()

	// Case 3: Flat format with lowercase status="error" and substantive response
	sinkFlatResult := newMockTurnSink()
	turn3 := &TurnContext{
		TurnID:    "t-err-flat-result",
		Sink:      sinkFlatResult,
		CreatedAt: time.Now(),
	}
	daemon.inflight = []*TurnContext{turn3}

	flatLineResult := `{"event":"result","status":"error","error":"429 quota exhaustion","response":"Here is the flat result string."}`
	daemon.dispatchNDJSONLine(flatLineResult)

	select {
	case <-sinkFlatResult.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for sink completion")
	}

	sinkFlatResult.mu.Lock()
	if sinkFlatResult.err != nil {
		t.Errorf("expected nil error for flat result string with error status, got %v", sinkFlatResult.err)
	}
	if sinkFlatResult.result == nil || sinkFlatResult.result.Response != "Here is the flat result string." {
		t.Errorf("unexpected result: %+v", sinkFlatResult.result)
	}
	sinkFlatResult.mu.Unlock()

	// Case 4: Nested format with lowercase status="error" and response in "result"
	sinkNestedResult := newMockTurnSink()
	turn4 := &TurnContext{
		TurnID:    "t-err-nested-result",
		Sink:      sinkNestedResult,
		CreatedAt: time.Now(),
	}
	daemon.inflight = []*TurnContext{turn4}

	nestedLineResult := `{"event":"result","result":{"status":"error","error":"model retry notice","result":"Here is nested result string."}}`
	daemon.dispatchNDJSONLine(nestedLineResult)

	select {
	case <-sinkNestedResult.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for sink completion")
	}

	sinkNestedResult.mu.Lock()
	if sinkNestedResult.err != nil {
		t.Errorf("expected nil error for nested result with error status, got %v", sinkNestedResult.err)
	}
	if sinkNestedResult.result == nil || sinkNestedResult.result.Response != "Here is nested result string." {
		t.Errorf("unexpected result: %+v", sinkNestedResult.result)
	}
	sinkNestedResult.mu.Unlock()
}

func TestStreamingDaemon_DefaultHandshakeTimeout(t *testing.T) {
	if DefaultHandshakeTimeout != 30*time.Second {
		t.Fatalf("expected DefaultHandshakeTimeout to be 30s, got %v", DefaultHandshakeTimeout)
	}

	// Verify that when cfg.Timeout is 0 (default), StartStreamingDaemon allows handshakes taking longer than old 5s limit
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 999}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	go func() {
		// Emit init event after a slight delay
		time.Sleep(100 * time.Millisecond)
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"00000000-0000-0000-0000-000000000099\"}\n"))
	}()

	cfg := DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000099",
		Timeout:   0, // Default timeout (30s)
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("expected successful daemon startup with default 30s timeout, got error: %v", err)
	}
	if daemon == nil {
		t.Fatal("expected non-nil daemon")
	}
	defer daemon.Close()

	if daemon.SessionID() != "00000000-0000-0000-0000-000000000099" {
		t.Errorf("expected session ID 00000000-0000-0000-0000-000000000099, got: %s", daemon.SessionID())
	}
}

func TestStreamingDaemon_TranscriptRescue_EmptyResponse(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 1001}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	go func() {
		_, _ = io.Copy(io.Discard, inR)
	}()
	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000101\"}\n"))
	}()

	rescuedCalled := false
	cfg := DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000101",
		TranscriptRescuer: func(convID string, since time.Time) string {
			rescuedCalled = true
			return "Rescued substantive response from transcript"
		},
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	sink := newMockTurnSink()
	turnCtx := &TurnContext{
		TurnID:    "t-1",
		Prompt:    "hello",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send("hello", turnCtx); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// Emit result event with empty response
	_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"response\":\"\"}}\n"))

	select {
	case <-sink.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sink result")
	}

	if !rescuedCalled {
		t.Errorf("expected TranscriptRescuer to be called")
	}
	if sink.err != nil {
		t.Errorf("expected nil error, got %v", sink.err)
	}
	if sink.result == nil || sink.result.Response != "Rescued substantive response from transcript" {
		t.Errorf("expected rescued response, got %+v", sink.result)
	}
}

func TestStreamingDaemon_TranscriptRescue_ErrorSuppression(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 1002}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	go func() {
		_, _ = io.Copy(io.Discard, inR)
	}()
	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000102\"}\n"))
	}()

	cfg := DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000102",
		TranscriptRescuer: func(convID string, since time.Time) string {
			return "Rescued substantive response after error"
		},
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	sink := newMockTurnSink()
	turnCtx := &TurnContext{
		TurnID:    "t-2",
		Prompt:    "hello",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send("hello", turnCtx); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// Emit ERROR event
	_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":\"quota exhausted\"}}\n"))

	select {
	case <-sink.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sink result")
	}

	if sink.err != nil {
		t.Errorf("expected error to be suppressed when rescued, got %v", sink.err)
	}
	if sink.result == nil || sink.result.Response != "Rescued substantive response after error" {
		t.Errorf("expected rescued response, got %+v", sink.result)
	}
}

func TestStreamingDaemon_TranscriptRescue_EOFRescue(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 1003}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	go func() {
		_, _ = io.Copy(io.Discard, inR)
	}()
	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000103\"}\n"))
	}()

	cfg := DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000103",
		TranscriptRescuer: func(convID string, since time.Time) string {
			return "Rescued substantive response on EOF"
		},
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	sink := newMockTurnSink()
	turnCtx := &TurnContext{
		TurnID:    "t-3",
		Prompt:    "hello",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send("hello", turnCtx); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// Close stdout to trigger unexpected EOF
	_ = outW.Close()

	select {
	case <-sink.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sink result")
	}

	if sink.err != nil {
		t.Errorf("expected error to be suppressed on EOF when rescued, got %v", sink.err)
	}
	if sink.result == nil || sink.result.Response != "Rescued substantive response on EOF" {
		t.Errorf("expected rescued response on EOF, got %+v", sink.result)
	}
}

func TestStreamingDaemon_TranscriptRescue_FallbackOnError(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 1004}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	go func() {
		_, _ = io.Copy(io.Discard, inR)
	}()
	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000104\"}\n"))
	}()

	cfg := DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000104",
		TranscriptRescuer: func(convID string, since time.Time) string {
			return "" // No substantive response available in transcript
		},
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	// Case 1: Result event with ERROR and empty response falls back to OnError
	sink1 := newMockTurnSink()
	turn1 := &TurnContext{
		TurnID:    "t-fallback-1",
		Prompt:    "hello",
		Sink:      sink1,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send("hello", turn1); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":\"quota limit reached\",\"response\":\"\"}}\n"))

	select {
	case <-sink1.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sink1 error")
	}

	if sink1.err == nil || !strings.Contains(sink1.err.Error(), "quota limit reached") {
		t.Errorf("expected quota limit reached error, got %v", sink1.err)
	}
	if sink1.result != nil {
		t.Errorf("expected nil result on unrescued error, got %+v", sink1.result)
	}

	// Case 2: Unexpected EOF when TranscriptRescuer returns "" falls back to OnError
	sink2 := newMockTurnSink()
	turn2 := &TurnContext{
		TurnID:    "t-fallback-2",
		Prompt:    "hello again",
		Sink:      sink2,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send("hello again", turn2); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	_ = outW.Close()

	select {
	case <-sink2.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sink2 error")
	}

	if sink2.err == nil || !strings.Contains(sink2.err.Error(), "unexpected EOF") {
		t.Errorf("expected unexpected EOF error on unrescued EOF, got %v", sink2.err)
	}
	if sink2.result != nil {
		t.Errorf("expected nil result on unrescued EOF, got %+v", sink2.result)
	}
}

func TestStreamingDaemon_SetTranscriptRescuer(t *testing.T) {
	t.Parallel()

	var nilDaemon *StreamingDaemon
	nilDaemon.SetTranscriptRescuer(nil)

	d := &StreamingDaemon{}
	var called bool
	rescuer := func(convID string, since time.Time) string {
		called = true
		return "rescued"
	}
	d.SetTranscriptRescuer(rescuer)
	if d.cfg.TranscriptRescuer == nil {
		t.Fatalf("expected TranscriptRescuer to be set")
	}
	d.cfg.TranscriptRescuer("conv", time.Now())
	if !called {
		t.Fatalf("expected rescuer function to be executed")
	}
}

func TestExtractResponseStringAndPopulateUsage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw  map[string]any
		want string
	}{
		{raw: map[string]any{"result": map[string]any{"response": "res-resp"}}, want: "res-resp"},
		{raw: map[string]any{"result": map[string]any{"result": "res-res"}}, want: "res-res"},
		{raw: map[string]any{"result": map[string]any{"content": "res-content"}}, want: "res-content"},
		{raw: map[string]any{"result": map[string]any{"text": "res-text"}}, want: "res-text"},
		{raw: map[string]any{"response": "root-resp"}, want: "root-resp"},
		{raw: map[string]any{"content": "root-content"}, want: "root-content"},
		{raw: map[string]any{"text": "root-text"}, want: "root-text"},
		{raw: map[string]any{"unknown": "val"}, want: ""},
		{raw: nil, want: ""},
	}

	for _, tc := range cases {
		got := extractResponseString(tc.raw)
		if got != tc.want {
			t.Errorf("extractResponseString(%v) = %q, want %q", tc.raw, got, tc.want)
		}
	}

	// Test populateUsage edge cases
	res := &TurnResult{}
	populateUsage(nil, nil)
	populateUsage(res, nil)
	populateUsage(nil, map[string]any{"usage": map[string]any{}})

	// Root usage
	populateUsage(res, map[string]any{
		"usage": map[string]any{
			"input_tokens":  float64(10),
			"output_tokens": float64(20),
			"total_tokens":  float64(30),
		},
	})
	if res.Usage.InputTokens != 10 || res.Usage.OutputTokens != 20 || res.Usage.TotalTokens != 30 {
		t.Errorf("unexpected usage from root: %+v", res.Usage)
	}

	// Result nested usage
	res2 := &TurnResult{}
	populateUsage(res2, map[string]any{
		"result": map[string]any{
			"usage": map[string]any{
				"input_tokens":  float64(40),
				"output_tokens": float64(50),
				"total_tokens":  float64(90),
			},
		},
	})
	if res2.Usage.InputTokens != 40 || res2.Usage.OutputTokens != 50 || res2.Usage.TotalTokens != 90 {
		t.Errorf("unexpected usage from nested: %+v", res2.Usage)
	}
}

func TestStreamingDaemon_Send_WithMemoryRetriever(t *testing.T) {
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

	retriever := func(ctx context.Context, query string) (string, error) {
		return "<retrieved_memory>\n- User prefers dark mode\n</retrieved_memory>", nil
	}

	cfg := DaemonConfig{
		SessionID:       "00000000-0000-0000-0000-000000000002",
		MemoryRetriever: retriever,
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed to start daemon: %v", err)
	}
	defer inR.Close()
	defer daemon.Close()

	var receivedStdin bytes.Buffer
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 2048)
		n, _ := inR.Read(buf)
		receivedStdin.Write(buf[:n])
	}()

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-1", Prompt: "Hello Aerial", Sink: sink, CreatedAt: time.Now()}
	if err := daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("failed sending turn: %v", err)
	}

	<-readDone
	var payload streamInputPayload
	if err := json.Unmarshal(receivedStdin.Bytes(), &payload); err != nil {
		t.Fatalf("failed unmarshaling stdin JSON: %v, raw: %s", err, receivedStdin.String())
	}
	if !strings.Contains(payload.Message.Content, "<retrieved_memory>") || !strings.Contains(payload.Message.Content, "User prefers dark mode") {
		t.Errorf("expected stdin to contain retrieved memory, got: %s", payload.Message.Content)
	}
	if !strings.Contains(payload.Message.Content, "Hello Aerial") {
		t.Errorf("expected stdin to contain prompt, got: %s", payload.Message.Content)
	}

	// Verify no double injection if prompt already contains <retrieved_memory>
	calledAgain := false
	daemon.cfg.MemoryRetriever = func(ctx context.Context, query string) (string, error) {
		calledAgain = true
		return "<retrieved_memory>second</retrieved_memory>", nil
	}
	turn2 := &TurnContext{TurnID: "turn-2", Prompt: "<retrieved_memory>first</retrieved_memory>\nHello", Sink: newMockTurnSink(), CreatedAt: time.Now()}
	readDone2 := make(chan struct{})
	var receivedStdin2 bytes.Buffer
	go func() {
		defer close(readDone2)
		buf := make([]byte, 2048)
		n, _ := inR.Read(buf)
		receivedStdin2.Write(buf[:n])
	}()
	if err := daemon.Send(turn2.Prompt, turn2); err != nil {
		t.Fatalf("failed sending turn2: %v", err)
	}
	<-readDone2
	if calledAgain {
		t.Errorf("expected MemoryRetriever not to be called when prompt already has <retrieved_memory>")
	}
}

func TestStreamingDaemon_Send_WithAmbientContextRetriever(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, _ := io.Pipe()

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 889}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000003"}` + "\n"))
	}()

	retriever := func(ctx context.Context) (string, error) {
		return "<ambient_context>\nLiving Room: ON\n</ambient_context>", nil
	}

	cfg := DaemonConfig{
		SessionID:               "00000000-0000-0000-0000-000000000003",
		AmbientContextRetriever: retriever,
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed to start daemon: %v", err)
	}
	defer inR.Close()
	defer daemon.Close()

	var receivedStdin bytes.Buffer
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 2048)
		n, _ := inR.Read(buf)
		receivedStdin.Write(buf[:n])
	}()

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-amb-1", Prompt: "Turn off lights", Sink: sink, CreatedAt: time.Now()}
	if err := daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("failed sending turn: %v", err)
	}

	<-readDone
	var payload streamInputPayload
	if err := json.Unmarshal(receivedStdin.Bytes(), &payload); err != nil {
		t.Fatalf("failed unmarshaling stdin JSON: %v, raw: %s", err, receivedStdin.String())
	}
	if !strings.Contains(payload.Message.Content, "<ambient_context>") || !strings.Contains(payload.Message.Content, "Living Room: ON") {
		t.Errorf("expected stdin to contain ambient context, got: %s", payload.Message.Content)
	}
	if !strings.Contains(payload.Message.Content, "Turn off lights") {
		t.Errorf("expected stdin to contain prompt, got: %s", payload.Message.Content)
	}

	// Verify no double injection
	calledAgain := false
	daemon.cfg.AmbientContextRetriever = func(ctx context.Context) (string, error) {
		calledAgain = true
		return "<ambient_context>second</ambient_context>", nil
	}
	turn2 := &TurnContext{TurnID: "turn-amb-2", Prompt: "<ambient_context>first</ambient_context>\nHello", Sink: newMockTurnSink(), CreatedAt: time.Now()}
	readDone2 := make(chan struct{})
	var receivedStdin2 bytes.Buffer
	go func() {
		defer close(readDone2)
		buf := make([]byte, 2048)
		n, _ := inR.Read(buf)
		receivedStdin2.Write(buf[:n])
	}()
	if err := daemon.Send(turn2.Prompt, turn2); err != nil {
		t.Fatalf("failed sending turn2: %v", err)
	}
	<-readDone2
	if calledAgain {
		t.Errorf("expected AmbientContextRetriever not to be called when prompt already has <ambient_context>")
	}

	// Verify injection on subsequent turns (turnCount > 0) when prompt lacks <ambient_context>
	daemon.mu.Lock()
	daemon.turnCount = 1
	daemon.mu.Unlock()
	calledOnSubsequentTurn := false
	daemon.cfg.AmbientContextRetriever = func(ctx context.Context) (string, error) {
		calledOnSubsequentTurn = true
		return "<ambient_context>subsequent</ambient_context>", nil
	}
	turn3 := &TurnContext{TurnID: "turn-amb-3", Prompt: "Clean prompt turn 3", Sink: newMockTurnSink(), CreatedAt: time.Now()}
	readDone3 := make(chan struct{})
	var receivedStdin3 bytes.Buffer
	go func() {
		defer close(readDone3)
		buf := make([]byte, 2048)
		n, _ := inR.Read(buf)
		receivedStdin3.Write(buf[:n])
	}()
	if err := daemon.Send(turn3.Prompt, turn3); err != nil {
		t.Fatalf("failed sending turn3: %v", err)
	}
	<-readDone3
	if !calledOnSubsequentTurn {
		t.Errorf("expected AmbientContextRetriever to be called on subsequent turn (turnCount > 0)")
	}
	var payload3 streamInputPayload
	if err := json.Unmarshal(receivedStdin3.Bytes(), &payload3); err != nil {
		t.Fatalf("failed unmarshaling stdin JSON: %v, raw: %s", err, receivedStdin3.String())
	}
	expectedContent := "<ambient_context>subsequent</ambient_context>\n\nClean prompt turn 3"
	if payload3.Message.Content != expectedContent {
		t.Errorf("expected turn 3 content %q, got: %s", expectedContent, payload3.Message.Content)
	}
}

func TestStreamingDaemon_ToolAndSkillLifecycleEvents(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 2001}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	go func() {
		_, _ = io.Copy(io.Discard, inR)
	}()
	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000201\"}\n"))
	}()

	cfg := DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000201",
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-tools-1",
		Prompt:    "run tool check",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	// 1. Native tool: run_command RUNNING -> DONE
	_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":1,\"tool_name\":\"run_command\",\"tool_info\":{\"parameters\":{\"CommandLine\":\"echo hello\"}}}}\n"))
	time.Sleep(10 * time.Millisecond)
	_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"DONE\",\"type\":\"tool_call\",\"step_index\":1,\"tool_name\":\"run_command\"}}\n"))

	// 2. MCP tool: call_mcp_tool RUNNING -> DONE
	_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":2,\"tool_name\":\"call_mcp_tool\",\"tool_info\":{\"parameters\":{\"ServerName\":\"github\",\"ToolName\":\"get_me\"}}}}\n"))
	time.Sleep(10 * time.Millisecond)
	_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"DONE\",\"type\":\"tool_call\",\"step_index\":2,\"tool_name\":\"call_mcp_tool\"}}\n"))

	// 3. Skill activation via view_file: RUNNING -> DONE
	_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":3,\"tool_name\":\"view_file\",\"tool_info\":{\"parameters\":{\"AbsolutePath\":\"/share/aerial-config/custom-skills/home-assistant/SKILL.md\"}}}}\n"))
	time.Sleep(10 * time.Millisecond)
	_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"DONE\",\"type\":\"tool_call\",\"step_index\":3,\"tool_name\":\"view_file\"}}\n"))

	// 4. In-flight tool aborted on result
	_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":4,\"tool_name\":\"manage_task\",\"tool_info\":{\"parameters\":{}}}}\n"))
	time.Sleep(10 * time.Millisecond)
	_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"done\"}}\n"))

	select {
	case <-sink.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn completion")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if len(sink.completedToolDetails) != 4 {
		t.Fatalf("expected 4 completed tool records, got %d: %+v", len(sink.completedToolDetails), sink.completedToolDetails)
	}

	// Verify run_command
	c0 := sink.completedToolDetails[0]
	if c0.ToolName != "run_command" || c0.MCPServer != "native" || c0.Status != "ok" {
		t.Errorf("unexpected record 0: %+v", c0)
	}

	// Verify MCP get_me
	c1 := sink.completedToolDetails[1]
	if c1.ToolName != "get_me" || c1.MCPServer != "github" || c1.Status != "ok" {
		t.Errorf("unexpected record 1: %+v", c1)
	}

	// Verify view_file
	c2 := sink.completedToolDetails[2]
	if c2.ToolName != "view_file" || c2.MCPServer != "native" || c2.Status != "ok" {
		t.Errorf("unexpected record 2: %+v", c2)
	}

	// Verify aborted tool
	c3 := sink.completedToolDetails[3]
	if c3.ToolName != "manage_task" || c3.MCPServer != "native" || c3.Status != "aborted" {
		t.Errorf("unexpected record 3: %+v", c3)
	}

	// Verify skill activation
	if len(sink.activatedSkills) != 1 {
		t.Fatalf("expected 1 skill activation, got %d: %+v", len(sink.activatedSkills), sink.activatedSkills)
	}
	expectedSkill := "home-assistant:discord"
	if sink.activatedSkills[0] != expectedSkill {
		t.Errorf("expected skill activation %q, got %q", expectedSkill, sink.activatedSkills[0])
	}

	// Verify tool calls (only tool starts, canonical MCP names, no DONE duplicates)
	expectedToolCalls := []string{"run_command:echo hello", "get_me:", "view_file:", "manage_task:"}
	if len(sink.toolCalls) != len(expectedToolCalls) {
		t.Fatalf("expected %d tool calls, got %d: %+v", len(expectedToolCalls), len(sink.toolCalls), sink.toolCalls)
	}
	for i, tc := range expectedToolCalls {
		if sink.toolCalls[i] != tc {
			t.Errorf("expected tool call %d to be %q, got %q", i, tc, sink.toolCalls[i])
		}
	}
}

func TestStreamingDaemon_ToolDrainingOnUnexpectedEOF(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mockHandle := &MockProcessHandle{pid: 2002}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}

	go func() {
		_, _ = io.Copy(io.Discard, inR)
	}()
	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000202\"}\n"))
	}()

	cfg := DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000202",
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer daemon.Close()

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-eof-1",
		Prompt:    "run tool until crash",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// Tool started
	_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":10,\"tool_name\":\"long_job\",\"tool_info\":{\"parameters\":{}}}}\n"))
	time.Sleep(10 * time.Millisecond)

	// Unexpected EOF
	_ = outW.Close()

	select {
	case <-sink.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for sink to finalize on EOF")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if len(sink.completedToolDetails) != 1 {
		t.Fatalf("expected 1 aborted tool drained, got %d", len(sink.completedToolDetails))
	}
	c := sink.completedToolDetails[0]
	if c.ToolName != "long_job" || c.Status != "aborted" {
		t.Errorf("expected long_job aborted, got %+v", c)
	}
}

func TestStreamingDaemon_ResultWithErrorStatus_RecordsCapacityThrottle(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 3001}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000301\"}\n"))
	}()

	daemon, err := StartStreamingDaemon(context.Background(), DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000301",
		Model:     "test-model",
	}, mock)
	if err != nil {
		t.Fatalf("failed creating daemon: %v", err)
	}
	defer daemon.Close()

	sink := newMockTurnSink()
	turn := &TurnContext{
		Ctx:       context.Background(),
		Model:     "test-model-turn",
		Prompt:    "test prompt",
		CreatedAt: time.Now(),
		Sink:      sink,
	}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn)
	daemon.inflightMu.Unlock()

	expectedErr := "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 1s."
	payload := fmt.Sprintf(`{"event":"result","result":{"status":"ERROR","error":%q,"response":"Recovered test text."}}`+"\n", expectedErr)
	daemon.dispatchNDJSONLine(payload)

	select {
	case <-sink.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for turn result")
	}

	sink.mu.Lock()
	res := sink.result
	sink.mu.Unlock()

	if res == nil {
		t.Fatalf("expected non-nil result from daemon with substantive response")
	}
	if res.Response != "Recovered test text." {
		t.Errorf("expected response 'Recovered test text.', got %q", res.Response)
	}
	if res.Stderr != expectedErr {
		t.Errorf("expected stderr %q, got %q", expectedErr, res.Stderr)
	}
}

func TestStreamingDaemon_NilActiveTurn_NoPanic(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 3002}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000302\"}\n"))
	}()

	daemon, err := StartStreamingDaemon(context.Background(), DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000302",
		Model:     "test-model",
	}, mock)
	if err != nil {
		t.Fatalf("failed creating daemon: %v", err)
	}
	defer daemon.Close()

	// Dispatch line without active inflight turns
	daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"OK","response":"Unsolicited"}}`)
}

func TestStreamingDaemon_TranscriptRescue_RecordsCapacityThrottle(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeQuietly(inR)
	defer closeQuietly(errW)

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 3003}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000303\"}\n"))
	}()

	daemon, err := StartStreamingDaemon(context.Background(), DaemonConfig{
		SessionID: "00000000-0000-0000-0000-000000000303",
		Model:     "test-model",
		TranscriptRescuer: func(convID string, since time.Time) string {
			return "Rescued from transcript successfully."
		},
	}, mock)
	if err != nil {
		t.Fatalf("failed creating daemon: %v", err)
	}
	defer daemon.Close()

	sink := newMockTurnSink()
	turn := &TurnContext{
		Ctx:       context.Background(),
		Model:     "gemini-3.8-flash-high",
		Prompt:    "test rescue prompt",
		CreatedAt: time.Now(),
		Sink:      sink,
	}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn)
	daemon.inflightMu.Unlock()

	expectedErr := "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s."
	payload := fmt.Sprintf(`{"event":"result","result":{"status":"ERROR","error":%q,"response":""}}`+"\n", expectedErr)
	daemon.dispatchNDJSONLine(payload)

	select {
	case <-sink.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for turn result")
	}

	sink.mu.Lock()
	res := sink.result
	sink.mu.Unlock()

	if res == nil {
		t.Fatalf("expected non-nil result rescued from transcript")
	}
	if res.Response != "Rescued from transcript successfully." {
		t.Errorf("expected rescued response, got %q", res.Response)
	}
	if res.Stderr != expectedErr {
		t.Errorf("expected stderr %q, got %q", expectedErr, res.Stderr)
	}
}

func TestStreamingDaemon_StepAwareDeltaPurging(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, _ := io.Pipe()

	testSessionUUID := "00000000-0000-0000-0000-000000000001"
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 999}, nil
		},
	}

	initPayload := fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", testSessionUUID)
	go func() {
		_, _ = outW.Write([]byte(initPayload))
	}()

	daemon, err := StartStreamingDaemon(context.Background(), DaemonConfig{
		SessionID: testSessionUUID,
	}, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	defer func() {
		_ = daemon.Close()
		_ = inR.Close()
	}()

	mockSink := newMockTurnSink()
	bufSink := NewBufferingTurnSink(BufferingTurnSinkConfig{
		OnStepStarted: mockSink.OnStepStarted,
		OnTextDelta:   mockSink.OnTextDelta,
		OnComplete:    mockSink.OnResult,
		OnError:       mockSink.OnError,
	})

	turn := &TurnContext{
		TurnID:    "turn-step-aware",
		SessionID: testSessionUUID,
		Model:     "gemini-3.8-flash-high",
		Prompt:    "test step-aware purging",
		CreatedAt: time.Now(),
		Sink:      bufSink,
	}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn)
	daemon.inflightMu.Unlock()

	// Step 1: Intermediate waiting text in su["step_index"]
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","text_delta":"Let me wait for task..."}}`)
	if bufSink.AccumulatedText() != "Let me wait for task..." {
		t.Fatalf("expected step 1 delta accumulated, got %q", bufSink.AccumulatedText())
	}

	// Step 2: System update / intermediate step in su["step_index"]
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"step_index":2,"state":"ACTIVE","step_type":"agent_response","text_delta":"Reviewing logs..."}}`)
	if bufSink.AccumulatedText() != "Reviewing logs..." {
		t.Fatalf("expected step 1 purged and step 2 accumulated, got %q", bufSink.AccumulatedText())
	}

	// Step 3: Terminal step with top-level raw["step_index"] fallback
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_index":3,"step_update":{"state":"ACTIVE","step_type":"agent_response","text_delta":"All work completed! Ready to roll."}}`)
	if bufSink.AccumulatedText() != "All work completed! Ready to roll." {
		t.Fatalf("expected step 2 purged and step 3 accumulated, got %q", bufSink.AccumulatedText())
	}

	// agy emits final result with multi-step concatenated text
	daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"Let me wait for task... Reviewing logs... All work completed! Ready to roll."}}`)

	res, err := bufSink.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	expected := "All work completed! Ready to roll."
	if res.Response != expected {
		t.Errorf("expected clean terminal deliverable %q, got %q", expected, res.Response)
	}

	mockSink.mu.Lock()
	stepStarts := mockSink.stepStarts
	mockSink.mu.Unlock()

	if len(stepStarts) != 3 || stepStarts[0] != 1 || stepStarts[1] != 2 || stepStarts[2] != 3 {
		t.Errorf("unexpected step starts recorded: %v", stepStarts)
	}
}

func TestStreamingDaemon_InitialTurnCount(t *testing.T) {
	_, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, _ := io.Pipe()

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
	}()

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 403}, nil
		},
	}

	ctx := context.Background()
	d, err := StartStreamingDaemon(ctx, DaemonConfig{
		SessionID:        "00000000-0000-0000-0000-000000000001",
		InitialTurnCount: 4,
	}, mock)
	if err != nil {
		t.Fatalf("StartStreamingDaemon error: %v", err)
	}
	defer d.Close()

	if d.TurnCount() != 4 {
		t.Errorf("expected TurnCount 4, got %d", d.TurnCount())
	}
}
