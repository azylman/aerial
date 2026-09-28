package runner

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type mockTurnSink struct {
	mu        sync.Mutex
	started   bool
	thinking  int
	toolCalls []string
	deltas    []string
	result    *TurnResult
	err       error
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

func (s *mockTurnSink) OnToolCall(toolName, commandName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.toolCalls = append(s.toolCalls, toolName+":"+commandName)
}

func (s *mockTurnSink) OnTextDelta(delta string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deltas = append(s.deltas, delta)
}

func (s *mockTurnSink) OnResult(res *TurnResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result = res
}

func (s *mockTurnSink) OnError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
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
	origTimeout := streamingHandshakeTimeout
	streamingHandshakeTimeout = 50 * time.Millisecond
	defer func() { streamingHandshakeTimeout = origTimeout }()

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

	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001"}
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

