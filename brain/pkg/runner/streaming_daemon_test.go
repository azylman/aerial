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
	s.mu.Lock(); defer s.mu.Unlock(); s.started = true
}
func (s *mockTurnSink) OnThinking() {
	s.mu.Lock(); defer s.mu.Unlock(); s.thinking++
}
func (s *mockTurnSink) OnStepStarted(stepIndex int) {
	s.mu.Lock(); defer s.mu.Unlock(); s.stepStarts = append(s.stepStarts, stepIndex)
}
func (s *mockTurnSink) OnToolCall(toolName, commandName string) {
	s.mu.Lock(); defer s.mu.Unlock(); s.toolCalls = append(s.toolCalls, toolName+":"+commandName)
}
func (s *mockTurnSink) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {
	s.mu.Lock(); defer s.mu.Unlock()
	s.completedTools = append(s.completedTools, toolName+":"+mcpServer+":"+status)
	s.completedToolDetails = append(s.completedToolDetails, mockCompletedTool{toolName, mcpServer, duration, status})
}
func (s *mockTurnSink) OnSkillActivated(skillName, source string) {
	s.mu.Lock(); defer s.mu.Unlock(); s.activatedSkills = append(s.activatedSkills, skillName+":"+source)
}
func (s *mockTurnSink) OnTextDelta(delta string) {
	s.mu.Lock(); defer s.mu.Unlock(); s.deltas = append(s.deltas, delta)
}
func (s *mockTurnSink) OnResult(res *TurnResult) {
	s.mu.Lock(); defer s.mu.Unlock()
	s.result = res
	if s.done != nil {
		select { case <-s.done: default: close(s.done) }
	}
}
func (s *mockTurnSink) OnError(err error) {
	s.mu.Lock(); defer s.mu.Unlock()
	s.err = err
	if s.done != nil {
		select { case <-s.done: default: close(s.done) }
	}
}

type daemonTestHarness struct {
	daemon *StreamingDaemon
	inR    *io.PipeReader
	inW    *io.PipeWriter
	outW   *io.PipeWriter
	errW   *io.PipeWriter
	handle *MockProcessHandle
}

func cfgForSession(id string) DaemonConfig {
	return DaemonConfig{SessionID: id}
}

func startTestStreamingDaemon(t *testing.T, cfg DaemonConfig, initLine string, discardStdin bool) *daemonTestHarness {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	handle := &MockProcessHandle{pid: 999}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, c DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, handle, nil
		},
	}
	if initLine == "" {
		initLine = fmt.Sprintf("{\"event\":\"init\",\"session_id\":%q}\n", cfg.SessionID)
	}
	go func() { _, _ = outW.Write([]byte(initLine)) }()
	if discardStdin {
		go func() { _, _ = io.Copy(io.Discard, inR) }()
	}
	daemon, err := StartStreamingDaemon(context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("startTestStreamingDaemon failed: %v", err)
	}
	t.Cleanup(func() {
		_ = daemon.Close()
		closeQuietly(inR)
		closeQuietly(inW)
		closeQuietly(outW)
		closeQuietly(errW)
	})
	return &daemonTestHarness{daemon: daemon, inR: inR, inW: inW, outW: outW, errW: errW, handle: handle}
}

func TestStreamingDaemon_HandshakeSuccessAndDefaults(t *testing.T) {
	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001", Timeout: 2 * time.Second}
	h := startTestStreamingDaemon(t, cfg, "", false)
	daemon := h.daemon
	if daemon.State() != StateReady { t.Errorf("expected state %s, got %s", StateReady, daemon.State()) }
	if daemon.SessionID() != cfg.SessionID { t.Errorf("expected latched session ID, got %s", daemon.SessionID()) }
	if daemon.TurnCount() != 0 { t.Errorf("expected 0 turns, got %d", daemon.TurnCount()) }
	if daemon.StepCount() != 0 { t.Errorf("expected 0 steps, got %d", daemon.StepCount()) }
	if daemon.TaskTracker() == nil { t.Error("expected non-nil TaskTracker") }
	if time.Since(daemon.LastUsed()) > 5*time.Second { t.Errorf("last used timestamp is too old: %v", daemon.LastUsed()) }

	if DefaultHandshakeTimeout != 30*time.Second {
		t.Fatalf("expected DefaultHandshakeTimeout to be 30s, got %v", DefaultHandshakeTimeout)
	}
	hDefault := startTestStreamingDaemon(t, DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000099", Timeout: 0}, "{\"event\":\"init\",\"conversation_id\":\"00000000-0000-0000-0000-000000000099\"}\n", false)
	if hDefault.daemon.SessionID() != "00000000-0000-0000-0000-000000000099" {
		t.Errorf("expected session ID 00000000-0000-0000-0000-000000000099, got: %s", hDefault.daemon.SessionID())
	}

	hInitTurn := startTestStreamingDaemon(t, DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000002", InitialTurnCount: 4}, "", false)
	if hInitTurn.daemon.TurnCount() != 4 {
		t.Errorf("expected TurnCount 4, got %d", hInitTurn.daemon.TurnCount())
	}
}

func TestStreamingDaemon_HandshakeFailures(t *testing.T) {
	inR, inW := io.Pipe(); outR, outW := io.Pipe(); errR, errW := io.Pipe()
	mockHandle := &MockProcessHandle{pid: 888, killErr: errors.New("kill error on startup")}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, mockHandle, nil
		},
	}
	_, err := StartStreamingDaemon(context.Background(), DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001", Timeout: 50 * time.Millisecond}, mock)
	closeQuietly(outR); closeQuietly(outW); closeQuietly(inR); closeQuietly(inW); closeQuietly(errR); closeQuietly(errW)
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for init event") {
		t.Fatalf("expected handshake timeout error, got: %v", err)
	}
	mockHandle.mu.Lock()
	if !mockHandle.killed { t.Error("expected process to be killed on handshake timeout") }
	mockHandle.mu.Unlock()

	inR, inW = io.Pipe(); outR, outW = io.Pipe(); errR, errW = io.Pipe()
	closeQuietly(outW)
	mock = &MockDaemonSpawner{SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		return inW, outR, errR, &MockProcessHandle{pid: 777}, nil
	}}
	_, err = StartStreamingDaemon(context.Background(), cfgForSession("00000000-0000-0000-0000-000000000001"), mock)
	closeQuietly(outR); closeQuietly(inR); closeQuietly(inW); closeQuietly(errR); closeQuietly(errW)
	if err == nil || !strings.Contains(err.Error(), "daemon stdout closed before init event") {
		t.Fatalf("expected stdout closed error, got: %v", err)
	}

	mock = &MockDaemonSpawner{SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		return nil, nil, nil, nil, errors.New("spawn binary not found")
	}}
	if _, err := StartStreamingDaemon(context.Background(), cfgForSession("00000000-0000-0000-0000-000000000001"), mock); err == nil || !strings.Contains(err.Error(), "failed spawning daemon") {
		t.Fatalf("expected spawn error, got: %v", err)
	}

	if _, err := StartStreamingDaemon(context.Background(), DaemonConfig{AgyBin: "nonexistent-executable-binary-12345", SessionID: "00000000-0000-0000-0000-000000000001"}, nil); err == nil || !strings.Contains(err.Error(), "failed spawning daemon") {
		t.Fatalf("expected spawn error with nil spawner, got: %v", err)
	}

	inR, inW = io.Pipe(); outR, outW = io.Pipe(); errR, errW = io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mock = &MockDaemonSpawner{SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		return inW, outR, errR, &MockProcessHandle{pid: 555}, nil
	}}
	_, err = StartStreamingDaemon(ctx, cfgForSession("00000000-0000-0000-0000-000000000001"), mock)
	closeQuietly(outR); closeQuietly(outW); closeQuietly(inR); closeQuietly(inW); closeQuietly(errR); closeQuietly(errW)
	if err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("expected cancellation error, got: %v", err)
	}
}

func TestStreamingDaemon_HandshakeMalformedInit(t *testing.T) {
	tests := []struct{ name, line string }{
		{name: "invalid json", line: "not json at all\n"},
		{name: "missing session_id", line: `{"event":"init"}` + "\n"},
		{name: "not an init event", line: `{"event":"other","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"},
		{name: "invalid uuid in init event", line: `{"event":"init","session_id":"not-a-valid-uuid"}` + "\n"},
	}

	for _, tc := range tests {
		inR, inW := io.Pipe(); outR, outW := io.Pipe(); errR, errW := io.Pipe()
		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				return inW, outR, errR, &MockProcessHandle{pid: 666}, nil
			},
		}
		go func(l string) { defer outW.Close(); _, _ = outW.Write([]byte(l)) }(tc.line)
		daemon, err := StartStreamingDaemon(context.Background(), cfgForSession("00000000-0000-0000-0000-000000000001"), mock)
		closeQuietly(inR); closeQuietly(inW); closeQuietly(outR); closeQuietly(errR); closeQuietly(errW)
		if daemon != nil { _ = daemon.Close() }
		if err == nil || !strings.Contains(err.Error(), "failed parsing init event") {
			t.Errorf("%s: expected 'failed parsing init event' error, got %v", tc.name, err)
		}
	}
}

func TestStreamingDaemon_DispatchEventsAndYieldWaiting(t *testing.T) {
	h := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000001"), "", false)
	daemon := h.daemon

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-1", Prompt: "hello", Sink: sink, CreatedAt: time.Now().Add(-100 * time.Millisecond)}

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, turn)
	daemon.inflightMu.Unlock()

	_, _ = h.outW.Write([]byte(`{"event":"step_update","thinking":true}` + "\n"))
	_, _ = h.outW.Write([]byte(`{"event":"step_update","delta":"chunk1"}` + "\n"))
	_, _ = h.outW.Write([]byte(`{"event":"step_update","tool_call":{"name":"bash","command":"ls -la"}}` + "\n"))
	_, _ = h.outW.Write([]byte(`{"event":"step_update","delta":"chunk2"}` + "\n"))
	_, _ = h.outW.Write([]byte("invalid json line\n"))
	_, _ = h.outW.Write([]byte(`{"event":"result","result":{"response":"completed answer"}}` + "\n"))

	select {
	case <-sink.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result event")
	}

	sink.mu.Lock()
	if sink.thinking != 1 { t.Errorf("expected 1 thinking event, got %d", sink.thinking) }
	if len(sink.deltas) != 2 || sink.deltas[0] != "chunk1" || sink.deltas[1] != "chunk2" {
		t.Errorf("unexpected deltas: %v", sink.deltas)
	}
	if len(sink.toolCalls) != 1 || sink.toolCalls[0] != "bash:ls -la" {
		t.Errorf("unexpected tool calls: %v", sink.toolCalls)
	}
	if sink.result == nil || sink.result.Response != "completed answer" {
		t.Errorf("unexpected result: %+v", sink.result)
	}
	sink.mu.Unlock()

	if daemon.StepCount() != 4 { t.Errorf("expected 4 steps, got %d", daemon.StepCount()) }
	if daemon.TurnCount() != 1 { t.Errorf("expected 1 turn, got %d", daemon.TurnCount()) }
	if daemon.State() != StateReady { t.Errorf("expected StateReady, got %s", daemon.State()) }

	hYield := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000002"), "", false)
	hYield.daemon.TaskTracker().Add(TaskMetadata{TaskID: "bg-1", CommandLine: "run long task", StartedAt: time.Now()})

	sinkYield := newMockTurnSink()
	turnYield := &TurnContext{TurnID: "turn-1", Prompt: "start background task", Sink: sinkYield, CreatedAt: time.Now()}

	hYield.daemon.inflightMu.Lock()
	hYield.daemon.inflight = append(hYield.daemon.inflight, turnYield)
	hYield.daemon.inflightMu.Unlock()

	_, _ = hYield.outW.Write([]byte(`{"event":"result","response":"task started"}` + "\n"))

	select {
	case <-sinkYield.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result event")
	}

	if hYield.daemon.State() != StateYieldWaiting {
		t.Errorf("expected StateYieldWaiting when active background tasks exist, got %s", hYield.daemon.State())
	}
	if sinkYield.result == nil || sinkYield.result.Response != "task started" {
		t.Errorf("expected response 'task started', got %+v", sinkYield.result)
	}
}

func TestStreamingDaemon_LifecycleAndCloseScenarios(t *testing.T) {
	// 1. UnexpectedStdoutEOFDuringInflight
	hEOF := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000001"), "", false)
	sinkEOF := newMockTurnSink()
	hEOF.daemon.inflightMu.Lock()
	hEOF.daemon.inflight = append(hEOF.daemon.inflight, &TurnContext{TurnID: "turn-eof", Sink: sinkEOF, CreatedAt: time.Now()})
	hEOF.daemon.inflightMu.Unlock()
	closeQuietly(hEOF.outW)
	select {
	case <-sinkEOF.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for EOF error callback on sink")
	}
	sinkEOF.mu.Lock()
	if sinkEOF.err == nil || !strings.Contains(sinkEOF.err.Error(), "unexpected EOF") {
		t.Errorf("unexpected sink error: %v", sinkEOF.err)
	}
	sinkEOF.mu.Unlock()
	if hEOF.daemon.State() != StateClosed {
		t.Errorf("expected StateClosed on stdout EOF, got %s", hEOF.daemon.State())
	}

	// 2. CloseWhileInflight
	hClose := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000002"), "", false)
	sink2 := newMockTurnSink()
	hClose.daemon.inflightMu.Lock()
	hClose.daemon.inflight = append(hClose.daemon.inflight, &TurnContext{TurnID: "turn-close", Sink: sink2, CreatedAt: time.Now()})
	hClose.daemon.inflightMu.Unlock()
	if err := hClose.daemon.Close(); err != nil { t.Fatalf("unexpected error closing daemon: %v", err) }
	if err := hClose.daemon.Close(); err != nil { t.Fatalf("idempotent close returned error: %v", err) }
	closeQuietly(hClose.outW)
	sink2.mu.Lock()
	if sink2.err == nil || !strings.Contains(sink2.err.Error(), "daemon closed while turn was in-flight") {
		t.Errorf("expected 'daemon closed while turn was in-flight' error, got %v", sink2.err)
	}
	sink2.mu.Unlock()
	if hClose.daemon.State() != StateClosed { t.Errorf("expected StateClosed, got %s", hClose.daemon.State()) }
	hClose.handle.mu.Lock()
	if !hClose.handle.killed { t.Error("expected process handle to be killed on Close") }
	hClose.handle.mu.Unlock()

	// 3. CloseStreamQuietly
	closeStreamQuietly(nil, "nil-stream")
	closeStreamQuietly(&errCloser{err: errors.New("stream close failure")}, "err-stream")

	// 4. CloseProcessKillError
	hKill := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000003"), "", false)
	hKill.handle.killErr = errors.New("kill failed")
	closeQuietly(hKill.outW)
	if closeErr := hKill.daemon.Close(); closeErr == nil || !strings.Contains(closeErr.Error(), "kill failed") {
		t.Errorf("expected kill failed error from Close(), got %v", closeErr)
	}

	// 5. ResultAfterCloseDoesNotReopenState
	hRace := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000004"), "", false)
	hRace.daemon.inflightMu.Lock()
	hRace.daemon.inflight = append(hRace.daemon.inflight, &TurnContext{TurnID: "turn-close-race", Sink: newMockTurnSink(), CreatedAt: time.Now()})
	hRace.daemon.inflightMu.Unlock()
	if err := hRace.daemon.Close(); err != nil { t.Fatalf("failed to close daemon: %v", err) }
	hRace.daemon.dispatchNDJSONLine(`{"event":"result","result":{"response":"late result"}}`)
	if hRace.daemon.State() != StateClosed {
		t.Errorf("daemon state regressed from StateClosed to %s on late result event", hRace.daemon.State())
	}

	// 6. StderrClosedOnClose
	hStderr := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000005"), "", false)
	go func() {
		_, _ = hStderr.errW.Write([]byte("some stderr log\n"))
		closeQuietly(hStderr.errW)
	}()
	if err := hStderr.daemon.Close(); err != nil { t.Fatalf("unexpected Close error: %v", err) }

	// 7. NilSinkCleanlyPopsOnResult
	hNilSink := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000006"), "", false)
	turnNil := &TurnContext{TurnID: "turn-nil-sink", Sink: nil, CreatedAt: time.Now()}
	sinkActive := newMockTurnSink()
	turnActive := &TurnContext{TurnID: "turn-with-sink", Sink: sinkActive, CreatedAt: time.Now()}
	hNilSink.daemon.inflightMu.Lock()
	hNilSink.daemon.inflight = append(hNilSink.daemon.inflight, turnNil, turnActive)
	hNilSink.daemon.inflightMu.Unlock()

	hNilSink.daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"nil-sink-delta"}`)
	hNilSink.daemon.dispatchNDJSONLine(`{"event":"result","result":{"response":"turn1 answer"}}`)

	hNilSink.daemon.inflightMu.Lock()
	rem := len(hNilSink.daemon.inflight)
	headID := ""
	if rem > 0 { headID = hNilSink.daemon.inflight[0].TurnID }
	hNilSink.daemon.inflightMu.Unlock()
	if rem != 1 || headID != "turn-with-sink" {
		t.Fatalf("expected 1 turn remaining (turn-with-sink), got %d (%s)", rem, headID)
	}
	if hNilSink.daemon.TurnCount() != 1 { t.Errorf("expected turnCount 1, got %d", hNilSink.daemon.TurnCount()) }

	hNilSink.daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"turn2-delta"}`)
	hNilSink.daemon.dispatchNDJSONLine(`{"event":"result","result":{"response":"turn2 answer"}}`)
	sinkActive.mu.Lock()
	if len(sinkActive.deltas) != 1 || sinkActive.deltas[0] != "turn2-delta" {
		t.Errorf("expected turn2 deltas, got %v", sinkActive.deltas)
	}
	if sinkActive.result == nil || sinkActive.result.Response != "turn2 answer" {
		t.Errorf("expected turn2 result, got %+v", sinkActive.result)
	}
	sinkActive.mu.Unlock()
	if hNilSink.daemon.TurnCount() != 2 { t.Errorf("expected turnCount 2, got %d", hNilSink.daemon.TurnCount()) }

	// 8. NilActiveTurn_NoPanic
	hNoPanic := startTestStreamingDaemon(t, DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000302", Model: "test-model"}, "", false)
	hNoPanic.daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"OK","response":"Unsolicited"}}`)

	// 9. SetTranscriptRescuer
	var nilDaemon *StreamingDaemon
	nilDaemon.SetTranscriptRescuer(nil)
	dRescuer := &StreamingDaemon{}
	var rescuerCalled bool
	dRescuer.SetTranscriptRescuer(func(convID string, since time.Time) string {
		rescuerCalled = true
		return "rescued"
	})
	if dRescuer.cfg.TranscriptRescuer == nil { t.Fatalf("expected TranscriptRescuer to be set") }
	dRescuer.cfg.TranscriptRescuer("conv", time.Now())
	if !rescuerCalled { t.Fatalf("expected rescuer function to be executed") }
}

func TestStreamingDaemon_Send_PipeliningErrorsAndConcurrency(t *testing.T) {
	// 1. SendFIFOPipelining
	hPipe := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000002"), "", true)
	sink1 := newMockTurnSink()
	turn1 := &TurnContext{TurnID: "turn-1", Prompt: "first prompt", Sink: sink1, CreatedAt: time.Now()}
	sink2 := newMockTurnSink()
	turn2 := &TurnContext{TurnID: "turn-2", Prompt: "second prompt", Sink: sink2, CreatedAt: time.Now()}
	if err := hPipe.daemon.Send(turn1.Prompt, turn1); err != nil { t.Fatalf("failed sending turn1: %v", err) }
	if err := hPipe.daemon.Send(turn2.Prompt, turn2); err != nil { t.Fatalf("failed sending turn2: %v", err) }
	if hPipe.daemon.InflightCount() != 2 { t.Fatalf("expected 2 inflight turns, got %d", hPipe.daemon.InflightCount()) }
	go func() {
		_, _ = hPipe.outW.Write([]byte(`{"event":"step_update","delta":"hello "}` + "\n"))
		_, _ = hPipe.outW.Write([]byte(`{"event":"result","result":{"response":"hello world"}}` + "\n"))
		_, _ = hPipe.outW.Write([]byte(`{"event":"step_update","delta":"second "}` + "\n"))
		_, _ = hPipe.outW.Write([]byte(`{"event":"result","result":{"response":"second response"}}` + "\n"))
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
	if hPipe.daemon.InflightCount() != 0 {
		t.Errorf("expected 0 inflight turns remaining, got %d", hPipe.daemon.InflightCount())
	}

	// 2. SendClosed
	hClosed := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000003"), "", false)
	if hClosed.daemon.IsDirty() { t.Error("expected fresh daemon not to be dirty") }
	if err := hClosed.daemon.Close(); err != nil { t.Fatalf("failed to close daemon: %v", err) }
	turnClosed := &TurnContext{TurnID: "turn-closed", Prompt: "test"}
	if err := hClosed.daemon.Send(turnClosed.Prompt, turnClosed); err == nil || !strings.Contains(err.Error(), "cannot send to closed streaming daemon") {
		t.Fatalf("expected 'cannot send to closed streaming daemon', got: %v", err)
	}

	// 3. SendStdinWriteError
	hWriteErr := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000004"), "", false)
	_ = hWriteErr.inR.Close(); _ = hWriteErr.inW.Close()
	turnWriteErr := &TurnContext{TurnID: "turn-write-err", Prompt: "fail", Sink: newMockTurnSink()}
	if err := hWriteErr.daemon.Send(turnWriteErr.Prompt, turnWriteErr); err == nil || !strings.Contains(err.Error(), "failed writing prompt to daemon stdin") {
		t.Fatalf("expected write error, got: %v", err)
	}
	if !hWriteErr.daemon.IsDirty() { t.Error("expected daemon to be marked dirty after stdin write error") }
	if hWriteErr.daemon.State() != StateClosed {
		t.Errorf("expected daemon state %s after write error, got %s", StateClosed, hWriteErr.daemon.State())
	}

	// 4. SendConcurrency
	hConc := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000005"), "", true)
	const concurrency = 20
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			turn := &TurnContext{TurnID: fmt.Sprintf("turn-%d", idx), Sink: newMockTurnSink(), CreatedAt: time.Now()}
			if sendErr := hConc.daemon.Send(fmt.Sprintf("prompt-%d", idx), turn); sendErr != nil {
				errCh <- sendErr
			}
		}(i)
	}
	wg.Wait(); close(errCh)
	for sendErr := range errCh { t.Errorf("concurrent send failed: %v", sendErr) }
	if hConc.daemon.InflightCount() != concurrency {
		t.Errorf("expected %d inflight turns, got %d", concurrency, hConc.daemon.InflightCount())
	}
}

func TestStreamingDaemon_DispatchEdgeCasesAndNestedDeltas(t *testing.T) {
	h := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000001"), "", false)
	daemon := h.daemon

	daemon.dispatchNDJSONLine("")
	daemon.dispatchNDJSONLine("   \n")
	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"orphan"}`)

	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, &TurnContext{TurnID: "nil-sink", Sink: nil})
	daemon.inflightMu.Unlock()
	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"no-sink"}`)

	daemon.inflightMu.Lock()
	daemon.inflight = nil
	daemon.inflightMu.Unlock()

	sink := newMockTurnSink()
	daemon.inflightMu.Lock()
	daemon.inflight = append(daemon.inflight, &TurnContext{TurnID: "t-1", Sink: sink})
	daemon.inflightMu.Unlock()
	daemon.dispatchNDJSONLine(`{"event":"unknown_event_type"}`)

	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":""}`)
	if len(sink.deltas) != 0 { t.Errorf("expected 0 deltas for empty string, got %d", len(sink.deltas)) }

	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","tool_name":"no_idx_tool"}}`)
	if len(sink.toolCalls) != 1 || sink.toolCalls[0] != "no_idx_tool:" {
		t.Errorf("expected tool start for no_idx_tool, got %+v", sink.toolCalls)
	}
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","tool_name":"no_idx_tool"}}`)
	if len(sink.completedToolDetails) != 1 || sink.completedToolDetails[0].ToolName != "no_idx_tool" {
		t.Errorf("expected tool completion for no_idx_tool, got %+v", sink.completedToolDetails)
	}

	if res := extractResponseString(map[string]any{}); res != "" { t.Errorf("expected empty string, got %q", res) }
	if res := extractResponseString(map[string]any{"result": map[string]any{"response": "nested"}}); res != "nested" {
		t.Errorf("expected 'nested', got %q", res)
	}
	if res := extractResponseString(map[string]any{"response": "flat"}); res != "flat" { t.Errorf("expected 'flat', got %q", res) }
	if res := extractResponseString(map[string]any{"result": "not-a-map"}); res != "" {
		t.Errorf("expected empty string for non-map result, got %q", res)
	}

	sinkNested := newMockTurnSink()
	daemon.inflightMu.Lock()
	daemon.inflight = []*TurnContext{{TurnID: "t-nested", Sink: sinkNested, CreatedAt: time.Now()}}
	daemon.inflightMu.Unlock()

	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","text_delta":"nested chunk 1"}}`)
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","delta":"nested chunk 2"}}`)
	daemon.dispatchNDJSONLine(`{"event":"step_update","text_delta":"root text_delta"}`)
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","thinking":"internal thought"}}`)

	if len(sinkNested.deltas) != 3 {
		t.Fatalf("expected 3 deltas, got %d: %v", len(sinkNested.deltas), sinkNested.deltas)
	}
	if sinkNested.deltas[0] != "nested chunk 1" || sinkNested.deltas[1] != "nested chunk 2" || sinkNested.deltas[2] != "root text_delta" {
		t.Errorf("unexpected deltas: %v", sinkNested.deltas)
	}
	if sinkNested.thinking != 1 {
		t.Errorf("expected 1 thinking event, got %d", sinkNested.thinking)
	}
}

func TestStreamingDaemon_ResultStatusAndErrorHandling(t *testing.T) {
	tests := []struct {
		name, rescuedText, ndjsonLine, wantResp, wantStderr, wantErrSubstr string
	}{
		{name: "SubstantiveResponseWithStatusError", ndjsonLine: `{"event":"result","result":{"conversation_id":"conv-1","status":"ERROR","error":"API error (attempt 2): RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 1s.","response":"Here is the full and complete answer to your query."}}`, wantResp: "Here is the full and complete answer to your query."},
		{name: "EmptyResponseWithStatusError", ndjsonLine: `{"event":"result","result":{"conversation_id":"conv-2","status":"ERROR","error":"hard model failure","response":""}}`, wantErrSubstr: "hard model failure"},
		{name: "FlatStatusErrorWithSubstantiveResponse", ndjsonLine: `{"event":"result","status":"error","error":"429 quota exhaustion","response":"Here is the flat result string."}`, wantResp: "Here is the flat result string."},
		{name: "NestedStatusErrorWithResponseInResult", ndjsonLine: `{"event":"result","result":{"status":"error","error":"model retry notice","result":"Here is nested result string."}}`, wantResp: "Here is nested result string."},
		{name: "ResultWithErrorStatus_RecordsCapacityThrottle", ndjsonLine: `{"event":"result","result":{"status":"ERROR","error":"RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 1s.","response":"Recovered test text."}}`, wantResp: "Recovered test text.", wantStderr: "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 1s."},
		{name: "TranscriptRescue_RecordsCapacityThrottle", rescuedText: "Rescued from transcript successfully.", ndjsonLine: `{"event":"result","result":{"status":"ERROR","error":"RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.","response":""}}`, wantResp: "Rescued from transcript successfully.", wantStderr: "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s."},
	}

	for _, tc := range tests {
		cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001", Model: "gemini-3.8-flash-high"}
		if tc.rescuedText != "" {
			cfg.TranscriptRescuer = func(convID string, since time.Time) string { return tc.rescuedText }
		}
		daemon := &StreamingDaemon{cfg: cfg, sessionID: cfg.SessionID}
		sink := newMockTurnSink()
		turn := &TurnContext{TurnID: "turn-" + tc.name, Model: "gemini-3.8-flash-high", Sink: sink, CreatedAt: time.Now()}
		daemon.inflight = []*TurnContext{turn}

		daemon.dispatchNDJSONLine(tc.ndjsonLine)

		select {
		case <-sink.done:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("%s: timed out waiting for sink", tc.name)
		}

		sink.mu.Lock()
		if tc.wantErrSubstr != "" {
			if sink.err == nil || !strings.Contains(sink.err.Error(), tc.wantErrSubstr) {
				t.Errorf("%s: expected error containing %q, got %v", tc.name, tc.wantErrSubstr, sink.err)
			}
			if sink.result != nil {
				t.Errorf("%s: expected nil result on error, got %+v", tc.name, sink.result)
			}
		} else {
			if sink.err != nil {
				t.Errorf("%s: expected nil error, got %v", tc.name, sink.err)
			}
			if sink.result == nil || sink.result.Response != tc.wantResp {
				t.Errorf("%s: expected response %q, got %+v", tc.name, tc.wantResp, sink.result)
			}
			if tc.wantStderr != "" && (sink.result == nil || sink.result.Stderr != tc.wantStderr) {
				t.Errorf("%s: expected stderr %q, got %+v", tc.name, tc.wantStderr, sink.result)
			}
		}
		sink.mu.Unlock()
	}
}

func TestStreamingDaemon_TranscriptRescue_ResultEvents(t *testing.T) {
	tests := []struct {
		name, rescuedText, resultLine, wantResp, wantErrSubstr string
	}{
		{name: "EmptyResponse", rescuedText: "Rescued substantive response from transcript", resultLine: "{\"event\":\"result\",\"result\":{\"response\":\"\"}}\n", wantResp: "Rescued substantive response from transcript"},
		{name: "ErrorSuppression", rescuedText: "Rescued substantive response after error", resultLine: "{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":\"quota exhausted\"}}\n", wantResp: "Rescued substantive response after error"},
		{name: "FallbackOnError", resultLine: "{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":\"quota limit reached\",\"response\":\"\"}}\n", wantErrSubstr: "quota limit reached"},
	}

	for _, tc := range tests {
		cfg := DaemonConfig{
			SessionID: "00000000-0000-0000-0000-000000000101",
			TranscriptRescuer: func(convID string, since time.Time) string { return tc.rescuedText },
		}
		h := startTestStreamingDaemon(t, cfg, "", true)
		sink := newMockTurnSink()
		turn := &TurnContext{TurnID: "turn-" + tc.name, Prompt: "hello", Sink: sink, CreatedAt: time.Now()}
		if err := h.daemon.Send("hello", turn); err != nil {
			t.Fatalf("%s: send failed: %v", tc.name, err)
		}

		_, _ = h.outW.Write([]byte(tc.resultLine))

		select {
		case <-sink.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: timed out waiting for sink result", tc.name)
		}

		sink.mu.Lock()
		if tc.wantErrSubstr != "" {
			if sink.err == nil || !strings.Contains(sink.err.Error(), tc.wantErrSubstr) {
				t.Errorf("%s: expected error containing %q, got %v", tc.name, tc.wantErrSubstr, sink.err)
			}
			if sink.result != nil {
				t.Errorf("%s: expected nil result on error, got %+v", tc.name, sink.result)
			}
		} else {
			if sink.err != nil {
				t.Errorf("%s: expected nil error, got %v", tc.name, sink.err)
			}
			if sink.result == nil || sink.result.Response != tc.wantResp {
				t.Errorf("%s: expected response %q, got %+v", tc.name, tc.wantResp, sink.result)
			}
		}
		sink.mu.Unlock()
	}
}

func TestStreamingDaemon_TranscriptRescue_UnexpectedEOF(t *testing.T) {
	tests := []struct {
		name, rescuedText, wantResp, wantErrSubstr string
	}{
		{name: "EOFRescue", rescuedText: "Rescued substantive response on EOF", wantResp: "Rescued substantive response on EOF"},
		{name: "FallbackOnError", wantErrSubstr: "unexpected EOF"},
	}

	for _, tc := range tests {
		cfg := DaemonConfig{
			SessionID: "00000000-0000-0000-0000-000000000103",
			TranscriptRescuer: func(convID string, since time.Time) string { return tc.rescuedText },
		}
		h := startTestStreamingDaemon(t, cfg, "", true)
		sink := newMockTurnSink()
		turn := &TurnContext{TurnID: "turn-" + tc.name, Prompt: "hello", Sink: sink, CreatedAt: time.Now()}
		if err := h.daemon.Send("hello", turn); err != nil {
			t.Fatalf("%s: send failed: %v", tc.name, err)
		}

		_ = h.outW.Close()

		select {
		case <-sink.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: timed out waiting for sink result", tc.name)
		}

		sink.mu.Lock()
		if tc.wantErrSubstr != "" {
			if sink.err == nil || !strings.Contains(sink.err.Error(), tc.wantErrSubstr) {
				t.Errorf("%s: expected error containing %q, got %v", tc.name, tc.wantErrSubstr, sink.err)
			}
			if sink.result != nil {
				t.Errorf("%s: expected nil result on unrescued EOF, got %+v", tc.name, sink.result)
			}
		} else {
			if sink.err != nil {
				t.Errorf("%s: expected error to be suppressed on EOF when rescued, got %v", tc.name, sink.err)
			}
			if sink.result == nil || sink.result.Response != tc.wantResp {
				t.Errorf("%s: expected response %q on EOF, got %+v", tc.name, tc.wantResp, sink.result)
			}
		}
		sink.mu.Unlock()
	}
}

func TestStreamingDaemon_Send_ContextRetrievers(t *testing.T) {
	tests := []struct {
		name, memoryBlock, ambientBlock, prompt, wantContentSub string
		turnCount                                                int
		wantMemCalled, wantAmbCalled                             bool
	}{
		{name: "MemoryInjected", memoryBlock: "<retrieved_memory>\n- User prefers dark mode\n</retrieved_memory>", prompt: "Hello Aerial", wantContentSub: "<retrieved_memory>\n- User prefers dark mode\n</retrieved_memory>", wantMemCalled: true},
		{name: "MemorySkippedWhenAlreadyPresent", memoryBlock: "<retrieved_memory>second</retrieved_memory>", prompt: "<retrieved_memory>first</retrieved_memory>\nHello", wantContentSub: "<retrieved_memory>first</retrieved_memory>"},
		{name: "AmbientInjectedTurn0", ambientBlock: "<ambient_context>\nLiving Room: ON\n</ambient_context>", prompt: "Turn off lights", wantContentSub: "<ambient_context>\nLiving Room: ON\n</ambient_context>", wantAmbCalled: true},
		{name: "AmbientSkippedWhenAlreadyPresent", ambientBlock: "<ambient_context>second</ambient_context>", prompt: "<ambient_context>first</ambient_context>\nHello", wantContentSub: "<ambient_context>first</ambient_context>"},
		{name: "AmbientInjectedSubsequentTurn", turnCount: 1, ambientBlock: "<ambient_context>subsequent</ambient_context>", prompt: "Clean prompt turn 3", wantContentSub: "<ambient_context>subsequent</ambient_context>\n\nClean prompt turn 3", wantAmbCalled: true},
	}

	for _, tc := range tests {
		var memCalled, ambCalled atomic.Bool
		cfg := cfgForSession("00000000-0000-0000-0000-000000000002")
		if tc.memoryBlock != "" {
			cfg.MemoryRetriever = func(ctx context.Context, query string) (string, error) {
				memCalled.Store(true)
				return tc.memoryBlock, nil
			}
		}
		if tc.ambientBlock != "" {
			cfg.AmbientContextRetriever = func(ctx context.Context) (string, error) {
				ambCalled.Store(true)
				return tc.ambientBlock, nil
			}
		}

		h := startTestStreamingDaemon(t, cfg, "", false)
		if tc.turnCount > 0 {
			h.daemon.mu.Lock()
			h.daemon.turnCount = tc.turnCount
			h.daemon.mu.Unlock()
		}

		var receivedStdin bytes.Buffer
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			buf := make([]byte, 2048)
			n, _ := h.inR.Read(buf)
			receivedStdin.Write(buf[:n])
		}()

		turn := &TurnContext{TurnID: "turn-" + tc.name, Prompt: tc.prompt, Sink: newMockTurnSink(), CreatedAt: time.Now()}
		if err := h.daemon.Send(turn.Prompt, turn); err != nil {
			t.Fatalf("%s: failed sending turn: %v", tc.name, err)
		}

		select {
		case <-readDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: timed out reading stdin", tc.name)
		}

		if memCalled.Load() != tc.wantMemCalled {
			t.Errorf("%s: memCalled = %v, want %v", tc.name, memCalled.Load(), tc.wantMemCalled)
		}
		if ambCalled.Load() != tc.wantAmbCalled {
			t.Errorf("%s: ambCalled = %v, want %v", tc.name, ambCalled.Load(), tc.wantAmbCalled)
		}

		var payload streamInputPayload
		if err := json.Unmarshal(receivedStdin.Bytes(), &payload); err != nil {
			t.Fatalf("%s: failed unmarshaling stdin JSON: %v, raw: %s", tc.name, err, receivedStdin.String())
		}
		if !strings.Contains(payload.Message.Content, tc.wantContentSub) {
			t.Errorf("%s: expected payload content to contain %q, got %q", tc.name, tc.wantContentSub, payload.Message.Content)
		}
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

	res := &TurnResult{}
	populateUsage(nil, nil); populateUsage(res, nil); populateUsage(nil, map[string]any{"usage": map[string]any{}})
	populateUsage(res, map[string]any{"usage": map[string]any{"input_tokens": float64(10), "output_tokens": float64(20), "total_tokens": float64(30)}})
	if res.Usage.InputTokens != 10 || res.Usage.OutputTokens != 20 || res.Usage.TotalTokens != 30 {
		t.Errorf("unexpected usage from root: %+v", res.Usage)
	}

	res2 := &TurnResult{}
	populateUsage(res2, map[string]any{"result": map[string]any{"usage": map[string]any{"input_tokens": float64(40), "output_tokens": float64(50), "total_tokens": float64(90)}}})
	if res2.Usage.InputTokens != 40 || res2.Usage.OutputTokens != 50 || res2.Usage.TotalTokens != 90 {
		t.Errorf("unexpected usage from nested: %+v", res2.Usage)
	}
}

func TestStreamingDaemon_OnTurnFinishedCallback(t *testing.T) {
	daemon := &StreamingDaemon{sessionID: "test-on-turn-finished", state: StateExecuting}
	cbCalled := make(chan *StreamingDaemon, 1)
	daemon.SetOnTurnFinished(func(d *StreamingDaemon) { cbCalled <- d })

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "t-1", Sink: sink, CreatedAt: time.Now()}
	daemon.inflight = append(daemon.inflight, turn)

	daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"done"}}`)

	select {
	case d := <-cbCalled:
		if d != daemon { t.Errorf("expected callback with daemon %p, got %p", daemon, d) }
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for onTurnFinished callback")
	}

	var secondCbCalled atomic.Bool
	daemon.SetOnTurnFinished(func(d *StreamingDaemon) { secondCbCalled.Store(true) })

	turn1 := &TurnContext{TurnID: "t-1", Sink: newMockTurnSink(), CreatedAt: time.Now()}
	turn2 := &TurnContext{TurnID: "t-2", Sink: newMockTurnSink(), CreatedAt: time.Now()}
	daemon.inflight = []*TurnContext{turn1, turn2}

	daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"done-1"}}`)
	if secondCbCalled.Load() {
		t.Error("expected onTurnFinished NOT to be called when inflight turns remain")
	}
}

func TestStreamingDaemon_ToolAndSkillLifecycleEvents(t *testing.T) {
	h := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000201"), "", true)
	daemon := h.daemon

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-tools-1", Prompt: "run tool check", Sink: sink, CreatedAt: time.Now()}
	if err := daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	_, _ = h.outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":1,\"tool_name\":\"run_command\",\"tool_info\":{\"parameters\":{\"CommandLine\":\"echo hello\"}}}}\n"))
	_, _ = h.outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"DONE\",\"type\":\"tool_call\",\"step_index\":1,\"tool_name\":\"run_command\"}}\n"))
	_, _ = h.outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":2,\"tool_name\":\"call_mcp_tool\",\"tool_info\":{\"parameters\":{\"ServerName\":\"github\",\"ToolName\":\"get_me\"}}}}\n"))
	_, _ = h.outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"DONE\",\"type\":\"tool_call\",\"step_index\":2,\"tool_name\":\"call_mcp_tool\"}}\n"))
	_, _ = h.outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":3,\"tool_name\":\"view_file\",\"tool_info\":{\"parameters\":{\"AbsolutePath\":\"/share/aerial-config/custom-skills/home-assistant/SKILL.md\"}}}}\n"))
	_, _ = h.outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"DONE\",\"type\":\"tool_call\",\"step_index\":3,\"tool_name\":\"view_file\"}}\n"))
	_, _ = h.outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":4,\"tool_name\":\"manage_task\",\"tool_info\":{\"parameters\":{}}}}\n"))
	_, _ = h.outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"done\"}}\n"))

	select {
	case <-sink.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn completion")
	}

	sink.mu.Lock(); defer sink.mu.Unlock()
	if len(sink.completedToolDetails) != 4 {
		t.Fatalf("expected 4 completed tool records, got %d: %+v", len(sink.completedToolDetails), sink.completedToolDetails)
	}

	expectedDetails := []mockCompletedTool{
		{ToolName: "run_command", MCPServer: "native", Status: "ok"},
		{ToolName: "get_me", MCPServer: "github", Status: "ok"},
		{ToolName: "view_file", MCPServer: "native", Status: "ok"},
		{ToolName: "manage_task", MCPServer: "native", Status: "aborted"},
	}
	for i, exp := range expectedDetails {
		c := sink.completedToolDetails[i]
		if c.ToolName != exp.ToolName || c.MCPServer != exp.MCPServer || c.Status != exp.Status {
			t.Errorf("record %d: got %+v, want %+v", i, c, exp)
		}
	}

	if len(sink.activatedSkills) != 1 {
		t.Fatalf("expected 1 skill activation, got %d: %+v", len(sink.activatedSkills), sink.activatedSkills)
	}
	if sink.activatedSkills[0] != "home-assistant:discord" {
		t.Errorf("expected skill activation 'home-assistant:discord', got %q", sink.activatedSkills[0])
	}

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
	h := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000202"), "", true)
	daemon := h.daemon

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-eof-1", Prompt: "run tool until crash", Sink: sink, CreatedAt: time.Now()}
	if err := daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	_, _ = h.outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":10,\"tool_name\":\"long_job\",\"tool_info\":{\"parameters\":{}}}}\n"))
	_ = h.outW.Close()

	select {
	case <-sink.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for sink to finalize on EOF")
	}

	sink.mu.Lock(); defer sink.mu.Unlock()
	if len(sink.completedToolDetails) != 1 {
		t.Fatalf("expected 1 aborted tool drained, got %d", len(sink.completedToolDetails))
	}
	if c := sink.completedToolDetails[0]; c.ToolName != "long_job" || c.Status != "aborted" {
		t.Errorf("expected long_job aborted, got %+v", c)
	}
}

func TestStreamingDaemon_StepAwareDeltaPurging(t *testing.T) {
	testSessionUUID := "00000000-0000-0000-0000-000000000001"
	h := startTestStreamingDaemon(t, cfgForSession(testSessionUUID), "", true)
	daemon := h.daemon

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

	stepUpdates := []string{
		`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","text_delta":"Let me wait for task..."}}`,
		`{"event":"step_update","step_update":{"step_index":2,"state":"ACTIVE","step_type":"agent_response","text_delta":"Reviewing logs..."}}`,
		`{"event":"step_update","step_index":3,"step_update":{"state":"ACTIVE","step_type":"agent_response","text_delta":"All work completed! Ready to roll."}}`,
	}
	expectedAccum := []string{
		"Let me wait for task...",
		"Reviewing logs...",
		"All work completed! Ready to roll.",
	}
	for i, su := range stepUpdates {
		daemon.dispatchNDJSONLine(su)
		if bufSink.AccumulatedText() != expectedAccum[i] {
			t.Fatalf("step %d: got %q, want %q", i+1, bufSink.AccumulatedText(), expectedAccum[i])
		}
	}

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
