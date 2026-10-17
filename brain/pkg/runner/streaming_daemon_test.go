package runner

import (
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

func (s *mockTurnSink) notifyDone() {
	if s.done != nil {
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	}
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
	s.completedToolDetails = append(s.completedToolDetails, mockCompletedTool{toolName, mcpServer, duration, status})
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
	defer s.mu.Unlock()
	s.result = res
	s.notifyDone()
}

func (s *mockTurnSink) OnError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
	s.notifyDone()
}

func closeAll(closers ...io.Closer) {
	for _, c := range closers {
		closeQuietly(c)
	}
}

func enqueueInflightTurn(d *StreamingDaemon, turnID string, sink TurnSink, opts ...func(*TurnContext)) *TurnContext {
	turn := &TurnContext{
		TurnID:    turnID,
		Prompt:    "test",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	for _, opt := range opts {
		opt(turn)
	}
	d.inflightMu.Lock()
	d.inflight = append(d.inflight, turn)
	d.inflightMu.Unlock()
	return turn
}

func waitAndAssertSinkResult(t *testing.T, label string, sink *mockTurnSink, wantResp, wantStderr, wantErrSubstr string) {
	t.Helper()
	select {
	case <-sink.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: timed out waiting for sink result", label)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if wantErrSubstr != "" {
		if sink.err == nil || !strings.Contains(sink.err.Error(), wantErrSubstr) {
			t.Errorf("%s: expected error containing %q, got %v", label, wantErrSubstr, sink.err)
		}
		if sink.result != nil {
			t.Errorf("%s: expected nil result on error, got %+v", label, sink.result)
		}
		return
	}

	if sink.err != nil {
		t.Errorf("%s: expected nil error, got %v", label, sink.err)
	}
	if sink.result == nil || sink.result.Response != wantResp {
		t.Errorf("%s: expected response %q, got %+v", label, wantResp, sink.result)
	}
	if wantStderr != "" && (sink.result == nil || sink.result.Stderr != wantStderr) {
		t.Errorf("%s: expected stderr %q, got %+v", label, wantStderr, sink.result)
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
		closeAll(inR, inW, outW, errW)
	})
	return &daemonTestHarness{daemon: daemon, inR: inR, inW: inW, outW: outW, errW: errW, handle: handle}
}

func tryStartWithPipes(t *testing.T, ctx context.Context, cfg DaemonConfig, closeOutImmediately bool, initLine string, optHandle ...*MockProcessHandle) (*MockProcessHandle, error) {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeAll(outR, outW, inR, inW, errR, errW)

	handle := &MockProcessHandle{pid: 777}
	if len(optHandle) > 0 && optHandle[0] != nil {
		handle = optHandle[0]
	}

	if initLine != "" {
		go func() {
			defer outW.Close()
			_, _ = outW.Write([]byte(initLine))
		}()
	} else if closeOutImmediately {
		closeQuietly(outW)
	}

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, c DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, handle, nil
		},
	}
	_, err := StartStreamingDaemon(ctx, cfg, mock)
	return handle, err
}

func TestStreamingDaemon_HandshakeSuccessAndDefaults(t *testing.T) {
	cfg := DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001", Timeout: 2 * time.Second}
	h := startTestStreamingDaemon(t, cfg, "", false)
	d := h.daemon

	if d.State() != StateReady || d.SessionID() != cfg.SessionID || d.TurnCount() != 0 || d.StepCount() != 0 || d.TaskTracker() == nil || time.Since(d.LastUsed()) > 5*time.Second {
		t.Errorf("unexpected initial state: state=%s, session=%s, turns=%d, steps=%d", d.State(), d.SessionID(), d.TurnCount(), d.StepCount())
	}
	if DefaultHandshakeTimeout != 30*time.Second {
		t.Fatalf("expected DefaultHandshakeTimeout 30s, got %v", DefaultHandshakeTimeout)
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
	mockHandle := &MockProcessHandle{pid: 888, killErr: errors.New("kill error on startup")}
	handle, err := tryStartWithPipes(t, context.Background(), DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000001", Timeout: 50 * time.Millisecond}, false, "", mockHandle)
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for init event") {
		t.Fatalf("expected handshake timeout error, got: %v", err)
	}
	handle.mu.Lock()
	killed := handle.killed
	handle.mu.Unlock()
	if !killed {
		t.Error("expected process to be killed on handshake timeout")
	}

	_, err = tryStartWithPipes(t, context.Background(), cfgForSession("00000000-0000-0000-0000-000000000001"), true, "")
	if err == nil || !strings.Contains(err.Error(), "daemon stdout closed before init event") {
		t.Fatalf("expected stdout closed error, got: %v", err)
	}

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("spawn binary not found")
		},
	}
	if _, err := StartStreamingDaemon(context.Background(), cfgForSession("00000000-0000-0000-0000-000000000001"), mock); err == nil || !strings.Contains(err.Error(), "failed spawning daemon") {
		t.Fatalf("expected spawn error, got: %v", err)
	}

	if _, err := StartStreamingDaemon(context.Background(), DaemonConfig{AgyBin: "nonexistent-executable-binary-12345", SessionID: "00000000-0000-0000-0000-000000000001"}, nil); err == nil || !strings.Contains(err.Error(), "failed spawning daemon") {
		t.Fatalf("expected spawn error with nil spawner, got: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = tryStartWithPipes(t, ctx, cfgForSession("00000000-0000-0000-0000-000000000001"), false, "")
	if err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("expected cancellation error, got: %v", err)
	}
}

func TestStreamingDaemon_HandshakeMalformedInit(t *testing.T) {
	tests := []struct {
		name, line string
	}{
		{"invalid json", "not json at all\n"},
		{"missing session_id", `{"event":"init"}` + "\n"},
		{"not an init event", `{"event":"other","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"},
		{"invalid uuid in init event", `{"event":"init","session_id":"not-a-valid-uuid"}` + "\n"},
	}

	for _, tc := range tests {
		_, err := tryStartWithPipes(t, context.Background(), cfgForSession("00000000-0000-0000-0000-000000000001"), false, tc.line)
		if err == nil || !strings.Contains(err.Error(), "failed parsing init event") {
			t.Errorf("%s: expected 'failed parsing init event' error, got %v", tc.name, err)
		}
	}
}

func TestStreamingDaemon_DispatchEvents(t *testing.T) {
	h := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000001"), "", false)
	sink := newMockTurnSink()
	enqueueInflightTurn(h.daemon, "turn-1", sink, func(tc *TurnContext) {
		tc.Prompt = "hello"
		tc.CreatedAt = time.Now().Add(-100 * time.Millisecond)
	})

	events := `{"event":"step_update","thinking":true}` + "\n" +
		`{"event":"step_update","delta":"chunk1"}` + "\n" +
		`{"event":"step_update","tool_call":{"name":"bash","command":"ls -la"}}` + "\n" +
		`{"event":"step_update","delta":"chunk2"}` + "\n" +
		"invalid json line\n" +
		`{"event":"result","result":{"response":"completed answer"}}` + "\n"
	_, _ = h.outW.Write([]byte(events))

	waitAndAssertSinkResult(t, "DispatchEvents", sink, "completed answer", "", "")

	sink.mu.Lock()
	thinking, deltas, toolCalls := sink.thinking, sink.deltas, sink.toolCalls
	sink.mu.Unlock()

	if thinking != 1 || len(deltas) != 2 || deltas[0] != "chunk1" || deltas[1] != "chunk2" || len(toolCalls) != 1 || toolCalls[0] != "bash:ls -la" {
		t.Errorf("unexpected events: thinking=%d, deltas=%v, tools=%v", thinking, deltas, toolCalls)
	}
	if h.daemon.StepCount() != 4 || h.daemon.TurnCount() != 1 || h.daemon.State() != StateReady {
		t.Errorf("unexpected daemon metrics: steps=%d, turns=%d, state=%s", h.daemon.StepCount(), h.daemon.TurnCount(), h.daemon.State())
	}
}

func TestStreamingDaemon_DispatchResultYieldWaiting(t *testing.T) {
	hYield := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000002"), "", false)
	hYield.daemon.TaskTracker().Add(TaskMetadata{TaskID: "bg-1", CommandLine: "run long task", StartedAt: time.Now()})

	sinkYield := newMockTurnSink()
	enqueueInflightTurn(hYield.daemon, "turn-1", sinkYield, func(tc *TurnContext) {
		tc.Prompt = "start background task"
	})

	_, _ = hYield.outW.Write([]byte(`{"event":"result","response":"task started"}` + "\n"))

	waitAndAssertSinkResult(t, "YieldWaiting", sinkYield, "task started", "", "")

	if hYield.daemon.State() != StateYieldWaiting {
		t.Errorf("expected StateYieldWaiting when active background tasks exist, got %s", hYield.daemon.State())
	}
}

func TestStreamingDaemon_UnexpectedStdoutEOFDuringInflight(t *testing.T) {
	h := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000001"), "", false)
	sink := newMockTurnSink()
	enqueueInflightTurn(h.daemon, "turn-eof", sink)
	closeQuietly(h.outW)

	waitAndAssertSinkResult(t, "UnexpectedStdoutEOF", sink, "", "", "unexpected EOF")
	if h.daemon.State() != StateClosed {
		t.Errorf("expected StateClosed on stdout EOF, got %s", h.daemon.State())
	}
}

func TestStreamingDaemon_CloseWhileInflightAndKillError(t *testing.T) {
	hClose := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000002"), "", false)
	sink := newMockTurnSink()
	enqueueInflightTurn(hClose.daemon, "turn-close", sink)
	if err := hClose.daemon.Close(); err != nil || hClose.daemon.Close() != nil {
		t.Fatalf("unexpected or non-idempotent close error")
	}
	closeQuietly(hClose.outW)

	waitAndAssertSinkResult(t, "CloseWhileInflight", sink, "", "", "daemon closed while turn was in-flight")
	hClose.handle.mu.Lock()
	killed := hClose.handle.killed
	hClose.handle.mu.Unlock()
	if hClose.daemon.State() != StateClosed || !killed {
		t.Errorf("expected StateClosed and killed handle, got state=%s killed=%v", hClose.daemon.State(), killed)
	}

	hKill := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000003"), "", false)
	hKill.handle.killErr = errors.New("kill failed")
	closeQuietly(hKill.outW)
	if err := hKill.daemon.Close(); err == nil || !strings.Contains(err.Error(), "kill failed") {
		t.Errorf("expected kill failed error from Close(), got %v", err)
	}
}

func TestStreamingDaemon_CloseRaceAndStderrDrain(t *testing.T) {
	hRace := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000004"), "", false)
	enqueueInflightTurn(hRace.daemon, "turn-close-race", newMockTurnSink())
	if err := hRace.daemon.Close(); err != nil {
		t.Fatalf("failed to close daemon: %v", err)
	}
	hRace.daemon.dispatchNDJSONLine(`{"event":"result","result":{"response":"late result"}}`)
	if hRace.daemon.State() != StateClosed {
		t.Errorf("daemon state regressed from StateClosed to %s on late result event", hRace.daemon.State())
	}

	hStderr := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000005"), "", false)
	go func() {
		_, _ = hStderr.errW.Write([]byte("some stderr log\n"))
		closeQuietly(hStderr.errW)
	}()
	if err := hStderr.daemon.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
}

func TestStreamingDaemon_NilSinkAndNilActiveTurnGuards(t *testing.T) {
	h := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000006"), "", false)
	enqueueInflightTurn(h.daemon, "turn-nil-sink", nil)
	sinkActive := newMockTurnSink()
	enqueueInflightTurn(h.daemon, "turn-with-sink", sinkActive)

	h.daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"nil-sink-delta"}`)
	h.daemon.dispatchNDJSONLine(`{"event":"result","result":{"response":"turn1 answer"}}`)

	h.daemon.inflightMu.Lock()
	rem := len(h.daemon.inflight)
	headID := ""
	if rem > 0 {
		headID = h.daemon.inflight[0].TurnID
	}
	h.daemon.inflightMu.Unlock()
	if rem != 1 || headID != "turn-with-sink" || h.daemon.TurnCount() != 1 {
		t.Fatalf("expected 1 turn remaining (turn-with-sink) with turnCount=1, got rem=%d (%s) turns=%d", rem, headID, h.daemon.TurnCount())
	}

	h.daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"turn2-delta"}`)
	h.daemon.dispatchNDJSONLine(`{"event":"result","result":{"response":"turn2 answer"}}`)
	waitAndAssertSinkResult(t, "turn2", sinkActive, "turn2 answer", "", "")
	sinkActive.mu.Lock()
	deltas := sinkActive.deltas
	sinkActive.mu.Unlock()
	if len(deltas) != 1 || deltas[0] != "turn2-delta" || h.daemon.TurnCount() != 2 {
		t.Errorf("unexpected turn2 deltas or turnCount: deltas=%v turns=%d", deltas, h.daemon.TurnCount())
	}

	hNoPanic := startTestStreamingDaemon(t, DaemonConfig{SessionID: "00000000-0000-0000-0000-000000000302", Model: "test-model"}, "", false)
	hNoPanic.daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"OK","response":"Unsolicited"}}`)
}

func TestStreamingDaemon_SetTranscriptRescuerAndStreamQuietly(t *testing.T) {
	closeStreamQuietly(nil, "nil-stream")
	closeStreamQuietly(&errCloser{err: errors.New("stream close failure")}, "err-stream")

	var nilDaemon *StreamingDaemon
	nilDaemon.SetTranscriptRescuer(nil)
	dRescuer := &StreamingDaemon{}
	var rescuerCalled bool
	dRescuer.SetTranscriptRescuer(func(convID string, since time.Time) string {
		rescuerCalled = true
		return "rescued"
	})
	if dRescuer.cfg.TranscriptRescuer == nil || dRescuer.cfg.TranscriptRescuer("conv", time.Now()) != "rescued" || !rescuerCalled {
		t.Fatalf("TranscriptRescuer was not properly configured or executed")
	}
}

func TestStreamingDaemon_SendFIFOPipelining(t *testing.T) {
	hPipe := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000002"), "", true)
	sink1, sink2 := newMockTurnSink(), newMockTurnSink()
	if err := hPipe.daemon.Send("first prompt", &TurnContext{TurnID: "turn-1", Prompt: "first prompt", Sink: sink1, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("failed sending turn1: %v", err)
	}
	if err := hPipe.daemon.Send("second prompt", &TurnContext{TurnID: "turn-2", Prompt: "second prompt", Sink: sink2, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("failed sending turn2: %v", err)
	}
	if hPipe.daemon.InflightCount() != 2 {
		t.Fatalf("expected 2 inflight turns, got %d", hPipe.daemon.InflightCount())
	}
	go func() {
		payload := `{"event":"step_update","delta":"hello "}` + "\n" +
			`{"event":"result","result":{"response":"hello world"}}` + "\n" +
			`{"event":"step_update","delta":"second "}` + "\n" +
			`{"event":"result","result":{"response":"second response"}}` + "\n"
		_, _ = hPipe.outW.Write([]byte(payload))
	}()
	waitAndAssertSinkResult(t, "turn 1", sink1, "hello world", "", "")
	waitAndAssertSinkResult(t, "turn 2", sink2, "second response", "", "")
	if hPipe.daemon.InflightCount() != 0 {
		t.Errorf("expected 0 inflight turns remaining, got %d", hPipe.daemon.InflightCount())
	}
}

func TestStreamingDaemon_SendClosedAndWriteErrors(t *testing.T) {
	hClosed := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000003"), "", false)
	if hClosed.daemon.IsDirty() {
		t.Error("expected fresh daemon not to be dirty")
	}
	if err := hClosed.daemon.Close(); err != nil {
		t.Fatalf("failed to close daemon: %v", err)
	}
	if err := hClosed.daemon.Send("test", &TurnContext{TurnID: "turn-closed", Prompt: "test"}); err == nil || !strings.Contains(err.Error(), "cannot send to closed streaming daemon") {
		t.Fatalf("expected 'cannot send to closed streaming daemon', got: %v", err)
	}

	hWriteErr := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000004"), "", false)
	_ = hWriteErr.inR.Close()
	_ = hWriteErr.inW.Close()
	if err := hWriteErr.daemon.Send("fail", &TurnContext{TurnID: "turn-write-err", Prompt: "fail", Sink: newMockTurnSink()}); err == nil || !strings.Contains(err.Error(), "failed writing prompt to daemon stdin") {
		t.Fatalf("expected write error, got: %v", err)
	}
	if !hWriteErr.daemon.IsDirty() || hWriteErr.daemon.State() != StateClosed {
		t.Errorf("expected dirty and closed state, got dirty=%v state=%s", hWriteErr.daemon.IsDirty(), hWriteErr.daemon.State())
	}
}

func TestStreamingDaemon_SendConcurrency(t *testing.T) {
	hConc := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000005"), "", true)
	const concurrency = 20
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := hConc.daemon.Send(fmt.Sprintf("prompt-%d", idx), &TurnContext{TurnID: fmt.Sprintf("turn-%d", idx), Sink: newMockTurnSink(), CreatedAt: time.Now()}); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for sendErr := range errCh {
		t.Errorf("concurrent send failed: %v", sendErr)
	}
	if hConc.daemon.InflightCount() != concurrency {
		t.Errorf("expected %d inflight turns, got %d", concurrency, hConc.daemon.InflightCount())
	}
}

func TestStreamingDaemon_DispatchEdgeCasesAndNestedDeltas(t *testing.T) {
	h := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000001"), "", false)
	daemon := h.daemon

	for _, line := range []string{"", "   \n", `{"event":"step_update","delta":"orphan"}`} {
		daemon.dispatchNDJSONLine(line)
	}

	enqueueInflightTurn(daemon, "nil-sink", nil)
	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":"no-sink"}`)

	daemon.inflightMu.Lock()
	daemon.inflight = nil
	daemon.inflightMu.Unlock()

	sink := newMockTurnSink()
	enqueueInflightTurn(daemon, "t-1", sink)
	daemon.dispatchNDJSONLine(`{"event":"unknown_event_type"}`)
	daemon.dispatchNDJSONLine(`{"event":"step_update","delta":""}`)
	if len(sink.deltas) != 0 {
		t.Errorf("expected 0 deltas for empty string, got %d", len(sink.deltas))
	}

	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","tool_name":"no_idx_tool"}}`)
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","tool_name":"no_idx_tool"}}`)
	if len(sink.toolCalls) != 1 || sink.toolCalls[0] != "no_idx_tool:" || len(sink.completedToolDetails) != 1 || sink.completedToolDetails[0].ToolName != "no_idx_tool" {
		t.Errorf("unexpected tool tracking: calls=%+v, details=%+v", sink.toolCalls, sink.completedToolDetails)
	}

	daemon.inflightMu.Lock()
	daemon.inflight = nil
	daemon.inflightMu.Unlock()

	sinkNested := newMockTurnSink()
	enqueueInflightTurn(daemon, "t-nested", sinkNested)

	nestedEvents := []string{
		`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","text_delta":"nested chunk 1"}}`,
		`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","delta":"nested chunk 2"}}`,
		`{"event":"step_update","text_delta":"root text_delta"}`,
		`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","thinking":"internal thought"}}`,
	}
	for _, ne := range nestedEvents {
		daemon.dispatchNDJSONLine(ne)
	}

	if len(sinkNested.deltas) != 3 || sinkNested.deltas[0] != "nested chunk 1" || sinkNested.deltas[1] != "nested chunk 2" || sinkNested.deltas[2] != "root text_delta" {
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
		enqueueInflightTurn(daemon, "turn-"+tc.name, sink, func(trn *TurnContext) {
			trn.Model = "gemini-3.8-flash-high"
		})

		daemon.dispatchNDJSONLine(tc.ndjsonLine)
		waitAndAssertSinkResult(t, tc.name, sink, tc.wantResp, tc.wantStderr, tc.wantErrSubstr)
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
			SessionID:         "00000000-0000-0000-0000-000000000101",
			TranscriptRescuer: func(convID string, since time.Time) string { return tc.rescuedText },
		}
		h := startTestStreamingDaemon(t, cfg, "", true)
		sink := newMockTurnSink()
		if err := h.daemon.Send("hello", &TurnContext{TurnID: "turn-" + tc.name, Prompt: "hello", Sink: sink, CreatedAt: time.Now()}); err != nil {
			t.Fatalf("%s: send failed: %v", tc.name, err)
		}

		_, _ = h.outW.Write([]byte(tc.resultLine))
		waitAndAssertSinkResult(t, tc.name, sink, tc.wantResp, "", tc.wantErrSubstr)
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
			SessionID:         "00000000-0000-0000-0000-000000000103",
			TranscriptRescuer: func(convID string, since time.Time) string { return tc.rescuedText },
		}
		h := startTestStreamingDaemon(t, cfg, "", true)
		sink := newMockTurnSink()
		if err := h.daemon.Send("hello", &TurnContext{TurnID: "turn-" + tc.name, Prompt: "hello", Sink: sink, CreatedAt: time.Now()}); err != nil {
			t.Fatalf("%s: send failed: %v", tc.name, err)
		}

		_ = h.outW.Close()
		waitAndAssertSinkResult(t, tc.name, sink, tc.wantResp, "", tc.wantErrSubstr)
	}
}

func TestStreamingDaemon_Send_ContextRetrievers(t *testing.T) {
	tests := []struct {
		name, memoryBlock, ambientBlock, prompt, wantContentSub string
		turnCount                                               int
		wantMemCalled, wantAmbCalled                            bool
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

		readDone := make(chan []byte, 1)
		go func() {
			buf := make([]byte, 2048)
			n, _ := h.inR.Read(buf)
			readDone <- buf[:n]
		}()

		if err := h.daemon.Send(tc.prompt, &TurnContext{TurnID: "turn-" + tc.name, Prompt: tc.prompt, Sink: newMockTurnSink(), CreatedAt: time.Now()}); err != nil {
			t.Fatalf("%s: failed sending turn: %v", tc.name, err)
		}

		var received []byte
		select {
		case received = <-readDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: timed out reading stdin", tc.name)
		}

		if memCalled.Load() != tc.wantMemCalled || ambCalled.Load() != tc.wantAmbCalled {
			t.Errorf("%s: mem=%v (want %v), amb=%v (want %v)", tc.name, memCalled.Load(), tc.wantMemCalled, ambCalled.Load(), tc.wantAmbCalled)
		}

		var payload streamInputPayload
		if err := json.Unmarshal(received, &payload); err != nil || !strings.Contains(payload.Message.Content, tc.wantContentSub) {
			t.Errorf("%s: unmarshal err=%v, content=%q, wantSub=%q", tc.name, err, payload.Message.Content, tc.wantContentSub)
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
		{raw: map[string]any{"result": "not-a-map"}, want: ""},
		{raw: map[string]any{}, want: ""},
		{raw: nil, want: ""},
	}

	for _, tc := range cases {
		if got := extractResponseString(tc.raw); got != tc.want {
			t.Errorf("extractResponseString(%v) = %q, want %q", tc.raw, got, tc.want)
		}
	}

	assertUsage := func(r *TurnResult, in, out, tot int) {
		t.Helper()
		if r.Usage.InputTokens != in || r.Usage.OutputTokens != out || r.Usage.TotalTokens != tot {
			t.Errorf("unexpected usage: %+v", r.Usage)
		}
	}

	res := &TurnResult{}
	populateUsage(nil, nil)
	populateUsage(res, nil)
	populateUsage(nil, map[string]any{"usage": map[string]any{}})
	populateUsage(res, map[string]any{"usage": map[string]any{"input_tokens": float64(10), "output_tokens": float64(20), "total_tokens": float64(30)}})
	assertUsage(res, 10, 20, 30)

	res2 := &TurnResult{}
	populateUsage(res2, map[string]any{"result": map[string]any{"usage": map[string]any{"input_tokens": float64(40), "output_tokens": float64(50), "total_tokens": float64(90)}}})
	assertUsage(res2, 40, 50, 90)
}

func TestStreamingDaemon_OnTurnFinishedCallback(t *testing.T) {
	daemon := &StreamingDaemon{sessionID: "test-on-turn-finished", state: StateExecuting}
	cbCalled := make(chan *StreamingDaemon, 1)
	daemon.SetOnTurnFinished(func(d *StreamingDaemon) { cbCalled <- d })

	sink := newMockTurnSink()
	enqueueInflightTurn(daemon, "t-1", sink)

	daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"done"}}`)

	select {
	case d := <-cbCalled:
		if d != daemon {
			t.Errorf("expected callback with daemon %p, got %p", daemon, d)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for onTurnFinished callback")
	}

	var secondCbCalled atomic.Bool
	daemon.SetOnTurnFinished(func(d *StreamingDaemon) { secondCbCalled.Store(true) })

	enqueueInflightTurn(daemon, "t-1", newMockTurnSink())
	enqueueInflightTurn(daemon, "t-2", newMockTurnSink())

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

	events := `{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":1,"tool_name":"run_command","tool_info":{"parameters":{"CommandLine":"echo hello"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":1,"tool_name":"run_command"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":2,"tool_name":"call_mcp_tool","tool_info":{"parameters":{"ServerName":"github","ToolName":"get_me"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":2,"tool_name":"call_mcp_tool"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":3,"tool_name":"view_file","tool_info":{"parameters":{"AbsolutePath":"/share/aerial-config/custom-skills/home-assistant/SKILL.md"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":3,"tool_name":"view_file"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":4,"tool_name":"manage_task","tool_info":{"parameters":{}}}}` + "\n" +
		`{"event":"result","result":{"status":"SUCCESS","response":"done"}}` + "\n"
	_, _ = h.outW.Write([]byte(events))

	waitAndAssertSinkResult(t, "ToolEvents", sink, "done", "", "")

	sink.mu.Lock()
	defer sink.mu.Unlock()
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

	if len(sink.activatedSkills) != 1 || sink.activatedSkills[0] != "home-assistant:discord" {
		t.Errorf("unexpected skill activation: %+v", sink.activatedSkills)
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
	sink := newMockTurnSink()
	if err := h.daemon.Send("run tool until crash", &TurnContext{TurnID: "turn-eof-1", Prompt: "run tool until crash", Sink: sink, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	_, _ = h.outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"step_index\":10,\"tool_name\":\"long_job\",\"tool_info\":{\"parameters\":{}}}}\n"))
	_ = h.outW.Close()

	waitAndAssertSinkResult(t, "ToolDraining", sink, "", "", "unexpected EOF")

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.completedToolDetails) != 1 || sink.completedToolDetails[0].ToolName != "long_job" || sink.completedToolDetails[0].Status != "aborted" {
		t.Fatalf("expected long_job aborted tool drained, got %+v", sink.completedToolDetails)
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

	enqueueInflightTurn(daemon, "turn-step-aware", bufSink, func(tc *TurnContext) {
		tc.SessionID = testSessionUUID
		tc.Model = "gemini-3.8-flash-high"
		tc.Prompt = "test step-aware purging"
	})

	steps := []struct{ line, want string }{
		{`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","text_delta":"Let me wait for task..."}}`, "Let me wait for task..."},
		{`{"event":"step_update","step_update":{"step_index":2,"state":"ACTIVE","step_type":"agent_response","text_delta":"Reviewing logs..."}}`, "Reviewing logs..."},
		{`{"event":"step_update","step_index":3,"step_update":{"state":"ACTIVE","step_type":"agent_response","text_delta":"All work completed! Ready to roll."}}`, "All work completed! Ready to roll."},
	}
	for i, s := range steps {
		daemon.dispatchNDJSONLine(s.line)
		if bufSink.AccumulatedText() != s.want {
			t.Fatalf("step %d: got %q, want %q", i+1, bufSink.AccumulatedText(), s.want)
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
