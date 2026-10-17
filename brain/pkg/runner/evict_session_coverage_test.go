package runner

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestUnifiedProcessPool_EvictAndRescuerCoverage(t *testing.T) {
	t.Parallel()

	// 1. EvictSession coverage
	var nilUnified *UnifiedProcessPool
	if err := nilUnified.EvictSession("target-1"); err != nil {
		t.Errorf("expected nil error on nil UnifiedProcessPool.EvictSession")
	}

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000001\"}\n"))
				_, _ = io.Copy(io.Discard, inR)
			}()
			return inW, outR, errR, NewMockProcessHandle(99999), nil
		},
	}

	uPool := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mock)
	defer uPool.Close()

	if err := uPool.EvictSession("nonexistent"); err != nil {
		t.Errorf("expected nil error on nonexistent target eviction: %v", err)
	}

	sess, err := uPool.GetOrCreate(context.Background(), "target-evict-live", "")
	if err != nil {
		t.Fatalf("unexpected error spawning daemon: %v", err)
	}
	if sess == nil {
		t.Fatalf("expected non-nil daemon")
	}

	if err := uPool.EvictSession("target-evict-live"); err != nil {
		t.Errorf("unexpected error evicting active daemon: %v", err)
	}

	if uPool.HasDaemon("target-evict-live") {
		t.Errorf("expected target-evict-live to be removed from pool")
	}

	// 2. SetTranscriptRescuer coverage
	var nilPool *UnifiedProcessPool
	nilPool.SetTranscriptRescuer(func(convID string, since time.Time) string { return "" })

	mockRescuer := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000003\"}\n"))
				_, _ = io.Copy(io.Discard, inR)
			}()
			return inW, outR, errR, NewMockProcessHandle(88888), nil
		},
	}

	uPoolRescuer := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mockRescuer)
	defer uPoolRescuer.Close()

	_, _ = uPoolRescuer.GetOrCreate(context.Background(), "target-rescuer", "")

	rescuerCalled := false
	uPoolRescuer.SetTranscriptRescuer(func(convID string, since time.Time) string {
		rescuerCalled = true
		return "rescued"
	})

	if uPoolRescuer.cfg.TranscriptRescuer == nil {
		t.Errorf("expected cfg.TranscriptRescuer to be set")
	}
	_ = uPoolRescuer.cfg.TranscriptRescuer("conv-1", time.Now())
	if !rescuerCalled {
		t.Errorf("expected rescuer callback to be invoked")
	}

	// 3. ThrowawayTurnSink coverage
	sink := NewThrowawayTurnSink()
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("run_command", "bash")
	sink.OnToolCompleted("run_command", "default", 100*time.Millisecond, "DONE")
	sink.OnSkillActivated("self-improvement", "trigger")
	sink.OnTextDelta("some text")

	sinkCtx, sinkCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer sinkCancel()
	_, _ = sink.ResultContext(sinkCtx)

	res := &TurnResult{Response: "completed response"}
	sink.OnResult(res)
	sink.OnResult(res)

	select {
	case out := <-sink.resCh:
		if out != "completed response" {
			t.Errorf("expected 'completed response', got %q", out)
		}
	default:
		t.Errorf("expected response in resCh")
	}
}

func TestIsDaemonMatch_Comprehensive(t *testing.T) {
	t.Parallel()

	dClosed := &StreamingDaemon{state: StateClosed}
	dDirty := &StreamingDaemon{dirty: true, state: StateReady}
	dReady := &StreamingDaemon{state: StateReady, sessionID: "sess-abc"}
	dEmptySess := &StreamingDaemon{state: StateReady, sessionID: ""}

	tests := []struct {
		name string
		d    *StreamingDaemon
		sess string
		want bool
	}{
		{name: "NilDaemon", d: nil, sess: "", want: false},
		{name: "ClosedDaemon", d: dClosed, sess: "", want: false},
		{name: "DirtyDaemon", d: dDirty, sess: "", want: false},
		{name: "EmptyTrimmedSess", d: dReady, sess: "", want: true},
		{name: "EmptyDaemonSessionID", d: dEmptySess, sess: "sess-target", want: true},
		{name: "MatchingSessionID", d: dReady, sess: "sess-abc", want: true},
		{name: "MismatchedSessionID", d: dReady, sess: "sess-xyz", want: false},
	}

	for _, tc := range tests {
		got := isDaemonMatch(tc.d, tc.sess)
		if got != tc.want {
			t.Errorf("%s: isDaemonMatch = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestPure_Helpers_BranchCoverage(t *testing.T) {
	t.Parallel()

	cleanTests := []struct {
		input string
		want  string
	}{
		{input: "", want: ""},
		{input: `"\"nested\""`, want: "nested"},
		{input: `\hello\`, want: "hello"},
	}
	for _, tc := range cleanTests {
		if got := CleanUnescapedString(tc.input); got != tc.want {
			t.Errorf("CleanUnescapedString(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}

	mcpTests := []struct {
		callName string
		args     map[string]any
		wantCan  string
		wantSrv  string
	}{
		{callName: "", args: nil, wantCan: "unknown", wantSrv: "native"},
		{callName: "call_mcp_tool", args: nil, wantCan: "call_mcp_tool", wantSrv: "unknown"},
		{callName: "call_mcp_tool", args: map[string]any{"ServerName": "my-server"}, wantCan: "call_mcp_tool", wantSrv: "my-server"},
		{callName: "call_mcp_tool", args: map[string]any{"ToolName": "my-tool"}, wantCan: "my-tool", wantSrv: "unknown"},
		{callName: "mcp_singlepart", args: nil, wantCan: "singlepart", wantSrv: "unknown"},
	}
	for _, tc := range mcpTests {
		gotCan, gotSrv := ExtractMCPToolInfo(tc.callName, tc.args)
		if gotCan != tc.wantCan || gotSrv != tc.wantSrv {
			t.Errorf("ExtractMCPToolInfo(%q, %v) = (%q, %q), want (%q, %q)", tc.callName, tc.args, gotCan, gotSrv, tc.wantCan, tc.wantSrv)
		}
	}

	skillTests := []struct {
		target string
		want   string
		wantOK bool
	}{
		{target: "", want: "", wantOK: false},
		{target: "no-skill-here", want: "", wantOK: false},
		{target: "view_file /skills/foo/SKILL.md", want: "foo", wantOK: true},
	}
	for _, tc := range skillTests {
		got, ok := ExtractSkillFromTarget(tc.target)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("ExtractSkillFromTarget(%q) = (%q, %v), want (%q, %v)", tc.target, got, ok, tc.want, tc.wantOK)
		}
	}
}
