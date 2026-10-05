package runner

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestPools_EvictSession_Coverage(t *testing.T) {
	t.Parallel()

	// 1. UnifiedProcessPool.EvictSession
	{
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

		// Evicting nonexistent target
		if err := uPool.EvictSession("nonexistent"); err != nil {
			t.Errorf("expected nil error on nonexistent target eviction: %v", err)
		}

		// Spawn daemon, then evict
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

		// Verify daemon was removed from pool
		if uPool.HasDaemon("target-evict-live") {
			t.Errorf("expected target-evict-live to be removed from pool")
		}
	}

	// 2. DynamicVoicePool.EvictSession
	{
		var nilDynamic *DynamicVoicePool
		if err := nilDynamic.EvictSession("voice-1"); err != nil {
			t.Errorf("expected nil error on nil DynamicVoicePool.EvictSession")
		}

		plain := &plainPoolWithoutModel{}
		dyn := NewDynamicVoicePoolWithInitial(plain, "plain-model", nil)
		if err := dyn.EvictSession("voice-target"); err != nil {
			t.Errorf("unexpected error on DynamicVoicePool.EvictSession: %v", err)
		}
	}

	// 3. GeminiAPIPool.EvictSession
	{
		var nilGemini *GeminiAPIPool
		if err := nilGemini.EvictSession("gem-1"); err != nil {
			t.Errorf("expected nil error on nil GeminiAPIPool.EvictSession")
		}

		gPool := NewGeminiAPIPool(GeminiAPIPoolConfig{
			APIKey: "test-key",
			Model:  "gemini-2.5-flash",
		})
		if err := gPool.EvictSession("gem-target"); err != nil {
			t.Errorf("unexpected error on GeminiAPIPool.EvictSession: %v", err)
		}
	}

	// 4. InterchangeablePool.EvictSession
	{
		var nilInter *InterchangeablePool
		if err := nilInter.EvictSession("inter-1"); err != nil {
			t.Errorf("expected nil error on nil InterchangeablePool.EvictSession")
		}

		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				outR, outW := io.Pipe()
				inR, inW := io.Pipe()
				errR, _ := io.Pipe()
				go func() {
					_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000002\"}\n"))
					_, _ = io.Copy(io.Discard, inR)
				}()
				return inW, outR, errR, NewMockProcessHandle(99998), nil
			},
		}
		uPool := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mock)
		defer uPool.Close()

		interPool := NewInterchangeablePool(uPool, InterchangeablePoolConfig{WorkerCount: 1})
		defer interPool.Close()

		if err := interPool.EvictSession("inter-target"); err != nil {
			t.Errorf("unexpected error on InterchangeablePool.EvictSession: %v", err)
		}
	}
}

func TestThrowawayTurnSink_NoOpCallbacks_Coverage(t *testing.T) {
	t.Parallel()

	sink := NewThrowawayTurnSink()
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("run_command", "bash")
	sink.OnToolCompleted("run_command", "default", 100*time.Millisecond, "DONE")
	sink.OnSkillActivated("self-improvement", "trigger")
	sink.OnTextDelta("some text")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, _ = sink.ResultContext(ctx)

	res := &TurnResult{Response: "completed response"}
	sink.OnResult(res)

	// Call again to verify sync.Once branch
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

func TestUnifiedProcessPool_SetTranscriptRescuer_Coverage(t *testing.T) {
	t.Parallel()

	var nilPool *UnifiedProcessPool
	nilPool.SetTranscriptRescuer(func(convID string, since time.Time) string { return "" })

	mock := &MockDaemonSpawner{
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

	uPool := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mock)
	defer uPool.Close()

	_, _ = uPool.GetOrCreate(context.Background(), "target-rescuer", "")

	rescuerCalled := false
	uPool.SetTranscriptRescuer(func(convID string, since time.Time) string {
		rescuerCalled = true
		return "rescued"
	})

	if uPool.cfg.TranscriptRescuer == nil {
		t.Errorf("expected cfg.TranscriptRescuer to be set")
	}
	_ = uPool.cfg.TranscriptRescuer("conv-1", time.Now())
	if !rescuerCalled {
		t.Errorf("expected rescuer callback to be invoked")
	}
}

func TestIsDaemonMatch_Comprehensive(t *testing.T) {
	t.Parallel()

	// 1. nil daemon
	if isDaemonMatch(nil, "") {
		t.Errorf("expected false for nil daemon")
	}

	// 2. closed daemon
	dClosed := &StreamingDaemon{state: StateClosed}
	if isDaemonMatch(dClosed, "") {
		t.Errorf("expected false for closed daemon")
	}

	// 3. dirty daemon
	dDirty := &StreamingDaemon{dirty: true, state: StateReady}
	if isDaemonMatch(dDirty, "") {
		t.Errorf("expected false for dirty daemon")
	}

	// 4. empty trimmedSess matches any non-closed, non-dirty daemon
	dReady := &StreamingDaemon{state: StateReady, sessionID: "sess-abc"}
	if !isDaemonMatch(dReady, "") {
		t.Errorf("expected true for empty trimmedSess on ready daemon")
	}

	// 5. daemon with empty sessionID matches matching session
	dEmptySess := &StreamingDaemon{state: StateReady, sessionID: ""}
	if !isDaemonMatch(dEmptySess, "sess-target") {
		t.Errorf("expected true for daemon with empty sessionID against any session")
	}

	// 6. daemon with matching sessionID
	if !isDaemonMatch(dReady, "sess-abc") {
		t.Errorf("expected true for matching sessionID")
	}

	// 7. daemon with non-matching sessionID
	if isDaemonMatch(dReady, "sess-xyz") {
		t.Errorf("expected false for mismatched sessionID")
	}
}

func TestPure_Helpers_BranchCoverage(t *testing.T) {
	t.Parallel()

	// CleanUnescapedString edge cases
	if CleanUnescapedString("") != "" {
		t.Errorf("expected empty string")
	}
	if CleanUnescapedString(`"\"nested\""`) != "nested" {
		t.Errorf("expected nested unwrapped")
	}
	if CleanUnescapedString(`\hello\`) != "hello" {
		t.Errorf("expected hello unwrapped")
	}

	// ExtractMCPToolInfo branches
	can, srv := ExtractMCPToolInfo("", nil)
	if can != "unknown" || srv != "native" {
		t.Errorf("expected unknown/native, got %s/%s", can, srv)
	}

	can, srv = ExtractMCPToolInfo("call_mcp_tool", nil)
	if can != "call_mcp_tool" || srv != "unknown" {
		t.Errorf("expected call_mcp_tool/unknown, got %s/%s", can, srv)
	}

	can, srv = ExtractMCPToolInfo("call_mcp_tool", map[string]any{"ServerName": "my-server"})
	if can != "call_mcp_tool" || srv != "my-server" {
		t.Errorf("expected call_mcp_tool/my-server, got %s/%s", can, srv)
	}

	can, srv = ExtractMCPToolInfo("call_mcp_tool", map[string]any{"ToolName": "my-tool"})
	if can != "my-tool" || srv != "unknown" {
		t.Errorf("expected my-tool/unknown, got %s/%s", can, srv)
	}

	can, srv = ExtractMCPToolInfo("mcp_singlepart", nil)
	if can != "singlepart" || srv != "unknown" {
		t.Errorf("expected singlepart/unknown, got %s/%s", can, srv)
	}

	// ExtractSkillFromTarget branches
	if s, ok := ExtractSkillFromTarget(""); ok || s != "" {
		t.Errorf("expected false for empty target")
	}
	if s, ok := ExtractSkillFromTarget("no-skill-here"); ok || s != "" {
		t.Errorf("expected false for no SKILL.md")
	}
	if s, ok := ExtractSkillFromTarget("view_file /skills/foo/SKILL.md"); !ok || s != "foo" {
		t.Errorf("expected foo, got %s (ok=%v)", s, ok)
	}
}
