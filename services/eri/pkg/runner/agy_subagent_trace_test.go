package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func writeTestTranscript(t *testing.T, homeDir, convID, content string) string {
	t.Helper()
	p := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs", "transcript.jsonl")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("failed creating transcript dir: %v", err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("failed writing transcript: %v", err)
	}
	return p
}

func TestParseAgySubagentTranscript(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		lines      string
		wantOK     bool
		wantModel  string
		wantStatus string
		wantUsage  AgyUsage
		wantTools  []string
	}{
		{
			name: "top-level usage and parallel tool completions",
			lines: `{"step_index":0,"type":"USER_INPUT","created_at":"2026-11-27T06:17:58.100Z","content":"audit code"}` + "\n" +
				`{"step_index":1,"type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-27T06:17:59.500Z","model":"gemini-4-argon-medium","input_tokens":2000,"output_tokens":150,"cache_read_tokens":500,"tool_calls":[{"name":"read_file","args":{"Path":"/share/a.go"}},{"name":"call_mcp_tool","args":{"ServerName":"\"github\"","ToolName":"\"get_me\""}}]}` + "\n" +
				`{"step_index":2,"type":"GENERIC","status":"DONE","created_at":"2026-11-27T06:18:00.200Z","content":"Created At: 2026-11-26T22:17:59-08:00\nCompleted At: 2026-11-26T22:18:00-08:00\nfile content"}` + "\n" +
				`{"step_index":3,"type":"GENERIC","status":"ERROR","created_at":"2026-11-27T06:18:01.300Z","content":"Created At: 2026-11-26T22:18:00-08:00\nCompleted At: 2026-11-26T22:18:01-08:00\nerror"}` + "\n" +
				`{"step_index":4,"type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-27T06:18:03.000Z","model":"gemini-4-argon-medium","input_tokens":3000,"output_tokens":250,"cache_read_tokens":1500,"content":"done"}` + "\n",
			wantOK:     true,
			wantModel:  "gemini-4-argon-medium",
			wantStatus: "ok",
			wantUsage:  AgyUsage{InputTokens: 5000, OutputTokens: 400, CacheReadTokens: 2000, TotalTokens: 5400},
			wantTools:  []string{"read_file:native:ok", "get_me:github:error"},
		},
		{
			name:       "nested usage object and uncompleted tool marked aborted",
			lines:      `{"step_index":1,"type":"PLANNER_RESPONSE","status":"ERROR","created_at":"2026-11-27T06:18:00Z","model":"gemini-3.8-flash","usage":{"input_tokens":100,"output_tokens":20},"tool_calls":[{"name":"run_command","args":{}}]}` + "\n",
			wantOK:     true,
			wantModel:  "gemini-3.8-flash",
			wantStatus: "error",
			wantUsage:  AgyUsage{InputTokens: 100, OutputTokens: 20, TotalTokens: 120},
			wantTools:  []string{"run_command:native:aborted"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := writeTestTranscript(t, t.TempDir(), "sub-1", tc.lines)
			summary, ok := parseAgySubagentTranscript(path, "step-3", "sub-1")
			if ok != tc.wantOK {
				t.Fatalf("parseAgySubagentTranscript ok = %v, want %v", ok, tc.wantOK)
			}
			if summary.Model != tc.wantModel || summary.Status != tc.wantStatus || summary.Usage != tc.wantUsage {
				t.Errorf("summary = (model=%q, status=%q, usage=%+v), want (%q, %q, %+v)", summary.Model, summary.Status, summary.Usage, tc.wantModel, tc.wantStatus, tc.wantUsage)
			}
			if len(summary.Steps) != len(tc.wantTools) {
				t.Fatalf("steps = %+v, want %v", summary.Steps, tc.wantTools)
			}
			for i, st := range summary.Steps {
				got := st.Name + ":" + st.MCPServer + ":" + st.Status
				if got != tc.wantTools[i] {
					t.Errorf("step %d = %q, want %q", i, got, tc.wantTools[i])
				}
				if st.EndTime.Before(st.StartTime) {
					t.Errorf("step %d EndTime %v precedes StartTime %v", i, st.EndTime, st.StartTime)
				}
			}
		})
	}
}

func TestStreamingDaemon_AgyAsyncSubagentTraceLifecycle(t *testing.T) {
	t.Parallel()
	homeDir := t.TempDir()
	parentID := "00000000-0000-0000-0000-000000000901"
	subID := "67c495ef-882b-4faf-994d-00518cd0bea6"

	subTranscript := writeTestTranscript(t, homeDir, subID,
		`{"step_index":0,"type":"USER_INPUT","created_at":"2026-11-27T06:17:58Z","content":"review plan"}`+"\n"+
			`{"step_index":1,"type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-27T06:17:59Z","model":"gemini-4-argon-medium","input_tokens":1200,"output_tokens":80,"cache_read_tokens":400,"tool_calls":[{"name":"read_file","args":{"Path":"/share/main.go"}}]}`+"\n"+
			`{"step_index":2,"type":"GENERIC","status":"DONE","created_at":"2026-11-27T06:18:01Z","content":"Created At: 2026-11-26T22:17:59-08:00\nCompleted At: 2026-11-26T22:18:01-08:00\npackage main"}`+"\n"+
			`{"step_index":3,"type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-27T06:18:05Z","model":"gemini-4-argon-medium","input_tokens":1500,"output_tokens":120,"cache_read_tokens":1000,"content":"LGTM"}`+"\n",
	)

	parentLines := `{"step_index":3,"type":"GENERIC","status":"DONE","content":"Created At: 2026-11-26T22:17:56-08:00\nCompleted At: 2026-11-26T22:17:58-08:00\nCreated the following subagent:\n{\n  \"conversationId\": \"` + subID + `\",\n  \"logAbsoluteUri\": \"file://` + subTranscript + `\"\n}\n"}` + "\n"
	parentPath := writeTestTranscript(t, homeDir, parentID, parentLines)

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	turnCtx, turnSpan := tp.Tracer("test").Start(context.Background(), "turn")

	inner := newMockTurnSink()
	traceSink := NewTracingTurnSink(turnCtx, inner)
	bufSink := NewBufferingTurnSink(BufferingTurnSinkConfig{
		OnTextDelta: inner.OnTextDelta,
		OnComplete:  inner.OnResult,
	})
	wrappedSink := &leaseTurnSinkWrapper{inner: traceSink, buf: bufSink}

	cfg := DaemonConfig{SessionID: parentID, GeminiHomeDir: homeDir}
	daemon := &StreamingDaemon{
		cfg:           cfg,
		sessionID:     parentID,
		inFlightTools: make(map[int]inFlightToolCall),
	}
	if turn := enqueueInflightTurn(daemon, "turn-agy-sub", wrappedSink); turn == nil {
		t.Fatal("expected non-nil turn")
	}

	// 1. invoke_subagent starts and completes at step 3
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"RUNNING","type":"subagent","step_index":3,"tool_name":"invoke_subagent","tool_info":{"parameters":{"Role":"Reviewer"}}}}`)
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"DONE","type":"subagent","step_index":3,"tool_name":"invoke_subagent"}}`)

	// At this point execute_tool invoke_subagent has ended, while invoke_agent is still open
	if ended := rec.Ended(); len(ended) != 1 || ended[0].Name() != "execute_tool invoke_subagent" {
		t.Fatalf("expected only execute_tool invoke_subagent ended before wakeup, got %v", spanNames(ended))
	}

	// 2. Append SYSTEM_MESSAGE wakeup to parent transcript and emit step 5 update
	wakeupLine := `{"step_index":4,"type":"SYSTEM_MESSAGE","content":"[Message] sender=` + subID + ` content=LGTM"}` + "\n"
	if err := os.WriteFile(parentPath, []byte(parentLines+wakeupLine), 0o644); err != nil {
		t.Fatalf("failed appending wakeup line: %v", err)
	}
	daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"ACTIVE","step_type":"agent_response","step_index":5,"text_delta":"Subagent finished."}}`)
	daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"Subagent finished.","usage":{"input_tokens":800,"output_tokens":50}}}`)
	turnSpan.End()

	want := []wantSpan{
		{name: "execute_tool invoke_subagent", parent: "turn", code: codes.Unset},
		{name: "execute_tool read_file", parent: "invoke_agent", code: codes.Unset, attrs: []attribute.KeyValue{AttrGenAIToolName.String("read_file"), AttrMCPServer.String("native")}},
		{name: "chat", parent: "invoke_agent", code: codes.Unset, attrs: []attribute.KeyValue{AttrGenAIRequestModel.String("gemini-4-argon-medium"), AttrGenAIUsageInputTokens.Int(2700), AttrGenAIUsageOutputTokens.Int(200), AttrGenAIUsageCacheRead.Int(1400)}},
		{name: "invoke_agent", parent: "execute_tool invoke_subagent", code: codes.Unset, attrs: []attribute.KeyValue{AttrGenAIConversationID.String(subID)}},
		{name: "chat", parent: "turn", code: codes.Unset, attrs: []attribute.KeyValue{AttrGenAIUsageInputTokens.Int(800), AttrGenAIUsageOutputTokens.Int(50)}},
	}
	assertTracingTurnSpans(t, tracingTurnSinkCase{want: want}, rec.Ended())
}

func TestAgySubagentTranscriptResolution(t *testing.T) {
	t.Parallel()
	d := &StreamingDaemon{cfg: DaemonConfig{GeminiHomeDir: ""}}
	if p := d.agyTranscriptPath("conv-1"); p != "" {
		t.Errorf("expected empty transcript path when GeminiHomeDir empty, got %q", p)
	}
	d.cfg.GeminiHomeDir = t.TempDir()
	if p := d.agyTranscriptPath("../bad"); p != "" {
		t.Errorf("expected empty transcript path on path traversal, got %q", p)
	}
	if id, p := parseCreatedSubagentBlock("no marker"); id != "" || p != "" {
		t.Errorf("expected empty on missing marker, got %q %q", id, p)
	}
	if id, p := parseCreatedSubagentBlock("Created the following subagent: not-json"); id != "" || p != "" {
		t.Errorf("expected empty on missing braces, got %q %q", id, p)
	}
	if id, p := parseCreatedSubagentBlock("Created the following subagent: {invalid}"); id != "" || p != "" {
		t.Errorf("expected empty on invalid json, got %q %q", id, p)
	}
	if id, p := extractAgySubagentFromParentTranscript("", 1); id != "" || p != "" {
		t.Errorf("expected empty on empty parent path")
	}
	if id, p := extractAgySubagentFromParentTranscript("/nonexistent/transcript.jsonl", 1); id != "" || p != "" {
		t.Errorf("expected empty on missing file")
	}
	if parentTranscriptHasSubagentSender("", "sub-1", 1) || parentTranscriptHasSubagentSender("/nonexistent", "sub-1", 1) {
		t.Errorf("expected false on empty or missing parent transcript")
	}
	if _, ok := parseAgySubagentTranscript("", "p", "c"); ok {
		t.Errorf("expected false on empty subagent transcript path")
	}
	if _, ok := parseAgySubagentTranscript("/nonexistent", "p", "c"); ok {
		t.Errorf("expected false on missing subagent transcript")
	}

	// Check that a transcript containing only an earlier subagent block returns empty for a later step
	earlierTranscript := filepath.Join(t.TempDir(), "transcript.jsonl")
	if err := os.WriteFile(earlierTranscript, []byte(`{"step_index":3,"type":"PLANNER_RESPONSE","content":"Created the following subagent: {\"conversationId\":\"old-child\",\"logAbsoluteUri\":\"/tmp/old.jsonl\"}"}`+"\n"), 0o644); err != nil {
		t.Fatalf("failed writing earlier transcript: %v", err)
	}
	if id, p := extractAgySubagentFromParentTranscript(earlierTranscript, 5); id != "" || p != "" {
		t.Errorf("expected empty conversation ID and path when step_index does not match, got %q %q", id, p)
	}
}

func TestAgySubagentTurnEndAbortWithoutWakeup(t *testing.T) {
	t.Parallel()
	d := &StreamingDaemon{cfg: DaemonConfig{GeminiHomeDir: t.TempDir()}}

	// Verify fallback completion at turn end when SYSTEM_MESSAGE was not probed on step_update.
	// A child transcript with no wakeup message in parent transcript must mark invoke_agent aborted.
	subID := "sub-fallback-end"
	subPath := writeTestTranscript(t, d.cfg.GeminiHomeDir, subID, `{"step_index":1,"type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-27T06:20:00Z","model":"gemini-4-argon-medium","tool_calls":[{"name":"bash","args":{"command":"ls"}}],"input_tokens":50,"output_tokens":10}`+"\n")
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	turnCtx, turnSpan := tp.Tracer("test").Start(context.Background(), "turn")
	traceSink := NewTracingTurnSink(turnCtx, newMockTurnSink())
	traceSink.OnToolCall(ToolCall{ID: "step-2", Name: "invoke_subagent", Subagent: true})
	traceSink.OnToolCompleted(ToolCall{ID: "step-2", Name: "invoke_subagent", Subagent: true, SubagentConversationID: subID}, "native", time.Millisecond, "ok")

	turn := &TurnContext{
		Sink: traceSink,
		pendingAgySubagents: []pendingAgySubagent{{
			parentCallID:   "step-2",
			conversationID: subID,
			transcriptPath: subPath,
			stepIndex:      2,
		}},
	}
	d.completePendingAgySubagents(turn)
	traceSink.OnResult(&TurnResult{})
	turnSpan.End()
	if len(rec.Ended()) != 6 {
		t.Errorf("expected 6 spans ended (parent tool, child bash, child chat, invoke_agent, parent chat, turn), got %d: %v", len(rec.Ended()), spanNames(rec.Ended()))
	}
	var invokeAgentSpan sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "invoke_agent" {
			invokeAgentSpan = s
			break
		}
	}
	if invokeAgentSpan == nil {
		t.Fatalf("expected invoke_agent span")
	}
	if invokeAgentSpan.Status().Code != codes.Error || invokeAgentSpan.Status().Description != "aborted" {
		t.Errorf("expected invoke_agent aborted when no wakeup message, got code %v desc %q", invokeAgentSpan.Status().Code, invokeAgentSpan.Status().Description)
	}
}

func TestAgySubagentProbeMissingTranscriptDoesNotSynthesizeOK(t *testing.T) {
	t.Parallel()
	homeDir := t.TempDir()
	d := &StreamingDaemon{cfg: DaemonConfig{GeminiHomeDir: homeDir, SessionID: "parent-sess"}}
	d.sessionID = "parent-sess"

	parentPath := writeTestTranscript(t, homeDir, "parent-sess",
		`{"step_index":4,"type":"SYSTEM_MESSAGE","content":"[Message] sender=sub-missing content=LGTM"}`+"\n",
	)
	if parentPath == "" {
		t.Fatal("expected non-empty parent transcript path")
	}

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	turnCtx, turnSpan := tp.Tracer("test").Start(context.Background(), "turn")
	traceSink := NewTracingTurnSink(turnCtx, newMockTurnSink())
	traceSink.OnToolCall(ToolCall{ID: "step-2", Name: "invoke_subagent", Subagent: true})
	traceSink.OnToolCompleted(ToolCall{ID: "step-2", Name: "invoke_subagent", Subagent: true, SubagentConversationID: "sub-missing"}, "native", time.Millisecond, "ok")

	turn := &TurnContext{
		Sink: traceSink,
		pendingAgySubagents: []pendingAgySubagent{{
			parentCallID:   "step-2",
			conversationID: "sub-missing",
			transcriptPath: filepath.Join(homeDir, "nonexistent-transcript.jsonl"),
			stepIndex:      2,
		}},
	}

	// Probe on step 5 update where wakeup was seen in parent transcript, but child transcript is missing
	d.probePendingAgySubagentsOnStep(turn, stepUpdateFields{hasStepIdx: true, stepIdx: 5})

	// Subagent must remain pending (not completed as synthetic OK)
	d.inflightMu.Lock()
	pendingCount := len(turn.pendingAgySubagents)
	d.inflightMu.Unlock()
	if pendingCount != 1 {
		t.Fatalf("expected subagent to remain pending on missing transcript, got %d", pendingCount)
	}

	// invoke_agent span must still be open (only execute_tool invoke_subagent ended)
	if ended := rec.Ended(); len(ended) != 1 || ended[0].Name() != "execute_tool invoke_subagent" {
		t.Fatalf("expected only execute_tool invoke_subagent ended, got %v", spanNames(ended))
	}

	// Complete turn: since transcript never materialized, invoke_agent aborts on closeTurn
	d.completePendingAgySubagents(turn)
	traceSink.OnResult(&TurnResult{})
	turnSpan.End()

	var invokeAgentSpan sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "invoke_agent" {
			invokeAgentSpan = s
			break
		}
	}
	if invokeAgentSpan == nil {
		t.Fatalf("expected invoke_agent span")
	}
	if invokeAgentSpan.Status().Code != codes.Error || invokeAgentSpan.Status().Description != "aborted" {
		t.Errorf("expected invoke_agent aborted when transcript missing, got code %v desc %q", invokeAgentSpan.Status().Code, invokeAgentSpan.Status().Description)
	}
}

func TestAgySubagentResolvePreservesURITranscriptWhenDerivedNotOnDisk(t *testing.T) {
	t.Parallel()
	geminiHome := t.TempDir()
	d := &StreamingDaemon{
		cfg: DaemonConfig{GeminiHomeDir: geminiHome, SessionID: "parent-sess"},
	}
	d.sessionID = "parent-sess"

	customDir := t.TempDir()
	subID := "child-custom-1"
	customSubTranscript := filepath.Join(customDir, subID, "transcript.jsonl")
	if err := os.MkdirAll(filepath.Dir(customSubTranscript), 0o755); err != nil {
		t.Fatalf("failed creating custom sub dir: %v", err)
	}
	if err := os.WriteFile(customSubTranscript, []byte(`{"step_index":1}`), 0o644); err != nil {
		t.Fatalf("failed writing custom sub transcript: %v", err)
	}

	parentContent := `{"step_index":2,"type":"PLANNER_RESPONSE","content":"Created the following subagent: {\"conversationId\":\"` + subID + `\",\"logAbsoluteUri\":\"` + customSubTranscript + `\"}"}` + "\n"
	writeTestTranscript(t, geminiHome, "parent-sess", parentContent)

	activeTurn := &TurnContext{}
	rec := inFlightToolCall{
		stepIndex: 2,
		call: ToolCall{
			ID:       "call-2",
			Name:     "invoke_subagent",
			Subagent: true,
		},
	}

	call := d.resolveAgyCompletedToolCall(activeTurn, rec, "ok")
	if call.SubagentConversationID != subID {
		t.Errorf("expected SubagentConversationID %q, got %q", subID, call.SubagentConversationID)
	}

	if len(activeTurn.pendingAgySubagents) != 1 {
		t.Fatalf("expected 1 pending subagent, got %d", len(activeTurn.pendingAgySubagents))
	}
	// The derived candidate path does not exist on disk, so the valid URI path must be preserved
	if got := activeTurn.pendingAgySubagents[0].transcriptPath; got != customSubTranscript {
		t.Errorf("expected transcriptPath preserved as %q, got %q", customSubTranscript, got)
	}
}
