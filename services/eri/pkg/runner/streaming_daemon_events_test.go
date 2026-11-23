package runner

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamingDaemon_ToolCallIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		engine          string
		lines           []string
		wantCalls       []ToolCall
		wantCompletions []ToolCall
	}{
		{
			name:   "claude subagent children carry parent_tool_use_id",
			engine: "claude",
			lines: []string{
				`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_agent","name":"Agent","input":{"description":"research"}}]}}`,
				`{"type":"assistant","parent_tool_use_id":"toolu_agent","message":{"content":[{"type":"tool_use","id":"toolu_a","name":"Grep","input":{"pattern":"x"}},{"type":"tool_use","id":"toolu_b","name":"Grep","input":{"pattern":"y"}}]}}`,
				`{"type":"user","parent_tool_use_id":"toolu_agent","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_b","is_error":true}]}}`,
				`{"type":"user","parent_tool_use_id":"toolu_agent","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_a"}]}}`,
				`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_agent"}]}}`,
			},
			wantCalls: []ToolCall{
				{ID: "toolu_agent", Name: "Agent", Subagent: true},
				{ID: "toolu_a", ParentID: "toolu_agent", Name: "Grep"},
				{ID: "toolu_b", ParentID: "toolu_agent", Name: "Grep"},
			},
			wantCompletions: []ToolCall{
				{ID: "toolu_b", ParentID: "toolu_agent", Name: "Grep"},
				{ID: "toolu_a", ParentID: "toolu_agent", Name: "Grep"},
				{ID: "toolu_agent", Name: "Agent", Subagent: true},
			},
		},
		{
			name:   "agy steps use step index ids and subagent step type",
			engine: "",
			lines: []string{
				`{"event":"step_update","step_update":{"step_index":3,"type":"subagent","tool_name":"invoke_subagent"}}`,
				`{"event":"step_update","step_update":{"step_index":4,"type":"tool_call","tool_name":"grep_search"}}`,
				`{"event":"step_update","step_update":{"step_index":4,"type":"tool_call","state":"DONE"}}`,
				`{"event":"step_update","step_update":{"step_index":3,"type":"subagent","state":"ERROR"}}`,
			},
			wantCalls: []ToolCall{
				{ID: "step-3", Name: "invoke_subagent", Subagent: true},
				{ID: "step-4", Name: "grep_search"},
			},
			wantCompletions: []ToolCall{
				{ID: "step-4", Name: "grep_search"},
				{ID: "step-3", Name: "invoke_subagent", Subagent: true},
			},
		},
		{
			name:   "claude mcp tools carry their server and tool",
			engine: "claude",
			lines: []string{
				`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_m","name":"mcp__docket__create","input":{}},{"type":"tool_use","id":"toolu_n","name":"Grep","input":{}}]}}`,
				`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_m"}]}}`,
			},
			wantCalls: []ToolCall{
				{ID: "toolu_m", Name: "create", MCPServer: "docket", MCPTool: "create"},
				{ID: "toolu_n", Name: "Grep"},
			},
			wantCompletions: []ToolCall{{ID: "toolu_m", Name: "create"}},
		},
		{
			name:   "agy call_mcp_tool keeps its name and carries the resolved server and tool",
			engine: "",
			lines: []string{
				`{"event":"step_update","step_update":{"step_index":5,"type":"tool_call","tool_name":"call_mcp_tool","tool_info":{"parameters":{"ServerName":"github","ToolName":"get_me"}}}}`,
				`{"event":"step_update","step_update":{"step_index":6,"type":"tool_call","tool_name":"call_mcp_tool","tool_info":{"parameters":{"ToolName":"get_me"}}}}`,
				`{"event":"step_update","step_update":{"step_index":5,"type":"tool_call","state":"DONE"}}`,
			},
			wantCalls: []ToolCall{
				{ID: "step-5", Name: "call_mcp_tool", Command: "get_me", MCPServer: "github", MCPTool: "get_me"},
				{ID: "step-6", Name: "call_mcp_tool", Command: "get_me"},
			},
			wantCompletions: []ToolCall{{ID: "step-5", Name: "get_me"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := DaemonConfig{Engine: tc.engine, SessionID: "99999999-8888-7777-6666-000000000001", ClaudeBin: "claude", GeminiHomeDir: t.TempDir()}
			h := startTestStreamingDaemon(t, cfg, "", true)
			defer h.daemon.Close()
			sink := newMockTurnSink()
			turn := &TurnContext{TurnID: "identity", Prompt: "p", Sink: sink, CreatedAt: time.Now()}
			if err := h.daemon.Send(turn.Prompt, turn); err != nil {
				t.Fatalf("send failed: %v", err)
			}
			for _, line := range tc.lines {
				h.daemon.dispatchNDJSONLine(line)
			}
			sink.mu.Lock()
			defer sink.mu.Unlock()
			if !reflect.DeepEqual(sink.calls, tc.wantCalls) {
				t.Errorf("calls = %+v, want %+v", sink.calls, tc.wantCalls)
			}
			if !reflect.DeepEqual(sink.completions, tc.wantCompletions) {
				t.Errorf("completions = %+v, want %+v", sink.completions, tc.wantCompletions)
			}
		})
	}
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
	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-tools-1", Prompt: "run tool check", Sink: sink, CreatedAt: time.Now()}
	if err := h.daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	events := `{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":1,"tool_name":"run_command","tool_info":{"parameters":{"CommandLine":"echo hello"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":1,"tool_name":"run_command"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":2,"tool_name":"call_mcp_tool","tool_info":{"parameters":{"ServerName":"github","ToolName":"get_me"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":2,"tool_name":"call_mcp_tool"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":3,"tool_name":"view_file","tool_info":{"parameters":{"AbsolutePath":"/share/aerial-config/custom-skills/home-assistant/SKILL.md"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":3,"tool_name":"view_file"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":4,"tool_name":"invoke_subagent","tool_info":{"parameters":{"Role":"Researcher"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","step_type":"subagent","step_index":4,"tool_name":"invoke_subagent"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"ACTIVE","step_type":"tool","step_index":5,"tool_name":"custom_step_tool","tool_info":{"parameters":{}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"ERROR","step_type":"generic","step_index":5,"tool_name":"custom_step_tool"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":6,"tool_name":"read_file","tool_info":{"parameters":{"Path":"/data/runtimes/discord/.gemini/config/skills/incident-triage/SKILL.md"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":6,"tool_name":"read_file","tool_info":{"parameters":{"Path":"/data/runtimes/discord/.gemini/config/skills/incident-triage/SKILL.md"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":6,"tool_name":"read_file"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":7,"tool_name":"run_command","tool_info":{"parameters":{"CommandLine":"cat /share/aerial/.agents/skills/discord/self-improvement/SKILL.md","toolSummary":"Read skill"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":7,"tool_name":"run_command"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":8,"tool_name":"run_command","tool_info":{"parameters":{"CommandLine":"rg -n foo /share/aerial/.agents/skills/discord/self-improvement/SKILL.md","toolSummary":"Search skill"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":8,"tool_name":"run_command"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":9,"tool_name":"edit_file","tool_info":{"parameters":{"Path":"/share/aerial/.agents/skills/discord/self-improvement/SKILL.md"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":9,"tool_name":"edit_file"}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":10,"tool_name":"manage_task","tool_info":{"parameters":{}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","step_type":"agent_response","step_index":11}}` + "\n" +
		`{"event":"result","result":{"status":"SUCCESS","response":"done"}}` + "\n"
	_, _ = h.outW.Write([]byte(events))

	waitAndAssertSinkResult(t, "ToolEvents", sink, "done", "", "")
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.completedToolDetails) != 10 {
		t.Fatalf("expected 10 completed tool records, got %d: %+v", len(sink.completedToolDetails), sink.completedToolDetails)
	}
	expectedDetails := []mockCompletedTool{
		{ToolName: "run_command", MCPServer: "native", Status: "ok"},
		{ToolName: "get_me", MCPServer: "github", Status: "ok"},
		{ToolName: "view_file", MCPServer: "native", Status: "ok"},
		{ToolName: "invoke_subagent", MCPServer: "native", Status: "ok"},
		{ToolName: "custom_step_tool", MCPServer: "native", Status: "error"},
		{ToolName: "read_file", MCPServer: "native", Status: "ok"},
		{ToolName: "run_command", MCPServer: "native", Status: "ok"},
		{ToolName: "run_command", MCPServer: "native", Status: "ok"},
		{ToolName: "edit_file", MCPServer: "native", Status: "ok"},
		{ToolName: "manage_task", MCPServer: "native", Status: "aborted"},
	}
	for i, exp := range expectedDetails {
		c := sink.completedToolDetails[i]
		if c.ToolName != exp.ToolName || c.MCPServer != exp.MCPServer || c.Status != exp.Status {
			t.Errorf("record %d: got %+v, want %+v", i, c, exp)
		}
	}
	wantSkills := []string{"home-assistant:discord", "incident-triage:discord", "self-improvement:discord"}
	if len(sink.activatedSkills) != len(wantSkills) || strings.Join(sink.activatedSkills, ",") != strings.Join(wantSkills, ",") {
		t.Errorf("unexpected skill activations: got %+v, want %+v", sink.activatedSkills, wantSkills)
	}
	for i, tc := range []string{"run_command:echo", "call_mcp_tool:get_me", "view_file:SKILL.md", "invoke_subagent:Researcher", "custom_step_tool:", "read_file:SKILL.md", "run_command:Read skill", "run_command:Search skill", "edit_file:SKILL.md", "manage_task:"} {
		if len(sink.toolCalls) <= i || sink.toolCalls[i] != tc {
			t.Errorf("expected tool call %d to be %q, got %v", i, tc, sink.toolCalls)
		}
	}
}

func TestExtractToolDetail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		toolName    string
		params      map[string]any
		fallbackCmd string
		want        string
	}{
		{
			name:     "edit_file extracts path basename",
			toolName: "edit_file",
			params:   map[string]any{"Path": "/path/to/main.go"},
			want:     "main.go",
		},
		{
			name:     "write_file extracts target_file",
			toolName: "write_file",
			params:   map[string]any{"target_file": "server.ts"},
			want:     "server.ts",
		},
		{
			name:     "run_command prefers toolSummary",
			toolName: "run_command",
			params:   map[string]any{"toolSummary": "Build Go Binary", "CommandLine": "go build ./..."},
			want:     "Build Go Binary",
		},
		{
			name:     "run_command falls back to sanitized command from CommandLine",
			toolName: "run_command",
			params:   map[string]any{"CommandLine": "echo 'first line'\necho 'second line'"},
			want:     "echo",
		},
		{
			name:     "run_command sanitizes command with environment variables and flags",
			toolName: "run_command",
			params:   map[string]any{"CommandLine": "FOO=bar ./deploy.sh --prod"},
			want:     "deploy.sh",
		},
		{
			name:     "call_mcp_tool extracts ToolName",
			toolName: "call_mcp_tool",
			params:   map[string]any{"ToolName": "ha_get_overview"},
			want:     "ha_get_overview",
		},
		{
			name:     "grep_search extracts query",
			toolName: "grep_search",
			params:   map[string]any{"query": "ExtractToolDetail"},
			want:     "ExtractToolDetail",
		},
		{
			name:     "invoke_subagent extracts role",
			toolName: "invoke_subagent",
			params:   map[string]any{"Role": "Research Analyst"},
			want:     "Research Analyst",
		},
		{
			name:        "fallbackCmd used when params empty",
			toolName:    "unknown_tool",
			params:      nil,
			fallbackCmd: "fallback-value",
			want:        "fallback-value",
		},
		{
			name:        "run_command rejects unsanitized subshell command without leaking fallback",
			toolName:    "run_command",
			params:      map[string]any{"CommandLine": "(cd dir && make)"},
			fallbackCmd: "(cd dir && make)",
			want:        "",
		},
		{
			name:        "run_command rejects env-only command without leaking fallback",
			toolName:    "run_command",
			params:      map[string]any{"CommandLine": "SECRET_KEY=12345"},
			fallbackCmd: "SECRET_KEY=12345",
			want:        "",
		},
		{
			name:        "generic tool rejects unsanitized command without leaking fallback",
			toolName:    "custom_runner",
			params:      map[string]any{"CommandLine": "(cd dir && make)"},
			fallbackCmd: "(cd dir && make)",
			want:        "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ExtractToolDetail(tc.toolName, tc.params, tc.fallbackCmd)
			if got != tc.want {
				t.Errorf("ExtractToolDetail(%q, %v, %q) = %q, want %q", tc.toolName, tc.params, tc.fallbackCmd, got, tc.want)
			}
		})
	}
}

func TestStreamingDaemon_UnsanitizedCommandLineDoesNotLeak(t *testing.T) {
	h := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000203"), "", true)
	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-leak-check", Prompt: "run unsanitized command", Sink: sink, CreatedAt: time.Now()}
	if err := h.daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	events := `{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":1,"tool_name":"run_command","tool_info":{"parameters":{"CommandLine":"(cd /secret && cat token.txt)"}}}}` + "\n" +
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","step_index":1,"tool_name":"run_command"}}` + "\n" +
		`{"event":"result","result":{"status":"SUCCESS","response":"done"}}` + "\n"
	_, _ = h.outW.Write([]byte(events))

	waitAndAssertSinkResult(t, "UnsanitizedCommandNoLeak", sink, "done", "", "")
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.toolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d: %+v", len(sink.toolCalls), sink.toolCalls)
	}
	if sink.toolCalls[0] != "run_command:" {
		t.Errorf("expected tool call to be sanitized to 'run_command:', got %q", sink.toolCalls[0])
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
	mockSink := newMockTurnSink()
	bufSink := NewBufferingTurnSink(BufferingTurnSinkConfig{
		OnStepStarted:   mockSink.OnStepStarted,
		OnToolCall:      mockSink.recordToolCall,
		OnToolCompleted: mockSink.recordToolCompleted,
		OnTextDelta:     mockSink.OnTextDelta,
		OnComplete:      mockSink.OnResult,
		OnError:         mockSink.OnError,
	})
	enqueueInflightTurn(h.daemon, "turn-step-aware", bufSink, func(tc *TurnContext) {
		tc.SessionID = testSessionUUID
		tc.Model = "gemini-3.8-flash-high"
		tc.Prompt = "test step-aware purging"
	})
	steps := []struct{ line, want string }{
		{`{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"agent_response","text_delta":"Let me wait for task..."}}`, "Let me wait for task..."},
		{`{"event":"step_update","step_update":{"step_index":2,"state":"ACTIVE","step_type":"tool","tool_name":"bg_daemon"}}`, ""},
		{`{"event":"step_update","step_index":3,"step_update":{"state":"ACTIVE","step_type":"agent_response","text_delta":"All work completed! Ready to roll."}}`, "All work completed! Ready to roll."},
	}
	for i, s := range steps {
		h.daemon.dispatchNDJSONLine(s.line)
		if bufSink.AccumulatedText() != s.want {
			t.Fatalf("step %d: got %q, want %q", i+1, bufSink.AccumulatedText(), s.want)
		}
	}
	h.daemon.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"Let me wait for task... All work completed! Ready to roll."}}`)
	res, err := bufSink.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	expected := "All work completed! Ready to roll."
	if res.Response != expected {
		t.Errorf("expected clean terminal deliverable %q, got %q", expected, res.Response)
	}
	mockSink.mu.Lock()
	stepStarts, completed := mockSink.stepStarts, mockSink.completedToolDetails
	mockSink.mu.Unlock()
	if len(stepStarts) != 3 || stepStarts[0] != 1 || stepStarts[1] != 2 || stepStarts[2] != 3 {
		t.Errorf("unexpected step starts recorded: %v", stepStarts)
	}
	if len(completed) != 1 || completed[0].ToolName != "bg_daemon" || completed[0].Status != "aborted" {
		t.Errorf("expected bg_daemon aborted completion without purging final response, got %+v", completed)
	}
}

func dispatchClaudeLifecycleEvents(d *StreamingDaemon) {
	// 1. System init event latching session ID
	d.dispatchNDJSONLine(`{"type":"system","subtype":"init","session_id":"12345678-1234-5678-1234-567812345678"}`)
	// 2. Stream event: message_start
	d.dispatchNDJSONLine(`{"type":"stream_event","event":{"type":"message_start"}}`)
	// 3. Stream event: thinking_delta
	d.dispatchNDJSONLine(`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta"}}}`)
	// 4. Stream event: text_delta
	d.dispatchNDJSONLine(`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"Thinking about "}}}`)
	d.dispatchNDJSONLine(`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"it..."}}}`)
	// 5. Assistant event: tool_use (double underscore mcp format and skill)
	d.dispatchNDJSONLine(`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"call_1","name":"mcp__github__create_pull_request","input":{"title":"my PR"}},{"type":"tool_use","id":"call_2","name":"run_command","input":{"CommandLine":"cat /share/aerial-config/custom-skills/ha-operations/SKILL.md"}}]}}`)
	// 6. User event: tool_result
	d.dispatchNDJSONLine(`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"call_1","is_error":false}]}}`)
	d.dispatchNDJSONLine(`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"call_2","is_error":true}]}}`)
	// 7. Result event: success with usage tokens
	d.dispatchNDJSONLine(`{"type":"result","result":"all done successfully","usage":{"input_tokens":100,"output_tokens":50,"cache_creation_input_tokens":20,"cache_read_input_tokens":10}}`)
}

func assertClaudeTurnSinkState(t *testing.T, sink *mockTurnSink) {
	t.Helper()
	sink.mu.Lock()
	defer sink.mu.Unlock()

	if sink.thinking < 1 {
		t.Errorf("expected OnThinking to be called, got %d", sink.thinking)
	}
	if strings.Join(sink.deltas, "") != "Thinking about it..." {
		t.Errorf("unexpected deltas: %v", sink.deltas)
	}
	if len(sink.toolCalls) != 2 {
		t.Fatalf("expected 2 tool calls, got %d: %v", len(sink.toolCalls), sink.toolCalls)
	}
	if sink.toolCalls[0] != "create_pull_request:" {
		t.Errorf("expected toolCall[0] 'create_pull_request:', got %q", sink.toolCalls[0])
	}
	if sink.toolCalls[1] != "run_command:cat" {
		t.Errorf("expected toolCall[1] 'run_command:cat', got %q", sink.toolCalls[1])
	}
	if len(sink.activatedSkills) != 1 || sink.activatedSkills[0] != "ha-operations:discord" {
		t.Errorf("expected skill 'ha-operations:discord', got %v", sink.activatedSkills)
	}
	if len(sink.completedToolDetails) != 2 {
		t.Fatalf("expected 2 completed tools, got %d: %+v", len(sink.completedToolDetails), sink.completedToolDetails)
	}
	if sink.completedToolDetails[0].ToolName != "create_pull_request" || sink.completedToolDetails[0].MCPServer != "github" || sink.completedToolDetails[0].Status != "ok" {
		t.Errorf("unexpected completed tool 0: %+v", sink.completedToolDetails[0])
	}
	if sink.completedToolDetails[1].ToolName != "run_command" || sink.completedToolDetails[1].MCPServer != "native" || sink.completedToolDetails[1].Status != "error" {
		t.Errorf("unexpected completed tool 1: %+v", sink.completedToolDetails[1])
	}
	if sink.result == nil {
		t.Fatalf("expected non-nil result")
	}
	// input: 100 + 20 = 120, cache_read: 10, total: 120 + 50 = 170
	if sink.result.Usage.InputTokens != 120 || sink.result.Usage.CacheReadTokens != 10 || sink.result.Usage.TotalTokens != 170 || sink.result.Usage.OutputTokens != 50 {
		t.Errorf("unexpected usage in result: %+v", sink.result.Usage)
	}
}

func TestStreamingDaemon_Claude_FullLifecycle(t *testing.T) {
	claudeSess := "99999999-8888-7777-6666-555555555555"
	cfg := DaemonConfig{
		Engine:        "claude",
		SessionID:     claudeSess,
		ClaudeBin:     "claude",
		GeminiHomeDir: "/tmp/claude-home",
	}
	h := startTestStreamingDaemon(t, cfg, "", true)
	defer h.daemon.Close()

	if h.daemon.State() != StateReady {
		t.Fatalf("expected StateReady on spawn, got %s", h.daemon.State())
	}
	if h.daemon.SessionID() != claudeSess {
		t.Fatalf("expected session ID %s, got %s", claudeSess, h.daemon.SessionID())
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "claude-turn-1",
		Prompt:    "fix the bug",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	if err := h.daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	dispatchClaudeLifecycleEvents(h.daemon)
	if h.daemon.SessionID() != "12345678-1234-5678-1234-567812345678" {
		t.Errorf("expected latched session ID, got %s", h.daemon.SessionID())
	}

	waitAndAssertSinkResult(t, "ClaudeTurn", sink, "all done successfully", "", "")
	assertClaudeTurnSinkState(t, sink)
}

func TestStreamingDaemon_Claude_ErrorHandling(t *testing.T) {
	tests := []struct {
		name       string
		ndjsonLine string
		wantResp   string
		wantErrSub string
		wantStatus string
	}{
		{
			name:       "IsErrorTrueWithStringResult",
			ndjsonLine: `{"type":"result","is_error":true,"result":"rate limit exceeded"}`,
			wantErrSub: "rate limit exceeded",
		},
		{
			name:       "SubtypeErrorWithErrorsArray",
			ndjsonLine: `{"type":"result","subtype":"error_rate_limit","errors":[{"message":"too many tokens"},{"message":"retry later"}]}`,
			wantErrSub: "too many tokens; retry later",
		},
		{
			name:       "SubtypeErrorWithStringErrorsArray",
			ndjsonLine: `{"type":"result","subtype":"error_invalid_request","errors":["malformed payload"]}`,
			wantErrSub: "malformed payload",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DaemonConfig{
				Engine:    "claude",
				SessionID: "11111111-2222-3333-4444-555555555555",
			}
			daemon := &StreamingDaemon{
				cfg:           cfg,
				sessionID:     cfg.SessionID,
				claudeToolIDs: make(map[string]int),
			}
			sink := newMockTurnSink()
			enqueueInflightTurn(daemon, "turn-"+tc.name, sink)
			daemon.dispatchNDJSONLine(tc.ndjsonLine)
			waitAndAssertSinkResult(t, tc.name, sink, tc.wantResp, "", tc.wantErrSub)
		})
	}

	// Verify tailBuffer Write and String
	tb := newTailBuffer(10)
	if n, err := tb.Write([]byte("0123456789extra")); err != nil || n != 15 || tb.String() != "56789extra" {
		t.Errorf("tailBuffer failed: %q %v", tb.String(), err)
	}

	// Verify Startup with empty SessionID generates UUID
	hGen := startTestStreamingDaemon(t, DaemonConfig{Engine: "claude"}, "", false)
	defer hGen.daemon.Close()
	if !IsValidUUID(hGen.daemon.SessionID()) {
		t.Errorf("expected valid generated UUID, got %q", hGen.daemon.SessionID())
	}

	// Verify StartStreamingDaemon timeout with stderr tail diagnostics
	mockErrSpawner := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, c DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			go func() {
				_, _ = errW.Write([]byte("fatal startup error from binary"))
				closeQuietly(errW)
			}()
			_ = inR
			_ = outW
			return inW, outR, errR, &MockProcessHandle{pid: 9999}, nil
		},
	}
	_, errStart := StartStreamingDaemon(context.Background(), context.Background(), DaemonConfig{Timeout: 50 * time.Millisecond}, mockErrSpawner)
	if errStart == nil || !strings.Contains(errStart.Error(), "fatal startup error from binary") {
		t.Errorf("expected stderr diagnostics in timeout error, got %v", errStart)
	}

	// Verify tool_calls array in step_update
	hToolCalls := startTestStreamingDaemon(t, cfgForSession("00000000-0000-0000-0000-000000000301"), "", true)
	defer hToolCalls.daemon.Close()
	sinkTC := newMockTurnSink()
	enqueueInflightTurn(hToolCalls.daemon, "turn-tc", sinkTC)
	hToolCalls.daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"type":"tool_call","state":"RUNNING","tool_calls":[{"name":"bash","args":{"command":"echo test"}}]}}`)
	if len(sinkTC.toolCalls) != 1 || sinkTC.toolCalls[0] != "bash:echo" {
		t.Errorf("expected tool_calls entry handled, got %v", sinkTC.toolCalls)
	}

	// Verify formatEOFErr branches
	dNilTail := &StreamingDaemon{}
	if err := dNilTail.formatEOFErr(io.EOF); err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("expected unexpected EOF, got %v", err)
	}
	dEmptyTail := &StreamingDaemon{stderrTail: newTailBuffer(10)}
	if err := dEmptyTail.formatEOFErr(io.EOF); err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("expected unexpected EOF, got %v", err)
	}
	dWithTail := &StreamingDaemon{stderrTail: newTailBuffer(100)}
	_, _ = dWithTail.stderrTail.Write([]byte("crash log"))
	if err := dWithTail.formatEOFErr(io.EOF); err == nil || !strings.Contains(err.Error(), "crash log") {
		t.Errorf("expected crash log, got %v", err)
	}

	// Verify recordToolStart unindexed duplicate
	dTools := &StreamingDaemon{inFlightTools: make(map[int]inFlightToolCall)}
	if !dTools.recordToolStart(-1, false, ToolCall{Name: "read_file"}, "native") {
		t.Errorf("expected initial unindexed tool start true")
	}
	if dTools.recordToolStart(-1, false, ToolCall{Name: "read_file"}, "native") {
		t.Errorf("expected duplicate unindexed tool start false")
	}
}

func TestStreamingDaemon_TurnEnvFile(t *testing.T) {
	tests := []struct {
		name         string
		saTokenSrc   string
		wantTokenEnv string
	}{
		{"token file is copied and exported", "token", "export AERIAL_SA_TOKEN_PATH="},
		{"unreadable token source logs and omits the export", "token-dir", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setTurnSATokenSrc(t, tc.saTokenSrc)
			runTurnEnvFileCase(t, tc.wantTokenEnv)
		})
	}
}

func runTurnEnvFileCase(t *testing.T, wantTokenEnv string) {
	t.Helper()
	tempDir := t.TempDir()
	envFile := filepath.Join(tempDir, "turn_env", "test-daemon.sh")
	if err := os.MkdirAll(filepath.Dir(envFile), 0o755); err != nil {
		t.Fatalf("failed creating turn_env dir: %v", err)
	}
	if err := os.WriteFile(envFile, []byte("legacy\n"), 0o600); err != nil {
		t.Fatalf("failed pre-seeding 0600 turn env file: %v", err)
	}

	cfg := DaemonConfig{
		SessionID:   "00000000-0000-0000-0000-000000000305",
		ThreadID:    "discord-fallback-123",
		TurnEnvFile: envFile,
	}
	h := startTestStreamingDaemon(t, cfg, "", true)

	// Turn 1: explicit TargetID and VanadisChunkID
	turn1 := &TurnContext{TurnID: "t1", TargetID: "discord-987654", VanadisChunkID: "build-abc-chunk-1", Prompt: "turn 1", Sink: newMockTurnSink(), CreatedAt: time.Now()}
	if err := h.daemon.Send("turn 1", turn1); err != nil {
		t.Fatalf("send turn 1 failed: %v", err)
	}

	data1, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatalf("failed reading turn env file: %v", err)
	}
	if info, statErr := os.Stat(envFile); statErr != nil {
		t.Fatalf("failed stating turn env file: %v", statErr)
	} else if gotPerm := info.Mode().Perm(); gotPerm != 0o644 {
		t.Errorf("expected turn env file permissions 0644, got %04o", gotPerm)
	}
	content1 := string(data1)
	if !strings.Contains(content1, "unset BASH_ENV\n") ||
		!strings.Contains(content1, "export AERIAL_TARGET_ID='987654'\n") ||
		!strings.Contains(content1, "export AERIAL_VANADIS_CHUNK_ID='build-abc-chunk-1'\n") {
		t.Errorf("unexpected turn 1 env file content:\n%s", content1)
	}
	wantTokenLine := ""
	if wantTokenEnv != "" {
		wantTokenLine = wantTokenEnv + "'" + filepath.Join(tempDir, "turn_env", "sa-token") + "'\n"
	}
	if gotToken := strings.Contains(content1, "AERIAL_SA_TOKEN_PATH"); gotToken != (wantTokenLine != "") || !strings.Contains(content1, wantTokenLine) {
		t.Errorf("turn env file SA token export = %v, want line %q:\n%s", gotToken, wantTokenLine, content1)
	}

	// Turn 2: empty TargetID falls back to cfg.ThreadID, empty VanadisChunkID unsets
	turn2 := &TurnContext{TurnID: "t2", Prompt: "turn 2", Sink: newMockTurnSink(), CreatedAt: time.Now()}
	if err = h.daemon.Send("turn 2", turn2); err != nil {
		t.Fatalf("send turn 2 failed: %v", err)
	}

	data2, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatalf("failed reading turn env file for turn 2: %v", err)
	}
	content2 := string(data2)
	if !strings.Contains(content2, "export AERIAL_TARGET_ID='fallback-123'\n") ||
		!strings.Contains(content2, "unset AERIAL_VANADIS_CHUNK_ID\n") {
		t.Errorf("unexpected turn 2 env file content:\n%s", content2)
	}

	// Verify Close removes turn env file
	if err := h.daemon.Close(); err != nil {
		t.Fatalf("daemon close failed: %v", err)
	}
	if _, statErr := os.Stat(envFile); !os.IsNotExist(statErr) {
		t.Errorf("expected turn env file to be removed after Close(), got stat error: %v", statErr)
	}
}
