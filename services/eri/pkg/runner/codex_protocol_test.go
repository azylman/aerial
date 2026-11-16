package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func runMockCodexHandshakeServer(inR io.Reader, outW io.WriteCloser, replies []string, onTurnStart chan<- []byte) {
	defer closeQuietly(outW)
	scanner := bufio.NewScanner(inR)
	replyIdx := 0
	for scanner.Scan() {
		line := scanner.Text()
		var req codexRPCRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			continue
		}
		if req.Method == "initialized" {
			continue
		}
		if req.Method == "turn/start" && onTurnStart != nil {
			onTurnStart <- []byte(line)
		}
		if replyIdx < len(replies) {
			rep := replies[replyIdx]
			replyIdx++
			if rep == "CLOSE" {
				_ = outW.Close()
				return
			}
			_, _ = outW.Write([]byte(rep))
		}
	}
}

func TestStreamingDaemon_Codex_Handshake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfg           DaemonConfig
		mockReplies   []string
		wantErrSub    string
		wantSessionID string
	}{
		{
			name: "start new thread",
			cfg:  DaemonConfig{Engine: "codex", Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":3,"result":{"thread":{"id":"th_started_123"}}}` + "\n",
			},
			wantSessionID: "th_started_123",
		},
		{
			name: "resume existing thread",
			cfg:  DaemonConfig{Engine: "codex", SessionID: "th_existing_456", ResumeSession: true, Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"th_existing_456"}}}` + "\n",
			},
			wantSessionID: "th_existing_456",
		},
		{
			name: "resume error falls back to start",
			cfg:  DaemonConfig{Engine: "codex", SessionID: "th_stale", ResumeSession: true, Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"thread not found"}}` + "\n",
				`{"jsonrpc":"2.0","id":3,"result":{"thread":{"id":"th_fallback_789"}}}` + "\n",
			},
			wantSessionID: "th_fallback_789",
		},
		{
			name: "initialize error",
			cfg:  DaemonConfig{Engine: "codex", Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"unauthorized client"}}` + "\n",
			},
			wantErrSub: "unauthorized client",
		},
		{
			name: "thread start error",
			cfg:  DaemonConfig{Engine: "codex", Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"sandbox failed"}}` + "\n",
			},
			wantErrSub: "sandbox failed",
		},
		{
			name:        "handshake timeout",
			cfg:         DaemonConfig{Engine: "codex", Timeout: 50 * time.Millisecond},
			mockReplies: nil,
			wantErrSub:  "timed out waiting for init event",
		},
		{
			name:        "stdout closed immediately",
			cfg:         DaemonConfig{Engine: "codex", Timeout: 2 * time.Second},
			mockReplies: []string{"CLOSE"},
			wantErrSub:  "daemon stdout closed before init event",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer closeAll(inR, inW, outR, outW, errR, errW)

			switch {
			case len(tc.mockReplies) > 0 && tc.mockReplies[0] == "CLOSE":
				go func() { _, _ = io.Copy(io.Discard, inR) }()
				closeQuietly(outW)
			case tc.mockReplies != nil:
				go runMockCodexHandshakeServer(inR, outW, tc.mockReplies, nil)
			default:
				go func() { _, _ = io.Copy(io.Discard, inR) }()
			}

			handle := &MockProcessHandle{pid: 888}
			mock := &MockDaemonSpawner{
				SpawnFn: func(ctx context.Context, c DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
					return inW, outR, errR, handle, nil
				},
			}

			daemon, err := StartStreamingDaemon(context.Background(), context.Background(), tc.cfg, mock)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("expected error containing %q, got: %v", tc.wantErrSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected StartStreamingDaemon error: %v", err)
			}
			defer daemon.Close()

			if daemon.State() != StateReady {
				t.Errorf("expected StateReady, got %s", daemon.State())
			}
			if daemon.SessionID() != tc.wantSessionID {
				t.Errorf("expected SessionID %q, got %q", tc.wantSessionID, daemon.SessionID())
			}
		})
	}
}

func TestStreamingDaemon_Codex_FullLifecycle(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeAll(inR, inW, outR, outW, errR, errW)

	mockReplies := []string{
		`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
		`{"jsonrpc":"2.0","id":3,"result":{"thread":{"id":"th_codex_life_1"}}}` + "\n",
	}
	readTurnStart := make(chan []byte, 1)
	go runMockCodexHandshakeServer(inR, outW, mockReplies, readTurnStart)

	handle := &MockProcessHandle{pid: 999}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, c DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, handle, nil
		},
	}

	cfg := DaemonConfig{
		Engine:    "codex",
		Model:     "gpt-5-high",
		Effort:    "low",
		SessionID: "th_codex_life_1",
	}
	daemon, err := StartStreamingDaemon(context.Background(), context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("StartStreamingDaemon failed: %v", err)
	}
	defer daemon.Close()

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-codex-1",
		Prompt:    "hello codex",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	if err := daemon.Send(turn.Prompt, turn); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	var rawSent []byte
	select {
	case rawSent = <-readTurnStart:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out reading turn/start from stdin")
	}

	var sentReq codexRPCRequest
	if err := json.Unmarshal(rawSent, &sentReq); err != nil {
		t.Fatalf("failed unmarshaling sent turn/start: %v", err)
	}
	if sentReq.Method != "turn/start" {
		t.Errorf("expected method turn/start, got %q", sentReq.Method)
	}
	paramsMap, ok := sentReq.Params.(map[string]any)
	if !ok {
		t.Fatalf("expected map params, got %T", sentReq.Params)
	}
	if paramsMap["model"] != "gpt-5" || paramsMap["effort"] != "high" {
		t.Errorf("expected model gpt-5 effort high, got model=%v effort=%v", paramsMap["model"], paramsMap["effort"])
	}

	streamEvents := []string{
		`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"item-mcp-1","type":"mcpToolCall","server":"docket","tool":"mcp_docket_create","arguments":{"title":"fix bug"}}}}`,
		`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"item-cmd-1","type":"commandExecution","command":"git status"}}}`,
		`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"item-file-1","type":"fileChange","path":"/path/to/main.go"}}}`,
		`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"item-web-1","type":"webSearch","query":"aerial project"}}}`,
		`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"item-collab-1","type":"collabAgentToolCall","agent":"researcher"}}}`,
		`{"jsonrpc":"2.0","method":"item/reasoning/textDelta","params":{"itemId":"reason-1","delta":"thinking step 1"}}`,
		`{"jsonrpc":"2.0","method":"item/reasoning/textDelta","params":{"itemId":"reason-1","delta":"thinking step 2"}}`,
		`{"jsonrpc":"2.0","method":"item/reasoning/summaryTextDelta","params":{"itemId":"reason-2","delta":"summary step 1"}}`,
		`{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"delta":"Here is the result"}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-mcp-1","type":"mcpToolCall","status":"completed"}}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-cmd-1","type":"commandExecution","status":"completed"}}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-file-1","type":"fileChange","status":"completed"}}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-web-1","type":"webSearch","status":"completed"}}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-collab-1","type":"collabAgentToolCall","status":"completed"}}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-msg-1","type":"agentMessage","text":"Here is the full completed result."}}}`,
		`{"jsonrpc":"2.0","method":"thread/tokenUsage/updated","params":{"tokenUsage":{"last":{"inputTokens":120,"cachedInputTokens":20,"outputTokens":60,"reasoningOutputTokens":15,"totalTokens":180}}}}`,
		`{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"status":"completed"}}}`,
	}

	for _, ev := range streamEvents {
		daemon.dispatchNDJSONLine(ev)
	}

	verifyCodexLifecycleSink(t, sink)
}

func verifyCodexLifecycleSink(t *testing.T, sink *mockTurnSink) {
	t.Helper()
	waitAndAssertSinkResult(t, "CodexFullLifecycle", sink, "Here is the full completed result.", "", "")

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if sink.thinking != 2 {
		t.Errorf("expected 2 thinking notifications (one per reasoning item ID), got %d", sink.thinking)
	}
	if len(sink.deltas) != 1 || sink.deltas[0] != "Here is the result" {
		t.Errorf("unexpected deltas: %v", sink.deltas)
	}
	if len(sink.toolCalls) != 5 {
		t.Errorf("expected 5 tool calls, got %d: %v", len(sink.toolCalls), sink.toolCalls)
	}
	if len(sink.completedTools) != 5 {
		t.Errorf("expected 5 completed tools, got %d: %v", len(sink.completedTools), sink.completedTools)
	}

	if sink.result.Usage.InputTokens != 100 || sink.result.Usage.CacheReadTokens != 20 || sink.result.Usage.OutputTokens != 60 || sink.result.Usage.ThinkingTokens != 15 || sink.result.Usage.TotalTokens != 180 {
		t.Errorf("unexpected usage in result: %+v", sink.result.Usage)
	}
}

func TestStreamingDaemon_Codex_ErrorHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ndjsonLine string
		wantErrSub string
	}{
		{
			name:       "turn failed with error message",
			ndjsonLine: `{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"status":"failed","error":{"message":"rate limit reached"}}}}`,
			wantErrSub: "rate limit reached",
		},
		{
			name:       "turn aborted",
			ndjsonLine: `{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"status":"aborted"}}}`,
			wantErrSub: "turn aborted",
		},
		{
			name:       "turn error status without error message",
			ndjsonLine: `{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"status":"error"}}}`,
			wantErrSub: "turn error",
		},
		{
			name:       "rpc error response",
			ndjsonLine: `{"jsonrpc":"2.0","id":4,"error":{"message":"internal error in codex engine"}}`,
			wantErrSub: "internal error in codex engine",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			daemon := &StreamingDaemon{
				cfg: DaemonConfig{
					Engine: "codex",
				},
				claudeToolIDs: make(map[string]int),
			}
			sink := newMockTurnSink()
			enqueueInflightTurn(daemon, "turn-"+tc.name, sink)
			daemon.dispatchNDJSONLine(tc.ndjsonLine)
			waitAndAssertSinkResult(t, tc.name, sink, "", "", tc.wantErrSub)
		})
	}
}

func TestCodexProtocol_Helpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"thread object with id", `{"thread":{"id":"th_1"}}`, "th_1"},
		{"threadId field", `{"threadId":"th_2"}`, "th_2"},
		{"id field", `{"id":"th_3"}`, "th_3"},
		{"empty string", ``, ""},
		{"invalid json", `{bad json`, ""},
		{"no id field", `{"other":"val"}`, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractThreadID(json.RawMessage(tc.raw))
			if got != tc.want {
				t.Errorf("extractThreadID(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}

	// Empty model and effort
	bEmpty, err := marshalCodexTurnStart("th_1", "hello", "", "")
	if err != nil {
		t.Fatalf("marshalCodexTurnStart empty failed: %v", err)
	}
	if strings.Contains(string(bEmpty), `"model"`) || strings.Contains(string(bEmpty), `"effort"`) {
		t.Errorf("expected model and effort omitted when empty: %s", string(bEmpty))
	}
}

func TestCodexProtocol_ExtractAgentMessageText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		item map[string]any
		want string
	}{
		{
			name: "text string takes precedence",
			item: map[string]any{"text": "direct reply", "content": []any{"ignored"}},
			want: "direct reply",
		},
		{
			name: "content slice of strings",
			item: map[string]any{"content": []any{"part 1 ", "part 2"}},
			want: "part 1 part 2",
		},
		{
			name: "content slice of map blocks with text or content",
			item: map[string]any{"content": []any{map[string]any{"text": "block 1\n"}, map[string]any{"content": "block 2"}}},
			want: "block 1\nblock 2",
		},
		{
			name: "non-slice content",
			item: map[string]any{"content": "not-a-slice"},
			want: "",
		},
		{
			name: "empty item",
			item: map[string]any{},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractCodexAgentMessageText(tc.item)
			if got != tc.want {
				t.Errorf("extractCodexAgentMessageText() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCodexProtocol_DispatchStreamEvents(t *testing.T) {
	t.Parallel()

	daemon := &StreamingDaemon{
		cfg: DaemonConfig{
			Engine: "codex",
			Model:  "gpt-5",
			Effort: "medium",
		},
		claudeToolIDs: make(map[string]int),
		stderrTail:    newTailBuffer(4096),
	}

	daemon.dispatchCodexJSONRPCLine("")
	daemon.dispatchCodexJSONRPCLine("not-json")
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"unknown/method"}`)

	sink := newMockTurnSink()
	turn := enqueueInflightTurn(daemon, "turn-branches", sink)

	// Map-form text delta
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"delta":{"text":"streamed map text"}}}`)
	sink.mu.Lock()
	deltas := append([]string{}, sink.deltas...)
	sink.mu.Unlock()
	if len(deltas) == 0 || deltas[len(deltas)-1] != "streamed map text" {
		t.Errorf("expected streamed map text in sink.deltas, got %v", deltas)
	}

	// String-form text delta
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"delta":"plain delta string"}}`)
	sink.mu.Lock()
	deltas = append([]string{}, sink.deltas...)
	sink.mu.Unlock()
	if len(deltas) == 0 || deltas[len(deltas)-1] != "plain delta string" {
		t.Errorf("expected plain delta string in sink.deltas, got %v", deltas)
	}

	// Reasoning delta
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/reasoning/textDelta","params":{"itemId":"r-new"}}`)
	sink.mu.Lock()
	thinkingCount := sink.thinking
	sink.mu.Unlock()
	if thinkingCount == 0 {
		t.Errorf("expected thinking count to increase")
	}

	// First model call token usage
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"thread/tokenUsage/updated","params":{"tokenUsage":{"last":{"inputTokens":50.0,"cachedInputTokens":10.0,"outputTokens":20.0,"totalTokens":0.0}}}}`)

	daemon.inflightMu.Lock()
	usage := turn.streamUsage["codex-turn"]
	daemon.inflightMu.Unlock()
	if usage.TotalTokens != 70 {
		t.Errorf("expected fallback total tokens 70, got %d", usage.TotalTokens)
	}

	// Second model call in same turn: accumulated token usage with int types
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"thread/tokenUsage/updated","params":{"tokenUsage":{"last":{"inputTokens":10,"cachedInputTokens":2,"outputTokens":5,"reasoningOutputTokens":3,"totalTokens":15}}}}`)
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"thread/tokenUsage/updated","params":{"tokenUsage":{}}}`)
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"thread/tokenUsage/updated"}`)

	daemon.inflightMu.Lock()
	usage = turn.streamUsage["codex-turn"]
	daemon.inflightMu.Unlock()
	if usage.TotalTokens != 85 || usage.ThinkingTokens != 3 {
		t.Errorf("expected accumulated tokens total=85 thinking=3, got total=%d thinking=%d", usage.TotalTokens, usage.ThinkingTokens)
	}

	// Completed agent message and turn completion
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"agentMessage","text":"final answer text"}}}`)
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"status":"completed"}}}`)
	sink.mu.Lock()
	res := sink.result
	sink.mu.Unlock()
	if res == nil || res.Response != "final answer text" {
		t.Errorf("expected completed turn result with response 'final answer text', got %v", res)
	}
}

func TestCodexProtocol_DispatchStreamToolEvents(t *testing.T) {
	t.Parallel()

	daemon := &StreamingDaemon{
		cfg: DaemonConfig{
			Engine: "codex",
		},
		claudeToolIDs: make(map[string]int),
	}

	sink := newMockTurnSink()
	enqueueInflightTurn(daemon, "turn-tools", sink)

	// Tool call starts
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"tool-1","type":"mcpToolCall","server":"docket","tool":"mcp_tool"}}}`)

	// A non-tool reasoning item completes while tool-1 is in flight: must NOT complete tool-1
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"reason-1","type":"reasoning"}}}`)
	sink.mu.Lock()
	completedBefore := len(sink.completedTools)
	sink.mu.Unlock()
	if completedBefore != 0 {
		t.Errorf("expected 0 completed tools after reasoning completion, got %d", completedBefore)
	}

	// Tool-1 completes with is_error: true
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"tool-1","type":"mcpToolCall","is_error":true}}}`)
	sink.mu.Lock()
	completedAfter := len(sink.completedTools)
	var completedInfo string
	if completedAfter > 0 {
		completedInfo = sink.completedTools[0]
	}
	sink.mu.Unlock()
	if completedAfter != 1 || !strings.Contains(completedInfo, ":error") {
		t.Errorf("expected 1 completed tool with error status, got count=%d info=%s", completedAfter, completedInfo)
	}

	// Command execution tool completes with status: failed
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"cmd-1","type":"commandExecution","command":"ls"}}}`)
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"cmd-1","type":"commandExecution","status":"failed"}}}`)
	sink.mu.Lock()
	var foundFailedCmd bool
	for _, comp := range sink.completedTools {
		if strings.Contains(comp, "bash") && strings.Contains(comp, ":error") {
			foundFailedCmd = true
			break
		}
	}
	sink.mu.Unlock()
	if !foundFailedCmd {
		t.Errorf("expected failed bash tool in completedTools: %v", sink.completedTools)
	}
}

func TestCodexProtocol_DispatchStreamToolVarieties(t *testing.T) {
	t.Parallel()

	daemon := &StreamingDaemon{
		cfg: DaemonConfig{
			Engine: "codex",
		},
		claudeToolIDs: make(map[string]int),
	}

	sink := newMockTurnSink()
	enqueueInflightTurn(daemon, "turn-varieties", sink)

	// Tool item starts for fileChange, webSearch, and collabAgentToolCall
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"edit-1","type":"fileChange","path":"/path/to/file.go"}}}`)
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"search-1","type":"webSearch","query":"test query"}}}`)
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"subagent-1","type":"collabAgentToolCall","agent":"reviewer"}}}`)

	// MCP tool with string arguments (both valid JSON and raw string fallback)
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"tool-json-str","type":"mcpToolCall","tool":"mcp_tool","arguments":"{\"query\":\"hello\"}"}}}`)
	daemon.dispatchCodexJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"tool-raw-str","type":"mcpToolCall","tool":"mcp_tool","arguments":"not-json"}}}`)

	sink.mu.Lock()
	callsByID := make(map[string]ToolCall)
	for _, call := range sink.calls {
		callsByID[call.ID] = call
	}
	sink.mu.Unlock()

	if call, ok := callsByID["edit-1"]; !ok || call.Name != "edit" {
		t.Errorf("expected edit tool call, got %v", call)
	}
	if call, ok := callsByID["search-1"]; !ok || call.Name != "search_web" {
		t.Errorf("expected search_web tool call, got %v", call)
	}
	if call, ok := callsByID["subagent-1"]; !ok || call.Name != canonicalToolSubagent {
		t.Errorf("expected subagent tool call, got %v", call)
	}
	if call, ok := callsByID["tool-json-str"]; !ok || call.Name != "mcp_tool" {
		t.Errorf("expected tool-json-str tool call for mcp_tool, got %v", call)
	}
	if call, ok := callsByID["tool-raw-str"]; !ok || call.Name != "mcp_tool" {
		t.Errorf("expected tool-raw-str tool call for mcp_tool, got %v", call)
	}
}

func TestCodexProtocol_TurnOutcomeAndErrors(t *testing.T) {
	t.Parallel()

	if status, _ := parseCodexTurnOutcome(nil); status != statusUpperDone {
		t.Errorf("expected %s, got %s", statusUpperDone, status)
	}
	if status, errStr := parseCodexTurnOutcome(map[string]any{"status": "custom_fail"}); status != statusUpperDone || errStr != "" {
		t.Errorf("expected non-error status to remain done, got %s, %s", status, errStr)
	}
	if status, errStr := parseCodexTurnOutcome(map[string]any{"error": "direct error"}); status != statusUpperError || errStr != "direct error" {
		t.Errorf("expected direct error, got %s, %s", status, errStr)
	}

	daemon := &StreamingDaemon{
		cfg:        DaemonConfig{Engine: "codex"},
		stderrTail: newTailBuffer(4096),
	}
	daemon.stderrTail.Write([]byte("some error log"))
	if got := daemon.appendStderrTail("failed"); !strings.Contains(got, "some error log") {
		t.Errorf("appendStderrTail missing log tail: %s", got)
	}
}

func TestCodexProtocol_HandshakeAndResume(t *testing.T) {
	t.Parallel()

	daemonNoResume := &StreamingDaemon{
		cfg: DaemonConfig{Engine: "codex", ResumeSession: false},
	}
	if id, err := daemonNoResume.tryResumeCodexThread(context.Background(), nil, time.Now()); id != "" || err != nil {
		t.Errorf("expected empty resume without error")
	}
	daemonNoID := &StreamingDaemon{
		cfg: DaemonConfig{Engine: "codex", ResumeSession: true, SessionID: ""},
	}
	if id, err := daemonNoID.tryResumeCodexThread(context.Background(), nil, time.Now()); id != "" || err != nil {
		t.Errorf("expected empty resume without error")
	}

	daemonWithTail := &StreamingDaemon{
		stderrTail: newTailBuffer(1024),
	}
	daemonWithTail.stderrTail.Write([]byte("fatal crash in codex"))
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelErr := daemonWithTail.handleHandshakeReadErr(cancelCtx, cancelCtx.Err())
	if !strings.Contains(cancelErr.Error(), "cancelled") {
		t.Errorf("expected cancelled in error: %v", cancelErr)
	}
	timeoutErr := daemonWithTail.handleHandshakeReadErr(context.Background(), errors.New("timeout"))
	if !strings.Contains(timeoutErr.Error(), "fatal crash in codex") {
		t.Errorf("expected stderr tail in timeout error: %v", timeoutErr)
	}
	readErr := daemonWithTail.handleHandshakeReadErr(context.Background(), io.EOF)
	if !strings.Contains(readErr.Error(), "fatal crash in codex") {
		t.Errorf("expected stderr tail in read error: %v", readErr)
	}
}
