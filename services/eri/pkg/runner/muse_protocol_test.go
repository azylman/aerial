package runner

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStreamingDaemon_Muse_Handshake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfg           DaemonConfig
		mockReplies   []string
		wantErrSub    string
		wantSessionID string
	}{
		{
			name: "start new session",
			cfg:  DaemonConfig{Engine: "muse", Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":4,"result":{"session":{"id":"018e3f94-1111-7000-8000-000000000001"}}}` + "\n",
			},
			wantSessionID: "018e3f94-1111-7000-8000-000000000001",
		},
		{
			name: "resume existing session",
			cfg:  DaemonConfig{Engine: "muse", SessionID: "018e3f94-2222-7000-8000-000000000002", ResumeSession: true, Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":2,"result":{"session":{"id":"018e3f94-2222-7000-8000-000000000002"}}}` + "\n",
				`{"jsonrpc":"2.0","id":3,"result":{}}` + "\n",
			},
			wantSessionID: "018e3f94-2222-7000-8000-000000000002",
		},
		{
			name: "resume error falls back to start",
			cfg:  DaemonConfig{Engine: "muse", SessionID: "018e3f94-3333-7000-8000-000000000003", ResumeSession: true, Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"session not found"}}` + "\n",
				`{"jsonrpc":"2.0","id":4,"result":{"session":{"id":"018e3f94-4444-7000-8000-000000000004"}}}` + "\n",
			},
			wantSessionID: "018e3f94-4444-7000-8000-000000000004",
		},
		{
			name: "initialize error",
			cfg:  DaemonConfig{Engine: "muse", Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"unauthorized client"}}` + "\n",
			},
			wantErrSub: "unauthorized client",
		},
		{
			name: "session start error",
			cfg:  DaemonConfig{Engine: "muse", Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":4,"error":{"code":-32000,"message":"sandbox failed"}}` + "\n",
			},
			wantErrSub: "sandbox failed",
		},
		{
			name: "resume approval mode error",
			cfg:  DaemonConfig{Engine: "muse", SessionID: "018e3f94-2222-7000-8000-000000000002", ResumeSession: true, Timeout: 2 * time.Second},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":2,"result":{"session":{"id":"018e3f94-2222-7000-8000-000000000002"}}}` + "\n",
				`{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"approval mode rejected"}}` + "\n",
			},
			wantErrSub: "approval mode rejected",
		},
		{
			name:        "handshake timeout",
			cfg:         DaemonConfig{Engine: "muse", Timeout: 50 * time.Millisecond},
			mockReplies: nil,
			wantErrSub:  "timed out waiting for init event",
		},
		{
			name:        "stdout closed immediately",
			cfg:         DaemonConfig{Engine: "muse", Timeout: 2 * time.Second},
			mockReplies: []string{"CLOSE"},
			wantErrSub:  "daemon stdout closed before init event",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertHandshakeReplySession(t, tc.cfg, tc.mockReplies, tc.wantErrSub, tc.wantSessionID)
		})
	}
}

func TestStreamingDaemon_Muse_FullLifecycle(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeAll(inR, inW, outR, outW, errR, errW)

	mockReplies := []string{
		`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
		`{"jsonrpc":"2.0","id":4,"result":{"session":{"id":"018e3f94-5555-7000-8000-000000000005"}}}` + "\n",
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
		Engine:    "muse",
		Model:     "llama-3.3-70b",
		Effort:    "high",
		SessionID: "018e3f94-5555-7000-8000-000000000005",
	}
	daemon, err := StartStreamingDaemon(context.Background(), context.Background(), cfg, mock)
	if err != nil {
		t.Fatalf("StartStreamingDaemon failed: %v", err)
	}
	defer daemon.Close()

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-muse-1",
		Prompt:    "hello muse",
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
	if sentReq.Method != methodTurnStart {
		t.Errorf("expected method %s, got %q", methodTurnStart, sentReq.Method)
	}
	paramsMap, ok := sentReq.Params.(map[string]any)
	if !ok {
		t.Fatalf("expected map params, got %T", sentReq.Params)
	}
	if paramsMap["reasoningEffort"] != "high" {
		t.Errorf("expected reasoningEffort high, got %v", paramsMap["reasoningEffort"])
	}

	streamEvents := []string{
		`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"item-mcp-1","type":"mcpToolCall","server":"docket","tool":"mcp_docket_create","arguments":{"title":"fix bug"}}}}`,
		`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"item-cmd-1","type":"commandExecution","command":"git status"}}}`,
		`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"item-tool-1","type":"toolCall","tool":"read_file","arguments":{"path":"main.go"}}}}`,
		`{"jsonrpc":"2.0","method":"item/reasoning/textDelta","params":{"itemId":"reason-1","delta":"thinking step 1"}}`,
		`{"jsonrpc":"2.0","method":"item/reasoning/textDelta","params":{"itemId":"reason-1","delta":"thinking step 2"}}`,
		`{"jsonrpc":"2.0","method":"item/reasoning/summaryTextDelta","params":{"itemId":"reason-2","delta":"summary step 1"}}`,
		`{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"delta":"Here is the result"}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-mcp-1","type":"mcpToolCall","status":"completed"}}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-cmd-1","type":"commandExecution","status":"completed"}}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-tool-1","type":"toolCall","status":"completed"}}}`,
		`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"item-msg-1","type":"agentMessage","text":"Here is the full completed result."}}}`,
		`{"jsonrpc":"2.0","method":"session/tokenUsage","params":{"usage":{"inputTokens":120,"cachedTokens":20,"outputTokens":60}}}`,
		`{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"status":"completed"}}}`,
	}

	for _, ev := range streamEvents {
		daemon.dispatchNDJSONLine(ev)
	}

	waitAndAssertSinkResult(t, "MuseFullLifecycle", sink, "Here is the full completed result.", "", "")

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if sink.thinking != 2 {
		t.Errorf("expected 2 thinking notifications, got %d", sink.thinking)
	}
	if len(sink.deltas) != 1 || sink.deltas[0] != "Here is the result" {
		t.Errorf("unexpected deltas: %v", sink.deltas)
	}
	if len(sink.toolCalls) != 3 {
		t.Errorf("expected 3 tool calls, got %d: %v", len(sink.toolCalls), sink.toolCalls)
	}
	if len(sink.completedTools) != 3 {
		t.Errorf("expected 3 completed tools, got %d: %v", len(sink.completedTools), sink.completedTools)
	}

	if sink.result.Usage.InputTokens != 100 || sink.result.Usage.CacheReadTokens != 20 || sink.result.Usage.OutputTokens != 60 || sink.result.Usage.TotalTokens != 180 {
		t.Errorf("unexpected usage in result: %+v", sink.result.Usage)
	}
}

func TestStreamingDaemon_Muse_ErrorHandling(t *testing.T) {
	t.Parallel()

	runDaemonErrorTests(t, "muse", []daemonErrorTestCase{
		{
			name:       "turn failed with error message",
			ndjsonLine: `{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"status":"failed","error":{"message":"rate limit reached"}}}}`,
			wantErrSub: "rate limit reached",
		},
		{
			name:       "turn terminal boolean failed",
			ndjsonLine: `{"jsonrpc":"2.0","method":"turn/completed","params":{"status":"aborted","terminal":true}}`,
			wantErrSub: "turn aborted",
		},
		{
			name:       "turn terminal string failed",
			ndjsonLine: `{"jsonrpc":"2.0","method":"turn/completed","params":{"terminal":"failed"}}`,
			wantErrSub: "turn failed",
		},
		{
			name:       "top-level rpc error",
			ndjsonLine: `{"jsonrpc":"2.0","id":10,"error":{"message":"internal error in muse engine"}}`,
			wantErrSub: "internal error in muse engine",
		},
	})
}

func TestMuseProtocol_UUIDv7(t *testing.T) {
	t.Parallel()

	id := uuidV7()
	if !isUUIDv7(id) {
		t.Errorf("expected uuidV7() to produce valid UUIDv7, got %q", id)
	}

	v4 := uuid.New().String()
	if isUUIDv7(v4) {
		t.Errorf("expected UUIDv4 to return false for isUUIDv7: %s", v4)
	}

	tests := []struct {
		input string
		want  bool
	}{
		{"", false},
		{"not-a-uuid", false},
		{"00000000-0000-0000-0000-000000000000", false},
	}
	for _, tc := range tests {
		if got := isUUIDv7(tc.input); got != tc.want {
			t.Errorf("isUUIDv7(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestMuseProtocol_ExtractSessionID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      string
		fallback string
		want     string
	}{
		{"session object with id", `{"session":{"id":"sess-1"}}`, "fallback", "sess-1"},
		{"session object with sessionId", `{"session":{"sessionId":"sess-2"}}`, "fallback", "sess-2"},
		{"top-level sessionId", `{"sessionId":"sess-3"}`, "fallback", "sess-3"},
		{"top-level id", `{"id":"sess-4"}`, "fallback", "sess-4"},
		{"empty result uses fallback", ``, "fallback", "fallback"},
		{"invalid json uses fallback", `{bad`, "fallback", "fallback"},
		{"unrelated json uses fallback", `{"foo":"bar"}`, "fallback", "fallback"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractMuseSessionID(json.RawMessage(tc.raw), tc.fallback)
			if got != tc.want {
				t.Errorf("extractMuseSessionID(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestMuseProtocol_MarshalTurnStart(t *testing.T) {
	t.Parallel()

	bWithEffort, err := marshalMuseTurnStart("sess-123", "do work", "medium")
	if err != nil {
		t.Fatalf("marshalMuseTurnStart with effort failed: %v", err)
	}
	if !strings.Contains(string(bWithEffort), `"reasoningEffort":"medium"`) || !strings.Contains(string(bWithEffort), `"sessionId":"sess-123"`) {
		t.Errorf("unexpected marshaled turn start: %s", string(bWithEffort))
	}

	bNoEffort, err := marshalMuseTurnStart("sess-123", "do work", "")
	if err != nil {
		t.Fatalf("marshalMuseTurnStart no effort failed: %v", err)
	}
	if strings.Contains(string(bNoEffort), `"reasoningEffort"`) {
		t.Errorf("expected reasoningEffort omitted when empty: %s", string(bNoEffort))
	}
}

func TestMuseProtocol_IsChildSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		params        map[string]any
		rootSessionID string
		want          bool
	}{
		{
			name:          "nil params",
			params:        nil,
			rootSessionID: "root-1",
			want:          false,
		},
		{
			name:          "matching session id",
			params:        map[string]any{"sessionId": "root-1"},
			rootSessionID: "root-1",
			want:          false,
		},
		{
			name:          "different session id",
			params:        map[string]any{"sessionId": "child-1"},
			rootSessionID: "root-1",
			want:          true,
		},
		{
			name: "item with different session id",
			params: map[string]any{
				"item": map[string]any{"sessionId": "child-2"},
			},
			rootSessionID: "root-1",
			want:          true,
		},
		{
			name: "item with kind reminderChild",
			params: map[string]any{
				"item": map[string]any{"kind": "reminderChild"},
			},
			rootSessionID: "root-1",
			want:          true,
		},
		{
			name: "params with kind reminderChild",
			params: map[string]any{
				"kind": "reminderChild",
			},
			rootSessionID: "root-1",
			want:          true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isChildMuseSession(tc.params, tc.rootSessionID)
			if got != tc.want {
				t.Errorf("isChildMuseSession() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMuseProtocol_ToolTypeAndDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		itemType string
		want     bool
	}{
		{itemTypeToolCall, true},
		{codexToolMCP, true},
		{codexToolCommandExecution, true},
		{itemTypeAgentMessage, false},
		{itemTypeReasoning, false},
		{"other", false},
	}
	for _, tc := range tests {
		if got := isMuseToolItemType(tc.itemType); got != tc.want {
			t.Errorf("isMuseToolItemType(%q) = %v, want %v", tc.itemType, got, tc.want)
		}
	}

	// extractMuseItemType
	if got := extractMuseItemType(map[string]any{fieldType: "t1"}, "def"); got != "t1" {
		t.Errorf("extractMuseItemType params type = %q, want t1", got)
	}
	if got := extractMuseItemType(map[string]any{"item": map[string]any{fieldType: "t2"}}, "def"); got != "t2" {
		t.Errorf("extractMuseItemType item type = %q, want t2", got)
	}
	if got := extractMuseItemType(map[string]any{"delta": map[string]any{fieldType: "t3"}}, "def"); got != "t3" {
		t.Errorf("extractMuseItemType delta type = %q, want t3", got)
	}
	if got := extractMuseItemType(nil, "def"); got != "def" {
		t.Errorf("extractMuseItemType nil = %q, want def", got)
	}

	// extractMuseDeltaText
	if got := extractMuseDeltaText(map[string]any{"delta": "text-delta"}); got != "text-delta" {
		t.Errorf("extractMuseDeltaText string = %q, want text-delta", got)
	}
	if got := extractMuseDeltaText(map[string]any{"delta": map[string]any{fieldText: "map-delta"}}); got != "map-delta" {
		t.Errorf("extractMuseDeltaText map = %q, want map-delta", got)
	}
	if got := extractMuseDeltaText(map[string]any{fieldText: "top-text"}); got != "top-text" {
		t.Errorf("extractMuseDeltaText top text = %q, want top-text", got)
	}
}

func TestMuseProtocol_ExtractUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		params     map[string]any
		wantInput  int
		wantOutput int
		wantCached int
	}{
		{
			name:       "nil params",
			params:     nil,
			wantInput:  0,
			wantOutput: 0,
			wantCached: 0,
		},
		{
			name: "usage map float",
			params: map[string]any{
				"usage": map[string]any{"inputTokens": 100.0, "outputTokens": 50.0, "cachedTokens": 20.0},
			},
			wantInput:  100,
			wantOutput: 50,
			wantCached: 20,
		},
		{
			name: "tokenUsage last map int",
			params: map[string]any{
				"tokenUsage": map[string]any{
					"last": map[string]any{"input_tokens": 80, "output_tokens": 40, "cached_input_tokens": 10},
				},
			},
			wantInput:  80,
			wantOutput: 40,
			wantCached: 10,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in, out, cached := extractMuseUsage(tc.params)
			if in != tc.wantInput || out != tc.wantOutput || cached != tc.wantCached {
				t.Errorf("extractMuseUsage() = (%d, %d, %d), want (%d, %d, %d)", in, out, cached, tc.wantInput, tc.wantOutput, tc.wantCached)
			}
		})
	}
}

func TestMuseProtocol_ParseTurnOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		params     map[string]any
		wantStatus string
		wantErrMsg string
	}{
		{
			name:       "nil params returns done",
			params:     nil,
			wantStatus: statusUpperDone,
			wantErrMsg: "",
		},
		{
			name:       "completed status returns done",
			params:     map[string]any{fieldStatus: "completed"},
			wantStatus: statusUpperDone,
			wantErrMsg: "",
		},
		{
			name:       "failed status with message",
			params:     map[string]any{fieldStatus: statusFailed, fieldError: map[string]any{"message": "rate limit"}},
			wantStatus: statusUpperError,
			wantErrMsg: "rate limit",
		},
		{
			name:       "cancelled status string error",
			params:     map[string]any{fieldStatus: statusCancelled, fieldError: "aborted by user"},
			wantStatus: statusUpperError,
			wantErrMsg: "aborted by user",
		},
		{
			name: "nested turn object status",
			params: map[string]any{
				"turn": map[string]any{fieldStatus: statusError, fieldError: "timeout"},
			},
			wantStatus: statusUpperError,
			wantErrMsg: "timeout",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status, errMsg := parseMuseTurnOutcome(tc.params)
			if status != tc.wantStatus || errMsg != tc.wantErrMsg {
				t.Errorf("parseMuseTurnOutcome() = (%q, %q), want (%q, %q)", status, errMsg, tc.wantStatus, tc.wantErrMsg)
			}
		})
	}
}

func TestMuseProtocol_DispatchStreamEvents(t *testing.T) {
	t.Parallel()

	daemon := &StreamingDaemon{
		cfg: DaemonConfig{
			Engine: "muse",
			Model:  "llama-3.3-70b",
			Effort: "medium",
		},
		claudeToolIDs: make(map[string]int),
		stderrTail:    newTailBuffer(4096),
	}
	daemon.SetSessionID("root-muse-1")

	daemon.dispatchMuseJSONRPCLine("")
	daemon.dispatchMuseJSONRPCLine("not-json")
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"unknown/method"}`)

	sink := newMockTurnSink()
	turn := enqueueInflightTurn(daemon, "turn-branches", sink)

	// Child session events carrying a different sessionId must be ignored
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/delta","params":{"sessionId":"child-muse-2","delta":"child delta"}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/reasoning/textDelta","params":{"sessionId":"child-muse-2","itemId":"r-child"}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"sessionId":"child-muse-2","status":"completed"}}`)

	sink.mu.Lock()
	if len(sink.deltas) != 0 {
		t.Errorf("expected 0 deltas after child event, got %v", sink.deltas)
	}
	if sink.thinking != 0 {
		t.Errorf("expected 0 thinking after child reasoning delta, got %d", sink.thinking)
	}
	if sink.result != nil {
		t.Errorf("expected nil result after child turn/completed, got %v", sink.result)
	}
	sink.mu.Unlock()

	// Tool item started and completed
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"tool-1","type":"toolCall","tool":"read_file","arguments":"{\"path\":\"a.txt\"}"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"tool-1","type":"toolCall","status":"completed"}}}`)

	sink.mu.Lock()
	toolCallCount := len(sink.toolCalls)
	toolCompletedCount := len(sink.completedTools)
	sink.mu.Unlock()
	if toolCallCount != 1 || toolCompletedCount != 1 {
		t.Errorf("expected 1 tool call and completion, got %d and %d", toolCallCount, toolCompletedCount)
	}

	// Tool item completed with error
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"cmd-1","type":"commandExecution","command":"false"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"cmd-1","type":"commandExecution","status":"failed"}}}`)

	sink.mu.Lock()
	var foundError bool
	for _, ct := range sink.completedTools {
		if strings.Contains(ct, ":error") {
			foundError = true
			break
		}
	}
	sink.mu.Unlock()
	if !foundError {
		t.Errorf("expected error tool completion in sink: %v", sink.completedTools)
	}

	// Final answer and completion
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"agentMessage","text":"done answer"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"status":"completed"}}`)

	sink.mu.Lock()
	res := sink.result
	sink.mu.Unlock()
	if res == nil || res.Response != "done answer" {
		t.Errorf("expected done answer in result, got %v", res)
	}
	_ = turn
}

func TestMuseProtocol_ItemStartedAndErrorExtraction(t *testing.T) {
	t.Parallel()

	daemon := &StreamingDaemon{
		cfg:           DaemonConfig{Engine: "muse"},
		claudeToolIDs: make(map[string]int),
	}
	daemon.SetSessionID("sess-cov-1")

	// handleMuseItemStarted nil checks
	daemon.handleMuseItemStarted(nil, nil)
	turn := enqueueInflightTurn(daemon, "turn-cov", newMockTurnSink())
	daemon.handleMuseItemStarted(turn, nil)

	// Reasoning item started + duplicate
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"r-1","type":"reasoning"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"r-1","type":"reasoning"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"msg-1","type":"agentMessage"}}}`)

	// Tool item with parameters fallback and top-level item params
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"id":"t-param","type":"toolCall","tool":"view_file","parameters":{"path":"b.txt"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"id":"t-param","type":"toolCall","status":"completed"}}}`)

	// Tool item with invalid JSON string arguments
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"mcp-bad","type":"mcpToolCall","tool":"bad_tool","arguments":"invalid-json"}}}`)

	// sendJSONRPCRequest when stdin is nil
	if err := daemon.sendJSONRPCRequest(codexRPCRequest{}, "nil stdin test"); err == nil {
		t.Errorf("expected error on nil stdin")
	}

	// isCodexToolItemType verification
	for _, tc := range []struct {
		t    string
		want bool
	}{
		{codexToolCollabAgent, true},
		{codexToolCommandExecution, true},
		{codexToolFileChange, true},
		{codexToolWebSearch, true},
		{codexToolMCP, true},
		{"unrelated", false},
	} {
		if got := isCodexToolItemType(tc.t); got != tc.want {
			t.Errorf("isCodexToolItemType(%q) = %v, want %v", tc.t, got, tc.want)
		}
	}

	// extractRPCErrorMessage tests
	if got := extractRPCErrorMessage(map[string]any{"error": "string"}); got != "" {
		t.Errorf("expected empty for string error, got %q", got)
	}
	if got := extractRPCErrorMessage(map[string]any{"error": map[string]any{"message": "err"}}); got != "" {
		t.Errorf("expected empty for error without ID, got %q", got)
	}
	if got := extractRPCErrorMessage(map[string]any{"id": 1, "error": map[string]any{}}); got != "muse rpc error" {
		t.Errorf("expected 'muse rpc error', got %q", got)
	}
	if got := extractRPCErrorMessage(map[string]any{"id": 2, "error": map[string]any{"message": "custom err"}}); got != "custom err" {
		t.Errorf("expected 'custom err', got %q", got)
	}

	// Item completed error status branches
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"tool-is-err","type":"toolCall","is_error":true}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"cmd-canc","type":"commandExecution","status":"cancelled"}}}`)
}
