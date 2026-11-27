package runner

import (
	"bufio"
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

type handshakeApprovalModeTestCase struct {
	name             string
	cfg              DaemonConfig
	mockReplies      []string
	wantMethod       string
	wantParamKey     string
	wantParamVal     string
	wantOmittedParam string
	wantSessionID    string
}

func assertHandshakeApprovalMode(t *testing.T, tc handshakeApprovalModeTestCase) {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeAll(inR, inW, outR, outW, errR, errW)

	capturedRequests := make(chan codexRPCRequest, 10)
	go func() {
		defer closeQuietly(outW)
		scanner := bufio.NewScanner(inR)
		replyIdx := 0
		for scanner.Scan() {
			var req codexRPCRequest
			if err := json.Unmarshal(scanner.Bytes(), &req); err == nil {
				capturedRequests <- req
			}
			if req.Method == "initialized" {
				continue
			}
			if replyIdx < len(tc.mockReplies) {
				rep := tc.mockReplies[replyIdx]
				replyIdx++
				_, _ = outW.Write([]byte(rep))
			}
		}
	}()

	handle := &MockProcessHandle{pid: 777}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, c DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, handle, nil
		},
	}

	daemon, err := StartStreamingDaemon(context.Background(), context.Background(), tc.cfg, mock)
	if err != nil {
		t.Fatalf("StartStreamingDaemon failed: %v", err)
	}
	defer daemon.Close()

	if tc.wantSessionID != "" && daemon.SessionID() != tc.wantSessionID {
		t.Errorf("expected sessionID %q, got %q", tc.wantSessionID, daemon.SessionID())
	}

	var foundReq *codexRPCRequest
CaptureLoop:
	for {
		select {
		case r := <-capturedRequests:
			if r.Method == tc.wantMethod {
				foundReq = &r
				break CaptureLoop
			}
		default:
			break CaptureLoop
		}
	}
	if foundReq == nil {
		t.Fatalf("did not receive request with method %s", tc.wantMethod)
	}
	params, ok := foundReq.Params.(map[string]any)
	if !ok {
		t.Fatalf("params for %s is not map: %T", tc.wantMethod, foundReq.Params)
	}
	if params[tc.wantParamKey] != tc.wantParamVal {
		t.Errorf("expected %s[%s] = %q, got %v", tc.wantMethod, tc.wantParamKey, tc.wantParamVal, params[tc.wantParamKey])
	}
	if tc.wantOmittedParam != "" {
		if _, exists := params[tc.wantOmittedParam]; exists {
			t.Errorf("expected %s to omit %q, got %v", tc.wantMethod, tc.wantOmittedParam, params[tc.wantOmittedParam])
		}
	}
}

func TestStreamingDaemon_Muse_HandshakeApprovalMode(t *testing.T) {
	t.Parallel()

	tests := []handshakeApprovalModeTestCase{
		{
			name: "start session sends approvalMode allowAll",
			cfg: DaemonConfig{
				Engine:  "muse",
				Timeout: 2 * time.Second,
			},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":4,"result":{"session":{"id":"018e3f94-1111-7000-8000-000000000001"}}}` + "\n",
			},
			wantMethod:   "session/start",
			wantParamKey: "approvalMode",
			wantParamVal: museApprovalAllowAll,
		},
		{
			name: "resume session sends setApprovalMode allowAll",
			cfg: DaemonConfig{
				Engine:        "muse",
				SessionID:     "018e3f94-2222-7000-8000-000000000002",
				ResumeSession: true,
				Timeout:       2 * time.Second,
			},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":2,"result":{"session":{"id":"018e3f94-2222-7000-8000-000000000002"}}}` + "\n",
				`{"jsonrpc":"2.0","id":3,"result":{}}` + "\n",
			},
			wantMethod:   "session/setApprovalMode",
			wantParamKey: "mode",
			wantParamVal: museApprovalAllowAll,
		},
		{
			name: "failed resume fallback to start omits colliding sessionId",
			cfg: DaemonConfig{
				Engine:        "muse",
				SessionID:     "018e3f94-3333-7000-8000-000000000003",
				ResumeSession: true,
				Timeout:       2 * time.Second,
			},
			mockReplies: []string{
				`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
				`{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"abandoned incomplete session/start"}}` + "\n",
				`{"jsonrpc":"2.0","id":4,"result":{"session":{"id":"018e3f94-4444-7000-8000-000000000004"}}}` + "\n",
			},
			wantMethod:       "session/start",
			wantParamKey:     "approvalMode",
			wantParamVal:     museApprovalAllowAll,
			wantOmittedParam: fieldSessionID,
			wantSessionID:    "018e3f94-4444-7000-8000-000000000004",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertHandshakeApprovalMode(t, tc)
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
		{name: "nil params", params: nil, rootSessionID: "root-1", want: false},
		{name: "matching session id", params: map[string]any{"sessionId": "root-1"}, rootSessionID: "root-1", want: false},
		{name: "different session id", params: map[string]any{"sessionId": "child-1"}, rootSessionID: "root-1", want: true},
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
		{name: "params with kind reminderChild", params: map[string]any{"kind": "reminderChild"}, rootSessionID: "root-1", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isMuseReminderChild(tc.params) || extractMuseChildSessionID(tc.params, tc.rootSessionID) != ""
			if got != tc.want {
				t.Errorf("child session check = %v, want %v", got, tc.want)
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
		{name: "nil params", params: nil, wantInput: 0, wantOutput: 0, wantCached: 0},
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
			name: "tokenUsage last map float",
			params: map[string]any{
				"tokenUsage": map[string]any{
					"last": map[string]any{"input_tokens": 80.0, "output_tokens": 40.0, "cached_input_tokens": 10.0},
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
		{name: "nil params returns done", params: nil, wantStatus: statusUpperDone, wantErrMsg: ""},
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
	daemon.sessionID = "root-muse-1"

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
	daemon.sessionID = "sess-cov-1"

	// handleMuseItemStarted nil checks
	daemon.handleMuseItemStarted(nil, nil, "")
	sink := newMockTurnSink()
	turn := enqueueInflightTurn(daemon, "turn-cov", sink)
	daemon.handleMuseItemStarted(turn, nil, "")

	// Reasoning item started + duplicate (must only increment thinking once)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"r-1","type":"reasoning"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"r-1","type":"reasoning"}}}`)

	sink.mu.Lock()
	thinkingCount := sink.thinking
	sink.mu.Unlock()
	if thinkingCount != 1 {
		t.Errorf("expected 1 thinking notification after deduplication, got %d", thinkingCount)
	}

	// Tool item with parameters fallback and top-level item params
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"id":"t-param","type":"toolCall","tool":"view_file","parameters":{"path":"b.txt"}}}`)
	sink.mu.Lock()
	if len(sink.calls) != 1 || sink.calls[0].Command != "b.txt" || sink.calls[0].Name != "view_file" {
		t.Errorf("expected tool call with command 'b.txt', got %v", sink.calls)
	}
	sink.mu.Unlock()

	// Tool item delta must NOT leak into sink text deltas or codexResponseText
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/delta","params":{"itemId":"t-param","delta":"tool output text"}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/delta","params":{"type":"toolCall","delta":"toolCall delta text"}}`)

	sink.mu.Lock()
	deltaCount := len(sink.deltas)
	sink.mu.Unlock()
	if deltaCount != 0 {
		t.Errorf("expected 0 text deltas from tool item deltas, got %d: %v", deltaCount, sink.deltas)
	}

	// Agent message started + delta MUST be emitted
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"msg-1","type":"agentMessage"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/delta","params":{"itemId":"msg-1","delta":"visible answer"}}`)

	sink.mu.Lock()
	if len(sink.deltas) != 1 || sink.deltas[0] != "visible answer" {
		t.Errorf("expected 1 agent message delta 'visible answer', got %v", sink.deltas)
	}
	sink.mu.Unlock()

	// Tool item completed (fixed closing brace)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"id":"t-param","type":"toolCall","status":"completed"}}`)
	sink.mu.Lock()
	if len(sink.completedTools) != 1 || !strings.Contains(sink.completedTools[0], ":ok") {
		t.Errorf("expected 1 completed tool with :ok, got %v", sink.completedTools)
	}
	sink.mu.Unlock()

	// Tool item with invalid JSON string arguments
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"mcp-bad","type":"mcpToolCall","tool":"bad_tool","arguments":"invalid-json"}}}`)
	sink.mu.Lock()
	if len(sink.calls) != 2 || sink.calls[1].Name != "bad_tool" {
		t.Errorf("expected toolCalls to record bad_tool, got %v", sink.calls)
	}
	sink.mu.Unlock()

	// Item completed error status branches
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"tool-is-err","type":"toolCall","tool":"read_file"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"tool-is-err","type":"toolCall","is_error":true}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"cmd-canc","type":"commandExecution","command":"ls"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"cmd-canc","type":"commandExecution","status":"cancelled"}}}`)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	errorCompletedCount := 0
	for _, ct := range sink.completedTools {
		if strings.Contains(ct, ":error") {
			errorCompletedCount++
		}
	}
	if errorCompletedCount != 2 {
		t.Errorf("expected 2 error completed tools, got %d in %v", errorCompletedCount, sink.completedTools)
	}
}

func TestStreamingDaemon_SendJSONRPCRequest_NilStdin(t *testing.T) {
	t.Parallel()

	nilDaemon := &StreamingDaemon{}
	if err := nilDaemon.sendJSONRPCRequest(codexRPCRequest{}, "nil stdin test"); err == nil {
		t.Errorf("expected error on nil stdin")
	}
}

func TestMuseProtocol_ExtractRPCErrorMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  map[string]any
		want string
	}{
		{name: "string error", raw: map[string]any{"error": "string"}, want: ""},
		{name: "error without ID", raw: map[string]any{"error": map[string]any{"message": "err"}}, want: ""},
		{name: "empty message fallback", raw: map[string]any{"id": 1.0, "error": map[string]any{}}, want: "muse rpc error"},
		{name: "custom message", raw: map[string]any{"id": 2.0, "error": map[string]any{"message": "custom err"}}, want: "custom err"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := extractRPCErrorMessage(tc.raw); got != tc.want {
				t.Errorf("extractRPCErrorMessage() = %q, want %q", got, tc.want)
			}
		})
	}
}

func startMuseSubscribeRecorder(inR io.Reader) <-chan string {
	subscribedSessions := make(chan string, 4)
	go func() {
		scanner := bufio.NewScanner(inR)
		for scanner.Scan() {
			var req codexRPCRequest
			if err := json.Unmarshal(scanner.Bytes(), &req); err != nil || req.Method != "view/subscribe" {
				continue
			}
			if p, ok := req.Params.(map[string]any); ok {
				if sid, ok := p[fieldSessionID].(string); ok {
					subscribedSessions <- sid
				}
			}
		}
	}()
	return subscribedSessions
}

func assertMuseSubagentSinkOutcome(t *testing.T, sink *mockTurnSink) {
	t.Helper()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.calls) != 3 {
		t.Fatalf("expected 3 tool calls, got %d: %+v", len(sink.calls), sink.calls)
	}
	if !sink.calls[0].Subagent || sink.calls[0].ID != "sub-item-1" || sink.calls[0].Command != "code-researcher" {
		t.Errorf("unexpected subagent call 0: %+v", sink.calls[0])
	}
	if sink.calls[1].ParentID != "sub-item-1" || sink.calls[1].Name != "bash" {
		t.Errorf("unexpected child tool call 1: %+v", sink.calls[1])
	}
	if !sink.calls[2].Subagent || sink.calls[2].ID != "sub-item-2" {
		t.Errorf("unexpected subagent call 2: %+v", sink.calls[2])
	}
	if sink.result == nil || sink.result.Response != "final parent answer" {
		t.Fatalf("expected final parent answer, got %+v", sink.result)
	}
	wantUsage := AgyUsage{InputTokens: 65, CacheReadTokens: 15, OutputTokens: 40, TotalTokens: 120}
	if sink.result.Usage != wantUsage {
		t.Errorf("usage = %+v, want %+v", sink.result.Usage, wantUsage)
	}
}

func TestMuseProtocol_SubagentNestingAndViewSubscribe(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	defer closeAll(inR, inW)
	subscribedSessions := startMuseSubscribeRecorder(inR)

	daemon := &StreamingDaemon{
		cfg:           DaemonConfig{Engine: "muse"},
		stdin:         inW,
		claudeToolIDs: make(map[string]int),
	}
	daemon.sessionID = "root-muse-sub"

	sink := newMockTurnSink()
	turn := enqueueInflightTurn(daemon, "turn-subagent", sink)

	// 1. Subagent item starts with childSessionId immediately available
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"sub-item-1","type":"subagent","childSessionId":"child-1","arguments":{"role":"code-researcher"}}}}`)
	// Duplicate item/updated for the same childSessionId must not send a second view/subscribe
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/updated","params":{"item":{"id":"sub-item-1","childSessionId":"child-1"}}}`)
	// Top-level item/updated without item wrapper and without id falls back to active subagent stack
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/updated","params":{"childSessionId":"child-2"}}`)

	var gotSubs []string
	for range 2 {
		select {
		case sid := <-subscribedSessions:
			gotSubs = append(gotSubs, sid)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for view/subscribe")
		}
	}
	if len(gotSubs) != 2 || gotSubs[0] != "child-1" || gotSubs[1] != "child-2" {
		t.Errorf("unexpected view/subscribe sessions: %v", gotSubs)
	}

	// 2. Child non-tool items (agentMessage, reasoning, reminderChild) must be ignored
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"sessionId":"child-1","item":{"id":"c-msg","type":"agentMessage"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"sessionId":"child-1","item":{"id":"c-msg","type":"agentMessage","text":"child response must not overwrite parent"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"sessionId":"child-1","item":{"id":"c-rem","kind":"reminderChild","type":"toolCall","tool":"bash"}}}`)

	// 3. Child toolCall started and completed -> routed with ParentID="sub-item-1"
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"sessionId":"child-1","item":{"id":"c-tool-1","type":"commandExecution","command":"ls"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"sessionId":"child-1","item":{"id":"c-tool-1","type":"commandExecution","status":"completed"}}}`)

	// 4. Child tokenUsage is folded once; subsequent item/completed usage for child-1 is skipped to prevent double-counting
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"session/tokenUsage","params":{"sessionId":"child-1","usage":{"inputTokens":50,"outputTokens":25,"cachedTokens":10}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"sub-item-1","type":"subagent","status":"completed","usage":{"inputTokens":50,"outputTokens":25,"cachedTokens":10}}}}`)

	// 5. Second subagent started via toolCall "subagent" whose usage is only reported on item/completed
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"sub-item-2","type":"toolCall","tool":"subagent","arguments":{"role":"reviewer"}}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"sub-item-2","type":"toolCall","tool":"subagent","status":"completed","usage":{"inputTokens":30,"outputTokens":15,"cachedTokens":5}}}}`)

	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"agentMessage","text":"final parent answer"}}}`)
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"status":"completed"}}`)

	assertMuseSubagentSinkOutcome(t, sink)
	_ = turn
}

func TestMuseProtocol_ChildUnmatchedCompletionIgnored(t *testing.T) {
	t.Parallel()

	daemon := &StreamingDaemon{
		cfg:           DaemonConfig{Engine: "muse"},
		claudeToolIDs: make(map[string]int),
		inFlightTools: make(map[int]inFlightToolCall),
	}
	daemon.sessionID = "root-sess"

	sink := newMockTurnSink()
	turn := enqueueInflightTurn(daemon, "turn-unmatched", sink)

	// 1 subagent starts
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"sub-item-1","type":"subagent","childSessionId":"child-sess"}}}`)

	// Only sub-item-1 is in flight
	daemon.inflightMu.Lock()
	inFlightCount := len(daemon.inFlightTools)
	daemon.inflightMu.Unlock()
	if inFlightCount != 1 {
		t.Fatalf("expected 1 in-flight tool, got %d", inFlightCount)
	}

	// Send child commandExecution completion that had no start
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"sessionId":"child-sess","item":{"id":"ghost-cmd","type":"commandExecution","status":"completed"}}}`)

	// Confirm sub-item-1 stays open in flight
	daemon.inflightMu.Lock()
	inFlightCount = len(daemon.inFlightTools)
	daemon.inflightMu.Unlock()
	if inFlightCount != 1 {
		t.Fatalf("expected sub-item-1 to stay in flight, got %d in flight", inFlightCount)
	}
	sink.mu.Lock()
	completedCalls := len(sink.completions)
	sink.mu.Unlock()
	if completedCalls != 0 {
		t.Errorf("expected 0 completed calls on sink, got %d", completedCalls)
	}

	// sub-item-1 completes normally
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"sub-item-1","type":"subagent","status":"completed"}}}`)
	sink.mu.Lock()
	completedCalls = len(sink.completions)
	sink.mu.Unlock()
	if completedCalls != 1 {
		t.Errorf("expected sub-item-1 to complete, got %d completions", completedCalls)
	}
	if turn == nil {
		t.Fatal("expected non-nil turn")
	}
}

func TestMuseProtocol_ChildSessionStackFallbackAndUsageDeduplication(t *testing.T) {
	t.Parallel()

	daemon := &StreamingDaemon{
		cfg:           DaemonConfig{Engine: "muse"},
		claudeToolIDs: make(map[string]int),
	}
	daemon.sessionID = "root-sess"

	sink := newMockTurnSink()
	turn := enqueueInflightTurn(daemon, "turn-fallback", sink)

	// 1 subagent with no childSessionId
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"sub-item-1","type":"subagent","arguments":{"role":"researcher"}}}}`)

	// Child token usage arrives for child-sess without an explicit childSessionId on start
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"session/tokenUsage","params":{"sessionId":"child-sess","usage":{"inputTokens":100,"outputTokens":20,"cachedTokens":30}}}`)

	// Parent subagent completes with the same usage payload
	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"id":"sub-item-1","type":"subagent","status":"completed","usage":{"inputTokens":100,"outputTokens":20,"cachedTokens":30}}}}`)

	daemon.dispatchMuseJSONRPCLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"status":"completed"}}`)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.result == nil {
		t.Fatalf("expected turn result")
	}
	wantUsage := AgyUsage{InputTokens: 70, CacheReadTokens: 30, OutputTokens: 20, TotalTokens: 120}
	if wantUsage != sink.result.Usage {
		t.Errorf("usage = %+v, want %+v", sink.result.Usage, wantUsage)
	}
	if turn == nil {
		t.Fatal("expected non-nil turn")
	}
}
