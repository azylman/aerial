package runner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestThrowawayTurnSink_AllMethodsAndSuccess(t *testing.T) {
	sink := NewThrowawayTurnSink()

	// Verify no-ops do not panic
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("bash", "echo hi")
	sink.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
	sink.OnSkillActivated("self-improvement", "discord")
	sink.OnTextDelta("some text")

	// Test successful result delivery
	sink.OnResult(&TurnResult{Response: "test response"})

	// sync.Once test: duplicate calls should be ignored
	sink.OnResult(&TurnResult{Response: "ignored second result"})
	sink.OnError(errors.New("ignored error after result"))

	res, err := sink.Result()
	if err != nil {
		t.Fatalf("unexpected error from Result: %v", err)
	}
	if res != "test response" {
		t.Fatalf("expected 'test response', got %q", res)
	}
}

func TestThrowawayTurnSink_NilResult(t *testing.T) {
	sink := NewThrowawayTurnSink()
	sink.OnResult(nil)

	res, err := sink.ResultContext(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "" {
		t.Fatalf("expected empty string for nil TurnResult, got %q", res)
	}
}

func TestThrowawayTurnSink_ErrorDelivery(t *testing.T) {
	sink := NewThrowawayTurnSink()
	expectedErr := errors.New("turn execution crashed")
	sink.OnError(expectedErr)

	// Idempotent sync.Once check
	sink.OnError(errors.New("second error"))
	sink.OnResult(&TurnResult{Response: "ignored"})

	_, err := sink.ResultContext(context.Background())
	if err == nil || !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

func TestThrowawayTurnSink_ContextCancelled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		preCancel  bool
		waitCancel bool
		wantErr    error
	}{
		{name: "PreCancelled", preCancel: true, wantErr: context.Canceled},
		{name: "CancelledWhileWaiting", waitCancel: true, wantErr: context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sink := NewThrowawayTurnSink()
			var ctx context.Context
			var cancel context.CancelFunc
			if tc.preCancel {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			} else if tc.waitCancel {
				ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
			}
			_, err := sink.ResultContext(ctx)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestIsSubstantiveResponse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"empty string", "", false},
		{"only whitespace", "   \t\n  ", false},
		{"tool call requested prefix", "[Tool Call Requested]: ha_call_service", false},
		{"tool call with whitespace", "   [Tool Call Requested]: bash   ", false},
		{"unclosed turn tag", "<turn>unclosed content", false},
		{"closed turn tag with content", "<turn>content</turn>", true},
		{"normal text response", "Hello, I am Aerial.", true},
		{"code block response", "```go\nfmt.Println(1)\n```", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := IsSubstantiveResponse(tt.input)
			if actual != tt.expected {
				t.Errorf("IsSubstantiveResponse(%q) = %v; want %v", tt.input, actual, tt.expected)
			}
		})
	}
}

func TestBufferingTurnSink_AllHooksAndSuccess(t *testing.T) {
	var (
		startedCalled  bool
		thinkingCalled bool
		toolNameCalled string
		cmdNameCalled  string
		deltasReceived []string
		completedRes   *TurnResult
	)

	sink := NewBufferingTurnSink(BufferingTurnSinkConfig{
		OnTurnStarted: func() { startedCalled = true },
		OnThinking:    func() { thinkingCalled = true },
		OnToolCall: func(toolName, cmdName string) {
			toolNameCalled = toolName
			cmdNameCalled = cmdName
		},
		OnTextDelta: func(delta string) {
			deltasReceived = append(deltasReceived, delta)
		},
		OnComplete: func(res *TurnResult) {
			completedRes = res
		},
	})

	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("bash", "echo test")
	sink.OnTextDelta("Hello ")
	sink.OnTextDelta("world!")

	if !startedCalled || !thinkingCalled {
		t.Errorf("expected started and thinking hooks called")
	}
	if toolNameCalled != "bash" || cmdNameCalled != "echo test" {
		t.Errorf("unexpected tool hook args: %s, %s", toolNameCalled, cmdNameCalled)
	}
	if sink.AccumulatedText() != "Hello world!" {
		t.Errorf("expected accumulated 'Hello world!', got %q", sink.AccumulatedText())
	}

	sink.SetEmitted(true)
	if !sink.EmittedAny() {
		t.Errorf("expected EmittedAny to be true")
	}
	sink.SetEmitted(false)
	sink.MarkEmitted()
	if !sink.EmittedAny() {
		t.Errorf("expected MarkEmitted to set EmittedAny to true")
	}

	sink.OnResult(&TurnResult{Response: "Explicit response"})

	res, err := sink.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Response != "Hello world!" {
		t.Errorf("expected buffer response 'Hello world!', got %q", res.Response)
	}
	if completedRes == nil || completedRes.Response != "Hello world!" {
		t.Errorf("expected completedRes hook called with buffer response")
	}
}

func TestBufferingTurnSink_EmptyResultBackfilledFromDeltas(t *testing.T) {
	sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	sink.OnTextDelta("Streamed delta message.")
	sink.OnResult(&TurnResult{Response: ""})

	res, err := sink.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Response != "Streamed delta message." {
		t.Errorf("expected backfilled response, got %q", res.Response)
	}
}

func TestBufferingTurnSink_NilTurnResultBackfilledFromDeltas(t *testing.T) {
	sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	sink.OnTextDelta("Streamed delta for nil result.")
	sink.OnResult(nil)

	res, err := sink.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Response != "Streamed delta for nil result." {
		t.Errorf("expected backfilled response from nil result, got %q", res.Response)
	}
}

func TestBufferingTurnSink_OnResultPrecedence(t *testing.T) {
	tests := []struct {
		name         string
		deltas       []string
		incomingRes  *TurnResult
		expectedResp string
	}{
		{
			name:         "Buffer preferred when both buffer and res.Response are non-empty",
			deltas:       []string{"Clean terminal output."},
			incomingRes:  &TurnResult{Response: "Concatenated multi-step noise"},
			expectedResp: "Clean terminal output.",
		},
		{
			name:         "res.Response fallback when buffer is empty",
			deltas:       nil,
			incomingRes:  &TurnResult{Response: "Non-streaming fallback response"},
			expectedResp: "Non-streaming fallback response",
		},
		{
			name:         "res.Response fallback when buffer is whitespace-only",
			deltas:       []string{"   \n\t  "},
			incomingRes:  &TurnResult{Response: "Fallback for whitespace buffer"},
			expectedResp: "Fallback for whitespace buffer",
		},
		{
			name:         "nil result initialized with buffer deltas",
			deltas:       []string{"Delta text"},
			incomingRes:  nil,
			expectedResp: "Delta text",
		},
		{
			name:         "both empty yields empty response",
			deltas:       nil,
			incomingRes:  &TurnResult{Response: ""},
			expectedResp: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
			for _, d := range tc.deltas {
				sink.OnTextDelta(d)
			}
			sink.OnResult(tc.incomingRes)

			res, err := sink.Wait(context.Background())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res == nil {
				t.Fatalf("expected non-nil result")
			}
			if res.Response != tc.expectedResp {
				t.Errorf("expected %q, got %q", tc.expectedResp, res.Response)
			}
		})
	}
}

func TestBufferingTurnSink_ToolResetPurgesBuffer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		preDeltas            []string
		triggerToolCall      bool
		toolName             string
		toolInput            string
		midDeltas            []string
		triggerToolComplete  bool
		postDeltas           []string
		resultPayload        string
		wantAccumulatedAfter string
		wantEmittedAny       bool
		wantResponse         string
	}{
		{
			name:                 "OnToolCall resets accumulated buffer and suppresses pre-tool monologue resurrection",
			preDeltas:            []string{"Pre-tool commentary that must be discarded."},
			triggerToolCall:      true,
			toolName:             "run_command",
			toolInput:            "bash",
			resultPayload:        "Pre-tool commentary that must be discarded.",
			wantAccumulatedAfter: "",
			wantEmittedAny:       false,
			wantResponse:         "",
		},
		{
			name:                 "OnToolCompleted resets accumulated buffer",
			preDeltas:            []string{"Stray delta leaked while tool was executing."},
			triggerToolComplete:  true,
			toolName:             "run_command",
			wantAccumulatedAfter: "",
			wantEmittedAny:       false,
		},
		{
			name:                "Multi-step turn lifecycle isolates final deliverable",
			preDeltas:           []string{"Let me run tests first..."},
			triggerToolCall:     true,
			toolName:            "run_command",
			toolInput:           "go test ./...",
			midDeltas:           []string{"tool progress chunk..."},
			triggerToolComplete: true,
			postDeltas:          []string{"All tests passed! ", "Locked in and ready."},
			resultPayload:       "Let me run tests first... All tests passed! Locked in and ready.",
			wantResponse:        "All tests passed! Locked in and ready.",
		},
		{
			name:                "Non-streaming turn with tool execution preserves final result",
			triggerToolCall:     true,
			toolName:            "run_command",
			toolInput:           "bash -c 'echo hello'",
			triggerToolComplete: true,
			resultPayload:       "Completed task output.",
			wantResponse:        "Completed task output.",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
			for _, d := range tc.preDeltas {
				sink.OnTextDelta(d)
			}
			if len(tc.preDeltas) > 0 && sink.AccumulatedText() == "" {
				t.Fatalf("expected non-empty buffer before tool transition")
			}
			if tc.triggerToolCall {
				sink.OnToolCall(tc.toolName, tc.toolInput)
			}
			for _, d := range tc.midDeltas {
				sink.OnTextDelta(d)
			}
			if tc.triggerToolComplete {
				sink.OnToolCompleted(tc.toolName, "native", 100*time.Millisecond, "DONE")
			}
			if tc.wantAccumulatedAfter != "" || (!tc.triggerToolComplete && tc.triggerToolCall) || (tc.triggerToolComplete && len(tc.postDeltas) == 0 && tc.resultPayload == "") {
				if sink.AccumulatedText() != tc.wantAccumulatedAfter {
					t.Errorf("expected accumulated text %q, got %q", tc.wantAccumulatedAfter, sink.AccumulatedText())
				}
				if sink.EmittedAny() != tc.wantEmittedAny {
					t.Errorf("expected emittedAny %v, got %v", tc.wantEmittedAny, sink.EmittedAny())
				}
			}
			for _, d := range tc.postDeltas {
				sink.OnTextDelta(d)
			}
			if tc.resultPayload != "" {
				sink.OnResult(&TurnResult{Response: tc.resultPayload})
				res, err := sink.Wait(context.Background())
			if err != nil {
				t.Fatalf("unexpected wait error: %v", err)
			}
			if res.Response != tc.wantResponse {
				t.Errorf("expected response %q, got %q", tc.wantResponse, res.Response)
			}
			}
		})
	}
}

func TestBufferingTurnSink_StepResetPurgesBuffer(t *testing.T) {
	t.Parallel()
	type stepEvent struct {
		stepIdx int
		deltas  []string
	}
	tests := []struct {
		name          string
		steps         []stepEvent
		resultPayload string
		wantResponse  string
		wantHookCalls []int
	}{
		{
			name: "Step transition purges intermediate chatter from prior steps",
			steps: []stepEvent{
				{stepIdx: 348, deltas: []string{"I will wait for the background task to complete..."}},
				{stepIdx: 349, deltas: []string{"Checking tasks..."}},
				{stepIdx: 352, deltas: []string{"All tasks completed cleanly! ", "Deliverable is ready."}},
			},
			resultPayload: "I will wait for the background task to complete... Checking tasks... All tasks completed cleanly! Deliverable is ready.",
			wantResponse:  "All tasks completed cleanly! Deliverable is ready.",
			wantHookCalls: []int{348, 349, 352},
		},
		{
			name: "Step transition with silent terminal step suppresses resurrected chatter",
			steps: []stepEvent{
				{stepIdx: 1, deltas: []string{"Intermediate waiting commentary."}},
				{stepIdx: 2, deltas: nil},
			},
			resultPayload: "Intermediate waiting commentary.",
			wantResponse:  "",
		},
		{
			name: "Single step with deltas retains output without purge",
			steps: []stepEvent{
				{stepIdx: 1, deltas: []string{"Clean direct response."}},
			},
			resultPayload: "Clean direct response.",
			wantResponse:  "Clean direct response.",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var hookCalls []int
			cfg := BufferingTurnSinkConfig{}
			if tc.wantHookCalls != nil {
				cfg.OnStepStarted = func(idx int) {
					hookCalls = append(hookCalls, idx)
				}
			}
			sink := NewBufferingTurnSink(cfg)
			for i, s := range tc.steps {
				sink.OnStepStarted(s.stepIdx)
				if i > 0 && len(s.deltas) == 0 {
					if sink.AccumulatedText() != "" {
						t.Errorf("expected empty buffer after step start %d, got %q", s.stepIdx, sink.AccumulatedText())
					}
				}
				for _, d := range s.deltas {
					sink.OnTextDelta(d)
				}
				if len(s.deltas) > 0 && sink.AccumulatedText() == "" {
					t.Fatalf("expected non-empty buffer during step %d", s.stepIdx)
				}
			}
			sink.OnResult(&TurnResult{Response: tc.resultPayload})
			res, err := sink.Wait(context.Background())
			if err != nil {
				t.Fatalf("unexpected wait error: %v", err)
			}
			if res.Response != tc.wantResponse {
				t.Errorf("expected response %q, got %q", tc.wantResponse, res.Response)
			}
			if tc.wantHookCalls != nil {
				if len(hookCalls) != len(tc.wantHookCalls) {
					t.Fatalf("expected hook calls %v, got %v", tc.wantHookCalls, hookCalls)
				}
				for i, want := range tc.wantHookCalls {
					if hookCalls[i] != want {
						t.Errorf("step hook call %d: got %d, want %d", i, hookCalls[i], want)
					}
				}
			}
		})
	}
}

func TestBufferingTurnSink_DeltaRescueOnError(t *testing.T) {
	var completedRes *TurnResult
	sink := NewBufferingTurnSink(BufferingTurnSinkConfig{
		OnComplete: func(res *TurnResult) {
			completedRes = res
		},
	})
	sink.OnTextDelta("Recoverable substantive text before daemon crash.")
	sink.OnError(errors.New("API error: 429 RESOURCE_EXHAUSTED"))

	res, err := sink.Wait(context.Background())
	if err != nil {
		t.Fatalf("expected salvaged result without error, got: %v", err)
	}
	if res.Response != "Recoverable substantive text before daemon crash." {
		t.Errorf("expected recovered text, got %q", res.Response)
	}
	if completedRes == nil || completedRes.Response != res.Response {
		t.Errorf("expected OnComplete called on salvaged deltas")
	}
}

func TestBufferingTurnSink_DeltaRescueDisabled(t *testing.T) {
	var errReported error
	sink := NewBufferingTurnSink(BufferingTurnSinkConfig{
		DisableDeltaRescue: true,
		OnError: func(err error) {
			errReported = err
		},
	})
	sink.OnTextDelta("Some text")
	expectedErr := errors.New("hard failure")
	sink.OnError(expectedErr)

	res, err := sink.Wait(context.Background())
	if res != nil {
		t.Errorf("expected nil result, got: %+v", res)
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
	if !errors.Is(errReported, expectedErr) {
		t.Errorf("expected OnError hook called with %v", expectedErr)
	}
}

func TestBufferingTurnSink_NonSubstantiveErrorPropagated(t *testing.T) {
	sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	sink.OnTextDelta("[Tool Call Requested]: uncompleted tool")
	expectedErr := errors.New("aborted before response")
	sink.OnError(expectedErr)

	res, err := sink.Wait(context.Background())
	if res != nil {
		t.Errorf("expected nil result, got: %+v", res)
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

func TestBufferingTurnSink_WaitContextCancelled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		preCancel  bool
		waitCancel bool
		wantErr    error
	}{
		{name: "PreCancelled", preCancel: true, wantErr: context.Canceled},
		{name: "CancelledWhileWaiting", waitCancel: true, wantErr: context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
			var ctx context.Context
			var cancel context.CancelFunc
			if tc.preCancel {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			} else if tc.waitCancel {
				ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
			}
			_, err := sink.Wait(ctx)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestBufferingTurnSink_ToolAndSkillHooks(t *testing.T) {
	var (
		completedTool string
		mcp           string
		dur           time.Duration
		st            string
		actSkill      string
		src           string
	)
	sink := NewBufferingTurnSink(BufferingTurnSinkConfig{
		OnToolCompleted: func(toolName, mcpServer string, duration time.Duration, status string) {
			completedTool = toolName
			mcp = mcpServer
			dur = duration
			st = status
		},
		OnSkillActivated: func(skillName, source string) {
			actSkill = skillName
			src = source
		},
	})

	sink.OnToolCompleted("bash", "native", 50*time.Millisecond, "ok")
	if completedTool != "bash" || mcp != "native" || dur != 50*time.Millisecond || st != "ok" {
		t.Fatalf("unexpected tool completed hook args")
	}

	sink.OnSkillActivated("self-improvement", "discord")
	if actSkill != "self-improvement" || src != "discord" {
		t.Fatalf("unexpected skill activated hook args")
	}

	// Test nil callbacks don't panic
	nilSink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	nilSink.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
	nilSink.OnSkillActivated("self-improvement", "discord")
}

func TestThrowawayTurnSink_UnusedHooks(t *testing.T) {
	sink := NewThrowawayTurnSink()
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("tool", "cmd")
	sink.OnToolCompleted("tool", "srv", time.Second, "ok")
	sink.OnSkillActivated("skill", "src")
	sink.OnTextDelta("delta")
}
