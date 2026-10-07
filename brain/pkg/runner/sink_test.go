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
	t.Run("PreCancelled", func(t *testing.T) {
		sink := NewThrowawayTurnSink()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := sink.ResultContext(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("CancelledWhileWaiting", func(t *testing.T) {
		sink := NewThrowawayTurnSink()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err := sink.ResultContext(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	})
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
	t.Run("OnToolCall resets accumulated buffer and suppresses pre-tool monologue resurrection", func(t *testing.T) {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
		sink.OnTextDelta("Pre-tool commentary that must be discarded.")
		if sink.AccumulatedText() == "" {
			t.Fatalf("expected non-empty buffer before tool call")
		}

		sink.OnToolCall("run_command", "bash")
		if sink.AccumulatedText() != "" {
			t.Errorf("expected empty buffer after OnToolCall, got %q", sink.AccumulatedText())
		}
		if sink.EmittedAny() {
			t.Errorf("expected emittedAny to be false after OnToolCall")
		}

		// When no subsequent deltas arrive after tool execution (silent turn or waiting step),
		// OnResult suppresses resurrected pre-tool chatter in res.Response.
		sink.OnResult(&TurnResult{Response: "Pre-tool commentary that must be discarded."})
		res, err := sink.Wait(context.Background())
		if err != nil {
			t.Fatalf("unexpected wait error: %v", err)
		}
		if res.Response != "" {
			t.Errorf("expected empty response for tool-executed turn with no post-tool deltas, got %q", res.Response)
		}
	})

	t.Run("OnToolCompleted resets accumulated buffer", func(t *testing.T) {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
		sink.OnTextDelta("Stray delta leaked while tool was executing.")
		if sink.AccumulatedText() == "" {
			t.Fatalf("expected non-empty buffer before tool completion")
		}

		sink.OnToolCompleted("run_command", "native", 100*time.Millisecond, "DONE")
		if sink.AccumulatedText() != "" {
			t.Errorf("expected empty buffer after OnToolCompleted, got %q", sink.AccumulatedText())
		}
		if sink.EmittedAny() {
			t.Errorf("expected emittedAny to be false after OnToolCompleted")
		}
	})

	t.Run("Multi-step turn lifecycle isolates final deliverable", func(t *testing.T) {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})

		// Step 1: Pre-tool commentary
		sink.OnTextDelta("Let me run tests first...")
		sink.OnToolCall("run_command", "go test ./...")

		// Tool running: stray logs leaked
		sink.OnTextDelta("tool progress chunk...")
		sink.OnToolCompleted("run_command", "native", 500*time.Millisecond, "DONE")

		// Step 2: Final response generated without tools
		sink.OnTextDelta("All tests passed! ")
		sink.OnTextDelta("Locked in and ready.")

		// agy emits result with concatenated multi-step text
		sink.OnResult(&TurnResult{
			Response: "Let me run tests first... All tests passed! Locked in and ready.",
		})

		res, err := sink.Wait(context.Background())
		if err != nil {
			t.Fatalf("unexpected wait error: %v", err)
		}
		expected := "All tests passed! Locked in and ready."
		if res.Response != expected {
			t.Errorf("expected clean terminal response %q, got %q", expected, res.Response)
		}
	})

	t.Run("Non-streaming turn with tool execution preserves final result", func(t *testing.T) {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
		// In a non-streaming runner (e.g. legacy runner adapter, mock runner),
		// tool hooks fire (e.g. from watchdog or adapter) without any streamed text deltas.
		sink.OnToolCall("run_command", "bash -c 'echo hello'")
		sink.OnToolCompleted("run_command", "native", 50*time.Millisecond, "DONE")

		// The runner finishes and delivers the full substantive output in OnResult
		sink.OnResult(&TurnResult{Response: "Completed task output."})

		res, err := sink.Wait(context.Background())
		if err != nil {
			t.Fatalf("unexpected wait error: %v", err)
		}
		if res.Response != "Completed task output." {
			t.Errorf("expected non-streaming response preserved, got %q", res.Response)
		}
	})
}

func TestBufferingTurnSink_StepResetPurgesBuffer(t *testing.T) {
	t.Run("Step transition purges intermediate chatter from prior steps", func(t *testing.T) {
		var stepHookCalls []int
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{
			OnStepStarted: func(idx int) {
				stepHookCalls = append(stepHookCalls, idx)
			},
		})

		// Step 348: Model emits intermediate waiting text
		sink.OnStepStarted(348)
		sink.OnTextDelta("I will wait for the background task to complete...")
		if sink.AccumulatedText() == "" {
			t.Fatalf("expected non-empty buffer during step 348")
		}

		// Step 349: Subagent review completes (system update) -> step index advances
		sink.OnStepStarted(349)
		if sink.AccumulatedText() != "" {
			t.Errorf("expected buffer reset after step 349, got %q", sink.AccumulatedText())
		}

		// Step 350: Intermediate triage delta
		sink.OnTextDelta("Checking tasks...")

		// Step 352: Terminal deliverable step
		sink.OnStepStarted(352)
		if sink.AccumulatedText() != "" {
			t.Errorf("expected buffer reset after step 352, got %q", sink.AccumulatedText())
		}
		sink.OnTextDelta("All tasks completed cleanly! ")
		sink.OnTextDelta("Deliverable is ready.")

		// Incoming agy result with concatenated multi-step text
		sink.OnResult(&TurnResult{
			Response: "I will wait for the background task to complete... Checking tasks... All tasks completed cleanly! Deliverable is ready.",
		})

		res, err := sink.Wait(context.Background())
		if err != nil {
			t.Fatalf("unexpected wait error: %v", err)
		}
		expected := "All tasks completed cleanly! Deliverable is ready."
		if res.Response != expected {
			t.Errorf("expected clean terminal response %q, got %q", expected, res.Response)
		}

		// Verify step hooks fired in order
		if len(stepHookCalls) != 3 || stepHookCalls[0] != 348 || stepHookCalls[1] != 349 || stepHookCalls[2] != 352 {
			t.Errorf("unexpected step hook calls: %v", stepHookCalls)
		}
	})

	t.Run("Step transition with silent terminal step suppresses resurrected chatter", func(t *testing.T) {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})

		// Step 1: Model emits waiting text
		sink.OnStepStarted(1)
		sink.OnTextDelta("Intermediate waiting commentary.")

		// Step 2: Tool execution with zero post-tool deltas
		sink.OnStepStarted(2)
		if sink.AccumulatedText() != "" {
			t.Errorf("expected buffer reset on step 2, got %q", sink.AccumulatedText())
		}

		// agy returns concatenated result containing step 1 text
		sink.OnResult(&TurnResult{
			Response: "Intermediate waiting commentary.",
		})

		res, err := sink.Wait(context.Background())
		if err != nil {
			t.Fatalf("unexpected wait error: %v", err)
		}
		if res.Response != "" {
			t.Errorf("expected resurrected chatter to be suppressed, got %q", res.Response)
		}
	})

	t.Run("Single step with deltas retains output without purge", func(t *testing.T) {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
		sink.OnStepStarted(1)
		sink.OnTextDelta("Clean direct response.")

		sink.OnResult(&TurnResult{Response: "Clean direct response."})

		res, err := sink.Wait(context.Background())
		if err != nil {
			t.Fatalf("unexpected wait error: %v", err)
		}
		if res.Response != "Clean direct response." {
			t.Errorf("expected response preserved, got %q", res.Response)
		}
	})
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
	t.Run("PreCancelled", func(t *testing.T) {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := sink.Wait(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("CancelledWhileWaiting", func(t *testing.T) {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err := sink.Wait(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	})
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
	sink.OnStepStarted(1)
	sink.OnToolCall("tool", "cmd")
	sink.OnToolCompleted("tool", "srv", time.Second, "ok")
	sink.OnSkillActivated("skill", "src")
	sink.OnTextDelta("delta")
}
