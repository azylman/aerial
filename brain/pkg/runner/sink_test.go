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
	if res.Response != "Explicit response" {
		t.Errorf("expected 'Explicit response', got %q", res.Response)
	}
	if completedRes == nil || completedRes.Response != "Explicit response" {
		t.Errorf("expected completedRes hook called with explicit response")
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
