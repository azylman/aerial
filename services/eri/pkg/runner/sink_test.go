package runner

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func (s *BufferingTurnSink) AccumulatedText() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.builder.String()
}

func TestThrowawayTurnSink(t *testing.T) {
	s := NewThrowawayTurnSink()
	s.OnTurnStarted()
	s.OnThinking()
	s.OnToolCall(ToolCall{Name: "bash", Command: "ls"})
	s.OnToolCompleted(ToolCall{Name: "bash"}, "native", time.Millisecond, "ok")
	s.OnSkillActivated("sk", "src")
	s.OnTextDelta("txt")
	s.OnResult(&TurnResult{Response: "res"})
	s.OnResult(&TurnResult{Response: "dup"})
	s.OnError(errors.New("ign"))
	if res, err := s.ResultContext(context.Background()); err != nil || res != "res" {
		t.Fatalf("Result = (%q, %v)", res, err)
	}

	sNil := NewThrowawayTurnSink()
	sNil.OnResult(nil)
	if res, err := sNil.ResultContext(context.Background()); err != nil || res != "" {
		t.Fatalf("nil TurnResult = (%q, %v)", res, err)
	}

	sErr, expErr := NewThrowawayTurnSink(), errors.New("boom")
	sErr.OnError(expErr)
	sErr.OnError(errors.New("dup"))
	sErr.OnResult(&TurnResult{Response: "ign"})
	if _, err := sErr.ResultContext(context.Background()); !errors.Is(err, expErr) {
		t.Fatalf("expected error %v, got %v", expErr, err)
	}

	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewThrowawayTurnSink().ResultContext(ctxCancel); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", err)
	}
	ctxTimeout, cancelT := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancelT()
	if _, err := NewThrowawayTurnSink().ResultContext(ctxTimeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	// Exercise ctxErr checks when both ctx.Done() and resCh/errCh are concurrently available
	for range 50 {
		resCh := make(chan string, 1)
		resCh <- "ready"
		_, _ = selectContext(ctxCancel, resCh, nil, "")

		errCh := make(chan error, 1)
		errCh <- errors.New("ready")
		_, _ = selectContext(ctxCancel, nil, errCh, "")
	}
}

func TestIsSubstantiveResponse(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"", false},
		{"   \t\n  ", false},
		{"[Tool Call Requested]: ha_call_service", false},
		{"   [Tool Call Requested]: bash   ", false},
		{"<turn>unclosed content", false},
		{"<turn>content</turn>", true},
		{"Hello, I am Aerial.", true},
		{"```go\nfmt.Println(1)\n```", true},
	} {
		if got := IsSubstantiveResponse(tc.in); got != tc.want {
			t.Errorf("IsSubstantiveResponse(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestBufferingTurnSink_LifecycleAndHooks(t *testing.T) {
	var started, thinking bool
	var toolName, cmdName, completedTool, mcp, status, skill, source string
	var dur time.Duration
	var completedRes *TurnResult

	sink := NewBufferingTurnSink(BufferingTurnSinkConfig{
		OnTurnStarted:    func() { started = true },
		OnThinking:       func() { thinking = true },
		OnToolCall:       func(tn, cn string) { toolName, cmdName = tn, cn },
		OnToolCompleted:  func(tn, ms string, d time.Duration, st string) { completedTool, mcp, dur, status = tn, ms, d, st },
		OnSkillActivated: func(sk, src string) { skill, source = sk, src },
		OnComplete:       func(res *TurnResult) { completedRes = res },
	})
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall(ToolCall{Name: "bash", Command: "echo test"})
	sink.OnToolCompleted(ToolCall{Name: "bash"}, "native", 50*time.Millisecond, "ok")
	sink.OnSkillActivated("self-improvement", "discord")
	sink.OnTextDelta("Hello ")
	sink.OnTextDelta("world!")
	if !started || !thinking || toolName != "bash" || cmdName != "echo test" || completedTool != "bash" || mcp != "native" || dur != 50*time.Millisecond || status != "ok" || skill != "self-improvement" || source != "discord" {
		t.Fatalf("hook callbacks missing or mismatched")
	}
	if sink.AccumulatedText() != "Hello world!" {
		t.Errorf("accumulated = %q", sink.AccumulatedText())
	}
	sink.MarkEmitted()
	if !sink.EmittedAny() {
		t.Errorf("MarkEmitted failed")
	}
	sink.OnResult(&TurnResult{Response: "Explicit response"})
	res, err := sink.Wait(context.Background())
	if err != nil || res.Response != "Hello world!" || completedRes == nil || completedRes.Response != "Hello world!" {
		t.Fatalf("Wait or OnComplete mismatch: (%v, %v)", res, err)
	}
	nilSink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	nilSink.OnToolCompleted(ToolCall{Name: "bash"}, "native", 10*time.Millisecond, "ok")
	nilSink.OnSkillActivated("self-improvement", "discord")
}

func TestBufferingTurnSink_OnResultPrecedence(t *testing.T) {
	for _, tc := range []struct {
		deltas   []string
		incoming *TurnResult
		wantResp string
	}{
		{[]string{"Clean terminal output."}, &TurnResult{Response: "Concatenated multi-step noise"}, "Clean terminal output."},
		{nil, &TurnResult{Response: "Non-streaming fallback response"}, "Non-streaming fallback response"},
		{[]string{"   \n\t  "}, &TurnResult{Response: "Fallback for whitespace buffer"}, "Fallback for whitespace buffer"},
		{[]string{"Delta text"}, nil, "Delta text"},
		{nil, &TurnResult{Response: ""}, ""},
		{[]string{"Streamed delta message."}, &TurnResult{Response: ""}, "Streamed delta message."},
		{[]string{"Streamed delta for nil result."}, nil, "Streamed delta for nil result."},
	} {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
		for _, d := range tc.deltas {
			sink.OnTextDelta(d)
		}
		sink.OnResult(tc.incoming)
		res, err := sink.Wait(context.Background())
		if err != nil || res == nil || res.Response != tc.wantResp {
			t.Errorf("OnResultPrecedence = (%v, %v), want %q", res, err, tc.wantResp)
		}
	}
}

// purgeEvent is one sink callback in a scripted purge scenario: a text delta, a tool call, a
// tool completion (name + status) or a step start.
type purgeEvent struct {
	kind, text, status string
	step               int
}

func purgeDelta(text string) purgeEvent { return purgeEvent{kind: "delta", text: text} }
func purgeToolCall() purgeEvent         { return purgeEvent{kind: "toolCall", text: "run_command"} }
func purgeToolDone(name, status string) purgeEvent {
	return purgeEvent{kind: "toolDone", text: name, status: status}
}
func purgeStep(idx int) purgeEvent { return purgeEvent{kind: "step", step: idx} }

func TestBufferingTurnSink_PurgesBufferOnReset(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		events    []purgeEvent
		res       string // "" asserts AccumulatedText instead of the waited result
		want      string
		wantHooks []int // non-nil installs an OnStepStarted hook
	}{
		{name: "tool call purges pre-tool commentary", events: []purgeEvent{purgeDelta("Pre-tool commentary."), purgeToolCall()}, res: "Pre-tool commentary."},
		{name: "tool completion purges stray delta", events: []purgeEvent{purgeDelta("Stray delta."), purgeToolDone("run_command", "DONE")}},
		{name: "post-tool deltas replace noisy result", events: []purgeEvent{
			purgeDelta("Let me run tests..."), purgeToolCall(), purgeDelta("chunk..."), purgeToolDone("run_command", "DONE"), purgeDelta("All tests passed! "), purgeDelta("Ready."),
		}, res: "Noise. All tests passed! Ready.", want: "All tests passed! Ready."},
		{name: "no deltas keeps result", events: []purgeEvent{purgeToolCall(), purgeToolDone("run_command", "DONE")}, res: "Completed task output.", want: "Completed task output."},
		{name: "aborted subagent keeps final deliverable", events: []purgeEvent{
			purgeDelta("Spawning subagent..."), purgeToolCall(), purgeDelta("Final deliverable preserved."), purgeToolDone("invoke_subagent", "aborted"),
		}, res: "Spawning subagent... Final deliverable preserved.", want: "Final deliverable preserved."},
		{name: "aborted tool keeps streamed response", events: []purgeEvent{purgeDelta("Streamed final response."), purgeToolDone("invoke_subagent", "aborted")}, res: "Streamed final response.", want: "Streamed final response."},
		{name: "step starts purge earlier steps", events: []purgeEvent{
			purgeStep(348), purgeDelta("wait..."), purgeStep(349), purgeDelta("check..."), purgeStep(352), purgeDelta("Clean! "), purgeDelta("Ready."),
		}, res: "wait... check... Clean! Ready.", want: "Clean! Ready.", wantHooks: []int{348, 349, 352}},
		{name: "empty final step purges commentary", events: []purgeEvent{purgeStep(1), purgeDelta("Waiting commentary."), purgeStep(2)}, res: "Waiting commentary."},
		{name: "single step keeps direct response", events: []purgeEvent{purgeStep(1), purgeDelta("Clean direct response.")}, res: "Clean direct response.", want: "Clean direct response."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var hooks []int
			cfg := BufferingTurnSinkConfig{}
			if tc.wantHooks != nil {
				cfg.OnStepStarted = func(idx int) { hooks = append(hooks, idx) }
			}
			sink := NewBufferingTurnSink(cfg)
			for _, ev := range tc.events {
				switch ev.kind {
				case "delta":
					sink.OnTextDelta(ev.text)
				case "toolCall":
					sink.OnToolCall(ToolCall{Name: ev.text, Command: "bash"})
				case "toolDone":
					sink.OnToolCompleted(ToolCall{Name: ev.text}, "native", 100*time.Millisecond, ev.status)
				case "step":
					sink.OnStepStarted(ev.step)
				}
			}
			if tc.res == "" {
				if got := sink.AccumulatedText(); got != tc.want {
					t.Errorf("accum = %q, want %q", got, tc.want)
				}
			} else {
				sink.OnResult(&TurnResult{Response: tc.res})
				if res, err := sink.Wait(context.Background()); err != nil || res.Response != tc.want {
					t.Errorf("response = (%v, %v), want %q", res, err, tc.want)
				}
			}
			if !reflect.DeepEqual(hooks, tc.wantHooks) {
				t.Errorf("hooks = %v, want %v", hooks, tc.wantHooks)
			}
		})
	}
}

func TestBufferingTurnSink_ErrorAndDeltaRescue(t *testing.T) {
	errRes, errHard, errAbort := errors.New("API error: 429 RESOURCE_EXHAUSTED"), errors.New("hard failure"), errors.New("aborted before response")
	for _, tc := range []struct {
		disableRescue bool
		delta         string
		inErr         error
		wantResp      string
		wantErr       error
		wantTokens    int
	}{
		{false, "Recoverable substantive text before daemon crash.", errRes, "Recoverable substantive text before daemon crash.", nil, 0},
		{true, "Some text", errHard, "", errHard, 0},
		{false, "[Tool Call Requested]: uncompleted tool", errAbort, "", errAbort, 0},
		{false, "Text streamed before the daemon was killed.", &TurnError{Err: errHard, Usage: AgyUsage{InputTokens: 30, OutputTokens: 5, TotalTokens: 35}}, "Text streamed before the daemon was killed.", nil, 35},
	} {
		var hookErr error
		var completedRes *TurnResult
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{
			DisableDeltaRescue: tc.disableRescue,
			OnError:            func(err error) { hookErr = err },
			OnComplete:         func(res *TurnResult) { completedRes = res },
		})
		sink.OnTextDelta(tc.delta)
		sink.OnError(tc.inErr)
		res, err := sink.Wait(context.Background())
		if tc.wantErr != nil {
			if res != nil || !errors.Is(err, tc.wantErr) || (tc.disableRescue && !errors.Is(hookErr, tc.wantErr)) {
				t.Errorf("expected error %v, got res=%v, err=%v, hookErr=%v", tc.wantErr, res, err, hookErr)
			}
		} else if err != nil || res == nil || res.Response != tc.wantResp || res.Usage.TotalTokens != tc.wantTokens || completedRes == nil || completedRes.Response != tc.wantResp {
			t.Errorf("expected rescued response %q, got res=%v, err=%v, completed=%v", tc.wantResp, res, err, completedRes)
		}
	}
}

func TestBufferingTurnSink_WaitContextCancelled(t *testing.T) {
	t.Parallel()
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	ctxTimeout, cancelT := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelT()
	if _, err := NewBufferingTurnSink(BufferingTurnSinkConfig{}).Wait(ctxCancel); !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got %v", err)
	}
	if _, err := NewBufferingTurnSink(BufferingTurnSinkConfig{}).Wait(ctxTimeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}
