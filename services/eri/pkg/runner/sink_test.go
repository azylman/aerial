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
	s.OnToolCall("bash", "ls")
	s.OnToolCompleted("bash", "native", time.Millisecond, "ok")
	s.OnSkillActivated("sk", "src")
	s.OnTextDelta("txt")
	s.OnResult(&TurnResult{Response: "res"})
	s.OnResult(&TurnResult{Response: "dup"})
	s.OnError(errors.New("ign"))
	if res, err := s.Result(); err != nil || res != "res" {
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
	sink.OnToolCall("bash", "echo test")
	sink.OnToolCompleted("bash", "native", 50*time.Millisecond, "ok")
	sink.OnSkillActivated("self-improvement", "discord")
	sink.OnTextDelta("Hello ")
	sink.OnTextDelta("world!")
	if !started || !thinking || toolName != "bash" || cmdName != "echo test" || completedTool != "bash" || mcp != "native" || dur != 50*time.Millisecond || status != "ok" || skill != "self-improvement" || source != "discord" {
		t.Fatalf("hook callbacks missing or mismatched")
	}
	if sink.AccumulatedText() != "Hello world!" {
		t.Errorf("accumulated = %q", sink.AccumulatedText())
	}
	sink.SetEmitted(true)
	sink.SetEmitted(false)
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
	nilSink.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
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

func TestBufferingTurnSink_ToolResetPurgesBuffer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		pre, mid, post []string
		call, done     bool
		res, want      string
	}{
		{pre: []string{"Pre-tool commentary."}, call: true, res: "Pre-tool commentary.", want: ""},
		{pre: []string{"Stray delta."}, done: true, res: "", want: ""},
		{pre: []string{"Let me run tests..."}, mid: []string{"chunk..."}, post: []string{"All tests passed! ", "Ready."}, call: true, done: true, res: "Noise. All tests passed! Ready.", want: "All tests passed! Ready."},
		{call: true, done: true, res: "Completed task output.", want: "Completed task output."},
	} {
		sink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
		feed := func(deltas []string) {
			for _, d := range deltas {
				sink.OnTextDelta(d)
			}
		}
		feed(tc.pre)
		if tc.call {
			sink.OnToolCall("run_command", "bash")
		}
		feed(tc.mid)
		if tc.done {
			sink.OnToolCompleted("run_command", "native", 100*time.Millisecond, "DONE")
		}
		feed(tc.post)
		if tc.res != "" {
			sink.OnResult(&TurnResult{Response: tc.res})
			if res, err := sink.Wait(context.Background()); err != nil || res.Response != tc.want {
				t.Errorf("response = (%v, %v), want %q", res, err, tc.want)
			}
		} else if sink.AccumulatedText() != tc.want {
			t.Errorf("accum = %q, want %q", sink.AccumulatedText(), tc.want)
		}
	}
}

func TestBufferingTurnSink_StepResetPurgesBuffer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		steps         [][]string
		stepIndices   []int
		res, wantResp string
		wantHooks     []int
	}{
		{[][]string{{"wait..."}, {"check..."}, {"Clean! ", "Ready."}}, []int{348, 349, 352}, "wait... check... Clean! Ready.", "Clean! Ready.", []int{348, 349, 352}},
		{[][]string{{"Waiting commentary."}, nil}, []int{1, 2}, "Waiting commentary.", "", nil},
		{[][]string{{"Clean direct response."}}, []int{1}, "Clean direct response.", "Clean direct response.", nil},
	} {
		var hooks []int
		cfg := BufferingTurnSinkConfig{}
		if tc.wantHooks != nil {
			cfg.OnStepStarted = func(idx int) { hooks = append(hooks, idx) }
		}
		sink := NewBufferingTurnSink(cfg)
		for i, deltas := range tc.steps {
			sink.OnStepStarted(tc.stepIndices[i])
			for _, d := range deltas {
				sink.OnTextDelta(d)
			}
		}
		sink.OnResult(&TurnResult{Response: tc.res})
		if res, err := sink.Wait(context.Background()); err != nil || res.Response != tc.wantResp {
			t.Errorf("response = (%v, %v), want %q", res, err, tc.wantResp)
		}
		if tc.wantHooks != nil && !reflect.DeepEqual(hooks, tc.wantHooks) {
			t.Errorf("hooks = %v, want %v", hooks, tc.wantHooks)
		}
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
	}{
		{false, "Recoverable substantive text before daemon crash.", errRes, "Recoverable substantive text before daemon crash.", nil}, {true, "Some text", errHard, "", errHard}, {false, "[Tool Call Requested]: uncompleted tool", errAbort, "", errAbort},
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
		} else if err != nil || res == nil || res.Response != tc.wantResp || completedRes == nil || completedRes.Response != tc.wantResp {
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
