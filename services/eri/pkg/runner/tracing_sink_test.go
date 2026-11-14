package runner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// sinkEvent is one scripted TurnSink callback: a tool start (start=true), a tool
// completion, or the terminal result/error when call is zero.
type sinkEvent struct {
	start  bool
	call   ToolCall
	server string
	status string
}

// wantSpan describes an ended span by name: its parent's name ("turn" is the root),
// status, and attributes that must be present.
type wantSpan struct {
	name   string
	parent string
	code   codes.Code
	desc   string
	attrs  []attribute.KeyValue
}

const leakedPAT = "ghp_abcdefghijklmnopqrstuvwxyz0123456789"

type tracingTurnSinkCase struct {
	name     string
	events   []sinkEvent
	result   *TurnResult
	turnErr  error
	want     []wantSpan
	innerOK  int // tool callbacks the inner sink must still receive
	noLeakOf string
	wantUse  AgyUsage // usage reported by Usage once the turn closes
}

func driveTracingTurnSinkEvents(sink *TracingTurnSink, tc tracingTurnSinkCase, turn trace.Span) {
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnStepStarted(1)
	sink.OnSkillActivated("self-improvement", "discord")
	sink.OnTextDelta("partial")
	for _, ev := range tc.events {
		if ev.start {
			sink.OnToolCall(ev.call)
		} else {
			sink.OnToolCompleted(ev.call, ev.server, time.Millisecond, ev.status)
		}
	}
	if tc.turnErr != nil {
		sink.OnError(tc.turnErr)
	} else {
		sink.OnResult(tc.result)
	}
	sink.OnResult(&TurnResult{}) // late duplicate terminal callbacks must not re-end spans
	turn.End()
}

func assertTracingSpanProperties(t *testing.T, s sdktrace.ReadOnlySpan, w wantSpan, parentName, noLeakOf string, i int) {
	t.Helper()
	if s.Name() != w.name || parentName != w.parent {
		t.Errorf("span %d = %q (parent %q), want %q (parent %q)", i, s.Name(), parentName, w.name, w.parent)
	}
	if s.Status().Code != w.code || (w.desc != "" && s.Status().Description != w.desc) {
		t.Errorf("span %q status = %v %q, want %v %q", w.name, s.Status().Code, s.Status().Description, w.code, w.desc)
	}
	have := map[attribute.Key]attribute.Value{}
	for _, kv := range s.Attributes() {
		have[kv.Key] = kv.Value
	}
	for _, kv := range w.attrs {
		if v, ok := have[kv.Key]; !ok || v != kv.Value {
			t.Errorf("span %q attr %s = %v, want %v", w.name, kv.Key, v.String(), kv.Value.String())
		}
	}
	if noLeakOf != "" {
		var dump strings.Builder
		dump.WriteString(s.Status().Description)
		for _, kv := range s.Attributes() {
			dump.WriteString(" " + kv.Value.String())
		}
		if strings.Contains(dump.String(), noLeakOf) {
			t.Errorf("span %q leaked %q: %s", w.name, noLeakOf, dump.String())
		}
	}
}

func assertTracingTurnSpans(t *testing.T, tc tracingTurnSinkCase, ended []sdktrace.ReadOnlySpan) {
	t.Helper()
	names := map[string]string{}
	for _, s := range ended {
		names[s.SpanContext().SpanID().String()] = s.Name()
	}
	got := ended[:len(ended)-1] // drop the root "turn" span
	if len(got) != len(tc.want) {
		t.Fatalf("ended %d spans, want %d: %v", len(got), len(tc.want), spanNames(got))
	}
	for i, w := range tc.want {
		assertTracingSpanProperties(t, got[i], w, names[got[i].Parent().SpanID().String()], tc.noLeakOf, i)
	}
}

func assertInnerTurnSinkState(t *testing.T, tc tracingTurnSinkCase, inner *mockTurnSink) {
	t.Helper()
	inner.mu.Lock()
	forwarded := len(inner.calls) + len(inner.completions)
	passthrough := inner.started && inner.thinking == 1 && len(inner.stepStarts) == 1 && len(inner.activatedSkills) == 1 && len(inner.deltas) == 1
	inner.mu.Unlock()
	if forwarded != tc.innerOK || !passthrough {
		t.Errorf("inner sink got %d tool callbacks (passthrough=%v), want %d", forwarded, passthrough, tc.innerOK)
	}
}

func runTracingTurnSinkCase(t *testing.T, tc tracingTurnSinkCase) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	turnCtx, turn := tp.Tracer("test").Start(context.Background(), "turn")

	inner := newMockTurnSink()
	sink := NewTracingTurnSink(turnCtx, inner)
	driveTracingTurnSinkEvents(sink, tc, turn)

	if use, ok := sink.Usage(context.Background()); !ok || use != tc.wantUse {
		t.Errorf("Usage() = %+v, %t; want %+v, true", use, ok, tc.wantUse)
	}

	assertTracingTurnSpans(t, tc, rec.Ended())
	assertInnerTurnSinkState(t, tc, inner)
}

func TestTracingTurnSink(t *testing.T) {
	t.Parallel()
	tests := []tracingTurnSinkCase{
		{
			name: "single tool",
			events: []sinkEvent{
				{start: true, call: ToolCall{ID: "toolu_1", Name: "Bash", Command: "rm -rf secret-arg"}},
				{call: ToolCall{ID: "toolu_1", Name: "Bash"}, server: "native", status: "ok"},
			},
			result: &TurnResult{Usage: AgyUsage{InputTokens: 120, OutputTokens: 30, CacheReadTokens: 80, CacheCreationTokens: 20}},
			want: []wantSpan{
				{name: "execute_tool Bash", parent: "turn", code: codes.Unset, attrs: []attribute.KeyValue{AttrGenAIToolCallID.String("toolu_1"), AttrMCPServer.String("native"), AttrGenAIOperationName.String("execute_tool")}},
				{name: "chat", parent: "turn", code: codes.Unset, attrs: []attribute.KeyValue{AttrGenAIUsageInputTokens.Int(120), AttrGenAIUsageOutputTokens.Int(30), AttrGenAIUsageCacheRead.Int(80), AttrGenAIUsageCacheCreation.Int(20)}},
			},
			innerOK:  2,
			noLeakOf: "secret-arg",
			wantUse:  AgyUsage{InputTokens: 120, OutputTokens: 30, CacheReadTokens: 80, CacheCreationTokens: 20},
		},
		{
			name: "parallel tools correlate by call id",
			events: []sinkEvent{
				{start: true, call: ToolCall{ID: "toolu_a", Name: "Read"}},
				{start: true, call: ToolCall{ID: "toolu_b", Name: "Read"}},
				{call: ToolCall{ID: "toolu_b", Name: "Read"}, status: "error"},
				{call: ToolCall{ID: "toolu_a", Name: "Read"}, status: "ok"},
			},
			result: &TurnResult{},
			want: []wantSpan{
				{name: "execute_tool Read", parent: "turn", code: codes.Error, desc: "error", attrs: []attribute.KeyValue{AttrGenAIToolCallID.String("toolu_b")}},
				{name: "execute_tool Read", parent: "turn", code: codes.Unset, attrs: []attribute.KeyValue{AttrGenAIToolCallID.String("toolu_a")}},
				{name: "chat", parent: "turn", code: codes.Unset},
			},
			innerOK: 4,
		},
		{
			name: "agy without call ids correlates fifo and adopts canonical name",
			events: []sinkEvent{
				{start: true, call: ToolCall{Name: "call_mcp_tool", Command: "get_me"}},
				{start: true, call: ToolCall{Name: "run_command"}},
				{call: ToolCall{Name: "run_command"}, server: "native", status: "ok"},
				{call: ToolCall{Name: "get_me"}, server: "github", status: "ok"},
			},
			result: &TurnResult{},
			want: []wantSpan{
				{name: "execute_tool run_command", parent: "turn", code: codes.Unset, attrs: []attribute.KeyValue{AttrMCPServer.String("native")}},
				{name: "execute_tool get_me", parent: "turn", code: codes.Unset, attrs: []attribute.KeyValue{AttrGenAIToolName.String("get_me"), AttrMCPServer.String("github")}},
				{name: "chat", parent: "turn", code: codes.Unset},
			},
			innerOK: 4,
		},
		{
			name: "subagent nesting",
			events: []sinkEvent{
				{start: true, call: ToolCall{ID: "toolu_agent", Name: "Agent", Subagent: true}},
				{start: true, call: ToolCall{ID: "toolu_inner", ParentID: "toolu_agent", Name: "Grep"}},
				{call: ToolCall{ID: "toolu_inner", ParentID: "toolu_agent", Name: "Grep"}, status: "ok"},
				{call: ToolCall{ID: "toolu_agent", Name: "Agent", Subagent: true}, status: "ok"},
			},
			result: &TurnResult{},
			want: []wantSpan{
				{name: "execute_tool Grep", parent: "invoke_agent", code: codes.Unset},
				{name: "invoke_agent", parent: "execute_tool Agent", code: codes.Unset, attrs: []attribute.KeyValue{AttrGenAIOperationName.String("invoke_agent")}},
				{name: "execute_tool Agent", parent: "turn", code: codes.Unset},
				{name: "chat", parent: "turn", code: codes.Unset},
			},
			innerOK: 4,
		},
		{
			name: "daemon death closes open spans",
			events: []sinkEvent{
				{start: true, call: ToolCall{ID: "step-3", Name: "invoke_subagent", Subagent: true}},
				{start: true, call: ToolCall{ID: "step-4", ParentID: "step-3", Name: "run_command"}},
			},
			turnErr: errors.New("daemon closed while turn was in-flight"),
			want: []wantSpan{
				{name: "execute_tool run_command", parent: "invoke_agent", code: codes.Error, desc: "aborted"},
				{name: "invoke_agent", parent: "execute_tool invoke_subagent", code: codes.Error, desc: "aborted"},
				{name: "execute_tool invoke_subagent", parent: "turn", code: codes.Error, desc: "aborted"},
				{name: "chat", parent: "turn", code: codes.Error, desc: "daemon closed while turn was in-flight"},
			},
			innerOK: 2,
		},
		{
			name:    "failed turn carries consumed usage",
			turnErr: &TurnError{Err: errors.New("daemon closed while turn was in-flight"), Usage: AgyUsage{InputTokens: 900, OutputTokens: 40, CacheReadTokens: 5000, CacheCreationTokens: 850}},
			wantUse: AgyUsage{InputTokens: 900, OutputTokens: 40, CacheReadTokens: 5000, CacheCreationTokens: 850},
			want: []wantSpan{
				{name: "chat", parent: "turn", code: codes.Error, desc: "daemon closed while turn was in-flight", attrs: []attribute.KeyValue{AttrGenAIUsageInputTokens.Int(900), AttrGenAIUsageOutputTokens.Int(40), AttrGenAIUsageCacheRead.Int(5000), AttrGenAIUsageCacheCreation.Int(850)}},
			},
		},
		{
			name: "error status is redacted",
			events: []sinkEvent{
				{start: true, call: ToolCall{ID: "toolu_x", Name: "Bash"}},
				{call: ToolCall{ID: "toolu_x", Name: "Bash"}, status: "failed: token " + leakedPAT},
			},
			turnErr:  errors.New("push failed: https://x-access-token:" + leakedPAT + "@github.com/o/r"),
			want:     []wantSpan{{name: "execute_tool Bash", parent: "turn", code: codes.Error}, {name: "chat", parent: "turn", code: codes.Error}},
			innerOK:  2,
			noLeakOf: leakedPAT,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTracingTurnSinkCase(t, tc)
		})
	}
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name())
	}
	return out
}

func TestProviderForModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		model string
		want  string
	}{
		{name: "claude match is case-insensitive", model: "Claude-Opus-5-5", want: "anthropic"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ProviderForModel(tc.model); got != tc.want {
				t.Errorf("ProviderForModel(%q) = %q, want %q", tc.model, got, tc.want)
			}
		})
	}
}

// toolClaim is one ClaimToolSpan request and the call ID of the execute_tool span it must
// claim ("" = no span).
type toolClaim struct {
	server, tool, callID string
	want                 string
}

// fakeClaimTimer is a claimTimer whose start is announced on armed. It fires as soon as it
// starts when fire is set, otherwise never.
type fakeClaimTimer struct {
	fire  bool
	armed chan struct{}
}

func (f *fakeClaimTimer) start(time.Duration) (<-chan time.Time, func() bool) {
	c := make(chan time.Time, 1)
	if f.fire {
		c <- time.Time{}
	}
	f.armed <- struct{}{}
	return c, func() bool { return true }
}

func TestTracingTurnSink_ClaimToolSpan(t *testing.T) {
	t.Parallel()
	docketCreate := func(id, parent string) ToolCall {
		return ToolCall{ID: id, ParentID: parent, Name: "create", MCPServer: "docket", MCPTool: "create"}
	}
	agyGetMe := func(id string) ToolCall {
		return ToolCall{ID: id, Name: "call_mcp_tool", MCPServer: "github", MCPTool: "get_me"}
	}
	tests := []struct {
		name      string
		opened    []ToolCall  // started before the claims
		ended     *TurnResult // turn result delivered before the claims; nil = turn open
		claims    []toolClaim // run in order
		openLater *ToolCall   // started once the last claim is waiting; nil = the wait times out
	}{
		{
			name:   "claims by call id",
			opened: []ToolCall{docketCreate("toolu_a", ""), docketCreate("toolu_b", "")},
			claims: []toolClaim{{server: "docket", tool: "create", callID: "toolu_b", want: "toolu_b"}},
		},
		{
			name:   "unknown call id falls back to server and tool after the wait",
			opened: []ToolCall{{ID: "toolu_x", Name: "Bash"}, docketCreate("toolu_a", "")},
			claims: []toolClaim{{server: "docket", tool: "create", callID: "toolu_zz", want: "toolu_a"}},
		},
		{
			name:   "fifo by server and tool",
			opened: []ToolCall{agyGetMe("step-2"), {ID: "step-3", Name: "run_command"}, agyGetMe("step-4")},
			claims: []toolClaim{{server: "github", tool: "get_me", want: "step-2"}, {server: "github", tool: "get_me", want: "step-4"}},
		},
		{
			name:   "two identical calls get distinct spans and a third waits",
			opened: []ToolCall{agyGetMe("step-1"), agyGetMe("step-2")},
			claims: []toolClaim{{server: "github", tool: "get_me", want: "step-1"}, {server: "github", tool: "get_me", want: "step-2"}, {server: "github", tool: "get_me"}},
		},
		{
			name:   "claimed span is not claimed again by id",
			opened: []ToolCall{docketCreate("toolu_a", "")},
			claims: []toolClaim{{server: "docket", tool: "create", callID: "toolu_a", want: "toolu_a"}, {server: "docket", tool: "create", callID: "toolu_a"}},
		},
		{
			name:   "subagent tool span",
			opened: []ToolCall{{ID: "toolu_agent", Name: "Agent", Subagent: true}, docketCreate("toolu_inner", "toolu_agent")},
			claims: []toolClaim{{server: "docket", tool: "create", callID: "toolu_inner", want: "toolu_inner"}},
		},
		{
			name:      "call id waits for its own span instead of taking a parallel sibling's",
			opened:    []ToolCall{docketCreate("toolu_a", "")},
			claims:    []toolClaim{{server: "docket", tool: "create", callID: "toolu_b", want: "toolu_b"}},
			openLater: &ToolCall{ID: "toolu_b", Name: "create", MCPServer: "docket", MCPTool: "create"},
		},
		{
			name:      "span opened during the wait is claimed",
			opened:    []ToolCall{{ID: "toolu_x", Name: "Bash"}},
			claims:    []toolClaim{{server: "docket", tool: "create", callID: "toolu_late", want: "toolu_late"}},
			openLater: &ToolCall{ID: "toolu_late", Name: "create", MCPServer: "docket", MCPTool: "create"},
		},
		{
			name:      "non-matching span opened during the wait keeps waiting until the turn closes",
			claims:    []toolClaim{{server: "docket", tool: "create", callID: "toolu_late"}},
			openLater: &ToolCall{ID: "toolu_other", Name: "Bash"},
		},
		{
			name:   "no match returns false after the wait and leaks no goroutine",
			opened: []ToolCall{agyGetMe("step-1")},
			claims: []toolClaim{{server: "docket", tool: "create"}},
		},
		{
			name:   "closed turn returns false",
			opened: []ToolCall{docketCreate("toolu_a", "")},
			ended:  &TurnResult{},
			claims: []toolClaim{{server: "docket", tool: "create", callID: "toolu_a"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			turnCtx, turn := tp.Tracer("test").Start(context.Background(), "turn")
			timer := &fakeClaimTimer{fire: tc.openLater == nil, armed: make(chan struct{}, 1)}
			sink := NewTracingTurnSink(turnCtx, nil, WithToolSpanClaimWait(time.Hour))
			sink.newTimer = timer.start
			for _, call := range tc.opened {
				sink.OnToolCall(call)
			}
			if tc.ended != nil {
				sink.OnResult(tc.ended)
			}
			got := make([]trace.SpanContext, len(tc.claims))
			for i, c := range tc.claims[:len(tc.claims)-1] {
				got[i], _ = sink.ClaimToolSpan(context.Background(), c.server, c.tool, c.callID)
			}
			last := tc.claims[len(tc.claims)-1]
			done := make(chan trace.SpanContext, 1)
			go func() {
				sc, _ := sink.ClaimToolSpan(context.Background(), last.server, last.tool, last.callID)
				done <- sc
			}()
			if tc.openLater != nil {
				<-timer.armed
				sink.OnToolCall(*tc.openLater)
				if last.want == "" {
					sink.OnResult(&TurnResult{}) // the timer never fires; closing the turn ends the wait
				}
			}
			got[len(got)-1] = <-done
			sink.OnResult(&TurnResult{})
			turn.End()
			assertClaimedToolSpans(t, rec.Ended(), tc.claims, got)
		})
	}
}

func assertClaimedToolSpans(t *testing.T, ended []sdktrace.ReadOnlySpan, claims []toolClaim, got []trace.SpanContext) {
	t.Helper()
	byCallID := map[string]trace.SpanID{}
	for _, s := range ended {
		for _, kv := range s.Attributes() {
			if kv.Key == AttrGenAIToolCallID {
				byCallID[kv.Value.AsString()] = s.SpanContext().SpanID()
			}
		}
	}
	for i, c := range claims {
		switch {
		case c.want == "" && got[i].IsValid():
			t.Errorf("claim %d (%s/%s %q) = span %v, want none", i, c.server, c.tool, c.callID, got[i].SpanID())
		case c.want != "" && got[i].SpanID() != byCallID[c.want]:
			t.Errorf("claim %d (%s/%s %q) = span %v, want the %s span %v", i, c.server, c.tool, c.callID, got[i].SpanID(), c.want, byCallID[c.want])
		}
	}
}
