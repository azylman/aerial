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

func TestTracingTurnSink(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		events   []sinkEvent
		result   *TurnResult
		turnErr  error
		want     []wantSpan
		innerOK  int // tool callbacks the inner sink must still receive
		noLeakOf string
	}{
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
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			turnCtx, turn := tp.Tracer("test").Start(context.Background(), "turn")

			inner := newMockTurnSink()
			sink := NewTracingTurnSink(turnCtx, inner)
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

			ended := rec.Ended()
			names := map[string]string{}
			for _, s := range ended {
				names[s.SpanContext().SpanID().String()] = s.Name()
			}
			got := ended[:len(ended)-1] // drop the root "turn" span
			if len(got) != len(tc.want) {
				t.Fatalf("ended %d spans, want %d: %v", len(got), len(tc.want), spanNames(got))
			}
			for i, w := range tc.want {
				s := got[i]
				if s.Name() != w.name || names[s.Parent().SpanID().String()] != w.parent {
					t.Errorf("span %d = %q (parent %q), want %q (parent %q)", i, s.Name(), names[s.Parent().SpanID().String()], w.name, w.parent)
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
				if tc.noLeakOf != "" {
					var dump strings.Builder
					dump.WriteString(s.Status().Description)
					for _, kv := range s.Attributes() {
						dump.WriteString(" " + kv.Value.String())
					}
					if strings.Contains(dump.String(), tc.noLeakOf) {
						t.Errorf("span %q leaked %q: %s", w.name, tc.noLeakOf, dump.String())
					}
				}
			}
			inner.mu.Lock()
			forwarded := len(inner.calls) + len(inner.completions)
			passthrough := inner.started && inner.thinking == 1 && len(inner.stepStarts) == 1 && len(inner.activatedSkills) == 1 && len(inner.deltas) == 1
			inner.mu.Unlock()
			if forwarded != tc.innerOK || !passthrough {
				t.Errorf("inner sink got %d tool callbacks (passthrough=%v), want %d", forwarded, passthrough, tc.innerOK)
			}
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
