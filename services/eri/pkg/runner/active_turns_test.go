package runner

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func testTurnSpan(n byte) trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1, n}, SpanID: trace.SpanID{2, n}, TraceFlags: trace.FlagsSampled})
}

// fixedToolSpans is a ToolSpanSource whose every claim resolves to span 9.
type fixedToolSpans struct{}

func (fixedToolSpans) ClaimToolSpan(context.Context, string, string, string) (trace.SpanContext, bool) {
	return testTurnSpan(9), true
}

type activeTurnRegistryCase struct {
	name         string
	set          []ActiveTurn // span n = index+1
	toolSpans    []string     // daemons given a fixedToolSpans source after set
	reset        []ActiveTurn // set again after toolSpans; span n = len(set)+index+1
	clear        []string
	get          string
	wantSpan     byte // 0 = not found
	wantToolSpan byte // span ToolSpan(get) claims; 0 = none
	target       string
	wantSole     string
	conversation string
	wantConvID   string
	wantTarget   string
}

func TestActiveTurnRegistry(t *testing.T) {
	t.Parallel()
	d1 := ActiveTurn{DaemonID: "p1/discord-1", Target: "discord", ConversationID: "conv-1"}
	d2 := ActiveTurn{DaemonID: "p1/discord-2", Target: "discord", ConversationID: "conv-2"}
	e1 := ActiveTurn{DaemonID: "p2/ephemeral:worker-0", Target: "ephemeral"}
	tests := []activeTurnRegistryCase{
		{name: "set then get", set: []ActiveTurn{d1}, get: d1.DaemonID, wantSpan: 1, target: "discord", wantSole: d1.DaemonID, conversation: "conv-1", wantConvID: d1.DaemonID, wantTarget: "discord"},
		{name: "cleared turn is gone", set: []ActiveTurn{d1}, clear: []string{d1.DaemonID}, get: d1.DaemonID, target: "discord", conversation: "conv-1"},
		{name: "two turns in a target are ambiguous", set: []ActiveTurn{d1, d2}, get: d2.DaemonID, wantSpan: 2, target: "discord", conversation: "conv-2", wantConvID: d2.DaemonID, wantTarget: "discord"},
		{name: "other targets do not count", set: []ActiveTurn{d1, e1}, get: e1.DaemonID, wantSpan: 2, target: "discord", wantSole: d1.DaemonID},
		{name: "re-set replaces the turn span", set: []ActiveTurn{d1, d1}, get: d1.DaemonID, wantSpan: 2, target: "discord", wantSole: d1.DaemonID},
		{name: "clearing one leaves the other sole", set: []ActiveTurn{d1, d2}, clear: []string{d1.DaemonID}, get: d1.DaemonID, target: "discord", wantSole: d2.DaemonID},
		{name: "turn without daemon id is ignored", set: []ActiveTurn{{Target: "discord"}}, get: "", target: "discord"},
		{name: "unknown daemon", get: "p9/x", target: "voice"},
		{name: "tool span claims through the attached source", set: []ActiveTurn{d1}, toolSpans: []string{d1.DaemonID}, get: d1.DaemonID, wantSpan: 1, wantToolSpan: 9, target: "discord", wantSole: d1.DaemonID},
		{name: "tool spans need a registered turn", toolSpans: []string{d1.DaemonID}, get: d1.DaemonID, target: "discord"},
		{name: "clear drops the tool spans", set: []ActiveTurn{d1}, toolSpans: []string{d1.DaemonID}, clear: []string{d1.DaemonID}, get: d1.DaemonID, target: "discord"},
		{name: "re-set drops the previous attempt's tool spans", set: []ActiveTurn{d1}, toolSpans: []string{d1.DaemonID}, reset: []ActiveTurn{d1}, get: d1.DaemonID, wantSpan: 2, target: "discord", wantSole: d1.DaemonID},
		{name: "setting another daemon keeps the source", set: []ActiveTurn{d1}, toolSpans: []string{d1.DaemonID}, reset: []ActiveTurn{e1}, get: d1.DaemonID, wantSpan: 1, wantToolSpan: 9, target: "discord", wantSole: d1.DaemonID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewActiveTurnRegistry()
			for i, turn := range tc.set {
				r.Set(turn, testTurnSpan(byte(i+1)))
			}
			for _, id := range tc.toolSpans {
				r.SetToolSpans(id, fixedToolSpans{})
			}
			for i, turn := range tc.reset {
				r.Set(turn, testTurnSpan(byte(len(tc.set)+i+1)))
			}
			for _, id := range tc.clear {
				r.Clear(id)
			}
			assertActiveTurnRegistry(t, r, tc)
		})
	}
}

func assertActiveTurnRegistry(t *testing.T, r *ActiveTurnRegistry, tc activeTurnRegistryCase) {
	t.Helper()
	sc, ok := r.Get(tc.get)
	if ok != (tc.wantSpan != 0) || (ok && !sc.Equal(testTurnSpan(tc.wantSpan))) {
		t.Errorf("Get(%q) = %v, %v; want span %d", tc.get, sc.SpanID(), ok, tc.wantSpan)
	}
	tool, ok := r.ToolSpan(context.Background(), tc.get, "github", "get_me", "toolu_1")
	if ok != (tc.wantToolSpan != 0) || (ok && !tool.Equal(testTurnSpan(tc.wantToolSpan))) {
		t.Errorf("ToolSpan(%q) = %v, %v; want span %d", tc.get, tool.SpanID(), ok, tc.wantToolSpan)
	}
	sole, ok := r.SoleDaemonInTarget(tc.target)
	if sole != tc.wantSole || ok != (tc.wantSole != "") {
		t.Errorf("SoleDaemonInTarget(%q) = %q, %v; want %q", tc.target, sole, ok, tc.wantSole)
	}
	id, target, ok := r.DaemonByConversation(tc.conversation)
	if id != tc.wantConvID || target != tc.wantTarget || ok != (tc.wantConvID != "") {
		t.Errorf("DaemonByConversation(%q) = %q, %q, %v; want %q, %q", tc.conversation, id, target, ok, tc.wantConvID, tc.wantTarget)
	}
	if tc.wantConvID != "" {
		if got, ok := r.Target(tc.wantConvID); !ok || got != tc.wantTarget {
			t.Errorf("Target(%q) = %q, %v; want %q", tc.wantConvID, got, ok, tc.wantTarget)
		}
	}
}
