package runner

import (
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func testTurnSpan(n byte) trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1, n}, SpanID: trace.SpanID{2, n}, TraceFlags: trace.FlagsSampled})
}

func TestActiveTurnRegistry(t *testing.T) {
	t.Parallel()
	d1 := ActiveTurn{DaemonID: "p1/discord-1", Target: "discord", ConversationID: "conv-1"}
	d2 := ActiveTurn{DaemonID: "p1/discord-2", Target: "discord", ConversationID: "conv-2"}
	e1 := ActiveTurn{DaemonID: "p2/ephemeral:worker-0", Target: "ephemeral"}
	tests := []struct {
		name         string
		set          []ActiveTurn // span n = index+1
		clear        []string
		get          string
		wantSpan     byte // 0 = not found
		target       string
		wantSole     byte
		conversation string
		wantConvID   string
		wantTarget   string
	}{
		{name: "set then get", set: []ActiveTurn{d1}, get: d1.DaemonID, wantSpan: 1, target: "discord", wantSole: 1, conversation: "conv-1", wantConvID: d1.DaemonID, wantTarget: "discord"},
		{name: "cleared turn is gone", set: []ActiveTurn{d1}, clear: []string{d1.DaemonID}, get: d1.DaemonID, target: "discord", conversation: "conv-1"},
		{name: "two turns in a target are ambiguous", set: []ActiveTurn{d1, d2}, get: d2.DaemonID, wantSpan: 2, target: "discord", conversation: "conv-2", wantConvID: d2.DaemonID, wantTarget: "discord"},
		{name: "other targets do not count", set: []ActiveTurn{d1, e1}, get: e1.DaemonID, wantSpan: 2, target: "discord", wantSole: 1},
		{name: "re-set replaces the turn span", set: []ActiveTurn{d1, d1}, get: d1.DaemonID, wantSpan: 2, target: "discord", wantSole: 2},
		{name: "clearing one leaves the other sole", set: []ActiveTurn{d1, d2}, clear: []string{d1.DaemonID}, get: d1.DaemonID, target: "discord", wantSole: 2},
		{name: "turn without daemon id is ignored", set: []ActiveTurn{{Target: "discord"}}, get: "", target: "discord"},
		{name: "unknown daemon", get: "p9/x", target: "voice"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewActiveTurnRegistry()
			for i, turn := range tc.set {
				r.Set(turn, testTurnSpan(byte(i+1)))
			}
			for _, id := range tc.clear {
				r.Clear(id)
			}
			sc, ok := r.Get(tc.get)
			if ok != (tc.wantSpan != 0) || (ok && !sc.Equal(testTurnSpan(tc.wantSpan))) {
				t.Errorf("Get(%q) = %v, %v; want span %d", tc.get, sc.SpanID(), ok, tc.wantSpan)
			}
			sole, ok := r.SoleTurnInTarget(tc.target)
			if ok != (tc.wantSole != 0) || (ok && !sole.Equal(testTurnSpan(tc.wantSole))) {
				t.Errorf("SoleTurnInTarget(%q) = %v, %v; want span %d", tc.target, sole.SpanID(), ok, tc.wantSole)
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
		})
	}
}
