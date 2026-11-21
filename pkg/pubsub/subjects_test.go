package pubsub

import (
	"testing"
)

func TestEffectiveAgent(t *testing.T) {
	t.Parallel()

	indexedCases := []struct {
		name string
		ev   TranscriptsIndexedEvent
		want string
	}{
		{name: "agent set", ev: TranscriptsIndexedEvent{Agent: "agent-beta", Stack: "aerial"}, want: "agent-beta"},
		{name: "stack fallback", ev: TranscriptsIndexedEvent{Stack: "agent-alpha"}, want: "agent-alpha"},
		{name: "both empty", ev: TranscriptsIndexedEvent{}, want: "aerial"},
	}

	for _, tc := range indexedCases {
		t.Run("indexed_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.ev.EffectiveAgent(); got != tc.want {
				t.Errorf("TranscriptsIndexedEvent.EffectiveAgent() = %q, want %q", got, tc.want)
			}
		})
	}

	covenCases := []struct {
		name string
		args CovenExtractJobArgs
		want string
	}{
		{name: "agent set", args: CovenExtractJobArgs{Agent: "agent-beta", Stack: "aerial"}, want: "agent-beta"},
		{name: "stack fallback", args: CovenExtractJobArgs{Stack: "agent-alpha"}, want: "agent-alpha"},
		{name: "both empty", args: CovenExtractJobArgs{}, want: "aerial"},
	}

	for _, tc := range covenCases {
		t.Run("coven_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.args.EffectiveAgent(); got != tc.want {
				t.Errorf("CovenExtractJobArgs.EffectiveAgent() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSubjectHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, got, want string
	}{
		{"coven extract normal", SubjectCovenExtractCoalesced("thread-123"), "aerial.coalesced.coven_extract.thread-123"},
		{"coven extract empty", SubjectCovenExtractCoalesced("   "), "aerial.coalesced.coven_extract.unknown"},
		{"coven extract sanitize wildcards", SubjectCovenExtractCoalesced("thread*>1"), "aerial.coalesced.coven_extract.thread-1"},
		{"coven extract for agent normal", SubjectCovenExtractCoalescedForAgent("agent-alpha", "thread-123"), "aerial.coalesced.coven_extract.agent-alpha.thread-123"},
		{"coven extract for agent wildcard", SubjectCovenExtractCoalescedForAgent("agent-alpha", ">"), "aerial.coalesced.coven_extract.agent-alpha.>"},
		{"coven extract for agent empty agent", SubjectCovenExtractCoalescedForAgent("", "thread-123"), "aerial.coalesced.coven_extract.aerial.thread-123"},
		{"build widget sync normal", SubjectBuildWidgetSyncCoalesced("build-123"), "aerial.coalesced.widget_sync.build.build-123"},
		{"build widget sync empty", SubjectBuildWidgetSyncCoalesced(""), "aerial.coalesced.widget_sync.build.unknown"},
		{"transcripts steps normal", SubjectTranscriptsSteps("sess-1"), "aerial.transcripts.steps.sess-1"},
		{"transcripts steps empty", SubjectTranscriptsSteps("   "), "aerial.transcripts.steps.unknown"},
		{"transcripts steps sanitize wildcards", SubjectTranscriptsSteps("sess*>1"), "aerial.transcripts.steps.sess-1"},
		{"pr widget sync with slash", SubjectPRWidgetSyncCoalesced("azylman/aerial", 123), "aerial.coalesced.widget_sync.pr.azylman.aerial.123"},
		{"pr widget sync empty repo", SubjectPRWidgetSyncCoalesced("  ", 456), "aerial.coalesced.widget_sync.pr.unknown.456"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
			}
		})
	}
}
