package pubsub

import (
	"testing"
)

func TestSubjectHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, got, want string
	}{
		{"coven extract normal", SubjectCovenExtractCoalesced("thread-123"), "aerial.coalesced.coven_extract.thread-123"},
		{"coven extract empty", SubjectCovenExtractCoalesced("   "), "aerial.coalesced.coven_extract.unknown"},
		{"coven extract sanitize wildcards", SubjectCovenExtractCoalesced("thread*>1"), "aerial.coalesced.coven_extract.thread-1"},
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
