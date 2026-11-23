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
		{"transcripts steps normal", SubjectTranscriptsSteps("sess-1"), "aerial.transcripts.steps.sess-1"},
		{"transcripts steps empty", SubjectTranscriptsSteps("   "), "aerial.transcripts.steps.unknown"},
		{"transcripts steps sanitize wildcards", SubjectTranscriptsSteps("sess*>1"), "aerial.transcripts.steps.sess-1"},
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
