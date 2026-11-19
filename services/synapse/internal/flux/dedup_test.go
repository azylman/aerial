package flux

import (
	"testing"
)

func TestDeployDedupTracker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recordRepo string
		recordJob  string
		recordSHA  string
		recordPR   int
		checkRepo  string
		checkJob   string
		checkSHA   string
		checkPR    int
		wantBefore bool
		wantAfter  bool
	}{
		{
			name:       "pr number dedup",
			recordRepo: "azylman/aerial",
			recordPR:   42,
			checkRepo:  "azylman/aerial",
			checkPR:    42,
			wantBefore: false,
			wantAfter:  true,
		},
		{
			name:       "sha and job dedup",
			recordRepo: "azylman/aerial",
			recordJob:  "deploy",
			recordSHA:  "abcdef123456",
			checkRepo:  "azylman/aerial",
			checkJob:   "deploy",
			checkSHA:   "abcdef123456",
			wantBefore: false,
			wantAfter:  true,
		},
		{
			name:       "different pr not dispatched",
			recordRepo: "azylman/aerial",
			recordPR:   10,
			checkRepo:  "azylman/aerial",
			checkPR:    11,
			wantBefore: false,
			wantAfter:  false,
		},
		{
			name:       "empty coordinates not dispatched",
			recordRepo: "azylman/aerial",
			checkRepo:  "azylman/aerial",
			wantBefore: false,
			wantAfter:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tracker := &DeployDedupTracker{}
			if got := tracker.WasDispatched(tc.checkRepo, tc.checkJob, tc.checkSHA, tc.checkPR); got != tc.wantBefore {
				t.Fatalf("before record: WasDispatched() = %v, want %v", got, tc.wantBefore)
			}
			tracker.RecordDispatched(tc.recordRepo, tc.recordJob, tc.recordSHA, tc.recordPR)
			if got := tracker.WasDispatched(tc.checkRepo, tc.checkJob, tc.checkSHA, tc.checkPR); got != tc.wantAfter {
				t.Fatalf("after record: WasDispatched() = %v, want %v", got, tc.wantAfter)
			}
		})
	}
}
