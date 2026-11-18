package transcript

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
)

type syncSessionTranscriptTestCase struct {
	name           string
	nilStore       bool
	sessionID      string
	hasTranscript  bool
	transcriptData string
	secondPass     bool
	seedMessages   []db.Message
	failGetKnown   bool
	wantErr        bool
	errContains    string
	wantSteps      int
	wantSkipped    int
	wantSynced     int
}

func runSyncSessionTranscriptTest(t *testing.T, tc syncSessionTranscriptTestCase) {
	t.Helper()
	ctx := context.Background()
	tempDir := t.TempDir()

	var store db.TranscriptStore
	if !tc.nilStore {
		fakeStore := dbtest.NewFakeStore()
		for _, m := range tc.seedMessages {
			_ = fakeStore.InsertMessage(ctx, m)
		}
		if tc.failGetKnown {
			store = newMockTranscriptStore(fakeStore, false, true, false, false)
		} else {
			store = fakeStore
		}
	}

	if tc.hasTranscript && tc.sessionID != "" {
		writeTranscriptFile(t, tempDir, tc.sessionID, tc.transcriptData, time.Now().Add(-5*time.Minute))
	} else if tc.hasTranscript && tc.sessionID == "" {
		writeTranscriptFile(t, tempDir, "fallback-sess", tc.transcriptData, time.Now().Add(-5*time.Minute))
	}

	opts := SyncOptions{
		BrainDir:      tempDir,
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute,
		HeadLimit:     1000,
		TailLimit:     1000,
	}

	if tc.secondPass && store != nil {
		_, firstErr := SyncSessionTranscript(ctx, store, tempDir, tc.sessionID, opts)
		if firstErr != nil {
			t.Fatalf("first pass SyncSessionTranscript failed: %v", firstErr)
		}
	}

	stats, err := SyncSessionTranscript(ctx, store, tempDir, tc.sessionID, opts)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", tc.errContains)
		}
		if !strings.Contains(err.Error(), tc.errContains) {
			t.Fatalf("expected error containing %q, got: %v", tc.errContains, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stats.StepsInserted != tc.wantSteps {
		t.Errorf("stats.StepsInserted = %d, want %d", stats.StepsInserted, tc.wantSteps)
	}
	if stats.SkippedUnchanged != tc.wantSkipped {
		t.Errorf("stats.SkippedUnchanged = %d, want %d", stats.SkippedUnchanged, tc.wantSkipped)
	}
	if stats.Synced != tc.wantSynced {
		t.Errorf("stats.Synced = %d, want %d", stats.Synced, tc.wantSynced)
	}
}

func TestSyncSessionTranscript_TableDriven(t *testing.T) {
	t.Parallel()
	const sampleJSONL = "{\"step_index\":0,\"source\":\"USER_EXPLICIT\",\"type\":\"USER_INPUT\",\"content\":\"hello\"}\n{\"step_index\":1,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"world\"}\n"

	tests := []syncSessionTranscriptTestCase{
		{
			name:        "nil store returns error",
			nilStore:    true,
			sessionID:   "sess-nil",
			wantErr:     true,
			errContains: "transcript store cannot be nil",
		},
		{
			name:           "empty session id falls back to SyncTranscripts",
			sessionID:      "",
			hasTranscript:  true,
			transcriptData: sampleJSONL,
			wantSteps:      2,
			wantSynced:     1,
		},
		{
			name:          "nonexistent transcript file returns zero stats and nil error",
			sessionID:     "sess-missing",
			hasTranscript: false,
			wantSteps:     0,
			wantSynced:    0,
		},
		{
			name:           "valid session transcript indexes steps into database",
			sessionID:      "sess-valid",
			hasTranscript:  true,
			transcriptData: sampleJSONL,
			wantSteps:      2,
			wantSynced:     1,
		},
		{
			name:           "untracked session skipped when turn provenance exists",
			sessionID:      "sess-untracked",
			hasTranscript:  true,
			transcriptData: sampleJSONL,
			seedMessages:   []db.Message{{ID: "m1", ConversationID: "sess-tracked"}},
			wantSteps:      0,
			wantSynced:     0,
		},
		{
			name:           "error loading known turn session ids propagated",
			sessionID:      "sess-err-known",
			hasTranscript:  true,
			transcriptData: sampleJSONL,
			failGetKnown:   true,
			wantErr:        true,
			errContains:    "load known turn session ids",
		},
		{
			name:           "second pass on unchanged transcript skips sync",
			sessionID:      "sess-unchanged",
			hasTranscript:  true,
			transcriptData: sampleJSONL,
			secondPass:     true,
			wantSteps:      0,
			wantSkipped:    1,
			wantSynced:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSyncSessionTranscriptTest(t, tc)
		})
	}
}
