package prstate

import (
	"context"
	"errors"
	"strings"
	"testing"
)

var errFakeProjection = errors.New("fake projection store error")

// TestProjectRegistry freezes the journaled projection send: valid snapshots
// project exactly once, invalid snapshots never reach the store, and store
// failures propagate without recording a write. The state transition itself
// is never gated on this result; callers keep the committed state.
func TestProjectRegistry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		snap         ProjectionSnapshot
		storeErr     error
		nilStore     bool
		cancelCtx    bool
		wantErr      bool
		wantRecorded int
	}{
		{
			name:         "open pr projects",
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "x", TargetID: "123", Status: "open", Generation: 2, Metadata: map[string]any{}},
			wantErr:      false,
			wantRecorded: 1,
		},
		{
			name:         "merged pr projects merge sha",
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 102, Branch: "feat/y", HeadSHA: "abc123", MergeSHA: "def456", Title: "y", TargetID: "123", Status: "merged", Generation: 1, Metadata: map[string]any{}},
			wantErr:      false,
			wantRecorded: 1,
		},
		{
			name:         "ci failed pr projects",
			snap:         ProjectionSnapshot{Repo: "aerial", PRNumber: 103, Branch: "feat/z", HeadSHA: "abc123", Title: "z", TargetID: "123", Status: "ci_failed", Generation: 1, Metadata: map[string]any{"ci_checks": map[string]any{"lint": "failed"}}},
			wantErr:      false,
			wantRecorded: 1,
		},
		{
			name:         "empty repo never reaches store",
			snap:         ProjectionSnapshot{Repo: "", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "x", TargetID: "123", Status: "open"},
			wantErr:      true,
			wantRecorded: 0,
		},
		{
			name:         "zero pr never reaches store",
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 0, Branch: "feat/x", HeadSHA: "abc123", Title: "x", TargetID: "123", Status: "open"},
			wantErr:      true,
			wantRecorded: 0,
		},
		{
			name:         "empty status never reaches store",
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "x", TargetID: "123", Status: ""},
			wantErr:      true,
			wantRecorded: 0,
		},
		{
			name:         "nil store errors without panic",
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "x", TargetID: "123", Status: "open"},
			nilStore:     true,
			wantErr:      true,
			wantRecorded: 0,
		},
		{
			name:         "store failure propagates unrecorded",
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "x", TargetID: "123", Status: "open"},
			storeErr:     errFakeProjection,
			wantErr:      true,
			wantRecorded: 0,
		},
		{
			name:         "cancelled context errors before store",
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "x", TargetID: "123", Status: "open"},
			cancelCtx:    true,
			wantErr:      true,
			wantRecorded: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &FakeProjectionStore{Err: tc.storeErr}
			ctx := context.Background()
			if tc.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			var err error
			if tc.nilStore {
				err = ProjectRegistry(ctx, nil, tc.snap)
			} else {
				err = ProjectRegistry(ctx, store, tc.snap)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("ProjectRegistry err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := store.RecordedCount(); got != tc.wantRecorded {
				t.Fatalf("recorded = %d, want %d", got, tc.wantRecorded)
			}
		})
	}
}

// TestRebuildProjection freezes rebuild-from-index: keys enumerate only the
// RepoObject touched-PR index (never the projection being rebuilt), missing
// keys skip, and a store failure aborts with the committed state intact.
func TestRebuildProjection(t *testing.T) {
	t.Parallel()
	openSnap := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "x", TargetID: "123", Status: "open", Generation: 1, Metadata: map[string]any{}}
	mergedSnap := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 102, Branch: "feat/y", HeadSHA: "abc123", MergeSHA: "def456", Title: "y", TargetID: "123", Status: "merged", Generation: 1, Metadata: map[string]any{}}
	tests := []struct {
		name         string
		keys         []string
		snaps        map[string]ProjectionSnapshot
		storeErr     error
		broken       string
		wantErr      bool
		wantRecorded int
	}{
		{
			name:         "empty index projects nothing",
			keys:         nil,
			snaps:        map[string]ProjectionSnapshot{},
			wantErr:      false,
			wantRecorded: 0,
		},
		{
			name:         "single key projects once",
			keys:         []string{"azylman/aerial#101"},
			snaps:        map[string]ProjectionSnapshot{"azylman/aerial#101": openSnap},
			wantErr:      false,
			wantRecorded: 1,
		},
		{
			name:         "two keys project twice",
			keys:         []string{"azylman/aerial#101", "azylman/aerial#102"},
			snaps:        map[string]ProjectionSnapshot{"azylman/aerial#101": openSnap, "azylman/aerial#102": mergedSnap},
			wantErr:      false,
			wantRecorded: 2,
		},
		{
			name:         "missing key skips without error",
			keys:         []string{"azylman/aerial#101", "azylman/aerial#999"},
			snaps:        map[string]ProjectionSnapshot{"azylman/aerial#101": openSnap},
			wantErr:      false,
			wantRecorded: 1,
		},
		{
			name:         "duplicate index key projects once",
			keys:         []string{"azylman/aerial#101", "azylman/aerial#101"},
			snaps:        map[string]ProjectionSnapshot{"azylman/aerial#101": openSnap},
			wantErr:      false,
			wantRecorded: 1,
		},
		{
			name:         "store failure aborts rebuild",
			keys:         []string{"azylman/aerial#101"},
			snaps:        map[string]ProjectionSnapshot{"azylman/aerial#101": openSnap},
			storeErr:     errFakeProjection,
			wantErr:      true,
			wantRecorded: 0,
		},
		{
			name:         "nil store errors",
			keys:         []string{"azylman/aerial#101"},
			snaps:        map[string]ProjectionSnapshot{"azylman/aerial#101": openSnap},
			broken:       "store",
			wantErr:      true,
			wantRecorded: 0,
		},
		{
			name:         "nil repo errors",
			keys:         []string{"azylman/aerial#101"},
			snaps:        map[string]ProjectionSnapshot{"azylman/aerial#101": openSnap},
			broken:       "repo",
			wantErr:      true,
			wantRecorded: 0,
		},
		{
			name:         "nil loader errors",
			keys:         []string{"azylman/aerial#101"},
			snaps:        map[string]ProjectionSnapshot{"azylman/aerial#101": openSnap},
			broken:       "loader",
			wantErr:      true,
			wantRecorded: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := NewRepoState("aerial")
			for _, k := range tc.keys {
				repo.TouchPRKey(k)
			}
			store := &FakeProjectionStore{Err: tc.storeErr}
			var err error
			switch tc.broken {
			case "store":
				err = RebuildProjection(context.Background(), nil, repo, MapSnapshotLoader{Snaps: tc.snaps})
			case "repo":
				err = RebuildProjection(context.Background(), store, nil, MapSnapshotLoader{Snaps: tc.snaps})
			case "loader":
				err = RebuildProjection(context.Background(), store, repo, nil)
			default:
				err = RebuildProjection(context.Background(), store, repo, MapSnapshotLoader{Snaps: tc.snaps})
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("RebuildProjection err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := store.RecordedCount(); got != tc.wantRecorded {
				t.Fatalf("recorded = %d, want %d", got, tc.wantRecorded)
			}
		})
	}
}

// TestBuildProjection freezes the pure state-to-row mapping: status derives
// only from committed Restate state, repo normalizes, metadata carries only
// owned keys, and the journal key is generation-scoped.
func TestBuildProjection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		pr         int
		headSHA    string
		failedSHA  string
		check      string
		conflict   string
		terminal   TerminalState
		mergeSHA   string
		wantStatus string
		wantRepo   string
		wantChecks int
		wantGenKey string
	}{
		{name: "open normalizes bare repo", pr: 101, headSHA: "abc123", wantStatus: "open", wantRepo: "azylman/aerial", wantChecks: 0, wantGenKey: "azylman/aerial#101@abc123:gen0:projection"},
		{name: "failed ci maps ci_failed", pr: 101, headSHA: "abc123", failedSHA: "abc123", check: "lint", wantStatus: "ci_failed", wantRepo: "azylman/aerial", wantChecks: 1, wantGenKey: "azylman/aerial#101@abc123:gen0:projection"},
		{name: "roborev failure maps ci_failed", pr: 101, headSHA: "abc123", failedSHA: "abc123", check: "roborev", wantStatus: "ci_failed", wantRepo: "azylman/aerial", wantChecks: 1, wantGenKey: "azylman/aerial#101@abc123:gen0:projection"},
		{name: "conflict maps conflict", pr: 101, headSHA: "abc123", conflict: "abc123", wantStatus: "conflict", wantRepo: "azylman/aerial", wantChecks: 0, wantGenKey: "azylman/aerial#101@abc123:gen0:projection"},
		{name: "merged maps merged", pr: 101, headSHA: "abc123", terminal: TerminalMerged, mergeSHA: "def456", wantStatus: "merged", wantRepo: "azylman/aerial", wantChecks: 0, wantGenKey: "azylman/aerial#101@abc123:gen0:projection"},
		{name: "closed maps closed", pr: 101, headSHA: "abc123", terminal: TerminalClosed, wantStatus: "closed", wantRepo: "azylman/aerial", wantChecks: 0, wantGenKey: "azylman/aerial#101@abc123:gen0:projection"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("aerial", tc.pr)
			s.HeadSHA = tc.headSHA
			s.RecordFailedCISHA(tc.failedSHA)
			s.RecordFailedCheckRunDetailed(tc.failedSHA, tc.check)
			s.MarkConflictPublished(tc.conflict)
			s.MarkTerminal(tc.terminal)
			snap := BuildProjection(s, "feat/x", "title", "123", tc.mergeSHA)
			if snap.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", snap.Status, tc.wantStatus)
			}
			if snap.Repo != tc.wantRepo {
				t.Fatalf("Repo = %q, want %q", snap.Repo, tc.wantRepo)
			}
			checks, _ := snap.Metadata["ci_checks"].(map[string]any)
			if got := len(checks); got != tc.wantChecks {
				t.Fatalf("ci_checks entries = %d, want %d", got, tc.wantChecks)
			}
			for k := range snap.Metadata {
				if !IsOwnedMetadataKey(k) {
					t.Fatalf("metadata key %q not in owned partition", k)
				}
			}
			if IsOwnedMetadataKey("jobs") {
				t.Fatalf("jobs must stay outside the owned partition")
			}
			if got := ProjectionJournalKey(snap); got != tc.wantGenKey {
				t.Fatalf("JournalKey = %q, want %q", got, tc.wantGenKey)
			}
		})
	}
}

// TestBuildProjectionNilState freezes the nil-state degenerate case: a nil
// Restate state projects to an empty open snapshot instead of panicking.
func TestBuildProjectionNilState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		wantStatus string
		wantGenKey string
	}{
		{name: "nil state maps open empty", wantStatus: "open", wantGenKey: "#0@:gen0:projection"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			snap := BuildProjection(nil, "feat/x", "title", "123", "")
			if snap.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", snap.Status, tc.wantStatus)
			}
			if got := ProjectionStatus(nil); got != tc.wantStatus {
				t.Fatalf("nil ProjectionStatus = %q, want %q", got, tc.wantStatus)
			}
			if got := ProjectionJournalKey(snap); got != tc.wantGenKey {
				t.Fatalf("JournalKey = %q, want %q", got, tc.wantGenKey)
			}
		})
	}
}

// TestProjectionUpsertQuery freezes the real pr_registry layout: the upsert
// targets repo/pr_number, merges only owned metadata keys, always clears the
// banner, and never invents columns.
func TestProjectionUpsertQuery(t *testing.T) {
	t.Parallel()
	snap := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", MergeSHA: "def456", Title: "t", TargetID: "123", Status: "merged", Generation: 1, Metadata: map[string]any{"ci_checks": map[string]any{"lint": "failed"}}}
	tests := []struct {
		name       string
		wantSubstr string
	}{
		{name: "targets pr_registry", wantSubstr: "pr_registry"},
		{name: "upserts on repo pr_number", wantSubstr: "ON CONFLICT (repo, pr_number)"},
		{name: "writes head_sha column", wantSubstr: "head_sha"},
		{name: "writes merge_sha column", wantSubstr: "merge_sha"},
		{name: "writes status column", wantSubstr: "status"},
		{name: "merges owned ci_checks key", wantSubstr: "ci_checks"},
		{name: "clears banner like registry writes", wantSubstr: "- 'banner'"},
		{name: "never whole blob overwrite", wantSubstr: "COALESCE(metadata, '{}'::jsonb)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			query, args := ProjectionUpsertQuery(snap)
			if !strings.Contains(query, tc.wantSubstr) {
				t.Fatalf("query missing %q:\n%s", tc.wantSubstr, query)
			}
			if len(args) == 0 {
				t.Fatalf("expected bound args, got none")
			}
		})
	}
}
