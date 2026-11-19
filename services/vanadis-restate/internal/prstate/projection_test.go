package prstate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
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
			if tc.wantRecorded > 0 && !reflect.DeepEqual(store.Recorded[0], tc.snap) {
				t.Fatalf("recorded snapshot mangled: got %+v, want %+v", store.Recorded[0], tc.snap)
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
		bypassTouch  bool
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
			bypassTouch:  true,
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
			if tc.bypassTouch {
				repo.TouchedPRKeys = append(repo.TouchedPRKeys, tc.keys...)
			} else {
				for _, k := range tc.keys {
					repo.TouchPRKey(k)
				}
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

// TestBuildProjection freezes the pure state-to-row mapping: status and
// ci_checks derive only from committed Restate state scoped to the current
// head SHA, repo normalizes, metadata carries only owned keys, and the
// journal key is generation-scoped.
func TestBuildProjection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		pr          int
		headSHA     string
		failedSHA   string
		check       string
		conflict    string
		terminal    TerminalState
		mergeSHA    string
		gen         uint64
		reopened    bool
		wantStatus  string
		wantRepo    string
		wantChecks  int
		wantKeyPrep string
	}{
		{name: "open normalizes bare repo", pr: 101, headSHA: "abc123", wantStatus: "open", wantRepo: "azylman/aerial", wantChecks: 0, wantKeyPrep: "azylman/aerial#101@abc123:gen0:epoch0:projection:"},
		{name: "failed ci maps ci_failed", pr: 101, headSHA: "abc123", failedSHA: "abc123", check: "lint", gen: 1, wantStatus: "ci_failed", wantRepo: "azylman/aerial", wantChecks: 1, wantKeyPrep: "azylman/aerial#101@abc123:gen1:epoch0:projection:"},
		{name: "roborev failure maps ci_failed", pr: 101, headSHA: "abc123", failedSHA: "abc123", check: "roborev", gen: 2, wantStatus: "ci_failed", wantRepo: "azylman/aerial", wantChecks: 1, wantKeyPrep: "azylman/aerial#101@abc123:gen2:epoch0:projection:"},
		{name: "conflict maps conflict", pr: 101, headSHA: "abc123", conflict: "abc123", gen: 1, wantStatus: "conflict", wantRepo: "azylman/aerial", wantChecks: 0, wantKeyPrep: "azylman/aerial#101@abc123:gen1:epoch0:projection:"},
		{name: "superseded failure projects open", pr: 101, headSHA: "def456", failedSHA: "abc123", check: "lint", gen: 1, wantStatus: "open", wantRepo: "azylman/aerial", wantChecks: 0, wantKeyPrep: "azylman/aerial#101@def456:gen1:epoch0:projection:"},
		{name: "superseded conflict projects open", pr: 101, headSHA: "def456", conflict: "abc123", wantStatus: "open", wantRepo: "azylman/aerial", wantChecks: 0, wantKeyPrep: "azylman/aerial#101@def456:gen0:epoch0:projection:"},
		{name: "reopened projects open on new epoch", pr: 101, headSHA: "abc123", terminal: TerminalMerged, reopened: true, wantStatus: "open", wantRepo: "azylman/aerial", wantChecks: 0, wantKeyPrep: "azylman/aerial#101@abc123:gen0:epoch1:projection:"},
		{name: "merged maps merged", pr: 101, headSHA: "abc123", terminal: TerminalMerged, mergeSHA: "def456", gen: 3, wantStatus: "merged", wantRepo: "azylman/aerial", wantChecks: 0, wantKeyPrep: "azylman/aerial#101@abc123:gen3:epoch0:projection:"},
		{name: "closed maps closed", pr: 101, headSHA: "abc123", terminal: TerminalClosed, wantStatus: "closed", wantRepo: "azylman/aerial", wantChecks: 0, wantKeyPrep: "azylman/aerial#101@abc123:gen0:epoch0:projection:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("aerial", tc.pr)
			s.RecordFailedCISHA(tc.failedSHA)
			s.RecordFailedCheckRunDetailed(tc.failedSHA, tc.check)
			s.MarkConflictPublished(tc.conflict)
			s.HeadSHA = tc.headSHA
			s.MarkTerminal(tc.terminal)
			if tc.reopened {
				s.Reopen()
			}
			for range tc.gen {
				s.RecordKustomizationRequests()
			}
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
			if got := ProjectionJournalKey(snap); !strings.HasPrefix(got, tc.wantKeyPrep) {
				t.Fatalf("JournalKey = %q, want prefix %q", got, tc.wantKeyPrep)
			}
		})
	}
	if IsOwnedMetadataKey("jobs") {
		t.Fatal("jobs must stay outside the owned partition")
	}
}

// TestBuildProjectionNilState freezes the nil-state degenerate case: a nil
// Restate state projects to an empty open snapshot instead of panicking.
func TestBuildProjectionNilState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		wantStatus  string
		wantKeyPrep string
	}{
		{name: "nil state maps open empty", wantStatus: "open", wantKeyPrep: "#0@:gen0:epoch0:projection:"},
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
			if got := ProjectionJournalKey(snap); !strings.HasPrefix(got, tc.wantKeyPrep) {
				t.Fatalf("JournalKey = %q, want prefix %q", got, tc.wantKeyPrep)
			}
		})
	}
}

// TestProjectionJournalKey freezes content addressing: every snapshot below
// shares repo/PR/headSHA/generation, yet each mints a distinct key, while an
// identical snapshot reuses the same key and dedupes. A generation-scoped
// key alone would collide all of these and suppress newer projections.
func TestProjectionJournalKey(t *testing.T) {
	t.Parallel()
	base := ProjectionSnapshot{Repo: "aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "t", TargetID: "123", Status: "open", Generation: 1, Metadata: map[string]any{"ci_checks": map[string]any{}}}
	statusDiff := base
	statusDiff.Status = "ci_failed"
	epochDiff := base
	epochDiff.MetaEpoch = 1
	mergeDiff := base
	mergeDiff.MergeSHA = "def456"
	mergeDiff.Status = "merged"
	checksDiff := base
	checksDiff.Status = "ci_failed"
	checksDiff.Metadata = map[string]any{"ci_checks": map[string]any{"lint": "failed"}}
	branchDiff := base
	branchDiff.Branch = "feat/y"
	genDiff := base
	genDiff.Generation = 2
	tests := []struct {
		name string
		snap ProjectionSnapshot
	}{
		{name: "base open snapshot", snap: base},
		{name: "status differs", snap: statusDiff},
		{name: "reopen epoch differs", snap: epochDiff},
		{name: "merge sha differs", snap: mergeDiff},
		{name: "checks differ", snap: checksDiff},
		{name: "branch differs", snap: branchDiff},
		{name: "generation differs", snap: genDiff},
	}
	var mu sync.Mutex
	seen := map[string]string{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			key := ProjectionJournalKey(tc.snap)
			mu.Lock()
			prev, dup := seen[key]
			if !dup {
				seen[key] = tc.name
			}
			mu.Unlock()
			if dup {
				t.Fatalf("key collision: %q already minted for %q", key, prev)
			}
			if again := ProjectionJournalKey(tc.snap); again != key {
				t.Fatalf("identical snapshot minted %q then %q, want stable key", key, again)
			}
		})
	}
	if ProjectionJournalKey(base) == "" {
		t.Fatal("journal key must not be empty")
	}
	emptyMeta := base
	emptyMeta.Metadata = map[string]any{}
	nilMeta := base
	nilMeta.Metadata = nil
	if ProjectionJournalKey(nilMeta) != ProjectionJournalKey(emptyMeta) {
		t.Fatal("nil metadata must key like an empty object, matching the SQL fallback")
	}
	badMeta := base
	badMeta.Metadata = map[string]any{"ci_checks": func() {}}
	if ProjectionJournalKey(badMeta) != ProjectionJournalKey(emptyMeta) {
		t.Fatal("unmarshalable metadata must fall back to the empty object key")
	}
}

// TestProjectionUpsertQueryBranches freezes the real pr_registry layout per
// branch: the insert half carries the owned ci_checks merge from the
// snapshot so a first-time projection lands it, and the conflict half merges
// the same key over existing metadata with the banner removed.
func TestProjectionUpsertQueryBranches(t *testing.T) {
	t.Parallel()
	snap := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", MergeSHA: "def456", Title: "t", TargetID: "123", Status: "merged", Generation: 1}
	tests := []struct {
		name       string
		part       string
		wantSubstr string
	}{
		{name: "insert defaults missing checks to empty", part: "insert", wantSubstr: "COALESCE($9::jsonb->'ci_checks', '{}'::jsonb)"},
		{name: "conflict target", part: "all", wantSubstr: "ON CONFLICT (repo, pr_number)"},
		{name: "update removes banner", part: "update", wantSubstr: "- 'banner'"},
		{name: "update coalesces null metadata", part: "update", wantSubstr: "COALESCE(metadata, '{}'::jsonb)"},
		{name: "update defaults missing checks to empty", part: "update", wantSubstr: "COALESCE($9::jsonb->'ci_checks', '{}'::jsonb)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			query, _ := ProjectionUpsertQuery(snap)
			halves := strings.SplitN(query, "ON CONFLICT", 2)
			if len(halves) != 2 {
				t.Fatalf("query missing ON CONFLICT branch:\n%s", query)
			}
			scope := query
			if tc.part == "insert" {
				scope = halves[0]
			}
			if tc.part == "update" {
				scope = halves[1]
			}
			if !strings.Contains(scope, tc.wantSubstr) {
				t.Fatalf("%s half missing %q:\n%s", tc.part, tc.wantSubstr, scope)
			}
		})
	}
}

// TestProjectionUpsertQueryArgs freezes the exact ordered argument binding:
// a swapped argument or mis-bound $9 fails the whole-slice comparison.
func TestProjectionUpsertQueryArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		snap ProjectionSnapshot
		want []any
	}{
		{
			name: "full snapshot binds in order",
			snap: ProjectionSnapshot{Repo: "aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", MergeSHA: "def456", Title: "t", TargetID: "123", Status: "merged", Generation: 1, Metadata: map[string]any{"ci_checks": map[string]any{"lint": "failed"}}},
			want: []any{"azylman/aerial", 101, "feat/x", "abc123", "def456", "123", "t", "merged", `{"ci_checks":{"lint":"failed"}}`},
		},
		{
			name: "nil metadata binds empty object",
			snap: ProjectionSnapshot{Repo: "aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "t", TargetID: "123", Status: "open", Generation: 1},
			want: []any{"azylman/aerial", 101, "feat/x", "abc123", "", "123", "t", "open", "{}"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, args := ProjectionUpsertQuery(tc.snap)
			if !reflect.DeepEqual(args, tc.want) {
				t.Fatalf("args = %#v, want %#v", args, tc.want)
			}
		})
	}
}

// TestProjectionUpsertQueryGuardsBadMetadata freezes the marshal-failure
// fallback: an unmarshalable metadata value still binds '{}' instead of
// propagating garbage to $9.
func TestProjectionUpsertQueryGuardsBadMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		want any
	}{
		{name: "unmarshalable metadata binds empty object", want: "{}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			snap := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "t", TargetID: "123", Status: "open", Metadata: map[string]any{"ci_checks": func() {}}}
			_, args := ProjectionUpsertQuery(snap)
			if len(args) != 9 {
				t.Fatalf("args = %d, want 9", len(args))
			}
			if !reflect.DeepEqual(args[8], tc.want) {
				t.Fatalf("args[8] = %#v, want %#v", args[8], tc.want)
			}
		})
	}
}
