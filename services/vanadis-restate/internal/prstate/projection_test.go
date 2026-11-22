package prstate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/x", HeadSHA: "abc123", Title: "x", TargetID: "123", Status: "open", Generation: 2, Metadata: ProjectionMetadata{}},
			wantErr:      false,
			wantRecorded: 1,
		},
		{
			name:         "merged pr projects merge sha",
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 102, Branch: "feat/y", HeadSHA: "abc123", MergeSHA: "def456", Title: "y", TargetID: "123", Status: "merged", Generation: 1, Metadata: ProjectionMetadata{}},
			wantErr:      false,
			wantRecorded: 1,
		},
		{
			name:         "ci failed pr projects",
			snap:         ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 103, Branch: "feat/z", HeadSHA: "abc123", Title: "z", TargetID: "123", Status: "ci_failed", Generation: 1, Metadata: ProjectionMetadata{CIChecks: map[string]string{"lint": "failed"}}},
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

// FakeProjectionStore is the hermetic ProjectionStore: it records snapshots
// on success and fails closed with Err without recording.
type FakeProjectionStore struct {
	mu       sync.Mutex
	Err      error
	Recorded []ProjectionSnapshot
}

func (f *FakeProjectionStore) UpsertProjection(_ context.Context, snap ProjectionSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	f.Recorded = append(f.Recorded, snap)
	return nil
}

func (f *FakeProjectionStore) RecordedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Recorded)
}

// fullState is a PRState carrying every registry row field.
func fullState() *PRState {
	s := NewPRState("azylman/aerial", 101)
	s.Agent, s.BotID = "agent-beta", "agent-beta"
	s.Branch, s.HeadSHA, s.MergeSHA, s.Title, s.TargetID = " feat/x ", "h1", "m1", "T", snowA
	s.Status, s.Terminal, s.GateGeneration, s.ReopenEpoch = RegistryStatusDeploying, TerminalMerged, 2, 1
	s.Jobs, s.StartedJobs = []string{"eri", " coven", "eri"}, []string{"eri"}
	s.DeployedJobs, s.DeployedKustomizations = []string{}, []string{"core"}
	s.JobStates, s.CIChecks = map[string]string{"eri": "verifying_health"}, map[string]string{}
	s.KustomizationRequests = map[string]KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "t0"}}
	s.BuildMetadata = &BuildMetadata{Version: 1, Kind: "final", RunNumber: 7}
	s.Banner, s.CIFailCount, s.DeploySuccessPending = "b", 2, true
	s.VanadisChunkID, s.WidgetState = "chunk-1", "w"
	return s
}

// TestBuildProjection pins the full state-to-row mapping: every column and
// metadata key, repo normalization, set cleaning, empty collections absent,
// and the default agent column for an unowned PR.
func TestBuildProjection(t *testing.T) {
	t.Parallel()
	unowned := NewPRState("azylman/aerial", 7)
	unowned.HeadSHA = "h7"
	tests := []struct {
		name  string
		state *PRState
		want  ProjectionSnapshot
	}{
		{name: "nil state projects defaults", state: nil, want: ProjectionSnapshot{Agent: "aerial", Status: "open"}},
		{name: "unowned pr projects default agent and open", state: unowned, want: ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 7, Agent: "aerial", HeadSHA: "h7", Status: "open"}},
		{
			name: "full row projects every column and key", state: fullState(),
			want: ProjectionSnapshot{
				Repo: "azylman/aerial", PRNumber: 101, Agent: "agent-beta", Branch: "feat/x", HeadSHA: "h1", MergeSHA: "m1", Title: "T", TargetID: snowA,
				Status: "deploying", Generation: 2, MetaEpoch: 1,
				Metadata: ProjectionMetadata{
					Agent: "agent-beta", BotID: "agent-beta", Banner: "b", BuildMetadata: &BuildMetadata{Version: 1, Kind: "final", RunNumber: 7},
					CIFailCount: 2, DeploySuccessPending: true, DeployedKustomizations: []string{"core"},
					JobStates: map[string]string{"eri": "verifying_health"}, Jobs: []string{"coven", "eri"},
					KustomizationRequests: map[string]KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "t0"}},
					StartedJobs:           []string{"eri"}, VanadisChunkID: "chunk-1", WidgetState: "w",
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := BuildProjection(tc.state); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("BuildProjection =\n%+v\nwant\n%+v", got, tc.want)
			}
		})
	}
}

// TestProjectionJournalKey pins content scoping: identical content shares a
// key regardless of set order, any field change mints a new key, and the
// readable prefix names agent, PR, head, generation, and epoch.
func TestProjectionJournalKey(t *testing.T) {
	t.Parallel()
	base := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Agent: "agent-beta", HeadSHA: "h1", Status: "open", Generation: 2, MetaEpoch: 1, Metadata: ProjectionMetadata{Jobs: []string{"a", "b"}}}
	reordered := base
	reordered.Metadata = ProjectionMetadata{Jobs: []string{"b", "a", "a"}}
	retitled := base
	retitled.Title = "new"
	rechecked := base
	rechecked.Metadata = ProjectionMetadata{Jobs: []string{"a", "b"}, CIChecks: map[string]string{"ci": "passed"}}
	blankAgent := base
	blankAgent.Agent = " "
	tests := []struct {
		name       string
		a, b       ProjectionSnapshot
		wantEqual  bool
		wantPrefix string
	}{
		{name: "identical snapshots share a key", a: base, b: base, wantEqual: true, wantPrefix: "projection:agent-beta:azylman/aerial:101@h1:gen2:epoch1:"},
		{name: "set order and duplicates never change the key", a: base, b: reordered, wantEqual: true, wantPrefix: "projection:agent-beta:azylman/aerial:101@h1:gen2:epoch1:"},
		{name: "title change mints a new key", a: base, b: retitled, wantEqual: false, wantPrefix: "projection:agent-beta:azylman/aerial:101@h1:gen2:epoch1:"},
		{name: "metadata change mints a new key", a: base, b: rechecked, wantEqual: false, wantPrefix: "projection:agent-beta:azylman/aerial:101@h1:gen2:epoch1:"},
		{name: "blank agent keys as default agent", a: blankAgent, b: blankAgent, wantEqual: true, wantPrefix: "projection:aerial:azylman/aerial:101@h1:gen2:epoch1:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ka, kb := ProjectionJournalKey(tc.a), ProjectionJournalKey(tc.b)
			if (ka == kb) != tc.wantEqual {
				t.Fatalf("keys equal = %v, want %v (%s vs %s)", ka == kb, tc.wantEqual, ka, kb)
			}
			if !strings.HasPrefix(ka, tc.wantPrefix) || len(ka) != len(tc.wantPrefix)+16 {
				t.Fatalf("key %q, want prefix %q plus 16 hex digits", ka, tc.wantPrefix)
			}
		})
	}
}

// TestProjectionUpsertQuery pins the full-row upsert argument binding:
// row column args and typed scalar args, and that the statement never
// names the legacy metadata JSON column.
func TestProjectionUpsertQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		snap     ProjectionSnapshot
		wantArgs []any
	}{
		{
			name: "full snapshot",
			snap: ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Agent: "agent-beta", Branch: " feat ", HeadSHA: "h1", MergeSHA: "m1", TargetID: snowA, Title: "T", Status: "merged", Metadata: ProjectionMetadata{Jobs: []string{"eri", "coven"}, DeploySuccessPending: true}},
			wantArgs: append([]any{
				"azylman/aerial", 101, "agent-beta", "feat", "h1", "m1", snowA, "T", "merged",
				"", "", 0, false, true, "", "",
			}, make([]any, 18)...),
		},
		{
			name:     "blank agent and empty metadata",
			snap:     ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 5, Status: "open"},
			wantArgs: append([]any{"azylman/aerial", 5, "aerial", "", "", "", "", "", "open", "", "", 0, false, false, "", ""}, make([]any, 18)...),
		},
		{
			name: "typed scalars and a pre-final build snapshot",
			snap: ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 6, Agent: "miorine", Status: "ci_failed", Metadata: ProjectionMetadata{
				VanadisChunkID: "c1", BotID: "miorine", CIFailCount: 3, DeployFailedPublished: true, Banner: "b", WidgetState: "w",
				BuildMetadata: &BuildMetadata{Version: 1, Kind: "pre", Repository: "azylman/aerial", WorkflowPath: "ci.yml", RunID: 9, RunNumber: 8, RunAttempt: 2, Event: "pull_request", SourceSHA: "s1", ExecutionSHA: "e1"},
			}},
			wantArgs: []any{
				"azylman/aerial", 6, "miorine", "", "", "", "", "", "ci_failed",
				"c1", "miorine", 3, true, false, "b", "w",
				1, "pre", "azylman/aerial", "ci.yml", int64(9), int64(8), 2, "pull_request", "s1", "e1", (*int)(nil),
				(*string)(nil), (*string)(nil), (*string)(nil), (*bool)(nil), (*int)(nil), []string(nil), (*string)(nil),
			},
		},
		{
			name: "final build snapshot projects omitempty fields",
			snap: ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 7, Status: "open", Metadata: ProjectionMetadata{
				BuildMetadata: &BuildMetadata{Version: 1, Kind: "final", PRNumber: new(7), Service: "eri", ImageName: "aerial-eri", State: "pushed", Pushed: true, SourceAttempt: 1, ImageRefs: []string{"ghcr.io/x:1"}, Digest: "sha256:d"},
			}},
			wantArgs: []any{
				"azylman/aerial", 7, "aerial", "", "", "", "", "", "open",
				"", "", 0, false, false, "", "",
				1, "final", "", "", int64(0), int64(0), 0, "", "", "", new(7),
				new("eri"), new("aerial-eri"), new("pushed"), new(true), new(1),
				[]string{"ghcr.io/x:1"},
				new("sha256:d"),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			query, args := ProjectionUpsertQuery(tc.snap)
			if query == "" || strings.Contains(query, "metadata") {
				t.Fatalf("query must be non-empty and never touch metadata: %s", query)
			}
			if !reflect.DeepEqual(args, tc.wantArgs) {
				t.Fatalf("args =\n%#v\nwant\n%#v", args, tc.wantArgs)
			}
		})
	}
}

// fakeProjectionTx is the hermetic pgx.Tx: Exec records each statement's
// args and fails on the failAt-th call (1-based); Commit and Rollback record
// how the transaction ended. Unused pgx.Tx methods panic via the nil embed.
type fakeProjectionTx struct {
	pgx.Tx

	db *fakeProjectionDB
}

func (t *fakeProjectionTx) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	t.db.execs = append(t.db.execs, args)
	if len(t.db.execs) == t.db.failAt {
		return pgconn.CommandTag{}, errFakeProjection
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (t *fakeProjectionTx) Commit(context.Context) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if t.db.ended != "" {
		return pgx.ErrTxClosed
	}
	t.db.ended = "commit"
	return nil
}

func (t *fakeProjectionTx) Rollback(context.Context) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if t.db.ended != "" {
		return pgx.ErrTxClosed
	}
	t.db.ended = "rollback"
	return nil
}

// fakeProjectionDB is the hermetic ProjectionDB: Begin fails with beginErr
// or opens a fakeProjectionTx that records into this struct.
type fakeProjectionDB struct {
	mu       sync.Mutex
	beginErr error
	failAt   int
	execs    [][]any
	ended    string
}

func (f *fakeProjectionDB) Begin(context.Context) (pgx.Tx, error) {
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	return &fakeProjectionTx{db: f}, nil
}

// TestPgProjectionStoreUpsert pins the production writer: every projection
// statement runs in one transaction that commits on success and rolls back
// on the first failure, errors wrapped, nil database refused.
func TestPgProjectionStoreUpsert(t *testing.T) {
	t.Parallel()
	snap := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Status: "open"}
	tests := []struct {
		name      string
		nilDB     bool
		beginErr  error
		failAt    int
		wantErr   string
		wantExecs int
		wantEnded string
	}{
		{name: "writes row and child tables then commits", wantExecs: 10, wantEnded: "commit"},
		{name: "child failure rolls back the row", failAt: 4, wantErr: "upsert pr_registry projection azylman/aerial#101: fake projection store error", wantExecs: 4, wantEnded: "rollback"},
		{name: "begin failure wraps", beginErr: errFakeProjection, wantErr: "upsert pr_registry projection azylman/aerial#101: fake projection store error"},
		{name: "nil database refused", nilDB: true, wantErr: "projection database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := &fakeProjectionDB{beginErr: tc.beginErr, failAt: tc.failAt}
			store := NewPgProjectionStore(db)
			if tc.nilDB {
				store = NewPgProjectionStore(nil)
			}
			err := store.UpsertProjection(context.Background(), snap)
			if gotErr := errString(err); gotErr != tc.wantErr {
				t.Fatalf("err = %q, want %q", gotErr, tc.wantErr)
			}
			if len(db.execs) != tc.wantExecs || db.ended != tc.wantEnded {
				t.Fatalf("execs = %d ended = %q, want %d %q", len(db.execs), db.ended, tc.wantExecs, tc.wantEnded)
			}
			if tc.wantExecs > 0 && db.execs[0][0] != "azylman/aerial" {
				t.Fatalf("repo arg = %v, want azylman/aerial", db.execs[0][0])
			}
		})
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestProjectionStatements pins the child-table sync of one projection: the
// row upsert first, then delete+upsert per child table with hand-derived
// keys (sorted unions for jobs and kustomizations, 1-based first positions
// for build entries, row head for checks, unparseable requested_at as
// empty), and one upsert per build result. Empty sets bind empty arrays.
func TestProjectionStatements(t *testing.T) {
	t.Parallel()
	full := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 9, Status: "deploying", Metadata: ProjectionMetadata{
		CIChecks:               map[string]string{"test": "passed", "lint": "failed"},
		Jobs:                   []string{"eri"},
		StartedJobs:            []string{"eri", "coven"},
		DeployedJobs:           []string{"coven"},
		JobStates:              map[string]string{"haro": "verifying_health"},
		KustomizationRequests:  map[string]KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "2026-11-21T22:56:58.123456789Z"}, "apps": {Namespace: "flux-system", RequestedAt: "t0"}},
		DeployedKustomizations: []string{"infra"},
		BuildMetadata: &BuildMetadata{
			Kind:     "final",
			Selected: []BuildSelection{{Service: "eri", ImageName: "aerial-eri"}, {Service: "coven", ImageName: "aerial-coven"}, {Service: "eri", ImageName: "aerial-eri"}},
			Results: []BuildResult{
				{Service: "eri", ImageName: "aerial-eri", State: "pushed", Pushed: true, SourceAttempt: 1, ImageRefs: []string{"r1", "r2"}, Digest: "d1"},
				{Service: "coven", ImageName: "aerial-coven", State: "skipped"},
				{Service: "eri", ImageName: "aerial-eri", State: "failed"},
			},
		},
	}}
	tests := []struct {
		name string
		snap ProjectionSnapshot
		want [][]any
	}{
		{
			name: "empty metadata clears every child table",
			snap: ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 5, Status: "open"},
			want: [][]any{
				{"azylman/aerial", 5, []string{}},
				{"azylman/aerial", 5, []string{}, []string{}},
				{"azylman/aerial", 5, []string{}},
				{"azylman/aerial", 5, []string{}, []bool{}, []bool{}, []bool{}, []string{}},
				{"azylman/aerial", 5, []string{}},
				{"azylman/aerial", 5, []string{}, []string{}, []string{}, []bool{}},
				{"azylman/aerial", 5, []string{}, []string{}},
				{"azylman/aerial", 5, []string{}, []string{}, []int{}},
				{"azylman/aerial", 5, []string{}, []string{}},
			},
		},
		{
			name: "full metadata syncs every child table",
			snap: full,
			want: [][]any{
				{"azylman/aerial", 9, []string{"lint", "test"}},
				{"azylman/aerial", 9, []string{"lint", "test"}, []string{"failed", "passed"}},
				{"azylman/aerial", 9, []string{"coven", "eri", "haro"}},
				{"azylman/aerial", 9, []string{"coven", "eri", "haro"}, []bool{false, true, false}, []bool{true, true, false}, []bool{true, false, false}, []string{"", "", "verifying_health"}},
				{"azylman/aerial", 9, []string{"apps", "core", "infra"}},
				{"azylman/aerial", 9, []string{"apps", "core", "infra"}, []string{"flux-system", "flux-system", ""}, []string{"", "2026-11-21T22:56:58.123456789Z", ""}, []bool{false, false, true}},
				{"azylman/aerial", 9, []string{"eri", "coven"}, []string{"aerial-eri", "aerial-coven"}},
				{"azylman/aerial", 9, []string{"eri", "coven"}, []string{"aerial-eri", "aerial-coven"}, []int{1, 2}},
				{"azylman/aerial", 9, []string{"eri", "coven"}, []string{"aerial-eri", "aerial-coven"}},
				{"azylman/aerial", 9, "eri", "aerial-eri", 1, "pushed", true, 1, []string{"r1", "r2"}, "d1"},
				{"azylman/aerial", 9, "coven", "aerial-coven", 2, "skipped", false, 0, []string{}, ""},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stmts := ProjectionStatements(tc.snap)
			_, upsertArgs := ProjectionUpsertQuery(tc.snap)
			if len(stmts) != len(tc.want)+1 || !reflect.DeepEqual(stmts[0].Args, upsertArgs) {
				t.Fatalf("got %d statements, want row upsert plus %d", len(stmts), len(tc.want))
			}
			for i, want := range tc.want {
				if stmts[i+1].SQL == "" || !reflect.DeepEqual(stmts[i+1].Args, want) {
					t.Fatalf("statement %d args =\n%#v\nwant\n%#v", i+1, stmts[i+1].Args, want)
				}
			}
		})
	}
}
