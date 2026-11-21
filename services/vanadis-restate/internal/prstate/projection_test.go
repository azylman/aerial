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
			snap:         ProjectionSnapshot{Repo: "aerial", PRNumber: 103, Branch: "feat/z", HeadSHA: "abc123", Title: "z", TargetID: "123", Status: "ci_failed", Generation: 1, Metadata: ProjectionMetadata{CIChecks: map[string]string{"lint": "failed"}}},
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
	s := NewPRState("aerial", 101)
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
	base := ProjectionSnapshot{Repo: "aerial", PRNumber: 101, Agent: "agent-beta", HeadSHA: "h1", Status: "open", Generation: 2, MetaEpoch: 1, Metadata: ProjectionMetadata{Jobs: []string{"a", "b"}}}
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

var ownedKeysLiteral = []string{
	"agent", "banner", "bot_id", "build_metadata", "ci_checks", "ci_fail_count",
	"deploy_failed_published", "deploy_success_pending", "deployed_jobs",
	"deployed_kustomizations", "job_states", "jobs", "kustomization_requests",
	"started_jobs", "vanadis_chunk_id", "widget_state",
}

// TestProjectionUpsertQuery pins the full-row upsert argument binding:
// column args, the owned-key replacement list, and the metadata blob.
func TestProjectionUpsertQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		snap     ProjectionSnapshot
		wantArgs []any
	}{
		{
			name:     "full snapshot",
			snap:     ProjectionSnapshot{Repo: "aerial", PRNumber: 101, Agent: "agent-beta", Branch: " feat ", HeadSHA: "h1", MergeSHA: "m1", TargetID: snowA, Title: "T", Status: "merged", Metadata: ProjectionMetadata{Jobs: []string{"eri", "coven"}, DeploySuccessPending: true}},
			wantArgs: []any{"azylman/aerial", 101, "agent-beta", "feat", "h1", "m1", snowA, "T", "merged", `{"deploy_success_pending":true,"jobs":["coven","eri"]}`, ownedKeysLiteral},
		},
		{
			name:     "blank agent and empty metadata",
			snap:     ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 5, Status: "open"},
			wantArgs: []any{"azylman/aerial", 5, "aerial", "", "", "", "", "", "open", `{}`, ownedKeysLiteral},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			query, args := ProjectionUpsertQuery(tc.snap)
			if query == "" {
				t.Fatal("expected non-empty query")
			}
			if !reflect.DeepEqual(args, tc.wantArgs) {
				t.Fatalf("args =\n%#v\nwant\n%#v", args, tc.wantArgs)
			}
		})
	}
}

// fakeRow is a hermetic pgx.Row that scans fixed string/byte values.
type fakeRow struct {
	vals []any
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			v, _ := r.vals[i].(string)
			*p = v
		case *[]byte:
			v, _ := r.vals[i].([]byte)
			*p = v
		}
	}
	return nil
}

// fakeProjectionDB is the hermetic ProjectionDB: Exec records the call and
// returns execErr; QueryRow returns row.
type fakeProjectionDB struct {
	mu      sync.Mutex
	execErr error
	row     fakeRow
	execs   int
	args    []any
}

func (f *fakeProjectionDB) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs++
	f.args = args
	return pgconn.NewCommandTag("INSERT 0 1"), f.execErr
}

func (f *fakeProjectionDB) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.args = args
	return f.row
}

// TestPgProjectionStoreUpsert pins the production writer: one Exec per
// projection with the upsert args, errors wrapped, nil database refused.
func TestPgProjectionStoreUpsert(t *testing.T) {
	t.Parallel()
	snap := ProjectionSnapshot{Repo: "aerial", PRNumber: 101, Status: "open"}
	tests := []struct {
		name      string
		nilDB     bool
		execErr   error
		wantErr   string
		wantExecs int
		wantRepo  any
	}{
		{name: "writes one row", wantExecs: 1, wantRepo: "azylman/aerial"},
		{name: "exec failure wraps", execErr: errFakeProjection, wantErr: "upsert pr_registry projection aerial#101: fake projection store error", wantExecs: 1, wantRepo: "azylman/aerial"},
		{name: "nil database refused", nilDB: true, wantErr: "projection database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := &fakeProjectionDB{execErr: tc.execErr}
			store := NewPgProjectionStore(db)
			if tc.nilDB {
				store = NewPgProjectionStore(nil)
			}
			err := store.UpsertProjection(context.Background(), snap)
			if gotErr := errString(err); gotErr != tc.wantErr {
				t.Fatalf("err = %q, want %q", gotErr, tc.wantErr)
			}
			if db.execs != tc.wantExecs {
				t.Fatalf("execs = %d, want %d", db.execs, tc.wantExecs)
			}
			if tc.wantExecs > 0 && db.args[0] != tc.wantRepo {
				t.Fatalf("repo arg = %v, want %v", db.args[0], tc.wantRepo)
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
