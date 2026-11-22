package store

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
)

func TestRepoMatchCandidates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		owner string
		repo  string
		want  []string
	}{
		{name: "empty repo", owner: "example-org", repo: "", want: nil},
		{name: "blank repo", owner: "example-org", repo: "  ", want: nil},
		{name: "bare repo adds qualified", owner: "example-org", repo: "aerial", want: []string{"aerial", "example-org/aerial"}},
		{name: "bare repo trimmed", owner: "example-org", repo: " aerial ", want: []string{"aerial", "example-org/aerial"}},
		{name: "owner repo adds bare", owner: "example-org", repo: "example-org/aerial", want: []string{"example-org/aerial", "aerial"}},
		{name: "other owner as given", owner: "example-org", repo: "other/x", want: []string{"other/x"}},
		{name: "other configured owner", owner: "acme", repo: "aerial", want: []string{"aerial", "acme/aerial"}},
		{name: "configured owner trimmed", owner: " acme/ ", repo: "acme/aerial", want: []string{"acme/aerial", "aerial"}},
		{name: "owner prefix with nested path", owner: "example-org", repo: "example-org/a/b", want: []string{"example-org/a/b"}},
		{name: "owner prefix with empty name", owner: "example-org", repo: "example-org/", want: []string{"example-org/"}},
		{name: "empty owner bare repo", owner: "", repo: "aerial", want: []string{"aerial"}},
		{name: "empty owner qualified repo", owner: "", repo: "example-org/aerial", want: []string{"example-org/aerial"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := RepoMatchCandidates(tc.owner, tc.repo)
			if !slices.Equal(got, tc.want) {
				t.Errorf("RepoMatchCandidates(%q, %q) = %v, want %v", tc.owner, tc.repo, got, tc.want)
			}
		})
	}
}

func TestCapChunkEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, effort, want string
	}{
		{name: "invalid clamps to medium", effort: "invalid", want: "medium"},
		{name: "empty clamps to medium", effort: "", want: "medium"},
		{name: "valid kept", effort: "low", want: "low"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CapChunkEffort(tc.effort); got != tc.want {
				t.Errorf("CapChunkEffort(%q) = %q, want %q", tc.effort, got, tc.want)
			}
		})
	}
}

type stubScanRow struct {
	vals []any
	err  error
}

func (r stubScanRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(r.vals) > len(dest) {
		return errors.New("stub row holds more values than destinations")
	}
	for i, v := range r.vals {
		if v == nil {
			continue
		}
		ptr, val := reflect.ValueOf(dest[i]), reflect.ValueOf(v)
		if ptr.Kind() != reflect.Pointer || ptr.IsNil() || ptr.Elem().Type() != val.Type() {
			return errors.New("stub row value type mismatch")
		}
		ptr.Elem().Set(val)
	}
	return nil
}

func TestScanBuild(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	row := stubScanRow{vals: []any{"b1", "aerial", "thread-1", 2, "Title", "Objective", "ctx", "running", "msg-1", true, now, now}}
	got, err := ScanBuild(row)
	if err != nil {
		t.Fatalf("ScanBuild unexpected error: %v", err)
	}
	if got.ID != "b1" || got.Agent != "aerial" || got.ThreadID != "thread-1" || got.Tier != 2 || !got.Quiet {
		t.Errorf("ScanBuild decoded %+v", got)
	}
	if _, err := ScanBuild(stubScanRow{err: errors.New("scan boom")}); err == nil {
		t.Error("ScanBuild expected propagated scan error, got nil")
	}
}

func TestApplyRawChunkCIChecks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		raw        string
		wantHead   string
		wantMerge  string
		wantRemed  string
		wantChecks map[string]string
	}{
		{name: "empty", raw: ""},
		{name: "null", raw: "null"},
		{name: "empty object", raw: "{}"},
		{name: "invalid json", raw: "{oops"},
		{name: "full", raw: `{"_head_sha":"h","_merge_sha":"m","_remediated_sha":"r","lint":"passed"}`, wantHead: "h", wantMerge: "m", wantRemed: "r", wantChecks: map[string]string{"lint": "passed"}},
		{name: "internal only", raw: `{"_head_sha":"h"}`, wantHead: "h"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var c model.VanadisChunk
			ApplyRawChunkCIChecks(&c, []byte(tc.raw))
			if c.HeadSHA != tc.wantHead || c.MergeSHA != tc.wantMerge || c.RemediatedSHA != tc.wantRemed {
				t.Errorf("ApplyRawChunkCIChecks(%q) SHAs = %q/%q/%q", tc.raw, c.HeadSHA, c.MergeSHA, c.RemediatedSHA)
			}
			if len(c.CIChecks) != len(tc.wantChecks) {
				t.Errorf("ApplyRawChunkCIChecks(%q) checks = %v", tc.raw, c.CIChecks)
			}
			for k, v := range tc.wantChecks {
				if c.CIChecks[k] != v {
					t.Errorf("ApplyRawChunkCIChecks(%q) checks[%q] = %q, want %q", tc.raw, k, c.CIChecks[k], v)
				}
			}
		})
	}
}

type stubChunkRows struct {
	pgx.Rows

	chunks  []model.VanadisChunk
	checks  []string
	states  []string
	idx     int
	scanErr error
	rowsErr error
}

func (*stubChunkRows) Close() {}

func (r *stubChunkRows) Err() error { return r.rowsErr }

func (r *stubChunkRows) Next() bool {
	r.idx++
	return r.idx <= len(r.chunks)
}

func (r *stubChunkRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	c := r.chunks[r.idx-1]
	checks, state := []byte("{}"), []byte("{}")
	if r.idx-1 < len(r.checks) {
		checks = []byte(r.checks[r.idx-1])
	}
	if r.idx-1 < len(r.states) {
		state = []byte(r.states[r.idx-1])
	}
	vals := []any{c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, c.DependsOn, c.CIFailCount, checks, state, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt}
	if len(vals) != len(dest) {
		return errors.New("stub chunk row arity mismatch")
	}
	for i, v := range vals {
		ptr, val := reflect.ValueOf(dest[i]), reflect.ValueOf(v)
		if ptr.Kind() != reflect.Pointer || ptr.IsNil() || ptr.Elem().Type() != val.Type() {
			return errors.New("stub chunk row value type mismatch")
		}
		ptr.Elem().Set(val)
	}
	return nil
}

func TestScanChunks(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	withTimes := model.VanadisChunk{ID: "c1", BuildID: "b1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "T1", Instructions: "I1", Effort: "high", Status: "pending", StartedAt: now, CompletedAt: now, CreatedAt: now, UpdatedAt: now}
	bare := model.VanadisChunk{ID: "c2", BuildID: "b1", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Title: "T2", Instructions: "I2", Status: "pending", CreatedAt: now, UpdatedAt: now}
	empty := []string{}
	// Effort is capped to medium, nil slices decode as empty non-nil, the
	// checks/rollout JSON is decoded and a NULL started_at stays zero.
	decodedTimes := model.VanadisChunk{
		ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Title: "T1", Instructions: "I1", Effort: "medium", Status: "pending",
		TargetFiles: empty, TargetSymbols: empty, AcceptanceCriteria: empty, DependsOn: empty,
		HeadSHA: "h", CIChecks: map[string]string{"lint": "passed"}, RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}},
		StartedAt: now, CompletedAt: now, CreatedAt: now, UpdatedAt: now,
	}
	decodedBare := model.VanadisChunk{
		ID: "c2", BuildID: "b1", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Title: "T2", Instructions: "I2", Effort: "medium", Status: "pending",
		TargetFiles: empty, TargetSymbols: empty, AcceptanceCriteria: empty, DependsOn: empty, CreatedAt: now, UpdatedAt: now,
	}
	tests := []struct {
		name    string
		rows    stubChunkRows
		want    []model.VanadisChunk
		wantErr string
	}{
		{
			name: "mixed rows decode",
			rows: stubChunkRows{
				chunks: []model.VanadisChunk{withTimes, bare},
				checks: []string{`{"_head_sha":"h","lint":"passed"}`, "null"},
				states: []string{`{"expected_jobs":["eri"]}`, "{}"},
			},
			want: []model.VanadisChunk{decodedTimes, decodedBare},
		},
		{name: "scan error", rows: stubChunkRows{chunks: []model.VanadisChunk{withTimes}, scanErr: errors.New("scan boom")}, wantErr: "failed to scan vanadis chunk: scan boom"},
		{name: "rows error", rows: stubChunkRows{chunks: []model.VanadisChunk{withTimes}, rowsErr: errors.New("rows boom")}, wantErr: "error iterating vanadis chunks: rows boom"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ScanChunks(&tc.rows)
			if gotErr := errString(err); gotErr != tc.wantErr {
				t.Fatalf("ScanChunks err = %q, want %q", gotErr, tc.wantErr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ScanChunks = %#v\nwant %#v", got, tc.want)
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

func sweepChunk(id string, idx int, mode, repo, status string, completedAt time.Time) model.VanadisChunk {
	return model.VanadisChunk{ID: id, BuildID: "b1", ChunkIndex: idx, Mode: mode, Repo: repo, Status: status, CompletedAt: completedAt}
}

func TestEarlierDeploymentSweepIDs(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	later, later2 := base.Add(time.Hour), base.Add(2*time.Hour)
	pr := func(id string, idx int, status string, at time.Time) model.VanadisChunk {
		return sweepChunk(id, idx, "pr", "aerial", status, at)
	}
	dep := func(idx int, at time.Time) model.VanadisChunk { return pr("deploying", idx, "deploying", at) }
	repair := model.VanadisChunk{ID: "repair", BuildID: "b1", Mode: "action", Repo: "aerial", Status: "merged", PRNumber: 7, CompletedAt: base}
	plain := model.VanadisChunk{ID: "plain", BuildID: "b1", ChunkIndex: 1, Mode: "action", Repo: "aerial", Status: "merged", CompletedAt: base}
	tests := []struct {
		name   string
		chunks []model.VanadisChunk
		want   []string
	}{
		{"earlier merged same repo completes", mutateChunks(pr("earlier", 0, "merged", base), dep(1, later)), []string{"earlier"}},
		{
			"earlier deploying and synced same repo complete",
			mutateChunks(pr("early-deploying", 0, "deploying", base), pr("early-synced", 1, "synced", base), dep(2, later)),
			[]string{"early-deploying", "early-synced"},
		},
		{"earlier merged action repair is swept but plain action is not", mutateChunks(repair, plain, dep(2, later)), []string{"repair"}},
		{"later merged same repo stays untouched", mutateChunks(pr("earlier", 0, "merged", base), dep(1, later), pr("later", 2, "merged", later2)), []string{"earlier"}},
		{
			"earlier merged different repo stays untouched",
			mutateChunks(sweepChunk("other-repo", 0, "pr", "mirrormere", "merged", base), pr("earlier", 1, "merged", base), dep(2, later)),
			[]string{"earlier"},
		},
		{
			"earlier terminal chunks stay untouched",
			mutateChunks(pr("failed", 0, "failed", base), pr("cancelled", 1, "cancelled", base), pr("completed", 2, "completed", base), pr("earlier", 3, "merged", base), dep(4, later)),
			[]string{"earlier"},
		},
		{
			"earlier never merged chunks stay untouched",
			mutateChunks(pr("opened", 0, "pr_opened", time.Time{}), pr("coding", 1, "coding", time.Time{}), pr("earlier", 2, "merged", base), dep(3, later)),
			[]string{"earlier"},
		},
		{"merge stamp tie breaks by lower chunk index", mutateChunks(pr("tie-early", 0, "merged", base), dep(1, base), pr("tie-late", 2, "merged", base)), []string{"tie-early"}},
		{"owner qualified repo spelling matches", mutateChunks(pr("earlier", 0, "merged", base), sweepChunk("deploying", 1, "pr", "example-org/aerial", "deploying", later)), []string{"earlier"}},
		{"unknown deploying stamp skips earlier merged candidate", mutateChunks(pr("earlier", 0, "merged", base), dep(1, time.Time{})), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := EarlierDeploymentSweepIDs("example-org", tc.chunks, "deploying"); !slices.Equal(got, tc.want) {
				t.Errorf("EarlierDeploymentSweepIDs = %q, want %q", got, tc.want)
			}
		})
	}
}

func xbuildChunk(id, buildID string, idx int, mode, repo, status string, pr int, completedAt time.Time, jobs []string, checks map[string]string) model.VanadisChunk {
	var rs *pubsub.PRRolloutState
	if jobs != nil {
		rs = &pubsub.PRRolloutState{ExpectedJobs: jobs}
	}
	return model.VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Mode: mode, Repo: repo, Status: status, PRNumber: pr, CompletedAt: completedAt, RolloutState: rs, CIChecks: checks}
}

func TestCrossBuildDeploymentSweepIDs(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	later := base.Add(time.Hour)
	eri := []string{"eri"}
	deploying := xbuildChunk("deploying", "b1", 2, "pr", "aerial", "deploying", 7, later, eri, nil)
	depChecks := func(checks map[string]string) model.VanadisChunk {
		return xbuildChunk("deploying", "b1", 2, "pr", "aerial", "deploying", 7, later, nil, checks)
	}
	cand := func(buildID, status string, at time.Time, jobs []string, checks map[string]string) model.VanadisChunk {
		return xbuildChunk("f1", buildID, 0, "pr", "aerial", status, 5, at, jobs, checks)
	}
	f1 := []SweptChunk{{ChunkID: "f1", BuildID: "b2"}}
	tests := []struct {
		name       string
		deploying  model.VanadisChunk
		candidates []model.VanadisChunk
		want       []SweptChunk
	}{
		{"earlier merged foreign build with shared job completes", deploying, mutateChunks(cand("b2", "merged", base, eri, nil)), f1},
		{
			"earlier deploying and synced foreign builds with shared job complete", deploying,
			mutateChunks(cand("b2", "deploying", base, eri, nil), xbuildChunk("f2", "b3", 1, "pr", "aerial", "synced", 6, base, eri, nil)),
			[]SweptChunk{{ChunkID: "f1", BuildID: "b2"}, {ChunkID: "f2", BuildID: "b3"}},
		},
		{"foreign build with disjoint jobs stays untouched", deploying, mutateChunks(cand("b2", "merged", base, []string{"worker"}, nil)), nil},
		{"same build candidate stays untouched", deploying, mutateChunks(cand("b1", "merged", base, eri, nil)), nil},
		{"equal stamp across builds never sweeps despite lower chunk index", deploying, mutateChunks(cand("b2", "merged", later, eri, nil)), nil},
		{"missing expected jobs falls back to build and push service", depChecks(map[string]string{"Build & Push (eri)": "passed"}), mutateChunks(cand("b2", "merged", base, eri, nil)), f1},
		{
			"fallback services on both sides intersect", depChecks(map[string]string{"Build and Push (worker)": "passed"}),
			mutateChunks(cand("b2", "merged", base, nil, map[string]string{"Build & Push (worker)": "passed"})), f1,
		},
		{
			"fallback services disjoint stay untouched", depChecks(map[string]string{"Build & Push (eri)": "passed"}),
			mutateChunks(cand("b2", "merged", base, nil, map[string]string{"Build & Push (worker)": "passed"})), nil,
		},
		{"no jobs anywhere stays untouched", deploying, mutateChunks(cand("b2", "merged", base, nil, map[string]string{"lint": "passed"})), nil},
		{
			"non pr mode and missing pr number stay untouched", deploying,
			mutateChunks(xbuildChunk("act", "b2", 0, "action", "aerial", "merged", 5, base, eri, nil), xbuildChunk("nopr", "b2", 1, "pr", "aerial", "merged", 0, base, eri, nil)), nil,
		},
		{"owner qualified repo spelling matches", deploying, mutateChunks(xbuildChunk("f1", "b2", 0, "pr", "example-org/aerial", "merged", 5, base, eri, nil)), f1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CrossBuildDeploymentSweepIDs("example-org", tc.deploying, tc.candidates); !slices.Equal(got, tc.want) {
				t.Errorf("CrossBuildDeploymentSweepIDs = %v, want %v", got, tc.want)
			}
		})
	}
}

func sweepMockStore(queryErr, execErr bool, execTag, failChunk string, candidates []model.VanadisChunk, updates *int) *Store {
	return New(sqltest.DB{
		ExecFn: func(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
			*updates++
			if execErr {
				return pgconn.CommandTag{}, errors.New("database unavailable")
			}
			if failChunk != "" && len(args) > 1 && args[1] == failChunk {
				return pgconn.CommandTag{}, errors.New("database unavailable")
			}
			return pgconn.NewCommandTag(execTag), nil
		},
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			if queryErr {
				return nil, errors.New("database unavailable")
			}
			return &sqltest.Rows{Items: candidates}, nil
		},
	}, "example-org")
}

func TestSweepEarlierDeploymentsAcrossBuilds(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	deploying := xbuildChunk("deploying", "b1", 2, "pr", "aerial", "deploying", 7, base.Add(time.Hour), []string{"eri"}, nil)
	cand := func(id, buildID string, idx int, status string, pr int) model.VanadisChunk {
		return xbuildChunk(id, buildID, idx, "pr", "aerial", status, pr, base, nil, nil)
	}
	f1 := cand("f1", "b2", 0, "merged", 5)
	sweptF1 := []SweptChunk{{ChunkID: "f1", BuildID: "b2"}}
	tests := []struct {
		name          string
		nilDB         bool
		candidates    []model.VanadisChunk
		queryErr      bool
		execErr       bool
		execTag       string
		failChunk     string
		want          []SweptChunk
		wantRecovered []SweptChunk
		wantErr       bool
		wantUpdates   int
	}{
		{name: "sweeps eligible candidate and returns ids", candidates: mutateChunks(f1), execTag: "UPDATE 1", want: sweptF1, wantUpdates: 1},
		{name: "nil database errors", nilDB: true, wantErr: true},
		{name: "query error aborts", candidates: mutateChunks(f1), queryErr: true, wantErr: true},
		{name: "update error aborts", candidates: mutateChunks(f1), execErr: true, wantErr: true, wantUpdates: 1},
		{name: "skipped transition is harmless", candidates: mutateChunks(f1), execTag: "UPDATE 0", wantUpdates: 1},
		{
			name: "recovers already completed eligible chunk", candidates: mutateChunks(f1, cand("r1", "b3", 1, "completed", 5)), execTag: "UPDATE 1",
			want: sweptF1, wantRecovered: []SweptChunk{{ChunkID: "r1", BuildID: "b3"}}, wantUpdates: 1,
		},
		{
			name: "failing candidate keeps earlier successes alongside error", candidates: mutateChunks(f1, cand("f2", "b2", 1, "merged", 6)), execTag: "UPDATE 1",
			failChunk: "f2", want: sweptF1, wantErr: true, wantUpdates: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var updates int
			var st *Store
			if tc.nilDB {
				st = &Store{}
			} else {
				st = sweepMockStore(tc.queryErr, tc.execErr, tc.execTag, tc.failChunk, tc.candidates, &updates)
			}
			got, recovered, err := st.SweepEarlierDeploymentsAcrossBuilds(context.Background(), deploying)
			if (err != nil) != tc.wantErr {
				t.Errorf("SweepEarlierDeploymentsAcrossBuilds err = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("SweepEarlierDeploymentsAcrossBuilds = %v, want %v", got, tc.want)
			}
			if !slices.Equal(recovered, tc.wantRecovered) {
				t.Errorf("SweepEarlierDeploymentsAcrossBuilds recovered = %v, want %v", recovered, tc.wantRecovered)
			}
			if updates != tc.wantUpdates {
				t.Errorf("SweepEarlierDeploymentsAcrossBuilds updates = %d, want %d", updates, tc.wantUpdates)
			}
		})
	}
}
