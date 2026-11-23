package store

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
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

// cgBuildSelect reads builds with the production column list.
const cgBuildSelect = `SELECT ` + BuildColumns + ` FROM vanadis_builds WHERE id = $1`

func TestScanBuild(t *testing.T) {
	t.Parallel()
	stored := model.VanadisBuild{ID: "b1", Agent: "aerial", ThreadID: "thread-1", Tier: 2, Title: "Title", Objective: "Objective", SharedContext: "ctx", Status: "running", StatusMessageID: "msg-1", Quiet: true}
	tests := []struct {
		name    string
		id      string
		nilRow  bool
		want    model.VanadisBuild
		wantErr bool
	}{
		{name: "stored row decodes", id: "b1", want: model.VanadisBuild{ID: "b1", Agent: "aerial", ThreadID: "thread-1", Tier: 2, Title: "Title", Objective: "Objective", SharedContext: "ctx", Status: "running", StatusMessageID: "msg-1", Quiet: true}},
		{name: "missing row propagates scan error", id: "b9", wantErr: true},
		{name: "nil scanner", nilRow: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, pool := openStore(t)
			seedBuild(t, pool, stored)
			var row rowScanner
			if !tc.nilRow {
				row = pool.QueryRow(context.Background(), cgBuildSelect, tc.id)
			}
			got, err := ScanBuild(row)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ScanBuild err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
				t.Errorf("ScanBuild timestamps = %v/%v, want stored defaults", got.CreatedAt, got.UpdatedAt)
			}
			got.CreatedAt, got.UpdatedAt = time.Time{}, time.Time{}
			if got != tc.want {
				t.Errorf("ScanBuild = %+v, want %+v", got, tc.want)
			}
		})
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
		{name: "merge queue preserved", raw: `{"_head_sha":"h","_merge_queue":"2/3","lint":"passed"}`, wantHead: "h", wantChecks: map[string]string{"lint": "passed", "_merge_queue": "2/3"}},
		{name: "merge queue trimmed and preserved", raw: `{"_head_sha":"h","_merge_queue":" 1/2 ","_remediated_sha":"r"}`, wantHead: "h", wantRemed: "r", wantChecks: map[string]string{"_merge_queue": "1/2"}},
		{name: "empty merge queue stripped", raw: `{"_head_sha":"h","_merge_queue":"   "}`, wantHead: "h"},
		{name: "merge queue only", raw: `{"_merge_queue":"1/2"}`, wantChecks: map[string]string{"_merge_queue": "1/2"}},
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

// cgChunkSelect reads every chunk with the production column list.
const cgChunkSelect = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, started_at, completed_at, created_at, updated_at FROM vanadis_chunks ORDER BY chunk_index ASC`

// cgStampChunksSQL pins the row timestamps the database defaults.
const cgStampChunksSQL = `UPDATE vanadis_chunks SET created_at = '2026-05-01 12:00:00+00', updated_at = '2026-05-01 12:00:00+00'`

func TestScanChunks(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	withTimes := model.VanadisChunk{
		ID: "c1", BuildID: "b1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "T1", Instructions: "I1", Effort: "high", Status: "pending", StartedAt: now, CompletedAt: now,
		HeadSHA: "h", CIChecks: map[string]string{"lint": "passed"}, RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}},
	}
	bare := model.VanadisChunk{ID: "c2", BuildID: "b1", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Title: "T2", Instructions: "I2", Status: "pending"}
	boom := model.VanadisChunk{ID: "c1", BuildID: "b1", Banner: "boom"}
	empty := []string{}
	// Effort is capped to medium, empty arrays decode as empty non-nil, the
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
		name     string
		chunks   []model.VanadisChunk
		sabotage string
		want     []model.VanadisChunk
		wantErr  string
	}{
		{
			name:     "mixed rows decode",
			chunks:   []model.VanadisChunk{withTimes, bare},
			sabotage: cgStampChunksSQL + `; UPDATE vanadis_chunks SET ci_checks = 'null' WHERE id = 'c2'`,
			want:     []model.VanadisChunk{decodedTimes, decodedBare},
		},
		{name: "scan error", chunks: []model.VanadisChunk{withTimes}, sabotage: breakChunkScanSQL, wantErr: "failed to scan vanadis chunk: "},
		{name: "rows error", chunks: []model.VanadisChunk{boom}, sabotage: failChunkRowsSQL, wantErr: "error iterating vanadis chunks: "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, pool := openStore(t)
			seed(t, pool, []model.VanadisBuild{{ID: "b1", ThreadID: "t1"}}, tc.chunks)
			mustExec(t, pool, tc.sabotage)
			rows, err := pool.Query(context.Background(), cgChunkSelect)
			if err != nil {
				t.Fatalf("query chunks: %v", err)
			}
			defer rows.Close()
			got, err := ScanChunks(rows)
			gotErr := errString(err)
			if (tc.wantErr == "") != (gotErr == "") || !strings.HasPrefix(gotErr, tc.wantErr) {
				t.Fatalf("ScanChunks err = %q, want prefix %q", gotErr, tc.wantErr)
			}
			for i := range got {
				got[i].StartedAt, got[i].CompletedAt = cgUTC(got[i].StartedAt), cgUTC(got[i].CompletedAt)
				got[i].CreatedAt, got[i].UpdatedAt = cgUTC(got[i].CreatedAt), cgUTC(got[i].UpdatedAt)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ScanChunks = %#v\nwant %#v", got, tc.want)
			}
		})
	}
}

// cgUTC normalizes a decoded timestamp to UTC, keeping the zero time zero.
func cgUTC(ts time.Time) time.Time {
	if ts.IsZero() {
		return time.Time{}
	}
	return ts.UTC()
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
			"earlier deploy_failed same repo completes",
			mutateChunks(pr("early-failed", 0, "deploy_failed", base), dep(1, later)),
			[]string{"early-failed"},
		},
		{
			"earlier deploy_failed without merge stamp stays untouched",
			mutateChunks(pr("early-failed", 0, "deploy_failed", time.Time{}), dep(1, later)),
			nil,
		},
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
			"earlier deploy_failed foreign build with shared job completes", deploying,
			mutateChunks(cand("b2", "deploy_failed", base, eri, nil)), f1,
		},
		{
			"earlier deploy_failed foreign build without merge stamp stays untouched", deploying,
			mutateChunks(cand("b2", "deploy_failed", time.Time{}, eri, nil)), nil,
		},
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

func TestSweepEarlierDeploymentsAcrossBuilds(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	deploying := xbuildChunk("deploying", "b1", 2, "pr", "aerial", "deploying", 7, base.Add(time.Hour), []string{"eri"}, nil)
	noID := deploying
	noID.ID = " "
	noBuildID := deploying
	noBuildID.BuildID = " "
	noRepo := deploying
	noRepo.Repo = ""
	cand := func(id, buildID string, idx int, status string, pr int) model.VanadisChunk {
		return xbuildChunk(id, buildID, idx, "pr", "aerial", status, pr, base, []string{"eri"}, nil)
	}
	f1 := cand("f1", "b2", 0, "merged", 5)
	boom := f1
	boom.Banner = "boom"
	sweptF1 := []SweptChunk{{ChunkID: "f1", BuildID: "b2"}}
	tests := []struct {
		name         string
		nilDB        bool
		deploying    model.VanadisChunk
		candidates   []model.VanadisChunk
		sabotage     string
		want         []SweptChunk
		wantErr      bool
		wantStatuses map[string]string
	}{
		{name: "sweeps eligible candidate and returns ids", deploying: deploying, candidates: mutateChunks(f1), want: sweptF1, wantStatuses: map[string]string{"f1": "merged"}},
		{name: "nil database errors", nilDB: true, deploying: deploying, wantErr: true},
		{name: "blank deploying id is a no-op", deploying: noID, candidates: mutateChunks(f1), wantStatuses: map[string]string{"f1": "merged"}},
		{name: "blank deploying build id is a no-op", deploying: noBuildID, candidates: mutateChunks(f1), wantStatuses: map[string]string{"f1": "merged"}},
		{name: "blank deploying repo is a no-op", deploying: noRepo, candidates: mutateChunks(f1), wantStatuses: map[string]string{"f1": "merged"}},
		{name: "query error aborts", deploying: deploying, candidates: mutateChunks(f1), sabotage: cgBreakChunkPrepareSQL, wantErr: true},
		{name: "scan error aborts", deploying: deploying, candidates: mutateChunks(f1), sabotage: breakChunkScanSQL, wantErr: true},
		{name: "rows error aborts", deploying: deploying, candidates: mutateChunks(boom), sabotage: failChunkRowsSQL, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := cgStore(t, false, tc.nilDB)
			seed(t, pool, []model.VanadisBuild{{ID: "b2", ThreadID: "t2"}, {ID: "b3", ThreadID: "t3"}}, tc.candidates)
			mustExec(t, pool, tc.sabotage)
			got, err := st.SweepEarlierDeploymentsAcrossBuilds(context.Background(), tc.deploying)
			if (err != nil) != tc.wantErr {
				t.Fatalf("SweepEarlierDeploymentsAcrossBuilds err = %v, wantErr %v", err, tc.wantErr)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("SweepEarlierDeploymentsAcrossBuilds = %v, want %v", got, tc.want)
			}
			for id, want := range tc.wantStatuses {
				if got := loadChunk(t, pool, id).Status; got != want {
					t.Errorf("chunk %s status = %q, want %q", id, got, want)
				}
			}
		})
	}
}
