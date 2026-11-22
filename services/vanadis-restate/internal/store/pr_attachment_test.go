package store

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

var aerialRepoSet = []string{"aerial", "azylman/aerial"}

func attachRoutes(mode string, exists, cancelled bool) []sqlRoute {
	return []sqlRoute{
		{match: "SELECT mode", row: valuesRow(mode)},
		{match: "c.status = 'cancelled'", row: valuesRow(cancelled)},
		{match: "SELECT EXISTS", row: valuesRow(exists)},
	}
}

func assertCandidates(t *testing.T, got *[]string, want []string) {
	t.Helper()
	if want != nil && !slices.Equal(*got, want) {
		t.Errorf("repo candidates = %v, want %v", *got, want)
	}
}

func attachRequest(chunkID, repo string) AttachChunkPRRequest {
	return AttachChunkPRRequest{ChunkID: chunkID, PRNumber: 101, Repo: repo}
}

func attachmentScript(chunk, build, thread, status, repo string) dbScript {
	return dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow(build)}}, buildRow: &mockRow{b: &model.VanadisBuild{ID: build, ThreadID: thread, Status: status}}, chunks: []model.VanadisChunk{{ID: chunk, BuildID: build, Mode: "pr", Repo: repo, Status: "running"}}}
}

func TestFirstAttachmentRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, parent, sibling string
		wantConflict          bool
	}{
		{"failed parent", "failed", "completed", false},
		{"running parent", "running", "pending", false},
		{"failed sibling", "failed", "failed", true},
		{"cancelled sibling", "failed", "cancelled", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := model.VanadisBuild{ID: "b1", Status: tc.parent}
			st, _ := dbScript{buildRow: &mockRow{b: &b}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "failed"}, {ID: "other", Status: tc.sibling}}, routes: []sqlRoute{{match: "UPDATE vanadis_chunks", row: valuesRow("b1", "c1", "")}, {match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow("b1")}}, row: valuesRow("b1", "c1", "")}.open()
			_, _, _, err := st.AttachChunkPR(context.Background(), attachRequest("c1", "aerial"))
			if tc.wantConflict {
				assertStoreErr(t, err, ErrBuildConflict, false)
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// This fixture exercises Go selection and error propagation, not SQL effects or row locking.
func TestAttachmentSelectionAndErrors(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("write failed")
	tests := []struct {
		name, parent, status, sibling, failure string
		previous, cap, index                   int
		commitErr, wantErr                     error
	}{
		{name: "first", parent: "failed", status: "failed"},
		{name: "middle", parent: "failed", status: "failed", index: 1},
		{name: "final", parent: "failed", status: "failed", index: 2},
		{name: "duplicate after failure", parent: "failed", status: "failed", previous: 101},
		{name: "cancelled chunk", parent: "running", status: "cancelled", wantErr: ErrBuildConflict},
		{name: "completed different PR", parent: "running", status: "completed", previous: 100, wantErr: ErrBuildConflict},
		{name: "completed matching PR", parent: "running", status: "completed", previous: 101},
		{name: "merged matching PR", parent: "running", status: "merged", previous: 101},
		{name: "merged different PR", parent: "running", status: "merged", previous: 100, wantErr: ErrBuildConflict},
		{name: "synced matching PR", parent: "running", status: "synced", previous: 101},
		{name: "synced different PR", parent: "running", status: "synced", previous: 100, wantErr: ErrBuildConflict},
		{name: "deploying matching PR", parent: "running", status: "deploying", previous: 101},
		{name: "deploying different PR", parent: "running", status: "deploying", previous: 100, wantErr: ErrBuildConflict},
		{name: "deployed matching PR", parent: "running", status: "deployed", previous: 101},
		{name: "deployed different PR", parent: "running", status: "deployed", previous: 100, wantErr: ErrBuildConflict},
		{name: "deploy_failed matching PR", parent: "running", status: "deploy_failed", previous: 101},
		{name: "deploy_failed different PR", parent: "running", status: "deploy_failed", previous: 100, wantErr: ErrBuildConflict},
		{name: "duplicate at cap", parent: "failed", status: "failed", previous: 101, cap: 3},
		{name: "cancelled parent", parent: "cancelled", status: "failed", wantErr: ErrBuildConflict},
		{name: "completed parent", parent: "completed", status: "failed", wantErr: ErrBuildConflict},
		{name: "failed sibling", parent: "failed", status: "failed", sibling: "failed", wantErr: ErrBuildConflict},
		{name: "cancelled sibling", parent: "failed", status: "failed", sibling: "cancelled", wantErr: ErrBuildConflict},
		{name: "chunk write failure", parent: "failed", status: "failed", failure: "UPDATE vanadis_chunks", wantErr: writeErr},
		{name: "parent write failure", parent: "failed", status: "failed", failure: "UPDATE vanadis_builds", wantErr: writeErr},
		{name: "commit failure", parent: "failed", status: "failed", commitErr: writeErr, wantErr: writeErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunks := []model.VanadisChunk{
				{ID: "before", Status: "completed", Repo: "other", ChunkIndex: 0},
				{ID: "between", Status: "completed", Repo: "other", ChunkIndex: 1},
				{ID: "after", Status: "completed", Repo: "other", ChunkIndex: 2},
			}
			chunks[tc.index] = model.VanadisChunk{ID: "selected", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: tc.status, ChunkIndex: tc.index, PRNumber: tc.previous, CIFailCount: tc.cap}
			if tc.sibling != "" {
				chunks[(tc.index+1)%3].Status = tc.sibling
			}
			script := attachmentScript("selected", "b1", "thread-1", tc.parent, "aerial")
			script.chunks = chunks
			script.failExec, script.execErr, script.commitErr = tc.failure, writeErr, tc.commitErr
			// No failure route means writes succeed; this fake does not apply SQL state changes.
			if tc.failure == "" {
				script.execErr = nil
			}
			st, _ := script.open()
			b, c, th, err := st.AttachChunkPR(context.Background(), attachRequest("selected", "aerial"))
			if assertStoreErr(t, err, tc.wantErr, false) {
				return
			}
			if b != "b1" || c != "selected" || th != "thread-1" {
				t.Fatalf("attachment = %q %q %q", b, c, th)
			}
		})
	}
}

func TestFirstWorkerFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, mode, status string
		previous, failures int
		reqPR              int
		want               bool
	}{
		{"eligible unattached", "pr", "failed", 0, 0, 0, true},
		{"already attached no req", "pr", "failed", 101, 0, 0, false},
		{"already attached matching req", "pr", "failed", 101, 0, 101, true},
		{"already attached mismatched req", "pr", "failed", 101, 0, 102, false},
		{"CI attempts", "pr", "failed", 0, 3, 0, false},
		{"action", "action", "failed", 0, 0, 0, false},
		{"running", "pr", "running", 0, 0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := model.VanadisChunk{Mode: tc.mode, Status: tc.status, PRNumber: tc.previous, CIFailCount: tc.failures}
			var got bool
			if tc.reqPR > 0 {
				got = firstWorkerFailure(chunk, tc.reqPR)
			} else {
				got = firstWorkerFailure(chunk)
			}
			if got != tc.want {
				t.Fatalf("eligible = %v, want %v", got, tc.want)
			}
		})
	}
}

type attachResult struct{ BuildID, ChunkID, ThreadID string }

func TestPostgresVanadisStore_AttachChunkPR(t *testing.T) {
	t.Parallel()
	var (
		errModeQuery = errors.New("attach db err")
		errLookup    = errors.New("lookup err")
		errBegin     = errors.New("begin standalone tx err")
		errBuildIns  = errors.New("insert build err")
		errChunkIns  = errors.New("insert chunk err")
		errCommit    = errors.New("commit standalone err")
	)
	noRows := errRow(pgx.ErrNoRows)
	attached := func(mode string) *genericMockRow { return valuesRow("b-"+mode, "c-"+mode, "thread-"+mode) }
	existing := attachRequest("c1", "aerial")
	standalone := attachRequest("", "aerial")
	tests := []struct {
		name            string
		req             AttachChunkPRRequest
		db              dbScript
		wantErrIs       error
		wantErr         bool
		wantErrContains string
		want            attachResult
		wantNewBuild    bool
		wantCandidates  []string
	}{
		{name: "nil receiver", req: existing, db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", req: existing, db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id and zero pr number", req: AttachChunkPRRequest{Repo: "aerial"}, wantErrIs: ErrInvalidBuildInput},
		{name: "chunk not found", req: existing, db: dbScript{row: noRows}, wantErrIs: ErrChunkNotFound},
		{name: "mode query error", req: existing, db: dbScript{row: errRow(errModeQuery)}, wantErrIs: errModeQuery},
		{name: "action chunk requires repo", req: attachRequest("c1", ""), db: dbScript{routes: attachRoutes("action", true, false), row: noRows}, wantErrIs: ErrInvalidBuildInput},
		{name: "action blank repo", req: attachRequest("c1", " \t"), db: dbScript{routes: attachRoutes("action", true, false)}, wantErrIs: ErrInvalidBuildInput},
		{name: "negative pr number", req: AttachChunkPRRequest{ChunkID: "c1", PRNumber: -1}, wantErrIs: ErrInvalidBuildInput},
		{name: "mismatched repo", req: existing, db: attachmentScript("c1", "b1", "", "running", "other"), wantErrIs: ErrBuildConflict},
		{name: "cancelled chunk or build with repo conflicts", req: existing, db: attachmentScript("c1", "b1", "", "cancelled", "aerial"), wantErrIs: ErrBuildConflict},
		{name: "cancelled chunk or build without repo conflicts", req: attachRequest("c1", ""), db: attachmentScript("c1", "b1", "", "cancelled", "aerial"), wantErrIs: ErrBuildConflict},
		{name: "attaches pr chunk without repo", req: attachRequest("c-pr", ""), db: attachmentScript("c-pr", "b-pr", "thread-pr", "running", "aerial"), want: attachResult{"b-pr", "c-pr", "thread-pr"}},
		{name: "attaches pr chunk matching repo candidates", req: attachRequest("c-pr", "aerial"), db: attachmentScript("c-pr", "b-pr", "thread-pr", "running", "aerial"), want: attachResult{"b-pr", "c-pr", "thread-pr"}},
		{name: "attaches action chunk", req: attachRequest("c-act", "aerial"), db: dbScript{routes: attachRoutes("action", true, false), row: attached("action")}, want: attachResult{"b-action", "c-action", "thread-action"}, wantCandidates: aerialRepoSet},
		{name: "action attachment removed chunk", req: existing, db: dbScript{routes: attachRoutes("action", false, false), row: noRows}, wantErrIs: ErrChunkNotFound},
		{name: "action attachment terminal parent", req: existing, db: dbScript{routes: attachRoutes("action", true, true), row: noRows}, wantErrIs: ErrBuildConflict},
		{name: "action attachment mismatching repository", req: existing, db: dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("action")}, {match: "c.status = 'cancelled'", row: valuesRow(false)}, {match: "repo != ''", row: valuesRow(true)}, {match: "SELECT EXISTS", row: valuesRow(true)}}, row: noRows}, wantErrIs: ErrBuildConflict},
		{name: "action attachment unknown rejection", req: existing, db: dbScript{routes: attachRoutes("action", true, false), row: noRows}, wantErr: true},
		{name: "pr transaction begin error", req: existing, db: dbScript{routes: attachRoutes("pr", true, false), beginErr: errBegin}, wantErrIs: errBegin},
		{name: "pr transaction chunk removed", req: existing, db: dbScript{routes: attachRoutes("pr", true, false), row: noRows}, wantErrIs: ErrChunkNotFound},
		{name: "pr transaction lookup failure", req: existing, db: dbScript{routes: attachRoutes("pr", true, false), row: errRow(errLookup)}, wantErrIs: errLookup},
		{name: "pr parent lock failure", req: existing, db: dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow("b1")}}, buildRow: errRow(errLookup)}, wantErrIs: errLookup},
		{name: "pr locked chunk absent", req: existing, db: dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow("b1")}}, buildRow: &mockRow{b: &model.VanadisBuild{ID: "b1", Status: "running"}}}, wantErrIs: ErrChunkNotFound},
		{name: "standalone requires repo", req: attachRequest("", ""), wantErrIs: ErrInvalidBuildInput},
		{name: "standalone begin error", req: standalone, db: dbScript{beginErr: errBegin}, wantErrIs: errBegin},
		{name: "standalone existing pr lookup error", req: standalone, db: dbScript{row: errRow(errLookup)}, wantErrIs: errLookup},
		{name: "standalone reuses existing pr chunk", req: standalone, db: dbScript{row: valuesRow("b-existing", "c-existing", "thread-existing")}, want: attachResult{"b-existing", "c-existing", "thread-existing"}, wantCandidates: aerialRepoSet},
		{name: "standalone insert build error", req: standalone, db: dbScript{row: noRows, failExec: "INSERT INTO vanadis_builds", execErr: errBuildIns}, wantErrIs: errBuildIns},
		{name: "standalone insert chunk error", req: standalone, db: dbScript{row: noRows, failExec: "INSERT INTO vanadis_chunks", execErr: errChunkIns}, wantErrIs: errChunkIns},
		{name: "standalone commit error", req: standalone, db: dbScript{row: noRows, commitErr: errCommit}, wantErrIs: errCommit},
		{name: "standalone creates build in target thread", req: AttachChunkPRRequest{Repo: "aerial", PRNumber: 101, TargetID: "123456789012345678", Branch: "dependabot/npm_and_yarn/foo", Title: "Bump foo"}, db: dbScript{row: noRows}, want: attachResult{ThreadID: "123456789012345678"}, wantNewBuild: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, candidates := tc.db.open()
			bID, cID, threadID, err := st.AttachChunkPR(context.Background(), tc.req)
			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("err = %v, want error containing %q", err, tc.wantErrContains)
				}
				return
			}
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			got := attachResult{bID, cID, threadID}
			if tc.wantNewBuild {
				if !strings.HasPrefix(bID, "build-") || cID != bID+"-chunk-1" {
					t.Errorf("AttachChunkPR minted build %q chunk %q, want build-* with chunk <build>-chunk-1", bID, cID)
				}
				got.BuildID, got.ChunkID = "", ""
			}
			if got != tc.want {
				t.Errorf("AttachChunkPR = %+v, want %+v", got, tc.want)
			}
			assertCandidates(t, candidates, tc.wantCandidates)
		})
	}
}

func TestAttachStandalonePR_RecordsOwnerAndRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		agent      string
		runner     string
		wantAgent  string
		wantRunner string
	}{
		{name: "defaults to the default agent", wantAgent: "aerial", wantRunner: "aerial"},
		{name: "caller owns and its runner executes", agent: "agent-alpha", runner: "agent-alpha", wantAgent: "agent-alpha", wantRunner: "agent-alpha"},
		{name: "names are normalized", agent: " Agent-Beta ", runner: "AGENT-BETA", wantAgent: "agent-beta", wantRunner: "agent-beta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buildArgs []any
			queryRow := func(context.Context, string, ...any) pgx.Row { return errRow(pgx.ErrNoRows) }
			exec := func(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
				if strings.Contains(query, "INSERT INTO vanadis_builds") {
					buildArgs = args
				}
				return pgconn.NewCommandTag("INSERT 0 1"), nil
			}
			tx := &mockTx{queryRowFn: queryRow, execFn: exec}
			st := New(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return tx, nil }}, "azylman")
			req := AttachChunkPRRequest{Repo: "aerial", PRNumber: 9, Agent: tc.agent, RunnerAgent: tc.runner}
			if _, _, _, err := st.AttachChunkPR(context.Background(), req); err != nil {
				t.Fatalf("AttachChunkPR: %v", err)
			}
			if len(buildArgs) < 3 {
				t.Fatalf("build insert args = %v, want id, agent, runner_agent first", buildArgs)
			}
			if buildArgs[1] != tc.wantAgent || buildArgs[2] != tc.wantRunner {
				t.Errorf("inserted agent, runner_agent = %v, %v; want %q, %q", buildArgs[1], buildArgs[2], tc.wantAgent, tc.wantRunner)
			}
		})
	}
}
