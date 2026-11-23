package store

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// paSkipChunkUpdatesSQL makes every UPDATE of vanadis_chunks silently affect
// no row (a BEFORE trigger returning NULL), so an UPDATE ... RETURNING comes
// back empty although the chunk still exists unchanged.
const paSkipChunkUpdatesSQL = `CREATE FUNCTION pa_skip() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TRIGGER pa_skip BEFORE UPDATE ON vanadis_chunks FOR EACH ROW EXECUTE FUNCTION pa_skip();`

// paFixture is the store a table row runs against: a nil receiver, a Store
// without a database, a closed pool, or a private database seeded with builds
// and chunks and then sabotaged.
type paFixture struct {
	nilReceiver, nilDB, closed bool
	builds                     []model.VanadisBuild
	chunks                     []model.VanadisChunk
	sabotage                   string
}

func (f paFixture) open(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	switch {
	case f.nilReceiver:
		return nil, nil
	case f.nilDB:
		return &Store{}, nil
	case f.closed:
		return closedStore(t), nil
	}
	st, pool := openStore(t)
	seed(t, pool, f.builds, f.chunks)
	mustExec(t, pool, f.sabotage)
	return st, pool
}

func (f paFixture) store(t *testing.T) *Store {
	t.Helper()
	switch {
	case f.nilReceiver:
		return nil
	case f.nilDB:
		return &Store{}
	case f.closed:
		return closedStore(t)
	}
	st, pool := openStore(t)
	seed(t, pool, f.builds, f.chunks)
	mustExec(t, pool, f.sabotage)
	return st
}

// paDeleteOnUpdateSQL deletes a chunk the moment an UPDATE reaches it and
// skips the UPDATE: the chunk existed when the attachment started but is gone
// when the attachment write runs.
const paDeleteOnUpdateSQL = `CREATE FUNCTION pa_delete_on_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN DELETE FROM vanadis_chunks WHERE id = OLD.id; RETURN NULL; END $$;
CREATE TRIGGER pa_delete_on_update BEFORE UPDATE ON vanadis_chunks FOR EACH ROW EXECUTE FUNCTION pa_delete_on_update();`

func attachRequest(chunkID, repo string) AttachChunkPRRequest {
	return AttachChunkPRRequest{ChunkID: chunkID, PRNumber: 101, Repo: repo}
}

func paBuild(id, thread, status string) model.VanadisBuild {
	return model.VanadisBuild{ID: id, ThreadID: thread, Status: status}
}

func paChunk(id, build, mode, repo, status string, idx, pr int) model.VanadisChunk {
	return model.VanadisChunk{ID: id, BuildID: build, ChunkIndex: idx, Mode: mode, Repo: repo, Status: status, PRNumber: pr, Title: "T", Instructions: "I"}
}

func TestFirstAttachmentRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, parent, sibling string
		wantConflict          bool
	}{
		{"failed parent", "failed", "completed", false},
		{"running parent", "running", "pending", false},
		{"failed sibling", "failed", "failed", false},
		{"cancelled sibling", "failed", "cancelled", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := openStore(t)
			seed(t, pool, []model.VanadisBuild{paBuild("b1", "thread-1", tc.parent)},
				[]model.VanadisChunk{paChunk("c1", "b1", "pr", "aerial", "failed", 0, 0), paChunk("other", "b1", "pr", "aerial", tc.sibling, 1, 0)})
			_, _, _, err := st.AttachChunkPR(context.Background(), attachRequest("c1", "aerial"))
			if tc.wantConflict {
				assertStoreErr(t, err, ErrBuildConflict, false)
				if got := loadChunk(t, pool, "c1"); got.Status != "failed" || got.PRNumber != 0 {
					t.Fatalf("rejected attachment stored status=%s pr=%d, want failed/0", got.Status, got.PRNumber)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := loadChunk(t, pool, "c1"); got.Status != "pr_opened" || got.PRNumber != 101 {
				t.Fatalf("stored status=%s pr=%d, want pr_opened/101", got.Status, got.PRNumber)
			}
			if got := loadBuild(t, pool, "b1"); got.Status != "running" {
				t.Fatalf("parent status = %s, want running", got.Status)
			}
		})
	}
}

// Real rows drive selection, the recovery rule and the SQL status CASE;
// sabotage rows inject write and commit failures.
func TestAttachmentSelectionAndErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, parent, status, sibling, sabotage string
		previous, cap, index                    int
		wantErrIs                               error
		wantErr                                 bool
		wantStatus, wantParent                  string
	}{
		{name: "first", parent: "failed", status: "failed", wantStatus: "pr_opened", wantParent: "running"},
		{name: "middle", parent: "failed", status: "failed", index: 1, wantStatus: "pr_opened", wantParent: "running"},
		{name: "final", parent: "failed", status: "failed", index: 2, wantStatus: "pr_opened", wantParent: "running"},
		{name: "duplicate after failure", parent: "failed", status: "failed", previous: 101, wantStatus: "pr_opened", wantParent: "running"},
		{name: "cancelled chunk", parent: "running", status: "cancelled", wantErrIs: ErrBuildConflict},
		{name: "completed different PR", parent: "running", status: "completed", previous: 100, wantErrIs: ErrBuildConflict},
		{name: "completed matching PR", parent: "running", status: "completed", previous: 101, wantStatus: "completed", wantParent: "running"},
		{name: "merged matching PR", parent: "running", status: "merged", previous: 101, wantStatus: "merged", wantParent: "running"},
		{name: "merged different PR", parent: "running", status: "merged", previous: 100, wantErrIs: ErrBuildConflict},
		{name: "synced matching PR", parent: "running", status: "synced", previous: 101, wantStatus: "synced", wantParent: "running"},
		{name: "synced different PR", parent: "running", status: "synced", previous: 100, wantErrIs: ErrBuildConflict},
		{name: "deploying matching PR", parent: "running", status: "deploying", previous: 101, wantStatus: "deploying", wantParent: "running"},
		{name: "deploying different PR", parent: "running", status: "deploying", previous: 100, wantErrIs: ErrBuildConflict},
		{name: "deployed matching PR", parent: "running", status: "deployed", previous: 101, wantStatus: "deployed", wantParent: "running"},
		{name: "deployed different PR", parent: "running", status: "deployed", previous: 100, wantErrIs: ErrBuildConflict},
		{name: "deploy_failed matching PR", parent: "running", status: "deploy_failed", previous: 101, wantStatus: "deploy_failed", wantParent: "running"},
		{name: "deploy_failed different PR", parent: "running", status: "deploy_failed", previous: 100, wantErrIs: ErrBuildConflict},
		{name: "duplicate at cap", parent: "failed", status: "failed", previous: 101, cap: 3, wantStatus: "failed", wantParent: "failed"},
		{name: "cancelled parent", parent: "cancelled", status: "failed", wantErrIs: ErrBuildConflict},
		{name: "completed parent", parent: "completed", status: "failed", wantErrIs: ErrBuildConflict},
		{name: "failed sibling", parent: "failed", status: "failed", sibling: "failed", wantStatus: "pr_opened", wantParent: "running"},
		{name: "cancelled sibling", parent: "failed", status: "failed", sibling: "cancelled", wantErrIs: ErrBuildConflict},
		{name: "chunk write failure", parent: "failed", status: "failed", sabotage: failStatement("vanadis_chunks", "UPDATE"), wantErr: true},
		{name: "parent write failure", parent: "failed", status: "failed", sabotage: failStatement("vanadis_builds", "UPDATE"), wantErr: true},
		{name: "commit failure", parent: "failed", status: "failed", sabotage: failCommit("vanadis_chunks", "UPDATE"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunks := []model.VanadisChunk{
				paChunk("before", "b1", "pr", "other", "completed", 0, 0),
				paChunk("between", "b1", "pr", "other", "completed", 1, 0),
				paChunk("after", "b1", "pr", "other", "completed", 2, 0),
			}
			chunks[tc.index] = paChunk("selected", "b1", "pr", "aerial", tc.status, tc.index, tc.previous)
			chunks[tc.index].CIFailCount = tc.cap
			if tc.sibling != "" {
				chunks[(tc.index+1)%3].Status = tc.sibling
			}
			st, pool := paFixture{builds: []model.VanadisBuild{paBuild("b1", "thread-1", tc.parent)}, chunks: chunks, sabotage: tc.sabotage}.open(t)
			b, c, th, err := st.AttachChunkPR(context.Background(), attachRequest("selected", "aerial"))
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				if got := loadChunk(t, pool, "selected"); got.Status != tc.status || got.PRNumber != tc.previous {
					t.Fatalf("failed attachment stored status=%s pr=%d, want %s/%d", got.Status, got.PRNumber, tc.status, tc.previous)
				}
				if got := loadBuild(t, pool, "b1"); got.Status != tc.parent {
					t.Fatalf("failed attachment parent = %s, want %s", got.Status, tc.parent)
				}
				return
			}
			if b != "b1" || c != "selected" || th != "thread-1" {
				t.Fatalf("attachment = %q %q %q", b, c, th)
			}
			if got := loadChunk(t, pool, "selected"); got.Status != tc.wantStatus || got.PRNumber != 101 {
				t.Fatalf("stored status=%s pr=%d, want %s/101", got.Status, got.PRNumber, tc.wantStatus)
			}
			if got := loadBuild(t, pool, "b1"); got.Status != tc.wantParent {
				t.Fatalf("parent status = %s, want %s", got.Status, tc.wantParent)
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
	running := func(id, thread string) []model.VanadisBuild {
		return []model.VanadisBuild{paBuild(id, thread, "running")}
	}
	prChunk := func(id, build, repo, status string, pr int) []model.VanadisChunk {
		return []model.VanadisChunk{paChunk(id, build, "pr", repo, status, 0, pr)}
	}
	actionChunk := func(id, build, repo, status string, pr int) []model.VanadisChunk {
		return []model.VanadisChunk{paChunk(id, build, "action", repo, status, 0, pr)}
	}
	existing := attachRequest("c1", "aerial")
	standalone := attachRequest("", "aerial")
	tests := []struct {
		name         string
		req          AttachChunkPRRequest
		db           paFixture
		wantErrIs    error
		wantErr      bool
		want         attachResult
		wantNewBuild bool
		// wantRepo, wantStatus describe the attached chunk stored afterwards.
		wantRepo, wantStatus string
	}{
		{name: "nil receiver", req: existing, db: paFixture{nilReceiver: true}, wantErr: true},
		{name: "nil db", req: existing, db: paFixture{nilDB: true}, wantErr: true},
		{name: "empty chunk id and zero pr number", req: AttachChunkPRRequest{Repo: "aerial"}, wantErrIs: ErrInvalidBuildInput},
		{name: "chunk not found", req: existing, wantErrIs: ErrChunkNotFound},
		{name: "mode query error", req: existing, db: paFixture{builds: running("b1", ""), chunks: prChunk("c1", "b1", "aerial", "running", 0), sabotage: `ALTER TABLE vanadis_chunks RENAME COLUMN mode TO mode_x;`}, wantErr: true},
		{name: "action chunk requires repo", req: attachRequest("c1", ""), db: paFixture{builds: running("b1", ""), chunks: actionChunk("c1", "b1", "", "running", 0)}, wantErrIs: ErrInvalidBuildInput},
		{name: "action blank repo", req: attachRequest("c1", " \t"), db: paFixture{builds: running("b1", ""), chunks: actionChunk("c1", "b1", "", "running", 0)}, wantErrIs: ErrInvalidBuildInput},
		{name: "negative pr number", req: AttachChunkPRRequest{ChunkID: "c1", PRNumber: -1}, wantErrIs: ErrInvalidBuildInput},
		{name: "mismatched repo", req: existing, db: paFixture{builds: running("b1", ""), chunks: prChunk("c1", "b1", "other", "running", 0)}, wantErrIs: ErrBuildConflict},
		{name: "cancelled chunk or build with repo conflicts", req: existing, db: paFixture{builds: []model.VanadisBuild{paBuild("b1", "", "cancelled")}, chunks: prChunk("c1", "b1", "aerial", "running", 0)}, wantErrIs: ErrBuildConflict},
		{name: "cancelled chunk or build without repo conflicts", req: attachRequest("c1", ""), db: paFixture{builds: []model.VanadisBuild{paBuild("b1", "", "cancelled")}, chunks: prChunk("c1", "b1", "aerial", "running", 0)}, wantErrIs: ErrBuildConflict},
		{
			name: "attaches pr chunk without repo", req: attachRequest("c-pr", ""), db: paFixture{builds: running("b-pr", "thread-pr"), chunks: prChunk("c-pr", "b-pr", "aerial", "running", 0)},
			want: attachResult{"b-pr", "c-pr", "thread-pr"}, wantRepo: "aerial", wantStatus: "pr_opened",
		},
		{
			name: "attaches pr chunk matching repo candidates", req: attachRequest("c-pr", "aerial"), db: paFixture{builds: running("b-pr", "thread-pr"), chunks: prChunk("c-pr", "b-pr", "example-org/aerial", "running", 0)},
			want: attachResult{"b-pr", "c-pr", "thread-pr"}, wantRepo: "example-org/aerial", wantStatus: "pr_opened",
		},
		{
			name: "attaches action chunk", req: attachRequest("c-act", "aerial"), db: paFixture{builds: running("b-action", "thread-action"), chunks: actionChunk("c-act", "b-action", "", "running", 0)},
			want: attachResult{"b-action", "c-act", "thread-action"}, wantRepo: "aerial", wantStatus: "pr_opened",
		},
		{
			name: "action chunk matches owner-qualified attached repo", req: attachRequest("c-act", "aerial"), db: paFixture{builds: running("b-action", "thread-action"), chunks: actionChunk("c-act", "b-action", "example-org/aerial", "ci_failed", 101)},
			want: attachResult{"b-action", "c-act", "thread-action"}, wantRepo: "aerial", wantStatus: "ci_failed",
		},
		{name: "action attachment terminal parent", req: existing, db: paFixture{builds: []model.VanadisBuild{paBuild("b1", "", "completed")}, chunks: actionChunk("c1", "b1", "", "running", 0)}, wantErrIs: ErrBuildConflict},
		{name: "action attachment mismatching repository", req: existing, db: paFixture{builds: running("b1", ""), chunks: actionChunk("c1", "b1", "other/repo", "pr_opened", 5)}, wantErrIs: ErrBuildConflict},
		{name: "action attachment unknown rejection", req: existing, db: paFixture{builds: running("b1", ""), chunks: actionChunk("c1", "b1", "", "running", 0), sabotage: paSkipChunkUpdatesSQL}, wantErr: true},
		{name: "pr transaction lookup failure", req: existing, db: paFixture{builds: running("b1", ""), chunks: prChunk("c1", "b1", "aerial", "running", 0), sabotage: `ALTER TABLE vanadis_chunks RENAME COLUMN build_id TO build_id_x;`}, wantErr: true},
		{name: "pr parent lock failure", req: existing, db: paFixture{builds: running("b1", ""), chunks: prChunk("c1", "b1", "aerial", "running", 0), sabotage: `ALTER TABLE vanadis_builds RENAME COLUMN quiet TO quiet_x;`}, wantErr: true},
		{name: "pr chunk removed after parent lock", req: existing, db: paFixture{builds: running("b1", ""), chunks: prChunk("c1", "b1", "aerial", "running", 0), sabotage: vanishChunkOnBuildReadSQL}, wantErrIs: ErrChunkNotFound},
		{name: "action chunk removed during attachment", req: existing, db: paFixture{builds: running("b1", ""), chunks: actionChunk("c1", "b1", "", "running", 0), sabotage: paDeleteOnUpdateSQL}, wantErrIs: ErrChunkNotFound},
		{name: "pr chunk lock failure", req: existing, db: paFixture{builds: running("b1", ""), chunks: prChunk("c1", "b1", "aerial", "running", 0), sabotage: `ALTER TABLE vanadis_chunks RENAME COLUMN banner TO banner_x;`}, wantErr: true},
		{name: "standalone requires repo", req: attachRequest("", ""), wantErrIs: ErrInvalidBuildInput},
		{name: "standalone begin error", req: standalone, db: paFixture{closed: true}, wantErr: true},
		{name: "standalone existing pr lookup error", req: standalone, db: paFixture{sabotage: `ALTER TABLE vanadis_builds RENAME COLUMN thread_id TO thread_x;`}, wantErr: true},
		{
			name: "standalone reuses existing pr chunk", req: standalone, db: paFixture{builds: running("b-existing", "thread-existing"), chunks: prChunk("c-existing", "b-existing", "example-org/aerial", "pr_opened", 101)},
			want: attachResult{"b-existing", "c-existing", "thread-existing"}, wantRepo: "example-org/aerial", wantStatus: "pr_opened",
		},
		{name: "standalone insert build error", req: standalone, db: paFixture{sabotage: failStatement("vanadis_builds", "INSERT")}, wantErr: true},
		{name: "standalone insert chunk error", req: standalone, db: paFixture{sabotage: failStatement("vanadis_chunks", "INSERT")}, wantErr: true},
		{name: "standalone commit error", req: standalone, db: paFixture{sabotage: failCommit("vanadis_builds", "INSERT")}, wantErr: true},
		{
			name: "standalone creates build in target thread", req: AttachChunkPRRequest{Repo: "aerial", PRNumber: 101, TargetID: "123456789012345678", Branch: "dependabot/npm_and_yarn/foo", Title: "Bump foo"},
			want: attachResult{ThreadID: "123456789012345678"}, wantNewBuild: true, wantRepo: "aerial", wantStatus: "pr_opened",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := tc.db.open(t)
			bID, cID, threadID, err := st.AttachChunkPR(context.Background(), tc.req)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			got := attachResult{bID, cID, threadID}
			if tc.wantNewBuild {
				if !strings.HasPrefix(bID, "build-") || cID != bID+"-chunk-1" {
					t.Errorf("AttachChunkPR minted build %q chunk %q, want build-* with chunk <build>-chunk-1", bID, cID)
				}
				if b := loadBuild(t, pool, bID); b.ThreadID != tc.want.ThreadID || b.Title != "Bump foo" || b.Status != "running" {
					t.Errorf("stored standalone build = %+v", b)
				}
				got.BuildID, got.ChunkID = "", ""
			}
			if got != tc.want {
				t.Errorf("AttachChunkPR = %+v, want %+v", got, tc.want)
			}
			if c := loadChunk(t, pool, cID); c.Repo != tc.wantRepo || c.Status != tc.wantStatus || c.PRNumber != 101 {
				t.Errorf("stored chunk repo=%q status=%q pr=%d, want %q/%q/101", c.Repo, c.Status, c.PRNumber, tc.wantRepo, tc.wantStatus)
			}
		})
	}
}

// TestAttachPRTransaction drives the PR-mode transaction directly for the
// failures AttachChunkPR's mode probe would otherwise intercept.
func TestAttachPRTransaction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		db        paFixture
		wantErrIs error
		wantErr   bool
	}{
		{name: "begin error", db: paFixture{closed: true}, wantErr: true},
		{name: "chunk removed", wantErrIs: ErrChunkNotFound},
		{name: "parent missing", db: paFixture{sabotage: `ALTER TABLE vanadis_chunks DROP CONSTRAINT vanadis_chunks_build_id_fkey; INSERT INTO vanadis_chunks (id, build_id, chunk_index, title, instructions, repo) VALUES ('c1', 'gone', 0, 'T', 'I', 'aerial');`}, wantErrIs: ErrBuildNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := tc.db.store(t)
			_, _, _, err := st.attachPRTransaction(context.Background(), "c1", attachRequest("c1", "aerial"))
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestAttachStandalonePR_RecordsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		agent     string
		wantAgent string
	}{
		{name: "defaults to the default agent", wantAgent: "aerial"},
		{name: "caller owns the build", agent: "agent-alpha", wantAgent: "agent-alpha"},
		{name: "names are normalized", agent: " Agent-Beta ", wantAgent: "agent-beta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := openStore(t)
			req := AttachChunkPRRequest{Repo: "aerial", PRNumber: 9, Agent: tc.agent}
			buildID, _, _, err := st.AttachChunkPR(context.Background(), req)
			if err != nil {
				t.Fatalf("AttachChunkPR: %v", err)
			}
			if got := loadBuild(t, pool, buildID).Agent; got != tc.wantAgent {
				t.Errorf("stored agent = %q; want %q", got, tc.wantAgent)
			}
		})
	}
}

type donorHydrationCase struct {
	name             string
	mode             string
	donorStatus      string
	donorBuildStatus string
	targetStatus     string
	targetPR         int
	targetParent     string
	donorBanner      string
	reqRepo          string
	targetRepo       string
	wantStatus       string
	wantBanner       string
	wantJobs         []string
	wantCIChecks     bool
	wantCompletedAt  bool
	wantParentStatus string
}

func initHydrationFixture(tc donorHydrationCase, stamp time.Time, donorCIChecks map[string]string, donorRollout *pubsub.PRRolloutState) (paFixture, AttachChunkPRRequest) {
	dBuildStatus := strutil.FirstNonEmpty(tc.donorBuildStatus, "running")
	tParentStatus := strutil.FirstNonEmpty(tc.targetParent, "running")
	tRepo := tc.targetRepo
	if tRepo == "" && tc.mode == "pr" {
		tRepo = "aerial"
	}
	donorBuild := paBuild("b-donor", "thread-donor", dBuildStatus)
	targetBuild := paBuild("b-target", "thread-target", tParentStatus)
	donorChunk := paChunk("c-donor", "b-donor", "pr", "aerial", tc.donorStatus, 0, 101)
	donorChunk.RolloutState = donorRollout
	donorChunk.CIChecks = donorCIChecks
	donorChunk.Banner = tc.donorBanner
	donorChunk.CompletedAt = stamp
	targetChunk := paChunk("c-target", "b-target", tc.mode, tRepo, tc.targetStatus, 0, tc.targetPR)
	if tc.targetPR != 0 && (tc.targetStatus == statusCompleted || tc.targetStatus == statusDeployed || tc.targetStatus == statusMerged) {
		targetChunk.CompletedAt = stamp
	}
	reqRepo := "aerial"
	if tc.mode == "pr" && tc.targetRepo != "" && tc.reqRepo == "" {
		reqRepo = ""
	} else if tc.reqRepo != "" {
		reqRepo = tc.reqRepo
	}
	fix := paFixture{
		builds: []model.VanadisBuild{donorBuild, targetBuild},
		chunks: []model.VanadisChunk{donorChunk, targetChunk},
	}
	req := AttachChunkPRRequest{ChunkID: "c-target", PRNumber: 101, Repo: reqRepo}
	return fix, req
}

func checkHydratedChunkFields(t *testing.T, got model.VanadisChunk, tc donorHydrationCase, donorCIChecks map[string]string) {
	t.Helper()
	if got.Status != tc.wantStatus {
		t.Errorf("status = %q, want %q", got.Status, tc.wantStatus)
	}
	if got.Banner != tc.wantBanner {
		t.Errorf("banner = %q, want %q", got.Banner, tc.wantBanner)
	}
	if got.PRNumber != 101 {
		t.Errorf("pr_number = %d, want 101", got.PRNumber)
	}
	if tc.wantJobs != nil {
		if got.RolloutState == nil {
			t.Fatalf("expected rollout_state, got nil")
		}
		if !reflect.DeepEqual(tc.wantJobs, got.RolloutState.ExpectedJobs) {
			t.Errorf("jobs = %v, want %v", got.RolloutState.ExpectedJobs, tc.wantJobs)
		}
	}
	if tc.wantCIChecks && !reflect.DeepEqual(donorCIChecks, got.CIChecks) {
		t.Errorf("ci_checks = %v, want %v", got.CIChecks, donorCIChecks)
	}
	if tc.wantCompletedAt && got.CompletedAt.IsZero() {
		t.Errorf("expected completed_at to be set, was zero")
	}
}

func checkHydratedBuild(t *testing.T, pool *pgxpool.Pool, wantStatus string) {
	t.Helper()
	if wantStatus == "" {
		return
	}
	gotBuild := loadBuild(t, pool, "b-target")
	if gotBuild.Status != wantStatus {
		t.Errorf("parent build status = %q, want %q", gotBuild.Status, wantStatus)
	}
}

func donorHydrationPRCases() []donorHydrationCase {
	return []donorHydrationCase{
		{
			name:            "pr chunk adopts completed from completed donor",
			mode:            "pr",
			donorStatus:     "completed",
			targetStatus:    "running",
			wantStatus:      "completed",
			wantBanner:      "",
			wantJobs:        []string{"eri"},
			wantCIChecks:    true,
			wantCompletedAt: true,
		},
		{
			name:            "pr chunk maps deployed donor to completed",
			mode:            "pr",
			donorStatus:     "deployed",
			targetStatus:    "running",
			wantStatus:      "completed",
			wantBanner:      "",
			wantJobs:        []string{"eri"},
			wantCIChecks:    true,
			wantCompletedAt: true,
		},
		{
			name:            "pr chunk adopts merged from merged donor",
			mode:            "pr",
			donorStatus:     "merged",
			targetStatus:    "running",
			wantStatus:      "merged",
			wantBanner:      "",
			wantJobs:        []string{"eri"},
			wantCIChecks:    true,
			wantCompletedAt: true,
		},
		{
			name:            "pr chunk adopts ci_failed and banner from donor",
			mode:            "pr",
			donorStatus:     "ci_failed",
			targetStatus:    "running",
			donorBanner:     "tests failed",
			wantStatus:      "ci_failed",
			wantBanner:      "tests failed",
			wantJobs:        []string{"eri"},
			wantCIChecks:    true,
			wantCompletedAt: false,
		},
		{
			name:            "pr chunk adopts deploy_failed and banner from donor",
			mode:            "pr",
			donorStatus:     "deploy_failed",
			targetStatus:    "running",
			donorBanner:     "deploy failed",
			wantStatus:      "deploy_failed",
			wantBanner:      "deploy failed",
			wantJobs:        []string{"eri"},
			wantCIChecks:    true,
			wantCompletedAt: true,
		},
		{
			name:            "pr chunk adopts conflict and banner from donor",
			mode:            "pr",
			donorStatus:     "conflict",
			targetStatus:    "running",
			donorBanner:     "merge conflict",
			wantStatus:      "conflict",
			wantBanner:      "merge conflict",
			wantJobs:        []string{"eri"},
			wantCIChecks:    true,
			wantCompletedAt: false,
		},
		{
			name:            "pr_opened donor does not advance target chunk",
			mode:            "pr",
			donorStatus:     "pr_opened",
			targetStatus:    "running",
			wantStatus:      "pr_opened",
			wantBanner:      "",
			wantJobs:        nil,
			wantCIChecks:    false,
			wantCompletedAt: false,
		},
		{
			name:             "cancelled donor build is ignored",
			mode:             "pr",
			donorStatus:      "completed",
			donorBuildStatus: "cancelled",
			targetStatus:     "running",
			wantStatus:       "pr_opened",
			wantBanner:       "",
			wantJobs:         nil,
			wantCIChecks:     false,
			wantCompletedAt:  false,
		},
		{
			name:             "recovers failed worker and reopens parent build even with completed donor",
			mode:             "pr",
			donorStatus:      "completed",
			targetStatus:     "failed",
			targetParent:     "failed",
			wantStatus:       "completed",
			wantBanner:       "",
			wantJobs:         []string{"eri"},
			wantCIChecks:     true,
			wantCompletedAt:  true,
			wantParentStatus: "running",
		},
		{
			name:            "empty req repo derives repo from pr chunk",
			mode:            "pr",
			donorStatus:     "completed",
			targetStatus:    "running",
			targetRepo:      "aerial",
			reqRepo:         "",
			wantStatus:      "completed",
			wantBanner:      "",
			wantJobs:        []string{"eri"},
			wantCIChecks:    true,
			wantCompletedAt: true,
		},
		{
			name:            "completed target chunk does not regress when attached with merged donor",
			mode:            "pr",
			donorStatus:     "merged",
			targetStatus:    "completed",
			targetPR:        101,
			wantStatus:      "completed",
			wantBanner:      "",
			wantJobs:        nil,
			wantCIChecks:    false,
			wantCompletedAt: true,
		},
	}
}

func donorHydrationActionCases() []donorHydrationCase {
	return []donorHydrationCase{
		{
			name:            "action chunk with deployed donor stays in running and clears banner",
			mode:            "action",
			donorStatus:     "deployed",
			targetStatus:    "running",
			donorBanner:     "ignore me",
			wantStatus:      "running",
			wantBanner:      "",
			wantJobs:        []string{"eri"},
			wantCIChecks:    true,
			wantCompletedAt: false,
		},
		{
			name:            "action chunk in failed status with completed donor stays failed",
			mode:            "action",
			donorStatus:     "completed",
			targetStatus:    "failed",
			wantStatus:      "failed",
			wantBanner:      "",
			wantJobs:        []string{"eri"},
			wantCIChecks:    true,
			wantCompletedAt: false,
		},
		{
			name:            "action chunk already completed for same PR preserves completed with deployed donor",
			mode:            "action",
			donorStatus:     "deployed",
			targetStatus:    "completed",
			targetRepo:      "aerial",
			targetPR:        101,
			wantStatus:      "completed",
			wantBanner:      "",
			wantJobs:        nil,
			wantCIChecks:    false,
			wantCompletedAt: true,
		},
	}
}

func donorHydrationCases() []donorHydrationCase {
	return append(donorHydrationPRCases(), donorHydrationActionCases()...)
}

func TestPostgresVanadisStore_AttachChunkPR_DonorHydration(t *testing.T) {
	t.Parallel()
	stamp := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	donorCIChecks := map[string]string{"ci": "passed"}
	donorRollout := &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}
	for _, tc := range donorHydrationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fix, req := initHydrationFixture(tc, stamp, donorCIChecks, donorRollout)
			st, pool := fix.open(t)
			bID, cID, thID, err := st.AttachChunkPR(context.Background(), req)
			if err != nil {
				t.Fatalf("AttachChunkPR failed: %v", err)
			}
			if bID != "b-target" || cID != "c-target" || thID != "thread-target" {
				t.Fatalf("AttachChunkPR returned (%q, %q, %q), want (b-target, c-target, thread-target)", bID, cID, thID)
			}

			got := loadChunk(t, pool, "c-target")
			checkHydratedChunkFields(t, got, tc, donorCIChecks)
			checkHydratedBuild(t, pool, tc.wantParentStatus)
		})
	}
}
