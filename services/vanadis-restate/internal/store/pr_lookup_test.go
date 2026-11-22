package store

// PR lookup tests moved with the production methods they cover. Every row
// runs the production SQL against the test's private Postgres database.

import (
	"context"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// bmPRChunk is a pr chunk of build b1 in repo with PR number pr.
func bmPRChunk(id string, idx int, repo string, pr int, status string) model.VanadisChunk {
	return model.VanadisChunk{ID: id, BuildID: "b1", ChunkIndex: idx, Mode: "pr", Repo: repo, PRNumber: pr, Status: status, Effort: "high"}
}

// bmUnattached is a started, unattached running pr chunk of build b1.
func bmUnattached(id string, idx int, repo string) model.VanadisChunk {
	return model.VanadisChunk{ID: id, BuildID: "b1", ChunkIndex: idx, Mode: "pr", Repo: repo, Status: "running", StartedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)}
}

// bmBoomModeRowsSQL swaps vanadis_chunks for a view whose mode column raises
// while the WHERE clause evaluates a row with banner "boom", so the lookup
// query prepares but fails mid-execution (rows.Err()).
const bmBoomModeRowsSQL = `CREATE FUNCTION bm_boom(v text, b text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$
BEGIN
	IF b = 'boom' THEN RAISE EXCEPTION 'sabotaged row'; END IF;
	RETURN v;
END $$;
ALTER TABLE vanadis_chunks RENAME TO vanadis_chunks_base;
CREATE VIEW vanadis_chunks AS SELECT id, build_id, chunk_index, bm_boom(mode, banner) AS mode, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, stall_nudges, depends_on, ci_fail_count, ci_checks, rollout_state, banner, started_at, completed_at, created_at, updated_at FROM vanadis_chunks_base;`

// bmNullChunkIDSQL lets a stored chunk id be NULL so scanning it into a
// string fails.
const bmNullChunkIDSQL = `ALTER TABLE vanadis_chunks DROP CONSTRAINT vanadis_chunks_pkey; ALTER TABLE vanadis_chunks ALTER COLUMN id DROP NOT NULL; UPDATE vanadis_chunks SET id = NULL;`

func TestPostgresVanadisStore_LookupPRChunk(t *testing.T) {
	t.Parallel()
	running := bmBuilds("running")
	tests := []struct {
		name      string
		repo      string
		pr        int
		db        bmFixture
		wantErrIs error
		wantErr   bool
		wantRoute BuildRoute
		wantChunk string
	}{
		{name: "nil db", repo: "aerial", pr: 123, db: bmFixture{nilDB: true}, wantErr: true},
		{name: "empty repo", repo: " ", pr: 123, wantErrIs: ErrChunkNotFound},
		{name: "zero pr", repo: "aerial", wantErrIs: ErrChunkNotFound},
		{name: "query error", repo: "aerial", pr: 123, db: bmFixture{closed: true}, wantErr: true},
		{name: "not found", repo: "aerial", pr: 123, db: bmFixture{builds: running}, wantErrIs: ErrChunkNotFound},
		{name: "cancelled build ignored", repo: "aerial", pr: 123, db: bmFixture{builds: bmBuilds("cancelled"), chunks: mutateChunks(bmPRChunk("c1", 0, "aerial", 123, "pr_opened"))}, wantErrIs: ErrChunkNotFound},
		{name: "success", repo: "aerial", pr: 123, db: bmFixture{builds: running, chunks: mutateChunks(bmPRChunk("c1", 0, "aerial", 123, "pr_opened"))}, wantRoute: BuildRoute{BuildID: "b1"}, wantChunk: "c1"},
		{
			name: "qualified spelling matches bare repo", repo: "example-org/aerial", pr: 123,
			db:        bmFixture{builds: running, chunks: mutateChunks(bmPRChunk("c0", 0, "aerial", 7, "merged"), bmPRChunk("c1", 1, "aerial", 123, "pr_opened"))},
			wantRoute: BuildRoute{BuildID: "b1"}, wantChunk: "c1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := bmOpen(t, tc.db)
			route, chunkID, err := st.LookupPRChunk(context.Background(), tc.repo, tc.pr)
			if !assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) && (route != tc.wantRoute || chunkID != tc.wantChunk) {
				t.Errorf("LookupPRChunk = (%+v, %q), want (%+v, %q)", route, chunkID, tc.wantRoute, tc.wantChunk)
			}
		})
	}
}

func TestPostgresVanadisStore_LookupChunkBuildID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		chunkID   string
		db        bmFixture
		wantErrIs error
		wantErr   bool
		wantRoute BuildRoute
	}{
		{name: "nil db", chunkID: "c1", db: bmFixture{nilDB: true}, wantErr: true},
		{name: "empty chunk id", chunkID: " ", wantErr: true},
		{name: "query error", chunkID: "c1", db: bmFixture{closed: true}, wantErr: true},
		{name: "not found", chunkID: "c1", db: bmFixture{builds: bmBuilds("running")}, wantErrIs: ErrChunkNotFound},
		{name: "success", chunkID: " c1 ", db: bmFixture{builds: bmBuilds("running"), chunks: mutateChunks(ch("c1", "pr", "pending"))}, wantRoute: BuildRoute{BuildID: "b1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := bmOpen(t, tc.db)
			route, err := st.LookupChunkBuildID(context.Background(), tc.chunkID)
			if !assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) && route != tc.wantRoute {
				t.Errorf("LookupChunkBuildID = %+v, want %+v", route, tc.wantRoute)
			}
		})
	}
}

func TestPostgresVanadisStore_GetChunkByPR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		repo    string
		pr      int
		db      bmFixture
		wantErr bool
		want    VanadisChunkPRLookup
	}{
		{name: "nil db", repo: "aerial", pr: 101, db: bmFixture{nilDB: true}, wantErr: true},
		{name: "empty repo unmatched", repo: "", pr: 101},
		{name: "zero pr unmatched", repo: "aerial"},
		{name: "find error", repo: "aerial", pr: 101, db: bmFixture{closed: true}, wantErr: true},
		{name: "no chunk unmatched", repo: "aerial", pr: 101, db: bmFixture{builds: bmBuilds("running")}},
		{
			name: "matched with capped effort", repo: "aerial", pr: 101,
			db:   bmFixture{builds: bmBuilds("running"), chunks: mutateChunks(bmPRChunk("c1", 0, "example-org/aerial", 101, "pr_opened"))},
			want: VanadisChunkPRLookup{Matched: true, ChunkID: "c1", BuildID: "b1", Effort: "medium"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := bmOpen(t, tc.db)
			got, err := st.GetChunkByPR(context.Background(), tc.repo, tc.pr)
			if assertStoreErr(t, err, nil, tc.wantErr) {
				return
			}
			if *got != tc.want {
				t.Errorf("GetChunkByPR = %+v, want %+v", *got, tc.want)
			}
		})
	}
}

func TestPostgresVanadisStore_UpdateChunkBannerAndStatus(t *testing.T) {
	t.Parallel()
	running := bmBuilds("running")
	failedBuilds := bmBuilds("failed")
	stamp := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	failedPR := bmPRChunk("c1", 0, "aerial", 123, "failed")
	failedPR.CompletedAt = stamp
	failedPR.Banner = "old banner"
	tests := []struct {
		name, chunkID, status string
		db                    bmFixture
		wantErrIs             error
		wantErr               bool
		// wantStatus/wantBanner are c1's stored values after the call ("" skips).
		wantStatus, wantBanner string
		wantBuildStatus        string
		wantCompletedAtNil     bool
	}{
		{name: "nil db", chunkID: "c1", status: "status", db: bmFixture{nilDB: true}, wantErr: true},
		{name: "empty chunk id", chunkID: "", status: "status", db: bmFixture{builds: running}, wantErr: true},
		{name: "exec failure", chunkID: "c1", status: "status", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "running")), sabotage: failStatement("vanadis_chunks", "UPDATE")}, wantErr: true},
		{name: "missing chunk skipped", chunkID: "c1", status: "status", db: bmFixture{builds: running}, wantErrIs: ErrPRTransitionSkipped},
		{name: "updates status and banner", chunkID: " c1 ", status: " ci_failed ", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "pr_opened"))}, wantStatus: "ci_failed", wantBanner: "banner"},
		{name: "empty status keeps stored status", chunkID: "c1", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "pr_opened"))}, wantStatus: "pr_opened", wantBanner: "banner"},
		{name: "terminal chunk skipped", chunkID: "c1", status: "status", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "completed"))}, wantErrIs: ErrPRTransitionSkipped, wantStatus: "completed"},
		{name: "cancelled build skipped", chunkID: "c1", status: "status", db: bmFixture{builds: bmBuilds("cancelled"), chunks: mutateChunks(ch("c1", "pr", "running"))}, wantErrIs: ErrPRTransitionSkipped, wantStatus: "running"},
		{name: "ci failure after merge skipped", chunkID: "c1", status: "ci_failed", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "merged"))}, wantErrIs: ErrPRTransitionSkipped, wantStatus: "merged"},
		{
			name:    "failed pr chunk transitions to conflict, clears completed_at, and reopens build",
			chunkID: "c1", status: "conflict",
			db:                 bmFixture{builds: failedBuilds, chunks: mutateChunks(failedPR)},
			wantStatus:         "conflict",
			wantBanner:         "banner",
			wantBuildStatus:    "running",
			wantCompletedAtNil: true,
		},
		{
			name:    "failed pr chunk transitions to deploy_failed and reopens build",
			chunkID: "c1", status: "deploy_failed",
			db:                 bmFixture{builds: failedBuilds, chunks: mutateChunks(failedPR)},
			wantStatus:         "deploy_failed",
			wantBanner:         "banner",
			wantBuildStatus:    "running",
			wantCompletedAtNil: true,
		},
		{
			name:    "failed pr chunk transition to ci_failed is skipped",
			chunkID: "c1", status: "ci_failed",
			db:         bmFixture{builds: failedBuilds, chunks: mutateChunks(failedPR)},
			wantErrIs:  ErrPRTransitionSkipped,
			wantStatus: "failed",
			wantBanner: "old banner",
		},
		{
			name:    "failed action chunk transition to conflict is skipped",
			chunkID: "c1", status: "conflict",
			db:         bmFixture{builds: failedBuilds, chunks: mutateChunks(ch("c1", "action", "failed"))},
			wantErrIs:  ErrPRTransitionSkipped,
			wantStatus: "failed",
		},
		{
			name:    "failed pr chunk with failed sibling does not reopen build",
			chunkID: "c1", status: "conflict",
			db:                 bmFixture{builds: failedBuilds, chunks: mutateChunks(failedPR, bmPRChunk("c2", 1, "aerial", 124, "failed"))},
			wantStatus:         "conflict",
			wantBanner:         "banner",
			wantBuildStatus:    "failed",
			wantCompletedAtNil: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := bmOpen(t, tc.db)
			err := st.UpdateChunkBannerAndStatus(context.Background(), tc.chunkID, tc.status, " banner ")
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			if tc.wantStatus == "" {
				return
			}
			if c := loadChunk(t, pool, "c1"); c.Status != tc.wantStatus || c.Banner != tc.wantBanner {
				t.Errorf("stored status/banner = %q/%q, want %q/%q", c.Status, c.Banner, tc.wantStatus, tc.wantBanner)
			} else if tc.wantCompletedAtNil && !c.CompletedAt.IsZero() {
				t.Errorf("stored completed_at = %v, want zero", c.CompletedAt)
			}
			if tc.wantBuildStatus != "" {
				b := loadBuild(t, pool, "b1")
				if b.Status != tc.wantBuildStatus {
					t.Errorf("stored build status = %q, want %q", b.Status, tc.wantBuildStatus)
				}
			}
		})
	}
}

// TestPostgresVanadisStore_FailChunkCancelledBuild pins that failing a
// running action chunk of a cancelled build is a committed no-op.
func TestPostgresVanadisStore_FailChunkCancelledBuild(t *testing.T) {
	t.Parallel()
	st, pool := bmOpen(t, bmFixture{builds: bmBuilds("cancelled"), chunks: mutateChunks(ch("c1", "action", "running"))})
	if _, _, failed, err := st.FailChunk(context.Background(), "c1", "reason", ""); err != nil || failed {
		t.Errorf("FailChunk on cancelled build: failed=%v, err=%v", failed, err)
	}
	if c := loadChunk(t, pool, "c1"); c.Status != "running" {
		t.Errorf("stored status = %q, want running", c.Status)
	}
}

func TestPostgresVanadisStore_RecordPRRolloutState(t *testing.T) {
	t.Parallel()
	running := bmBuilds("running")
	failedBuilds := bmBuilds("failed")
	failedPR := bmPRChunk("c1", 0, "aerial", 123, "failed")
	failedPR.Banner = "deploy failed"
	eri := &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}
	tests := []struct {
		name, chunkID, status string
		state                 *pubsub.PRRolloutState
		db                    bmFixture
		wantErrIs             error
		wantErr               bool
		// wantStatus is c1's stored status after the call ("" skips).
		wantStatus      string
		wantJobs        []string
		wantBanner      string
		checkBanner     bool
		wantBuildStatus string
	}{
		{name: "nil db", chunkID: "c1", status: "open", db: bmFixture{nilDB: true}, wantErr: true},
		{name: "empty chunk id", chunkID: "", status: "open", db: bmFixture{builds: running}, wantErr: true},
		{name: "missing chunk", chunkID: "c1", status: "open", state: &pubsub.PRRolloutState{}, db: bmFixture{builds: running}, wantErrIs: ErrChunkNotFound},
		{name: "exec failure", chunkID: "c1", status: "open", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "running")), sabotage: failStatement("vanadis_chunks", "UPDATE")}, wantErr: true},
		{name: "records state and status", chunkID: " c1 ", status: " deploying ", state: eri, db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "merged"))}, wantStatus: "deploying", wantJobs: []string{"eri"}},
		{name: "nil state clears rollout and keeps status", chunkID: "c1", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "merged"))}, wantStatus: "merged"},
		{name: "terminal chunk keeps status and state", chunkID: "c1", status: "deploying", state: eri, db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "completed"))}, wantStatus: "completed"},
		{
			name:    "failed pr chunk updates state and status, clears banner, and reopens failed build",
			chunkID: " c1 ", status: " deploying ", state: eri,
			db:              bmFixture{builds: failedBuilds, chunks: mutateChunks(failedPR)},
			wantStatus:      "deploying",
			wantJobs:        []string{"eri"},
			wantBanner:      "",
			checkBanner:     true,
			wantBuildStatus: "running",
		},
		{
			name:    "failed pr chunk without status keeps failed status and banner",
			chunkID: "c1", status: "", state: eri,
			db:              bmFixture{builds: failedBuilds, chunks: mutateChunks(failedPR)},
			wantStatus:      "failed",
			wantJobs:        []string{"eri"},
			wantBanner:      "deploy failed",
			checkBanner:     true,
			wantBuildStatus: "failed",
		},
		{
			name:    "failed action chunk keeps status and state",
			chunkID: "c1", status: "deploying", state: eri,
			db:         bmFixture{builds: failedBuilds, chunks: mutateChunks(ch("c1", "action", "failed"))},
			wantStatus: "failed",
		},
		{
			name:    "failed pr chunk with failed sibling chunk updates status but keeps build failed",
			chunkID: "c1", status: "deploying", state: eri,
			db:              bmFixture{builds: failedBuilds, chunks: mutateChunks(failedPR, bmPRChunk("c2", 1, "aerial", 124, "failed"))},
			wantStatus:      "deploying",
			wantJobs:        []string{"eri"},
			wantBuildStatus: "failed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := bmOpen(t, tc.db)
			err := st.RecordPRRolloutState(context.Background(), tc.chunkID, tc.status, tc.state)
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			if tc.wantStatus == "" {
				return
			}
			c := loadChunk(t, pool, "c1")
			var jobs []string
			if c.RolloutState != nil {
				jobs = c.RolloutState.ExpectedJobs
			}
			if c.Status != tc.wantStatus || len(jobs) != len(tc.wantJobs) || (len(jobs) > 0 && jobs[0] != tc.wantJobs[0]) {
				t.Errorf("stored status/jobs = %q/%v, want %q/%v", c.Status, jobs, tc.wantStatus, tc.wantJobs)
			}
			if tc.checkBanner && c.Banner != tc.wantBanner {
				t.Errorf("stored banner = %q, want %q", c.Banner, tc.wantBanner)
			}
			if tc.wantBuildStatus != "" {
				b := loadBuild(t, pool, "b1")
				if b.Status != tc.wantBuildStatus {
					t.Errorf("stored build status = %q, want %q", b.Status, tc.wantBuildStatus)
				}
			}
		})
	}
}

func TestPostgresVanadisStore_LookupUnattachedChunkByThreadAndRepo(t *testing.T) {
	t.Parallel()
	builds := []model.VanadisBuild{{ID: "b1", ThreadID: "123", Objective: "Obj", Status: "running"}}
	attached := bmUnattached("c-attached", 0, "aerial")
	attached.PRNumber = 9
	unstarted := bmUnattached("c-unstarted", 1, "aerial")
	unstarted.StartedAt = time.Time{}
	boom := bmUnattached("c-boom", 1, "aerial")
	boom.Banner = "boom"
	tests := []struct {
		name      string
		threadID  string
		repo      string
		db        bmFixture
		wantBuild string
		wantChunk string
		wantErrIs error
		wantErr   bool
	}{
		{name: "nil db", threadID: "123", repo: "aerial", db: bmFixture{nilDB: true}, wantErr: true},
		{name: "empty thread id", threadID: "", repo: "aerial", wantErrIs: ErrChunkNotFound},
		{name: "empty repo", threadID: "123", repo: "", wantErrIs: ErrChunkNotFound},
		{name: "query error", threadID: "123", repo: "aerial", db: bmFixture{closed: true}, wantErr: true},
		{name: "scan error", threadID: "123", repo: "aerial", db: bmFixture{builds: builds, chunks: mutateChunks(bmUnattached("c1", 0, "aerial")), sabotage: bmNullChunkIDSQL}, wantErr: true},
		{name: "rows error", threadID: "123", repo: "aerial", db: bmFixture{builds: builds, chunks: mutateChunks(bmUnattached("c1", 0, "aerial"), boom), sabotage: bmBoomModeRowsSQL}, wantErr: true},
		{name: "0 rows not found", threadID: "123", repo: "aerial", db: bmFixture{builds: builds, chunks: mutateChunks(attached, unstarted)}, wantErrIs: ErrChunkNotFound},
		{name: "1 row unambiguous match", threadID: "123", repo: "aerial", db: bmFixture{builds: builds, chunks: mutateChunks(attached, bmUnattached("c1", 2, "aerial"))}, wantBuild: "b1", wantChunk: "c1"},
		{name: "qualified thread and repo spelling", threadID: "thread:123", repo: "example-org/aerial", db: bmFixture{builds: builds, chunks: mutateChunks(bmUnattached("c1", 0, "aerial"))}, wantBuild: "b1", wantChunk: "c1"},
		{name: "2 rows ambiguous rejection", threadID: "123", repo: "aerial", db: bmFixture{builds: builds, chunks: mutateChunks(bmUnattached("c1", 0, "aerial"), bmUnattached("c2", 1, "example-org/aerial"))}, wantErrIs: ErrChunkNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := bmOpen(t, tc.db)
			route, cID, err := st.LookupUnattachedChunkByThreadAndRepo(context.Background(), tc.threadID, tc.repo)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if route.BuildID != tc.wantBuild || cID != tc.wantChunk {
				t.Fatalf("LookupUnattachedChunkByThreadAndRepo = (%+v, %q), want (%q, %q)", route, cID, tc.wantBuild, tc.wantChunk)
			}
		})
	}
}
