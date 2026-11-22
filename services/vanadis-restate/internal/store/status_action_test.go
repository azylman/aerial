package store

// Status and action-completion store tests against the real schema.

import (
	"cmp"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// saLosePathAfter returns DDL whose trigger clears search_path for the rest
// of the transaction after an UPDATE of table, so the next not-yet-prepared
// statement fails to resolve its unqualified tables while the UPDATE itself
// succeeds.
func saLosePathAfter(table string) string {
	return fmt.Sprintf(`CREATE FUNCTION sa_lose_path() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('search_path', 'sa_nowhere', true); RETURN NULL; END $$;
CREATE TRIGGER sa_lose_path AFTER UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION sa_lose_path();`, table)
}

// saFailCommitOnBuildReadSQL makes reading a build's status_message_id write
// a side row whose deferred trigger fails COMMIT, so even a transaction that
// only reads and locks rows cannot commit.
const saFailCommitOnBuildReadSQL = failFunctionSQL + `
CREATE TABLE sa_side (n int);
CREATE CONSTRAINT TRIGGER sa_side_commit AFTER INSERT ON sa_side DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pgtest_fail();
CREATE FUNCTION sa_touch(v text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$ BEGIN INSERT INTO sa_side VALUES (1); RETURN v; END $$;
ALTER TABLE vanadis_builds RENAME TO vanadis_builds_base;
CREATE VIEW vanadis_builds AS SELECT id, agent, thread_id, tier, title, objective, shared_context, status, sa_touch(status_message_id) AS status_message_id, quiet, created_at, updated_at FROM vanadis_builds_base;`

// saStamped is a completed action chunk carrying start and completion stamps.
var saStamped = model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Title: "Action Chunk", Instructions: "Instructions", Status: "completed", StartedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), CompletedAt: time.Date(2026, 5, 1, 13, 0, 0, 0, time.UTC)}

// saActionBuild is the parent every completion and failure row seeds.
func saActionBuild(status string) []model.VanadisBuild {
	return []model.VanadisBuild{{ID: "b1", ThreadID: "thread-1", Tier: 2, Title: "Build 1", Objective: "Objective 1", Status: status, StatusMessageID: "msg-1"}}
}

// saChunk is chunk c1 (index 0) of b1; an action chunk with a PR carries the
// repo the schema requires for it.
func saChunk(id string, idx int, mode, status string, generation, pr int) model.VanadisChunk {
	c := model.VanadisChunk{ID: id, BuildID: "b1", ChunkIndex: idx, Mode: mode, Title: "Action Chunk", Instructions: "Instructions", Status: status, StallNudges: generation, PRNumber: pr}
	if mode == "pr" || pr > 0 {
		c.Repo = "aerial"
	}
	return c
}

type advanceOutcome struct {
	Matched, Advanced, BuildCompleted bool
	NextChunkIndex                    int
	BuildStatus                       string
	Chunks                            int
}

func outcomeOf(res *VanadisAdvancementResult) advanceOutcome {
	return advanceOutcome{Matched: res.Matched, Advanced: res.Advanced, BuildCompleted: res.BuildCompleted, NextChunkIndex: res.NextChunkIndex, BuildStatus: res.BuildStatus, Chunks: len(res.Chunks)}
}

func TestPostgresVanadisStore_UpdateBuildStatus(t *testing.T) {
	t.Parallel()
	failedBuild := []model.VanadisBuild{{ID: "b1", ThreadID: "t", Status: "failed"}}
	tests := []struct {
		name      string
		id        string
		db        paFixture
		wantErrIs error
		wantErr   bool
	}{
		{name: "nil db", id: "b1", db: paFixture{nilDB: true}, wantErr: true},
		{name: "empty id", id: "", wantErr: true},
		{name: "exec error", id: "b1", db: paFixture{builds: failedBuild, sabotage: failStatement("vanadis_builds", "UPDATE")}, wantErr: true},
		{name: "not found", id: "b1", wantErrIs: ErrBuildNotFound},
		{name: "success", id: "b1", db: paFixture{builds: failedBuild}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := tc.db.open(t)
			if assertStoreErr(t, st.UpdateBuildStatus(context.Background(), tc.id, "running"), tc.wantErrIs, tc.wantErr) {
				return
			}
			if got := loadBuild(t, pool, "b1").Status; got != "running" {
				t.Errorf("stored status = %q, want running", got)
			}
		})
	}
}

func TestPostgresVanadisStore_CompareAndSwapBuildStatusMessageID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		db               paFixture
		stored           string
		id, oldID, newID string
		wantSwapped      bool
		wantErr          bool
		wantStored       string
	}{
		{name: "match from empty", stored: "", id: "b1", oldID: "", newID: "m1", wantSwapped: true, wantStored: "m1"},
		{name: "match from previous id", stored: "m0", id: "b1", oldID: "m0", newID: "m1", wantSwapped: true, wantStored: "m1"},
		{name: "another writer stored a different id", stored: "m2", id: "b1", oldID: "m0", newID: "m1", wantStored: "m2"},
		{name: "unknown build", stored: "m0", id: "b9", oldID: "m0", newID: "m1", wantStored: "m0"},
		{name: "trims arguments", stored: "m0", id: " b1 ", oldID: " m0 ", newID: " m1 ", wantSwapped: true, wantStored: "m1"},
		{name: "nil db", db: paFixture{nilDB: true}, id: "b1", newID: "m1", wantErr: true},
		{name: "empty build id", stored: "m0", id: " ", newID: "m1", wantErr: true, wantStored: "m0"},
		{name: "empty new id", stored: "m0", id: "b1", oldID: "m0", newID: " ", wantErr: true, wantStored: "m0"},
		{name: "exec error", stored: "m0", db: paFixture{sabotage: failStatement("vanadis_builds", "UPDATE")}, id: "b1", oldID: "m0", newID: "m1", wantErr: true, wantStored: "m0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := tc.db
			fx.builds = []model.VanadisBuild{{ID: "b1", ThreadID: "t", StatusMessageID: tc.stored}}
			st, pool := fx.open(t)
			swapped, err := st.CompareAndSwapBuildStatusMessageID(context.Background(), tc.id, tc.oldID, tc.newID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CompareAndSwapBuildStatusMessageID err = %v, wantErr %v", err, tc.wantErr)
			}
			if swapped != tc.wantSwapped {
				t.Errorf("swapped = %v, want %v", swapped, tc.wantSwapped)
			}
			if pool != nil {
				if got := loadBuild(t, pool, "b1").StatusMessageID; got != tc.wantStored {
					t.Errorf("stored = %q, want %q", got, tc.wantStored)
				}
			}
		})
	}
}

// TestCompareAndSwapBuildStatusMessageID_Concurrent races widget syncs that
// each re-posted the card: exactly one swap from the shared old ID wins and
// its ID is the one stored.
func TestCompareAndSwapBuildStatusMessageID_Concurrent(t *testing.T) {
	t.Parallel()
	const writers = 8
	st, pool := openStore(t)
	seedBuild(t, pool, model.VanadisBuild{ID: "b1", ThreadID: "t", StatusMessageID: "m0"})
	type outcome struct {
		newID   string
		swapped bool
		err     error
	}
	results := make(chan outcome, writers)
	var wg sync.WaitGroup
	for i := range writers {
		newID := fmt.Sprintf("m-%d", i)
		wg.Go(func() {
			swapped, err := st.CompareAndSwapBuildStatusMessageID(context.Background(), "b1", "m0", newID)
			results <- outcome{newID, swapped, err}
		})
	}
	wg.Wait()
	close(results)
	var winners []string
	for r := range results {
		if r.err != nil {
			t.Fatalf("CAS %s: %v", r.newID, r.err)
		}
		if r.swapped {
			winners = append(winners, r.newID)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("winners = %v, want exactly one", winners)
	}
	if got := loadBuild(t, pool, "b1").StatusMessageID; got != winners[0] {
		t.Fatalf("stored = %q, want winner %q", got, winners[0])
	}
}

func TestPostgresVanadisStore_ReserveChunkForLaunch(t *testing.T) {
	t.Parallel()
	withChunk := func(status, sabotage string) paFixture {
		return paFixture{builds: saActionBuild("running"), chunks: []model.VanadisChunk{saChunk("c1", 0, "action", status, 0, 0)}, sabotage: sabotage}
	}
	tests := []struct {
		name       string
		chunkID    string
		db         paFixture
		wantErrIs  error
		wantErr    bool
		wantStatus string
	}{
		{name: "nil receiver", chunkID: "c1", db: paFixture{nilReceiver: true}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: paFixture{nilDB: true}, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "exec error", chunkID: "c1", db: withChunk("pending", failStatement("vanadis_chunks", "UPDATE")), wantErr: true},
		{name: "chunk not found", chunkID: "c1", wantErrIs: ErrChunkNotFound},
		{name: "reserves chunk successfully", chunkID: "c1", db: withChunk("pending", ""), wantStatus: "running"},
		{name: "queued chunk reserved", chunkID: " c1 ", db: withChunk("queued", ""), wantStatus: "running"},
		{name: "launched chunk keeps status", chunkID: "c1", db: withChunk("completed", ""), wantStatus: "completed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := tc.db.open(t)
			if assertStoreErr(t, st.ReserveChunkForLaunch(context.Background(), tc.chunkID), tc.wantErrIs, tc.wantErr) {
				return
			}
			if got := loadChunk(t, pool, "c1"); got.Status != tc.wantStatus || (tc.wantStatus == "running" && got.StartedAt.IsZero()) {
				t.Errorf("stored status=%q started=%v, want %q with start stamp", got.Status, got.StartedAt, tc.wantStatus)
			}
		})
	}
}

func TestPostgresVanadisStore_CompleteActionChunk(t *testing.T) {
	t.Parallel()
	fixture := func(bStatus, sabotage string, chunks ...model.VanadisChunk) paFixture {
		return paFixture{builds: saActionBuild(bStatus), chunks: chunks, sabotage: sabotage}
	}
	running := saChunk("c1", 0, "action", "running", 0, 0)
	retried := saChunk("c1", 0, "action", "running", 1, 0)
	sibling := saChunk("c2", 1, "action", "pending", 0, 0)
	tests := []struct {
		name       string
		chunkID    string
		dispatchID string
		db         paFixture
		wantErrIs  error
		wantErr    bool
		want       advanceOutcome
		// wantChunk and wantBuild are the stored statuses afterwards.
		wantChunk, wantBuild string
	}{
		{name: "nil receiver", chunkID: "c1", db: paFixture{nilReceiver: true}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: paFixture{nilDB: true}, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: paFixture{closed: true}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: fixture("running", ""), wantErrIs: ErrChunkNotFound},
		{name: "chunk removed after parent lock", chunkID: "c1", db: fixture("running", vanishChunkOnBuildReadSQL, running), wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: fixture("running", `ALTER TABLE vanadis_chunks RENAME COLUMN stall_nudges TO stall_x;`, running), wantErr: true},
		{name: "build not found", chunkID: "c1", wantErrIs: ErrChunkNotFound},
		{name: "select build error", chunkID: "c1", db: fixture("running", `ALTER TABLE vanadis_builds RENAME COLUMN tier TO tier_x;`, running), wantErr: true},
		{name: "pr mode chunk is a matched no-op", chunkID: "c1", db: fixture("running", "", saChunk("c1", 0, "pr", "running", 0, 0)), want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}, wantChunk: "running", wantBuild: "running"},
		{name: "completed action chunk is a matched no-op", chunkID: "c1", db: fixture("running", "", saChunk("c1", 0, "action", "completed", 0, 0)), want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}, wantChunk: "completed", wantBuild: "running"},
		{name: "failed action chunk is a matched no-op", chunkID: "c1", db: fixture("running", "", saChunk("c1", 0, "action", "failed", 0, 0)), want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}, wantChunk: "failed", wantBuild: "running"},
		{name: "cancelled parent rejects success", chunkID: "c1", db: fixture("cancelled", "", running), want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "cancelled"}, wantChunk: "running", wantBuild: "cancelled"},
		{name: "missing retry identity", chunkID: "c1", dispatchID: "", db: fixture("running", "", retried, sibling), want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}, wantChunk: "running", wantBuild: "running"},
		{name: "stale retry identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: fixture("running", "", retried, sibling), want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}, wantChunk: "running", wantBuild: "running"},
		{name: "matching retry identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: fixture("running", "", retried, sibling), want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}, wantChunk: "completed", wantBuild: "running"},
		{name: "matching resume identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1-resume-3", db: fixture("running", "", retried, sibling), want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}, wantChunk: "completed", wantBuild: "running"},
		{name: "mismatched first identity", chunkID: "c1", dispatchID: "wrong", db: fixture("running", "", running, sibling), want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}, wantChunk: "running", wantBuild: "running"},
		{name: "remaining count failure", chunkID: "c1", db: fixture("running", saLosePathAfter("vanadis_chunks"), running, sibling), wantErr: true},
		{name: "completion commit failure", chunkID: "c1", db: fixture("running", failCommit("vanadis_chunks", "UPDATE"), running, sibling), wantErr: true},
		{name: "rejected completion commit failure", chunkID: "c1", db: fixture("running", saFailCommitOnBuildReadSQL, saChunk("c1", 0, "action", "completed", 0, 0)), wantErr: true},
		{name: "update chunk error", chunkID: "c1", db: fixture("running", failStatement("vanadis_chunks", "UPDATE"), running), wantErr: true},
		{name: "completes action chunk and marks build completed when no remaining chunks", chunkID: "c1", db: fixture("running", "", running), want: advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: 1, BuildStatus: "completed", Chunks: 1}, wantChunk: "completed", wantBuild: "completed"},
		{name: "complete build error in finishActionBuild", chunkID: "c1", db: fixture("running", failStatement("vanadis_builds", "UPDATE"), running), wantErr: true},
		{name: "query chunks error in finishActionBuild", chunkID: "c1", db: fixture("running", saLosePathAfter("vanadis_builds"), running), wantErr: true},
		{name: "completes action chunk", chunkID: "c1", db: fixture("running", "", running, sibling), want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}, wantChunk: "completed", wantBuild: "running"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := tc.db.open(t)
			res, err := st.CompleteActionChunk(context.Background(), tc.chunkID, tc.dispatchID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if got := outcomeOf(res); got != tc.want {
				t.Errorf("CompleteActionChunk = %+v, want %+v", got, tc.want)
			}
			if res.ThreadID != "thread-1" || res.StatusMessageID != "msg-1" || res.MergedChunkTitle != "Action Chunk" {
				t.Errorf("CompleteActionChunk build fields = %q/%q/%q", res.ThreadID, res.StatusMessageID, res.MergedChunkTitle)
			}
			if got := loadChunk(t, pool, "c1").Status; got != tc.wantChunk {
				t.Errorf("stored chunk status = %q, want %q", got, tc.wantChunk)
			}
			if got := loadBuild(t, pool, "b1").Status; got != tc.wantBuild {
				t.Errorf("stored build status = %q, want %q", got, tc.wantBuild)
			}
		})
	}
}

func TestPostgresVanadisStore_FailChunk(t *testing.T) {
	t.Parallel()
	fixture := func(cStatus, bStatus, sabotage string, generation, pr int) paFixture {
		return paFixture{builds: saActionBuild(bStatus), chunks: []model.VanadisChunk{saChunk("c1", 0, "action", cStatus, generation, pr)}, sabotage: sabotage}
	}
	running := fixture("running", "running", "", 0, 0)
	prOpened := fixture("pr_opened", "running", "", 1, 42)
	tests := []struct {
		name       string
		chunkID    string
		db         paFixture
		wantErrIs  error
		wantErr    bool
		wantFailed bool
		wantBuild  string
		dispatchID string
		// wantStored is the chunk status stored afterwards ("" skips the check).
		wantStored string
	}{
		{name: "nil receiver", chunkID: "c1", db: paFixture{nilReceiver: true}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: paFixture{nilDB: true}, wantErr: true},
		{name: "empty chunk id", chunkID: "", wantErr: true},
		{name: "whitespace chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: paFixture{closed: true}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", wantErrIs: ErrChunkNotFound},
		{name: "chunk removed after parent lock", chunkID: "c1", db: fixture("running", "running", vanishChunkOnBuildReadSQL, 0, 0), wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: fixture("running", "running", `ALTER TABLE vanadis_chunks RENAME COLUMN instructions TO instructions_x;`, 0, 0), wantErr: true},
		{name: "stamped completed chunk is a no-op", chunkID: "c1", db: paFixture{builds: saActionBuild("running"), chunks: []model.VanadisChunk{saStamped}}, wantStored: "completed"},
		{name: "no-op commit error", chunkID: "c1", db: paFixture{builds: saActionBuild("running"), chunks: []model.VanadisChunk{saStamped}, sabotage: saFailCommitOnBuildReadSQL}, wantErr: true, wantStored: "completed"},
		{name: "already terminal chunk is a no-op", chunkID: "c1", db: fixture("failed", "failed", "", 0, 0), wantBuild: "failed", wantStored: "failed"},
		{name: "completed chunk is a no-op", chunkID: "c1", db: fixture("completed", "running", "", 0, 0), wantStored: "completed"},
		{name: "cancelled chunk is a no-op", chunkID: "c1", db: fixture("cancelled", "running", "", 0, 0), wantStored: "cancelled"},
		{name: "update chunk error", chunkID: "c1", db: fixture("running", "running", failStatement("vanadis_chunks", "UPDATE"), 0, 0), wantErr: true, wantStored: "running"},
		{name: "commit error", chunkID: "c1", db: fixture("running", "running", failCommit("vanadis_chunks", "UPDATE"), 0, 0), wantErr: true, wantStored: "running"},
		{name: "retry missing identity", chunkID: "c1", db: fixture("running", "running", "", 1, 0), wantStored: "running"},
		{name: "retry old identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: fixture("running", "running", "", 1, 0), wantStored: "running"},
		{name: "retry current identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: fixture("running", "running", "", 1, 0), wantFailed: true, wantStored: "failed"},
		{name: "parent lock error", chunkID: "c1", db: fixture("running", "running", `ALTER TABLE vanadis_chunks RENAME COLUMN build_id TO build_id_x;`, 0, 0), wantErr: true},
		{name: "stale identity", chunkID: "c1", dispatchID: "stale", db: running, wantStored: "running"},
		{name: "fails running chunk and keeps build running", chunkID: "c1", db: running, wantFailed: true, wantStored: "failed"},
		{name: "fails running chunk of failed build", chunkID: "c1", db: fixture("running", "failed", "", 0, 0), wantFailed: true, wantBuild: "failed", wantStored: "failed"},
		{name: "pr closed without merging", chunkID: "c1", db: prOpened, wantFailed: true, wantStored: "failed"},
		{name: "pr-attached current action", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: prOpened, wantFailed: true, wantStored: "failed"},
		{name: "pr-attached stale action", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: prOpened, wantStored: "pr_opened"},
		{name: "pr-attached ci remediation", chunkID: "c1", dispatchID: "vanadis-c1-cifail-1-abcdef0", db: prOpened, wantFailed: true, wantStored: "failed"},
		{name: "pr-attached conflict remediation", chunkID: "c1", dispatchID: "vanadis-conflict-c1", db: prOpened, wantFailed: true, wantStored: "failed"},
		{name: "deploy failed fences delayed action", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: fixture("deploy_failed", "running", "", 1, 42), wantStored: "deploy_failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := tc.db.open(t)
			b, c, failed, err := st.FailChunk(context.Background(), tc.chunkID, "unrecoverable failure", tc.dispatchID)
			if tc.wantStored != "" {
				defer func() {
					if got := loadChunk(t, pool, "c1").Status; got != tc.wantStored {
						t.Errorf("stored chunk status = %q, want %q", got, tc.wantStored)
					}
				}()
			}
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			saAssertFailed(t, pool, b, c, failed, tc.wantFailed, cmp.Or(tc.wantBuild, "running"))
		})
	}
}

// saAssertFailed checks FailChunk's result and the stored build and chunk.
func saAssertFailed(t *testing.T, pool *pgxpool.Pool, b *model.VanadisBuild, c *model.VanadisChunk, failed, wantFailed bool, wantBuild string) {
	t.Helper()
	if failed != wantFailed {
		t.Fatalf("FailChunk failed = %v, want %v", failed, wantFailed)
	}
	if got := loadBuild(t, pool, "b1").Status; got != wantBuild {
		t.Errorf("stored build status = %q, want %q", got, wantBuild)
	}
	if !wantFailed {
		return
	}
	if b.Status != wantBuild || c.Status != "failed" {
		t.Errorf("FailChunk statuses = build %q chunk %q, want %s/failed", b.Status, c.Status, wantBuild)
	}
	if got := loadChunk(t, pool, "c1").Banner; got != "Chunk 1 failed: unrecoverable failure" {
		t.Errorf("stored banner = %q", got)
	}
}

func TestActionResumeIndex(t *testing.T) {
	t.Parallel()
	gen1 := model.VanadisChunk{ID: "c1", StallNudges: 1}
	tests := []struct {
		name       string
		chunk      model.VanadisChunk
		dispatchID string
		wantK      int
		wantOK     bool
	}{
		{"launch id is resume zero", gen1, "vanadis-c1-turn-1", 0, true},
		{"first resume", gen1, "vanadis-c1-turn-1-resume-1", 1, true},
		{"last resume", gen1, "vanadis-c1-turn-1-resume-12", 12, true},
		{"resume past cap rejected", gen1, "vanadis-c1-turn-1-resume-13", 0, false},
		{"resume zero rejected", gen1, "vanadis-c1-turn-1-resume-0", 0, false},
		{"padded ordinal rejected", gen1, "vanadis-c1-turn-1-resume-01", 0, false},
		{"non numeric ordinal rejected", gen1, "vanadis-c1-turn-1-resume-x", 0, false},
		{"older generation launch rejected", gen1, "vanadis-c1-turn-0", 0, false},
		{"older generation resume rejected", gen1, "vanadis-c1-turn-0-resume-3", 0, false},
		{"other chunk rejected", gen1, "vanadis-c2-turn-1-resume-1", 0, false},
		{"no-pr resume rejected", gen1, "vanadis-c1-nopr-resume-1", 0, false},
		{"empty id rejected", gen1, "", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k, ok := ActionResumeIndex(tc.chunk, tc.dispatchID)
			if k != tc.wantK || ok != tc.wantOK {
				t.Errorf("ActionResumeIndex(%q) = (%d, %v), want (%d, %v)", tc.dispatchID, k, ok, tc.wantK, tc.wantOK)
			}
		})
	}
}

func TestActionResumeDispatchID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		chunk model.VanadisChunk
		k     int
		want  string
	}{
		{"first generation first resume", model.VanadisChunk{ID: "c1"}, 1, "vanadis-c1-turn-0-resume-1"},
		{"retried generation last resume", model.VanadisChunk{ID: "c1", StallNudges: 2}, 12, "vanadis-c1-turn-2-resume-12"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ActionResumeDispatchID(tc.chunk, tc.k); got != tc.want {
				t.Errorf("ActionResumeDispatchID = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestActionCompletionAllowed(t *testing.T) {
	t.Parallel()
	running := model.VanadisChunk{ID: "c1", Mode: "action", Status: "running", StallNudges: 1}
	tests := []struct {
		name        string
		chunk       model.VanadisChunk
		buildStatus string
		dispatchID  string
		want        bool
	}{
		{"launch id accepted", running, "running", "vanadis-c1-turn-1", true},
		{"current resume id accepted", running, "running", "vanadis-c1-turn-1-resume-5", true},
		{"stale generation resume rejected", running, "running", "vanadis-c1-turn-0-resume-5", false},
		{"resume past cap rejected", running, "running", "vanadis-c1-turn-1-resume-13", false},
		{"empty id rejected after retry", running, "running", "", false},
		{"empty id accepted on first generation", model.VanadisChunk{ID: "c1", Mode: "action", Status: "running"}, "running", "", true},
		{"pr chunk rejected", model.VanadisChunk{ID: "c1", Mode: "pr", Status: "running", StallNudges: 1}, "running", "vanadis-c1-turn-1", false},
		{"deployed repair pr accepted", model.VanadisChunk{ID: "c1", Mode: "action", Status: "running", PRNumber: 7, StallNudges: 1}, "running", "vanadis-c1-turn-1-resume-1", true},
		{"repair pr in flight rejected", model.VanadisChunk{ID: "c1", Mode: "action", Status: "deploying", PRNumber: 7, StallNudges: 1}, "running", "vanadis-c1-turn-1-resume-1", false},
		{"failed build accepted", running, "failed", "vanadis-c1-turn-1-resume-1", true},
		{"completed build rejected", running, "completed", "vanadis-c1-turn-1-resume-1", false},
		{"cancelled build rejected", running, "cancelled", "vanadis-c1-turn-1-resume-1", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ActionCompletionAllowed(tc.chunk, tc.buildStatus, tc.dispatchID); got != tc.want {
				t.Errorf("ActionCompletionAllowed = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestActionFailureAllowed(t *testing.T) {
	t.Parallel()
	running := model.VanadisChunk{ID: "c1", Mode: "action", Status: "running", StallNudges: 1}
	repair := model.VanadisChunk{ID: "c1", Mode: "action", Status: "pr_opened", PRNumber: 7, StallNudges: 1}
	tests := []struct {
		name        string
		chunk       model.VanadisChunk
		buildStatus string
		dispatchID  string
		want        bool
	}{
		{"running launch id accepted", running, "running", "vanadis-c1-turn-1", true},
		{"running last resume accepted", running, "running", "vanadis-c1-turn-1-resume-12", true},
		{"running stale resume rejected", running, "running", "vanadis-c1-turn-0-resume-12", false},
		{"repair launch id accepted", repair, "running", "vanadis-c1-turn-1", true},
		{"repair resume id accepted", repair, "running", "vanadis-c1-turn-1-resume-12", true},
		{"repair ci remediation accepted", repair, "running", "vanadis-c1-cifail-abc-1", true},
		{"repair stale resume rejected", repair, "running", "vanadis-c1-turn-0-resume-1", false},
		{"deploying repair rejected", model.VanadisChunk{ID: "c1", Mode: "action", Status: "deploying", PRNumber: 7, StallNudges: 1}, "running", "vanadis-c1-turn-1-resume-1", false},
		{"repair on failed build accepted", repair, "failed", "vanadis-c1-turn-1-resume-1", true},
		{"repair on cancelled build rejected", repair, "cancelled", "vanadis-c1-turn-1-resume-1", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ActionFailureAllowed(tc.chunk, tc.buildStatus, tc.dispatchID); got != tc.want {
				t.Errorf("ActionFailureAllowed = %v, want %v", got, tc.want)
			}
		})
	}
}
