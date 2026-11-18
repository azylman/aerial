package store

// Status and action-completion store tests, moved with the production
// methods they cover from the root package. Assertions unchanged.

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

var nilReceiver = &bareStore{}

func actionChunkRow(mode, cStatus string) *genericMockRow {
	return valuesRow("c1", "b1", 0, mode, "Action Chunk", cStatus, 0)
}

func actionBuildRow(bStatus string) *genericMockRow {
	return valuesRow("b1", "thread-1", 2, "Build 1", "Objective 1", bStatus, "msg-1")
}

func failRow(cStatus, bStatus string, generation ...int) *genericMockRow {
	return &genericMockRow{scanFn: func(dest ...any) error {
		now := time.Now()
		nudges, prNumber := 0, 0
		if len(generation) > 0 {
			nudges = generation[0]
		}
		if len(generation) > 1 {
			prNumber = generation[1]
		}
		return scanValues(dest,
			"c1", "b1", 0, "action", "", "Fail Chunk Title", "Instructions", []string{}, []string{}, "medium", []string{}, cStatus, prNumber, sql.NullTime{Time: now, Valid: true}, sql.NullTime{Time: now, Valid: true}, now, now, nudges,
			"b1", "thread-1", 2, "Build 1", "Objective 1", "Context", bStatus, "msg-1", now, now,
		)
	}}
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

func TestPostgresVanadisStore_StatusUpdates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		action    string
		id        string
		val       string
		db        dbScript
		wantErr   bool
		wantErrIs error
	}{
		{name: "chunk status nil db", action: "chunk", id: "c1", val: "running", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "chunk status empty id", action: "chunk", id: "", val: "running", wantErr: true},
		{name: "chunk status empty val", action: "chunk", id: "c1", val: "", wantErr: true},
		{name: "chunk status not found", action: "chunk", id: "c1", val: "running", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrChunkNotFound},
		{name: "chunk status db error", action: "chunk", id: "c1", val: "running", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("db error")}, wantErr: true},
		{name: "chunk status success", action: "chunk", id: "c1", val: "running", db: dbScript{execTag: "UPDATE 1"}},

		{name: "build status nil db", action: "build", id: "b1", val: "running", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "build status empty id", action: "build", id: "", val: "running", wantErr: true},
		{name: "build status not found", action: "build", id: "b1", val: "running", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrBuildNotFound},
		{name: "build status success", action: "build", id: "b1", val: "running", db: dbScript{execTag: "UPDATE 1"}},

		{name: "msg id nil db", action: "msg", id: "b1", val: "m1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "msg id empty id", action: "msg", id: "", val: "m1", wantErr: true},
		{name: "msg id not found", action: "msg", id: "b1", val: "m1", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrBuildNotFound},
		{name: "msg id success", action: "msg", id: "b1", val: "m1", db: dbScript{execTag: "UPDATE 1"}},

		{name: "reserve nil db", action: "reserve", id: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "reserve empty id", action: "reserve", id: "", wantErr: true},
		{name: "reserve db error", action: "reserve", id: "c1", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("db error")}, wantErr: true},
		{name: "reserve not found", action: "reserve", id: "c1", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrChunkNotFound},
		{name: "reserve success", action: "reserve", id: "c1", db: dbScript{execTag: "UPDATE 1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			var err error
			ctx := context.Background()
			switch tc.action {
			case "chunk":
				err = st.UpdateChunkStatus(ctx, tc.id, tc.val)
			case "build":
				err = st.UpdateBuildStatus(ctx, tc.id, tc.val)
			case "msg":
				err = st.UpdateBuildStatusMessageID(ctx, tc.id, tc.val)
			case "reserve":
				err = st.ReserveChunkForLaunch(ctx, tc.id)
			}
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, wantErrIs %v", err, tc.wantErrIs)
				}
				return
			}
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestPostgresVanadisStore_ReserveChunkForLaunch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		chunkID   string
		db        dbScript
		wantErrIs error
		wantErr   bool
	}{
		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "exec error", chunkID: "c1", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("exec error")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrChunkNotFound},
		{name: "reserves chunk successfully", chunkID: "c1", db: dbScript{execTag: "UPDATE 1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			err := st.ReserveChunkForLaunch(context.Background(), tc.chunkID)
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestPostgresVanadisStore_CompleteActionChunk(t *testing.T) {
	t.Parallel()
	running := actionChunkRow("action", "running")
	runningBuild := actionBuildRow("running")
	tests := []struct {
		name       string
		chunkID    string
		dispatchID string
		db         dbScript
		wantErrIs  error
		wantErr    bool
		want       advanceOutcome
	}{
		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: dbScript{beginErr: errors.New("begin tx err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: dbScript{row: errRow(errors.New("select chunk err"))}, wantErr: true},
		{name: "build not found", chunkID: "c1", db: dbScript{row: running, buildRow: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select build error", chunkID: "c1", db: dbScript{row: running, buildRow: errRow(errors.New("db build error"))}, wantErr: true},
		{name: "pr mode chunk is a matched no-op", chunkID: "c1", db: dbScript{row: actionChunkRow("pr", "running"), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "completed action chunk is a matched no-op", chunkID: "c1", db: dbScript{row: actionChunkRow("action", "completed"), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "failed action chunk is a matched no-op", chunkID: "c1", db: dbScript{row: actionChunkRow("action", "failed"), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "cancelled parent rejects success", chunkID: "c1", db: dbScript{row: running, buildRow: actionBuildRow("cancelled")}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "cancelled"}},
		{name: "missing retry identity", chunkID: "c1", dispatchID: "", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: false, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "stale retry identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: false, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "matching retry identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "mismatched first identity", chunkID: "c1", dispatchID: "wrong", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 0), buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: false, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "remaining count failure", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: errRow(errors.New("count unavailable"))}, wantErr: true},
		{name: "completion commit failure", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(1), commitErr: errors.New("commit unavailable")}, wantErr: true},
		{name: "rejected completion commit failure", chunkID: "c1", db: dbScript{row: actionChunkRow("action", "completed"), buildRow: runningBuild, commitErr: errors.New("commit unavailable")}, wantErr: true},

		{name: "update chunk error", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, failExec: "UPDATE vanadis_chunks", execErr: errors.New("update action chunk err")}, wantErr: true},
		{name: "completes action chunk and marks build completed when no remaining chunks", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(0), chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "action", Repo: "aerial", Title: "T", Status: "completed"}}}, want: advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: 1, BuildStatus: "completed", Chunks: 1}},
		{name: "complete build error in finishActionBuild", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(0), failExec: "UPDATE vanadis_builds", execErr: errors.New("upd build err")}, wantErr: true},
		{name: "query chunks error in finishActionBuild", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(0), queryErr: errors.New("query err")}, wantErr: true},
		{name: "completes action chunk", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.db.buildRow == nil {
				tc.db.buildRow = runningBuild
			}
			st, _ := tc.db.open()
			res, err := st.CompleteActionChunk(context.Background(), tc.chunkID, tc.dispatchID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if got := outcomeOf(res); got != tc.want {
				t.Errorf("CompleteActionChunk = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestPostgresVanadisStore_FailChunk(t *testing.T) {
	t.Parallel()
	running := failRow("running", "running")
	tests := []struct {
		name       string
		chunkID    string
		db         dbScript
		wantErrIs  error
		wantErr    bool
		wantFailed bool
		dispatchID string
	}{
		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "", wantErr: true},
		{name: "whitespace chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: dbScript{beginErr: errors.New("begin tx err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: dbScript{row: errRow(errors.New("select chunk fail err"))}, wantErr: true},
		{name: "already terminal chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("failed", "failed")}},
		{name: "completed chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("completed", "running")}},
		{name: "cancelled chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("cancelled", "running")}},
		{name: "update chunk error", chunkID: "c1", db: dbScript{row: running, failExec: "UPDATE vanadis_chunks", execErr: errors.New("update chunk fail err")}, wantErr: true},
		{name: "update build error", chunkID: "c1", db: dbScript{row: running, failExec: "UPDATE vanadis_builds", execErr: errors.New("update build fail err")}, wantErr: true},
		{name: "commit error", chunkID: "c1", db: dbScript{row: running, commitErr: errors.New("commit fail err")}, wantErr: true},
		{name: "retry missing identity", chunkID: "c1", db: dbScript{row: failRow("running", "running", 1)}},
		{name: "retry old identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{row: failRow("running", "running", 1)}},
		{name: "retry current identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: dbScript{row: failRow("running", "running", 1)}, wantFailed: true},
		{name: "parent lock error", chunkID: "c1", db: dbScript{routes: []sqlRoute{{match: "SELECT id FROM vanadis_builds", row: errRow(context.Canceled)}}}, wantErrIs: context.Canceled},
		{name: "stale identity", chunkID: "c1", dispatchID: "stale", db: dbScript{row: running}},
		{name: "fails running chunk and build", chunkID: "c1", db: dbScript{row: running}, wantFailed: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			b, c, failed, err := st.FailChunk(context.Background(), tc.chunkID, "unrecoverable failure", tc.dispatchID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if failed != tc.wantFailed {
				t.Fatalf("FailChunk failed = %v, want %v", failed, tc.wantFailed)
			}
			if tc.wantFailed && (b.Status != "failed" || c.Status != "failed") {
				t.Errorf("FailChunk statuses = build %q chunk %q, want failed/failed", b.Status, c.Status)
			}
		})
	}
}
