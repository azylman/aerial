package store

// Status and action-completion store tests, moved with the production
// methods they cover from the root package. Assertions unchanged.

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
)

func actionChunkRow(mode, cStatus string) sqltest.Values {
	return sqltest.ValuesRow("c1", "b1", 0, mode, "Action Chunk", cStatus, 0)
}

func actionBuildRow(bStatus string) sqltest.Values {
	return sqltest.ValuesRow("b1", "thread-1", 2, "Build 1", "Objective 1", bStatus, "msg-1")
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
	tests := []struct {
		name      string
		id        string
		db        dbScript
		wantErrIs error
		wantErr   bool
	}{
		{name: "nil db", id: "b1", db: nilDB, wantErr: true},
		{name: "empty id", id: "", wantErr: true},
		{name: "exec error", id: "b1", db: dbScript{ExecErr: errors.New("exec error")}, wantErr: true},
		{name: "not found", id: "b1", db: dbScript{ExecTag: "UPDATE 0"}, wantErrIs: ErrBuildNotFound},
		{name: "success", id: "b1", db: dbScript{ExecTag: "UPDATE 1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			assertStoreErr(t, st.UpdateBuildStatus(context.Background(), tc.id, "running"), tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestPostgresVanadisStore_CompareAndSwapBuildStatusMessageID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		stored           map[string]string
		db               dbScript
		id, oldID, newID string
		wantSwapped      bool
		wantErr          bool
		wantStored       map[string]string
	}{
		{name: "match from empty", stored: map[string]string{"b1": ""}, id: "b1", oldID: "", newID: "m1", wantSwapped: true, wantStored: map[string]string{"b1": "m1"}},
		{name: "match from previous id", stored: map[string]string{"b1": "m0"}, id: "b1", oldID: "m0", newID: "m1", wantSwapped: true, wantStored: map[string]string{"b1": "m1"}},
		{name: "another writer stored a different id", stored: map[string]string{"b1": "m2"}, id: "b1", oldID: "m0", newID: "m1", wantStored: map[string]string{"b1": "m2"}},
		{name: "unknown build", stored: map[string]string{"b1": "m0"}, id: "b9", oldID: "m0", newID: "m1", wantStored: map[string]string{"b1": "m0"}},
		{name: "trims arguments", stored: map[string]string{"b1": "m0"}, id: " b1 ", oldID: " m0 ", newID: " m1 ", wantSwapped: true, wantStored: map[string]string{"b1": "m1"}},
		{name: "nil db", db: nilDB, id: "b1", newID: "m1", wantErr: true},
		{name: "empty build id", stored: map[string]string{}, id: " ", newID: "m1", wantErr: true, wantStored: map[string]string{}},
		{name: "empty new id", stored: map[string]string{"b1": "m0"}, id: "b1", oldID: "m0", newID: " ", wantErr: true, wantStored: map[string]string{"b1": "m0"}},
		{name: "exec error", db: dbScript{ExecErr: errors.New("exec error")}, id: "b1", newID: "m1", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			script := tc.db
			script.StatusMessageIDs = tc.stored
			st, _ := open(script)
			swapped, err := st.CompareAndSwapBuildStatusMessageID(context.Background(), tc.id, tc.oldID, tc.newID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CompareAndSwapBuildStatusMessageID err = %v, wantErr %v", err, tc.wantErr)
			}
			if swapped != tc.wantSwapped {
				t.Errorf("swapped = %v, want %v", swapped, tc.wantSwapped)
			}
			if tc.wantStored != nil && !maps.Equal(tc.stored, tc.wantStored) {
				t.Errorf("stored = %v, want %v", tc.stored, tc.wantStored)
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
		{name: "nil receiver", chunkID: "c1", db: nilReceiver, wantErr: true},
		{name: "nil db", chunkID: "c1", db: nilDB, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "exec error", chunkID: "c1", db: dbScript{FailExec: "UPDATE vanadis_chunks", ExecErr: errors.New("exec error")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{ExecTag: "UPDATE 0"}, wantErrIs: ErrChunkNotFound},
		{name: "reserves chunk successfully", chunkID: "c1", db: dbScript{ExecTag: "UPDATE 1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
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
		{name: "nil receiver", chunkID: "c1", db: nilReceiver, wantErr: true},
		{name: "nil db", chunkID: "c1", db: nilDB, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: dbScript{BeginErr: errors.New("begin tx err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: dbScript{Row: sqltest.ErrRow(errors.New("select chunk err"))}, wantErr: true},
		{name: "build not found", chunkID: "c1", db: dbScript{Row: running, BuildRow: sqltest.ErrRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select build error", chunkID: "c1", db: dbScript{Row: running, BuildRow: sqltest.ErrRow(errors.New("db build error"))}, wantErr: true},
		{name: "pr mode chunk is a matched no-op", chunkID: "c1", db: dbScript{Row: actionChunkRow("pr", "running"), BuildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "completed action chunk is a matched no-op", chunkID: "c1", db: dbScript{Row: actionChunkRow("action", "completed"), BuildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "failed action chunk is a matched no-op", chunkID: "c1", db: dbScript{Row: actionChunkRow("action", "failed"), BuildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "cancelled parent rejects success", chunkID: "c1", db: dbScript{Row: running, BuildRow: actionBuildRow("cancelled")}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "cancelled"}},
		{name: "missing retry identity", chunkID: "c1", dispatchID: "", db: dbScript{Row: sqltest.ValuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), BuildRow: runningBuild, CountRow: sqltest.ValuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: false, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "stale retry identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{Row: sqltest.ValuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), BuildRow: runningBuild, CountRow: sqltest.ValuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: false, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "matching retry identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: dbScript{Row: sqltest.ValuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), BuildRow: runningBuild, CountRow: sqltest.ValuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "matching resume identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1-resume-3", db: dbScript{Row: sqltest.ValuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), BuildRow: runningBuild, CountRow: sqltest.ValuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "mismatched first identity", chunkID: "c1", dispatchID: "wrong", db: dbScript{Row: sqltest.ValuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 0), BuildRow: runningBuild, CountRow: sqltest.ValuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: false, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "remaining count failure", chunkID: "c1", db: dbScript{Row: running, BuildRow: runningBuild, CountRow: sqltest.ErrRow(errors.New("count unavailable"))}, wantErr: true},
		{name: "completion commit failure", chunkID: "c1", db: dbScript{Row: running, BuildRow: runningBuild, CountRow: sqltest.ValuesRow(1), CommitErr: errors.New("commit unavailable")}, wantErr: true},
		{name: "rejected completion commit failure", chunkID: "c1", db: dbScript{Row: actionChunkRow("action", "completed"), BuildRow: runningBuild, CommitErr: errors.New("commit unavailable")}, wantErr: true},

		{name: "update chunk error", chunkID: "c1", db: dbScript{Row: running, BuildRow: runningBuild, FailExec: "UPDATE vanadis_chunks", ExecErr: errors.New("update action chunk err")}, wantErr: true},
		{name: "completes action chunk and marks build completed when no remaining chunks", chunkID: "c1", db: dbScript{Row: running, BuildRow: runningBuild, CountRow: sqltest.ValuesRow(0), Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "action", Repo: "aerial", Title: "T", Status: "completed"}}}, want: advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: 1, BuildStatus: "completed", Chunks: 1}},
		{name: "complete build error in finishActionBuild", chunkID: "c1", db: dbScript{Row: running, BuildRow: runningBuild, CountRow: sqltest.ValuesRow(0), FailExec: "UPDATE vanadis_builds", ExecErr: errors.New("upd build err")}, wantErr: true},
		{name: "query chunks error in finishActionBuild", chunkID: "c1", db: dbScript{Row: running, BuildRow: runningBuild, CountRow: sqltest.ValuesRow(0), QueryErr: errors.New("query err")}, wantErr: true},
		{name: "completes action chunk", chunkID: "c1", db: dbScript{Row: running, BuildRow: runningBuild, CountRow: sqltest.ValuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.db.BuildRow == nil {
				tc.db.BuildRow = runningBuild
			}
			st, _ := open(tc.db)
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
	running := sqltest.FailRow("running", "running")
	tests := []struct {
		name       string
		chunkID    string
		db         dbScript
		wantErrIs  error
		wantErr    bool
		wantFailed bool
		wantBuild  string
		dispatchID string
	}{
		{name: "nil receiver", chunkID: "c1", db: nilReceiver, wantErr: true},
		{name: "nil db", chunkID: "c1", db: nilDB, wantErr: true},
		{name: "empty chunk id", chunkID: "", wantErr: true},
		{name: "whitespace chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: dbScript{BeginErr: errors.New("begin tx err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: dbScript{Row: sqltest.ErrRow(errors.New("select chunk fail err"))}, wantErr: true},
		{name: "already terminal chunk is a no-op", chunkID: "c1", db: dbScript{Row: sqltest.FailRow("failed", "failed")}},
		{name: "completed chunk is a no-op", chunkID: "c1", db: dbScript{Row: sqltest.FailRow("completed", "running")}},
		{name: "cancelled chunk is a no-op", chunkID: "c1", db: dbScript{Row: sqltest.FailRow("cancelled", "running")}},
		{name: "update chunk error", chunkID: "c1", db: dbScript{Row: running, FailExec: "UPDATE vanadis_chunks", ExecErr: errors.New("update chunk fail err")}, wantErr: true},
		{name: "commit error", chunkID: "c1", db: dbScript{Row: running, CommitErr: errors.New("commit fail err")}, wantErr: true},
		{name: "retry missing identity", chunkID: "c1", db: dbScript{Row: sqltest.FailRow("running", "running", 1)}},
		{name: "retry old identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{Row: sqltest.FailRow("running", "running", 1)}},
		{name: "retry current identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: dbScript{Row: sqltest.FailRow("running", "running", 1)}, wantFailed: true},
		{name: "parent lock error", chunkID: "c1", db: dbScript{Routes: []sqlRoute{{Match: "SELECT id FROM vanadis_builds", Row: sqltest.ErrRow(context.Canceled)}}}, wantErrIs: context.Canceled},
		{name: "stale identity", chunkID: "c1", dispatchID: "stale", db: dbScript{Row: running}},
		{name: "fails running chunk and keeps build running", chunkID: "c1", db: dbScript{Row: running}, wantFailed: true},
		{name: "fails running chunk of failed build", chunkID: "c1", db: dbScript{Row: sqltest.FailRow("running", "failed")}, wantFailed: true, wantBuild: "failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			b, c, failed, err := st.FailChunk(context.Background(), tc.chunkID, "unrecoverable failure", tc.dispatchID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if failed != tc.wantFailed {
				t.Fatalf("FailChunk failed = %v, want %v", failed, tc.wantFailed)
			}
			wantBuild := tc.wantBuild
			if wantBuild == "" {
				wantBuild = "running"
			}
			if tc.wantFailed && (b.Status != wantBuild || c.Status != "failed") {
				t.Errorf("FailChunk statuses = build %q chunk %q, want %s/failed", b.Status, c.Status, wantBuild)
			}
		})
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
