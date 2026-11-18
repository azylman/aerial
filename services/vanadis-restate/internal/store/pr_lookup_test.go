package store

// PR lookup tests moved with the production methods they cover.

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

func TestPostgresVanadisStore_LookupsAndAppend(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		db        dbScript
		wantErr   bool
		wantErrIs error
		wantBuild string
	}{
		{name: "lookup pr chunk not found", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "lookup pr chunk success", db: dbScript{row: valuesRow("c1", "b1", 0, "medium", false)}, wantBuild: "b1"},
		{name: "lookup chunk build id not found", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "lookup chunk build id success", db: dbScript{row: valuesRow("b1")}, wantBuild: "b1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			switch tc.name {
			case "lookup pr chunk not found", "lookup pr chunk success":
				bID, _, err := st.LookupPRChunk(context.Background(), "aerial", 123)
				if !assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) && bID != tc.wantBuild {
					t.Errorf("LookupPRChunk = %q, want %q", bID, tc.wantBuild)
				}
			case "lookup chunk build id not found", "lookup chunk build id success":
				bID, err := st.LookupChunkBuildID(context.Background(), "c1")
				if !assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) && bID != tc.wantBuild {
					t.Errorf("LookupChunkBuildID = %q, want %q", bID, tc.wantBuild)
				}
			}
		})
	}
}

func TestPostgresVanadisStore_BannerAndLookups(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	st, _ := dbScript{bare: nilDB}.open()
	if err := st.UpdateChunkBannerAndStatus(ctx, "c1", "status", "banner"); err == nil {
		t.Error("expected error with nilDB")
	}
	storeActive, _ := dbScript{execTag: "UPDATE 0"}.open()
	if err := storeActive.UpdateChunkBannerAndStatus(ctx, "", "status", "banner"); err == nil {
		t.Error("expected error with empty chunkID")
	}
	if _, err := st.GetChunkByPR(ctx, "aerial", 101); err == nil {
		t.Error("expected error with nil db")
	}
	if lookup, err := storeActive.GetChunkByPR(ctx, "", 101); err != nil || lookup.Matched {
		t.Errorf("expected unmatched with empty repo: %+v, %v", lookup, err)
	}
	if lookup, err := storeActive.GetChunkByPR(ctx, "aerial", 0); err != nil || lookup.Matched {
		t.Errorf("expected unmatched with 0 pr: %+v, %v", lookup, err)
	}
	storeFindErr, _ := dbScript{row: errRow(errors.New("db find err"))}.open()
	if _, err := storeFindErr.GetChunkByPR(ctx, "aerial", 101); err == nil {
		t.Error("expected error when findChunkPRMatch fails")
	}

	storeFailCancelledBuild, _ := dbScript{row: failRow("running", "cancelled"), execTag: "UPDATE 1"}.open()
	if _, _, failed, err := storeFailCancelledBuild.FailChunk(ctx, "c1", "reason", ""); err != nil || failed {
		t.Errorf("FailChunk on cancelled build: failed=%v, err=%v", failed, err)
	}
}

func TestPostgresVanadisStore_StatusAndRolloutUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storeNil, _ := dbScript{bare: nilDB}.open()
	if err := storeNil.RecordPRRolloutState(ctx, "c1", "open", nil); err == nil {
		t.Error("expected error with nil db")
	}
	storeActive, _ := dbScript{execTag: "UPDATE 0"}.open()
	if err := storeActive.RecordPRRolloutState(ctx, "", "open", nil); err == nil {
		t.Error("expected error with empty chunkID")
	}
	if err := storeActive.RecordPRRolloutState(ctx, "c1", "open", &pubsub.PRRolloutState{}); !errors.Is(err, ErrChunkNotFound) {
		t.Errorf("expected ErrChunkNotFound, got %v", err)
	}
	storeRolloutOK, _ := dbScript{execTag: "UPDATE 1"}.open()
	if err := storeRolloutOK.RecordPRRolloutState(ctx, "c1", "open", &pubsub.PRRolloutState{}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if _, err := storeNil.LookupChunkBuildID(ctx, "c1"); err == nil {
		t.Error("expected error with nil db")
	}
	if _, err := storeActive.LookupChunkBuildID(ctx, ""); err == nil {
		t.Error("expected error with empty chunkID")
	}

	storeExecErr, _ := dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("exec error")}.open()
	if err := storeExecErr.RecordPRRolloutState(ctx, "c1", "open", nil); err == nil {
		t.Error("expected error on exec failure")
	}
	if err := storeExecErr.UpdateChunkBannerAndStatus(ctx, "c1", "status", "banner"); err == nil {
		t.Error("expected error on exec failure")
	}
}

func TestPostgresVanadisStore_LookupUnattachedChunkByThreadAndRepo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name      string
		threadID  string
		repo      string
		db        dbScript
		wantBuild string
		wantChunk string
		wantErrIs error
		wantErr   bool
	}{
		{name: "nil db", threadID: "123", repo: "aerial", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty thread id", threadID: "", repo: "aerial", db: dbScript{}, wantErrIs: ErrChunkNotFound},
		{name: "empty repo", threadID: "123", repo: "", db: dbScript{}, wantErrIs: ErrChunkNotFound},
		{name: "query error", threadID: "123", repo: "aerial", db: dbScript{queryErr: errors.New("query failed")}, wantErr: true},
		{name: "scan error", threadID: "123", repo: "aerial", db: dbScript{chunks: []model.VanadisChunk{{BuildID: "b1", ID: "c1"}}, scanErr: errors.New("scan failed")}, wantErr: true},
		{name: "rows error", threadID: "123", repo: "aerial", db: dbScript{chunks: []model.VanadisChunk{{BuildID: "b1", ID: "c1"}}, rowsErr: errors.New("rows failed")}, wantErr: true},
		{name: "0 rows not found", threadID: "123", repo: "aerial", db: dbScript{chunks: []model.VanadisChunk{}}, wantErrIs: ErrChunkNotFound},
		{name: "1 row unambiguous match", threadID: "123", repo: "aerial", db: dbScript{
			chunks: []model.VanadisChunk{{BuildID: "b1", ID: "c1"}},
		}, wantBuild: "b1", wantChunk: "c1"},
		{name: "2 rows ambiguous rejection", threadID: "123", repo: "aerial", db: dbScript{
			chunks: []model.VanadisChunk{{BuildID: "b1", ID: "c1"}, {BuildID: "b1", ID: "c2"}},
		}, wantErrIs: ErrChunkNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			bID, cID, err := st.LookupUnattachedChunkByThreadAndRepo(ctx, tc.threadID, tc.repo)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if bID != tc.wantBuild || cID != tc.wantChunk {
				t.Fatalf("LookupUnattachedChunkByThreadAndRepo = (%q, %q), want (%q, %q)", bID, cID, tc.wantBuild, tc.wantChunk)
			}
		})
	}
}
