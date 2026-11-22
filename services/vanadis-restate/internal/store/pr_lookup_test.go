package store

// PR lookup tests moved with the production methods they cover.

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
)

func TestPostgresVanadisStore_LookupPRChunk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		db        dbScript
		wantErrIs error
		wantRoute BuildRoute
	}{
		{name: "not found", db: dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "success", db: dbScript{Row: sqltest.ValuesRow("c1", "b1", 0, "medium", false)}, wantRoute: BuildRoute{BuildID: "b1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			route, _, err := st.LookupPRChunk(context.Background(), "aerial", 123)
			if !assertStoreErr(t, err, tc.wantErrIs, false) && route != tc.wantRoute {
				t.Errorf("LookupPRChunk = %+v, want %+v", route, tc.wantRoute)
			}
		})
	}
}

func TestPostgresVanadisStore_LookupChunkBuildID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		db        dbScript
		wantErrIs error
		wantRoute BuildRoute
	}{
		{name: "not found", db: dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "success", db: dbScript{Row: sqltest.ValuesRow("b1")}, wantRoute: BuildRoute{BuildID: "b1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			route, err := st.LookupChunkBuildID(context.Background(), "c1")
			if !assertStoreErr(t, err, tc.wantErrIs, false) && route != tc.wantRoute {
				t.Errorf("LookupChunkBuildID = %+v, want %+v", route, tc.wantRoute)
			}
		})
	}
}

func TestPostgresVanadisStore_BannerAndLookups(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	st, _ := open(nilDB)
	if err := st.UpdateChunkBannerAndStatus(ctx, "c1", "status", "banner"); err == nil {
		t.Error("expected error with nilDB")
	}
	storeActive, _ := open(dbScript{ExecTag: "UPDATE 0"})
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
	storeFindErr, _ := open(dbScript{Row: sqltest.ErrRow(errors.New("db find err"))})
	if _, err := storeFindErr.GetChunkByPR(ctx, "aerial", 101); err == nil {
		t.Error("expected error when findChunkPRMatch fails")
	}

	storeFailCancelledBuild, _ := open(dbScript{Row: sqltest.FailRow("running", "cancelled"), ExecTag: "UPDATE 1"})
	if _, _, failed, err := storeFailCancelledBuild.FailChunk(ctx, "c1", "reason", ""); err != nil || failed {
		t.Errorf("FailChunk on cancelled build: failed=%v, err=%v", failed, err)
	}
}

func TestPostgresVanadisStore_StatusAndRolloutUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storeNil, _ := open(nilDB)
	if err := storeNil.RecordPRRolloutState(ctx, "c1", "open", nil); err == nil {
		t.Error("expected error with nil db")
	}
	storeActive, _ := open(dbScript{ExecTag: "UPDATE 0"})
	if err := storeActive.RecordPRRolloutState(ctx, "", "open", nil); err == nil {
		t.Error("expected error with empty chunkID")
	}
	if err := storeActive.RecordPRRolloutState(ctx, "c1", "open", &pubsub.PRRolloutState{}); !errors.Is(err, ErrChunkNotFound) {
		t.Errorf("expected ErrChunkNotFound, got %v", err)
	}
	storeRolloutOK, _ := open(dbScript{ExecTag: "UPDATE 1"})
	if err := storeRolloutOK.RecordPRRolloutState(ctx, "c1", "open", &pubsub.PRRolloutState{}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if _, err := storeNil.LookupChunkBuildID(ctx, "c1"); err == nil {
		t.Error("expected error with nil db")
	}
	if _, err := storeActive.LookupChunkBuildID(ctx, ""); err == nil {
		t.Error("expected error with empty chunkID")
	}

	storeExecErr, _ := open(dbScript{FailExec: "UPDATE vanadis_chunks", ExecErr: errors.New("exec error")})
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
		{name: "nil db", threadID: "123", repo: "aerial", db: nilDB, wantErr: true},
		{name: "empty thread id", threadID: "", repo: "aerial", db: dbScript{}, wantErrIs: ErrChunkNotFound},
		{name: "empty repo", threadID: "123", repo: "", db: dbScript{}, wantErrIs: ErrChunkNotFound},
		{name: "query error", threadID: "123", repo: "aerial", db: dbScript{QueryErr: errors.New("query failed")}, wantErr: true},
		{name: "scan error", threadID: "123", repo: "aerial", db: dbScript{Chunks: []model.VanadisChunk{{BuildID: "b1", ID: "c1"}}, ScanErr: errors.New("scan failed")}, wantErr: true},
		{name: "rows error", threadID: "123", repo: "aerial", db: dbScript{Chunks: []model.VanadisChunk{{BuildID: "b1", ID: "c1"}}, RowsErr: errors.New("rows failed")}, wantErr: true},
		{name: "0 rows not found", threadID: "123", repo: "aerial", db: dbScript{Chunks: []model.VanadisChunk{}}, wantErrIs: ErrChunkNotFound},
		{name: "1 row unambiguous match", threadID: "123", repo: "aerial", db: dbScript{
			Chunks: []model.VanadisChunk{{BuildID: "b1", ID: "c1"}},
		}, wantBuild: "b1", wantChunk: "c1"},
		{name: "2 rows ambiguous rejection", threadID: "123", repo: "aerial", db: dbScript{
			Chunks: []model.VanadisChunk{{BuildID: "b1", ID: "c1"}, {BuildID: "b1", ID: "c2"}},
		}, wantErrIs: ErrChunkNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			route, cID, err := st.LookupUnattachedChunkByThreadAndRepo(ctx, tc.threadID, tc.repo)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if route.BuildID != tc.wantBuild || cID != tc.wantChunk {
				t.Fatalf("LookupUnattachedChunkByThreadAndRepo = (%+v, %q), want (%q, %q)", route, cID, tc.wantBuild, tc.wantChunk)
			}
		})
	}
}
