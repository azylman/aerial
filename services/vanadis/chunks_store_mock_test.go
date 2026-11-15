package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestComputeNextChunkIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		chunks     []VanadisChunkSummary
		currentIdx int
		want       int
	}{
		{
			name:       "empty chunks",
			chunks:     nil,
			currentIdx: 0,
			want:       -1,
		},
		{
			name: "all done",
			chunks: []VanadisChunkSummary{
				{ChunkIndex: 0, Status: "completed"},
				{ChunkIndex: 1, Status: "merged"},
				{ChunkIndex: 2, Status: "completed"},
			},
			currentIdx: 0,
			want:       -1,
		},
		{
			name: "next chunk greater than current",
			chunks: []VanadisChunkSummary{
				{ChunkIndex: 0, Status: "completed"},
				{ChunkIndex: 1, Status: "pending"},
				{ChunkIndex: 2, Status: "pending"},
			},
			currentIdx: 0,
			want:       1,
		},
		{
			name: "fallback when all remaining are lower than current",
			chunks: []VanadisChunkSummary{
				{ChunkIndex: 0, Status: "pending"},
				{ChunkIndex: 1, Status: "completed"},
			},
			currentIdx: 1,
			want:       0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := computeNextChunkIndex(tc.chunks, tc.currentIdx)
			if got != tc.want {
				t.Fatalf("computeNextChunkIndex() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestValidateAppendChunksInputs(t *testing.T) {
	t.Parallel()

	assertErr := func(err error, wantSub string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), wantSub) {
			t.Fatalf("expected error containing %q, got %v", wantSub, err)
		}
	}

	assertErr(validateAppendChunksInputs("", []CreateChunkInput{{Title: "T", Instructions: "I"}}), "build id and at least one chunk")
	assertErr(validateAppendChunksInputs("b1", nil), "build id and at least one chunk")
	assertErr(validateAppendChunksInputs("b1", []CreateChunkInput{{Title: "", Instructions: "I"}}), "title and instructions cannot be empty")
	assertErr(validateAppendChunksInputs("b1", []CreateChunkInput{{Title: "T", Instructions: ""}}), "title and instructions cannot be empty")
	assertErr(validateAppendChunksInputs("b1", []CreateChunkInput{{ID: "invalid ID with spaces", Title: "T", Instructions: "I"}}), "invalid id")
	assertErr(validateAppendChunksInputs("b1", []CreateChunkInput{{Title: "T", Instructions: "I", Mode: "action", Repo: "aerial"}}), "action chunk must have empty repo")
	assertErr(validateAppendChunksInputs("b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial", Effort: "gigantic"}}), "invalid chunk effort")

	if err := validateAppendChunksInputs("b1", []CreateChunkInput{{Title: "T", Instructions: "I", Mode: "pr", Repo: "aerial"}}); err != nil {
		t.Fatalf("expected valid append inputs, got %v", err)
	}
}

func TestResolveAppendedChunkID(t *testing.T) {
	t.Parallel()

	assertErr := func(err error, wantSub string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), wantSub) {
			t.Fatalf("expected error containing %q, got %v", wantSub, err)
		}
	}

	used := map[string]bool{"c-used": true, "b1-chunk-1": true}
	id, err := resolveAppendedChunkID("b1", "c-custom", 0, used)
	if err != nil || id != "c-custom" {
		t.Fatalf("expected c-custom, got %s (err: %v)", id, err)
	}
	_, err = resolveAppendedChunkID("b1", "c-used", 0, used)
	assertErr(err, "duplicate chunk id")

	genID, err := resolveAppendedChunkID("b1", "", 0, used)
	if err != nil || genID != "b1-chunk-2" {
		t.Fatalf("expected b1-chunk-2, got %s (err: %v)", genID, err)
	}
}

func TestBuildAppendedChunk(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	usedMap := map[string]bool{}
	cPR, err := buildAppendedChunk("b1", CreateChunkInput{
		Title:        "Title PR",
		Instructions: "Instr",
		Mode:         "pr",
		Repo:         "azylman/aerial",
		DependsOn:    []string{"dep-1"},
	}, 1, "", usedMap, now)
	if err != nil {
		t.Fatalf("buildAppendedChunk pr err: %v", err)
	}
	if cPR.Mode != "pr" || cPR.Repo != "azylman/aerial" || len(cPR.DependsOn) != 1 || cPR.DependsOn[0] != "dep-1" {
		t.Fatalf("unexpected chunk PR: %+v", cPR)
	}

	cAction, err := buildAppendedChunk("b1", CreateChunkInput{
		Title:        "Title Action",
		Instructions: "Instr Action",
		Mode:         "action",
	}, 2, "prev-id", usedMap, now)
	if err != nil {
		t.Fatalf("buildAppendedChunk action err: %v", err)
	}
	if cAction.Mode != "action" || cAction.Repo != "" || len(cAction.DependsOn) != 1 || cAction.DependsOn[0] != "prev-id" {
		t.Fatalf("unexpected chunk Action: %+v", cAction)
	}

	cDefaultDeps, err := buildAppendedChunk("b1", CreateChunkInput{
		Title:        "Title Default",
		Instructions: "Instr Default",
		Mode:         "action",
	}, 3, "", usedMap, now)
	if err != nil {
		t.Fatalf("buildAppendedChunk default err: %v", err)
	}
	if len(cDefaultDeps.DependsOn) != 0 {
		t.Fatalf("expected empty DependsOn, got %v", cDefaultDeps.DependsOn)
	}

	// Errors
	_, err = buildAppendedChunk("b1", CreateChunkInput{ID: "c-used"}, 0, "", map[string]bool{"c-used": true}, now)
	if err == nil {
		t.Fatal("expected error on duplicate chunk ID")
	}
	_, err = buildAppendedChunk("b1", CreateChunkInput{Mode: "badmode"}, 0, "", usedMap, now)
	if err == nil {
		t.Fatal("expected error on bad mode")
	}
	_, err = buildAppendedChunk("b1", CreateChunkInput{Effort: "huge"}, 0, "", usedMap, now)
	if err == nil {
		t.Fatal("expected error on bad effort")
	}
}

func TestPostgresStore_ResolveAttachChunkPRError_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. Chunk does not exist
	sNotExist := newMockStore(mockDB{
		queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*bool) = false
				return nil
			}}
		},
	})
	err := sNotExist.resolveAttachChunkPRError(ctx, "c1", "aerial", 10)
	if !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("expected ErrChunkNotFound, got %v", err)
	}

	// 2. Repo is empty
	sRepoEmpty := newMockStore(mockDB{
		queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*bool) = true
				return nil
			}}
		},
	})
	err = sRepoEmpty.resolveAttachChunkPRError(ctx, "c1", "", 10)
	if !errors.Is(err, ErrBuildConflict) {
		t.Fatalf("expected ErrBuildConflict, got %v", err)
	}

	// 3. Chunk is cancelled
	callCount := 0
	sCancelled := newMockStore(mockDB{
		queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			callCount++
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*bool) = true
				return nil
			}}
		},
	})
	err = sCancelled.resolveAttachChunkPRError(ctx, "c1", "aerial", 10)
	if !errors.Is(err, ErrBuildConflict) {
		t.Fatalf("expected ErrBuildConflict, got %v", err)
	}

	// 4. Mismatched repo
	checkCount := 0
	sMismatched := newMockStore(mockDB{
		queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			checkCount++
			return &genericMockRow{scanFn: func(dest ...any) error {
				if checkCount == 1 {
					*dest[0].(*bool) = true // exists
				} else {
					*dest[0].(*bool) = false // not cancelled
				}
				return nil
			}}
		},
	})
	err = sMismatched.resolveAttachChunkPRError(ctx, "c1", "aerial", 10)
	if err == nil || !strings.Contains(err.Error(), "cannot attach pr 10 (mismatched repo)") {
		t.Fatalf("expected mismatched repo error, got %v", err)
	}

	// 5. attachToExistingChunk db error (L252)
	modeChecked := false
	sAttachErr := newMockStore(mockDB{
		queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			if strings.Contains(sql, "SELECT mode FROM vanadis_chunks") {
				modeChecked = true
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "pr"
					return nil
				}}
			}
			return &mockRow{err: errors.New("update chunk pr db err")}
		},
	})
	if _, _, _, err := sAttachErr.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 10, Repo: "aerial"}); err == nil {
		t.Fatal("expected error on attach chunk update error")
	}
	if !modeChecked {
		t.Fatal("expected mode check to run")
	}
}

func TestPostgresStore_AttachStandalonePR_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) {
		return nil, errors.New("begin error")
	}})
	// Empty repo
	if _, _, _, err := s.attachStandalonePR(ctx, AttachChunkPRRequest{Repo: ""}); err == nil {
		t.Fatal("expected error on empty repo")
	}
	// Begin error
	if _, _, _, err := s.attachStandalonePR(ctx, AttachChunkPRRequest{Repo: "aerial", PRNumber: 10}); err == nil {
		t.Fatal("expected error on begin error")
	}

	// Lock error
	txLockErr := &mockTx{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag(""), errors.New("lock error")
		},
	}
	sLockErr := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txLockErr, nil }})
	if _, _, _, err := sLockErr.attachStandalonePR(ctx, AttachChunkPRRequest{Repo: "aerial", PRNumber: 10}); err == nil {
		t.Fatal("expected error on lock error")
	}

	// Find query error (not ErrNoRows)
	txFindErr := &mockTx{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag(""), nil
		},
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &genericMockRow{scanFn: func(...any) error {
				return errors.New("query scan error")
			}}
		},
	}
	sFindErr := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txFindErr, nil }})
	if _, _, _, err := sFindErr.attachStandalonePR(ctx, AttachChunkPRRequest{Repo: "aerial", PRNumber: 10}); err == nil {
		t.Fatal("expected error on find query error")
	}

	// Existing match with commit error
	txCommitErr := &mockTx{
		commitErr: errors.New("commit error"),
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag(""), nil
		},
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*string) = "b-exist"
				*dest[1].(*string) = "c-exist"
				*dest[2].(*string) = "th-exist"
				return nil
			}}
		},
	}
	sCommitErr := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txCommitErr, nil }})
	if _, _, _, err := sCommitErr.attachStandalonePR(ctx, AttachChunkPRRequest{Repo: "aerial", PRNumber: 10}); err == nil {
		t.Fatal("expected error on commit error")
	}

	// Existing match success
	txSuccess := &mockTx{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag(""), nil
		},
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*string) = "b-exist"
				*dest[1].(*string) = "c-exist"
				*dest[2].(*string) = "th-exist"
				return nil
			}}
		},
	}
	sSuccess := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txSuccess, nil }})
	bID, cID, thID, err := sSuccess.attachStandalonePR(ctx, AttachChunkPRRequest{Repo: "aerial", PRNumber: 10})
	if err != nil || bID != "b-exist" || cID != "c-exist" || thID != "th-exist" {
		t.Fatalf("unexpected success result: bID=%s cID=%s thID=%s err=%v", bID, cID, thID, err)
	}
}

func TestPostgresStore_CompleteActionChunk_And_FailChunk_MockDB_Errors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var nilStore *PostgresVanadisStore
	if _, err := nilStore.CompleteActionChunk(ctx, "c1"); err == nil {
		t.Fatal("expected error on nil store CompleteActionChunk")
	}
	if _, _, _, err := nilStore.FailChunk(ctx, "c1", "reason"); err == nil {
		t.Fatal("expected error on nil store FailChunk")
	}

	storeEmptyID := newMockStore(mockDB{})
	if _, err := storeEmptyID.CompleteActionChunk(ctx, "  "); err == nil {
		t.Fatal("expected error on empty chunk id CompleteActionChunk")
	}
	if _, _, _, err := storeEmptyID.FailChunk(ctx, "  ", "reason"); err == nil {
		t.Fatal("expected error on empty chunk id FailChunk")
	}

	// Begin error
	storeBeginErr := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) {
		return nil, errors.New("begin error")
	}})
	if _, err := storeBeginErr.CompleteActionChunk(ctx, "c1"); err == nil {
		t.Fatal("expected error on begin error CompleteActionChunk")
	}
	if _, _, _, err := storeBeginErr.FailChunk(ctx, "c1", "reason"); err == nil {
		t.Fatal("expected error on begin error FailChunk")
	}

	// Lock chunk ErrNoRows
	txNoChunk := &mockTx{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &genericMockRow{scanFn: func(...any) error { return pgx.ErrNoRows }}
		},
	}
	storeNoChunk := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txNoChunk, nil }})
	if _, err := storeNoChunk.CompleteActionChunk(ctx, "c1"); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("expected ErrChunkNotFound, got %v", err)
	}
	if _, _, _, err := storeNoChunk.FailChunk(ctx, "c1", "reason"); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("expected ErrChunkNotFound, got %v", err)
	}

	// Lock build ErrNoRows on CompleteActionChunk
	callScan := 0
	txNoBuild := &mockTx{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			callScan++
			return &genericMockRow{scanFn: func(dest ...any) error {
				if callScan == 1 {
					*dest[0].(*string) = "c1"
					*dest[1].(*string) = "b1"
					*dest[2].(*int) = 0
					*dest[3].(*string) = "action"
					*dest[4].(*string) = "Title"
					*dest[5].(*string) = "running"
					return nil
				}
				return pgx.ErrNoRows
			}}
		},
	}
	storeNoBuild := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txNoBuild, nil }})
	if _, err := storeNoBuild.CompleteActionChunk(ctx, "c1"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}
}

func TestPostgresStore_GetBuild_And_ListBuilds_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// GetBuild nil store and empty ID
	var nilStore *PostgresVanadisStore
	if _, _, err := nilStore.GetBuild(ctx, "b1"); err == nil {
		t.Fatal("expected error on nil store GetBuild")
	}
	s := newMockStore(mockDB{})
	if _, _, err := s.GetBuild(ctx, ""); err == nil {
		t.Fatal("expected error on empty build ID")
	}

	// GetBuild queryRow ErrNoRows
	sNoBuild := newMockStore(mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{err: pgx.ErrNoRows}
		},
	})
	if _, _, err := sNoBuild.GetBuild(ctx, "b1"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}

	// GetBuild chunks query error
	sChunksErr := newMockStore(mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{b: &VanadisBuild{ID: "b1", Title: "Build"}}
		},
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return nil, errors.New("chunks query error")
		},
	})
	if _, _, err := sChunksErr.GetBuild(ctx, "b1"); err == nil {
		t.Fatal("expected error on chunks query error")
	}

	// GetBuild success with scanChunks
	sSuccess := newMockStore(mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{b: &VanadisBuild{ID: "b1", Title: "Build"}}
		},
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{items: []VanadisChunk{
				{ID: "c1", BuildID: "b1", Title: "Chunk 1", StartedAt: time.Now(), CompletedAt: time.Now()},
			}}, nil
		},
	})
	b, chs, err := sSuccess.GetBuild(ctx, "b1")
	if err != nil || b == nil || len(chs) != 1 {
		t.Fatalf("unexpected GetBuild result: b=%+v chs=%+v err=%v", b, chs, err)
	}
	if chs[0].CIChecks == nil || chs[0].RolloutState == nil {
		t.Fatalf("expected CIChecks and RolloutState to be unmarshaled, got: %+v", chs[0])
	}

	// ListBuilds nil store
	if _, err := nilStore.ListBuilds(ctx, 10); err == nil {
		t.Fatal("expected error on nil store ListBuilds")
	}

	// ListBuilds query builds error
	sListErr := newMockStore(mockDB{
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return nil, errors.New("query builds error")
		},
	})
	if _, err := sListErr.ListBuilds(ctx, 0); err == nil {
		t.Fatal("expected error on query builds error")
	}

	// ListBuilds empty (0 builds)
	sListEmpty := newMockStore(mockDB{
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{builds: []VanadisBuild{}}, nil
		},
	})
	entries, err := sListEmpty.ListBuilds(ctx, 50)
	if err != nil || len(entries) != 0 {
		t.Fatalf("expected empty entries, got %v err %v", entries, err)
	}

	// ListBuilds chunks query error
	sListChunksErr := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "FROM vanadis_builds") {
				return &mockRows{builds: []VanadisBuild{{ID: "b1", Status: "running"}}}, nil
			}
			return nil, errors.New("query chunks error")
		},
	})
	if _, err := sListChunksErr.ListBuilds(ctx, 50); err == nil {
		t.Fatal("expected error on chunks query error in ListBuilds")
	}

	// ListBuilds success
	sListSuccess := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "FROM vanadis_builds") {
				return &mockRows{builds: []VanadisBuild{
					{ID: "b1", Title: "Build 1", Status: "running"},
					{ID: "b2", Title: "Build 2", Status: "completed"},
				}}, nil
			}
			return &mockRows{items: []VanadisChunk{
				{ID: "c1", BuildID: "b1", Title: "Chunk 1", Status: "running"},
				{ID: "c2", BuildID: "b2", Title: "Chunk 2", Status: "completed"},
			}}, nil
		},
	})
	allEntries, err := sListSuccess.ListBuilds(ctx, 50)
	if err != nil || len(allEntries) != 2 {
		t.Fatalf("expected 2 entries, got %v err %v", allEntries, err)
	}
}

func TestPostgresStore_CreateBuild_And_Updates_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var nilStore *PostgresVanadisStore
	if err := nilStore.CreateBuild(ctx, VanadisBuild{ID: "b1"}, []VanadisChunk{{ID: "c1"}}); err == nil {
		t.Fatal("expected error on nil store CreateBuild")
	}
	s := newMockStore(mockDB{})
	if err := s.CreateBuild(ctx, VanadisBuild{}, []VanadisChunk{{ID: "c1"}}); err == nil {
		t.Fatal("expected error on empty build ID")
	}
	if err := s.CreateBuild(ctx, VanadisBuild{ID: "b1"}, nil); err == nil {
		t.Fatal("expected error on empty chunks")
	}

	// Begin error
	sBeginErr := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return nil, errors.New("begin error") }})
	if err := sBeginErr.CreateBuild(ctx, VanadisBuild{ID: "b1"}, []VanadisChunk{{ID: "c1"}}); err == nil {
		t.Fatal("expected error on begin error")
	}

	// Insert build error
	txInsertBuildErr := &mockTx{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, errors.New("insert build error")
		},
	}
	sInsertBuildErr := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txInsertBuildErr, nil }})
	if err := sInsertBuildErr.CreateBuild(ctx, VanadisBuild{ID: "b1"}, []VanadisChunk{{ID: "c1"}}); err == nil {
		t.Fatal("expected error on insert build error")
	}

	// Insert chunk error
	insertCount := 0
	txInsertChunkErr := &mockTx{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			insertCount++
			if insertCount > 1 {
				return pgconn.CommandTag{}, errors.New("insert chunk error")
			}
			return pgconn.NewCommandTag("INSERT 1"), nil
		},
	}
	sInsertChunkErr := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txInsertChunkErr, nil }})
	if err := sInsertChunkErr.CreateBuild(ctx, VanadisBuild{ID: "b1"}, []VanadisChunk{{ID: "c1"}}); err == nil {
		t.Fatal("expected error on insert chunk error")
	}

	// Commit error
	txCommitErr := &mockTx{
		commitErr: errors.New("commit error"),
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 1"), nil
		},
	}
	sCommitErr := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txCommitErr, nil }})
	if err := sCommitErr.CreateBuild(ctx, VanadisBuild{ID: "b1"}, []VanadisChunk{{ID: "c1"}}); err == nil {
		t.Fatal("expected error on commit error")
	}

	// Success
	txSuccess := &mockTx{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 1"), nil
		},
	}
	sSuccess := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return txSuccess, nil }})
	if err := sSuccess.CreateBuild(ctx, VanadisBuild{ID: "b1", ThreadID: "th-1", Objective: "obj"}, []VanadisChunk{{ID: "c1", Repo: "azylman/aerial", Title: "c1", Instructions: "do c1"}}); err != nil {
		t.Fatalf("expected success on CreateBuild, got %v", err)
	}

	// execUpdateByID via UpdateBuildStatus and UpdateBuildStatusMessageID
	if err := s.UpdateBuildStatus(ctx, "", "running"); err == nil {
		t.Fatal("expected error on empty build ID")
	}
	if err := s.UpdateBuildStatus(ctx, "b1", ""); err == nil {
		t.Fatal("expected error on empty status")
	}
	if err := s.UpdateChunkStatus(ctx, "c1", ""); err == nil {
		t.Fatal("expected error on empty chunk status")
	}

	// UpdateBuildStatus not found (0 rows)
	sNotFound := newMockStore(mockDB{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		},
	})
	if err := sNotFound.UpdateBuildStatus(ctx, "b1", "running"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}
	if err := sNotFound.UpdateChunkStatus(ctx, "c1", "running"); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("expected ErrChunkNotFound, got %v", err)
	}

	// UpdateBuildStatus success
	sUpdateSuccess := newMockStore(mockDB{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	})
	if err := sUpdateSuccess.UpdateBuildStatus(ctx, "b1", "running"); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if err := sUpdateSuccess.UpdateChunkStatus(ctx, "c1", "running"); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if err := sUpdateSuccess.UpdateBuildStatusMessageID(ctx, "b1", "msg-123"); err != nil {
		t.Fatalf("expected success on UpdateBuildStatusMessageID, got %v", err)
	}
}

func TestProcessStalledChunkCheck_Errors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// ClaimStalledChunk error
	sErr := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) {
		return nil, errors.New("db error")
	}})
	srvErr := NewVanadisServer(Config{}, sErr, nil)
	if err := srvErr.ProcessStalledChunkCheck(ctx, "c1"); err == nil {
		t.Fatal("expected error on claim failure")
	}

	// redispatchStalledChunk errors: GetBuild fails or chunk not in build
	store := newMemoryVanadisStore()
	srv := NewVanadisServer(Config{}, store, nil)
	if err := srv.redispatchStalledChunk(ctx, "b-missing", "c1"); err == nil {
		t.Fatal("expected error on missing build")
	}
	b := VanadisBuild{ID: "b-empty", ThreadID: "th", Objective: "O", Status: "running"}
	c := VanadisChunk{ID: "c-other", BuildID: "b-empty", ChunkIndex: 0, Title: "T", Status: "running"}
	_ = store.CreateBuild(ctx, b, []VanadisChunk{c})
	if err := srv.redispatchStalledChunk(ctx, "b-empty", "c-missing"); err == nil {
		t.Fatal("expected error on missing chunk in build")
	}
}

func TestScanVanadisChunkSummaries(t *testing.T) {
	t.Parallel()

	// 1. Success with merged chunk of action mode and pr mode
	now := time.Now().UTC()
	rows := &mockRows{
		items: []VanadisChunk{
			{ID: "c-act", ChunkIndex: 0, Mode: "action", Status: "running", StartedAt: now},
			{ID: "c-pr", ChunkIndex: 1, Mode: "pr", Repo: "azylman/aerial", Status: "running", StartedAt: now},
		},
	}
	sums, err := scanVanadisChunkSummaries(rows, "c-act")
	if err != nil || len(sums) != 2 {
		t.Fatalf("unexpected scan error: %v", err)
	}
	if sums[0].Status != "completed" {
		t.Errorf("expected c-act status completed, got %s", sums[0].Status)
	}

	rowsPR := &mockRows{
		items: []VanadisChunk{
			{ID: "c-pr", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Status: "running"},
		},
	}
	sumsPR, err := scanVanadisChunkSummaries(rowsPR, "c-pr")
	if err != nil || len(sumsPR) != 1 {
		t.Fatalf("unexpected scan error: %v", err)
	}
	if sumsPR[0].Status != "merged" {
		t.Errorf("expected c-pr status merged, got %s", sumsPR[0].Status)
	}

	// 2. Scan error
	rowsScanErr := &mockRows{
		items:   []VanadisChunk{{ID: "c1"}},
		scanErr: errors.New("scan error"),
	}
	if _, err := scanVanadisChunkSummaries(rowsScanErr, "c1"); err == nil {
		t.Fatal("expected error on scan failure")
	}

	// 3. Rows error
	rowsIterErr := &mockRows{
		items:   []VanadisChunk{{ID: "c1"}},
		rowsErr: errors.New("iter error"),
	}
	if _, err := scanVanadisChunkSummaries(rowsIterErr, "c1"); err == nil {
		t.Fatal("expected error on rows iter failure")
	}
}
