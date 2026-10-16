package db

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
)

func runVanadisTests(t *testing.T, store Store) {
	ctx := context.Background()

	// 1. Validation errors on CreateVanadisBuild
	validBuild := VanadisBuild{
		ID:        "build-101",
		Repo:      "aerial",
		ThreadID:  "1555405874565091380",
		Objective: "Implement vanadis build queue",
	}
	validChunk := VanadisChunk{
		ID:           "chunk-101-0",
		ChunkIndex:   0,
		Title:        "Task 1: Schema and store",
		Instructions: "Add vanadis_builds and vanadis_chunks tables",
		TargetFiles:  []string{"brain/pkg/db/schema.sql", "brain/pkg/db/vanadis.go"},
	}

	badBuildCases := []struct {
		name   string
		build  VanadisBuild
		chunks []VanadisChunk
	}{
		{"empty build id", VanadisBuild{ID: "", Repo: "aerial", ThreadID: "123", Objective: "obj"}, nil},
		{"empty repo", VanadisBuild{ID: "b1", Repo: "", ThreadID: "123", Objective: "obj"}, nil},
		{"empty thread_id", VanadisBuild{ID: "b1", Repo: "aerial", ThreadID: "", Objective: "obj"}, nil},
		{"empty objective", VanadisBuild{ID: "b1", Repo: "aerial", ThreadID: "123", Objective: ""}, nil},
		{"empty chunk id", validBuild, []VanadisChunk{{ID: "", Title: "t", Instructions: "i"}}},
		{"mismatched chunk build_id", validBuild, []VanadisChunk{{ID: "c1", BuildID: "other-build", Title: "t", Instructions: "i"}}},
		{"empty chunk title", validBuild, []VanadisChunk{{ID: "c1", Title: "", Instructions: "i"}}},
		{"empty chunk instructions", validBuild, []VanadisChunk{{ID: "c1", Title: "t", Instructions: ""}}},
	}

	for _, tc := range badBuildCases {
		if err := store.CreateVanadisBuild(ctx, tc.build, tc.chunks); err == nil {
			t.Errorf("[%s] expected validation error, got nil", tc.name)
		}
	}

	// 2. CreateVanadisBuild with multiple chunks (inserted out of chunk_index order to test ordering)
	chunks := []VanadisChunk{
		{
			ID:           "chunk-101-1",
			BuildID:      "build-101",
			ChunkIndex:   1,
			Title:        "Task 2: River worker",
			Instructions: "Implement VanadisWorker",
			TargetFiles:  []string{"brain/pkg/vanadis/worker.go", `file,with"special\chars.go`},
		},
		validChunk,
		{
			ID:           "chunk-101-2",
			ChunkIndex:   2,
			Title:        "Task 3: Verification",
			Instructions: "Run tests",
			TargetFiles:  nil,
			Status:       "queued",
			PRNumber:     42,
			DiffStat:     "1 file changed",
		},
	}

	if err := store.CreateVanadisBuild(ctx, validBuild, chunks); err != nil {
		t.Fatalf("CreateVanadisBuild failed: %v", err)
	}

	// Duplicate build ID should fail
	if err := store.CreateVanadisBuild(ctx, validBuild, nil); err == nil {
		t.Errorf("expected error when creating duplicate build ID")
	}

	// Duplicate chunk ID in a new build should fail and rollback the build
	dupChunkBuild := VanadisBuild{
		ID:        "build-dup-chunk",
		Repo:      "aerial",
		ThreadID:  "1555405874565091380",
		Objective: "Should rollback",
		Tier:      3,
		Status:    "queued",
	}
	if err := store.CreateVanadisBuild(ctx, dupChunkBuild, []VanadisChunk{validChunk}); err == nil {
		t.Errorf("expected error when inserting duplicate chunk ID")
	}
	if _, _, err := store.GetVanadisBuild(ctx, "build-dup-chunk"); !errors.Is(err, ErrVanadisBuildNotFound) {
		t.Errorf("expected build-dup-chunk to be rolled back (ErrVanadisBuildNotFound), got %v", err)
	}

	// 3. GetVanadisBuild
	gotBuild, gotChunks, err := store.GetVanadisBuild(ctx, "build-101")
	if err != nil {
		t.Fatalf("GetVanadisBuild failed: %v", err)
	}
	if gotBuild.ID != "build-101" || gotBuild.Repo != "azylman/aerial" || gotBuild.ThreadID != "1555405874565091380" || gotBuild.Tier != 2 || gotBuild.Status != "running" {
		t.Errorf("unexpected build fields: %+v", gotBuild)
	}
	if len(gotChunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(gotChunks))
	}
	if gotChunks[0].ID != "chunk-101-0" || gotChunks[0].ChunkIndex != 0 || gotChunks[0].Status != "pending" {
		t.Errorf("unexpected chunk[0]: %+v", gotChunks[0])
	}
	if !reflect.DeepEqual(gotChunks[0].TargetFiles, []string{"brain/pkg/db/schema.sql", "brain/pkg/db/vanadis.go"}) {
		t.Errorf("unexpected chunk[0] TargetFiles: %#v", gotChunks[0].TargetFiles)
	}
	if gotChunks[1].ID != "chunk-101-1" || gotChunks[1].ChunkIndex != 1 {
		t.Errorf("unexpected chunk[1]: %+v", gotChunks[1])
	}
	if !reflect.DeepEqual(gotChunks[1].TargetFiles, []string{"brain/pkg/vanadis/worker.go", `file,with"special\chars.go`}) {
		t.Errorf("unexpected chunk[1] TargetFiles: %#v", gotChunks[1].TargetFiles)
	}
	if gotChunks[2].ID != "chunk-101-2" || gotChunks[2].ChunkIndex != 2 || gotChunks[2].Status != "queued" || gotChunks[2].PRNumber != 42 {
		t.Errorf("unexpected chunk[2]: %+v", gotChunks[2])
	}
	if len(gotChunks[2].TargetFiles) != 0 {
		t.Errorf("expected empty TargetFiles on chunk[2], got %#v", gotChunks[2].TargetFiles)
	}

	// Non-existent and empty build ID
	if _, _, err := store.GetVanadisBuild(ctx, "nonexistent"); !errors.Is(err, ErrVanadisBuildNotFound) {
		t.Errorf("expected ErrVanadisBuildNotFound, got %v", err)
	}
	if _, _, err := store.GetVanadisBuild(ctx, "  "); err == nil {
		t.Errorf("expected validation error for empty buildID in GetVanadisBuild")
	}

	// 4. GetVanadisChunk
	gotChunk0, err := store.GetVanadisChunk(ctx, "chunk-101-0")
	if err != nil {
		t.Fatalf("GetVanadisChunk failed: %v", err)
	}
	if gotChunk0.Title != "Task 1: Schema and store" || gotChunk0.BuildID != "build-101" {
		t.Errorf("unexpected chunk from GetVanadisChunk: %+v", gotChunk0)
	}
	if _, err := store.GetVanadisChunk(ctx, "nonexistent-chunk"); !errors.Is(err, ErrVanadisChunkNotFound) {
		t.Errorf("expected ErrVanadisChunkNotFound, got %v", err)
	}
	if _, err := store.GetVanadisChunk(ctx, ""); err == nil {
		t.Errorf("expected error for empty chunkID in GetVanadisChunk")
	}

	// 5. UpdateVanadisChunkStatus
	if err := store.UpdateVanadisChunkStatus(ctx, "chunk-101-0", "running"); err != nil {
		t.Fatalf("UpdateVanadisChunkStatus failed: %v", err)
	}
	gotChunk0After, err := store.GetVanadisChunk(ctx, "chunk-101-0")
	if err != nil || gotChunk0After.Status != "running" {
		t.Fatalf("expected chunk status 'running', got %+v (err: %v)", gotChunk0After, err)
	}
	if err := store.UpdateVanadisChunkStatus(ctx, "nonexistent-chunk", "running"); !errors.Is(err, ErrVanadisChunkNotFound) {
		t.Errorf("expected ErrVanadisChunkNotFound, got %v", err)
	}
	if err := store.UpdateVanadisChunkStatus(ctx, "", "running"); err == nil {
		t.Errorf("expected error for empty chunkID in UpdateVanadisChunkStatus")
	}
	if err := store.UpdateVanadisChunkStatus(ctx, "chunk-101-0", ""); err == nil {
		t.Errorf("expected error for empty status in UpdateVanadisChunkStatus")
	}

	// 6. AttachVanadisChunkPR
	if err := store.AttachVanadisChunkPR(ctx, "chunk-101-0", 512, "3 files changed, 120 insertions(+)"); err != nil {
		t.Fatalf("AttachVanadisChunkPR failed: %v", err)
	}
	gotChunk0PR, err := store.GetVanadisChunk(ctx, "chunk-101-0")
	if err != nil {
		t.Fatalf("GetVanadisChunk after AttachVanadisChunkPR failed: %v", err)
	}
	if gotChunk0PR.PRNumber != 512 || gotChunk0PR.DiffStat != "3 files changed, 120 insertions(+)" || gotChunk0PR.Status != "pr_opened" {
		t.Errorf("unexpected chunk after AttachVanadisChunkPR: %+v", gotChunk0PR)
	}
	if err := store.AttachVanadisChunkPR(ctx, "nonexistent-chunk", 512, "diff"); !errors.Is(err, ErrVanadisChunkNotFound) {
		t.Errorf("expected ErrVanadisChunkNotFound on AttachVanadisChunkPR, got %v", err)
	}
	if err := store.AttachVanadisChunkPR(ctx, "", 512, "diff"); err == nil {
		t.Errorf("expected error for empty chunkID in AttachVanadisChunkPR")
	}
	if err := store.AttachVanadisChunkPR(ctx, "chunk-101-0", 0, "diff"); err == nil {
		t.Errorf("expected error for pr_number <= 0 in AttachVanadisChunkPR")
	}

	// 7. UpdateVanadisBuildStatus
	if err := store.UpdateVanadisBuildStatus(ctx, "build-101", "completed"); err != nil {
		t.Fatalf("UpdateVanadisBuildStatus failed: %v", err)
	}
	gotBuildAfter, _, err := store.GetVanadisBuild(ctx, "build-101")
	if err != nil || gotBuildAfter.Status != "completed" {
		t.Fatalf("expected build status 'completed', got %+v (err: %v)", gotBuildAfter, err)
	}
	if err := store.UpdateVanadisBuildStatus(ctx, "nonexistent-build", "completed"); !errors.Is(err, ErrVanadisBuildNotFound) {
		t.Errorf("expected ErrVanadisBuildNotFound on UpdateVanadisBuildStatus, got %v", err)
	}
	if err := store.UpdateVanadisBuildStatus(ctx, "", "completed"); err == nil {
		t.Errorf("expected error for empty buildID in UpdateVanadisBuildStatus")
	}
	if err := store.UpdateVanadisBuildStatus(ctx, "build-101", ""); err == nil {
		t.Errorf("expected error for empty status in UpdateVanadisBuildStatus")
	}

	// 8. UpdateVanadisBuildStatusMessageID
	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "build-101", "  msg-build-999  "); err != nil {
		t.Fatalf("UpdateVanadisBuildStatusMessageID failed: %v", err)
	}
	gotBuildWithMsg, _, err := store.GetVanadisBuild(ctx, "build-101")
	if err != nil || gotBuildWithMsg.StatusMessageID != "msg-build-999" {
		t.Fatalf("expected status_message_id 'msg-build-999', got %+v (err: %v)", gotBuildWithMsg, err)
	}
	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "nonexistent-build", "msg-build-999"); !errors.Is(err, ErrVanadisBuildNotFound) {
		t.Errorf("expected ErrVanadisBuildNotFound on UpdateVanadisBuildStatusMessageID, got %v", err)
	}
	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "", "msg-build-999"); err == nil {
		t.Errorf("expected error for empty buildID in UpdateVanadisBuildStatusMessageID")
	}
}

func TestVanadis_SQLStore(t *testing.T) {
	t.Parallel()
	store := NewTestStore(t)
	runVanadisTests(t, store)
}

func TestVanadis_FakeStore(t *testing.T) {
	t.Parallel()
	store := NewFakeStore()
	runVanadisTests(t, store)

	// Verify duplicate chunk_index check in FakeStore
	ctx := context.Background()
	err := store.CreateVanadisBuild(ctx, VanadisBuild{
		ID:        "build-dup-idx",
		Repo:      "aerial",
		ThreadID:  "123",
		Objective: "obj",
	}, []VanadisChunk{
		{ID: "c-idx-1", ChunkIndex: 0, Title: "t1", Instructions: "i1"},
		{ID: "c-idx-2", ChunkIndex: 0, Title: "t2", Instructions: "i2"},
	})
	if err == nil {
		t.Errorf("expected error on duplicate chunk_index in FakeStore")
	}
}

func TestVanadis_FakeStore_FailuresAndClosed(t *testing.T) {
	t.Parallel()
	store := NewFakeStore()
	ctx := context.Background()
	simErr := errors.New("simulated vanadis error")

	store.FailNext("CreateVanadisBuild", simErr)
	if err := store.CreateVanadisBuild(ctx, VanadisBuild{}, nil); !errors.Is(err, simErr) {
		t.Errorf("expected %v, got %v", simErr, err)
	}

	store.FailNext("GetVanadisBuild", simErr)
	if _, _, err := store.GetVanadisBuild(ctx, "b1"); !errors.Is(err, simErr) {
		t.Errorf("expected %v, got %v", simErr, err)
	}

	store.FailNext("GetVanadisChunk", simErr)
	if _, err := store.GetVanadisChunk(ctx, "c1"); !errors.Is(err, simErr) {
		t.Errorf("expected %v, got %v", simErr, err)
	}

	store.FailNext("UpdateVanadisChunkStatus", simErr)
	if err := store.UpdateVanadisChunkStatus(ctx, "c1", "s"); !errors.Is(err, simErr) {
		t.Errorf("expected %v, got %v", simErr, err)
	}

	store.FailNext("AttachVanadisChunkPR", simErr)
	if err := store.AttachVanadisChunkPR(ctx, "c1", 1, "d"); !errors.Is(err, simErr) {
		t.Errorf("expected %v, got %v", simErr, err)
	}

	store.FailNext("UpdateVanadisBuildStatus", simErr)
	if err := store.UpdateVanadisBuildStatus(ctx, "b1", "s"); !errors.Is(err, simErr) {
		t.Errorf("expected %v, got %v", simErr, err)
	}

	store.FailNext("UpdateVanadisBuildStatusMessageID", simErr)
	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "b1", "msg-1"); !errors.Is(err, simErr) {
		t.Errorf("expected %v, got %v", simErr, err)
	}

	_ = store.Close()
	if err := store.CreateVanadisBuild(ctx, VanadisBuild{}, nil); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected ErrConnDone, got %v", err)
	}
	if _, _, err := store.GetVanadisBuild(ctx, "b1"); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected ErrConnDone, got %v", err)
	}
	if _, err := store.GetVanadisChunk(ctx, "c1"); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected ErrConnDone, got %v", err)
	}
	if err := store.UpdateVanadisChunkStatus(ctx, "c1", "s"); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected ErrConnDone, got %v", err)
	}
	if err := store.AttachVanadisChunkPR(ctx, "c1", 1, "d"); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected ErrConnDone, got %v", err)
	}
	if err := store.UpdateVanadisBuildStatus(ctx, "b1", "s"); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected ErrConnDone, got %v", err)
	}
	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "b1", "msg-1"); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected ErrConnDone, got %v", err)
	}
}

func TestVanadis_SQLStore_NilAndClosedBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var nilStore *SQLStore
	storeNilDB := &SQLStore{db: nil}

	for i, s := range []*SQLStore{nilStore, storeNilDB} {
		if err := s.CreateVanadisBuild(ctx, VanadisBuild{}, nil); err == nil {
			t.Errorf("[%d] expected error on nil CreateVanadisBuild", i)
		}
		if _, _, err := s.GetVanadisBuild(ctx, "b1"); err == nil {
			t.Errorf("[%d] expected error on nil GetVanadisBuild", i)
		}
		if _, err := s.GetVanadisChunk(ctx, "c1"); err == nil {
			t.Errorf("[%d] expected error on nil GetVanadisChunk", i)
		}
		if err := s.UpdateVanadisChunkStatus(ctx, "c1", "s"); err == nil {
			t.Errorf("[%d] expected error on nil UpdateVanadisChunkStatus", i)
		}
		if err := s.AttachVanadisChunkPR(ctx, "c1", 1, "d"); err == nil {
			t.Errorf("[%d] expected error on nil AttachVanadisChunkPR", i)
		}
		if err := s.UpdateVanadisBuildStatus(ctx, "b1", "s"); err == nil {
			t.Errorf("[%d] expected error on nil UpdateVanadisBuildStatus", i)
		}
		if err := s.UpdateVanadisBuildStatusMessageID(ctx, "b1", "msg-1"); err == nil {
			t.Errorf("[%d] expected error on nil UpdateVanadisBuildStatusMessageID", i)
		}
	}

	tmpPath := t.TempDir() + "/closed_vanadis.db"
	closedDB, err := initTestSQLite(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	closedStore := NewSQLStore(closedDB)
	_ = closedDB.Close()

	validBuild := VanadisBuild{ID: "b1", Repo: "aerial", ThreadID: "123", Objective: "obj"}
	if err := closedStore.CreateVanadisBuild(ctx, validBuild, nil); err == nil {
		t.Errorf("expected error on closed CreateVanadisBuild")
	}
	if _, _, err := closedStore.GetVanadisBuild(ctx, "b1"); err == nil {
		t.Errorf("expected error on closed GetVanadisBuild")
	}
	if _, err := closedStore.GetVanadisChunk(ctx, "c1"); err == nil {
		t.Errorf("expected error on closed GetVanadisChunk")
	}
	if err := closedStore.UpdateVanadisChunkStatus(ctx, "c1", "s"); err == nil {
		t.Errorf("expected error on closed UpdateVanadisChunkStatus")
	}
	if err := closedStore.AttachVanadisChunkPR(ctx, "c1", 1, "d"); err == nil {
		t.Errorf("expected error on closed AttachVanadisChunkPR")
	}
	if err := closedStore.UpdateVanadisBuildStatus(ctx, "b1", "s"); err == nil {
		t.Errorf("expected error on closed UpdateVanadisBuildStatus")
	}
	if err := closedStore.UpdateVanadisBuildStatusMessageID(ctx, "b1", "msg-1"); err == nil {
		t.Errorf("expected error on closed UpdateVanadisBuildStatusMessageID")
	}
}

func TestParsePostgresTextArrayEdgeCases(t *testing.T) {
	t.Parallel()
	if got := parsePostgresTextArray(""); len(got) != 0 {
		t.Errorf("expected empty slice for empty string, got %#v", got)
	}
	if got := parsePostgresTextArray("{}"); len(got) != 0 {
		t.Errorf("expected empty slice for '{}', got %#v", got)
	}
	if got := parsePostgresTextArray("{   }"); len(got) != 0 {
		t.Errorf("expected empty slice for '{   }', got %#v", got)
	}
	if got := parsePostgresTextArray("{a,b,c}"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("expected [a b c], got %#v", got)
	}
}

func TestVanadis_SQLStore_PostgresBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockDB := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return testCustomResult{rows: 1}, nil
		},
	}
	pgStore := &SQLStore{db: mockDB, isPostgres: true}

	err := pgStore.CreateVanadisBuild(ctx, VanadisBuild{
		ID:        "b-pg-1",
		Repo:      "aerial",
		ThreadID:  "123",
		Objective: "pg branch",
	}, []VanadisChunk{
		{
			ID:           "c-pg-1",
			ChunkIndex:   0,
			Repo:         "aerial-config",
			Title:        "t1",
			Instructions: "i1",
			TargetFiles:  nil,
		},
	})
	if err != nil {
		t.Fatalf("CreateVanadisBuild with isPostgres=true failed: %v", err)
	}
}

