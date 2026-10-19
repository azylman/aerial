package db

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func formatTestPostgresTextArray(items []string) string {
	if len(items) == 0 {
		return "{}"
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, item := range items {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		for j := 0; j < len(item); j++ {
			ch := item[j]
			if ch == '\\' || ch == '"' {
				b.WriteByte('\\')
			}
			b.WriteByte(ch)
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func seedVanadisBuildSQLite(t *testing.T, store Store, build VanadisBuild, chunks []VanadisChunk) {
	t.Helper()
	sqlStore, ok := store.(*SQLStore)
	if !ok {
		t.Fatalf("expected *SQLStore, got %T", store)
	}
	ctx := context.Background()

	buildTier := build.Tier
	if buildTier <= 0 {
		buildTier = 2
	}
	buildTitle := build.Title
	if strings.TrimSpace(buildTitle) == "" {
		buildTitle = build.Objective
	}
	buildStatus := build.Status
	if strings.TrimSpace(buildStatus) == "" {
		buildStatus = "running"
	}

	_, err := sqlStore.db.ExecContext(ctx, `
		INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
	`, build.ID, build.ThreadID, buildTier, buildTitle, build.Objective, build.SharedContext, buildStatus, build.StatusMessageID)
	if err != nil {
		t.Fatalf("seedVanadisBuildSQLite failed to insert build: %v", err)
	}

	for _, c := range chunks {
		var prNum sql.NullInt64
		if c.PRNumber > 0 {
			prNum = sql.NullInt64{Int64: int64(c.PRNumber), Valid: true}
		}
		var startedAt sql.NullTime
		if !c.StartedAt.IsZero() {
			startedAt = sql.NullTime{Time: c.StartedAt.UTC(), Valid: true}
		} else if c.Status == "running" || c.Status == "coding" {
			startedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
		}

		repo := c.Repo
		if strings.TrimSpace(repo) == "" {
			repo = "azylman/aerial"
		}
		status := c.Status
		if strings.TrimSpace(status) == "" {
			status = "pending"
		}
		effort := strings.ToLower(strings.TrimSpace(c.Effort))
		if effort == "" {
			effort = "medium"
		}

		targetFilesArg := formatTestPostgresTextArray(c.TargetFiles)
		targetSymbolsArg := formatTestPostgresTextArray(c.TargetSymbols)
		acceptanceCriteriaArg := formatTestPostgresTextArray(c.AcceptanceCriteria)

		_, err := sqlStore.db.ExecContext(ctx, `
			INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, started_at, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
		`, c.ID, c.BuildID, c.ChunkIndex, repo, c.Title, c.Instructions, targetFilesArg, targetSymbolsArg, effort, acceptanceCriteriaArg, status, prNum, startedAt)
		if err != nil {
			t.Fatalf("seedVanadisBuildSQLite failed to insert chunk %s: %v", c.ID, err)
		}
	}
}

func testVanadisReadAndLifecycle(t *testing.T, store Store, explicitStart time.Time) {
	t.Helper()
	ctx := context.Background()

	// 1. GetVanadisBuild (verify Title defaults to Objective when omitted)
	gotBuild, gotChunks, err := store.GetVanadisBuild(ctx, "build-101")
	if err != nil {
		t.Fatalf("GetVanadisBuild failed: %v", err)
	}
	if gotBuild.ID != "build-101" || gotBuild.ThreadID != "1555405874565091380" || gotBuild.Tier != 2 || gotBuild.Title != "Implement vanadis build queue" || gotBuild.Objective != "Implement vanadis build queue" || gotBuild.Status != "running" || gotBuild.SharedContext != "Shared context for build 101" {
		t.Errorf("unexpected build fields: %+v", gotBuild)
	}

	// Verify explicit Title is preserved independently of Objective, and chunk created with 'running' gets StartedAt
	gotTitledBuild, gotTitledChunks, err := store.GetVanadisBuild(ctx, "build-titled-102")
	if err != nil {
		t.Fatalf("GetVanadisBuild for build-titled-102 failed: %v", err)
	}
	if gotTitledBuild.Title != "Redesign Vanadis Build Status Cards" || gotTitledBuild.Objective != "Detailed objective for redesigning the cards" {
		t.Errorf("expected explicit Title and Objective preserved, got %+v", gotTitledBuild)
	}
	if len(gotTitledChunks) != 1 || gotTitledChunks[0].StartedAt.IsZero() {
		t.Errorf("expected chunk created with status 'running' to have non-zero StartedAt, got %+v", gotTitledChunks)
	}
	if len(gotChunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(gotChunks))
	}
	if gotChunks[0].ID != "chunk-101-0" || gotChunks[0].ChunkIndex != 0 || gotChunks[0].Repo != "azylman/aerial" || gotChunks[0].Status != "pending" || gotChunks[0].Effort != "medium" || !gotChunks[0].StartedAt.IsZero() {
		t.Errorf("unexpected chunk[0]: %+v", gotChunks[0])
	}
	if !reflect.DeepEqual(gotChunks[0].TargetFiles, []string{"brain/pkg/db/schema.sql", "brain/pkg/db/vanadis.go"}) {
		t.Errorf("unexpected chunk[0] TargetFiles: %#v", gotChunks[0].TargetFiles)
	}
	if !reflect.DeepEqual(gotChunks[0].TargetSymbols, []string{"VanadisBuild", "VanadisChunk"}) {
		t.Errorf("unexpected chunk[0] TargetSymbols: %#v", gotChunks[0].TargetSymbols)
	}
	if !reflect.DeepEqual(gotChunks[0].AcceptanceCriteria, []string{"Unit tests pass", "Coverage >= 95%"}) {
		t.Errorf("unexpected chunk[0] AcceptanceCriteria: %#v", gotChunks[0].AcceptanceCriteria)
	}
	if gotChunks[1].ID != "chunk-101-1" || gotChunks[1].ChunkIndex != 1 || gotChunks[1].Repo != "azylman/aerial-config" || gotChunks[1].Effort != "high" {
		t.Errorf("unexpected chunk[1]: %+v", gotChunks[1])
	}
	if !reflect.DeepEqual(gotChunks[1].TargetFiles, []string{"brain/pkg/vanadis/worker.go", `file,with"special\chars.go`}) {
		t.Errorf("unexpected chunk[1] TargetFiles: %#v", gotChunks[1].TargetFiles)
	}
	if !reflect.DeepEqual(gotChunks[1].TargetSymbols, []string{`Sym,with"quote`}) {
		t.Errorf("unexpected chunk[1] TargetSymbols: %#v", gotChunks[1].TargetSymbols)
	}
	if !reflect.DeepEqual(gotChunks[1].AcceptanceCriteria, []string{"Worker processes chunk successfully"}) {
		t.Errorf("unexpected chunk[1] AcceptanceCriteria: %#v", gotChunks[1].AcceptanceCriteria)
	}
	if gotChunks[2].ID != "chunk-101-2" || gotChunks[2].ChunkIndex != 2 || gotChunks[2].Repo != "azylman/aerial" || gotChunks[2].Status != "queued" || gotChunks[2].PRNumber != 42 || gotChunks[2].Effort != "medium" || !gotChunks[2].StartedAt.Equal(explicitStart) {
		t.Errorf("unexpected chunk[2]: %+v", gotChunks[2])
	}
	if len(gotChunks[2].TargetFiles) != 0 {
		t.Errorf("expected empty TargetFiles on chunk[2], got %#v", gotChunks[2].TargetFiles)
	}
	if len(gotChunks[2].TargetSymbols) != 0 {
		t.Errorf("expected empty TargetSymbols on chunk[2], got %#v", gotChunks[2].TargetSymbols)
	}
	if len(gotChunks[2].AcceptanceCriteria) != 0 {
		t.Errorf("expected empty AcceptanceCriteria on chunk[2], got %#v", gotChunks[2].AcceptanceCriteria)
	}

	// Non-existent and empty build ID
	if _, _, err := store.GetVanadisBuild(ctx, "nonexistent"); !errors.Is(err, ErrVanadisBuildNotFound) {
		t.Errorf("expected ErrVanadisBuildNotFound, got %v", err)
	}
	if _, _, err := store.GetVanadisBuild(ctx, "  "); err == nil {
		t.Errorf("expected validation error for empty buildID in GetVanadisBuild")
	}

	// 2. GetVanadisChunk
	gotChunk0, err := store.GetVanadisChunk(ctx, "chunk-101-0")
	if err != nil {
		t.Fatalf("GetVanadisChunk failed: %v", err)
	}
	if gotChunk0.Title != "Task 1: Schema and store" || gotChunk0.BuildID != "build-101" || gotChunk0.Effort != "medium" || !gotChunk0.StartedAt.IsZero() {
		t.Errorf("unexpected chunk from GetVanadisChunk: %+v", gotChunk0)
	}
	if !reflect.DeepEqual(gotChunk0.TargetSymbols, []string{"VanadisBuild", "VanadisChunk"}) {
		t.Errorf("unexpected chunk0 TargetSymbols: %#v", gotChunk0.TargetSymbols)
	}
	if !reflect.DeepEqual(gotChunk0.AcceptanceCriteria, []string{"Unit tests pass", "Coverage >= 95%"}) {
		t.Errorf("unexpected chunk0 AcceptanceCriteria: %#v", gotChunk0.AcceptanceCriteria)
	}
	if _, err := store.GetVanadisChunk(ctx, "nonexistent-chunk"); !errors.Is(err, ErrVanadisChunkNotFound) {
		t.Errorf("expected ErrVanadisChunkNotFound, got %v", err)
	}
	if _, err := store.GetVanadisChunk(ctx, ""); err == nil {
		t.Errorf("expected error for empty chunkID in GetVanadisChunk")
	}

	// 3. AttachVanadisChunkPR (verify StartedAt is preserved on chunk-102-0, and populated if zero on chunk-101-1)
	gotChunk102Before, err := store.GetVanadisChunk(ctx, "chunk-102-0")
	if err != nil {
		t.Fatalf("GetVanadisChunk failed: %v", err)
	}
	initialStartedAt := gotChunk102Before.StartedAt
	if initialStartedAt.IsZero() {
		t.Fatalf("expected non-zero initial StartedAt on chunk-102-0")
	}

	if err := store.AttachVanadisChunkPR(ctx, "chunk-102-0", 512); err != nil {
		t.Fatalf("AttachVanadisChunkPR failed: %v", err)
	}
	gotChunk102PR, err := store.GetVanadisChunk(ctx, "chunk-102-0")
	if err != nil {
		t.Fatalf("GetVanadisChunk after AttachVanadisChunkPR failed: %v", err)
	}
	if gotChunk102PR.PRNumber != 512 || gotChunk102PR.Status != "pr_opened" {
		t.Errorf("unexpected chunk after AttachVanadisChunkPR: %+v", gotChunk102PR)
	}
	if !gotChunk102PR.StartedAt.Equal(initialStartedAt) {
		t.Errorf("expected StartedAt %v preserved after AttachVanadisChunkPR, got %v", initialStartedAt, gotChunk102PR.StartedAt)
	}

	// AttachVanadisChunkPR on a chunk that never entered 'running' still populates StartedAt
	if err := store.AttachVanadisChunkPR(ctx, "chunk-101-1", 513); err != nil {
		t.Fatalf("AttachVanadisChunkPR on chunk-101-1 failed: %v", err)
	}
	gotChunk1PR, err := store.GetVanadisChunk(ctx, "chunk-101-1")
	if err != nil || gotChunk1PR.StartedAt.IsZero() {
		t.Errorf("expected non-zero StartedAt on chunk-101-1 after AttachVanadisChunkPR, got %+v (err: %v)", gotChunk1PR, err)
	}
	if err := store.AttachVanadisChunkPR(ctx, "nonexistent-chunk", 512); !errors.Is(err, ErrVanadisChunkNotFound) {
		t.Errorf("expected ErrVanadisChunkNotFound on AttachVanadisChunkPR, got %v", err)
	}
	if err := store.AttachVanadisChunkPR(ctx, "", 512); err == nil {
		t.Errorf("expected error for empty chunkID in AttachVanadisChunkPR")
	}
	if err := store.AttachVanadisChunkPR(ctx, "chunk-101-0", 0); err == nil {
		t.Errorf("expected error for pr_number <= 0 in AttachVanadisChunkPR")
	}

	// 4. UpdateVanadisBuildStatusMessageID
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

	validBuild := VanadisBuild{
		ID:            "build-101",
		ThreadID:      "1555405874565091380",
		Objective:     "Implement vanadis build queue",
		SharedContext: "Shared context for build 101",
	}
	validChunk := VanadisChunk{
		ID:                 "chunk-101-0",
		BuildID:            "build-101",
		ChunkIndex:         0,
		Title:              "Task 1: Schema and store",
		Instructions:       "Add vanadis_builds and vanadis_chunks tables",
		TargetFiles:        []string{"brain/pkg/db/schema.sql", "brain/pkg/db/vanadis.go"},
		TargetSymbols:      []string{"VanadisBuild", "VanadisChunk"},
		Effort:             "medium",
		AcceptanceCriteria: []string{"Unit tests pass", "Coverage >= 95%"},
	}

	explicitStart := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	chunks := []VanadisChunk{
		validChunk,
		{
			ID:                 "chunk-101-1",
			BuildID:            "build-101",
			ChunkIndex:         1,
			Repo:               "azylman/aerial-config",
			Title:              "Task 2: River worker",
			Instructions:       "Implement VanadisWorker",
			TargetFiles:        []string{"brain/pkg/vanadis/worker.go", `file,with"special\chars.go`},
			TargetSymbols:      []string{`Sym,with"quote`},
			Effort:             "HIGH",
			AcceptanceCriteria: []string{"Worker processes chunk successfully"},
		},
		{
			ID:           "chunk-101-2",
			BuildID:      "build-101",
			ChunkIndex:   2,
			Title:        "Task 3: Verification",
			Instructions: "Run tests",
			TargetFiles:  nil,
			Effort:       "",
			Status:       "queued",
			PRNumber:     42,
			StartedAt:    explicitStart,
		},
	}

	seedVanadisBuildSQLite(t, store, validBuild, chunks)

	titledBuild := VanadisBuild{
		ID:        "build-titled-102",
		ThreadID:  "1555405874565091380",
		Title:     "Redesign Vanadis Build Status Cards",
		Objective: "Detailed objective for redesigning the cards",
	}
	runningChunk := VanadisChunk{
		ID:           "chunk-102-0",
		BuildID:      "build-titled-102",
		ChunkIndex:   0,
		Title:        "Already running chunk",
		Instructions: "Run immediately",
		Status:       "running",
	}
	seedVanadisBuildSQLite(t, store, titledBuild, []VanadisChunk{runningChunk})

	testVanadisReadAndLifecycle(t, store, explicitStart)
}

func TestVanadis_FakeStore(t *testing.T) {
	t.Parallel()
	store := NewFakeStore()
	ctx := context.Background()

	validBuild := VanadisBuild{
		ID:            "build-101",
		ThreadID:      "1555405874565091380",
		Objective:     "Implement vanadis build queue",
		SharedContext: "Shared context for build 101",
	}
	validChunk := VanadisChunk{
		ID:                 "chunk-101-0",
		ChunkIndex:         0,
		Title:              "Task 1: Schema and store",
		Instructions:       "Add vanadis_builds and vanadis_chunks tables",
		TargetFiles:        []string{"brain/pkg/db/schema.sql", "brain/pkg/db/vanadis.go"},
		TargetSymbols:      []string{"VanadisBuild", "VanadisChunk"},
		Effort:             "medium",
		AcceptanceCriteria: []string{"Unit tests pass", "Coverage >= 95%"},
	}

	badBuildCases := []struct {
		name   string
		build  VanadisBuild
		chunks []VanadisChunk
	}{
		{"empty build id", VanadisBuild{ID: "", ThreadID: "123", Objective: "obj"}, nil},
		{"empty thread_id", VanadisBuild{ID: "b1", ThreadID: "", Objective: "obj"}, nil},
		{"empty objective", VanadisBuild{ID: "b1", ThreadID: "123", Objective: ""}, nil},
		{"empty chunk id", validBuild, []VanadisChunk{{ID: "", Title: "t", Instructions: "i"}}},
		{"mismatched chunk build_id", validBuild, []VanadisChunk{{ID: "c1", BuildID: "other-build", Title: "t", Instructions: "i"}}},
		{"empty chunk title", validBuild, []VanadisChunk{{ID: "c1", Title: "", Instructions: "i"}}},
		{"empty chunk instructions", validBuild, []VanadisChunk{{ID: "c1", Title: "t", Instructions: ""}}},
		{"invalid chunk effort", validBuild, []VanadisChunk{{ID: "c1", Title: "t", Instructions: "i", Effort: "ultra"}}},
		{"numeric chunk effort", validBuild, []VanadisChunk{{ID: "c1", Title: "t", Instructions: "i", Effort: "10"}}},
	}

	for _, tc := range badBuildCases {
		if err := store.CreateVanadisBuild(ctx, tc.build, tc.chunks); err == nil {
			t.Errorf("[%s] expected validation error, got nil", tc.name)
		}
	}

	explicitStart := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	chunks := []VanadisChunk{
		{
			ID:                 "chunk-101-1",
			BuildID:            "build-101",
			ChunkIndex:         1,
			Repo:               "azylman/aerial-config",
			Title:              "Task 2: River worker",
			Instructions:       "Implement VanadisWorker",
			TargetFiles:        []string{"brain/pkg/vanadis/worker.go", `file,with"special\chars.go`},
			TargetSymbols:      []string{`Sym,with"quote`},
			Effort:             "HIGH",
			AcceptanceCriteria: []string{"Worker processes chunk successfully"},
		},
		validChunk,
		{
			ID:           "chunk-101-2",
			ChunkIndex:   2,
			Title:        "Task 3: Verification",
			Instructions: "Run tests",
			TargetFiles:  nil,
			Effort:       "",
			Status:       "queued",
			PRNumber:     42,
			StartedAt:    explicitStart,
		},
	}

	if err := store.CreateVanadisBuild(ctx, validBuild, chunks); err != nil {
		t.Fatalf("CreateVanadisBuild failed: %v", err)
	}

	// Duplicate build ID should fail
	if err := store.CreateVanadisBuild(ctx, validBuild, nil); err == nil {
		t.Errorf("expected error when creating duplicate build ID")
	}

	// Duplicate chunk ID in a new build should fail
	dupChunkBuild := VanadisBuild{
		ID:        "build-dup-chunk",
		ThreadID:  "1555405874565091380",
		Objective: "Should fail",
		Tier:      3,
		Status:    "queued",
	}
	if err := store.CreateVanadisBuild(ctx, dupChunkBuild, []VanadisChunk{validChunk}); err == nil {
		t.Errorf("expected error when inserting duplicate chunk ID")
	}

	titledBuild := VanadisBuild{
		ID:        "build-titled-102",
		ThreadID:  "1555405874565091380",
		Title:     "Redesign Vanadis Build Status Cards",
		Objective: "Detailed objective for redesigning the cards",
	}
	runningChunk := VanadisChunk{
		ID:           "chunk-102-0",
		ChunkIndex:   0,
		Title:        "Already running chunk",
		Instructions: "Run immediately",
		Status:       "running",
	}
	if err := store.CreateVanadisBuild(ctx, titledBuild, []VanadisChunk{runningChunk}); err != nil {
		t.Fatalf("CreateVanadisBuild with explicit Title failed: %v", err)
	}

	testVanadisReadAndLifecycle(t, store, explicitStart)

	// Verify duplicate chunk_index check in FakeStore
	err := store.CreateVanadisBuild(ctx, VanadisBuild{
		ID:        "build-dup-idx",
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

	store.FailNext("AttachVanadisChunkPR", simErr)
	if err := store.AttachVanadisChunkPR(ctx, "c1", 1); !errors.Is(err, simErr) {
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
	if err := store.AttachVanadisChunkPR(ctx, "c1", 1); !errors.Is(err, sql.ErrConnDone) {
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
		if _, _, err := s.GetVanadisBuild(ctx, "b1"); err == nil {
			t.Errorf("[%d] expected error on nil GetVanadisBuild", i)
		}
		if _, err := s.GetVanadisChunk(ctx, "c1"); err == nil {
			t.Errorf("[%d] expected error on nil GetVanadisChunk", i)
		}
		if err := s.AttachVanadisChunkPR(ctx, "c1", 1); err == nil {
			t.Errorf("[%d] expected error on nil AttachVanadisChunkPR", i)
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

	if _, _, err := closedStore.GetVanadisBuild(ctx, "b1"); err == nil {
		t.Errorf("expected error on closed GetVanadisBuild")
	}
	if _, err := closedStore.GetVanadisChunk(ctx, "c1"); err == nil {
		t.Errorf("expected error on closed GetVanadisChunk")
	}
	if err := closedStore.AttachVanadisChunkPR(ctx, "c1", 1); err == nil {
		t.Errorf("expected error on closed AttachVanadisChunkPR")
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

func TestNormalizeVanadisEffort(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input       string
		expected    string
		expectError bool
	}{
		{"", "medium", false},
		{"   ", "medium", false},
		{"low", "low", false},
		{"LOW", "low", false},
		{"  Medium  ", "medium", false},
		{"high", "high", false},
		{"HIGH", "high", false},
		{"invalid", "", true},
		{"unknown", "", true},
	}
	for _, tc := range cases {
		got, err := normalizeVanadisEffort(tc.input)
		if (err != nil) != tc.expectError {
			t.Errorf("normalizeVanadisEffort(%q) error = %v, expectError = %v", tc.input, err, tc.expectError)
		}
		if got != tc.expected {
			t.Errorf("normalizeVanadisEffort(%q) = %q, expected %q", tc.input, got, tc.expected)
		}
	}
}
