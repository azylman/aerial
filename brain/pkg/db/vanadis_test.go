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
	escaped := make([]string, len(items))
	for i, it := range items {
		escaped[i] = `"` + strings.ReplaceAll(strings.ReplaceAll(it, `\`, `\\`), `"`, `\"`) + `"`
	}
	return "{" + strings.Join(escaped, ",") + "}"
}

func seedVanadisBuildSQLite(t *testing.T, store Store, build VanadisBuild, chunks []VanadisChunk) {
	t.Helper()
	sqlStore, ctx := store.(*SQLStore), context.Background()
	title, status, tier := build.Title, build.Status, build.Tier
	if strings.TrimSpace(title) == "" { title = build.Objective }
	if strings.TrimSpace(status) == "" { status = "running" }
	if tier <= 0 { tier = 2 }
	if _, err := sqlStore.db.ExecContext(ctx, `INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`, build.ID, build.ThreadID, tier, title, build.Objective, build.SharedContext, status, build.StatusMessageID); err != nil {
		t.Fatalf("seed build: %v", err)
	}
	for _, c := range chunks {
		var prNum sql.NullInt64
		if c.PRNumber > 0 { prNum = sql.NullInt64{Int64: int64(c.PRNumber), Valid: true} }
		var startedAt sql.NullTime
		if !c.StartedAt.IsZero() {
			startedAt = sql.NullTime{Time: c.StartedAt.UTC(), Valid: true}
		} else if c.Status == "running" || c.Status == "coding" {
			startedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
		}
		repo, cStatus, effort := c.Repo, c.Status, strings.ToLower(strings.TrimSpace(c.Effort))
		if strings.TrimSpace(repo) == "" { repo = "azylman/aerial" }
		if strings.TrimSpace(cStatus) == "" { cStatus = "pending" }
		if effort == "" { effort = "medium" }
		if _, err := sqlStore.db.ExecContext(ctx, `INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, started_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`, c.ID, c.BuildID, c.ChunkIndex, repo, c.Title, c.Instructions, formatTestPostgresTextArray(c.TargetFiles), formatTestPostgresTextArray(c.TargetSymbols), effort, formatTestPostgresTextArray(c.AcceptanceCriteria), cStatus, prNum, startedAt); err != nil {
			t.Fatalf("seed chunk %s: %v", c.ID, err)
		}
	}
}

func testVanadisReadAndLifecycle(t *testing.T, store Store, explicitStart time.Time) {
	t.Helper()
	ctx := context.Background()

	gotBuild, gotChunks, err := store.GetVanadisBuild(ctx, "build-101")
	if err != nil || gotBuild.ID != "build-101" || gotBuild.Title != "Implement vanadis build queue" || gotBuild.Status != "running" {
		t.Fatalf("unexpected build: %+v (err: %v)", gotBuild, err)
	}

	gotTitledBuild, gotTitledChunks, err := store.GetVanadisBuild(ctx, "build-titled-102")
	if err != nil || gotTitledBuild.Title != "Redesign Vanadis Build Status Cards" || len(gotTitledChunks) != 1 || gotTitledChunks[0].StartedAt.IsZero() {
		t.Fatalf("unexpected titled build/chunks: %+v, %+v (err: %v)", gotTitledBuild, gotTitledChunks, err)
	}
	if len(gotChunks) != 3 || gotChunks[0].ID != "chunk-101-0" || gotChunks[0].Repo != "azylman/aerial" || !gotChunks[0].StartedAt.IsZero() {
		t.Fatalf("unexpected chunks: %+v", gotChunks)
	}
	if !reflect.DeepEqual(gotChunks[0].TargetFiles, []string{"brain/pkg/db/schema.sql", "brain/pkg/db/vanadis.go"}) || !reflect.DeepEqual(gotChunks[0].TargetSymbols, []string{"VanadisBuild", "VanadisChunk"}) || !reflect.DeepEqual(gotChunks[0].AcceptanceCriteria, []string{"Unit tests pass", "Coverage >= 95%"}) {
		t.Errorf("unexpected chunk[0] metadata: %+v", gotChunks[0])
	}
	if gotChunks[1].ID != "chunk-101-1" || gotChunks[1].Repo != "azylman/aerial-config" || gotChunks[1].Effort != "high" || !reflect.DeepEqual(gotChunks[1].TargetFiles, []string{"brain/pkg/vanadis/worker.go", `file,with"special\chars.go`}) || !reflect.DeepEqual(gotChunks[1].TargetSymbols, []string{`Sym,with"quote`}) {
		t.Errorf("unexpected chunk[1] metadata: %+v", gotChunks[1])
	}
	if gotChunks[2].ID != "chunk-101-2" || gotChunks[2].Status != "queued" || gotChunks[2].PRNumber != 42 || !gotChunks[2].StartedAt.Equal(explicitStart) || len(gotChunks[2].TargetFiles) != 0 {
		t.Errorf("unexpected chunk[2]: %+v", gotChunks[2])
	}

	if _, _, err := store.GetVanadisBuild(ctx, "nonexistent"); !errors.Is(err, ErrVanadisBuildNotFound) {
		t.Errorf("expected ErrVanadisBuildNotFound, got %v", err)
	}
	if _, _, err := store.GetVanadisBuild(ctx, "  "); err == nil {
		t.Error("expected error for empty buildID in GetVanadisBuild")
	}

	gotChunk0, err := store.GetVanadisChunk(ctx, "chunk-101-0")
	if err != nil || gotChunk0.Title != "Task 1: Schema and store" || !reflect.DeepEqual(gotChunk0.TargetSymbols, []string{"VanadisBuild", "VanadisChunk"}) {
		t.Fatalf("GetVanadisChunk unexpected result: %+v (err: %v)", gotChunk0, err)
	}
	if _, err := store.GetVanadisChunk(ctx, "nonexistent-chunk"); !errors.Is(err, ErrVanadisChunkNotFound) {
		t.Errorf("expected ErrVanadisChunkNotFound, got %v", err)
	}
	if _, err := store.GetVanadisChunk(ctx, ""); err == nil {
		t.Error("expected error for empty chunkID in GetVanadisChunk")
	}

	gotChunk102Before, err := store.GetVanadisChunk(ctx, "chunk-102-0")
	if err != nil || gotChunk102Before.StartedAt.IsZero() {
		t.Fatalf("expected non-zero initial StartedAt on chunk-102-0: %+v", gotChunk102Before)
	}
	if err := store.AttachVanadisChunkPR(ctx, "chunk-102-0", 512); err != nil {
		t.Fatalf("AttachVanadisChunkPR failed: %v", err)
	}
	gotChunk102PR, err := store.GetVanadisChunk(ctx, "chunk-102-0")
	if err != nil || gotChunk102PR.PRNumber != 512 || gotChunk102PR.Status != "pr_opened" || !gotChunk102PR.StartedAt.Equal(gotChunk102Before.StartedAt) {
		t.Errorf("unexpected chunk after AttachVanadisChunkPR: %+v (err: %v)", gotChunk102PR, err)
	}

	if err := store.AttachVanadisChunkPR(ctx, "chunk-101-1", 513); err != nil {
		t.Fatalf("AttachVanadisChunkPR on chunk-101-1 failed: %v", err)
	}
	gotChunk1PR, err := store.GetVanadisChunk(ctx, "chunk-101-1")
	if err != nil || gotChunk1PR.StartedAt.IsZero() {
		t.Errorf("expected non-zero StartedAt after AttachVanadisChunkPR: %+v", gotChunk1PR)
	}
	if err := store.AttachVanadisChunkPR(ctx, "nonexistent-chunk", 512); !errors.Is(err, ErrVanadisChunkNotFound) {
		t.Errorf("expected ErrVanadisChunkNotFound, got %v", err)
	}
	if err := store.AttachVanadisChunkPR(ctx, "", 512); err == nil {
		t.Error("expected error for empty chunkID in AttachVanadisChunkPR")
	}
	if err := store.AttachVanadisChunkPR(ctx, "chunk-101-0", 0); err == nil {
		t.Error("expected error for pr_number <= 0 in AttachVanadisChunkPR")
	}

	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "build-101", "  msg-build-999  "); err != nil {
		t.Fatalf("UpdateVanadisBuildStatusMessageID failed: %v", err)
	}
	gotBuildWithMsg, _, err := store.GetVanadisBuild(ctx, "build-101")
	if err != nil || gotBuildWithMsg.StatusMessageID != "msg-build-999" {
		t.Fatalf("expected status_message_id 'msg-build-999', got %+v (err: %v)", gotBuildWithMsg, err)
	}
	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "nonexistent-build", "msg-build-999"); !errors.Is(err, ErrVanadisBuildNotFound) {
		t.Errorf("expected ErrVanadisBuildNotFound, got %v", err)
	}
	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "", "msg-build-999"); err == nil {
		t.Error("expected error for empty buildID in UpdateVanadisBuildStatusMessageID")
	}
}

func TestVanadis_StoreParity(t *testing.T) {
	validBuild := VanadisBuild{ID: "build-101", ThreadID: "1555405874565091380", Objective: "Implement vanadis build queue", SharedContext: "Shared context for build 101"}
	validChunk := VanadisChunk{
		ID: "chunk-101-0", BuildID: "build-101", ChunkIndex: 0, Title: "Task 1: Schema and store", Instructions: "Add vanadis_builds and vanadis_chunks tables",
		TargetFiles: []string{"brain/pkg/db/schema.sql", "brain/pkg/db/vanadis.go"}, TargetSymbols: []string{"VanadisBuild", "VanadisChunk"}, Effort: "medium", AcceptanceCriteria: []string{"Unit tests pass", "Coverage >= 95%"},
	}
	explicitStart := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	chunks := []VanadisChunk{
		validChunk,
		{
			ID: "chunk-101-1", BuildID: "build-101", ChunkIndex: 1, Repo: "azylman/aerial-config", Title: "Task 2: River worker", Instructions: "Implement VanadisWorker",
			TargetFiles: []string{"brain/pkg/vanadis/worker.go", `file,with"special\chars.go`}, TargetSymbols: []string{`Sym,with"quote`}, Effort: "HIGH", AcceptanceCriteria: []string{"Worker processes chunk successfully"},
		},
		{ID: "chunk-101-2", BuildID: "build-101", ChunkIndex: 2, Title: "Task 3: Verification", Instructions: "Run tests", Status: "queued", PRNumber: 42, StartedAt: explicitStart},
	}
	titledBuild := VanadisBuild{ID: "build-titled-102", ThreadID: "1555405874565091380", Title: "Redesign Vanadis Build Status Cards", Objective: "Detailed objective for redesigning the cards"}
	runningChunk := VanadisChunk{ID: "chunk-102-0", BuildID: "build-titled-102", ChunkIndex: 0, Title: "Already running chunk", Instructions: "Run immediately", Status: "running"}

	t.Run("SQLStore", func(t *testing.T) {
		store := NewTestStore(t)
		seedVanadisBuildSQLite(t, store, validBuild, chunks)
		seedVanadisBuildSQLite(t, store, titledBuild, []VanadisChunk{runningChunk})
		testVanadisReadAndLifecycle(t, store, explicitStart)
	})

	t.Run("FakeStore", func(t *testing.T) {
		store := NewFakeStore()
		ctx := context.Background()
		if err := store.CreateVanadisBuild(ctx, validBuild, chunks); err != nil {
			t.Fatalf("CreateVanadisBuild failed: %v", err)
		}
		if err := store.CreateVanadisBuild(ctx, validBuild, nil); err == nil {
			t.Error("expected error on duplicate build ID")
		}
		if err := store.CreateVanadisBuild(ctx, VanadisBuild{ID: "build-dup", ThreadID: "1555405874565091380", Objective: "obj", Tier: 3, Status: "queued"}, []VanadisChunk{validChunk}); err == nil {
			t.Error("expected error on duplicate chunk ID")
		}
		if err := store.CreateVanadisBuild(ctx, titledBuild, []VanadisChunk{runningChunk}); err != nil {
			t.Fatalf("CreateVanadisBuild with explicit Title failed: %v", err)
		}
		testVanadisReadAndLifecycle(t, store, explicitStart)
		if err := store.CreateVanadisBuild(ctx, VanadisBuild{ID: "b-dup-idx", ThreadID: "123", Objective: "obj"}, []VanadisChunk{
			{ID: "c-1", ChunkIndex: 0, Title: "t1", Instructions: "i1"},
			{ID: "c-2", ChunkIndex: 0, Title: "t2", Instructions: "i2"},
		}); err == nil {
			t.Error("expected error on duplicate chunk_index")
		}
	})
}

func TestVanadis_Validation(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()
	validBuild := VanadisBuild{ID: "b1", ThreadID: "t1", Objective: "obj"}

	for _, tc := range []struct {
		name   string
		build  VanadisBuild
		chunks []VanadisChunk
	}{
		{"empty_build_id", VanadisBuild{ID: "", ThreadID: "123", Objective: "obj"}, nil},
		{"empty_thread_id", VanadisBuild{ID: "b1", ThreadID: "", Objective: "obj"}, nil},
		{"empty_objective", VanadisBuild{ID: "b1", ThreadID: "123", Objective: ""}, nil},
		{"empty_chunk_id", validBuild, []VanadisChunk{{ID: "", Title: "t", Instructions: "i"}}},
		{"mismatched_chunk_build_id", validBuild, []VanadisChunk{{ID: "c1", BuildID: "other-build", Title: "t", Instructions: "i"}}},
		{"empty_chunk_title", validBuild, []VanadisChunk{{ID: "c1", Title: "", Instructions: "i"}}},
		{"empty_chunk_instructions", validBuild, []VanadisChunk{{ID: "c1", Title: "t", Instructions: ""}}},
		{"invalid_chunk_effort", validBuild, []VanadisChunk{{ID: "c1", Title: "t", Instructions: "i", Effort: "ultra"}}},
		{"numeric_chunk_effort", validBuild, []VanadisChunk{{ID: "c1", Title: "t", Instructions: "i", Effort: "10"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.CreateVanadisBuild(ctx, tc.build, tc.chunks); err == nil {
				t.Errorf("[%s] expected validation error", tc.name)
			}
		})
	}
}

func TestVanadis_FakeStore_FailuresAndClosed(t *testing.T) {
	simErr := errors.New("simulated error")
	for _, op := range []string{"CreateVanadisBuild", "GetVanadisBuild", "GetVanadisChunk", "AttachVanadisChunkPR", "UpdateVanadisBuildStatusMessageID"} {
		t.Run(op, func(t *testing.T) {
			store := NewFakeStore()
			ctx := context.Background()
			store.FailNext(op, simErr)
			var err error
			switch op {
			case "CreateVanadisBuild":
				err = store.CreateVanadisBuild(ctx, VanadisBuild{}, nil)
			case "GetVanadisBuild":
				_, _, err = store.GetVanadisBuild(ctx, "b1")
			case "GetVanadisChunk":
				_, err = store.GetVanadisChunk(ctx, "c1")
			case "AttachVanadisChunkPR":
				err = store.AttachVanadisChunkPR(ctx, "c1", 1)
			case "UpdateVanadisBuildStatusMessageID":
				err = store.UpdateVanadisBuildStatusMessageID(ctx, "b1", "m")
			}
			if !errors.Is(err, simErr) {
				t.Fatalf("expected %v, got %v", simErr, err)
			}
		})
	}

	closed := NewFakeStore()
	_ = closed.Close()
	ctx := context.Background()
	for _, err := range []error{
		closed.CreateVanadisBuild(ctx, VanadisBuild{}, nil),
		c2(closed.GetVanadisBuild(ctx, "b1")),
		c1(closed.GetVanadisChunk(ctx, "c1")),
		closed.AttachVanadisChunkPR(ctx, "c1", 1),
		closed.UpdateVanadisBuildStatusMessageID(ctx, "b1", "m"),
	} {
		if !errors.Is(err, sql.ErrConnDone) {
			t.Errorf("expected ErrConnDone, got %v", err)
		}
	}
}

func TestVanadis_SQLStore_NilAndClosedBranches(t *testing.T) {
	ctx := context.Background()
	closedDB, err := initTestSQLite(t.TempDir() + "/closed_vanadis.db")
	if err != nil {
		t.Fatalf("initTestSQLite failed: %v", err)
	}
	closedStore := NewSQLStore(closedDB)
	_ = closedDB.Close()

	for _, tc := range []struct {
		name  string
		store *SQLStore
	}{
		{"nil_store", nil},
		{"nil_db", &SQLStore{db: nil}},
		{"closed_db", closedStore.(*SQLStore)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, err := range []error{
				c2(tc.store.GetVanadisBuild(ctx, "b1")),
				c1(tc.store.GetVanadisChunk(ctx, "c1")),
				tc.store.AttachVanadisChunkPR(ctx, "c1", 1),
				tc.store.UpdateVanadisBuildStatusMessageID(ctx, "b1", "m"),
			} {
				if err == nil {
					t.Errorf("[%s] expected error", tc.name)
				}
			}
		})
	}
}

func TestParsePostgresTextArrayEdgeCases(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", []string{}},
		{"braces_only", "{}", []string{}},
		{"spaces_in_braces", "{   }", []string{}},
		{"elements", "{a,b,c}", []string{"a", "b", "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := parsePostgresTextArray(tc.in); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parsePostgresTextArray(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeVanadisEffort(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
		err  bool
	}{
		{"empty", "", "medium", false},
		{"whitespace", "   ", "medium", false},
		{"low", "low", "low", false},
		{"LOW", "LOW", "low", false},
		{"medium_padded", "  Medium  ", "medium", false},
		{"high", "high", "high", false},
		{"HIGH", "HIGH", "high", false},
		{"invalid", "invalid", "", true},
		{"unknown", "unknown", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeVanadisEffort(tc.in)
			if (err != nil) != tc.err || got != tc.want {
				t.Errorf("normalizeVanadisEffort(%q) = (%q, %v), want (%q, err=%v)", tc.in, got, err, tc.want, tc.err)
			}
		})
	}
}

type fakeRowScanner struct {
	scanFn func(dest ...any) error
}

func (f fakeRowScanner) Scan(dest ...any) error { return f.scanFn(dest...) }

func TestScanVanadisChunk_EdgeCases(t *testing.T) {
	if _, err := scanVanadisChunk(fakeRowScanner{scanFn: func(dest ...any) error { return errors.New("fail") }}, false); err == nil {
		t.Error("expected scan failure")
	}
	s := fakeRowScanner{scanFn: func(dest ...any) error {
		*dest[0].(*string), *dest[1].(*string), *dest[3].(*string) = "c1", "b1", "repo"
		*dest[4].(*string), *dest[5].(*string), *dest[8].(*string) = "title", "inst", ""
		*dest[10].(*string), *dest[12].(*sql.NullTime) = "pending", sql.NullTime{Time: time.Now(), Valid: true}
		*dest[13].(*time.Time), *dest[14].(*time.Time) = time.Now(), time.Now()
		return nil
	}}
	c, err := scanVanadisChunk(s, true)
	if err != nil || c.Effort != "medium" || len(c.TargetFiles) != 0 || len(c.TargetSymbols) != 0 || len(c.AcceptanceCriteria) != 0 {
		t.Fatalf("unexpected chunk: %+v, err=%v", c, err)
	}
}
