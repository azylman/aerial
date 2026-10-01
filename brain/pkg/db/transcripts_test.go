package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestTranscriptStore_CRUD_And_SyncStates(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	// 1. Initially empty
	syncStates, err := store.GetSessionSyncStates(ctx)
	if err != nil {
		t.Fatalf("GetSessionSyncStates failed: %v", err)
	}
	if len(syncStates) != 0 {
		t.Fatalf("expected 0 sync states, got %d", len(syncStates))
	}

	// 2. Error cases on UpsertSessionSummary
	if err := store.UpsertSessionSummary(ctx, SessionSummary{SessionID: ""}); err == nil {
		t.Errorf("expected error for empty session ID, got nil")
	}

	var nilStore *SQLStore
	if err := nilStore.UpsertSessionSummary(ctx, SessionSummary{SessionID: "s1"}); err == nil {
		t.Errorf("expected error on nil store UpsertSessionSummary")
	}
	if _, err := nilStore.GetSessionSyncStates(ctx); err == nil {
		t.Errorf("expected error on nil store GetSessionSyncStates")
	}

	// 3. Upsert a new SessionSummary
	now := time.Now().UTC().Truncate(time.Second)
	summary := SessionSummary{
		SessionID:            "sess-1",
		ThreadID:             "th-1",
		Summary:              "Debugged and stabilized the neural control interface",
		LastIndexedStep:      10,
		LastMtime:            now,
		SummaryStepWatermark: 5,
		IsSettled:            false,
	}
	if err := store.UpsertSessionSummary(ctx, summary); err != nil {
		t.Fatalf("UpsertSessionSummary failed: %v", err)
	}

	syncStates, err = store.GetSessionSyncStates(ctx)
	if err != nil {
		t.Fatalf("GetSessionSyncStates after insert failed: %v", err)
	}
	state1, ok := syncStates["sess-1"]
	if !ok {
		t.Fatalf("expected sess-1 in sync states")
	}
	if state1.LastIndexedStep != 10 {
		t.Errorf("expected LastIndexedStep=10, got %d", state1.LastIndexedStep)
	}
	if state1.IsSettled {
		t.Errorf("expected IsSettled=false, got true")
	}
	if state1.LastMtime.IsZero() {
		t.Errorf("expected non-zero LastMtime")
	}

	// 4. Update existing SessionSummary to settled state
	updateSummary := SessionSummary{
		SessionID:            "sess-1",
		Summary:              "Updated: Completed neural interface stabilization and verified tests",
		LastIndexedStep:      25,
		SummaryStepWatermark: 25,
		IsSettled:            true,
	}
	if err := store.UpsertSessionSummary(ctx, updateSummary); err != nil {
		t.Fatalf("UpsertSessionSummary update failed: %v", err)
	}

	syncStates, err = store.GetSessionSyncStates(ctx)
	if err != nil {
		t.Fatalf("GetSessionSyncStates after update failed: %v", err)
	}
	stateUpdated := syncStates["sess-1"]
	if stateUpdated.LastIndexedStep != 25 {
		t.Errorf("expected LastIndexedStep=25, got %d", stateUpdated.LastIndexedStep)
	}
	if !stateUpdated.IsSettled {
		t.Errorf("expected IsSettled=true, got false")
	}

	// 5. Test sentinel reset for file truncation (LastIndexedStep: -2 resets to -1)
	resetSummary := SessionSummary{
		SessionID:       "sess-1",
		LastIndexedStep: -2,
	}
	if err := store.UpsertSessionSummary(ctx, resetSummary); err != nil {
		t.Fatalf("UpsertSessionSummary reset failed: %v", err)
	}

	syncStates, err = store.GetSessionSyncStates(ctx)
	if err != nil {
		t.Fatalf("GetSessionSyncStates after reset failed: %v", err)
	}
	if syncStates["sess-1"].LastIndexedStep != -1 {
		t.Errorf("expected LastIndexedStep=-1 after reset, got %d", syncStates["sess-1"].LastIndexedStep)
	}

	// 6. Test direct package-level functions with nil / background context
	if _, err := GetSessionSyncStates(nil); err == nil {
		t.Errorf("expected error on GetSessionSyncStates with nil db")
	}
	if err := UpsertSessionSummary(nil, summary); err == nil {
		t.Errorf("expected error on UpsertSessionSummary with nil db")
	}
}

func TestTranscriptStore_BatchInsertTranscriptSteps(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	// 1. Initial upsert of parent session
	if err := store.UpsertSessionSummary(ctx, SessionSummary{
		SessionID: "sess-steps-1",
		ThreadID:  "th-steps-1",
		Summary:   "Session for testing step batch inserts",
	}); err != nil {
		t.Fatalf("UpsertSessionSummary failed: %v", err)
	}

	// 2. Batch insert steps
	now := time.Now().UTC()
	steps := []TranscriptStep{
		{
			SessionID: "sess-steps-1",
			StepIndex: 0,
			StepType:  "user_message",
			ToolName:  "",
			Content:   "Deploy the application to staging environment",
			CreatedAt: now.Add(-2 * time.Minute),
		},
		{
			SessionID: "sess-steps-1",
			StepIndex: 1,
			StepType:  "tool_call",
			ToolName:  "run_command",
			Content:   "kubectl apply -f deployment.yaml",
			CreatedAt: now.Add(-1 * time.Minute),
		},
		{
			SessionID: "sess-steps-1",
			StepIndex: 2,
			StepType:  "tool_result",
			ToolName:  "run_command",
			Content:   "deployment.apps/aerial configured",
			CreatedAt: now,
		},
	}

	if err := store.BatchInsertTranscriptSteps(ctx, steps); err != nil {
		t.Fatalf("BatchInsertTranscriptSteps failed: %v", err)
	}

	// 3. Search to verify all 3 steps exist
	results, err := store.SearchTranscriptSteps(ctx, "", "sess-steps-1", "", 10)
	if err != nil {
		t.Fatalf("SearchTranscriptSteps failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(results))
	}

	// 4. Batch insert with duplicate step (conflict test) - must be idempotent
	dupSteps := []TranscriptStep{
		{
			SessionID: "sess-steps-1",
			StepIndex: 1, // Duplicate
			StepType:  "tool_call",
			ToolName:  "run_command",
			Content:   "DIFFERENT CONTENT THAT SHOULD BE IGNORED",
		},
		{
			SessionID: "sess-steps-1",
			StepIndex: 3, // New step
			StepType:  "model_response",
			ToolName:  "",
			Content:   "Deployment completed successfully",
		},
	}
	if err := store.BatchInsertTranscriptSteps(ctx, dupSteps); err != nil {
		t.Fatalf("BatchInsertTranscriptSteps with duplicates failed: %v", err)
	}

	resultsAfter, err := store.SearchTranscriptSteps(ctx, "", "sess-steps-1", "", 10)
	if err != nil {
		t.Fatalf("SearchTranscriptSteps after dup failed: %v", err)
	}
	if len(resultsAfter) != 4 {
		t.Fatalf("expected 4 steps total after conflict-ignoring insert, got %d", len(resultsAfter))
	}

	// Verify original content for step 1 was preserved
	for _, r := range resultsAfter {
		if r.StepIndex == 1 && r.Content != "kubectl apply -f deployment.yaml" {
			t.Errorf("expected original content for step 1 preserved, got %q", r.Content)
		}
	}

	// 5. Empty slice and invalid steps
	if err := store.BatchInsertTranscriptSteps(ctx, nil); err != nil {
		t.Errorf("expected nil error for nil slice, got %v", err)
	}
	if err := store.BatchInsertTranscriptSteps(ctx, []TranscriptStep{{SessionID: ""}}); err != nil {
		t.Errorf("expected nil error for empty sessionID step, got %v", err)
	}

	// Nil store and nil db
	var nilStore *SQLStore
	if err := nilStore.BatchInsertTranscriptSteps(ctx, steps); err == nil {
		t.Errorf("expected error on nil store BatchInsertTranscriptSteps")
	}
	if err := BatchInsertTranscriptSteps(nil, steps); err == nil {
		t.Errorf("expected error on BatchInsertTranscriptSteps with nil db")
	}
}

func TestTranscriptStore_CascadeDelete(t *testing.T) {
	database := initTestSQLiteDB(t)
	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. Insert session summary
	if err := store.UpsertSessionSummary(ctx, SessionSummary{
		SessionID: "sess-cascade-1",
		ThreadID:  "th-cascade-1",
		Summary:   "Session to verify foreign key cascade deletion",
	}); err != nil {
		t.Fatalf("UpsertSessionSummary failed: %v", err)
	}

	// 2. Insert transcript steps for sess-cascade-1
	steps := []TranscriptStep{
		{
			SessionID: "sess-cascade-1",
			StepIndex: 0,
			Content:   "Step 0 to be cascaded",
		},
		{
			SessionID: "sess-cascade-1",
			StepIndex: 1,
			Content:   "Step 1 to be cascaded",
		},
	}
	if err := store.BatchInsertTranscriptSteps(ctx, steps); err != nil {
		t.Fatalf("BatchInsertTranscriptSteps failed: %v", err)
	}

	// Verify steps exist
	beforeDelete, err := store.SearchTranscriptSteps(ctx, "", "sess-cascade-1", "", 10)
	if err != nil || len(beforeDelete) != 2 {
		t.Fatalf("expected 2 steps before delete, got %d (err: %v)", len(beforeDelete), err)
	}

	// 3. Delete parent session summary directly via SQL
	if _, err := database.Exec("DELETE FROM session_summaries WHERE session_id = ?", "sess-cascade-1"); err != nil {
		t.Fatalf("failed to delete session summary: %v", err)
	}

	// 4. Verify transcript steps were deleted via CASCADE
	afterDelete, err := store.SearchTranscriptSteps(ctx, "", "sess-cascade-1", "", 10)
	if err != nil {
		t.Fatalf("SearchTranscriptSteps after cascade delete failed: %v", err)
	}
	if len(afterDelete) != 0 {
		t.Fatalf("expected 0 steps after cascade delete, got %d", len(afterDelete))
	}
}

func TestTranscriptStore_SearchSessionSummaries(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	embVector := make([]float32, ExpectedEmbeddingDim)
	embVector[0] = 1.0 // Unit vector along dim 0

	embQueue := make([]float32, ExpectedEmbeddingDim)
	embQueue[1] = 1.0 // Orthogonal vector along dim 1

	// Insert test summaries
	summaries := []SessionSummary{
		{
			SessionID: "sess-vector",
			ThreadID:  "th-vector",
			Summary:   "Investigated and fixed vector cosine similarity search in database",
			Embedding: embVector,
		},
		{
			SessionID: "sess-queue",
			ThreadID:  "th-queue",
			Summary:   "Architectural optimization of worker pool event queue processing concurrency",
			Embedding: embQueue,
		},
		{
			SessionID: "sess-discord",
			ThreadID:  "th-discord",
			Summary:   "Implemented Discord bot webhook notification dispatching and formatting",
		},
	}

	for _, s := range summaries {
		if err := store.UpsertSessionSummary(ctx, s); err != nil {
			t.Fatalf("UpsertSessionSummary failed for %s: %v", s.SessionID, err)
		}
	}

	// 1. Sparse text-only search
	t.Run("sparse text-only search", func(t *testing.T) {
		results, err := store.SearchSessionSummaries(ctx, nil, "vector cosine", 5, 0.1)
		if err != nil {
			t.Fatalf("SearchSessionSummaries failed: %v", err)
		}
		if len(results) == 0 {
			t.Fatalf("expected results for 'vector cosine', got 0")
		}
		if results[0].SessionID != "sess-vector" {
			t.Errorf("expected top result 'sess-vector', got %q", results[0].SessionID)
		}

		resultsQueue, err := store.SearchSessionSummaries(ctx, nil, "worker pool concurrency", 5, 0.1)
		if err != nil {
			t.Fatalf("SearchSessionSummaries failed: %v", err)
		}
		if len(resultsQueue) == 0 || resultsQueue[0].SessionID != "sess-queue" {
			t.Errorf("expected top result 'sess-queue', got %v", resultsQueue)
		}
	})

	// 2. Dense vector-only search
	t.Run("dense vector-only search", func(t *testing.T) {
		results, err := store.SearchSessionSummaries(ctx, embVector, "", 5, 0.5)
		if err != nil {
			t.Fatalf("SearchSessionSummaries failed: %v", err)
		}
		if len(results) == 0 {
			t.Fatalf("expected results for embVector, got 0")
		}
		if results[0].SessionID != "sess-vector" {
			t.Errorf("expected top result 'sess-vector', got %q", results[0].SessionID)
		}
		if results[0].Score < 0.99 {
			t.Errorf("expected cosine similarity close to 1.0, got %f", results[0].Score)
		}
	})

	// 3. Hybrid search (50/50 RRF)
	t.Run("hybrid search", func(t *testing.T) {
		results, err := store.SearchSessionSummaries(ctx, embVector, "vector similarity", 5, 0.3)
		if err != nil {
			t.Fatalf("SearchSessionSummaries failed: %v", err)
		}
		if len(results) == 0 {
			t.Fatalf("expected hybrid search results, got 0")
		}
		if results[0].SessionID != "sess-vector" {
			t.Errorf("expected top result 'sess-vector', got %q", results[0].SessionID)
		}
		if results[0].Score <= 0 {
			t.Errorf("expected positive score, got %f", results[0].Score)
		}
	})

	// 4. MinScore and limit
	t.Run("minScore and limit", func(t *testing.T) {
		highScoreResults, err := store.SearchSessionSummaries(ctx, nil, "worker", 5, 2.0)
		if err != nil {
			t.Fatalf("SearchSessionSummaries failed: %v", err)
		}
		if len(highScoreResults) != 0 {
			t.Errorf("expected 0 results with impossible minScore 2.0, got %d", len(highScoreResults))
		}

		limitResults, err := store.SearchSessionSummaries(ctx, nil, "in", 1, 0.0)
		if err != nil {
			t.Fatalf("SearchSessionSummaries failed: %v", err)
		}
		if len(limitResults) > 1 {
			t.Errorf("expected max 1 result with limit=1, got %d", len(limitResults))
		}
	})

	// 5. Edge cases: empty query & embedding, nil store, nil db
	t.Run("edge cases", func(t *testing.T) {
		res, err := store.SearchSessionSummaries(ctx, nil, "", 10, 0.0)
		if err != nil || res != nil {
			t.Errorf("expected nil, nil on empty query and embedding, got %v, %v", res, err)
		}

		var nilStore *SQLStore
		if _, err := nilStore.SearchSessionSummaries(ctx, embVector, "q", 10, 0.1); err == nil {
			t.Errorf("expected error on nil store SearchSessionSummaries")
		}
		if res, err := SearchSessionSummaries(nil, embVector, "q", 10, 0.1); res != nil || err != nil {
			t.Errorf("expected nil, nil on SearchSessionSummaries with nil db")
		}
	})
}

func TestTranscriptStore_SearchTranscriptSteps(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	_ = store.UpsertSessionSummary(ctx, SessionSummary{SessionID: "sess-steps-a"})
	_ = store.UpsertSessionSummary(ctx, SessionSummary{SessionID: "sess-steps-b"})

	now := time.Now().UTC()
	steps := []TranscriptStep{
		{
			SessionID: "sess-steps-a",
			StepIndex: 0,
			StepType:  "user_message",
			ToolName:  "",
			Content:   "Deploying cluster ingress controller and cert-manager",
			CreatedAt: now.Add(-3 * time.Minute),
		},
		{
			SessionID: "sess-steps-a",
			StepIndex: 1,
			StepType:  "tool_call",
			ToolName:  "run_command",
			Content:   "kubectl apply -f ingress.yaml",
			CreatedAt: now.Add(-2 * time.Minute),
		},
		{
			SessionID: "sess-steps-a",
			StepIndex: 2,
			StepType:  "tool_result",
			ToolName:  "run_command",
			Content:   "error: 502 bad gateway connecting to webhook backend",
			CreatedAt: now.Add(-1 * time.Minute),
		},
		{
			SessionID: "sess-steps-b",
			StepIndex: 0,
			StepType:  "tool_call",
			ToolName:  "read_file",
			Content:   "reading ingress.yaml configuration for host domain",
			CreatedAt: now,
		},
	}

	if err := store.BatchInsertTranscriptSteps(ctx, steps); err != nil {
		t.Fatalf("BatchInsertTranscriptSteps failed: %v", err)
	}

	// 1. Text search
	res, err := store.SearchTranscriptSteps(ctx, "bad gateway", "", "", 10)
	if err != nil {
		t.Fatalf("SearchTranscriptSteps failed: %v", err)
	}
	if len(res) != 1 || res[0].StepIndex != 2 {
		t.Fatalf("expected step 2 for 'bad gateway', got %v", res)
	}

	// 2. Session filter
	resSession, err := store.SearchTranscriptSteps(ctx, "ingress", "sess-steps-b", "", 10)
	if err != nil {
		t.Fatalf("SearchTranscriptSteps with session filter failed: %v", err)
	}
	if len(resSession) != 1 || resSession[0].SessionID != "sess-steps-b" {
		t.Fatalf("expected 1 result from sess-steps-b, got %v", resSession)
	}

	// 3. Tool filter
	resTool, err := store.SearchTranscriptSteps(ctx, "ingress", "", "read_file", 10)
	if err != nil {
		t.Fatalf("SearchTranscriptSteps with tool filter failed: %v", err)
	}
	if len(resTool) != 1 || resTool[0].ToolName != "read_file" {
		t.Fatalf("expected 1 result with tool read_file, got %v", resTool)
	}

	// 4. Empty query with tool filter
	resToolOnly, err := store.SearchTranscriptSteps(ctx, "", "", "read_file", 10)
	if err != nil {
		t.Fatalf("SearchTranscriptSteps with empty query and tool filter failed: %v", err)
	}
	if len(resToolOnly) != 1 || resToolOnly[0].ToolName != "read_file" {
		t.Fatalf("expected 1 result with tool read_file, got %v", resToolOnly)
	}

	// 5. Empty query with no filters
	resEmpty, err := store.SearchTranscriptSteps(ctx, "", "", "", 10)
	if err != nil || resEmpty != nil {
		t.Errorf("expected nil, nil on empty query with no filters, got %v, %v", resEmpty, err)
	}

	// 6. Nil store and nil db
	var nilStore *SQLStore
	if _, err := nilStore.SearchTranscriptSteps(ctx, "q", "", "", 10); err == nil {
		t.Errorf("expected error on nil store SearchTranscriptSteps")
	}
	if res, err := SearchTranscriptSteps(nil, "q", "", "", 10); res != nil || err != nil {
		t.Errorf("expected nil, nil on SearchTranscriptSteps with nil db")
	}
}

func TestTranscriptStore_FakeStore(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	embValid := make([]float32, ExpectedEmbeddingDim)
	embValid[0] = 1.0

	// 1. Initial state
	states, err := s.GetSessionSyncStates(ctx)
	if err != nil || len(states) != 0 {
		t.Fatalf("expected empty sync states, got %v (err: %v)", states, err)
	}

	// 2. Upsert error on empty session ID
	if err := s.UpsertSessionSummary(ctx, SessionSummary{SessionID: ""}); err == nil {
		t.Errorf("expected error for empty sessionID on FakeStore")
	}

	// 3. Upsert valid summary
	now := time.Now().UTC()
	summary := SessionSummary{
		SessionID:            "fake-sess-1",
		ThreadID:             "fake-th-1",
		Summary:              "Testing FakeStore transcript methods and hybrid search",
		Embedding:            embValid,
		LastIndexedStep:      5,
		LastMtime:            now,
		SummaryStepWatermark: 5,
		IsSettled:            false,
	}
	if err := s.UpsertSessionSummary(ctx, summary); err != nil {
		t.Fatalf("FakeStore.UpsertSessionSummary failed: %v", err)
	}

	states, err = s.GetSessionSyncStates(ctx)
	if err != nil || len(states) != 1 {
		t.Fatalf("expected 1 sync state, got %d (err: %v)", len(states), err)
	}
	if states["fake-sess-1"].LastIndexedStep != 5 {
		t.Errorf("expected LastIndexedStep=5, got %d", states["fake-sess-1"].LastIndexedStep)
	}

	// 4. Update existing summary in FakeStore
	update := SessionSummary{
		SessionID:            "fake-sess-1",
		LastIndexedStep:      20,
		SummaryStepWatermark: 20,
		IsSettled:            true,
	}
	if err := s.UpsertSessionSummary(ctx, update); err != nil {
		t.Fatalf("FakeStore.UpsertSessionSummary update failed: %v", err)
	}
	states, _ = s.GetSessionSyncStates(ctx)
	if !states["fake-sess-1"].IsSettled || states["fake-sess-1"].LastIndexedStep != 20 {
		t.Errorf("expected updated settled state in FakeStore, got %+v", states["fake-sess-1"])
	}

	// Test sentinel reset
	_ = s.UpsertSessionSummary(ctx, SessionSummary{SessionID: "fake-sess-1", LastIndexedStep: -2})
	states, _ = s.GetSessionSyncStates(ctx)
	if states["fake-sess-1"].LastIndexedStep != -1 {
		t.Errorf("expected reset to -1, got %d", states["fake-sess-1"].LastIndexedStep)
	}

	// 5. BatchInsertTranscriptSteps on FakeStore
	steps := []TranscriptStep{
		{
			SessionID: "fake-sess-1",
			StepIndex: 0,
			ToolName:  "run_command",
			Content:   "go test ./pkg/db/...",
		},
		{
			SessionID: "fake-sess-1",
			StepIndex: 1,
			ToolName:  "run_command",
			Content:   "PASS: all tests green",
		},
		{
			SessionID: "", // Ignored
		},
	}
	if err := s.BatchInsertTranscriptSteps(ctx, steps); err != nil {
		t.Fatalf("FakeStore.BatchInsertTranscriptSteps failed: %v", err)
	}

	// Conflict deduplication test
	if err := s.BatchInsertTranscriptSteps(ctx, []TranscriptStep{{SessionID: "fake-sess-1", StepIndex: 0, Content: "ignored"}}); err != nil {
		t.Fatalf("BatchInsertTranscriptSteps duplicate failed: %v", err)
	}

	// 6. SearchSessionSummaries on FakeStore
	resSummaries, err := s.SearchSessionSummaries(ctx, embValid, "hybrid search", 5, 0.2)
	if err != nil {
		t.Fatalf("FakeStore.SearchSessionSummaries failed: %v", err)
	}
	if len(resSummaries) != 1 || resSummaries[0].SessionID != "fake-sess-1" {
		t.Errorf("expected match for fake-sess-1, got %v", resSummaries)
	}

	// Empty query & embedding
	resEmptySum, err := s.SearchSessionSummaries(ctx, nil, "", 5, 0.0)
	if err != nil || resEmptySum != nil {
		t.Errorf("expected nil, nil on empty query on FakeStore, got %v, %v", resEmptySum, err)
	}

	// 7. SearchTranscriptSteps on FakeStore
	resSteps, err := s.SearchTranscriptSteps(ctx, "green", "", "", 5)
	if err != nil {
		t.Fatalf("FakeStore.SearchTranscriptSteps failed: %v", err)
	}
	if len(resSteps) != 1 || resSteps[0].StepIndex != 1 {
		t.Errorf("expected step 1 for 'green', got %v", resSteps)
	}

	// Tool filter
	resTool, err := s.SearchTranscriptSteps(ctx, "", "", "run_command", 5)
	if err != nil || len(resTool) != 2 {
		t.Errorf("expected 2 steps with tool filter, got %d (err: %v)", len(resTool), err)
	}

	// Empty query and no filters
	resEmptySteps, err := s.SearchTranscriptSteps(ctx, "", "", "", 5)
	if err != nil || resEmptySteps != nil {
		t.Errorf("expected nil, nil on empty steps search, got %v, %v", resEmptySteps, err)
	}

	// 8. DeleteSessionSummary cascade in FakeStore
	s.DeleteSessionSummary("fake-sess-1")
	statesAfterDel, _ := s.GetSessionSyncStates(ctx)
	if len(statesAfterDel) != 0 {
		t.Errorf("expected 0 sync states after DeleteSessionSummary, got %d", len(statesAfterDel))
	}
	stepsAfterDel, _ := s.SearchTranscriptSteps(ctx, "", "fake-sess-1", "", 10)
	if len(stepsAfterDel) != 0 {
		t.Errorf("expected 0 steps after DeleteSessionSummary, got %d", len(stepsAfterDel))
	}

	// 9. FailNext verification for all 5 methods
	simErr := errors.New("simulated error")
	methods := []string{
		"GetSessionSyncStates",
		"UpsertSessionSummary",
		"BatchInsertTranscriptSteps",
		"SearchSessionSummaries",
		"SearchTranscriptSteps",
	}
	for _, m := range methods {
		s.FailNext(m, simErr)
		var callErr error
		switch m {
		case "GetSessionSyncStates":
			_, callErr = s.GetSessionSyncStates(ctx)
		case "UpsertSessionSummary":
			callErr = s.UpsertSessionSummary(ctx, SessionSummary{SessionID: "s"})
		case "BatchInsertTranscriptSteps":
			callErr = s.BatchInsertTranscriptSteps(ctx, nil)
		case "SearchSessionSummaries":
			_, callErr = s.SearchSessionSummaries(ctx, nil, "q", 5, 0.1)
		case "SearchTranscriptSteps":
			_, callErr = s.SearchTranscriptSteps(ctx, "q", "", "", 5)
		}
		if !errors.Is(callErr, simErr) {
			t.Errorf("expected simulated error for %s, got %v", m, callErr)
		}
	}

	// 10. Close verification for all 5 methods
	_ = s.Close()
	if _, err := s.GetSessionSyncStates(ctx); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected sql.ErrConnDone on GetSessionSyncStates")
	}
	if err := s.UpsertSessionSummary(ctx, SessionSummary{SessionID: "s"}); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected sql.ErrConnDone on UpsertSessionSummary")
	}
	if err := s.BatchInsertTranscriptSteps(ctx, nil); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected sql.ErrConnDone on BatchInsertTranscriptSteps")
	}
	if _, err := s.SearchSessionSummaries(ctx, nil, "q", 5, 0.1); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected sql.ErrConnDone on SearchSessionSummaries")
	}
	if _, err := s.SearchTranscriptSteps(ctx, "q", "", "", 5); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected sql.ErrConnDone on SearchTranscriptSteps")
	}
}

func TestTranscriptStore_FakeStore_WithTx(t *testing.T) {
	ctx := context.Background()

	t.Run("commit persists transcript data", func(t *testing.T) {
		s := NewFakeStore()
		err := s.WithTx(ctx, func(tx Store) error {
			if err := tx.UpsertSessionSummary(ctx, SessionSummary{
				SessionID: "tx-sess-1",
				Summary:   "Transactional session summary",
			}); err != nil {
				return err
			}
			return tx.BatchInsertTranscriptSteps(ctx, []TranscriptStep{
				{
					SessionID: "tx-sess-1",
					StepIndex: 0,
					Content:   "Transactional step content",
				},
			})
		})
		if err != nil {
			t.Fatalf("WithTx failed: %v", err)
		}

		states, err := s.GetSessionSyncStates(ctx)
		if err != nil || len(states) != 1 {
			t.Fatalf("expected 1 session committed, got %d", len(states))
		}
		steps, err := s.SearchTranscriptSteps(ctx, "Transactional", "", "", 10)
		if err != nil || len(steps) != 1 {
			t.Fatalf("expected 1 step committed, got %d", len(steps))
		}
	})

	t.Run("rollback discards transcript data", func(t *testing.T) {
		s := NewFakeStore()
		expectedErr := errors.New("abort tx")
		err := s.WithTx(ctx, func(tx Store) error {
			_ = tx.UpsertSessionSummary(ctx, SessionSummary{
				SessionID: "tx-rollback-1",
				Summary:   "Should be discarded",
			})
			_ = tx.BatchInsertTranscriptSteps(ctx, []TranscriptStep{
				{
					SessionID: "tx-rollback-1",
					StepIndex: 0,
					Content:   "Should be discarded",
				},
			})
			return expectedErr
		})
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected %v, got %v", expectedErr, err)
		}

		states, _ := s.GetSessionSyncStates(ctx)
		if len(states) != 0 {
			t.Errorf("expected 0 sessions after rollback, got %d", len(states))
		}
		steps, _ := s.SearchTranscriptSteps(ctx, "discarded", "", "", 10)
		if len(steps) != 0 {
			t.Errorf("expected 0 steps after rollback, got %d", len(steps))
		}
	})
}

func TestTranscriptStore_PostgresBranchesCoverage(t *testing.T) {
	database := initTestSQLiteDB(t)
	ctx := context.Background()

	embValid := make([]float32, ExpectedEmbeddingDim)
	embValid[0] = 1.0

	// Force isPg = true on non-pg DB to exercise the Postgres SQL generation branches
	// 1. UpsertSessionSummaryWithContext
	summary := SessionSummary{
		SessionID:       "pg-sess-1",
		ThreadID:        "th-pg-1",
		Summary:         "Testing postgres branch SQL",
		Embedding:       embValid,
		LastIndexedStep: 10,
	}
	_ = UpsertSessionSummaryWithContext(ctx, database, true, summary)

	// 2. BatchInsertTranscriptStepsWithContext
	steps := []TranscriptStep{
		{
			SessionID: "pg-sess-1",
			StepIndex: 0,
			Content:   "Postgres branch step",
		},
	}
	_ = BatchInsertTranscriptStepsWithContext(ctx, database, true, steps)

	// 3. SearchSessionSummariesWithContext (hybrid, dense-only, sparse-only)
	_, _ = SearchSessionSummariesWithContext(ctx, database, true, embValid, "postgres", 5, 0.1)
	_, _ = SearchSessionSummariesWithContext(ctx, database, true, embValid, "", 5, 0.1)
	_, _ = SearchSessionSummariesWithContext(ctx, database, true, nil, "postgres", 5, 0.1)

	// 4. SearchTranscriptStepsWithContext (with text query and without text query)
	_, _ = SearchTranscriptStepsWithContext(ctx, database, true, "postgres", "pg-sess-1", "run_command", 5)
	_, _ = SearchTranscriptStepsWithContext(ctx, database, true, "", "pg-sess-1", "run_command", 5)

	// 5. ParseDBTime edge cases in transcripts
	tStr := "2026-10-01T12:00:00Z"
	if parsed, ok := ParseDBTime(tStr); !ok || parsed.IsZero() {
		t.Errorf("expected parsed time from RFC3339 string")
	}
	if _, ok := ParseDBTime(nil); ok {
		t.Errorf("expected false for ParseDBTime(nil)")
	}
}

func TestTranscriptStore_GetSessionSyncStatesWithContext_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbMem, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbMem.Close()

	// 1. Success with postgres query
	var capturedQuery string
	mockSuccess := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			capturedQuery = query
			return dbMem.QueryContext(ctx, "SELECT 'sess-1' AS session_id, '2026-10-01 12:00:00' AS last_mtime, 10 AS last_indexed_step, 1 AS is_settled")
		},
	}
	states, err := GetSessionSyncStatesWithContext(ctx, mockSuccess, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(capturedQuery, "SELECT session_id, last_mtime, last_indexed_step, is_settled FROM session_summaries") {
		t.Errorf("unexpected query: %s", capturedQuery)
	}
	if len(states) != 1 || states["sess-1"].LastIndexedStep != 10 || !states["sess-1"].IsSettled {
		t.Errorf("unexpected states: %+v", states)
	}

	// 2. Scan error (wrong column count)
	mockScanErr := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return dbMem.QueryContext(ctx, "SELECT 'sess-1' AS session_id")
		},
	}
	_, err = GetSessionSyncStatesWithContext(ctx, mockScanErr, true)
	if err == nil || !strings.Contains(err.Error(), "scan session sync state") {
		t.Errorf("expected scan error, got: %v", err)
	}

	// 3. Query error
	mockQueryErr := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return nil, errors.New("simulated sync states query failure")
		},
	}
	_, err = GetSessionSyncStatesWithContext(ctx, mockQueryErr, true)
	if err == nil || !strings.Contains(err.Error(), "query session sync states") {
		t.Errorf("expected query error, got: %v", err)
	}
}

func TestTranscriptStore_UpsertSessionSummaryWithContext_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 0.5
	summary := SessionSummary{
		SessionID:            "sess-upsert-1",
		ThreadID:             "th-1",
		Summary:              "Summary text",
		Embedding:            emb,
		LastIndexedStep:      5,
		LastMtime:            time.Now().UTC(),
		SummaryStepWatermark: 5,
		IsSettled:            true,
	}

	// 1. Success on postgres exec
	var capturedQuery string
	var capturedArgs []any
	mockSuccess := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			capturedQuery = query
			capturedArgs = args
			return testCustomResult{rows: 1}, nil
		},
	}
	err := UpsertSessionSummaryWithContext(ctx, mockSuccess, true, summary)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(capturedQuery, "INSERT INTO session_summaries") || !strings.Contains(capturedQuery, "ON CONFLICT (session_id) DO UPDATE SET") {
		t.Errorf("unexpected query: %s", capturedQuery)
	}
	if len(capturedArgs) != 10 {
		t.Errorf("expected 10 args, got %d", len(capturedArgs))
	}

	// 2. Exec error
	mockExecErr := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return nil, errors.New("simulated upsert exec failure")
		},
	}
	err = UpsertSessionSummaryWithContext(ctx, mockExecErr, true, summary)
	if err == nil || !strings.Contains(err.Error(), "upsert session summary failed") {
		t.Errorf("expected exec error, got: %v", err)
	}
}

func TestTranscriptStore_BatchInsertTranscriptStepsWithContext_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. Postgres exec with chunking > 500 steps (550 steps -> 6 chunks of batchSize 100)
	steps := make([]TranscriptStep, 550)
	for i := 0; i < 550; i++ {
		steps[i] = TranscriptStep{
			SessionID: "sess-batch-chunk",
			StepIndex: i,
			StepType:  "tool",
			ToolName:  "run_command",
			Content:   fmt.Sprintf("step content %d", i),
			CreatedAt: time.Now().UTC(),
		}
	}

	chunkExecCount := 0
	var lastQuery string
	mockSuccess := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			chunkExecCount++
			lastQuery = query
			return testCustomResult{rows: int64(len(args) / 6)}, nil
		},
	}
	err := BatchInsertTranscriptStepsWithContext(ctx, mockSuccess, true, steps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chunkExecCount != 6 {
		t.Errorf("expected 6 chunks for 550 steps (batchSize=100), got %d", chunkExecCount)
	}
	if !strings.Contains(lastQuery, "INSERT INTO transcript_steps") || !strings.Contains(lastQuery, "ON CONFLICT (session_id, step_index) DO NOTHING") {
		t.Errorf("unexpected query: %s", lastQuery)
	}

	// 2. Exec error
	mockExecErr := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return nil, errors.New("simulated batch chunk exec failure")
		},
	}
	err = BatchInsertTranscriptStepsWithContext(ctx, mockExecErr, true, steps[:10])
	if err == nil || !strings.Contains(err.Error(), "batch insert transcript steps chunk failed") {
		t.Errorf("expected exec chunk error, got: %v", err)
	}
}

func TestTranscriptStore_SearchSessionSummariesWithContext_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbMem, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbMem.Close()

	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	// Helper to create mock rows for session summary scan
	mockScanQuery := `
	SELECT 's-pg-1' AS session_id, 'th-1' AS thread_id, 'PG summary result' AS summary,
	       5 AS last_indexed_step, '2026-10-01 12:00:00' AS last_mtime,
	       5 AS summary_step_watermark, 1 AS is_settled,
	       '2026-10-01 11:00:00' AS created_at, '2026-10-01 12:00:00' AS updated_at,
	       0.85 AS score
	`

	// a. Hybrid branch (both emb and text)
	var capturedQuery string
	mockHybrid := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			capturedQuery = query
			return dbMem.QueryContext(ctx, mockScanQuery)
		},
	}
	resHybrid, err := SearchSessionSummariesWithContext(ctx, mockHybrid, true, emb, "query text", 10, 0.5)
	if err != nil {
		t.Fatalf("hybrid search failed: %v", err)
	}
	if !strings.Contains(capturedQuery, "vector_hits") || !strings.Contains(capturedQuery, "text_hits") || !strings.Contains(capturedQuery, "FULL OUTER JOIN") {
		t.Errorf("expected hybrid RRF query with vector_hits and text_hits, got: %s", capturedQuery)
	}
	if len(resHybrid) != 1 || resHybrid[0].SessionID != "s-pg-1" || resHybrid[0].Score != 0.85 {
		t.Errorf("unexpected hybrid result: %+v", resHybrid)
	}

	// b. Dense-only branch (emb only, empty text)
	capturedQuery = ""
	mockDense := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			capturedQuery = query
			return dbMem.QueryContext(ctx, mockScanQuery)
		},
	}
	resDense, err := SearchSessionSummariesWithContext(ctx, mockDense, true, emb, "", 10, 0.5)
	if err != nil {
		t.Fatalf("dense search failed: %v", err)
	}
	if !strings.Contains(capturedQuery, "WITH candidates AS") || !strings.Contains(capturedQuery, "similarity AS score") {
		t.Errorf("expected dense-only query with candidates CTE, got: %s", capturedQuery)
	}
	if len(resDense) != 1 || resDense[0].Score != 0.85 {
		t.Errorf("unexpected dense result: %+v", resDense)
	}

	// c. Sparse-only branch (text only, nil emb)
	capturedQuery = ""
	mockSparse := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			capturedQuery = query
			return dbMem.QueryContext(ctx, mockScanQuery)
		},
	}
	resSparse, err := SearchSessionSummariesWithContext(ctx, mockSparse, true, nil, "only text", 10, 0.5)
	if err != nil {
		t.Fatalf("sparse search failed: %v", err)
	}
	if !strings.Contains(capturedQuery, "WITH text_hits AS") || !strings.Contains(capturedQuery, "ts_rank_cd(fts_tokens") {
		t.Errorf("expected sparse-only query with text_hits CTE, got: %s", capturedQuery)
	}
	if len(resSparse) != 1 || resSparse[0].Score != 0.85 {
		t.Errorf("unexpected sparse result: %+v", resSparse)
	}

	// d. Query error
	mockErr := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return nil, errors.New("simulated pg search summaries failure")
		},
	}
	_, err = SearchSessionSummariesWithContext(ctx, mockErr, true, emb, "test", 10, 0.5)
	if err == nil || !strings.Contains(err.Error(), "search session summaries query failed") {
		t.Errorf("expected search session summaries query failed error, got: %v", err)
	}

	// e. Scan error
	mockScanErr := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return dbMem.QueryContext(ctx, "SELECT 's-pg-1' AS session_id")
		},
	}
	_, err = SearchSessionSummariesWithContext(ctx, mockScanErr, true, emb, "test", 10, 0.5)
	if err == nil || !strings.Contains(err.Error(), "scan session summary result") {
		t.Errorf("expected scan error, got: %v", err)
	}
}

func TestTranscriptStore_SearchTranscriptStepsWithContext_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbMem, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbMem.Close()

	mockScanQuery := `
	SELECT 's-pg-1' AS session_id, 0 AS step_index, 'tool' AS step_type,
	       'run_command' AS tool_name, 'echo test' AS content,
	       '2026-10-01 12:00:00' AS created_at, 1.0 AS rank_score
	`

	// a. With text query, session filter, and tool filter
	var capturedQuery string
	var capturedArgs []any
	mockWithText := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			capturedQuery = query
			capturedArgs = args
			return dbMem.QueryContext(ctx, mockScanQuery)
		},
	}
	stepsWithText, err := SearchTranscriptStepsWithContext(ctx, mockWithText, true, "build command", "sess-1", "run_command", 15)
	if err != nil {
		t.Fatalf("search with text failed: %v", err)
	}
	if !strings.Contains(capturedQuery, "fts_tokens @@ websearch_to_tsquery") || !strings.Contains(capturedQuery, "rank_score") {
		t.Errorf("expected FTS text search query, got: %s", capturedQuery)
	}
	if len(capturedArgs) != 4 || capturedArgs[0] != "build command" || capturedArgs[1] != "sess-1" || capturedArgs[2] != "run_command" || capturedArgs[3] != 15 {
		t.Errorf("unexpected args: %+v", capturedArgs)
	}
	if len(stepsWithText) != 1 || stepsWithText[0].SessionID != "s-pg-1" || stepsWithText[0].ToolName != "run_command" {
		t.Errorf("unexpected steps: %+v", stepsWithText)
	}

	// b. Without text query (filters only)
	capturedQuery = ""
	capturedArgs = nil
	mockNoText := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			capturedQuery = query
			capturedArgs = args
			return dbMem.QueryContext(ctx, mockScanQuery)
		},
	}
	stepsNoText, err := SearchTranscriptStepsWithContext(ctx, mockNoText, true, "", "sess-1", "run_command", 10)
	if err != nil {
		t.Fatalf("search without text failed: %v", err)
	}
	if !strings.Contains(capturedQuery, "1.0 AS rank_score") || strings.Contains(capturedQuery, "fts_tokens @@") {
		t.Errorf("expected query without FTS, got: %s", capturedQuery)
	}
	if len(capturedArgs) != 3 || capturedArgs[0] != "sess-1" || capturedArgs[1] != "run_command" || capturedArgs[2] != 10 {
		t.Errorf("unexpected args: %+v", capturedArgs)
	}
	if len(stepsNoText) != 1 {
		t.Errorf("expected 1 step result, got %d", len(stepsNoText))
	}

	// c. Query error
	mockErr := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return nil, errors.New("simulated transcript steps query failure")
		},
	}
	_, err = SearchTranscriptStepsWithContext(ctx, mockErr, true, "test", "", "", 10)
	if err == nil || !strings.Contains(err.Error(), "search transcript steps query failed") {
		t.Errorf("expected search transcript steps query failed error, got: %v", err)
	}

	// d. Scan error
	mockScanErr := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return dbMem.QueryContext(ctx, "SELECT 's-pg-1' AS session_id")
		},
	}
	_, err = SearchTranscriptStepsWithContext(ctx, mockScanErr, true, "test", "", "", 10)
	if err == nil || !strings.Contains(err.Error(), "scan transcript step result") {
		t.Errorf("expected scan error, got: %v", err)
	}
}

func TestFakeStore_CloneTranscriptSteps_WithTx(t *testing.T) {
	ctx := context.Background()

	// 1. Commit branch: Verify existing steps are cloned into txCopy and preserved/updated on commit
	t.Run("commit preserves and updates cloned transcript steps", func(t *testing.T) {
		s := NewFakeStore()
		sessID := "tx-clone-sess-1"

		// Pre-populate with existing steps
		initSteps := []TranscriptStep{
			{SessionID: sessID, StepIndex: 0, Content: "Existing step 0", CreatedAt: time.Now().UTC()},
			{SessionID: sessID, StepIndex: 1, Content: "Existing step 1", CreatedAt: time.Now().UTC()},
		}
		if err := s.BatchInsertTranscriptSteps(ctx, initSteps); err != nil {
			t.Fatalf("initial BatchInsertTranscriptSteps failed: %v", err)
		}

		err := s.WithTx(ctx, func(tx Store) error {
			// Inside tx, existing steps were cloned via cloneTranscriptSteps
			// Verify we can search them
			found, err := tx.SearchTranscriptSteps(ctx, "", sessID, "", 10)
			if err != nil || len(found) != 2 {
				t.Fatalf("expected 2 cloned steps inside tx, got %d, err: %v", len(found), err)
			}

			// Add a new step inside tx
			return tx.BatchInsertTranscriptSteps(ctx, []TranscriptStep{
				{SessionID: sessID, StepIndex: 2, Content: "Tx step 2", CreatedAt: time.Now().UTC()},
			})
		})
		if err != nil {
			t.Fatalf("WithTx commit failed: %v", err)
		}

		// After commit, all 3 steps must exist in store
		afterCommit, err := s.SearchTranscriptSteps(ctx, "", sessID, "", 10)
		if err != nil {
			t.Fatalf("SearchTranscriptSteps after commit failed: %v", err)
		}
		if len(afterCommit) != 3 {
			t.Errorf("expected 3 steps after commit, got %d", len(afterCommit))
		}
	})

	// 2. Rollback branch: Verify modifications to cloned steps inside tx are discarded on rollback
	t.Run("rollback discards changes to cloned transcript steps", func(t *testing.T) {
		s := NewFakeStore()
		sessID := "tx-clone-sess-2"

		initSteps := []TranscriptStep{
			{SessionID: sessID, StepIndex: 0, Content: "Existing step before rollback", CreatedAt: time.Now().UTC()},
		}
		if err := s.BatchInsertTranscriptSteps(ctx, initSteps); err != nil {
			t.Fatalf("initial BatchInsertTranscriptSteps failed: %v", err)
		}

		expectedErr := errors.New("simulated tx rollback")
		err := s.WithTx(ctx, func(tx Store) error {
			_ = tx.BatchInsertTranscriptSteps(ctx, []TranscriptStep{
				{SessionID: sessID, StepIndex: 1, Content: "Should be rolled back", CreatedAt: time.Now().UTC()},
			})
			return expectedErr
		})
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected %v, got %v", expectedErr, err)
		}

		// After rollback, only initial step exists
		afterRollback, err := s.SearchTranscriptSteps(ctx, "", sessID, "", 10)
		if err != nil {
			t.Fatalf("SearchTranscriptSteps after rollback failed: %v", err)
		}
		if len(afterRollback) != 1 {
			t.Errorf("expected 1 step after rollback, got %d", len(afterRollback))
		}
		if afterRollback[0].Content != "Existing step before rollback" {
			t.Errorf("unexpected content: %s", afterRollback[0].Content)
		}
	})

	// 3. Directly test cloneTranscriptSteps with nil and non-nil
	t.Run("cloneTranscriptSteps unit", func(t *testing.T) {
		if cloneTranscriptSteps(nil) != nil {
			t.Errorf("expected nil for cloneTranscriptSteps(nil)")
		}
		sample := []TranscriptStep{
			{SessionID: "s", StepIndex: 0, Content: "c"},
		}
		cp := cloneTranscriptSteps(sample)
		if len(cp) != 1 || cp[0].Content != "c" {
			t.Errorf("unexpected clone result: %+v", cp)
		}
	})
}

func TestTranscriptStore_EdgeCases_And_Fallbacks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbMem, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbMem.Close()

	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	// 1. Nil context handling across transcripts.go functions
	states, err := GetSessionSyncStatesWithContext(nil, dbMem, false)
	if err != nil {
		t.Errorf("expected nil error on GetSessionSyncStatesWithContext(nil ctx), got %v", err)
	}
	if states == nil {
		t.Errorf("expected non-nil states map")
	}

	summary := SessionSummary{
		SessionID: "sess-edge-1",
		Summary:   "Edge case session",
		Embedding: emb,
	}
	if err := UpsertSessionSummaryWithContext(nil, dbMem, false, summary); err != nil {
		t.Errorf("expected nil error on UpsertSessionSummaryWithContext(nil ctx), got %v", err)
	}

	steps := []TranscriptStep{
		{SessionID: "sess-edge-1", StepIndex: 0, Content: "edge step 0", ToolName: "toolA"},
		{SessionID: "sess-edge-1", StepIndex: 1, Content: "edge step 1", ToolName: "toolB"},
		{SessionID: "sess-edge-1", StepIndex: 2, Content: "edge step 2", ToolName: "toolA"},
	}
	if err := BatchInsertTranscriptStepsWithContext(nil, dbMem, false, steps); err != nil {
		t.Errorf("expected nil error on BatchInsertTranscriptStepsWithContext(nil ctx), got %v", err)
	}

	// 2. SearchSessionSummariesWithContext: nil ctx, limit <= 0, minScore < 0
	sumResults, err := SearchSessionSummariesWithContext(nil, dbMem, false, emb, "Edge case", -5, -1.0)
	if err != nil || len(sumResults) != 1 {
		t.Fatalf("expected 1 summary result, got %d, err: %v", len(sumResults), err)
	}

	// 3. SearchTranscriptStepsWithContext: nil ctx, limit <= 0, and SQLite early break when len(matched) >= limit
	stepResults, err := SearchTranscriptStepsWithContext(nil, dbMem, false, "edge", "", "", 1)
	if err != nil || len(stepResults) != 1 {
		t.Fatalf("expected 1 step hit on limit 1, got %d, err: %v", len(stepResults), err)
	}

	// SearchTranscriptStepsWithContext with negative limit
	stepResultsNeg, err := SearchTranscriptStepsWithContext(ctx, dbMem, false, "edge", "", "", -1)
	if err != nil || len(stepResultsNeg) != 3 {
		t.Fatalf("expected 3 step hits with default limit, got %d, err: %v", len(stepResultsNeg), err)
	}

	// 4. SQLite query and scan errors using mockDBTX with isPg=false
	mockSqliteErr := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return nil, errors.New("simulated sqlite query error")
		},
	}
	_, err = SearchSessionSummariesWithContext(ctx, mockSqliteErr, false, emb, "query", 10, 0.0)
	if err == nil || !strings.Contains(err.Error(), "sqlite search session summaries query") {
		t.Errorf("expected sqlite search summaries query error, got: %v", err)
	}

	_, err = SearchTranscriptStepsWithContext(ctx, mockSqliteErr, false, "query", "", "", 10)
	if err == nil || !strings.Contains(err.Error(), "sqlite search transcript steps query") {
		t.Errorf("expected sqlite search transcript steps query error, got: %v", err)
	}

	mockSqliteScanErr := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return dbMem.QueryContext(ctx, "SELECT 's-1' AS col1")
		},
	}
	_, err = SearchSessionSummariesWithContext(ctx, mockSqliteScanErr, false, emb, "query", 10, 0.0)
	if err == nil || !strings.Contains(err.Error(), "sqlite scan session summary") {
		t.Errorf("expected sqlite scan summary error, got: %v", err)
	}

	_, err = SearchTranscriptStepsWithContext(ctx, mockSqliteScanErr, false, "query", "", "", 10)
	if err == nil || !strings.Contains(err.Error(), "sqlite scan transcript step") {
		t.Errorf("expected sqlite scan step error, got: %v", err)
	}
}

func TestFakeStore_TranscriptEdgeCases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewFakeStore()

	// 1. SearchSessionSummaries: nil query and empty embedding
	resNil, err := store.SearchSessionSummaries(ctx, nil, "", 10, 0.0)
	if err != nil || resNil != nil {
		t.Errorf("expected nil, nil for empty query and embedding, got %v, %v", resNil, err)
	}

	// Insert session summaries
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0
	for i := 0; i < 5; i++ {
		_ = store.UpsertSessionSummary(ctx, SessionSummary{
			SessionID: fmt.Sprintf("fs-sess-%d", i),
			Summary:   fmt.Sprintf("Deploying Kubernetes cluster service %d", i),
			Embedding: emb,
		})
	}

	// Search with limit <= 0 and minScore < 0 and partial word matches
	resScored, err := store.SearchSessionSummaries(ctx, emb, "Kubernetes cluster service", -1, -0.5)
	if err != nil || len(resScored) != 5 {
		t.Fatalf("expected 5 results with default limit 10, got %d, err: %v", len(resScored), err)
	}

	// Search with limit 2
	resLimited, err := store.SearchSessionSummaries(ctx, emb, "Kubernetes", 2, 0.0)
	if err != nil || len(resLimited) != 2 {
		t.Fatalf("expected 2 results with limit 2, got %d, err: %v", len(resLimited), err)
	}

	// 2. SearchTranscriptSteps in FakeStore
	// Empty query and empty filters returns nil, nil
	stepsNil, err := store.SearchTranscriptSteps(ctx, "", "", "", 10)
	if err != nil || stepsNil != nil {
		t.Errorf("expected nil, nil for empty transcript search, got %v, %v", stepsNil, err)
	}

	// Insert steps across two sessions and tools
	for i := 0; i < 10; i++ {
		sess := "fs-sess-0"
		if i >= 5 {
			sess = "fs-sess-1"
		}
		tool := "run_command"
		if i%2 == 0 {
			tool = "docker_ps"
		}
		_ = store.BatchInsertTranscriptSteps(ctx, []TranscriptStep{
			{
				SessionID: sess,
				StepIndex: i,
				ToolName:  tool,
				Content:   fmt.Sprintf("execution step content %d", i),
				CreatedAt: time.Now().Add(time.Duration(i) * time.Minute),
			},
		})
	}

	// Search with sessionFilter and toolFilter
	toolFiltered, err := store.SearchTranscriptSteps(ctx, "execution", "fs-sess-0", "docker_ps", 10)
	if err != nil {
		t.Fatalf("search with filters failed: %v", err)
	}
	for _, s := range toolFiltered {
		if s.SessionID != "fs-sess-0" || s.ToolName != "docker_ps" {
			t.Errorf("unexpected filter result: %+v", s)
		}
	}

	// Search with limit <= 0
	allMatched, err := store.SearchTranscriptSteps(ctx, "execution step", "", "", -1)
	if err != nil || len(allMatched) != 10 {
		t.Errorf("expected 10 steps with default limit, got %d, err: %v", len(allMatched), err)
	}

	// Search with limit 3
	cappedMatched, err := store.SearchTranscriptSteps(ctx, "execution", "", "", 3)
	if err != nil || len(cappedMatched) != 3 {
		t.Errorf("expected 3 steps capped by limit, got %d, err: %v", len(cappedMatched), err)
	}
}


