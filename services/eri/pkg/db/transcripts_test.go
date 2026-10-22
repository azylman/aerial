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
	for _, store := range []Store{NewTestStore(t)} {
		ctx := context.Background()
		syncStates, err := store.GetSessionSyncStates(ctx)
		if err != nil || len(syncStates) != 0 {
			t.Fatalf("expected 0 sync states, got %d, err: %v", len(syncStates), err)
		}
		if err := store.UpsertSessionSummary(ctx, SessionSummary{SessionID: ""}); err == nil {
			t.Errorf("expected error for empty session ID")
		}

		now := time.Now().UTC().Truncate(time.Second)
		summary := SessionSummary{
			SessionID: "sess-1", ThreadID: "th-1", Summary: "Debugged interface",
			LastIndexedStep: 10, LastMtime: now, SummaryStepWatermark: 5, IsSettled: false,
		}
		if err := store.UpsertSessionSummary(ctx, summary); err != nil {
			t.Fatalf("UpsertSessionSummary failed: %v", err)
		}
		syncStates, err = store.GetSessionSyncStates(ctx)
		if err != nil || syncStates["sess-1"].LastIndexedStep != 10 || syncStates["sess-1"].IsSettled || syncStates["sess-1"].LastMtime.IsZero() {
			t.Fatalf("unexpected state1: %+v", syncStates["sess-1"])
		}

		updateSummary := SessionSummary{SessionID: "sess-1", Summary: "Updated", LastIndexedStep: 25, SummaryStepWatermark: 25, IsSettled: true}
		if err := store.UpsertSessionSummary(ctx, updateSummary); err != nil {
			t.Fatalf("UpsertSessionSummary update failed: %v", err)
		}
		syncStates, err = store.GetSessionSyncStates(ctx)
		if err != nil || syncStates["sess-1"].LastIndexedStep != 25 || !syncStates["sess-1"].IsSettled {
			t.Fatalf("unexpected state after update: %+v, err: %v", syncStates["sess-1"], err)
		}

		if err := store.UpsertSessionSummary(ctx, SessionSummary{SessionID: "sess-1", LastIndexedStep: -2}); err != nil {
			t.Fatalf("UpsertSessionSummary reset failed: %v", err)
		}
		syncStates, err = store.GetSessionSyncStates(ctx)
		if err != nil || syncStates["sess-1"].LastIndexedStep != -1 {
			t.Fatalf("expected LastIndexedStep=-1, got %+v", syncStates["sess-1"])
		}
		if states, err := store.GetSessionSyncStates(nil); err != nil || states == nil {
			t.Errorf("expected non-nil states on nil ctx: %v", err)
		}
		if err := store.UpsertSessionSummary(nil, summary); err != nil {
			t.Errorf("expected nil error on UpsertSessionSummary with nil ctx: %v", err)
		}
	}

	var nilStore *SQLStore
	if err := nilStore.UpsertSessionSummary(context.Background(), SessionSummary{SessionID: "s1"}); err == nil {
		t.Errorf("expected error on nil store UpsertSessionSummary")
	}
	if _, err := nilStore.GetSessionSyncStates(context.Background()); err == nil {
		t.Errorf("expected error on nil store GetSessionSyncStates")
	}
	if _, err := nilStore.SearchSessionSummaries(context.Background(), nil, "q", 10, 0.1); err == nil {
		t.Errorf("expected error on nil store SearchSessionSummaries")
	}
	if _, err := nilStore.SearchTranscriptSteps(context.Background(), "q", "", "", 10); err == nil {
		t.Errorf("expected error on nil store SearchTranscriptSteps")
	}
	if parsed, ok := ParseDBTime("2026-10-01T12:00:00Z"); !ok || parsed.IsZero() {
		t.Errorf("expected parsed time from RFC3339 string")
	}
	if _, ok := ParseDBTime(nil); ok {
		t.Errorf("expected false for ParseDBTime(nil)")
	}
}

func TestTranscriptStore_BatchInsertTranscriptSteps(t *testing.T) {
	for _, store := range []Store{NewTestStore(t)} {
		ctx := context.Background()
		if err := store.UpsertSessionSummary(ctx, SessionSummary{SessionID: "sess-steps-1", ThreadID: "th-steps-1", Summary: "Session for steps"}); err != nil {
			t.Fatalf("UpsertSessionSummary failed: %v", err)
		}

		now := time.Now().UTC()
		steps := []TranscriptStep{
			{SessionID: "sess-steps-1", StepIndex: 0, StepType: "user_message", Content: "Deploy app", CreatedAt: now.Add(-2 * time.Minute)},
			{SessionID: "sess-steps-1", StepIndex: 1, StepType: "tool_call", ToolName: "run_command", Content: "kubectl apply -f deployment.yaml", CreatedAt: now.Add(-1 * time.Minute)},
			{SessionID: "sess-steps-1", StepIndex: 2, StepType: "tool_result", ToolName: "run_command", Content: "deployment configured", CreatedAt: now},
		}
		if err := store.BatchInsertTranscriptSteps(ctx, steps); err != nil {
			t.Fatalf("BatchInsertTranscriptSteps failed: %v", err)
		}

		results, err := store.SearchTranscriptSteps(ctx, "", "sess-steps-1", "", 10)
		if err != nil || len(results) != 3 {
			t.Fatalf("expected 3 steps, got %d (err: %v)", len(results), err)
		}

		dupSteps := []TranscriptStep{
			{SessionID: "sess-steps-1", StepIndex: 1, StepType: "tool_call", ToolName: "run_command", Content: "DIFFERENT CONTENT IGNORED"},
			{SessionID: "sess-steps-1", StepIndex: 3, StepType: "model_response", Content: "Deployment completed"},
		}
		if err := store.BatchInsertTranscriptSteps(ctx, dupSteps); err != nil {
			t.Fatalf("BatchInsertTranscriptSteps with duplicates failed: %v", err)
		}

		resultsAfter, err := store.SearchTranscriptSteps(ctx, "", "sess-steps-1", "", 10)
		if err != nil || len(resultsAfter) != 4 {
			t.Fatalf("expected 4 steps total, got %d (err: %v)", len(resultsAfter), err)
		}
		for _, r := range resultsAfter {
			if r.StepIndex == 1 && r.Content != "kubectl apply -f deployment.yaml" {
				t.Errorf("expected original content for step 1 preserved, got %q", r.Content)
			}
		}

		if err := store.BatchInsertTranscriptSteps(ctx, nil); err != nil {
			t.Errorf("expected nil error for nil slice")
		}
		if err := store.BatchInsertTranscriptSteps(ctx, []TranscriptStep{{SessionID: ""}}); err != nil {
			t.Errorf("expected nil error for empty sessionID step")
		}
		if err := store.BatchInsertTranscriptSteps(nil, steps[:1]); err != nil {
			t.Errorf("expected nil error on nil ctx")
		}
	}

	var nilStore *SQLStore
	if err := nilStore.BatchInsertTranscriptSteps(context.Background(), nil); err == nil {
		t.Errorf("expected error on nil store BatchInsertTranscriptSteps")
	}
}

func TestTranscriptStore_CascadeDelete(t *testing.T) {
	database := initTestSQLiteDB(t)
	store, ctx := NewSQLStore(database), context.Background()

	_ = store.UpsertSessionSummary(ctx, SessionSummary{SessionID: "sess-cascade-1", ThreadID: "th-cascade-1", Summary: "Cascade test"})
	_ = store.BatchInsertTranscriptSteps(ctx, []TranscriptStep{{SessionID: "sess-cascade-1", StepIndex: 0, Content: "Step 0"}, {SessionID: "sess-cascade-1", StepIndex: 1, Content: "Step 1"}})
	if before, err := store.SearchTranscriptSteps(ctx, "", "sess-cascade-1", "", 10); err != nil || len(before) != 2 {
		t.Fatalf("expected 2 steps before delete: len=%d, err=%v", len(before), err)
	}
	if _, err := database.Exec("DELETE FROM session_summaries WHERE session_id = ?", "sess-cascade-1"); err != nil {
		t.Fatalf("failed to delete session summary: %v", err)
	}
	if after, err := store.SearchTranscriptSteps(ctx, "", "sess-cascade-1", "", 10); err != nil || len(after) != 0 {
		t.Fatalf("expected 0 steps after cascade delete: len=%d, err=%v", len(after), err)
	}
}

type testCustomResult struct {
	rows int64
	err  error
}

func (r testCustomResult) LastInsertId() (int64, error) { return 0, nil }
func (r testCustomResult) RowsAffected() (int64, error) { return r.rows, r.err }

type testMockDBTX struct {
	execFn     func(ctx context.Context, query string, args ...any) (sql.Result, error)
	queryFn    func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	queryRowFn func(ctx context.Context, query string, args ...any) *sql.Row
}

func (m testMockDBTX) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if m.execFn != nil {
		return m.execFn(ctx, query, args...)
	}
	return testCustomResult{}, nil
}

func (m testMockDBTX) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, query, args...)
	}
	return nil, nil
}

func (m testMockDBTX) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if m.queryRowFn != nil {
		return m.queryRowFn(ctx, query, args...)
	}
	return nil
}

func TestTranscriptStore_GetSessionSyncStatesWithContext_MockDB(t *testing.T) {
	dbMem := initTestSQLiteDB(t)
	for _, tc := range []struct {
		name, querySQL, wantErr string
		queryErr                error
		wantLen                 int
	}{
		{name: "success query", querySQL: "SELECT 'sess-1' AS session_id, '2026-10-01 12:00:00' AS last_mtime, 10 AS last_indexed_step, 1 AS is_settled", wantLen: 1},
		{name: "scan error wrong column count", querySQL: "SELECT 'sess-1' AS session_id", wantErr: "scan session sync state"},
		{name: "query failure error", queryErr: errors.New("simulated sync states query failure"), wantErr: "query session sync states"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capturedQuery string
			mock := testMockDBTX{queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
				capturedQuery = query
				if tc.queryErr != nil {
					return nil, tc.queryErr
				}
				return dbMem.QueryContext(ctx, tc.querySQL)
			}}
			states, err := (&SQLStore{db: mock, isPostgres: true}).GetSessionSyncStates(context.Background())
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			} else if tc.wantErr == "" && (err != nil || len(states) != tc.wantLen || states["sess-1"].LastIndexedStep != 10 || !states["sess-1"].IsSettled) {
				t.Fatalf("unexpected states: %+v, err: %v", states, err)
			}
			if tc.wantErr == "" && !strings.Contains(capturedQuery, "SELECT session_id, last_mtime, last_indexed_step, is_settled FROM session_summaries") {
				t.Errorf("unexpected query: %s", capturedQuery)
			}
		})
	}
}

func TestTranscriptStore_UpsertSessionSummaryWithContext_MockDB(t *testing.T) {
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 0.5
	summary := SessionSummary{
		SessionID: "sess-upsert-1", ThreadID: "th-1", Summary: "Summary text", Embedding: emb,
		LastIndexedStep: 5, LastMtime: time.Now().UTC(), SummaryStepWatermark: 5, IsSettled: true,
	}
	for _, tc := range []struct {
		name, wantErr string
		execErr       error
	}{
		{name: "success on postgres exec"},
		{name: "exec error", execErr: errors.New("simulated upsert exec failure"), wantErr: "upsert session summary failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capturedQuery string
			var capturedArgs []any
			mock := testMockDBTX{execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
				capturedQuery, capturedArgs = query, args
				if tc.execErr != nil {
					return nil, tc.execErr
				}
				return testCustomResult{rows: 1}, nil
			}}
			err := (&SQLStore{db: mock, isPostgres: true}).UpsertSessionSummary(context.Background(), summary)
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			} else if tc.wantErr == "" && (err != nil || !strings.Contains(capturedQuery, "INSERT INTO session_summaries") || len(capturedArgs) != 10) {
				t.Fatalf("unexpected result: err=%v query=%s argsLen=%d", err, capturedQuery, len(capturedArgs))
			}
		})
	}
}

func TestTranscriptStore_BatchInsertTranscriptStepsWithContext_MockDB(t *testing.T) {
	steps := make([]TranscriptStep, 550)
	for i := 0; i < 550; i++ {
		steps[i] = TranscriptStep{
			SessionID: "sess-batch-chunk", StepIndex: i, StepType: "tool", ToolName: "run_command",
			Content: fmt.Sprintf("step content %d", i), CreatedAt: time.Now().UTC(),
		}
	}
	for _, tc := range []struct {
		name, wantErr          string
		stepsCount, wantChunks int
		execErr                error
	}{
		{name: "chunking 550 steps into 6 chunks", stepsCount: 550, wantChunks: 6},
		{name: "exec failure error", stepsCount: 10, execErr: errors.New("simulated batch chunk exec failure"), wantErr: "batch insert transcript steps chunk failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunkCount := 0
			var lastQuery string
			mock := testMockDBTX{execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
				chunkCount++
				lastQuery = query
				if tc.execErr != nil {
					return nil, tc.execErr
				}
				return testCustomResult{rows: int64(len(args) / 6)}, nil
			}}
			err := (&SQLStore{db: mock, isPostgres: true}).BatchInsertTranscriptSteps(context.Background(), steps[:tc.stepsCount])
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			} else if tc.wantErr == "" && (err != nil || chunkCount != tc.wantChunks || !strings.Contains(lastQuery, "INSERT INTO transcript_steps")) {
				t.Fatalf("unexpected chunks=%d query=%s err=%v", chunkCount, lastQuery, err)
			}
		})
	}
}

func TestTranscriptStore_SearchSessionSummariesWithContext_MockDB(t *testing.T) {
	dbMem := initTestSQLiteDB(t)
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	mockScanQuery := "SELECT 's-pg-1' AS session_id, 'th-1' AS thread_id, 'PG summary result' AS summary, 5 AS last_indexed_step, '2026-10-01 12:00:00' AS last_mtime, 5 AS summary_step_watermark, 1 AS is_settled, '2026-10-01 11:00:00' AS created_at, '2026-10-01 12:00:00' AS updated_at, 0.85 AS score"

	for _, tc := range []struct {
		name, query, scanSQL, wantErrMsg string
		emb                              []float32
		queryErr                         error
		wantPatterns                     []string
		wantLen                          int
	}{
		{name: "hybrid", emb: emb, query: "query text", scanSQL: mockScanQuery, wantPatterns: []string{"vector_hits", "text_hits", "FULL OUTER JOIN"}, wantLen: 1},
		{name: "dense", emb: emb, scanSQL: mockScanQuery, wantPatterns: []string{"WITH candidates AS", "similarity AS score"}, wantLen: 1},
		{name: "sparse", query: "only text", scanSQL: mockScanQuery, wantPatterns: []string{"WITH text_hits AS", "ts_rank_cd(fts_tokens"}, wantLen: 1},
		{name: "query error", emb: emb, query: "test", queryErr: errors.New("simulated pg search summaries failure"), wantErrMsg: "search session summaries query failed"},
		{name: "scan error", emb: emb, query: "test", scanSQL: "SELECT 's-pg-1' AS session_id", wantErrMsg: "scan session summary result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capturedQuery string
			mockDB := testMockDBTX{queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
				capturedQuery = query
				if tc.queryErr != nil {
					return nil, tc.queryErr
				}
				return dbMem.QueryContext(ctx, tc.scanSQL)
			}}
			res, err := (&SQLStore{db: mockDB, isPostgres: true}).SearchSessionSummaries(context.Background(), tc.emb, tc.query, 10, 0.5)
			if tc.wantErrMsg != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrMsg)) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
			} else if tc.wantErrMsg == "" && (err != nil || len(res) != tc.wantLen) {
				t.Fatalf("unexpected res len=%d err=%v", len(res), err)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(capturedQuery, pat) {
					t.Errorf("expected query containing %q, got: %s", pat, capturedQuery)
				}
			}
		})
	}
}

func TestTranscriptStore_SearchTranscriptStepsWithContext_MockDB(t *testing.T) {
	dbMem := initTestSQLiteDB(t)

	mockScanQuery := "SELECT 's-pg-1' AS session_id, 0 AS step_index, 'tool' AS step_type, 'run_command' AS tool_name, 'echo test' AS content, '2026-10-01 12:00:00' AS created_at, 1.0 AS rank_score"

	for _, tc := range []struct {
		name, query, sess, tool, scanSQL, wantErrMsg string
		limit, wantLen                               int
		queryErr                                     error
		wantPatterns                                 []string
	}{
		{name: "with text", query: "build command", sess: "sess-1", tool: "run_command", limit: 15, scanSQL: mockScanQuery, wantPatterns: []string{"fts_tokens @@ websearch_to_tsquery", "rank_score"}, wantLen: 1},
		{name: "filters only", sess: "sess-1", tool: "run_command", limit: 10, scanSQL: mockScanQuery, wantPatterns: []string{"1.0 AS rank_score"}, wantLen: 1},
		{name: "query error", query: "test", limit: 10, queryErr: errors.New("simulated transcript steps query failure"), wantErrMsg: "search transcript steps query failed"},
		{name: "scan error", query: "test", limit: 10, scanSQL: "SELECT 's-pg-1' AS session_id", wantErrMsg: "scan transcript step result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capturedQuery string
			mockDB := testMockDBTX{queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
				capturedQuery = query
				if tc.queryErr != nil {
					return nil, tc.queryErr
				}
				return dbMem.QueryContext(ctx, tc.scanSQL)
			}}
			steps, err := (&SQLStore{db: mockDB, isPostgres: true}).SearchTranscriptSteps(context.Background(), tc.query, tc.sess, tc.tool, tc.limit)
			if tc.wantErrMsg != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrMsg)) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
			} else if tc.wantErrMsg == "" && (err != nil || len(steps) != tc.wantLen) {
				t.Fatalf("unexpected steps len=%d err=%v", len(steps), err)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(capturedQuery, pat) {
					t.Errorf("expected query containing %q, got: %s", pat, capturedQuery)
				}
			}
		})
	}
}

func TestTranscriptStore_GetTranscriptStepRange(t *testing.T) {
	ctx := context.Background()
	for _, store := range []Store{NewTestStore(t)} {
		_ = store.UpsertSessionSummary(ctx, SessionSummary{SessionID: "sess-range-a"})
		_ = store.UpsertSessionSummary(ctx, SessionSummary{SessionID: "sess-range-b"})

		now := time.Now().UTC()
		steps := []TranscriptStep{
			{SessionID: "sess-range-a", StepIndex: 0, StepType: "user_message", Content: "initial prompt", CreatedAt: now.Add(-4 * time.Minute)},
			{SessionID: "sess-range-a", StepIndex: 1, StepType: "tool_call", ToolName: "run_command", Content: "git status", CreatedAt: now.Add(-3 * time.Minute)},
			{SessionID: "sess-range-a", StepIndex: 2, StepType: "tool_result", ToolName: "run_command", Content: "on branch main", CreatedAt: now.Add(-2 * time.Minute)},
			{SessionID: "sess-range-a", StepIndex: 3, StepType: "tool_call", ToolName: "view_file", Content: "reading config", CreatedAt: now.Add(-1 * time.Minute)},
			{SessionID: "sess-range-a", StepIndex: 4, StepType: "assistant_turn", Content: "done", CreatedAt: now},
			{SessionID: "sess-range-b", StepIndex: 0, StepType: "user_message", Content: "hello", CreatedAt: now},
		}
		if err := store.BatchInsertTranscriptSteps(ctx, steps); err != nil {
			t.Fatalf("BatchInsertTranscriptSteps failed: %v", err)
		}

		tests := []struct {
			name      string
			session   string
			startStep int
			endStep   int
			wantLen   int
			wantFirst int
			wantLast  int
		}{
			{name: "normal range", session: "sess-range-a", startStep: 1, endStep: 3, wantLen: 3, wantFirst: 1, wantLast: 3},
			{name: "single step", session: "sess-range-a", startStep: 2, endStep: 2, wantLen: 1, wantFirst: 2, wantLast: 2},
			{name: "negative start clamped", session: "sess-range-a", startStep: -5, endStep: 1, wantLen: 2, wantFirst: 0, wantLast: 1},
			{name: "end before start clamped", session: "sess-range-a", startStep: 3, endStep: 1, wantLen: 1, wantFirst: 3, wantLast: 3},
			{name: "empty session returns nil", session: "", startStep: 0, endStep: 5, wantLen: 0},
			{name: "whitespace session returns nil", session: "   ", startStep: 0, endStep: 5, wantLen: 0},
			{name: "unknown session", session: "sess-unknown", startStep: 0, endStep: 5, wantLen: 0},
			{name: "out of bounds high range", session: "sess-range-a", startStep: 10, endStep: 20, wantLen: 0},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := store.GetTranscriptStepRange(ctx, tc.session, tc.startStep, tc.endStep)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(got) != tc.wantLen {
					t.Fatalf("got len=%d, want %d", len(got), tc.wantLen)
				}
				if tc.wantLen > 0 {
					if got[0].StepIndex != tc.wantFirst {
						t.Errorf("got first step index=%d, want %d", got[0].StepIndex, tc.wantFirst)
					}
					if got[len(got)-1].StepIndex != tc.wantLast {
						t.Errorf("got last step index=%d, want %d", got[len(got)-1].StepIndex, tc.wantLast)
					}
					if got[0].RankScore != 1.0 {
						t.Errorf("got rank score %f, want 1.0", got[0].RankScore)
					}
				}
			})
		}
	}

	var nilSQLStore *SQLStore
	if _, err := nilSQLStore.GetTranscriptStepRange(ctx, "sess", 0, 2); err == nil {
		t.Error("expected error for nil SQLStore")
	}
}

func TestTranscriptStore_GetTranscriptStepRange_MockDB(t *testing.T) {
	dbMem := initTestSQLiteDB(t)
	tests := []struct {
		name       string
		session    string
		startStep  int
		endStep    int
		queryErr   error
		scanSQL    string
		wantErrMsg string
		wantLen    int
	}{
		{
			name:       "query execution failure",
			session:    "sess-err",
			startStep:  0,
			endStep:    5,
			queryErr:   errors.New("db connection lost"),
			wantErrMsg: "get transcript step range query failed",
		},
		{
			name:       "scan failure invalid columns",
			session:    "sess-bad-scan",
			startStep:  0,
			endStep:    5,
			scanSQL:    "SELECT 'col1' AS wrong_col",
			wantErrMsg: "scan transcript step result",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockDB := testMockDBTX{queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
				if tc.queryErr != nil {
					return nil, tc.queryErr
				}
				return dbMem.QueryContext(ctx, tc.scanSQL)
			}}
			steps, err := (&SQLStore{db: mockDB, isPostgres: true}).GetTranscriptStepRange(context.Background(), tc.session, tc.startStep, tc.endStep)
			if tc.wantErrMsg != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrMsg)) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
			} else if tc.wantErrMsg == "" && (err != nil || len(steps) != tc.wantLen) {
				t.Fatalf("unexpected steps len=%d err=%v", len(steps), err)
			}
		})
	}
}
