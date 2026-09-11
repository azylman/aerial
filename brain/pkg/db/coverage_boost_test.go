package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestIsDBTXNilVariants(t *testing.T) {
	// 1. Untyped nil
	if !isDBTXNil(nil) {
		t.Errorf("expected isDBTXNil(nil) to be true")
	}

	// 2. Typed nil pointer
	var nilDB *sql.DB = nil
	if !isDBTXNil(nilDB) {
		t.Errorf("expected isDBTXNil(nilDB) to be true")
	}

	var nilTx *sql.Tx = nil
	if !isDBTXNil(nilTx) {
		t.Errorf("expected isDBTXNil(nilTx) to be true")
	}

	// 3. Non-nil pointer
	database := setupTestDB(t)
	defer database.Close()
	if isDBTXNil(database) {
		t.Errorf("expected isDBTXNil(database) to be false")
	}

	// 4. Non-pointer struct
	if isDBTXNil(dummyDBTXNoDriver{}) {
		t.Errorf("expected isDBTXNil(dummyDBTXNoDriver{}) to be false")
	}

	// 5. NewTxStore with untyped nil
	if store := NewTxStore(nil, false); store != nil {
		t.Errorf("expected NewTxStore(nil, false) to be nil, got %v", store)
	}
}

func TestSQLStoreCloseAndWithTxBranches(t *testing.T) {
	ctx := context.Background()

	// 1. Close on nil SQLStore
	var nilStore *SQLStore
	if err := nilStore.Close(); err != nil {
		t.Errorf("expected nil error on nilStore.Close(), got %v", err)
	}

	// 2. Close when s.db is nil
	storeWithNilDB := &SQLStore{db: nil}
	if err := storeWithNilDB.Close(); err != nil {
		t.Errorf("expected nil error on storeWithNilDB.Close(), got %v", err)
	}

	// 3. Close when s.db does NOT implement Closer
	storeNonCloser := &SQLStore{db: dummyDBTXNoDriver{}}
	if err := storeNonCloser.Close(); err != nil {
		t.Errorf("expected nil error on storeNonCloser.Close(), got %v", err)
	}

	// 4. WithTx on nil store or nil db
	if err := nilStore.WithTx(ctx, func(s Store) error { return nil }); err == nil {
		t.Errorf("expected error on nilStore.WithTx, got nil")
	}
	if err := storeWithNilDB.WithTx(ctx, func(s Store) error { return nil }); err == nil {
		t.Errorf("expected error on storeWithNilDB.WithTx, got nil")
	}

	// 5. WithTx when s.db does NOT implement beginner (e.g. already in tx or dummyDBTX)
	executed := false
	err := storeNonCloser.WithTx(ctx, func(s Store) error {
		executed = true
		return nil
	})
	if err != nil || !executed {
		t.Errorf("expected WithTx without beginner to execute fn directly, err=%v executed=%v", err, executed)
	}

	// 6. WithTx failure when BeginTx fails (e.g. closed DB)
	tmpPath := filepath.Join(t.TempDir(), "closed.db")
	db, err := initDB(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	closedStore := NewSQLStore(db)
	_ = db.Close()

	if err := closedStore.WithTx(ctx, func(s Store) error { return nil }); err == nil {
		t.Errorf("expected error on closedStore.WithTx, got nil")
	}
}

func TestSQLStoreUpdateCronEffortAndPendingMessages(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. Nil store error branch
	var nilStore *SQLStore
	if err := nilStore.UpdateCronScheduleEffort(ctx, "cs1", "high"); err == nil {
		t.Errorf("expected error on nilStore.UpdateCronScheduleEffort, got nil")
	}

	// 2. Calling store.UpdateCronScheduleEffort with empty id and low effort
	if err := store.UpdateCronScheduleEffort(ctx, "", "high"); err != nil {
		t.Errorf("expected nil error for empty id in store.UpdateCronScheduleEffort, got %v", err)
	}
	if err := store.UpdateCronScheduleEffort(ctx, "cs1", "bad-effort"); err != nil {
		t.Errorf("expected nil error for bad effort in store.UpdateCronScheduleEffort, got %v", err)
	}

	// 3. Success via store
	cron := CronSchedule{
		ID:        "cs-effort-1",
		TargetID:  "chan-1",
		CronExpr:  "*/5 * * * *",
		Prompt:    "Prompt",
		NextRunAt: time.Now().Add(time.Hour),
		Enabled:   true,
	}
	if err := store.CreateCronSchedule(ctx, cron); err != nil {
		t.Fatalf("failed to create cron schedule: %v", err)
	}
	if err := store.UpdateCronScheduleEffort(ctx, "cs-effort-1", "low"); err != nil {
		t.Fatalf("store.UpdateCronScheduleEffort failed: %v", err)
	}

	// 4. GetPendingOrProcessingMessages error branch
	tmpPath := filepath.Join(t.TempDir(), "closed2.db")
	closedDB, err := initDB(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	closedStore := NewSQLStore(closedDB)
	_ = closedDB.Close()

	if _, err := closedStore.GetPendingOrProcessingMessages(ctx, 10); err == nil {
		t.Errorf("expected error from closedStore.GetPendingOrProcessingMessages, got nil")
	}

	// 5. ClaimNextPendingMessage error and non-pending branches
	if _, err := closedStore.ClaimNextPendingMessage(ctx, "w1"); err == nil {
		t.Errorf("expected error from closedStore.ClaimNextPendingMessage, got nil")
	}

	// Active store with only completed messages returns nil, nil
	_ = InsertMessage(database, Message{ID: "m-comp-only", ThreadID: "t1", Status: StatusCompleted})
	claimed, err := store.ClaimNextPendingMessage(ctx, "w1")
	if err != nil || claimed != nil {
		t.Errorf("expected nil, nil when no pending messages exist, got (%v, %v)", claimed, err)
	}
}

func TestMessagesValidationAndEdgeCases(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// 1. InsertMessage validations
	if err := InsertMessage(database, Message{ID: ""}); err == nil {
		t.Errorf("expected error on empty ID in InsertMessage")
	}

	// Default status assignment when Status is empty
	msgNoStatus := Message{ID: "m-default-stat", ThreadID: "t1", Status: ""}
	if err := InsertMessage(database, msgNoStatus); err != nil {
		t.Fatalf("InsertMessage with empty status failed: %v", err)
	}
	mFetched, err := GetMessage(database, "m-default-stat")
	if err != nil || mFetched == nil || mFetched.Status != StatusPending {
		t.Errorf("expected status to default to PENDING, got %+v", mFetched)
	}

	// 2. Empty ID checks on updates
	if err := UpdateMessageStatus(database, "", StatusCompleted, ""); err == nil {
		t.Errorf("expected error for empty id in UpdateMessageStatus")
	}
	if err := UpdateMessageCompleted(database, "", "res"); err == nil {
		t.Errorf("expected error for empty id in UpdateMessageCompleted")
	}
	if err := IncrementMessageRetry(database, "", "err"); err == nil {
		t.Errorf("expected error for empty id in IncrementMessageRetry")
	}
	if err := IncrementMessageRestart(database, "", "err"); err == nil {
		t.Errorf("expected error for empty id in IncrementMessageRestart")
	}
	if err := ResetMessageToPendingWithRestart(database, "", "err"); err == nil {
		t.Errorf("expected error for empty id in ResetMessageToPendingWithRestart")
	}

	// 3. GetMessage & MessageExists with empty ID
	if m, err := GetMessage(database, ""); err != nil || m != nil {
		t.Errorf("expected nil, nil for empty ID in GetMessage, got (%v, %v)", m, err)
	}
	if exists, err := MessageExists(database, ""); err != nil || exists {
		t.Errorf("expected false, nil for empty ID in MessageExists, got (%v, %v)", exists, err)
	}

	// 4. ClaimPendingMessage nil DB or empty ID
	if claimed, err := ClaimPendingMessage(nil, "m1"); err != nil || claimed {
		t.Errorf("expected false, nil on ClaimPendingMessage(nil, id), got (%v, %v)", claimed, err)
	}
	if claimed, err := ClaimPendingMessage(database, ""); err != nil || claimed {
		t.Errorf("expected false, nil on ClaimPendingMessage(db, \"\"), got (%v, %v)", claimed, err)
	}

	// 5. GetMaxMessageRowID nil DB or empty threadID
	if maxID, err := GetMaxMessageRowID(nil, "t1"); err != nil || maxID != 0 {
		t.Errorf("expected 0, nil on GetMaxMessageRowID(nil, t1), got (%d, %v)", maxID, err)
	}
	if maxID, err := GetMaxMessageRowID(database, ""); err != nil || maxID != 0 {
		t.Errorf("expected 0, nil on GetMaxMessageRowID(db, \"\"), got (%d, %v)", maxID, err)
	}

	// 6. Closed DB error paths
	tmpPath := filepath.Join(t.TempDir(), "closed3.db")
	closedDB, err := initDB(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	_ = closedDB.Close()

	if _, err := GetMessage(closedDB, "m1"); err == nil {
		t.Errorf("expected error on GetMessage with closed DB")
	}
	if _, err := MessageExists(closedDB, "m1"); err == nil {
		t.Errorf("expected error on MessageExists with closed DB")
	}
	if _, err := GetActiveRecentThreadIDs(closedDB, time.Hour); err == nil {
		t.Errorf("expected error on GetActiveRecentThreadIDs with closed DB")
	}
	if _, err := GetRecentThreadMessages(closedDB, "t1", 10); err == nil {
		t.Errorf("expected error on GetRecentThreadMessages with closed DB")
	}
	if _, err := GetMaxMessageRowID(closedDB, "t1"); err == nil {
		t.Errorf("expected error on GetMaxMessageRowID with closed DB")
	}
}

func TestSchedulesValidationAndMetricsBranches(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// 1. InsertMessageAndConsumeOneShot non-existent schedule error
	msg := Message{ID: "m-consume-err", ThreadID: "t1", Status: StatusPending}
	if err := InsertMessageAndConsumeOneShot(database, "non-existent-oneshot", msg); err == nil {
		t.Errorf("expected error consuming non-existent one-shot, got nil")
	}

	// 2. UpdateCronScheduleEffort branches
	if err := UpdateCronScheduleEffort(database, "", "high"); err != nil {
		t.Errorf("expected nil error on empty id in UpdateCronScheduleEffort, got %v", err)
	}
	if err := UpdateCronScheduleEffort(database, "cs1", "low"); err != nil {
		t.Errorf("expected nil error on low effort in UpdateCronScheduleEffort, got %v", err)
	}
	if err := UpdateCronScheduleEffort(database, "cs1", "other"); err != nil {
		t.Errorf("expected nil error on other effort in UpdateCronScheduleEffort, got %v", err)
	}

	// 3. CreateScheduleRun validation and effort fallback
	if err := CreateScheduleRun(database, ScheduleRun{ID: ""}); err == nil {
		t.Errorf("expected error on empty id in CreateScheduleRun")
	}
	now := time.Now().UTC()
	runDefaultEff := ScheduleRun{
		ID:           "run-eff-fallback",
		ScheduleID:   "cs1",
		ScheduleType: "cron",
		Effort:       "unrecognized_effort",
		Status:       "running",
		StartedAt:    now,
	}
	if err := CreateScheduleRun(database, runDefaultEff); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}
	runValidEff := ScheduleRun{
		ID:           "run-valid-effort",
		ScheduleID:   "cs1",
		ScheduleType: "cron",
		Effort:       "low",
		Status:       "running",
		StartedAt:    now,
	}
	if err := CreateScheduleRun(database, runValidEff); err != nil {
		t.Fatalf("CreateScheduleRun with low effort failed: %v", err)
	}

	// 4. UpdateScheduleRunStatus validations and len(sets) == 0 branch
	if err := UpdateScheduleRunStatus(database, UpdateRunParams{RunID: ""}); err == nil {
		t.Errorf("expected error on empty RunID in UpdateScheduleRunStatus")
	}
	if err := UpdateScheduleRunStatus(database, UpdateRunParams{RunID: "run-no-sets"}); err != nil {
		t.Errorf("expected nil error when no fields are updated in UpdateScheduleRunStatus, got %v", err)
	}
	if err := UpdateScheduleRunStatus(database, UpdateRunParams{RunID: "run-eff-fallback", Status: "completed"}); err != nil {
		t.Errorf("expected nil error on status completed in UpdateScheduleRunStatus, got %v", err)
	}

	// 5. GetScheduleRunsPaginated boundary clamps
	runs1, _, err := GetScheduleRunsPaginated(database, -5, -10, "", "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated negative bounds failed: %v", err)
	}
	_ = runs1

	runs2, _, err := GetScheduleRunsPaginated(database, 200, 0, "", "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated limit > 100 failed: %v", err)
	}
	_ = runs2

	// 6. GetScheduleSummaryMetrics next run precedence branches
	// Branch A: Cron next run is BEFORE one-shot next run
	_, _ = database.Exec("DELETE FROM cron_schedules")
	_, _ = database.Exec("DELETE FROM one_shot_schedules")

	tEarlier := now.Add(2 * time.Minute).Truncate(time.Second).UTC()
	tLater := now.Add(10 * time.Minute).Truncate(time.Second).UTC()

	_, err = database.Exec("INSERT INTO cron_schedules (id, target_id, cron_expr, prompt, enabled, next_run_at, created_at) VALUES ('cs-earlier', 't1', '*/5 * * * *', 'P', 1, ?, datetime('now'))", tEarlier)
	if err != nil {
		t.Fatalf("insert cron schedule failed: %v", err)
	}
	_, err = database.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at) VALUES ('os-later', 't1', 'P', ?, datetime('now'))", tLater)
	if err != nil {
		t.Fatalf("insert oneshot schedule failed: %v", err)
	}

	metricsA, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics failed: %v", err)
	}
	if metricsA.NextRunAt == nil || !metricsA.NextRunAt.Equal(tEarlier) {
		t.Errorf("expected next run to be earlier cron (%v), got %v", tEarlier, metricsA.NextRunAt)
	}

	// Branch B: Only cron schedule exists (cronNext.Valid && !oneShotNext.Valid)
	_, _ = database.Exec("DELETE FROM one_shot_schedules")
	metricsB, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics only cron failed: %v", err)
	}
	if metricsB.NextRunAt == nil || !metricsB.NextRunAt.Equal(tEarlier) {
		t.Errorf("expected next run to be cron (%v), got %v", tEarlier, metricsB.NextRunAt)
	}

	// Branch C: Only one-shot schedule exists (!cronNext.Valid && oneShotNext.Valid)
	_, _ = database.Exec("DELETE FROM cron_schedules")
	_, err = database.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at) VALUES ('os-only', 't1', 'P', ?, datetime('now'))", tLater)
	if err != nil {
		t.Fatalf("insert oneshot schedule failed: %v", err)
	}
	metricsC, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics only oneshot failed: %v", err)
	}
	if metricsC.NextRunAt == nil || !metricsC.NextRunAt.Equal(tLater) {
		t.Errorf("expected next run to be oneshot (%v), got %v", tLater, metricsC.NextRunAt)
	}

	// 7. PruneScheduleRuns clamps and closed DB errors
	affected, err := PruneScheduleRuns(database, 0, 0)
	if err != nil {
		t.Fatalf("PruneScheduleRuns with 0 bounds failed: %v", err)
	}
	_ = affected

	tmpPath := filepath.Join(t.TempDir(), "closed4.db")
	closedDB, err := initDB(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	_ = closedDB.Close()

	if _, err := GetScheduleSummaryMetrics(closedDB); err == nil {
		t.Errorf("expected error on GetScheduleSummaryMetrics with closed DB")
	}
	if _, err := PruneScheduleRuns(closedDB, 10, time.Hour); err == nil {
		t.Errorf("expected error on PruneScheduleRuns with closed DB")
	}
}

func TestSessionsValidationAndEdgeCases(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// 1. IncrementSessionTurnCount & RotateSessionID validations
	if _, err := IncrementSessionTurnCount(nil, "k"); err == nil {
		t.Errorf("expected error for nil DB in IncrementSessionTurnCount")
	}
	if _, err := IncrementSessionTurnCount(database, ""); err == nil {
		t.Errorf("expected error for empty key in IncrementSessionTurnCount")
	}

	if err := RotateSessionID(nil, "k", "n"); err == nil {
		t.Errorf("expected error for nil DB in RotateSessionID")
	}
	if err := RotateSessionID(database, "", "n"); err == nil {
		t.Errorf("expected error for empty key in RotateSessionID")
	}

	// 2. GetTurnState on thread with no messages (sql.ErrNoRows branch)
	_ = SaveSessionID(database, "th-empty-msgs", "sess-init-1")
	state, err := GetTurnState(database, "th-empty-msgs")
	if err != nil {
		t.Fatalf("GetTurnState failed on thread without messages: %v", err)
	}
	if state == nil || state.ExternalID != "th-empty-msgs" || state.InternalID != "sess-init-1" || state.LastMessageID != "" {
		t.Errorf("unexpected state on empty thread: %+v", state)
	}

	if s, err := GetTurnState(nil, "th1"); err != nil || s != nil {
		t.Errorf("expected nil, nil on GetTurnState(nil, th1), got (%v, %v)", s, err)
	}
	if s, err := GetTurnState(database, ""); err != nil || s != nil {
		t.Errorf("expected nil, nil on GetTurnState(db, \"\"), got (%v, %v)", s, err)
	}

	// 3. GetThreadSummary closed DB error
	tmpPath := filepath.Join(t.TempDir(), "closed5.db")
	closedDB, err := initDB(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	_ = closedDB.Close()

	if _, _, err := GetThreadSummary(closedDB, "t1"); err == nil {
		t.Errorf("expected error on GetThreadSummary with closed DB")
	}
}

func TestFactsAndTasksEdgeCases(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	embValid := make([]float32, ExpectedEmbeddingDim)
	embValid[0] = 1.0

	// 1. InsertFactWithContext with nil ctx
	id1, err := InsertFactWithContext(nil, database, false, "cat", "fact text nil ctx", 1.0, "th1", embValid)
	if err != nil || id1 <= 0 {
		t.Fatalf("InsertFactWithContext with nil ctx failed: %v, id=%d", err, id1)
	}

	// 2. GetFactsPaginatedWithContext with nil ctx
	res1, err := GetFactsPaginatedWithContext(nil, database, false, FactsFilter{Limit: 5})
	if err != nil || res1.Total == 0 {
		t.Fatalf("GetFactsPaginatedWithContext with nil ctx failed: %v", err)
	}

	// 3. GetFactsPaginatedWithContext isPg=true with Limit=0, Offset>0
	// This exercises line 504 (paginationSQL = "OFFSET $1") and line 521 (QueryContext error on sqlite)
	_, _ = GetFactsPaginatedWithContext(context.Background(), database, true, FactsFilter{Offset: 2})

	// 4. SearchSimilarFactsWithContext candidateLimit >= 30 (limit = 20)
	facts, err := SearchSimilarFactsWithContext(context.Background(), database, false, embValid, 20, 0.1, "th1")
	if err != nil {
		t.Fatalf("SearchSimilarFactsWithContext limit=20 failed: %v", err)
	}
	_ = facts

	// 5. CleanTaskSummary edge cases
	// Prompt on same line
	summary1 := CleanTaskSummary("<USER_REQUEST>\nPrompt: Clean inline prompt\n</USER_REQUEST>")
	if summary1 != "Clean inline prompt" {
		t.Errorf("expected 'Clean inline prompt', got %q", summary1)
	}

	// - content: format
	summary2 := CleanTaskSummary("<USER_REQUEST>\n- content: Bullet content format\n</USER_REQUEST>")
	if summary2 != "Bullet content format" {
		t.Errorf("expected 'Bullet content format', got %q", summary2)
	}

	// Only XML tags
	summary3 := CleanTaskSummary("<random_tag></random_tag>")
	if summary3 != "Agent Task" {
		t.Errorf("expected 'Agent Task', got %q", summary3)
	}

	// Empty string
	summary4 := CleanTaskSummary("   ")
	if summary4 != "Agent Task" {
		t.Errorf("expected 'Agent Task', got %q", summary4)
	}

	// 6. GetActiveTasks with empty message summary
	_, err = database.Exec("INSERT INTO messages (id, thread_id, content, summary, status, created_at, updated_at) VALUES ('m-empty-summary', 't-task', 'Prompt from task content', '', 'PENDING', datetime('now'), datetime('now'))")
	if err != nil {
		t.Fatalf("failed to insert message with empty summary: %v", err)
	}

	tasks, err := GetActiveTasks(database)
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}
	var foundTask bool
	for _, task := range tasks {
		if task.ID == "m-empty-summary" {
			foundTask = true
			if task.Summary != "Prompt from task content" {
				t.Errorf("expected auto-cleaned summary 'Prompt from task content', got %q", task.Summary)
			}
		}
	}
	if !foundTask {
		t.Errorf("expected to find task m-empty-summary in active tasks")
	}
}

func TestInitDBPostgresInvalidConfig(t *testing.T) {
	// postgres://%zz -> invalid url percent-encoding fails pgx.ParseConfig
	if _, err := initDB("postgres://%zz"); err == nil {
		t.Errorf("expected error parsing postgres://%%zz, got nil")
	}
}
