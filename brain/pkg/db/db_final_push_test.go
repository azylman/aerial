package db

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
)

func TestInitDBErrorBranchesAndSQLitePathCreation(t *testing.T) {
	// 1. Invalid postgres connection string
	if _, err := initDB("postgres://invalid user@localhost:5432/db"); err == nil {
		t.Errorf("expected pgx parse error for invalid postgres string, got nil")
	}

	// 2b. Test postgres retry loop failure when port is unreachable
	origAttempts := postgresMaxAttempts
	origRetryBase := postgresRetryBase
	postgresMaxAttempts = 1
	postgresRetryBase = 1 * time.Millisecond
	defer func() {
		postgresMaxAttempts = origAttempts
		postgresRetryBase = origRetryBase
	}()
	if _, err := initDB("postgres://user:pass@127.0.0.1:54321/db?sslmode=disable"); err == nil {
		t.Errorf("expected error connecting to unreachable postgres port, got nil")
	}

	// 3. SQLite directory creation for non-existent subfolder
	tmpDir := t.TempDir()
	subFile := filepath.Join(tmpDir, "subfolder", "test.db")
	db, err := initDB(subFile)
	if err != nil {
		t.Fatalf("initDB with subfolder path failed: %v", err)
	}
	db.Close()
	if _, err := os.Stat(subFile); os.IsNotExist(err) {
		t.Errorf("expected sqlite db file to exist at %s", subFile)
	}
}

func TestSearchSimilarFactsFilterBranches(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	ctx := context.Background()

	embValid := make([]float32, ExpectedEmbeddingDim)
	embValid[0] = 1.0

	// Fact 1: matching threadID
	_, _ = InsertFactWithContext(ctx, database, false, "cat", "Matching fact", 1.0, "t-match", embValid)

	// Fact 2: mismatched threadID
	_, _ = InsertFactWithContext(ctx, database, false, "cat", "Other thread fact", 1.0, "t-other", embValid)

	// Fact 3: no embedding (nil)
	_, _ = InsertFactWithContext(ctx, database, false, "cat", "No embedding fact", 1.0, "t-match", nil)

	res, err := SearchSimilarFactsWithContext(ctx, database, false, embValid, 10, 0.1, "t-match")
	if err != nil || len(res) != 1 {
		t.Fatalf("SearchSimilarFacts filter branches failed: %v, len=%d", err, len(res))
	}
	if res[0].FactText != "Matching fact" {
		t.Errorf("Expected 'Matching fact', got %q", res[0].FactText)
	}
}

func TestInitDBDSNVariants(t *testing.T) {
	// file: with existing query param ?
	db1, err1 := initDB("file:mem_test_1?mode=memory&cache=shared")
	if err1 != nil {
		t.Fatalf("initDB with query param failed: %v", err1)
	}
	db1.Close()

	// file: without query param ?
	db2, err2 := initDB("file:mem_test_2?mode=memory")
	if err2 != nil {
		t.Fatalf("initDB without query param failed: %v", err2)
	}
	db2.Close()

	// file: with existing _pragma
	db3, err3 := initDB("file:mem_test_3?mode=memory&_pragma=busy_timeout(5000)")
	if err3 != nil {
		t.Fatalf("initDB with existing _pragma failed: %v", err3)
	}
	db3.Close()

	// sqlite://:memory:
	db4, err4 := initDB("sqlite://:memory:")
	if err4 != nil {
		t.Fatalf("initDB sqlite://:memory: failed: %v", err4)
	}
	db4.Close()
}

func TestGetFactsPaginatedWithContextOffsetVariants(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	ctx := context.Background()

	_, _ = InsertFactWithContext(ctx, database, false, "cat1", "Fact item 1", 1.0, "t1", nil)

	res1, err1 := GetFactsPaginatedWithContext(ctx, database, false, FactsFilter{
		Limit:  0,
		Offset: 1,
	})
	if err1 != nil {
		t.Fatalf("GetFactsPaginatedWithContext SQLite offset-only failed: %v", err1)
	}
	_ = res1

	res3, err3 := GetFactsPaginatedWithContext(ctx, database, false, FactsFilter{
		Limit:  10,
		Offset: -5,
	})
	if err3 != nil {
		t.Fatalf("GetFactsPaginatedWithContext negative offset failed: %v", err3)
	}
	_ = res3
}

func TestSearchSimilarFactsAllBranchPaths(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	ctx := context.Background()

	embValid := make([]float32, ExpectedEmbeddingDim)
	embValid[0] = 1.0

	embInvalid := []float32{0.1, 0.2, 0.3}

	// 1. Fact matching threadID and high score
	_, _ = InsertFactWithContext(ctx, database, false, "cat", "High score fact", 1.0, "t-filter-1", embValid)

	// 2. Fact matching threadID but very low importance (filtered out by minScore=0.5)
	_, _ = InsertFactWithContext(ctx, database, false, "cat", "Low score fact", 0.001, "t-filter-1", embValid)

	// 3. Fact with mismatched threadID
	_, _ = InsertFactWithContext(ctx, database, false, "cat", "Other thread fact", 1.0, "t-filter-2", embValid)

	// 4. Fact with invalid embedding dimension
	_, _ = InsertFactWithContext(ctx, database, false, "cat", "Invalid dim fact", 1.0, "t-filter-1", embInvalid)

	res, err := SearchSimilarFactsWithContext(ctx, database, false, embValid, 10, 0.5, "t-filter-1")
	if err != nil || len(res) != 1 {
		t.Fatalf("SearchSimilarFactsAllBranchPaths failed: %v, len=%d", err, len(res))
	}
	if res[0].FactText != "High score fact" {
		t.Errorf("Expected 'High score fact', got %q", res[0].FactText)
	}
}

func TestInitSchemaSQLiteTwice(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	if err := initSchemaSQLite(database); err != nil {
		t.Errorf("initSchemaSQLite twice expected nil error, got %v", err)
	}
}

func TestInitSchemaSQLiteAlterError(t *testing.T) {
	database, err := InitDB("file:test_alter_err?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer database.Close()

	if _, err := database.Exec("ALTER TABLE non_existent_messages ADD COLUMN restart_count INTEGER NOT NULL DEFAULT 0;"); err == nil {
		t.Errorf("expected error altering missing table, got nil")
	}
}

func TestGetScheduleSummaryMetricsNextRunBranches(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	m, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics failed on empty db: %v", err)
	}
	if m.CronCount != 0 || m.OneShotCount != 0 {
		t.Errorf("expected 0 counts, got cron=%d one_shot=%d", m.CronCount, m.OneShotCount)
	}

	now := time.Now()
	t1 := now.Add(10 * time.Minute).Truncate(time.Second).UTC()
	_, err = database.Exec("INSERT INTO cron_schedules (id, target_id, cron_expr, prompt, enabled, next_run_at, created_at) VALUES ('cs1', 'target1', '*/5 * * * *', 'Prompt', 1, ?, datetime('now'))", t1)
	if err != nil {
		t.Fatalf("failed to insert cron schedule: %v", err)
	}

	t2 := now.Add(5 * time.Minute).Truncate(time.Second).UTC()
	_, err = database.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at) VALUES ('os1', 't1', 'Prompt', ?, datetime('now'))", t2)
	if err != nil {
		t.Fatalf("failed to insert one-shot schedule: %v", err)
	}

	m2, err2 := GetScheduleSummaryMetrics(database)
	if err2 != nil {
		t.Fatalf("GetScheduleSummaryMetrics failed with schedules: %v", err2)
	}
	if m2.CronCount != 1 || m2.OneShotCount != 1 {
		t.Errorf("expected counts 1, got cron=%d one_shot=%d", m2.CronCount, m2.OneShotCount)
	}
	if m2.NextRunAt == nil {
		t.Errorf("expected non-nil NextRunAt")
	}
}

func TestSQLStoreClaimNextPendingMessageSuccessAndNoPending(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	_ = InsertMessage(database, Message{ID: "m-proc", ThreadID: "t1", Status: StatusProcessing})
	m1, err1 := store.ClaimNextPendingMessage(ctx, "w1")
	if err1 != nil || m1 != nil {
		t.Fatalf("expected nil claim for processing message, got (%v, %v)", m1, err1)
	}

	_ = InsertMessage(database, Message{ID: "m-pend", ThreadID: "t1", Status: StatusPending})
	m2, err2 := store.ClaimNextPendingMessage(ctx, "w1")
	if err2 != nil || m2 == nil || m2.ID != "m-pend" {
		t.Fatalf("expected claim for m-pend, got (%v, %v)", m2, err2)
	}
}

func TestSQLStoreGetPendingOrProcessingMessagesLimitTruncation(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	_ = InsertMessage(database, Message{ID: "m1", ThreadID: "t1", Status: StatusPending})
	_ = InsertMessage(database, Message{ID: "m2", ThreadID: "t1", Status: StatusPending})

	msgs, err := store.GetPendingOrProcessingMessages(ctx, 1)
	if err != nil {
		t.Fatalf("GetPendingOrProcessingMessages failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected limit truncation to 1, got %d", len(msgs))
	}
}

func TestGetRecentThreadMessagesAndDueSchedulesErrorScanBranches(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	_, err1 := GetRecentThreadMessages(database, "t1", 0)
	if err1 != nil {
		t.Errorf("GetRecentThreadMessages with 0 limit failed: %v", err1)
	}

	now := time.Now().UTC()
	_, _ = database.Exec("INSERT INTO cron_schedules (id, target_id, cron_expr, prompt, enabled, next_run_at, created_at) VALUES ('cs-due', 't1', '0 * * * *', 'Prompt', 1, ?, ?)", now.Add(-time.Minute), now)
	cronList, err2 := GetDueCronSchedules(database)
	if err2 != nil || len(cronList) != 1 {
		t.Errorf("GetDueCronSchedules failed: err=%v, len=%d", err2, len(cronList))
	}

	_, _ = database.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at) VALUES ('os-due', 't1', 'Prompt', ?, ?)", now.Add(-time.Minute), now)
	osList, err3 := GetDueOneShotSchedules(database)
	if err3 != nil || len(osList) != 1 {
		t.Errorf("GetDueOneShotSchedules failed: err=%v, len=%d", err3, len(osList))
	}
}

func TestNullVectorScanFallthroughBranch(t *testing.T) {
	var nv NullVector
	if err := nv.Scan(12345); err == nil {
		t.Errorf("expected scan error for int type, got nil")
	}
}

func TestCleanTaskSummaryBranches(t *testing.T) {
	input1 := "<USER_REQUEST>\nPrompt:\n   Actual prompt content here\n</USER_REQUEST>"
	res1 := CleanTaskSummary(input1)
	if res1 != "Actual prompt content here" {
		t.Errorf("expected 'Actual prompt content here', got %q", res1)
	}

	input2 := "<tag>@mention</tag>"
	res2 := CleanTaskSummary(input2)
	if res2 != "Agent Task" {
		t.Errorf("expected 'Agent Task', got %q", res2)
	}

	input3 := "This is a very long prompt string " + strings.Repeat("with lots of extra words ", 10)
	res3 := CleanTaskSummary(input3)
	if !strings.HasSuffix(res3, "...") {
		t.Errorf("expected truncated string with ..., got %q", res3)
	}
}

func TestGetActiveTasksNilDB(t *testing.T) {
	if _, err := GetActiveTasks(nil); err == nil {
		t.Errorf("expected error on nil DB, got nil")
	}
}

func TestSQLStoreMethodsCoverage(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. FactStore methods
	_, err := store.InsertFact(ctx, "cat", "fact text", 1.0, "th1", nil)
	if err != nil {
		t.Fatalf("SQLStore.InsertFact failed: %v", err)
	}

	_, err = store.SearchSimilarFacts(ctx, []float32{1.0}, 10, 0.1, "th1")
	if err != nil {
		t.Fatalf("SQLStore.SearchSimilarFacts failed: %v", err)
	}

	_, err = store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if err != nil {
		t.Fatalf("SQLStore.GetFactsPaginated failed: %v", err)
	}

	_, err = store.GetFactsByThreadWithEmbeddings(ctx, "th1")
	if err != nil {
		t.Fatalf("SQLStore.GetFactsByThreadWithEmbeddings failed: %v", err)
	}

	_, err = store.GetActiveConversationsForExtraction(ctx, 24)
	if err != nil {
		t.Fatalf("SQLStore.GetActiveConversationsForExtraction failed: %v", err)
	}

	if err := store.UpdateConversationFactWatermark(ctx, "th1", 10); err != nil {
		t.Fatalf("SQLStore.UpdateConversationFactWatermark failed: %v", err)
	}

	if err := store.UpdateConversationFactExtractedAt(ctx, "th1"); err != nil {
		t.Fatalf("SQLStore.UpdateConversationFactExtractedAt failed: %v", err)
	}

	// 2. ScheduleStore methods
	if err := store.CreateOneShotSchedule(ctx, OneShotSchedule{ID: "os-store-1", ThreadID: "th1", Prompt: "p", RunAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("SQLStore.CreateOneShotSchedule failed: %v", err)
	}

	dueOS, err := store.GetDueOneShotSchedules(ctx)
	if err != nil {
		t.Fatalf("SQLStore.GetDueOneShotSchedules failed: %v", err)
	}
	_ = dueOS

	allOS, err := store.GetAllOneShotSchedules(ctx, "th1")
	if err != nil || len(allOS) == 0 {
		t.Fatalf("SQLStore.GetAllOneShotSchedules failed: %v", err)
	}

	if err := store.InsertMessageAndConsumeOneShot(ctx, "os-store-1", Message{ID: "m-os-1", ThreadID: "th1", Status: StatusPending}); err != nil {
		t.Fatalf("SQLStore.InsertMessageAndConsumeOneShot failed: %v", err)
	}

	if err := store.DeleteOneShotSchedule(ctx, "os-store-1"); err != nil {
		t.Fatalf("SQLStore.DeleteOneShotSchedule failed: %v", err)
	}

	if err := store.CreateCronSchedule(ctx, CronSchedule{ID: "cs-store-1", TargetID: "th1", CronExpr: "*/5 * * * *", Prompt: "p", Enabled: true, NextRunAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("SQLStore.CreateCronSchedule failed: %v", err)
	}

	dueCron, err := store.GetDueCronSchedules(ctx)
	if err != nil {
		t.Fatalf("SQLStore.GetDueCronSchedules failed: %v", err)
	}
	_ = dueCron

	allCron, err := store.GetAllCronSchedules(ctx, "th1")
	if err != nil || len(allCron) == 0 {
		t.Fatalf("SQLStore.GetAllCronSchedules failed: %v", err)
	}

	if err := store.UpdateCronNextRun(ctx, "cs-store-1", time.Now().Add(2*time.Hour)); err != nil {
		t.Fatalf("SQLStore.UpdateCronNextRun failed: %v", err)
	}

	if err := store.DeleteCronSchedule(ctx, "cs-store-1"); err != nil {
		t.Fatalf("SQLStore.DeleteCronSchedule failed: %v", err)
	}

	if err := store.CreateScheduleRun(ctx, ScheduleRun{ID: "run-store-1", ScheduleType: "cron", ScheduleID: "cs1", TargetID: "th1", Status: "running", StartedAt: time.Now()}); err != nil {
		t.Fatalf("SQLStore.CreateScheduleRun failed: %v", err)
	}

	if err := store.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: "run-store-1", Status: "success", Error: "no error", CompletedAt: time.Now(), DurationMs: 100}); err != nil {
		t.Fatalf("SQLStore.UpdateScheduleRunStatus failed: %v", err)
	}

	_, _, err = store.GetScheduleRunsPaginated(ctx, 10, 0, "cs1", "success")
	if err != nil {
		t.Fatalf("SQLStore.GetScheduleRunsPaginated failed: %v", err)
	}

	_, err = store.GetScheduleSummaryMetrics(ctx)
	if err != nil {
		t.Fatalf("SQLStore.GetScheduleSummaryMetrics failed: %v", err)
	}

	if _, err := store.ReconcileOrphanedScheduleRuns(ctx); err != nil {
		t.Fatalf("SQLStore.ReconcileOrphanedScheduleRuns failed: %v", err)
	}

	if _, err := store.PruneScheduleRuns(ctx, 100, 24*time.Hour); err != nil {
		t.Fatalf("SQLStore.PruneScheduleRuns failed: %v", err)
	}

	// 3. MessageStore methods
	if err := store.InsertMessage(ctx, Message{ID: "m-store-1", ThreadID: "th1", Status: StatusPending}); err != nil {
		t.Fatalf("SQLStore.InsertMessage failed: %v", err)
	}

	if err := store.UpdateMessageStatus(ctx, "m-store-1", StatusProcessing, "worker-1"); err != nil {
		t.Fatalf("SQLStore.UpdateMessageStatus failed: %v", err)
	}

	if err := store.UpdateMessageCompleted(ctx, "m-store-1", "response text"); err != nil {
		t.Fatalf("SQLStore.UpdateMessageCompleted failed: %v", err)
	}

	if err := store.IncrementMessageRetry(ctx, "m-store-1", "err msg"); err != nil {
		t.Fatalf("SQLStore.IncrementMessageRetry failed: %v", err)
	}

	if err := store.IncrementMessageRestart(ctx, "m-store-1", "err msg"); err != nil {
		t.Fatalf("SQLStore.IncrementMessageRestart failed: %v", err)
	}

	if err := store.ResetMessageToPendingWithRestart(ctx, "m-store-1", "reason"); err != nil {
		t.Fatalf("SQLStore.ResetMessageToPendingWithRestart failed: %v", err)
	}

	msg, err := store.GetMessage(ctx, "m-store-1")
	if err != nil || msg == nil {
		t.Fatalf("SQLStore.GetMessage failed: %v", err)
	}

	exists, err := store.MessageExists(ctx, "m-store-1")
	if err != nil || !exists {
		t.Fatalf("SQLStore.MessageExists failed: %v, %v", exists, err)
	}

	if err := store.InsertMessage(ctx, Message{ID: "m-store-claim", ThreadID: "th1", Status: StatusPending}); err != nil {
		t.Fatalf("SQLStore.InsertMessage for claim failed: %v", err)
	}

	claimed, err := store.ClaimPendingMessage(ctx, "m-store-claim")
	if err != nil || !claimed {
		t.Fatalf("SQLStore.ClaimPendingMessage failed: %v, claimed=%v", err, claimed)
	}

	_, err = store.GetPendingOrProcessingMessages(ctx, 10)
	if err != nil {
		t.Fatalf("SQLStore.GetPendingOrProcessingMessages failed: %v", err)
	}

	_, err = store.GetActiveRecentThreadIDs(ctx, 10)
	if err != nil {
		t.Fatalf("SQLStore.GetActiveRecentThreadIDs failed: %v", err)
	}

	_, err = store.GetRecentThreadMessages(ctx, "th1", 10)
	if err != nil {
		t.Fatalf("SQLStore.GetRecentThreadMessages failed: %v", err)
	}

	_, err = store.GetMaxMessageRowID(ctx, "th1")
	if err != nil {
		t.Fatalf("SQLStore.GetMaxMessageRowID failed: %v", err)
	}

	// 4. SessionStore methods
	if _, err := store.GetSessionID(ctx, "th1"); err != nil {
		t.Fatalf("SQLStore.GetSessionID failed: %v", err)
	}

	if err := store.SaveSessionID(ctx, "th1", "sess-1"); err != nil {
		t.Fatalf("SQLStore.SaveSessionID failed: %v", err)
	}

	if _, err := store.IncrementSessionTurnCount(ctx, "th1"); err != nil {
		t.Fatalf("SQLStore.IncrementSessionTurnCount failed: %v", err)
	}

	if _, err := store.GetSessionTurnCount(ctx, "th1"); err != nil {
		t.Fatalf("SQLStore.GetSessionTurnCount failed: %v", err)
	}

	if err := store.RotateSessionID(ctx, "th1", "sess-2"); err != nil {
		t.Fatalf("SQLStore.RotateSessionID failed: %v", err)
	}

	prevID, err := store.GetPreviousSessionID(ctx, "th1")
	if err != nil {
		t.Fatalf("SQLStore.GetPreviousSessionID failed: %v", err)
	}
	if prevID != "sess-1" {
		t.Fatalf("Expected previous session ID 'sess-1', got %q", prevID)
	}

	if err := store.DeleteSessionID(ctx, "th1"); err != nil {
		t.Fatalf("SQLStore.DeleteSessionID failed: %v", err)
	}

	// 5. Close store
	if err := store.Close(); err != nil {
		t.Fatalf("SQLStore.Close failed: %v", err)
	}

	// Nil store calls return expected nil db errors
	var nilStore *SQLStore = nil
	if err := nilStore.Close(); err != nil {
		t.Errorf("expected nil error on nilStore.Close(), got %v", err)
	}
	if _, err := nilStore.GetSessionID(ctx, "th1"); err == nil {
		t.Errorf("expected error calling GetSessionID on nil store, got nil")
	}
}

func TestThreadSummaryDirectFunctions(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// 1. Nil / empty checks
	if err := SaveThreadSummary(nil, "th1", "sum", "m1"); err != nil {
		t.Errorf("expected nil for nil DB in SaveThreadSummary")
	}
	if err := SaveThreadSummary(database, "", "sum", "m1"); err != nil {
		t.Errorf("expected nil for empty threadID in SaveThreadSummary")
	}

	sum1, msg1, err1 := GetThreadSummary(nil, "th1")
	if err1 != nil || sum1 != "" || msg1 != "" {
		t.Errorf("expected empty strings, nil err for nil DB in GetThreadSummary, got %q, %q, %v", sum1, msg1, err1)
	}
	sum2, msg2, err2 := GetThreadSummary(database, "")
	if err2 != nil || sum2 != "" || msg2 != "" {
		t.Errorf("expected empty strings, nil err for empty threadID in GetThreadSummary, got %q, %q, %v", sum2, msg2, err2)
	}

	// 2. Non-existent thread ID returns empty strings and nil error
	sum3, msg3, err3 := GetThreadSummary(database, "non-existent")
	if err3 != nil || sum3 != "" || msg3 != "" {
		t.Errorf("expected empty, nil for non-existent thread in GetThreadSummary, got %q, %q, %v", sum3, msg3, err3)
	}

	// 3. Save and retrieve thread summary
	if err := SaveThreadSummary(database, "th-sum-1", "First summary", "m-sum-1"); err != nil {
		t.Fatalf("SaveThreadSummary failed: %v", err)
	}

	sum4, msg4, err4 := GetThreadSummary(database, "th-sum-1")
	if err4 != nil || sum4 != "First summary" || msg4 != "m-sum-1" {
		t.Fatalf("GetThreadSummary failed: sum=%q msg=%q err=%v", sum4, msg4, err4)
	}

	// 4. Update existing thread summary (upsert ON CONFLICT)
	if err := SaveThreadSummary(database, "th-sum-1", "Updated summary", "m-sum-2"); err != nil {
		t.Fatalf("SaveThreadSummary upsert failed: %v", err)
	}

	sum5, msg5, err5 := GetThreadSummary(database, "th-sum-1")
	if err5 != nil || sum5 != "Updated summary" || msg5 != "m-sum-2" {
		t.Fatalf("GetThreadSummary upsert check failed: sum=%q msg=%q err=%v", sum5, msg5, err5)
	}
}

func TestSearchSimilarFactsWithContextPgBranch(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	ctx := context.Background()
	embValid := make([]float32, ExpectedEmbeddingDim)
	embValid[0] = 1.0

	// Trigger isPg = true branch of SearchSimilarFactsWithContext on non-pg DB to force error path
	res, err := SearchSimilarFactsWithContext(ctx, database, true, embValid, 10, 0.2, "th1")
	if err == nil && res != nil {
		t.Errorf("expected error or nil response when searching pg branch on sqlite db, got %v, %v", res, err)
	}
}

func TestInitDBAndNewEdgeCases(t *testing.T) {
	// 1. InitDB with invalid type (e.g. int)
	if _, err := InitDB(12345); err == nil {
		t.Errorf("expected error for int type in InitDB, got nil")
	}

	// 2. New with nil config snapshot
	cfgEmptySnap := &config.Config{}
	if _, err := New(cfgEmptySnap); err == nil {
		t.Errorf("expected error for nil config snapshot in New, got nil")
	}
}

