package db

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestWithTxCommit(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	err := store.WithTx(ctx, func(txStore Store) error {
		_, err := txStore.InsertFact(ctx, "test", "Transactional fact", 1.0, nil)
		return err
	})
	if err != nil {
		t.Fatalf("WithTx failed: %v", err)
	}

	facts, err := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if err != nil {
		t.Fatalf("GetFactsPaginated failed: %v", err)
	}
	if len(facts.Facts) != 1 {
		t.Fatalf("Expected 1 fact committed, got %d", len(facts.Facts))
	}
	if facts.Facts[0].FactText != "Transactional fact" {
		t.Errorf("Expected 'Transactional fact', got %q", facts.Facts[0].FactText)
	}
}

func TestWithTxRollback(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	expectedErr := errors.New("simulated transaction failure")
	err := store.WithTx(ctx, func(txStore Store) error {
		_, err := txStore.InsertFact(ctx, "test", "Uncommitted fact", 1.0, nil)
		if err != nil {
			return err
		}
		return expectedErr
	})

	if !errors.Is(err, expectedErr) {
		t.Fatalf("Expected %v, got %v", expectedErr, err)
	}

	facts, err := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if err != nil {
		t.Fatalf("GetFactsPaginated failed: %v", err)
	}
	if len(facts.Facts) != 0 {
		t.Fatalf("Expected 0 facts after rollback, got %d", len(facts.Facts))
	}
}

func TestNewTxStoreTypedNil(t *testing.T) {
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

	// 5. NewTxStore with untyped nil and typed nil
	if store := NewTxStore(nil, false); store != nil {
		t.Errorf("expected NewTxStore(nil, false) to be nil, got %v", store)
	}
	if store := NewTxStore(nilTx, false); store != nil {
		t.Fatalf("Expected NewTxStore to return nil for typed nil *sql.Tx, got %v", store)
	}

	// 6. SQLStore.Close() branches
	var nilStore *SQLStore
	if err := nilStore.Close(); err != nil {
		t.Errorf("expected nil error on nilStore.Close(), got %v", err)
	}

	storeWithNilDB := &SQLStore{db: nil}
	if err := storeWithNilDB.Close(); err != nil {
		t.Errorf("expected nil error on storeWithNilDB.Close(), got %v", err)
	}

	storeNonCloser := &SQLStore{db: dummyDBTXNoDriver{}}
	if err := storeNonCloser.Close(); err != nil {
		t.Errorf("expected nil error on storeNonCloser.Close(), got %v", err)
	}

	// 7. SQLStore.WithTx branches
	ctx := context.Background()
	if err := nilStore.WithTx(ctx, func(s Store) error { return nil }); err == nil {
		t.Errorf("expected error on nilStore.WithTx, got nil")
	}
	if err := storeWithNilDB.WithTx(ctx, func(s Store) error { return nil }); err == nil {
		t.Errorf("expected error on storeWithNilDB.WithTx, got nil")
	}

	executed := false
	err := storeNonCloser.WithTx(ctx, func(s Store) error {
		executed = true
		return nil
	})
	if err != nil || !executed {
		t.Errorf("expected WithTx without beginner to execute fn directly, err=%v executed=%v", err, executed)
	}

	tmpPath := filepath.Join(t.TempDir(), "closed.db")
	db, err := initTestSQLite(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	closedStore := NewSQLStore(db)
	_ = db.Close()

	if err := closedStore.WithTx(ctx, func(s Store) error { return nil }); err == nil {
		t.Errorf("expected error on closedStore.WithTx, got nil")
	}

	if _, err := nilStore.InsertFact(ctx, "cat", "text", 1.0, nil); err == nil {
		t.Fatalf("Expected error when calling InsertFact on nil *SQLStore, got nil")
	}
}

func TestSQLStoreNilDatabaseBranchCoverage(t *testing.T) {
	var s *SQLStore = nil
	ctx := context.Background()

	if store := NewSQLStore(nil); store != nil {
		t.Errorf("NewSQLStore(nil) expected nil, got %v", store)
	}

	if err := s.WithTx(ctx, nil); err == nil {
		t.Errorf("expected error calling WithTx on nil SQLStore, got nil")
	}

	if _, err := s.InsertFact(ctx, "c", "f", 1.0, nil); err == nil {
		t.Errorf("expected error on InsertFact nil store")
	}
	if _, err := s.SearchSimilarFacts(ctx, nil, "", 10, 0.1); err == nil {
		t.Errorf("expected error on SearchSimilarFacts nil store")
	}
	if _, err := s.GetFactsPaginated(ctx, FactsFilter{}); err == nil {
		t.Errorf("expected error on GetFactsPaginated nil store")
	}
	if _, err := s.GetFactsByThreadWithEmbeddings(ctx, "t"); err == nil {
		t.Errorf("expected error on GetFactsByThreadWithEmbeddings nil store")
	}
	if _, err := s.GetActiveConversationsForExtraction(ctx, 10); err == nil {
		t.Errorf("expected error on GetActiveConversationsForExtraction nil store")
	}
	if err := s.UpdateConversationFactWatermark(ctx, "t", 1); err == nil {
		t.Errorf("expected error on UpdateConversationFactWatermark nil store")
	}
	if err := s.UpdateConversationFactExtractedAt(ctx, "t"); err == nil {
		t.Errorf("expected error on UpdateConversationFactExtractedAt nil store")
	}

	if err := s.CreateOneShotSchedule(ctx, OneShotSchedule{}); err == nil {
		t.Errorf("expected error on CreateOneShotSchedule nil store")
	}
	if _, err := s.GetDueOneShotSchedules(ctx); err == nil {
		t.Errorf("expected error on GetDueOneShotSchedules nil store")
	}
	if err := s.DeleteOneShotSchedule(ctx, "id"); err == nil {
		t.Errorf("expected error on DeleteOneShotSchedule nil store")
	}
	if err := s.InsertMessageAndConsumeOneShot(ctx, "id", Message{}); err == nil {
		t.Errorf("expected error on InsertMessageAndConsumeOneShot nil store")
	}
	if _, err := s.GetAllOneShotSchedules(ctx, "t"); err == nil {
		t.Errorf("expected error on GetAllOneShotSchedules nil store")
	}
	if err := s.CreateCronSchedule(ctx, CronSchedule{}); err == nil {
		t.Errorf("expected error on CreateCronSchedule nil store")
	}
	if _, err := s.GetDueCronSchedules(ctx); err == nil {
		t.Errorf("expected error on GetDueCronSchedules nil store")
	}
	if _, err := s.GetAllCronSchedules(ctx, "t"); err == nil {
		t.Errorf("expected error on GetAllCronSchedules nil store")
	}
	if err := s.DeleteCronSchedule(ctx, "id"); err == nil {
		t.Errorf("expected error on DeleteCronSchedule nil store")
	}
	if err := s.UpdateCronNextRun(ctx, "id", time.Now()); err == nil {
		t.Errorf("expected error on UpdateCronNextRun nil store")
	}
	if err := s.CreateScheduleRun(ctx, ScheduleRun{}); err == nil {
		t.Errorf("expected error on CreateScheduleRun nil store")
	}
	if err := s.UpdateScheduleRunStatus(ctx, UpdateRunParams{}); err == nil {
		t.Errorf("expected error on UpdateScheduleRunStatus nil store")
	}
	if _, _, err := s.GetScheduleRunsPaginated(ctx, 10, 0, "", ""); err == nil {
		t.Errorf("expected error on GetScheduleRunsPaginated nil store")
	}
	if _, err := s.GetScheduleSummaryMetrics(ctx); err == nil {
		t.Errorf("expected error on GetScheduleSummaryMetrics nil store")
	}
	if _, err := s.ReconcileOrphanedScheduleRuns(ctx); err == nil {
		t.Errorf("expected error on ReconcileOrphanedScheduleRuns nil store")
	}
	if _, err := s.PruneScheduleRuns(ctx, 10, time.Hour); err == nil {
		t.Errorf("expected error on PruneScheduleRuns nil store")
	}

	if err := s.InsertMessage(ctx, Message{}); err == nil {
		t.Errorf("expected error on InsertMessage nil store")
	}
	if err := s.UpdateMessageStatus(ctx, "id", "", ""); err == nil {
		t.Errorf("expected error on UpdateMessageStatus nil store")
	}
	if err := s.UpdateMessageCompleted(ctx, "id", ""); err == nil {
		t.Errorf("expected error on UpdateMessageCompleted nil store")
	}
	if err := s.IncrementMessageRetry(ctx, "id", ""); err == nil {
		t.Errorf("expected error on IncrementMessageRetry nil store")
	}
	if err := s.IncrementMessageRestart(ctx, "id", ""); err == nil {
		t.Errorf("expected error on IncrementMessageRestart nil store")
	}
	if err := s.ResetMessageToPendingWithRestart(ctx, "id", ""); err == nil {
		t.Errorf("expected error on ResetMessageToPendingWithRestart nil store")
	}
	if _, err := s.GetPendingOrProcessingMessages(ctx, 10); err == nil {
		t.Errorf("expected error on GetPendingOrProcessingMessages nil store")
	}
	if _, err := s.GetMessage(ctx, "id"); err == nil {
		t.Errorf("expected error on GetMessage nil store")
	}
	if _, err := s.MessageExists(ctx, "id"); err == nil {
		t.Errorf("expected error on MessageExists nil store")
	}
	if _, err := s.ClaimPendingMessage(ctx, "id"); err == nil {
		t.Errorf("expected error on ClaimPendingMessage nil store")
	}
	if _, err := s.ClaimNextPendingMessage(ctx, "w"); err == nil {
		t.Errorf("expected error on ClaimNextPendingMessage nil store")
	}
	if _, err := s.GetActiveRecentThreadIDs(ctx, time.Hour); err == nil {
		t.Errorf("expected error on GetActiveRecentThreadIDs nil store")
	}
	if _, err := s.GetRecentThreadMessages(ctx, "t", 10); err == nil {
		t.Errorf("expected error on GetRecentThreadMessages nil store")
	}
	if _, err := s.GetMaxMessageRowID(ctx, "t"); err == nil {
		t.Errorf("expected error on GetMaxMessageRowID nil store")
	}

	if _, err := s.GetSessionID(ctx, "t"); err == nil {
		t.Errorf("expected error on GetSessionID nil store")
	}
	if err := s.SaveSessionID(ctx, "t", "s"); err == nil {
		t.Errorf("expected error on SaveSessionID nil store")
	}
	if err := s.DeleteSessionID(ctx, "t"); err == nil {
		t.Errorf("expected error on DeleteSessionID nil store")
	}
	if _, err := s.IncrementSessionTurnCount(ctx, "k"); err == nil {
		t.Errorf("expected error on IncrementSessionTurnCount nil store")
	}
	if _, err := s.GetSessionTurnCount(ctx, "k"); err == nil {
		t.Errorf("expected error on GetSessionTurnCount nil store")
	}
	if err := s.RotateSessionID(ctx, "k", "n"); err == nil {
		t.Errorf("expected error on RotateSessionID nil store")
	}
	if _, err := s.GetPreviousSessionID(ctx, "k"); err == nil {
		t.Errorf("expected error on GetPreviousSessionID nil store")
	}

	if _, _, err := s.GetThreadSummary(ctx, "t"); err == nil {
		t.Errorf("expected error on GetThreadSummary nil store")
	}
	if err := s.SaveThreadSummary(ctx, "t", "s", "w"); err == nil {
		t.Errorf("expected error on SaveThreadSummary nil store")
	}
	if _, err := s.GetSessionActivityStats(ctx, "t"); err == nil {
		t.Errorf("expected error on GetSessionActivityStats nil store")
	}
	if _, err := s.GetFactsMissingEmbeddings(ctx, 10); err == nil {
		t.Errorf("expected error on GetFactsMissingEmbeddings nil store")
	}
	if err := s.UpdateFactEmbedding(ctx, 1, nil); err == nil {
		t.Errorf("expected error on UpdateFactEmbedding nil store")
	}
	if _, err := s.GetSessionSyncStates(ctx); err == nil {
		t.Errorf("expected error on GetSessionSyncStates nil store")
	}
	if err := s.UpsertSessionSummary(ctx, SessionSummary{}); err == nil {
		t.Errorf("expected error on UpsertSessionSummary nil store")
	}
	if err := s.BatchInsertTranscriptSteps(ctx, nil); err == nil {
		t.Errorf("expected error on BatchInsertTranscriptSteps nil store")
	}
	if _, err := s.SearchSessionSummaries(ctx, nil, "q", 10, 0.1); err == nil {
		t.Errorf("expected error on SearchSessionSummaries nil store")
	}
	if _, err := s.SearchTranscriptSteps(ctx, "q", "s", "t", 10); err == nil {
		t.Errorf("expected error on SearchTranscriptSteps nil store")
	}
	if err := s.UpdateCronScheduleEffort(ctx, "cs1", "high"); err == nil {
		t.Errorf("expected error on UpdateCronScheduleEffort nil store")
	}
	if _, err := s.GetActiveTasks(ctx); err == nil {
		t.Errorf("expected error on GetActiveTasks nil store")
	}
	if _, err := s.GetExternalConversationID(ctx, "test"); err == nil {
		t.Errorf("expected error on GetExternalConversationID nil store")
	}
	if err := s.SaveConversationMapping(ctx, "ext", "int"); err == nil {
		t.Errorf("expected error on SaveConversationMapping nil store")
	}

	storeWithNilDB := &SQLStore{db: nil}
	if _, err := storeWithNilDB.GetActiveTasks(ctx); err == nil {
		t.Errorf("expected error calling GetActiveTasks with nil db")
	}
	if _, err := storeWithNilDB.GetExternalConversationID(ctx, "test"); err == nil {
		t.Errorf("expected error calling GetExternalConversationID with nil db")
	}
	if err := storeWithNilDB.SaveConversationMapping(ctx, "ext", "int"); err == nil {
		t.Errorf("expected error calling SaveConversationMapping with nil db")
	}
	if err := storeWithNilDB.UpdateCronScheduleEffort(ctx, "cs1", "high"); err == nil {
		t.Errorf("expected error calling UpdateCronScheduleEffort with nil db")
	}
	if _, err := storeWithNilDB.SearchSimilarFacts(ctx, nil, "", 10, 0.1); err == nil {
		t.Errorf("expected error on SearchSimilarFacts storeWithNilDB")
	}
	if _, err := storeWithNilDB.GetAllFactsWithEmbeddings(ctx); err == nil {
		t.Errorf("expected error on GetAllFactsWithEmbeddings storeWithNilDB")
	}
	if err := storeWithNilDB.CreateOneShotSchedule(ctx, OneShotSchedule{}); err == nil {
		t.Errorf("expected error on CreateOneShotSchedule storeWithNilDB")
	}
	if _, err := storeWithNilDB.GetDueOneShotSchedules(ctx); err == nil {
		t.Errorf("expected error on GetDueOneShotSchedules storeWithNilDB")
	}
	if err := storeWithNilDB.InsertMessageAndConsumeOneShot(ctx, "id", Message{}); err == nil {
		t.Errorf("expected error on InsertMessageAndConsumeOneShot storeWithNilDB")
	}
	if _, err := storeWithNilDB.ClaimPendingMessage(ctx, "id"); err == nil {
		t.Errorf("expected error on ClaimPendingMessage storeWithNilDB")
	}
	if _, err := storeWithNilDB.SearchSessionSummaries(ctx, nil, "q", 10, 0.1); err == nil {
		t.Errorf("expected error on SearchSessionSummaries storeWithNilDB")
	}
}

func TestSQLStoreHoistedMethods(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	// 1. Thread summary
	sum, wm, err := store.GetThreadSummary(ctx, "thread-test-1")
	if err != nil {
		t.Fatalf("GetThreadSummary failed: %v", err)
	}
	if sum != "" || wm != "" {
		t.Fatalf("expected empty summary and watermark, got %q, %q", sum, wm)
	}

	err = store.SaveThreadSummary(ctx, "thread-test-1", "Summary of conversation", "msg-watermark-1")
	if err != nil {
		t.Fatalf("SaveThreadSummary failed: %v", err)
	}

	sum, wm, err = store.GetThreadSummary(ctx, "thread-test-1")
	if err != nil {
		t.Fatalf("GetThreadSummary after save failed: %v", err)
	}
	if sum != "Summary of conversation" || wm != "msg-watermark-1" {
		t.Fatalf("expected saved summary, got %q, %q", sum, wm)
	}

	// 2. Session activity stats
	emptyStats, err := store.GetSessionActivityStats(ctx, "   ")
	if err != nil || emptyStats == nil {
		t.Fatalf("expected empty stats for whitespace threadID")
	}

	stats, err := store.GetSessionActivityStats(ctx, "thread-test-1")
	if err != nil {
		t.Fatalf("GetSessionActivityStats failed: %v", err)
	}
	if stats == nil {
		t.Fatalf("expected non-nil stats")
	}

	_ = store.SaveSessionID(ctx, "thread-test-1", "internal-sess-1")
	stats, err = store.GetSessionActivityStats(ctx, "thread-test-1")
	if err != nil || stats.InternalSessionID != "internal-sess-1" {
		t.Fatalf("expected internal-sess-1 in stats, got %+v (err: %v)", stats, err)
	}

	// Insert completed message and re-check stats
	_ = store.InsertMessage(ctx, Message{
		ID:           "msg-stats-1",
		ThreadID:     "thread-test-1",
		Status:       StatusCompleted,
		ResponseText: "Hello there!",
	})
	stats, err = store.GetSessionActivityStats(ctx, "thread-test-1")
	if err != nil {
		t.Fatalf("GetSessionActivityStats with completed message failed: %v", err)
	}
	if stats.CompletedTurns != 1 {
		t.Fatalf("expected 1 completed turn, got %d", stats.CompletedTurns)
	}
	if stats.LastCompletedMessageCreatedAt.IsZero() {
		t.Errorf("expected non-zero LastCompletedMessageCreatedAt")
	}

	// 3. Facts missing embeddings & UpdateFactEmbedding
	factID, err := store.InsertFact(ctx, "test", "Fact without embedding", 1.0, nil)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	missing, err := store.GetFactsMissingEmbeddings(ctx, 10)
	if err != nil {
		t.Fatalf("GetFactsMissingEmbeddings failed: %v", err)
	}
	var found bool
	for _, f := range missing {
		if f.ID == factID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected factID %d in missing embeddings list", factID)
	}

	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 0.5
	err = store.UpdateFactEmbedding(ctx, factID, emb)
	if err != nil {
		t.Fatalf("UpdateFactEmbedding failed: %v", err)
	}
}

func TestSQLStoreDelegationAndQueries(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. Conversation mapping & lookup
	if err := store.SaveConversationMapping(ctx, "ext-thread-99", "internal-sess-99"); err != nil {
		t.Fatalf("SaveConversationMapping failed: %v", err)
	}
	extID, err := store.GetExternalConversationID(ctx, "internal-sess-99")
	if err != nil || extID != "ext-thread-99" {
		t.Fatalf("GetExternalConversationID returned (%q, %v), want ext-thread-99", extID, err)
	}

	// 2. Active tasks with pending message
	err = store.InsertMessage(ctx, Message{
		ID:        "m-sqlstore-task",
		ThreadID:  "ext-thread-99",
		Content:   "SQLStore task content",
		Summary:   "SQLStore task summary",
		Status:    StatusPending,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}
	tasks, err := store.GetActiveTasks(ctx)
	if err != nil {
		t.Fatalf("SQLStore.GetActiveTasks failed: %v", err)
	}
	var foundTask bool
	for _, task := range tasks {
		if task.ID == "m-sqlstore-task" {
			foundTask = true
			if task.Summary != "SQLStore task summary" {
				t.Errorf("expected summary 'SQLStore task summary', got %q", task.Summary)
			}
		}
	}
	if !foundTask {
		t.Errorf("expected to find m-sqlstore-task in active tasks")
	}

	// 3. ClaimNextPendingMessage when non-pending vs pending
	emptyStore := NewSQLStore(setupTestDB(t))
	_ = emptyStore.InsertMessage(ctx, Message{ID: "m-comp-only", ThreadID: "t1", Status: StatusCompleted})
	claimedEmpty, err := emptyStore.ClaimNextPendingMessage(ctx, "w1")
	if err != nil || claimedEmpty != nil {
		t.Errorf("expected nil, nil when no pending messages exist, got (%v, %v)", claimedEmpty, err)
	}

	claimed, err := store.ClaimNextPendingMessage(ctx, "w1")
	if err != nil || claimed == nil || claimed.ID != "m-sqlstore-task" {
		t.Fatalf("expected claim for m-sqlstore-task, got (%v, %v)", claimed, err)
	}

	// 4. GetPendingOrProcessingMessages truncation and closed DB error
	_ = store.InsertMessage(ctx, Message{ID: "m-proc-1", ThreadID: "t1", Status: StatusProcessing})
	_ = store.InsertMessage(ctx, Message{ID: "m-proc-2", ThreadID: "t1", Status: StatusProcessing})
	msgs, err := store.GetPendingOrProcessingMessages(ctx, 1)
	if err != nil {
		t.Fatalf("GetPendingOrProcessingMessages failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected limit truncation to 1, got %d", len(msgs))
	}

	tmpPath := filepath.Join(t.TempDir(), "closed-delegation.db")
	closedDB, err := initTestSQLite(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	closedStore := NewSQLStore(closedDB)
	_ = closedDB.Close()
	if _, err := closedStore.GetPendingOrProcessingMessages(ctx, 10); err == nil {
		t.Errorf("expected error from closedStore.GetPendingOrProcessingMessages, got nil")
	}
	if _, err := closedStore.ClaimNextPendingMessage(ctx, "w1"); err == nil {
		t.Errorf("expected error from closedStore.ClaimNextPendingMessage, got nil")
	}

	// 5. UpdateCronScheduleEffort validation and success
	if err := store.UpdateCronScheduleEffort(ctx, "", "high"); err != nil {
		t.Errorf("expected nil error for empty id in store.UpdateCronScheduleEffort, got %v", err)
	}
	if err := store.UpdateCronScheduleEffort(ctx, "cs1", "bad-effort"); err != nil {
		t.Errorf("expected nil error for bad effort in store.UpdateCronScheduleEffort, got %v", err)
	}
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

	// 6. Comprehensive pass-through coverage for remaining SQLStore methods
	_, _ = store.GetAllFactsWithEmbeddings(ctx)
	_, _ = store.GetFactsByThreadWithEmbeddings(ctx, "ext-thread-99")
	_, _ = store.GetActiveConversationsForExtraction(ctx, 24)
	_ = store.UpdateConversationFactWatermark(ctx, "ext-thread-99", 10)
	_ = store.UpdateConversationFactExtractedAt(ctx, "ext-thread-99")
	_, _ = store.GetAllOneShotSchedules(ctx, "ext-thread-99")
	_, _ = store.GetAllCronSchedules(ctx, "chan-1")
	_ = store.DeleteOneShotSchedule(ctx, "nonexistent")
	_ = store.DeleteCronSchedule(ctx, "nonexistent")
	_ = store.UpdateCronNextRun(ctx, "cs-effort-1", time.Now().Add(2*time.Hour))
	_, _, _ = store.GetScheduleRunsPaginated(ctx, 10, 0, "cs1", "success")
	_, _ = store.GetScheduleSummaryMetrics(ctx)
	_, _ = store.ReconcileOrphanedScheduleRuns(ctx)
	_, _ = store.PruneScheduleRuns(ctx, 100, 24*time.Hour)
	_ = store.UpdateMessageStatus(ctx, "m-sqlstore-task", StatusProcessing, "w1")
	_ = store.UpdateMessageCompleted(ctx, "m-sqlstore-task", "done")
	_ = store.IncrementMessageRetry(ctx, "m-sqlstore-task", "err")
	_ = store.IncrementMessageRestart(ctx, "m-sqlstore-task", "err")
	_ = store.ResetMessageToPendingWithRestart(ctx, "m-sqlstore-task", "reason")
	_, _ = store.GetMessage(ctx, "m-sqlstore-task")
	_, _ = store.MessageExists(ctx, "m-sqlstore-task")
	_, _ = store.GetActiveRecentThreadIDs(ctx, time.Hour)
	_, _ = store.GetRecentThreadMessages(ctx, "ext-thread-99", 10)
	_, _ = store.GetMaxMessageRowID(ctx, "ext-thread-99")
	_, _ = store.GetSessionID(ctx, "ext-thread-99")
	_ = store.SaveSessionID(ctx, "ext-thread-99", "sess-1")
	_, _ = store.IncrementSessionTurnCount(ctx, "ext-thread-99")
	_, _ = store.GetSessionTurnCount(ctx, "ext-thread-99")
	_ = store.RotateSessionID(ctx, "ext-thread-99", "sess-2")
	_, _ = store.GetPreviousSessionID(ctx, "ext-thread-99")
	_ = store.DeleteSessionID(ctx, "ext-thread-99")
}
