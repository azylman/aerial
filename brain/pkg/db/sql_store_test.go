package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLStoreIsDBTXNilAndClose(t *testing.T) {
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

	// 5. SQLStore.Close() branches
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

	ctx := context.Background()
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

	if _, err := s.InsertFact(ctx, "c", "f", 1.0, nil); err == nil {
		t.Errorf("expected error on InsertFact nil store")
	}
	if _, err := s.SearchSimilarFacts(ctx, nil, "", 10, 0.1); err == nil {
		t.Errorf("expected error on SearchSimilarFacts nil store")
	}
	if _, err := s.GetFactsPaginated(ctx, FactsFilter{}); err == nil {
		t.Errorf("expected error on GetFactsPaginated nil store")
	}
	if _, err := s.GetActiveConversationsForExtraction(ctx, 10); err == nil {
		t.Errorf("expected error on GetActiveConversationsForExtraction nil store")
	}
	if _, err := s.GetUnextractedAdminTurns(ctx, "t"); err == nil {
		t.Errorf("expected error on GetUnextractedAdminTurns nil store")
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
	if err := s.InsertMessageAndConsumeOneShot(ctx, "id", Message{}); err == nil {
		t.Errorf("expected error on InsertMessageAndConsumeOneShot nil store")
	}
	if _, err := s.GetAllOneShotSchedules(ctx, "t"); err == nil {
		t.Errorf("expected error on GetAllOneShotSchedules nil store")
	}
	if _, err := s.GetDueCronSchedules(ctx); err == nil {
		t.Errorf("expected error on GetDueCronSchedules nil store")
	}
	if _, err := s.GetAllCronSchedules(ctx, "t"); err == nil {
		t.Errorf("expected error on GetAllCronSchedules nil store")
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
	if _, err := s.GetActiveTasks(ctx); err == nil {
		t.Errorf("expected error on GetActiveTasks nil store")
	}
	if _, err := s.GetExternalConversationID(ctx, "test"); err == nil {
		t.Errorf("expected error on GetExternalConversationID nil store")
	}
	if _, err := s.FindUnrotatedSessions(ctx, 8); err == nil {
		t.Errorf("expected error on FindUnrotatedSessions nil store")
	}

	storeWithNilDB := &SQLStore{db: nil}
	if _, err := storeWithNilDB.GetActiveTasks(ctx); err == nil {
		t.Errorf("expected error calling GetActiveTasks with nil db")
	}
	if _, err := storeWithNilDB.GetExternalConversationID(ctx, "test"); err == nil {
		t.Errorf("expected error calling GetExternalConversationID with nil db")
	}
	if _, err := storeWithNilDB.SearchSimilarFacts(ctx, nil, "", 10, 0.1); err == nil {
		t.Errorf("expected error on SearchSimilarFacts storeWithNilDB")
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
	if err := store.SaveSessionID(ctx, "ext-thread-99", "internal-sess-99"); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
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

	// 3. GetPendingOrProcessingMessages truncation and closed DB error
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
}
