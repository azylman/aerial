package db

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestWithTxCommit(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	err := store.WithTx(ctx, func(txStore Store) error {
		_, err := txStore.InsertFact(ctx, "test", "Transactional fact", 1.0, "thread-tx-1", nil)
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
		_, err := txStore.InsertFact(ctx, "test", "Uncommitted fact", 1.0, "thread-tx-2", nil)
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
	var tx *sql.Tx = nil
	store := NewTxStore(tx, false)
	if store != nil {
		t.Fatalf("Expected NewTxStore to return nil for typed nil *sql.Tx, got %v", store)
	}

	var nilStore *SQLStore = nil
	ctx := context.Background()
	_, err := nilStore.InsertFact(ctx, "cat", "text", 1.0, "th", nil)
	if err == nil {
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

	if _, err := s.InsertFact(ctx, "c", "f", 1.0, "t", nil); err == nil {
		t.Errorf("expected error on InsertFact nil store")
	}
	if _, err := s.SearchSimilarFacts(ctx, nil, 10, 0.1, "t"); err == nil {
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

	// 3. Facts missing embeddings & UpdateFactEmbedding
	factID, err := store.InsertFact(ctx, "test", "Fact without embedding", 1.0, "thread-test-1", nil)
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
