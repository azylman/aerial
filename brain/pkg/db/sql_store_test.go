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
}
