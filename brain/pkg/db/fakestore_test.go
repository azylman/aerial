package db

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestFakeStoreLifecycleAndFailNext(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	// Initial check
	if err := s.InsertMessage(ctx, Message{ID: "m1", ThreadID: "t1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test FailNext
	testErr := errors.New("simulated error")
	s.FailNext("GetMessage", testErr)

	if _, err := s.GetMessage(ctx, "m1"); !errors.Is(err, testErr) {
		t.Fatalf("expected simulated error %v, got %v", testErr, err)
	}
	// FailNext should be consumed
	if _, err := s.GetMessage(ctx, "m1"); err != nil {
		t.Fatalf("expected success after consumed FailNext, got %v", err)
	}

	// Close
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Subsequent calls return sql.ErrConnDone
	if err := s.InsertMessage(ctx, Message{ID: "m2"}); !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("expected sql.ErrConnDone, got %v", err)
	}
	if _, err := s.GetMessage(ctx, "m1"); !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("expected sql.ErrConnDone, got %v", err)
	}
	if err := s.WithTx(ctx, func(tx Store) error { return nil }); !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("expected sql.ErrConnDone on WithTx, got %v", err)
	}
}

func TestFakeStoreWithTx(t *testing.T) {
	ctx := context.Background()

	t.Run("commit mutates parent store", func(t *testing.T) {
		s := NewFakeStore()
		err := s.WithTx(ctx, func(tx Store) error {
			if err := tx.InsertMessage(ctx, Message{ID: "msg-1", ThreadID: "th-1"}); err != nil {
				return err
			}
			if err := tx.SaveSessionID(ctx, "th-1", "sess-1"); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatalf("WithTx commit failed: %v", err)
		}

		// Verify state was applied
		msg, err := s.GetMessage(ctx, "msg-1")
		if err != nil || msg == nil {
			t.Fatalf("expected msg-1 in store, err: %v", err)
		}
		sessID, err := s.GetSessionID(ctx, "th-1")
		if err != nil || sessID != "sess-1" {
			t.Fatalf("expected sess-1, got %q (err: %v)", sessID, err)
		}
	})

	t.Run("rollback discards changes", func(t *testing.T) {
		s := NewFakeStore()
		simulatedErr := errors.New("rollback please")
		err := s.WithTx(ctx, func(tx Store) error {
			_ = tx.InsertMessage(ctx, Message{ID: "msg-rollback", ThreadID: "th-1"})
			_ = tx.SaveSessionID(ctx, "th-1", "sess-rollback")
			return simulatedErr
		})
		if !errors.Is(err, simulatedErr) {
			t.Fatalf("expected %v, got %v", simulatedErr, err)
		}

		// Verify changes discarded
		if exists, _ := s.MessageExists(ctx, "msg-rollback"); exists {
			t.Errorf("expected msg-rollback to NOT exist")
		}
		if sessID, _ := s.GetSessionID(ctx, "th-1"); sessID != "" {
			t.Errorf("expected empty session ID, got %q", sessID)
		}
	})

	t.Run("commit fails if store closed concurrently", func(t *testing.T) {
		s := NewFakeStore()
		err := s.WithTx(ctx, func(tx Store) error {
			_ = s.Close()
			return nil
		})
		if !errors.Is(err, sql.ErrConnDone) {
			t.Fatalf("expected sql.ErrConnDone, got %v", err)
		}
	})
}

func TestFakeStoreMessageOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	// InsertMessage & Idempotency
	m1 := Message{ID: "msg-1", ThreadID: "th-1", Content: "Hello"}
	if err := s.InsertMessage(ctx, m1); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}
	// Idempotent re-insert
	if err := s.InsertMessage(ctx, m1); err != nil {
		t.Fatalf("Idempotent InsertMessage failed: %v", err)
	}

	m2 := Message{ID: "msg-2", ThreadID: "th-1", Content: "World"}
	if err := s.InsertMessage(ctx, m2); err != nil {
		t.Fatalf("InsertMessage msg-2 failed: %v", err)
	}

	// MessageExists
	exists, err := s.MessageExists(ctx, "msg-1")
	if err != nil || !exists {
		t.Fatalf("expected exists=true, got %v, err=%v", exists, err)
	}
	exists, err = s.MessageExists(ctx, "msg-none")
	if err != nil || exists {
		t.Fatalf("expected exists=false, got %v, err=%v", exists, err)
	}

	// GetMessage
	msg, err := s.GetMessage(ctx, "msg-1")
	if err != nil || msg.Content != "Hello" {
		t.Fatalf("unexpected msg: %+v, err: %v", msg, err)
	}
	_, err = s.GetMessage(ctx, "msg-none")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}

	// UpdateMessageStatus
	if err := s.UpdateMessageStatus(ctx, "msg-1", StatusProcessing, "busy"); err != nil {
		t.Fatalf("UpdateMessageStatus failed: %v", err)
	}
	// Non-existent message update is no-op
	if err := s.UpdateMessageStatus(ctx, "msg-none", StatusProcessing, ""); err != nil {
		t.Fatalf("UpdateMessageStatus non-existent failed: %v", err)
	}

	// IncrementMessageRetry & IncrementMessageRestart
	if err := s.IncrementMessageRetry(ctx, "msg-1", "timeout"); err != nil {
		t.Fatalf("IncrementMessageRetry failed: %v", err)
	}
	if err := s.IncrementMessageRestart(ctx, "msg-1", "sigterm"); err != nil {
		t.Fatalf("IncrementMessageRestart failed: %v", err)
	}
	_ = s.IncrementMessageRetry(ctx, "msg-none", "")
	_ = s.IncrementMessageRestart(ctx, "msg-none", "")

	msg, _ = s.GetMessage(ctx, "msg-1")
	if msg.RetryCount != 1 || msg.RestartCount != 1 {
		t.Fatalf("expected retry=1 restart=1, got %+v", msg)
	}

	// ResetMessageToPendingWithRestart
	if err := s.ResetMessageToPendingWithRestart(ctx, "msg-1", "recovered"); err != nil {
		t.Fatalf("ResetMessageToPendingWithRestart failed: %v", err)
	}
	_ = s.ResetMessageToPendingWithRestart(ctx, "msg-none", "")
	msg, _ = s.GetMessage(ctx, "msg-1")
	if msg.Status != StatusPending || msg.RestartCount != 2 {
		t.Fatalf("expected status pending, restart=2, got %+v", msg)
	}

	// ClaimPendingMessage
	claimed, err := s.ClaimPendingMessage(ctx, "msg-1")
	if err != nil || !claimed {
		t.Fatalf("expected claim=true, got %v (err: %v)", claimed, err)
	}
	// Already claimed
	claimed, err = s.ClaimPendingMessage(ctx, "msg-1")
	if err != nil || claimed {
		t.Fatalf("expected claim=false for already processing, got %v", claimed)
	}
	// Non-existent claim
	claimed, err = s.ClaimPendingMessage(ctx, "msg-none")
	if err != nil || claimed {
		t.Fatalf("expected claim=false for msg-none, got %v", claimed)
	}

	// ClaimNextPendingMessage
	// msg-2 is still pending
	claimedMsg, err := s.ClaimNextPendingMessage(ctx, "w1")
	if err != nil || claimedMsg == nil || claimedMsg.ID != "msg-2" {
		t.Fatalf("expected claim msg-2, got %+v (err: %v)", claimedMsg, err)
	}
	// No more pending
	claimedMsg, err = s.ClaimNextPendingMessage(ctx, "w1")
	if err != nil || claimedMsg != nil {
		t.Fatalf("expected no pending message, got %+v", claimedMsg)
	}

	// UpdateMessageCompleted
	if err := s.UpdateMessageCompleted(ctx, "msg-1", "Done response"); err != nil {
		t.Fatalf("UpdateMessageCompleted failed: %v", err)
	}
	_ = s.UpdateMessageCompleted(ctx, "msg-none", "No-op")
	msg, _ = s.GetMessage(ctx, "msg-1")
	if msg.Status != StatusCompleted || msg.ResponseText != "Done response" {
		t.Fatalf("expected completed with Done response, got %+v", msg)
	}

	// GetPendingOrProcessingMessages
	pendingMsgs, err := s.GetPendingOrProcessingMessages(ctx, 10)
	if err != nil || len(pendingMsgs) != 1 || pendingMsgs[0].ID != "msg-2" {
		t.Fatalf("expected msg-2 in pending/processing, got %+v", pendingMsgs)
	}

	// GetMaxMessageRowID
	maxRowID, err := s.GetMaxMessageRowID(ctx, "th-1")
	if err != nil || maxRowID != 1 {
		t.Fatalf("expected maxRowID=1, got %d (err: %v)", maxRowID, err)
	}

	// GetActiveRecentThreadIDs & GetRecentThreadMessages
	threadIDs, err := s.GetActiveRecentThreadIDs(ctx, time.Hour)
	if err != nil || len(threadIDs) != 1 || threadIDs[0] != "th-1" {
		t.Fatalf("expected [th-1], got %v (err: %v)", threadIDs, err)
	}

	recentMsgs, err := s.GetRecentThreadMessages(ctx, "th-1", 10)
	if err != nil || len(recentMsgs) != 2 {
		t.Fatalf("expected 2 recent messages, got %d (err: %v)", len(recentMsgs), err)
	}
	if recentMsgs[0].ID != "msg-1" || recentMsgs[1].ID != "msg-2" {
		t.Fatalf("expected chronological order msg-1 then msg-2, got %+v", recentMsgs)
	}
}

func TestFakeStoreSessionOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	// Initial session ID should be empty
	id, err := s.GetSessionID(ctx, "th-1")
	if err != nil || id != "" {
		t.Fatalf("expected empty session ID, got %q, err: %v", id, err)
	}

	// SaveSessionID
	if err := s.SaveSessionID(ctx, "th-1", "sess-1"); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}
	id, err = s.GetSessionID(ctx, "th-1")
	if err != nil || id != "sess-1" {
		t.Fatalf("expected sess-1, got %q, err: %v", id, err)
	}

	// Update session ID
	if err := s.SaveSessionID(ctx, "th-1", "sess-1-v2"); err != nil {
		t.Fatalf("SaveSessionID update failed: %v", err)
	}
	id, _ = s.GetSessionID(ctx, "th-1")
	if id != "sess-1-v2" {
		t.Fatalf("expected sess-1-v2, got %q", id)
	}

	// IncrementSessionTurnCount & GetSessionTurnCount
	count, err := s.IncrementSessionTurnCount(ctx, "th-1")
	if err != nil || count != 1 {
		t.Fatalf("expected count=1, got %d (err: %v)", count, err)
	}
	count, err = s.IncrementSessionTurnCount(ctx, "th-1")
	if err != nil || count != 2 {
		t.Fatalf("expected count=2, got %d (err: %v)", count, err)
	}
	count, err = s.GetSessionTurnCount(ctx, "th-1")
	if err != nil || count != 2 {
		t.Fatalf("expected count=2, got %d (err: %v)", count, err)
	}

	// Increment on non-existent session
	count, err = s.IncrementSessionTurnCount(ctx, "th-new")
	if err != nil || count != 1 {
		t.Fatalf("expected count=1 on new session, got %d (err: %v)", count, err)
	}
	if c, _ := s.GetSessionTurnCount(ctx, "th-nonexistent"); c != 0 {
		t.Fatalf("expected count=0 on non-existent session, got %d", c)
	}

	// RotateSessionID
	if err := s.RotateSessionID(ctx, "th-1", "sess-2"); err != nil {
		t.Fatalf("RotateSessionID failed: %v", err)
	}
	currID, _ := s.GetSessionID(ctx, "th-1")
	prevID, err := s.GetPreviousSessionID(ctx, "th-1")
	if err != nil || currID != "sess-2" || prevID != "sess-1-v2" {
		t.Fatalf("expected curr=sess-2 prev=sess-1-v2, got curr=%q prev=%q", currID, prevID)
	}
	// Turn count should reset to 0
	count, _ = s.GetSessionTurnCount(ctx, "th-1")
	if count != 0 {
		t.Fatalf("expected turn count 0 after rotation, got %d", count)
	}

	// Rotate on non-existent session
	if err := s.RotateSessionID(ctx, "th-brand-new", "sess-init"); err != nil {
		t.Fatalf("RotateSessionID brand-new failed: %v", err)
	}

	// GetSessionInfo
	info, err := s.GetSessionInfo(ctx, "th-1")
	if err != nil || info == nil || info.InternalSessionID != "sess-2" {
		t.Fatalf("unexpected SessionInfo: %+v, err: %v", info, err)
	}
	info, err = s.GetSessionInfo(ctx, "th-nonexistent")
	if err != nil || info != nil {
		t.Fatalf("expected nil for non-existent session, got %+v", info)
	}

	// Thread summary
	sum, wm, err := s.GetThreadSummary(ctx, "th-1")
	if err != nil || sum != "" || wm != "" {
		t.Fatalf("expected empty thread summary, got sum=%q wm=%q", sum, wm)
	}
	if err := s.SaveThreadSummary(ctx, "th-1", "Summarized text", "msg-100"); err != nil {
		t.Fatalf("SaveThreadSummary failed: %v", err)
	}
	sum, wm, err = s.GetThreadSummary(ctx, "th-1")
	if err != nil || sum != "Summarized text" || wm != "msg-100" {
		t.Fatalf("expected summary='Summarized text' wm='msg-100', got %q, %q", sum, wm)
	}

	// Session activity stats
	_ = s.InsertMessage(ctx, Message{ID: "m-active-1", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "Valid response"})
	_ = s.InsertMessage(ctx, Message{ID: "m-active-2", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[EXPIRED_STALE] ignored"})
	_ = s.InsertMessage(ctx, Message{ID: "m-active-3", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[AMBIENT] ignored"})
	_ = s.InsertMessage(ctx, Message{ID: "m-active-4", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[IGNORED] ignored"})
	_ = s.InsertMessage(ctx, Message{ID: "m-active-5", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "Second valid response"})

	stats, err := s.GetSessionActivityStats(ctx, "th-1")
	if err != nil {
		t.Fatalf("GetSessionActivityStats failed: %v", err)
	}
	if stats.InternalSessionID != "sess-2" {
		t.Errorf("expected InternalSessionID sess-2, got %q", stats.InternalSessionID)
	}
	if stats.CompletedTurns != 2 {
		t.Errorf("expected 2 completed turns (excluding filtered prefixes), got %d", stats.CompletedTurns)
	}
	if stats.LastMessageAt.IsZero() {
		t.Errorf("expected non-zero LastMessageAt")
	}

	// DeleteSessionID
	if err := s.DeleteSessionID(ctx, "th-1"); err != nil {
		t.Fatalf("DeleteSessionID failed: %v", err)
	}
	id, _ = s.GetSessionID(ctx, "th-1")
	if id != "" {
		t.Fatalf("expected empty session ID after delete, got %q", id)
	}
	if prev, _ := s.GetPreviousSessionID(ctx, "th-none"); prev != "" {
		t.Fatalf("expected empty previous session ID for th-none, got %q", prev)
	}
}

func TestFakeStoreScheduleOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	// OneShot schedules
	s1 := OneShotSchedule{ID: "os-1", ThreadID: "th-1", Prompt: "P1", RunAt: now.Add(-time.Minute)}
	s2 := OneShotSchedule{ID: "os-2", ThreadID: "th-1", Prompt: "P2", RunAt: now.Add(time.Hour)}
	if err := s.CreateOneShotSchedule(ctx, s1); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}
	if err := s.CreateOneShotSchedule(ctx, s2); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}

	// GetDueOneShotSchedules
	due, err := s.GetDueOneShotSchedules(ctx)
	if err != nil || len(due) != 1 || due[0].ID != "os-1" {
		t.Fatalf("expected due [os-1], got %+v (err: %v)", due, err)
	}

	// GetAllOneShotSchedules
	all, err := s.GetAllOneShotSchedules(ctx, "th-1")
	if err != nil || len(all) != 2 {
		t.Fatalf("expected 2 schedules, got %d (err: %v)", len(all), err)
	}
	allGlobal, err := s.GetAllOneShotSchedules(ctx, "")
	if err != nil || len(allGlobal) != 2 {
		t.Fatalf("expected 2 schedules globally, got %d", len(allGlobal))
	}

	// InsertMessageAndConsumeOneShot
	msg := Message{ID: "msg-os-1", ThreadID: "th-1", Content: "Executed"}
	if err := s.InsertMessageAndConsumeOneShot(ctx, "os-1", msg); err != nil {
		t.Fatalf("InsertMessageAndConsumeOneShot failed: %v", err)
	}
	// Second consume should return ErrScheduleAlreadyConsumed
	err = s.InsertMessageAndConsumeOneShot(ctx, "os-1", msg)
	if !errors.Is(err, ErrScheduleAlreadyConsumed) {
		t.Fatalf("expected ErrScheduleAlreadyConsumed, got %v", err)
	}

	// DeleteOneShotSchedule
	if err := s.DeleteOneShotSchedule(ctx, "os-2"); err != nil {
		t.Fatalf("DeleteOneShotSchedule failed: %v", err)
	}
	all, _ = s.GetAllOneShotSchedules(ctx, "th-1")
	if len(all) != 0 {
		t.Fatalf("expected 0 schedules after deletion, got %d", len(all))
	}

	// Cron schedules
	c1 := CronSchedule{ID: "cron-1", TargetID: "t-1", CronExpr: "*/5 * * * *", Enabled: true, NextRunAt: now.Add(-time.Minute)}
	c2 := CronSchedule{ID: "cron-2", TargetID: "t-1", CronExpr: "0 * * * *", Enabled: false, NextRunAt: now.Add(-time.Minute)}
	c3 := CronSchedule{ID: "cron-3", TargetID: "t-2", CronExpr: "0 0 * * *", Enabled: true, NextRunAt: now.Add(time.Hour)}
	_ = s.CreateCronSchedule(ctx, c1)
	_ = s.CreateCronSchedule(ctx, c2)
	_ = s.CreateCronSchedule(ctx, c3)

	dueCron, err := s.GetDueCronSchedules(ctx)
	if err != nil || len(dueCron) != 1 || dueCron[0].ID != "cron-1" {
		t.Fatalf("expected due cron-1 only (c2 is disabled), got %+v", dueCron)
	}

	allCrons, err := s.GetAllCronSchedules(ctx, "t-1")
	if err != nil || len(allCrons) != 2 {
		t.Fatalf("expected 2 crons for target t-1, got %d", len(allCrons))
	}
	allCronsGlobal, err := s.GetAllCronSchedules(ctx, "")
	if err != nil || len(allCronsGlobal) != 3 {
		t.Fatalf("expected 3 crons globally, got %d", len(allCronsGlobal))
	}

	// UpdateCronNextRun & UpdateCronScheduleEffort
	next := now.Add(10 * time.Minute)
	if err := s.UpdateCronNextRun(ctx, "cron-1", next); err != nil {
		t.Fatalf("UpdateCronNextRun failed: %v", err)
	}
	if err := s.UpdateCronScheduleEffort(ctx, "cron-1", "high"); err != nil {
		t.Fatalf("UpdateCronScheduleEffort failed: %v", err)
	}
	_ = s.UpdateCronNextRun(ctx, "cron-none", next)
	_ = s.UpdateCronScheduleEffort(ctx, "cron-none", "low")

	// DeleteCronSchedule
	if err := s.DeleteCronSchedule(ctx, "cron-2"); err != nil {
		t.Fatalf("DeleteCronSchedule failed: %v", err)
	}

	// Schedule Runs
	r1 := ScheduleRun{ID: "run-1", ScheduleID: "cron-1", ScheduleType: "cron", Status: "enqueued", StartedAt: now.Add(-2 * time.Hour)}
	r2 := ScheduleRun{ID: "run-2", ScheduleID: "cron-1", ScheduleType: "cron", Status: "completed", StartedAt: now.Add(-1 * time.Hour), DurationMs: 500}
	r3 := ScheduleRun{ID: "run-3", ScheduleID: "cron-3", ScheduleType: "cron", Status: "running", StartedAt: now.Add(-30 * time.Minute)}
	_ = s.CreateScheduleRun(ctx, r1)
	_ = s.CreateScheduleRun(ctx, r2)
	_ = s.CreateScheduleRun(ctx, r3)

	// UpdateScheduleRunStatus
	completedAt := now
	err = s.UpdateScheduleRunStatus(ctx, UpdateRunParams{
		RunID:       "run-1",
		Status:      "completed",
		MessageID:   "msg-run-1",
		DurationMs:  1200,
		CompletedAt: completedAt,
		Model:       "claude",
	})
	if err != nil {
		t.Fatalf("UpdateScheduleRunStatus failed: %v", err)
	}
	_ = s.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: "run-none"})

	// GetScheduleRunsPaginated
	runs, total, err := s.GetScheduleRunsPaginated(ctx, 10, 0, "cron-1", "")
	if err != nil || total != 2 || len(runs) != 2 {
		t.Fatalf("expected 2 runs for cron-1, got len=%d total=%d (err: %v)", len(runs), total, err)
	}
	runs, _, _ = s.GetScheduleRunsPaginated(ctx, 1, 0, "", "completed")
	if len(runs) != 1 {
		t.Fatalf("expected 1 completed run with limit 1, got %d", len(runs))
	}
	runs, _, _ = s.GetScheduleRunsPaginated(ctx, 10, 50, "", "")
	if runs != nil {
		t.Fatalf("expected nil runs for large offset, got %+v", runs)
	}

	// GetScheduleSummaryMetrics
	metrics, err := s.GetScheduleSummaryMetrics(ctx)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics failed: %v", err)
	}
	if metrics.CronCount != 2 || metrics.OneShotCount != 0 || metrics.TotalRuns24h != 3 {
		t.Errorf("unexpected metrics: %+v", metrics)
	}

	// ReconcileOrphanedScheduleRuns
	reconciled, err := s.ReconcileOrphanedScheduleRuns(ctx)
	if err != nil || reconciled != 1 {
		t.Fatalf("expected 1 reconciled run (run-3 was running), got %d (err: %v)", reconciled, err)
	}

	// PruneScheduleRuns
	pruned, err := s.PruneScheduleRuns(ctx, 1, 48*time.Hour)
	if err != nil || pruned != 2 {
		t.Fatalf("expected 2 runs pruned down to maxCount=1, got %d (err: %v)", pruned, err)
	}
}

func TestFakeStoreFactOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	emb1 := []float32{1.0, 0.0, 0.0}
	emb2 := []float32{0.9, 0.1, 0.0}
	emb3 := []float32{0.0, 1.0, 0.0}

	id1, err := s.InsertFact(ctx, "personal", "Likes matcha latte", 0.9, "th-facts", emb1)
	if err != nil || id1 != 1 {
		t.Fatalf("InsertFact 1 failed: %v, id=%d", err, id1)
	}
	id2, err := s.InsertFact(ctx, "tech", "Gundam pilot", 0.8, "th-facts", emb2)
	if err != nil || id2 != 2 {
		t.Fatalf("InsertFact 2 failed: %v, id=%d", err, id2)
	}
	id3, err := s.InsertFact(ctx, "tech", "Missing embedding fact", 0.5, "th-facts", nil)
	if err != nil || id3 != 3 {
		t.Fatalf("InsertFact 3 failed: %v, id=%d", err, id3)
	}

	// SearchSimilarFacts
	matches, err := s.SearchSimilarFacts(ctx, emb1, 5, 0.8, "th-facts")
	if err != nil || len(matches) != 2 {
		t.Fatalf("expected 2 similar matches, got %d (err: %v)", len(matches), err)
	}
	if matches[0].ID != id1 {
		t.Errorf("expected highest score match to be id1, got id=%d", matches[0].ID)
	}

	// FindDuplicateFact
	dup, sim, err := s.FindDuplicateFact(ctx, emb1, 0.85)
	if err != nil || dup == nil || dup.ID != id1 {
		t.Fatalf("expected dup id1 with high sim, got dup=%+v sim=%f (err: %v)", dup, sim, err)
	}
	// No dup if minSim too high
	dupNone, _, _ := s.FindDuplicateFact(ctx, emb3, 0.99)
	if dupNone != nil {
		t.Fatalf("expected no dup for orthogonal vector, got %+v", dupNone)
	}

	// GetFactsPaginated
	res, err := s.GetFactsPaginated(ctx, FactsFilter{Category: "tech", Limit: 10})
	if err != nil || res.Total != 2 || len(res.Facts) != 2 {
		t.Fatalf("expected 2 tech facts, got total=%d len=%d (err: %v)", res.Total, len(res.Facts), err)
	}
	resQuery, _ := s.GetFactsPaginated(ctx, FactsFilter{Query: "matcha"})
	if len(resQuery.Facts) != 1 || resQuery.Facts[0].ID != id1 {
		t.Fatalf("expected matcha fact, got %+v", resQuery)
	}
	resOffset, _ := s.GetFactsPaginated(ctx, FactsFilter{Offset: 50})
	if resOffset.Facts != nil {
		t.Fatalf("expected nil facts for offset beyond total, got %+v", resOffset)
	}

	// GetFactsByThreadWithEmbeddings
	threadFacts, err := s.GetFactsByThreadWithEmbeddings(ctx, "th-facts")
	if err != nil || len(threadFacts) != 3 {
		t.Fatalf("expected 3 thread facts, got %d (err: %v)", len(threadFacts), err)
	}
	globalFacts, _ := s.GetFactsByThreadWithEmbeddings(ctx, "")
	if len(globalFacts) != 3 {
		t.Fatalf("expected 3 global facts, got %d", len(globalFacts))
	}

	// GetFactsMissingEmbeddings
	missing, err := s.GetFactsMissingEmbeddings(ctx, 10)
	if err != nil || len(missing) != 1 || missing[0].ID != id3 {
		t.Fatalf("expected missing id3, got %+v (err: %v)", missing, err)
	}

	// UpdateFactEmbedding
	if err := s.UpdateFactEmbedding(ctx, id3, emb3); err != nil {
		t.Fatalf("UpdateFactEmbedding failed: %v", err)
	}
	if err := s.UpdateFactEmbedding(ctx, 9999, emb3); !errors.Is(err, ErrFactNotFound) {
		t.Fatalf("expected ErrFactNotFound on non-existent fact, got %v", err)
	}
	missingAfter, _ := s.GetFactsMissingEmbeddings(ctx, 10)
	if len(missingAfter) != 0 {
		t.Fatalf("expected 0 missing embeddings after update, got %d", len(missingAfter))
	}

	// ReinforceFact
	if err := s.ReinforceFact(ctx, id1, "Likes iced matcha latte", emb1, 0.1); err != nil {
		t.Fatalf("ReinforceFact failed: %v", err)
	}
	if err := s.ReinforceFact(ctx, 9999, "", nil, 0.1); !errors.Is(err, ErrFactNotFound) {
		t.Fatalf("expected ErrFactNotFound on reinforce non-existent, got %v", err)
	}
	rechecked, _ := s.GetFactsByThreadWithEmbeddings(ctx, "th-facts")
	if rechecked[0].Fact.FactText != "Likes iced matcha latte" || rechecked[0].Fact.Importance != 1.0 {
		t.Errorf("unexpected reinforced fact: %+v", rechecked[0])
	}

	// DecayAndPruneFacts
	decayed, pruned, err := s.DecayAndPruneFacts(ctx, 0.2, 0.6, 0)
	if err != nil {
		t.Fatalf("DecayAndPruneFacts failed: %v", err)
	}
	if decayed != 3 {
		t.Errorf("expected 3 decayed facts, got %d", decayed)
	}
	if pruned == 0 {
		t.Errorf("expected at least 1 pruned fact (id3 dropped below 0.6), got %d", pruned)
	}

	// Watermarks & ExtractedAt
	if err := s.UpdateConversationFactWatermark(ctx, "th-done", 42); err != nil {
		t.Fatalf("UpdateConversationFactWatermark failed: %v", err)
	}
	if err := s.UpdateConversationFactExtractedAt(ctx, "th-done"); err != nil {
		t.Fatalf("UpdateConversationFactExtractedAt failed: %v", err)
	}
	_ = s.InsertMessage(ctx, Message{ID: "m-extract-0", ThreadID: "th-done"})
	_ = s.InsertMessage(ctx, Message{ID: "m-extract-1", ThreadID: "th-facts"})
	_ = s.InsertMessage(ctx, Message{ID: "m-extract-2", ThreadID: "th-facts"})
	_ = s.InsertMessage(ctx, Message{ID: "m-extract-3", ThreadID: "th-other"})
	activeThreads, err := s.GetActiveConversationsForExtraction(ctx, 24)
	if err != nil {
		t.Fatalf("GetActiveConversationsForExtraction failed: %v", err)
	}
	if len(activeThreads) != 2 {
		t.Fatalf("expected 2 active threads, got %d (%v)", len(activeThreads), activeThreads)
	}
}

func TestFakeStoreCloneNil(t *testing.T) {
	if cloneMessage(nil) != nil {
		t.Errorf("expected nil message clone")
	}
	if cloneSessionInfo(nil) != nil {
		t.Errorf("expected nil session clone")
	}
	if cloneOneShot(nil) != nil {
		t.Errorf("expected nil one-shot clone")
	}
	if cloneCron(nil) != nil {
		t.Errorf("expected nil cron clone")
	}
	if cloneRun(nil) != nil {
		t.Errorf("expected nil run clone")
	}
	if cloneFactWithEmbedding(nil) != nil {
		t.Errorf("expected nil fact clone")
	}
}

func TestFakeStoreClosedCoverage(t *testing.T) {
	s := NewFakeStore()
	_ = s.Close()
	ctx := context.Background()

	_ = s.InsertMessage(ctx, Message{})
	_ = s.UpdateMessageStatus(ctx, "m", "s", "e")
	_ = s.UpdateMessageCompleted(ctx, "m", "r")
	_ = s.IncrementMessageRetry(ctx, "m", "e")
	_ = s.IncrementMessageRestart(ctx, "m", "e")
	_ = s.ResetMessageToPendingWithRestart(ctx, "m", "r")
	_, _ = s.GetPendingOrProcessingMessages(ctx, 10)
	_, _ = s.GetMessage(ctx, "m")
	_, _ = s.MessageExists(ctx, "m")
	_, _ = s.ClaimPendingMessage(ctx, "m")
	_, _ = s.ClaimNextPendingMessage(ctx, "w")
	_, _ = s.GetActiveRecentThreadIDs(ctx, time.Hour)
	_, _ = s.GetRecentThreadMessages(ctx, "t", 10)
	_, _ = s.GetMaxMessageRowID(ctx, "t")

	_, _ = s.GetSessionID(ctx, "t")
	_, _ = s.GetPreviousSessionID(ctx, "t")
	_ = s.SaveSessionID(ctx, "t", "s")
	_ = s.DeleteSessionID(ctx, "t")
	_, _ = s.IncrementSessionTurnCount(ctx, "t")
	_, _ = s.GetSessionTurnCount(ctx, "t")
	_ = s.RotateSessionID(ctx, "t", "s")
	_, _ = s.GetSessionInfo(ctx, "t")
	_, _, _ = s.GetThreadSummary(ctx, "t")
	_ = s.SaveThreadSummary(ctx, "t", "s", "w")
	_, _ = s.GetSessionActivityStats(ctx, "t")
	_, _ = s.GetActiveTasks(ctx)
	_, _ = s.GetExternalConversationID(ctx, "t")
	_ = s.SaveConversationMapping(ctx, "ext", "int")

	_ = s.CreateOneShotSchedule(ctx, OneShotSchedule{})
	_, _ = s.GetDueOneShotSchedules(ctx)
	_ = s.DeleteOneShotSchedule(ctx, "id")
	_ = s.InsertMessageAndConsumeOneShot(ctx, "id", Message{})
	_, _ = s.GetAllOneShotSchedules(ctx, "t")
	_ = s.CreateCronSchedule(ctx, CronSchedule{})
	_, _ = s.GetDueCronSchedules(ctx)
	_, _ = s.GetAllCronSchedules(ctx, "t")
	_ = s.DeleteCronSchedule(ctx, "id")
	_ = s.UpdateCronNextRun(ctx, "id", time.Now())
	_ = s.UpdateCronScheduleEffort(ctx, "id", "high")
	_ = s.CreateScheduleRun(ctx, ScheduleRun{})
	_ = s.UpdateScheduleRunStatus(ctx, UpdateRunParams{})
	_, _, _ = s.GetScheduleRunsPaginated(ctx, 10, 0, "", "")
	_, _ = s.GetScheduleRun("id")
	_, _ = s.GetScheduleSummaryMetrics(ctx)
	_, _ = s.ReconcileOrphanedScheduleRuns(ctx)
	_, _ = s.PruneScheduleRuns(ctx, 10, time.Hour)

	_, _ = s.InsertFact(ctx, "c", "f", 1.0, "t", nil)
	_, _ = s.SearchSimilarFacts(ctx, nil, 10, 0.5, "t")
	_, _ = s.GetFactsPaginated(ctx, FactsFilter{})
	_, _ = s.GetFactsByThreadWithEmbeddings(ctx, "t")
	_, _ = s.GetActiveConversationsForExtraction(ctx, 24)
	_ = s.UpdateConversationFactWatermark(ctx, "t", 1)
	_ = s.UpdateConversationFactExtractedAt(ctx, "t")
	_, _, _ = s.FindDuplicateFact(ctx, nil, 0.5)
	_ = s.ReinforceFact(ctx, 1, "f", nil, 0.1)
	_, _, _ = s.DecayAndPruneFacts(ctx, 0.1, 0.5, 30)
	_, _ = s.GetFactsMissingEmbeddings(ctx, 10)
	_ = s.UpdateFactEmbedding(ctx, 1, nil)
	_ = s.WithTx(ctx, func(tx Store) error { return nil })
}

func TestFakeStoreWithTxFullState(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	// Seed store with all entity types
	_ = s.InsertMessage(ctx, Message{ID: "m-tx-seed", ThreadID: "th-tx"})
	_ = s.SaveSessionID(ctx, "th-tx", "sess-tx")
	_ = s.SaveThreadSummary(ctx, "th-tx", "Sum", "wm")
	_ = s.CreateOneShotSchedule(ctx, OneShotSchedule{ID: "os-tx", ThreadID: "th-tx", RunAt: now})
	_ = s.CreateCronSchedule(ctx, CronSchedule{ID: "cron-tx", TargetID: "tgt-tx", Enabled: true, NextRunAt: now})
	_ = s.CreateScheduleRun(ctx, ScheduleRun{ID: "run-tx", ScheduleID: "cron-tx", Status: "running", StartedAt: now})
	_, _ = s.InsertFact(ctx, "cat", "Fact text", 0.9, "th-tx", []float32{1.0, 0.0})
	_ = s.UpdateConversationFactWatermark(ctx, "th-tx", 10)
	_ = s.UpdateConversationFactExtractedAt(ctx, "th-tx")

	err := s.WithTx(ctx, func(tx Store) error {
		_ = tx.InsertMessage(ctx, Message{ID: "m-tx-new", ThreadID: "th-tx"})
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx with full state failed: %v", err)
	}

	exists, _ := s.MessageExists(ctx, "m-tx-new")
	if !exists {
		t.Errorf("expected m-tx-new to exist after commit")
	}
}

func TestFakeStoreTestHelpers(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	// 1. SetSessionInfo
	s.SetSessionInfo(SessionInfo{
		ThreadID:          "th-helper",
		InternalSessionID: "sess-helper",
		TurnCount:         5,
		CreatedAt:         now.Add(-time.Hour),
		UpdatedAt:         now.Add(-10 * time.Minute),
	})
	sess, err := s.GetSessionInfo(ctx, "th-helper")
	if err != nil || sess == nil || sess.TurnCount != 5 || sess.InternalSessionID != "sess-helper" {
		t.Fatalf("SetSessionInfo failed: sess=%+v, err=%v", sess, err)
	}

	// 2. SetMessageUpdatedAt
	_ = s.InsertMessage(ctx, Message{ID: "m-helper", ThreadID: "th-helper", Status: StatusPending})
	customTime := now.Add(-30 * time.Minute)
	s.SetMessageUpdatedAt("m-helper", customTime)
	s.SetMessageUpdatedAt("non-existent", customTime) // no-op branch
	m, err := s.GetMessage(ctx, "m-helper")
	if err != nil || m == nil || !m.UpdatedAt.Equal(customTime) {
		t.Fatalf("SetMessageUpdatedAt failed: m=%+v, err=%v", m, err)
	}

	// 3. GetScheduleRun
	_, err = s.GetScheduleRun("non-existent-run")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for non-existent run, got: %v", err)
	}

	_ = s.CreateScheduleRun(ctx, ScheduleRun{
		ID:         "run-helper",
		ScheduleID: "sched-helper",
		Status:     "completed",
		StartedAt:  now,
	})
	run, err := s.GetScheduleRun("run-helper")
	if err != nil || run == nil || run.Status != "completed" {
		t.Fatalf("GetScheduleRun failed: run=%+v, err=%v", run, err)
	}
}

func TestFakeStoreActiveTasksAndConversationMapping(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	// 1. GetActiveTasks on empty store
	tasks, err := s.GetActiveTasks(ctx)
	if err != nil {
		t.Fatalf("GetActiveTasks on empty store failed: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(tasks))
	}

	// 2. Add sessions and messages
	_ = s.SaveSessionID(ctx, "th-1", "sess-1")
	_ = s.InsertMessage(ctx, Message{
		ID:        "m-1",
		ThreadID:  "th-1",
		Content:   "First task content",
		Summary:   "Custom summary",
		Status:    StatusPending,
		CreatedAt: now.Add(-10 * time.Minute),
	})
	_ = s.InsertMessage(ctx, Message{
		ID:        "m-2",
		ThreadID:  "th-1",
		Content:   "Second task content without summary",
		Status:    StatusProcessing,
		CreatedAt: now.Add(-5 * time.Minute),
	})
	_ = s.InsertMessage(ctx, Message{
		ID:        "m-completed",
		ThreadID:  "th-1",
		Content:   "Completed task",
		Status:    StatusCompleted,
		CreatedAt: now.Add(-time.Hour),
	})

	tasks, err = s.GetActiveTasks(ctx)
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 active tasks, got %d", len(tasks))
	}
	if tasks[0].ID != "m-1" || tasks[0].SessionID != "sess-1" || tasks[0].Summary != "Custom summary" {
		t.Errorf("unexpected task[0]: %+v", tasks[0])
	}
	if tasks[1].ID != "m-2" || tasks[1].Summary != "Second task content without summary" {
		t.Errorf("unexpected task[1]: %+v", tasks[1])
	}

	// Clamping past 50 active tasks
	for i := 3; i <= 60; i++ {
		_ = s.InsertMessage(ctx, Message{
			ID:        "m-bulk-" + string(rune(i)),
			ThreadID:  "th-1",
			Content:   "bulk task",
			Status:    StatusPending,
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		})
	}
	clampedTasks, err := s.GetActiveTasks(ctx)
	if err != nil {
		t.Fatalf("GetActiveTasks with >50 tasks failed: %v", err)
	}
	if len(clampedTasks) != 50 {
		t.Errorf("expected 50 clamped tasks, got %d", len(clampedTasks))
	}

	// 3. GetExternalConversationID and SaveConversationMapping
	if err := s.SaveConversationMapping(ctx, "ext-123", "internal-abc"); err != nil {
		t.Fatalf("SaveConversationMapping failed: %v", err)
	}
	extID, err := s.GetExternalConversationID(ctx, "internal-abc")
	if err != nil || extID != "ext-123" {
		t.Errorf("GetExternalConversationID = (%q, %v), want ext-123", extID, err)
	}

	// Empty internalID returns empty string without error
	emptyExt, err := s.GetExternalConversationID(ctx, "")
	if err != nil || emptyExt != "" {
		t.Errorf("GetExternalConversationID('') = (%q, %v), want empty", emptyExt, err)
	}

	// Non-existent internalID returns empty string
	notFoundExt, err := s.GetExternalConversationID(ctx, "nonexistent-internal")
	if err != nil || notFoundExt != "" {
		t.Errorf("GetExternalConversationID('nonexistent') = (%q, %v), want empty", notFoundExt, err)
	}

	// 4. Closed store returns error
	_ = s.Close()
	if _, err := s.GetActiveTasks(ctx); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected ErrConnDone on GetActiveTasks, got %v", err)
	}
	if _, err := s.GetExternalConversationID(ctx, "internal-abc"); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected ErrConnDone on GetExternalConversationID, got %v", err)
	}
}

