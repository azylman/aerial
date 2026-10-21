package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestFakeStoreLifecycleAndFailNext(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	mustOK(t, s.InsertMessage(ctx, Message{ID: "m1", ThreadID: "t1"}), "InsertMessage")

	testErr := errors.New("simulated error")
	s.FailNext("GetMessage", testErr)
	if _, err := s.GetMessage(ctx, "m1"); !errors.Is(err, testErr) {
		t.Fatalf("expected simulated error %v, got %v", testErr, err)
	}
	if _, err := s.GetMessage(ctx, "m1"); err != nil {
		t.Fatalf("expected success after consumed FailNext, got %v", err)
	}

	mustOK(t, s.Close(), "Close")
	assertStoreUnavailable(t, s)
}

func TestFakeStoreMessageOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	m1 := Message{ID: "msg-1", ThreadID: "th-1", Content: "Hello", Summary: "Summ"}
	mustOK(t, s.InsertMessage(ctx, m1), "InsertMessage")
	mustOK(t, s.InsertMessage(ctx, m1), "Idempotent InsertMessage")
	mustOK(t, s.InsertMessage(ctx, Message{ID: "msg-2", ThreadID: "th-1", Content: "World"}), "InsertMessage msg-2")

	if ok, err := s.MessageExists(ctx, "msg-1"); err != nil || !ok {
		t.Errorf("MessageExists(msg-1) = %v, %v, want true, nil", ok, err)
	}
	if ok, err := s.MessageExists(ctx, "msg-none"); err != nil || ok {
		t.Errorf("MessageExists(msg-none) = %v, %v, want false, nil", ok, err)
	}
	if msg, err := s.GetMessage(ctx, "msg-1"); err != nil || msg.Content != "Hello" {
		t.Errorf("GetMessage(msg-1) = %+v, %v", msg, err)
	}
	if _, err := s.GetMessage(ctx, "msg-none"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("expected ErrNoRows, got %v", err)
	}

	_ = s.UpdateMessageStatus(ctx, "msg-1", StatusProcessing, "busy")
	_ = s.UpdateMessageStatus(ctx, "msg-none", StatusProcessing, "")
	_ = s.IncrementMessageRetry(ctx, "msg-1", "timeout")
	_ = s.IncrementMessageRetry(ctx, "msg-none", "")
	if msg, _ := s.GetMessage(ctx, "msg-1"); msg.RetryCount != 1 {
		t.Errorf("expected retry=1, got %d", msg.RetryCount)
	}

	_ = s.ResetMessageToPendingWithRestart(ctx, "msg-1", "recovered")
	_ = s.ResetMessageToPendingWithRestart(ctx, "msg-none", "")
	if msg, _ := s.GetMessage(ctx, "msg-1"); msg.Status != StatusPending || msg.RestartCount != 1 {
		t.Errorf("expected pending, restart=1, got %+v", msg)
	}

	if ok, err := s.ClaimPendingMessage(ctx, "msg-1"); err != nil || !ok {
		t.Errorf("ClaimPendingMessage(msg-1) = %v, %v, want true, nil", ok, err)
	}
	if ok, _ := s.ClaimPendingMessage(ctx, "msg-1"); ok {
		t.Errorf("ClaimPendingMessage already claimed = true, want false")
	}
	if ok, _ := s.ClaimPendingMessage(ctx, "msg-none"); ok {
		t.Errorf("ClaimPendingMessage non-existent = true, want false")
	}

	_ = s.UpdateMessageCompleted(ctx, "msg-1", "Done response")
	_ = s.UpdateMessageCompleted(ctx, "msg-none", "No-op")
	if msg, _ := s.GetMessage(ctx, "msg-1"); msg.Status != StatusCompleted || msg.ResponseText != "Done response" {
		t.Errorf("expected completed, got %+v", msg)
	}

	if pending, err := s.GetPendingOrProcessingMessages(ctx, 10); err != nil || len(pending) != 1 || pending[0].ID != "msg-2" {
		t.Errorf("GetPendingOrProcessingMessages = %+v, %v", pending, err)
	}
	if maxRow, err := s.GetMaxMessageRowID(ctx, "th-1"); err != nil || maxRow != 1 {
		t.Errorf("GetMaxMessageRowID = %d, %v, want 1, nil", maxRow, err)
	}
	if threadIDs, err := s.GetActiveRecentThreadIDs(ctx, time.Hour); err != nil || len(threadIDs) != 1 {
		t.Errorf("GetActiveRecentThreadIDs = %v, %v", threadIDs, err)
	}
	if recent, err := s.GetRecentThreadMessages(ctx, "th-1", 10); err != nil || len(recent) != 2 {
		t.Errorf("GetRecentThreadMessages = %+v, %v", recent, err)
	}
	_, _ = s.GetRecentThreadMessages(ctx, "th-1", 0)

	customTime := now.Add(-30 * time.Minute)
	s.SetMessageUpdatedAt("msg-1", customTime)
	s.SetMessageUpdatedAt("non-existent", customTime)
	if m, _ := s.GetMessage(ctx, "msg-1"); m == nil || !m.UpdatedAt.Equal(customTime) {
		t.Errorf("SetMessageUpdatedAt failed: m=%+v", m)
	}

	if tasks, err := s.GetActiveTasks(ctx); err != nil || len(tasks) != 1 || tasks[0].ID != "msg-2" {
		t.Errorf("GetActiveTasks = %+v, %v", tasks, err)
	}
	for i := 3; i <= 60; i++ {
		_ = s.InsertMessage(ctx, Message{ID: "m-b-" + string(rune(i)), ThreadID: "th-1", Status: StatusPending, CreatedAt: now.Add(time.Duration(i) * time.Second)})
	}
	if clamped, err := s.GetActiveTasks(ctx); err != nil || len(clamped) != 50 {
		t.Errorf("clamped active tasks len = %d, want 50", len(clamped))
	}
}

func TestFakeStoreSessionOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	if id, err := s.GetSessionID(ctx, "th-1"); err != nil || id != "" {
		t.Errorf("expected empty session ID, got %q, %v", id, err)
	}
	mustOK(t, s.SaveSessionID(ctx, "th-1", "sess-1"), "SaveSessionID")
	if id, _ := s.GetSessionID(ctx, "th-1"); id != "sess-1" {
		t.Errorf("expected sess-1, got %q", id)
	}
	_ = s.SaveSessionID(ctx, "th-1", "sess-1-v2")

	if cnt, err := s.IncrementSessionTurnCount(ctx, "th-1"); err != nil || cnt != 1 {
		t.Errorf("IncrementSessionTurnCount = %d, %v, want 1, nil", cnt, err)
	}
	_, _ = s.IncrementSessionTurnCount(ctx, "th-1")
	if cnt, _ := s.GetSessionTurnCount(ctx, "th-1"); cnt != 2 {
		t.Errorf("GetSessionTurnCount = %d, want 2", cnt)
	}
	if cnt, _ := s.GetSessionTurnCount(ctx, "th-none"); cnt != 0 {
		t.Errorf("GetSessionTurnCount non-existent = %d, want 0", cnt)
	}

	mustOK(t, s.RotateSessionID(ctx, "th-1", "sess-2"), "RotateSessionID")
	curr, _ := s.GetSessionID(ctx, "th-1")
	prev, _ := s.GetPreviousSessionID(ctx, "th-1")
	turns, _ := s.GetSessionTurnCount(ctx, "th-1")
	if curr != "sess-2" || prev != "sess-1-v2" || turns != 0 {
		t.Errorf("post-rotation state: curr=%q, prev=%q, turns=%d", curr, prev, turns)
	}
	_ = s.RotateSessionID(ctx, "th-brand-new", "sess-init")

	if sum, wm, err := s.GetThreadSummary(ctx, "th-1"); err != nil || sum != "" || wm != "" {
		t.Errorf("GetThreadSummary empty = %q, %q, %v", sum, wm, err)
	}
	mustOK(t, s.SaveThreadSummary(ctx, "th-1", "Summary text", "msg-100"), "SaveThreadSummary")
	if sum, wm, _ := s.GetThreadSummary(ctx, "th-1"); sum != "Summary text" || wm != "msg-100" {
		t.Errorf("GetThreadSummary = %q, %q", sum, wm)
	}

	_ = s.InsertMessage(ctx, Message{ID: "m-act-1", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "Valid"})
	_ = s.InsertMessage(ctx, Message{ID: "m-act-2", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[EXPIRED_STALE]"})
	_ = s.InsertMessage(ctx, Message{ID: "m-act-3", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[AMBIENT]"})
	_ = s.InsertMessage(ctx, Message{ID: "m-act-4", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[IGNORED]"})
	_ = s.InsertMessage(ctx, Message{ID: "m-act-5", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "Valid 2"})
	if stats, err := s.GetSessionActivityStats(ctx, "th-1"); err != nil || stats.InternalSessionID != "sess-2" || stats.CompletedTurns != 2 {
		t.Errorf("GetSessionActivityStats = %+v, %v", stats, err)
	}

	_ = s.SaveSessionID(ctx, "ext-1", "int-1")
	if ext, _ := s.GetExternalConversationID(ctx, "int-1"); ext != "ext-1" {
		t.Errorf("GetExternalConversationID = %q, want ext-1", ext)
	}
	if ext, _ := s.GetExternalConversationID(ctx, ""); ext != "" {
		t.Errorf("GetExternalConversationID empty = %q, want empty", ext)
	}

	for i := 0; i < 8; i++ {
		_, _ = s.IncrementSessionTurnCount(ctx, "th-unrotated")
	}
	s.SetSessionUpdatedAt("th-unrotated", time.Now().UTC().Add(-2*time.Hour))
	if unrot, _ := s.FindUnrotatedSessions(ctx, 5); len(unrot) != 0 {
		t.Errorf("expected 0 unrotated sessions within 1h, got %d", len(unrot))
	}
	if unrotLong, _ := s.FindUnrotatedSessions(ctx, 5, 4*time.Hour); len(unrotLong) != 1 {
		t.Errorf("expected 1 unrotated session within 4h, got %d", len(unrotLong))
	}
	_, _ = s.FindUnrotatedSessions(ctx, 0)

	s.SetSessionInfo(SessionInfo{ThreadID: "th-helper", InternalSessionID: "sess-helper", TurnCount: 5, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-10 * time.Minute)})
	if sessID, _ := s.GetSessionID(ctx, "th-helper"); sessID != "sess-helper" {
		t.Errorf("SetSessionInfo: sessID=%q", sessID)
	}
	s.SetSessionUpdatedAt("th-helper", now.Add(-5*time.Minute))
	s.SetSessionUpdatedAt("non-existent-sess", now)
	_, _ = s.GetPreviousSessionID(ctx, "th-helper")
	_, _ = s.GetPreviousSessionID(ctx, "non-existent-sess")
}

func TestFakeStoreScheduleOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	_ = s.CreateOneShotSchedule(ctx, OneShotSchedule{ID: "os-1", ThreadID: "th-1", Prompt: "P1", RunAt: now.Add(-time.Minute)})
	_ = s.CreateOneShotSchedule(ctx, OneShotSchedule{ID: "os-2", ThreadID: "th-1", Prompt: "P2", RunAt: now.Add(time.Hour)})

	if due, _ := s.GetDueOneShotSchedules(ctx); len(due) != 1 || due[0].ID != "os-1" {
		t.Errorf("GetDueOneShotSchedules = %+v, want [os-1]", due)
	}
	if allOS, _ := s.GetAllOneShotSchedules(ctx, "th-1"); len(allOS) != 2 {
		t.Errorf("GetAllOneShotSchedules = %d, want 2", len(allOS))
	}
	_, _ = s.GetAllOneShotSchedules(ctx, "")

	msg := Message{ID: "msg-os-1", ThreadID: "th-1", Content: "Executed"}
	mustOK(t, s.InsertMessageAndConsumeOneShot(ctx, "os-1", msg), "InsertMessageAndConsumeOneShot")
	if err := s.InsertMessageAndConsumeOneShot(ctx, "os-1", msg); !errors.Is(err, ErrScheduleAlreadyConsumed) {
		t.Errorf("expected ErrScheduleAlreadyConsumed, got %v", err)
	}

	_ = s.CreateCronSchedule(ctx, CronSchedule{ID: "cron-1", TargetID: "t-1", CronExpr: "*/5 * * * *", Enabled: true, NextRunAt: now.Add(-time.Minute)})
	_ = s.CreateCronSchedule(ctx, CronSchedule{ID: "cron-2", TargetID: "t-1", CronExpr: "0 * * * *", Enabled: false, NextRunAt: now.Add(-time.Minute)})
	_ = s.CreateCronSchedule(ctx, CronSchedule{ID: "cron-3", TargetID: "t-2", CronExpr: "0 0 * * *", Enabled: true, NextRunAt: now.Add(time.Hour)})

	if dueCron, _ := s.GetDueCronSchedules(ctx); len(dueCron) != 1 || dueCron[0].ID != "cron-1" {
		t.Errorf("GetDueCronSchedules = %+v, want [cron-1]", dueCron)
	}
	if allCrons, _ := s.GetAllCronSchedules(ctx, "t-1"); len(allCrons) != 2 {
		t.Errorf("GetAllCronSchedules = %d, want 2", len(allCrons))
	}
	_, _ = s.GetAllCronSchedules(ctx, "")
	_ = s.UpdateCronNextRun(ctx, "cron-1", now.Add(10*time.Minute))
	_ = s.UpdateCronNextRun(ctx, "cron-none", now)

	_ = s.CreateScheduleRun(ctx, ScheduleRun{ID: "run-1", ScheduleID: "cron-1", ScheduleType: "cron", Status: "enqueued", StartedAt: now.Add(-2 * time.Hour)})
	_ = s.CreateScheduleRun(ctx, ScheduleRun{ID: "run-2", ScheduleID: "cron-1", ScheduleType: "cron", Status: "completed", StartedAt: now.Add(-1 * time.Hour), DurationMs: 500})
	_ = s.CreateScheduleRun(ctx, ScheduleRun{ID: "run-3", ScheduleID: "cron-3", ScheduleType: "cron", Status: "running", StartedAt: now.Add(-30 * time.Minute)})

	_ = s.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: "run-1", Status: "completed", MessageID: "msg-run-1", DurationMs: 1200, CompletedAt: now, Model: "claude"})
	_ = s.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: "run-none"})

	if runs, total, _ := s.GetScheduleRunsPaginated(ctx, 10, 0, "cron-1", ""); total != 2 || len(runs) != 2 {
		t.Errorf("GetScheduleRunsPaginated = len %d, total %d, want 2, 2", len(runs), total)
	}
	if runsComp, _, _ := s.GetScheduleRunsPaginated(ctx, 1, 0, "", "completed"); len(runsComp) != 1 {
		t.Errorf("GetScheduleRunsPaginated completed = %d, want 1", len(runsComp))
	}
	if runsLarge, _, _ := s.GetScheduleRunsPaginated(ctx, 10, 50, "", ""); runsLarge != nil {
		t.Errorf("expected nil runs for large offset, got %+v", runsLarge)
	}

	if metrics, err := s.GetScheduleSummaryMetrics(ctx); err != nil || metrics.CronCount != 2 || metrics.OneShotCount != 1 || metrics.TotalRuns24h != 3 {
		t.Errorf("GetScheduleSummaryMetrics = %+v, %v", metrics, err)
	}

	if rec, err := s.ReconcileOrphanedScheduleRuns(ctx); err != nil || rec != 1 {
		t.Errorf("ReconcileOrphanedScheduleRuns = %d, %v, want 1, nil", rec, err)
	}
	if prun, err := s.PruneScheduleRuns(ctx, 1, 48*time.Hour); err != nil || prun != 2 {
		t.Errorf("PruneScheduleRuns = %d, %v, want 2, nil", prun, err)
	}

	if _, err := s.GetScheduleRun("non-existent-run"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("expected sql.ErrNoRows for non-existent run, got: %v", err)
	}
	_ = s.CreateScheduleRun(ctx, ScheduleRun{ID: "run-helper", ScheduleID: "sched-helper", Status: "completed", StartedAt: now})
	if run, err := s.GetScheduleRun("run-helper"); err != nil || run == nil || run.Status != "completed" {
		t.Errorf("GetScheduleRun failed: run=%+v, err=%v", run, err)
	}
	_, _ = s.PruneScheduleRuns(ctx, 0, 0)
	for i := 0; i < 5; i++ {
		_ = s.CreateScheduleRun(ctx, ScheduleRun{ID: fmt.Sprintf("run-limit-%d", i), ScheduleID: "sched-helper", Status: "completed", StartedAt: now.Add(time.Duration(i) * time.Minute)})
	}
	_, _ = s.PruneScheduleRuns(ctx, 2, time.Hour)
}

func TestFakeStoreTranscriptsPRAndVanadis(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	mustOK(t, s.UpsertSessionSummary(ctx, SessionSummary{SessionID: "sess-1", ThreadID: "th-1", Summary: "Cockpit debug session"}), "UpsertSessionSummary")
	if syncStates, err := s.GetSessionSyncStates(ctx); err != nil || len(syncStates) != 1 || syncStates["sess-1"].SessionID != "sess-1" {
		t.Fatalf("GetSessionSyncStates failed: %v", err)
	}
	steps := []TranscriptStep{{SessionID: "sess-1", StepIndex: 0, StepType: "user", Content: "launch aerial"}, {SessionID: "sess-1", StepIndex: 1, StepType: "assistant", ToolName: "fly", Content: "flying aerial"}}
	mustOK(t, s.BatchInsertTranscriptSteps(ctx, steps), "BatchInsertTranscriptSteps")
	if stepResults, _ := s.SearchTranscriptSteps(ctx, "aerial", "sess-1", "", 10); len(stepResults) != 2 {
		t.Errorf("SearchTranscriptSteps = %d, want 2", len(stepResults))
	}
	if summaries, _ := s.SearchSessionSummaries(ctx, nil, "debug", 10, 0.0); len(summaries) != 1 {
		t.Errorf("SearchSessionSummaries = %d, want 1", len(summaries))
	}

	mustOK(t, s.UpsertPR(ctx, PRRecord{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/test", HeadSHA: "abc1234", TargetID: "th-1", Status: "open", Title: "Test PR"}), "UpsertPR")
	gotPR, err := s.GetPRByNumber(ctx, "azylman/aerial", 101)
	if err != nil || gotPR.Title != "Test PR" {
		t.Fatalf("GetPRByNumber failed: %v, pr=%+v", err, gotPR)
	}
	_ = s.UpdatePRStatusMessageID(ctx, "azylman/aerial", 101, "msg-status-1")
	if gotPR, _ = s.GetPRByNumber(ctx, "azylman/aerial", 101); gotPR.StatusMessageID != "msg-status-1" {
		t.Errorf("expected StatusMessageID 'msg-status-1', got %q", gotPR.StatusMessageID)
	}

	build := VanadisBuild{ID: "b-1", ThreadID: "th-1", Tier: 2, Title: "Vanadis Build 1", Objective: "Test Objective", Status: "running"}
	chunks := []VanadisChunk{{ID: "c-1", BuildID: "b-1", ChunkIndex: 0, Repo: "aerial", Title: "Chunk 1", Instructions: "Execute chunk 1", Status: "running"}}
	mustOK(t, s.CreateVanadisBuild(ctx, build, chunks), "CreateVanadisBuild")
	gotBuild, gotChunks, err := s.GetVanadisBuild(ctx, "b-1")
	if err != nil || gotBuild.ID != "b-1" || len(gotChunks) != 1 {
		t.Fatalf("GetVanadisBuild failed: %v", err)
	}
	chunk, err := s.GetVanadisChunk(ctx, "c-1")
	if err != nil || chunk.Title != "Chunk 1" {
		t.Fatalf("GetVanadisChunk failed: %v", err)
	}
	_ = s.AttachVanadisChunkPR(ctx, "c-1", 101)
	if chunk, _ = s.GetVanadisChunk(ctx, "c-1"); chunk.PRNumber != 101 || chunk.Status != "pr_opened" {
		t.Errorf("unexpected chunk after attach: %+v", chunk)
	}
	_ = s.UpdateVanadisBuildStatusMessageID(ctx, "b-1", "msg-build-status")
	if gotBuild, _, _ = s.GetVanadisBuild(ctx, "b-1"); gotBuild.StatusMessageID != "msg-build-status" {
		t.Errorf("expected StatusMessageID 'msg-build-status', got %q", gotBuild.StatusMessageID)
	}
}

func TestFakeStoreCloneNil(t *testing.T) {
	if cloneMessage(nil) != nil || clonePtr[SessionInfo](nil) != nil || cloneRun(nil) != nil ||
		cloneSessionSummary(nil) != nil || cloneTranscriptSteps(nil) != nil {
		t.Errorf("expected nil clones for nil inputs")
	}
	if got := cloneSliceNonNil[string](nil); got == nil || len(got) != 0 {
		t.Errorf("cloneSliceNonNil(nil) = %v, want empty slice", got)
	}
	if got := cloneSliceNonNil([]string{"a"}); len(got) != 1 || got[0] != "a" {
		t.Errorf("cloneSliceNonNil non-nil failed: %v", got)
	}
	if cloned := cloneVanadisChunk(&VanadisChunk{ID: "c1", TargetFiles: []string{"f1"}}); cloned.Effort != "medium" || len(cloned.TargetFiles) != 1 {
		t.Errorf("cloneVanadisChunk failed: %+v", cloned)
	}
}
