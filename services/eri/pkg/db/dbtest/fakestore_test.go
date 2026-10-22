package dbtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
)

func mustOK(t *testing.T, err error, desc string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s failed: %v", desc, err)
	}
}

func requireErr(t *testing.T, err error, op string) {
	t.Helper()
	if err == nil {
		t.Errorf("expected error for %s on unavailable store, got nil", op)
	}
}

func c1[T any](_ T, err error) error         { return err }
func c2[A, B any](_ A, _ B, err error) error { return err }

func assertStoreUnavailable(t *testing.T, s db.Store) {
	t.Helper()
	ctx := context.Background()
	requireErr(t, s.CreateOneShotSchedule(ctx, db.OneShotSchedule{}), "CreateOneShotSchedule")
	requireErr(t, c1(s.GetDueOneShotSchedules(ctx)), "GetDueOneShotSchedules")
	requireErr(t, s.InsertMessageAndConsumeOneShot(ctx, "id", db.Message{}), "InsertMessageAndConsumeOneShot")
	requireErr(t, c1(s.GetAllOneShotSchedules(ctx, "t")), "GetAllOneShotSchedules")
	requireErr(t, c1(s.GetDueCronSchedules(ctx)), "GetDueCronSchedules")
	requireErr(t, c1(s.GetAllCronSchedules(ctx, "t")), "GetAllCronSchedules")
	requireErr(t, s.UpdateCronNextRun(ctx, "id", time.Now()), "UpdateCronNextRun")
	requireErr(t, s.CreateScheduleRun(ctx, db.ScheduleRun{}), "CreateScheduleRun")
	requireErr(t, s.UpdateScheduleRunStatus(ctx, db.UpdateRunParams{}), "UpdateScheduleRunStatus")
	requireErr(t, c2(s.GetScheduleRunsPaginated(ctx, 10, 0, "", "")), "GetScheduleRunsPaginated")
	requireErr(t, c1(s.GetScheduleSummaryMetrics(ctx)), "GetScheduleSummaryMetrics")
	requireErr(t, c1(s.ReconcileOrphanedScheduleRuns(ctx)), "ReconcileOrphanedScheduleRuns")
	requireErr(t, c1(s.PruneScheduleRuns(ctx, 10, time.Hour)), "PruneScheduleRuns")
	requireErr(t, s.InsertMessage(ctx, db.Message{}), "InsertMessage")
	requireErr(t, s.UpdateMessageStatus(ctx, "id", "", ""), "UpdateMessageStatus")
	requireErr(t, s.UpdateMessageCompleted(ctx, "id", ""), "UpdateMessageCompleted")
	requireErr(t, s.IncrementMessageRetry(ctx, "id", ""), "IncrementMessageRetry")
	requireErr(t, s.ResetMessageToPendingWithRestart(ctx, "id", ""), "ResetMessageToPendingWithRestart")
	requireErr(t, c1(s.GetPendingOrProcessingMessages(ctx, 10)), "GetPendingOrProcessingMessages")
	requireErr(t, c1(s.GetMessage(ctx, "id")), "GetMessage")
	requireErr(t, c1(s.MessageExists(ctx, "id")), "MessageExists")
	requireErr(t, c1(s.ClaimPendingMessage(ctx, "id")), "ClaimPendingMessage")
	requireErr(t, c1(s.GetActiveRecentThreadIDs(ctx, time.Hour)), "GetActiveRecentThreadIDs")
	requireErr(t, c1(s.GetRecentThreadMessages(ctx, "t", 10)), "GetRecentThreadMessages")
	requireErr(t, c1(s.GetActiveTasks(ctx)), "GetActiveTasks")
	requireErr(t, c1(s.GetSessionID(ctx, "t")), "GetSessionID")
	requireErr(t, s.SaveSessionID(ctx, "t", "s"), "SaveSessionID")
	requireErr(t, c1(s.IncrementSessionTurnCount(ctx, "k")), "IncrementSessionTurnCount")
	requireErr(t, c1(s.GetSessionTurnCount(ctx, "k")), "GetSessionTurnCount")
	requireErr(t, s.RotateSessionID(ctx, "k", "n"), "RotateSessionID")
	requireErr(t, c1(s.GetPreviousSessionID(ctx, "k")), "GetPreviousSessionID")
	requireErr(t, c2(s.GetThreadSummary(ctx, "t")), "GetThreadSummary")
	requireErr(t, s.SaveThreadSummary(ctx, "t", "s", "w"), "SaveThreadSummary")
	requireErr(t, c1(s.GetSessionActivityStats(ctx, "t")), "GetSessionActivityStats")
	requireErr(t, c1(s.GetExternalConversationID(ctx, "test")), "GetExternalConversationID")
	requireErr(t, c1(s.FindUnrotatedSessions(ctx, 8)), "FindUnrotatedSessions")
}

func TestFakeStoreLifecycleAndFailNext(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	mustOK(t, s.InsertMessage(ctx, db.Message{ID: "m1", ThreadID: "t1"}), "InsertMessage")

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

	m1 := db.Message{ID: "msg-1", ThreadID: "th-1", Content: "Hello", Summary: "Summ"}
	mustOK(t, s.InsertMessage(ctx, m1), "InsertMessage")
	mustOK(t, s.InsertMessage(ctx, m1), "Idempotent InsertMessage")
	mustOK(t, s.InsertMessage(ctx, db.Message{ID: "msg-2", ThreadID: "th-1", Content: "World"}), "InsertMessage msg-2")

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

	_ = s.UpdateMessageStatus(ctx, "msg-1", db.StatusProcessing, "busy")
	_ = s.UpdateMessageStatus(ctx, "msg-none", db.StatusProcessing, "")
	_ = s.IncrementMessageRetry(ctx, "msg-1", "timeout")
	_ = s.IncrementMessageRetry(ctx, "msg-none", "")
	if msg, _ := s.GetMessage(ctx, "msg-1"); msg.RetryCount != 1 {
		t.Errorf("expected retry=1, got %d", msg.RetryCount)
	}

	_ = s.ResetMessageToPendingWithRestart(ctx, "msg-1", "recovered")
	_ = s.ResetMessageToPendingWithRestart(ctx, "msg-none", "")
	if msg, _ := s.GetMessage(ctx, "msg-1"); msg.Status != db.StatusPending || msg.RestartCount != 1 {
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
	if msg, _ := s.GetMessage(ctx, "msg-1"); msg.Status != db.StatusCompleted || msg.ResponseText != "Done response" {
		t.Errorf("expected completed, got %+v", msg)
	}

	if pending, err := s.GetPendingOrProcessingMessages(ctx, 10); err != nil || len(pending) != 1 || pending[0].ID != "msg-2" {
		t.Errorf("GetPendingOrProcessingMessages = %+v, %v", pending, err)
	}
	if threadIDs, err := s.GetActiveRecentThreadIDs(ctx, time.Hour); err != nil || len(threadIDs) != 1 {
		t.Errorf("GetActiveRecentThreadIDs = %v, %v", threadIDs, err)
	}
	if recent, err := s.GetRecentThreadMessages(ctx, "th-1", 10); err != nil || len(recent) != 2 {
		t.Errorf("GetRecentThreadMessages = %+v, %v", recent, err)
	}
	_, _ = s.GetRecentThreadMessages(ctx, "th-1", 0)

	if tasks, err := s.GetActiveTasks(ctx); err != nil || len(tasks) != 1 || tasks[0].ID != "msg-2" {
		t.Errorf("GetActiveTasks = %+v, %v", tasks, err)
	}
	for i := 3; i <= 60; i++ {
		_ = s.InsertMessage(ctx, db.Message{ID: "m-b-" + string(rune(i)), ThreadID: "th-1", Status: db.StatusPending, CreatedAt: now.Add(time.Duration(i) * time.Second)})
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

	_ = s.InsertMessage(ctx, db.Message{ID: "m-act-1", ThreadID: "th-1", Status: db.StatusCompleted, ResponseText: "Valid"})
	_ = s.InsertMessage(ctx, db.Message{ID: "m-act-2", ThreadID: "th-1", Status: db.StatusCompleted, ResponseText: "[EXPIRED_STALE]"})
	_ = s.InsertMessage(ctx, db.Message{ID: "m-act-3", ThreadID: "th-1", Status: db.StatusCompleted, ResponseText: "[AMBIENT]"})
	_ = s.InsertMessage(ctx, db.Message{ID: "m-act-4", ThreadID: "th-1", Status: db.StatusCompleted, ResponseText: "[IGNORED]"})
	_ = s.InsertMessage(ctx, db.Message{ID: "m-act-5", ThreadID: "th-1", Status: db.StatusCompleted, ResponseText: "Valid 2"})
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

	s.SetSessionInfo(db.SessionInfo{ThreadID: "th-helper", InternalSessionID: "sess-helper", TurnCount: 5, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-10 * time.Minute)})
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

	_ = s.CreateOneShotSchedule(ctx, db.OneShotSchedule{ID: "os-1", ThreadID: "th-1", Prompt: "P1", RunAt: now.Add(-time.Minute)})
	_ = s.CreateOneShotSchedule(ctx, db.OneShotSchedule{ID: "os-2", ThreadID: "th-1", Prompt: "P2", RunAt: now.Add(time.Hour)})

	if due, _ := s.GetDueOneShotSchedules(ctx); len(due) != 1 || due[0].ID != "os-1" {
		t.Errorf("GetDueOneShotSchedules = %+v, want [os-1]", due)
	}
	if allOS, _ := s.GetAllOneShotSchedules(ctx, "th-1"); len(allOS) != 2 {
		t.Errorf("GetAllOneShotSchedules = %d, want 2", len(allOS))
	}
	_, _ = s.GetAllOneShotSchedules(ctx, "")

	msg := db.Message{ID: "msg-os-1", ThreadID: "th-1", Content: "Executed"}
	mustOK(t, s.InsertMessageAndConsumeOneShot(ctx, "os-1", msg), "InsertMessageAndConsumeOneShot")
	if err := s.InsertMessageAndConsumeOneShot(ctx, "os-1", msg); !errors.Is(err, db.ErrScheduleAlreadyConsumed) {
		t.Errorf("expected ErrScheduleAlreadyConsumed, got %v", err)
	}

	_ = s.CreateCronSchedule(ctx, db.CronSchedule{ID: "cron-1", TargetID: "t-1", CronExpr: "*/5 * * * *", Enabled: true, NextRunAt: now.Add(-time.Minute)})
	_ = s.CreateCronSchedule(ctx, db.CronSchedule{ID: "cron-2", TargetID: "t-1", CronExpr: "0 * * * *", Enabled: false, NextRunAt: now.Add(-time.Minute)})
	_ = s.CreateCronSchedule(ctx, db.CronSchedule{ID: "cron-3", TargetID: "t-2", CronExpr: "0 0 * * *", Enabled: true, NextRunAt: now.Add(time.Hour)})

	if dueCron, _ := s.GetDueCronSchedules(ctx); len(dueCron) != 1 || dueCron[0].ID != "cron-1" {
		t.Errorf("GetDueCronSchedules = %+v, want [cron-1]", dueCron)
	}
	if allCrons, _ := s.GetAllCronSchedules(ctx, "t-1"); len(allCrons) != 2 {
		t.Errorf("GetAllCronSchedules = %d, want 2", len(allCrons))
	}
	_, _ = s.GetAllCronSchedules(ctx, "")
	_ = s.UpdateCronNextRun(ctx, "cron-1", now.Add(10*time.Minute))
	_ = s.UpdateCronNextRun(ctx, "cron-none", now)

	_ = s.CreateScheduleRun(ctx, db.ScheduleRun{ID: "run-1", ScheduleID: "cron-1", ScheduleType: "cron", Status: "enqueued", StartedAt: now.Add(-2 * time.Hour)})
	_ = s.CreateScheduleRun(ctx, db.ScheduleRun{ID: "run-2", ScheduleID: "cron-1", ScheduleType: "cron", Status: "completed", StartedAt: now.Add(-1 * time.Hour), DurationMs: 500})
	_ = s.CreateScheduleRun(ctx, db.ScheduleRun{ID: "run-3", ScheduleID: "cron-3", ScheduleType: "cron", Status: "running", StartedAt: now.Add(-30 * time.Minute)})

	_ = s.UpdateScheduleRunStatus(ctx, db.UpdateRunParams{RunID: "run-1", Status: "completed", MessageID: "msg-run-1", DurationMs: 1200, CompletedAt: now, Model: "claude"})
	_ = s.UpdateScheduleRunStatus(ctx, db.UpdateRunParams{RunID: "run-none"})

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
	_ = s.CreateScheduleRun(ctx, db.ScheduleRun{ID: "run-helper", ScheduleID: "sched-helper", Status: "completed", StartedAt: now})
	if run, err := s.GetScheduleRun("run-helper"); err != nil || run == nil || run.Status != "completed" {
		t.Errorf("GetScheduleRun failed: run=%+v, err=%v", run, err)
	}
	_, _ = s.PruneScheduleRuns(ctx, 0, 0)
	for i := 0; i < 5; i++ {
		_ = s.CreateScheduleRun(ctx, db.ScheduleRun{ID: fmt.Sprintf("run-limit-%d", i), ScheduleID: "sched-helper", Status: "completed", StartedAt: now.Add(time.Duration(i) * time.Minute)})
	}
	_, _ = s.PruneScheduleRuns(ctx, 2, time.Hour)
}

func TestFakeStoreTranscriptsPRAndVanadis(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	mustOK(t, s.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-1", ThreadID: "th-1", Summary: "Cockpit debug session"}), "UpsertSessionSummary")
	if syncStates, err := s.GetSessionSyncStates(ctx); err != nil || len(syncStates) != 1 || syncStates["sess-1"].SessionID != "sess-1" {
		t.Fatalf("GetSessionSyncStates failed: %v", err)
	}
	steps := []db.TranscriptStep{{SessionID: "sess-1", StepIndex: 0, StepType: "user", Content: "launch aerial"}, {SessionID: "sess-1", StepIndex: 1, StepType: "assistant", ToolName: "fly", Content: "flying aerial"}}
	mustOK(t, s.BatchInsertTranscriptSteps(ctx, steps), "BatchInsertTranscriptSteps")
	if stepResults, _ := s.SearchTranscriptSteps(ctx, "aerial", "sess-1", "", 10); len(stepResults) != 2 {
		t.Errorf("SearchTranscriptSteps = %d, want 2", len(stepResults))
	}
	if summaries, _ := s.SearchSessionSummaries(ctx, nil, "debug", 10, 0.0); len(summaries) != 1 {
		t.Errorf("SearchSessionSummaries = %d, want 1", len(summaries))
	}

	mustOK(t, s.UpsertPR(ctx, db.PRRecord{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/test", HeadSHA: "abc1234", TargetID: "th-1", Status: "open", Title: "Test PR"}), "UpsertPR")
	gotPR, err := s.GetPRByNumber(ctx, "azylman/aerial", 101)
	if err != nil || gotPR.Title != "Test PR" {
		t.Fatalf("GetPRByNumber failed: %v, pr=%+v", err, gotPR)
	}
	_ = s.UpdatePRStatusMessageID(ctx, "azylman/aerial", 101, "msg-status-1")
	if gotPR, _ = s.GetPRByNumber(ctx, "azylman/aerial", 101); gotPR.StatusMessageID != "msg-status-1" {
		t.Errorf("expected StatusMessageID 'msg-status-1', got %q", gotPR.StatusMessageID)
	}

	build := db.VanadisBuild{ID: "b-1", ThreadID: "th-1", Tier: 2, Title: "Vanadis Build 1", Objective: "Test Objective", Status: "running"}
	chunks := []db.VanadisChunk{{ID: "c-1", BuildID: "b-1", ChunkIndex: 0, Repo: "aerial", Title: "Chunk 1", Instructions: "Execute chunk 1", Status: "running"}}
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
	if cloneSlice[int](nil) != nil || len(cloneSlice([]int{1, 2, 3})) != 3 {
		t.Errorf("unexpected cloneSlice results")
	}
	if cloneMessage(nil) != nil || clonePtr[db.SessionInfo](nil) != nil || cloneRun(nil) != nil ||
		cloneSessionSummary(nil) != nil {
		t.Errorf("expected nil clones for nil inputs")
	}
	if got := cloneSliceNonNil[string](nil); got == nil || len(got) != 0 {
		t.Errorf("cloneSliceNonNil(nil) = %v, want empty slice", got)
	}
	if got := cloneSliceNonNil([]string{"a"}); len(got) != 1 || got[0] != "a" {
		t.Errorf("cloneSliceNonNil non-nil failed: %v", got)
	}
	if cloned := cloneVanadisChunk(&db.VanadisChunk{ID: "c1", TargetFiles: []string{"f1"}}); cloned.Effort != "medium" || len(cloned.TargetFiles) != 1 {
		t.Errorf("cloneVanadisChunk failed: %+v", cloned)
	}
}

func TestCosineSimilarity(t *testing.T) {
	simTests := []struct {
		name     string
		a, b     []float32
		expected float64
	}{
		{name: "both_nil", a: nil, b: nil, expected: 0.0},
		{name: "one_empty", a: []float32{}, b: []float32{1.0}, expected: 0.0},
		{name: "mismatched_len", a: []float32{1.0}, b: []float32{1.0, 2.0}, expected: 0.0},
		{name: "zero_vector", a: []float32{0.0, 0.0}, b: []float32{1.0, 1.0}, expected: 0.0},
		{name: "identical", a: []float32{1.0, 0.0}, b: []float32{1.0, 0.0}, expected: 1.0},
	}
	for _, tc := range simTests {
		t.Run(tc.name, func(t *testing.T) {
			got := cosineSimilarity(tc.a, tc.b)
			if math.Abs(got-tc.expected) > 0.001 {
				t.Errorf("cosineSimilarity() = %f, want %f", got, tc.expected)
			}
		})
	}
}

func TestTranscriptStore_FakeStore_CRUD_And_SyncStates(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()
	syncStates, err := store.GetSessionSyncStates(ctx)
	if err != nil || len(syncStates) != 0 {
		t.Fatalf("expected 0 sync states, got %d, err: %v", len(syncStates), err)
	}
	if err := store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: ""}); err == nil {
		t.Errorf("expected error for empty session ID")
	}

	now := time.Now().UTC().Truncate(time.Second)
	summary := db.SessionSummary{
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

	updateSummary := db.SessionSummary{SessionID: "sess-1", Summary: "Updated", LastIndexedStep: 25, SummaryStepWatermark: 25, IsSettled: true, Embedding: []float32{1.0, 2.0}}
	if err := store.UpsertSessionSummary(ctx, updateSummary); err != nil {
		t.Fatalf("UpsertSessionSummary update failed: %v", err)
	}
	syncStates, err = store.GetSessionSyncStates(ctx)
	if err != nil || syncStates["sess-1"].LastIndexedStep != 25 || !syncStates["sess-1"].IsSettled {
		t.Fatalf("unexpected state after update: %+v, err: %v", syncStates["sess-1"], err)
	}

	if err := store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-1", LastIndexedStep: 30, IsSettled: false}); err != nil {
		t.Fatalf("UpsertSessionSummary unsettle failed: %v", err)
	}
	syncStates, err = store.GetSessionSyncStates(ctx)
	if err != nil || syncStates["sess-1"].IsSettled {
		t.Fatalf("expected IsSettled=false after LastIndexedStep > SummaryStepWatermark, got %+v", syncStates["sess-1"])
	}

	if err := store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-1", LastIndexedStep: -2}); err != nil {
		t.Fatalf("UpsertSessionSummary reset failed: %v", err)
	}
	syncStates, err = store.GetSessionSyncStates(ctx)
	if err != nil || syncStates["sess-1"].LastIndexedStep != -1 {
		t.Fatalf("expected LastIndexedStep=-1, got %+v", syncStates["sess-1"])
	}
}

func TestTranscriptStore_FakeStore_BatchInsertTranscriptSteps(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()
	if err := store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-steps-1", ThreadID: "th-steps-1", Summary: "Session for steps"}); err != nil {
		t.Fatalf("UpsertSessionSummary failed: %v", err)
	}

	now := time.Now().UTC()
	steps := []db.TranscriptStep{
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

	dupSteps := []db.TranscriptStep{
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
	if err := store.BatchInsertTranscriptSteps(ctx, []db.TranscriptStep{{SessionID: ""}}); err != nil {
		t.Errorf("expected nil error for empty sessionID step")
	}
}

func TestTranscriptStore_SearchSessionSummaries(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()

	embVector, embQueue := make([]float32, db.ExpectedEmbeddingDim), make([]float32, db.ExpectedEmbeddingDim)
	embVector[0], embQueue[1] = 1.0, 1.0

	for _, s := range []db.SessionSummary{
		{SessionID: "sess-vector", ThreadID: "th-vector", Summary: "Investigated and fixed vector cosine similarity search in database", Embedding: embVector},
		{SessionID: "sess-queue", ThreadID: "th-queue", Summary: "Architectural optimization of worker pool event queue processing concurrency", Embedding: embQueue},
		{SessionID: "sess-discord", ThreadID: "th-discord", Summary: "Implemented Discord bot webhook notification dispatching and formatting"},
	} {
		if err := store.UpsertSessionSummary(ctx, s); err != nil {
			t.Fatalf("UpsertSessionSummary failed: %v", err)
		}
	}

	tests := []struct {
		name, query, wantID     string
		emb                     []float32
		limit, wantMin, wantMax int
		minScore, wantMinScore  float64
	}{
		{name: "sparse text-only search", query: "vector cosine", limit: 5, minScore: 0.1, wantID: "sess-vector", wantMin: 1, wantMax: 5},
		{name: "sparse text-only queue search", query: "worker pool concurrency", limit: 5, minScore: 0.1, wantID: "sess-queue", wantMin: 1, wantMax: 5},
		{name: "dense vector-only search", emb: embVector, limit: 5, minScore: 0.5, wantID: "sess-vector", wantMin: 1, wantMax: 5, wantMinScore: 0.99},
		{name: "hybrid search", emb: embVector, query: "vector similarity", limit: 5, minScore: 0.3, wantID: "sess-vector", wantMin: 1, wantMax: 5, wantMinScore: 0.01},
		{name: "impossible minScore", query: "worker", limit: 5, minScore: 2.0},
		{name: "clamped limit", query: "in", limit: 1, wantMin: 1, wantMax: 1},
		{name: "empty query and vector", limit: 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			results, err := store.SearchSessionSummaries(ctx, tc.emb, tc.query, tc.limit, tc.minScore)
			if err != nil {
				t.Fatalf("SearchSessionSummaries failed: %v", err)
			}
			if len(results) < tc.wantMin || len(results) > tc.wantMax {
				t.Fatalf("got %d results, want between %d and %d", len(results), tc.wantMin, tc.wantMax)
			}
			if tc.wantID != "" && (len(results) == 0 || results[0].SessionID != tc.wantID) {
				t.Errorf("got top result %v, want ID %s", results, tc.wantID)
			}
			if tc.wantMinScore > 0 && len(results) > 0 && results[0].Score < tc.wantMinScore {
				t.Errorf("got score %f, want >= %f", results[0].Score, tc.wantMinScore)
			}
		})
	}

	if err := store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: ""}); err == nil {
		t.Errorf("expected error for empty sessionID on FakeStore")
	}
	if resScored, err := store.SearchSessionSummaries(ctx, embVector, "vector", -1, -0.5); err != nil || len(resScored) == 0 {
		t.Errorf("expected results with default limit, got %d (err: %v)", len(resScored), err)
	}

	simErr := errors.New("simulated error")
	for _, m := range []string{"GetSessionSyncStates", "UpsertSessionSummary", "SearchSessionSummaries"} {
		store.FailNext(m, simErr)
		var callErr error
		switch m {
		case "GetSessionSyncStates":
			_, callErr = store.GetSessionSyncStates(ctx)
		case "UpsertSessionSummary":
			callErr = store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "s"})
		case "SearchSessionSummaries":
			_, callErr = store.SearchSessionSummaries(ctx, nil, "q", 5, 0.1)
		}
		if !errors.Is(callErr, simErr) {
			t.Errorf("expected simulated error for %s, got %v", m, callErr)
		}
	}

	_ = store.Close()
	if _, err := store.GetSessionSyncStates(ctx); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("expected sql.ErrConnDone on GetSessionSyncStates")
	}
}

func TestTranscriptStore_SearchTranscriptSteps(t *testing.T) {
	store, ctx, now := NewFakeStore(), context.Background(), time.Now().UTC()
	_ = store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-steps-a"})
	_ = store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-steps-b"})

	steps := []db.TranscriptStep{
		{SessionID: "sess-steps-a", StepIndex: 0, StepType: "user_message", Content: "Deploying cluster ingress controller", CreatedAt: now.Add(-3 * time.Minute)},
		{SessionID: "sess-steps-a", StepIndex: 1, StepType: "tool_call", ToolName: "run_command", Content: "kubectl apply -f ingress.yaml", CreatedAt: now.Add(-2 * time.Minute)},
		{SessionID: "sess-steps-a", StepIndex: 2, StepType: "tool_result", ToolName: "run_command", Content: "error: 502 bad gateway connecting to webhook", CreatedAt: now.Add(-1 * time.Minute)},
		{SessionID: "sess-steps-b", StepIndex: 0, StepType: "tool_call", ToolName: "read_file", Content: "reading ingress.yaml configuration", CreatedAt: now},
	}
	if err := store.BatchInsertTranscriptSteps(ctx, steps); err != nil {
		t.Fatalf("BatchInsertTranscriptSteps failed: %v", err)
	}

	tests := []struct {
		name, query, session, tool string
		limit, wantLen, wantStep   int
	}{
		{name: "text search", query: "bad gateway", limit: 10, wantLen: 1, wantStep: 2},
		{name: "multi word non-contiguous search", query: "error webhook", limit: 10, wantLen: 1, wantStep: 2},
		{name: "session filter", query: "ingress", session: "sess-steps-b", limit: 10, wantLen: 1},
		{name: "tool filter", query: "ingress", tool: "read_file", limit: 10, wantLen: 1},
		{name: "empty query tool filter", tool: "read_file", limit: 10, wantLen: 1},
		{name: "empty query and filters", limit: 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := store.SearchTranscriptSteps(ctx, tc.query, tc.session, tc.tool, tc.limit)
			if err != nil {
				t.Fatalf("SearchTranscriptSteps failed: %v", err)
			}
			if len(res) != tc.wantLen {
				t.Fatalf("got %d results, want %d", len(res), tc.wantLen)
			}
			if tc.wantLen > 0 {
				if tc.wantStep != 0 && res[0].StepIndex != tc.wantStep {
					t.Errorf("got step index %d, want %d", res[0].StepIndex, tc.wantStep)
				}
				if tc.tool != "" && res[0].ToolName != tc.tool {
					t.Errorf("got tool %s, want %s", res[0].ToolName, tc.tool)
				}
				if tc.session != "" && res[0].SessionID != tc.session {
					t.Errorf("got session %s, want %s", res[0].SessionID, tc.session)
				}
			}
		})
	}

	if allMatched, err := store.SearchTranscriptSteps(ctx, "ingress", "", "", -1); err != nil || len(allMatched) == 0 {
		t.Errorf("expected steps with default limit, got %d, err: %v", len(allMatched), err)
	}
}

func TestTranscriptStore_FakeStore_GetTranscriptStepRange(t *testing.T) {
	ctx := context.Background()
	store := NewFakeStore()
	_ = store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-range-a"})
	_ = store.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-range-b"})

	now := time.Now().UTC()
	steps := []db.TranscriptStep{
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

func TestPRRegistry_FakeStore_Lifecycle(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()

	for _, bad := range []db.PRRecord{
		{Repo: "", PRNumber: 1, Branch: "b", HeadSHA: "s", TargetID: "t"},
		{Repo: "aerial", PRNumber: 0, Branch: "b", HeadSHA: "s", TargetID: "t"},
		{Repo: "aerial", PRNumber: -1, Branch: "b", HeadSHA: "s", TargetID: "t"},
		{Repo: "aerial", PRNumber: 1, Branch: "", HeadSHA: "s", TargetID: "t"},
		{Repo: "aerial", PRNumber: 1, Branch: "b", HeadSHA: "", TargetID: "t"},
		{Repo: "aerial", PRNumber: 1, Branch: "b", HeadSHA: "s", TargetID: ""},
	} {
		if err := store.UpsertPR(ctx, bad); err == nil {
			t.Errorf("expected validation error for %+v", bad)
		}
	}

	rec := db.PRRecord{Repo: "aerial", PRNumber: 101, Branch: "feat/push-pipeline", HeadSHA: "head1234", TargetID: "1555405874565091380", Title: "feat: add push pipeline", Metadata: `{"author":"alex"}`}
	if err := store.UpsertPR(ctx, rec); err != nil {
		t.Fatalf("UpsertPR failed: %v", err)
	}

	got, err := store.GetPRByNumber(ctx, "aerial", 101)
	if err != nil || got.Repo != "azylman/aerial" || got.PRNumber != 101 || got.Branch != "feat/push-pipeline" || got.HeadSHA != "head1234" || got.TargetID != "1555405874565091380" || got.Status != "open" {
		t.Fatalf("unexpected record content: %+v (err: %v)", got, err)
	}
	if _, err := store.GetPRByNumber(ctx, "aerial", 9999); !errors.Is(err, db.ErrPRNotFound) {
		t.Errorf("expected ErrPRNotFound, got %v", err)
	}

	rec.HeadSHA, rec.Title, rec.Metadata = "head5678", "feat: add push pipeline updated", `{"author":"alex","v":2}`
	if err := store.UpsertPR(ctx, rec); err != nil {
		t.Fatalf("UpsertPR update failed: %v", err)
	}
	if updated, err := store.GetPRByNumber(ctx, "aerial", 101); err != nil || updated.HeadSHA != "head5678" || updated.Title != "feat: add push pipeline updated" {
		t.Fatalf("updated record mismatch: %+v (err: %v)", updated, err)
	}

	recDeploying := db.PRRecord{Repo: "aerial", PRNumber: 102, Branch: "feat/deploying", HeadSHA: "head102", TargetID: "1555405874565091380", Status: "deploying"}
	if err := store.UpsertPR(ctx, recDeploying); err != nil {
		t.Fatalf("UpsertPR deploying failed: %v", err)
	}
	recDeploying.Status = "open"
	if err := store.UpsertPR(ctx, recDeploying); err != nil {
		t.Fatalf("UpsertPR reupsert failed: %v", err)
	}
	if afterReupsert, err := store.GetPRByNumber(ctx, "aerial", 102); err != nil || afterReupsert.Status != "deploying" {
		t.Fatalf("expected deploying status preserved, got %+v (err: %v)", afterReupsert, err)
	}

	if err := store.UpdatePRStatusMessageID(ctx, "aerial", 101, "  msg-widget-777  "); err != nil {
		t.Fatalf("UpdatePRStatusMessageID failed: %v", err)
	}
	if withMsgID, err := store.GetPRByNumber(ctx, "aerial", 101); err != nil || withMsgID.StatusMessageID != "msg-widget-777" {
		t.Fatalf("expected msg-widget-777, got %+v (err: %v)", withMsgID, err)
	}
	if err := store.UpdatePRStatusMessageID(ctx, "aerial", 9999, "msg-widget-999"); !errors.Is(err, db.ErrPRNotFound) {
		t.Errorf("expected ErrPRNotFound on non-existent PR, got %v", err)
	}
	if err := store.UpdatePRStatusMessageID(ctx, "", 101, "msg-widget-777"); err == nil {
		t.Errorf("expected validation error on empty repo")
	}
}

func TestPRRegistry_FakeStore_Failures(t *testing.T) {
	simErr := errors.New("simulated error")
	for _, op := range []string{"UpsertPR", "GetPRByNumber", "UpdatePRStatusMessageID"} {
		t.Run(op, func(t *testing.T) {
			store := NewFakeStore()
			ctx := context.Background()
			store.FailNext(op, simErr)
			var err error
			switch op {
			case "UpsertPR":
				err = store.UpsertPR(ctx, db.PRRecord{Repo: "a", PRNumber: 1, Branch: "b", HeadSHA: "c", TargetID: "d"})
			case "GetPRByNumber":
				_, err = store.GetPRByNumber(ctx, "a", 1)
			case "UpdatePRStatusMessageID":
				err = store.UpdatePRStatusMessageID(ctx, "a", 1, "msg-1")
			}
			if !errors.Is(err, simErr) {
				t.Fatalf("expected %v, got %v", simErr, err)
			}
		})
	}
}

func testVanadisReadAndLifecycle(t *testing.T, store db.Store, explicitStart time.Time) {
	t.Helper()
	ctx := context.Background()

	gotBuild, gotChunks, err := store.GetVanadisBuild(ctx, "build-101")
	if err != nil || gotBuild.ID != "build-101" || gotBuild.Title != "Implement vanadis build queue" || gotBuild.Status != "running" {
		t.Fatalf("unexpected build: %+v (err: %v)", gotBuild, err)
	}

	gotTitledBuild, gotTitledChunks, err := store.GetVanadisBuild(ctx, "build-titled-102")
	if err != nil || gotTitledBuild.Title != "Redesign Vanadis Build Status Cards" || len(gotTitledChunks) != 1 || gotTitledChunks[0].StartedAt.IsZero() {
		t.Fatalf("unexpected titled build/chunks: %+v, %+v (err: %v)", gotTitledBuild, gotTitledChunks, err)
	}
	if len(gotChunks) != 3 || gotChunks[0].ID != "chunk-101-0" || gotChunks[0].Repo != "azylman/aerial" || !gotChunks[0].StartedAt.IsZero() {
		t.Fatalf("unexpected chunks: %+v", gotChunks)
	}
	if !reflect.DeepEqual(gotChunks[0].TargetFiles, []string{"brain/pkg/db/schema.sql", "brain/pkg/db/vanadis.go"}) || !reflect.DeepEqual(gotChunks[0].TargetSymbols, []string{"VanadisBuild", "VanadisChunk"}) || !reflect.DeepEqual(gotChunks[0].AcceptanceCriteria, []string{"Unit tests pass", "Coverage >= 95%"}) {
		t.Errorf("unexpected chunk[0] metadata: %+v", gotChunks[0])
	}
	if gotChunks[1].ID != "chunk-101-1" || gotChunks[1].Repo != "azylman/aerial-config" || gotChunks[1].Effort != "high" || !reflect.DeepEqual(gotChunks[1].TargetFiles, []string{"brain/pkg/vanadis/worker.go", `file,with"special\chars.go`}) || !reflect.DeepEqual(gotChunks[1].TargetSymbols, []string{`Sym,with"quote`}) {
		t.Errorf("unexpected chunk[1] metadata: %+v", gotChunks[1])
	}
	if gotChunks[2].ID != "chunk-101-2" || gotChunks[2].Status != "queued" || gotChunks[2].PRNumber != 42 || !gotChunks[2].StartedAt.Equal(explicitStart) || len(gotChunks[2].TargetFiles) != 0 {
		t.Errorf("unexpected chunk[2]: %+v", gotChunks[2])
	}

	if _, _, err := store.GetVanadisBuild(ctx, "nonexistent"); !errors.Is(err, db.ErrVanadisBuildNotFound) {
		t.Errorf("expected ErrVanadisBuildNotFound, got %v", err)
	}
	if _, _, err := store.GetVanadisBuild(ctx, "  "); err == nil {
		t.Error("expected error for empty buildID in GetVanadisBuild")
	}

	gotChunk0, err := store.GetVanadisChunk(ctx, "chunk-101-0")
	if err != nil || gotChunk0.Title != "Task 1: Schema and store" || !reflect.DeepEqual(gotChunk0.TargetSymbols, []string{"VanadisBuild", "VanadisChunk"}) {
		t.Fatalf("GetVanadisChunk unexpected result: %+v (err: %v)", gotChunk0, err)
	}
	if _, err := store.GetVanadisChunk(ctx, "nonexistent-chunk"); !errors.Is(err, db.ErrVanadisChunkNotFound) {
		t.Errorf("expected ErrVanadisChunkNotFound, got %v", err)
	}
	if _, err := store.GetVanadisChunk(ctx, ""); err == nil {
		t.Error("expected error for empty chunkID in GetVanadisChunk")
	}

	gotChunk102Before, err := store.GetVanadisChunk(ctx, "chunk-102-0")
	if err != nil || gotChunk102Before.StartedAt.IsZero() {
		t.Fatalf("expected non-zero initial StartedAt on chunk-102-0: %+v", gotChunk102Before)
	}
	if err := store.AttachVanadisChunkPR(ctx, "chunk-102-0", 512); err != nil {
		t.Fatalf("AttachVanadisChunkPR failed: %v", err)
	}
	gotChunk102PR, err := store.GetVanadisChunk(ctx, "chunk-102-0")
	if err != nil || gotChunk102PR.PRNumber != 512 || gotChunk102PR.Status != "pr_opened" || !gotChunk102PR.StartedAt.Equal(gotChunk102Before.StartedAt) {
		t.Errorf("unexpected chunk after AttachVanadisChunkPR: %+v (err: %v)", gotChunk102PR, err)
	}

	if err := store.AttachVanadisChunkPR(ctx, "chunk-101-1", 513); err != nil {
		t.Fatalf("AttachVanadisChunkPR on chunk-101-1 failed: %v", err)
	}
	gotChunk1PR, err := store.GetVanadisChunk(ctx, "chunk-101-1")
	if err != nil || gotChunk1PR.StartedAt.IsZero() {
		t.Errorf("expected non-zero StartedAt after AttachVanadisChunkPR: %+v", gotChunk1PR)
	}
	if err := store.AttachVanadisChunkPR(ctx, "nonexistent-chunk", 512); !errors.Is(err, db.ErrVanadisChunkNotFound) {
		t.Errorf("expected ErrVanadisChunkNotFound, got %v", err)
	}
	if err := store.AttachVanadisChunkPR(ctx, "", 512); err == nil {
		t.Error("expected error for empty chunkID in AttachVanadisChunkPR")
	}
	if err := store.AttachVanadisChunkPR(ctx, "chunk-101-0", 0); err == nil {
		t.Error("expected error for pr_number <= 0 in AttachVanadisChunkPR")
	}

	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "build-101", "  msg-build-999  "); err != nil {
		t.Fatalf("UpdateVanadisBuildStatusMessageID failed: %v", err)
	}
	gotBuildWithMsg, _, err := store.GetVanadisBuild(ctx, "build-101")
	if err != nil || gotBuildWithMsg.StatusMessageID != "msg-build-999" {
		t.Fatalf("expected status_message_id 'msg-build-999', got %+v (err: %v)", gotBuildWithMsg, err)
	}
	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "nonexistent-build", "msg-build-999"); !errors.Is(err, db.ErrVanadisBuildNotFound) {
		t.Errorf("expected ErrVanadisBuildNotFound, got %v", err)
	}
	if err := store.UpdateVanadisBuildStatusMessageID(ctx, "", "msg-build-999"); err == nil {
		t.Error("expected error for empty buildID in UpdateVanadisBuildStatusMessageID")
	}
}

func TestVanadis_FakeStore_Lifecycle(t *testing.T) {
	validBuild := db.VanadisBuild{ID: "build-101", ThreadID: "1555405874565091380", Objective: "Implement vanadis build queue", SharedContext: "Shared context for build 101"}
	validChunk := db.VanadisChunk{
		ID: "chunk-101-0", BuildID: "build-101", ChunkIndex: 0, Title: "Task 1: Schema and store", Instructions: "Add vanadis_builds and vanadis_chunks tables",
		TargetFiles: []string{"brain/pkg/db/schema.sql", "brain/pkg/db/vanadis.go"}, TargetSymbols: []string{"VanadisBuild", "VanadisChunk"}, Effort: "medium", AcceptanceCriteria: []string{"Unit tests pass", "Coverage >= 95%"},
	}
	explicitStart := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	chunks := []db.VanadisChunk{
		validChunk,
		{
			ID: "chunk-101-1", BuildID: "build-101", ChunkIndex: 1, Repo: "azylman/aerial-config", Title: "Task 2: River worker", Instructions: "Implement VanadisWorker",
			TargetFiles: []string{"brain/pkg/vanadis/worker.go", `file,with"special\chars.go`}, TargetSymbols: []string{`Sym,with"quote`}, Effort: "HIGH", AcceptanceCriteria: []string{"Worker processes chunk successfully"},
		},
		{ID: "chunk-101-2", BuildID: "build-101", ChunkIndex: 2, Title: "Task 3: Verification", Instructions: "Run tests", Status: "queued", PRNumber: 42, StartedAt: explicitStart},
	}
	titledBuild := db.VanadisBuild{ID: "build-titled-102", ThreadID: "1555405874565091380", Title: "Redesign Vanadis Build Status Cards", Objective: "Detailed objective for redesigning the cards"}
	runningChunk := db.VanadisChunk{ID: "chunk-102-0", BuildID: "build-titled-102", ChunkIndex: 0, Title: "Already running chunk", Instructions: "Run immediately", Status: "running"}

	store := NewFakeStore()
	ctx := context.Background()
	if err := store.CreateVanadisBuild(ctx, validBuild, chunks); err != nil {
		t.Fatalf("CreateVanadisBuild failed: %v", err)
	}
	if err := store.CreateVanadisBuild(ctx, validBuild, nil); err == nil {
		t.Error("expected error on duplicate build ID")
	}
	if err := store.CreateVanadisBuild(ctx, db.VanadisBuild{ID: "build-dup", ThreadID: "1555405874565091380", Objective: "obj", Tier: 3, Status: "queued"}, []db.VanadisChunk{validChunk}); err == nil {
		t.Error("expected error on duplicate chunk ID")
	}
	if err := store.CreateVanadisBuild(ctx, titledBuild, []db.VanadisChunk{runningChunk}); err != nil {
		t.Fatalf("CreateVanadisBuild with explicit Title failed: %v", err)
	}
	testVanadisReadAndLifecycle(t, store, explicitStart)
	if err := store.CreateVanadisBuild(ctx, db.VanadisBuild{ID: "b-dup-idx", ThreadID: "123", Objective: "obj"}, []db.VanadisChunk{
		{ID: "c-1", ChunkIndex: 0, Title: "t1", Instructions: "i1"},
		{ID: "c-2", ChunkIndex: 0, Title: "t2", Instructions: "i2"},
	}); err == nil {
		t.Error("expected error on duplicate chunk_index")
	}
}

func TestVanadis_Validation(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()
	validBuild := db.VanadisBuild{ID: "b1", ThreadID: "t1", Objective: "obj"}

	for _, tc := range []struct {
		name   string
		build  db.VanadisBuild
		chunks []db.VanadisChunk
	}{
		{"empty_build_id", db.VanadisBuild{ID: "", ThreadID: "123", Objective: "obj"}, nil},
		{"empty_thread_id", db.VanadisBuild{ID: "b1", ThreadID: "", Objective: "obj"}, nil},
		{"empty_objective", db.VanadisBuild{ID: "b1", ThreadID: "123", Objective: ""}, nil},
		{"empty_chunk_id", validBuild, []db.VanadisChunk{{ID: "", Title: "t", Instructions: "i"}}},
		{"mismatched_chunk_build_id", validBuild, []db.VanadisChunk{{ID: "c1", BuildID: "other-build", Title: "t", Instructions: "i"}}},
		{"empty_chunk_title", validBuild, []db.VanadisChunk{{ID: "c1", Title: "", Instructions: "i"}}},
		{"empty_chunk_instructions", validBuild, []db.VanadisChunk{{ID: "c1", Title: "t", Instructions: ""}}},
		{"invalid_chunk_effort", validBuild, []db.VanadisChunk{{ID: "c1", Title: "t", Instructions: "i", Effort: "ultra"}}},
		{"numeric_chunk_effort", validBuild, []db.VanadisChunk{{ID: "c1", Title: "t", Instructions: "i", Effort: "10"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.CreateVanadisBuild(ctx, tc.build, tc.chunks); err == nil {
				t.Errorf("[%s] expected validation error", tc.name)
			}
		})
	}
}

func TestVanadis_FakeStore_FailuresAndClosed(t *testing.T) {
	simErr := errors.New("simulated error")
	for _, op := range []string{"CreateVanadisBuild", "GetVanadisBuild", "GetVanadisChunk", "AttachVanadisChunkPR", "UpdateVanadisBuildStatusMessageID"} {
		t.Run(op, func(t *testing.T) {
			store := NewFakeStore()
			ctx := context.Background()
			store.FailNext(op, simErr)
			var err error
			switch op {
			case "CreateVanadisBuild":
				err = store.CreateVanadisBuild(ctx, db.VanadisBuild{}, nil)
			case "GetVanadisBuild":
				_, _, err = store.GetVanadisBuild(ctx, "b1")
			case "GetVanadisChunk":
				_, err = store.GetVanadisChunk(ctx, "c1")
			case "AttachVanadisChunkPR":
				err = store.AttachVanadisChunkPR(ctx, "c1", 1)
			case "UpdateVanadisBuildStatusMessageID":
				err = store.UpdateVanadisBuildStatusMessageID(ctx, "b1", "m")
			}
			if !errors.Is(err, simErr) {
				t.Fatalf("expected %v, got %v", simErr, err)
			}
		})
	}

	closed := NewFakeStore()
	_ = closed.Close()
	ctx := context.Background()
	for _, err := range []error{
		closed.CreateVanadisBuild(ctx, db.VanadisBuild{}, nil),
		c2(closed.GetVanadisBuild(ctx, "b1")),
		c1(closed.GetVanadisChunk(ctx, "c1")),
		closed.AttachVanadisChunkPR(ctx, "c1", 1),
		closed.UpdateVanadisBuildStatusMessageID(ctx, "b1", "m"),
	} {
		if !errors.Is(err, sql.ErrConnDone) {
			t.Errorf("expected ErrConnDone, got %v", err)
		}
	}
}

func TestNormalizeVanadisEffort(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
		err  bool
	}{
		{"empty", "", "medium", false},
		{"whitespace", "   ", "medium", false},
		{"low", "low", "low", false},
		{"LOW", "LOW", "low", false},
		{"medium_padded", "  Medium  ", "medium", false},
		{"high", "high", "high", false},
		{"HIGH", "HIGH", "high", false},
		{"invalid", "invalid", "", true},
		{"unknown", "unknown", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeVanadisEffort(tc.in)
			if (err != nil) != tc.err || got != tc.want {
				t.Errorf("normalizeVanadisEffort(%q) = (%q, %v), want (%q, err=%v)", tc.in, got, err, tc.want, tc.err)
			}
		})
	}
}
