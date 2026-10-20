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

	if err := s.InsertMessage(ctx, Message{ID: "m1", ThreadID: "t1"}); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	testErr := errors.New("simulated error")
	s.FailNext("GetMessage", testErr)
	if _, err := s.GetMessage(ctx, "m1"); !errors.Is(err, testErr) {
		t.Fatalf("expected simulated error %v, got %v", testErr, err)
	}
	if _, err := s.GetMessage(ctx, "m1"); err != nil {
		t.Fatalf("expected success after consumed FailNext, got %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	assertStoreUnavailable(t, s)
}

func TestFakeStoreMessageOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	m1 := Message{ID: "msg-1", ThreadID: "th-1", Content: "Hello", Summary: "Summ"}
	if err := s.InsertMessage(ctx, m1); err != nil || s.InsertMessage(ctx, m1) != nil {
		t.Fatalf("InsertMessage or idempotent insert failed")
	}
	m2 := Message{ID: "msg-2", ThreadID: "th-1", Content: "World"}
	if err := s.InsertMessage(ctx, m2); err != nil {
		t.Fatalf("InsertMessage msg-2 failed: %v", err)
	}

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
	msg, _ := s.GetMessage(ctx, "msg-1")
	if msg.RetryCount != 1 {
		t.Errorf("expected retry=1, got %d", msg.RetryCount)
	}

	_ = s.ResetMessageToPendingWithRestart(ctx, "msg-1", "recovered")
	_ = s.ResetMessageToPendingWithRestart(ctx, "msg-none", "")
	msg, _ = s.GetMessage(ctx, "msg-1")
	if msg.Status != StatusPending || msg.RestartCount != 1 {
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
	msg, _ = s.GetMessage(ctx, "msg-1")
	if msg.Status != StatusCompleted || msg.ResponseText != "Done response" {
		t.Errorf("expected completed, got %+v", msg)
	}

	pending, err := s.GetPendingOrProcessingMessages(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].ID != "msg-2" {
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

	tasks, err := s.GetActiveTasks(ctx)
	if err != nil || len(tasks) != 1 || tasks[0].ID != "msg-2" {
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

	if id, err := s.GetSessionID(ctx, "th-1"); err != nil || id != "" {
		t.Errorf("expected empty session ID, got %q, %v", id, err)
	}
	if err := s.SaveSessionID(ctx, "th-1", "sess-1"); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}
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

	if err := s.RotateSessionID(ctx, "th-1", "sess-2"); err != nil {
		t.Fatalf("RotateSessionID failed: %v", err)
	}
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
	if err := s.SaveThreadSummary(ctx, "th-1", "Summary text", "msg-100"); err != nil {
		t.Fatalf("SaveThreadSummary failed: %v", err)
	}
	if sum, wm, _ := s.GetThreadSummary(ctx, "th-1"); sum != "Summary text" || wm != "msg-100" {
		t.Errorf("GetThreadSummary = %q, %q", sum, wm)
	}

	_ = s.InsertMessage(ctx, Message{ID: "m-act-1", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "Valid"})
	_ = s.InsertMessage(ctx, Message{ID: "m-act-2", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[EXPIRED_STALE]"})
	_ = s.InsertMessage(ctx, Message{ID: "m-act-3", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[AMBIENT]"})
	_ = s.InsertMessage(ctx, Message{ID: "m-act-4", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[IGNORED]"})
	_ = s.InsertMessage(ctx, Message{ID: "m-act-5", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "Valid 2"})
	stats, err := s.GetSessionActivityStats(ctx, "th-1")
	if err != nil || stats.InternalSessionID != "sess-2" || stats.CompletedTurns != 2 {
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
}

func TestFakeStoreScheduleOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	s1 := OneShotSchedule{ID: "os-1", ThreadID: "th-1", Prompt: "P1", RunAt: now.Add(-time.Minute)}
	s2 := OneShotSchedule{ID: "os-2", ThreadID: "th-1", Prompt: "P2", RunAt: now.Add(time.Hour)}
	_ = s.CreateOneShotSchedule(ctx, s1)
	_ = s.CreateOneShotSchedule(ctx, s2)

	due, _ := s.GetDueOneShotSchedules(ctx)
	if len(due) != 1 || due[0].ID != "os-1" {
		t.Errorf("GetDueOneShotSchedules = %+v, want [os-1]", due)
	}
	allOS, _ := s.GetAllOneShotSchedules(ctx, "th-1")
	if len(allOS) != 2 {
		t.Errorf("GetAllOneShotSchedules = %d, want 2", len(allOS))
	}
	_, _ = s.GetAllOneShotSchedules(ctx, "")

	msg := Message{ID: "msg-os-1", ThreadID: "th-1", Content: "Executed"}
	if err := s.InsertMessageAndConsumeOneShot(ctx, "os-1", msg); err != nil {
		t.Fatalf("InsertMessageAndConsumeOneShot failed: %v", err)
	}
	if err := s.InsertMessageAndConsumeOneShot(ctx, "os-1", msg); !errors.Is(err, ErrScheduleAlreadyConsumed) {
		t.Errorf("expected ErrScheduleAlreadyConsumed, got %v", err)
	}

	c1 := CronSchedule{ID: "cron-1", TargetID: "t-1", CronExpr: "*/5 * * * *", Enabled: true, NextRunAt: now.Add(-time.Minute)}
	c2 := CronSchedule{ID: "cron-2", TargetID: "t-1", CronExpr: "0 * * * *", Enabled: false, NextRunAt: now.Add(-time.Minute)}
	c3 := CronSchedule{ID: "cron-3", TargetID: "t-2", CronExpr: "0 0 * * *", Enabled: true, NextRunAt: now.Add(time.Hour)}
	_ = s.CreateCronSchedule(ctx, c1)
	_ = s.CreateCronSchedule(ctx, c2)
	_ = s.CreateCronSchedule(ctx, c3)

	dueCron, _ := s.GetDueCronSchedules(ctx)
	if len(dueCron) != 1 || dueCron[0].ID != "cron-1" {
		t.Errorf("GetDueCronSchedules = %+v, want [cron-1]", dueCron)
	}
	allCrons, _ := s.GetAllCronSchedules(ctx, "t-1")
	if len(allCrons) != 2 {
		t.Errorf("GetAllCronSchedules = %d, want 2", len(allCrons))
	}
	_, _ = s.GetAllCronSchedules(ctx, "")
	_ = s.UpdateCronNextRun(ctx, "cron-1", now.Add(10*time.Minute))
	_ = s.UpdateCronNextRun(ctx, "cron-none", now)

	r1 := ScheduleRun{ID: "run-1", ScheduleID: "cron-1", ScheduleType: "cron", Status: "enqueued", StartedAt: now.Add(-2 * time.Hour)}
	r2 := ScheduleRun{ID: "run-2", ScheduleID: "cron-1", ScheduleType: "cron", Status: "completed", StartedAt: now.Add(-1 * time.Hour), DurationMs: 500}
	r3 := ScheduleRun{ID: "run-3", ScheduleID: "cron-3", ScheduleType: "cron", Status: "running", StartedAt: now.Add(-30 * time.Minute)}
	_ = s.CreateScheduleRun(ctx, r1)
	_ = s.CreateScheduleRun(ctx, r2)
	_ = s.CreateScheduleRun(ctx, r3)

	_ = s.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: "run-1", Status: "completed", MessageID: "msg-run-1", DurationMs: 1200, CompletedAt: now, Model: "claude"})
	_ = s.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: "run-none"})

	runs, total, _ := s.GetScheduleRunsPaginated(ctx, 10, 0, "cron-1", "")
	if total != 2 || len(runs) != 2 {
		t.Errorf("GetScheduleRunsPaginated = len %d, total %d, want 2, 2", len(runs), total)
	}
	runsComp, _, _ := s.GetScheduleRunsPaginated(ctx, 1, 0, "", "completed")
	if len(runsComp) != 1 {
		t.Errorf("GetScheduleRunsPaginated completed = %d, want 1", len(runsComp))
	}
	runsLarge, _, _ := s.GetScheduleRunsPaginated(ctx, 10, 50, "", "")
	if runsLarge != nil {
		t.Errorf("expected nil runs for large offset, got %+v", runsLarge)
	}

	metrics, err := s.GetScheduleSummaryMetrics(ctx)
	if err != nil || metrics.CronCount != 2 || metrics.OneShotCount != 1 || metrics.TotalRuns24h != 3 {
		t.Errorf("GetScheduleSummaryMetrics = %+v, %v", metrics, err)
	}

	if rec, err := s.ReconcileOrphanedScheduleRuns(ctx); err != nil || rec != 1 {
		t.Errorf("ReconcileOrphanedScheduleRuns = %d, %v, want 1, nil", rec, err)
	}
	if prun, err := s.PruneScheduleRuns(ctx, 1, 48*time.Hour); err != nil || prun != 2 {
		t.Errorf("PruneScheduleRuns = %d, %v, want 2, nil", prun, err)
	}
}

func TestFakeStoreFactOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	emb1 := []float32{1.0, 0.0, 0.0}
	emb2 := []float32{0.9, 0.1, 0.0}
	emb3 := []float32{0.0, 1.0, 0.0}

	id1, _ := s.InsertFact(ctx, "personal", "Likes matcha latte", 0.9, emb1)
	_, _ = s.InsertFact(ctx, "tech", "Gundam pilot", 0.8, emb2)
	id3, _ := s.InsertFact(ctx, "tech", "Missing embedding fact", 0.5, nil)

	matches, _ := s.SearchSimilarFacts(ctx, emb1, "", 5, 0.75)
	if len(matches) != 2 || matches[0].ID != id1 {
		t.Errorf("SearchSimilarFacts = %+v, want top id1", matches)
	}

	dup, sim, _ := s.FindDuplicateFact(ctx, emb1, 0.85)
	if dup == nil || dup.ID != id1 || sim <= 0.85 {
		t.Errorf("FindDuplicateFact = %+v, %f", dup, sim)
	}
	if dupNone, _, _ := s.FindDuplicateFact(ctx, emb3, 0.99); dupNone != nil {
		t.Errorf("expected no dup, got %+v", dupNone)
	}

	res, _ := s.GetFactsPaginated(ctx, FactsFilter{Category: "tech", Limit: 10})
	if res.Total != 2 || len(res.Facts) != 2 {
		t.Errorf("GetFactsPaginated tech = total %d, len %d, want 2, 2", res.Total, len(res.Facts))
	}
	resQ, _ := s.GetFactsPaginated(ctx, FactsFilter{Query: "matcha"})
	if len(resQ.Facts) != 1 || resQ.Facts[0].ID != id1 {
		t.Errorf("GetFactsPaginated query matcha = %+v", resQ)
	}
	resOff, _ := s.GetFactsPaginated(ctx, FactsFilter{Offset: 50})
	if resOff.Facts != nil {
		t.Errorf("expected nil facts for offset beyond total, got %+v", resOff)
	}

	threadFacts, _ := s.GetFactsByThreadWithEmbeddings(ctx, "th-facts")
	if len(threadFacts) != 3 {
		t.Errorf("GetFactsByThreadWithEmbeddings = %d, want 3", len(threadFacts))
	}

	missing, _ := s.GetFactsMissingEmbeddings(ctx, 10)
	if len(missing) != 1 || missing[0].ID != id3 {
		t.Errorf("GetFactsMissingEmbeddings = %+v, want id3", missing)
	}

	_ = s.UpdateFactEmbedding(ctx, id3, emb3)
	if err := s.UpdateFactEmbedding(ctx, 9999, emb3); !errors.Is(err, ErrFactNotFound) {
		t.Errorf("expected ErrFactNotFound, got %v", err)
	}
	if missAfter, _ := s.GetFactsMissingEmbeddings(ctx, 10); len(missAfter) != 0 {
		t.Errorf("GetFactsMissingEmbeddings after update = %d, want 0", len(missAfter))
	}

	_ = s.ReinforceFact(ctx, id1, "Likes iced matcha latte", emb1, 0.1)
	if err := s.ReinforceFact(ctx, 9999, "", nil, 0.1); !errors.Is(err, ErrFactNotFound) {
		t.Errorf("expected ErrFactNotFound on reinforce, got %v", err)
	}

	decayed, pruned, err := s.DecayAndPruneFacts(ctx, 0.2, 0.6, 0)
	if err != nil || decayed != 3 || pruned == 0 {
		t.Errorf("DecayAndPruneFacts = decayed %d, pruned %d, err %v", decayed, pruned, err)
	}

	_ = s.UpdateConversationFactWatermark(ctx, "th-done", 42)
	_ = s.UpdateConversationFactExtractedAt(ctx, "th-done")
	adminMeta := MessageMetadata{IsAdmin: true}
	_ = s.InsertMessage(ctx, Message{ID: "m-ext-0", ThreadID: "th-done", Status: StatusCompleted, Content: "q0", ResponseText: "a0", Metadata: adminMeta})
	_ = s.InsertMessage(ctx, Message{ID: "m-ext-1", ThreadID: "th-facts", Status: StatusCompleted, Content: "q1", ResponseText: "a1", Metadata: adminMeta})
	_ = s.InsertMessage(ctx, Message{ID: "m-ext-2", ThreadID: "th-facts", Status: StatusCompleted, Content: "q2", ResponseText: "[AMBIENT_SILENT]", Metadata: adminMeta})
	_ = s.InsertMessage(ctx, Message{ID: "m-ext-3", ThreadID: "th-other", Status: StatusCompleted, Content: "q3", ResponseText: "a3", Metadata: adminMeta})
	activeThreads, _ := s.GetActiveConversationsForExtraction(ctx, 24)
	if len(activeThreads) != 2 {
		t.Errorf("GetActiveConversationsForExtraction = %d (%v), want 2", len(activeThreads), activeThreads)
	}
	turns, _ := s.GetUnextractedAdminTurns(ctx, "th-facts")
	if len(turns) != 1 || turns[0].ID != "m-ext-1" {
		t.Errorf("GetUnextractedAdminTurns = %+v, want m-ext-1", turns)
	}
	emptyTurns, _ := s.GetUnextractedAdminTurns(ctx, "   ")
	if len(emptyTurns) != 0 {
		t.Errorf("expected empty turns, got %+v", emptyTurns)
	}
}

func TestFakeStoreTranscriptsPRAndVanadis(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	summary := SessionSummary{SessionID: "sess-1", ThreadID: "th-1", Summary: "Cockpit debug session"}
	if err := s.UpsertSessionSummary(ctx, summary); err != nil {
		t.Fatalf("UpsertSessionSummary failed: %v", err)
	}
	syncStates, err := s.GetSessionSyncStates(ctx)
	if err != nil || len(syncStates) != 1 || syncStates["sess-1"].SessionID != "sess-1" {
		t.Fatalf("GetSessionSyncStates failed: %v", err)
	}
	steps := []TranscriptStep{
		{SessionID: "sess-1", StepIndex: 0, StepType: "user", Content: "launch aerial"},
		{SessionID: "sess-1", StepIndex: 1, StepType: "assistant", ToolName: "fly", Content: "flying aerial"},
	}
	if err := s.BatchInsertTranscriptSteps(ctx, steps); err != nil {
		t.Fatalf("BatchInsertTranscriptSteps failed: %v", err)
	}
	stepResults, _ := s.SearchTranscriptSteps(ctx, "aerial", "sess-1", "", 10)
	if len(stepResults) != 2 {
		t.Errorf("SearchTranscriptSteps = %d, want 2", len(stepResults))
	}
	summaries, _ := s.SearchSessionSummaries(ctx, nil, "debug", 10, 0.0)
	if len(summaries) != 1 {
		t.Errorf("SearchSessionSummaries = %d, want 1", len(summaries))
	}

	pr := PRRecord{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat/test", HeadSHA: "abc1234", TargetID: "th-1", Status: "open", Title: "Test PR"}
	if err := s.UpsertPR(ctx, pr); err != nil {
		t.Fatalf("UpsertPR failed: %v", err)
	}
	gotPR, err := s.GetPRByNumber(ctx, "azylman/aerial", 101)
	if err != nil || gotPR.Title != "Test PR" {
		t.Fatalf("GetPRByNumber failed: %v, pr=%+v", err, gotPR)
	}
	_ = s.UpdatePRStatusMessageID(ctx, "azylman/aerial", 101, "msg-status-1")
	gotPR, _ = s.GetPRByNumber(ctx, "azylman/aerial", 101)
	if gotPR.StatusMessageID != "msg-status-1" {
		t.Errorf("expected StatusMessageID 'msg-status-1', got %q", gotPR.StatusMessageID)
	}

	build := VanadisBuild{ID: "b-1", ThreadID: "th-1", Tier: 2, Title: "Vanadis Build 1", Objective: "Test Objective", Status: "running"}
	chunks := []VanadisChunk{
		{ID: "c-1", BuildID: "b-1", ChunkIndex: 0, Repo: "aerial", Title: "Chunk 1", Instructions: "Execute chunk 1", Status: "running"},
	}
	if err := s.CreateVanadisBuild(ctx, build, chunks); err != nil {
		t.Fatalf("CreateVanadisBuild failed: %v", err)
	}
	gotBuild, gotChunks, err := s.GetVanadisBuild(ctx, "b-1")
	if err != nil || gotBuild.ID != "b-1" || len(gotChunks) != 1 {
		t.Fatalf("GetVanadisBuild failed: %v", err)
	}
	chunk, err := s.GetVanadisChunk(ctx, "c-1")
	if err != nil || chunk.Title != "Chunk 1" {
		t.Fatalf("GetVanadisChunk failed: %v", err)
	}
	_ = s.AttachVanadisChunkPR(ctx, "c-1", 101)
	chunk, _ = s.GetVanadisChunk(ctx, "c-1")
	if chunk.PRNumber != 101 || chunk.Status != "pr_opened" {
		t.Errorf("unexpected chunk after attach: %+v", chunk)
	}
	_ = s.UpdateVanadisBuildStatusMessageID(ctx, "b-1", "msg-build-status")
	gotBuild, _, _ = s.GetVanadisBuild(ctx, "b-1")
	if gotBuild.StatusMessageID != "msg-build-status" {
		t.Errorf("expected StatusMessageID 'msg-build-status', got %q", gotBuild.StatusMessageID)
	}
}

func TestFakeStoreCloneNil(t *testing.T) {
	if got := cloneMessage(nil); got != nil {
		t.Errorf("cloneMessage(nil) = %v, want nil", got)
	}
	if got := clonePtr[SessionInfo](nil); got != nil {
		t.Errorf("clonePtr[SessionInfo](nil) = %v, want nil", got)
	}
	if got := cloneRun(nil); got != nil {
		t.Errorf("cloneRun(nil) = %v, want nil", got)
	}
	if got := cloneFactWithEmbedding(nil); got != nil {
		t.Errorf("cloneFactWithEmbedding(nil) = %v, want nil", got)
	}
	if got := cloneSessionSummary(nil); got != nil {
		t.Errorf("cloneSessionSummary(nil) = %v, want nil", got)
	}
	if got := cloneTranscriptSteps(nil); got != nil {
		t.Errorf("cloneTranscriptSteps(nil) = %v, want nil", got)
	}
	if got := cloneSliceNonNil[string](nil); got == nil || len(got) != 0 {
		t.Errorf("cloneSliceNonNil(nil) = %v, want empty slice", got)
	}
	if got := cloneSliceNonNil([]string{"a"}); len(got) != 1 || got[0] != "a" {
		t.Errorf("cloneSliceNonNil non-nil failed: %v", got)
	}
	chunkNoEffort := &VanadisChunk{ID: "c1", TargetFiles: []string{"f1"}}
	clonedChunk := cloneVanadisChunk(chunkNoEffort)
	if clonedChunk.Effort != "medium" || len(clonedChunk.TargetFiles) != 1 {
		t.Errorf("cloneVanadisChunk failed: %+v", clonedChunk)
	}
}

func TestFakeStoreTestHelpers(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	s.SetSessionInfo(SessionInfo{ThreadID: "th-helper", InternalSessionID: "sess-helper", TurnCount: 5, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-10 * time.Minute)})
	sessID, _ := s.GetSessionID(ctx, "th-helper")
	turns, _ := s.GetSessionTurnCount(ctx, "th-helper")
	if sessID != "sess-helper" || turns != 5 {
		t.Errorf("SetSessionInfo: sessID=%q, turns=%d", sessID, turns)
	}
	s.SetSessionUpdatedAt("th-helper", now.Add(-5*time.Minute))
	s.SetSessionUpdatedAt("non-existent-sess", now)
	prevSess, _ := s.GetPreviousSessionID(ctx, "th-helper")
	_ = prevSess
	prevNone, _ := s.GetPreviousSessionID(ctx, "non-existent-sess")
	_ = prevNone

	_ = s.InsertMessage(ctx, Message{ID: "m-helper", ThreadID: "th-helper", Status: StatusPending})
	customTime := now.Add(-30 * time.Minute)
	s.SetMessageUpdatedAt("m-helper", customTime)
	s.SetMessageUpdatedAt("non-existent", customTime)
	m, _ := s.GetMessage(ctx, "m-helper")
	if m == nil || !m.UpdatedAt.Equal(customTime) {
		t.Errorf("SetMessageUpdatedAt failed: m=%+v", m)
	}
	recent0, _ := s.GetRecentThreadMessages(ctx, "th-helper", 0)
	_ = recent0
	recent10, _ := s.GetRecentThreadMessages(ctx, "th-helper", 10)
	_ = recent10

	if _, err := s.GetScheduleRun("non-existent-run"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("expected sql.ErrNoRows for non-existent run, got: %v", err)
	}
	_ = s.CreateScheduleRun(ctx, ScheduleRun{ID: "run-helper", ScheduleID: "sched-helper", Status: "completed", StartedAt: now})
	run, err := s.GetScheduleRun("run-helper")
	if err != nil || run == nil || run.Status != "completed" {
		t.Errorf("GetScheduleRun failed: run=%+v, err=%v", run, err)
	}

	// PruneScheduleRuns defaults & count cap
	_, _ = s.PruneScheduleRuns(ctx, 0, 0)
	for i := 0; i < 5; i++ {
		_ = s.CreateScheduleRun(ctx, ScheduleRun{ID: fmt.Sprintf("run-limit-%d", i), ScheduleID: "sched-helper", Status: "completed", StartedAt: now.Add(time.Duration(i) * time.Minute)})
	}
	_, _ = s.PruneScheduleRuns(ctx, 2, time.Hour)

	// Facts: tiebreaker sort, defaults, search modes
	_, _ = s.InsertFact(ctx, "cat", "Fact A", 0.5, []float32{1.0, 0.0})
	_, _ = s.InsertFact(ctx, "cat", "Fact B", 0.5, []float32{0.0, 1.0})
	fbth, err := s.GetFactsByThreadWithEmbeddings(ctx, "any")
	if err != nil || len(fbth) < 2 {
		t.Errorf("GetFactsByThreadWithEmbeddings failed: %v, len=%d", err, len(fbth))
	}
	_, _, _ = s.DecayAndPruneFacts(ctx, 0, 0, 0)
	none, _ := s.SearchSimilarFacts(ctx, nil, "", 0, 0)
	if none != nil {
		t.Errorf("expected nil for empty vec and query")
	}
	_, _ = s.SearchSimilarFacts(ctx, []float32{1.0, 0.0}, "", 0, 0)
	_, _ = s.SearchSimilarFacts(ctx, nil, "Fact", 0, 0)
	_, _ = s.SearchSimilarFacts(ctx, []float32{1.0, 0.0}, "Fact", 0, 0)
	_, _, _ = s.FindDuplicateFact(ctx, []float32{1.0, 0.0}, 0)
}
