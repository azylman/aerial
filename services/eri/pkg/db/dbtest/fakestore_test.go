package dbtest

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
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
	requireErr(t, c1(s.HasActiveThreadTurn(ctx, "t")), "HasActiveThreadTurn")
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
	requireErr(t, c1(s.FindUnrotatedSessions(ctx, 8)), "FindUnrotatedSessions")
	requireErr(t, c1(s.AcquireLeaderLock(ctx)), "AcquireLeaderLock")
}

func TestFakeStore_HasActiveThreadTurn(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	active, err := s.HasActiveThreadTurn(ctx, "")
	if err != nil || active {
		t.Fatalf("expected false, nil for empty threadID, got %v, %v", active, err)
	}

	active, err = s.HasActiveThreadTurn(ctx, "th-1")
	if err != nil || active {
		t.Fatalf("expected false, nil for empty store, got %v, %v", active, err)
	}

	mustOK(t, s.InsertMessage(ctx, db.Message{ID: "m1", ThreadID: "th-1", Status: db.StatusPending}), "InsertMessage")
	active, err = s.HasActiveThreadTurn(ctx, "th-1")
	if err != nil || !active {
		t.Fatalf("expected true, nil for pending message, got %v, %v", active, err)
	}

	mustOK(t, s.UpdateMessageStatus(ctx, "m1", db.StatusProcessing, ""), "UpdateMessageStatus")
	active, err = s.HasActiveThreadTurn(ctx, "th-1")
	if err != nil || !active {
		t.Fatalf("expected true, nil for processing message, got %v, %v", active, err)
	}

	mustOK(t, s.UpdateMessageStatus(ctx, "m1", db.StatusCompleted, ""), "UpdateMessageStatus")
	active, err = s.HasActiveThreadTurn(ctx, "th-1")
	if err != nil || active {
		t.Fatalf("expected false, nil for completed message, got %v, %v", active, err)
	}
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

	if rel, err := s.AcquireLeaderLock(ctx); err != nil || rel == nil {
		t.Fatalf("expected AcquireLeaderLock success, got %v", err)
	} else {
		rel()
	}
	var hookCalled bool
	s.SetLeaderLockHook(func(ctx context.Context) (func(), error) {
		hookCalled = true
		return func() {}, nil
	})
	if rel, err := s.AcquireLeaderLock(ctx); err != nil || rel == nil || !hookCalled {
		t.Fatalf("expected custom leaderLockHook called, got hookCalled=%v, err=%v", hookCalled, err)
	}
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.AcquireLeaderLock(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from AcquireLeaderLock, got %v", err)
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
	if p1, err := s.GetPendingOrProcessingMessages(ctx, 1); err != nil || len(p1) != 1 {
		t.Errorf("GetPendingOrProcessingMessages(limit=1) = %+v, %v", p1, err)
	}
	if threadIDs, err := s.GetActiveRecentThreadIDs(ctx, time.Hour); err != nil || len(threadIDs) != 1 {
		t.Errorf("GetActiveRecentThreadIDs = %v, %v", threadIDs, err)
	}
	if recent, err := s.GetRecentThreadMessages(ctx, "th-1", 10); err != nil || len(recent) != 2 {
		t.Errorf("GetRecentThreadMessages = %+v, %v", recent, err)
	}
	if r1, err := s.GetRecentThreadMessages(ctx, "th-1", 1); err != nil || len(r1) != 1 {
		t.Errorf("GetRecentThreadMessages(limit=1) = %+v, %v", r1, err)
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

func TestFakeStore_GetActiveTasks_FiltersAndOrdering(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	_ = s.SaveSessionID(ctx, "th-filter", "prev-sess-0")
	_ = s.RotateSessionID(ctx, "th-filter", "int-sess-1")
	_ = s.SaveThreadSummary(ctx, "th-filter", "Summary of thread filter", "w")

	adminTrue := true
	adminFalse := false

	_ = s.InsertMessage(ctx, db.Message{
		ID:           "msg-f1",
		ThreadID:     "th-filter",
		Content:      "Prompt 1",
		Summary:      "Summary 1",
		ResponseText: "Response 1",
		Status:       db.StatusCompleted,
		IsAdmin:      true,
		CreatedAt:    now.Add(-3 * time.Minute),
		UpdatedAt:    now.Add(-3 * time.Minute),
	})
	_ = s.InsertMessage(ctx, db.Message{
		ID:           "msg-f2",
		ThreadID:     "th-filter",
		Content:      "Prompt 2",
		Summary:      "Summary 2",
		ResponseText: "Response 2",
		Status:       db.StatusPending,
		Effort:       "medium",
		IsAdmin:      false,
		CreatedAt:    now.Add(-2 * time.Minute),
		UpdatedAt:    now.Add(-2 * time.Minute),
	})
	_ = s.InsertMessage(ctx, db.Message{
		ID:           "msg-f3",
		ThreadID:     "th-other",
		Content:      "Prompt 3",
		Summary:      "Summary 3",
		ResponseText: "Response 3",
		Status:       db.StatusProcessing,
		IsAdmin:      true,
		CreatedAt:    now.Add(-1 * time.Minute),
		UpdatedAt:    now.Add(-1 * time.Minute),
	})

	tests := []struct {
		name          string
		filter        db.TasksFilter
		wantCount     int
		wantFirstID   string
		wantSess      string
		checkSess     bool
		wantPrevSess  string
		wantThreadSum string
		wantRespText  string
	}{
		{
			name:          "thread_id filter and session join",
			filter:        db.TasksFilter{ThreadID: "th-filter", Statuses: []string{"COMPLETED"}},
			wantCount:     1,
			wantFirstID:   "msg-f1",
			wantSess:      "int-sess-1",
			checkSess:     true,
			wantPrevSess:  "prev-sess-0",
			wantThreadSum: "Summary of thread filter",
			wantRespText:  "Response 1",
		},
		{
			name:        "is_admin false filter matches thread session",
			filter:      db.TasksFilter{IsAdmin: &adminFalse},
			wantCount:   1,
			wantFirstID: "msg-f2",
			wantSess:    "int-sess-1",
			checkSess:   true,
		},
		{
			name:        "is_admin true filter with status",
			filter:      db.TasksFilter{IsAdmin: &adminTrue, Statuses: []string{"PROCESSING"}},
			wantCount:   1,
			wantFirstID: "msg-f3",
		},
		{
			name:        "multiple statuses filter",
			filter:      db.TasksFilter{Statuses: []string{"PENDING", "PROCESSING"}},
			wantCount:   2,
			wantFirstID: "msg-f3",
		},
		{
			name:        "after_row_id orders asc",
			filter:      db.TasksFilter{AfterRowID: 1},
			wantCount:   2,
			wantFirstID: "msg-f2",
		},
		{
			name:        "since filter matches recent",
			filter:      db.TasksFilter{Since: now.Add(-90 * time.Second)},
			wantCount:   1,
			wantFirstID: "msg-f3",
		},
		{
			name:        "before_row_id filter",
			filter:      db.TasksFilter{BeforeRowID: 1},
			wantCount:   1,
			wantFirstID: "msg-f1",
		},
		{
			name:        "clamped limit above 200",
			filter:      db.TasksFilter{Limit: 250},
			wantCount:   3,
			wantFirstID: "msg-f3",
		},
		{
			name:      "unmatched status",
			filter:    db.TasksFilter{Statuses: []string{"FAILED"}},
			wantCount: 0,
		},
		{
			name:      "unmatched thread",
			filter:    db.TasksFilter{ThreadID: "nonexistent"},
			wantCount: 0,
		},
		{
			name:      "unmatched since",
			filter:    db.TasksFilter{Since: now.Add(time.Hour)},
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tasks, err := s.GetActiveTasks(ctx, tc.filter)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tasks) != tc.wantCount {
				t.Fatalf("got count %d, want %d", len(tasks), tc.wantCount)
			}
			if tc.wantCount > 0 {
				if tasks[0].ID != tc.wantFirstID {
					t.Errorf("got first ID %q, want %q", tasks[0].ID, tc.wantFirstID)
				}
				if tc.checkSess && tasks[0].SessionID != tc.wantSess {
					t.Errorf("got SessionID %q, want %q", tasks[0].SessionID, tc.wantSess)
				}
				if tc.wantPrevSess != "" && tasks[0].PreviousSessionID != tc.wantPrevSess {
					t.Errorf("got PreviousSessionID %q, want %q", tasks[0].PreviousSessionID, tc.wantPrevSess)
				}
				if tc.wantThreadSum != "" && tasks[0].ThreadSummary != tc.wantThreadSum {
					t.Errorf("got ThreadSummary %q, want %q", tasks[0].ThreadSummary, tc.wantThreadSum)
				}
				if tc.wantRespText != "" && tasks[0].ResponseText != tc.wantRespText {
					t.Errorf("got ResponseText %q, want %q", tasks[0].ResponseText, tc.wantRespText)
				}
			}
		})
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

	for i := 0; i < 8; i++ {
		_, _ = s.IncrementSessionTurnCount(ctx, "th-unrotated")
	}
	for i := 0; i < 12; i++ {
		_, _ = s.IncrementSessionTurnCount(ctx, "th-unrotated-2")
	}
	s.SetSessionUpdatedAt("th-unrotated", time.Now().UTC().Add(-2*time.Hour))
	s.SetSessionUpdatedAt("th-unrotated-2", time.Now().UTC().Add(-2*time.Hour))
	if unrot, _ := s.FindUnrotatedSessions(ctx, 5); len(unrot) != 0 {
		t.Errorf("expected 0 unrotated sessions within 1h, got %d", len(unrot))
	}
	if unrotLong, _ := s.FindUnrotatedSessions(ctx, 5, 4*time.Hour); len(unrotLong) != 2 {
		t.Errorf("expected 2 unrotated sessions within 4h, got %d", len(unrotLong))
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

func TestFakeStoreTranscriptsPRAndVanadis(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	mustOK(t, s.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-1", ThreadID: "th-1", Summary: "Cockpit debug session"}), "UpsertSessionSummary")
	mustOK(t, s.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-1", LastMtime: time.Now().UTC()}), "UpsertSessionSummary with LastMtime")
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
}

func TestFakeStoreCloneNil(t *testing.T) {
	if cloneSlice[int](nil) != nil || len(cloneSlice([]int{1, 2, 3})) != 3 {
		t.Errorf("unexpected cloneSlice results")
	}
	if cloneMessage(nil) != nil || clonePtr[db.SessionInfo](nil) != nil ||
		cloneSessionSummary(nil) != nil {
		t.Errorf("expected nil clones for nil inputs")
	}
	if got := cloneSliceNonNil[string](nil); got == nil || len(got) != 0 {
		t.Errorf("cloneSliceNonNil(nil) = %v, want empty slice", got)
	}
	if got := cloneSliceNonNil([]string{"a"}); len(got) != 1 || got[0] != "a" {
		t.Errorf("cloneSliceNonNil non-nil failed: %v", got)
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
		{SessionID: "sess-steps-b", StepIndex: 1, StepType: "tool_result", ToolName: "read_file", Content: "done reading configuration", CreatedAt: now},
	}
	if err := store.BatchInsertTranscriptSteps(ctx, steps); err != nil {
		t.Fatalf("BatchInsertTranscriptSteps failed: %v", err)
	}

	tests := []struct {
		name, query, session, tool string
		since                      time.Time
		limit, wantLen, wantStep   int
	}{
		{name: "text search", query: "bad gateway", limit: 10, wantLen: 1, wantStep: 2},
		{name: "multi word non-contiguous search", query: "error webhook", limit: 10, wantLen: 1, wantStep: 2},
		{name: "session filter", query: "ingress", session: "sess-steps-b", limit: 10, wantLen: 1},
		{name: "tool filter", query: "ingress", tool: "read_file", limit: 10, wantLen: 1},
		{name: "empty query tool filter", tool: "read_file", limit: 10, wantLen: 2},
		{name: "empty query and filters", limit: 10},
		{name: "since filter without query orders asc", since: now.Add(-5 * time.Minute), limit: 10, wantLen: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := store.SearchTranscriptSteps(ctx, tc.query, tc.session, tc.tool, tc.limit, tc.since)
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

func TestFakeStore_DiscordSessionIDNormalization(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	// 1. Save and Get session ID with raw numeric snowflake
	mustOK(t, s.SaveSessionID(ctx, "1234567890", "conv-1"), "SaveSessionID raw")
	sessID, err := s.GetSessionID(ctx, "1234567890")
	mustOK(t, err, "GetSessionID raw")
	if sessID != "conv-1" {
		t.Errorf("expected conv-1, got %s", sessID)
	}

	// 2. Fetch using discord-prefixed key
	sessIDPrefixed, err := s.GetSessionID(ctx, "discord-1234567890")
	mustOK(t, err, "GetSessionID prefixed")
	if sessIDPrefixed != "conv-1" {
		t.Errorf("expected conv-1, got %s", sessIDPrefixed)
	}

	// 3. Turn counts and rotation
	turns, err := s.IncrementSessionTurnCount(ctx, "1234567890")
	mustOK(t, err, "IncrementSessionTurnCount")
	if turns != 1 {
		t.Errorf("expected 1 turn, got %d", turns)
	}

	mustOK(t, s.RotateSessionID(ctx, "1234567890", "conv-2"), "RotateSessionID")
	newTurns, _ := s.GetSessionTurnCount(ctx, "discord-1234567890")
	if newTurns != 0 {
		t.Errorf("expected 0 turns after rotation, got %d", newTurns)
	}
	prevID, _ := s.GetPreviousSessionID(ctx, "1234567890")
	if prevID != "conv-1" {
		t.Errorf("expected prevID conv-1, got %s", prevID)
	}
}

func TestFakeStore_DispatchStoreLifecycle(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	// 1. Insert & Get
	d1 := db.Dispatch{
		ID:        "disp-f1",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-1",
		Prompt:    "Prompt for fake dispatch 1",
	}
	mustOK(t, s.InsertDispatch(ctx, d1), "InsertDispatch")

	got, err := s.GetDispatch(ctx, "disp-f1")
	mustOK(t, err, "GetDispatch")
	if got == nil || got.ID != "disp-f1" || got.Status != db.StatusPending || got.Summary == "" {
		t.Fatalf("unexpected dispatch: %+v", got)
	}

	// 2. Claim pending
	claimed, err := s.ClaimPendingDispatch(ctx, "disp-f1")
	mustOK(t, err, "ClaimPendingDispatch")
	if !claimed {
		t.Errorf("expected claimed=true")
	}
	claimedAgain, err := s.ClaimPendingDispatch(ctx, "disp-f1")
	mustOK(t, err, "ClaimPendingDispatch again")
	if claimedAgain {
		t.Errorf("expected claimed=false for already processing dispatch")
	}

	// 3. Update status & retry
	mustOK(t, s.UpdateDispatchStatus(ctx, "disp-f1", "PROCESSING", "in flight"), "UpdateDispatchStatus")
	mustOK(t, s.IncrementDispatchRetry(ctx, "disp-f1", "network error"), "IncrementDispatchRetry")
	got, _ = s.GetDispatch(ctx, "disp-f1")
	if got.RetryCount != 1 || got.ErrorMessage != "network error" {
		t.Errorf("unexpected retry state: %+v", got)
	}

	// 4. Reset to pending with restart
	mustOK(t, s.ResetDispatchToPendingWithRestart(ctx, "disp-f1", "runner restart"), "ResetDispatchToPendingWithRestart")
	got, _ = s.GetDispatch(ctx, "disp-f1")
	if got.Status != db.StatusPending || got.RestartCount != 1 {
		t.Errorf("unexpected restart state: %+v", got)
	}

	// 5. Update completed with conversation ID
	mustOK(t, s.UpdateDispatchCompleted(ctx, "disp-f1", "Result text", "conv-f123"), "UpdateDispatchCompleted")
	got, _ = s.GetDispatch(ctx, "disp-f1")
	if got.Status != db.StatusCompleted || got.ResponseText != "Result text" || got.ConversationID != "conv-f123" {
		t.Errorf("unexpected completed state: %+v", got)
	}

	// 6. Recent session dispatches
	d2 := db.Dispatch{
		ID:        "disp-f2",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-1",
		Prompt:    "Prompt for fake dispatch 2",
	}
	mustOK(t, s.InsertDispatch(ctx, d2), "InsertDispatch disp-f2")
	mustOK(t, s.UpdateDispatchCompleted(ctx, "disp-f2", "Result 2"), "UpdateDispatchCompleted disp-f2")

	recent, err := s.GetRecentSessionDispatches(ctx, "heartbeat-hb-1", 10)
	mustOK(t, err, "GetRecentSessionDispatches")
	if len(recent) != 2 || recent[0].ID != "disp-f1" || recent[1].ID != "disp-f2" {
		t.Errorf("unexpected recent session dispatches: %+v", recent)
	}

	// 7. Active check
	d3 := db.Dispatch{
		ID:        "disp-f3",
		Source:    "vanadis",
		SessionID: "build-1",
		Prompt:    "Build ID: b-999\nChunk 1",
	}
	mustOK(t, s.InsertDispatch(ctx, d3), "InsertDispatch disp-f3")
	hasActive, err := s.HasActiveDispatchTurn(ctx, "build-1", "")
	mustOK(t, err, "HasActiveDispatchTurn")
	if !hasActive {
		t.Errorf("expected active dispatch for session build-1")
	}

	// 8. Cancel pending dispatches
	cancelled, err := s.CancelPendingDispatches(ctx, "", "b-999", "cancelled build")
	mustOK(t, err, "CancelPendingDispatches")
	if len(cancelled) != 1 || cancelled[0] != "disp-f3" {
		t.Errorf("expected cancelled [disp-f3], got %v", cancelled)
	}
	got, _ = s.GetDispatch(ctx, "disp-f3")
	if got.Status != db.StatusFailed || got.ErrorMessage != "cancelled build" {
		t.Errorf("unexpected status after cancel: %+v", got)
	}
}

func TestFakeStore_GetActiveTasks_DispatchesUnion(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)

	mustOK(t, s.InsertMessage(ctx, db.Message{
		ID:        "msg-fake-1",
		ThreadID:  "123456",
		Content:   "Discord user prompt",
		Status:    db.StatusPending,
		CreatedAt: now.Add(-2 * time.Minute),
		UpdatedAt: now.Add(-2 * time.Minute),
	}), "InsertMessage")

	mustOK(t, s.InsertDispatch(ctx, db.Dispatch{
		ID:        "disp-fake-stateful",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-1",
		Prompt:    "Heartbeat prompt",
		Status:    db.StatusPending,
		CreatedAt: now.Add(-1 * time.Minute),
		UpdatedAt: now.Add(-1 * time.Minute),
	}), "InsertDispatch stateful")

	mustOK(t, s.InsertDispatch(ctx, db.Dispatch{
		ID:        "disp-fake-ephem",
		Source:    "webhook",
		SessionID: "",
		Prompt:    "Webhook prompt",
		Status:    db.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}), "InsertDispatch ephem")

	// Unfiltered query unions messages and dispatches
	unfiltered, err := s.GetActiveTasks(ctx)
	mustOK(t, err, "GetActiveTasks unfiltered")
	if len(unfiltered) != 3 {
		t.Fatalf("expected 3 tasks in unfiltered HUD, got %d", len(unfiltered))
	}
	if unfiltered[0].ID != "msg-fake-1" || unfiltered[1].ID != "disp-fake-stateful" || unfiltered[2].ID != "disp-fake-ephem" {
		t.Errorf("unexpected task order: %+v", unfiltered)
	}
	if unfiltered[1].TriggerType != "heartbeat" || unfiltered[2].TriggerType != "webhook" {
		t.Errorf("unexpected trigger types: %s, %s", unfiltered[1].TriggerType, unfiltered[2].TriggerType)
	}
	if unfiltered[2].ThreadID != "dispatch-disp-fake-ephem" {
		t.Errorf("unexpected threadID for ephemeral dispatch: %s", unfiltered[2].ThreadID)
	}

	// Filtered query excludes dispatches
	filtered, err := s.GetActiveTasks(ctx, db.TasksFilter{Statuses: []string{"PENDING"}})
	mustOK(t, err, "GetActiveTasks filtered")
	if len(filtered) != 1 || filtered[0].ID != "msg-fake-1" {
		t.Errorf("expected filtered tasks to contain only messages, got %+v", filtered)
	}
}

func TestFakeStore_GetActiveRecentThreadIDs_StrippingDiscordPrefix(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	mustOK(t, s.SaveSessionID(ctx, "112233445566778899", "conv-sess-1"), "SaveSessionID numeric")
	mustOK(t, s.SaveSessionID(ctx, "discord-998877665544332211", "conv-sess-2"), "SaveSessionID discord prefixed")

	threadIDs, err := s.GetActiveRecentThreadIDs(ctx, time.Hour)
	mustOK(t, err, "GetActiveRecentThreadIDs")

	found1 := slices.Contains(threadIDs, "112233445566778899")
	found2 := slices.Contains(threadIDs, "998877665544332211")
	if !found1 || !found2 {
		t.Errorf("GetActiveRecentThreadIDs missing stripped thread IDs: got %v", threadIDs)
	}
}
