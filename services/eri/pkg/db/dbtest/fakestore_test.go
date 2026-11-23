package dbtest

import (
	"context"
	"database/sql"
	"errors"
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
	requireErr(t, s.UpdateMessageCompleted(ctx, "id", "", ""), "UpdateMessageCompleted")
	requireErr(t, c1(s.GetMessage(ctx, "id")), "GetMessage")
	requireErr(t, c1(s.MessageExists(ctx, "id")), "MessageExists")
	requireErr(t, c1(s.ClaimPendingMessage(ctx, "id")), "ClaimPendingMessage")
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
	requireErr(t, c1(s.GetMessagesByID(ctx, []string{"id"})), "GetMessagesByID")
	requireErr(t, c1(s.FailMessagesIfActive(ctx, []string{"id"}, "r")), "FailMessagesIfActive")
	requireErr(t, c1(s.FailMessagesIfPending(ctx, []string{"id"}, "r")), "FailMessagesIfPending")
	requireErr(t, c1(s.RequeueProcessingMessages(ctx, []string{"id"}, "r")), "RequeueProcessingMessages")
	requireErr(t, c1(s.BeginMessageAttempt(ctx, "id", 1)), "BeginMessageAttempt")
	requireErr(t, c1(s.FailDispatchesIfActive(ctx, []string{"id"}, "r")), "FailDispatchesIfActive")
	requireErr(t, c1(s.FailDispatchesIfPending(ctx, []string{"id"}, "r")), "FailDispatchesIfPending")
	requireErr(t, c1(s.BeginDispatchAttempt(ctx, "id", 1)), "BeginDispatchAttempt")
	requireErr(t, c1(s.ListActiveDispatchSessionKeys(ctx, "d", "b")), "ListActiveDispatchSessionKeys")
}

func TestFakeStore_UpdateMessageStatusKeepsTelemetry(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	mustOK(t, s.InsertMessage(ctx, db.Message{ID: "m1", ThreadID: "th-1", Status: db.StatusPending}), "InsertMessage")
	mustOK(t, s.UpdateMessageStatus(ctx, "m1", db.StatusProcessing, "busy", db.TurnTelemetry{DurationMs: 100, TotalTokens: 50}), "UpdateMessageStatus with telemetry")
	mustOK(t, s.UpdateMessageStatus(ctx, "m1", db.StatusCompleted, ""), "UpdateMessageStatus without telemetry")
	msgM1, _ := s.GetMessage(ctx, "m1")
	if msgM1.Status != db.StatusCompleted || msgM1.DurationMs != 100 || msgM1.TotalTokens != 50 {
		t.Errorf("expected completed with telemetry preserved on m1, got %+v", msgM1)
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

	mustOK(t, s.Close(), "Close")
	assertStoreUnavailable(t, s)
}

func assertFakeStoreMessageStatusTransitions(ctx context.Context, t *testing.T, s *FakeStore) {
	t.Helper()
	_ = s.UpdateMessageStatus(ctx, "msg-1", db.StatusProcessing, "busy")
	_ = s.UpdateMessageStatus(ctx, "msg-none", db.StatusProcessing, "")
	if msg, _ := s.GetMessage(ctx, "msg-1"); msg.Status != db.StatusProcessing || msg.ErrorMessage != "busy" {
		t.Errorf("expected processing, got %+v", msg)
	}
	if ok, _ := s.ClaimPendingMessage(ctx, "msg-1"); ok {
		t.Errorf("ClaimPendingMessage on processing row = true, want false")
	}

	_ = s.UpdateMessageStatus(ctx, "msg-1", db.StatusPending, "")
	if ok, err := s.ClaimPendingMessage(ctx, "msg-1"); err != nil || !ok {
		t.Errorf("ClaimPendingMessage(msg-1) = %v, %v, want true, nil", ok, err)
	}
	if ok, _ := s.ClaimPendingMessage(ctx, "msg-1"); ok {
		t.Errorf("ClaimPendingMessage already claimed = true, want false")
	}
	if ok, _ := s.ClaimPendingMessage(ctx, "msg-none"); ok {
		t.Errorf("ClaimPendingMessage non-existent = true, want false")
	}

	_ = s.UpdateMessageCompleted(ctx, "msg-1", "Done response", "conv-1", db.TurnTelemetry{DurationMs: 50, TotalTokens: 25})
	_ = s.UpdateMessageCompleted(ctx, "msg-none", "No-op", "")
	if msg, _ := s.GetMessage(ctx, "msg-1"); msg.Status != db.StatusCompleted || msg.ResponseText != "Done response" || msg.ConversationID != "conv-1" || msg.DurationMs != 50 {
		t.Errorf("expected completed with conv-1 and telemetry, got %+v", msg)
	}
}

func assertFakeStoreMessageQueries(ctx context.Context, t *testing.T, s *FakeStore, now time.Time) {
	t.Helper()
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

func TestFakeStoreMessageOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	m1 := db.Message{ID: "msg-1", ThreadID: "th-1", Content: "Hello", Summary: "Summ"}
	mustOK(t, s.InsertMessage(ctx, m1), "InsertMessage")
	mustOK(t, s.InsertMessage(ctx, m1), "Idempotent InsertMessage")
	mustOK(t, s.InsertMessage(ctx, db.Message{ID: ""}), "Empty ID InsertMessage")
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

	assertFakeStoreMessageStatusTransitions(ctx, t, s)
	assertFakeStoreMessageQueries(ctx, t, s, now)
}

type fakeStoreActiveTaskTestCase struct {
	name          string
	filter        db.TasksFilter
	wantCount     int
	wantFirstID   string
	wantSess      string
	checkSess     bool
	wantPrevSess  string
	wantThreadSum string
	wantRespText  string
}

func assertFakeStoreActiveTaskCase(t *testing.T, tasks []db.ActiveTask, tc fakeStoreActiveTaskTestCase) {
	t.Helper()
	if tc.wantCount == 0 {
		return
	}
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

	tests := []fakeStoreActiveTaskTestCase{
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
			assertFakeStoreActiveTaskCase(t, tasks, tc)
		})
	}
}

func assertFakeStoreUnrotatedSessions(ctx context.Context, t *testing.T, s *FakeStore, now time.Time) {
	t.Helper()
	s.SetSessionInfo(db.SessionInfo{ThreadID: "th-helper", InternalSessionID: "sess-helper", TurnCount: 5, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-10 * time.Minute)})
	s.SetSessionUpdatedAt("th-helper", now.Add(-5*time.Minute))
	if sessID, _ := s.GetSessionID(ctx, "th-helper"); sessID != "sess-helper" {
		t.Errorf("SetSessionInfo: sessID=%q", sessID)
	}
}

func assertRotateSessionIDIdempotency(ctx context.Context, t *testing.T, s *FakeStore) {
	t.Helper()
	mustOK(t, s.RotateSessionID(ctx, "th-1", "sess-2"), "RotateSessionID repeated")
	curr, _ := s.GetSessionID(ctx, "th-1")
	prev, _ := s.GetPreviousSessionID(ctx, "th-1")
	if curr != "sess-2" || prev != "sess-1-v2" {
		t.Errorf("post-repeated-rotation state: curr=%q (want sess-2), prev=%q (want sess-1-v2)", curr, prev)
	}
}

func TestFakeStoreSessionOperations(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()

	if id, err := s.GetSessionID(ctx, "th-1"); err != nil || id != "" {
		t.Errorf("expected empty session ID, got %q, %v", id, err)
	}
	if prev, err := s.GetPreviousSessionID(ctx, "th-1"); err != nil || prev != "" {
		t.Errorf("expected empty previous session ID, got %q, %v", prev, err)
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
	assertRotateSessionIDIdempotency(ctx, t, s)
	_ = s.RotateSessionID(ctx, "th-brand-new", "sess-init")

	if sum, wm, err := s.GetThreadSummary(ctx, "th-1"); err != nil || sum != "" || wm != "" {
		t.Errorf("GetThreadSummary empty = %q, %q, %v", sum, wm, err)
	}
	mustOK(t, s.SaveThreadSummary(ctx, "th-1", "Summary text", "msg-100"), "SaveThreadSummary")
	if sum, wm, _ := s.GetThreadSummary(ctx, "th-1"); sum != "Summary text" || wm != "msg-100" {
		t.Errorf("GetThreadSummary = %q, %q", sum, wm)
	}

	_ = s.SaveSessionID(ctx, "ext-1", "int-1")

	assertFakeStoreUnrotatedSessions(ctx, t, s, now)
}

func TestFakeStoreCloneNil(t *testing.T) {
	if cloneSlice[int](nil) != nil || len(cloneSlice([]int{1, 2, 3})) != 3 {
		t.Errorf("unexpected cloneSlice results")
	}
	if cloneMessage(nil) != nil || clonePtr[db.SessionInfo](nil) != nil {
		t.Errorf("expected nil clones for nil inputs")
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

func mustInsertDispatch(ctx context.Context, t *testing.T, store db.Store, d db.Dispatch, msg string) {
	t.Helper()
	_, err := store.InsertDispatch(ctx, d)
	mustOK(t, err, msg)
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
	mustInsertDispatch(ctx, t, s, d1, "InsertDispatch")

	got, err := s.GetDispatch(ctx, "disp-f1")
	mustOK(t, err, "GetDispatch")
	if got == nil || got.ID != "disp-f1" || got.Status != db.StatusPending || got.Summary == "" {
		t.Fatalf("unexpected dispatch: %+v", got)
	}

	missing, err := s.GetDispatch(ctx, "non-existent")
	mustOK(t, err, "GetDispatch missing")
	if missing != nil {
		t.Errorf("expected nil dispatch for non-existent ID, got %+v", missing)
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

	// 3. Update status
	mustOK(t, s.UpdateDispatchStatus(ctx, "disp-f1", "PROCESSING", "in flight", db.TurnTelemetry{DurationMs: 120, TotalTokens: 50}), "UpdateDispatchStatus")
	got, _ = s.GetDispatch(ctx, "disp-f1")
	if got.Status != db.StatusProcessing || got.ErrorMessage != "in flight" {
		t.Errorf("unexpected processing state: %+v", got)
	}

	// 4. Update completed with conversation ID and telemetry
	tel := db.TurnTelemetry{DurationMs: 4000, InputTokens: 400, OutputTokens: 150, ThinkingTokens: 50, CacheReadTokens: 25, TotalTokens: 625}
	mustOK(t, s.UpdateDispatchCompleted(ctx, "disp-f1", "Result text", "conv-f123", tel), "UpdateDispatchCompleted")
	got, _ = s.GetDispatch(ctx, "disp-f1")
	if got.Status != db.StatusCompleted || got.ResponseText != "Result text" || got.ConversationID != "conv-f123" {
		t.Errorf("unexpected completed state: %+v", got)
	}
	if got.DurationMs != 4000 || got.TotalTokens != 625 {
		t.Errorf("unexpected telemetry on fake dispatch: %+v", got)
	}

	// 5. Recent session dispatches
	d2 := db.Dispatch{
		ID:        "disp-f2",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-1",
		Prompt:    "Prompt for fake dispatch 2",
	}
	mustInsertDispatch(ctx, t, s, d2, "InsertDispatch disp-f2")
	mustOK(t, s.UpdateDispatchCompleted(ctx, "disp-f2", "Result 2", ""), "UpdateDispatchCompleted disp-f2")

	recent, err := s.GetRecentSessionDispatches(ctx, "heartbeat-hb-1", 10)
	mustOK(t, err, "GetRecentSessionDispatches")
	if len(recent) != 2 || recent[0].ID != "disp-f1" || recent[1].ID != "disp-f2" {
		t.Errorf("unexpected recent session dispatches: %+v", recent)
	}
}

func TestFakeStore_GetActiveTasks_DispatchesUnion(t *testing.T) {
	s := NewFakeStore()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)

	mustOK(t, s.InsertMessage(ctx, db.Message{
		ID:              "msg-fake-1",
		ThreadID:        "123456",
		Content:         "Discord user prompt",
		Status:          db.StatusPending,
		DurationMs:      800,
		InputTokens:     80,
		OutputTokens:    40,
		ThinkingTokens:  10,
		CacheReadTokens: 5,
		TotalTokens:     135,
		CreatedAt:       now.Add(-2 * time.Minute),
		UpdatedAt:       now.Add(-2 * time.Minute),
	}), "InsertMessage")

	mustInsertDispatch(ctx, t, s, db.Dispatch{
		ID:              "disp-fake-stateful",
		Source:          "heartbeat",
		SessionID:       "heartbeat-hb-1",
		Prompt:          "Heartbeat prompt",
		Status:          db.StatusPending,
		DurationMs:      1800,
		InputTokens:     150,
		OutputTokens:    70,
		ThinkingTokens:  20,
		CacheReadTokens: 10,
		TotalTokens:     250,
		CreatedAt:       now.Add(-1 * time.Minute),
		UpdatedAt:       now.Add(-1 * time.Minute),
	}, "InsertDispatch stateful")

	mustInsertDispatch(ctx, t, s, db.Dispatch{
		ID:        "disp-fake-ephem",
		Source:    "webhook",
		SessionID: "",
		Prompt:    "Webhook prompt",
		Status:    db.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}, "InsertDispatch ephem")

	// Unfiltered query unions messages and dispatches
	unfiltered, err := s.GetActiveTasks(ctx)
	mustOK(t, err, "GetActiveTasks unfiltered")
	if len(unfiltered) != 3 {
		t.Fatalf("expected 3 tasks in unfiltered HUD, got %d", len(unfiltered))
	}
	if unfiltered[0].ID != "msg-fake-1" || unfiltered[1].ID != "disp-fake-stateful" || unfiltered[2].ID != "disp-fake-ephem" {
		t.Errorf("unexpected task order: %+v", unfiltered)
	}
	if unfiltered[0].DurationMs != 800 || unfiltered[0].TotalTokens != 135 {
		t.Errorf("unexpected telemetry on fake task 0: %+v", unfiltered[0])
	}
	if unfiltered[1].DurationMs != 1800 || unfiltered[1].TotalTokens != 250 {
		t.Errorf("unexpected telemetry on fake task 1: %+v", unfiltered[1])
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

type fakeStoreKnownTurnCase struct {
	name       string
	failNext   error
	closed     bool
	messages   []db.Message
	sessions   []db.SessionInfo
	dispatches []db.Dispatch
	wantErr    error
	wantNil    bool
	wantIDs    []string
}

func runFakeStoreKnownTurnCase(t *testing.T, tc fakeStoreKnownTurnCase) {
	t.Helper()
	s := NewFakeStore()
	ctx := context.Background()
	for _, m := range tc.messages {
		_ = s.InsertMessage(ctx, m)
	}
	for _, sess := range tc.sessions {
		s.SetSessionInfo(sess)
	}
	for _, d := range tc.dispatches {
		_, _ = s.InsertDispatch(ctx, d)
	}
	if tc.failNext != nil {
		s.FailNext("GetKnownTurnSessionIDs", tc.failNext)
	}
	if tc.closed {
		_ = s.Close()
	}
	got, err := s.GetKnownTurnSessionIDs(ctx)
	if tc.wantErr != nil {
		if !errors.Is(err, tc.wantErr) {
			t.Fatalf("expected error %v, got %v", tc.wantErr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantNil {
		if got != nil {
			t.Fatalf("expected nil map, got %+v", got)
		}
		return
	}
	if got == nil {
		t.Fatalf("expected non-nil map, got nil")
	}
	if len(got) != len(tc.wantIDs) {
		t.Fatalf("got %d session IDs (%v), want %d (%v)", len(got), got, len(tc.wantIDs), tc.wantIDs)
	}
	for _, wantID := range tc.wantIDs {
		if !got[wantID] {
			t.Errorf("missing expected session ID %q in got %+v", wantID, got)
		}
	}
}

func TestFakeStore_GetKnownTurnSessionIDs(t *testing.T) {
	t.Parallel()
	simErr := errors.New("provenance query down")
	tests := []fakeStoreKnownTurnCase{
		{
			name:     "simulated fail next error",
			failNext: simErr,
			wantErr:  simErr,
		},
		{
			name:    "closed store returns ErrConnDone",
			closed:  true,
			wantErr: sql.ErrConnDone,
		},
		{
			name:    "empty store returns nil",
			wantNil: true,
		},
		{
			name: "heartbeat only returns empty non-nil map",
			sessions: []db.SessionInfo{
				{ThreadID: "heartbeat-health", InternalSessionID: "hb-conv-1", PreviousSessionID: "hb-prev-1"},
			},
			dispatches: []db.Dispatch{
				{ID: "d-hb", Source: "heartbeat", ConversationID: "hb-disp-1"},
			},
			wantIDs: []string{},
		},
		{
			name: "only dispatches returns allowed ids",
			dispatches: []db.Dispatch{
				{ID: "d-only-1", Source: "user", ConversationID: "conv-only-disp"},
			},
			wantIDs: []string{"conv-only-disp"},
		},
		{
			name: "populated store filters heartbeat and returns allowed ids",
			messages: []db.Message{
				{ID: "m1", ConversationID: "conv-msg-1"},
				{ID: "m2", ConversationID: ""},
			},
			sessions: []db.SessionInfo{
				{ThreadID: "discord-100", InternalSessionID: "conv-sess-1", PreviousSessionID: "conv-prev-1"},
				{ThreadID: "heartbeat-poll", InternalSessionID: "hb-ignored-1", PreviousSessionID: "hb-ignored-2"},
			},
			dispatches: []db.Dispatch{
				{ID: "d1", Source: "cron", ConversationID: "conv-disp-1"},
				{ID: "d2", Source: "heartbeat", ConversationID: "hb-disp-ignored"},
			},
			wantIDs: []string{"conv-msg-1", "conv-sess-1", "conv-prev-1", "conv-disp-1"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runFakeStoreKnownTurnCase(t, tc)
		})
	}
}

func TestFakeStore_RecordConformance(t *testing.T) {
	t.Parallel()
	RunRecordStoreConformance(t, func(*testing.T) db.Store { return NewFakeStore() })
}
