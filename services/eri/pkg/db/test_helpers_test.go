package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// NewTestStore creates an isolated hermetic SQLite test store for unit tests.
func NewTestStore(t testing.TB) Store {
	t.Helper()
	sqlitePath := filepath.Join(t.TempDir(), "aerial_test.db")
	db, err := initTestSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("NewTestStore failed to initialize SQLite test database: %v", err)
	}
	store := NewSQLStore(db)
	t.Cleanup(func() { _ = db.Close() })
	return store
}

func requireErr(t *testing.T, err error, op string) {
	t.Helper()
	if err == nil {
		t.Errorf("expected error for %s on unavailable store, got nil", op)
	}
}

func c1[T any](_ T, err error) error         { return err }
func c2[A, B any](_ A, _ B, err error) error { return err }

func assertStoreUnavailable(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	requireErr(t, s.CreateOneShotSchedule(ctx, OneShotSchedule{}), "CreateOneShotSchedule")
	requireErr(t, c1(s.GetDueOneShotSchedules(ctx)), "GetDueOneShotSchedules")
	requireErr(t, s.InsertMessageAndConsumeOneShot(ctx, "id", Message{}), "InsertMessageAndConsumeOneShot")
	requireErr(t, c1(s.GetAllOneShotSchedules(ctx, "t")), "GetAllOneShotSchedules")
	requireErr(t, c1(s.GetDueCronSchedules(ctx)), "GetDueCronSchedules")
	requireErr(t, c1(s.GetAllCronSchedules(ctx, "t")), "GetAllCronSchedules")
	requireErr(t, s.UpdateCronNextRun(ctx, "id", time.Now()), "UpdateCronNextRun")
	requireErr(t, s.CreateScheduleRun(ctx, ScheduleRun{}), "CreateScheduleRun")
	requireErr(t, s.UpdateScheduleRunStatus(ctx, UpdateRunParams{}), "UpdateScheduleRunStatus")
	requireErr(t, c2(s.GetScheduleRunsPaginated(ctx, 10, 0, "", "")), "GetScheduleRunsPaginated")
	requireErr(t, c1(s.GetScheduleSummaryMetrics(ctx)), "GetScheduleSummaryMetrics")
	requireErr(t, c1(s.ReconcileOrphanedScheduleRuns(ctx)), "ReconcileOrphanedScheduleRuns")
	requireErr(t, c1(s.PruneScheduleRuns(ctx, 10, time.Hour)), "PruneScheduleRuns")
	requireErr(t, s.InsertMessage(ctx, Message{}), "InsertMessage")
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
