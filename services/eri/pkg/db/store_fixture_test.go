package db

// Postgres-backed store fixture: DB-backed tests open a private database
// cloned from schema.sql (pgtest.NewDSN) through the production constructor
// and run the production SQL against real PostgreSQL 18 + pgvector.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
)

// newTestDB opens a private cloned database through the production
// constructor (Open: pool sizing, ping, metrics.RegisterDBStats).
func newTestDB(t testing.TB) *sql.DB {
	t.Helper()
	database, err := Open(pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// newTestStore returns a store over a private database plus the handle for
// seeding and sabotage.
func newTestStore(t testing.TB) (*SQLStore, *sql.DB) {
	t.Helper()
	database := newTestDB(t)
	return NewSQLStore(database), database
}

// closedTestStore returns a store whose private database handle is already
// closed, so every query fails with a retryable driver error.
func closedTestStore(t testing.TB) *SQLStore {
	t.Helper()
	store, database := newTestStore(t)
	if err := database.Close(); err != nil {
		t.Fatalf("close test database: %v", err)
	}
	return store
}

// execSQL runs seed or sabotage SQL (multi-statement allowed without args).
func execSQL(t testing.TB, database *sql.DB, query string, args ...any) {
	t.Helper()
	if query == "" {
		return
	}
	if _, err := database.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// failTriggerSQL makes every row-level event on table raise sqlstate.
func failTriggerSQL(table, event, sqlstate string) string {
	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION pgtest_fail_%[1]s_%[2]s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'sabotaged %% on %%', TG_OP, TG_TABLE_NAME USING ERRCODE = '%[3]s';
END $$;
CREATE TRIGGER pgtest_fail_%[1]s_%[2]s BEFORE %[2]s ON %[1]s FOR EACH ROW EXECUTE FUNCTION pgtest_fail_%[1]s_%[2]s();`, table, event, sqlstate)
}

// seedMessages writes messages through the production InsertMessage, which
// persists status, retry/restart counts, error text and timestamps as given.
func seedMessages(t testing.TB, store *SQLStore, msgs []Message) {
	t.Helper()
	for _, m := range msgs {
		if err := store.InsertMessage(context.Background(), m); err != nil {
			t.Fatalf("seed message %s: %v", m.ID, err)
		}
	}
}

// seedDispatches writes dispatches through the production InsertDispatch.
func seedDispatches(t testing.TB, store *SQLStore, dispatches []Dispatch) {
	t.Helper()
	for _, d := range dispatches {
		if _, err := store.InsertDispatch(context.Background(), d); err != nil {
			t.Fatalf("seed dispatch %s: %v", d.ID, err)
		}
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

// assertStoreUnavailable checks every Store method fails on a store whose
// database is nil or closed.
func assertStoreUnavailable(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	requireErr(t, s.InsertMessage(ctx, Message{ID: "id"}), "InsertMessage")
	requireErr(t, s.UpdateMessageStatus(ctx, "id", "", ""), "UpdateMessageStatus")
	requireErr(t, s.UpdateMessageCompleted(ctx, "id", "", ""), "UpdateMessageCompleted")
	requireErr(t, c1(s.GetMessage(ctx, "id")), "GetMessage")
	requireErr(t, c1(s.MessageExists(ctx, "id")), "MessageExists")
	requireErr(t, c1(s.ClaimPendingMessage(ctx, "id")), "ClaimPendingMessage")
	requireErr(t, c1(s.GetRecentThreadMessages(ctx, "t", 10)), "GetRecentThreadMessages")
	requireErr(t, c1(s.GetActiveTasks(ctx)), "GetActiveTasks")
	requireErr(t, c1(s.GetActiveTasks(ctx, TasksFilter{ThreadID: "t"})), "GetActiveTasks filtered")
	requireErr(t, c1(s.GetSessionID(ctx, "t")), "GetSessionID")
	requireErr(t, s.SaveSessionID(ctx, "t", "s"), "SaveSessionID")
	requireErr(t, c1(s.IncrementSessionTurnCount(ctx, "k")), "IncrementSessionTurnCount")
	requireErr(t, c1(s.GetSessionTurnCount(ctx, "k")), "GetSessionTurnCount")
	requireErr(t, s.RotateSessionID(ctx, "k", "n"), "RotateSessionID")
	requireErr(t, c1(s.GetPreviousSessionID(ctx, "k")), "GetPreviousSessionID")
	requireErr(t, c2(s.GetThreadSummary(ctx, "t")), "GetThreadSummary")
	requireErr(t, s.SaveThreadSummary(ctx, "t", "s", "w"), "SaveThreadSummary")
	requireErr(t, c1(s.InsertDispatch(ctx, Dispatch{ID: "id"})), "InsertDispatch")
	requireErr(t, c1(s.GetDispatch(ctx, "id")), "GetDispatch")
	requireErr(t, c1(s.ClaimPendingDispatch(ctx, "id")), "ClaimPendingDispatch")
	requireErr(t, s.UpdateDispatchStatus(ctx, "id", "", ""), "UpdateDispatchStatus")
	requireErr(t, s.UpdateDispatchCompleted(ctx, "id", "", ""), "UpdateDispatchCompleted")
	requireErr(t, c1(s.GetRecentSessionDispatches(ctx, "s", 10)), "GetRecentSessionDispatches")
	requireErr(t, c1(s.GetMessagesByID(ctx, []string{"id"})), "GetMessagesByID")
	requireErr(t, c1(s.FailMessagesIfActive(ctx, []string{"id"}, "r")), "FailMessagesIfActive")
	requireErr(t, c1(s.FailMessagesIfPending(ctx, []string{"id"}, "r")), "FailMessagesIfPending")
	requireErr(t, c1(s.SkipMessagesIfActive(ctx, []string{"id"}, "r")), "SkipMessagesIfActive")
	requireErr(t, c1(s.RequeueProcessingMessages(ctx, []string{"id"}, "r")), "RequeueProcessingMessages")
	requireErr(t, c1(s.BeginMessageAttempt(ctx, "id", 1)), "BeginMessageAttempt")
	requireErr(t, c1(s.FailDispatchesIfActive(ctx, []string{"id"}, "r")), "FailDispatchesIfActive")
	requireErr(t, c1(s.FailDispatchesIfPending(ctx, []string{"id"}, "r")), "FailDispatchesIfPending")
	requireErr(t, c1(s.SkipDispatchesIfActive(ctx, []string{"id"}, "r")), "SkipDispatchesIfActive")
	requireErr(t, c1(s.BeginDispatchAttempt(ctx, "id", 1)), "BeginDispatchAttempt")
	requireErr(t, c1(s.ListActiveDispatchSessionKeys(ctx, "d", "b")), "ListActiveDispatchSessionKeys")
}
