package queue

// Postgres-backed store fixture for the queue package: every test that needs a
// store opens a private database cloned from services/eri/pkg/db/schema.sql
// (pgtest.NewDSN) through the production constructor (db.Open) and seeds real
// rows through the production write methods, so coordinator and turn logic run
// the production SQL. Failure rows sabotage the test's own database instead of
// scripting store errors.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

// newTestDB opens a private cloned database through db.Open (pool sizing, ping
// and DB stats registration, as in production) and closes it on cleanup.
func newTestDB(t testing.TB) *sql.DB {
	t.Helper()
	sqlDB, err := db.Open(pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return sqlDB
}

// newTestStore returns a production SQLStore over a private cloned database plus
// the raw handle for seeding and sabotage.
func newTestStore(t testing.TB) (*db.SQLStore, *sql.DB) {
	t.Helper()
	sqlDB := newTestDB(t)
	return db.NewSQLStore(sqlDB), sqlDB
}

// execSQL runs seed or sabotage SQL against the test's own database; an empty
// query is a no-op so table rows without sabotage need no branch.
func execSQL(t testing.TB, sqlDB *sql.DB, query string) {
	t.Helper()
	if query == "" {
		return
	}
	if _, err := sqlDB.ExecContext(context.Background(), query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// failTriggerSQL makes every <event> on table fail with SQLSTATE sqlstate inside
// the test's private database. Pick the SQLSTATE by the branch under test:
// classifyStoreError treats classes 22, 23, 42, 0A, 2B, 2D and 25 as terminal
// (e.g. 23505) and everything else as retryable (e.g. 40001).
func failTriggerSQL(table, event, sqlstate string) string {
	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION pgtest_fail_%[1]s_%[2]s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'sabotaged %% on %%', TG_OP, TG_TABLE_NAME USING ERRCODE = '%[3]s';
END $$;
CREATE TRIGGER pgtest_fail_%[1]s_%[2]s BEFORE %[2]s ON %[1]s FOR EACH ROW EXECUTE FUNCTION pgtest_fail_%[1]s_%[2]s();`, table, event, sqlstate)
}

// seedMessageRows writes msgs through the production InsertMessage, which persists
// status, retry/restart counts and timestamps as given.
func seedMessageRows(t testing.TB, store *db.SQLStore, msgs []db.Message) {
	t.Helper()
	for _, m := range msgs {
		if err := store.InsertMessage(context.Background(), m); err != nil {
			t.Fatalf("seed message %s: %v", m.ID, err)
		}
	}
}

// seedDispatchRows writes disps through the production InsertDispatch.
func seedDispatchRows(t testing.TB, store *db.SQLStore, disps []db.Dispatch) {
	t.Helper()
	for _, d := range disps {
		if _, err := store.InsertDispatch(context.Background(), d); err != nil {
			t.Fatalf("seed dispatch %s: %v", d.ID, err)
		}
	}
}

// seedSessionTurns raises sessionKey's turn count to turns through the
// production IncrementSessionTurnCount.
func seedSessionTurns(t testing.TB, store *db.SQLStore, sessionKey string, turns int) {
	t.Helper()
	for range turns {
		if _, err := store.IncrementSessionTurnCount(context.Background(), sessionKey); err != nil {
			t.Fatalf("seed session %s turn: %v", sessionKey, err)
		}
	}
}

// seedSessionInfo writes info's session through the production SaveSessionID and
// IncrementSessionTurnCount, then backdates created_at/updated_at with a plain
// UPDATE (production only ever stamps the current time) for idle-age rows.
func seedSessionInfo(t testing.TB, store *db.SQLStore, sqlDB *sql.DB, info db.SessionInfo) {
	t.Helper()
	if err := store.SaveSessionID(context.Background(), info.ThreadID, info.InternalSessionID); err != nil {
		t.Fatalf("seed session %s: %v", info.ThreadID, err)
	}
	seedSessionTurns(t, store, info.ThreadID, info.TurnCount)
	if _, err := sqlDB.ExecContext(context.Background(), `UPDATE sessions SET created_at = $1, updated_at = $2 WHERE session_id = $3`, info.CreatedAt, info.UpdatedAt, db.DiscordSessionID(info.ThreadID)); err != nil {
		t.Fatalf("backdate session %s: %v", info.ThreadID, err)
	}
}
