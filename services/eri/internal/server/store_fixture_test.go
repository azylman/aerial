package server

// Every DB-backed test in this package runs the production SQL against a real
// PostgreSQL 18 + pgvector server started once per test binary by pgtest.Main
// (see TestMain in goleak_test.go). Each test gets its own database cloned
// from services/eri/pkg/db/schema.sql, so tests may call t.Parallel().

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

// newTestDB opens a private cloned database through the production
// constructor (db.Open: pool sizing, ping, metrics.RegisterDBStats).
func newTestDB(t testing.TB) *sql.DB {
	t.Helper()
	database, err := db.Open(pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})
	return database
}

// newTestStore returns a production SQLStore over a private cloned database.
func newTestStore(t testing.TB) *db.SQLStore {
	t.Helper()
	return db.NewSQLStore(newTestDB(t))
}

// newTestStoreDB returns a production SQLStore plus its *sql.DB, for tests
// that seed rows or sabotage the private database.
func newTestStoreDB(t testing.TB) (*db.SQLStore, *sql.DB) {
	t.Helper()
	database := newTestDB(t)
	return db.NewSQLStore(database), database
}

// newClosedTestStore returns a production SQLStore whose handle is already
// closed, so every query fails with a retryable driver error.
func newClosedTestStore(t testing.TB) *db.SQLStore {
	t.Helper()
	database, err := db.Open(pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close test database: %v", err)
	}
	return db.NewSQLStore(database)
}

// failTriggerSQL makes every <event> on table raise with the given SQLSTATE.
func failTriggerSQL(table, event, sqlstate string) string {
	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION test_fail_%[1]s_%[2]s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'injected %[1]s %[2]s failure' USING ERRCODE = '%[3]s';
END $$;
CREATE TRIGGER test_fail_%[1]s_%[2]s BEFORE %[2]s ON %[1]s FOR EACH ROW EXECUTE FUNCTION test_fail_%[1]s_%[2]s();`,
		table, strings.ToLower(event), sqlstate)
}

// mustExec runs setup or sabotage SQL against a test's private database.
func mustExec(t testing.TB, database *sql.DB, query string) {
	t.Helper()
	if _, err := database.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}
