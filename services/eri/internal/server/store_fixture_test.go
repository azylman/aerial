package server

// Every DB-backed test in this package runs the production SQL against a real
// PostgreSQL 18 + pgvector server started once per test binary by pgtest.Main
// (see TestMain in goleak_test.go). Each test gets its own database cloned
// from services/eri/pkg/db/schema.sql, so tests may call t.Parallel().

import (
	"database/sql"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

// testTranscriptStack is the transcript stack every test transcript store uses.
const testTranscriptStack = "eri"

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

// newTestTranscriptStore returns a production SQLTranscriptStore over a
// private cloned database.
func newTestTranscriptStore(t testing.TB) *db.SQLTranscriptStore {
	t.Helper()
	return db.NewSQLTranscriptStore(newTestDB(t), testTranscriptStack)
}
