package transcript

import (
	"database/sql"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

// newTestDB opens a private cloned Postgres database through the production
// constructor.
func newTestDB(t testing.TB) *sql.DB {
	t.Helper()
	database, err := db.Open(pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// newTestTranscriptStore returns a production transcript store (stack
// testStack) over a private cloned database.
func newTestTranscriptStore(t testing.TB) (*db.SQLTranscriptStore, *sql.DB) {
	t.Helper()
	database := newTestDB(t)
	return db.NewSQLTranscriptStore(database, testStack), database
}

const testStack = "local"
