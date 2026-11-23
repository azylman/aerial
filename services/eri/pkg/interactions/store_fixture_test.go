package interactions

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

// newTestStore returns a production SQLStore over a private cloned database
// plus the *sql.DB for seeding and sabotage.
func newTestStore(t testing.TB) (*db.SQLStore, *sql.DB) {
	t.Helper()
	database := newTestDB(t)
	return db.NewSQLStore(database), database
}
