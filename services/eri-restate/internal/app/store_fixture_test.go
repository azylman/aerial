package app

import (
	"os"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

// TestMain starts one throwaway Postgres for the package (pgtest.Main).
func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

// newTestStore returns a production SQLStore over a private cloned database.
func newTestStore(t testing.TB) *db.SQLStore {
	t.Helper()
	conn, err := db.Open(pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return db.NewSQLStore(conn)
}
