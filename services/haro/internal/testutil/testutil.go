// Package testutil opens hermetic PostgreSQL databases for haro tests. It is
// imported ONLY from *_test.go files - never from production code.
//
// Every handle points at a private database cloned from the production
// schema by pkg/pgtest, so the package under test must call
// os.Exit(pgtest.Main(m)) from its TestMain. Handles are opened through the
// production constructor store.OpenDB so pool wiring is exercised too.
package testutil

import (
	"context"
	"database/sql"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/store"
)

// NewDB returns an open handle on a fresh clone of the production schema.
// The handle is closed (and the clone dropped) when t finishes.
func NewDB(t testing.TB) *sql.DB {
	t.Helper()
	return openDB(t, pgtest.NewDSN(t))
}

// NewClosedDB returns a handle on a fresh clone that is already closed, so
// every query on it fails with "sql: database is closed".
func NewClosedDB(t testing.TB) *sql.DB {
	t.Helper()
	db := NewDB(t)
	_ = db.Close()
	return db
}

// openDB opens dsn with store.OpenDB and closes the handle on cleanup; an
// open failure fails t.
func openDB(t testing.TB, dsn string) *sql.DB {
	t.Helper()
	db, err := store.OpenDB(context.Background(), &config.Config{DatabaseURL: dsn})
	if err != nil {
		t.Fatalf("testutil: open test database: %v", err)
		return nil
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
