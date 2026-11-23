package db

// Postgres-backed store fixture: DB-backed tests open a private database
// cloned from schema.sql (pgtest.NewDSN) through the production constructor
// and run the production SQL against real PostgreSQL 18 + pgvector.

import (
	"context"
	"database/sql"
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
