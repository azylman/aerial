// Package testutil provides shared Postgres test fixtures for Prospera domain
// packages. Every fixture is a private database cloned by pkg/pgtest from the
// real production schema, opened through the production constructor, so the
// calling package's TestMain must run os.Exit(pgtest.Main(m)).
package testutil

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/prospera/internal/store"
)

// OpenTestDB opens a private Postgres database through store.OpenDB, the
// production constructor, and closes it when t finishes.
func OpenTestDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := store.OpenDB(context.Background(), pgtest.NewDSN(t))
	require.NoError(t, err, "testutil: open test database")
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// SetUpTestStore opens a private test database and wraps it in a store.PGStore.
func SetUpTestStore(t testing.TB) *store.PGStore {
	t.Helper()
	return store.NewPGStore(OpenTestDB(t))
}
