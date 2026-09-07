package db

import (
	"path/filepath"
	"testing"
)

// NewTestStore creates an isolated hermetic SQLite test store for unit tests.
func NewTestStore(t testing.TB) Store {
	t.Helper()
	sqlitePath := filepath.Join(t.TempDir(), "aerial_test.db")
	db, err := initDB(sqlitePath)
	if err != nil {
		t.Fatalf("NewTestStore failed to initialize SQLite test database: %v", err)
	}
	store := NewSQLStore(db)
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}
