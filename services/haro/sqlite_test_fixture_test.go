package main

import (
	"database/sql"

	"github.com/azylman/aerial/services/haro/internal/testutil"
)

func initTestSQLite(trimmed string) (*sql.DB, error) {
	return testutil.OpenSQLiteTestDB(trimmed)
}

// RegisterSQLiteTestHook allows unit tests to inject in-memory databases.
func RegisterSQLiteTestHook(fn func(string) (*sql.DB, error)) { sqliteTestInitHook = fn }

func init() {
	RegisterSQLiteTestHook(initTestSQLite)
}
