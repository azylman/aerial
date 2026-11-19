// Package testutil provides shared SQLite test fixtures for Prospera domain
// packages. The canonical voice_calls and voice_messages DDL lives here so
// every package-level test boots an identical schema, and the schema is
// verified on every setup to fail fast on drift.
package testutil

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"testing"

	// SQLite driver registration for the in-memory test databases opened by OpenTestSQLiteDB.
	_ "modernc.org/sqlite"

	"github.com/azylman/aerial/services/prospera/internal/store"
)

// Canonical table names asserted by verifyTestSchema.
const (
	testTableVoiceCalls    = "voice_calls"
	testTableVoiceMessages = "voice_messages"
)

// testSchema is the canonical SQLite DDL for voice_calls and voice_messages
// used by all Prospera package tests, kept as a testdata fixture so the
// schema stays a single source of truth outside application code.

//go:embed testdata/schema.sql
var testSchema string

// OpenTestSQLiteDB opens a private in-memory SQLite database carrying the
// canonical Prospera test schema.
func OpenTestSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err == nil {
		db.SetMaxOpenConns(1)
		_, err = db.ExecContext(context.Background(), testSchema)
	}
	if err == nil {
		err = db.PingContext(context.Background())
	}
	if err == nil {
		err = verifyTestSchema(db)
	}
	if err != nil {
		t.Fatalf("testutil: set up in-memory sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// SetUpTestStore opens a canonical test database and wraps it in a store.PGStore.
func SetUpTestStore(t *testing.T) *store.PGStore {
	t.Helper()
	db := OpenTestSQLiteDB(t)
	st := store.NewPGStore(db)
	return st
}

// tableCount returns the number of tables with the given name.
func tableCount(db *sql.DB, table string) (int, error) {
	var count int
	err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// columnCount returns the number of columns with the given name in a table.
func columnCount(db *sql.DB, table, column string) (int, error) {
	var count int
	err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?", table, column).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// verifyTestSchema ensures the canonical voice_calls and voice_messages
// tables exist with the expected key columns.
func verifyTestSchema(db *sql.DB) error {
	for _, table := range []string{testTableVoiceCalls, testTableVoiceMessages} {
		count, err := tableCount(db, table)
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("testutil: missing test table %s", table)
		}
	}
	for _, check := range []struct {
		table  string
		column string
	}{
		{table: testTableVoiceCalls, column: "call_id"},
		{table: testTableVoiceCalls, column: "report_status"},
		{table: testTableVoiceMessages, column: "message_sid"},
		{table: testTableVoiceMessages, column: "body"},
	} {
		count, err := columnCount(db, check.table, check.column)
		if err != nil || count != 1 {
			return fmt.Errorf("testutil: %s missing column %s", check.table, check.column)
		}
	}
	return nil
}
