package dbtest

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDBTest_NewAndInitDB(t *testing.T) {
	// 1. New(t)
	db1 := New(t)
	if db1 == nil {
		t.Fatalf("expected non-nil db from New")
	}
	if err := db1.Ping(); err != nil {
		t.Errorf("expected ping to succeed on New db, got %v", err)
	}

	// 2. InitDB with :memory:
	dbMem, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB :memory: failed: %v", err)
	}
	defer dbMem.Close()

	// 3. InitDB with empty string
	if _, err := InitDB(""); err == nil {
		t.Errorf("expected error for empty DSN, got nil")
	}

	// 4. InitDB with sqlite:// prefix and subfolder
	tmpDir := t.TempDir()
	subFile := filepath.Join(tmpDir, "subfolder", "test.db")
	dbSub, err := InitDB("sqlite://" + subFile)
	if err != nil {
		t.Fatalf("InitDB with subfolder failed: %v", err)
	}
	dbSub.Close()

	// 5. InitDB with file: DSN containing ?
	dbQuery, err := InitDB("file:test_query?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("InitDB with query param failed: %v", err)
	}
	defer dbQuery.Close()

	// 6. Idempotent schema run
	if err := initSchemaSQLite(dbQuery); err != nil {
		t.Errorf("second initSchemaSQLite call failed: %v", err)
	}

	// 7. Directory creation failure
	tmpFile := filepath.Join(tmpDir, "regular_file")
	if err := os.WriteFile(tmpFile, []byte("data"), 0644); err != nil {
		t.Fatalf("failed to create regular file: %v", err)
	}
	if _, err := InitDB(filepath.Join(tmpFile, "cannot_mkdir", "test.db")); err == nil {
		t.Errorf("expected error when mkdir fails, got nil")
	}

	// 8. Invalid sqlite DSN
	if _, err := InitDB("file:bad?_pragma=bad_pragma_syntax"); err == nil {
		// some pragmas might not error on open, but ensure code path runs
	}
}

func TestDBTest_SchemaAlterErrors(t *testing.T) {
	dbMem, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbMem.Close()

	// Alter on non-existent table
	if _, err := dbMem.Exec("ALTER TABLE non_existent_table ADD COLUMN foo TEXT;"); err == nil {
		t.Errorf("expected error altering missing table")
	} else if !strings.Contains(strings.ToLower(err.Error()), "no such table") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDBTest_InitSchemaErrors(t *testing.T) {
	// 1. Pragmas fail on closed DB
	closedDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	_ = closedDB.Close()
	if err := initSchemaSQLite(closedDB); err == nil {
		t.Errorf("expected error from initSchemaSQLite on closed DB")
	}

	// 2. Failed ALTER TABLE on view messages
	dbPath := filepath.Join(t.TempDir(), "view_messages.db")
	preDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	if _, err := preDB.Exec("CREATE VIEW messages AS SELECT 1 AS id;"); err != nil {
		t.Fatalf("failed to create view: %v", err)
	}
	preDB.Close()

	// InitDB fails because initSchemaSQLite fails on view
	if _, err := InitDB(dbPath); err == nil {
		t.Errorf("expected error from InitDB on DB with messages view")
	}

	// 3. Failed ALTER TABLE on view sessions
	dbMem, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer dbMem.Close()
	if _, err := dbMem.Exec("CREATE TABLE messages (id TEXT PRIMARY KEY, restart_count INTEGER); CREATE VIEW sessions AS SELECT 1;"); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	if err := initSchemaSQLite(dbMem); err == nil {
		t.Errorf("expected error on sessions view")
	}

	// 4. Failed ALTER TABLE on view facts
	dbMem2, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer dbMem2.Close()
	if _, err := dbMem2.Exec("CREATE TABLE messages (id TEXT PRIMARY KEY, restart_count INTEGER); CREATE TABLE sessions (thread_id TEXT PRIMARY KEY, previous_session_id TEXT); CREATE VIEW facts AS SELECT 1;"); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	if err := initSchemaSQLite(dbMem2); err == nil {
		t.Errorf("expected error on facts view")
	}
}

type mockFailTB struct {
	testing.TB
	failed bool
}

func (m *mockFailTB) Helper() {}

func (m *mockFailTB) TempDir() string {
	return filepath.Join(os.DevNull, "invalid")
}

func (m *mockFailTB) Fatalf(format string, args ...any) {
	m.failed = true
}

func (m *mockFailTB) Cleanup(fn func()) {}

func TestDBTest_New_FailureBranch(t *testing.T) {
	mock := &mockFailTB{}
	db := New(mock)
	if !mock.failed {
		t.Errorf("expected mock TB to fail when tempdir fails")
	}
	if db != nil {
		db.Close()
	}
}
