package testutil

import (
	"database/sql"
	"testing"

	"github.com/azylman/aerial/services/prospera/internal/store"
)

func TestOpenTestSQLiteDB(t *testing.T) {
	t.Parallel()
	db := OpenTestSQLiteDB(t)
	tests := []struct {
		name  string
		table string
	}{
		{name: "voice calls table exists", table: "voice_calls"},
		{name: "voice messages table exists", table: "voice_messages"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			count, err := tableCount(db, tc.table)
			if err != nil {
				t.Fatalf("tableCount(%q) error = %v", tc.table, err)
			}
			if count != 1 {
				t.Errorf("tableCount(%q) = %d, want 1", tc.table, count)
			}
		})
	}
}

func TestVerifyTestSchema(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		schema  string
		closed  bool
		wantErr bool
	}{
		{name: "canonical schema passes", schema: testSchema, wantErr: false},
		{name: "empty database fails", schema: ``, wantErr: true},
		{name: "missing voice messages fails", schema: `CREATE TABLE voice_calls (
	call_id TEXT PRIMARY KEY,
	direction TEXT NOT NULL,
	state TEXT NOT NULL,
	from_number TEXT NOT NULL,
	to_number TEXT NOT NULL,
	report_status TEXT NOT NULL DEFAULT 'pending'
);`, wantErr: true},
		{name: "closed database fails", schema: ``, closed: true, wantErr: true},
		{name: "voice calls missing report status fails", schema: `CREATE TABLE voice_calls (
	call_id TEXT PRIMARY KEY,
	direction TEXT NOT NULL,
	state TEXT NOT NULL,
	from_number TEXT NOT NULL,
	to_number TEXT NOT NULL
);
CREATE TABLE voice_messages (
	message_sid TEXT PRIMARY KEY,
	direction TEXT NOT NULL,
	from_number TEXT NOT NULL,
	to_number TEXT NOT NULL,
	body TEXT NOT NULL
);`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatalf("sql.Open() error = %v", err)
			}
			defer func() { _ = db.Close() }()
			if tc.closed {
				_ = db.Close()
			} else if tc.schema != "" {
				_, err = db.Exec(tc.schema)
				if err != nil {
					t.Fatalf("db.Exec(schema) error = %v", err)
				}
			}
			err = verifyTestSchema(db)
			if (err != nil) != tc.wantErr {
				t.Errorf("verifyTestSchema() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestTableCount(t *testing.T) {
	t.Parallel()
	db := OpenTestSQLiteDB(t)
	closed, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	_ = closed.Close()
	tests := []struct {
		name      string
		db        *sql.DB
		table     string
		wantCount int
		wantErr   bool
	}{
		{name: "voice calls counted", db: db, table: "voice_calls", wantCount: 1, wantErr: false},
		{name: "voice messages counted", db: db, table: "voice_messages", wantCount: 1, wantErr: false},
		{name: "missing table counts zero", db: db, table: "no_such_table", wantCount: 0, wantErr: false},
		{name: "closed database errors", db: closed, table: "voice_calls", wantCount: 0, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			count, err := tableCount(tc.db, tc.table)
			if (err != nil) != tc.wantErr {
				t.Fatalf("tableCount(%q) error = %v, wantErr = %v", tc.table, err, tc.wantErr)
			}
			if !tc.wantErr && count != tc.wantCount {
				t.Errorf("tableCount(%q) = %d, want %d", tc.table, count, tc.wantCount)
			}
		})
	}
}

func TestColumnCount(t *testing.T) {
	t.Parallel()
	db := OpenTestSQLiteDB(t)
	closed, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	_ = closed.Close()
	tests := []struct {
		name      string
		db        *sql.DB
		table     string
		column    string
		wantCount int
		wantErr   bool
	}{
		{name: "call id column counted", db: db, table: "voice_calls", column: "call_id", wantCount: 1, wantErr: false},
		{name: "body column counted", db: db, table: "voice_messages", column: "body", wantCount: 1, wantErr: false},
		{name: "missing column counts zero", db: db, table: "voice_calls", column: "no_such_column", wantCount: 0, wantErr: false},
		{name: "closed database errors", db: closed, table: "voice_calls", column: "call_id", wantCount: 0, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			count, err := columnCount(tc.db, tc.table, tc.column)
			if (err != nil) != tc.wantErr {
				t.Fatalf("columnCount(%q, %q) error = %v, wantErr = %v", tc.table, tc.column, err, tc.wantErr)
			}
			if !tc.wantErr && count != tc.wantCount {
				t.Errorf("columnCount(%q, %q) = %d, want %d", tc.table, tc.column, count, tc.wantCount)
			}
		})
	}
}

func TestSetUpTestStoreIsSQLiteBacked(t *testing.T) {
	t.Parallel()
	st := SetUpTestStore(t)
	var _ store.Store = st
	if !st.IsSQLite() {
		t.Errorf("IsSQLite() = false, want true for test store")
	}
}
