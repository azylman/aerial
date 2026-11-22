package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

func TestNewDB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		closed  bool
		wantErr string
	}{
		{name: "open handle sees production schema"},
		{name: "closed handle rejects queries", closed: true, wantErr: "sql: database is closed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var db *sql.DB
			if tc.closed {
				db = NewClosedDB(t)
			} else {
				db = NewDB(t)
			}
			var n int
			err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM cron_schedules`).Scan(&n)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("query err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || n != 0 {
				t.Fatalf("SELECT COUNT(*) FROM cron_schedules = (%d, %v), want (0, nil)", n, err)
			}
		})
	}
}

// fatalRecorder is a testing.TB that records Fatalf instead of stopping the
// goroutine, so openDB's failure branch can be observed.
type fatalRecorder struct {
	testing.TB

	fatal string
}

func (r *fatalRecorder) Fatalf(format string, args ...any) { r.fatal = fmt.Sprintf(format, args...) }

func TestOpenDB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		dsn       string
		wantNil   bool
		wantFatal string
	}{
		{name: "unsupported scheme fails the test", dsn: "mysql://localhost/db", wantNil: true, wantFatal: "testutil: open test database: unsupported database scheme"},
		{name: "empty dsn fails the test", dsn: "", wantNil: true, wantFatal: "testutil: open test database: connection string cannot be empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &fatalRecorder{TB: t}
			db := openDB(rec, tc.dsn)
			if (db == nil) != tc.wantNil {
				t.Errorf("openDB returned db=%v, wantNil=%v", db, tc.wantNil)
			}
			if !strings.HasPrefix(rec.fatal, tc.wantFatal) {
				t.Errorf("Fatalf message = %q, want prefix %q", rec.fatal, tc.wantFatal)
			}
		})
	}
}
