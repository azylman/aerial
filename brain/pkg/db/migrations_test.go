package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSplitSQLStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty",
			input:    "",
			expected: nil,
		},
		{
			name:     "whitespace only",
			input:    "   \n\t  ",
			expected: nil,
		},
		{
			name:     "single statement without semicolon",
			input:    "SELECT 1",
			expected: []string{"SELECT 1"},
		},
		{
			name:     "single statement with semicolon",
			input:    "SELECT 1;",
			expected: []string{"SELECT 1"},
		},
		{
			name:     "multiple statements",
			input:    "CREATE TABLE t1 (id int); CREATE TABLE t2 (id int);",
			expected: []string{"CREATE TABLE t1 (id int)", "CREATE TABLE t2 (id int)"},
		},
		{
			name: "statements with line comments and block comments",
			input: `-- comment 1
CREATE TABLE t1 (
    -- inside comment
    id int
);
/* block comment
   line 2 */
CREATE TABLE t2 (id int);`,
			expected: []string{
				"-- comment 1\nCREATE TABLE t1 (\n    -- inside comment\n    id int\n)",
				"/* block comment\n   line 2 */\nCREATE TABLE t2 (id int)",
			},
		},
		{
			name: "semicolons inside single quotes and escaped quotes",
			input: `INSERT INTO t (val) VALUES ('hello; world');
INSERT INTO t (val) VALUES ('it''s; a; test');`,
			expected: []string{
				"INSERT INTO t (val) VALUES ('hello; world')",
				"INSERT INTO t (val) VALUES ('it''s; a; test')",
			},
		},
		{
			name: "semicolons inside double quotes",
			input: `SELECT "col;1", "col;2" FROM t;`,
			expected: []string{
				`SELECT "col;1", "col;2" FROM t`,
			},
		},
		{
			name: "dollar-quoted function with nested semicolons",
			input: `CREATE FUNCTION test_fn() RETURNS void AS $$
BEGIN
    SELECT 1;
    SELECT 2;
END;
$$ LANGUAGE plpgsql;
SELECT 3;`,
			expected: []string{
				"CREATE FUNCTION test_fn() RETURNS void AS $$\nBEGIN\n    SELECT 1;\n    SELECT 2;\nEND;\n$$ LANGUAGE plpgsql",
				"SELECT 3",
			},
		},
		{
			name: "custom tagged dollar-quotes",
			input: `CREATE FUNCTION test_fn2() RETURNS void AS $body$
    SELECT 1;
$body$;`,
			expected: []string{
				"CREATE FUNCTION test_fn2() RETURNS void AS $body$\n    SELECT 1;\n$body$",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SplitSQLStatements(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("expected %d statements, got %d:\nGot: %#v\nExpected: %#v", len(tt.expected), len(got), got, tt.expected)
			}
			for i := range got {
				if strings.TrimSpace(got[i]) != strings.TrimSpace(tt.expected[i]) {
					t.Errorf("statement %d mismatch:\nGot:      %q\nExpected: %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestParseAtlasSum(t *testing.T) {
	hashes := parseAtlasSum()
	if len(hashes) == 0 {
		t.Fatalf("expected parseAtlasSum() to load hashes from embedded atlas.sum, got empty map")
	}

	foundBaseline := false
	for k, v := range hashes {
		if strings.Contains(k, "baseline.sql") {
			foundBaseline = true
			if v == "" {
				t.Errorf("empty hash for baseline file %q", k)
			}
		}
	}
	if !foundBaseline {
		t.Errorf("expected atlas.sum to contain a baseline.sql entry, got map: %#v", hashes)
	}
}

func TestRunMigrations_NilDB(t *testing.T) {
	ctx := context.Background()
	err := RunMigrations(ctx, nil)
	if err == nil || !strings.Contains(err.Error(), "database cannot be nil") {
		t.Fatalf("expected error for nil database, got: %v", err)
	}
}

func TestRunMigrations_ClosedDB(t *testing.T) {
	addr := startMockPostgres(t)
	dsn := fmt.Sprintf("postgres://mock:mock@%s/testdb?sslmode=disable", addr)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	_ = db.Close()

	ctx := context.Background()
	err = RunMigrations(ctx, db)
	if err == nil {
		t.Fatalf("expected error running migrations on closed DB, got nil")
	}
}

func TestRunMigrations_AgainstDevPostgresIfAvailable(t *testing.T) {
	devURL := os.Getenv("AERIAL_TEST_DEV_POSTGRES_URL")
	if devURL == "" {
		// Check if local dev container is reachable
		devURL = "postgres://postgres:dev@aerial-atlas-dev:5432/dev?sslmode=disable"
	}

	db, err := sql.Open("pgx", devURL)
	if err != nil {
		t.Skip("skipping live dev postgres test: cannot open connection")
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("skipping live dev postgres test: cannot ping %s: %v", devURL, err)
	}

	// Clean public and atlas_schema_revisions schemas for isolated contract test
	_, _ = db.ExecContext(ctx, "DROP SCHEMA IF EXISTS atlas_schema_revisions CASCADE;")
	_, _ = db.ExecContext(ctx, "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
	_, _ = db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS vector;")

	// 1. First run: should apply all migrations cleanly
	if err := RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations fresh install failed: %v", err)
	}

	// Verify all tables exist
	tables := []string{
		"cron_schedules", "facts", "messages", "one_shot_schedules",
		"schedule_runs", "session_summaries", "sessions", "transcript_steps",
	}
	for _, tbl := range tables {
		var exists bool
		query := fmt.Sprintf("SELECT to_regclass('public.%s') IS NOT NULL;", tbl)
		if err := db.QueryRowContext(ctx, query).Scan(&exists); err != nil || !exists {
			t.Errorf("expected table %s to exist, got exists=%v, err=%v", tbl, exists, err)
		}
	}

	// Verify revision recorded
	var appliedCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM atlas_schema_revisions.atlas_schema_revisions;").Scan(&appliedCount); err != nil {
		t.Fatalf("failed to query revisions: %v", err)
	}
	if appliedCount == 0 {
		t.Errorf("expected at least 1 applied revision, got 0")
	}

	// 2. Second run: must be 100% idempotent and zero changes
	if err := RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations second idempotent run failed: %v", err)
	}

	// 3. Test baseline guard: if facts table exists and revisions table is empty, auto-stamp
	_, _ = db.ExecContext(ctx, "TRUNCATE TABLE atlas_schema_revisions.atlas_schema_revisions;")
	if err := RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations baseline guard run failed: %v", err)
	}
	var stampedCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM atlas_schema_revisions.atlas_schema_revisions WHERE version = $1;", baselineVersion).Scan(&stampedCount); err != nil || stampedCount != 1 {
		t.Errorf("expected baseline version %s to be stamped, got count=%d, err=%v", baselineVersion, stampedCount, err)
	}
}
