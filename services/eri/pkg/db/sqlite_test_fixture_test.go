package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/metrics"
	_ "modernc.org/sqlite"
)

const sqliteSchema = `CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    row_id INTEGER,
    thread_id TEXT NOT NULL DEFAULT '',
    guild_id TEXT NOT NULL DEFAULT '',
    author_id TEXT NOT NULL DEFAULT '',
    author_name TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    restart_count INTEGER NOT NULL DEFAULT 0,
    effort TEXT NOT NULL DEFAULT '',
    error_message TEXT,
    response_text TEXT,
    schedule_run_id TEXT NOT NULL DEFAULT '',
    channel_id TEXT NOT NULL DEFAULT '',
    channel_name TEXT NOT NULL DEFAULT '',
    thread_name TEXT NOT NULL DEFAULT '',
    author_global_name TEXT NOT NULL DEFAULT '',
    author_bot BOOLEAN NOT NULL DEFAULT 0,
    is_admin BOOLEAN NOT NULL DEFAULT 0,
    mentions TEXT NOT NULL DEFAULT '{}',
    mention_user_ids TEXT NOT NULL DEFAULT '{}',
    mention_role_ids TEXT NOT NULL DEFAULT '{}',
    replying_to_author TEXT NOT NULL DEFAULT '',
    replying_to_content TEXT NOT NULL DEFAULT '',
    attachments TEXT NOT NULL DEFAULT '{}',
    model TEXT NOT NULL DEFAULT '',
    silent BOOLEAN NOT NULL DEFAULT 0,
    ephemeral BOOLEAN,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TRIGGER IF NOT EXISTS trg_messages_row_id AFTER INSERT ON messages WHEN new.row_id IS NULL OR new.row_id = 0 BEGIN UPDATE messages SET row_id = (SELECT COALESCE(MAX(row_id), 0) + 1 FROM messages) WHERE id = new.id; END;
CREATE INDEX IF NOT EXISTS idx_messages_thread_status ON messages(thread_id, status);
CREATE INDEX IF NOT EXISTS idx_messages_status_created_at ON messages(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_messages_admin_completed ON messages(status, is_admin, updated_at);
CREATE TABLE IF NOT EXISTS sessions (
    thread_id TEXT PRIMARY KEY,
    internal_session_id TEXT NOT NULL DEFAULT '',
    previous_session_id TEXT NOT NULL DEFAULT '',
    turn_count INTEGER NOT NULL DEFAULT 0,
    summary TEXT NOT NULL DEFAULT '',
    last_summarized_message_id TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS session_summaries (
    session_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    embedding BLOB,
    last_indexed_step INTEGER NOT NULL DEFAULT -1,
    last_mtime DATETIME,
    summary_step_watermark INTEGER NOT NULL DEFAULT -1,
    is_settled BOOLEAN NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_session_summaries_mtime ON session_summaries(last_mtime);
CREATE INDEX IF NOT EXISTS idx_session_summaries_thread ON session_summaries(thread_id);
CREATE TABLE IF NOT EXISTS transcript_steps (
    session_id TEXT NOT NULL,
    step_index INTEGER NOT NULL,
    step_type TEXT NOT NULL DEFAULT '',
    tool_name TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_id, step_index),
    FOREIGN KEY (session_id) REFERENCES session_summaries(session_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_transcript_steps_tool ON transcript_steps(tool_name);`

func initSchemaSQLite(database *sql.DB) error {
	pragmas := "PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000; PRAGMA synchronous = NORMAL;"
	if _, err := database.Exec(pragmas); err != nil {
		return fmt.Errorf("failed to set sqlite pragmas: %w", err)
	}
	if _, err := database.Exec(sqliteSchema); err != nil {
		return fmt.Errorf("failed to run sqlite migrations: %w", err)
	}
	return nil
}

func initTestSQLite(dsn string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return nil, fmt.Errorf("db: connection string cannot be empty")
	}
	trimmed = strings.TrimPrefix(trimmed, "sqlite://")
	if trimmed != ":memory:" && !strings.HasPrefix(trimmed, "file:") {
		if err := os.MkdirAll(filepath.Dir(trimmed), 0o755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
	}
	sqliteDSN := trimmed
	if trimmed != ":memory:" && !strings.Contains(trimmed, "_pragma") {
		sep := "?"
		if strings.Contains(trimmed, "?") {
			sep = "&"
		}
		sqliteDSN += sep + "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)"
	}
	database, err := sql.Open("sqlite", sqliteDSN)
	if err != nil {
		return nil, err
	}
	if trimmed == ":memory:" || strings.Contains(trimmed, "mode=memory") {
		database.SetMaxOpenConns(1)
	}
	if err := initSchemaSQLite(database); err != nil {
		_ = database.Close()
		return nil, err
	}
	metrics.RegisterDBStats(database)
	return database, nil
}

func initTestSQLiteDB(t testing.TB) *sql.DB {
	t.Helper()
	sqlitePath := filepath.Join(t.TempDir(), "aerial_test.db")
	database, err := initTestSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("initTestSQLiteDB failed to initialize SQLite database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func init() {
	sqliteTestInitHook = initTestSQLite
}

func TestSQLiteTestFixtureHook(t *testing.T) {
	if sqliteTestInitHook == nil {
		t.Fatalf("expected sqliteTestInitHook to be registered")
	}
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open with hooked sqlite failed: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Errorf("expected hooked sqlite db ping to succeed: %v", err)
	}
}
