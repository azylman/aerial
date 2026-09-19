package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/brain/pkg/metrics"
	_ "modernc.org/sqlite"
)

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS messages (
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
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER IF NOT EXISTS trg_messages_row_id AFTER INSERT ON messages WHEN new.row_id IS NULL OR new.row_id = 0 BEGIN
	UPDATE messages SET row_id = (SELECT COALESCE(MAX(row_id), 0) + 1 FROM messages) WHERE id = new.id;
END;

CREATE TABLE IF NOT EXISTS sessions (
	thread_id TEXT PRIMARY KEY,
	internal_session_id TEXT NOT NULL DEFAULT '',
	previous_session_id TEXT NOT NULL DEFAULT '',
	turn_count INTEGER NOT NULL DEFAULT 0,
	last_extracted_rowid INTEGER NOT NULL DEFAULT 0,
	fact_extracted_at DATETIME,
	summary TEXT NOT NULL DEFAULT '',
	last_summarized_message_id TEXT NOT NULL DEFAULT '',
	active_tasks TEXT NOT NULL DEFAULT '[]',
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS one_shot_schedules (
	id TEXT PRIMARY KEY,
	thread_id TEXT NOT NULL,
	prompt TEXT NOT NULL,
	run_at DATETIME NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cron_schedules (
	id TEXT PRIMARY KEY,
	target_id TEXT NOT NULL,
	title_prefix TEXT NOT NULL DEFAULT '',
	cron_expr TEXT NOT NULL,
	prompt TEXT NOT NULL,
	timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
	next_run_at DATETIME NOT NULL,
	enabled BOOLEAN NOT NULL DEFAULT 1,
	effort TEXT NOT NULL DEFAULT 'high',
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS schedule_runs (
	id TEXT PRIMARY KEY,
	schedule_id TEXT NOT NULL,
	schedule_type TEXT NOT NULL,
	message_id TEXT NOT NULL DEFAULT '',
	target_id TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	title TEXT NOT NULL DEFAULT '',
	prompt TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'enqueued',
	started_at DATETIME NOT NULL,
	completed_at DATETIME,
	duration_ms INTEGER DEFAULT 0,
	effort TEXT NOT NULL DEFAULT 'high',
	model TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS facts (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	category TEXT NOT NULL DEFAULT 'general',
	fact_text TEXT NOT NULL,
	importance REAL NOT NULL DEFAULT 1.0,
	thread_id TEXT NOT NULL DEFAULT '',
	embedding BLOB,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	last_reinforced_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	last_decayed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	reinforce_count INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_messages_thread_status ON messages(thread_id, status);
CREATE INDEX IF NOT EXISTS idx_messages_status_created_at ON messages(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_sessions_fact_extracted ON sessions(last_extracted_rowid, fact_extracted_at);
CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);
CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_started ON schedule_runs(schedule_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_status_started ON schedule_runs(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_runs_message_id ON schedule_runs(message_id);
CREATE INDEX IF NOT EXISTS idx_facts_thread_id ON facts(thread_id);
CREATE INDEX IF NOT EXISTS idx_facts_category ON facts(category);
CREATE INDEX IF NOT EXISTS idx_facts_created_at ON facts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_importance_created_at ON facts(importance DESC, created_at DESC);
`

func initSchemaSQLite(database *sql.DB) error {
	pragmas := `
	PRAGMA journal_mode = WAL;
	PRAGMA busy_timeout = 5000;
	PRAGMA synchronous = NORMAL;
	`
	if _, err := database.Exec(pragmas); err != nil {
		return fmt.Errorf("failed to set sqlite pragmas: %w", err)
	}

	if _, err := database.Exec(sqliteSchema); err != nil {
		return fmt.Errorf("failed to run sqlite migrations: %w", err)
	}

	if _, err := database.Exec("ALTER TABLE messages ADD COLUMN restart_count INTEGER NOT NULL DEFAULT 0;"); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("failed to add restart_count column to messages: %w", err)
		}
	}

	_, _ = database.Exec("ALTER TABLE messages ADD COLUMN effort TEXT NOT NULL DEFAULT ''")
	_, _ = database.Exec("ALTER TABLE cron_schedules ADD COLUMN effort TEXT NOT NULL DEFAULT 'high'")
	_, _ = database.Exec("ALTER TABLE schedule_runs ADD COLUMN effort TEXT NOT NULL DEFAULT 'high'")
	_, _ = database.Exec("ALTER TABLE schedule_runs ADD COLUMN model TEXT NOT NULL DEFAULT ''")

	_, _ = database.Exec("ALTER TABLE sessions ADD COLUMN summary TEXT NOT NULL DEFAULT ''")
	_, _ = database.Exec("ALTER TABLE sessions ADD COLUMN last_summarized_message_id TEXT NOT NULL DEFAULT ''")
	_, _ = database.Exec("ALTER TABLE sessions ADD COLUMN active_tasks TEXT NOT NULL DEFAULT '[]'")
	if _, err := database.Exec("ALTER TABLE sessions ADD COLUMN previous_session_id TEXT NOT NULL DEFAULT ''"); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("failed to add previous_session_id column to sessions: %w", err)
		}
	}

	if _, err := database.Exec("ALTER TABLE facts ADD COLUMN last_reinforced_at DATETIME NOT NULL DEFAULT '';"); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("failed to add last_reinforced_at column to facts: %w", err)
		}
	}
	if _, err := database.Exec("ALTER TABLE facts ADD COLUMN last_decayed_at DATETIME NOT NULL DEFAULT '';"); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("failed to add last_decayed_at column to facts: %w", err)
		}
	}
	if _, err := database.Exec("ALTER TABLE facts ADD COLUMN reinforce_count INTEGER NOT NULL DEFAULT 1;"); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("failed to add reinforce_count column to facts: %w", err)
		}
	}
	_, _ = database.Exec("CREATE INDEX IF NOT EXISTS idx_facts_last_reinforced_at ON facts(last_reinforced_at DESC)")
	_, _ = database.Exec("UPDATE facts SET last_reinforced_at = created_at WHERE last_reinforced_at = '' OR (reinforce_count = 1 AND last_reinforced_at > created_at)")
	_, _ = database.Exec("UPDATE facts SET last_decayed_at = created_at WHERE last_decayed_at = ''")
	return nil
}

func initTestSQLite(dsn string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return nil, fmt.Errorf("db: connection string cannot be empty")
	}

	trimmed = strings.TrimPrefix(trimmed, "sqlite://")

	if trimmed != ":memory:" && !strings.HasPrefix(trimmed, "file:") {
		if err := os.MkdirAll(filepath.Dir(trimmed), 0755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
	}

	sqliteDSN := trimmed
	if trimmed != ":memory:" && !strings.Contains(trimmed, "_pragma") {
		if strings.Contains(trimmed, "?") {
			sqliteDSN += "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		} else {
			sqliteDSN += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		}
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
	t.Cleanup(func() {
		_ = database.Close()
	})
	return database
}

func init() {
	RegisterSQLiteTestHook(initTestSQLite)
}

func TestSQLiteTestFixtureHook(t *testing.T) {
	if sqliteTestInitHook == nil {
		t.Fatalf("expected sqliteTestInitHook to be registered")
	}
	db, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB with hooked sqlite failed: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Errorf("expected hooked sqlite db ping to succeed: %v", err)
	}
}

