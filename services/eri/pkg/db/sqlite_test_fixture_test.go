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

const sqliteSchema = `CREATE TABLE IF NOT EXISTS messages ( id TEXT PRIMARY KEY, row_id INTEGER, thread_id TEXT NOT NULL DEFAULT '', guild_id TEXT NOT NULL DEFAULT '', author_id TEXT NOT NULL DEFAULT '', author_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL DEFAULT '', summary TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'PENDING', retry_count INTEGER NOT NULL DEFAULT 0, restart_count INTEGER NOT NULL DEFAULT 0, effort TEXT NOT NULL DEFAULT '', error_message TEXT, response_text TEXT, schedule_run_id TEXT NOT NULL DEFAULT '', metadata TEXT NOT NULL DEFAULT '{}', created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ); CREATE TRIGGER IF NOT EXISTS trg_messages_row_id AFTER INSERT ON messages WHEN new.row_id IS NULL OR new.row_id = 0 BEGIN UPDATE messages SET row_id = (SELECT COALESCE(MAX(row_id), 0) + 1 FROM messages) WHERE id = new.id; END; CREATE TABLE IF NOT EXISTS sessions ( thread_id TEXT PRIMARY KEY, internal_session_id TEXT NOT NULL DEFAULT '', previous_session_id TEXT NOT NULL DEFAULT '', turn_count INTEGER NOT NULL DEFAULT 0, last_extracted_rowid INTEGER NOT NULL DEFAULT 0, fact_extracted_at DATETIME, summary TEXT NOT NULL DEFAULT '', last_summarized_message_id TEXT NOT NULL DEFAULT '', active_tasks TEXT NOT NULL DEFAULT '[]', created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ); CREATE TABLE IF NOT EXISTS one_shot_schedules ( id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, prompt TEXT NOT NULL, run_at DATETIME NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, effort TEXT NOT NULL DEFAULT 'low' ); CREATE TABLE IF NOT EXISTS cron_schedules ( id TEXT PRIMARY KEY, target_id TEXT NOT NULL, title_prefix TEXT NOT NULL DEFAULT '', cron_expr TEXT NOT NULL, prompt TEXT NOT NULL, timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles', next_run_at DATETIME NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, effort TEXT NOT NULL DEFAULT 'low', created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ); CREATE TABLE IF NOT EXISTS heartbeat_schedules ( id TEXT PRIMARY KEY, target_id TEXT NOT NULL, title_prefix TEXT NOT NULL DEFAULT '', cron_expr TEXT NOT NULL, prompt TEXT NOT NULL, timezone TEXT NOT NULL DEFAULT 'UTC', effort TEXT NOT NULL DEFAULT 'low', next_run_at DATETIME NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ); CREATE TABLE IF NOT EXISTS schedule_runs ( id TEXT PRIMARY KEY, schedule_id TEXT NOT NULL, schedule_type TEXT NOT NULL, message_id TEXT NOT NULL DEFAULT '', target_id TEXT NOT NULL, thread_id TEXT NOT NULL, title TEXT NOT NULL DEFAULT '', prompt TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'enqueued', started_at DATETIME NOT NULL, completed_at DATETIME, duration_ms INTEGER DEFAULT 0, effort TEXT NOT NULL DEFAULT 'low', model TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '' ); CREATE TABLE IF NOT EXISTS facts ( id INTEGER PRIMARY KEY AUTOINCREMENT, fact_text TEXT NOT NULL, embedding BLOB NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, last_reinforced_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, reinforce_count INTEGER NOT NULL DEFAULT 1 ); CREATE INDEX IF NOT EXISTS idx_messages_thread_status ON messages(thread_id, status); CREATE INDEX IF NOT EXISTS idx_messages_status_created_at ON messages(status, created_at ASC); CREATE INDEX IF NOT EXISTS idx_sessions_fact_extracted ON sessions(last_extracted_rowid, fact_extracted_at); CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at); CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at); CREATE INDEX IF NOT EXISTS idx_heartbeat_schedules_next_run ON heartbeat_schedules(enabled, next_run_at); CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC); CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_started ON schedule_runs(schedule_id, started_at DESC); CREATE INDEX IF NOT EXISTS idx_schedule_runs_status_started ON schedule_runs(status, started_at DESC); CREATE INDEX IF NOT EXISTS idx_schedule_runs_message_id ON schedule_runs(message_id); CREATE INDEX IF NOT EXISTS idx_facts_activity ON facts(reinforce_count DESC, last_reinforced_at DESC); CREATE TABLE IF NOT EXISTS session_summaries ( session_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL DEFAULT '', summary TEXT NOT NULL DEFAULT '', embedding BLOB, last_indexed_step INTEGER NOT NULL DEFAULT -1, last_mtime DATETIME, summary_step_watermark INTEGER NOT NULL DEFAULT -1, is_settled BOOLEAN NOT NULL DEFAULT 0, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ); CREATE TABLE IF NOT EXISTS transcript_steps ( session_id TEXT NOT NULL, step_index INTEGER NOT NULL, step_type TEXT NOT NULL DEFAULT '', tool_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL DEFAULT '', created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (session_id, step_index), FOREIGN KEY (session_id) REFERENCES session_summaries(session_id) ON DELETE CASCADE ); CREATE INDEX IF NOT EXISTS idx_session_summaries_mtime ON session_summaries(last_mtime); CREATE INDEX IF NOT EXISTS idx_session_summaries_thread ON session_summaries(thread_id); CREATE INDEX IF NOT EXISTS idx_transcript_steps_tool ON transcript_steps(tool_name); CREATE TABLE IF NOT EXISTS pr_registry ( id INTEGER PRIMARY KEY AUTOINCREMENT, repo TEXT NOT NULL, pr_number INTEGER NOT NULL, branch TEXT NOT NULL, head_sha TEXT NOT NULL, merge_sha TEXT, target_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open', title TEXT NOT NULL DEFAULT '', status_message_id TEXT NOT NULL DEFAULT '', metadata TEXT NOT NULL DEFAULT '{}', created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT uq_pr_registry_repo_pr UNIQUE (repo, pr_number) ); CREATE INDEX IF NOT EXISTS idx_pr_registry_head_sha ON pr_registry (repo, head_sha); CREATE INDEX IF NOT EXISTS idx_pr_registry_merge_sha ON pr_registry (repo, merge_sha); CREATE INDEX IF NOT EXISTS idx_pr_registry_status ON pr_registry (status); CREATE INDEX IF NOT EXISTS idx_pr_registry_target_id ON pr_registry (target_id); CREATE TABLE IF NOT EXISTS vanadis_builds ( id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, tier INTEGER NOT NULL DEFAULT 2, title TEXT NOT NULL DEFAULT '', objective TEXT NOT NULL, shared_context TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'running', status_message_id TEXT NOT NULL DEFAULT '', created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ); CREATE INDEX IF NOT EXISTS idx_vanadis_builds_status ON vanadis_builds (status); CREATE INDEX IF NOT EXISTS idx_vanadis_builds_thread_id ON vanadis_builds (thread_id); CREATE TABLE IF NOT EXISTS vanadis_chunks ( id TEXT PRIMARY KEY, build_id TEXT NOT NULL REFERENCES vanadis_builds(id) ON DELETE CASCADE, chunk_index INTEGER NOT NULL, repo TEXT NOT NULL DEFAULT 'azylman/aerial', title TEXT NOT NULL, instructions TEXT NOT NULL, target_files TEXT NOT NULL DEFAULT '{}', target_symbols TEXT NOT NULL DEFAULT '{}', effort TEXT NOT NULL DEFAULT 'low', acceptance_criteria TEXT NOT NULL DEFAULT '{}', status TEXT NOT NULL DEFAULT 'pending', pr_number INTEGER, started_at DATETIME, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT uq_vanadis_chunks_build_index UNIQUE (build_id, chunk_index) ); CREATE INDEX IF NOT EXISTS idx_vanadis_chunks_build_status ON vanadis_chunks (build_id, status); CREATE INDEX IF NOT EXISTS idx_vanadis_chunks_pr_number ON vanadis_chunks (repo, pr_number) WHERE pr_number IS NOT NULL; CREATE TABLE IF NOT EXISTS procedural_candidates ( id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL DEFAULT '', domain TEXT NOT NULL DEFAULT 'discord', loaded_skill TEXT NOT NULL DEFAULT '', trigger_pattern TEXT NOT NULL CHECK (trim(trigger_pattern) <> ''), steps_json TEXT NOT NULL DEFAULT '[]', avoid_json TEXT NOT NULL DEFAULT '[]', embedding BLOB NOT NULL, status TEXT NOT NULL DEFAULT 'pending', pr_number INTEGER, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ); CREATE INDEX IF NOT EXISTS idx_procedural_candidates_status ON procedural_candidates (status, created_at ASC); CREATE INDEX IF NOT EXISTS idx_procedural_candidates_loaded_skill ON procedural_candidates (status, loaded_skill); CREATE TABLE IF NOT EXISTS event_triggers ( id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, source TEXT NOT NULL, target TEXT NOT NULL, event_type TEXT NOT NULL, prompt TEXT NOT NULL, effort TEXT NOT NULL DEFAULT 'medium', created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ); CREATE INDEX IF NOT EXISTS idx_event_triggers_lookup ON event_triggers (source, target, event_type); CREATE INDEX IF NOT EXISTS idx_event_triggers_channel ON event_triggers (channel_id, created_at ASC);`

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
