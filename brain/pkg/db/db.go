package db

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	pgvector "github.com/pgvector/pgvector-go"
	_ "modernc.org/sqlite"
)

const (
	StatusPending    = "PENDING"
	StatusProcessing = "PROCESSING"
	StatusCompleted  = "COMPLETED"
	StatusFailed     = "FAILED"

	ExpectedEmbeddingDim = 384
)

type Message struct {
	ID            string    `json:"id"`
	RowID         int64     `json:"row_id,omitempty"`
	ThreadID      string    `json:"thread_id"`
	GuildID       string    `json:"guild_id"`
	AuthorID      string    `json:"author_id"`
	AuthorName    string    `json:"author_name"`
	Content       string    `json:"content"`
	Summary       string    `json:"summary"`
	Status        string    `json:"status"`
	RetryCount    int       `json:"retry_count"`
	ErrorMessage  string    `json:"error_message,omitempty"`
	ResponseText  string    `json:"response_text,omitempty"`
	ScheduleRunID string    `json:"schedule_run_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func GetDBPath() string {
	if testURL := os.Getenv("TEST_DATABASE_URL"); testURL != "" {
		return testURL
	}
	if envDSN := os.Getenv("DATABASE_URL"); envDSN != "" {
		return envDSN
	}
	dbUser := os.Getenv("POSTGRES_USER")
	if dbUser == "" {
		dbUser = "aerial"
	}
	dbPass := os.Getenv("POSTGRES_PASSWORD")
	if dbPass == "" {
		dbPass = "aerial_secure_pass"
	}
	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "postgres"
	}
	dbPort := os.Getenv("POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("POSTGRES_DB")
	if dbName == "" {
		dbName = "aerial"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)
}

func isPostgres(database *sql.DB) bool {
	if database == nil {
		return false
	}
	driverType := fmt.Sprintf("%T", database.Driver())
	return strings.Contains(driverType, "stdlib") || strings.Contains(driverType, "pgx")
}

func rebindQuery(query string, isPg bool) string {
	if !isPg {
		// Convert $1, $2, ... to ? for SQLite
		var b strings.Builder
		for i := 0; i < len(query); i++ {
			if query[i] == '$' && i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9' {
				b.WriteByte('?')
				for i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9' {
					i++
				}
			} else {
				b.WriteByte(query[i])
			}
		}
		res := b.String()
		res = strings.ReplaceAll(res, "ILIKE", "LIKE")
		return res
	}
	// For Postgres, convert ? to $1, $2, ...
	var b strings.Builder
	paramIdx := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteString(fmt.Sprintf("$%d", paramIdx))
			paramIdx++
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

func InitDB(dsn string) (*sql.DB, error) {
	if dsn == "" {
		dsn = GetDBPath()
	}

	isPg := strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")

	if isPg {
		var database *sql.DB
		var err error

		// 1. Connection retry loop with exponential backoff for containerized startup
		for attempt := 1; attempt <= 10; attempt++ {
			database, err = sql.Open("pgx", dsn)
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				pingErr := database.PingContext(ctx)
				cancel()
				if pingErr == nil {
					break
				}
				err = pingErr
				_ = database.Close()
			}
			log.Printf("[DB] Waiting for PostgreSQL (attempt %d/10): %v", attempt, err)
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		if err != nil {
			return nil, fmt.Errorf("could not connect to PostgreSQL after retries: %w", err)
		}

		// 2. Tune connection pool
		database.SetMaxOpenConns(25)
		database.SetMaxIdleConns(10)
		database.SetConnMaxLifetime(5 * time.Minute)
		database.SetConnMaxIdleTime(2 * time.Minute)

		// 3. Serialize schema creation using PostgreSQL advisory lock
		const migrationLockID = 849201948201
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if _, err := database.ExecContext(ctx, "SELECT pg_advisory_lock($1);", migrationLockID); err != nil {
			return nil, fmt.Errorf("failed to acquire migration advisory lock: %w", err)
		}
		defer func() {
			_, _ = database.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1);", migrationLockID)
		}()

		schema := `
		CREATE EXTENSION IF NOT EXISTS vector;

		CREATE TABLE IF NOT EXISTS messages (
			id TEXT PRIMARY KEY,
			row_id BIGSERIAL UNIQUE,
			thread_id TEXT NOT NULL DEFAULT '',
			guild_id TEXT NOT NULL DEFAULT '',
			author_id TEXT NOT NULL DEFAULT '',
			author_name TEXT NOT NULL DEFAULT '',
			content TEXT NOT NULL DEFAULT '',
			summary TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'PENDING',
			retry_count INTEGER NOT NULL DEFAULT 0,
			error_message TEXT,
			response_text TEXT,
			schedule_run_id TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS sessions (
			thread_id TEXT PRIMARY KEY,
			internal_session_id TEXT NOT NULL DEFAULT '',
			turn_count INTEGER NOT NULL DEFAULT 0,
			last_extracted_rowid BIGINT NOT NULL DEFAULT 0,
			fact_extracted_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS one_shot_schedules (
			id TEXT PRIMARY KEY,
			thread_id TEXT NOT NULL,
			prompt TEXT NOT NULL,
			run_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS cron_schedules (
			id TEXT PRIMARY KEY,
			target_id TEXT NOT NULL,
			title_prefix TEXT NOT NULL DEFAULT '',
			cron_expr TEXT NOT NULL,
			prompt TEXT NOT NULL,
			timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
			next_run_at TIMESTAMPTZ NOT NULL,
			enabled BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
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
			started_at TIMESTAMPTZ NOT NULL,
			completed_at TIMESTAMPTZ,
			duration_ms BIGINT DEFAULT 0,
			error TEXT NOT NULL DEFAULT ''
		);

		CREATE TABLE IF NOT EXISTS facts (
			id BIGSERIAL PRIMARY KEY,
			category TEXT NOT NULL DEFAULT 'general',
			fact_text TEXT NOT NULL,
			importance REAL NOT NULL DEFAULT 1.0,
			thread_id TEXT NOT NULL DEFAULT '',
			embedding vector(384),
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX IF NOT EXISTS idx_messages_thread_status ON messages(thread_id, status);
		CREATE INDEX IF NOT EXISTS idx_messages_thread_row_id ON messages(thread_id, row_id);
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
		CREATE INDEX IF NOT EXISTS idx_facts_embedding_hnsw ON facts USING hnsw (embedding vector_cosine_ops);
		`
		if _, err := database.ExecContext(ctx, schema); err != nil {
			return nil, fmt.Errorf("failed to run postgres migrations: %w", err)
		}

		// Idempotent sequence resynchronization in case of manual data restoration
		_, _ = database.ExecContext(ctx, `
			SELECT setval(pg_get_serial_sequence('facts', 'id'), COALESCE((SELECT MAX(id) FROM facts), 1), (SELECT COUNT(*) > 0 FROM facts));
			SELECT setval(pg_get_serial_sequence('messages', 'row_id'), COALESCE((SELECT MAX(row_id) FROM messages), 1), (SELECT COUNT(*) > 0 FROM messages));
		`)

		log.Printf("[DB] PostgreSQL initialized successfully with pgvector at %s", dsn)
		return database, nil
	}

	// SQLite fallback for in-memory unit tests and local runs
	if dsn != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(dsn), 0755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
	}

	sqliteDSN := dsn
	if dsn != ":memory:" && !strings.Contains(dsn, "_pragma") {
		if strings.Contains(dsn, "?") {
			sqliteDSN += "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		} else {
			sqliteDSN += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		}
	}

	database, err := sql.Open("sqlite", sqliteDSN)
	if err != nil {
		return nil, err
	}

	if dsn == ":memory:" || strings.Contains(dsn, "mode=memory") {
		database.SetMaxOpenConns(1)
	}

	pragmas := `
	PRAGMA journal_mode = WAL;
	PRAGMA busy_timeout = 5000;
	PRAGMA synchronous = NORMAL;
	`
	_, _ = database.Exec(pragmas)

	sqliteSchema := `
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
		turn_count INTEGER NOT NULL DEFAULT 0,
		last_extracted_rowid INTEGER NOT NULL DEFAULT 0,
		fact_extracted_at DATETIME,
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
		error TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS facts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		category TEXT NOT NULL DEFAULT 'general',
		fact_text TEXT NOT NULL,
		importance REAL NOT NULL DEFAULT 1.0,
		thread_id TEXT NOT NULL DEFAULT '',
		embedding BLOB,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
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
	`
	if _, err := database.Exec(sqliteSchema); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("failed to run sqlite migrations: %w", err)
	}

	log.Printf("[DB] SQLite initialized successfully at %s", dsn)
	return database, nil
}

// Message CRUD Operations

func InsertMessage(database *sql.DB, msg Message) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if msg.ID == "" {
		return fmt.Errorf("message id cannot be empty")
	}
	if msg.Status == "" {
		msg.Status = StatusPending
	}
	if strings.TrimSpace(msg.Summary) == "" {
		msg.Summary = CleanTaskSummary(msg.Content)
	}
	now := time.Now().UTC()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.UpdatedAt.IsZero() {
		msg.UpdatedAt = now
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	INSERT INTO messages (id, thread_id, guild_id, author_id, author_name, content, summary, status, retry_count, error_message, response_text, schedule_run_id, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	ON CONFLICT (id) DO NOTHING;
	`
	_, err := database.ExecContext(ctx, query, msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName, msg.Content, msg.Summary, msg.Status, msg.RetryCount, msg.ErrorMessage, msg.ResponseText, msg.ScheduleRunID, msg.CreatedAt, msg.UpdatedAt)
	return err
}

func UpdateMessageStatus(database *sql.DB, id string, status string, errorMsg string) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if id == "" {
		return fmt.Errorf("message id cannot be empty")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	UPDATE messages
	SET status = $1, error_message = $2, updated_at = $3
	WHERE id = $4
	`
	_, err := database.ExecContext(ctx, query, status, errorMsg, now, id)
	return err
}

func UpdateMessageCompleted(database *sql.DB, id string, responseText string) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if id == "" {
		return fmt.Errorf("message id cannot be empty")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	UPDATE messages
	SET status = $1, response_text = $2, error_message = '', updated_at = $3
	WHERE id = $4
	`
	_, err := database.ExecContext(ctx, query, StatusCompleted, responseText, now, id)
	return err
}

func IncrementMessageRetry(database *sql.DB, id string, errorMsg string) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if id == "" {
		return fmt.Errorf("message id cannot be empty")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	UPDATE messages
	SET retry_count = retry_count + 1, error_message = $1, updated_at = $2
	WHERE id = $3
	`
	_, err := database.ExecContext(ctx, query, errorMsg, now, id)
	return err
}

func GetPendingOrProcessingMessages(database *sql.DB) ([]Message, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
	SELECT id, COALESCE(row_id, 0), thread_id, guild_id, author_id, author_name, content, COALESCE(summary, ''), status, retry_count, COALESCE(error_message, ''), COALESCE(response_text, ''), COALESCE(schedule_run_id, ''), created_at, updated_at
	FROM messages
	WHERE status IN ('PENDING', 'PROCESSING')
	ORDER BY created_at ASC
	`
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []Message
	for rows.Next() {
		var m Message
		var errMsg, respText, schedID sql.NullString
		if err := rows.Scan(&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary, &m.Status, &m.RetryCount, &errMsg, &respText, &schedID, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		if errMsg.Valid {
			m.ErrorMessage = errMsg.String
		}
		if respText.Valid {
			m.ResponseText = respText.String
		}
		if schedID.Valid {
			m.ScheduleRunID = schedID.String
		}
		if m.Summary == "" {
			m.Summary = CleanTaskSummary(m.Content)
		}
		results = append(results, m)
	}
	return results, nil
}

type ActiveTask struct {
	ID            string    `json:"id"`
	RowID         int64     `json:"row_id,omitempty"`
	ThreadID      string    `json:"thread_id"`
	SessionID     string    `json:"session_id,omitempty"`
	AuthorName    string    `json:"author_name"`
	AuthorID      string    `json:"author_id"`
	Prompt        string    `json:"prompt"`
	Summary       string    `json:"summary"`
	Status        string    `json:"status"`
	RetryCount    int       `json:"retry_count"`
	ScheduleRunID string    `json:"schedule_run_id,omitempty"`
	TriggerType   string    `json:"trigger_type"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func CleanTaskSummary(content string) string {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return "Agent Task"
	}

	if strings.Contains(trimmed, "<USER_REQUEST>") {
		lines := strings.Split(trimmed, "\n")
		var extracted string
		for i, line := range lines {
			lineTrimmed := strings.TrimSpace(line)
			if strings.HasPrefix(lineTrimmed, "- content:") {
				val := strings.TrimSpace(strings.TrimPrefix(lineTrimmed, "- content:"))
				if val != "" {
					extracted = val
					break
				}
			}
			if strings.HasPrefix(lineTrimmed, "Prompt:") {
				val := strings.TrimSpace(strings.TrimPrefix(lineTrimmed, "Prompt:"))
				if val != "" {
					extracted = val
					break
				} else if i+1 < len(lines) {
					for j := i + 1; j < len(lines); j++ {
						nextTrimmed := strings.TrimSpace(lines[j])
						if nextTrimmed != "" && !strings.HasPrefix(nextTrimmed, "</USER_REQUEST>") {
							extracted = nextTrimmed
							break
						}
					}
					if extracted != "" {
						break
					}
				}
			}
		}
		if extracted != "" {
			trimmed = extracted
		}
	}

	tagRegex := regexp.MustCompile(`(?s)<[A-Za-z0-9_-]+.*?>.*?</[A-Za-z0-9_-]+>|<[^>]+>`)
	cleaned := tagRegex.ReplaceAllString(trimmed, " ")

	mentionRegex := regexp.MustCompile(`<@!?[0-9]+>`)
	cleaned = mentionRegex.ReplaceAllString(cleaned, "")

	cleaned = regexp.MustCompile(`[#*_` + "`" + `>]+`).ReplaceAllString(cleaned, "")
	spaceRegex := regexp.MustCompile(`\s+`)
	cleaned = strings.TrimSpace(spaceRegex.ReplaceAllString(cleaned, " "))

	if cleaned == "" {
		cleaned = "Agent Task"
	}

	runes := []rune(cleaned)
	if len(runes) > 140 {
		return strings.TrimSpace(string(runes[:137])) + "..."
	}
	return cleaned
}

func InferTriggerType(authorID, scheduleRunID string) string {
	if authorID == "http-client" {
		return "http"
	}
	if scheduleRunID != "" {
		if strings.HasPrefix(scheduleRunID, "cron-") {
			return "cron"
		}
		return "reminder"
	}
	return "discord"
}

func GetActiveTasks(database *sql.DB) ([]ActiveTask, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
	SELECT 
		m.id,
		COALESCE(m.row_id, 0),
		m.thread_id,
		COALESCE(s.internal_session_id, '') AS session_id,
		m.author_name,
		m.author_id,
		m.content,
		COALESCE(m.summary, '') AS summary,
		m.status,
		m.retry_count,
		COALESCE(m.schedule_run_id, '') AS schedule_run_id,
		m.created_at,
		m.updated_at
	FROM messages m
	LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.status IN ('PENDING', 'PROCESSING')
	ORDER BY m.created_at ASC
	LIMIT 50;
	`
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query active tasks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	tasks := make([]ActiveTask, 0)
	for rows.Next() {
		var t ActiveTask
		var schedID sql.NullString
		if err := rows.Scan(
			&t.ID,
			&t.RowID,
			&t.ThreadID,
			&t.SessionID,
			&t.AuthorName,
			&t.AuthorID,
			&t.Prompt,
			&t.Summary,
			&t.Status,
			&t.RetryCount,
			&schedID,
			&t.CreatedAt,
			&t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan active task: %w", err)
		}
		if schedID.Valid {
			t.ScheduleRunID = schedID.String
		}
		if t.Summary == "" {
			t.Summary = CleanTaskSummary(t.Prompt)
		}
		t.TriggerType = InferTriggerType(t.AuthorID, t.ScheduleRunID)
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active tasks: %w", err)
	}
	return tasks, nil
}

func GetMessage(database *sql.DB, id string) (*Message, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if id == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	SELECT id, COALESCE(row_id, 0), thread_id, guild_id, author_id, author_name, content, COALESCE(summary, ''), status, retry_count, COALESCE(error_message, ''), COALESCE(response_text, ''), COALESCE(schedule_run_id, ''), created_at, updated_at
	FROM messages
	WHERE id = $1
	`
	var m Message
	var errMsg, respText, schedID sql.NullString
	err := database.QueryRowContext(ctx, query, id).Scan(&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary, &m.Status, &m.RetryCount, &errMsg, &respText, &schedID, &m.CreatedAt, &m.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if errMsg.Valid {
		m.ErrorMessage = errMsg.String
	}
	if respText.Valid {
		m.ResponseText = respText.String
	}
	if schedID.Valid {
		m.ScheduleRunID = schedID.String
	}
	if m.Summary == "" {
		m.Summary = CleanTaskSummary(m.Content)
	}
	return &m, nil
}

func MessageExists(database *sql.DB, id string) (bool, error) {
	if database == nil {
		return false, fmt.Errorf("database is nil")
	}
	if id == "" {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var exists int
	err := database.QueryRowContext(ctx, "SELECT 1 FROM messages WHERE id = $1 LIMIT 1", id).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ClaimPendingMessage atomically transitions a message from PENDING to PROCESSING using CAS.
// It returns true if and only if the message was successfully claimed from PENDING state (strictly-once).
func ClaimPendingMessage(database *sql.DB, id string) (bool, error) {
	if database == nil || id == "" {
		return false, nil
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	UPDATE messages
	SET status = 'PROCESSING', updated_at = $1
	WHERE id = $2 AND status = 'PENDING'
	RETURNING id;
	`
	var claimedID string
	err := database.QueryRowContext(ctx, query, now, id).Scan(&claimedID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed claiming pending message %s: %w", id, err)
	}
	return true, nil
}

func GetActiveRecentThreadIDs(database *sql.DB, since time.Duration) ([]string, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	cutoff := time.Now().UTC().Add(-since)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	SELECT DISTINCT thread_id
	FROM (
		SELECT thread_id, updated_at FROM messages WHERE thread_id != '' AND updated_at >= $1
		UNION
		SELECT thread_id, updated_at FROM sessions WHERE thread_id != '' AND updated_at >= $2
	) combined
	ORDER BY updated_at DESC
	LIMIT 50
	`
	rows, err := database.QueryContext(ctx, query, cutoff, cutoff)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var threadIDs []string
	for rows.Next() {
		var thID string
		if err := rows.Scan(&thID); err != nil {
			return nil, err
		}
		if thID != "" {
			threadIDs = append(threadIDs, thID)
		}
	}
	return threadIDs, nil
}

func GetRecentThreadMessages(database *sql.DB, threadID string, limit int) ([]Message, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if limit <= 0 {
		limit = 10
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	SELECT id, COALESCE(row_id, 0), thread_id, guild_id, author_id, author_name, content, summary, status,
	       retry_count, error_message, response_text, schedule_run_id, created_at, updated_at
	FROM (
		SELECT id, COALESCE(row_id, 0) AS row_id, thread_id, guild_id, author_id, author_name, content, summary, status,
		       retry_count, error_message, response_text, schedule_run_id, created_at, updated_at
		FROM messages
		WHERE thread_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	) recent
	ORDER BY created_at ASC
	`
	rows, err := database.QueryContext(ctx, query, threadID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent thread messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	msgs := make([]Message, 0)
	for rows.Next() {
		var m Message
		var errMsg, respText, schedID sql.NullString
		if err := rows.Scan(
			&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary,
			&m.Status, &m.RetryCount, &errMsg, &respText, &schedID, &m.CreatedAt, &m.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan recent message: %w", err)
		}
		if errMsg.Valid {
			m.ErrorMessage = errMsg.String
		}
		if respText.Valid {
			m.ResponseText = respText.String
		}
		if schedID.Valid {
			m.ScheduleRunID = schedID.String
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent messages: %w", err)
	}
	return msgs, nil
}

// Session CRUD Operations

func GetSessionID(database *sql.DB, threadID string) (string, error) {
	if database == nil || threadID == "" {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var sessionID string
	err := database.QueryRowContext(ctx, "SELECT internal_session_id FROM sessions WHERE thread_id = $1", threadID).Scan(&sessionID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return sessionID, err
}

func SaveSessionID(database *sql.DB, threadID, sessionID string) error {
	if database == nil || threadID == "" || sessionID == "" {
		return nil
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	INSERT INTO sessions (thread_id, internal_session_id, created_at, updated_at)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT(thread_id) DO UPDATE SET
		internal_session_id = EXCLUDED.internal_session_id,
		updated_at = EXCLUDED.updated_at
	`
	_, err := database.ExecContext(ctx, query, threadID, sessionID, now, now)
	return err
}

func DeleteSessionID(database *sql.DB, threadID string) error {
	if database == nil || threadID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := database.ExecContext(ctx, "DELETE FROM sessions WHERE thread_id = $1", threadID)
	return err
}

func IncrementSessionTurnCount(database *sql.DB, sessionKey string) (int, error) {
	if database == nil {
		return 0, fmt.Errorf("database is nil")
	}
	if sessionKey == "" {
		return 0, fmt.Errorf("session key cannot be empty")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	INSERT INTO sessions (thread_id, internal_session_id, turn_count, created_at, updated_at)
	VALUES ($1, '', 1, $2, $3)
	ON CONFLICT(thread_id) DO UPDATE SET
		turn_count = sessions.turn_count + 1,
		updated_at = EXCLUDED.updated_at
	RETURNING turn_count;
	`
	var turnCount int
	err := database.QueryRowContext(ctx, query, sessionKey, now, now).Scan(&turnCount)
	if err != nil {
		return 0, err
	}
	return turnCount, nil
}

func RotateSessionID(database *sql.DB, sessionKey, newSessionID string) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if sessionKey == "" {
		return fmt.Errorf("session key cannot be empty")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	INSERT INTO sessions (thread_id, internal_session_id, turn_count, created_at, updated_at)
	VALUES ($1, $2, 0, $3, $4)
	ON CONFLICT(thread_id) DO UPDATE SET
		internal_session_id = EXCLUDED.internal_session_id,
		turn_count = 0,
		updated_at = EXCLUDED.updated_at;
	`
	_, err := database.ExecContext(ctx, query, sessionKey, newSessionID, now, now)
	return err
}

func GetSessionTurnCount(database *sql.DB, sessionKey string) (int, error) {
	if database == nil || sessionKey == "" {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := database.QueryRowContext(ctx, "SELECT turn_count FROM sessions WHERE thread_id = $1", sessionKey).Scan(&count)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return count, err
}

// Legacy conversation compatibility helpers

func GetInternalConversationID(database *sql.DB, externalID string) (string, error) {
	return GetSessionID(database, externalID)
}

func GetExternalConversationID(database *sql.DB, internalID string) (string, error) {
	if database == nil || internalID == "" {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var threadID string
	err := database.QueryRowContext(ctx, "SELECT thread_id FROM sessions WHERE internal_session_id = $1", internalID).Scan(&threadID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return threadID, err
}

func SaveConversationMapping(database *sql.DB, externalID, internalID string) error {
	return SaveSessionID(database, externalID, internalID)
}

type ConversationTurnState struct {
	ExternalID    string
	InternalID    string
	IsProcessing  bool
	LastMessageID string
	LastPrompt    string
	UpdatedAt     time.Time
}

func RegisterTurn(database *sql.DB, externalID, messageID, prompt string) error {
	if database == nil || externalID == "" {
		return nil
	}
	msg := Message{
		ID:        messageID,
		ThreadID:  externalID,
		Content:   prompt,
		Status:    StatusProcessing,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	return InsertMessage(database, msg)
}

func SetTurnProcessing(database *sql.DB, externalID string, isProcessing bool, lastMessageID string) error {
	if database == nil || externalID == "" {
		return nil
	}
	status := StatusCompleted
	if isProcessing {
		status = StatusProcessing
	}
	if lastMessageID != "" {
		return UpdateMessageStatus(database, lastMessageID, status, "")
	}
	return nil
}

func GetTurnState(database *sql.DB, externalID string) (*ConversationTurnState, error) {
	if database == nil || externalID == "" {
		return nil, nil
	}
	sessID, _ := GetSessionID(database, externalID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var m Message
	query := `
	SELECT id, thread_id, status, content, updated_at
	FROM messages
	WHERE thread_id = $1
	ORDER BY created_at DESC
	LIMIT 1
	`
	err := database.QueryRowContext(ctx, query, externalID).Scan(&m.ID, &m.ThreadID, &m.Status, &m.Content, &m.UpdatedAt)
	if err == sql.ErrNoRows {
		return &ConversationTurnState{
			ExternalID: externalID,
			InternalID: sessID,
		}, nil
	}
	if err != nil {
		return nil, err
	}
	return &ConversationTurnState{
		ExternalID:    externalID,
		InternalID:    sessID,
		IsProcessing:  m.Status == StatusProcessing,
		LastMessageID: m.ID,
		LastPrompt:    m.Content,
		UpdatedAt:     m.UpdatedAt,
	}, nil
}

func GetInterruptedTurns(database *sql.DB) ([]ConversationTurnState, error) {
	if database == nil {
		return nil, nil
	}
	messages, err := GetPendingOrProcessingMessages(database)
	if err != nil {
		return nil, err
	}
	var results []ConversationTurnState
	for _, m := range messages {
		sessID, _ := GetSessionID(database, m.ThreadID)
		results = append(results, ConversationTurnState{
			ExternalID:    m.ThreadID,
			InternalID:    sessID,
			IsProcessing:  m.Status == StatusProcessing,
			LastMessageID: m.ID,
			LastPrompt:    m.Content,
			UpdatedAt:     m.UpdatedAt,
		})
	}
	return results, nil
}

// Scheduling definitions

type OneShotSchedule struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
}

type CronSchedule struct {
	ID          string    `json:"id"`
	TargetID    string    `json:"target_id"`
	TitlePrefix string    `json:"title_prefix"`
	CronExpr    string    `json:"cron_expr"`
	Prompt      string    `json:"prompt"`
	Timezone    string    `json:"timezone"`
	NextRunAt   time.Time `json:"next_run_at"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
}

func CreateOneShotSchedule(database *sql.DB, s OneShotSchedule) error {
	if database == nil {
		return nil
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at) VALUES ($1, $2, $3, $4, $5)`
	_, err := database.ExecContext(ctx, query, s.ID, s.ThreadID, s.Prompt, s.RunAt, s.CreatedAt)
	return err
}

func GetDueOneShotSchedules(database *sql.DB) ([]OneShotSchedule, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT id, thread_id, prompt, run_at, created_at FROM one_shot_schedules WHERE run_at <= $1`
	rows, err := database.QueryContext(ctx, query, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []OneShotSchedule
	for rows.Next() {
		var s OneShotSchedule
		if err := rows.Scan(&s.ID, &s.ThreadID, &s.Prompt, &s.RunAt, &s.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, s)
	}
	return results, nil
}

func DeleteOneShotSchedule(database *sql.DB, id string) error {
	if database == nil || id == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := database.ExecContext(ctx, `DELETE FROM one_shot_schedules WHERE id = $1`, id)
	return err
}

func InsertMessageAndConsumeOneShot(database *sql.DB, scheduleID string, msg Message) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if msg.ID == "" {
		return fmt.Errorf("message id cannot be empty")
	}
	if msg.Status == "" {
		msg.Status = StatusPending
	}
	now := time.Now().UTC()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.UpdatedAt.IsZero() {
		msg.UpdatedAt = now
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	insertQuery := `
	INSERT INTO messages (id, thread_id, guild_id, author_id, author_name, content, summary, status, retry_count, error_message, response_text, schedule_run_id, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	ON CONFLICT (id) DO NOTHING;
	`
	if _, err := tx.ExecContext(ctx, insertQuery, msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName, msg.Content, msg.Summary, msg.Status, msg.RetryCount, msg.ErrorMessage, msg.ResponseText, msg.ScheduleRunID, msg.CreatedAt, msg.UpdatedAt); err != nil {
		return err
	}

	var deletedID string
	deleteQuery := `DELETE FROM one_shot_schedules WHERE id = $1 RETURNING id;`
	err = tx.QueryRowContext(ctx, deleteQuery, scheduleID).Scan(&deletedID)
	if err == sql.ErrNoRows {
		return fmt.Errorf("one-shot schedule %s not found or already consumed", scheduleID)
	}
	if err != nil {
		return err
	}

	return tx.Commit()
}

func GetAllOneShotSchedules(database *sql.DB, threadID string) ([]OneShotSchedule, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT id, thread_id, prompt, run_at, created_at FROM one_shot_schedules`
	var rows *sql.Rows
	var err error
	if threadID != "" {
		query += ` WHERE thread_id = $1 ORDER BY run_at ASC`
		rows, err = database.QueryContext(ctx, query, threadID)
	} else {
		query += ` ORDER BY run_at ASC`
		rows, err = database.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []OneShotSchedule
	for rows.Next() {
		var s OneShotSchedule
		if err := rows.Scan(&s.ID, &s.ThreadID, &s.Prompt, &s.RunAt, &s.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, s)
	}
	return results, nil
}

func CreateCronSchedule(database *sql.DB, c CronSchedule) error {
	if database == nil {
		return nil
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	if c.Timezone == "" {
		c.Timezone = "America/Los_Angeles"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	_, err := database.ExecContext(ctx, query, c.ID, c.TargetID, c.TitlePrefix, c.CronExpr, c.Prompt, c.Timezone, c.NextRunAt, c.Enabled, c.CreatedAt)
	return err
}

func GetDueCronSchedules(database *sql.DB) ([]CronSchedule, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at FROM cron_schedules WHERE enabled = TRUE AND next_run_at <= $1`
	rows, err := database.QueryContext(ctx, query, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []CronSchedule
	for rows.Next() {
		var c CronSchedule
		if err := rows.Scan(&c.ID, &c.TargetID, &c.TitlePrefix, &c.CronExpr, &c.Prompt, &c.Timezone, &c.NextRunAt, &c.Enabled, &c.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, c)
	}
	return results, nil
}

func GetAllCronSchedules(database *sql.DB, targetID string) ([]CronSchedule, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at FROM cron_schedules WHERE enabled = TRUE`
	var rows *sql.Rows
	var err error
	if targetID != "" {
		query += ` AND target_id = $1 ORDER BY created_at ASC`
		rows, err = database.QueryContext(ctx, query, targetID)
	} else {
		query += ` ORDER BY created_at ASC`
		rows, err = database.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []CronSchedule
	for rows.Next() {
		var c CronSchedule
		if err := rows.Scan(&c.ID, &c.TargetID, &c.TitlePrefix, &c.CronExpr, &c.Prompt, &c.Timezone, &c.NextRunAt, &c.Enabled, &c.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, c)
	}
	return results, nil
}

func DeleteCronSchedule(database *sql.DB, id string) error {
	if database == nil || id == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := database.ExecContext(ctx, `DELETE FROM cron_schedules WHERE id = $1`, id)
	return err
}

func UpdateCronNextRun(database *sql.DB, id string, nextRunAt time.Time) error {
	if database == nil || id == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := database.ExecContext(ctx, `UPDATE cron_schedules SET next_run_at = $1 WHERE id = $2`, nextRunAt, id)
	return err
}

// Schedule Execution Run definitions and CRUD

type ScheduleRun struct {
	ID           string     `json:"id"`
	ScheduleID   string     `json:"schedule_id"`
	ScheduleType string     `json:"schedule_type"`
	MessageID    string     `json:"message_id"`
	TargetID     string     `json:"target_id"`
	ThreadID     string     `json:"thread_id"`
	Title        string     `json:"title"`
	Prompt       string     `json:"prompt"`
	Status       string     `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	DurationMs   int64      `json:"duration_ms"`
	Error        string     `json:"error,omitempty"`
}

type UpdateRunParams struct {
	RunID       string    `json:"run_id"`
	MessageID   string    `json:"message_id"`
	Status      string    `json:"status"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMs  int64     `json:"duration_ms"`
	Error       string    `json:"error"`
}

type ScheduleSummaryMetrics struct {
	TotalActive    int        `json:"total_active"`
	CronCount      int        `json:"cron_count"`
	OneShotCount   int        `json:"one_shot_count"`
	TotalRuns24h   int        `json:"total_runs_24h"`
	NextRunAt      *time.Time `json:"next_run_at"`
	SuccessRate24h float64    `json:"success_rate_24h"`
}

func CreateScheduleRun(database *sql.DB, run ScheduleRun) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if run.ID == "" {
		return fmt.Errorf("schedule run id cannot be empty")
	}
	if run.Status == "" {
		run.Status = "enqueued"
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = time.Now().UTC()
	}

	var completedAtVal interface{}
	if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
		completedAtVal = *run.CompletedAt
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	INSERT INTO schedule_runs (id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`
	_, err := database.ExecContext(ctx, query,
		run.ID,
		run.ScheduleID,
		run.ScheduleType,
		run.MessageID,
		run.TargetID,
		run.ThreadID,
		run.Title,
		run.Prompt,
		run.Status,
		run.StartedAt,
		completedAtVal,
		run.DurationMs,
		run.Error,
	)
	return err
}

func UpdateScheduleRunStatus(database *sql.DB, params UpdateRunParams) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if params.RunID == "" {
		return fmt.Errorf("schedule run id cannot be empty")
	}

	var completedAtVal interface{}
	if !params.CompletedAt.IsZero() {
		completedAtVal = params.CompletedAt
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	UPDATE schedule_runs
	SET status = CASE WHEN $1 != '' THEN $2 ELSE status END,
	    message_id = CASE WHEN $3 != '' THEN $4 ELSE message_id END,
	    completed_at = CASE WHEN $5 IS NOT NULL THEN $6 ELSE completed_at END,
	    duration_ms = CASE WHEN $7 != 0 THEN $8 ELSE duration_ms END,
	    error = CASE WHEN $9 = 'completed' THEN '' WHEN $10 != '' THEN $11 ELSE error END
	WHERE id = $12
	`
	_, err := database.ExecContext(ctx, query,
		params.Status, params.Status,
		params.MessageID, params.MessageID,
		completedAtVal, completedAtVal,
		params.DurationMs, params.DurationMs,
		params.Status, params.Error, params.Error,
		params.RunID,
	)
	return err
}

func GetScheduleRunsPaginated(database *sql.DB, limit, offset int, scheduleID, status string) ([]ScheduleRun, int, error) {
	if database == nil {
		return nil, 0, fmt.Errorf("database is nil")
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	var whereClauses []string
	var args []interface{}
	argIdx := 1

	if strings.TrimSpace(scheduleID) != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("schedule_id = $%d", argIdx))
		args = append(args, strings.TrimSpace(scheduleID))
		argIdx++
	}
	if strings.TrimSpace(status) != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, strings.TrimSpace(status))
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	countQuery := "SELECT COUNT(*) FROM schedule_runs" + whereSQL
	var total int
	if err := database.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count schedule runs: %w", err)
	}

	selectQuery := fmt.Sprintf(`
		SELECT id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, completed_at, duration_ms, error
		FROM schedule_runs
		%s
		ORDER BY started_at DESC, id DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, argIdx, argIdx+1)

	queryArgs := append(args, limit, offset)
	rows, err := database.QueryContext(ctx, selectQuery, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query schedule runs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	runs := make([]ScheduleRun, 0)
	for rows.Next() {
		var r ScheduleRun
		var completedAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.ScheduleID, &r.ScheduleType, &r.MessageID, &r.TargetID, &r.ThreadID, &r.Title, &r.Prompt, &r.Status, &r.StartedAt, &completedAt, &r.DurationMs, &r.Error); err != nil {
			return nil, 0, fmt.Errorf("failed to scan schedule run: %w", err)
		}
		if completedAt.Valid {
			r.CompletedAt = &completedAt.Time
		}
		runs = append(runs, r)
	}

	return runs, total, nil
}

func GetScheduleSummaryMetrics(database *sql.DB) (ScheduleSummaryMetrics, error) {
	if database == nil {
		return ScheduleSummaryMetrics{}, fmt.Errorf("database is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var metrics ScheduleSummaryMetrics

	// 1. Cron count (enabled only)
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM cron_schedules WHERE enabled = TRUE").Scan(&metrics.CronCount); err != nil {
		return metrics, fmt.Errorf("failed to count cron schedules: %w", err)
	}

	// 2. One-shot count
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM one_shot_schedules").Scan(&metrics.OneShotCount); err != nil {
		return metrics, fmt.Errorf("failed to count one-shot schedules: %w", err)
	}

	metrics.TotalActive = metrics.CronCount + metrics.OneShotCount

	// 3. Next run timestamp
	var cronNext, oneShotNext sql.NullTime
	errCron := database.QueryRowContext(ctx, "SELECT next_run_at FROM cron_schedules WHERE enabled = TRUE ORDER BY next_run_at ASC LIMIT 1").Scan(&cronNext)
	if errCron != nil && errCron != sql.ErrNoRows {
		return metrics, fmt.Errorf("failed to query next cron run: %w", errCron)
	}
	errOneShot := database.QueryRowContext(ctx, "SELECT run_at FROM one_shot_schedules ORDER BY run_at ASC LIMIT 1").Scan(&oneShotNext)
	if errOneShot != nil && errOneShot != sql.ErrNoRows {
		return metrics, fmt.Errorf("failed to query next one-shot run: %w", errOneShot)
	}

	if cronNext.Valid && oneShotNext.Valid {
		if cronNext.Time.Before(oneShotNext.Time) {
			metrics.NextRunAt = &cronNext.Time
		} else {
			metrics.NextRunAt = &oneShotNext.Time
		}
	} else if cronNext.Valid {
		metrics.NextRunAt = &cronNext.Time
	} else if oneShotNext.Valid {
		metrics.NextRunAt = &oneShotNext.Time
	}

	// 4. 24-hour run stats and success rate
	cutoff24h := time.Now().UTC().Add(-24 * time.Hour)
	runs24hQuery := `
	SELECT 
		COUNT(*),
		COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0)
	FROM schedule_runs
	WHERE started_at >= $1
	`
	var totalRuns, completedRuns int
	if err := database.QueryRowContext(ctx, runs24hQuery, cutoff24h).Scan(&totalRuns, &completedRuns); err != nil {
		return metrics, fmt.Errorf("failed to query 24h run metrics: %w", err)
	}

	metrics.TotalRuns24h = totalRuns
	if totalRuns == 0 {
		metrics.SuccessRate24h = 100.0
	} else {
		metrics.SuccessRate24h = math.Round((float64(completedRuns)/float64(totalRuns))*1000.0) / 10.0
	}

	return metrics, nil
}

func ReconcileOrphanedScheduleRuns(database *sql.DB) (int64, error) {
	if database == nil {
		return 0, fmt.Errorf("database is nil")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	UPDATE schedule_runs
	SET status = 'failed',
	    error = 'Interrupted by server restart',
	    completed_at = $1
	WHERE status IN ('enqueued', 'running')
	`
	res, err := database.ExecContext(ctx, query, now)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func PruneScheduleRuns(database *sql.DB, maxCount int, maxAge time.Duration) (int64, error) {
	if database == nil {
		return 0, fmt.Errorf("database is nil")
	}
	if maxCount <= 0 {
		maxCount = 1000
	}
	if maxAge <= 0 {
		maxAge = 30 * 24 * time.Hour
	}
	cutoff := time.Now().UTC().Add(-maxAge)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
	DELETE FROM schedule_runs
	WHERE started_at < $1
	   OR id NOT IN (
		   SELECT id FROM schedule_runs
		   ORDER BY started_at DESC, id DESC
		   LIMIT $2
	   )
	`
	res, err := database.ExecContext(ctx, query, cutoff, maxCount)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Memory and Fact definitions

type Fact struct {
	ID         int64     `json:"id"`
	Category   string    `json:"category"`
	FactText   string    `json:"fact_text"`
	Importance float64   `json:"importance"`
	ThreadID   string    `json:"thread_id"`
	CreatedAt  time.Time `json:"created_at"`
}

type FactWithEmbedding struct {
	Fact      Fact
	Embedding []float32
}

func Float32ToBytes(slice []float32) []byte {
	buf := make([]byte, len(slice)*4)
	for i, f := range slice {
		bits := math.Float32bits(f)
		binary.LittleEndian.PutUint32(buf[i*4:], bits)
	}
	return buf
}

func BytesToFloat32(buf []byte) []float32 {
	if len(buf)%4 != 0 {
		return nil
	}
	slice := make([]float32, len(buf)/4)
	for i := range slice {
		bits := binary.LittleEndian.Uint32(buf[i*4:])
		slice[i] = math.Float32frombits(bits)
	}
	return slice
}

func InsertFact(database *sql.DB, category, factText string, importance float64, threadID string, embedding []float32) (int64, error) {
	if database == nil {
		return 0, fmt.Errorf("database is nil")
	}
	if factText == "" {
		return 0, fmt.Errorf("fact text cannot be empty")
	}
	if category == "" {
		category = "general"
	}
	if importance <= 0 {
		importance = 1.0
	}
	now := time.Now().UTC()

	var vecVal interface{}
	if len(embedding) == ExpectedEmbeddingDim {
		if isPostgres(database) {
			vecVal = pgvector.NewVector(embedding)
		} else {
			vecVal = Float32ToBytes(embedding)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `INSERT INTO facts (category, fact_text, importance, thread_id, embedding, created_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`
	var insertedID int64
	err := database.QueryRowContext(ctx, query, category, factText, importance, threadID, vecVal, now).Scan(&insertedID)
	if err != nil {
		return 0, err
	}
	return insertedID, nil
}

type NullVector struct {
	Vector []float32
	Valid  bool
}

func (nv *NullVector) Scan(src any) error {
	if src == nil {
		nv.Vector = nil
		nv.Valid = false
		return nil
	}
	switch v := src.(type) {
	case string:
		var vec pgvector.Vector
		if err := vec.Scan(v); err == nil {
			nv.Vector = vec.Slice()
			nv.Valid = true
			return nil
		}
	case []byte:
		var vec pgvector.Vector
		if err := vec.Scan(v); err == nil {
			nv.Vector = vec.Slice()
			nv.Valid = true
			return nil
		}
		if len(v)%4 == 0 {
			nv.Vector = BytesToFloat32(v)
			nv.Valid = len(nv.Vector) > 0
			return nil
		}
	}
	var vec pgvector.Vector
	if err := vec.Scan(src); err != nil {
		return err
	}
	nv.Vector = vec.Slice()
	nv.Valid = true
	return nil
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

func UpdateFactEmbedding(database *sql.DB, id int64, embedding []float32) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(embedding))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	vec := pgvector.NewVector(embedding)
	_, err := database.ExecContext(ctx, "UPDATE facts SET embedding = $1 WHERE id = $2", vec, id)
	return err
}

func GetAllFactsWithEmbeddings(database *sql.DB) ([]FactWithEmbedding, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `SELECT id, category, fact_text, importance, thread_id, embedding, created_at FROM facts ORDER BY created_at DESC`
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []FactWithEmbedding
	for rows.Next() {
		var f Fact
		var nv NullVector
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &nv, &f.CreatedAt); err != nil {
			return nil, err
		}
		var emb []float32
		if nv.Valid {
			emb = nv.Vector
		}
		results = append(results, FactWithEmbedding{
			Fact:      f,
			Embedding: emb,
		})
	}
	return results, nil
}

func GetFactsByThreadWithEmbeddings(database *sql.DB, threadID string) ([]FactWithEmbedding, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `SELECT id, category, fact_text, importance, thread_id, embedding, created_at FROM facts`
	var rows *sql.Rows
	var err error
	if threadID != "" {
		query += ` WHERE thread_id = $1 ORDER BY created_at DESC`
		rows, err = database.QueryContext(ctx, query, threadID)
	} else {
		query += ` ORDER BY created_at DESC`
		rows, err = database.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []FactWithEmbedding
	for rows.Next() {
		var f Fact
		var nv NullVector
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &nv, &f.CreatedAt); err != nil {
			return nil, err
		}
		var emb []float32
		if nv.Valid {
			emb = nv.Vector
		}
		results = append(results, FactWithEmbedding{
			Fact:      f,
			Embedding: emb,
		})
	}
	return results, nil
}

// SearchSimilarFacts executes an HNSW index-accelerated candidate fetch followed by importance-weighted scoring.
func SearchSimilarFacts(database *sql.DB, embedding []float32, limit int, minScore float64, threadID string) ([]Fact, error) {
	if database == nil || len(embedding) != ExpectedEmbeddingDim {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	if minScore <= 0 {
		minScore = 0.20
	}

	if !isPostgres(database) {
		// SQLite in-memory fallback for unit tests: rank in memory
		allFacts, err := GetAllFactsWithEmbeddings(database)
		if err != nil {
			return nil, err
		}
		type scoredFact struct {
			fact  Fact
			score float64
		}
		var scored []scoredFact
		for _, fwe := range allFacts {
			if threadID != "" && fwe.Fact.ThreadID != "" && fwe.Fact.ThreadID != threadID {
				continue
			}
			if len(fwe.Embedding) != ExpectedEmbeddingDim {
				continue
			}
			sim := cosineSimilarity(embedding, fwe.Embedding)
			totalScore := sim * fwe.Fact.Importance
			if totalScore >= minScore {
				scored = append(scored, scoredFact{fact: fwe.Fact, score: totalScore})
			}
		}
		sort.Slice(scored, func(i, j int) bool {
			return scored[i].score > scored[j].score
		})
		var facts []Fact
		for i := 0; i < len(scored) && i < limit; i++ {
			facts = append(facts, scored[i].fact)
		}
		return facts, nil
	}

	candidateLimit := limit * 3
	if candidateLimit < 30 {
		candidateLimit = 30
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	WITH candidates AS (
		SELECT 
			id, 
			category, 
			fact_text, 
			importance, 
			thread_id, 
			created_at,
			(1.0 - (embedding <=> $1)) AS similarity
		FROM facts
		WHERE ($2 = '' OR thread_id = $2 OR thread_id = '')
		  AND embedding IS NOT NULL
		ORDER BY embedding <=> $1
		LIMIT $3
	)
	SELECT 
		id, 
		category, 
		fact_text, 
		importance, 
		thread_id, 
		created_at
	FROM candidates
	WHERE (similarity * importance) >= $4
	ORDER BY (similarity * importance) DESC
	LIMIT $5;
	`

	vec := pgvector.NewVector(embedding)
	rows, err := database.QueryContext(ctx, query, vec, threadID, candidateLimit, minScore, limit)
	if err != nil {
		return nil, fmt.Errorf("vector search failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var facts []Fact
	for rows.Next() {
		var f Fact
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &f.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan fact error: %w", err)
		}
		facts = append(facts, f)
	}

	return facts, nil
}

// GetMaxMessageRowID returns the maximum row_id for COMPLETED messages in the specified thread.
func GetMaxMessageRowID(database *sql.DB, threadID string) (int64, error) {
	if database == nil || threadID == "" {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var maxRowID sql.NullInt64
	query := `SELECT MAX(row_id) FROM messages WHERE thread_id = $1 AND status = 'COMPLETED'`
	err := database.QueryRowContext(ctx, query, threadID).Scan(&maxRowID)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if maxRowID.Valid {
		return maxRowID.Int64, nil
	}
	return 0, nil
}

func GetActiveConversationsForExtraction(database *sql.DB, activeHours int) ([]string, error) {
	if database == nil {
		return nil, nil
	}
	if activeHours <= 0 {
		activeHours = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(activeHours) * time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
	SELECT DISTINCT m.thread_id
	FROM messages m
	LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.thread_id != ''
	  AND m.created_at >= $1
	  AND m.status = 'COMPLETED'
	  AND (s.last_extracted_rowid IS NULL OR m.row_id > s.last_extracted_rowid)
	ORDER BY m.thread_id
	LIMIT 20
	`
	rows, err := database.QueryContext(ctx, query, cutoff)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var tids []string
	for rows.Next() {
		var tid string
		if err := rows.Scan(&tid); err != nil {
			return nil, err
		}
		if tid != "" {
			tids = append(tids, tid)
		}
	}
	return tids, nil
}

func UpdateConversationFactWatermark(database *sql.DB, threadID string, maxRowID int64) error {
	if database == nil || threadID == "" {
		return nil
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	INSERT INTO sessions (thread_id, internal_session_id, last_extracted_rowid, fact_extracted_at, created_at, updated_at)
	VALUES ($1, '', $2, $3, $4, $5)
	ON CONFLICT(thread_id) DO UPDATE SET
		last_extracted_rowid = CASE 
			WHEN EXCLUDED.last_extracted_rowid > sessions.last_extracted_rowid 
			THEN EXCLUDED.last_extracted_rowid 
			ELSE sessions.last_extracted_rowid 
		END,
		fact_extracted_at = EXCLUDED.fact_extracted_at,
		updated_at = EXCLUDED.updated_at
	`
	_, err := database.ExecContext(ctx, query, threadID, maxRowID, now, now, now)
	return err
}

func UpdateConversationFactExtractedAt(database *sql.DB, threadID string) error {
	maxRowID, _ := GetMaxMessageRowID(database, threadID)
	return UpdateConversationFactWatermark(database, threadID, maxRowID)
}

type FactsFilter struct {
	Category string `json:"category"`
	Query    string `json:"query"`
	Limit    int    `json:"limit"`
	Offset   int    `json:"offset"`
}

type FactsResult struct {
	Facts  []Fact `json:"facts"`
	Total  int    `json:"total"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

func EscapeSQLLike(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

func GetFactsPaginated(database *sql.DB, filter FactsFilter) (*FactsResult, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}

	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 50
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	var whereClauses []string
	var args []interface{}
	argIdx := 1

	if strings.TrimSpace(filter.Category) != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("category = $%d", argIdx))
		args = append(args, strings.TrimSpace(filter.Category))
		argIdx++
	}

	if strings.TrimSpace(filter.Query) != "" {
		escaped := EscapeSQLLike(strings.TrimSpace(filter.Query))
		whereClauses = append(whereClauses, fmt.Sprintf("fact_text ILIKE $%d ESCAPE '\\'", argIdx))
		args = append(args, "%"+escaped+"%")
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	countQuery := "SELECT COUNT(*) FROM facts" + whereSQL
	var total int
	if err := database.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to count facts: %w", err)
	}

	selectQuery := fmt.Sprintf(`
		SELECT id, category, fact_text, importance, thread_id, created_at
		FROM facts
		%s
		ORDER BY created_at DESC, id DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, argIdx, argIdx+1)

	queryArgs := append(args, filter.Limit, filter.Offset)
	rows, err := database.QueryContext(ctx, selectQuery, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query facts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	facts := make([]Fact, 0)
	for rows.Next() {
		var f Fact
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &f.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan fact: %w", err)
		}
		facts = append(facts, f)
	}

	return &FactsResult{
		Facts:  facts,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}, nil
}
