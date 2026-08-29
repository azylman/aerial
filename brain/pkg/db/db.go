package db

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	StatusPending    = "PENDING"
	StatusProcessing = "PROCESSING"
	StatusCompleted  = "COMPLETED"
	StatusFailed     = "FAILED"
)

type Message struct {
	ID           string    `json:"id"`
	ThreadID     string    `json:"thread_id"`
	GuildID      string    `json:"guild_id"`
	AuthorID     string    `json:"author_id"`
	AuthorName   string    `json:"author_name"`
	Content      string    `json:"content"`
	Status       string    `json:"status"`
	RetryCount   int       `json:"retry_count"`
	ErrorMessage string    `json:"error_message,omitempty"`
	ResponseText string    `json:"response_text,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func GetDBPath() string {
	if _, err := os.Stat("/data"); err == nil {
		return "/data/aerial.db"
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "./aerial.db"
	}
	return filepath.Join(homeDir, ".gemini", "aerial.db")
}

func InitDB(dbPath string) (*sql.DB, error) {
	if dbPath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
	}

	dsn := dbPath
	if dbPath != ":memory:" && !strings.Contains(dbPath, "_pragma") {
		if strings.Contains(dbPath, "?") {
			dsn += "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		} else {
			dsn += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		}
	}

	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	// Configure SQLite PRAGMAs for performance and concurrency
	pragmas := `
	PRAGMA journal_mode = WAL;
	PRAGMA busy_timeout = 5000;
	PRAGMA synchronous = NORMAL;
	`
	if _, err := database.Exec(pragmas); err != nil {
		log.Printf("Warning: failed to execute PRAGMAs: %v", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS messages (
		id TEXT PRIMARY KEY,
		thread_id TEXT NOT NULL,
		guild_id TEXT NOT NULL,
		author_id TEXT NOT NULL,
		author_name TEXT NOT NULL,
		content TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'PENDING',
		retry_count INTEGER NOT NULL DEFAULT 0,
		error_message TEXT,
		response_text TEXT,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_messages_thread_status ON messages(thread_id, status);
	CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);

	CREATE TABLE IF NOT EXISTS sessions (
		thread_id TEXT PRIMARY KEY,
		internal_session_id TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS conversations (
		external_id TEXT PRIMARY KEY,
		internal_id TEXT NOT NULL,
		is_processing BOOLEAN NOT NULL DEFAULT FALSE,
		last_message_id TEXT NOT NULL DEFAULT '',
		last_prompt TEXT NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_conversations_internal_id ON conversations(internal_id);

	CREATE TABLE IF NOT EXISTS one_shot_schedules (
		id TEXT PRIMARY KEY,
		thread_id TEXT NOT NULL,
		prompt TEXT NOT NULL,
		run_at DATETIME NOT NULL,
		created_at DATETIME NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);

	CREATE TABLE IF NOT EXISTS cron_schedules (
		id TEXT PRIMARY KEY,
		target_id TEXT NOT NULL,
		title_prefix TEXT NOT NULL DEFAULT '',
		cron_expr TEXT NOT NULL,
		prompt TEXT NOT NULL,
		timezone TEXT NOT NULL DEFAULT 'UTC',
		next_run_at DATETIME NOT NULL,
		enabled BOOLEAN NOT NULL DEFAULT TRUE,
		created_at DATETIME NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);

	CREATE TABLE IF NOT EXISTS facts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		category TEXT NOT NULL,
		fact_text TEXT NOT NULL,
		importance REAL NOT NULL DEFAULT 1.0,
		thread_id TEXT NOT NULL DEFAULT '',
		embedding BLOB,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_facts_thread_id ON facts(thread_id);
	`
	if _, err := database.Exec(schema); err != nil {
		return nil, err
	}

	// Safe column migration for response_text on existing DBs
	_, _ = database.Exec(`ALTER TABLE messages ADD COLUMN response_text TEXT;`)

	// Safe column migrations for cron_schedules on existing DBs
	_, _ = database.Exec(`ALTER TABLE cron_schedules ADD COLUMN title_prefix TEXT NOT NULL DEFAULT '';`)
	_, _ = database.Exec(`ALTER TABLE cron_schedules ADD COLUMN timezone TEXT NOT NULL DEFAULT 'UTC';`)

	// Data migration from legacy conversations to sessions table if any exist
	_, _ = database.Exec(`
	INSERT OR IGNORE INTO sessions (thread_id, internal_session_id, created_at, updated_at)
	SELECT external_id, internal_id, created_at, updated_at
	FROM conversations
	WHERE internal_id != ''
	`)

	log.Printf("SQLite database initialized and migrated at %s", dbPath)
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
	now := time.Now().UTC()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.UpdatedAt.IsZero() {
		msg.UpdatedAt = now
	}

	query := `
	INSERT OR IGNORE INTO messages (id, thread_id, guild_id, author_id, author_name, content, status, retry_count, error_message, response_text, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err := database.Exec(query, msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName, msg.Content, msg.Status, msg.RetryCount, msg.ErrorMessage, msg.ResponseText, msg.CreatedAt, msg.UpdatedAt)
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
	query := `
	UPDATE messages
	SET status = ?, error_message = ?, updated_at = ?
	WHERE id = ?
	`
	_, err := database.Exec(query, status, errorMsg, now, id)
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
	query := `
	UPDATE messages
	SET status = ?, response_text = ?, error_message = '', updated_at = ?
	WHERE id = ?
	`
	_, err := database.Exec(query, StatusCompleted, responseText, now, id)
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
	query := `
	UPDATE messages
	SET retry_count = retry_count + 1, error_message = ?, updated_at = ?
	WHERE id = ?
	`
	_, err := database.Exec(query, errorMsg, now, id)
	return err
}

func GetPendingOrProcessingMessages(database *sql.DB) ([]Message, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	query := `
	SELECT id, thread_id, guild_id, author_id, author_name, content, status, retry_count, COALESCE(error_message, ''), COALESCE(response_text, ''), created_at, updated_at
	FROM messages
	WHERE status IN ('PENDING', 'PROCESSING')
	ORDER BY created_at ASC
	`
	rows, err := database.Query(query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Status, &m.RetryCount, &m.ErrorMessage, &m.ResponseText, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		results = append(results, m)
	}
	return results, nil
}

func GetMessage(database *sql.DB, id string) (*Message, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if id == "" {
		return nil, nil
	}
	query := `
	SELECT id, thread_id, guild_id, author_id, author_name, content, status, retry_count, COALESCE(error_message, ''), COALESCE(response_text, ''), created_at, updated_at
	FROM messages
	WHERE id = ?
	`
	var m Message
	err := database.QueryRow(query, id).Scan(&m.ID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Status, &m.RetryCount, &m.ErrorMessage, &m.ResponseText, &m.CreatedAt, &m.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// Session CRUD Operations

func GetSessionID(database *sql.DB, threadID string) (string, error) {
	if database == nil || threadID == "" {
		return "", nil
	}
	var sessionID string
	err := database.QueryRow("SELECT internal_session_id FROM sessions WHERE thread_id = ?", threadID).Scan(&sessionID)
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
	query := `
	INSERT INTO sessions (thread_id, internal_session_id, created_at, updated_at)
	VALUES (?, ?, ?, ?)
	ON CONFLICT(thread_id) DO UPDATE SET
		internal_session_id = excluded.internal_session_id,
		updated_at = excluded.updated_at
	`
	_, err := database.Exec(query, threadID, sessionID, now, now)
	return err
}

func DeleteSessionID(database *sql.DB, threadID string) error {
	if database == nil || threadID == "" {
		return nil
	}
	_, err := database.Exec("DELETE FROM sessions WHERE thread_id = ?", threadID)
	return err
}

// Legacy conversation compatibility helpers

func GetInternalConversationID(database *sql.DB, externalID string) (string, error) {
	return GetSessionID(database, externalID)
}

func GetExternalConversationID(database *sql.DB, internalID string) (string, error) {
	if database == nil || internalID == "" {
		return "", nil
	}
	var threadID string
	err := database.QueryRow("SELECT thread_id FROM sessions WHERE internal_session_id = ?", internalID).Scan(&threadID)
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
	var m Message
	query := `
	SELECT id, thread_id, status, content, updated_at
	FROM messages
	WHERE thread_id = ?
	ORDER BY created_at DESC
	LIMIT 1
	`
	err := database.QueryRow(query, externalID).Scan(&m.ID, &m.ThreadID, &m.Status, &m.Content, &m.UpdatedAt)
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
	ID        string
	ThreadID  string
	Prompt    string
	RunAt     time.Time
	CreatedAt time.Time
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
	query := `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at) VALUES (?, ?, ?, ?, ?)`
	_, err := database.Exec(query, s.ID, s.ThreadID, s.Prompt, s.RunAt, s.CreatedAt)
	return err
}

func GetDueOneShotSchedules(database *sql.DB) ([]OneShotSchedule, error) {
	if database == nil {
		return nil, nil
	}
	query := `SELECT id, thread_id, prompt, run_at, created_at FROM one_shot_schedules WHERE run_at <= ?`
	rows, err := database.Query(query, time.Now().UTC())
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
	_, err := database.Exec(`DELETE FROM one_shot_schedules WHERE id = ?`, id)
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

	tx, err := database.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	insertQuery := `
	INSERT OR IGNORE INTO messages (id, thread_id, guild_id, author_id, author_name, content, status, retry_count, error_message, response_text, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	if _, err := tx.Exec(insertQuery, msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName, msg.Content, msg.Status, msg.RetryCount, msg.ErrorMessage, msg.ResponseText, msg.CreatedAt, msg.UpdatedAt); err != nil {
		return err
	}

	if _, err := tx.Exec(`DELETE FROM one_shot_schedules WHERE id = ?`, scheduleID); err != nil {
		return err
	}

	return tx.Commit()
}

func GetAllOneShotSchedules(database *sql.DB, threadID string) ([]OneShotSchedule, error) {
	if database == nil {
		return nil, nil
	}
	query := `SELECT id, thread_id, prompt, run_at, created_at FROM one_shot_schedules`
	var rows *sql.Rows
	var err error
	if threadID != "" {
		query += ` WHERE thread_id = ? ORDER BY run_at ASC`
		rows, err = database.Query(query, threadID)
	} else {
		query += ` ORDER BY run_at ASC`
		rows, err = database.Query(query)
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
	query := `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := database.Exec(query, c.ID, c.TargetID, c.TitlePrefix, c.CronExpr, c.Prompt, c.Timezone, c.NextRunAt, c.Enabled, c.CreatedAt)
	return err
}

func GetDueCronSchedules(database *sql.DB) ([]CronSchedule, error) {
	if database == nil {
		return nil, nil
	}
	query := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at FROM cron_schedules WHERE enabled = TRUE AND next_run_at <= ?`
	rows, err := database.Query(query, time.Now().UTC())
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
	query := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at FROM cron_schedules WHERE enabled = TRUE`
	var rows *sql.Rows
	var err error
	if targetID != "" {
		query += ` AND target_id = ? ORDER BY created_at ASC`
		rows, err = database.Query(query, targetID)
	} else {
		query += ` ORDER BY created_at ASC`
		rows, err = database.Query(query)
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
	_, err := database.Exec(`DELETE FROM cron_schedules WHERE id = ?`, id)
	return err
}

func UpdateCronNextRun(database *sql.DB, id string, nextRunAt time.Time) error {
	if database == nil || id == "" {
		return nil
	}
	_, err := database.Exec(`UPDATE cron_schedules SET next_run_at = ? WHERE id = ?`, nextRunAt, id)
	return err
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
	now := time.Now().UTC()
	var embBytes []byte
	if len(embedding) > 0 {
		embBytes = Float32ToBytes(embedding)
	}
	query := `INSERT INTO facts (category, fact_text, importance, thread_id, embedding, created_at) VALUES (?, ?, ?, ?, ?, ?)`
	res, err := database.Exec(query, category, factText, importance, threadID, embBytes, now)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func GetAllFactsWithEmbeddings(database *sql.DB) ([]FactWithEmbedding, error) {
	if database == nil {
		return nil, nil
	}
	query := `SELECT id, category, fact_text, importance, thread_id, embedding, created_at FROM facts`
	rows, err := database.Query(query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []FactWithEmbedding
	for rows.Next() {
		var f Fact
		var embBytes []byte
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &embBytes, &f.CreatedAt); err != nil {
			return nil, err
		}
		var emb []float32
		if len(embBytes) > 0 {
			emb = BytesToFloat32(embBytes)
		}
		results = append(results, FactWithEmbedding{
			Fact:      f,
			Embedding: emb,
		})
	}
	return results, nil
}

func GetActiveConversationsForExtraction(database *sql.DB, activeHours int) ([]string, error) {
	if database == nil {
		return nil, nil
	}
	if activeHours <= 0 {
		activeHours = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(activeHours) * time.Hour)
	query := `
	SELECT DISTINCT thread_id
	FROM messages
	WHERE created_at >= ?
	`
	rows, err := database.Query(query, cutoff)
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

func UpdateConversationFactExtractedAt(database *sql.DB, threadID string) error {
	if database == nil || threadID == "" {
		return nil
	}
	return nil
}
