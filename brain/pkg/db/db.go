package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

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
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}
	database, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	schema := `
	CREATE TABLE IF NOT EXISTS conversations (
		external_id TEXT PRIMARY KEY,
		internal_id TEXT NOT NULL,
		is_processing BOOLEAN NOT NULL DEFAULT FALSE,
		last_message_id TEXT NOT NULL DEFAULT '',
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
		cron_expr TEXT NOT NULL,
		prompt TEXT NOT NULL,
		next_run_at DATETIME NOT NULL,
		enabled BOOLEAN NOT NULL DEFAULT TRUE,
		created_at DATETIME NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);
	`
	if _, err := database.Exec(schema); err != nil {
		return nil, err
	}

	migrations := []string{
		"ALTER TABLE conversations ADD COLUMN is_processing BOOLEAN NOT NULL DEFAULT FALSE;",
		"ALTER TABLE conversations ADD COLUMN last_message_id TEXT NOT NULL DEFAULT '';",
	}
	for _, m := range migrations {
		_, _ = database.Exec(m)
	}

	log.Printf("SQLite conversation database initialized and migrated at %s", dbPath)
	return database, nil
}

func GetInternalConversationID(database *sql.DB, externalID string) (string, error) {
	if database == nil || externalID == "" {
		return "", nil
	}
	var internalID string
	err := database.QueryRow("SELECT internal_id FROM conversations WHERE external_id = ?", externalID).Scan(&internalID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return internalID, err
}

func GetExternalConversationID(database *sql.DB, internalID string) (string, error) {
	if database == nil || internalID == "" {
		return "", nil
	}
	var externalID string
	err := database.QueryRow("SELECT external_id FROM conversations WHERE internal_id = ?", internalID).Scan(&externalID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return externalID, err
}

func SaveConversationMapping(database *sql.DB, externalID, internalID string) error {
	if database == nil || externalID == "" || internalID == "" {
		return nil
	}
	now := time.Now().UTC()
	query := `
	INSERT INTO conversations (external_id, internal_id, created_at, updated_at)
	VALUES (?, ?, ?, ?)
	ON CONFLICT(external_id) DO UPDATE SET
		internal_id = excluded.internal_id,
		updated_at = excluded.updated_at
	`
	_, err := database.Exec(query, externalID, internalID, now, now)
	if err != nil {
		log.Printf("Failed to save conversation mapping (%s -> %s): %v", externalID, internalID, err)
	} else {
		log.Printf("Saved conversation mapping: %s -> %s", externalID, internalID)
	}
	return err
}

type ConversationTurnState struct {
	ExternalID    string
	InternalID    string
	IsProcessing  bool
	LastMessageID string
	UpdatedAt     time.Time
}

func SetTurnProcessing(database *sql.DB, externalID string, isProcessing bool, lastMessageID string) error {
	if database == nil || externalID == "" {
		return nil
	}
	now := time.Now().UTC()
	var query string
	var err error
	if lastMessageID != "" {
		query = `
		UPDATE conversations 
		SET is_processing = ?, last_message_id = ?, updated_at = ?
		WHERE external_id = ?
		`
		_, err = database.Exec(query, isProcessing, lastMessageID, now, externalID)
	} else {
		query = `
		UPDATE conversations 
		SET is_processing = ?, updated_at = ?
		WHERE external_id = ?
		`
		_, err = database.Exec(query, isProcessing, now, externalID)
	}
	if err != nil {
		log.Printf("Failed to update turn processing state for %s: %v", externalID, err)
	}
	return err
}

func GetTurnState(database *sql.DB, externalID string) (*ConversationTurnState, error) {
	if database == nil || externalID == "" {
		return nil, nil
	}
	var state ConversationTurnState
	query := `
	SELECT external_id, internal_id, is_processing, last_message_id, updated_at
	FROM conversations
	WHERE external_id = ?
	`
	err := database.QueryRow(query, externalID).Scan(&state.ExternalID, &state.InternalID, &state.IsProcessing, &state.LastMessageID, &state.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func GetInterruptedTurns(database *sql.DB) ([]ConversationTurnState, error) {
	if database == nil {
		return nil, nil
	}
	query := `
	SELECT external_id, internal_id, is_processing, last_message_id, updated_at
	FROM conversations
	WHERE is_processing = TRUE
	`
	rows, err := database.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ConversationTurnState
	for rows.Next() {
		var state ConversationTurnState
		if err := rows.Scan(&state.ExternalID, &state.InternalID, &state.IsProcessing, &state.LastMessageID, &state.UpdatedAt); err != nil {
			return nil, err
		}
		results = append(results, state)
	}
	return results, nil
}

type OneShotSchedule struct {
	ID        string
	ThreadID  string
	Prompt    string
	RunAt     time.Time
	CreatedAt time.Time
}

type CronSchedule struct {
	ID        string
	TargetID  string
	CronExpr  string
	Prompt    string
	NextRunAt time.Time
	Enabled   bool
	CreatedAt time.Time
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
	defer rows.Close()

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

func CreateCronSchedule(database *sql.DB, c CronSchedule) error {
	if database == nil {
		return nil
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	query := `INSERT INTO cron_schedules (id, target_id, cron_expr, prompt, next_run_at, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := database.Exec(query, c.ID, c.TargetID, c.CronExpr, c.Prompt, c.NextRunAt, c.Enabled, c.CreatedAt)
	return err
}

func GetDueCronSchedules(database *sql.DB) ([]CronSchedule, error) {
	if database == nil {
		return nil, nil
	}
	query := `SELECT id, target_id, cron_expr, prompt, next_run_at, enabled, created_at FROM cron_schedules WHERE enabled = TRUE AND next_run_at <= ?`
	rows, err := database.Query(query, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []CronSchedule
	for rows.Next() {
		var c CronSchedule
		if err := rows.Scan(&c.ID, &c.TargetID, &c.CronExpr, &c.Prompt, &c.NextRunAt, &c.Enabled, &c.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, c)
	}
	return results, nil
}

func UpdateCronNextRun(database *sql.DB, id string, nextRunAt time.Time) error {
	if database == nil || id == "" {
		return nil
	}
	_, err := database.Exec(`UPDATE cron_schedules SET next_run_at = ? WHERE id = ?`, nextRunAt, id)
	return err
}

