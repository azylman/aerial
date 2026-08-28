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
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_conversations_internal_id ON conversations(internal_id);
	`
	if _, err := database.Exec(schema); err != nil {
		return nil, err
	}
	log.Printf("SQLite conversation database initialized at %s", dbPath)
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
