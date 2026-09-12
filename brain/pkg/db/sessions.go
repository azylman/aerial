package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type ConversationTurnState struct {
	ExternalID    string
	InternalID    string
	IsProcessing  bool
	LastMessageID string
	LastPrompt    string
	UpdatedAt     time.Time
}

// GetSessionID retrieves the active session ID for a thread.
func GetSessionID(database DBTX, threadID string) (string, error) {
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

// GetPreviousSessionID retrieves the previous session ID for a thread.
func GetPreviousSessionID(database DBTX, threadID string) (string, error) {
	if database == nil || threadID == "" {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var prevSessionID string
	err := database.QueryRowContext(ctx, "SELECT COALESCE(previous_session_id, '') FROM sessions WHERE thread_id = $1", threadID).Scan(&prevSessionID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return prevSessionID, err
}

// ParseDBTime attempts to parse a database time value from various types and layouts.
func ParseDBTime(val any) (time.Time, bool) {
	if val == nil {
		return time.Time{}, false
	}
	switch v := val.(type) {
	case time.Time:
		return v, true
	case *time.Time:
		if v != nil {
			return *v, true
		}
		return time.Time{}, false
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return time.Time{}, false
		}
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05 -0700 MST",
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05.999999999Z07:00",
			"2006-01-02 15:04:05-07:00",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
		} {
			if t, err := time.Parse(layout, trimmed); err == nil {
				return t, true
			}
		}
	case []byte:
		return ParseDBTime(string(v))
	}
	return time.Time{}, false
}

// SessionInfo encapsulates active session state and lifecycle timestamps.
type SessionInfo struct {
	ThreadID          string
	InternalSessionID string
	PreviousSessionID string
	TurnCount         int
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// GetSessionInfo retrieves the full session state for a thread in a single atomic query.
func GetSessionInfo(database DBTX, threadID string) (*SessionInfo, error) {
	if database == nil || threadID == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		info         SessionInfo
		rawCreatedAt any
		rawUpdatedAt any
	)
	query := `
	SELECT thread_id, internal_session_id, COALESCE(previous_session_id, ''), turn_count, created_at, updated_at
	FROM sessions
	WHERE thread_id = $1
	`
	err := database.QueryRowContext(ctx, query, threadID).Scan(
		&info.ThreadID,
		&info.InternalSessionID,
		&info.PreviousSessionID,
		&info.TurnCount,
		&rawCreatedAt,
		&rawUpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if t, ok := ParseDBTime(rawCreatedAt); ok {
		info.CreatedAt = t
	}
	if t, ok := ParseDBTime(rawUpdatedAt); ok {
		info.UpdatedAt = t
	}
	return &info, nil
}

// SaveSessionID associates an internal session ID with a thread ID.
func SaveSessionID(database DBTX, threadID, sessionID string) error {
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
		created_at = CASE
			WHEN sessions.internal_session_id != EXCLUDED.internal_session_id THEN EXCLUDED.created_at
			ELSE sessions.created_at
		END,
		internal_session_id = EXCLUDED.internal_session_id,
		updated_at = EXCLUDED.updated_at
	`
	_, err := database.ExecContext(ctx, query, threadID, sessionID, now, now)
	return err
}

// DeleteSessionID removes the session association for a thread.
func DeleteSessionID(database DBTX, threadID string) error {
	if database == nil || threadID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := database.ExecContext(ctx, "DELETE FROM sessions WHERE thread_id = $1", threadID)
	return err
}

// IncrementSessionTurnCount atomically increments and returns the turn count for a session key.
func IncrementSessionTurnCount(database DBTX, sessionKey string) (int, error) {
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

// RotateSessionID updates the internal session ID, stores the previous session ID, and resets turn count to zero.
func RotateSessionID(database DBTX, sessionKey, newSessionID string) error {
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
	INSERT INTO sessions (thread_id, internal_session_id, previous_session_id, turn_count, created_at, updated_at)
	VALUES ($1, $2, '', 0, $3, $4)
	ON CONFLICT(thread_id) DO UPDATE SET
		previous_session_id = CASE
			WHEN sessions.internal_session_id != '' AND sessions.internal_session_id != EXCLUDED.internal_session_id
				THEN sessions.internal_session_id
			ELSE sessions.previous_session_id
		END,
		internal_session_id = EXCLUDED.internal_session_id,
		turn_count = 0,
		created_at = EXCLUDED.created_at,
		updated_at = EXCLUDED.updated_at;
	`
	_, err := database.ExecContext(ctx, query, sessionKey, newSessionID, now, now)
	return err
}

// GetSessionTurnCount returns the current turn count for a session.
func GetSessionTurnCount(database DBTX, sessionKey string) (int, error) {
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

// GetInternalConversationID is a legacy helper returning the internal session ID for an external ID.
func GetInternalConversationID(database DBTX, externalID string) (string, error) {
	return GetSessionID(database, externalID)
}

// GetExternalConversationID retrieves the external thread ID given an internal session ID.
func GetExternalConversationID(database DBTX, internalID string) (string, error) {
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

// SaveConversationMapping associates an external conversation ID with an internal session ID.
func SaveConversationMapping(database DBTX, externalID, internalID string) error {
	return SaveSessionID(database, externalID, internalID)
}

// RegisterTurn records a new conversation turn into the database queue.
func RegisterTurn(database DBTX, externalID, messageID, prompt string) error {
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

// SetTurnProcessing updates the turn processing status.
func SetTurnProcessing(database DBTX, externalID string, isProcessing bool, lastMessageID string) error {
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

// GetTurnState retrieves the current active state for a conversation turn.
func GetTurnState(database DBTX, externalID string) (*ConversationTurnState, error) {
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

// GetInterruptedTurns retrieves turns that were interrupted mid-processing across server restarts.
func GetInterruptedTurns(database DBTX) ([]ConversationTurnState, error) {
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

// SaveThreadSummary persists a thread summary and its last summarized message ID watermark in the sessions table.
func SaveThreadSummary(database *sql.DB, threadID, summary, lastSummarizedMsgID string) error {
	if database == nil || threadID == "" {
		return nil
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	INSERT INTO sessions (thread_id, summary, last_summarized_message_id, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $4)
	ON CONFLICT(thread_id) DO UPDATE SET
		summary = EXCLUDED.summary,
		last_summarized_message_id = EXCLUDED.last_summarized_message_id,
		updated_at = EXCLUDED.updated_at
	`
	_, err := database.ExecContext(ctx, query, threadID, summary, lastSummarizedMsgID, now)
	return err
}

// GetThreadSummary retrieves the cached thread summary and last_summarized_message_id watermark for a thread.
func GetThreadSummary(database *sql.DB, threadID string) (summary string, lastSummarizedMsgID string, err error) {
	if database == nil || threadID == "" {
		return "", "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var sum, lastMsgID sql.NullString
	query := `SELECT summary, last_summarized_message_id FROM sessions WHERE thread_id = $1`
	err = database.QueryRowContext(ctx, query, threadID).Scan(&sum, &lastMsgID)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	return sum.String, lastMsgID.String, nil
}

