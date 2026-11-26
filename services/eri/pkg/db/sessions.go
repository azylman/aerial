package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// DiscordSessionID normalizes a raw numeric Discord snowflake into 'discord-<id>'
// while preserving keys that already carry a '<source>-' prefix (e.g. 'discord-*', 'heartbeat-*', 'dispatch-*').
func DiscordSessionID(key string) string {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return ""
	}
	for i := range len(trimmed) {
		if trimmed[i] < '0' || trimmed[i] > '9' {
			return trimmed
		}
	}
	return "discord-" + trimmed
}

func (s *SQLStore) querySessionString(ctx context.Context, key, query string) (string, error) {
	if err := s.checkDB(); err != nil {
		return "", err
	}
	if strings.TrimSpace(key) == "" {
		return "", nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var val sql.NullString
	err := s.db.QueryRowContext(queryCtx, query, s.effectiveAgent(), key).Scan(&val)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return val.String, err
}

// GetSessionID retrieves the active session ID for a thread.
func (s *SQLStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	return s.querySessionString(ctx, DiscordSessionID(threadID), "SELECT conversation_id FROM sessions WHERE agent = $1 AND session_id = $2")
}

// GetPreviousSessionID retrieves the previous session ID for a thread.
func (s *SQLStore) GetPreviousSessionID(ctx context.Context, threadID string) (string, error) {
	return s.querySessionString(ctx, DiscordSessionID(threadID), "SELECT previous_conversation_id FROM sessions WHERE agent = $1 AND session_id = $2")
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

// SaveSessionID associates an internal session ID with a thread ID.
func (s *SQLStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	threadID = DiscordSessionID(threadID)
	sessionID = strings.TrimSpace(sessionID)
	if threadID == "" {
		return nil
	}
	agent := s.effectiveAgent()
	query := `INSERT INTO sessions (agent, session_id, conversation_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT(agent, session_id) DO UPDATE SET created_at = CASE WHEN sessions.conversation_id != EXCLUDED.conversation_id THEN EXCLUDED.created_at ELSE sessions.created_at END, conversation_id = EXCLUDED.conversation_id, updated_at = EXCLUDED.updated_at WHERE EXCLUDED.conversation_id = '' OR sessions.conversation_id = '' OR sessions.previous_conversation_id IS DISTINCT FROM EXCLUDED.conversation_id`
	return s.execWithTimeout(ctx, query, agent, threadID, sessionID, time.Now().UTC(), time.Now().UTC())
}

// IncrementSessionTurnCount atomically increments and returns the turn count for a session key.
func (s *SQLStore) IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	sessionKey = DiscordSessionID(sessionKey)
	if sessionKey == "" {
		return 0, fmt.Errorf("session key cannot be empty")
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	agent := s.effectiveAgent()
	query := `INSERT INTO sessions (agent, session_id, conversation_id, turn_count, created_at, updated_at) VALUES ($1, $2, '', 1, $3, $4) ON CONFLICT(agent, session_id) DO UPDATE SET turn_count = sessions.turn_count + 1, updated_at = EXCLUDED.updated_at RETURNING turn_count;`
	var turnCount int
	err := s.db.QueryRowContext(queryCtx, query, agent, sessionKey, time.Now().UTC(), time.Now().UTC()).Scan(&turnCount)
	return turnCount, err
}

// RotateSessionID updates the internal session ID, stores the previous session ID, and resets turn count to zero.
func (s *SQLStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	sessionKey = DiscordSessionID(sessionKey)
	if sessionKey == "" {
		return fmt.Errorf("session key cannot be empty")
	}
	agent := s.effectiveAgent()
	query := `INSERT INTO sessions (agent, session_id, conversation_id, previous_conversation_id, turn_count, created_at, updated_at) VALUES ($1, $2, $3, '', 0, $4, $5) ON CONFLICT(agent, session_id) DO UPDATE SET previous_conversation_id = CASE WHEN sessions.conversation_id != '' AND sessions.conversation_id != EXCLUDED.conversation_id THEN sessions.conversation_id ELSE sessions.previous_conversation_id END, conversation_id = EXCLUDED.conversation_id, turn_count = 0, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at;`
	return s.execWithTimeout(ctx, query, agent, sessionKey, newSessionID, time.Now().UTC(), time.Now().UTC())
}

// GetSessionTurnCount returns the current turn count for a session.
func (s *SQLStore) GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	sessionKey = DiscordSessionID(sessionKey)
	if sessionKey == "" {
		return 0, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var count int
	err := s.db.QueryRowContext(queryCtx, "SELECT turn_count FROM sessions WHERE agent = $1 AND session_id = $2", s.effectiveAgent(), sessionKey).Scan(&count)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return count, err
}

// SaveThreadSummary persists a thread summary and its last summarized message ID watermark in the sessions table.
func (s *SQLStore) SaveThreadSummary(ctx context.Context, threadID, summary, lastSummarizedMsgID string) error {
	threadID = DiscordSessionID(threadID)
	if threadID == "" {
		return nil
	}
	agent := s.effectiveAgent()
	query := `INSERT INTO sessions (agent, session_id, summary, last_summarized_message_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5) ON CONFLICT(agent, session_id) DO UPDATE SET summary = EXCLUDED.summary, last_summarized_message_id = EXCLUDED.last_summarized_message_id, updated_at = EXCLUDED.updated_at`
	return s.execWithTimeout(ctx, query, agent, threadID, summary, lastSummarizedMsgID, time.Now().UTC())
}

// GetThreadSummary retrieves the cached thread summary and last_summarized_message_id watermark for a thread.
func (s *SQLStore) GetThreadSummary(ctx context.Context, threadID string) (string, string, error) {
	if err := s.checkDB(); err != nil {
		return "", "", err
	}
	threadID = DiscordSessionID(threadID)
	if threadID == "" {
		return "", "", nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var sum, lastMsgID sql.NullString
	err := s.db.QueryRowContext(queryCtx, "SELECT summary, last_summarized_message_id FROM sessions WHERE agent = $1 AND session_id = $2", s.effectiveAgent(), threadID).Scan(&sum, &lastMsgID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	return sum.String, lastMsgID.String, err
}
