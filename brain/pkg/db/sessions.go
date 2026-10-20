package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/session"
)

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
	err := s.db.QueryRowContext(queryCtx, query, key).Scan(&val)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return val.String, err
}

// GetSessionID retrieves the active session ID for a thread.
func (s *SQLStore) GetSessionID(ctx context.Context, threadID string) (string, error) {
	return s.querySessionString(ctx, threadID, "SELECT internal_session_id FROM sessions WHERE thread_id = $1")
}

// GetPreviousSessionID retrieves the previous session ID for a thread.
func (s *SQLStore) GetPreviousSessionID(ctx context.Context, threadID string) (string, error) {
	return s.querySessionString(ctx, threadID, "SELECT previous_session_id FROM sessions WHERE thread_id = $1")
}

// GetExternalConversationID retrieves the external thread ID given an internal session ID.
func (s *SQLStore) GetExternalConversationID(ctx context.Context, internalID string) (string, error) {
	return s.querySessionString(ctx, internalID, "SELECT thread_id FROM sessions WHERE internal_session_id = $1")
}

var dbTimeLayouts = []string{
	"2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999999 -0700 -0700",
	"2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999",
	time.RFC3339Nano, time.RFC3339, time.DateTime, "2006-01-02 15:04:05", "2006-01-02T15:04:05Z",
	"2006-01-02 15:04:05 -0700 MST", "2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05-07:00", "2006-01-02T15:04:05",
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
		if idx := strings.Index(trimmed, " m="); idx != -1 {
			trimmed = strings.TrimSpace(trimmed[:idx])
		}
		for _, layout := range dbTimeLayouts {
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

func scanSessionInfo(scanner interface{ Scan(dest ...any) error }) (SessionInfo, error) {
	var (
		info                       SessionInfo
		rawCreatedAt, rawUpdatedAt any
	)
	if err := scanner.Scan(&info.ThreadID, &info.InternalSessionID, &info.PreviousSessionID, &info.TurnCount, &rawCreatedAt, &rawUpdatedAt); err != nil {
		return SessionInfo{}, err
	}
	if t, ok := ParseDBTime(rawCreatedAt); ok {
		info.CreatedAt = t
	}
	if t, ok := ParseDBTime(rawUpdatedAt); ok {
		info.UpdatedAt = t
	}
	return info, nil
}

// SaveSessionID associates an internal session ID with a thread ID.
func (s *SQLStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	if threadID == "" || sessionID == "" {
		return nil
	}
	query := `INSERT INTO sessions (thread_id, internal_session_id, created_at, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT(thread_id) DO UPDATE SET created_at = CASE WHEN sessions.internal_session_id != EXCLUDED.internal_session_id THEN EXCLUDED.created_at ELSE sessions.created_at END, internal_session_id = EXCLUDED.internal_session_id, updated_at = EXCLUDED.updated_at`
	_, err := s.execWithTimeout(ctx, 5*time.Second, query, threadID, sessionID, time.Now().UTC(), time.Now().UTC())
	return err
}

// IncrementSessionTurnCount atomically increments and returns the turn count for a session key.
func (s *SQLStore) IncrementSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	if sessionKey == "" {
		return 0, fmt.Errorf("session key cannot be empty")
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO sessions (thread_id, internal_session_id, turn_count, created_at, updated_at) VALUES ($1, '', 1, $2, $3) ON CONFLICT(thread_id) DO UPDATE SET turn_count = sessions.turn_count + 1, updated_at = EXCLUDED.updated_at RETURNING turn_count;`
	var turnCount int
	err := s.db.QueryRowContext(queryCtx, query, sessionKey, time.Now().UTC(), time.Now().UTC()).Scan(&turnCount)
	return turnCount, err
}

// RotateSessionID updates the internal session ID, stores the previous session ID, and resets turn count to zero.
func (s *SQLStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	if sessionKey == "" {
		return fmt.Errorf("session key cannot be empty")
	}
	query := `INSERT INTO sessions (thread_id, internal_session_id, previous_session_id, turn_count, created_at, updated_at) VALUES ($1, $2, '', 0, $3, $4) ON CONFLICT(thread_id) DO UPDATE SET previous_session_id = CASE WHEN sessions.internal_session_id != '' AND sessions.internal_session_id != EXCLUDED.internal_session_id THEN sessions.internal_session_id ELSE sessions.previous_session_id END, internal_session_id = EXCLUDED.internal_session_id, turn_count = 0, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at;`
	_, err := s.execWithTimeout(ctx, 5*time.Second, query, sessionKey, newSessionID, time.Now().UTC(), time.Now().UTC())
	return err
}

// GetSessionTurnCount returns the current turn count for a session.
func (s *SQLStore) GetSessionTurnCount(ctx context.Context, sessionKey string) (int, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	if sessionKey == "" {
		return 0, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var count int
	err := s.db.QueryRowContext(queryCtx, "SELECT turn_count FROM sessions WHERE thread_id = $1", sessionKey).Scan(&count)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return count, err
}

// SaveThreadSummary persists a thread summary and its last summarized message ID watermark in the sessions table.
func (s *SQLStore) SaveThreadSummary(ctx context.Context, threadID, summary, lastSummarizedMsgID string) error {
	if threadID == "" {
		return nil
	}
	query := `INSERT INTO sessions (thread_id, summary, last_summarized_message_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $4) ON CONFLICT(thread_id) DO UPDATE SET summary = EXCLUDED.summary, last_summarized_message_id = EXCLUDED.last_summarized_message_id, updated_at = EXCLUDED.updated_at`
	_, err := s.execWithTimeout(ctx, 5*time.Second, query, threadID, summary, lastSummarizedMsgID, time.Now().UTC())
	return err
}

// GetThreadSummary retrieves the cached thread summary and last_summarized_message_id watermark for a thread.
func (s *SQLStore) GetThreadSummary(ctx context.Context, threadID string) (string, string, error) {
	if err := s.checkDB(); err != nil {
		return "", "", err
	}
	if threadID == "" {
		return "", "", nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var sum, lastMsgID sql.NullString
	err := s.db.QueryRowContext(queryCtx, "SELECT summary, last_summarized_message_id FROM sessions WHERE thread_id = $1", threadID).Scan(&sum, &lastMsgID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	return sum.String, lastMsgID.String, err
}

// GetSessionActivityStats queries the database to determine the most recent session activity.
func (s *SQLStore) GetSessionActivityStats(ctx context.Context, threadID string) (*SessionActivityStats, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(threadID) == "" {
		return &SessionActivityStats{}, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var (
		internalSessionID            sql.NullString
		turnCount                    sql.NullInt64
		rawSessUpdatedAt             any
		completedCount               int64
		rawMsgUpdated, rawMsgCreated any
	)
	if err := s.db.QueryRowContext(queryCtx, `SELECT internal_session_id, turn_count, updated_at FROM sessions WHERE thread_id = $1`, threadID).Scan(&internalSessionID, &turnCount, &rawSessUpdatedAt); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("querying session for thread %s: %w", threadID, err)
	}
	if err := s.db.QueryRowContext(queryCtx, `SELECT COUNT(*), MAX(updated_at), MAX(created_at) FROM messages WHERE thread_id = $1 AND status = 'COMPLETED' AND response_text NOT LIKE '[EXPIRED_STALE]%' AND response_text NOT LIKE '[AMBIENT%' AND response_text NOT LIKE '[IGNORED%'`, threadID).Scan(&completedCount, &rawMsgUpdated, &rawMsgCreated); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("querying completed messages for thread %s: %w", threadID, err)
	}

	stats := &SessionActivityStats{InternalSessionID: internalSessionID.String, TurnCount: int(turnCount.Int64), CompletedTurns: completedCount}
	if t, ok := ParseDBTime(rawSessUpdatedAt); ok {
		stats.SessionUpdatedAt = t
	}
	if t, ok := ParseDBTime(rawMsgUpdated); ok {
		stats.LastMessageAt = t
	}
	if t, ok := ParseDBTime(rawMsgCreated); ok {
		stats.LastCompletedMessageCreatedAt = t
	}
	return stats, nil
}

// DefaultUnrotatedSessionMaxAge defines the default recency horizon (2 hours) for detecting
// unrotated runaway sessions. Dormant sessions older than this horizon are excluded from alerting.
const DefaultUnrotatedSessionMaxAge = 2 * time.Hour

// FindUnrotatedSessions returns sessions whose turn count strictly exceeds minTurns (defaulting to session.DefaultMaxSessionTurns if minTurns <= 0)
// and were updated within maxAge (defaulting to DefaultUnrotatedSessionMaxAge if maxAge is empty; pass <= 0 to disable age filtering), ordered by turn_count DESC.
func (s *SQLStore) FindUnrotatedSessions(ctx context.Context, minTurns int, maxAge ...time.Duration) ([]SessionInfo, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if minTurns <= 0 {
		minTurns = session.DefaultMaxSessionTurns
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	limitAge := DefaultUnrotatedSessionMaxAge
	if len(maxAge) > 0 {
		limitAge = maxAge[0]
	}

	query, args := `SELECT thread_id, internal_session_id, COALESCE(previous_session_id, ''), turn_count, created_at, updated_at FROM sessions WHERE turn_count > $1`, []any{minTurns}
	if limitAge > 0 {
		query += ` AND updated_at >= $2`
		args = append(args, time.Now().UTC().Add(-limitAge))
	}

	rows, err := s.db.QueryContext(queryCtx, query+` ORDER BY turn_count DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []SessionInfo
	for rows.Next() {
		info, err := scanSessionInfo(rows)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, info)
	}
	return sessions, rows.Err()
}
