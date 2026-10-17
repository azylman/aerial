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

// GetSessionID retrieves the active session ID for a thread.
func GetSessionID(database DBTX, threadID string) (string, error) {
	if database == nil || threadID == "" {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var sessionID string
	err := database.QueryRowContext(ctx, "SELECT internal_session_id FROM sessions WHERE thread_id = $1", threadID).Scan(&sessionID)
	if errors.Is(err, sql.ErrNoRows) {
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
		if idx := strings.Index(trimmed, " m="); idx != -1 {
			trimmed = strings.TrimSpace(trimmed[:idx])
		}
		for _, layout := range []string{
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05.999999999 -0700 -0700",
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05.999999999",
			time.RFC3339Nano,
			time.RFC3339,
			time.DateTime,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05 -0700 MST",
			"2006-01-02 15:04:05.999999999Z07:00",
			"2006-01-02 15:04:05-07:00",
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

func scanSessionInfo(scanner interface{ Scan(dest ...any) error }) (SessionInfo, error) {
	var (
		info         SessionInfo
		rawCreatedAt any
		rawUpdatedAt any
	)
	if err := scanner.Scan(
		&info.ThreadID,
		&info.InternalSessionID,
		&info.PreviousSessionID,
		&info.TurnCount,
		&rawCreatedAt,
		&rawUpdatedAt,
	); err != nil {
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

// SaveThreadSummary persists a thread summary and its last summarized message ID watermark in the sessions table.
func (s *SQLStore) SaveThreadSummary(ctx context.Context, threadID, summary, lastSummarizedMsgID string) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if threadID == "" {
		return nil
	}
	now := time.Now().UTC()
	query := `
	INSERT INTO sessions (thread_id, summary, last_summarized_message_id, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $4)
	ON CONFLICT(thread_id) DO UPDATE SET
		summary = EXCLUDED.summary,
		last_summarized_message_id = EXCLUDED.last_summarized_message_id,
		updated_at = EXCLUDED.updated_at
	`
	_, err := s.db.ExecContext(ctx, query, threadID, summary, lastSummarizedMsgID, now)
	return err
}

// SaveThreadSummary persists a thread summary and its last summarized message ID watermark in the sessions table.
func SaveThreadSummary(database DBTX, threadID, summary, lastSummarizedMsgID string) error {
	if database == nil || threadID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return (&SQLStore{db: database}).SaveThreadSummary(ctx, threadID, summary, lastSummarizedMsgID)
}

// GetThreadSummary retrieves the cached thread summary and last_summarized_message_id watermark for a thread.
func (s *SQLStore) GetThreadSummary(ctx context.Context, threadID string) (summary string, lastSummarizedMsgID string, err error) {
	if err := s.checkDB(); err != nil {
		return "", "", err
	}
	if threadID == "" {
		return "", "", nil
	}
	var sum, lastMsgID sql.NullString
	query := `SELECT summary, last_summarized_message_id FROM sessions WHERE thread_id = $1`
	err = s.db.QueryRowContext(ctx, query, threadID).Scan(&sum, &lastMsgID)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	return sum.String, lastMsgID.String, nil
}

// GetThreadSummary retrieves the cached thread summary and last_summarized_message_id watermark for a thread.
func GetThreadSummary(database DBTX, threadID string) (summary string, lastSummarizedMsgID string, err error) {
	if database == nil || threadID == "" {
		return "", "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return (&SQLStore{db: database}).GetThreadSummary(ctx, threadID)
}

// GetSessionActivityStats queries the database to determine the most recent session activity.
func (s *SQLStore) GetSessionActivityStats(ctx context.Context, threadID string) (*SessionActivityStats, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(threadID) == "" {
		return &SessionActivityStats{}, nil
	}

	var (
		internalSessionID sql.NullString
		turnCount         sql.NullInt64
		rawSessUpdatedAt  any
	)

	err := s.db.QueryRowContext(
		ctx,
		`SELECT internal_session_id, turn_count, updated_at FROM sessions WHERE thread_id = $1`,
		threadID,
	).Scan(&internalSessionID, &turnCount, &rawSessUpdatedAt)

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("querying session for thread %s: %w", threadID, err)
	}

	var (
		completedCount int64
		rawMsgUpdated  any
		rawMsgCreated  any
	)

	msgErr := s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*), MAX(updated_at), MAX(created_at) FROM messages 
		 WHERE thread_id = $1 
		   AND status = 'COMPLETED' 
		   AND response_text NOT LIKE '[EXPIRED_STALE]%' 
		   AND response_text NOT LIKE '[AMBIENT%' 
		   AND response_text NOT LIKE '[IGNORED%'`,
		threadID,
	).Scan(&completedCount, &rawMsgUpdated, &rawMsgCreated)

	if msgErr != nil && !errors.Is(msgErr, sql.ErrNoRows) {
		return nil, fmt.Errorf("querying completed messages for thread %s: %w", threadID, msgErr)
	}

	stats := &SessionActivityStats{
		InternalSessionID: internalSessionID.String,
		TurnCount:         int(turnCount.Int64),
		CompletedTurns:    completedCount,
	}
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
func FindUnrotatedSessions(ctx context.Context, database DBTX, minTurns int, maxAge ...time.Duration) ([]SessionInfo, error) {
	if database == nil {
		return nil, nil
	}
	if minTurns <= 0 {
		minTurns = session.DefaultMaxSessionTurns
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	limitAge := DefaultUnrotatedSessionMaxAge
	if len(maxAge) > 0 {
		limitAge = maxAge[0]
	}

	var (
		rows *sql.Rows
		err  error
	)
	if limitAge > 0 {
		cutoff := time.Now().UTC().Add(-limitAge)
		query := `
		SELECT thread_id, internal_session_id, COALESCE(previous_session_id, ''), turn_count, created_at, updated_at
		FROM sessions
		WHERE turn_count > $1 AND updated_at >= $2
		ORDER BY turn_count DESC
		`
		rows, err = database.QueryContext(queryCtx, query, minTurns, cutoff)
	} else {
		query := `
		SELECT thread_id, internal_session_id, COALESCE(previous_session_id, ''), turn_count, created_at, updated_at
		FROM sessions
		WHERE turn_count > $1
		ORDER BY turn_count DESC
		`
		rows, err = database.QueryContext(queryCtx, query, minTurns)
	}
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sessions, nil
}
