package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/metrics"
)

const (
	StatusPending    = "PENDING"
	StatusProcessing = "PROCESSING"
	StatusCompleted  = "COMPLETED"
	StatusFailed     = "FAILED"
)

// MessageMetadata stores structured Discord and source metadata preserved throughout the queue lifecycle.
type MessageMetadata struct {
	ChannelID         string   `json:"channel_id,omitempty"`
	TargetThreadID    string   `json:"target_thread_id,omitempty"`
	GuildID           string   `json:"guild_id,omitempty"`
	AuthorUsername    string   `json:"author_username,omitempty"`
	AuthorGlobalName  string   `json:"author_global_name,omitempty"`
	AuthorBot         bool     `json:"author_bot,omitempty"`
	IsAdmin           bool     `json:"is_admin,omitempty"`
	Mentions          []string `json:"mentions,omitempty"`
	MentionUserIDs    []string `json:"mention_user_ids,omitempty"`
	MentionRoleIDs    []string `json:"mention_role_ids,omitempty"`
	ReplyingToAuthor  string   `json:"replying_to_author,omitempty"`
	ReplyingToContent string   `json:"replying_to_content,omitempty"`
	Attachments       []string `json:"attachments,omitempty"`
}

// IsEmpty returns true if all metadata fields are zero/empty.
func (m MessageMetadata) IsEmpty() bool {
	return m.ChannelID == "" &&
		m.TargetThreadID == "" &&
		m.GuildID == "" &&
		m.AuthorUsername == "" &&
		m.AuthorGlobalName == "" &&
		!m.AuthorBot &&
		!m.IsAdmin &&
		len(m.Mentions) == 0 &&
		len(m.MentionUserIDs) == 0 &&
		len(m.MentionRoleIDs) == 0 &&
		m.ReplyingToAuthor == "" &&
		m.ReplyingToContent == "" &&
		len(m.Attachments) == 0
}

// Value implements driver.Valuer for PostgreSQL JSONB and SQLite TEXT.
func (m MessageMetadata) Value() (driver.Value, error) {
	if m.IsEmpty() {
		return "{}", nil
	}
	bytes, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal message metadata: %w", err)
	}
	return string(bytes), nil
}

// Scan implements sql.Scanner for PostgreSQL JSONB and SQLite TEXT.
func (m *MessageMetadata) Scan(src any) error {
	if src == nil {
		*m = MessageMetadata{}
		return nil
	}
	var data []byte
	switch v := src.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("unsupported message metadata type: %T", src)
	}
	if len(data) == 0 || string(data) == "{}" || string(data) == "null" {
		*m = MessageMetadata{}
		return nil
	}
	return json.Unmarshal(data, m)
}

type Message struct {
	ID            string          `json:"id"`
	RowID         int64           `json:"row_id,omitempty"`
	ThreadID      string          `json:"thread_id"`
	GuildID       string          `json:"guild_id"`
	AuthorID      string          `json:"author_id"`
	AuthorName    string          `json:"author_name"`
	Content       string          `json:"content"`
	Summary       string          `json:"summary"`
	Status        string          `json:"status"`
	RetryCount    int             `json:"retry_count"`
	RestartCount  int             `json:"restart_count"`
	Effort        string          `json:"effort,omitempty"`
	ErrorMessage  string          `json:"error_message,omitempty"`
	ResponseText  string          `json:"response_text,omitempty"`
	ScheduleRunID string          `json:"schedule_run_id,omitempty"`
	Metadata      MessageMetadata `json:"metadata,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// ExtractMessageBody extracts the raw user utterance from a legacy prompt envelope string.
func ExtractMessageBody(content string) string {
	trimmed := strings.TrimSpace(content)
	if strings.Contains(trimmed, "<USER_REQUEST>") {
		contentMarker := "- content:"
		idx := strings.Index(trimmed, contentMarker)
		if idx != -1 {
			start := idx + len(contentMarker)
			rest := trimmed[start:]

			// Find boundary of next envelope field
			endIdx := -1
			markers := []string{"\n- timestamp:", "\n- mentions:", "\n- attachments:", "\n- ", "\n</USER_REQUEST>"}
			for _, m := range markers {
				if pos := strings.Index(rest, m); pos != -1 {
					if endIdx == -1 || pos < endIdx {
						endIdx = pos
					}
				}
			}
			var val string
			if endIdx != -1 {
				val = rest[:endIdx]
			} else {
				val = rest
			}
			val = strings.TrimSpace(val)
			val = strings.ReplaceAll(val, "<\\/USER_REQUEST>", "</USER_REQUEST>")
			val = strings.ReplaceAll(val, "<\\USER_REQUEST>", "<USER_REQUEST>")
			return val
		}
		inner := strings.TrimPrefix(trimmed, "<USER_REQUEST>")
		inner = strings.TrimSuffix(inner, "</USER_REQUEST>")
		return strings.TrimSpace(inner)
	}
	return trimmed
}

// BodyText returns the clean user utterance. If Metadata is present, m.Content is already the raw user message.
// If Metadata is empty, it checks for legacy prompt envelope wrapping for backward compatibility.
func (m Message) BodyText() string {
	if !m.Metadata.IsEmpty() {
		return m.Content
	}
	if strings.Contains(m.Content, "<USER_REQUEST>") {
		return ExtractMessageBody(m.Content)
	}
	return m.Content
}

// InsertMessage inserts a new message into the queue if not already present.
func InsertMessage(database DBTX, msg Message) (err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("insert_message", status, time.Since(start))
	}()
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
	INSERT INTO messages (id, thread_id, guild_id, author_id, author_name, content, summary, status, retry_count, restart_count, effort, error_message, response_text, schedule_run_id, metadata, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
	ON CONFLICT (id) DO NOTHING;
	`
	_, err = database.ExecContext(ctx, query, msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName, msg.Content, msg.Summary, msg.Status, msg.RetryCount, msg.RestartCount, msg.Effort, msg.ErrorMessage, msg.ResponseText, msg.ScheduleRunID, msg.Metadata, msg.CreatedAt, msg.UpdatedAt)
	return err
}

// UpdateMessageStatus updates the execution status and error message of a message.
func UpdateMessageStatus(database DBTX, id string, status string, errorMsg string) error {
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

// UpdateMessageCompleted marks a message as completed with its response text.
func UpdateMessageCompleted(database DBTX, id string, responseText string) error {
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

// IncrementMessageRetry increments the retry count and records the failure reason.
func IncrementMessageRetry(database DBTX, id string, errorMsg string) error {
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

// IncrementMessageRestart atomically increments the restart count and records the restart reason.
func IncrementMessageRestart(database DBTX, id string, errorMsg string) error {
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
	SET restart_count = restart_count + 1, error_message = $1, updated_at = $2
	WHERE id = $3
	`
	_, err := database.ExecContext(ctx, query, errorMsg, now, id)
	return err
}

// ResetMessageToPendingWithRestart atomically increments restart_count, sets status to PENDING, and updates error_message.
func ResetMessageToPendingWithRestart(database DBTX, id string, reason string) error {
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
	SET restart_count = restart_count + 1, status = 'PENDING', error_message = $1, updated_at = $2
	WHERE id = $3 AND status = 'PROCESSING'
	`
	_, err := database.ExecContext(ctx, query, reason, now, id)
	return err
}

// GetPendingOrProcessingMessages returns active messages awaiting processing or recovery.
func GetPendingOrProcessingMessages(database DBTX) (msgs []Message, err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("get_pending_messages", status, time.Since(start))
	}()
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
	SELECT id, COALESCE(row_id, 0), thread_id, guild_id, author_id, author_name, content, COALESCE(summary, ''), status, retry_count, COALESCE(restart_count, 0), COALESCE(effort, ''), COALESCE(error_message, ''), COALESCE(response_text, ''), COALESCE(schedule_run_id, ''), metadata, created_at, updated_at
	FROM messages
	WHERE status IN ('PENDING', 'PROCESSING')
	ORDER BY created_at ASC
	`
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer closeWarn(rows, "rows")

	var results []Message
	for rows.Next() {
		var m Message
		var errMsg, respText, schedID sql.NullString
		if err := rows.Scan(&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary, &m.Status, &m.RetryCount, &m.RestartCount, &m.Effort, &errMsg, &respText, &schedID, &m.Metadata, &m.CreatedAt, &m.UpdatedAt); err != nil {
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

// GetMessage retrieves a message by its ID.
func GetMessage(database DBTX, id string) (*Message, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if id == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	SELECT id, COALESCE(row_id, 0), thread_id, guild_id, author_id, author_name, content, COALESCE(summary, ''), status, retry_count, COALESCE(restart_count, 0), COALESCE(effort, ''), COALESCE(error_message, ''), COALESCE(response_text, ''), COALESCE(schedule_run_id, ''), metadata, created_at, updated_at
	FROM messages
	WHERE id = $1
	`
	var m Message
	var errMsg, respText, schedID sql.NullString
	err := database.QueryRowContext(ctx, query, id).Scan(&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary, &m.Status, &m.RetryCount, &m.RestartCount, &m.Effort, &errMsg, &respText, &schedID, &m.Metadata, &m.CreatedAt, &m.UpdatedAt)
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

// MessageExists checks if a message with the given ID exists.
func MessageExists(database DBTX, id string) (bool, error) {
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
func ClaimPendingMessage(database DBTX, id string) (claimed bool, err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("claim_message", status, time.Since(start))
	}()
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
	err = database.QueryRowContext(ctx, query, now, id).Scan(&claimedID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed claiming pending message %s: %w", id, err)
	}
	return true, nil
}

// GetActiveRecentThreadIDs returns distinct thread IDs that have had recent activity.
func GetActiveRecentThreadIDs(database DBTX, since time.Duration) ([]string, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	cutoff := time.Now().UTC().Add(-since)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	SELECT thread_id
	FROM (
		SELECT thread_id, MAX(updated_at) AS max_updated_at
		FROM (
			SELECT thread_id, updated_at FROM messages WHERE thread_id != '' AND updated_at >= $1
			UNION ALL
			SELECT thread_id, updated_at FROM sessions WHERE thread_id != '' AND updated_at >= $2
		) sub
		GROUP BY thread_id
	) combined
	ORDER BY max_updated_at DESC
	LIMIT 50
	`
	rows, err := database.QueryContext(ctx, query, cutoff, cutoff)
	if err != nil {
		return nil, err
	}
	defer closeWarn(rows, "rows")

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

// GetRecentThreadMessages returns the most recent messages in a thread in chronological order.
func GetRecentThreadMessages(database DBTX, threadID string, limit int) (msgs []Message, err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("get_recent_thread_messages", status, time.Since(start))
	}()
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
	       retry_count, COALESCE(restart_count, 0), COALESCE(effort, ''), error_message, response_text, schedule_run_id, metadata, created_at, updated_at
	FROM (
		SELECT id, COALESCE(row_id, 0) AS row_id, thread_id, guild_id, author_id, author_name, content, summary, status,
		       retry_count, COALESCE(restart_count, 0) AS restart_count, COALESCE(effort, '') AS effort, error_message, response_text, schedule_run_id, metadata, created_at, updated_at
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
	defer closeWarn(rows, "rows")

	msgs = make([]Message, 0)
	for rows.Next() {
		var m Message
		var errMsg, respText, schedID sql.NullString
		if err := rows.Scan(
			&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary,
			&m.Status, &m.RetryCount, &m.RestartCount, &m.Effort, &errMsg, &respText, &schedID, &m.Metadata, &m.CreatedAt, &m.UpdatedAt,
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

// GetMaxMessageRowID returns the maximum row_id for COMPLETED messages in the specified thread.
func GetMaxMessageRowID(database DBTX, threadID string) (int64, error) {
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
