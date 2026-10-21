package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
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
	ChannelName       string   `json:"channel_name,omitempty"`
	TargetThreadID    string   `json:"target_thread_id,omitempty"`
	ThreadName        string   `json:"thread_name,omitempty"`
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
	Model             string   `json:"model,omitempty"`
	Silent            bool     `json:"silent,omitempty"`
	VanadisChunkID    string   `json:"vanadis_chunk_id,omitempty"`
}

// IsEmpty returns true if all metadata fields are zero/empty.
func (m MessageMetadata) IsEmpty() bool {
	return m.ChannelID == "" && m.ChannelName == "" && m.TargetThreadID == "" && m.ThreadName == "" &&
		m.GuildID == "" && m.AuthorUsername == "" && m.AuthorGlobalName == "" && !m.AuthorBot && !m.IsAdmin &&
		len(m.Mentions) == 0 && len(m.MentionUserIDs) == 0 && len(m.MentionRoleIDs) == 0 &&
		m.ReplyingToAuthor == "" && m.ReplyingToContent == "" && len(m.Attachments) == 0 &&
		m.Model == "" && !m.Silent && m.VanadisChunkID == ""
}

// Value implements driver.Valuer for PostgreSQL JSONB and SQLite TEXT.
func (m MessageMetadata) Value() (driver.Value, error) {
	if m.IsEmpty() {
		return "{}", nil
	}
	bytes, err := json.Marshal(m)
	return string(bytes), err
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
		return fmt.Errorf("cannot scan type %T into MessageMetadata", src)
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
	Effort        string          `json:"effort"`
	ErrorMessage  string          `json:"error_message,omitempty"`
	ResponseText  string          `json:"response_text,omitempty"`
	ScheduleRunID string          `json:"schedule_run_id,omitempty"`
	Metadata      MessageMetadata `json:"metadata"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

func unescapeUserRequest(s string) string {
	return strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, `<\/USER_REQUEST>`, "</USER_REQUEST>"), `<\USER_REQUEST>`, "<USER_REQUEST>"))
}

// ExtractMessageBody extracts the raw user utterance from a legacy prompt envelope string.
func ExtractMessageBody(content string) string {
	trimmed := strings.TrimSpace(content)
	if !strings.Contains(trimmed, "<USER_REQUEST>") {
		return trimmed
	}
	if idx := strings.Index(trimmed, "- content:"); idx != -1 {
		rest := trimmed[idx+len("- content:"):]
		endIdx := -1
		for _, m := range []string{"\n- timestamp:", "\n- mentions:", "\n- attachments:", "\n- ", "\n</USER_REQUEST>"} {
			if pos := strings.Index(rest, m); pos != -1 && (endIdx == -1 || pos < endIdx) {
				endIdx = pos
			}
		}
		if endIdx != -1 {
			rest = rest[:endIdx]
		}
		return unescapeUserRequest(rest)
	}
	if pIdx := strings.Index(trimmed, "Prompt:"); pIdx != -1 {
		rest := trimmed[pIdx+len("Prompt:"):]
		if endIdx := strings.Index(rest, "</USER_REQUEST>"); endIdx != -1 {
			rest = rest[:endIdx]
		}
		return unescapeUserRequest(rest)
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "<USER_REQUEST>"), "</USER_REQUEST>"))
}

// BodyText returns the clean user utterance. If Metadata is present, m.Content is already the raw user message.
func (m Message) BodyText() string {
	if m.Metadata.IsEmpty() && strings.Contains(m.Content, "<USER_REQUEST>") {
		return ExtractMessageBody(m.Content)
	}
	return m.Content
}

const messageSelectCols = "id, COALESCE(row_id, 0) AS row_id, thread_id, guild_id, author_id, author_name, content, COALESCE(summary, '') AS summary, status, retry_count, COALESCE(restart_count, 0) AS restart_count, COALESCE(effort, '') AS effort, COALESCE(error_message, '') AS error_message, COALESCE(response_text, '') AS response_text, COALESCE(schedule_run_id, '') AS schedule_run_id, metadata, created_at, updated_at"

// InsertMessage stores a new message in the database.
func (s *SQLStore) InsertMessage(ctx context.Context, msg Message) (err error) {
	defer recordTranscriptQuery("insert_message", time.Now(), &err)
	if err = s.checkDB(); err != nil {
		return err
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
	_, err = s.execWithTimeout(ctx, 5*time.Second, `INSERT INTO messages (id, thread_id, guild_id, author_id, author_name, content, summary, status, retry_count, restart_count, effort, error_message, response_text, schedule_run_id, metadata, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) ON CONFLICT (id) DO NOTHING;`,
		msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName,
		msg.Content, msg.Summary, msg.Status, msg.RetryCount, msg.RestartCount,
		msg.Effort, msg.ErrorMessage, msg.ResponseText, msg.ScheduleRunID,
		msg.Metadata, msg.CreatedAt, msg.UpdatedAt,
	)
	return err
}

func scanMessage(scanner interface{ Scan(dest ...any) error }, autoCleanSummary bool) (Message, error) {
	var m Message
	var errMsg, respText, schedID sql.NullString
	if err := scanner.Scan(
		&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary,
		&m.Status, &m.RetryCount, &m.RestartCount, &m.Effort, &errMsg, &respText, &schedID, &m.Metadata, &m.CreatedAt, &m.UpdatedAt,
	); err != nil {
		return Message{}, err
	}
	m.ErrorMessage, m.ResponseText, m.ScheduleRunID = errMsg.String, respText.String, schedID.String
	if autoCleanSummary && m.Summary == "" {
		m.Summary = CleanTaskSummary(m.Content)
	}
	return m, nil
}

func scanMessages(rows *sql.Rows, autoCleanSummary bool) ([]Message, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	results := make([]Message, 0)
	for rows.Next() {
		m, err := scanMessage(rows, autoCleanSummary)
		if err != nil {
			return nil, err
		}
		results = append(results, m)
	}
	return results, rows.Err()
}

func (s *SQLStore) execMessageUpdate(ctx context.Context, id, query string, args ...any) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("message id cannot be empty")
	}
	_, err := s.execWithTimeout(ctx, 5*time.Second, query, args...)
	return err
}

func (s *SQLStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	return s.execMessageUpdate(ctx, id, `UPDATE messages SET status = $1, error_message = $2, updated_at = $3 WHERE id = $4`, status, errorMsg, time.Now().UTC(), id)
}

func (s *SQLStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	return s.execMessageUpdate(ctx, id, `UPDATE messages SET status = $1, response_text = $2, error_message = '', updated_at = $3 WHERE id = $4`, StatusCompleted, responseText, time.Now().UTC(), id)
}

func (s *SQLStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	return s.execMessageUpdate(ctx, id, `UPDATE messages SET retry_count = retry_count + 1, error_message = $1, updated_at = $2 WHERE id = $3`, errorMsg, time.Now().UTC(), id)
}

func (s *SQLStore) ResetMessageToPendingWithRestart(ctx context.Context, id, reason string) error {
	return s.execMessageUpdate(ctx, id, `UPDATE messages SET restart_count = restart_count + 1, status = 'PENDING', error_message = $1, updated_at = $2 WHERE id = $3 AND status = 'PROCESSING'`, reason, time.Now().UTC(), id)
}

func (s *SQLStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) (results []Message, err error) {
	defer recordTranscriptQuery("get_pending_messages", time.Now(), &err)
	if err = s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, queryErr := s.db.QueryContext(queryCtx, fmt.Sprintf("SELECT %s FROM messages WHERE status IN ('PENDING', 'PROCESSING') ORDER BY created_at ASC", messageSelectCols))
	if queryErr != nil {
		return nil, queryErr
	}
	results, err = scanMessages(rows, true)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func (s *SQLStore) GetMessage(ctx context.Context, id string) (*Message, error) {
	if err := s.checkDB(); err != nil || id == "" {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	m, err := scanMessage(s.db.QueryRowContext(queryCtx, fmt.Sprintf("SELECT %s FROM messages WHERE id = $1", messageSelectCols), id), true)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &m, err
}

func (s *SQLStore) MessageExists(ctx context.Context, id string) (bool, error) {
	if err := s.checkDB(); err != nil || id == "" {
		return false, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	var exists int
	err := s.db.QueryRowContext(queryCtx, "SELECT 1 FROM messages WHERE id = $1 LIMIT 1", id).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *SQLStore) ClaimPendingMessage(ctx context.Context, id string) (claimed bool, err error) {
	defer recordTranscriptQuery("claim_message", time.Now(), &err)
	if err = s.checkDB(); err != nil || id == "" {
		return false, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	var claimedID string
	err = s.db.QueryRowContext(queryCtx, `UPDATE messages SET status = 'PROCESSING', updated_at = $1 WHERE id = $2 AND status = 'PENDING' RETURNING id;`, time.Now().UTC(), id).Scan(&claimedID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed claiming pending message %s: %w", id, err)
	}
	return true, nil
}

func (s *SQLStore) GetActiveRecentThreadIDs(ctx context.Context, since time.Duration) ([]string, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if since <= 0 {
		since = 24 * time.Hour
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	cutoff := time.Now().UTC().Add(-since)
	rows, err := s.db.QueryContext(queryCtx, `SELECT thread_id FROM (SELECT thread_id, MAX(updated_at) AS max_updated_at FROM (SELECT thread_id, updated_at FROM messages WHERE thread_id != '' AND updated_at >= $1 UNION ALL SELECT thread_id, updated_at FROM sessions WHERE thread_id != '' AND updated_at >= $2) sub GROUP BY thread_id) combined ORDER BY max_updated_at DESC LIMIT 50`, cutoff, cutoff)
	if err != nil {
		return nil, err
	}
	return scanNonEmptyStrings(rows)
}

func (s *SQLStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) (msgs []Message, err error) {
	defer recordTranscriptQuery("get_recent_thread_messages", time.Now(), &err)
	if err = s.checkDB(); err != nil || threadID == "" {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, queryErr := s.db.QueryContext(queryCtx, fmt.Sprintf("SELECT %s FROM (SELECT %s FROM messages WHERE thread_id = $1 ORDER BY created_at DESC LIMIT $2) recent ORDER BY created_at ASC", messageSelectCols, messageSelectCols), threadID, limit)
	if queryErr != nil {
		return nil, fmt.Errorf("failed to query recent thread messages: %w", queryErr)
	}
	msgs, err = scanMessages(rows, false)
	if err != nil {
		return nil, fmt.Errorf("failed to scan recent message: %w", err)
	}
	return msgs, nil
}

func (s *SQLStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	if err := s.checkDB(); err != nil || threadID == "" {
		return 0, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	var maxRowID sql.NullInt64
	err := s.db.QueryRowContext(queryCtx, `SELECT MAX(row_id) FROM messages WHERE thread_id = $1 AND status = 'COMPLETED'`, threadID).Scan(&maxRowID)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return maxRowID.Int64, nil
}
