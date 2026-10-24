package db

import (
	"context"
	"database/sql"
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

type Message struct {
	ID                string    `json:"id"`
	RowID             int64     `json:"row_id,omitempty"`
	ThreadID          string    `json:"thread_id"`
	GuildID           string    `json:"guild_id"`
	AuthorID          string    `json:"author_id"`
	AuthorName        string    `json:"author_name"`
	Content           string    `json:"content"`
	Summary           string    `json:"summary"`
	Status            string    `json:"status"`
	RetryCount        int       `json:"retry_count"`
	RestartCount      int       `json:"restart_count"`
	Effort            string    `json:"effort"`
	ErrorMessage      string    `json:"error_message,omitempty"`
	ResponseText      string    `json:"response_text,omitempty"`
	ScheduleRunID     string    `json:"schedule_run_id,omitempty"`
	ChannelID         string    `json:"channel_id,omitempty"`
	ChannelName       string    `json:"channel_name,omitempty"`
	ThreadName        string    `json:"thread_name,omitempty"`
	AuthorGlobalName  string    `json:"author_global_name,omitempty"`
	AuthorBot         bool      `json:"author_bot,omitempty"`
	IsAdmin           bool      `json:"is_admin,omitempty"`
	Mentions          []string  `json:"mentions,omitempty"`
	MentionUserIDs    []string  `json:"mention_user_ids,omitempty"`
	MentionRoleIDs    []string  `json:"mention_role_ids,omitempty"`
	ReplyingToAuthor  string    `json:"replying_to_author,omitempty"`
	ReplyingToContent string    `json:"replying_to_content,omitempty"`
	Attachments       []string  `json:"attachments,omitempty"`
	Model             string    `json:"model,omitempty"`
	Silent            bool      `json:"silent,omitempty"`
	Ephemeral         *bool     `json:"ephemeral,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

func (m Message) HasStructuredFields() bool {
	return m.ChannelID != "" || m.ChannelName != "" || m.ThreadName != "" ||
		m.AuthorGlobalName != "" || m.AuthorBot || m.IsAdmin ||
		len(m.Mentions) > 0 || len(m.MentionUserIDs) > 0 || len(m.MentionRoleIDs) > 0 ||
		m.ReplyingToAuthor != "" || m.ReplyingToContent != "" || len(m.Attachments) > 0 ||
		m.Model != "" || m.Silent || m.Ephemeral != nil
}

type pgStringSlice struct{ dest *[]string }

func (s *pgStringSlice) Scan(src any) error {
	vals, err := parsePGTextArray(src)
	if err != nil {
		return err
	}
	*s.dest = vals
	return nil
}

func formatPGTextArray(elems []string) string {
	if len(elems) == 0 {
		return "{}"
	}
	var sb strings.Builder
	sb.WriteByte('{')
	for i, s := range elems {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('"')
		for _, r := range s {
			if r == '\\' || r == '"' {
				sb.WriteByte('\\')
			}
			sb.WriteRune(r)
		}
		sb.WriteByte('"')
	}
	sb.WriteByte('}')
	return sb.String()
}

func extractArrayString(src any) (content string, empty bool, err error) {
	if src == nil {
		return "", true, nil
	}
	var str string
	switch v := src.(type) {
	case string:
		str = v
	case []byte:
		str = string(v)
	default:
		return "", false, fmt.Errorf("cannot scan type %T into pgStringSlice", src)
	}
	str = strings.TrimSpace(str)
	if str == "" || str == "{}" || str == "NULL" {
		return "", true, nil
	}
	if !strings.HasPrefix(str, "{") || !strings.HasSuffix(str, "}") {
		return "", false, fmt.Errorf("malformed postgres array: %q", str)
	}
	return str[1 : len(str)-1], false, nil
}

func parsePGArrayContent(content string) ([]string, error) {
	if len(content) == 0 {
		return []string{}, nil
	}
	var res []string
	var cur strings.Builder
	inQuotes, escaped, hasItem := false, false, false

	for i := 0; i < len(content); i++ {
		ch := content[i]
		if escaped {
			cur.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == '"' {
			inQuotes = !inQuotes
			hasItem = true
			continue
		}
		if ch == ',' && !inQuotes {
			res = append(res, cur.String())
			cur.Reset()
			hasItem = false
			continue
		}
		cur.WriteByte(ch)
		hasItem = true
	}
	if inQuotes || escaped {
		return nil, fmt.Errorf("unclosed quote or escape in postgres array")
	}
	if hasItem || len(res) > 0 {
		res = append(res, cur.String())
	}
	return res, nil
}

func parsePGTextArray(src any) ([]string, error) {
	if slice, ok := src.([]string); ok {
		return slice, nil
	}
	content, empty, err := extractArrayString(src)
	if err != nil || empty {
		return []string{}, err
	}
	return parsePGArrayContent(content)
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

// BodyText returns the clean user utterance. If structured fields are present, m.Content is already the raw user message.
func (m Message) BodyText() string {
	if !m.HasStructuredFields() && strings.Contains(m.Content, "<USER_REQUEST>") {
		return ExtractMessageBody(m.Content)
	}
	return m.Content
}

const messageSelectCols = "id, COALESCE(row_id, 0) AS row_id, thread_id, guild_id, author_id, author_name, content, COALESCE(summary, '') AS summary, status, retry_count, COALESCE(restart_count, 0) AS restart_count, COALESCE(effort, '') AS effort, COALESCE(error_message, '') AS error_message, COALESCE(response_text, '') AS response_text, COALESCE(schedule_run_id, '') AS schedule_run_id, COALESCE(channel_id, '') AS channel_id, COALESCE(channel_name, '') AS channel_name, COALESCE(thread_name, '') AS thread_name, COALESCE(author_global_name, '') AS author_global_name, COALESCE(author_bot, false) AS author_bot, COALESCE(is_admin, false) AS is_admin, mentions, mention_user_ids, mention_role_ids, COALESCE(replying_to_author, '') AS replying_to_author, COALESCE(replying_to_content, '') AS replying_to_content, attachments, COALESCE(model, '') AS model, COALESCE(silent, false) AS silent, ephemeral, created_at, updated_at"

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
	err = s.execWithTimeout(ctx, `INSERT INTO messages (
		id, thread_id, guild_id, author_id, author_name,
		content, summary, status, retry_count, restart_count,
		effort, error_message, response_text, schedule_run_id,
		channel_id, channel_name, thread_name, author_global_name,
		author_bot, is_admin, mentions, mention_user_ids,
		mention_role_ids, replying_to_author, replying_to_content, attachments,
		model, silent, ephemeral, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5,
		$6, $7, $8, $9, $10,
		$11, $12, $13, $14,
		$15, $16, $17, $18,
		$19, $20, $21, $22,
		$23, $24, $25, $26,
		$27, $28, $29, $30, $31
	) ON CONFLICT (id) DO NOTHING;`,
		msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName,
		msg.Content, msg.Summary, msg.Status, msg.RetryCount, msg.RestartCount,
		msg.Effort, msg.ErrorMessage, msg.ResponseText, msg.ScheduleRunID,
		msg.ChannelID, msg.ChannelName, msg.ThreadName, msg.AuthorGlobalName,
		msg.AuthorBot, msg.IsAdmin, formatPGTextArray(msg.Mentions), formatPGTextArray(msg.MentionUserIDs),
		formatPGTextArray(msg.MentionRoleIDs), msg.ReplyingToAuthor, msg.ReplyingToContent, formatPGTextArray(msg.Attachments),
		msg.Model, msg.Silent, msg.Ephemeral, msg.CreatedAt, msg.UpdatedAt,
	)
	return err
}

func scanMessage(scanner interface{ Scan(dest ...any) error }, autoCleanSummary bool) (Message, error) {
	var m Message
	var errMsg, respText, schedID sql.NullString
	var ephemeral sql.NullBool
	if err := scanner.Scan(
		&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary,
		&m.Status, &m.RetryCount, &m.RestartCount, &m.Effort, &errMsg, &respText, &schedID,
		&m.ChannelID, &m.ChannelName, &m.ThreadName, &m.AuthorGlobalName,
		&m.AuthorBot, &m.IsAdmin,
		&pgStringSlice{dest: &m.Mentions},
		&pgStringSlice{dest: &m.MentionUserIDs},
		&pgStringSlice{dest: &m.MentionRoleIDs},
		&m.ReplyingToAuthor, &m.ReplyingToContent,
		&pgStringSlice{dest: &m.Attachments},
		&m.Model, &m.Silent, &ephemeral,
		&m.CreatedAt, &m.UpdatedAt,
	); err != nil {
		return Message{}, err
	}
	m.ErrorMessage, m.ResponseText, m.ScheduleRunID = errMsg.String, respText.String, schedID.String
	if ephemeral.Valid {
		m.Ephemeral = &ephemeral.Bool
	}
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
	return s.execWithTimeout(ctx, query, args...)
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

func (s *SQLStore) HasActiveThreadTurn(ctx context.Context, threadID string) (bool, error) {
	if err := s.checkDB(); err != nil {
		return false, err
	}
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return false, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	var dummy int
	err := s.db.QueryRowContext(queryCtx, `SELECT 1 FROM messages WHERE thread_id = $1 AND status IN ('PENDING', 'PROCESSING') LIMIT 1`, threadID).Scan(&dummy)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check active thread turn for %s: %w", threadID, err)
	}
	return true, nil
}
