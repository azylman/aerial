package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pgutil"
)

const (
	StatusPending    = "PENDING"
	StatusProcessing = "PROCESSING"
	StatusCompleted  = "COMPLETED"
	StatusFailed     = "FAILED"
)

type Message struct {
	ID                string   `json:"id"`
	RowID             int64    `json:"row_id,omitempty"`
	Agent             string   `json:"agent,omitempty"`
	ThreadID          string   `json:"thread_id"`
	GuildID           string   `json:"guild_id"`
	AuthorID          string   `json:"author_id"`
	AuthorName        string   `json:"author_name"`
	Content           string   `json:"content"`
	Summary           string   `json:"summary"`
	Status            string   `json:"status"`
	RetryCount        int      `json:"retry_count"`
	RestartCount      int      `json:"restart_count"`
	Effort            string   `json:"effort"`
	ErrorMessage      string   `json:"error_message,omitempty"`
	ResponseText      string   `json:"response_text,omitempty"`
	ConversationID    string   `json:"conversation_id,omitempty"`
	ChannelID         string   `json:"channel_id,omitempty"`
	ChannelName       string   `json:"channel_name,omitempty"`
	ThreadName        string   `json:"thread_name,omitempty"`
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
	DurationMs        int64    `json:"duration_ms,omitempty"`
	InputTokens       int      `json:"input_tokens,omitempty"`
	OutputTokens      int      `json:"output_tokens,omitempty"`
	ThinkingTokens    int      `json:"thinking_tokens,omitempty"`
	CacheReadTokens   int      `json:"cache_read_tokens,omitempty"`
	TotalTokens       int      `json:"total_tokens,omitempty"`
	TurnID            string   `json:"turn_id,omitempty"`
	// Traceparent is the W3C trace context of the ingress request that queued this turn.
	Traceparent string    `json:"traceparent,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (m Message) HasStructuredFields() bool {
	return m.ChannelID != "" || m.ChannelName != "" || m.ThreadName != "" ||
		m.AuthorGlobalName != "" || m.AuthorBot || m.IsAdmin ||
		len(m.Mentions) > 0 || len(m.MentionUserIDs) > 0 || len(m.MentionRoleIDs) > 0 ||
		m.ReplyingToAuthor != "" || m.ReplyingToContent != "" || len(m.Attachments) > 0 ||
		m.Model != ""
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
	if _, after, ok := strings.Cut(trimmed, "- content:"); ok {
		rest := after
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
	if _, after, ok := strings.Cut(trimmed, "Prompt:"); ok {
		rest := after
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

const messageSelectCols = "agent, id, COALESCE(row_id, 0) AS row_id, thread_id, guild_id, author_id, author_name, content, COALESCE(summary, '') AS summary, status, retry_count, COALESCE(restart_count, 0) AS restart_count, COALESCE(effort, '') AS effort, COALESCE(error_message, '') AS error_message, COALESCE(response_text, '') AS response_text, COALESCE(conversation_id, '') AS conversation_id, COALESCE(channel_id, '') AS channel_id, COALESCE(channel_name, '') AS channel_name, COALESCE(thread_name, '') AS thread_name, COALESCE(author_global_name, '') AS author_global_name, COALESCE(author_bot, false) AS author_bot, COALESCE(is_admin, false) AS is_admin, mentions, mention_user_ids, mention_role_ids, COALESCE(replying_to_author, '') AS replying_to_author, COALESCE(replying_to_content, '') AS replying_to_content, attachments, COALESCE(model, '') AS model, COALESCE(duration_ms, 0) AS duration_ms, COALESCE(input_tokens, 0) AS input_tokens, COALESCE(output_tokens, 0) AS output_tokens, COALESCE(thinking_tokens, 0) AS thinking_tokens, COALESCE(cache_read_tokens, 0) AS cache_read_tokens, COALESCE(total_tokens, 0) AS total_tokens, COALESCE(turn_id, '') AS turn_id, COALESCE(traceparent, '') AS traceparent, created_at, updated_at"

// InsertMessage stores a new message in the database.
func (s *SQLStore) InsertMessage(ctx context.Context, msg Message) (err error) {
	defer recordTranscriptQuery("insert_message", time.Now(), &err)
	if checkDBErr := s.checkDB(); checkDBErr != nil {
		return checkDBErr
	}
	if msg.ID == "" {
		return fmt.Errorf("message id cannot be empty")
	}
	agent := strings.TrimSpace(msg.Agent)
	if agent == "" {
		agent = s.effectiveAgent()
	}
	msg.Agent = agent
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
		agent, id, thread_id, guild_id, author_id, author_name,
		content, summary, status, retry_count, restart_count,
		effort, error_message, response_text, conversation_id,
		channel_id, channel_name, thread_name, author_global_name,
		author_bot, is_admin, mentions, mention_user_ids,
		mention_role_ids, replying_to_author, replying_to_content, attachments,
		model, duration_ms, input_tokens, output_tokens,
		thinking_tokens, cache_read_tokens, total_tokens, turn_id, traceparent,
		created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6,
		$7, $8, $9, $10, $11,
		$12, $13, $14, $15,
		$16, $17, $18, $19,
		$20, $21, $22, $23,
		$24, $25, $26, $27,
		$28, $29, $30, $31,
		$32, $33, $34, $35, $36,
		$37, $38
	) ON CONFLICT (agent, id) DO NOTHING;`,
		msg.Agent, msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName,
		msg.Content, msg.Summary, msg.Status, msg.RetryCount, msg.RestartCount,
		msg.Effort, msg.ErrorMessage, msg.ResponseText, msg.ConversationID,
		msg.ChannelID, msg.ChannelName, msg.ThreadName, msg.AuthorGlobalName,
		msg.AuthorBot, msg.IsAdmin, pgutil.FormatPGTextArray(msg.Mentions), pgutil.FormatPGTextArray(msg.MentionUserIDs),
		pgutil.FormatPGTextArray(msg.MentionRoleIDs), msg.ReplyingToAuthor, msg.ReplyingToContent, pgutil.FormatPGTextArray(msg.Attachments),
		msg.Model, msg.DurationMs, msg.InputTokens, msg.OutputTokens,
		msg.ThinkingTokens, msg.CacheReadTokens, msg.TotalTokens, msg.TurnID, msg.Traceparent,
		msg.CreatedAt, msg.UpdatedAt,
	)
	return err
}

func scanMessage(scanner interface{ Scan(dest ...any) error }, autoCleanSummary bool) (Message, error) {
	var m Message
	var errMsg, respText, convID sql.NullString
	if err := scanner.Scan(
		&m.Agent, &m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary,
		&m.Status, &m.RetryCount, &m.RestartCount, &m.Effort, &errMsg, &respText, &convID,
		&m.ChannelID, &m.ChannelName, &m.ThreadName, &m.AuthorGlobalName,
		&m.AuthorBot, &m.IsAdmin,
		&pgutil.PGStringSlice{Dest: &m.Mentions},
		&pgutil.PGStringSlice{Dest: &m.MentionUserIDs},
		&pgutil.PGStringSlice{Dest: &m.MentionRoleIDs},
		&m.ReplyingToAuthor, &m.ReplyingToContent,
		&pgutil.PGStringSlice{Dest: &m.Attachments},
		&m.Model,
		&m.DurationMs, &m.InputTokens, &m.OutputTokens, &m.ThinkingTokens, &m.CacheReadTokens, &m.TotalTokens,
		&m.TurnID,
		&m.Traceparent,
		&m.CreatedAt, &m.UpdatedAt,
	); err != nil {
		return Message{}, err
	}
	m.ErrorMessage, m.ResponseText, m.ConversationID = errMsg.String, respText.String, convID.String
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

func firstNonEmpty(vals []string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (s *SQLStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string, telemetry ...TurnTelemetry) error {
	agent := s.effectiveAgent()
	now := time.Now().UTC()
	if len(telemetry) > 0 {
		t := telemetry[0]
		return s.execMessageUpdate(ctx, id, `UPDATE messages SET status = $1, error_message = $2, duration_ms = $3, input_tokens = $4, output_tokens = $5, thinking_tokens = $6, cache_read_tokens = $7, total_tokens = $8, turn_id = CASE WHEN $9 <> '' THEN $9 ELSE turn_id END, updated_at = $10 WHERE agent = $11 AND id = $12`,
			status, errorMsg, t.DurationMs, t.InputTokens, t.OutputTokens, t.ThinkingTokens, t.CacheReadTokens, t.TotalTokens, t.TurnID, now, agent, id)
	}
	return s.execMessageUpdate(ctx, id, `UPDATE messages SET status = $1, error_message = $2, updated_at = $3 WHERE agent = $4 AND id = $5`, status, errorMsg, now, agent, id)
}

func (s *SQLStore) UpdateMessageCompleted(ctx context.Context, id, responseText, conversationID string, telemetry ...TurnTelemetry) error {
	agent := s.effectiveAgent()
	now := time.Now().UTC()
	var t TurnTelemetry
	if len(telemetry) > 0 {
		t = telemetry[0]
	}
	if strings.TrimSpace(conversationID) != "" {
		return s.execMessageUpdate(ctx, id, `UPDATE messages SET status = $1, response_text = $2, error_message = '', conversation_id = $3, duration_ms = $4, input_tokens = $5, output_tokens = $6, thinking_tokens = $7, cache_read_tokens = $8, total_tokens = $9, turn_id = CASE WHEN $10 <> '' THEN $10 ELSE turn_id END, updated_at = $11 WHERE agent = $12 AND id = $13`,
			StatusCompleted, responseText, strings.TrimSpace(conversationID), t.DurationMs, t.InputTokens, t.OutputTokens, t.ThinkingTokens, t.CacheReadTokens, t.TotalTokens, t.TurnID, now, agent, id)
	}
	return s.execMessageUpdate(ctx, id, `UPDATE messages SET status = $1, response_text = $2, error_message = '', duration_ms = $3, input_tokens = $4, output_tokens = $5, thinking_tokens = $6, cache_read_tokens = $7, total_tokens = $8, turn_id = CASE WHEN $9 <> '' THEN $9 ELSE turn_id END, updated_at = $10 WHERE agent = $11 AND id = $12`,
		StatusCompleted, responseText, t.DurationMs, t.InputTokens, t.OutputTokens, t.ThinkingTokens, t.CacheReadTokens, t.TotalTokens, t.TurnID, now, agent, id)
}

func (s *SQLStore) GetMessage(ctx context.Context, id string) (*Message, error) {
	if err := s.checkDB(); err != nil || id == "" {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	m, err := scanMessage(s.db.QueryRowContext(queryCtx, fmt.Sprintf("SELECT %s FROM messages WHERE agent = $1 AND id = $2", messageSelectCols), s.effectiveAgent(), id), true)
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
	err := s.db.QueryRowContext(queryCtx, "SELECT 1 FROM messages WHERE agent = $1 AND id = $2 LIMIT 1", s.effectiveAgent(), id).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *SQLStore) ClaimPendingMessage(ctx context.Context, id string, turnID ...string) (claimed bool, err error) {
	defer recordTranscriptQuery("claim_message", time.Now(), &err)
	if err = s.checkDB(); err != nil || id == "" {
		return false, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	tid := firstNonEmpty(turnID)
	var claimedID string
	err = s.db.QueryRowContext(queryCtx, `UPDATE messages SET status = 'PROCESSING', turn_id = CASE WHEN $3 <> '' THEN $3 ELSE turn_id END, updated_at = CURRENT_TIMESTAMP WHERE agent = $1 AND id = $2 AND status = 'PENDING' RETURNING id;`, s.effectiveAgent(), id, tid).Scan(&claimedID)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("failed claiming pending message %s: %w", id, err)
	}
	if tid != "" {
		_, backfillErr := s.db.ExecContext(queryCtx, `UPDATE messages SET turn_id = $3, updated_at = CURRENT_TIMESTAMP WHERE agent = $1 AND id = $2 AND status = 'PROCESSING' AND (turn_id = '' OR turn_id IS NULL);`, s.effectiveAgent(), id, tid)
		if backfillErr != nil {
			return false, fmt.Errorf("failed backfilling turn_id for processing message %s: %w", id, backfillErr)
		}
	}
	return false, nil
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
	rows, queryErr := s.db.QueryContext(queryCtx, fmt.Sprintf("SELECT %s FROM (SELECT %s FROM messages WHERE agent = $1 AND thread_id = $2 ORDER BY created_at DESC LIMIT $3) recent ORDER BY created_at ASC", messageSelectCols, messageSelectCols), s.effectiveAgent(), threadID, limit)
	if queryErr != nil {
		return nil, fmt.Errorf("failed to query recent thread messages: %w", queryErr)
	}
	msgs, err = scanMessages(rows, false)
	if err != nil {
		return nil, fmt.Errorf("failed to scan recent message: %w", err)
	}
	return msgs, nil
}
