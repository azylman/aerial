package db

import (
	"context"
	"database/sql"
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

type Message struct {
	ID            string    `json:"id"`
	RowID         int64     `json:"row_id,omitempty"`
	ThreadID      string    `json:"thread_id"`
	GuildID       string    `json:"guild_id"`
	AuthorID      string    `json:"author_id"`
	AuthorName    string    `json:"author_name"`
	Content       string    `json:"content"`
	Summary       string    `json:"summary"`
	Status        string    `json:"status"`
	RetryCount    int       `json:"retry_count"`
	ErrorMessage  string    `json:"error_message,omitempty"`
	ResponseText  string    `json:"response_text,omitempty"`
	ScheduleRunID string    `json:"schedule_run_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// InsertMessage inserts a new message into the queue if not already present.
func InsertMessage(database *sql.DB, msg Message) (err error) {
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
	INSERT INTO messages (id, thread_id, guild_id, author_id, author_name, content, summary, status, retry_count, error_message, response_text, schedule_run_id, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	ON CONFLICT (id) DO NOTHING;
	`
	_, err = database.ExecContext(ctx, query, msg.ID, msg.ThreadID, msg.GuildID, msg.AuthorID, msg.AuthorName, msg.Content, msg.Summary, msg.Status, msg.RetryCount, msg.ErrorMessage, msg.ResponseText, msg.ScheduleRunID, msg.CreatedAt, msg.UpdatedAt)
	return err
}

// UpdateMessageStatus updates the execution status and error message of a message.
func UpdateMessageStatus(database *sql.DB, id string, status string, errorMsg string) error {
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
func UpdateMessageCompleted(database *sql.DB, id string, responseText string) error {
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
func IncrementMessageRetry(database *sql.DB, id string, errorMsg string) error {
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

// GetPendingOrProcessingMessages returns active messages awaiting processing or recovery.
func GetPendingOrProcessingMessages(database *sql.DB) (msgs []Message, err error) {
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
	SELECT id, COALESCE(row_id, 0), thread_id, guild_id, author_id, author_name, content, COALESCE(summary, ''), status, retry_count, COALESCE(error_message, ''), COALESCE(response_text, ''), COALESCE(schedule_run_id, ''), created_at, updated_at
	FROM messages
	WHERE status IN ('PENDING', 'PROCESSING')
	ORDER BY created_at ASC
	`
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []Message
	for rows.Next() {
		var m Message
		var errMsg, respText, schedID sql.NullString
		if err := rows.Scan(&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary, &m.Status, &m.RetryCount, &errMsg, &respText, &schedID, &m.CreatedAt, &m.UpdatedAt); err != nil {
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
func GetMessage(database *sql.DB, id string) (*Message, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if id == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	SELECT id, COALESCE(row_id, 0), thread_id, guild_id, author_id, author_name, content, COALESCE(summary, ''), status, retry_count, COALESCE(error_message, ''), COALESCE(response_text, ''), COALESCE(schedule_run_id, ''), created_at, updated_at
	FROM messages
	WHERE id = $1
	`
	var m Message
	var errMsg, respText, schedID sql.NullString
	err := database.QueryRowContext(ctx, query, id).Scan(&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary, &m.Status, &m.RetryCount, &errMsg, &respText, &schedID, &m.CreatedAt, &m.UpdatedAt)
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
func MessageExists(database *sql.DB, id string) (bool, error) {
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
func ClaimPendingMessage(database *sql.DB, id string) (claimed bool, err error) {
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
func GetActiveRecentThreadIDs(database *sql.DB, since time.Duration) ([]string, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	cutoff := time.Now().UTC().Add(-since)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
	SELECT DISTINCT thread_id
	FROM (
		SELECT thread_id, updated_at FROM messages WHERE thread_id != '' AND updated_at >= $1
		UNION
		SELECT thread_id, updated_at FROM sessions WHERE thread_id != '' AND updated_at >= $2
	) combined
	ORDER BY updated_at DESC
	LIMIT 50
	`
	rows, err := database.QueryContext(ctx, query, cutoff, cutoff)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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
func GetRecentThreadMessages(database *sql.DB, threadID string, limit int) (msgs []Message, err error) {
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
	       retry_count, error_message, response_text, schedule_run_id, created_at, updated_at
	FROM (
		SELECT id, COALESCE(row_id, 0) AS row_id, thread_id, guild_id, author_id, author_name, content, summary, status,
		       retry_count, error_message, response_text, schedule_run_id, created_at, updated_at
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
	defer func() { _ = rows.Close() }()

	msgs = make([]Message, 0)
	for rows.Next() {
		var m Message
		var errMsg, respText, schedID sql.NullString
		if err := rows.Scan(
			&m.ID, &m.RowID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary,
			&m.Status, &m.RetryCount, &errMsg, &respText, &schedID, &m.CreatedAt, &m.UpdatedAt,
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
func GetMaxMessageRowID(database *sql.DB, threadID string) (int64, error) {
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
