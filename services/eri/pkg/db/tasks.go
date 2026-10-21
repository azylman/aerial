package db

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/sanitizer"
)

var (
	tagRegex      = regexp.MustCompile(`(?s)<[A-Za-z0-9_-]+.*?>.*?</[A-Za-z0-9_-]+>|<[^>]+>`)
	markdownRegex = regexp.MustCompile(`[#*_` + "`" + `>]+`)
)

type ActiveTask struct {
	ID            string    `json:"id"`
	RowID         int64     `json:"row_id,omitempty"`
	ThreadID      string    `json:"thread_id"`
	SessionID     string    `json:"session_id,omitempty"`
	AuthorName    string    `json:"author_name"`
	AuthorID      string    `json:"author_id"`
	Prompt        string    `json:"prompt"`
	Summary       string    `json:"summary"`
	Status        string    `json:"status"`
	RetryCount    int       `json:"retry_count"`
	ScheduleRunID string    `json:"schedule_run_id,omitempty"`
	TriggerType   string    `json:"trigger_type"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// CleanTaskSummary extracts and sanitizes a concise preview string from prompt markdown/XML content.
func CleanTaskSummary(content string) string {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return "Agent Task"
	}
	cleaned := tagRegex.ReplaceAllString(ExtractMessageBody(trimmed), " ")
	cleaned = sanitizer.NormalizeWhitespace(markdownRegex.ReplaceAllString(sanitizer.SanitizeMentions(cleaned), ""))
	if cleaned == "" {
		cleaned = "Agent Task"
	}
	if runes := []rune(cleaned); len(runes) > 140 {
		return strings.TrimSpace(string(runes[:137])) + "..."
	}
	return cleaned
}

// InferTriggerType deduces the source trigger type for a task from its author and schedule metadata.
func InferTriggerType(authorID, scheduleRunID string) string {
	if authorID == "http-client" {
		return "http"
	}
	if scheduleRunID == "" {
		return "discord"
	}
	if strings.HasPrefix(scheduleRunID, "cron-") {
		return "cron"
	}
	return "reminder"
}

// GetActiveTasks returns up to 50 active tasks for telemetry HUD visualization.
func (s *SQLStore) GetActiveTasks(ctx context.Context) ([]ActiveTask, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(queryCtx, `SELECT m.id, COALESCE(m.row_id, 0), m.thread_id, COALESCE(s.internal_session_id, '') AS session_id, m.author_name, m.author_id, m.content, COALESCE(m.summary, '') AS summary, m.status, m.retry_count, COALESCE(m.schedule_run_id, '') AS schedule_run_id, m.created_at, m.updated_at FROM messages m LEFT JOIN sessions s ON m.thread_id = s.thread_id WHERE m.status IN ('PENDING', 'PROCESSING') ORDER BY m.created_at ASC LIMIT 50;`)
	if err != nil {
		return nil, fmt.Errorf("query active tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]ActiveTask, 0)
	for rows.Next() {
		var t ActiveTask
		var schedID sql.NullString
		if err := rows.Scan(&t.ID, &t.RowID, &t.ThreadID, &t.SessionID, &t.AuthorName, &t.AuthorID, &t.Prompt, &t.Summary, &t.Status, &t.RetryCount, &schedID, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan active task: %w", err)
		}
		t.ScheduleRunID = schedID.String
		if t.Summary == "" {
			t.Summary = CleanTaskSummary(t.Prompt)
		}
		t.TriggerType = InferTriggerType(t.AuthorID, t.ScheduleRunID)
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}
