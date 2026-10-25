package db

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/sanitizer"
)

var (
	tagRegex      = regexp.MustCompile(`(?s)<[A-Za-z0-9_-]+.*?>.*?</[A-Za-z0-9_-]+>|<[^>]+>`)
	markdownRegex = regexp.MustCompile(`[#*_` + "`" + `>]+`)
)

type TasksFilter struct {
	Statuses    []string
	ThreadID    string
	IsAdmin     *bool
	Since       time.Time
	AfterRowID  int64
	BeforeRowID int64
	Limit       int
}

func (f TasksFilter) IsZero() bool {
	return len(f.Statuses) == 0 &&
		f.ThreadID == "" &&
		f.IsAdmin == nil &&
		f.Since.IsZero() &&
		f.AfterRowID == 0 &&
		f.BeforeRowID == 0 &&
		f.Limit == 0
}

type ActiveTask struct {
	ID                string    `json:"id"`
	RowID             int64     `json:"row_id,omitempty"`
	ThreadID          string    `json:"thread_id"`
	SessionID         string    `json:"session_id,omitempty"`
	PreviousSessionID string    `json:"previous_session_id,omitempty"`
	ThreadSummary     string    `json:"thread_summary,omitempty"`
	AuthorName        string    `json:"author_name"`
	AuthorID          string    `json:"author_id"`
	Prompt            string    `json:"prompt"`
	Summary           string    `json:"summary"`
	ResponseText      string    `json:"response_text,omitempty"`
	IsAdmin           bool      `json:"is_admin,omitempty"`
	Status            string    `json:"status"`
	RetryCount        int       `json:"retry_count"`
	ScheduleRunID     string    `json:"schedule_run_id,omitempty"`
	TriggerType       string    `json:"trigger_type"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
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

func buildActiveTasksQuery(flt TasksFilter, hasFilter bool) (string, []any) {
	baseQuery := `SELECT m.id, COALESCE(m.row_id, 0), m.thread_id, COALESCE(s.internal_session_id, ''), COALESCE(s.previous_session_id, ''), COALESCE(s.summary, ''), m.author_name, m.author_id, m.content, COALESCE(m.summary, ''), COALESCE(m.response_text, ''), m.is_admin, m.status, m.retry_count, COALESCE(m.schedule_run_id, ''), m.created_at, m.updated_at FROM messages m LEFT JOIN sessions s ON m.thread_id = s.thread_id`
	if !hasFilter {
		return baseQuery + ` WHERE m.status IN ('PENDING', 'PROCESSING') ORDER BY m.created_at ASC LIMIT 50;`, nil
	}

	var whereClauses []string
	var args []any

	if len(flt.Statuses) > 0 {
		var valid []string
		for _, st := range flt.Statuses {
			if v := strings.ToUpper(strings.TrimSpace(st)); v != "" {
				valid = append(valid, v)
			}
		}
		if len(valid) == 1 {
			args = append(args, valid[0])
			whereClauses = append(whereClauses, fmt.Sprintf("UPPER(m.status) = $%d", len(args)))
		} else if len(valid) > 1 {
			placeholders := make([]string, len(valid))
			for i, v := range valid {
				args = append(args, v)
				placeholders[i] = fmt.Sprintf("$%d", len(args))
			}
			whereClauses = append(whereClauses, fmt.Sprintf("UPPER(m.status) IN (%s)", strings.Join(placeholders, ", ")))
		}
	}

	if flt.ThreadID != "" {
		args = append(args, flt.ThreadID)
		whereClauses = append(whereClauses, fmt.Sprintf("m.thread_id = $%d", len(args)))
	}
	if flt.IsAdmin != nil {
		args = append(args, *flt.IsAdmin)
		whereClauses = append(whereClauses, fmt.Sprintf("m.is_admin = $%d", len(args)))
	}
	if !flt.Since.IsZero() {
		args = append(args, flt.Since)
		whereClauses = append(whereClauses, fmt.Sprintf("m.updated_at >= $%d", len(args)))
	}
	if flt.AfterRowID > 0 {
		args = append(args, flt.AfterRowID)
		whereClauses = append(whereClauses, fmt.Sprintf("m.row_id > $%d", len(args)))
	}
	if flt.BeforeRowID > 0 {
		args = append(args, flt.BeforeRowID)
		whereClauses = append(whereClauses, fmt.Sprintf("m.row_id <= $%d", len(args)))
	}

	orderClause := "ORDER BY m.row_id DESC"
	if flt.AfterRowID > 0 {
		orderClause = "ORDER BY m.row_id ASC"
	}

	limit := flt.Limit
	if limit <= 0 {
		limit = 50
	} else if limit > 200 {
		limit = 200
	}
	args = append(args, limit)
	limitClause := fmt.Sprintf("LIMIT $%d", len(args))

	query := baseQuery
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}
	return query + " " + orderClause + " " + limitClause + ";", args
}

func scanActiveTasks(rows *sql.Rows) ([]ActiveTask, error) {
	tasks := make([]ActiveTask, 0)
	for rows.Next() {
		var t ActiveTask
		if err := rows.Scan(
			&t.ID,
			&t.RowID,
			&t.ThreadID,
			&t.SessionID,
			&t.PreviousSessionID,
			&t.ThreadSummary,
			&t.AuthorName,
			&t.AuthorID,
			&t.Prompt,
			&t.Summary,
			&t.ResponseText,
			&t.IsAdmin,
			&t.Status,
			&t.RetryCount,
			&t.ScheduleRunID,
			&t.CreatedAt,
			&t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan active task: %w", err)
		}
		if t.Summary == "" {
			t.Summary = CleanTaskSummary(t.Prompt)
		}
		t.TriggerType = InferTriggerType(t.AuthorID, t.ScheduleRunID)
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// GetActiveTasks returns up to 50 active tasks for telemetry HUD visualization, or filtered tasks.
func (s *SQLStore) GetActiveTasks(ctx context.Context, filter ...TasksFilter) ([]ActiveTask, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	hasFilter := len(filter) > 0 && !filter[0].IsZero()
	var flt TasksFilter
	if hasFilter {
		flt = filter[0]
	}
	query, args := buildActiveTasksQuery(flt, hasFilter)

	rows, err := s.db.QueryContext(queryCtx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query active tasks: %w", err)
	}
	defer rows.Close()

	return scanActiveTasks(rows)
}
