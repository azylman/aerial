package db

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	tagRegex      = regexp.MustCompile(`(?s)<[A-Za-z0-9_-]+.*?>.*?</[A-Za-z0-9_-]+>|<[^>]+>`)
	mentionRegex  = regexp.MustCompile(`<@!?[0-9]+>`)
	markdownRegex = regexp.MustCompile(`[#*_` + "`" + `>]+`)
	spaceRegex    = regexp.MustCompile(`\s+`)
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

	if strings.Contains(trimmed, "<USER_REQUEST>") {
		lines := strings.Split(trimmed, "\n")
		var extracted string
		for i, line := range lines {
			lineTrimmed := strings.TrimSpace(line)
			if strings.HasPrefix(lineTrimmed, "- content:") {
				val := strings.TrimSpace(strings.TrimPrefix(lineTrimmed, "- content:"))
				if val != "" {
					extracted = val
					break
				}
			}
			if strings.HasPrefix(lineTrimmed, "Prompt:") {
				val := strings.TrimSpace(strings.TrimPrefix(lineTrimmed, "Prompt:"))
				if val != "" {
					extracted = val
					break
				} else if i+1 < len(lines) {
					for j := i + 1; j < len(lines); j++ {
						nextTrimmed := strings.TrimSpace(lines[j])
						if nextTrimmed != "" && !strings.HasPrefix(nextTrimmed, "</USER_REQUEST>") {
							extracted = nextTrimmed
							break
						}
					}
					if extracted != "" {
						break
					}
				}
			}
		}
		if extracted != "" {
			trimmed = extracted
		}
	}

	cleaned := tagRegex.ReplaceAllString(trimmed, " ")
	cleaned = mentionRegex.ReplaceAllString(cleaned, "")
	cleaned = markdownRegex.ReplaceAllString(cleaned, "")
	cleaned = strings.TrimSpace(spaceRegex.ReplaceAllString(cleaned, " "))

	if cleaned == "" {
		cleaned = "Agent Task"
	}

	runes := []rune(cleaned)
	if len(runes) > 140 {
		return strings.TrimSpace(string(runes[:137])) + "..."
	}
	return cleaned
}

// InferTriggerType deduces the source trigger type for a task from its author and schedule metadata.
func InferTriggerType(authorID, scheduleRunID string) string {
	if authorID == "http-client" {
		return "http"
	}
	if scheduleRunID != "" {
		if strings.HasPrefix(scheduleRunID, "cron-") {
			return "cron"
		}
		return "reminder"
	}
	return "discord"
}

// GetActiveTasks returns up to 50 active tasks for telemetry HUD visualization.
func GetActiveTasks(database *sql.DB) ([]ActiveTask, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
	SELECT 
		m.id,
		COALESCE(m.row_id, 0),
		m.thread_id,
		COALESCE(s.internal_session_id, '') AS session_id,
		m.author_name,
		m.author_id,
		m.content,
		COALESCE(m.summary, '') AS summary,
		m.status,
		m.retry_count,
		COALESCE(m.schedule_run_id, '') AS schedule_run_id,
		m.created_at,
		m.updated_at
	FROM messages m
	LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.status IN ('PENDING', 'PROCESSING')
	ORDER BY m.created_at ASC
	LIMIT 50;
	`
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query active tasks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	tasks := make([]ActiveTask, 0)
	for rows.Next() {
		var t ActiveTask
		var schedID sql.NullString
		if err := rows.Scan(
			&t.ID,
			&t.RowID,
			&t.ThreadID,
			&t.SessionID,
			&t.AuthorName,
			&t.AuthorID,
			&t.Prompt,
			&t.Summary,
			&t.Status,
			&t.RetryCount,
			&schedID,
			&t.CreatedAt,
			&t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan active task: %w", err)
		}
		if schedID.Valid {
			t.ScheduleRunID = schedID.String
		}
		if t.Summary == "" {
			t.Summary = CleanTaskSummary(t.Prompt)
		}
		t.TriggerType = InferTriggerType(t.AuthorID, t.ScheduleRunID)
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active tasks: %w", err)
	}
	return tasks, nil
}
