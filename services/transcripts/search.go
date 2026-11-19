package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// SessionResult is one indexed session in the read API.
type SessionResult struct {
	Stack           string    `json:"stack"`
	SessionID       string    `json:"session_id"`
	Engine          string    `json:"engine"`
	ThreadID        string    `json:"thread_id"`
	LastIndexedStep int       `json:"last_indexed_step"`
	LastMtime       time.Time `json:"last_mtime,omitempty"`
	IsSettled       bool      `json:"is_settled"`
}

// StepResult is one indexed transcript step in the read API.
type StepResult struct {
	Stack     string    `json:"stack"`
	SessionID string    `json:"session_id"`
	Engine    string    `json:"engine"`
	StepIndex int       `json:"step_index"`
	StepType  string    `json:"step_type"`
	ToolName  string    `json:"tool_name"`
	ToolKind  string    `json:"tool_kind"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// StackStats counts indexed sessions and steps for one stack.
type StackStats struct {
	Stack    string `json:"stack"`
	Sessions int    `json:"sessions"`
	Steps    int    `json:"steps"`
}

// SessionWatermark is the per-session ingest cursor consumers resume from.
type SessionWatermark struct {
	Stack           string    `json:"stack"`
	SessionID       string    `json:"session_id"`
	LastIndexedStep int       `json:"last_indexed_step"`
	LastMtime       time.Time `json:"last_mtime,omitempty"`
}

// collectReadRows runs one stack-scoped read query with a timeout and collects
// every row with scan, unifying the preamble the read paths would otherwise
// repeat five times over.
func collectReadRows[T any](ctx context.Context, s *Store, op, query string, scan func(*sql.Rows) (T, error), args ...any) ([]T, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(queryCtx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func scanSessionResult(r *sql.Rows) (SessionResult, error) {
	var out SessionResult
	var rawMtime any
	if err := r.Scan(&out.Stack, &out.SessionID, &out.Engine, &out.ThreadID, &out.LastIndexedStep, &rawMtime, &out.IsSettled); err != nil {
		return SessionResult{}, fmt.Errorf("scan session result: %w", err)
	}
	if t, ok := parseDBTime(rawMtime); ok {
		out.LastMtime = t
	}
	return out, nil
}

func scanStepResult(r *sql.Rows) (StepResult, error) {
	var out StepResult
	var rawCreatedAt any
	if err := r.Scan(&out.Stack, &out.SessionID, &out.Engine, &out.StepIndex, &out.StepType, &out.ToolName, &out.ToolKind, &out.Content, &rawCreatedAt); err != nil {
		return StepResult{}, fmt.Errorf("scan step result: %w", err)
	}
	if t, ok := parseDBTime(rawCreatedAt); ok {
		out.CreatedAt = t
	}
	return out, nil
}

func scanStackStat(r *sql.Rows) (StackStats, error) {
	var out StackStats
	if err := r.Scan(&out.Stack, &out.Sessions, &out.Steps); err != nil {
		return StackStats{}, fmt.Errorf("scan transcript stats: %w", err)
	}
	return out, nil
}

func scanSessionWatermark(r *sql.Rows) (SessionWatermark, error) {
	var out SessionWatermark
	var rawMtime any
	if err := r.Scan(&out.Stack, &out.SessionID, &out.LastIndexedStep, &rawMtime); err != nil {
		return SessionWatermark{}, fmt.Errorf("scan watermark: %w", err)
	}
	if t, ok := parseDBTime(rawMtime); ok {
		out.LastMtime = t
	}
	return out, nil
}

// parseDBTime normalizes driver time representations without failing the row.
func parseDBTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, !t.IsZero()
	case []byte:
		return parseDBTimeString(string(t))
	case string:
		return parseDBTimeString(t)
	}
	return time.Time{}, false
}

func parseDBTimeString(s string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999999-07", "2006-01-02 15:04:05-07", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, strings.TrimSpace(s)); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// SearchSessions finds indexed sessions by summary text. An empty stack
// searches every stack in the shared index.
func (s *Store) SearchSessions(ctx context.Context, stack, queryText string, limit int) ([]SessionResult, error) {
	stack, queryText = strings.TrimSpace(stack), strings.TrimSpace(queryText)
	if queryText == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	return collectReadRows(ctx, s, "search sessions query failed",
		`SELECT stack, session_id, engine, thread_id, last_indexed_step, last_mtime, is_settled
		 FROM session_summaries WHERE ($1 = '' OR stack = $1) AND summary <> '' AND summary LIKE '%' || $2 || '%'
		 ORDER BY updated_at DESC LIMIT $3`, scanSessionResult, stack, queryText, limit)
}

// SearchSteps finds indexed steps by content text with optional session and
// tool filters. An empty stack searches every stack in the shared index.
func (s *Store) SearchSteps(ctx context.Context, stack, queryText, sessionFilter, toolFilter string, limit int) ([]StepResult, error) {
	stack = strings.TrimSpace(stack)
	queryText, sessionFilter, toolFilter = strings.TrimSpace(queryText), strings.TrimSpace(sessionFilter), strings.TrimSpace(toolFilter)
	if queryText == "" && sessionFilter == "" && toolFilter == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	query, args := buildStepsQuery(stack, queryText, sessionFilter, toolFilter, limit)
	return collectReadRows(ctx, s, "search steps query failed", query, scanStepResult, args...)
}

const stepsSelectClause = `SELECT t.stack, t.session_id, s.engine, t.step_index, t.step_type, t.tool_name, t.tool_kind, t.content, t.created_at
	FROM transcript_steps t JOIN session_summaries s ON s.stack = t.stack AND s.session_id = t.session_id`

// buildStepsQuery renders the filtered steps query; the empty stack matches
// every stack while a set stack narrows to that stack's rows.
func buildStepsQuery(stack, queryText, sessionFilter, toolFilter string, limit int) (string, []any) {
	var clauses []string
	var args []any
	if queryText != "" {
		args = append(args, queryText)
		clauses = append(clauses, fmt.Sprintf("t.content LIKE '%%' || $%d || '%%'", len(args)))
	}
	if stack != "" {
		args = append(args, stack)
		clauses = append(clauses, fmt.Sprintf("t.stack = $%d", len(args)))
	}
	if sessionFilter != "" {
		args = append(args, sessionFilter)
		clauses = append(clauses, fmt.Sprintf("t.session_id = $%d", len(args)))
	}
	if toolFilter != "" {
		args = append(args, toolFilter)
		clauses = append(clauses, fmt.Sprintf("(t.tool_kind = $%d OR t.tool_name = $%d)", len(args), len(args)))
	}
	args = append(args, limit)
	return stepsSelectClause + ` WHERE ` + strings.Join(clauses, " AND ") +
		fmt.Sprintf(` ORDER BY t.created_at DESC LIMIT $%d`, len(args)), args
}

// GetSteps returns one session's indexed step range. An empty stack reads
// across every stack; rows order by stack then step for stable cross-stack reads.
func (s *Store) GetSteps(ctx context.Context, stack, sessionID string, startStep, endStep int) ([]StepResult, error) {
	stack, sessionID = strings.TrimSpace(stack), strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, nil
	}
	if startStep < 0 {
		startStep = 0
	}
	if endStep < startStep {
		endStep = startStep
	}
	return collectReadRows(ctx, s, "get steps query failed", stepsSelectClause+`
		WHERE t.session_id = $1 AND t.step_index BETWEEN $2 AND $3 AND ($4 = '' OR t.stack = $4)
		ORDER BY t.stack ASC, t.step_index ASC`, scanStepResult, sessionID, startStep, endStep, stack)
}

// GetStats counts indexed sessions and steps per stack. An empty stack covers
// every stack in the shared index.
func (s *Store) GetStats(ctx context.Context, stack string) ([]StackStats, error) {
	return collectReadRows(ctx, s, "query transcript stats",
		`WITH ss AS (SELECT stack, COUNT(*) AS sessions FROM session_summaries WHERE $1 = '' OR stack = $1 GROUP BY stack),
		 ts AS (SELECT stack, COUNT(*) AS steps FROM transcript_steps WHERE $1 = '' OR stack = $1 GROUP BY stack)
		 SELECT ss.stack, ss.sessions, COALESCE(ts.steps, 0) FROM ss LEFT JOIN ts ON ts.stack = ss.stack ORDER BY ss.stack`,
		scanStackStat, strings.TrimSpace(stack))
}

// GetWatermarks returns per-session ingest cursors. An empty stack covers every
// stack in the shared index; extractors resume from these cursors.
func (s *Store) GetWatermarks(ctx context.Context, stack string) ([]SessionWatermark, error) {
	return collectReadRows(ctx, s, "query watermarks",
		`SELECT stack, session_id, last_indexed_step, last_mtime FROM session_summaries
		 WHERE $1 = '' OR stack = $1 ORDER BY stack ASC, session_id ASC`,
		scanSessionWatermark, strings.TrimSpace(stack))
}
