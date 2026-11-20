package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// SessionResult is one indexed session in the read API.
type SessionResult struct {
	Agent           string    `json:"agent,omitempty"`
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
	Agent     string    `json:"agent,omitempty"`
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
	Agent    string `json:"agent,omitempty"`
	Stack    string `json:"stack"`
	Sessions int    `json:"sessions"`
	Steps    int    `json:"steps"`
}

// SessionWatermark is the per-session ingest cursor consumers resume from.
type SessionWatermark struct {
	Agent           string    `json:"agent,omitempty"`
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
	out.Agent = out.Stack
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
	out.Agent = out.Stack
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
	out.Agent = out.Stack
	return out, nil
}

func scanSessionWatermark(r *sql.Rows) (SessionWatermark, error) {
	var out SessionWatermark
	var rawMtime any
	if err := r.Scan(&out.Stack, &out.SessionID, &out.LastIndexedStep, &rawMtime); err != nil {
		return SessionWatermark{}, fmt.Errorf("scan watermark: %w", err)
	}
	out.Agent = out.Stack
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
		`SELECT agent, session_id, engine, thread_id, last_indexed_step, last_mtime, is_settled
		 FROM session_summaries WHERE ($1 = '' OR agent = $1) AND summary <> '' AND summary LIKE '%' || $2 || '%'
		 ORDER BY updated_at DESC LIMIT $3`, scanSessionResult, stack, queryText, limit)
}

// SearchSteps finds indexed steps by content text with optional session and
// tool filters and optional created_at lower bound. An empty stack searches every stack in the shared index.
func (s *Store) SearchSteps(ctx context.Context, stack, queryText, sessionFilter, toolFilter string, limit int, since ...time.Time) ([]StepResult, error) {
	stack = strings.TrimSpace(stack)
	queryText, sessionFilter, toolFilter = strings.TrimSpace(queryText), strings.TrimSpace(sessionFilter), strings.TrimSpace(toolFilter)
	hasSince := len(since) > 0 && !since[0].IsZero()
	if queryText == "" && sessionFilter == "" && toolFilter == "" && !hasSince {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	query, args := buildStepsQuery(stack, queryText, sessionFilter, toolFilter, limit, isPostgresDriver(s.db.Driver()), since...)
	return collectReadRows(ctx, s, "search steps query failed", query, scanStepResult, args...)
}

const stepsSelectClause = `SELECT t.agent, t.session_id, s.engine, t.step_index, t.step_type, t.tool_name, t.tool_kind, t.content, t.created_at
	FROM transcript_steps t JOIN session_summaries s ON s.agent = t.agent AND s.session_id = t.session_id`

// buildStepsQuery renders the filtered steps query; the empty stack matches
// every stack while a set stack narrows to that stack's rows. PostgreSQL
// matches content through the fts_tokens GIN index (the same
// websearch_to_tsquery shape the eri store and coven use); hermetic SQLite
// fixtures keep the LIKE scan since they carry no FTS index.
func buildStepsQuery(stack, queryText, sessionFilter, toolFilter string, limit int, useFTS bool, since ...time.Time) (string, []any) {
	var clauses []string
	var args []any
	if queryText != "" {
		args = append(args, queryText)
		if useFTS {
			clauses = append(clauses, fmt.Sprintf("t.fts_tokens @@ websearch_to_tsquery('simple', $%d)", len(args)))
		} else {
			clauses = append(clauses, fmt.Sprintf("t.content LIKE '%%' || $%d || '%%'", len(args)))
		}
	}
	if stack != "" {
		args = append(args, stack)
		clauses = append(clauses, fmt.Sprintf("t.agent = $%d", len(args)))
	}
	if sessionFilter != "" {
		var sessions []string
		for s := range strings.SplitSeq(sessionFilter, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				sessions = append(sessions, s)
			}
		}
		if len(sessions) == 1 {
			args = append(args, sessions[0])
			clauses = append(clauses, fmt.Sprintf("t.session_id = $%d", len(args)))
		} else if len(sessions) > 1 {
			placeholders := make([]string, 0, len(sessions))
			for _, s := range sessions {
				args = append(args, s)
				placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
			}
			clauses = append(clauses, fmt.Sprintf("t.session_id IN (%s)", strings.Join(placeholders, ", ")))
		}
	}
	if toolFilter != "" {
		args = append(args, toolFilter)
		clauses = append(clauses, fmt.Sprintf("(t.tool_kind = $%d OR t.tool_name = $%d)", len(args), len(args)))
	}
	if len(since) > 0 && !since[0].IsZero() {
		args = append(args, since[0].UTC())
		clauses = append(clauses, fmt.Sprintf("t.created_at >= $%d", len(args)))
	}
	args = append(args, limit)
	return stepsSelectClause + ` WHERE ` + strings.Join(clauses, " AND ") +
		fmt.Sprintf(` ORDER BY t.created_at DESC LIMIT $%d`, len(args)), args
}

var reRetrievedMemoryBlock = regexp.MustCompile(`(?is)<\s*\\?retrieved_memory\s*>(.*?)<\s*[\\/]+retrieved_memory\s*>`)

func truncateSingleLine(s string, maxLen int) string {
	s = strings.Join(strings.Fields(s), " ")
	if maxLen > 0 && len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

// formatTrajectoryToolCall formats a step into a tool_name/step_type summary line.
func formatTrajectoryToolCall(stepType, toolName, content string) string {
	toolName = strings.TrimSpace(toolName)
	stepType = strings.TrimSpace(stepType)
	if toolName == "" && stepType == "" {
		return ""
	}
	label := toolName
	if label == "" {
		label = stepType
	} else if stepType != "" {
		label = toolName + "/" + stepType
	}
	content = truncateSingleLine(content, 200)
	if content != "" {
		return fmt.Sprintf("%s: %s", label, content)
	}
	return label
}

func extractRetrievedFacts(content string, seenFacts map[string]struct{}) []string {
	var facts []string
	for _, block := range reRetrievedMemoryBlock.FindAllStringSubmatch(content, -1) {
		if len(block) <= 1 {
			continue
		}
		for line := range strings.SplitSeq(block[1], "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if _, seen := seenFacts[line]; !seen {
				seenFacts[line] = struct{}{}
				facts = append(facts, line)
			}
		}
	}
	return facts
}

// GetThreadTrajectory queries trajectory steps for threadID across session_summaries
// and extracts retrieved facts (<retrieved_memory> lines) and tool_calls.
func (s *Store) GetThreadTrajectory(ctx context.Context, stack, threadID string, limit int) ([]string, []string, error) {
	stack, threadID = strings.TrimSpace(stack), strings.TrimSpace(threadID)
	if threadID == "" {
		return nil, nil, nil
	}
	if limit <= 0 {
		limit = 50
	}
	if err := s.checkDB(); err != nil {
		return nil, nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(queryCtx,
		`SELECT t.step_type, t.tool_name, t.content
		 FROM transcript_steps t
		 JOIN session_summaries s ON s.agent = t.agent AND s.session_id = t.session_id
		 WHERE s.thread_id = $1 AND ($2 = '' OR t.agent = $2)
		 ORDER BY t.created_at DESC LIMIT $3`,
		threadID, stack, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("get thread trajectory: %w", err)
	}
	defer rows.Close()

	var retrievedFacts []string
	var toolCalls []string
	seenFacts := make(map[string]struct{})

	for rows.Next() {
		var stepType, toolName, content string
		if err := rows.Scan(&stepType, &toolName, &content); err != nil {
			return nil, nil, fmt.Errorf("scan thread trajectory row: %w", err)
		}
		retrievedFacts = append(retrievedFacts, extractRetrievedFacts(content, seenFacts)...)
		if strings.ToUpper(strings.TrimSpace(stepType)) != "USER_INPUT" {
			if call := formatTrajectoryToolCall(stepType, toolName, content); call != "" {
				toolCalls = append(toolCalls, call)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("thread trajectory rows: %w", err)
	}
	return retrievedFacts, toolCalls, nil
}

// GetWatermark returns the last indexed step for one session on the given stack.
// Returns -1 if the session is not found in session_summaries.
func (s *Store) GetWatermark(ctx context.Context, stack, sessionID string) (int, error) {
	stack, sessionID = strings.TrimSpace(stack), strings.TrimSpace(sessionID)
	if sessionID == "" {
		return -1, nil
	}
	if err := s.checkDB(); err != nil {
		return -1, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var lastStep int
	err := s.db.QueryRowContext(queryCtx,
		`SELECT last_indexed_step FROM session_summaries WHERE ($1 = '' OR agent = $1) AND session_id = $2`,
		stack, sessionID).Scan(&lastStep)
	if errors.Is(err, sql.ErrNoRows) {
		return -1, nil
	}
	if err != nil {
		return -1, fmt.Errorf("get watermark: %w", err)
	}
	return lastStep, nil
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
		WHERE t.session_id = $1 AND t.step_index BETWEEN $2 AND $3 AND ($4 = '' OR t.agent = $4)
		ORDER BY t.agent ASC, t.step_index ASC`, scanStepResult, sessionID, startStep, endStep, stack)
}

// GetStats counts indexed sessions and steps per stack. An empty stack covers
// every stack in the shared index.
func (s *Store) GetStats(ctx context.Context, stack string) ([]StackStats, error) {
	return collectReadRows(ctx, s, "query transcript stats",
		`WITH ss AS (SELECT agent, COUNT(*) AS sessions FROM session_summaries WHERE $1 = '' OR agent = $1 GROUP BY agent),
		 ts AS (SELECT agent, COUNT(*) AS steps FROM transcript_steps WHERE $1 = '' OR agent = $1 GROUP BY agent)
		 SELECT ss.agent, ss.sessions, COALESCE(ts.steps, 0) FROM ss LEFT JOIN ts ON ts.agent = ss.agent ORDER BY ss.agent`,
		scanStackStat, strings.TrimSpace(stack))
}

// GetWatermarks returns per-session ingest cursors. An empty stack covers every
// stack in the shared index; extractors resume from these cursors.
func (s *Store) GetWatermarks(ctx context.Context, stack string) ([]SessionWatermark, error) {
	return collectReadRows(ctx, s, "query watermarks",
		`SELECT agent, session_id, last_indexed_step, last_mtime FROM session_summaries
		 WHERE $1 = '' OR agent = $1 ORDER BY agent ASC, session_id ASC`,
		scanSessionWatermark, strings.TrimSpace(stack))
}
