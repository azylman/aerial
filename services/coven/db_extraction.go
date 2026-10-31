package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/pgvector/pgvector-go"
)

type eriTasksResponse struct {
	Status string        `json:"status"`
	Total  int           `json:"total"`
	Tasks  []eriTaskItem `json:"tasks"`
}

type eriTaskItem struct {
	ID                string    `json:"id"`
	RowID             int64     `json:"row_id"`
	ThreadID          string    `json:"thread_id"`
	SessionID         string    `json:"session_id"`
	PreviousSessionID string    `json:"previous_session_id"`
	ThreadSummary     string    `json:"thread_summary"`
	Prompt            string    `json:"prompt"`
	ResponseText      string    `json:"response_text"`
	Status            string    `json:"status"`
	IsAdmin           bool      `json:"is_admin"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

func (t eriTaskItem) toMessage() Message {
	return Message{
		ID:           t.ID,
		RowID:        t.RowID,
		ThreadID:     t.ThreadID,
		Content:      t.Prompt,
		ResponseText: t.ResponseText,
		Status:       t.Status,
		IsAdmin:      t.IsAdmin,
		CreatedAt:    t.CreatedAt,
		UpdatedAt:    t.UpdatedAt,
	}
}

type eriTranscriptSearchResult struct {
	Steps []eriTranscriptStepItem `json:"steps"`
}

type eriTranscriptStepItem struct {
	StepType string `json:"step_type"`
	ToolName string `json:"tool_name"`
	Content  string `json:"content"`
}

func (s *SQLStore) fetchBrainJSON(ctx context.Context, endpointURL, label string, dst any) error {
	if s.getBrainURL() == "" {
		return fmt.Errorf("brain API URL not configured")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create %s request: %w", label, err)
	}
	if s.brainToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.brainToken)
	}
	resp, err := s.getHTTPClient().Do(req)
	if err != nil {
		return fmt.Errorf("%s request failed: %w", label, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s request returned status %d", label, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		return fmt.Errorf("failed to decode %s response: %w", label, err)
	}
	return nil
}

func (s *SQLStore) fetchTasks(ctx context.Context, endpoint string) ([]eriTaskItem, error) {
	var data eriTasksResponse
	if err := s.fetchBrainJSON(ctx, endpoint, "eri tasks", &data); err != nil {
		return nil, err
	}
	return data.Tasks, nil
}

func (s *SQLStore) getWatermark(ctx context.Context, threadID string) (int64, *time.Time, error) {
	if err := s.checkDB(); err != nil {
		return 0, nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var lastRowID int64
	var factExtractedAt FactTime
	query := `SELECT COALESCE(last_extracted_rowid, 0), fact_extracted_at FROM extraction_watermarks WHERE thread_id = $1`
	err := s.db.QueryRowContext(queryCtx, query, threadID).Scan(&lastRowID, &factExtractedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil, nil
		}
		return 0, nil, err
	}
	var t *time.Time
	if factExtractedAt.Valid {
		t = &factExtractedAt.Time
	}
	return lastRowID, t, nil
}

func (s *SQLStore) getWatermarkMap(ctx context.Context) (map[string]int64, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT thread_id, COALESCE(last_extracted_rowid, 0) FROM extraction_watermarks`
	rows, err := s.db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	watermarks := make(map[string]int64)
	for rows.Next() {
		var tid string
		var rowID int64
		if err := rows.Scan(&tid, &rowID); err != nil {
			return nil, err
		}
		if tid = strings.TrimSpace(tid); tid != "" {
			watermarks[tid] = rowID
		}
	}
	return watermarks, rows.Err()
}

// GetActiveConversationsForExtraction finds threads with unextracted admin turns in the past activeHours.
func (s *SQLStore) GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if activeHours <= 0 {
		activeHours = 24
	}
	watermarks, err := s.getWatermarkMap(ctx)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().UTC().Add(-time.Duration(activeHours) * time.Hour).Format(time.RFC3339)
	endpoint := fmt.Sprintf("%s/tasks?status=COMPLETED&is_admin=true&since=%s&limit=200",
		s.getBrainURL(), url.QueryEscape(cutoff))

	tasks, err := s.fetchTasks(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	var threadIDs []string
	for _, t := range tasks {
		tid := strings.TrimSpace(t.ThreadID)
		if tid == "" {
			continue
		}
		lastRowID := watermarks[tid]
		if t.RowID > lastRowID {
			if _, exists := seen[tid]; !exists {
				seen[tid] = struct{}{}
				threadIDs = append(threadIDs, tid)
			}
		}
	}
	sort.Strings(threadIDs)
	if len(threadIDs) > 20 {
		threadIDs = threadIDs[:20]
	}
	return threadIDs, nil
}

// GetUnextractedAdminTurns retrieves completed admin turns for threadID that haven't been extracted.
func (s *SQLStore) GetUnextractedAdminTurns(ctx context.Context, threadID string) ([]Message, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return nil, nil
	}
	lastRowID, _, err := s.getWatermark(ctx, threadID)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s/tasks?thread_id=%s&status=COMPLETED&is_admin=true&after_row_id=%d&limit=50",
		s.getBrainURL(), url.QueryEscape(threadID), lastRowID)

	tasks, err := s.fetchTasks(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var turns []Message
	for _, t := range tasks {
		m := t.toMessage()
		if IsExtractableAdminTurn(m) {
			turns = append(turns, m)
		}
	}
	// Sort turns ascending by RowID to guarantee strictly chronological conversation order
	// regardless of whether Eri returned results in DESC order (e.g. when after_row_id=0).
	sort.Slice(turns, func(i, j int) bool {
		return turns[i].RowID < turns[j].RowID
	})
	return turns, nil
}

// GetMaxMessageRowID returns the maximum row ID of completed messages for a thread.
func (s *SQLStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return 0, nil
	}

	endpoint := fmt.Sprintf("%s/tasks?thread_id=%s&status=COMPLETED&limit=1",
		s.getBrainURL(), url.QueryEscape(threadID))

	tasks, err := s.fetchTasks(ctx, endpoint)
	if err != nil {
		return 0, err
	}
	if len(tasks) == 0 {
		return 0, nil
	}
	return tasks[0].RowID, nil
}

// UpdateConversationFactWatermark updates or inserts watermark for thread fact extraction.
func (s *SQLStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return nil
	}
	now := time.Now().UTC()
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO extraction_watermarks (thread_id, last_extracted_rowid, fact_extracted_at, updated_at)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT(thread_id) DO UPDATE SET
	  last_extracted_rowid = CASE WHEN EXCLUDED.last_extracted_rowid > extraction_watermarks.last_extracted_rowid THEN EXCLUDED.last_extracted_rowid ELSE extraction_watermarks.last_extracted_rowid END,
	  fact_extracted_at = EXCLUDED.fact_extracted_at,
	  updated_at = EXCLUDED.updated_at`
	_, err := s.db.ExecContext(queryCtx, query, threadID, maxRowID, now, now)
	return err
}

// GetThreadContextBeforeWatermark returns the thread summary and up to priorTurnLimit prior admin turns before the extraction watermark.
func (s *SQLStore) GetThreadContextBeforeWatermark(ctx context.Context, threadID string, priorTurnLimit int) (string, []Message, error) {
	if err := s.checkDB(); err != nil {
		return "", nil, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return "", nil, nil
	}
	if priorTurnLimit <= 0 {
		priorTurnLimit = 2
	}

	lastRowID, _, err := s.getWatermark(ctx, threadID)
	if err != nil {
		return "", nil, err
	}

	if lastRowID <= 0 {
		endpoint := fmt.Sprintf("%s/tasks?thread_id=%s&status=COMPLETED&limit=1",
			s.getBrainURL(), url.QueryEscape(threadID))
		tasks, err := s.fetchTasks(ctx, endpoint)
		if err != nil {
			return "", nil, err
		}
		var summary string
		if len(tasks) > 0 {
			summary = tasks[0].ThreadSummary
		}
		return summary, nil, nil
	}

	fetchLimit := priorTurnLimit * 3
	endpoint := fmt.Sprintf("%s/tasks?thread_id=%s&status=COMPLETED&is_admin=true&before_row_id=%d&limit=%d",
		s.getBrainURL(), url.QueryEscape(threadID), lastRowID, fetchLimit)
	tasks, err := s.fetchTasks(ctx, endpoint)
	if err != nil {
		return "", nil, err
	}

	var summary string
	if len(tasks) > 0 {
		summary = tasks[0].ThreadSummary
	} else {
		fbEndpoint := fmt.Sprintf("%s/tasks?thread_id=%s&status=COMPLETED&limit=1",
			s.getBrainURL(), url.QueryEscape(threadID))
		fbTasks, err := s.fetchTasks(ctx, fbEndpoint)
		if err == nil && len(fbTasks) > 0 {
			summary = fbTasks[0].ThreadSummary
		}
	}

	var priorTurns []Message
	for _, t := range tasks {
		m := t.toMessage()
		if IsExtractableAdminTurn(m) {
			priorTurns = append(priorTurns, m)
			if len(priorTurns) >= priorTurnLimit {
				break
			}
		}
	}

	for i, j := 0, len(priorTurns)-1; i < j; i, j = i+1, j-1 {
		priorTurns[i], priorTurns[j] = priorTurns[j], priorTurns[i]
	}

	return summary, priorTurns, nil
}

var (
	reRetrievedMemoryBlock = regexp.MustCompile(`(?is)<\s*\\?retrieved_memory\s*>(.*?)<\s*[\\/]+retrieved_memory\s*>`)
	reFactLine             = regexp.MustCompile(`(?m)^\s*(-\s*\[id:\d+\].*)$`)
	reSkillPath            = regexp.MustCompile(`/skills/(?:(?:common|discord|voice|ephemeral)/)?([a-z0-9-]+)/SKILL\.md`)
	reSkillYAML            = regexp.MustCompile(`(?m)(?:1:\s*)?---\s*(?:\n|\s+)(?:2:\s*)?name:\s*([a-z0-9-]+)`)
	reSkillLoaded          = regexp.MustCompile(`\[SKILL_LOADED:\s*([a-z0-9-]+)\]`)
)

// ExtractLoadedSkillName extracts a skill name from trajectory step content or tool parameters.
func ExtractLoadedSkillName(stepType, toolName, content string) string {
	cleanType := strings.ToUpper(strings.TrimSpace(stepType))
	cleanTool := strings.ToLower(strings.TrimSpace(toolName))

	normalized := strings.ReplaceAll(content, "\\", "/")
	if cleanType == "PLANNER_RESPONSE" || cleanTool == "read_file" || strings.Contains(normalized, "/skills/") {
		if m := reSkillPath.FindStringSubmatch(normalized); len(m) > 1 {
			return strings.TrimSpace(m[1])
		}
	}
	if cleanType == "GENERIC" || cleanType == "TOOL" || cleanType == "RUN_COMMAND" || strings.Contains(content, "---") {
		if m := reSkillYAML.FindStringSubmatch(content); len(m) > 1 {
			return strings.TrimSpace(m[1])
		}
		if m := reSkillPath.FindStringSubmatch(normalized); len(m) > 1 {
			return strings.TrimSpace(m[1])
		}
	}
	return ""
}

// ExtractLoadedSkillsFromCalls extracts deduplicated loaded skill names from tool calls.
func ExtractLoadedSkillsFromCalls(toolCalls []string) []string {
	var skills []string
	seen := make(map[string]struct{})
	for _, call := range toolCalls {
		for _, m := range reSkillLoaded.FindAllStringSubmatch(call, -1) {
			if len(m) > 1 {
				name := strings.TrimSpace(m[1])
				if name != "" {
					if _, exists := seen[name]; !exists {
						seen[name] = struct{}{}
						skills = append(skills, name)
					}
				}
			}
		}
	}
	return skills
}

func truncateSingleLine(s string, maxLen int) string {
	s = strings.Join(strings.Fields(s), " ")
	if maxLen > 0 && len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

func extractRetrievedMemoryFacts(content string) []string {
	var facts []string
	seen := make(map[string]struct{})
	for _, block := range reRetrievedMemoryBlock.FindAllStringSubmatch(content, -1) {
		if len(block) <= 1 {
			continue
		}
		for _, lineMatch := range reFactLine.FindAllStringSubmatch(block[1], -1) {
			if len(lineMatch) <= 1 {
				continue
			}
			line := strings.TrimSpace(lineMatch[1])
			if line == "" {
				continue
			}
			if _, exists := seen[line]; !exists {
				seen[line] = struct{}{}
				facts = append(facts, line)
			}
		}
	}
	return facts
}

func extractTrajectorySignals(stepType, toolName, content string) ([]string, string) {
	switch stepType {
	case "USER_INPUT":
		return extractRetrievedMemoryFacts(content), ""
	case "PLANNER_RESPONSE":
		trimmedTool := strings.TrimSpace(toolName)
		if trimmedTool != "" {
			skillName := ExtractLoadedSkillName(stepType, toolName, content)
			callStr := fmt.Sprintf("%s(%s)", trimmedTool, truncateSingleLine(content, 200))
			if skillName != "" {
				return nil, fmt.Sprintf("%s [SKILL_LOADED: %s]", callStr, skillName)
			}
			return nil, callStr
		}
	case "GENERIC", "TOOL", "RUN_COMMAND":
		skillName := ExtractLoadedSkillName(stepType, toolName, content)
		if skillName != "" {
			return nil, fmt.Sprintf("[SKILL_LOADED: %s]", skillName)
		}
		trimmedTool := strings.TrimSpace(toolName)
		if trimmedTool == "" {
			trimmedTool = strings.ToLower(stepType)
		}
		return nil, fmt.Sprintf("-> OUTPUT(%s): %s", trimmedTool, truncateSingleLine(content, 200))
	}
	return nil, ""
}

func extractSessionIDs(task eriTaskItem) []string {
	var parts []string
	if sessID := strings.TrimSpace(task.SessionID); sessID != "" {
		parts = append(parts, sessID)
	}
	if prevSessID := strings.TrimSpace(task.PreviousSessionID); prevSessID != "" && prevSessID != task.SessionID {
		parts = append(parts, prevSessID)
	}
	return parts
}

func parseTrajectorySteps(steps []eriTranscriptStepItem) ([]string, []string) {
	var retrievedFacts []string
	var toolCalls []string
	seenFacts := make(map[string]struct{})
	for _, step := range steps {
		content := step.Content
		r := []rune(content)
		if len(r) > 500 {
			content = string(r[:500])
		}
		rf, tc := extractTrajectorySignals(step.StepType, step.ToolName, content)
		for _, f := range rf {
			if _, exists := seenFacts[f]; !exists {
				seenFacts[f] = struct{}{}
				retrievedFacts = append(retrievedFacts, f)
			}
		}
		if tc != "" {
			toolCalls = append(toolCalls, tc)
		}
	}
	return retrievedFacts, toolCalls
}

func (s *SQLStore) fetchTranscriptSteps(ctx context.Context, sessionCSV, sinceRFC string) ([]eriTranscriptStepItem, error) {
	searchEndpoint := fmt.Sprintf("%s/transcripts/search?mode=steps&session=%s&since=%s&limit=50",
		s.getBrainURL(), url.QueryEscape(sessionCSV), url.QueryEscape(sinceRFC))

	var searchRes eriTranscriptSearchResult
	if err := s.fetchBrainJSON(ctx, searchEndpoint, "transcript search", &searchRes); err != nil {
		return nil, err
	}
	return searchRes.Steps, nil
}

// GetThreadTrajectoryContext queries the trajectory context (retrieved facts and tool calls) for an active thread.
func (s *SQLStore) GetThreadTrajectoryContext(ctx context.Context, threadID string) (retrievedFacts []string, toolCalls []string, err error) {
	if err := s.checkDB(); err != nil {
		return nil, nil, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return nil, nil, nil
	}

	_, factExtractedAt, err := s.getWatermark(ctx, threadID)
	if err != nil {
		return nil, nil, err
	}
	sinceTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if factExtractedAt != nil && !factExtractedAt.IsZero() {
		sinceTime = factExtractedAt.UTC()
	}

	endpoint := fmt.Sprintf("%s/tasks?thread_id=%s&status=COMPLETED&limit=1",
		s.getBrainURL(), url.QueryEscape(threadID))
	tasks, err := s.fetchTasks(ctx, endpoint)
	if err != nil {
		return nil, nil, err
	}
	if len(tasks) == 0 {
		return nil, nil, nil
	}

	sessionParts := extractSessionIDs(tasks[0])
	if len(sessionParts) == 0 {
		return nil, nil, nil
	}

	steps, err := s.fetchTranscriptSteps(ctx, strings.Join(sessionParts, ","), sinceTime.Format(time.RFC3339))
	if err != nil {
		return nil, nil, err
	}

	facts, tools := parseTrajectorySteps(steps)
	return facts, tools, nil
}

// InsertProceduralCandidate persists a staged procedural candidate into the procedural_candidates table.
func (s *SQLStore) InsertProceduralCandidate(ctx context.Context, cand ProceduralCandidate) (int64, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	trigger := strings.TrimSpace(cand.Trigger)
	if trigger == "" {
		return 0, fmt.Errorf("trigger pattern cannot be empty")
	}
	stepsBytes, err := json.Marshal(cand.Steps)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal steps: %w", err)
	}
	if cand.Steps == nil {
		stepsBytes = []byte("[]")
	}
	avoidBytes, err := json.Marshal(cand.Avoid)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal avoid: %w", err)
	}
	if cand.Avoid == nil {
		avoidBytes = []byte("[]")
	}
	domain := strings.TrimSpace(cand.Domain)
	if domain == "" {
		domain = "discord"
	}
	status := strings.TrimSpace(cand.Status)
	if status == "" {
		status = "pending"
	}

	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var prNum any
	if cand.PRNumber > 0 {
		prNum = cand.PRNumber
	}

	query := `INSERT INTO procedural_candidates (thread_id, domain, loaded_skill, trigger_pattern, steps_json, avoid_json, embedding, status, pr_number)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	RETURNING id`

	var id int64
	if err := s.db.QueryRowContext(queryCtx, query,
		cand.ThreadID,
		domain,
		cand.LoadedSkill,
		trigger,
		string(stepsBytes),
		string(avoidBytes),
		pgvector.NewVector(cand.Embedding),
		status,
		prNum,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("failed to insert procedural candidate: %w", err)
	}
	return id, nil
}

// GetPendingProceduralCandidates returns up to 100 pending candidates ordered by created_at ASC, id ASC.
func (s *SQLStore) GetPendingProceduralCandidates(ctx context.Context) ([]ProceduralCandidate, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT id, thread_id, domain, loaded_skill, trigger_pattern, steps_json, avoid_json, embedding, status, COALESCE(pr_number, 0), created_at
	FROM procedural_candidates
	WHERE status = 'pending'
	ORDER BY created_at ASC, id ASC
	LIMIT 100`

	rows, err := s.db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending procedural candidates: %w", err)
	}
	defer rows.Close()

	var candidates []ProceduralCandidate
	for rows.Next() {
		var (
			cand      ProceduralCandidate
			stepsRaw  []byte
			avoidRaw  []byte
			vec       pgvector.Vector
			createdAt FactTime
		)
		if err := rows.Scan(
			&cand.ID,
			&cand.ThreadID,
			&cand.Domain,
			&cand.LoadedSkill,
			&cand.Trigger,
			&stepsRaw,
			&avoidRaw,
			&vec,
			&cand.Status,
			&cand.PRNumber,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan procedural candidate: %w", err)
		}
		cand.CreatedAt = createdAt.Time
		cand.Embedding = vec.Slice()
		if len(stepsRaw) > 0 {
			if err := json.Unmarshal(stepsRaw, &cand.Steps); err != nil {
				cand.Steps = []string{}
			}
		} else {
			cand.Steps = []string{}
		}
		if len(avoidRaw) > 0 {
			if err := json.Unmarshal(avoidRaw, &cand.Avoid); err != nil {
				cand.Avoid = []string{}
			}
		} else {
			cand.Avoid = []string{}
		}
		candidates = append(candidates, cand)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read procedural candidate rows: %w", err)
	}
	return candidates, nil
}

func (s *SQLStore) updateProceduralCandidates(ctx context.Context, setClause, errLabel string, prefixArgs []any, ids []int64) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(prefixArgs)+len(ids))
	args = append(args, prefixArgs...)
	offset := len(prefixArgs)
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", offset+i+1)
		args = append(args, id)
	}

	query := fmt.Sprintf(`UPDATE procedural_candidates
	SET %s, updated_at = CURRENT_TIMESTAMP
	WHERE id IN (%s)`, setClause, strings.Join(placeholders, ", "))

	if _, err := s.db.ExecContext(queryCtx, query, args...); err != nil {
		return fmt.Errorf("failed to mark procedural candidates %s: %w", errLabel, err)
	}
	return nil
}

// MarkProceduralCandidatesConsolidated transitions candidate status to 'consolidated' and records the created PR number.
func (s *SQLStore) MarkProceduralCandidatesConsolidated(ctx context.Context, ids []int64, prNumber int) error {
	return s.updateProceduralCandidates(ctx, "status = 'consolidated', pr_number = $1", "consolidated", []any{prNumber}, ids)
}

// MarkProceduralCandidatesSkipped transitions candidate status to 'skipped'.
func (s *SQLStore) MarkProceduralCandidatesSkipped(ctx context.Context, ids []int64) error {
	return s.updateProceduralCandidates(ctx, "status = 'skipped'", "skipped", nil, ids)
}
