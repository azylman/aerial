package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pgvector/pgvector-go"
)

const domainDiscord = "discord"

// GetConversationFactWatermark retrieves the last extracted row ID and timestamp for a thread or sentinel key.
func (s *SQLStore) GetConversationFactWatermark(ctx context.Context, threadID string, agent ...string) (int64, *time.Time, error) {
	if err := s.checkDB(); err != nil {
		return 0, nil, err
	}
	targetAgent := s.effectiveAgent()
	if len(agent) > 0 && strings.TrimSpace(agent[0]) != "" {
		targetAgent = strings.TrimSpace(agent[0])
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var lastRowID int64
	var factExtractedAt FactTime
	query := `SELECT COALESCE(last_extracted_rowid, 0), fact_extracted_at FROM extraction_watermarks WHERE agent = $1 AND thread_id = $2`
	err := s.db.QueryRowContext(queryCtx, query, targetAgent, threadID).Scan(&lastRowID, &factExtractedAt)
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

// GetUnextractedAdminTurns retrieves completed admin turns for threadID that haven't been extracted.
func (s *SQLStore) GetUnextractedAdminTurns(ctx context.Context, threadID string, limit int, agent ...string) ([]Message, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return nil, nil
	}
	targetAgent := s.effectiveAgent()
	if len(agent) > 0 && strings.TrimSpace(agent[0]) != "" {
		targetAgent = strings.TrimSpace(agent[0])
	}
	lastRowID, _, err := s.GetConversationFactWatermark(ctx, threadID, targetAgent)
	if err != nil {
		return nil, err
	}

	batchLimit := 50
	if limit > 0 {
		batchLimit = limit
	}

	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT id, COALESCE(row_id, 0), thread_id, content, COALESCE(response_text, ''), status, is_admin, created_at, updated_at
	FROM messages
	WHERE agent = $1 AND thread_id = $2 AND status = 'COMPLETED' AND is_admin = true AND row_id > $3
	ORDER BY row_id ASC
	LIMIT $4`

	rows, err := s.db.QueryContext(queryCtx, query, targetAgent, threadID, lastRowID, batchLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var turns []Message
	for rows.Next() {
		var (
			m         Message
			createdAt FactTime
			updatedAt FactTime
		)
		if err := rows.Scan(&m.ID, &m.RowID, &m.ThreadID, &m.Content, &m.ResponseText, &m.Status, &m.IsAdmin, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan turn message: %w", err)
		}
		m.CreatedAt = createdAt.Time
		m.UpdatedAt = updatedAt.Time
		if IsExtractableAdminTurn(m) {
			turns = append(turns, m)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed reading unextracted turn rows: %w", err)
	}

	return turns, nil
}

// GetMaxMessageRowID returns the maximum row ID of completed messages for a thread.
func (s *SQLStore) GetMaxMessageRowID(ctx context.Context, threadID string, agent ...string) (int64, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return 0, nil
	}
	targetAgent := s.effectiveAgent()
	if len(agent) > 0 && strings.TrimSpace(agent[0]) != "" {
		targetAgent = strings.TrimSpace(agent[0])
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT COALESCE(MAX(row_id), 0) FROM messages WHERE agent = $1 AND thread_id = $2 AND status = 'COMPLETED'`
	var maxRowID int64
	err := s.db.QueryRowContext(queryCtx, query, targetAgent, threadID).Scan(&maxRowID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return maxRowID, nil
}

// UpdateConversationFactWatermark updates or inserts watermark for thread fact extraction.
func (s *SQLStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64, agent ...string) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return nil
	}
	targetAgent := s.effectiveAgent()
	if len(agent) > 0 && strings.TrimSpace(agent[0]) != "" {
		targetAgent = strings.TrimSpace(agent[0])
	}
	now := time.Now().UTC()
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO extraction_watermarks (agent, thread_id, last_extracted_rowid, fact_extracted_at, updated_at)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (agent, thread_id) DO UPDATE SET
	  last_extracted_rowid = GREATEST(extraction_watermarks.last_extracted_rowid, EXCLUDED.last_extracted_rowid),
	  fact_extracted_at = EXCLUDED.fact_extracted_at,
	  updated_at = EXCLUDED.updated_at`
	_, err := s.db.ExecContext(queryCtx, query, targetAgent, threadID, maxRowID, now, now)
	return err
}

// GetThreadContextBeforeWatermark returns the thread summary and up to priorTurnLimit prior admin turns before the extraction watermark.
func (s *SQLStore) GetThreadContextBeforeWatermark(ctx context.Context, threadID string, priorTurnLimit int, agent ...string) (string, []Message, error) {
	if err := s.checkDB(); err != nil {
		return "", nil, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return "", nil, nil
	}
	if priorTurnLimit <= 0 {
		priorTurnLimit = 2
	}
	targetAgent := s.effectiveAgent()
	if len(agent) > 0 && strings.TrimSpace(agent[0]) != "" {
		targetAgent = strings.TrimSpace(agent[0])
	}

	lastRowID, _, err := s.GetConversationFactWatermark(ctx, threadID, targetAgent)
	if err != nil {
		return "", nil, err
	}

	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	// Direct PostgreSQL query against sessions scoped by agent and session_id
	var summary string
	sessionQuery := `SELECT COALESCE(s.summary, '')
	FROM sessions s
	WHERE s.agent = $1 AND (s.session_id = $2 OR s.session_id = 'discord-' || $2)
	ORDER BY s.updated_at DESC
	LIMIT 1`
	if scanErr := s.db.QueryRowContext(queryCtx, sessionQuery, targetAgent, threadID).Scan(&summary); scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
		return "", nil, fmt.Errorf("failed to query session summary: %w", scanErr)
	}

	if summary == "" {
		joinQuery := `SELECT COALESCE(s.summary, '')
		FROM messages m
		JOIN sessions s ON s.agent = m.agent AND s.session_id = m.conversation_id
		WHERE m.agent = $1 AND m.thread_id = $2 AND s.summary != ''
		ORDER BY m.row_id DESC
		LIMIT 1`
		if joinErr := s.db.QueryRowContext(queryCtx, joinQuery, targetAgent, threadID).Scan(&summary); joinErr != nil && !errors.Is(joinErr, sql.ErrNoRows) {
			return "", nil, fmt.Errorf("failed to query joined session summary: %w", joinErr)
		}
	}

	if lastRowID <= 0 {
		return summary, nil, nil
	}

	fetchLimit := priorTurnLimit * 3
	turnsQuery := `SELECT id, COALESCE(row_id, 0), thread_id, content, COALESCE(response_text, ''), status, is_admin, created_at, updated_at
	FROM messages
	WHERE agent = $1 AND thread_id = $2 AND status = 'COMPLETED' AND is_admin = true AND row_id < $3
	ORDER BY row_id DESC
	LIMIT $4`

	rows, err := s.db.QueryContext(queryCtx, turnsQuery, targetAgent, threadID, lastRowID, fetchLimit)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()

	var priorTurns []Message
	for rows.Next() {
		var (
			m         Message
			createdAt FactTime
			updatedAt FactTime
		)
		if err := rows.Scan(&m.ID, &m.RowID, &m.ThreadID, &m.Content, &m.ResponseText, &m.Status, &m.IsAdmin, &createdAt, &updatedAt); err != nil {
			return "", nil, fmt.Errorf("failed to scan prior turn message: %w", err)
		}
		m.CreatedAt = createdAt.Time
		m.UpdatedAt = updatedAt.Time
		if IsExtractableAdminTurn(m) {
			priorTurns = append(priorTurns, m)
			if len(priorTurns) >= priorTurnLimit {
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		return "", nil, fmt.Errorf("failed reading prior turn rows: %w", err)
	}

	// Reverse to strictly chronological order (ascending row_id)
	for i, j := 0, len(priorTurns)-1; i < j; i, j = i+1, j-1 {
		priorTurns[i], priorTurns[j] = priorTurns[j], priorTurns[i]
	}

	return summary, priorTurns, nil
}

var reSkillLoaded = regexp.MustCompile(`\[SKILL_LOADED:\s*([a-z0-9-]+)\]`)

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

// GetThreadTrajectoryContext queries the trajectory context (retrieved facts and tool calls) for an active thread.
func (s *SQLStore) GetThreadTrajectoryContext(ctx context.Context, threadID string, agent ...string) ([]string, []string, error) {
	if checkDBErr := s.checkDB(); checkDBErr != nil {
		return nil, nil, checkDBErr
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return nil, nil, nil
	}
	targetAgent := s.effectiveAgent()
	if len(agent) > 0 && strings.TrimSpace(agent[0]) != "" {
		targetAgent = strings.TrimSpace(agent[0])
	}

	if s.indexerClient == nil {
		return nil, nil, nil
	}
	tc, tcErr := s.indexerClient.GetTrajectoryContext(ctx, targetAgent, threadID)
	if tcErr != nil {
		return nil, nil, tcErr
	}
	return tc.RetrievedFacts, tc.ToolCalls, nil
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
		domain = domainDiscord
	}
	status := strings.TrimSpace(cand.Status)
	if status == "" {
		status = "pending"
	}

	agent := strings.TrimSpace(cand.Agent)
	if agent == "" {
		agent = s.effectiveStack()
	}

	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var prNum any
	if cand.PRNumber > 0 {
		prNum = cand.PRNumber
	}

	query := `INSERT INTO procedural_candidates (thread_id, domain, loaded_skill, trigger_pattern, steps_json, avoid_json, embedding, status, pr_number, agent)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
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
		agent,
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

	query := `SELECT id, thread_id, domain, loaded_skill, trigger_pattern, steps_json, avoid_json, embedding, status, COALESCE(pr_number, 0), created_at, COALESCE(agent, 'aerial')
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
			&cand.Agent,
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
