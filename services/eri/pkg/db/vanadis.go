package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrVanadisBuildNotFound = errors.New("vanadis build not found")
	ErrVanadisChunkNotFound = errors.New("vanadis chunk not found")
)

func normalizeVanadisEffort(effort string) (string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(effort))
	if trimmed == "" {
		return "medium", nil
	}
	switch trimmed {
	case "low", "medium", "high":
		return trimmed, nil
	default:
		return "", fmt.Errorf("invalid vanadis effort %q: must be 'low', 'medium', or 'high'", effort)
	}
}

func parsePostgresTextArray(raw string) []string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "{}" {
		return []string{}
	}
	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	if trimmed == "" {
		return []string{}
	}
	var result []string
	var cur strings.Builder
	inQuotes, escaped := false, false
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
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
			continue
		}
		if ch == ',' && !inQuotes {
			result = append(result, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteByte(ch)
	}
	result = append(result, cur.String())
	return result
}

func validateAndNormalizeVanadisBuild(build VanadisBuild, chunks []VanadisChunk) (VanadisBuild, []VanadisChunk, error) {
	buildID := strings.TrimSpace(build.ID)
	if buildID == "" {
		return VanadisBuild{}, nil, fmt.Errorf("build id cannot be empty")
	}
	threadID := strings.TrimSpace(build.ThreadID)
	if threadID == "" {
		return VanadisBuild{}, nil, fmt.Errorf("thread_id cannot be empty")
	}
	objective := strings.TrimSpace(build.Objective)
	if objective == "" {
		return VanadisBuild{}, nil, fmt.Errorf("objective cannot be empty")
	}
	buildTitle := strings.TrimSpace(build.Title)
	if buildTitle == "" {
		buildTitle = objective
	}
	tier := build.Tier
	if tier <= 0 {
		tier = 2
	}
	status := strings.TrimSpace(build.Status)
	if status == "" {
		status = "running"
	}

	normBuild := build
	normBuild.ID, normBuild.ThreadID, normBuild.Objective = buildID, threadID, objective
	normBuild.Title, normBuild.Tier, normBuild.Status = buildTitle, tier, status
	normBuild.SharedContext, normBuild.StatusMessageID = strings.TrimSpace(build.SharedContext), strings.TrimSpace(build.StatusMessageID)

	normChunks := make([]VanadisChunk, len(chunks))
	for i, c := range chunks {
		cid := strings.TrimSpace(c.ID)
		if cid == "" {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] id cannot be empty", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		title, instructions := strings.TrimSpace(c.Title), strings.TrimSpace(c.Instructions)
		if title == "" {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] title cannot be empty", i)
		}
		if instructions == "" {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] instructions cannot be empty", i)
		}
		effort, err := normalizeVanadisEffort(c.Effort)
		if err != nil {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] %w", i, err)
		}
		chunkRepo, cStatus := normalizeRepo(c.Repo), strings.TrimSpace(c.Status)
		if chunkRepo == "" {
			chunkRepo = "azylman/aerial"
		}
		if cStatus == "" {
			cStatus = "pending"
		}
		files, symbols, criteria := c.TargetFiles, c.TargetSymbols, c.AcceptanceCriteria
		if files == nil {
			files = []string{}
		}
		if symbols == nil {
			symbols = []string{}
		}
		if criteria == nil {
			criteria = []string{}
		}

		normChunks[i] = VanadisChunk{
			ID: cid, BuildID: buildID, ChunkIndex: c.ChunkIndex, Repo: chunkRepo, Title: title, Instructions: instructions,
			TargetFiles: files, TargetSymbols: symbols, Effort: effort, AcceptanceCriteria: criteria, Status: cStatus, PRNumber: c.PRNumber, StartedAt: c.StartedAt,
		}
	}
	return normBuild, normChunks, nil
}

func scanVanadisBuild(scanner interface{ Scan(dest ...any) error }) (VanadisBuild, error) {
	var b VanadisBuild
	err := scanner.Scan(&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.CreatedAt, &b.UpdatedAt)
	return b, err
}

func scanVanadisChunk(scanner interface{ Scan(dest ...any) error }, isPostgres bool) (VanadisChunk, error) {
	var c VanadisChunk
	var startedAt sql.NullTime
	var rawTargetFiles, rawTargetSymbols, rawAcceptanceCriteria string
	targetFilesDest, targetSymbolsDest, acceptanceCriteriaDest := any(&c.TargetFiles), any(&c.TargetSymbols), any(&c.AcceptanceCriteria)
	if !isPostgres {
		targetFilesDest, targetSymbolsDest, acceptanceCriteriaDest = &rawTargetFiles, &rawTargetSymbols, &rawAcceptanceCriteria
	}

	if err := scanner.Scan(&c.ID, &c.BuildID, &c.ChunkIndex, &c.Repo, &c.Title, &c.Instructions, targetFilesDest, targetSymbolsDest, &c.Effort, acceptanceCriteriaDest, &c.Status, &c.PRNumber, &startedAt, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return VanadisChunk{}, err
	}
	if startedAt.Valid {
		c.StartedAt = startedAt.Time
	}
	if !isPostgres {
		c.TargetFiles = parsePostgresTextArray(rawTargetFiles)
		c.TargetSymbols = parsePostgresTextArray(rawTargetSymbols)
		c.AcceptanceCriteria = parsePostgresTextArray(rawAcceptanceCriteria)
	} else {
		if c.TargetFiles == nil {
			c.TargetFiles = []string{}
		}
		if c.TargetSymbols == nil {
			c.TargetSymbols = []string{}
		}
		if c.AcceptanceCriteria == nil {
			c.AcceptanceCriteria = []string{}
		}
	}
	if strings.TrimSpace(c.Effort) == "" {
		c.Effort = "medium"
	}
	return c, nil
}

// GetVanadisBuild retrieves a VanadisBuild and its ordered VanadisChunks by buildID.
func (s *SQLStore) GetVanadisBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if err := s.checkDB(); err != nil {
		return nil, nil, err
	}
	if buildID = strings.TrimSpace(buildID); buildID == "" {
		return nil, nil, fmt.Errorf("build id cannot be empty")
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	b, err := scanVanadisBuild(s.db.QueryRowContext(queryCtx, `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, shared_context, status, status_message_id, created_at, updated_at FROM vanadis_builds WHERE id = $1;`, buildID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrVanadisBuildNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query vanadis build: %w", err)
	}

	rows, err := s.db.QueryContext(queryCtx, `SELECT id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, COALESCE(pr_number, 0), started_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC;`, buildID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query vanadis chunks: %w", err)
	}
	defer rows.Close()

	var chunks []VanadisChunk
	for rows.Next() {
		c, err := scanVanadisChunk(rows, s.isPostgres)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan vanadis chunk: %w", err)
		}
		chunks = append(chunks, c)
	}
	return &b, chunks, rows.Err()
}

// GetVanadisChunk retrieves a single VanadisChunk by chunkID.
func (s *SQLStore) GetVanadisChunk(ctx context.Context, chunkID string) (*VanadisChunk, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if chunkID = strings.TrimSpace(chunkID); chunkID == "" {
		return nil, fmt.Errorf("chunk id cannot be empty")
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	c, err := scanVanadisChunk(s.db.QueryRowContext(queryCtx, `SELECT id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, COALESCE(pr_number, 0), started_at, created_at, updated_at FROM vanadis_chunks WHERE id = $1;`, chunkID), s.isPostgres)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrVanadisChunkNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query vanadis chunk: %w", err)
	}
	return &c, nil
}

// AttachVanadisChunkPR links an opened Pull Request to a VanadisChunk and marks it 'pr_opened'.
func (s *SQLStore) AttachVanadisChunkPR(ctx context.Context, chunkID string, prNumber int) error {
	if chunkID = strings.TrimSpace(chunkID); chunkID == "" {
		return fmt.Errorf("chunk id cannot be empty")
	}
	if prNumber <= 0 {
		return fmt.Errorf("pr_number must be greater than 0")
	}
	return s.execRequireOneRow(ctx, 5*time.Second, `UPDATE vanadis_chunks SET pr_number = $1, status = 'pr_opened', started_at = COALESCE(started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, ErrVanadisChunkNotFound, prNumber, chunkID)
}

// UpdateVanadisBuildStatusMessageID updates the status_message_id of a VanadisBuild.
func (s *SQLStore) UpdateVanadisBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	if buildID = strings.TrimSpace(buildID); buildID == "" {
		return fmt.Errorf("build id cannot be empty")
	}
	return s.execRequireOneRow(ctx, 5*time.Second, `UPDATE vanadis_builds SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, ErrVanadisBuildNotFound, strings.TrimSpace(messageID), buildID)
}
