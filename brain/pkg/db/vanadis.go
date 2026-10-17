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

func formatPostgresTextArray(items []string) string {
	if len(items) == 0 {
		return "{}"
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, item := range items {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		for j := 0; j < len(item); j++ {
			ch := item[j]
			if ch == '\\' || ch == '"' {
				b.WriteByte('\\')
			}
			b.WriteByte(ch)
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
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
	inQuotes := false
	escaped := false

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
	normBuild.ID = buildID
	normBuild.ThreadID = threadID
	normBuild.Objective = objective
	normBuild.Title = buildTitle
	normBuild.Tier = tier
	normBuild.Status = status
	normBuild.SharedContext = strings.TrimSpace(build.SharedContext)
	normBuild.StatusMessageID = strings.TrimSpace(build.StatusMessageID)

	normChunks := make([]VanadisChunk, len(chunks))
	for i, c := range chunks {
		cid := strings.TrimSpace(c.ID)
		if cid == "" {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] id cannot be empty", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		title := strings.TrimSpace(c.Title)
		if title == "" {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] title cannot be empty", i)
		}
		instructions := strings.TrimSpace(c.Instructions)
		if instructions == "" {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] instructions cannot be empty", i)
		}
		effort, err := normalizeVanadisEffort(c.Effort)
		if err != nil {
			return VanadisBuild{}, nil, fmt.Errorf("chunk[%d] %w", i, err)
		}
		chunkRepo := normalizeRepo(c.Repo)
		if chunkRepo == "" {
			chunkRepo = "azylman/aerial"
		}
		cStatus := strings.TrimSpace(c.Status)
		if cStatus == "" {
			cStatus = "pending"
		}
		files := c.TargetFiles
		if files == nil {
			files = []string{}
		}
		symbols := c.TargetSymbols
		if symbols == nil {
			symbols = []string{}
		}
		criteria := c.AcceptanceCriteria
		if criteria == nil {
			criteria = []string{}
		}

		normChunks[i] = VanadisChunk{
			ID:                 cid,
			BuildID:            buildID,
			ChunkIndex:         c.ChunkIndex,
			Repo:               chunkRepo,
			Title:              title,
			Instructions:       instructions,
			TargetFiles:        files,
			TargetSymbols:      symbols,
			Effort:             effort,
			AcceptanceCriteria: criteria,
			Status:             cStatus,
			PRNumber:           c.PRNumber,
			StartedAt:          c.StartedAt,
		}
	}

	return normBuild, normChunks, nil
}

func scanVanadisChunk(scanner interface{ Scan(dest ...any) error }, isPostgres bool) (VanadisChunk, error) {
	var c VanadisChunk
	var startedAt sql.NullTime
	var rawTargetFiles, rawTargetSymbols, rawAcceptanceCriteria string
	var targetFilesDest, targetSymbolsDest, acceptanceCriteriaDest any
	if isPostgres {
		targetFilesDest = &c.TargetFiles
		targetSymbolsDest = &c.TargetSymbols
		acceptanceCriteriaDest = &c.AcceptanceCriteria
	} else {
		targetFilesDest = &rawTargetFiles
		targetSymbolsDest = &rawTargetSymbols
		acceptanceCriteriaDest = &rawAcceptanceCriteria
	}

	if err := scanner.Scan(
		&c.ID, &c.BuildID, &c.ChunkIndex, &c.Repo, &c.Title, &c.Instructions, targetFilesDest, targetSymbolsDest, &c.Effort, acceptanceCriteriaDest, &c.Status, &c.PRNumber, &startedAt, &c.CreatedAt, &c.UpdatedAt,
	); err != nil {
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

// CreateVanadisBuild inserts a VanadisBuild and all of its ordered VanadisChunks in a single transaction.
func (s *SQLStore) CreateVanadisBuild(ctx context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database connection is nil")
	}

	normBuild, normChunks, err := validateAndNormalizeVanadisBuild(build, chunks)
	if err != nil {
		return err
	}

	return s.WithTx(ctx, func(txStore Store) error {
		txSQL, ok := txStore.(*SQLStore)
		if !ok {
			return fmt.Errorf("unexpected tx store type: %T", txStore)
		}
		execDB := txSQL.db

		buildQuery := `
			INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
		`
		if _, err := execDB.ExecContext(ctx, buildQuery, normBuild.ID, normBuild.ThreadID, normBuild.Tier, normBuild.Title, normBuild.Objective, normBuild.SharedContext, normBuild.Status, normBuild.StatusMessageID); err != nil {
			return fmt.Errorf("failed to insert vanadis build: %w", err)
		}

		chunkQuery := `
			INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, started_at, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
		`
		for _, c := range normChunks {
			var prNum sql.NullInt64
			if c.PRNumber > 0 {
				prNum = sql.NullInt64{Int64: int64(c.PRNumber), Valid: true}
			}
			var startedAt sql.NullTime
			if !c.StartedAt.IsZero() {
				startedAt = sql.NullTime{Time: c.StartedAt.UTC(), Valid: true}
			} else if c.Status == "running" || c.Status == "coding" {
				startedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
			}

			var targetFilesArg, targetSymbolsArg, acceptanceCriteriaArg any
			if txSQL.isPostgres {
				targetFilesArg = c.TargetFiles
				targetSymbolsArg = c.TargetSymbols
				acceptanceCriteriaArg = c.AcceptanceCriteria
			} else {
				targetFilesArg = formatPostgresTextArray(c.TargetFiles)
				targetSymbolsArg = formatPostgresTextArray(c.TargetSymbols)
				acceptanceCriteriaArg = formatPostgresTextArray(c.AcceptanceCriteria)
			}

			if _, err := execDB.ExecContext(ctx, chunkQuery,
				c.ID, c.BuildID, c.ChunkIndex, c.Repo, c.Title, c.Instructions, targetFilesArg, targetSymbolsArg, c.Effort, acceptanceCriteriaArg, c.Status, prNum, startedAt,
			); err != nil {
				return fmt.Errorf("failed to insert vanadis chunk %s: %w", c.ID, err)
			}
		}
		return nil
	})
}

// GetVanadisBuild retrieves a VanadisBuild and its ordered VanadisChunks by buildID.
func (s *SQLStore) GetVanadisBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, nil, fmt.Errorf("database connection is nil")
	}

	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return nil, nil, fmt.Errorf("build id cannot be empty")
	}

	buildQuery := `
		SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, shared_context, status, status_message_id, created_at, updated_at
		FROM vanadis_builds
		WHERE id = $1;
	`
	var b VanadisBuild
	err := s.db.QueryRowContext(ctx, buildQuery, buildID).Scan(
		&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.CreatedAt, &b.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrVanadisBuildNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query vanadis build: %w", err)
	}

	chunksQuery := `
		SELECT id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, COALESCE(pr_number, 0), started_at, created_at, updated_at
		FROM vanadis_chunks
		WHERE build_id = $1
		ORDER BY chunk_index ASC;
	`
	rows, err := s.db.QueryContext(ctx, chunksQuery, buildID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query vanadis chunks: %w", err)
	}
	defer rows.Close()

	chunks := make([]VanadisChunk, 0)
	for rows.Next() {
		c, err := scanVanadisChunk(rows, s.isPostgres)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan vanadis chunk: %w", err)
		}
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating vanadis chunks: %w", err)
	}

	return &b, chunks, nil
}

// GetVanadisChunk retrieves a single VanadisChunk by chunkID.
func (s *SQLStore) GetVanadisChunk(ctx context.Context, chunkID string) (*VanadisChunk, error) {
	if s == nil || isDBTXNil(s.db) {
		return nil, fmt.Errorf("database connection is nil")
	}

	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, fmt.Errorf("chunk id cannot be empty")
	}

	query := `
		SELECT id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, COALESCE(pr_number, 0), started_at, created_at, updated_at
		FROM vanadis_chunks
		WHERE id = $1;
	`
	c, err := scanVanadisChunk(s.db.QueryRowContext(ctx, query, chunkID), s.isPostgres)
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
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database connection is nil")
	}

	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return fmt.Errorf("chunk id cannot be empty")
	}
	if prNumber <= 0 {
		return fmt.Errorf("pr_number must be greater than 0")
	}

	query := `
		UPDATE vanadis_chunks
		SET pr_number = $1, status = 'pr_opened', started_at = COALESCE(started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP
		WHERE id = $2;
	`
	res, err := s.db.ExecContext(ctx, query, prNumber, chunkID)
	if err != nil {
		return fmt.Errorf("failed to attach pr to vanadis chunk: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrVanadisChunkNotFound
	}
	return nil
}

// UpdateVanadisBuildStatusMessageID updates the status_message_id of a VanadisBuild.
func (s *SQLStore) UpdateVanadisBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database connection is nil")
	}

	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return fmt.Errorf("build id cannot be empty")
	}

	query := `
		UPDATE vanadis_builds
		SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP
		WHERE id = $2;
	`
	res, err := s.db.ExecContext(ctx, query, strings.TrimSpace(messageID), buildID)
	if err != nil {
		return fmt.Errorf("failed to update vanadis build status_message_id: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrVanadisBuildNotFound
	}
	return nil
}
