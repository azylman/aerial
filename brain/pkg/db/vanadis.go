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

// CreateVanadisBuild inserts a VanadisBuild and all of its ordered VanadisChunks in a single transaction.
func (s *SQLStore) CreateVanadisBuild(ctx context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database connection is nil")
	}

	buildID := strings.TrimSpace(build.ID)
	if buildID == "" {
		return fmt.Errorf("build id cannot be empty")
	}
	repo := normalizeRepo(build.Repo)
	if repo == "" {
		return fmt.Errorf("repo cannot be empty")
	}
	threadID := strings.TrimSpace(build.ThreadID)
	if threadID == "" {
		return fmt.Errorf("thread_id cannot be empty")
	}
	objective := strings.TrimSpace(build.Objective)
	if objective == "" {
		return fmt.Errorf("objective cannot be empty")
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

	for i, c := range chunks {
		if strings.TrimSpace(c.ID) == "" {
			return fmt.Errorf("chunk[%d] id cannot be empty", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		if strings.TrimSpace(c.Title) == "" {
			return fmt.Errorf("chunk[%d] title cannot be empty", i)
		}
		if strings.TrimSpace(c.Instructions) == "" {
			return fmt.Errorf("chunk[%d] instructions cannot be empty", i)
		}
		if _, err := normalizeVanadisEffort(c.Effort); err != nil {
			return fmt.Errorf("chunk[%d] %w", i, err)
		}
	}

	return s.WithTx(ctx, func(txStore Store) error {
		txSQL, ok := txStore.(*SQLStore)
		if !ok {
			return fmt.Errorf("unexpected tx store type: %T", txStore)
		}
		execDB := txSQL.db

		buildStatusMessageID := strings.TrimSpace(build.StatusMessageID)
		sharedContext := strings.TrimSpace(build.SharedContext)
		buildQuery := `
			INSERT INTO vanadis_builds (id, repo, thread_id, tier, title, objective, shared_context, status, status_message_id, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
		`
		if _, err := execDB.ExecContext(ctx, buildQuery, buildID, repo, threadID, tier, buildTitle, objective, sharedContext, status, buildStatusMessageID); err != nil {
			return fmt.Errorf("failed to insert vanadis build: %w", err)
		}

		chunkQuery := `
			INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, started_at, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
		`
		for _, c := range chunks {
			chunkID := strings.TrimSpace(c.ID)
			title := strings.TrimSpace(c.Title)
			instructions := strings.TrimSpace(c.Instructions)
			chunkRepo := normalizeRepo(c.Repo)
			if chunkRepo == "" {
				chunkRepo = repo
			}
			cStatus := strings.TrimSpace(c.Status)
			if cStatus == "" {
				cStatus = "pending"
			}
			cEffort, err := normalizeVanadisEffort(c.Effort)
			if err != nil {
				return fmt.Errorf("chunk %s: %w", chunkID, err)
			}
			var prNum sql.NullInt64
			if c.PRNumber > 0 {
				prNum = sql.NullInt64{Int64: int64(c.PRNumber), Valid: true}
			}
			var startedAt sql.NullTime
			if !c.StartedAt.IsZero() {
				startedAt = sql.NullTime{Time: c.StartedAt.UTC(), Valid: true}
			} else if cStatus == "running" || cStatus == "coding" {
				startedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
			}

			var targetFilesArg, targetSymbolsArg, acceptanceCriteriaArg any
			if txSQL.isPostgres {
				files := c.TargetFiles
				if files == nil {
					files = []string{}
				}
				targetFilesArg = files

				symbols := c.TargetSymbols
				if symbols == nil {
					symbols = []string{}
				}
				targetSymbolsArg = symbols

				criteria := c.AcceptanceCriteria
				if criteria == nil {
					criteria = []string{}
				}
				acceptanceCriteriaArg = criteria
			} else {
				targetFilesArg = formatPostgresTextArray(c.TargetFiles)
				targetSymbolsArg = formatPostgresTextArray(c.TargetSymbols)
				acceptanceCriteriaArg = formatPostgresTextArray(c.AcceptanceCriteria)
			}

			if _, err := execDB.ExecContext(ctx, chunkQuery,
				chunkID, buildID, c.ChunkIndex, chunkRepo, title, instructions, targetFilesArg, targetSymbolsArg, cEffort, acceptanceCriteriaArg, cStatus, prNum, startedAt,
			); err != nil {
				return fmt.Errorf("failed to insert vanadis chunk %s: %w", chunkID, err)
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
		SELECT id, repo, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, shared_context, status, status_message_id, created_at, updated_at
		FROM vanadis_builds
		WHERE id = $1;
	`
	var b VanadisBuild
	err := s.db.QueryRowContext(ctx, buildQuery, buildID).Scan(
		&b.ID, &b.Repo, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.CreatedAt, &b.UpdatedAt,
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
		var c VanadisChunk
		var startedAt sql.NullTime
		var rawTargetFiles, rawTargetSymbols, rawAcceptanceCriteria string
		var targetFilesDest, targetSymbolsDest, acceptanceCriteriaDest any
		if s.isPostgres {
			targetFilesDest = &c.TargetFiles
			targetSymbolsDest = &c.TargetSymbols
			acceptanceCriteriaDest = &c.AcceptanceCriteria
		} else {
			targetFilesDest = &rawTargetFiles
			targetSymbolsDest = &rawTargetSymbols
			acceptanceCriteriaDest = &rawAcceptanceCriteria
		}
		if err := rows.Scan(
			&c.ID, &c.BuildID, &c.ChunkIndex, &c.Repo, &c.Title, &c.Instructions, targetFilesDest, targetSymbolsDest, &c.Effort, acceptanceCriteriaDest, &c.Status, &c.PRNumber, &startedAt, &c.CreatedAt, &c.UpdatedAt,
		); err != nil {
			return nil, nil, fmt.Errorf("failed to scan vanadis chunk: %w", err)
		}
		if startedAt.Valid {
			c.StartedAt = startedAt.Time
		}
		if !s.isPostgres {
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
	var c VanadisChunk
	var startedAt sql.NullTime
	var rawTargetFiles, rawTargetSymbols, rawAcceptanceCriteria string
	var targetFilesDest, targetSymbolsDest, acceptanceCriteriaDest any
	if s.isPostgres {
		targetFilesDest = &c.TargetFiles
		targetSymbolsDest = &c.TargetSymbols
		acceptanceCriteriaDest = &c.AcceptanceCriteria
	} else {
		targetFilesDest = &rawTargetFiles
		targetSymbolsDest = &rawTargetSymbols
		acceptanceCriteriaDest = &rawAcceptanceCriteria
	}

	err := s.db.QueryRowContext(ctx, query, chunkID).Scan(
		&c.ID, &c.BuildID, &c.ChunkIndex, &c.Repo, &c.Title, &c.Instructions, targetFilesDest, targetSymbolsDest, &c.Effort, acceptanceCriteriaDest, &c.Status, &c.PRNumber, &startedAt, &c.CreatedAt, &c.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrVanadisChunkNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query vanadis chunk: %w", err)
	}
	if startedAt.Valid {
		c.StartedAt = startedAt.Time
	}
	if !s.isPostgres {
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
	return &c, nil
}

// UpdateVanadisChunkStatus updates the lifecycle status of a VanadisChunk.
func (s *SQLStore) UpdateVanadisChunkStatus(ctx context.Context, chunkID, status string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database connection is nil")
	}

	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return fmt.Errorf("chunk id cannot be empty")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return fmt.Errorf("status cannot be empty")
	}

	query := `
		UPDATE vanadis_chunks
		SET status = $1,
		    started_at = CASE WHEN $1 IN ('running', 'coding') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $2;
	`
	res, err := s.db.ExecContext(ctx, query, status, chunkID)
	if err != nil {
		return fmt.Errorf("failed to update vanadis chunk status: %w", err)
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
		SET pr_number = $1,
		    status = 'pr_opened',
		    started_at = COALESCE(started_at, created_at, CURRENT_TIMESTAMP),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $2;
	`
	res, err := s.db.ExecContext(ctx, query, prNumber, chunkID)
	if err != nil {
		return fmt.Errorf("failed to attach vanadis chunk pr: %w", err)
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

// UpdateVanadisBuildStatus updates the lifecycle status of a VanadisBuild.
func (s *SQLStore) UpdateVanadisBuildStatus(ctx context.Context, buildID, status string) error {
	if s == nil || isDBTXNil(s.db) {
		return fmt.Errorf("database connection is nil")
	}

	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return fmt.Errorf("build id cannot be empty")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return fmt.Errorf("status cannot be empty")
	}

	query := `
		UPDATE vanadis_builds
		SET status = $1, updated_at = CURRENT_TIMESTAMP
		WHERE id = $2;
	`
	res, err := s.db.ExecContext(ctx, query, status, buildID)
	if err != nil {
		return fmt.Errorf("failed to update vanadis build status: %w", err)
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
