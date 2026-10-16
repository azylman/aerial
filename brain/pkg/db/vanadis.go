package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrVanadisBuildNotFound = errors.New("vanadis build not found")
	ErrVanadisChunkNotFound = errors.New("vanadis chunk not found")
)

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
	}

	return s.WithTx(ctx, func(txStore Store) error {
		txSQL, ok := txStore.(*SQLStore)
		if !ok {
			return fmt.Errorf("unexpected tx store type: %T", txStore)
		}
		execDB := txSQL.db

		buildStatusMessageID := strings.TrimSpace(build.StatusMessageID)
		buildQuery := `
			INSERT INTO vanadis_builds (id, repo, thread_id, tier, objective, status, status_message_id, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
		`
		if _, err := execDB.ExecContext(ctx, buildQuery, buildID, repo, threadID, tier, objective, status, buildStatusMessageID); err != nil {
			return fmt.Errorf("failed to insert vanadis build: %w", err)
		}

		chunkQuery := `
			INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions, target_files, status, pr_number, diff_stat, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
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
			var prNum sql.NullInt64
			if c.PRNumber > 0 {
				prNum = sql.NullInt64{Int64: int64(c.PRNumber), Valid: true}
			}

			var targetFilesArg any
			if txSQL.isPostgres {
				files := c.TargetFiles
				if files == nil {
					files = []string{}
				}
				targetFilesArg = files
			} else {
				targetFilesArg = formatPostgresTextArray(c.TargetFiles)
			}

			if _, err := execDB.ExecContext(ctx, chunkQuery,
				chunkID, buildID, c.ChunkIndex, chunkRepo, title, instructions, targetFilesArg, cStatus, prNum, c.DiffStat,
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
		SELECT id, repo, thread_id, tier, objective, status, status_message_id, created_at, updated_at
		FROM vanadis_builds
		WHERE id = $1;
	`
	var b VanadisBuild
	err := s.db.QueryRowContext(ctx, buildQuery, buildID).Scan(
		&b.ID, &b.Repo, &b.ThreadID, &b.Tier, &b.Objective, &b.Status, &b.StatusMessageID, &b.CreatedAt, &b.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrVanadisBuildNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query vanadis build: %w", err)
	}

	chunksQuery := `
		SELECT id, build_id, chunk_index, repo, title, instructions, target_files, status, COALESCE(pr_number, 0), diff_stat, created_at, updated_at
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
		var rawTargetFiles string
		var targetDest any
		if s.isPostgres {
			targetDest = &c.TargetFiles
		} else {
			targetDest = &rawTargetFiles
		}
		if err := rows.Scan(
			&c.ID, &c.BuildID, &c.ChunkIndex, &c.Repo, &c.Title, &c.Instructions, targetDest, &c.Status, &c.PRNumber, &c.DiffStat, &c.CreatedAt, &c.UpdatedAt,
		); err != nil {
			return nil, nil, fmt.Errorf("failed to scan vanadis chunk: %w", err)
		}
		if !s.isPostgres {
			c.TargetFiles = parsePostgresTextArray(rawTargetFiles)
		} else if c.TargetFiles == nil {
			c.TargetFiles = []string{}
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
		SELECT id, build_id, chunk_index, repo, title, instructions, target_files, status, COALESCE(pr_number, 0), diff_stat, created_at, updated_at
		FROM vanadis_chunks
		WHERE id = $1;
	`
	var c VanadisChunk
	var rawTargetFiles string
	var targetDest any
	if s.isPostgres {
		targetDest = &c.TargetFiles
	} else {
		targetDest = &rawTargetFiles
	}

	err := s.db.QueryRowContext(ctx, query, chunkID).Scan(
		&c.ID, &c.BuildID, &c.ChunkIndex, &c.Repo, &c.Title, &c.Instructions, targetDest, &c.Status, &c.PRNumber, &c.DiffStat, &c.CreatedAt, &c.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrVanadisChunkNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query vanadis chunk: %w", err)
	}
	if !s.isPostgres {
		c.TargetFiles = parsePostgresTextArray(rawTargetFiles)
	} else if c.TargetFiles == nil {
		c.TargetFiles = []string{}
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
		SET status = $1, updated_at = CURRENT_TIMESTAMP
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

// AttachVanadisChunkPR links an opened Pull Request and diff stat to a VanadisChunk and marks it 'pr_opened'.
func (s *SQLStore) AttachVanadisChunkPR(ctx context.Context, chunkID string, prNumber int, diffStat string) error {
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
		SET pr_number = $1, diff_stat = $2, status = 'pr_opened', updated_at = CURRENT_TIMESTAMP
		WHERE id = $3;
	`
	res, err := s.db.ExecContext(ctx, query, prNumber, diffStat, chunkID)
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
