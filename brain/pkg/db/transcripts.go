package db

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/metrics"
	pgvector "github.com/pgvector/pgvector-go"
)

// GetSessionSyncStates retrieves a snapshot map of session IDs to sync states.
func GetSessionSyncStates(database DBTX) (map[string]SessionSyncState, error) {
	return GetSessionSyncStatesWithContext(context.Background(), database, false)
}

// GetSessionSyncStatesWithContext retrieves sync states with context and database driver detection.
func GetSessionSyncStatesWithContext(ctx context.Context, database DBTX, isPg bool) (map[string]SessionSyncState, error) {
	start := time.Now()
	var err error
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("get_session_sync_states", status, time.Since(start))
	}()

	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `SELECT session_id, last_mtime, last_indexed_step, is_settled FROM session_summaries`
	rows, err := database.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("query session sync states: %w", err)
	}
	defer rows.Close()

	results := make(map[string]SessionSyncState)
	for rows.Next() {
		var (
			id        string
			rawMtime  any
			lastStep  int
			isSettled bool
		)
		if err := rows.Scan(&id, &rawMtime, &lastStep, &isSettled); err != nil {
			return nil, fmt.Errorf("scan session sync state: %w", err)
		}
		var mtime time.Time
		if t, ok := ParseDBTime(rawMtime); ok {
			mtime = t
		}
		results[id] = SessionSyncState{
			SessionID:       id,
			LastMtime:       mtime,
			LastIndexedStep: lastStep,
			IsSettled:       isSettled,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session sync states: %w", err)
	}
	return results, nil
}

// UpsertSessionSummary inserts or updates a session summary record.
func UpsertSessionSummary(database DBTX, summary SessionSummary) error {
	return UpsertSessionSummaryWithContext(context.Background(), database, false, summary)
}

// UpsertSessionSummaryWithContext inserts or updates a session summary record with context.
func UpsertSessionSummaryWithContext(ctx context.Context, database DBTX, isPg bool, summary SessionSummary) (err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("upsert_session_summary", status, time.Since(start))
	}()

	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if strings.TrimSpace(summary.SessionID) == "" {
		return fmt.Errorf("session ID cannot be empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	now := time.Now().UTC()
	createdAt := summary.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := now

	var mtimeVal any
	if !summary.LastMtime.IsZero() {
		mtimeVal = summary.LastMtime
	}

	var vecVal any
	if len(summary.Embedding) == ExpectedEmbeddingDim {
		vecVal = pgvector.NewVector(summary.Embedding)
	}

	query := `
	INSERT INTO session_summaries (
		session_id, thread_id, summary, embedding, last_indexed_step, last_mtime,
		summary_step_watermark, is_settled, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT (session_id) DO UPDATE SET
		thread_id = CASE WHEN EXCLUDED.thread_id != '' THEN EXCLUDED.thread_id ELSE session_summaries.thread_id END,
		summary = CASE WHEN EXCLUDED.summary != '' THEN EXCLUDED.summary ELSE session_summaries.summary END,
		embedding = CASE WHEN EXCLUDED.embedding IS NOT NULL THEN EXCLUDED.embedding ELSE session_summaries.embedding END,
		last_indexed_step = CASE 
			WHEN EXCLUDED.last_indexed_step >= 0 THEN EXCLUDED.last_indexed_step 
			WHEN EXCLUDED.last_indexed_step = -2 THEN -1
			ELSE session_summaries.last_indexed_step 
		END,
		last_mtime = CASE WHEN EXCLUDED.last_mtime IS NOT NULL THEN EXCLUDED.last_mtime ELSE session_summaries.last_mtime END,
		summary_step_watermark = CASE 
			WHEN EXCLUDED.summary_step_watermark >= 0 THEN EXCLUDED.summary_step_watermark 
			ELSE session_summaries.summary_step_watermark 
		END,
		is_settled = CASE 
			WHEN EXCLUDED.is_settled THEN TRUE 
			WHEN EXCLUDED.last_indexed_step > session_summaries.summary_step_watermark AND session_summaries.summary_step_watermark >= 0 THEN FALSE 
			ELSE session_summaries.is_settled 
		END,
		updated_at = EXCLUDED.updated_at;
	`
	_, err = database.ExecContext(queryCtx, query,
		summary.SessionID,
		summary.ThreadID,
		summary.Summary,
		vecVal,
		summary.LastIndexedStep,
		mtimeVal,
		summary.SummaryStepWatermark,
		summary.IsSettled,
		createdAt,
		updatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert session summary failed: %w", err)
	}
	return nil
}

// BatchInsertTranscriptSteps inserts multiple execution steps with conflict-ignoring semantics.
func BatchInsertTranscriptSteps(database DBTX, steps []TranscriptStep) error {
	return BatchInsertTranscriptStepsWithContext(context.Background(), database, false, steps)
}

// BatchInsertTranscriptStepsWithContext inserts steps with chunking and context.
func BatchInsertTranscriptStepsWithContext(ctx context.Context, database DBTX, isPg bool, steps []TranscriptStep) (err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("batch_insert_transcript_steps", status, time.Since(start))
	}()

	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if len(steps) == 0 {
		return nil
	}

	validSteps := make([]TranscriptStep, 0, len(steps))
	for _, s := range steps {
		if strings.TrimSpace(s.SessionID) != "" {
			validSteps = append(validSteps, s)
		}
	}
	if len(validSteps) == 0 {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	const batchSize = 100
	for i := 0; i < len(validSteps); i += batchSize {
		end := i + batchSize
		if end > len(validSteps) {
			end = len(validSteps)
		}
		chunk := validSteps[i:end]

		valStrings := make([]string, 0, len(chunk))
		valArgs := make([]any, 0, len(chunk)*6)
		now := time.Now().UTC()

		for j, s := range chunk {
			createdAt := s.CreatedAt
			if createdAt.IsZero() {
				createdAt = now
			}
			p := j * 6
			valStrings = append(valStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", p+1, p+2, p+3, p+4, p+5, p+6))
			valArgs = append(valArgs, s.SessionID, s.StepIndex, s.StepType, s.ToolName, s.Content, createdAt)
		}

		query := fmt.Sprintf(`
		INSERT INTO transcript_steps (session_id, step_index, step_type, tool_name, content, created_at)
		VALUES %s
		ON CONFLICT (session_id, step_index) DO NOTHING;
		`, strings.Join(valStrings, ", "))

		if _, err := database.ExecContext(queryCtx, query, valArgs...); err != nil {
			return fmt.Errorf("batch insert transcript steps chunk failed: %w", err)
		}
	}
	return nil
}

// SearchSessionSummaries searches macro session summaries using hybrid RRF scoring.
func SearchSessionSummaries(database DBTX, embedding []float32, queryText string, limit int, minScore float64) ([]SessionSummary, error) {
	return SearchSessionSummariesWithContext(context.Background(), database, false, embedding, queryText, limit, minScore)
}

// SearchSessionSummariesWithContext searches macro session summaries with context.
func SearchSessionSummariesWithContext(ctx context.Context, database DBTX, isPg bool, embedding []float32, queryText string, limit int, minScore float64) (results []SessionSummary, err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("search_session_summaries", status, time.Since(start))
	}()

	if database == nil {
		return nil, nil
	}

	hasEmbedding := len(embedding) == ExpectedEmbeddingDim
	queryText = strings.TrimSpace(queryText)
	hasText := queryText != ""

	if !hasEmbedding && !hasText {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	if minScore < 0 {
		minScore = 0.0
	}

	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// SQLite in-memory fallback for hermetic unit testing
	if !isPg && !isPostgres(database) {
		query := `
		SELECT session_id, thread_id, summary, embedding, last_indexed_step, last_mtime, 
		       summary_step_watermark, is_settled, created_at, updated_at 
		FROM session_summaries;
		`
		rows, err := database.QueryContext(queryCtx, query)
		if err != nil {
			return nil, fmt.Errorf("sqlite search session summaries query: %w", err)
		}
		defer rows.Close()

		var scored []SessionSummary
		qLower := strings.ToLower(queryText)

		for rows.Next() {
			var s SessionSummary
			var nv NullVector
			var rawMtime, rawCreatedAt, rawUpdatedAt any
			if err := rows.Scan(
				&s.SessionID, &s.ThreadID, &s.Summary, &nv, &s.LastIndexedStep,
				&rawMtime, &s.SummaryStepWatermark, &s.IsSettled, &rawCreatedAt, &rawUpdatedAt,
			); err != nil {
				return nil, fmt.Errorf("sqlite scan session summary: %w", err)
			}
			if nv.Valid {
				s.Embedding = nv.Vector
			}
			if t, ok := ParseDBTime(rawMtime); ok {
				s.LastMtime = t
			}
			if t, ok := ParseDBTime(rawCreatedAt); ok {
				s.CreatedAt = t
			}
			if t, ok := ParseDBTime(rawUpdatedAt); ok {
				s.UpdatedAt = t
			}

			var sim float64
			if hasEmbedding && len(s.Embedding) == ExpectedEmbeddingDim {
				sim = cosineSimilarity(embedding, s.Embedding)
			}
			var textScore float64
			if hasText {
				if strings.Contains(strings.ToLower(s.Summary), qLower) {
					textScore = 1.0
				} else {
					words := strings.Fields(qLower)
					matchedWords := 0
					for _, w := range words {
						if strings.Contains(strings.ToLower(s.Summary), w) {
							matchedWords++
						}
					}
					if len(words) > 0 && matchedWords > 0 {
						textScore = float64(matchedWords) / float64(len(words))
					}
				}
			}

			var score float64
			if hasEmbedding && hasText {
				score = 0.50*sim + 0.50*textScore
			} else if hasEmbedding {
				score = sim
			} else if hasText {
				score = textScore
			}

			if score >= minScore {
				s.Score = score
				scored = append(scored, s)
			}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("sqlite iterate session summaries: %w", err)
		}

		sort.Slice(scored, func(i, j int) bool {
			return scored[i].Score > scored[j].Score
		})
		if len(scored) > limit {
			scored = scored[:limit]
		}
		return scored, nil
	}

	candidateLimit := limit * 3
	if candidateLimit < 30 {
		candidateLimit = 30
	}

	var query string
	var rows *sql.Rows

	if hasEmbedding && hasText {
		// Hybrid 50/50 RRF normalized with K=60.0
		query = `
		WITH vector_hits AS (
			SELECT session_id, ROW_NUMBER() OVER (ORDER BY embedding <=> $1) AS rank
			FROM session_summaries
			WHERE embedding IS NOT NULL
			ORDER BY embedding <=> $1
			LIMIT $2
		),
		text_hits AS (
			SELECT session_id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $3)) DESC) AS rank
			FROM session_summaries
			WHERE fts_tokens @@ websearch_to_tsquery('simple', $3)
			LIMIT $2
		)
		SELECT s.session_id, s.thread_id, s.summary, s.last_indexed_step, s.last_mtime,
		       s.summary_step_watermark, s.is_settled, s.created_at, s.updated_at,
		       ((COALESCE(0.50 / (60.0 + v.rank), 0.0) + COALESCE(0.50 / (60.0 + t.rank), 0.0)) * 60.0) AS score
		FROM vector_hits v
		FULL OUTER JOIN text_hits t ON v.session_id = t.session_id
		JOIN session_summaries s ON s.session_id = COALESCE(v.session_id, t.session_id)
		WHERE ((COALESCE(0.50 / (60.0 + v.rank), 0.0) + COALESCE(0.50 / (60.0 + t.rank), 0.0)) * 60.0) >= $4
		ORDER BY score DESC
		LIMIT $5;
		`
		vec := pgvector.NewVector(embedding)
		rows, err = database.QueryContext(queryCtx, query, vec, candidateLimit, queryText, minScore, limit)
	} else if hasEmbedding {
		// Dense vector only query
		query = `
		WITH candidates AS (
			SELECT session_id, thread_id, summary, last_indexed_step, last_mtime,
			       summary_step_watermark, is_settled, created_at, updated_at,
			       (1.0 - (embedding <=> $1)) AS similarity
			FROM session_summaries
			WHERE embedding IS NOT NULL
			ORDER BY embedding <=> $1
			LIMIT $2
		)
		SELECT session_id, thread_id, summary, last_indexed_step, last_mtime,
		       summary_step_watermark, is_settled, created_at, updated_at,
		       similarity AS score
		FROM candidates
		WHERE similarity >= $3
		ORDER BY similarity DESC
		LIMIT $4;
		`
		vec := pgvector.NewVector(embedding)
		rows, err = database.QueryContext(queryCtx, query, vec, candidateLimit, minScore, limit)
	} else {
		// Sparse FTS only query
		query = `
		WITH text_hits AS (
			SELECT session_id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $1)) DESC) AS rank
			FROM session_summaries
			WHERE fts_tokens @@ websearch_to_tsquery('simple', $1)
			LIMIT $2
		)
		SELECT s.session_id, s.thread_id, s.summary, s.last_indexed_step, s.last_mtime,
		       s.summary_step_watermark, s.is_settled, s.created_at, s.updated_at,
		       ((1.0 / (60.0 + t.rank)) * 60.0) AS score
		FROM text_hits t
		JOIN session_summaries s ON s.session_id = t.session_id
		WHERE ((1.0 / (60.0 + t.rank)) * 60.0) >= $3
		ORDER BY score DESC
		LIMIT $4;
		`
		rows, err = database.QueryContext(queryCtx, query, queryText, candidateLimit, minScore, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("search session summaries query failed: %w", err)
	}
	defer rows.Close()

	var summaries []SessionSummary
	for rows.Next() {
		var s SessionSummary
		var rawMtime, rawCreatedAt, rawUpdatedAt any
		if err := rows.Scan(
			&s.SessionID, &s.ThreadID, &s.Summary, &s.LastIndexedStep, &rawMtime,
			&s.SummaryStepWatermark, &s.IsSettled, &rawCreatedAt, &rawUpdatedAt, &s.Score,
		); err != nil {
			return nil, fmt.Errorf("scan session summary result: %w", err)
		}
		if t, ok := ParseDBTime(rawMtime); ok {
			s.LastMtime = t
		}
		if t, ok := ParseDBTime(rawCreatedAt); ok {
			s.CreatedAt = t
		}
		if t, ok := ParseDBTime(rawUpdatedAt); ok {
			s.UpdatedAt = t
		}
		summaries = append(summaries, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search session summaries results: %w", err)
	}
	return summaries, nil
}

// SearchTranscriptSteps searches micro execution steps using FTS with optional session and tool filters.
func SearchTranscriptSteps(database DBTX, queryText, sessionFilter, toolFilter string, limit int) ([]TranscriptStep, error) {
	return SearchTranscriptStepsWithContext(context.Background(), database, false, queryText, sessionFilter, toolFilter, limit)
}

// SearchTranscriptStepsWithContext searches micro execution steps with context.
func SearchTranscriptStepsWithContext(ctx context.Context, database DBTX, isPg bool, queryText, sessionFilter, toolFilter string, limit int) (results []TranscriptStep, err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("search_transcript_steps", status, time.Since(start))
	}()

	if database == nil {
		return nil, nil
	}

	queryText = strings.TrimSpace(queryText)
	sessionFilter = strings.TrimSpace(sessionFilter)
	toolFilter = strings.TrimSpace(toolFilter)

	if queryText == "" && sessionFilter == "" && toolFilter == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}

	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// SQLite in-memory fallback for hermetic unit testing
	if !isPg && !isPostgres(database) {
		query := `
		SELECT session_id, step_index, step_type, tool_name, content, created_at
		FROM transcript_steps
		WHERE ($1 = '' OR session_id = $1)
		  AND ($2 = '' OR tool_name = $2)
		ORDER BY created_at DESC;
		`
		rows, err := database.QueryContext(queryCtx, query, sessionFilter, toolFilter)
		if err != nil {
			return nil, fmt.Errorf("sqlite search transcript steps query: %w", err)
		}
		defer rows.Close()

		var matched []TranscriptStep
		qLower := strings.ToLower(queryText)

		for rows.Next() {
			var step TranscriptStep
			var rawCreatedAt any
			if err := rows.Scan(
				&step.SessionID, &step.StepIndex, &step.StepType, &step.ToolName, &step.Content, &rawCreatedAt,
			); err != nil {
				return nil, fmt.Errorf("sqlite scan transcript step: %w", err)
			}
			if t, ok := ParseDBTime(rawCreatedAt); ok {
				step.CreatedAt = t
			}

			isMatch := queryText == ""
			if !isMatch {
				if strings.Contains(strings.ToLower(step.Content), qLower) {
					isMatch = true
				} else {
					words := strings.Fields(qLower)
					allMatch := len(words) > 0
					for _, w := range words {
						if !strings.Contains(strings.ToLower(step.Content), w) {
							allMatch = false
							break
						}
					}
					isMatch = allMatch
				}
			}

			if isMatch {
				step.RankScore = 1.0
				matched = append(matched, step)
				if len(matched) >= limit {
					break
				}
			}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("sqlite iterate transcript steps: %w", err)
		}
		return matched, nil
	}

	var rows *sql.Rows
	if queryText != "" {
		query := `
		SELECT session_id, step_index, step_type, tool_name, content, created_at,
		       ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $1)) AS rank_score
		FROM transcript_steps
		WHERE fts_tokens @@ websearch_to_tsquery('simple', $1)
		  AND ($2 = '' OR session_id = $2)
		  AND ($3 = '' OR tool_name = $3)
		ORDER BY rank_score DESC, created_at DESC
		LIMIT $4;
		`
		rows, err = database.QueryContext(queryCtx, query, queryText, sessionFilter, toolFilter, limit)
	} else {
		query := `
		SELECT session_id, step_index, step_type, tool_name, content, created_at,
		       1.0 AS rank_score
		FROM transcript_steps
		WHERE ($1 = '' OR session_id = $1)
		  AND ($2 = '' OR tool_name = $2)
		ORDER BY created_at DESC
		LIMIT $3;
		`
		rows, err = database.QueryContext(queryCtx, query, sessionFilter, toolFilter, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("search transcript steps query failed: %w", err)
	}
	defer rows.Close()

	var steps []TranscriptStep
	for rows.Next() {
		var step TranscriptStep
		var rawCreatedAt any
		if err := rows.Scan(
			&step.SessionID, &step.StepIndex, &step.StepType, &step.ToolName,
			&step.Content, &rawCreatedAt, &step.RankScore,
		); err != nil {
			return nil, fmt.Errorf("scan transcript step result: %w", err)
		}
		if t, ok := ParseDBTime(rawCreatedAt); ok {
			step.CreatedAt = t
		}
		steps = append(steps, step)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search transcript steps results: %w", err)
	}
	return steps, nil
}
