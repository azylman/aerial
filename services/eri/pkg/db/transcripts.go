package db

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/metrics"
	pgvector "github.com/pgvector/pgvector-go"
)

const (
	ExpectedEmbeddingDim = 384
	DefaultDenseWeight   = 0.70
	DefaultSparseWeight  = 0.30
	DefaultRRFK          = 60.0
)

func optionalVectorArg(emb []float32) any {
	if len(emb) == ExpectedEmbeddingDim {
		return pgvector.NewVector(emb)
	}
	return nil
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

func recordTranscriptQuery(op string, start time.Time, errp *error) {
	status := "success"
	if errp != nil && *errp != nil {
		status = "error"
	}
	metrics.RecordDBQuery(op, status, time.Since(start))
}

func scanSessionSummaries(rows *sql.Rows) ([]SessionSummary, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var summaries []SessionSummary
	for rows.Next() {
		var sm SessionSummary
		var rawMtime, rawCreatedAt, rawUpdatedAt any
		if err := rows.Scan(&sm.SessionID, &sm.ThreadID, &sm.Summary, &sm.LastIndexedStep,
			&rawMtime, &sm.SummaryStepWatermark, &sm.IsSettled,
			&rawCreatedAt, &rawUpdatedAt, &sm.Score); err != nil {
			return nil, fmt.Errorf("scan session summary result: %w", err)
		}
		if t, ok := ParseDBTime(rawMtime); ok {
			sm.LastMtime = t
		}
		if t, ok := ParseDBTime(rawCreatedAt); ok {
			sm.CreatedAt = t
		}
		if t, ok := ParseDBTime(rawUpdatedAt); ok {
			sm.UpdatedAt = t
		}
		summaries = append(summaries, sm)
	}
	return summaries, rows.Err()
}

func scanTranscriptSteps(rows *sql.Rows) ([]TranscriptStep, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var steps []TranscriptStep
	for rows.Next() {
		var step TranscriptStep
		var rawCreatedAt any
		if err := rows.Scan(&step.SessionID, &step.StepIndex, &step.StepType, &step.ToolName,
			&step.Content, &rawCreatedAt, &step.RankScore); err != nil {
			return nil, fmt.Errorf("scan transcript step result: %w", err)
		}
		if t, ok := ParseDBTime(rawCreatedAt); ok {
			step.CreatedAt = t
		}
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

func (s *SQLStore) GetSessionSyncStates(ctx context.Context) (results map[string]SessionSyncState, err error) {
	defer recordTranscriptQuery("get_session_sync_states", time.Now(), &err)
	if err = s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(queryCtx, `SELECT session_id, last_mtime, last_indexed_step, is_settled FROM session_summaries`)
	if err != nil {
		return nil, fmt.Errorf("query session sync states: %w", err)
	}
	defer rows.Close()

	results = make(map[string]SessionSyncState)
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
		results[id] = SessionSyncState{SessionID: id, LastMtime: mtime, LastIndexedStep: lastStep, IsSettled: isSettled}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session sync states: %w", err)
	}
	return results, nil
}

func (s *SQLStore) UpsertSessionSummary(ctx context.Context, summary SessionSummary) (err error) {
	defer recordTranscriptQuery("upsert_session_summary", time.Now(), &err)
	if err = s.checkDB(); err != nil {
		return err
	}
	if strings.TrimSpace(summary.SessionID) == "" {
		return fmt.Errorf("session ID cannot be empty")
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	now := time.Now().UTC()
	createdAt := summary.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	var mtimeVal any
	if !summary.LastMtime.IsZero() {
		mtimeVal = summary.LastMtime
	}

	query := `INSERT INTO session_summaries (
		session_id, thread_id, summary, embedding, last_indexed_step, last_mtime,
		summary_step_watermark, is_settled, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT (session_id) DO UPDATE SET
		thread_id = CASE WHEN EXCLUDED.thread_id != '' THEN EXCLUDED.thread_id ELSE session_summaries.thread_id END,
		summary = CASE WHEN EXCLUDED.summary != '' THEN EXCLUDED.summary ELSE session_summaries.summary END,
		embedding = CASE WHEN EXCLUDED.embedding IS NOT NULL THEN EXCLUDED.embedding ELSE session_summaries.embedding END,
		last_indexed_step = CASE WHEN EXCLUDED.last_indexed_step >= 0 THEN EXCLUDED.last_indexed_step WHEN EXCLUDED.last_indexed_step = -2 THEN -1 ELSE session_summaries.last_indexed_step END,
		last_mtime = CASE WHEN EXCLUDED.last_mtime IS NOT NULL THEN EXCLUDED.last_mtime ELSE session_summaries.last_mtime END,
		summary_step_watermark = CASE WHEN EXCLUDED.summary_step_watermark >= 0 THEN EXCLUDED.summary_step_watermark ELSE session_summaries.summary_step_watermark END,
		is_settled = CASE WHEN EXCLUDED.is_settled THEN TRUE WHEN EXCLUDED.last_indexed_step > session_summaries.summary_step_watermark AND session_summaries.summary_step_watermark >= 0 THEN FALSE ELSE session_summaries.is_settled END,
		updated_at = EXCLUDED.updated_at;`

	_, err = s.db.ExecContext(queryCtx, query, summary.SessionID, summary.ThreadID, summary.Summary, optionalVectorArg(summary.Embedding), summary.LastIndexedStep, mtimeVal, summary.SummaryStepWatermark, summary.IsSettled, createdAt, now)
	if err != nil {
		return fmt.Errorf("upsert session summary failed: %w", err)
	}
	return nil
}

func (s *SQLStore) BatchInsertTranscriptSteps(ctx context.Context, steps []TranscriptStep) (err error) {
	defer recordTranscriptQuery("batch_insert_transcript_steps", time.Now(), &err)
	if err = s.checkDB(); err != nil {
		return err
	}
	if len(steps) == 0 {
		return nil
	}
	validSteps := make([]TranscriptStep, 0, len(steps))
	for _, st := range steps {
		if strings.TrimSpace(st.SessionID) != "" {
			validSteps = append(validSteps, st)
		}
	}
	if len(validSteps) == 0 {
		return nil
	}

	queryCtx, cancel := withTimeout(ctx, 30*time.Second)
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

		for j, st := range chunk {
			createdAt := st.CreatedAt
			if createdAt.IsZero() {
				createdAt = now
			}
			p := j * 6
			valStrings = append(valStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", p+1, p+2, p+3, p+4, p+5, p+6))
			valArgs = append(valArgs, st.SessionID, st.StepIndex, st.StepType, st.ToolName, st.Content, createdAt)
		}

		query := fmt.Sprintf(`INSERT INTO transcript_steps (session_id, step_index, step_type, tool_name, content, created_at)
		VALUES %s ON CONFLICT (session_id, step_index) DO NOTHING;`, strings.Join(valStrings, ", "))

		if _, err := s.db.ExecContext(queryCtx, query, valArgs...); err != nil {
			return fmt.Errorf("batch insert transcript steps chunk failed: %w", err)
		}
	}
	return nil
}

func (s *SQLStore) SearchSessionSummaries(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) (results []SessionSummary, err error) {
	defer recordTranscriptQuery("search_session_summaries", time.Now(), &err)
	if err = s.checkDB(); err != nil {
		return nil, err
	}
	hasEmbedding, queryText := len(embedding) == ExpectedEmbeddingDim, strings.TrimSpace(queryText)
	if !hasEmbedding && queryText == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	if minScore < 0 {
		minScore = 0.0
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	candidateLimit := limit * 3
	if candidateLimit < 30 {
		candidateLimit = 30
	}

	var query string
	var rows *sql.Rows

	if hasEmbedding && queryText != "" {
		query = `WITH vector_hits AS (
			SELECT session_id, ROW_NUMBER() OVER (ORDER BY embedding <=> $1) AS rank
			FROM session_summaries WHERE embedding IS NOT NULL ORDER BY embedding <=> $1 LIMIT $2
		), text_hits AS (
			SELECT session_id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $3)) DESC) AS rank
			FROM session_summaries WHERE fts_tokens @@ websearch_to_tsquery('simple', $3) LIMIT $2
		)
		SELECT s.session_id, s.thread_id, s.summary, s.last_indexed_step, s.last_mtime, 
			s.summary_step_watermark, s.is_settled, s.created_at, s.updated_at,
			((COALESCE(0.5 / (60.0 + v.rank), 0.0) + COALESCE(0.5 / (60.0 + t.rank), 0.0)) * 60.0) AS score
		FROM vector_hits v FULL OUTER JOIN text_hits t ON v.session_id = t.session_id
		JOIN session_summaries s ON s.session_id = COALESCE(v.session_id, t.session_id)
		WHERE ((COALESCE(0.5 / (60.0 + v.rank), 0.0) + COALESCE(0.5 / (60.0 + t.rank), 0.0)) * 60.0) >= $4
		ORDER BY score DESC LIMIT $5;`
		rows, err = s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), candidateLimit, queryText, minScore, limit)
	} else if hasEmbedding {
		query = `WITH candidates AS (
			SELECT session_id, thread_id, summary, last_indexed_step, last_mtime, 
				summary_step_watermark, is_settled, created_at, updated_at, (1.0 - (embedding <=> $1)) AS similarity
			FROM session_summaries WHERE embedding IS NOT NULL ORDER BY embedding <=> $1 LIMIT $2
		)
		SELECT session_id, thread_id, summary, last_indexed_step, last_mtime, 
			summary_step_watermark, is_settled, created_at, updated_at, similarity AS score
		FROM candidates WHERE similarity >= $3 ORDER BY similarity DESC LIMIT $4;`
		rows, err = s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), candidateLimit, minScore, limit)
	} else {
		query = `WITH text_hits AS (
			SELECT session_id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $1)) DESC) AS rank
			FROM session_summaries WHERE fts_tokens @@ websearch_to_tsquery('simple', $1) LIMIT $2
		)
		SELECT s.session_id, s.thread_id, s.summary, s.last_indexed_step, s.last_mtime, 
			s.summary_step_watermark, s.is_settled, s.created_at, s.updated_at, (1.0 / (60.0 + t.rank) * 60.0) AS score
		FROM text_hits t JOIN session_summaries s ON s.session_id = t.session_id
		WHERE (1.0 / (60.0 + t.rank) * 60.0) >= $3 ORDER BY score DESC LIMIT $4;`
		rows, err = s.db.QueryContext(queryCtx, query, queryText, candidateLimit, minScore, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("search session summaries query failed: %w", err)
	}
	return scanSessionSummaries(rows)
}

func (s *SQLStore) SearchTranscriptSteps(ctx context.Context, queryText, sessionFilter, toolFilter string, limit int) (results []TranscriptStep, err error) {
	defer recordTranscriptQuery("search_transcript_steps", time.Now(), &err)
	if err = s.checkDB(); err != nil {
		return nil, err
	}
	queryText, sessionFilter, toolFilter = strings.TrimSpace(queryText), strings.TrimSpace(sessionFilter), strings.TrimSpace(toolFilter)
	if queryText == "" && sessionFilter == "" && toolFilter == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	var rows *sql.Rows
	if queryText != "" {
		query := `SELECT session_id, step_index, step_type, tool_name, content, created_at,
		       ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $1)) AS rank_score
		FROM transcript_steps
		WHERE fts_tokens @@ websearch_to_tsquery('simple', $1) AND ($2 = '' OR session_id = $2) AND ($3 = '' OR tool_name = $3)
		ORDER BY rank_score DESC, created_at DESC LIMIT $4;`
		rows, err = s.db.QueryContext(queryCtx, query, queryText, sessionFilter, toolFilter, limit)
	} else {
		query := `SELECT session_id, step_index, step_type, tool_name, content, created_at, 1.0 AS rank_score
		FROM transcript_steps WHERE ($1 = '' OR session_id = $1) AND ($2 = '' OR tool_name = $2)
		ORDER BY created_at DESC LIMIT $3;`
		rows, err = s.db.QueryContext(queryCtx, query, sessionFilter, toolFilter, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("search transcript steps query failed: %w", err)
	}
	return scanTranscriptSteps(rows)
}

func (s *SQLStore) GetTranscriptStepRange(ctx context.Context, sessionID string, startStep, endStep int) (results []TranscriptStep, err error) {
	defer recordTranscriptQuery("get_transcript_step_range", time.Now(), &err)
	if err = s.checkDB(); err != nil {
		return nil, err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, nil
	}
	if startStep < 0 {
		startStep = 0
	}
	if endStep < startStep {
		endStep = startStep
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `SELECT session_id, step_index, step_type, tool_name, content, created_at, 1.0 AS rank_score
FROM transcript_steps
WHERE session_id = $1 AND step_index BETWEEN $2 AND $3
ORDER BY step_index ASC;`
	rows, err := s.db.QueryContext(queryCtx, query, sessionID, startStep, endStep)
	if err != nil {
		return nil, fmt.Errorf("get transcript step range query failed: %w", err)
	}
	return scanTranscriptSteps(rows)
}
