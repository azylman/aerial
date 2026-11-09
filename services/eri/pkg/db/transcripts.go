package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
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

// MCPToolFamily is the bare tool filter that matches every normalized mcp:<server> tool kind.
const MCPToolFamily = "mcp"

func optionalVectorArg(emb []float32) any {
	if len(emb) == ExpectedEmbeddingDim {
		return pgvector.NewVector(emb)
	}
	return nil
}

func recordTranscriptQuery(op string, start time.Time, errp *error) {
	status := "success"
	if errp != nil && *errp != nil {
		status = "error"
	}
	metrics.RecordDBQuery(op, status, time.Since(start))
}

func scanRows[T any](rows *sql.Rows, scanOne func(*sql.Rows) (T, error)) ([]T, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var items []T
	for rows.Next() {
		item, err := scanOne(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func scanSessionSummaries(rows *sql.Rows) ([]SessionSummary, error) {
	return scanRows(rows, func(r *sql.Rows) (SessionSummary, error) {
		var sm SessionSummary
		var rawMtime, rawCreatedAt, rawUpdatedAt any
		if err := r.Scan(&sm.Stack, &sm.SessionID, &sm.Engine, &sm.ThreadID, &sm.Summary, &sm.LastIndexedStep,
			&rawMtime, &sm.SummaryStepWatermark, &sm.IsSettled,
			&rawCreatedAt, &rawUpdatedAt, &sm.Score); err != nil {
			return SessionSummary{}, fmt.Errorf("scan session summary result: %w", err)
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
		return sm, nil
	})
}

func scanTranscriptSteps(rows *sql.Rows) ([]TranscriptStep, error) {
	return scanRows(rows, func(r *sql.Rows) (TranscriptStep, error) {
		var step TranscriptStep
		var rawCreatedAt any
		if err := r.Scan(&step.Stack, &step.SessionID, &step.Engine, &step.StepIndex, &step.StepType, &step.ToolName,
			&step.ToolKind, &step.Content, &rawCreatedAt, &step.RankScore); err != nil {
			return TranscriptStep{}, fmt.Errorf("scan transcript step result: %w", err)
		}
		if t, ok := ParseDBTime(rawCreatedAt); ok {
			step.CreatedAt = t
		}
		return step, nil
	})
}

// SQLTranscriptStore persists transcript rows into a (possibly shared) transcript index.
// Writes are always scoped to stack; reads take an explicit stack filter where empty means all stacks.
type SQLTranscriptStore struct {
	db    DBTX
	stack string
}

// NewSQLTranscriptStore binds a transcript store to database, writing rows labeled with stack.
// An empty stack yields a read-only store.
func NewSQLTranscriptStore(database DBTX, stack string) *SQLTranscriptStore {
	return &SQLTranscriptStore{db: database, stack: strings.TrimSpace(stack)}
}

func (s *SQLTranscriptStore) checkDB() error {
	if s == nil || isDBTXNil(s.db) {
		return ErrNilDatabase
	}
	return nil
}

// checkWriter guards stack-scoped writes: a read-only store (empty stack) can search but never write.
func (s *SQLTranscriptStore) checkWriter() error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if s.stack == "" {
		return fmt.Errorf("transcript store stack cannot be empty for stack-scoped writes")
	}
	return nil
}

func (s *SQLTranscriptStore) GetSessionSyncStates(ctx context.Context) (results map[string]SessionSyncState, err error) {
	defer recordTranscriptQuery("get_session_sync_states", time.Now(), &err)
	if err := s.checkWriter(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(queryCtx, `SELECT session_id, engine, last_mtime, last_indexed_step, is_settled FROM session_summaries WHERE stack = $1`, s.stack)
	if err != nil {
		return nil, fmt.Errorf("query session sync states: %w", err)
	}
	defer rows.Close()

	results = make(map[string]SessionSyncState)
	for rows.Next() {
		var (
			id, engine string
			rawMtime   any
			lastStep   int
			isSettled  bool
		)
		if err := rows.Scan(&id, &engine, &rawMtime, &lastStep, &isSettled); err != nil {
			return nil, fmt.Errorf("scan session sync state: %w", err)
		}
		var mtime time.Time
		if t, ok := ParseDBTime(rawMtime); ok {
			mtime = t
		}
		results[id] = SessionSyncState{SessionID: id, Engine: engine, LastMtime: mtime, LastIndexedStep: lastStep, IsSettled: isSettled}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session sync states: %w", err)
	}
	return results, nil
}

func (s *SQLTranscriptStore) UpsertSessionSummary(ctx context.Context, summary SessionSummary) (err error) {
	defer recordTranscriptQuery("upsert_session_summary", time.Now(), &err)
	if err := s.checkWriter(); err != nil {
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
		stack, session_id, engine, thread_id, summary, embedding, last_indexed_step, last_mtime,
		summary_step_watermark, is_settled, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	ON CONFLICT (stack, session_id) DO UPDATE SET
		engine = CASE WHEN EXCLUDED.engine != '' THEN EXCLUDED.engine ELSE session_summaries.engine END,
		thread_id = CASE WHEN EXCLUDED.thread_id != '' THEN EXCLUDED.thread_id ELSE session_summaries.thread_id END,
		summary = CASE WHEN EXCLUDED.summary != '' THEN EXCLUDED.summary ELSE session_summaries.summary END,
		embedding = CASE WHEN EXCLUDED.embedding IS NOT NULL THEN EXCLUDED.embedding ELSE session_summaries.embedding END,
		last_indexed_step = CASE WHEN EXCLUDED.last_indexed_step >= 0 THEN EXCLUDED.last_indexed_step WHEN EXCLUDED.last_indexed_step = -2 THEN -1 ELSE session_summaries.last_indexed_step END,
		last_mtime = CASE WHEN EXCLUDED.last_mtime IS NOT NULL THEN EXCLUDED.last_mtime ELSE session_summaries.last_mtime END,
		summary_step_watermark = CASE WHEN EXCLUDED.summary_step_watermark >= 0 THEN EXCLUDED.summary_step_watermark ELSE session_summaries.summary_step_watermark END,
		is_settled = CASE WHEN EXCLUDED.is_settled THEN TRUE WHEN EXCLUDED.last_indexed_step > session_summaries.summary_step_watermark AND session_summaries.summary_step_watermark >= 0 THEN FALSE ELSE session_summaries.is_settled END,
		updated_at = EXCLUDED.updated_at;`

	_, err = s.db.ExecContext(queryCtx, query, s.stack, summary.SessionID, summary.Engine, summary.ThreadID, sanitizeTranscriptText(summary.Summary), optionalVectorArg(summary.Embedding), summary.LastIndexedStep, mtimeVal, summary.SummaryStepWatermark, summary.IsSettled, createdAt, now)
	if err != nil {
		return fmt.Errorf("upsert session summary failed: %w", err)
	}
	return nil
}

// SetSessionEngine stamps the engine on an existing local-stack session without touching its sync state.
func (s *SQLTranscriptStore) SetSessionEngine(ctx context.Context, sessionID, engine string) (err error) {
	defer recordTranscriptQuery("set_session_engine", time.Now(), &err)
	if err := s.checkWriter(); err != nil {
		return err
	}
	sessionID, engine = strings.TrimSpace(sessionID), strings.TrimSpace(engine)
	if sessionID == "" || engine == "" {
		return fmt.Errorf("session ID and engine cannot be empty")
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := s.db.ExecContext(queryCtx, `UPDATE session_summaries SET engine = $1 WHERE stack = $2 AND session_id = $3`, engine, s.stack, sessionID); err != nil {
		return fmt.Errorf("set session engine failed: %w", err)
	}
	return nil
}

func (s *SQLTranscriptStore) BatchInsertTranscriptSteps(ctx context.Context, steps []TranscriptStep) (err error) {
	defer recordTranscriptQuery("batch_insert_transcript_steps", time.Now(), &err)
	if err := s.checkWriter(); err != nil {
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

	const batchSize, cols = 100, 9
	for i := 0; i < len(validSteps); i += batchSize {
		end := min(i+batchSize, len(validSteps))
		chunk := validSteps[i:end]
		valStrings := make([]string, 0, len(chunk))
		valArgs := make([]any, 0, len(chunk)*cols)
		now := time.Now().UTC()

		for j, st := range chunk {
			createdAt := st.CreatedAt
			if createdAt.IsZero() {
				createdAt = now
			}
			placeholders := make([]string, cols)
			for k := range placeholders {
				placeholders[k] = fmt.Sprintf("$%d", j*cols+k+1)
			}
			valStrings = append(valStrings, "("+strings.Join(placeholders, ", ")+")")
			valArgs = append(valArgs, s.stack, st.SessionID, st.StepIndex, st.StepType, st.ToolName, st.ToolKind, st.ToolCallID, sanitizeTranscriptText(st.Content), createdAt)
		}

		query := fmt.Sprintf(`INSERT INTO transcript_steps (stack, session_id, step_index, step_type, tool_name, tool_kind, tool_call_id, content, created_at)
		VALUES %s ON CONFLICT (stack, session_id, step_index) DO NOTHING;`, strings.Join(valStrings, ", "))

		if _, err := s.db.ExecContext(queryCtx, query, valArgs...); err != nil {
			return fmt.Errorf("batch insert transcript steps chunk failed: %w", err)
		}
	}
	return nil
}

const (
	// toolKindBackfillBatchSize bounds the rows rewritten by one tool_kind backfill UPDATE so each
	// statement commits quickly despite the STORED fts_tokens recompute and index churn per row.
	toolKindBackfillBatchSize = 2000
	// toolKindBackfillBudget caps the wall time one BackfillToolKinds call spends issuing batches.
	toolKindBackfillBudget = 20 * time.Second
	// toolKindBackfillStatementTimeout bounds each individual discovery query or batch UPDATE.
	toolKindBackfillStatementTimeout = 10 * time.Second
)

// BackfillToolKinds fills tool_kind for local-stack steps ingested before normalization existed,
// committing bounded batches per distinct unmapped tool_name until the backlog drains or the
// per-call budget elapses. It returns the number of rows updated; partial progress is not an error.
func (s *SQLTranscriptStore) BackfillToolKinds(ctx context.Context, mapper func(string) string) (updated int64, err error) {
	return s.backfillToolKinds(ctx, mapper, toolKindBackfillBatchSize, toolKindBackfillBudget)
}

func (s *SQLTranscriptStore) backfillToolKinds(ctx context.Context, mapper func(string) string, batchSize int, budget time.Duration) (updated int64, err error) {
	start := time.Now()
	defer recordTranscriptQuery("backfill_tool_kinds", start, &err)
	if err := s.checkWriter(); err != nil {
		return 0, err
	}
	if mapper == nil {
		return 0, fmt.Errorf("tool kind mapper cannot be nil")
	}
	if batchSize <= 0 {
		return 0, fmt.Errorf("tool kind backfill batch size must be positive")
	}
	names, err := s.unmappedToolNames(ctx)
	if err != nil {
		return 0, err
	}
	deadline := start.Add(budget)
	for _, name := range names {
		kind := mapper(name)
		if kind == "" {
			continue
		}
		for {
			if !time.Now().Before(deadline) {
				return updated, nil
			}
			n, err := s.backfillToolKindBatch(ctx, kind, name, batchSize)
			if err != nil {
				return updated, fmt.Errorf("backfill tool kind for %q: %w", name, err)
			}
			updated += n
			if n < int64(batchSize) {
				break
			}
		}
	}
	return updated, nil
}

// unmappedToolNames lists the distinct local-stack tool names whose steps still lack a tool_kind.
func (s *SQLTranscriptStore) unmappedToolNames(ctx context.Context) ([]string, error) {
	queryCtx, cancel := withTimeout(ctx, toolKindBackfillStatementTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(queryCtx, `SELECT DISTINCT tool_name FROM transcript_steps WHERE stack = $1 AND tool_kind = '' AND tool_name <> ''`, s.stack)
	if err != nil {
		return nil, fmt.Errorf("query unmapped tool names: %w", err)
	}
	names, err := scanNonEmptyStrings(rows)
	if err != nil {
		return nil, fmt.Errorf("scan unmapped tool names: %w", err)
	}
	return names, nil
}

// backfillToolKindBatch maps at most limit unmapped local-stack steps named name to kind in one
// self-committing statement, returning the rows it rewrote.
func (s *SQLTranscriptStore) backfillToolKindBatch(ctx context.Context, kind, name string, limit int) (int64, error) {
	execCtx, cancel := withTimeout(ctx, toolKindBackfillStatementTimeout)
	defer cancel()
	res, err := s.db.ExecContext(execCtx, `UPDATE transcript_steps SET tool_kind = $1 WHERE (stack, session_id, step_index) IN (SELECT stack, session_id, step_index FROM transcript_steps WHERE stack = $2 AND tool_kind = '' AND tool_name = $3 LIMIT $4)`, kind, s.stack, name, limit)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return n, nil
}

// sessionEngineBatchSize bounds the session IDs bound into one engine backfill UPDATE.
const sessionEngineBatchSize = 500

// BackfillSessionEngines labels local-stack sessions whose engine is still empty, typically
// older sessions whose raw logs are gone, using infer over each session's distinct tool names.
// It returns the number of sessions updated.
func (s *SQLTranscriptStore) BackfillSessionEngines(ctx context.Context, infer func(toolNames []string) string) (updated int64, err error) {
	defer recordTranscriptQuery("backfill_session_engines", time.Now(), &err)
	if err := s.checkWriter(); err != nil {
		return 0, err
	}
	if infer == nil {
		return 0, fmt.Errorf("engine inference func cannot be nil")
	}
	queryCtx, cancel := withTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(queryCtx, `SELECT DISTINCT s.session_id, t.tool_name FROM session_summaries s
		JOIN transcript_steps t ON t.stack = s.stack AND t.session_id = s.session_id
		WHERE s.stack = $1 AND s.engine = '' AND t.tool_name <> ''
		ORDER BY s.session_id, t.tool_name`, s.stack)
	if err != nil {
		return 0, fmt.Errorf("query unlabeled session tool names: %w", err)
	}
	toolNames, order, err := scanSessionToolNames(rows)
	if err != nil {
		return 0, err
	}
	byEngine := make(map[string][]string)
	var engines []string
	for _, sessionID := range order {
		engine := infer(toolNames[sessionID])
		if engine == "" {
			continue
		}
		if _, seen := byEngine[engine]; !seen {
			engines = append(engines, engine)
		}
		byEngine[engine] = append(byEngine[engine], sessionID)
	}
	for _, engine := range engines {
		for batch := range slices.Chunk(byEngine[engine], sessionEngineBatchSize) {
			args := []any{engine, s.stack}
			placeholders := make([]string, len(batch))
			for i, sessionID := range batch {
				args = append(args, sessionID)
				placeholders[i] = fmt.Sprintf("$%d", len(args))
			}
			query := `UPDATE session_summaries SET engine = $1 WHERE stack = $2 AND engine = '' AND session_id IN (` + strings.Join(placeholders, ", ") + `)`
			res, err := s.db.ExecContext(queryCtx, query, args...)
			if err != nil {
				return updated, fmt.Errorf("backfill engine %q: %w", engine, err)
			}
			if n, err := res.RowsAffected(); err == nil {
				updated += n
			}
		}
	}
	return updated, nil
}

// scanSessionToolNames groups (session_id, tool_name) rows by session, preserving first-seen order.
func scanSessionToolNames(rows *sql.Rows) (map[string][]string, []string, error) {
	defer rows.Close()
	toolNames := make(map[string][]string)
	var order []string
	for rows.Next() {
		var sessionID, toolName string
		if err := rows.Scan(&sessionID, &toolName); err != nil {
			return nil, nil, fmt.Errorf("scan unlabeled session tool name: %w", err)
		}
		if _, seen := toolNames[sessionID]; !seen {
			order = append(order, sessionID)
		}
		toolNames[sessionID] = append(toolNames[sessionID], toolName)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate unlabeled session tool names: %w", err)
	}
	return toolNames, order, nil
}

// PurgeMigratedLocalTranscripts is a no-op for a single-database store: its database is the index, so
// it holds no stranded pre-migration rows. Routed stores reclaim those across the primary and shared DBs.
func (s *SQLTranscriptStore) PurgeMigratedLocalTranscripts(ctx context.Context) (int64, error) {
	return 0, s.checkDB()
}

// sessionIDsAfter pages the store's own-stack session IDs strictly after the keyset cursor after,
// in ascending order, returning at most limit IDs.
func (s *SQLTranscriptStore) sessionIDsAfter(ctx context.Context, after string, limit int) ([]string, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, toolKindBackfillStatementTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(queryCtx, `SELECT session_id FROM session_summaries WHERE stack = $1 AND session_id > $2 ORDER BY session_id LIMIT $3`, s.stack, after, limit)
	if err != nil {
		return nil, fmt.Errorf("query session ids: %w", err)
	}
	ids, err := scanNonEmptyStrings(rows)
	if err != nil {
		return nil, fmt.Errorf("scan session ids: %w", err)
	}
	return ids, nil
}

// existingSessionIDs returns the subset of ids that have an own-stack session summary in this store.
func (s *SQLTranscriptStore) existingSessionIDs(ctx context.Context, ids []string) ([]string, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	queryCtx, cancel := withTimeout(ctx, toolKindBackfillStatementTimeout)
	defer cancel()
	in, args := stackSessionInList(s.stack, ids)
	rows, err := s.db.QueryContext(queryCtx, `SELECT session_id FROM session_summaries WHERE stack = $1 AND session_id IN (`+in+`)`, args...)
	if err != nil {
		return nil, fmt.Errorf("query existing session ids: %w", err)
	}
	found, err := scanNonEmptyStrings(rows)
	if err != nil {
		return nil, fmt.Errorf("scan existing session ids: %w", err)
	}
	return found, nil
}

// deleteSessions removes the own-stack sessions in ids along with their steps, deleting steps
// explicitly rather than relying on the FK cascade, and returns the session rows deleted.
func (s *SQLTranscriptStore) deleteSessions(ctx context.Context, ids []string) (int64, error) {
	if err := s.checkWriter(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	execCtx, cancel := withTimeout(ctx, toolKindBackfillStatementTimeout)
	defer cancel()
	in, args := stackSessionInList(s.stack, ids)
	if _, err := s.db.ExecContext(execCtx, `DELETE FROM transcript_steps WHERE stack = $1 AND session_id IN (`+in+`)`, args...); err != nil {
		return 0, fmt.Errorf("delete transcript steps: %w", err)
	}
	res, err := s.db.ExecContext(execCtx, `DELETE FROM session_summaries WHERE stack = $1 AND session_id IN (`+in+`)`, args...)
	if err != nil {
		return 0, fmt.Errorf("delete session summaries: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return n, nil
}

// stackSessionInList binds stack as $1 followed by ids as $2..$n, returning the IN-list placeholders.
func stackSessionInList(stack string, ids []string) (string, []any) {
	args := make([]any, 0, len(ids)+1)
	args = append(args, stack)
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		args = append(args, id)
		placeholders[i] = fmt.Sprintf("$%d", len(args))
	}
	return strings.Join(placeholders, ", "), args
}

// sanitizeTranscriptText strips NUL bytes and replaces invalid UTF-8 so Postgres text columns
// accept the value; one dirty step must not reject its whole insert batch.
func sanitizeTranscriptText(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "\uFFFD")
}

const sessionSummaryColumns = `s.stack, s.session_id, s.engine, s.thread_id, s.summary, s.last_indexed_step, s.last_mtime,
			s.summary_step_watermark, s.is_settled, s.created_at, s.updated_at`

func (s *SQLTranscriptStore) SearchSessionSummaries(ctx context.Context, stack string, embedding []float32, queryText string, limit int, minScore float64) (results []SessionSummary, err error) {
	defer recordTranscriptQuery("search_session_summaries", time.Now(), &err)
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	stack = strings.TrimSpace(stack)
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

	candidateLimit := max(limit*3, 30)

	// $1 is always the stack filter (empty matches every stack) so all variants share one predicate.
	const stackPred = "($1 = '' OR ss.stack = $1)"
	var query string
	var args []any

	switch {
	case hasEmbedding && queryText != "":
		query = `WITH vector_hits AS (
			SELECT ss.stack, ss.session_id, ROW_NUMBER() OVER (ORDER BY ss.embedding <=> $2) AS rank
			FROM session_summaries ss WHERE ss.embedding IS NOT NULL AND ` + stackPred + ` ORDER BY ss.embedding <=> $2 LIMIT $3
		), text_hits AS (
			SELECT ss.stack, ss.session_id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(ss.fts_tokens, websearch_to_tsquery('simple', $4)) DESC) AS rank
			FROM session_summaries ss WHERE ss.fts_tokens @@ websearch_to_tsquery('simple', $4) AND ` + stackPred + ` LIMIT $3
		)
		SELECT ` + sessionSummaryColumns + `,
			((COALESCE(0.5 / (60.0 + v.rank), 0.0) + COALESCE(0.5 / (60.0 + t.rank), 0.0)) * 60.0) AS score
		FROM vector_hits v FULL OUTER JOIN text_hits t ON v.stack = t.stack AND v.session_id = t.session_id
		JOIN session_summaries s ON s.stack = COALESCE(v.stack, t.stack) AND s.session_id = COALESCE(v.session_id, t.session_id)
		WHERE ((COALESCE(0.5 / (60.0 + v.rank), 0.0) + COALESCE(0.5 / (60.0 + t.rank), 0.0)) * 60.0) >= $5
		ORDER BY score DESC LIMIT $6;`
		args = []any{stack, pgvector.NewVector(embedding), candidateLimit, queryText, minScore, limit}
	case hasEmbedding:
		query = `WITH candidates AS (
			SELECT ss.stack, ss.session_id, (1.0 - (ss.embedding <=> $2)) AS similarity
			FROM session_summaries ss WHERE ss.embedding IS NOT NULL AND ` + stackPred + ` ORDER BY ss.embedding <=> $2 LIMIT $3
		)
		SELECT ` + sessionSummaryColumns + `, c.similarity AS score
		FROM candidates c JOIN session_summaries s ON s.stack = c.stack AND s.session_id = c.session_id
		WHERE c.similarity >= $4 ORDER BY c.similarity DESC LIMIT $5;`
		args = []any{stack, pgvector.NewVector(embedding), candidateLimit, minScore, limit}
	default:
		query = `WITH text_hits AS (
			SELECT ss.stack, ss.session_id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(ss.fts_tokens, websearch_to_tsquery('simple', $2)) DESC) AS rank
			FROM session_summaries ss WHERE ss.fts_tokens @@ websearch_to_tsquery('simple', $2) AND ` + stackPred + ` LIMIT $3
		)
		SELECT ` + sessionSummaryColumns + `, (1.0 / (60.0 + t.rank) * 60.0) AS score
		FROM text_hits t JOIN session_summaries s ON s.stack = t.stack AND s.session_id = t.session_id
		WHERE (1.0 / (60.0 + t.rank) * 60.0) >= $4 ORDER BY score DESC LIMIT $5;`
		args = []any{stack, queryText, candidateLimit, minScore, limit}
	}

	return s.querySessionSummaries(queryCtx, hasEmbedding && stack != "", query, args...)
}

// querySessionSummaries runs a summary search. When exactScan is set on PostgreSQL it disables index
// scans for the transaction so the HNSW candidate cap cannot starve a stack-filtered vector query.
func (s *SQLTranscriptStore) querySessionSummaries(ctx context.Context, exactScan bool, query string, args ...any) ([]SessionSummary, error) {
	sqlDB, ok := s.db.(*sql.DB)
	if !exactScan || !ok || !isPostgresDriver(sqlDB.Driver()) {
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("search session summaries query failed: %w", err)
		}
		return scanSessionSummaries(rows)
	}
	tx, err := sqlDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin exact-scan search tx: %w", err)
	}
	results, err := queryExactSessionSummaries(ctx, tx, query, args...)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit exact-scan search tx: %w", err)
	}
	return results, nil
}

func queryExactSessionSummaries(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]SessionSummary, error) {
	if _, err := tx.ExecContext(ctx, "SET LOCAL enable_indexscan = off"); err != nil {
		return nil, fmt.Errorf("disable index scan for exact search: %w", err)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search session summaries query failed: %w", err)
	}
	return scanSessionSummaries(rows)
}

const transcriptStepFrom = `FROM transcript_steps t JOIN session_summaries s ON s.stack = t.stack AND s.session_id = t.session_id`

func transcriptStepSelect(rankExpr string) string {
	return "SELECT t.stack, t.session_id, s.engine, t.step_index, t.step_type, t.tool_name, t.tool_kind, t.content, t.created_at, " + rankExpr + " AS rank_score " + transcriptStepFrom
}

func buildSearchTranscriptStepsQuery(stack, queryText, sessionFilter, toolFilter string, limit int, sinceTime time.Time) (string, []any) {
	var whereClauses []string
	var args []any

	if queryText != "" {
		args = append(args, queryText)
		whereClauses = append(whereClauses, fmt.Sprintf("t.fts_tokens @@ websearch_to_tsquery('simple', $%d)", len(args)))
	}

	if stack != "" {
		args = append(args, stack)
		whereClauses = append(whereClauses, fmt.Sprintf("t.stack = $%d", len(args)))
	}

	if sessionFilter != "" {
		var sessions []string
		for raw := range strings.SplitSeq(sessionFilter, ",") {
			if trimmed := strings.TrimSpace(raw); trimmed != "" {
				sessions = append(sessions, trimmed)
			}
		}
		if len(sessions) == 1 {
			args = append(args, sessions[0])
			whereClauses = append(whereClauses, fmt.Sprintf("t.session_id = $%d", len(args)))
		} else if len(sessions) > 1 {
			placeholders := make([]string, len(sessions))
			for i, sid := range sessions {
				args = append(args, sid)
				placeholders[i] = fmt.Sprintf("$%d", len(args))
			}
			whereClauses = append(whereClauses, fmt.Sprintf("t.session_id IN (%s)", strings.Join(placeholders, ", ")))
		}
	}

	if toolFilter == MCPToolFamily {
		// Bare "mcp" is a family filter over every mcp:<server> kind.
		args = append(args, MCPToolFamily+":%")
		whereClauses = append(whereClauses, fmt.Sprintf("t.tool_kind LIKE $%d", len(args)))
	} else if toolFilter != "" {
		// Normalized kinds (shell, mcp:<server>, ...) and engine-native names never collide, so one
		// value matches either column: "shell" finds Bash and run_command, "Bash" finds only Bash.
		args = append(args, toolFilter)
		whereClauses = append(whereClauses, fmt.Sprintf("(t.tool_kind = $%d OR t.tool_name = $%d)", len(args), len(args)))
	}

	if !sinceTime.IsZero() {
		args = append(args, sinceTime)
		whereClauses = append(whereClauses, fmt.Sprintf("t.created_at > $%d", len(args)))
	}

	var selectClause string
	var orderClause string

	if queryText != "" {
		selectClause = transcriptStepSelect("ts_rank_cd(t.fts_tokens, websearch_to_tsquery('simple', $1))")
		orderClause = "ORDER BY rank_score DESC, t.created_at DESC"
	} else {
		selectClause = transcriptStepSelect("1.0")
		if !sinceTime.IsZero() {
			orderClause = "ORDER BY t.created_at ASC, t.step_index ASC"
		} else {
			orderClause = "ORDER BY t.created_at DESC"
		}
	}

	query := selectClause
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}
	args = append(args, limit)
	query += fmt.Sprintf(" %s LIMIT $%d;", orderClause, len(args))
	return query, args
}

func (s *SQLTranscriptStore) SearchTranscriptSteps(ctx context.Context, stack, queryText, sessionFilter, toolFilter string, limit int, since ...time.Time) (results []TranscriptStep, err error) {
	defer recordTranscriptQuery("search_transcript_steps", time.Now(), &err)
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	stack = strings.TrimSpace(stack)
	queryText, sessionFilter, toolFilter = strings.TrimSpace(queryText), strings.TrimSpace(sessionFilter), strings.TrimSpace(toolFilter)
	var sinceTime time.Time
	if len(since) > 0 {
		sinceTime = since[0]
	}
	if queryText == "" && sessionFilter == "" && toolFilter == "" && sinceTime.IsZero() {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	query, args := buildSearchTranscriptStepsQuery(stack, queryText, sessionFilter, toolFilter, limit, sinceTime)

	rows, err := s.db.QueryContext(queryCtx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search transcript steps query failed: %w", err)
	}
	return scanTranscriptSteps(rows)
}

func (s *SQLTranscriptStore) GetTranscriptStepRange(ctx context.Context, stack, sessionID string, startStep, endStep int) (results []TranscriptStep, err error) {
	defer recordTranscriptQuery("get_transcript_step_range", time.Now(), &err)
	if err := s.checkDB(); err != nil {
		return nil, err
	}
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
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	query := transcriptStepSelect("1.0") + `
WHERE t.session_id = $1 AND t.step_index BETWEEN $2 AND $3 AND ($4 = '' OR t.stack = $4)
ORDER BY t.stack ASC, t.step_index ASC;`
	rows, err := s.db.QueryContext(queryCtx, query, sessionID, startStep, endStep, stack)
	if err != nil {
		return nil, fmt.Errorf("get transcript step range query failed: %w", err)
	}
	return scanTranscriptSteps(rows)
}

// GetTranscriptStats counts indexed sessions and steps per stack, ordered by stack. An empty stack
// covers every stack in the shared index.
func (s *SQLTranscriptStore) GetTranscriptStats(ctx context.Context, stack string) (results []TranscriptStackStats, err error) {
	defer recordTranscriptQuery("get_transcript_stats", time.Now(), &err)
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(queryCtx, `WITH ss AS (SELECT stack, COUNT(*) AS sessions FROM session_summaries WHERE $1 = '' OR stack = $1 GROUP BY stack),
ts AS (SELECT stack, COUNT(*) AS steps FROM transcript_steps WHERE $1 = '' OR stack = $1 GROUP BY stack)
SELECT ss.stack, ss.sessions, COALESCE(ts.steps, 0) FROM ss LEFT JOIN ts ON ts.stack = ss.stack ORDER BY ss.stack`, strings.TrimSpace(stack))
	if err != nil {
		return nil, fmt.Errorf("query transcript stats: %w", err)
	}
	return scanRows(rows, func(r *sql.Rows) (TranscriptStackStats, error) {
		var st TranscriptStackStats
		if err := r.Scan(&st.Stack, &st.Sessions, &st.Steps); err != nil {
			return TranscriptStackStats{}, fmt.Errorf("scan transcript stats: %w", err)
		}
		return st, nil
	})
}
