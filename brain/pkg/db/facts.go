package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	pgvector "github.com/pgvector/pgvector-go"
)

const (
	ExpectedEmbeddingDim = 384
	DefaultDenseWeight   = 0.70
	DefaultSparseWeight  = 0.30
	DefaultRRFK          = 60.0
)

type Fact struct {
	ID               int64     `json:"id"`
	Category         string    `json:"category"`
	FactText         string    `json:"fact_text"`
	Importance       float64   `json:"importance"`
	CreatedAt        time.Time `json:"created_at"`
	LastReinforcedAt time.Time `json:"last_reinforced_at"`
	LastDecayedAt    time.Time `json:"last_decayed_at"`
	ReinforceCount   int       `json:"reinforce_count"`
}

type FactWithEmbedding struct {
	Fact      Fact
	Embedding []float32
}

func optionalVectorArg(emb []float32) any {
	if len(emb) == ExpectedEmbeddingDim {
		return pgvector.NewVector(emb)
	}
	return nil
}

func adminMetadataClause(isPg bool) string {
	if isPg {
		return `AND (m.metadata->>'is_admin')::boolean IS TRUE`
	}
	return `AND m.metadata LIKE '%"is_admin":true%'`
}

func (s *SQLStore) InsertFact(ctx context.Context, category, factText string, importance float64, embedding []float32) (id int64, err error) {
	defer recordTranscriptQuery("insert_fact", time.Now(), &err)
	if err = s.checkDB(); err != nil {
		return 0, err
	}
	if factText == "" {
		return 0, fmt.Errorf("fact text cannot be empty")
	}
	if category == "" {
		category = "general"
	}
	if importance <= 0 {
		importance = 0.5
	}
	now := time.Now().UTC()
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO facts (category, fact_text, importance, embedding, created_at, last_reinforced_at, last_decayed_at, reinforce_count) VALUES ($1, $2, $3, $4, $5, $5, $5, 1) RETURNING id`
	err = s.db.QueryRowContext(queryCtx, query, category, factText, importance, optionalVectorArg(embedding), now).Scan(&id)
	return id, err
}

type NullVector struct {
	Vector []float32
	Valid  bool
}

func (nv *NullVector) Scan(src any) error {
	if src == nil {
		nv.Vector, nv.Valid = nil, false
		return nil
	}
	switch v := src.(type) {
	case string, []byte:
		var vec pgvector.Vector
		if err := vec.Scan(v); err == nil {
			nv.Vector, nv.Valid = vec.Slice(), true
			return nil
		}
	}
	var vec pgvector.Vector
	if err := vec.Scan(src); err != nil {
		return err
	}
	nv.Vector, nv.Valid = vec.Slice(), true
	return nil
}

type FactTime struct {
	Time  time.Time
	Valid bool
}

func (ft *FactTime) Scan(src any) error {
	if src == nil {
		*ft = FactTime{}
		return nil
	}
	switch v := src.(type) {
	case *time.Time:
		if v == nil {
			*ft = FactTime{}
			return nil
		}
	case string:
		if strings.TrimSpace(v) == "" {
			*ft = FactTime{}
			return nil
		}
	case []byte:
		if strings.TrimSpace(string(v)) == "" {
			*ft = FactTime{}
			return nil
		}
	}
	if t, ok := ParseDBTime(src); ok {
		*ft = FactTime{Time: t, Valid: true}
		return nil
	}
	return fmt.Errorf("failed to parse FactTime from %v (%T)", src, src)
}

func scanFact(scanner interface{ Scan(dest ...any) error }) (Fact, error) {
	var f Fact
	var createdAt, lastReinforced, lastDecayed FactTime
	if err := scanner.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &createdAt, &lastReinforced, &lastDecayed, &f.ReinforceCount); err != nil {
		return Fact{}, err
	}
	f.CreatedAt, f.LastReinforcedAt, f.LastDecayedAt = createdAt.Time, lastReinforced.Time, lastDecayed.Time
	if f.LastReinforcedAt.IsZero() {
		f.LastReinforcedAt = f.CreatedAt
	}
	if f.LastDecayedAt.IsZero() {
		f.LastDecayedAt = f.CreatedAt
	}
	return f, nil
}

func scanFacts(rows *sql.Rows) ([]Fact, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var facts []Fact
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, err
		}
		facts = append(facts, f)
	}
	return facts, rows.Err()
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

func (s *SQLStore) UpdateFactEmbedding(ctx context.Context, id int64, embedding []float32) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(embedding))
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	vec := pgvector.NewVector(embedding)
	_, err := s.db.ExecContext(queryCtx, "UPDATE facts SET embedding = $1 WHERE id = $2", vec, id)
	return err
}

func (s *SQLStore) GetFactsMissingEmbeddings(ctx context.Context, limit int) ([]Fact, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	query := "SELECT id, category, fact_text, importance, created_at, COALESCE(last_reinforced_at, created_at), COALESCE(last_decayed_at, created_at), COALESCE(reinforce_count, 1) FROM facts WHERE embedding IS NULL ORDER BY id ASC"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := s.db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, err
	}
	return scanFacts(rows)
}

func (s *SQLStore) SearchSimilarFacts(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]Fact, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	hasEmbedding, queryText := len(embedding) == ExpectedEmbeddingDim, strings.TrimSpace(queryText)
	hasText := queryText != ""
	if !hasEmbedding && !hasText {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	if minScore <= 0 {
		minScore = 0.20
	}
	candidateLimit := limit * 3
	if candidateLimit < 30 {
		candidateLimit = 30
	}

	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var query string
	var rows *sql.Rows
	var err error

	if hasEmbedding && hasText {
		query = `WITH vector_hits AS (
			SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> $1) AS rank FROM facts WHERE embedding IS NOT NULL ORDER BY embedding <=> $1 LIMIT $2
		), text_hits AS (
			SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $3)) DESC) AS rank FROM facts WHERE fts_tokens @@ websearch_to_tsquery('simple', $3) LIMIT $2
		)
		SELECT f.id, f.category, f.fact_text, f.importance, f.created_at, COALESCE(f.last_reinforced_at, f.created_at) AS last_reinforced_at, COALESCE(f.last_decayed_at, f.created_at) AS last_decayed_at, COALESCE(f.reinforce_count, 1) AS reinforce_count
		FROM vector_hits v FULL OUTER JOIN text_hits t ON v.id = t.id JOIN facts f ON f.id = COALESCE(v.id, t.id)
		WHERE (((COALESCE($4::float8 / ($6::float8 + v.rank::float8), 0.0) + COALESCE($5::float8 / ($6::float8 + t.rank::float8), 0.0)) * $6::float8) * f.importance) >= $7
		ORDER BY (((COALESCE($4::float8 / ($6::float8 + v.rank::float8), 0.0) + COALESCE($5::float8 / ($6::float8 + t.rank::float8), 0.0)) * $6::float8) * f.importance) DESC LIMIT $8;`
		rows, err = s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), candidateLimit, queryText, DefaultDenseWeight, DefaultSparseWeight, DefaultRRFK, minScore, limit)
	} else if hasEmbedding {
		query = `WITH candidates AS (
			SELECT id, category, fact_text, importance, created_at, COALESCE(last_reinforced_at, created_at) AS last_reinforced_at, COALESCE(last_decayed_at, created_at) AS last_decayed_at, COALESCE(reinforce_count, 1) AS reinforce_count, (1.0 - (embedding <=> $1)) AS similarity
			FROM facts WHERE embedding IS NOT NULL ORDER BY embedding <=> $1 LIMIT $2
		)
		SELECT id, category, fact_text, importance, created_at, last_reinforced_at, last_decayed_at, reinforce_count FROM candidates WHERE (similarity * importance) >= $3 ORDER BY (similarity * importance) DESC LIMIT $4;`
		rows, err = s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), candidateLimit, minScore, limit)
	} else {
		query = `WITH text_hits AS (
			SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $1)) DESC) AS rank FROM facts WHERE fts_tokens @@ websearch_to_tsquery('simple', $1) LIMIT $2
		)
		SELECT f.id, f.category, f.fact_text, f.importance, f.created_at, COALESCE(f.last_reinforced_at, f.created_at) AS last_reinforced_at, COALESCE(f.last_decayed_at, f.created_at) AS last_decayed_at, COALESCE(f.reinforce_count, 1) AS reinforce_count
		FROM text_hits t JOIN facts f ON f.id = t.id WHERE ((($3::float8 / ($4::float8 + t.rank::float8)) * $4::float8) * f.importance) >= $5
		ORDER BY ((($3::float8 / ($4::float8 + t.rank::float8)) * $4::float8) * f.importance) DESC LIMIT $6;`
		rows, err = s.db.QueryContext(queryCtx, query, queryText, candidateLimit, 1.0, DefaultRRFK, minScore, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("hybrid search failed: %w", err)
	}
	return scanFacts(rows)
}

func IsExtractableAdminTurn(m Message) bool {
	if m.Status != StatusCompleted || !m.Metadata.IsAdmin {
		return false
	}
	resp := strings.TrimSpace(m.ResponseText)
	return resp != "" && !strings.HasPrefix(resp, "[")
}

func (s *SQLStore) GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if activeHours <= 0 {
		activeHours = 24
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	query := fmt.Sprintf(`SELECT DISTINCT m.thread_id FROM messages m LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.thread_id != '' AND m.created_at >= $1 AND m.status = 'COMPLETED' AND COALESCE(TRIM(m.response_text), '') != '' AND TRIM(m.response_text) NOT LIKE '[%%%%'
	  AND (s.last_extracted_rowid IS NULL OR m.row_id > s.last_extracted_rowid) %s ORDER BY m.thread_id LIMIT 20`, adminMetadataClause(s.isPostgres))
	rows, err := s.db.QueryContext(queryCtx, query, time.Now().UTC().Add(-time.Duration(activeHours)*time.Hour))
	if err != nil {
		return nil, err
	}
	return scanNonEmptyStrings(rows)
}

func (s *SQLStore) GetUnextractedAdminTurns(ctx context.Context, threadID string) ([]Message, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return nil, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := fmt.Sprintf(`SELECT m.id, m.row_id, m.thread_id, m.content, COALESCE(m.response_text, ''), m.status, m.metadata FROM messages m LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.thread_id = $1 AND m.status = 'COMPLETED' AND COALESCE(TRIM(m.response_text), '') != '' AND TRIM(m.response_text) NOT LIKE '[%%%%'
	  AND (s.last_extracted_rowid IS NULL OR m.row_id > s.last_extracted_rowid) %s ORDER BY m.row_id ASC LIMIT 50`, adminMetadataClause(s.isPostgres))

	rows, err := s.db.QueryContext(queryCtx, query, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var turns []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.RowID, &m.ThreadID, &m.Content, &m.ResponseText, &m.Status, &m.Metadata); err != nil {
			return nil, err
		}
		if IsExtractableAdminTurn(m) {
			turns = append(turns, m)
		}
	}
	return turns, rows.Err()
}

func (s *SQLStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if threadID == "" {
		return nil
	}
	now := time.Now().UTC()
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO sessions (thread_id, internal_session_id, last_extracted_rowid, fact_extracted_at, created_at, updated_at) VALUES ($1, '', $2, $3, $4, $5)
	ON CONFLICT(thread_id) DO UPDATE SET last_extracted_rowid = CASE WHEN EXCLUDED.last_extracted_rowid > sessions.last_extracted_rowid THEN EXCLUDED.last_extracted_rowid ELSE sessions.last_extracted_rowid END, fact_extracted_at = EXCLUDED.fact_extracted_at, updated_at = EXCLUDED.updated_at`
	_, err := s.db.ExecContext(queryCtx, query, threadID, maxRowID, now, now, now)
	return err
}

func (s *SQLStore) UpdateConversationFactExtractedAt(ctx context.Context, threadID string) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	maxRowID, err := s.GetMaxMessageRowID(ctx, threadID)
	if err != nil {
		log.Printf("[DB] Warning getting max message row ID for thread %s: %v", threadID, err)
	}
	return s.UpdateConversationFactWatermark(ctx, threadID, maxRowID)
}

type FactsFilter struct {
	Category string `json:"category"`
	Query    string `json:"query"`
	Limit    int    `json:"limit"`
	Offset   int    `json:"offset"`
}

type FactsResult struct {
	Facts  []Fact `json:"facts"`
	Total  int    `json:"total"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

func EscapeSQLLike(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	return strings.ReplaceAll(s, "_", "\\_")
}

func (s *SQLStore) GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var whereClauses []string
	var args []any
	argIdx := 1
	if cat := strings.TrimSpace(filter.Category); cat != "" {
		whereClauses, args, argIdx = append(whereClauses, fmt.Sprintf("category = $%d", argIdx)), append(args, cat), argIdx+1
	}
	if q := strings.TrimSpace(filter.Query); q != "" {
		whereClauses, args, argIdx = append(whereClauses, fmt.Sprintf("LOWER(fact_text) LIKE LOWER($%d) ESCAPE '\\'", argIdx)), append(args, "%"+EscapeSQLLike(q)+"%"), argIdx+1
	}
	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	var total int
	if err := s.db.QueryRowContext(queryCtx, "SELECT COUNT(*) FROM facts"+whereSQL, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to count facts: %w", err)
	}

	var paginationSQL string
	queryArgs := append([]any{}, args...)
	if filter.Limit > 0 {
		paginationSQL, queryArgs = fmt.Sprintf("LIMIT $%d OFFSET $%d", argIdx, argIdx+1), append(queryArgs, filter.Limit, filter.Offset)
	} else if filter.Offset > 0 {
		offsetClause := fmt.Sprintf("LIMIT -1 OFFSET $%d", argIdx)
		if s.isPostgres || isPostgres(s.db) {
			offsetClause = fmt.Sprintf("OFFSET $%d", argIdx)
		}
		paginationSQL, queryArgs = offsetClause, append(queryArgs, filter.Offset)
	}

	selectQuery := fmt.Sprintf(`SELECT id, category, fact_text, COALESCE(importance, 1.0) AS importance, created_at, COALESCE(last_reinforced_at, created_at) AS last_reinforced_at, COALESCE(last_decayed_at, created_at) AS last_decayed_at, COALESCE(reinforce_count, 1) AS reinforce_count FROM facts %s ORDER BY COALESCE(importance, 1.0) DESC, created_at DESC, id DESC %s`, whereSQL, paginationSQL)

	rows, err := s.db.QueryContext(queryCtx, selectQuery, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query facts: %w", err)
	}
	facts, err := scanFacts(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to scan fact: %w", err)
	}
	if facts == nil {
		facts = make([]Fact, 0)
	}
	return &FactsResult{Facts: facts, Total: total, Limit: filter.Limit, Offset: filter.Offset}, nil
}

func (s *SQLStore) FindDuplicateFact(ctx context.Context, embedding []float32, minSim float64) (*Fact, float64, error) {
	if err := s.checkDB(); err != nil {
		return nil, 0, err
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return nil, 0, nil
	}
	if minSim <= 0 {
		minSim = 0.85
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT id, category, fact_text, importance, created_at, COALESCE(last_reinforced_at, created_at), COALESCE(last_decayed_at, created_at), COALESCE(reinforce_count, 1), (1.0 - (embedding <=> $1)) AS similarity FROM facts WHERE embedding IS NOT NULL ORDER BY embedding <=> $1 LIMIT 1;`
	var f Fact
	var sim float64
	var createdAt, lastReinforced, lastDecayed FactTime
	err := s.db.QueryRowContext(queryCtx, query, pgvector.NewVector(embedding)).Scan(
		&f.ID, &f.Category, &f.FactText, &f.Importance, &createdAt,
		&lastReinforced, &lastDecayed, &f.ReinforceCount, &sim,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("failed to find duplicate fact: %w", err)
	}
	f.CreatedAt, f.LastReinforcedAt, f.LastDecayedAt = createdAt.Time, lastReinforced.Time, lastDecayed.Time
	if sim >= minSim {
		return &f, sim, nil
	}
	return nil, sim, nil
}

func (s *SQLStore) ReinforceFact(ctx context.Context, id int64, newText string, newEmbedding []float32, boost float64) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if boost <= 0 {
		boost = 0.15
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `UPDATE facts SET importance = LEAST(1.0, GREATEST(0.70, ROUND((importance + $1)::numeric, 2))), last_reinforced_at = $2, reinforce_count = reinforce_count + 1 WHERE id = $3`
	res, err := s.db.ExecContext(queryCtx, query, boost, time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("failed to reinforce fact: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrFactNotFound
	}
	return nil
}

func (s *SQLStore) DecayAndPruneFacts(ctx context.Context, decayStep, pruneFloor float64, pruneAgeDays int) (decayedCount, prunedCount int64, err error) {
	if err = s.checkDB(); err != nil {
		return 0, 0, err
	}
	if decayStep <= 0 {
		decayStep = 0.05
	}
	if pruneFloor <= 0 {
		pruneFloor = 0.15
	}
	if pruneAgeDays <= 0 {
		pruneAgeDays = 14
	}

	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	decayQuery := `UPDATE facts SET importance = GREATEST(0.0, ROUND((importance - $1)::numeric, 2)), last_decayed_at = $2
		WHERE (last_decayed_at IS NULL OR last_decayed_at < $3) AND (last_reinforced_at IS NULL OR last_reinforced_at < $3) AND importance > 0.0`
	resDecay, err := s.db.ExecContext(queryCtx, decayQuery, decayStep, now, now.Add(-24*time.Hour))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decay facts: %w", err)
	}
	if n, err := resDecay.RowsAffected(); err == nil {
		decayedCount = n
	}

	pruneQuery := `DELETE FROM facts WHERE importance <= $1 AND (last_reinforced_at IS NULL OR last_reinforced_at < $2)`
	resPrune, err := s.db.ExecContext(queryCtx, pruneQuery, pruneFloor, now.AddDate(0, 0, -pruneAgeDays))
	if err != nil {
		return decayedCount, 0, fmt.Errorf("failed to prune facts: %w", err)
	}
	if n, err := resPrune.RowsAffected(); err == nil {
		prunedCount = n
	}
	return decayedCount, prunedCount, nil
}
