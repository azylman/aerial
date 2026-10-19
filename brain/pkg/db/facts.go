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

	var vecVal interface{}
	if len(embedding) == ExpectedEmbeddingDim {
		vecVal = pgvector.NewVector(embedding)
	}

	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO facts (category, fact_text, importance, embedding, created_at, last_reinforced_at, last_decayed_at, reinforce_count) VALUES ($1, $2, $3, $4, $5, $5, $5, 1) RETURNING id`
	var insertedID int64
	err = s.db.QueryRowContext(queryCtx, query, category, factText, importance, vecVal, now).Scan(&insertedID)
	if err != nil {
		return 0, err
	}
	return insertedID, nil
}

type NullVector struct {
	Vector []float32
	Valid  bool
}

func (nv *NullVector) Scan(src any) error {
	if src == nil {
		nv.Vector = nil
		nv.Valid = false
		return nil
	}
	switch v := src.(type) {
	case string:
		var vec pgvector.Vector
		if err := vec.Scan(v); err == nil {
			nv.Vector = vec.Slice()
			nv.Valid = true
			return nil
		}
	case []byte:
		var vec pgvector.Vector
		if err := vec.Scan(v); err == nil {
			nv.Vector = vec.Slice()
			nv.Valid = true
			return nil
		}
	}
	var vec pgvector.Vector
	if err := vec.Scan(src); err != nil {
		return err
	}
	nv.Vector = vec.Slice()
	nv.Valid = true
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
	f.CreatedAt = createdAt.Time
	f.LastReinforcedAt = lastReinforced.Time
	if f.LastReinforcedAt.IsZero() {
		f.LastReinforcedAt = f.CreatedAt
	}
	f.LastDecayedAt = lastDecayed.Time
	if f.LastDecayedAt.IsZero() {
		f.LastDecayedAt = f.CreatedAt
	}
	return f, nil
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
	defer rows.Close()

	var results []Fact
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, f)
	}
	return results, rows.Err()
}


func (s *SQLStore) SearchSimilarFacts(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]Fact, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
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
		// Hybrid RRF query: combining dense HNSW vector search with sparse FTS using 70/30 weighting
		query = `
		WITH vector_hits AS (
			SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> $1) AS rank
			FROM facts
			WHERE embedding IS NOT NULL
			ORDER BY embedding <=> $1
			LIMIT $2
		),
		text_hits AS (
			SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $3)) DESC) AS rank
			FROM facts
			WHERE fts_tokens @@ websearch_to_tsquery('simple', $3)
			LIMIT $2
		)
		SELECT 
			f.id, 
			f.category, 
			f.fact_text, 
			f.importance, 
			f.created_at,
			COALESCE(f.last_reinforced_at, f.created_at) AS last_reinforced_at,
			COALESCE(f.last_decayed_at, f.created_at) AS last_decayed_at,
			COALESCE(f.reinforce_count, 1) AS reinforce_count
		FROM vector_hits v
		FULL OUTER JOIN text_hits t ON v.id = t.id
		JOIN facts f ON f.id = COALESCE(v.id, t.id)
		WHERE (((COALESCE($4::float8 / ($6::float8 + v.rank::float8), 0.0) + COALESCE($5::float8 / ($6::float8 + t.rank::float8), 0.0)) * $6::float8) * f.importance) >= $7
		ORDER BY (((COALESCE($4::float8 / ($6::float8 + v.rank::float8), 0.0) + COALESCE($5::float8 / ($6::float8 + t.rank::float8), 0.0)) * $6::float8) * f.importance) DESC
		LIMIT $8;
		`
		vec := pgvector.NewVector(embedding)
		rows, err = s.db.QueryContext(queryCtx, query, vec, candidateLimit, queryText, DefaultDenseWeight, DefaultSparseWeight, DefaultRRFK, minScore, limit)
	} else if hasEmbedding {
		// Dense vector only query
		query = `
		WITH candidates AS (
			SELECT 
				id, 
				category, 
				fact_text, 
				importance, 
				created_at,
				COALESCE(last_reinforced_at, created_at) AS last_reinforced_at,
				COALESCE(last_decayed_at, created_at) AS last_decayed_at,
				COALESCE(reinforce_count, 1) AS reinforce_count,
				(1.0 - (embedding <=> $1)) AS similarity
			FROM facts
			WHERE embedding IS NOT NULL
			ORDER BY embedding <=> $1
			LIMIT $2
		)
		SELECT 
			id, 
			category, 
			fact_text, 
			importance, 
			created_at,
			last_reinforced_at,
			last_decayed_at,
			reinforce_count
		FROM candidates
		WHERE (similarity * importance) >= $3
		ORDER BY (similarity * importance) DESC
		LIMIT $4;
		`
		vec := pgvector.NewVector(embedding)
		rows, err = s.db.QueryContext(queryCtx, query, vec, candidateLimit, minScore, limit)
	} else {
		// Sparse full-text search only query
		query = `
		WITH text_hits AS (
			SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $1)) DESC) AS rank
			FROM facts
			WHERE fts_tokens @@ websearch_to_tsquery('simple', $1)
			LIMIT $2
		)
		SELECT 
			f.id, 
			f.category, 
			f.fact_text, 
			f.importance, 
			f.created_at,
			COALESCE(f.last_reinforced_at, f.created_at) AS last_reinforced_at,
			COALESCE(f.last_decayed_at, f.created_at) AS last_decayed_at,
			COALESCE(f.reinforce_count, 1) AS reinforce_count
		FROM text_hits t
		JOIN facts f ON f.id = t.id
		WHERE ((($3::float8 / ($4::float8 + t.rank::float8)) * $4::float8) * f.importance) >= $5
		ORDER BY ((($3::float8 / ($4::float8 + t.rank::float8)) * $4::float8) * f.importance) DESC
		LIMIT $6;
		`
		rows, err = s.db.QueryContext(queryCtx, query, queryText, candidateLimit, 1.0, DefaultRRFK, minScore, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("hybrid search failed: %w", err)
	}
	defer rows.Close()

	var facts []Fact
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, fmt.Errorf("scan fact error: %w", err)
		}
		facts = append(facts, f)
	}

	return facts, nil
}

// IsExtractableAdminTurn returns true if the message is a completed human admin turn with a non-ambient response.
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
	cutoff := time.Now().UTC().Add(-time.Duration(activeHours) * time.Hour)
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	adminClause := `AND m.metadata LIKE '%"is_admin":true%'`
	if s.isPostgres {
		adminClause = `AND (m.metadata->>'is_admin')::boolean IS TRUE`
	}

	query := fmt.Sprintf(`
	SELECT DISTINCT m.thread_id
	FROM messages m
	LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.thread_id != ''
	  AND m.created_at >= $1
	  AND m.status = 'COMPLETED'
	  AND COALESCE(TRIM(m.response_text), '') != ''
	  AND TRIM(m.response_text) NOT LIKE '[%%'
	  AND (s.last_extracted_rowid IS NULL OR m.row_id > s.last_extracted_rowid)
	  %s
	ORDER BY m.thread_id
	LIMIT 20
	`, adminClause)
	rows, err := s.db.QueryContext(queryCtx, query, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tids []string
	for rows.Next() {
		var tid string
		if err := rows.Scan(&tid); err != nil {
			return nil, err
		}
		if tid != "" {
			tids = append(tids, tid)
		}
	}
	return tids, nil
}

func (s *SQLStore) GetUnextractedAdminTurns(ctx context.Context, threadID string) ([]Message, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return nil, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	adminClause := `AND m.metadata LIKE '%"is_admin":true%'`
	if s.isPostgres {
		adminClause = `AND (m.metadata->>'is_admin')::boolean IS TRUE`
	}

	query := fmt.Sprintf(`
	SELECT m.id, m.row_id, m.thread_id, m.content, COALESCE(m.response_text, ''), m.status, m.metadata
	FROM messages m
	LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.thread_id = $1
	  AND m.status = 'COMPLETED'
	  AND COALESCE(TRIM(m.response_text), '') != ''
	  AND TRIM(m.response_text) NOT LIKE '[%%'
	  AND (s.last_extracted_rowid IS NULL OR m.row_id > s.last_extracted_rowid)
	  %s
	ORDER BY m.row_id ASC
	LIMIT 50
	`, adminClause)

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

	query := `
	INSERT INTO sessions (thread_id, internal_session_id, last_extracted_rowid, fact_extracted_at, created_at, updated_at)
	VALUES ($1, '', $2, $3, $4, $5)
	ON CONFLICT(thread_id) DO UPDATE SET
		last_extracted_rowid = CASE 
			WHEN EXCLUDED.last_extracted_rowid > sessions.last_extracted_rowid 
			THEN EXCLUDED.last_extracted_rowid 
			ELSE sessions.last_extracted_rowid 
		END,
		fact_extracted_at = EXCLUDED.fact_extracted_at,
		updated_at = EXCLUDED.updated_at
	`
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
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

func (s *SQLStore) GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}

	if filter.Offset < 0 {
		filter.Offset = 0
	}

	var whereClauses []string
	var args []interface{}
	argIdx := 1

	if strings.TrimSpace(filter.Category) != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("category = $%d", argIdx))
		args = append(args, strings.TrimSpace(filter.Category))
		argIdx++
	}

	if strings.TrimSpace(filter.Query) != "" {
		escaped := EscapeSQLLike(strings.TrimSpace(filter.Query))
		whereClauses = append(whereClauses, fmt.Sprintf("LOWER(fact_text) LIKE LOWER($%d) ESCAPE '\\'", argIdx))
		args = append(args, "%"+escaped+"%")
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	countQuery := "SELECT COUNT(*) FROM facts" + whereSQL
	var total int
	if err := s.db.QueryRowContext(queryCtx, countQuery, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to count facts: %w", err)
	}

	var paginationSQL string
	queryArgs := append([]interface{}{}, args...)
	if filter.Limit > 0 {
		paginationSQL = fmt.Sprintf("LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
		queryArgs = append(queryArgs, filter.Limit, filter.Offset)
	} else if filter.Offset > 0 {
		if s.isPostgres || isPostgres(s.db) {
			paginationSQL = fmt.Sprintf("OFFSET $%d", argIdx)
		} else {
			paginationSQL = fmt.Sprintf("LIMIT -1 OFFSET $%d", argIdx)
		}
		queryArgs = append(queryArgs, filter.Offset)
	}

	selectQuery := fmt.Sprintf(`
		SELECT id, category, fact_text, COALESCE(importance, 1.0) AS importance, created_at,
		       COALESCE(last_reinforced_at, created_at) AS last_reinforced_at,
		       COALESCE(last_decayed_at, created_at) AS last_decayed_at,
		       COALESCE(reinforce_count, 1) AS reinforce_count
		FROM facts
		%s
		ORDER BY COALESCE(importance, 1.0) DESC, created_at DESC, id DESC
		%s
	`, whereSQL, paginationSQL)

	rows, err := s.db.QueryContext(queryCtx, selectQuery, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query facts: %w", err)
	}
	defer rows.Close()

	facts := make([]Fact, 0)
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan fact: %w", err)
		}
		facts = append(facts, f)
	}

	return &FactsResult{
		Facts:  facts,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}, nil
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

	query := `
	SELECT id, category, fact_text, importance, created_at,
	       COALESCE(last_reinforced_at, created_at), COALESCE(last_decayed_at, created_at), COALESCE(reinforce_count, 1),
	       (1.0 - (embedding <=> $1)) AS similarity
	FROM facts
	WHERE embedding IS NOT NULL
	ORDER BY embedding <=> $1
	LIMIT 1;
	`
	vec := pgvector.NewVector(embedding)
	var f Fact
	var sim float64
	var createdAt, lastReinforced, lastDecayed FactTime
	err := s.db.QueryRowContext(queryCtx, query, vec).Scan(
		&f.ID, &f.Category, &f.FactText, &f.Importance, &createdAt,
		&lastReinforced, &lastDecayed, &f.ReinforceCount, &sim,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("failed to find duplicate fact: %w", err)
	}
	f.CreatedAt = createdAt.Time
	f.LastReinforcedAt = lastReinforced.Time
	f.LastDecayedAt = lastDecayed.Time
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

	now := time.Now().UTC()
	query := `
		UPDATE facts
		SET importance = LEAST(1.0, GREATEST(0.70, ROUND((importance + $1)::numeric, 2))),
		    last_reinforced_at = $2,
		    reinforce_count = reinforce_count + 1
		WHERE id = $3
	`
	res, err := s.db.ExecContext(queryCtx, query, boost, now, id)
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

func (s *SQLStore) DecayAndPruneFacts(ctx context.Context, decayStep float64, pruneFloor float64, pruneAgeDays int) (int64, int64, error) {
	if err := s.checkDB(); err != nil {
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
	decayCutoff := now.Add(-24 * time.Hour)
	cutoffDate := now.AddDate(0, 0, -pruneAgeDays)

	var decayedCount, prunedCount int64

	decayQuery := `
		UPDATE facts
		SET importance = GREATEST(0.0, ROUND((importance - $1)::numeric, 2)),
		    last_decayed_at = $2
		WHERE (last_decayed_at IS NULL OR last_decayed_at < $3)
		  AND (last_reinforced_at IS NULL OR last_reinforced_at < $3)
		  AND importance > 0.0
	`
	resDecay, err := s.db.ExecContext(queryCtx, decayQuery, decayStep, now, decayCutoff)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decay facts: %w", err)
	}
	if n, err := resDecay.RowsAffected(); err == nil {
		decayedCount = n
	} else {
		log.Printf("[DB] Warning getting rows affected for decay: %v", err)
	}

	pruneQuery := `
		DELETE FROM facts
		WHERE importance <= $1
		  AND (last_reinforced_at IS NULL OR last_reinforced_at < $2)
	`
	resPrune, err := s.db.ExecContext(queryCtx, pruneQuery, pruneFloor, cutoffDate)
	if err != nil {
		return decayedCount, 0, fmt.Errorf("failed to prune facts: %w", err)
	}
	if n, err := resPrune.RowsAffected(); err == nil {
		prunedCount = n
	} else {
		log.Printf("[DB] Warning getting rows affected for prune: %v", err)
	}

	return decayedCount, prunedCount, nil
}
