package db

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/metrics"
	pgvector "github.com/pgvector/pgvector-go"
)

const (
	ExpectedEmbeddingDim = 384
)

type Fact struct {
	ID               int64     `json:"id"`
	Category         string    `json:"category"`
	FactText         string    `json:"fact_text"`
	Importance       float64   `json:"importance"`
	ThreadID         string    `json:"thread_id"`
	CreatedAt        time.Time `json:"created_at"`
	LastReinforcedAt time.Time `json:"last_reinforced_at"`
	LastDecayedAt    time.Time `json:"last_decayed_at"`
	ReinforceCount   int       `json:"reinforce_count"`
}

type FactWithEmbedding struct {
	Fact      Fact
	Embedding []float32
}

func Float32ToBytes(slice []float32) []byte {
	buf := make([]byte, len(slice)*4)
	for i, f := range slice {
		bits := math.Float32bits(f)
		binary.LittleEndian.PutUint32(buf[i*4:], bits)
	}
	return buf
}

func BytesToFloat32(buf []byte) []float32 {
	if len(buf)%4 != 0 {
		return nil
	}
	slice := make([]float32, len(buf)/4)
	for i := range slice {
		bits := binary.LittleEndian.Uint32(buf[i*4:])
		slice[i] = math.Float32frombits(bits)
	}
	return slice
}

func InsertFact(database DBTX, category, factText string, importance float64, threadID string, embedding []float32) (id int64, err error) {
	return InsertFactWithContext(context.Background(), database, false, category, factText, importance, threadID, embedding)
}

func InsertFactWithContext(ctx context.Context, database DBTX, isPg bool, category, factText string, importance float64, threadID string, embedding []float32) (id int64, err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.RecordDBQuery("insert_fact", status, time.Since(start))
	}()
	if database == nil {
		return 0, fmt.Errorf("database is nil")
	}
	if factText == "" {
		return 0, fmt.Errorf("fact text cannot be empty")
	}
	if category == "" {
		category = "general"
	}
	if importance <= 0 {
		importance = 1.0
	}
	now := time.Now().UTC()

	var vecVal interface{}
	if len(embedding) == ExpectedEmbeddingDim {
		if isPg || isPostgres(database) {
			vecVal = pgvector.NewVector(embedding)
		} else {
			vecVal = Float32ToBytes(embedding)
		}
	}

	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO facts (category, fact_text, importance, thread_id, embedding, created_at, last_reinforced_at, last_decayed_at, reinforce_count) VALUES ($1, $2, $3, $4, $5, $6, $6, $6, 1) RETURNING id`
	var insertedID int64
	err = database.QueryRowContext(queryCtx, query, category, factText, importance, threadID, vecVal, now).Scan(&insertedID)
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
		if len(v)%4 == 0 {
			nv.Vector = BytesToFloat32(v)
			nv.Valid = len(nv.Vector) > 0
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
		ft.Time = time.Time{}
		ft.Valid = false
		return nil
	}
	switch v := src.(type) {
	case time.Time:
		ft.Time = v
		ft.Valid = true
		return nil
	case string:
		for _, layout := range []string{
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05.999999999 -0700 -0700",
			"2006-01-02 15:04:05 -0700 MST",
			time.RFC3339Nano,
			time.RFC3339,
			time.DateTime,
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05-07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05",
		} {
			if t, err := time.Parse(layout, v); err == nil {
				ft.Time = t
				ft.Valid = true
				return nil
			}
		}
		// Also try trimming any trailing monotonic clock info (e.g. " m=+0.001234567")
		if idx := strings.Index(v, " m="); idx != -1 {
			trimmed := v[:idx]
			for _, layout := range []string{
				"2006-01-02 15:04:05.999999999 -0700 MST",
				"2006-01-02 15:04:05 -0700 MST",
			} {
				if t, err := time.Parse(layout, trimmed); err == nil {
					ft.Time = t
					ft.Valid = true
					return nil
				}
			}
		}
		return fmt.Errorf("cannot parse %q into FactTime", v)
	case []byte:
		return ft.Scan(string(v))
	}
	return fmt.Errorf("unsupported type %T for FactTime", src)
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

func UpdateFactEmbeddingWithContext(ctx context.Context, database DBTX, id int64, embedding []float32) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(embedding))
	}

	vec := pgvector.NewVector(embedding)
	_, err := database.ExecContext(ctx, "UPDATE facts SET embedding = $1 WHERE id = $2", vec, id)
	return err
}

func UpdateFactEmbedding(database DBTX, id int64, embedding []float32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return UpdateFactEmbeddingWithContext(ctx, database, id, embedding)
}

func GetFactsMissingEmbeddingsWithContext(ctx context.Context, database DBTX, limit int) ([]Fact, error) {
	if database == nil {
		return nil, nil
	}
	query := "SELECT id, category, fact_text, importance, thread_id, created_at, COALESCE(last_reinforced_at, created_at), COALESCE(last_decayed_at, created_at), COALESCE(reinforce_count, 1) FROM facts WHERE embedding IS NULL ORDER BY id ASC"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []Fact
	for rows.Next() {
		var f Fact
		var createdAt, lastReinforced, lastDecayed FactTime
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &createdAt, &lastReinforced, &lastDecayed, &f.ReinforceCount); err != nil {
			return nil, err
		}
		f.CreatedAt = createdAt.Time
		f.LastReinforcedAt = lastReinforced.Time
		f.LastDecayedAt = lastDecayed.Time
		results = append(results, f)
	}
	return results, rows.Err()
}

func GetAllFactsWithEmbeddings(database DBTX) ([]FactWithEmbedding, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `SELECT id, category, fact_text, importance, thread_id, embedding, created_at, COALESCE(last_reinforced_at, created_at), COALESCE(last_decayed_at, created_at), COALESCE(reinforce_count, 1) FROM facts ORDER BY created_at DESC`
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []FactWithEmbedding
	for rows.Next() {
		var f Fact
		var nv NullVector
		var createdAt, lastReinforced, lastDecayed FactTime
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &nv, &createdAt, &lastReinforced, &lastDecayed, &f.ReinforceCount); err != nil {
			return nil, err
		}
		f.CreatedAt = createdAt.Time
		f.LastReinforcedAt = lastReinforced.Time
		f.LastDecayedAt = lastDecayed.Time
		var emb []float32
		if nv.Valid {
			emb = nv.Vector
		}
		results = append(results, FactWithEmbedding{
			Fact:      f,
			Embedding: emb,
		})
	}
	return results, nil
}

func GetFactsByThreadWithEmbeddings(database DBTX, threadID string) ([]FactWithEmbedding, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `SELECT id, category, fact_text, importance, thread_id, embedding, created_at, COALESCE(last_reinforced_at, created_at), COALESCE(last_decayed_at, created_at), COALESCE(reinforce_count, 1) FROM facts`
	var rows *sql.Rows
	var err error
	if threadID != "" {
		query += ` WHERE thread_id = $1 ORDER BY created_at DESC`
		rows, err = database.QueryContext(ctx, query, threadID)
	} else {
		query += ` ORDER BY created_at DESC`
		rows, err = database.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []FactWithEmbedding
	for rows.Next() {
		var f Fact
		var nv NullVector
		var createdAt, lastReinforced, lastDecayed FactTime
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &nv, &createdAt, &lastReinforced, &lastDecayed, &f.ReinforceCount); err != nil {
			return nil, err
		}
		f.CreatedAt = createdAt.Time
		f.LastReinforcedAt = lastReinforced.Time
		f.LastDecayedAt = lastDecayed.Time
		var emb []float32
		if nv.Valid {
			emb = nv.Vector
		}
		results = append(results, FactWithEmbedding{
			Fact:      f,
			Embedding: emb,
		})
	}
	return results, nil
}

// SearchSimilarFacts executes an HNSW index-accelerated candidate fetch followed by importance-weighted scoring.
func SearchSimilarFacts(database DBTX, embedding []float32, limit int, minScore float64, threadID string) ([]Fact, error) {
	return SearchSimilarFactsWithContext(context.Background(), database, false, embedding, limit, minScore, threadID)
}

func SearchSimilarFactsWithContext(ctx context.Context, database DBTX, isPg bool, embedding []float32, limit int, minScore float64, threadID string) ([]Fact, error) {
	if database == nil || len(embedding) != ExpectedEmbeddingDim {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	if minScore <= 0 {
		minScore = 0.20
	}

	if !isPg && !isPostgres(database) {
		// SQLite in-memory fallback for unit tests: rank in memory
		allFacts, err := GetAllFactsWithEmbeddings(database)
		if err != nil {
			return nil, err
		}
		type scoredFact struct {
			fact  Fact
			score float64
		}
		var scored []scoredFact
		for _, fwe := range allFacts {
			if threadID != "" && fwe.Fact.ThreadID != "" && fwe.Fact.ThreadID != threadID {
				continue
			}
			if len(fwe.Embedding) != ExpectedEmbeddingDim {
				continue
			}
			sim := cosineSimilarity(embedding, fwe.Embedding)
			totalScore := sim * fwe.Fact.Importance
			if totalScore >= minScore {
				scored = append(scored, scoredFact{fact: fwe.Fact, score: totalScore})
			}
		}
		sort.Slice(scored, func(i, j int) bool {
			return scored[i].score > scored[j].score
		})
		var facts []Fact
		for i := 0; i < len(scored) && i < limit; i++ {
			facts = append(facts, scored[i].fact)
		}
		return facts, nil
	}

	candidateLimit := limit * 3
	if candidateLimit < 30 {
		candidateLimit = 30
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
	WITH candidates AS (
		SELECT 
			id, 
			category, 
			fact_text, 
			importance, 
			thread_id, 
			created_at,
			COALESCE(last_reinforced_at, created_at) AS last_reinforced_at,
			COALESCE(last_decayed_at, created_at) AS last_decayed_at,
			COALESCE(reinforce_count, 1) AS reinforce_count,
			(1.0 - (embedding <=> $1)) AS similarity
		FROM facts
		WHERE ($2 = '' OR thread_id = $2 OR thread_id = '')
		  AND embedding IS NOT NULL
		ORDER BY embedding <=> $1
		LIMIT $3
	)
	SELECT 
		id, 
		category, 
		fact_text, 
		importance, 
		thread_id, 
		created_at,
		last_reinforced_at,
		last_decayed_at,
		reinforce_count
	FROM candidates
	WHERE (similarity * importance) >= $4
	ORDER BY (similarity * importance) DESC
	LIMIT $5;
	`

	vec := pgvector.NewVector(embedding)
	rows, err := database.QueryContext(ctx, query, vec, threadID, candidateLimit, minScore, limit)
	if err != nil {
		return nil, fmt.Errorf("vector search failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var facts []Fact
	for rows.Next() {
		var f Fact
		var createdAt, lastReinforced, lastDecayed FactTime
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &createdAt, &lastReinforced, &lastDecayed, &f.ReinforceCount); err != nil {
			return nil, fmt.Errorf("scan fact error: %w", err)
		}
		f.CreatedAt = createdAt.Time
		f.LastReinforcedAt = lastReinforced.Time
		f.LastDecayedAt = lastDecayed.Time
		facts = append(facts, f)
	}

	return facts, nil
}

func GetActiveConversationsForExtraction(database DBTX, activeHours int) ([]string, error) {
	if database == nil {
		return nil, nil
	}
	if activeHours <= 0 {
		activeHours = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(activeHours) * time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
	SELECT DISTINCT m.thread_id
	FROM messages m
	LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.thread_id != ''
	  AND m.created_at >= $1
	  AND m.status = 'COMPLETED'
	  AND (s.last_extracted_rowid IS NULL OR m.row_id > s.last_extracted_rowid)
	ORDER BY m.thread_id
	LIMIT 20
	`
	rows, err := database.QueryContext(ctx, query, cutoff)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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

func UpdateConversationFactWatermark(database DBTX, threadID string, maxRowID int64) error {
	if database == nil || threadID == "" {
		return nil
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	_, err := database.ExecContext(ctx, query, threadID, maxRowID, now, now, now)
	return err
}

func UpdateConversationFactExtractedAt(database DBTX, threadID string) error {
	maxRowID, _ := GetMaxMessageRowID(database, threadID)
	return UpdateConversationFactWatermark(database, threadID, maxRowID)
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

// GetFactsPaginated retrieves facts based on the provided filter.
func GetFactsPaginated(database DBTX, filter FactsFilter) (*FactsResult, error) {
	return GetFactsPaginatedWithContext(context.Background(), database, false, filter)
}

func GetFactsPaginatedWithContext(ctx context.Context, database DBTX, isPg bool, filter FactsFilter) (*FactsResult, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
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

	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	countQuery := "SELECT COUNT(*) FROM facts" + whereSQL
	var total int
	if err := database.QueryRowContext(queryCtx, countQuery, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to count facts: %w", err)
	}

	var paginationSQL string
	queryArgs := append([]interface{}{}, args...)
	if filter.Limit > 0 {
		paginationSQL = fmt.Sprintf("LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
		queryArgs = append(queryArgs, filter.Limit, filter.Offset)
	} else if filter.Offset > 0 {
		if isPg || isPostgres(database) {
			paginationSQL = fmt.Sprintf("OFFSET $%d", argIdx)
		} else {
			paginationSQL = fmt.Sprintf("LIMIT -1 OFFSET $%d", argIdx)
		}
		queryArgs = append(queryArgs, filter.Offset)
	}

	selectQuery := fmt.Sprintf(`
		SELECT id, category, fact_text, COALESCE(importance, 1.0) AS importance, thread_id, created_at,
		       COALESCE(last_reinforced_at, created_at) AS last_reinforced_at,
		       COALESCE(last_decayed_at, created_at) AS last_decayed_at,
		       COALESCE(reinforce_count, 1) AS reinforce_count
		FROM facts
		%s
		ORDER BY COALESCE(importance, 1.0) DESC, created_at DESC, id DESC
		%s
	`, whereSQL, paginationSQL)

	rows, err := database.QueryContext(ctx, selectQuery, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query facts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	facts := make([]Fact, 0)
	for rows.Next() {
		var f Fact
		var createdAt, lastReinforced, lastDecayed FactTime
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &createdAt, &lastReinforced, &lastDecayed, &f.ReinforceCount); err != nil {
			return nil, fmt.Errorf("failed to scan fact: %w", err)
		}
		f.CreatedAt = createdAt.Time
		f.LastReinforcedAt = lastReinforced.Time
		f.LastDecayedAt = lastDecayed.Time
		facts = append(facts, f)
	}

	return &FactsResult{
		Facts:  facts,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}, nil
}

// FindDuplicateFact searches the facts table globally for an existing fact with cosine similarity >= minSim.
func FindDuplicateFact(database DBTX, embedding []float32, minSim float64) (*Fact, float64, error) {
	return FindDuplicateFactWithContext(context.Background(), database, false, embedding, minSim)
}

func FindDuplicateFactWithContext(ctx context.Context, database DBTX, isPg bool, embedding []float32, minSim float64) (*Fact, float64, error) {
	if database == nil || len(embedding) != ExpectedEmbeddingDim {
		return nil, 0, nil
	}
	if minSim <= 0 {
		minSim = 0.88
	}

	if !isPg && !isPostgres(database) {
		// SQLite in-memory fallback for unit tests
		allFacts, err := GetAllFactsWithEmbeddings(database)
		if err != nil {
			return nil, 0, err
		}
		var bestFact *Fact
		var bestSim float64
		for _, fwe := range allFacts {
			if len(fwe.Embedding) != ExpectedEmbeddingDim {
				continue
			}
			sim := cosineSimilarity(embedding, fwe.Embedding)
			if sim > bestSim {
				bestSim = sim
				f := fwe.Fact
				bestFact = &f
			}
		}
		if bestSim >= minSim && bestFact != nil {
			return bestFact, bestSim, nil
		}
		return nil, bestSim, nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
	SELECT id, category, fact_text, importance, thread_id, created_at,
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
	err := database.QueryRowContext(queryCtx, query, vec).Scan(
		&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &createdAt,
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

// ReinforceFact updates an existing fact with an importance boost, touching last_reinforced_at and updating text/embedding if provided.
func ReinforceFact(database DBTX, id int64, newText string, newEmbedding []float32, boost float64) error {
	return ReinforceFactWithContext(context.Background(), database, false, id, newText, newEmbedding, boost)
}

func ReinforceFactWithContext(ctx context.Context, database DBTX, isPg bool, id int64, newText string, newEmbedding []float32, boost float64) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if boost <= 0 {
		boost = 0.15
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	now := time.Now().UTC()
	var updateText bool
	var vecVal any
	if strings.TrimSpace(newText) != "" && len(newEmbedding) == ExpectedEmbeddingDim {
		updateText = true
		if isPg || isPostgres(database) {
			vecVal = pgvector.NewVector(newEmbedding)
		} else {
			vecVal = Float32ToBytes(newEmbedding)
		}
	}

	if isPg || isPostgres(database) {
		var query string
		var res sql.Result
		var err error
		if updateText {
			query = `
				UPDATE facts
				SET importance = LEAST(1.0, GREATEST(0.70, ROUND((importance + $1)::numeric, 2))),
				    fact_text = $2,
				    embedding = $3,
				    last_reinforced_at = $4,
				    reinforce_count = reinforce_count + 1
				WHERE id = $5
			`
			res, err = database.ExecContext(queryCtx, query, boost, newText, vecVal, now, id)
		} else {
			query = `
				UPDATE facts
				SET importance = LEAST(1.0, GREATEST(0.70, ROUND((importance + $1)::numeric, 2))),
				    last_reinforced_at = $2,
				    reinforce_count = reinforce_count + 1
				WHERE id = $3
			`
			res, err = database.ExecContext(queryCtx, query, boost, now, id)
		}
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

	// SQLite branch
	var query string
	var res sql.Result
	var err error
	if updateText {
		query = `
			UPDATE facts
			SET importance = MIN(1.0, MAX(0.70, ROUND(importance + ?, 2))),
			    fact_text = ?,
			    embedding = ?,
			    last_reinforced_at = ?,
			    reinforce_count = reinforce_count + 1
			WHERE id = ?
		`
		res, err = database.ExecContext(queryCtx, query, boost, newText, vecVal, now, id)
	} else {
		query = `
			UPDATE facts
			SET importance = MIN(1.0, MAX(0.70, ROUND(importance + ?, 2))),
			    last_reinforced_at = ?,
			    reinforce_count = reinforce_count + 1
			WHERE id = ?
		`
		res, err = database.ExecContext(queryCtx, query, boost, now, id)
	}
	if err != nil {
		return fmt.Errorf("failed to reinforce fact in sqlite: %w", err)
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

// DecayAndPruneFacts applies scheduled daily importance decay to unreinforced facts and prunes dead facts.
func DecayAndPruneFacts(database DBTX, decayStep float64, pruneFloor float64, pruneAgeDays int) (int64, int64, error) {
	return DecayAndPruneFactsWithContext(context.Background(), database, false, decayStep, pruneFloor, pruneAgeDays)
}

func DecayAndPruneFactsWithContext(ctx context.Context, database DBTX, isPg bool, decayStep float64, pruneFloor float64, pruneAgeDays int) (int64, int64, error) {
	if database == nil {
		return 0, 0, fmt.Errorf("database is nil")
	}
	if decayStep <= 0 {
		decayStep = 0.02
	}
	if pruneFloor <= 0 {
		pruneFloor = 0.10
	}
	if pruneAgeDays <= 0 {
		pruneAgeDays = 30
	}

	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	decayCutoff := now.Add(-24 * time.Hour)
	cutoffDate := now.AddDate(0, 0, -pruneAgeDays)

	var decayedCount, prunedCount int64

	if isPg || isPostgres(database) {
		decayQuery := `
			UPDATE facts
			SET importance = GREATEST(0.0, ROUND((importance - $1)::numeric, 2)),
			    last_decayed_at = $2
			WHERE (last_decayed_at IS NULL OR last_decayed_at < $3)
			  AND (last_reinforced_at IS NULL OR last_reinforced_at < $3)
			  AND importance > 0.0
		`
		resDecay, err := database.ExecContext(queryCtx, decayQuery, decayStep, now, decayCutoff)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to decay facts: %w", err)
		}
		decayedCount, _ = resDecay.RowsAffected()

		pruneQuery := `
			DELETE FROM facts
			WHERE importance <= $1
			  AND (last_reinforced_at IS NULL OR last_reinforced_at < $2)
		`
		resPrune, err := database.ExecContext(queryCtx, pruneQuery, pruneFloor, cutoffDate)
		if err != nil {
			return decayedCount, 0, fmt.Errorf("failed to prune facts: %w", err)
		}
		prunedCount, _ = resPrune.RowsAffected()

		return decayedCount, prunedCount, nil
	}

	// SQLite branch
	decayQuery := `
		UPDATE facts
		SET importance = MAX(0.0, ROUND(importance - ?, 2)),
		    last_decayed_at = ?
		WHERE (last_decayed_at IS NULL OR last_decayed_at < ?)
		  AND (last_reinforced_at IS NULL OR last_reinforced_at < ?)
		  AND importance > 0.0
	`
	resDecay, err := database.ExecContext(queryCtx, decayQuery, decayStep, now, decayCutoff, decayCutoff)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decay facts in sqlite: %w", err)
	}
	decayedCount, _ = resDecay.RowsAffected()

	pruneQuery := `
		DELETE FROM facts
		WHERE importance <= ?
		  AND (last_reinforced_at IS NULL OR last_reinforced_at < ?)
	`
	resPrune, err := database.ExecContext(queryCtx, pruneQuery, pruneFloor, cutoffDate)
	if err != nil {
		return decayedCount, 0, fmt.Errorf("failed to prune facts in sqlite: %w", err)
	}
	prunedCount, _ = resPrune.RowsAffected()

	return decayedCount, prunedCount, nil
}
