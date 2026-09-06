package db

import (
	"context"
	"database/sql"
	"encoding/binary"
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
	ID         int64     `json:"id"`
	Category   string    `json:"category"`
	FactText   string    `json:"fact_text"`
	Importance float64   `json:"importance"`
	ThreadID   string    `json:"thread_id"`
	CreatedAt  time.Time `json:"created_at"`
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

func InsertFact(database *sql.DB, category, factText string, importance float64, threadID string, embedding []float32) (id int64, err error) {
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
		if isPostgres(database) {
			vecVal = pgvector.NewVector(embedding)
		} else {
			vecVal = Float32ToBytes(embedding)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `INSERT INTO facts (category, fact_text, importance, thread_id, embedding, created_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`
	var insertedID int64
	err = database.QueryRowContext(ctx, query, category, factText, importance, threadID, vecVal, now).Scan(&insertedID)
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

func UpdateFactEmbedding(database *sql.DB, id int64, embedding []float32) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(embedding))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	vec := pgvector.NewVector(embedding)
	_, err := database.ExecContext(ctx, "UPDATE facts SET embedding = $1 WHERE id = $2", vec, id)
	return err
}

func GetAllFactsWithEmbeddings(database *sql.DB) ([]FactWithEmbedding, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `SELECT id, category, fact_text, importance, thread_id, embedding, created_at FROM facts ORDER BY created_at DESC`
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []FactWithEmbedding
	for rows.Next() {
		var f Fact
		var nv NullVector
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &nv, &f.CreatedAt); err != nil {
			return nil, err
		}
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

func GetFactsByThreadWithEmbeddings(database *sql.DB, threadID string) ([]FactWithEmbedding, error) {
	if database == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `SELECT id, category, fact_text, importance, thread_id, embedding, created_at FROM facts`
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
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &nv, &f.CreatedAt); err != nil {
			return nil, err
		}
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
func SearchSimilarFacts(database *sql.DB, embedding []float32, limit int, minScore float64, threadID string) ([]Fact, error) {
	if database == nil || len(embedding) != ExpectedEmbeddingDim {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	if minScore <= 0 {
		minScore = 0.20
	}

	if !isPostgres(database) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		created_at
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
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &f.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan fact error: %w", err)
		}
		facts = append(facts, f)
	}

	return facts, nil
}

func GetActiveConversationsForExtraction(database *sql.DB, activeHours int) ([]string, error) {
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

func UpdateConversationFactWatermark(database *sql.DB, threadID string, maxRowID int64) error {
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

func UpdateConversationFactExtractedAt(database *sql.DB, threadID string) error {
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

func GetFactsPaginated(database *sql.DB, filter FactsFilter) (*FactsResult, error) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	countQuery := "SELECT COUNT(*) FROM facts" + whereSQL
	var total int
	if err := database.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to count facts: %w", err)
	}

	var paginationSQL string
	queryArgs := append([]interface{}{}, args...)
	if filter.Limit > 0 {
		paginationSQL = fmt.Sprintf("LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
		queryArgs = append(queryArgs, filter.Limit, filter.Offset)
	} else if filter.Offset > 0 {
		paginationSQL = fmt.Sprintf("OFFSET $%d", argIdx)
		queryArgs = append(queryArgs, filter.Offset)
	}

	selectQuery := fmt.Sprintf(`
		SELECT id, category, fact_text, COALESCE(importance, 1.0) AS importance, thread_id, created_at
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
		if err := rows.Scan(&f.ID, &f.Category, &f.FactText, &f.Importance, &f.ThreadID, &f.CreatedAt); err != nil {
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
