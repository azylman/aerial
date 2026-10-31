package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pgvector/pgvector-go"
)

var (
	// ErrFactNotFound indicates that the requested fact does not exist in the store.
	ErrFactNotFound = errors.New("fact not found")

	sqliteTestInitHook func(string) (*sql.DB, error)
)

const (
	DefaultDenseWeight    = 0.70
	DefaultSparseWeight   = 0.30
	DefaultRRFK           = 60.0
	DefaultMinScore       = 0.20
	DefaultMaxFacts       = 10
	DefaultCandidateLimit = 30
	ExpectedEmbeddingDim  = 384
)

// Fact represents a stored piece of semantic knowledge.
type Fact struct {
	ID               int64     `json:"id"`
	FactText         string    `json:"fact_text"`
	CreatedAt        time.Time `json:"created_at"`
	LastReinforcedAt time.Time `json:"last_reinforced_at"`
	ReinforceCount   int       `json:"reinforce_count"`
}

// ScoredFact couples a fact with its retrieval relevance score.
type ScoredFact struct {
	Fact  Fact    `json:"fact"`
	Score float64 `json:"score"`
}

// ScoredFactWithEmbedding holds a candidate fact, its vector embedding, and hybrid retrieval score.
type ScoredFactWithEmbedding struct {
	Fact      Fact
	Embedding []float32
	Score     float64
}

// ProceduralCandidate represents a staged procedural skill pattern extracted from trajectory friction.
type ProceduralCandidate struct {
	ID          int64     `json:"id"`
	ThreadID    string    `json:"thread_id"`
	Domain      string    `json:"domain"`
	LoadedSkill string    `json:"loaded_skill"`
	Trigger     string    `json:"trigger"`
	Steps       []string  `json:"steps"`
	Avoid       []string  `json:"avoid"`
	Embedding   []float32 `json:"-"`
	Status      string    `json:"status"`
	PRNumber    int       `json:"pr_number,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// BlockedCoreSkills defines immutable core aerial and superpower skill names blocked from dynamic mutation.
var BlockedCoreSkills = map[string]struct{}{
	"self-improvement":               {},
	"incident-triage":                {},
	"transcript-search":              {},
	"brainstorming":                  {},
	"writing-plans":                  {},
	"test-driven-development":        {},
	"systematic-debugging":           {},
	"verification-before-completion": {},
	"receiving-code-review":          {},
	"dispatching-parallel-agents":    {},
	"writing-skills":                 {},
	"agy-customizations":             {},
	"antigravity-guide":              {},
}

// FactsFilter defines filtering options for paginated fact queries.
type FactsFilter struct {
	Query  string `json:"query"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

// FactsResult represents the paginated facts response contract.
type FactsResult struct {
	Facts  []Fact `json:"facts"`
	Total  int    `json:"total"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
	Status string `json:"status,omitempty"`
}

// Message represents a stored turn message.
type Message struct {
	ID           string    `json:"id"`
	RowID        int64     `json:"row_id,omitempty"`
	ThreadID     string    `json:"thread_id"`
	Content      string    `json:"content"`
	ResponseText string    `json:"response_text,omitempty"`
	Status       string    `json:"status"`
	IsAdmin      bool      `json:"is_admin,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// IsExtractableAdminTurn returns true if the message is completed, from an admin, and has non-empty, non-bracket response text.
func IsExtractableAdminTurn(m Message) bool {
	if m.Status != "COMPLETED" || !m.IsAdmin {
		return false
	}
	resp := strings.TrimSpace(m.ResponseText)
	return resp != "" && !strings.HasPrefix(resp, "[")
}

// DBTX abstracts common SQL execution methods shared by *sql.DB and *sql.Tx.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Store defines persistent storage operations for facts and procedural candidates.
type Store interface {
	InsertFact(ctx context.Context, factText string, embedding []float32) (int64, error)
	UpdateFact(ctx context.Context, id int64, factText string, embedding []float32) error
	ReinforceFact(ctx context.Context, id int64) error
	DeleteFact(ctx context.Context, id int64) error
	FindDuplicateFact(ctx context.Context, embedding []float32, minSim float64) (*Fact, float64, error)
	SearchCandidateFacts(ctx context.Context, embedding []float32, queryText string, candidateLimit int, minScore float64) ([]ScoredFactWithEmbedding, error)
	GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error)
	GetAllFactsWithEmbeddings(ctx context.Context) ([]ScoredFactWithEmbedding, error)
	GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error)
	GetUnextractedAdminTurns(ctx context.Context, threadID string) ([]Message, error)
	GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error)
	GetConversationFactWatermark(ctx context.Context, threadID string) (int64, *time.Time, error)
	UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error
	GetThreadContextBeforeWatermark(ctx context.Context, threadID string, priorTurnLimit int) (string, []Message, error)
	GetThreadTrajectoryContext(ctx context.Context, threadID string) (retrievedFacts []string, toolCalls []string, err error)
	FindNearestScoredFacts(ctx context.Context, embedding []float32, limit int, minSim float64) ([]ScoredFact, error)
	InsertProceduralCandidate(ctx context.Context, cand ProceduralCandidate) (int64, error)
	GetPendingProceduralCandidates(ctx context.Context) ([]ProceduralCandidate, error)
	MarkProceduralCandidatesConsolidated(ctx context.Context, ids []int64, prNumber int) error
	MarkProceduralCandidatesSkipped(ctx context.Context, ids []int64) error
}

// SQLStore implements Store backed by PostgreSQL and Eri HTTP APIs.
type SQLStore struct {
	db         DBTX
	brainURL   string
	httpClient *http.Client
	brainToken string
}

// NewSQLStore creates a new SQLStore instance using default Brain URL.
func NewSQLStore(db DBTX) *SQLStore {
	return NewSQLStoreWithBrainURL(db, DefaultBrainURL, nil)
}

// NewSQLStoreWithBrainURL creates a new SQLStore instance with custom Brain URL and HTTP client.
func NewSQLStoreWithBrainURL(db DBTX, brainURL string, client *http.Client) *SQLStore {
	cleanURL := strings.TrimRight(strings.TrimSpace(brainURL), "/")
	if cleanURL == "" {
		cleanURL = DefaultBrainURL
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &SQLStore{
		db:         db,
		brainURL:   cleanURL,
		httpClient: client,
	}
}

// SetBrainURL sets the Brain URL on an existing SQLStore.
func (s *SQLStore) SetBrainURL(brainURL string) {
	cleanURL := strings.TrimRight(strings.TrimSpace(brainURL), "/")
	if cleanURL == "" {
		cleanURL = DefaultBrainURL
	}
	s.brainURL = cleanURL
}

// SetBrainToken sets the Brain authentication token on an existing SQLStore.
func (s *SQLStore) SetBrainToken(token string) {
	if s != nil {
		s.brainToken = strings.TrimSpace(token)
	}
}

func (s *SQLStore) getBrainURL() string {
	if s != nil && s.brainURL != "" {
		return s.brainURL
	}
	return DefaultBrainURL
}

func (s *SQLStore) getHTTPClient() *http.Client {
	if s != nil && s.httpClient != nil {
		return s.httpClient
	}
	return &http.Client{Timeout: 15 * time.Second}
}

func (s *SQLStore) checkDB() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database connection is not initialized")
	}
	return nil
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, d)
}

// EscapeSQLLike escapes characters with special meaning in SQL LIKE clauses.
func EscapeSQLLike(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	return strings.ReplaceAll(s, "_", "\\_")
}

// FactTime scans timestamps cleanly across drivers.
type FactTime struct {
	Time  time.Time
	Valid bool
}

func (ft *FactTime) Scan(src any) error {
	*ft = FactTime{}
	if src == nil {
		return nil
	}
	switch v := src.(type) {
	case *time.Time:
		if v == nil {
			*ft = FactTime{}
			return nil
		}
		ft.Time, ft.Valid = v.UTC(), true
		return nil
	case time.Time:
		ft.Time, ft.Valid = v.UTC(), true
		return nil
	case string:
		if strings.TrimSpace(v) == "" {
			*ft = FactTime{}
			return nil
		}
		layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05-07", "2006-01-02 15:04:05"}
		for _, l := range layouts {
			if parsed, err := time.Parse(l, v); err == nil {
				ft.Time, ft.Valid = parsed.UTC(), true
				return nil
			}
		}
		return fmt.Errorf("cannot parse %q into FactTime", v)
	}
	return nil
}

// InsertFact persists a new fact into the database.
func (s *SQLStore) InsertFact(ctx context.Context, factText string, embedding []float32) (int64, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	factText = strings.TrimSpace(factText)
	if factText == "" {
		return 0, fmt.Errorf("fact text cannot be empty")
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return 0, fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(embedding))
	}
	now := time.Now().UTC()
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO facts (fact_text, embedding, created_at, last_reinforced_at, reinforce_count) VALUES ($1, $2, $3, $3, 1) RETURNING id`
	var id int64
	err := s.db.QueryRowContext(queryCtx, query, factText, pgvector.NewVector(embedding), now).Scan(&id)
	return id, err
}

// UpdateFact updates an existing fact with new values and increments reinforce_count.
func (s *SQLStore) UpdateFact(ctx context.Context, id int64, factText string, embedding []float32) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	factText = strings.TrimSpace(factText)
	if factText == "" {
		return fmt.Errorf("fact text cannot be empty")
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(embedding))
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `UPDATE facts SET fact_text = $1, embedding = $2, last_reinforced_at = NOW(), reinforce_count = COALESCE(reinforce_count, 1) + 1 WHERE id = $3`
	res, err := s.db.ExecContext(queryCtx, query, factText, pgvector.NewVector(embedding), id)
	if err != nil {
		return fmt.Errorf("failed to update fact: %w", err)
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

// ReinforceFact strengthens an existing fact by updating last_reinforced_at and incrementing reinforce_count.
func (s *SQLStore) ReinforceFact(ctx context.Context, id int64) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	now := time.Now().UTC()
	query := `UPDATE facts SET last_reinforced_at = $1, reinforce_count = COALESCE(reinforce_count, 1) + 1 WHERE id = $2`
	res, err := s.db.ExecContext(queryCtx, query, now, id)
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

// DeleteFact removes a fact by ID.
func (s *SQLStore) DeleteFact(ctx context.Context, id int64) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	res, err := s.db.ExecContext(queryCtx, `DELETE FROM facts WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to delete fact: %w", err)
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

// FindDuplicateFact finds an existing fact highly similar to the provided embedding vector.
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

	query := `SELECT id, fact_text, created_at, COALESCE(last_reinforced_at, created_at), COALESCE(reinforce_count, 1), (1.0 - (embedding <=> $1)) AS similarity FROM facts WHERE embedding IS NOT NULL ORDER BY embedding <=> $1 LIMIT 1;`
	var f Fact
	var sim float64
	var createdAt, lastReinforced FactTime
	err := s.db.QueryRowContext(queryCtx, query, pgvector.NewVector(embedding)).Scan(
		&f.ID, &f.FactText, &createdAt, &lastReinforced, &f.ReinforceCount, &sim,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("failed to find duplicate fact: %w", err)
	}
	f.CreatedAt, f.LastReinforcedAt = createdAt.Time, lastReinforced.Time
	if sim >= minSim {
		return &f, sim, nil
	}
	return nil, sim, nil
}

// SearchCandidateFacts performs Hybrid RRF retrieval over stored facts with read-time power-law activation scoring.
func (s *SQLStore) SearchCandidateFacts(ctx context.Context, embedding []float32, queryText string, candidateLimit int, minScore float64) ([]ScoredFactWithEmbedding, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryText = strings.TrimSpace(queryText)
	if len(embedding) != ExpectedEmbeddingDim || queryText == "" {
		return nil, nil
	}
	if candidateLimit <= 0 {
		candidateLimit = DefaultCandidateLimit
	}
	if minScore <= 0 {
		minScore = DefaultMinScore
	}

	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `WITH vector_hits AS (
	SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> $1) AS rank
	FROM facts
	ORDER BY embedding <=> $1 LIMIT $2
), text_hits AS (
	SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $3)) DESC) AS rank
	FROM facts
	WHERE fts_tokens @@ websearch_to_tsquery('simple', $3)
	LIMIT $2
), rrf AS (
	SELECT f.id, f.fact_text, f.created_at,
	       COALESCE(f.last_reinforced_at, f.created_at) AS last_reinforced_at,
	       COALESCE(f.reinforce_count, 1) AS reinforce_count,
	       f.embedding,
	       ((COALESCE($4::float8 / ($6::float8 + v.rank::float8), 0.0) + COALESCE($5::float8 / ($6::float8 + t.rank::float8), 0.0)) * $6::float8)
	       * (1.0 + 0.15 * LN(1.0 + COALESCE(f.reinforce_count, 1)::float8))
	       * POWER(1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - COALESCE(f.last_reinforced_at, f.created_at))) / 86400.0) / 14.0, -0.35) AS score
	FROM vector_hits v FULL OUTER JOIN text_hits t ON v.id = t.id JOIN facts f ON f.id = COALESCE(v.id, t.id)
)
SELECT id, fact_text, created_at, last_reinforced_at, reinforce_count, embedding, score
FROM rrf
WHERE score >= $7
ORDER BY score DESC LIMIT $2;`

	rows, err := s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), candidateLimit, queryText, DefaultDenseWeight, DefaultSparseWeight, DefaultRRFK, minScore)
	if err != nil {
		return nil, fmt.Errorf("candidate search failed: %w", err)
	}
	defer rows.Close()

	var results []ScoredFactWithEmbedding
	for rows.Next() {
		var f Fact
		var vec pgvector.Vector
		var score float64
		var createdAt, lastReinforced FactTime
		if err := rows.Scan(
			&f.ID, &f.FactText, &createdAt, &lastReinforced,
			&f.ReinforceCount, &vec, &score,
		); err != nil {
			return nil, fmt.Errorf("failed to scan candidate fact: %w", err)
		}
		f.CreatedAt = createdAt.Time
		f.LastReinforcedAt = lastReinforced.Time
		results = append(results, ScoredFactWithEmbedding{
			Fact:      f,
			Embedding: vec.Slice(),
			Score:     score,
		})
	}
	return results, rows.Err()
}

// GetFactsPaginated retrieves facts matching the filter with total count.
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
	if q := strings.TrimSpace(filter.Query); q != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("LOWER(fact_text) LIKE LOWER($%d) ESCAPE '\\'", argIdx))
		args = append(args, "%"+EscapeSQLLike(q)+"%")
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

	paginationSQL := ""
	limit := filter.Limit
	if limit > 0 {
		paginationSQL = fmt.Sprintf(" LIMIT %d OFFSET %d", limit, filter.Offset)
	} else if filter.Offset > 0 {
		paginationSQL = fmt.Sprintf(" OFFSET %d", filter.Offset)
	}

	selectQuery := fmt.Sprintf(`SELECT id, fact_text, created_at, COALESCE(last_reinforced_at, created_at) AS last_reinforced_at, COALESCE(reinforce_count, 1) AS reinforce_count FROM facts%s ORDER BY COALESCE(reinforce_count, 1) DESC, COALESCE(last_reinforced_at, created_at) DESC, id DESC%s`, whereSQL, paginationSQL)
	rows, err := s.db.QueryContext(queryCtx, selectQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query facts: %w", err)
	}
	defer rows.Close()

	facts, err := scanFacts(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to scan fact: %w", err)
	}

	return &FactsResult{
		Facts:  facts,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
		Status: "ok",
	}, nil
}

func scanFacts(rows *sql.Rows) ([]Fact, error) {
	if rows == nil {
		return nil, nil
	}
	facts := make([]Fact, 0)
	for rows.Next() {
		var f Fact
		var createdAt, lastReinforced FactTime
		if err := rows.Scan(
			&f.ID, &f.FactText,
			&createdAt, &lastReinforced,
			&f.ReinforceCount,
		); err != nil {
			return nil, err
		}
		f.CreatedAt, f.LastReinforcedAt = createdAt.Time, lastReinforced.Time
		facts = append(facts, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return facts, nil
}

// FindNearestScoredFacts returns top-K facts with cosine similarity >= minSim along with their scores.
func (s *SQLStore) FindNearestScoredFacts(ctx context.Context, embedding []float32, limit int, minSim float64) ([]ScoredFact, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return nil, nil
	}
	if limit <= 0 {
		limit = 5
	}
	if minSim <= 0 {
		minSim = 0.60
	}
	candidateLimit := limit * 3
	if candidateLimit < 15 {
		candidateLimit = 15
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `WITH candidates AS (
		SELECT id, fact_text, created_at,
		       COALESCE(last_reinforced_at, created_at) AS last_reinforced_at,
		       COALESCE(reinforce_count, 1) AS reinforce_count,
		       (1.0 - (embedding <=> $1)) AS similarity
		FROM facts
		WHERE embedding IS NOT NULL
		ORDER BY embedding <=> $1
		LIMIT $2
	)
	SELECT id, fact_text, created_at, last_reinforced_at, reinforce_count, similarity
	FROM candidates
	WHERE similarity >= $3
	ORDER BY similarity DESC
	LIMIT $4;`

	rows, err := s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), candidateLimit, minSim, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to find nearest scored facts: %w", err)
	}
	defer rows.Close()

	var results []ScoredFact
	for rows.Next() {
		var f Fact
		var sim float64
		var createdAt, lastReinforced FactTime
		if err := rows.Scan(
			&f.ID, &f.FactText,
			&createdAt, &lastReinforced,
			&f.ReinforceCount, &sim,
		); err != nil {
			return nil, fmt.Errorf("failed to scan nearest scored fact: %w", err)
		}
		f.CreatedAt = createdAt.Time
		f.LastReinforcedAt = lastReinforced.Time
		results = append(results, ScoredFact{
			Fact:  f,
			Score: sim,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read nearest scored facts rows: %w", err)
	}
	return results, nil
}

// GetAllFactsWithEmbeddings retrieves all facts that possess a vector embedding for clustering and consolidation.
func (s *SQLStore) GetAllFactsWithEmbeddings(ctx context.Context) ([]ScoredFactWithEmbedding, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `SELECT id, fact_text, created_at,
		       COALESCE(last_reinforced_at, created_at),
		       COALESCE(reinforce_count, 1),
		       embedding
		FROM facts
		WHERE embedding IS NOT NULL
		ORDER BY id ASC`

	rows, err := s.db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query facts with embeddings: %w", err)
	}
	defer rows.Close()

	var results []ScoredFactWithEmbedding
	for rows.Next() {
		var f Fact
		var vec pgvector.Vector
		var createdAt, lastReinforced FactTime
		if err := rows.Scan(
			&f.ID, &f.FactText,
			&createdAt, &lastReinforced,
			&f.ReinforceCount, &vec,
		); err != nil {
			return nil, fmt.Errorf("failed to scan fact with embedding: %w", err)
		}
		f.CreatedAt = createdAt.Time
		f.LastReinforcedAt = lastReinforced.Time
		results = append(results, ScoredFactWithEmbedding{
			Fact:      f,
			Embedding: vec.Slice(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read facts with embeddings rows: %w", err)
	}
	return results, nil
}

// OpenDB initializes database connection based on config.
func OpenDB(cfg *Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	return openDB(cfg.DatabaseURL)
}

func openDB(connStr string) (*sql.DB, error) {
	if sqliteTestInitHook != nil {
		return sqliteTestInitHook(connStr)
	}
	clean := strings.TrimSpace(connStr)
	if clean == "" {
		return nil, fmt.Errorf("database connection string is required")
	}
	db, err := sql.Open("pgx", clean)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		httpkit.CloseWarn(db, "database on ping failure")
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return db, nil
}
