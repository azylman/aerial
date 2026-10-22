package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"time"

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

// MessageMetadata stores structured Discord and source metadata preserved throughout the lifecycle.
type MessageMetadata struct {
	ChannelID         string   `json:"channel_id,omitempty"`
	ChannelName       string   `json:"channel_name,omitempty"`
	TargetThreadID    string   `json:"target_thread_id,omitempty"`
	ThreadName        string   `json:"thread_name,omitempty"`
	GuildID           string   `json:"guild_id,omitempty"`
	AuthorUsername    string   `json:"author_username,omitempty"`
	AuthorGlobalName  string   `json:"author_global_name,omitempty"`
	AuthorBot         bool     `json:"author_bot,omitempty"`
	IsAdmin           bool     `json:"is_admin,omitempty"`
	Mentions          []string `json:"mentions,omitempty"`
	MentionUserIDs    []string `json:"mention_user_ids,omitempty"`
	MentionRoleIDs    []string `json:"mention_role_ids,omitempty"`
	ReplyingToAuthor  string   `json:"replying_to_author,omitempty"`
	ReplyingToContent string   `json:"replying_to_content,omitempty"`
	Attachments       []string `json:"attachments,omitempty"`
	Model             string   `json:"model,omitempty"`
	Silent            bool     `json:"silent,omitempty"`
	VanadisChunkID    string   `json:"vanadis_chunk_id,omitempty"`
}

// Scan implements sql.Scanner for PostgreSQL JSONB.
func (m *MessageMetadata) Scan(src any) error {
	if src == nil {
		*m = MessageMetadata{}
		return nil
	}
	var data []byte
	switch v := src.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("cannot scan type %T into MessageMetadata", src)
	}
	if len(data) == 0 || string(data) == "{}" || string(data) == "null" {
		*m = MessageMetadata{}
		return nil
	}
	return json.Unmarshal(data, m)
}

// Message represents a stored turn message.
type Message struct {
	ID           string          `json:"id"`
	RowID        int64           `json:"row_id,omitempty"`
	ThreadID     string          `json:"thread_id"`
	Content      string          `json:"content"`
	ResponseText string          `json:"response_text,omitempty"`
	Status       string          `json:"status"`
	Metadata     MessageMetadata `json:"metadata"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// IsExtractableAdminTurn returns true if the message is completed, from an admin, and has non-empty, non-bracket response text.
func IsExtractableAdminTurn(m Message) bool {
	if m.Status != "COMPLETED" || !m.Metadata.IsAdmin {
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
	UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error
	GetThreadContextBeforeWatermark(ctx context.Context, threadID string, priorTurnLimit int) (string, []Message, error)
	GetThreadTrajectoryContext(ctx context.Context, threadID string) (retrievedFacts []string, toolCalls []string, err error)
	FindNearestScoredFacts(ctx context.Context, embedding []float32, limit int, minSim float64) ([]ScoredFact, error)
	InsertProceduralCandidate(ctx context.Context, cand ProceduralCandidate) (int64, error)
	GetPendingProceduralCandidates(ctx context.Context) ([]ProceduralCandidate, error)
	MarkProceduralCandidatesConsolidated(ctx context.Context, ids []int64, prNumber int) error
	MarkProceduralCandidatesSkipped(ctx context.Context, ids []int64) error
}

// SQLStore implements Store backed by PostgreSQL.
type SQLStore struct {
	db DBTX
}

// NewSQLStore creates a new SQLStore instance.
func NewSQLStore(db DBTX) *SQLStore {
	return &SQLStore{db: db}
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

func closeWarn(c io.Closer, resource string) {
	if c != nil {
		if err := c.Close(); err != nil {
			log.Printf("[Memory DB] Warning closing %s: %v", resource, err)
		}
	}
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

// GetActiveConversationsForExtraction finds threads with unextracted admin turns in the past activeHours.
func (s *SQLStore) GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if activeHours <= 0 {
		activeHours = 24
	}
	queryCtx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `SELECT DISTINCT m.thread_id FROM messages m LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.thread_id != '' AND m.created_at >= $1 AND m.status = 'COMPLETED' AND COALESCE(TRIM(m.response_text), '') != '' AND TRIM(m.response_text) NOT LIKE '[%%'
	  AND (s.last_extracted_rowid IS NULL OR m.row_id > s.last_extracted_rowid) AND (m.metadata->>'is_admin')::boolean IS TRUE ORDER BY m.thread_id LIMIT 20`
	rows, err := s.db.QueryContext(queryCtx, query, time.Now().UTC().Add(-time.Duration(activeHours)*time.Hour))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var threadIDs []string
	for rows.Next() {
		var tid string
		if err := rows.Scan(&tid); err != nil {
			return nil, err
		}
		if tid = strings.TrimSpace(tid); tid != "" {
			threadIDs = append(threadIDs, tid)
		}
	}
	return threadIDs, rows.Err()
}

// GetUnextractedAdminTurns retrieves completed admin turns for threadID that haven't been extracted.
func (s *SQLStore) GetUnextractedAdminTurns(ctx context.Context, threadID string) ([]Message, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return nil, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT m.id, m.row_id, m.thread_id, m.content, COALESCE(m.response_text, ''), m.status, m.metadata FROM messages m LEFT JOIN sessions s ON m.thread_id = s.thread_id
	WHERE m.thread_id = $1 AND m.status = 'COMPLETED' AND COALESCE(TRIM(m.response_text), '') != '' AND TRIM(m.response_text) NOT LIKE '[%%'
	  AND (s.last_extracted_rowid IS NULL OR m.row_id > s.last_extracted_rowid) AND (m.metadata->>'is_admin')::boolean IS TRUE ORDER BY m.row_id ASC LIMIT 50`

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

// GetMaxMessageRowID returns the maximum row ID of completed messages for a thread.
func (s *SQLStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	if threadID == "" {
		return 0, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var maxRowID sql.NullInt64
	err := s.db.QueryRowContext(queryCtx, `SELECT MAX(row_id) FROM messages WHERE thread_id = $1 AND status = 'COMPLETED'`, threadID).Scan(&maxRowID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return maxRowID.Int64, nil
}

// UpdateConversationFactWatermark updates or inserts watermark for thread fact extraction.
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

// GetThreadContextBeforeWatermark returns the thread summary and up to priorTurnLimit prior admin turns before the extraction watermark.
func (s *SQLStore) GetThreadContextBeforeWatermark(ctx context.Context, threadID string, priorTurnLimit int) (string, []Message, error) {
	if err := s.checkDB(); err != nil {
		return "", nil, err
	}
	if threadID = strings.TrimSpace(threadID); threadID == "" {
		return "", nil, nil
	}
	if priorTurnLimit <= 0 {
		priorTurnLimit = 2
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var summary string
	var lastExtractedRowID int64
	sessQuery := `SELECT COALESCE(summary, ''), COALESCE(last_extracted_rowid, 0) FROM sessions WHERE thread_id = $1`
	err := s.db.QueryRowContext(queryCtx, sessQuery, threadID).Scan(&summary, &lastExtractedRowID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("failed to query session context: %w", err)
	}

	if lastExtractedRowID <= 0 {
		return summary, nil, nil
	}

	msgQuery := `SELECT id, row_id, thread_id, content, COALESCE(response_text, ''), status, metadata
	FROM messages
	WHERE thread_id = $1 AND row_id <= $2 AND status = 'COMPLETED'
	  AND COALESCE(TRIM(response_text), '') != '' AND TRIM(response_text) NOT LIKE '[%'
	  AND (metadata->>'is_admin')::boolean IS TRUE
	ORDER BY row_id DESC LIMIT $3`

	rows, err := s.db.QueryContext(queryCtx, msgQuery, threadID, lastExtractedRowID, priorTurnLimit)
	if err != nil {
		return summary, nil, fmt.Errorf("failed to query prior turns: %w", err)
	}
	defer rows.Close()

	var priorTurns []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.RowID, &m.ThreadID, &m.Content, &m.ResponseText, &m.Status, &m.Metadata); err != nil {
			return summary, nil, fmt.Errorf("failed to scan prior turn: %w", err)
		}
		priorTurns = append(priorTurns, m)
	}
	if err := rows.Err(); err != nil {
		return summary, nil, fmt.Errorf("failed to read prior turns rows: %w", err)
	}

	for i, j := 0, len(priorTurns)-1; i < j; i, j = i+1, j-1 {
		priorTurns[i], priorTurns[j] = priorTurns[j], priorTurns[i]
	}

	return summary, priorTurns, nil
}

var (
	reRetrievedMemoryBlock = regexp.MustCompile(`(?is)<\s*\\?retrieved_memory\s*>(.*?)<\s*[\\/]+retrieved_memory\s*>`)
	reFactLine             = regexp.MustCompile(`(?m)^\s*(-\s*\[id:\d+\].*)$`)
	reSkillPath            = regexp.MustCompile(`/skills/(?:(?:common|discord|voice|ephemeral)/)?([a-z0-9-]+)/SKILL\.md`)
	reSkillYAML            = regexp.MustCompile(`(?m)(?:1:\s*)?---\s*(?:\n|\s+)(?:2:\s*)?name:\s*([a-z0-9-]+)`)
	reSkillLoaded          = regexp.MustCompile(`\[SKILL_LOADED:\s*([a-z0-9-]+)\]`)
)

// ExtractLoadedSkillName extracts a skill name from trajectory step content or tool parameters.
func ExtractLoadedSkillName(stepType, toolName, content string) string {
	cleanType := strings.ToUpper(strings.TrimSpace(stepType))
	cleanTool := strings.ToLower(strings.TrimSpace(toolName))

	normalized := strings.ReplaceAll(content, "\\", "/")
	if cleanType == "PLANNER_RESPONSE" || cleanTool == "read_file" || strings.Contains(normalized, "/skills/") {
		if m := reSkillPath.FindStringSubmatch(normalized); len(m) > 1 {
			return strings.TrimSpace(m[1])
		}
	}
	if cleanType == "GENERIC" || cleanType == "TOOL" || cleanType == "RUN_COMMAND" || strings.Contains(content, "---") {
		if m := reSkillYAML.FindStringSubmatch(content); len(m) > 1 {
			return strings.TrimSpace(m[1])
		}
		if m := reSkillPath.FindStringSubmatch(normalized); len(m) > 1 {
			return strings.TrimSpace(m[1])
		}
	}
	return ""
}

// ExtractLoadedSkillsFromCalls extracts deduplicated loaded skill names from tool calls.
func ExtractLoadedSkillsFromCalls(toolCalls []string) []string {
	var skills []string
	seen := make(map[string]struct{})
	for _, call := range toolCalls {
		for _, m := range reSkillLoaded.FindAllStringSubmatch(call, -1) {
			if len(m) > 1 {
				name := strings.TrimSpace(m[1])
				if name != "" {
					if _, exists := seen[name]; !exists {
						seen[name] = struct{}{}
						skills = append(skills, name)
					}
				}
			}
		}
	}
	return skills
}

func truncateSingleLine(s string, maxLen int) string {
	s = strings.Join(strings.Fields(s), " ")
	if maxLen > 0 && len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

func extractTrajectorySignals(stepType, toolName, content string) ([]string, string) {
	switch stepType {
	case "USER_INPUT":
		var facts []string
		seen := make(map[string]struct{})
		for _, block := range reRetrievedMemoryBlock.FindAllStringSubmatch(content, -1) {
			if len(block) > 1 {
				for _, lineMatch := range reFactLine.FindAllStringSubmatch(block[1], -1) {
					if len(lineMatch) > 1 {
						line := strings.TrimSpace(lineMatch[1])
						if line != "" {
							if _, exists := seen[line]; !exists {
								seen[line] = struct{}{}
								facts = append(facts, line)
							}
						}
					}
				}
			}
		}
		return facts, ""
	case "PLANNER_RESPONSE":
		trimmedTool := strings.TrimSpace(toolName)
		if trimmedTool != "" {
			skillName := ExtractLoadedSkillName(stepType, toolName, content)
			callStr := fmt.Sprintf("%s(%s)", trimmedTool, truncateSingleLine(content, 200))
			if skillName != "" {
				return nil, fmt.Sprintf("%s [SKILL_LOADED: %s]", callStr, skillName)
			}
			return nil, callStr
		}
	case "GENERIC", "TOOL", "RUN_COMMAND":
		skillName := ExtractLoadedSkillName(stepType, toolName, content)
		if skillName != "" {
			return nil, fmt.Sprintf("[SKILL_LOADED: %s]", skillName)
		}
		trimmedTool := strings.TrimSpace(toolName)
		if trimmedTool == "" {
			trimmedTool = strings.ToLower(stepType)
		}
		return nil, fmt.Sprintf("-> OUTPUT(%s): %s", trimmedTool, truncateSingleLine(content, 200))
	}
	return nil, ""
}

// GetThreadTrajectoryContext queries the trajectory context (retrieved facts and tool calls) for an active thread.
func (s *SQLStore) GetThreadTrajectoryContext(ctx context.Context, threadID string) (retrievedFacts []string, toolCalls []string, err error) {
	if err := s.checkDB(); err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(threadID) == "" {
		return nil, nil, nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT ts.step_type, ts.tool_name, LEFT(ts.content, 500)
	FROM transcript_steps ts
	JOIN sessions s ON s.thread_id = $1
	WHERE (ts.session_id = s.internal_session_id OR ts.session_id = s.previous_session_id)
	  AND (s.fact_extracted_at IS NULL OR ts.created_at >= s.fact_extracted_at)
	  AND (ts.step_type IN ('USER_INPUT', 'GENERIC', 'TOOL', 'RUN_COMMAND') OR (ts.step_type = 'PLANNER_RESPONSE' AND ts.tool_name != ''))
	ORDER BY ts.created_at ASC, ts.step_index ASC
	LIMIT 50`

	rows, err := s.db.QueryContext(queryCtx, query, threadID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query thread trajectory context: %w", err)
	}
	defer rows.Close()

	seenFacts := make(map[string]struct{})
	for rows.Next() {
		var stepType, toolName, content string
		if err := rows.Scan(&stepType, &toolName, &content); err != nil {
			return nil, nil, fmt.Errorf("failed to scan trajectory step: %w", err)
		}
		rf, tc := extractTrajectorySignals(stepType, toolName, content)
		for _, f := range rf {
			if _, exists := seenFacts[f]; !exists {
				seenFacts[f] = struct{}{}
				retrievedFacts = append(retrievedFacts, f)
			}
		}
		if tc != "" {
			toolCalls = append(toolCalls, tc)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to read trajectory rows: %w", err)
	}
	return retrievedFacts, toolCalls, nil
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

// InsertProceduralCandidate persists a staged procedural candidate into the procedural_candidates table.
func (s *SQLStore) InsertProceduralCandidate(ctx context.Context, cand ProceduralCandidate) (int64, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	trigger := strings.TrimSpace(cand.Trigger)
	if trigger == "" {
		return 0, fmt.Errorf("trigger pattern cannot be empty")
	}
	stepsBytes, err := json.Marshal(cand.Steps)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal steps: %w", err)
	}
	if cand.Steps == nil {
		stepsBytes = []byte("[]")
	}
	avoidBytes, err := json.Marshal(cand.Avoid)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal avoid: %w", err)
	}
	if cand.Avoid == nil {
		avoidBytes = []byte("[]")
	}
	domain := strings.TrimSpace(cand.Domain)
	if domain == "" {
		domain = "discord"
	}
	status := strings.TrimSpace(cand.Status)
	if status == "" {
		status = "pending"
	}

	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	var prNum any
	if cand.PRNumber > 0 {
		prNum = cand.PRNumber
	}

	query := `INSERT INTO procedural_candidates (thread_id, domain, loaded_skill, trigger_pattern, steps_json, avoid_json, embedding, status, pr_number)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	RETURNING id`

	var id int64
	if err := s.db.QueryRowContext(queryCtx, query,
		cand.ThreadID,
		domain,
		cand.LoadedSkill,
		trigger,
		string(stepsBytes),
		string(avoidBytes),
		pgvector.NewVector(cand.Embedding),
		status,
		prNum,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("failed to insert procedural candidate: %w", err)
	}
	return id, nil
}

// GetPendingProceduralCandidates returns up to 100 pending candidates ordered by created_at ASC, id ASC.
func (s *SQLStore) GetPendingProceduralCandidates(ctx context.Context) ([]ProceduralCandidate, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT id, thread_id, domain, loaded_skill, trigger_pattern, steps_json, avoid_json, embedding, status, COALESCE(pr_number, 0), created_at
	FROM procedural_candidates
	WHERE status = 'pending'
	ORDER BY created_at ASC, id ASC
	LIMIT 100`

	rows, err := s.db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending procedural candidates: %w", err)
	}
	defer rows.Close()

	var candidates []ProceduralCandidate
	for rows.Next() {
		var (
			cand      ProceduralCandidate
			stepsRaw  []byte
			avoidRaw  []byte
			vec       pgvector.Vector
			createdAt FactTime
		)
		if err := rows.Scan(
			&cand.ID,
			&cand.ThreadID,
			&cand.Domain,
			&cand.LoadedSkill,
			&cand.Trigger,
			&stepsRaw,
			&avoidRaw,
			&vec,
			&cand.Status,
			&cand.PRNumber,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan procedural candidate: %w", err)
		}
		cand.CreatedAt = createdAt.Time
		cand.Embedding = vec.Slice()
		if len(stepsRaw) > 0 {
			if err := json.Unmarshal(stepsRaw, &cand.Steps); err != nil {
				cand.Steps = []string{}
			}
		} else {
			cand.Steps = []string{}
		}
		if len(avoidRaw) > 0 {
			if err := json.Unmarshal(avoidRaw, &cand.Avoid); err != nil {
				cand.Avoid = []string{}
			}
		} else {
			cand.Avoid = []string{}
		}
		candidates = append(candidates, cand)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read procedural candidate rows: %w", err)
	}
	return candidates, nil
}

// MarkProceduralCandidatesConsolidated transitions candidate status to 'consolidated' and records the created PR number.
func (s *SQLStore) MarkProceduralCandidatesConsolidated(ctx context.Context, ids []int64, prNumber int) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids)+1)
	args = append(args, prNumber)
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, id)
	}

	query := fmt.Sprintf(`UPDATE procedural_candidates
	SET status = 'consolidated', pr_number = $1, updated_at = CURRENT_TIMESTAMP
	WHERE id IN (%s)`, strings.Join(placeholders, ", "))

	if _, err := s.db.ExecContext(queryCtx, query, args...); err != nil {
		return fmt.Errorf("failed to mark procedural candidates consolidated: %w", err)
	}
	return nil
}

// MarkProceduralCandidatesSkipped transitions candidate status to 'skipped'.
func (s *SQLStore) MarkProceduralCandidatesSkipped(ctx context.Context, ids []int64) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args = append(args, id)
	}

	query := fmt.Sprintf(`UPDATE procedural_candidates
	SET status = 'skipped', updated_at = CURRENT_TIMESTAMP
	WHERE id IN (%s)`, strings.Join(placeholders, ", "))

	if _, err := s.db.ExecContext(queryCtx, query, args...); err != nil {
		return fmt.Errorf("failed to mark procedural candidates skipped: %w", err)
	}
	return nil
}

// InitDB initializes database connection based on config.
func InitDB(cfg *Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	return initDB(cfg.DatabaseURL)
}

func initDB(connStr string) (*sql.DB, error) {
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
		closeWarn(db, "database on ping failure")
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return db, nil
}
