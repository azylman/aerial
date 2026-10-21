package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

// RegisterSQLiteTestHook allows unit tests to inject in-memory databases.
func RegisterSQLiteTestHook(fn func(string) (*sql.DB, error)) {
	sqliteTestInitHook = fn
}

// Fact represents a stored piece of semantic knowledge.
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

// FactsFilter defines filtering options for paginated fact queries.
type FactsFilter struct {
	Category string `json:"category"`
	Query    string `json:"query"`
	Limit    int    `json:"limit"`
	Offset   int    `json:"offset"`
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

// IsEmpty returns true if all metadata fields are zero/empty.
func (m MessageMetadata) IsEmpty() bool {
	return m.ChannelID == "" && m.ChannelName == "" && m.TargetThreadID == "" && m.ThreadName == "" &&
		m.GuildID == "" && m.AuthorUsername == "" && m.AuthorGlobalName == "" && !m.AuthorBot && !m.IsAdmin &&
		len(m.Mentions) == 0 && len(m.MentionUserIDs) == 0 && len(m.MentionRoleIDs) == 0 &&
		m.ReplyingToAuthor == "" && m.ReplyingToContent == "" && len(m.Attachments) == 0 &&
		m.Model == "" && !m.Silent && m.VanadisChunkID == ""
}

// Value implements driver.Valuer for PostgreSQL JSONB.
func (m MessageMetadata) Value() (driver.Value, error) {
	if m.IsEmpty() {
		return "{}", nil
	}
	bytes, err := json.Marshal(m)
	return string(bytes), err
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

// Store defines persistent storage operations for facts.
type Store interface {
	InsertFact(ctx context.Context, category, factText string, importance float64, embedding []float32) (int64, error)
	UpdateFact(ctx context.Context, id int64, category, factText string, importance float64, embedding []float32) error
	ReinforceFact(ctx context.Context, id int64, newText string, newEmbedding []float32, boost float64) error
	DeleteFact(ctx context.Context, id int64) error
	FindDuplicateFact(ctx context.Context, embedding []float32, minSim float64) (*Fact, float64, error)
	SearchCandidateFacts(ctx context.Context, embedding []float32, queryText, category string, candidateLimit int, minScore float64) ([]ScoredFactWithEmbedding, error)
	GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error)
	GetFactsMissingEmbeddings(ctx context.Context, limit int) ([]Fact, error)
	UpdateFactEmbedding(ctx context.Context, id int64, embedding []float32) error
	DecayAndPruneFacts(ctx context.Context, decayStep, pruneFloor float64, pruneAgeDays int) (decayedCount, prunedCount int64, err error)
	GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error)
	GetUnextractedAdminTurns(ctx context.Context, threadID string) ([]Message, error)
	GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error)
	UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error
	FindNearestFacts(ctx context.Context, embedding []float32, limit int, minSim float64) ([]Fact, error)
	Close() error
}

// SQLStore implements Store backed by PostgreSQL.
type SQLStore struct {
	db DBTX
}

// NewSQLStore creates a new SQLStore instance.
func NewSQLStore(db DBTX) *SQLStore {
	return &SQLStore{db: db}
}

// Close closes the underlying database handle if it implements io.Closer.
func (s *SQLStore) Close() error {
	if s != nil && s.db != nil {
		if closer, ok := s.db.(io.Closer); ok {
			return closer.Close()
		}
	}
	return nil
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

func optionalVectorArg(emb []float32) any {
	if len(emb) == ExpectedEmbeddingDim {
		return pgvector.NewVector(emb)
	}
	return nil
}

// EscapeSQLLike escapes characters with special meaning in SQL LIKE clauses.
func EscapeSQLLike(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	return strings.ReplaceAll(s, "_", "\\_")
}

// NullVector provides sql.Scanner support for pgvector nullable embedding columns.
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
	case pgvector.Vector:
		nv.Vector, nv.Valid = v.Slice(), true
		return nil
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
func (s *SQLStore) InsertFact(ctx context.Context, category, factText string, importance float64, embedding []float32) (int64, error) {
	if err := s.checkDB(); err != nil {
		return 0, err
	}
	factText = strings.TrimSpace(factText)
	if factText == "" {
		return 0, fmt.Errorf("fact text cannot be empty")
	}
	category = strings.TrimSpace(category)
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
	var id int64
	err := s.db.QueryRowContext(queryCtx, query, category, factText, importance, optionalVectorArg(embedding), now).Scan(&id)
	return id, err
}

// UpdateFact updates an existing fact with new values and increments reinforce_count.
func (s *SQLStore) UpdateFact(ctx context.Context, id int64, category, factText string, importance float64, embedding []float32) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	factText = strings.TrimSpace(factText)
	if factText == "" {
		return fmt.Errorf("fact text cannot be empty")
	}
	category = strings.TrimSpace(category)
	if category == "" {
		category = "general"
	}
	if importance <= 0 {
		importance = 0.5
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `UPDATE facts SET fact_text = $1, category = $2, importance = $3, embedding = $4, last_reinforced_at = NOW(), reinforce_count = reinforce_count + 1 WHERE id = $5`
	res, err := s.db.ExecContext(queryCtx, query, factText, category, importance, optionalVectorArg(embedding), id)
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

// ReinforceFact strengthens an existing fact's importance and updates its text/embedding if provided.
func (s *SQLStore) ReinforceFact(ctx context.Context, id int64, newText string, newEmbedding []float32, boost float64) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	if boost <= 0 {
		boost = 0.15
	}
	queryCtx, cancel := withTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `UPDATE facts SET importance = LEAST(1.0, GREATEST(0.70, ROUND((importance + $1)::numeric, 2))), last_reinforced_at = $2, reinforce_count = reinforce_count + 1, fact_text = CASE WHEN $3 != '' THEN $3 ELSE fact_text END, embedding = COALESCE($4, embedding) WHERE id = $5`
	res, err := s.db.ExecContext(queryCtx, query, boost, time.Now().UTC(), strings.TrimSpace(newText), optionalVectorArg(newEmbedding), id)
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

// SearchCandidateFacts performs 70/30 Hybrid RRF retrieval over stored facts with an optional category filter.
func (s *SQLStore) SearchCandidateFacts(ctx context.Context, embedding []float32, queryText, category string, candidateLimit int, minScore float64) ([]ScoredFactWithEmbedding, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryText = strings.TrimSpace(queryText)
	category = strings.TrimSpace(category)
	hasEmbedding := len(embedding) == ExpectedEmbeddingDim
	hasText := queryText != ""

	if !hasEmbedding && !hasText {
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

	var rows *sql.Rows
	var err error

	if hasEmbedding && hasText {
		if category != "" {
			query := `WITH vector_hits AS (
				SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> $1) AS rank
				FROM facts
				WHERE embedding IS NOT NULL AND category = $2
				ORDER BY embedding <=> $1 LIMIT $3
			), text_hits AS (
				SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $4)) DESC) AS rank
				FROM facts
				WHERE fts_tokens @@ websearch_to_tsquery('simple', $4) AND category = $2
				LIMIT $3
			)
			SELECT f.id, f.category, f.fact_text, f.importance, f.created_at,
			       COALESCE(f.last_reinforced_at, f.created_at) AS last_reinforced_at,
			       COALESCE(f.last_decayed_at, f.created_at) AS last_decayed_at,
			       COALESCE(f.reinforce_count, 1) AS reinforce_count,
			       f.embedding,
			       (((COALESCE($5::float8 / ($7::float8 + v.rank::float8), 0.0) + COALESCE($6::float8 / ($7::float8 + t.rank::float8), 0.0)) * $7::float8) * f.importance) AS score
			FROM vector_hits v FULL OUTER JOIN text_hits t ON v.id = t.id JOIN facts f ON f.id = COALESCE(v.id, t.id)
			WHERE (((COALESCE($5::float8 / ($7::float8 + v.rank::float8), 0.0) + COALESCE($6::float8 / ($7::float8 + t.rank::float8), 0.0)) * $7::float8) * f.importance) >= $8
			ORDER BY score DESC LIMIT $3;`
			rows, err = s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), category, candidateLimit, queryText, DefaultDenseWeight, DefaultSparseWeight, DefaultRRFK, minScore)
		} else {
			query := `WITH vector_hits AS (
				SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> $1) AS rank
				FROM facts
				WHERE embedding IS NOT NULL
				ORDER BY embedding <=> $1 LIMIT $2
			), text_hits AS (
				SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $3)) DESC) AS rank
				FROM facts
				WHERE fts_tokens @@ websearch_to_tsquery('simple', $3)
				LIMIT $2
			)
			SELECT f.id, f.category, f.fact_text, f.importance, f.created_at,
			       COALESCE(f.last_reinforced_at, f.created_at) AS last_reinforced_at,
			       COALESCE(f.last_decayed_at, f.created_at) AS last_decayed_at,
			       COALESCE(f.reinforce_count, 1) AS reinforce_count,
			       f.embedding,
			       (((COALESCE($4::float8 / ($6::float8 + v.rank::float8), 0.0) + COALESCE($5::float8 / ($6::float8 + t.rank::float8), 0.0)) * $6::float8) * f.importance) AS score
			FROM vector_hits v FULL OUTER JOIN text_hits t ON v.id = t.id JOIN facts f ON f.id = COALESCE(v.id, t.id)
			WHERE (((COALESCE($4::float8 / ($6::float8 + v.rank::float8), 0.0) + COALESCE($5::float8 / ($6::float8 + t.rank::float8), 0.0)) * $6::float8) * f.importance) >= $7
			ORDER BY score DESC LIMIT $2;`
			rows, err = s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), candidateLimit, queryText, DefaultDenseWeight, DefaultSparseWeight, DefaultRRFK, minScore)
		}
	} else if hasEmbedding {
		if category != "" {
			query := `WITH candidates AS (
				SELECT id, category, fact_text, importance, created_at,
				       COALESCE(last_reinforced_at, created_at) AS last_reinforced_at,
				       COALESCE(last_decayed_at, created_at) AS last_decayed_at,
				       COALESCE(reinforce_count, 1) AS reinforce_count,
				       embedding,
				       (1.0 - (embedding <=> $1)) AS similarity
				FROM facts
				WHERE embedding IS NOT NULL AND category = $2
				ORDER BY embedding <=> $1 LIMIT $3
			)
			SELECT id, category, fact_text, importance, created_at, last_reinforced_at, last_decayed_at, reinforce_count, embedding, (similarity * importance) AS score
			FROM candidates
			WHERE (similarity * importance) >= $4
			ORDER BY score DESC LIMIT $3;`
			rows, err = s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), category, candidateLimit, minScore)
		} else {
			query := `WITH candidates AS (
				SELECT id, category, fact_text, importance, created_at,
				       COALESCE(last_reinforced_at, created_at) AS last_reinforced_at,
				       COALESCE(last_decayed_at, created_at) AS last_decayed_at,
				       COALESCE(reinforce_count, 1) AS reinforce_count,
				       embedding,
				       (1.0 - (embedding <=> $1)) AS similarity
				FROM facts
				WHERE embedding IS NOT NULL
				ORDER BY embedding <=> $1 LIMIT $2
			)
			SELECT id, category, fact_text, importance, created_at, last_reinforced_at, last_decayed_at, reinforce_count, embedding, (similarity * importance) AS score
			FROM candidates
			WHERE (similarity * importance) >= $3
			ORDER BY score DESC LIMIT $2;`
			rows, err = s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), candidateLimit, minScore)
		}
	} else {
		if category != "" {
			query := `WITH text_hits AS (
				SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $1)) DESC) AS rank
				FROM facts
				WHERE fts_tokens @@ websearch_to_tsquery('simple', $1) AND category = $2
				LIMIT $3
			)
			SELECT f.id, f.category, f.fact_text, f.importance, f.created_at,
			       COALESCE(f.last_reinforced_at, f.created_at) AS last_reinforced_at,
			       COALESCE(f.last_decayed_at, f.created_at) AS last_decayed_at,
			       COALESCE(f.reinforce_count, 1) AS reinforce_count,
			       f.embedding,
			       ((($4::float8 / ($5::float8 + t.rank::float8)) * $5::float8) * f.importance) AS score
			FROM text_hits t JOIN facts f ON f.id = t.id
			WHERE ((($4::float8 / ($5::float8 + t.rank::float8)) * $5::float8) * f.importance) >= $6
			ORDER BY score DESC LIMIT $3;`
			rows, err = s.db.QueryContext(queryCtx, query, queryText, category, candidateLimit, 1.0, DefaultRRFK, minScore)
		} else {
			query := `WITH text_hits AS (
				SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $1)) DESC) AS rank
				FROM facts
				WHERE fts_tokens @@ websearch_to_tsquery('simple', $1)
				LIMIT $2
			)
			SELECT f.id, f.category, f.fact_text, f.importance, f.created_at,
			       COALESCE(f.last_reinforced_at, f.created_at) AS last_reinforced_at,
			       COALESCE(f.last_decayed_at, f.created_at) AS last_decayed_at,
			       COALESCE(f.reinforce_count, 1) AS reinforce_count,
			       f.embedding,
			       ((($3::float8 / ($4::float8 + t.rank::float8)) * $4::float8) * f.importance) AS score
			FROM text_hits t JOIN facts f ON f.id = t.id
			WHERE ((($3::float8 / ($4::float8 + t.rank::float8)) * $4::float8) * f.importance) >= $5
			ORDER BY score DESC LIMIT $2;`
			rows, err = s.db.QueryContext(queryCtx, query, queryText, candidateLimit, 1.0, DefaultRRFK, minScore)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("candidate search failed: %w", err)
	}
	defer rows.Close()

	var results []ScoredFactWithEmbedding
	for rows.Next() {
		var f Fact
		var nv NullVector
		var score float64
		var createdAt, lastReinforced, lastDecayed FactTime
		if err := rows.Scan(
			&f.ID, &f.Category, &f.FactText, &f.Importance,
			&createdAt, &lastReinforced, &lastDecayed,
			&f.ReinforceCount, &nv, &score,
		); err != nil {
			return nil, fmt.Errorf("failed to scan candidate fact: %w", err)
		}
		f.CreatedAt, f.LastReinforcedAt, f.LastDecayedAt = createdAt.Time, lastReinforced.Time, lastDecayed.Time
		results = append(results, ScoredFactWithEmbedding{
			Fact:      f,
			Embedding: nv.Vector,
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
	if cat := strings.TrimSpace(filter.Category); cat != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("category = $%d", argIdx))
		args = append(args, cat)
		argIdx++
	}
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

	selectQuery := fmt.Sprintf(`SELECT id, category, fact_text, COALESCE(importance, 1.0) AS importance, created_at, COALESCE(last_reinforced_at, created_at) AS last_reinforced_at, COALESCE(last_decayed_at, created_at) AS last_decayed_at, COALESCE(reinforce_count, 1) AS reinforce_count FROM facts %s ORDER BY COALESCE(importance, 1.0) DESC, created_at DESC, id DESC %s`, whereSQL, paginationSQL)
	rows, err := s.db.QueryContext(queryCtx, selectQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query facts: %w", err)
	}
	defer rows.Close()

	facts := make([]Fact, 0)
	for rows.Next() {
		var f Fact
		var createdAt, lastReinforced, lastDecayed FactTime
		if err := rows.Scan(
			&f.ID, &f.Category, &f.FactText, &f.Importance,
			&createdAt, &lastReinforced, &lastDecayed,
			&f.ReinforceCount,
		); err != nil {
			return nil, fmt.Errorf("failed to scan fact: %w", err)
		}
		f.CreatedAt, f.LastReinforcedAt, f.LastDecayedAt = createdAt.Time, lastReinforced.Time, lastDecayed.Time
		facts = append(facts, f)
	}

	return &FactsResult{
		Facts:  facts,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
		Status: "ok",
	}, rows.Err()
}

func scanFacts(rows *sql.Rows) ([]Fact, error) {
	if rows == nil {
		return nil, nil
	}
	var facts []Fact
	for rows.Next() {
		var f Fact
		var createdAt, lastReinforced, lastDecayed FactTime
		if err := rows.Scan(
			&f.ID, &f.Category, &f.FactText, &f.Importance,
			&createdAt, &lastReinforced, &lastDecayed,
			&f.ReinforceCount,
		); err != nil {
			return nil, err
		}
		f.CreatedAt, f.LastReinforcedAt, f.LastDecayedAt = createdAt.Time, lastReinforced.Time, lastDecayed.Time
		facts = append(facts, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return facts, nil
}

// GetFactsMissingEmbeddings retrieves facts lacking vector embeddings.
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

	facts, err := scanFacts(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to scan fact missing embedding: %w", err)
	}
	return facts, nil
}

// UpdateFactEmbedding updates the vector embedding for an existing fact.
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
	res, err := s.db.ExecContext(queryCtx, "UPDATE facts SET embedding = $1 WHERE id = $2", vec, id)
	if err != nil {
		return err
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

// DecayAndPruneFacts applies decay to untouched facts and removes stale facts.
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

// FindNearestFacts returns top-K facts with cosine similarity >= minSim.
func (s *SQLStore) FindNearestFacts(ctx context.Context, embedding []float32, limit int, minSim float64) ([]Fact, error) {
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
		SELECT id, category, fact_text, importance, created_at,
		       COALESCE(last_reinforced_at, created_at) AS last_reinforced_at,
		       COALESCE(last_decayed_at, created_at) AS last_decayed_at,
		       COALESCE(reinforce_count, 1) AS reinforce_count,
		       (1.0 - (embedding <=> $1)) AS similarity
		FROM facts
		WHERE embedding IS NOT NULL
		ORDER BY embedding <=> $1
		LIMIT $2
	)
	SELECT id, category, fact_text, importance, created_at, last_reinforced_at, last_decayed_at, reinforce_count
	FROM candidates
	WHERE similarity >= $3
	ORDER BY similarity DESC
	LIMIT $4;`

	rows, err := s.db.QueryContext(queryCtx, query, pgvector.NewVector(embedding), candidateLimit, minSim, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to find nearest facts: %w", err)
	}
	defer rows.Close()

	facts, err := scanFacts(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to scan nearest fact: %w", err)
	}
	return facts, nil
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
