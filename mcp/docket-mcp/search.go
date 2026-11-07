package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pgvector/pgvector-go"
)

// DBTX specifies the database interface required by SQLTaskStore.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// TaskStore defines persistence operations for task embeddings and resolutions.
type TaskStore interface {
	UpdateTaskEmbedding(ctx context.Context, taskID int64, embedding []float32) error
	UpdateTaskEmbeddingAndResolution(ctx context.Context, taskID int64, embedding []float32, resolution string) error
	UpdateTaskResolutionBatch(ctx context.Context, taskIDs []int64, resolution string) error
	FetchTaskResolutions(ctx context.Context, taskIDs []int64) (map[int64]string, error)
	SearchTasksRRF(ctx context.Context, embedding []float32, queryText string, statusFilter string, limit int) ([]ScoredTaskSummary, error)
}

// SQLTaskStore implements TaskStore on top of PostgreSQL 18 tasks table.
type SQLTaskStore struct {
	db DBTX
}

// NewSQLTaskStore creates a new SQLTaskStore.
func NewSQLTaskStore(db DBTX) *SQLTaskStore {
	return &SQLTaskStore{db: db}
}

// UpdateTaskEmbedding updates only the vector embedding for a task without modifying resolution.
func (s *SQLTaskStore) UpdateTaskEmbedding(ctx context.Context, taskID int64, embedding []float32) error {
	if len(embedding) == 0 {
		return nil
	}
	vec := pgvector.NewVector(embedding)
	query := `UPDATE tasks SET embedding = $1 WHERE id = $2;`
	_, err := s.db.ExecContext(ctx, query, vec, taskID)
	if err != nil {
		return fmt.Errorf("failed to update task embedding: %w", err)
	}
	return nil
}

// UpdateTaskEmbeddingAndResolution persists vector embedding and resolution string for a task.
func (s *SQLTaskStore) UpdateTaskEmbeddingAndResolution(ctx context.Context, taskID int64, embedding []float32, resolution string) error {
	trimmedRes := strings.TrimSpace(resolution)
	if len(embedding) > 0 {
		vec := pgvector.NewVector(embedding)
		query := `UPDATE tasks SET embedding = $1, resolution = $2 WHERE id = $3;`
		_, err := s.db.ExecContext(ctx, query, vec, trimmedRes, taskID)
		if err != nil {
			return fmt.Errorf("failed to update task embedding: %w", err)
		}
		return nil
	}
	query := `UPDATE tasks SET resolution = $1 WHERE id = $2;`
	_, err := s.db.ExecContext(ctx, query, trimmedRes, taskID)
	if err != nil {
		return fmt.Errorf("failed to update task resolution: %w", err)
	}
	return nil
}

// UpdateTaskResolutionBatch updates the resolution for multiple task IDs in a single query.
func (s *SQLTaskStore) UpdateTaskResolutionBatch(ctx context.Context, taskIDs []int64, resolution string) error {
	if len(taskIDs) == 0 {
		return nil
	}
	trimmedRes := strings.TrimSpace(resolution)
	query := `UPDATE tasks SET resolution = $1 WHERE id = ANY($2);`
	_, err := s.db.ExecContext(ctx, query, trimmedRes, taskIDs)
	if err != nil {
		return fmt.Errorf("failed to batch update task resolutions: %w", err)
	}
	return nil
}

// FetchTaskResolutions retrieves resolution strings for the given task IDs.
func (s *SQLTaskStore) FetchTaskResolutions(ctx context.Context, taskIDs []int64) (map[int64]string, error) {
	result := make(map[int64]string, len(taskIDs))
	if len(taskIDs) == 0 {
		return result, nil
	}
	query := `SELECT id, COALESCE(resolution, '') FROM tasks WHERE id = ANY($1);`
	rows, err := s.db.QueryContext(ctx, query, taskIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch task resolutions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var res string
		if err := rows.Scan(&id, &res); err != nil {
			return nil, fmt.Errorf("failed to scan task resolution: %w", err)
		}
		result[id] = res
	}
	return result, rows.Err()
}

// SearchTasksRRF executes a 70/30 Hybrid Weighted RRF search over Vikunja's tasks table.
func (s *SQLTaskStore) SearchTasksRRF(ctx context.Context, embedding []float32, queryText string, statusFilter string, limit int) ([]ScoredTaskSummary, error) {
	if limit <= 0 {
		limit = 10
	}
	filter := strings.TrimSpace(statusFilter)
	if filter == "" {
		filter = "open"
	}

	hasVector := len(embedding) == 384
	var vecArg any
	if hasVector {
		vecArg = pgvector.NewVector(embedding)
	} else {
		vecArg = pgvector.NewVector(make([]float32, 384))
	}

	query := `WITH filtered_tasks AS (
	SELECT id, title, done, resolution, embedding, fts_tokens
	FROM tasks
	WHERE (
		CASE
			WHEN $4 = 'open' THEN (done = false)
			WHEN $4 = 'completed' THEN (done = true AND COALESCE(resolution, '') != 'wont_do')
			WHEN $4 = 'wont_do' THEN (done = true AND resolution = 'wont_do')
			ELSE true
		END
	)
), vector_hits AS (
	SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> $1) AS rank
	FROM filtered_tasks
	WHERE $5 AND embedding IS NOT NULL
	ORDER BY embedding <=> $1
	LIMIT $2
), text_hits AS (
	SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(fts_tokens, websearch_to_tsquery('simple', $3)) DESC) AS rank
	FROM filtered_tasks
	WHERE fts_tokens @@ websearch_to_tsquery('simple', $3)
	LIMIT $2
), rrf AS (
	SELECT COALESCE(v.id, t.id) AS id,
	       ((COALESCE(0.70 / (60.0 + v.rank), 0.0) + COALESCE(0.30 / (60.0 + t.rank), 0.0)) * 60.0) AS score
	FROM vector_hits v
	FULL OUTER JOIN text_hits t ON v.id = t.id
)
SELECT f.id, f.title, f.done, COALESCE(f.resolution, ''), r.score
FROM rrf r
JOIN filtered_tasks f ON f.id = r.id
ORDER BY r.score DESC
LIMIT $2;`

	rows, err := s.db.QueryContext(ctx, query, vecArg, limit, strings.TrimSpace(queryText), filter, hasVector)
	if err != nil {
		return nil, fmt.Errorf("hybrid rrf search failed: %w", err)
	}
	defer rows.Close()

	var results []ScoredTaskSummary
	for rows.Next() {
		var id int64
		var title string
		var done bool
		var res string
		var score float64
		if err := rows.Scan(&id, &title, &done, &res, &score); err != nil {
			return nil, fmt.Errorf("failed to scan scored task row: %w", err)
		}
		status := ResolveTaskStatus(done, res)
		results = append(results, ScoredTaskSummary{
			ID:     id,
			Title:  title,
			Status: status,
			Score:  score,
		})
	}
	return results, rows.Err()
}
