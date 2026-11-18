package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

const (
	statusPending      = "pending"
	statusRunning      = "running"
	statusCoding       = "coding"
	statusOpen         = "open"
	statusPROpened     = "pr_opened"
	statusMerged       = "merged"
	statusSynced       = "synced"
	statusDeploying    = "deploying"
	statusDeployed     = "deployed"
	statusConflict     = "conflict"
	statusDeployFailed = "deploy_failed"
	statusCIFailed     = "ci_failed"
	chunkModeAction    = "action"
	defaultRepoOwner   = "azylman"
	effortLow          = "low"
	effortMedium       = "medium"
	effortHigh         = "high"
)

// ErrChunkNotFound aliases the store package sentinel so errors.Is identity
// holds for chunk lookups wherever the lookup executes.
var ErrChunkNotFound = store.ErrChunkNotFound

type CreateBuildRequest struct {
	ID            string                   `json:"id"`
	ThreadID      string                   `json:"thread_id"`
	Tier          int                      `json:"tier"`
	Title         string                   `json:"title"`
	Objective     string                   `json:"objective"`
	SharedContext string                   `json:"shared_context,omitempty"`
	Quiet         bool                     `json:"quiet"`
	Chunks        []store.CreateChunkInput `json:"chunks"`
}

type AppendChunksRequest struct {
	Chunks []store.CreateChunkInput `json:"chunks"`
}

type PostgresVanadisStore struct {
	*store.Store

	db store.PgxQueryExecer
}

func NewPostgresVanadisStore(db store.PgxQueryExecer) *PostgresVanadisStore {
	return &PostgresVanadisStore{Store: store.New(db), db: db}
}

func (s *PostgresVanadisStore) IncrementChunkCIFailCount(ctx context.Context, chunkID, banner string, headSHA ...string) (int, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return 0, errors.New("chunk_id cannot be empty")
	}
	var cleanSHA string
	if len(headSHA) > 0 {
		cleanSHA = strings.TrimSpace(headSHA[0])
	}
	const q = `UPDATE vanadis_chunks
SET ci_fail_count = ci_fail_count + 1,
    status = 'ci_failed',
    banner = $1,
    ci_checks = CASE WHEN $3 != '' THEN jsonb_set(COALESCE(ci_checks, '{}'::jsonb), '{_remediated_sha}', to_jsonb($3::text), true) ELSE ci_checks END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $2
  AND status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'deploy_failed', 'completed', 'failed', 'cancelled')
RETURNING ci_fail_count;`
	var failCount int
	err := s.db.QueryRow(ctx, q, strings.TrimSpace(banner), id, cleanSHA).Scan(&failCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, errPRTransitionSkipped
	}
	if err != nil {
		return 0, fmt.Errorf("increment chunk ci fail count: %w", err)
	}
	return failCount, nil
}
