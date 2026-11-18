package main

import (
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
