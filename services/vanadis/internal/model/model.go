// Package model holds Vanadis domain types, sentinel errors, and pure
// string/repo/branch helpers shared by the service packages.
package model

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/strutil"
)

type (
	// VanadisBuild represents a multi-chunk autonomous engineering build run.
	VanadisBuild = buildplan.VanadisBuild
	// VanadisChunk represents an individual ordered task chunk within a VanadisBuild.
	VanadisChunk = buildplan.VanadisChunk
)

const (
	effortLow    = "low"
	effortMedium = "medium"
)

const (
	statusMerged    = "merged"
	statusRunning   = "running"
	statusPending   = "pending"
	statusCompleted = "completed"
	statusCancelled = "cancelled"
	statusQueued    = "queued"
	statusCoding    = "coding"
	statusPROpened  = "pr_opened"
	statusDeploying = "deploying"
	statusFailed    = "failed"
)

var (
	// ErrBuildNotFound indicates a requested VanadisBuild does not exist in PostgreSQL.
	ErrBuildNotFound = errors.New("vanadis build not found")
	// ErrChunkNotFound indicates a requested VanadisChunk does not exist in PostgreSQL.
	ErrChunkNotFound = errors.New("vanadis chunk not found")
	// ErrBuildConflict indicates a VanadisBuild or chunk state conflict (e.g. not running or not pending).
	ErrBuildConflict = errors.New("vanadis build state conflict")
	// ErrInvalidBuildInput indicates invalid inputs for a VanadisBuild or chunk mutation.
	ErrInvalidBuildInput = buildplan.ErrInvalidBuildInput
)

// CapChunkEffort maps legacy effort values onto low/medium.
func CapChunkEffort(effort string) string {
	if strings.EqualFold(strings.TrimSpace(effort), "low") {
		return effortLow
	}
	return effortMedium
}

// VanadisBuildEntry represents a summarized VanadisBuild including overall queue state and chunk aggregates.
type VanadisBuildEntry struct {
	VanadisBuild

	QueueState      string         `json:"queue_state"`
	TotalChunks     int            `json:"total_chunks"`
	CompletedChunks int            `json:"completed_chunks"`
	RunningChunks   int            `json:"running_chunks"`
	PendingChunks   int            `json:"pending_chunks"`
	FailedChunks    int            `json:"failed_chunks"`
	Chunks          []VanadisChunk `json:"chunks"`
}

// ClassifyBuildQueueState computes the overall queue state ("running", "queued", or "completed") based on buildStatus and child chunks.
func ClassifyBuildQueueState(buildStatus string, chunks []VanadisChunk) string {
	switch strings.ToLower(strings.TrimSpace(buildStatus)) {
	case statusCompleted, statusMerged, statusFailed, statusCancelled, "canceled":
		return statusCompleted
	case statusRunning:
		for _, c := range chunks {
			if strings.ToLower(strings.TrimSpace(c.Status)) != statusPending {
				return statusRunning
			}
		}
		if len(chunks) > 0 {
			return statusQueued
		}
		return statusRunning
	default:
		return statusQueued
	}
}

// SummarizeBuildEntry constructs a VanadisBuildEntry with non-nil chunks, chunk status counts, and computed queue state.
func SummarizeBuildEntry(build VanadisBuild, chunks []VanadisChunk) VanadisBuildEntry {
	chunks = buildplan.NonNilSlice(chunks)
	var completed, running, pending, failed int
	for _, c := range chunks {
		switch strings.ToLower(strings.TrimSpace(c.Status)) {
		case statusCompleted, statusMerged:
			completed++
		case statusRunning, statusCoding, statusPROpened, statusDeploying:
			running++
		case statusFailed:
			failed++
		case statusPending:
			pending++
		}
	}
	return VanadisBuildEntry{
		VanadisBuild: build, QueueState: ClassifyBuildQueueState(build.Status, chunks),
		TotalChunks: len(chunks), CompletedChunks: completed, RunningChunks: running, PendingChunks: pending, FailedChunks: failed, Chunks: chunks,
	}
}

// CreateChunkInput defines a single chunk in a CreateBuildRequest.
type CreateChunkInput = buildplan.CreateChunkInput

// CreateBuildRequest defines the JSON request body for POST /builds.
type CreateBuildRequest struct {
	ID            string             `json:"id"`
	Agent         string             `json:"agent,omitempty"`
	ThreadID      string             `json:"thread_id"`
	Tier          int                `json:"tier"`
	Title         string             `json:"title"`
	Objective     string             `json:"objective"`
	SharedContext string             `json:"shared_context,omitempty"`
	Quiet         bool               `json:"quiet"`
	Chunks        []CreateChunkInput `json:"chunks"`
}

// UpdateActiveStepRequest carries the target chunk and active step description.
type UpdateActiveStepRequest = buildplan.UpdateActiveStepRequest

// ParseCreateBuildChunks validates raw chunk inputs and normalizes the build DAG.
func ParseCreateBuildChunks(buildID string, now time.Time, rawChunks []CreateChunkInput) ([]VanadisChunk, error) {
	chunks := make([]VanadisChunk, len(rawChunks))
	for i, c := range rawChunks {
		title, instructions := strings.TrimSpace(c.Title), strings.TrimSpace(c.Instructions)
		if title == "" || instructions == "" {
			return nil, fmt.Errorf("chunk[%d] title and instructions are required", i)
		}
		chunkID := strutil.FirstNonEmpty(c.ID, fmt.Sprintf("%s-chunk-%d", buildID, i+1))
		if !buildplan.IsValidVanadisChunkID(chunkID) {
			return nil, fmt.Errorf("chunks[%d].id must be 1..120 characters of [a-zA-Z0-9_-]", i)
		}
		mode, err := buildplan.NormalizeChunkMode(c.Mode)
		if err != nil {
			return nil, fmt.Errorf("chunk[%d] %w", i, err)
		}
		if valErr := buildplan.ValidateChunkModeAndRepo(i, c.Mode, c.Repo, c.TargetFiles, c.TargetSymbols); valErr != nil {
			return nil, valErr
		}
		repo := ""
		if mode == "pr" {
			repo = buildplan.TrimRepo(c.Repo)
		}
		effort, err := buildplan.NormalizeChunkEffort(c.Effort)
		if err != nil {
			return nil, fmt.Errorf("chunk[%d] invalid effort: %w", i, err)
		}
		chunks[i] = VanadisChunk{
			ID: chunkID, BuildID: buildID, ChunkIndex: i, Mode: mode,
			Repo: repo, Title: title, Instructions: instructions,
			TargetFiles: buildplan.NonNilSlice(c.TargetFiles), TargetSymbols: buildplan.NonNilSlice(c.TargetSymbols), Effort: effort,
			AcceptanceCriteria: buildplan.NonNilSlice(c.AcceptanceCriteria), Status: statusPending, DependsOn: c.DependsOn, CreatedAt: now, UpdatedAt: now,
		}
	}
	return buildplan.NormalizeAndValidateBuildDAG(chunks)
}
