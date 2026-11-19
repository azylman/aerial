// Package model holds Vanadis domain types, sentinel errors, and pure
// string/repo/branch helpers shared by the service packages.
package model

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

const (
	effortLow    = "low"
	effortMedium = "medium"
)

const (
	chunkModeAction = "action"
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
	ErrInvalidBuildInput = errors.New("invalid vanadis build input")
)

// FirstNonEmpty returns the first trimmed non-empty string.
func FirstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// NonNilSlice normalizes a nil slice to an empty slice.
func NonNilSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// CapChunkEffort maps legacy effort values onto low/medium.
func CapChunkEffort(effort string) string {
	if strings.EqualFold(strings.TrimSpace(effort), "low") {
		return effortLow
	}
	return effortMedium
}

// NormalizeChunkEffort validates and normalizes a chunk effort value.
func NormalizeChunkEffort(effort string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "", effortMedium, "high":
		return effortMedium, nil
	case effortLow:
		return effortLow, nil
	default:
		return "", fmt.Errorf("invalid chunk effort %q: must be 'low', 'medium', or 'high'", effort)
	}
}

// VanadisBuild represents a multi-chunk autonomous engineering build run.
type VanadisBuild struct {
	ID              string    `json:"id"`
	ThreadID        string    `json:"thread_id"`
	Tier            int       `json:"tier"`
	Title           string    `json:"title"`
	Objective       string    `json:"objective"`
	SharedContext   string    `json:"shared_context,omitempty"`
	Status          string    `json:"status"`
	StatusMessageID string    `json:"status_message_id,omitempty"`
	Quiet           bool      `json:"quiet"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// VanadisChunk represents an individual ordered task chunk within a VanadisBuild.
type VanadisChunk struct {
	ID                 string                 `json:"id"`
	BuildID            string                 `json:"build_id"`
	ChunkIndex         int                    `json:"chunk_index"`
	Mode               string                 `json:"mode"`
	Repo               string                 `json:"repo,omitempty"`
	Title              string                 `json:"title"`
	Instructions       string                 `json:"instructions"`
	TargetFiles        []string               `json:"target_files"`
	TargetSymbols      []string               `json:"target_symbols"`
	Effort             string                 `json:"effort"`
	AcceptanceCriteria []string               `json:"acceptance_criteria"`
	Status             string                 `json:"status"`
	PRNumber           int                    `json:"pr_number,omitempty"`
	StallNudges        int                    `json:"stall_nudges,omitempty"`
	DependsOn          []string               `json:"depends_on"`
	CIFailCount        int                    `json:"ci_fail_count"`
	CIChecks           map[string]string      `json:"ci_checks,omitempty"`
	RolloutState       *pubsub.PRRolloutState `json:"rollout_state,omitempty"`
	Banner             string                 `json:"banner,omitempty"`
	MergeAwakeableID   string                 `json:"merge_awakeable_id,omitempty"`
	DeployAwakeableID  string                 `json:"deploy_awakeable_id,omitempty"`
	StartedAt          time.Time              `json:"started_at,omitempty"`
	CompletedAt        time.Time              `json:"completed_at,omitempty"`
	CreatedAt          time.Time              `json:"created_at"`
	UpdatedAt          time.Time              `json:"updated_at"`
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
	chunks = NonNilSlice(chunks)
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

// NormalizeRepo trims whitespace from a repository reference.
func NormalizeRepo(repo string) string {
	return strings.TrimSpace(repo)
}

// NormalizeChunkMode validates and normalizes a chunk mode value.
func NormalizeChunkMode(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "pr":
		return "pr", nil
	case chunkModeAction:
		return chunkModeAction, nil
	default:
		return "", fmt.Errorf("invalid chunk mode %q: must be 'pr' or 'action'", raw)
	}
}

// ValidateChunkModeAndRepo validates the mode/repo/target combination for chunk idx.
func ValidateChunkModeAndRepo(idx int, modeStr, repo string, targetFiles, targetSymbols []string) error {
	mode, err := NormalizeChunkMode(modeStr)
	if err != nil {
		return fmt.Errorf("chunk[%d] %w", idx, err)
	}
	switch mode {
	case "pr":
		if NormalizeRepo(repo) == "" {
			return fmt.Errorf("chunk[%d] repo is required for pr mode", idx)
		}
	case chunkModeAction:
		if strings.TrimSpace(repo) != "" {
			return fmt.Errorf("chunk[%d] action chunk must have empty repo", idx)
		}
		if len(targetFiles) > 0 {
			return fmt.Errorf("chunk[%d] action chunk must not specify target_files", idx)
		}
		if len(targetSymbols) > 0 {
			return fmt.Errorf("chunk[%d] action chunk must not specify target_symbols", idx)
		}
	}
	return nil
}

// IsValidVanadisChunkID reports whether id is a valid chunk identifier.
func IsValidVanadisChunkID(id string) bool {
	id = strings.TrimSpace(id)
	if len(id) < 1 || len(id) > 120 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

// CreateChunkInput defines a single chunk in a CreateBuildRequest.
type CreateChunkInput struct {
	ID                 string   `json:"id"`
	Mode               string   `json:"mode,omitempty"`
	Repo               string   `json:"repo,omitempty"`
	Title              string   `json:"title"`
	Instructions       string   `json:"instructions"`
	TargetFiles        []string `json:"target_files"`
	TargetSymbols      []string `json:"target_symbols"`
	Effort             string   `json:"effort,omitempty"`
	AcceptanceCriteria []string `json:"acceptance_criteria"`
	DependsOn          []string `json:"depends_on"`
}

// CreateBuildRequest defines the JSON request body for POST /builds.
type CreateBuildRequest struct {
	ID            string             `json:"id"`
	ThreadID      string             `json:"thread_id"`
	Tier          int                `json:"tier"`
	Title         string             `json:"title"`
	Objective     string             `json:"objective"`
	SharedContext string             `json:"shared_context,omitempty"`
	Quiet         bool               `json:"quiet"`
	Chunks        []CreateChunkInput `json:"chunks"`
}

// GenerateBuildID generates a unique build identifier.
func GenerateBuildID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("build-%d-%s", time.Now().Unix(), hex.EncodeToString(b))
}

// ResolveChunkRef resolves a chunk ID or chunk index reference to a slice position.
func ResolveChunkRef(chunks []VanadisChunk, chunkRef string) int {
	clean := strings.TrimSpace(chunkRef)
	if clean == "" {
		return -1
	}
	for i, c := range chunks {
		if c.ID == clean {
			return i
		}
	}
	if idx, err := strconv.Atoi(clean); err == nil {
		for i, c := range chunks {
			if c.ChunkIndex == idx {
				return i
			}
		}
	}
	return -1
}
