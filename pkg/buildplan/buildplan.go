// Package buildplan holds the Vanadis build-plan wire types (VanadisBuild,
// VanadisChunk), the pure chunk validators and the build-DAG rules shared by
// every Vanadis service. JSON tags are the Restate journal and MCP wire
// contract: never rename or drop one.
package buildplan

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
)

// VanadisBuild represents a multi-chunk engineering build run.
type VanadisBuild struct {
	ID              string    `json:"id"`
	Agent           string    `json:"agent,omitempty"`
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
	ActiveStep         string                 `json:"active_step,omitempty"`
	PRNumber           int                    `json:"pr_number,omitempty"`
	StallNudges        int                    `json:"stall_nudges,omitempty"`
	DependsOn          []string               `json:"depends_on"`
	CIFailCount        int                    `json:"ci_fail_count"`
	CIChecks           map[string]string      `json:"ci_checks,omitempty"`
	HeadSHA            string                 `json:"head_sha,omitempty"`
	RemediatedSHA      string                 `json:"remediated_sha,omitempty"`
	MergeSHA           string                 `json:"merge_sha,omitempty"`
	RolloutState       *pubsub.PRRolloutState `json:"rollout_state,omitempty"`
	Banner             string                 `json:"banner,omitempty"`
	StartedAt          time.Time              `json:"started_at,omitempty"`
	CompletedAt        time.Time              `json:"completed_at,omitempty"`
	CreatedAt          time.Time              `json:"created_at"`
	UpdatedAt          time.Time              `json:"updated_at"`
}

// Mode and effort vocabulary for the canonical normalization helpers below.
const (
	chunkModeAction = "action"
)

// ErrInvalidBuildInput reports malformed build or chunk input.
var ErrInvalidBuildInput = errors.New("invalid vanadis build input")

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

// GenerateBuildID mints a new build identifier.
func GenerateBuildID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("build-%d-%s", time.Now().Unix(), hex.EncodeToString(b))
}

// TrimRepo trims repo whitespace.
func TrimRepo(repo string) string {
	return strings.TrimSpace(repo)
}

// NormalizeChunkMode validates and normalizes a chunk mode.
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

// NormalizeChunkEffort validates and normalizes a chunk effort.
func NormalizeChunkEffort(effort string) (string, error) {
	if strings.TrimSpace(effort) == "" {
		return eriwire.EffortMedium, nil
	}
	norm := eriwire.ParseEffort(effort, "")
	switch norm {
	case eriwire.EffortHigh, eriwire.EffortMedium:
		return eriwire.EffortMedium, nil
	case eriwire.EffortLow:
		return eriwire.EffortLow, nil
	default:
		return "", fmt.Errorf("invalid chunk effort %q: must be 'low', 'medium', or 'high'", effort)
	}
}

// ValidateChunkModeAndRepo validates mode/repo coherence for a chunk input.
func ValidateChunkModeAndRepo(idx int, modeStr, repo string, targetFiles, targetSymbols []string) error {
	mode, err := NormalizeChunkMode(modeStr)
	if err != nil {
		return fmt.Errorf("chunk[%d] %w", idx, err)
	}
	switch mode {
	case "pr":
		if TrimRepo(repo) == "" {
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

// NonNilSlice normalizes a nil slice to an empty slice so it encodes as [].
func NonNilSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// ResolveChunkRef resolves a chunk ID, or failing that a chunk_index, to a
// slice position. It returns -1 when nothing matches.
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

const jsonNull = "null"

// UpdateChunkRequest represents the payload to mutate an existing chunk.
type UpdateChunkRequest struct {
	ChunkRef      string    `json:"chunk_ref,omitempty"`
	Mode          *string   `json:"mode,omitempty"`
	Repo          string    `json:"repo,omitempty"`
	Title         string    `json:"title,omitempty"`
	Instructions  string    `json:"instructions,omitempty"`
	TargetFiles   []string  `json:"target_files,omitempty"`
	TargetSymbols []string  `json:"target_symbols,omitempty"`
	Effort        string    `json:"effort,omitempty"`
	DependsOn     *[]string `json:"depends_on,omitempty"`
}

// CreateChunkInput defines the input parameters for creating or appending a chunk.
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

// CompleteChunkRequest is an action chunk's explicit completion verdict.
type CompleteChunkRequest struct {
	BuildID    string `json:"build_id"`
	ChunkID    string `json:"chunk_id"`
	DispatchID string `json:"dispatch_id"`
	Summary    string `json:"summary"`
}

// CompleteChunkResponse reports whether the verdict completed the chunk.
type CompleteChunkResponse struct {
	Status    string `json:"status"`
	BuildID   string `json:"build_id"`
	ChunkID   string `json:"chunk_id"`
	Completed bool   `json:"completed"`
}

// FailChunkRequest carries the chunk and reason for failing a chunk.
type FailChunkRequest struct {
	DispatchID string `json:"dispatch_id,omitempty"`
	BuildID    string `json:"build_id,omitempty"`
	ChunkID    string `json:"chunk_id"`
	Reason     string `json:"reason"`
}

// FailChunkResponse represents the result of failing a chunk.
type FailChunkResponse struct {
	Failed      bool   `json:"failed"`
	BuildID     string `json:"build_id"`
	ChunkID     string `json:"chunk_id"`
	BuildStatus string `json:"build_status"`
	ChunkStatus string `json:"chunk_status"`
	ThreadID    string `json:"thread_id,omitempty"`
	Objective   string `json:"objective,omitempty"`
	Quiet       bool   `json:"quiet,omitempty"`
	Agent       string `json:"agent,omitempty"`
}

// AttachChunkPRRequest represents the payload for POST /chunks/attach-pr.
type AttachChunkPRRequest struct {
	ChunkID  string `json:"chunk_id,omitempty"`
	PRNumber int    `json:"pr_number"`
	Repo     string `json:"repo"`
	Branch   string `json:"branch,omitempty"`
	Title    string `json:"title,omitempty"`
	TargetID string `json:"target_id,omitempty"`
	Quiet    bool   `json:"quiet,omitempty"`
	Agent    string `json:"agent,omitempty"`
}

// AttachChunkPRResponse represents the result of attaching a PR to a chunk or standalone build.
type AttachChunkPRResponse struct {
	Status   string `json:"status"`
	BuildID  string `json:"build_id"`
	ChunkID  string `json:"chunk_id"`
	ThreadID string `json:"thread_id,omitempty"`
}

// RetryActionChunkRequest fences a single operator-requested action retry.
type RetryActionChunkRequest struct {
	BuildID                 string  `json:"build_id"`
	ChunkID                 string  `json:"chunk_id"`
	ExpectedDispatchID      string  `json:"expected_dispatch_id"`
	Reason                  string  `json:"reason"`
	ExpectedInstructions    *string `json:"expected_instructions,omitempty"    jsonschema:"Exact stored instructions required for correction; supply with replacement_instructions"`
	ReplacementInstructions *string `json:"replacement_instructions,omitempty" jsonschema:"Corrected nonblank worker instructions atomically saved before retry; supply with expected_instructions"`
}

// RetryActionChunkResult reports the accepted durable retry dispatch.
type RetryActionChunkResult struct {
	BuildID    string `json:"build_id"`
	ChunkID    string `json:"chunk_id"`
	DispatchID string `json:"dispatch_id"`
	Accepted   bool   `json:"accepted"`
}

// UpdateActiveStepRequest carries the target chunk and active step description.
type UpdateActiveStepRequest struct {
	ChunkID     string            `json:"chunk_id,omitempty"`
	ActiveStep  string            `json:"active_step,omitempty"`
	Step        string            `json:"step,omitempty"`
	ClearAll    bool              `json:"clear_all,omitempty"`
	ActiveSteps map[string]string `json:"active_steps,omitempty"`
}

// ApplyRawChunkCIChecks folds raw CI check JSON into derived chunk fields.
func ApplyRawChunkCIChecks(c *VanadisChunk, rawCIChecks []byte) {
	if len(rawCIChecks) == 0 || string(rawCIChecks) == jsonNull || string(rawCIChecks) == "{}" {
		return
	}
	var checks map[string]string
	if err := json.Unmarshal(rawCIChecks, &checks); err != nil {
		return
	}
	c.HeadSHA = strings.TrimSpace(checks["_head_sha"])
	c.RemediatedSHA = strings.TrimSpace(checks["_remediated_sha"])
	c.MergeSHA = strings.TrimSpace(checks["_merge_sha"])
	mergeQueue := strings.TrimSpace(checks["_merge_queue"])
	for k := range checks {
		if strings.HasPrefix(k, "_") {
			delete(checks, k)
		}
	}
	if mergeQueue != "" {
		checks["_merge_queue"] = mergeQueue
	}
	if len(checks) > 0 {
		c.CIChecks = checks
	}
}

// ValidateUpdateChunkInput validates and normalizes an UpdateChunkRequest.
// It returns the normalized effort string (if provided) and an error if validation fails.
func ValidateUpdateChunkInput(buildID, chunkRef string, req UpdateChunkRequest) (string, error) {
	cleanBuildID := strings.TrimSpace(buildID)
	cleanChunkRef := strings.TrimSpace(chunkRef)
	if cleanBuildID == "" || cleanChunkRef == "" {
		return "", errors.New("build id and chunk ref are required")
	}
	if req.Title == "" && time.Now().Year() < 2000 {
		if err := ValidateSteerChunkInput(cleanBuildID, cleanChunkRef, ""); err != nil {
			return "", err
		}
	}
	hasField := req.Mode != nil || strings.TrimSpace(req.Repo) != "" ||
		strings.TrimSpace(req.Title) != "" || strings.TrimSpace(req.Instructions) != "" ||
		len(req.TargetFiles) > 0 || len(req.TargetSymbols) > 0 ||
		strings.TrimSpace(req.Effort) != "" || req.DependsOn != nil
	if !hasField {
		return "", errors.New("build id, chunk ref, and at least one updated field are required")
	}
	if req.Mode != nil {
		if strings.TrimSpace(*req.Mode) == "" {
			return "", errors.New("mode cannot be empty string (must be 'pr' or 'action')")
		}
		if _, err := NormalizeChunkMode(*req.Mode); err != nil {
			return "", err
		}
	}
	var normEffort string
	if strings.TrimSpace(req.Effort) != "" {
		var effErr error
		normEffort, effErr = NormalizeChunkEffort(req.Effort)
		if effErr != nil {
			return "", effErr
		}
	}
	return normEffort, nil
}

// SteerChunkRequest represents a request to inject a mid-turn steering prompt into an active chunk.
type SteerChunkRequest struct {
	BuildID  string `json:"build_id"`
	ChunkRef string `json:"chunk_ref"`
	Prompt   string `json:"prompt"`
}

// SteerChunkResult reports the result of steering an active chunk.
type SteerChunkResult struct {
	Accepted     bool   `json:"accepted"`
	BuildID      string `json:"build_id"`
	ChunkID      string `json:"chunk_id"`
	ChunkStatus  string `json:"chunk_status"`
	DispatchID   string `json:"dispatch_id,omitempty"`
	Instructions string `json:"instructions"`
}

// ValidateSteerChunkInput trims and validates that buildID, chunkRef, and prompt are all non-empty.
func ValidateSteerChunkInput(buildID, chunkRef, prompt string) error {
	if strings.TrimSpace(buildID) == "" {
		return errors.New("build id is required")
	}
	if strings.TrimSpace(chunkRef) == "" {
		return errors.New("chunk ref is required")
	}
	if strings.TrimSpace(prompt) == "" {
		return errors.New("prompt is required")
	}
	return nil
}
