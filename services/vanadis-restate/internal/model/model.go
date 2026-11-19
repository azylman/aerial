// Package model holds the shared leaf build, chunk, and state types for
// vanadis-restate. It imports no sibling internal package.
package model

import (
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

// VanadisBuild represents a multi-chunk engineering build run.
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
	HeadSHA            string                 `json:"head_sha,omitempty"`
	RemediatedSHA      string                 `json:"remediated_sha,omitempty"`
	MergeSHA           string                 `json:"merge_sha,omitempty"`
	RolloutState       *pubsub.PRRolloutState `json:"rollout_state,omitempty"`
	Banner             string                 `json:"banner,omitempty"`
	MergeAwakeableID   string                 `json:"merge_awakeable_id,omitempty"`
	DeployAwakeableID  string                 `json:"deploy_awakeable_id,omitempty"`
	StartedAt          time.Time              `json:"started_at,omitempty"`
	CompletedAt        time.Time              `json:"completed_at,omitempty"`
	CreatedAt          time.Time              `json:"created_at"`
	UpdatedAt          time.Time              `json:"updated_at"`
}

// BuildObjectState represents the authoritative K/V state for a Vanadis build managed by BuildObject.
type BuildObjectState struct {
	TerminalNotified bool            `json:"terminal_notified,omitempty"`
	Build            VanadisBuild    `json:"build"`
	Chunks           []VanadisChunk  `json:"chunks"`
	Launched         map[string]bool `json:"launched,omitempty"`
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
}

// AttachChunkPRResponse represents the result of attaching a PR to a chunk or standalone build.
type AttachChunkPRResponse struct {
	Status   string `json:"status"`
	BuildID  string `json:"build_id"`
	ChunkID  string `json:"chunk_id"`
	ThreadID string `json:"thread_id"`
}

// Legacy Restate service names preserved for the dark-launch default agent.
const (
	legacyBuildServiceName       = "Build"
	legacyBuildRouterServiceName = "BuildRouter"
)

// NormalizeAgentName trims and lowercases a per-agent name for scoped Restate names.
func NormalizeAgentName(agent string) string {
	return strings.ToLower(strings.TrimSpace(agent))
}

// BuildServiceName derives the Restate Build Virtual Object name for an agent.
// Empty and aerial resolve to the legacy Build name so Phase 1 stays dark.
func BuildServiceName(agent string) string {
	name := NormalizeAgentName(agent)
	if name == "" || name == "aerial" {
		return legacyBuildServiceName
	}
	return legacyBuildServiceName + "-" + name
}

// BuildRouterServiceName derives the Restate BuildRouter service name for an agent.
// Empty and aerial resolve to the legacy BuildRouter name so Phase 1 stays dark.
func BuildRouterServiceName(agent string) string {
	name := NormalizeAgentName(agent)
	if name == "" || name == "aerial" {
		return legacyBuildRouterServiceName
	}
	return legacyBuildRouterServiceName + "-" + name
}
