// Package model holds the shared leaf build, chunk, and state types for
// vanadis-restate. It imports no sibling internal package.
package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/buildplan"
)

// VanadisBuild is the shared build-plan wire type; see pkg/buildplan.
type VanadisBuild = buildplan.VanadisBuild

// VanadisChunk is the shared chunk wire type; see pkg/buildplan.
type VanadisChunk = buildplan.VanadisChunk

// BuildObjectState represents the authoritative K/V state for a Vanadis build managed by BuildObject.
type BuildObjectState struct {
	TerminalNotified bool            `json:"terminal_notified,omitempty"`
	Build            VanadisBuild    `json:"build"`
	Chunks           []VanadisChunk  `json:"chunks"`
	Launched         map[string]bool `json:"launched,omitempty"`
	// DispatchDeferrals tracks launches eri keeps answering skipped_active,
	// keyed by launch key. Only the journaled reconcile step output writes it.
	DispatchDeferrals map[string]DispatchDeferral `json:"dispatch_deferrals,omitempty"`
	// DeferralReconcileSeq identifies the one pending delayed Reconcile that
	// retries deferred launches; zero means none is pending.
	DeferralReconcileSeq int `json:"deferral_reconcile_seq,omitempty"`
}

// DispatchDeferral records continuous skipped_active deferral of one launch.
type DispatchDeferral struct {
	FirstDeferredAt time.Time `json:"first_deferred_at"`
	Count           int       `json:"count"`
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

// ErrInvalidBuildInput reports malformed build or chunk input; see pkg/buildplan.
var ErrInvalidBuildInput = buildplan.ErrInvalidBuildInput

// ActionDispatchID identifies the launch turn dispatch for a chunk.
func ActionDispatchID(chunk VanadisChunk) string {
	return fmt.Sprintf("vanadis-%s-turn-%d", chunk.ID, chunk.StallNudges)
}

// CleanRepoName strips any owner prefix from a repo reference.
func CleanRepoName(repo string) string {
	repo = strings.TrimSpace(repo)
	if idx := strings.LastIndex(repo, "/"); idx >= 0 {
		return repo[idx+1:]
	}
	return repo
}

// NormalizeDiscordThreadID canonicalizes a Discord thread or channel marker.
func NormalizeDiscordThreadID(s string) string {
	clean := strings.TrimSpace(s)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// ExtractChunkIDFromBranch recovers the Vanadis chunk ID encoded in a
// feat/<repo>-pr-vanadis-<chunk>-<6hex> branch name, or "" when absent.
func ExtractChunkIDFromBranch(repo, branch string) string {
	repoName := strings.TrimSuffix(CleanRepoName(repo), ".git")
	if repoName == "" {
		return ""
	}
	branch = strings.TrimSpace(branch)
	branch = strings.TrimPrefix(branch, "refs/heads/")
	after, ok := strings.CutPrefix(branch, "feat/"+repoName+"-pr-vanadis-")
	if !ok {
		return ""
	}
	chunkID, suffix, hasSuffix := strings.CutLast(after, "-")
	if !hasSuffix || len(suffix) != 6 {
		return ""
	}
	for i := range suffix {
		if !isHexDigit(suffix[i]) {
			return ""
		}
	}
	if !buildplan.IsValidVanadisChunkID(chunkID) {
		return ""
	}
	return chunkID
}
