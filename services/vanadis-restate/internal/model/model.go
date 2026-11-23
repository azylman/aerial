// Package model holds the shared leaf build, chunk, and state types for
// vanadis-restate. It imports no sibling internal package.
package model

import (
	"fmt"
	"strings"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/discordutil"
	"github.com/azylman/aerial/pkg/prwire"
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
	// FailureNotified records chunk failures (failureNotifyKey) that already
	// woke the thread, so each failure wakes it exactly once.
	FailureNotified map[string]bool `json:"failure_notified,omitempty"`
	// FailureIsolation marks a build transitioned under failure isolation: a
	// failed chunk blocks only its dependents. Failed builds without it are
	// legacy pauses that resume only through an explicit retry.
	FailureIsolation bool `json:"failure_isolation,omitempty"`
}

// FailChunkRequest carries the chunk and reason for failing a chunk.
type FailChunkRequest = buildplan.FailChunkRequest

// FailChunkResponse represents the result of failing a chunk.
type FailChunkResponse = buildplan.FailChunkResponse

// AttachChunkPRResponse represents the result of attaching a PR to a chunk or standalone build.
type AttachChunkPRResponse = buildplan.AttachChunkPRResponse

// ErrInvalidBuildInput reports malformed build or chunk input; see pkg/buildplan.
var ErrInvalidBuildInput = buildplan.ErrInvalidBuildInput

// IsTerminalBuildStatus reports whether a build status is terminal; see pkg/buildplan.
var IsTerminalBuildStatus = buildplan.IsTerminalBuildStatus

// ActionDispatchID identifies the launch turn dispatch for a chunk.
func ActionDispatchID(chunk VanadisChunk) string {
	return fmt.Sprintf("vanadis-%s-turn-%d", chunk.ID, chunk.StallNudges)
}

// CleanRepoName strips any owner prefix from a repo reference.
func CleanRepoName(repo string) string {
	return prwire.CleanRepoName(repo)
}

// NormalizeDiscordThreadID canonicalizes a Discord thread or channel marker.
func NormalizeDiscordThreadID(s string) string {
	return discordutil.NormalizeThreadID(s)
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isDiscordSnowflake(s string) bool {
	if len(s) < 17 || len(s) > 20 {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
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
	if prefix, candidate, ok := strings.CutLast(chunkID, "-t"); ok {
		if isDiscordSnowflake(candidate) {
			chunkID = prefix
		}
	}
	if !buildplan.IsValidVanadisChunkID(chunkID) {
		return ""
	}
	return chunkID
}

// ExtractTargetIDFromBranch recovers the 17-20 digit Discord snowflake encoded
// in a feat/<repo>-pr-t<target_id>-<6hex> or feat/<repo>-pr-vanadis-<chunk>-t<target_id>-<6hex>
// branch name, or "" when absent.
func ExtractTargetIDFromBranch(repo, branch string) string {
	repoName := strings.TrimSuffix(CleanRepoName(repo), ".git")
	if repoName == "" {
		return ""
	}
	branch = strings.TrimSpace(branch)
	branch = strings.TrimPrefix(branch, "refs/heads/")
	after, ok := strings.CutPrefix(branch, "feat/"+repoName+"-pr-")
	if !ok {
		return ""
	}
	remainder, suffix, hasSuffix := strings.CutLast(after, "-")
	if !hasSuffix || len(suffix) != 6 {
		return ""
	}
	for i := range suffix {
		if !isHexDigit(suffix[i]) {
			return ""
		}
	}
	if strings.HasPrefix(remainder, "t") && isDiscordSnowflake(remainder[1:]) {
		return remainder[1:]
	}
	if _, candidate, ok := strings.CutLast(remainder, "-t"); ok {
		if isDiscordSnowflake(candidate) {
			return candidate
		}
	}
	return ""
}
