// Package executor hosts the side-effect dispatch surface for vanadis-restate.
//
// Frozen vocabulary and pure repo/branch helpers mirrored from the root
// package. Values stay byte-identical with the root copies until later
// chunks move their consumers; behavior is unchanged.
package executor

import (
	"fmt"
	"strings"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// Frozen status, mode, and ownership vocabulary mirrored from the root
// package (store.go / main.go / router.go).
const (
	chunkModeAction       = "action"
	defaultRepoOwner      = "azylman"
	statusPending         = "pending"
	statusRunning         = "running"
	statusOpen            = "open"
	statusPROpened        = "pr_opened"
	statusMerged          = "merged"
	statusSynced          = "synced"
	statusDeploying       = "deploying"
	statusDeployed        = "deployed"
	statusCompleted       = "completed"
	statusFailed          = "failed"
	statusCancelled       = "cancelled"
	statusCrashed         = "crashed"
	statusTimeout         = "timeout"
	dispatchStatusSuccess = "success"
)

// actionDispatchID mirrors the frozen root router.go copy byte-for-byte;
// router.go stays frozen until the router extraction chunk unifies both copies.
func actionDispatchID(chunk model.VanadisChunk) string {
	return fmt.Sprintf("vanadis-%s-turn-%d", chunk.ID, chunk.StallNudges)
}

// cleanRepoName mirrors the frozen root router.go copy byte-for-byte.
func cleanRepoName(repo string) string {
	repo = strings.TrimSpace(repo)
	if idx := strings.LastIndex(repo, "/"); idx >= 0 {
		return repo[idx+1:]
	}
	return repo
}

// normalizeDiscordThreadID mirrors the frozen root router.go and
// internal/store copies byte-for-byte.
func normalizeDiscordThreadID(s string) string {
	clean := strings.TrimSpace(s)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// extractChunkIDFromBranch mirrors the frozen root router.go copy
// byte-for-byte; router.go stays frozen until the router extraction chunk
// unifies both copies.
func extractChunkIDFromBranch(repo, branch string) string {
	repoName := strings.TrimSuffix(cleanRepoName(repo), ".git")
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
	if !store.IsValidVanadisChunkID(chunkID) {
		return ""
	}
	return chunkID
}
