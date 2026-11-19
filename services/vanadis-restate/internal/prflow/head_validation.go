// Package prflow owns live GitHub PR head SHA and attachment validation plus
// PR lifecycle and dispatch-outcome orchestration for vanadis-restate builds.
// It has no Restate or BuildObject dependency; the Virtual Object handler
// shell and journal step names stay in internal/build, which delegates here.
// Frozen wire values (event names, statuses, dispatch ID formats) stay
// byte-identical with the previous internal/build and internal/executor copies.
package prflow

import (
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// CanFallbackThreadRepoLookup reports whether a PR event without a branch
// chunk ID may fall back to thread-and-repo matching for unattached chunks.
func CanFallbackThreadRepoLookup(ev pubsub.PRLifecycleEvent) bool {
	if strings.TrimSpace(ev.TargetID) == "" || ev.Quiet {
		return false
	}
	branch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	return !strings.HasPrefix(branch, "dependabot/")
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

func cleanRepoName(repo string) string {
	repo = strings.TrimSpace(repo)
	if idx := strings.LastIndex(repo, "/"); idx >= 0 {
		return repo[idx+1:]
	}
	return repo
}

// ExtractChunkIDFromBranch recovers the Vanadis chunk ID encoded in a
// feat/<repo>-pr-vanadis-<chunk>-<6hex> branch name, or "" when absent.
func ExtractChunkIDFromBranch(repo, branch string) string {
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
