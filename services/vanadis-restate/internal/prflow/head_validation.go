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
