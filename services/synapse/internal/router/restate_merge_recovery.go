package router

import (
	"context"
	"log/slog"
	"strings"

	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// Merge recovery paths (synapse_restate_merge_recovered_total{path}).
const (
	mergeRecoveryPathDeploy      = "deploy"
	mergeRecoveryPathOwnedDeploy = "owned_deploy"
)

// resolveCommitPR discovers the PR for a commit SHA via the GitHub commits
// API, with its head SHA and merge state.
func (s *RouterServer) resolveCommitPR(ctx context.Context, repo, sha string) (github.CommitPR, error) {
	return github.ResolveCommitPR(ctx, s.ghClient(), repo, sha)
}

// mergeRecoveryIdentity is the event identity a recovered merge sends
// under: repo#pr plus the merge SHA, so every deploy event of one rollout
// that recovers the same merge dedups at the Restate ingress.
func mergeRecoveryIdentity(repo string, prNumber int, mergeSHA string) string {
	return "merge-recovery:" + restateclient.PRObjectKey(repo, prNumber) + "@" + strings.ToLower(strings.TrimSpace(mergeSHA))
}

// shouldRecoverMerge decides whether a GitHub commit lookup for sha proves
// a merge PRObject never saw: GitHub reports the PR merged with sha as its
// merge commit, and PRObject tracks the PR but not as merged on sha. A head
// match or an unmerged PR never infers a merge, and an untracked PR (no
// registration or hydrated row) gets no state from a deploy event.
func shouldRecoverMerge(pr github.CommitPR, sha string, view restateclient.DeployView) bool {
	if !pr.MergedOn(sha) || !view.Found {
		return false
	}
	return !strings.EqualFold(strings.TrimSpace(view.MergeSHA), strings.TrimSpace(sha))
}

// recoverLostMerge tells PRObject a PR merged when a deploy, rollout or
// main-branch commit SHA resolved to it only through the GitHub fallback
// (the SHA index missed) because its closed and main-push webhooks were
// lost. It sends HandleMerged before the caller forwards the deploy event,
// so the deploy event lands on a merged PR. Legacy events mirror it in
// shadow mode (legacy itself backfilled the row merge SHA); restate-owned
// events journal it. Failures are logged: the next deploy event of the same
// rollout retries the recovery.
func (s *RouterServer) recoverLostMerge(ctx context.Context, path, repo, sha string, pr github.CommitPR) {
	if !pr.MergedOn(sha) {
		return
	}
	c, err := s.ownedRestateClient()
	if err != nil {
		return
	}
	repo = normalizeRepo(repo)
	view, err := c.GetDeployView(ctx, repo, pr.Number)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] restate merge recovery skipped: deploy view read failed",
			"repo", repo, "pr", pr.Number, "merge_sha", sha, "error", err)
		return
	}
	if !shouldRecoverMerge(pr, sha, view) {
		return
	}
	ev := restateclient.PREvent{
		Repo: repo, PRNumber: pr.Number, HeadSHA: pr.HeadSHA,
		MergeSHA: strings.TrimSpace(sha), Branch: pr.Branch,
	}
	sendCtx := restateclient.WithEventIdentity(ctx, mergeRecoveryIdentity(repo, pr.Number, sha))
	if err := s.routePREvent(sendCtx, "HandleMerged", ev); err != nil {
		slog.WarnContext(ctx, "[synapse] restate merge recovery send failed",
			"repo", repo, "pr", pr.Number, "merge_sha", sha, "path", path, "error", err)
		return
	}
	metrics.RecordRestateMergeRecovered(path)
	slog.InfoContext(ctx, "[synapse] restate merge recovered from deploy path",
		"pr", restateclient.PRObjectKey(repo, pr.Number), "merge_sha", sha, "head_sha", pr.HeadSHA, "path", path)
}
