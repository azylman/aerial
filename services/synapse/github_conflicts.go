package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

func (s *RouterServer) isAutoUpdateAuthorAllowed(author string) bool {
	clean := strings.TrimSpace(author)
	return clean != "" && s != nil && slices.ContainsFunc(s.cfgSrc.Current().AutoUpdatePRAuthors, func(a string) bool {
		return strings.EqualFold(clean, strings.TrimSpace(a))
	})
}

func (s *RouterServer) transitionRegisteredConflict(ctx context.Context, repo string, prNumber int, branch, headSHA string) (bool, error) {
	if s.registry == nil {
		return false, nil
	}
	targetID, updated, err := s.registry.AtomicTransitionConflict(ctx, repo, prNumber)
	if err != nil {
		return false, err
	}
	normHeadSHA := strings.TrimSpace(headSHA)
	key := prCheckKey(repo, prNumber)
	if updated {
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        prConflict,
			Repo:         repo,
			PRNumber:     prNumber,
			Branch:       branch,
			HeadSHA:      headSHA,
			TargetID:     targetID,
			Status:       prConflict,
			RolloutState: s.loadPRRolloutState(ctx, repo, prNumber),
			Quiet:        isDependabotBranch(branch),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			return false, err
		}
		if normHeadSHA != "" {
			s.conflictPublishedSHAs.Store(key, normHeadSHA)
		}
		return true, nil
	}
	tid := targetID
	if tid == "" {
		if resolvedTID, _, _, _, rErr := s.registry.ResolvePRByNumber(ctx, repo, prNumber); rErr == nil && resolvedTID != "" {
			tid = resolvedTID
		}
	}
	if tid == "" {
		return false, nil
	}
	publishedSHA, ok := s.conflictPublishedSHAs.Load(key)
	if ok && strings.EqualFold(fmt.Sprint(publishedSHA), normHeadSHA) {
		return true, nil
	}
	if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:        prConflict,
		Repo:         repo,
		PRNumber:     prNumber,
		Branch:       branch,
		HeadSHA:      headSHA,
		TargetID:     tid,
		Status:       prConflict,
		RolloutState: s.loadPRRolloutState(ctx, repo, prNumber),
		Quiet:        isDependabotBranch(branch),
		OccurredAt:   time.Now().UTC(),
	}); err != nil {
		return false, err
	}
	if normHeadSHA != "" {
		s.conflictPublishedSHAs.Store(key, normHeadSHA)
	}
	return true, nil
}

func (s *RouterServer) handleDirtyPRConflict(ctx context.Context, repo string, pr GitHubPullRequest) {
	handled, err := s.transitionRegisteredConflict(ctx, repo, pr.Number, pr.Head.Ref, pr.Head.SHA)
	if err != nil {
		slog.ErrorContext(ctx, "[synapse] [registry] error transitioning PR to conflict", "pr", pr.Number, "error", err)
		return
	}
	if pr.Head.SHA != "" {
		s.nonRoboRevFailedSHAs.Store(prCheckKey(repo, pr.Number), strings.TrimSpace(pr.Head.SHA))
	}
	s.recordFailedCISHA(repo, pr.Number, pr.Head.SHA)
	if !handled {
		if alertErr := s.notifyCollaboratorConflict(ctx, repo, pr); alertErr != nil {
			slog.ErrorContext(ctx, "[synapse] [github] error notifying collaborator conflict", "repo", repo, "pr", pr.Number, "error", alertErr)
		}
	}
}

func (s *RouterServer) updatePRBranch(ctx context.Context, repo string, prNumber int) (bool, error) {
	req, err := s.newGitHubRequest(ctx, http.MethodPut, fmt.Sprintf("/repos/%s/pulls/%d/update-branch", normalizeRepo(repo), prNumber), strings.NewReader(`{}`))
	if err != nil {
		return false, fmt.Errorf("create update-branch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.githubClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("update-branch request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
		return false, nil
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if readErr != nil {
		body = nil
	}
	bodyLower := strings.ToLower(string(body))
	if resp.StatusCode == http.StatusForbidden {
		remaining := resp.Header.Get("X-Ratelimit-Remaining")
		return false, fmt.Errorf("github api rate limit or forbidden (remaining: %s)", remaining)
	}
	if resp.StatusCode == http.StatusConflict || (resp.StatusCode == http.StatusUnprocessableEntity && strings.Contains(bodyLower, "conflict")) {
		return true, nil
	}
	if resp.StatusCode == http.StatusUnprocessableEntity && strings.Contains(bodyLower, "no new commits on the base branch") {
		return false, nil
	}
	return false, fmt.Errorf("update-branch returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

func (s *RouterServer) handleStaleOpenPR(ctx context.Context, repo string, prNumber int) {
	s.clearFailedCISHA(repo, prNumber)
	s.updatedForCurrentMain.Delete(prCheckKey(repo, prNumber))
	if s.registry == nil {
		return
	}
	if targetID, closeErr := s.registry.UpdatePRClosedUnmerged(ctx, repo, prNumber); closeErr != nil {
		slog.WarnContext(ctx, "[synapse] [registry] error closing stale PR with missing head ref", "pr", prNumber, "error", closeErr)
	} else {
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        prClosedUnmerged,
			Repo:         repo,
			PRNumber:     prNumber,
			TargetID:     targetID,
			Status:       prClosedUnmerged,
			RolloutState: s.loadPRRolloutState(ctx, repo, prNumber),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			slog.WarnContext(ctx, "[synapse] [pr_lifecycle] failed publishing lifecycle event", "error", err)
		}
	}
}

func (s *RouterServer) isEligibleAutoUpdatePR(ctx context.Context, repo string, pr GitHubPullRequest) bool {
	if pr.Draft || !s.isAutoUpdateAuthorAllowed(pr.User.Login) {
		return false
	}
	if !s.cfgSrc.Current().DefaultOwner && !s.isPROwnedLocally(ctx, repo, pr.GetNumber()) {
		return false
	}
	return true
}

func (s *RouterServer) checkOpenPRConflicts(ctx context.Context, repo string) error {
	normRepo := normalizeRepo(repo)
	if len(s.cfgSrc.Current().AutoUpdatePRAuthors) == 0 || normRepo == "" {
		return nil
	}
	var openPRs []GitHubPullRequest
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/pulls?state=open&base=main&sort=created&direction=asc&per_page=100", normRepo), 5<<20, &openPRs); err != nil {
		return fmt.Errorf("list open prs: %w", err)
	}
	for _, pr := range openPRs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !s.isEligibleAutoUpdatePR(ctx, normRepo, pr) {
			continue
		}
		hasFailedOrPending := s.hasFailedCIForSHA(ctx, normRepo, pr.Number, pr.Head.SHA)
		if s.wasUpdatedForCurrentMain(normRepo, pr.Number) {
			// If the in-flight PR updated for current main actually failed CI, continue to update the next clean PR.
			// Otherwise (checks are still pending, passed, or initializing), break to prevent concurrent branch updates.
			if hasFailedOrPending && s.isCachedFailedCISHA(normRepo, pr.Number, pr.Head.SHA) {
				continue
			}
			break
		}
		if hasFailedOrPending {
			continue
		}
		conflict, err := s.updatePRBranch(ctx, normRepo, pr.Number)
		if err != nil {
			errLower := strings.ToLower(err.Error())
			if strings.Contains(err.Error(), "403") || strings.Contains(errLower, "rate limit") {
				return err
			}
			if strings.Contains(errLower, "head ref does not exist") {
				s.handleStaleOpenPR(ctx, normRepo, pr.Number)
				continue
			}
			slog.WarnContext(ctx, "[synapse] [github] error updating open PR branch", "pr", pr.Number, "error", err)
			continue
		}
		if conflict {
			s.handleDirtyPRConflict(ctx, normRepo, pr)
			continue
		}
		s.markUpdatedForCurrentMain(normRepo, pr.Number)
		break
	}
	return nil
}

func (s *RouterServer) healDependabotPR(ctx context.Context, res *ResolvedGitHubEvent) {
	if s == nil || !s.cfgSrc.Current().DefaultOwner || !strings.HasPrefix(strings.TrimSpace(res.Branch), "dependabot/") {
		return
	}
	targetID := s.resolveHeadlessPRTargetID(ctx, res.Repo, res.PRNumber)
	if targetID == "" {
		return
	}
	if _, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, "", targetID); err != nil {
		slog.WarnContext(ctx, "[synapse] [registry] error self-healing dependabot PR on CI failure", "repo", res.Repo, "pr", res.PRNumber, "error", err)
	}
}

func (s *RouterServer) handlePRClosedAction(ctx context.Context, res *ResolvedGitHubEvent, merged bool) error {
	s.clearFailedCISHA(res.Repo, res.PRNumber)
	s.updatedForCurrentMain.Delete(prCheckKey(res.Repo, res.PRNumber))
	if merged {
		targetID, err := s.registry.UpdatePRMerged(ctx, res.Repo, res.PRNumber, res.MergeSHA)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		return s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        prMerged,
			Repo:         res.Repo,
			PRNumber:     res.PRNumber,
			Branch:       res.Branch,
			HeadSHA:      res.HeadSHA,
			MergeSHA:     res.MergeSHA,
			TargetID:     res.TargetID,
			Status:       prMerged,
			RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
			Quiet:        isDependabotBranch(res.Branch),
			OccurredAt:   time.Now().UTC(),
		})
	}
	targetID, err := s.registry.UpdatePRClosedUnmerged(ctx, res.Repo, res.PRNumber)
	if err != nil {
		return err
	}
	res.TargetID = firstNonEmpty(res.TargetID, targetID)
	return s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:        prClosedUnmerged,
		Repo:         res.Repo,
		PRNumber:     res.PRNumber,
		Branch:       res.Branch,
		HeadSHA:      res.HeadSHA,
		TargetID:     res.TargetID,
		Status:       prClosedUnmerged,
		RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
		Quiet:        isDependabotBranch(res.Branch),
		OccurredAt:   time.Now().UTC(),
	})
}

func (s *RouterServer) handlePRAutoMergeDisabledAction(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest) error {
	pr = syncPRFallbackFields(res, pr)
	resolved, ok, err := s.resolveAutoMergeDisabledPR(ctx, res, pr)
	if err != nil || !ok {
		return err
	}
	pr = resolved

	if !isPRMergeConflict(pr) {
		slog.InfoContext(ctx, "[synapse] [github] ignoring auto_merge_disabled", "repo", res.Repo, "pr", res.PRNumber, "mergeable_state", pr.MergeableState)
		return nil
	}

	handled, err := s.transitionRegisteredConflict(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA)
	if err != nil {
		return err
	}
	if res.HeadSHA != "" {
		s.nonRoboRevFailedSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
	}
	s.recordFailedCISHA(res.Repo, res.PRNumber, res.HeadSHA)
	if handled {
		return nil
	}
	return s.notifyCollaboratorConflict(ctx, res.Repo, pr)
}

func (s *RouterServer) handlePullRequestRegistryAction(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest) error {
	cfg := s.cfgSrc.Current()
	meta := ParseAerialPRMetadata(pr.GetBody())
	shouldRegister := true
	if meta.BotID != "" && cfg.BotID != "" && !strings.EqualFold(meta.BotID, cfg.BotID) {
		shouldRegister = false
	} else if meta.BotID == "" && !cfg.DefaultOwner {
		shouldRegister = false
	}
	targetID := meta.TargetID
	if IsHeadlessSessionKey(targetID) {
		targetID = s.resolveHeadlessPRTargetID(ctx, res.Repo, res.PRNumber)
	}
	if shouldRegister && s.registry != nil && targetID != "" && (res.Action != ghActionClosed || pr.Merged) {
		// Self-heal DB registration only; initial Discord widget delivery on opened is owned by Brain's /internal/pr/register to avoid duplicate message creation races.
		resolvedTargetID, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, pr.Title, targetID)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, resolvedTargetID, targetID)
	}
	switch res.Action {
	case ghActionClosed:
		return s.handlePRClosedAction(ctx, res, pr.Merged)
	case ghActionSynchronize:
		targetID, err := s.registry.UpdatePRSync(ctx, res.Repo, res.PRNumber, res.HeadSHA)
		if err != nil {
			return err
		}
		s.clearFailedCISHA(res.Repo, res.PRNumber)
		s.nonRoboRevFailedSHAs.Delete(prCheckKey(res.Repo, res.PRNumber))
		s.cancelledCheckSHAs.Delete(prCheckKey(res.Repo, res.PRNumber))
		s.failedCheckRuns.Delete(prCheckKey(res.Repo, res.PRNumber))
		s.collaboratorAlertedSHAs.Delete(prCheckKey(res.Repo, res.PRNumber))
		s.collaboratorConflictSHAs.Delete(prCheckKey(res.Repo, res.PRNumber))
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        ghActionSynchronize,
			Repo:         res.Repo,
			PRNumber:     res.PRNumber,
			Branch:       res.Branch,
			HeadSHA:      res.HeadSHA,
			Title:        pr.Title,
			TargetID:     res.TargetID,
			Status:       prOpen,
			RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			return err
		}
	case "auto_merge_disabled":
		return s.handlePRAutoMergeDisabledAction(ctx, res, pr)
	}
	return nil
}

func isDependabotBranch(branch string) bool {
	return strings.HasPrefix(strings.TrimSpace(branch), "dependabot/")
}

func isDependabotPR(p GitHubPullRequestPayload) bool {
	return isDependabotBranch(p.PullRequest.Head.Ref) &&
		(strings.EqualFold(strings.TrimSpace(p.PullRequest.User.Login), "dependabot[bot]") ||
			strings.EqualFold(strings.TrimSpace(p.Sender.Login), "dependabot[bot]"))
}

func (s *RouterServer) handleDependabotPROpen(ctx context.Context, res *ResolvedGitHubEvent, p GitHubPullRequestPayload) {
	if s == nil || !s.cfgSrc.Current().DefaultOwner || !isDependabotPR(p) || (res.Action != ghActionOpened && res.Action != "reopened" && res.Action != ghActionSynchronize) {
		return
	}
	if s.registry == nil || res.PRNumber <= 0 {
		return
	}
	targetID := s.resolveHeadlessPRTargetID(ctx, res.Repo, res.PRNumber)
	if targetID == "" {
		return
	}
	resolvedTargetID, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, p.PullRequest.Title, targetID)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [registry] error registering dependabot PR", "repo", res.Repo, "pr", res.PRNumber, "error", err)
		return
	}
	res.TargetID = firstNonEmpty(res.TargetID, resolvedTargetID, targetID)
}
