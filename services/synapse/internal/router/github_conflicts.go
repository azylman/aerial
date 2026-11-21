package router

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

func (s *RouterServer) isAutoUpdateAuthorAllowed(author string) bool {
	if s == nil {
		return false
	}
	return github.IsAutoUpdateAuthorAllowed(author, s.cfgSrc.Current().AutoUpdatePRAuthors)
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
	if updated {
		// Authoritative: the Restate PRObject decides the republish and the
		// Postgres projection is never consulted. The atomic transition
		// above stays the guard; only the publish verdict moves to Restate.
		if s.restateAuthoritative() {
			return s.publishConflictAuthoritative(ctx, repo, prNumber, branch, headSHA, targetID)
		}
		// Journal before the legacy publish and conflict mark: a journal
		// failure leaves nothing recorded, so the redelivery retries the
		// journal instead of returning early via WasConflictPublished. The
		// merged HandleConflict verdict commits the conflict keys and
		// records the carried fact (or, restate-owned, publishes it).
		fact := s.conflictFact(ctx, repo, prNumber, branch, headSHA, targetID)
		if err := s.routeConflict(ctx, repo, prNumber, branch, headSHA, fact); err != nil {
			return false, err
		}
		if restateOwnsEffects(ctx) {
			return true, nil
		}
		if err := s.publishLegacyFact(ctx, fact); err != nil {
			return false, err
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
	// Authoritative: the Restate PRObject dedupes the repeat by recorded
	// conflict SHA and the projection is never consulted.
	if s.restateAuthoritative() {
		return s.publishConflictAuthoritative(ctx, repo, prNumber, branch, headSHA, tid)
	}
	// Journal before the publish check: the journal is the ordered record
	// and Restate dedups the repeat by the recorded conflict SHA.
	fact := s.conflictFact(ctx, repo, prNumber, branch, headSHA, tid)
	if err := s.routeConflict(ctx, repo, prNumber, branch, headSHA, fact); err != nil {
		return false, err
	}
	if restateOwnsEffects(ctx) {
		return true, nil
	}
	// Projection-safe (legacy fallback only): suppresses the duplicate
	// legacy publish when the projection already carries this head SHA; a
	// new SHA still publishes. Never runs when authoritative.
	if normHeadSHA != "" {
		if lc, lcErr := s.registry.LoadPRBuildLifecycle(ctx, repo, prNumber); lcErr == nil && strings.EqualFold(strings.TrimSpace(lc.HeadSHA), normHeadSHA) {
			return true, nil
		}
	}
	if err := s.publishLegacyFact(ctx, fact); err != nil {
		return false, err
	}
	return true, nil
}

func (s *RouterServer) handleDirtyPRConflict(ctx context.Context, repo string, pr GitHubPullRequest) {
	handled, err := s.transitionRegisteredConflict(ctx, repo, pr.Number, pr.Head.Ref, pr.Head.SHA)
	if err != nil {
		slog.ErrorContext(ctx, "[synapse] [registry] error transitioning PR to conflict", "pr", pr.Number, "error", err)
		return
	}
	if !handled {
		if alertErr := s.notifyCollaboratorConflict(ctx, repo, pr); alertErr != nil {
			slog.ErrorContext(ctx, "[synapse] [github] error notifying collaborator conflict", "repo", repo, "pr", pr.Number, "error", alertErr)
		}
	}
}

func (s *RouterServer) updatePRBranch(ctx context.Context, repo string, prNumber int) (bool, error) {
	return github.UpdatePRBranch(ctx, s.ghClient(), repo, prNumber)
}

func (s *RouterServer) handleStaleOpenPR(ctx context.Context, repo string, prNumber int) {
	if s.registry == nil {
		return
	}
	fetched, err := github.FetchSinglePR(ctx, s.ghClient(), repo, prNumber)
	if (err != nil && !isGitHubNotFound(err)) || fetched.Merged || strings.EqualFold(strings.TrimSpace(fetched.State), "open") {
		return
	}
	if targetID, closeErr := s.registry.UpdatePRClosedUnmerged(ctx, repo, prNumber); closeErr != nil {
		slog.WarnContext(ctx, "[synapse] [registry] error closing stale PR with missing head ref", "pr", prNumber, "error", closeErr)
	} else {
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        prClosedUnmerged,
			Repo:         repo,
			PRNumber:     prNumber,
			Branch:       fetched.Head.Ref,
			HeadSHA:      fetched.Head.SHA,
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
	openPRsModel, err := github.FetchOpenPullRequests(ctx, s.ghClient(), normRepo)
	if err != nil {
		return fmt.Errorf("list open prs: %w", err)
	}
	openPRs := openPRsModel
	for _, pr := range openPRs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !s.isEligibleAutoUpdatePR(ctx, normRepo, pr) {
			continue
		}
		if s.hasFailedCIForSHA(ctx, normRepo, pr.Number, pr.Head.SHA) {
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
	if merged {
		targetID, err := s.registry.UpdatePRMerged(ctx, res.Repo, res.PRNumber, res.MergeSHA)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		// Journal before the legacy publish: a journal failure leaves no
		// merged event behind, so the redelivery retries the journal
		// instead of duplicating the publish.
		if err := s.routePREvent(ctx, "HandleMerged", restateclient.PREvent{
			Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA,
			MergeSHA: res.MergeSHA, Branch: res.Branch,
		}); err != nil {
			return err
		}
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
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
		}); err != nil {
			return err
		}
		return nil
	}
	targetID, err := s.registry.UpdatePRClosedUnmerged(ctx, res.Repo, res.PRNumber)
	if err != nil {
		return err
	}
	res.TargetID = firstNonEmpty(res.TargetID, targetID)
	// Journal before the legacy publish, matching the merged branch above.
	if err := s.routePREvent(ctx, "HandleClosed", restateclient.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
	}); err != nil {
		return err
	}
	if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
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
	}); err != nil {
		return err
	}
	return nil
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
	if handled {
		return nil
	}
	return s.notifyCollaboratorConflict(ctx, res.Repo, pr)
}

func (s *RouterServer) handlePullRequestRegistryAction(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest) error {
	cfg := s.cfgSrc.Current()
	meta := ParseAerialPRMetadata(pr.GetBody())
	var cfgBotID string
	var defaultOwner bool
	if cfg != nil {
		cfgBotID = cfg.BotID
		defaultOwner = cfg.DefaultOwner
	}
	if isForeignBotPR(meta, cfgBotID) {
		return nil
	}
	shouldRegister := meta.BotID != "" || defaultOwner
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
		shadowWasConflict := s.shadowPreSyncConflict(ctx, res.Repo, res.PRNumber)
		// Journal before the legacy sync: a journal failure leaves the
		// registry untouched, so the redelivery retries both.
		if err := s.routePREvent(ctx, "HandleSynchronized", restateclient.PREvent{
			Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, BeforeSHA: res.BeforeSHA,
			Branch: res.Branch, Title: pr.Title,
		}); err != nil {
			return err
		}
		targetID, err := s.registry.UpdatePRSync(ctx, res.Repo, res.PRNumber, res.HeadSHA)
		if err != nil {
			return err
		}
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
		if shadowWasConflict {
			if err := s.routeConflictResolved(ctx, res); err != nil {
				return err
			}
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
	if s == nil || !s.cfgSrc.Current().DefaultOwner || !isDependabotPR(p) || (res.Action != ghActionOpened && res.Action != ghActionReopened && res.Action != ghActionSynchronize) {
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
