package router

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
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

// autoUpdatePolicy renders the stateless auto-update config RepoObject's
// cascade needs for repo: the allowlisted authors, the default-owner flag,
// and the repo's collaborator alert rules sorted by author.
func (s *RouterServer) autoUpdatePolicy(repo string) *restateclient.AutoUpdatePolicy {
	cfg := s.currentConfig()
	policy := &restateclient.AutoUpdatePolicy{Authors: slices.Clone(cfg.AutoUpdatePRAuthors), DefaultOwner: cfg.DefaultOwner}
	for _, rule := range cfg.CollaboratorCIAlerts {
		if !strings.EqualFold(rule.Repo, repo) {
			continue
		}
		authors := make([]string, 0, len(rule.Authors))
		for author := range rule.Authors {
			authors = append(authors, author)
		}
		slices.Sort(authors)
		for _, author := range authors {
			if mention := strings.TrimSpace(rule.Authors[author]); mention != "" {
				policy.Collaborators = append(policy.Collaborators, restateclient.CollaboratorAlert{Author: author, ChannelID: rule.ChannelID, Mention: mention})
			}
		}
	}
	return policy
}

// autoUpdateOpenPRs is the open-PR auto-update cascade trigger: a main push
// (prNumber 0, mainSHA), a CI failure, or a gate success (the PR head and
// check). It journals the trigger to RepoObject.HandleAutoUpdate carrying
// the rendered policy. A restate-owned trigger stops there: RepoObject runs
// the cascade, and a send failure surfaces so the job retries. A
// legacy-owned trigger runs today's cascade unchanged (a send failure is
// only logged) and records it as [synapse-effect] under the match key
// RepoObject records as intended.
func (s *RouterServer) autoUpdateOpenPRs(ctx context.Context, repo string, prNumber int, headSHA, mainSHA, checkName string) error {
	normRepo := normalizeRepo(repo)
	if normRepo == "" {
		return nil
	}
	ev := restateclient.RepoEvent{Repo: normRepo, PRNumber: prNumber, HeadSHA: headSHA, MainSHA: mainSHA, CheckName: checkName, AutoUpdate: s.autoUpdatePolicy(normRepo)}
	if err := s.routeRepoEvent(ctx, "HandleAutoUpdate", ev); err != nil {
		if restateOwnsEffects(ctx) {
			return err
		}
		slog.WarnContext(ctx, "[synapse] [restate-cutover] auto-update journal failed; legacy cascade continues", "repo", normRepo, "error", err)
	}
	if restateOwnsEffects(ctx) {
		return nil
	}
	err := s.checkOpenPRConflicts(ctx, normRepo)
	trigger := firstNonEmpty(strings.TrimSpace(headSHA), strings.TrimSpace(mainSHA))
	recordLegacyEffect(ctx, restateclient.EffectGitHubAutoUpdate,
		restateclient.EffectMatchKey(normRepo, prNumber, trigger, restateclient.EffectGitHubAutoUpdate, "", checkName), normRepo, prNumber)
	return err
}

// logAutoUpdateError logs a legacy cascade failure and returns only a
// restate-owned send failure, which must retry the job because nothing else
// runs the cascade.
func logAutoUpdateError(ctx context.Context, repo, trigger string, err error) error {
	if err == nil {
		return nil
	}
	if failed, _ := restateOwnedSendFailure(err); failed {
		return err
	}
	slog.ErrorContext(ctx, "[synapse] [github] error checking open pr conflicts", "repo", repo, "trigger", trigger, "error", err)
	return nil
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
		if err == nil {
			recordLegacyEffect(ctx, restateclient.EffectGitHubUpdateBranch,
				restateclient.EffectMatchKey(normRepo, pr.Number, pr.Head.SHA, restateclient.EffectGitHubUpdateBranch, "", ""), normRepo, pr.Number)
		}
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
	if s == nil {
		return
	}
	reg, ok := s.dependabotCIHealRegistration(ctx, res, s.resolveHeadlessPRTargetID)
	if !ok {
		return
	}
	// Journal before the legacy write: a journal failure leaves the
	// registry untouched, so the redelivery retries both.
	if err := s.routeSelfRegistration(ctx, res, reg); err != nil {
		slog.WarnContext(ctx, "[synapse] [restate-cutover] dependabot CI-failure self-registration journal failed", "repo", res.Repo, "pr", res.PRNumber, "error", err)
		return
	}
	if _, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, reg.Title, reg.TargetID, reg.Agent); err != nil {
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
	if routed, err := s.routeConflictCheck(ctx, res, pr); routed || err != nil {
		return err
	}
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
	meta := ParseAerialPRMetadata(pr.GetBody())
	// Same multi-bot predicate as processPullRequest and the restate-owned
	// path: a PR owned by any configured bot (siblings included) is
	// processed; only a bot outside cfg.Bots is dropped.
	if s.isForeignBotEvent(meta) {
		return nil
	}
	if reg, ok := s.metadataSelfRegistration(ctx, res, pr, s.resolveHeadlessPRTargetID); ok && s.registry != nil {
		// Self-heal DB registration only; initial Discord widget delivery on opened is owned by Brain's /internal/pr/register to avoid duplicate message creation races.
		// Journal before the legacy write: a journal failure leaves the
		// registry untouched, so the redelivery retries both.
		if err := s.routeSelfRegistration(ctx, res, reg); err != nil {
			return err
		}
		resolvedTargetID, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, pr.Title, reg.TargetID, meta.OwningAgent())
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, resolvedTargetID, reg.TargetID)
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
	if s == nil || s.registry == nil {
		return
	}
	reg, ok := s.dependabotSelfRegistration(ctx, res, p, s.resolveHeadlessPRTargetID)
	if !ok {
		return
	}
	// Journal before the legacy write: a journal failure leaves the
	// registry untouched, so the redelivery retries both.
	if err := s.routeSelfRegistration(ctx, res, reg); err != nil {
		slog.WarnContext(ctx, "[synapse] [restate-cutover] dependabot self-registration journal failed", "repo", res.Repo, "pr", res.PRNumber, "error", err)
		return
	}
	resolvedTargetID, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, reg.Title, reg.TargetID, reg.Agent)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [registry] error registering dependabot PR", "repo", res.Repo, "pr", res.PRNumber, "error", err)
		return
	}
	res.TargetID = firstNonEmpty(res.TargetID, resolvedTargetID, reg.TargetID)
}

// routeConflictCheck journals the mergeability conflict check synapse would
// fetch for an auto_merge_disabled PR whose mergeability is still unknown.
// A restate-owned event hands the check to PRObject.HandleConflictCheck
// (carrying the rendered collaborator DM) and reports routed, so synapse
// neither fetches nor transitions. A legacy-owned event journals the check
// for prstate's intended record and falls through to today's fetch; its
// send failure is only logged.
func (s *RouterServer) routeConflictCheck(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest) (bool, error) {
	if !isPRMergeabilityUnknown(pr) || res.PRNumber <= 0 || normalizeRepo(res.Repo) == "" {
		return false, nil
	}
	ev := restateclient.PREvent{Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch, Title: pr.Title}
	if restateOwnsEffects(ctx) {
		ev.EriEffects = s.collaboratorConflictEffects(ctx, res.Repo, pr)
		return true, s.routePREvent(ctx, "HandleConflictCheck", ev)
	}
	if err := s.routePREvent(ctx, "HandleConflictCheck", ev); err != nil {
		slog.WarnContext(ctx, "[synapse] [restate-cutover] conflict check journal failed; legacy fetch continues", "repo", res.Repo, "pr", res.PRNumber, "error", err)
	}
	return false, nil
}

// collaboratorConflictEffects renders the collaborator conflict DM for an
// unregistered collaborator PR, or nil when no rule routes one. PRObject
// fires it only if the check finds a conflict and the PR has no target.
func (s *RouterServer) collaboratorConflictEffects(ctx context.Context, repo string, pr GitHubPullRequest) []restateclient.EriEffect {
	if !s.hasCollaboratorCIAlertsForRepo(repo) {
		return nil
	}
	resolved, channelID, mention, ok, err := s.resolveCollaboratorAlertTarget(ctx, repo, pr.Number, pr.Head.SHA, pr)
	if err != nil || !ok {
		return nil
	}
	sha := firstNonEmpty(resolved.Head.SHA, pr.Head.SHA)
	branch := firstNonEmpty(resolved.Head.Ref, pr.Head.Ref)
	return collaboratorDMEffect(channelID, formatCollaboratorConflictMessage(mention, repo, resolved.Number, resolved.Title, branch, sha))
}
