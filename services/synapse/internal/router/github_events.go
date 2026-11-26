package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// GitHub path: for every webhook job synapse is a Postgres-free edge. It
// verifies (upstream), parses and
// normalizes the payload, renders the stateless facts and collaborator DMs
// each event carries, and sends one-way events to Restate: events with a PR
// number go to PRObject (key repo#pr), SHA-only events go to the RepoObject
// SHA handlers, which resolve the PR from their own durable index. It asks
// Restate for no verdicts and performs no lifecycle effect: PRObject/RepoObject decide, commit, publish facts, wake
// agents, run GitHub/Flux effects and write the registry projection.
//
// Stateless reads stay here: the "(#N)" commit-message parse, the GitHub
// commits-API PR lookup, the mergeability fetch for auto_merge_disabled, the
// RoboRev comment fetch for an empty failure body, and the build-metadata
// GitHub reads (state comes from PRObject.GetBuildLifecycle).
//
// Owner hints: only PR-body bot metadata on opened claims the owner
// (HandleOpened). Every other event carries the body agent or nothing as a
// hint that never overwrites a stored owner, and the target ID only fills a
// missing one.
//
// Main-branch workflow_run handling (image dispatch and promotion, failed
// job reruns, deploy failures) is deploy-path work: processOwnedWorkflowRun
// hands it to processOwnedMainWorkflowRun in restate_owned_deploy.go.

// processRestateOwnedGitHubEvent dispatches one webhook job by event type.
func (s *RouterServer) processRestateOwnedGitHubEvent(ctx context.Context, event, delivery string, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	switch event {
	case "ping":
		return s.processGitHubPingEvent(delivery, body, res)
	case ghEventPullRequest:
		return s.processOwnedPullRequest(ctx, delivery, body, res)
	case ghEventCheckRun:
		return s.processOwnedCheckRun(ctx, delivery, body, res)
	case ghEventWorkflowRun:
		return s.processOwnedWorkflowRun(ctx, delivery, body, res)
	case "push":
		return s.processOwnedPush(ctx, body, res)
	default:
		slog.InfoContext(ctx, "[synapse] [github] [unhandled] event", "event", event, "delivery", delivery, "payload_size_bytes", len(body))
		res.ResolutionSource = "unhandled_event"
		return res, nil
	}
}

// ownedFact renders one stateless aerial.pr.> fact for an event. It carries no rollout state: synapse holds no PR state, and
// PRObject pins the PR identity, fills a missing target and renders the
// rollout state from its committed state when it publishes the fact.
func ownedFact(res *ResolvedGitHubEvent, event, status string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{
		Event:      event,
		Repo:       res.Repo,
		PRNumber:   res.PRNumber,
		Branch:     res.Branch,
		HeadSHA:    res.HeadSHA,
		MergeSHA:   res.MergeSHA,
		TargetID:   res.TargetID,
		Status:     status,
		Quiet:      isDependabotBranch(res.Branch),
		OccurredAt: time.Now().UTC(),
	}
}

// processOwnedPullRequest normalizes one pull_request webhook into its
// PRObject event and the fact PRObject publishes for it.
func (s *RouterServer) processOwnedPullRequest(ctx context.Context, delivery string, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubPullRequestPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal pull_request payload: %w", err)
	}
	populatePullRequestEvent(&p, res)
	slog.InfoContext(ctx, "[synapse] [restate] pull_request", "repo", res.Repo, "pr", res.PRNumber, "action", res.Action, "head_sha", res.HeadSHA, "merge_sha", res.MergeSHA, "delivery", delivery)
	meta := ResolveAerialPRMetadata(res.Repo, res.Branch, p.PullRequest.Body)
	if meta.TargetID == "" && ExtractChunkIDFromBranch(res.Repo, res.Branch) != "" && s.currentConfig().DefaultOwner {
		meta.TargetID = s.systemChannelTarget()
	}
	if s.isForeignBotEvent(meta) || res.PRNumber <= 0 {
		return res, nil
	}
	// Self-registration first (the stored target wins in PRObject): it
	// alone fills the target and the opened/merged title, and an unmerged
	// close registers nothing.
	res.TargetID = ""
	if reg, ok := s.pullRequestSelfRegistration(res, p); ok {
		if err := s.routeSelfRegistration(ctx, res, reg); err != nil {
			return nil, err
		}
		res.TargetID = reg.TargetID
	}
	title := p.PullRequest.Title
	// Reopened and synchronize carry the title (PRObject fills it only
	// when empty); every other action
	// leaves it to the self-registration.
	ev := prwire.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
		Agent: meta.OwningAgent(),
	}
	var handler string
	switch res.Action {
	case ghActionOpened:
		handler = prwire.PRHandlerOpened
		fact := ownedFact(res, ghActionOpened, prOpen)
		fact.Title = title
		ev.Facts = facts(fact)
	case ghActionReopened:
		if strings.TrimSpace(res.HeadSHA) == "" {
			return res, nil
		}
		handler = prwire.PRHandlerReopened
		ev.Title = title
	case ghActionSynchronize:
		handler = prwire.PRHandlerSynchronized
		ev.Title = title
		ev.BeforeSHA = res.BeforeSHA
		fact := ownedFact(res, ghActionSynchronize, prOpen)
		fact.Title = title
		fact.Quiet = false
		ev.Facts = facts(fact)
	case ghActionClosed:
		if p.PullRequest.Merged {
			handler = prwire.PRHandlerMerged
			ev.MergeSHA = res.MergeSHA
			ev.Facts = facts(ownedFact(res, prMerged, prMerged))
		} else {
			handler = prwire.PRHandlerClosed
			ev.Facts = facts(ownedFact(res, prClosedUnmerged, prClosedUnmerged))
		}
	case "auto_merge_disabled":
		return res, s.processOwnedAutoMergeDisabled(ctx, res, p.PullRequest)
	default:
		return res, nil
	}
	if err := s.sendOwnedPR(ctx, handler, ev); err != nil {
		return nil, err
	}
	return res, nil
}

// processOwnedAutoMergeDisabled confirms an actionable merge conflict with
// a GitHub mergeability read, then routes it: collaborator PRs (no Aerial
// metadata, a configured collaborator author) get the rendered DM on
// HandleCollaboratorAlert, every other PR gets HandleConflict with the
// conflict fact. PRObject dedupes and publishes.
func (s *RouterServer) processOwnedAutoMergeDisabled(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest) error {
	pr = syncPRFallbackFields(res, pr)
	resolved, ok, err := s.resolveAutoMergeDisabledPR(ctx, res, pr)
	if err != nil || !ok {
		return err
	}
	if !isPRMergeConflict(resolved) {
		slog.InfoContext(ctx, "[synapse] [github] ignoring auto_merge_disabled", "repo", res.Repo, "pr", res.PRNumber, "mergeable_state", resolved.MergeableState)
		return nil
	}
	if s.hasCollaboratorCIAlertsForRepo(res.Repo) {
		if _, _, _, collaborator, cErr := s.resolveCollaboratorAlertTarget(ctx, res.Repo, res.PRNumber, res.HeadSHA, resolved); cErr != nil {
			return cErr
		} else if collaborator {
			return s.notifyCollaboratorConflict(ctx, res.Repo, resolved)
		}
	}
	return s.sendOwnedPR(ctx, prwire.PRHandlerConflict, prwire.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
		TargetID: res.TargetID, Facts: facts(ownedFact(res, prConflict, prConflict)),
	})
}

// resolveOwnedPayloadPR takes the PR from the webhook payload only; a
// SHA-only event leaves PRNumber zero so RepoObject resolves it.
func resolveOwnedPayloadPR(res *ResolvedGitHubEvent, prs []GitHubCheckRunPR, source string) {
	if len(prs) == 0 {
		return
	}
	res.PRNumber = prs[0].Number
	if res.Branch == "" {
		res.Branch = prs[0].Head.Ref
	}
	res.ResolutionSource = source
}

// resolveOwnedCommitPR fills a missing PR from the GitHub commits API, the
// stateless lookup that stays in synapse.
func (s *RouterServer) resolveOwnedCommitPR(ctx context.Context, res *ResolvedGitHubEvent) {
	if res.PRNumber > 0 || res.HeadSHA == "" || prwire.IsZeroSHA(res.HeadSHA) {
		return
	}
	prNum, branch, err := s.resolvePRFromCommitSHA(ctx, res.Repo, res.HeadSHA)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [github] failed resolving PR for commit", "event", res.Event, "head_sha", res.HeadSHA, "error", err)
		return
	}
	if prNum <= 0 {
		return
	}
	res.PRNumber = prNum
	res.Branch = strutil.FirstNonEmpty(branch, res.Branch)
	res.ResolutionSource = "commit_sha_lookup"
}

// processOwnedCheckRun normalizes one check_run webhook.
func (s *RouterServer) processOwnedCheckRun(ctx context.Context, delivery string, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubCheckRunPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal check_run payload: %w", err)
	}
	res.Repo = p.Repository.FullName
	res.Action = p.Action
	res.CheckName = p.CheckRun.Name
	res.HeadSHA = p.CheckRun.HeadSHA
	res.Status = p.CheckRun.Status
	res.Conclusion = p.CheckRun.Conclusion
	if model.IsBuildMetadataCheck(res.CheckName) {
		return res, s.processOwnedBuildMetadataCheck(ctx, p, res)
	}
	resolveOwnedPayloadPR(res, p.CheckRun.PullRequests, "check_run_payload")
	slog.InfoContext(ctx, "[synapse] [restate] check_run", "repo", res.Repo, "check", res.CheckName, "head_sha", res.HeadSHA, "pr", res.PRNumber, "status", res.Status, "conclusion", res.Conclusion, "delivery", delivery)
	details := strutil.FirstNonEmpty(strings.TrimSpace(p.CheckRun.Output.Text), strings.TrimSpace(p.CheckRun.Output.Summary), strings.TrimSpace(p.CheckRun.Output.Title))
	if lines := github.ExtractLogErrorLines(details, 10); len(lines) > 0 {
		details = strings.Join(lines, "\n")
	}
	details = github.SanitizeDetailsForPrompt(details, 2000)
	checkURL := strutil.FirstNonEmpty(strings.TrimSpace(p.CheckRun.HTMLURL), strings.TrimSpace(p.CheckRun.DetailsURL))
	if err := s.sendOwnedCheckOutcome(ctx, res, p.Action, details, checkURL, prwire.RepoHandlerCheckRunBySHA); err != nil {
		return nil, err
	}
	return res, nil
}

// processOwnedWorkflowRun normalizes one workflow_run webhook. PR runs
// carry their check outcome; main-branch runs belong to the deploy path.
func (s *RouterServer) processOwnedWorkflowRun(ctx context.Context, delivery string, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubWorkflowRunPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal workflow_run payload: %w", err)
	}
	res.Repo = p.Repository.FullName
	res.Action = p.Action
	res.CheckName = p.WorkflowRun.Name
	res.HeadSHA = p.WorkflowRun.HeadSHA
	res.Branch = p.WorkflowRun.HeadBranch
	res.Status = p.WorkflowRun.Status
	res.Conclusion = p.WorkflowRun.Conclusion
	resolveOwnedPayloadPR(res, p.WorkflowRun.PullRequests, "workflow_run_payload")
	mainBranch := isMainBranch(res.Branch)
	prBranch := ""
	if mainBranch {
		prBranch = s.resolveOwnedMainPR(ctx, res)
	}
	slog.InfoContext(ctx, "[synapse] [restate] workflow_run", "repo", res.Repo, "workflow", res.CheckName, "head_sha", res.HeadSHA, "pr", res.PRNumber, "branch", res.Branch, "status", res.Status, "conclusion", res.Conclusion, "delivery", delivery)
	if err := s.refreshOwnedWorkflowRunBuildMetadata(ctx, p, res); err != nil {
		logBuildMetadataWarning(ctx, "refreshing build metadata on workflow run", res.Repo, res.PRNumber, err)
	}
	if mainBranch {
		if err := s.processOwnedMainWorkflowRun(ctx, p, res, prBranch); err != nil {
			return nil, err
		}
		return res, nil
	}
	if p.Action == ghStatusCompleted && strings.EqualFold(res.Conclusion, conclusionFailure) && res.PRNumber > 0 && p.WorkflowRun.ID > 0 && p.WorkflowRun.RunAttempt == 1 {
		if strings.Contains(s.fetchLatestRoboRevComment(ctx, res.Repo, res.PRNumber, res.HeadSHA), "## roborev-override") {
			if err := s.sendOwnedRepo(ctx, prwire.RepoHandlerWorkflowRerun, prwire.RepoEvent{
				Repo:      res.Repo,
				PRNumber:  res.PRNumber,
				HeadSHA:   res.HeadSHA,
				CheckName: res.CheckName,
				RunID:     p.WorkflowRun.ID,
			}); err != nil {
				return nil, err
			}
			return res, nil
		}
	}
	details := fmt.Sprintf("Workflow %q concluded with %s", p.WorkflowRun.Name, p.WorkflowRun.Conclusion)
	if err := s.sendOwnedCheckOutcome(ctx, res, p.Action, details, strings.TrimSpace(p.WorkflowRun.HTMLURL), prwire.RepoHandlerWorkflowRunBySHA); err != nil {
		return nil, err
	}
	return res, nil
}

// sendOwnedCheckOutcome sends one check or workflow observation: the
// pending/passed ci_checks status with its check_updated fact, the CI
// recovery verdict with the recovery synchronize fact on success, the pass
// itself, or the failure with the rendered ci_failed fact (plus the
// collaborator DM for an untargeted collaborator PR). PRObject applies the
// failure slots, stale-head guards and dedup. Gate aggregators never fail a
// PR.
func (s *RouterServer) sendOwnedCheckOutcome(ctx context.Context, res *ResolvedGitHubEvent, action, details, checkURL, shaHandler string) error {
	if res.HeadSHA == "" || prwire.IsZeroSHA(res.HeadSHA) {
		return nil
	}
	if err := s.sendOwnedCheckStatus(ctx, res); err != nil {
		return err
	}
	base := prwire.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, MergeSHA: res.MergeSHA,
		CheckName: res.CheckName, Branch: res.Branch,
	}
	switch checkOutcomeHandler(res) {
	case prwire.PRHandlerCheckPassed:
		return s.sendOwnedCheckPassed(ctx, res, action, shaHandler, base)
	case prwire.PRHandlerCheckFailed:
		if prwire.IsGateAggregatorCheck(res.CheckName) {
			return nil
		}
		// Dependabot self-registration goes ahead of the failure.
		if reg, ok := s.dependabotCIHealRegistration(res); ok {
			if err := s.routeSelfRegistration(ctx, res, reg); err != nil {
				return err
			}
		}
		failed := base
		failed.Facts = facts(s.ownedCIFailedFact(ctx, res, details, checkURL))
		if err := s.sendOwnedPROrSHA(ctx, shaHandler, prwire.PRHandlerCheckFailed, res.HeadSHA, failed); err != nil {
			return err
		}
		// Legacy's ci_failure auto-update trigger: RepoObject dedupes it
		// per PR head, so every delivery may send it.
		if res.PRNumber > 0 {
			if err := s.sendOwnedAutoUpdate(ctx, res.Repo, res.PRNumber, res.HeadSHA, "", ""); err != nil {
				return err
			}
		}
		return s.notifyCollaboratorCIFailure(ctx, res, checkURL)
	default:
		return nil
	}
}

// sendOwnedCheckPassed sends one passing check: the CI recovery verdict
// with the recovery synchronize fact on a completed PR check, then the pass.
// Legacy re-runs the open-PR auto-update cascade on a gate or RoboRev
// success and on a recovered check_run success. Gate and RoboRev triggers
// are stateless and go straight to RepoObject; only PRObject knows a check
// recovered, so the recovery carries the policy and PRObject forwards the
// trigger when it applies.
func (s *RouterServer) sendOwnedCheckPassed(ctx context.Context, res *ResolvedGitHubEvent, action, shaHandler string, base prwire.PREvent) error {
	gateTrigger := ownedGateSuccessTrigger(res, action)
	if action == ghStatusCompleted && res.PRNumber > 0 {
		recovered := base
		recovered.Facts = facts(ownedFact(res, ghActionSynchronize, prOpen))
		if res.Event == ghEventCheckRun && !gateTrigger {
			recovered.AutoUpdate = s.autoUpdatePolicy(strings.TrimSpace(res.Repo))
		}
		if err := s.sendOwnedPR(ctx, prwire.PRHandlerCIRecovered, recovered); err != nil {
			return err
		}
	}
	if err := s.sendOwnedPROrSHA(ctx, shaHandler, prwire.PRHandlerCheckPassed, res.HeadSHA, base); err != nil {
		return err
	}
	if gateTrigger {
		return s.sendOwnedAutoUpdate(ctx, res.Repo, res.PRNumber, res.HeadSHA, "", res.CheckName)
	}
	return nil
}

// ownedGateSuccessTrigger reports whether a check outcome is the stateless
// gate_success auto-update trigger: a completed, successful gate
// aggregator or RoboRev check_run on a known PR.
func ownedGateSuccessTrigger(res *ResolvedGitHubEvent, action string) bool {
	return res.Event == ghEventCheckRun && action == ghStatusCompleted && res.Conclusion == conclusionSuccess &&
		res.PRNumber > 0 && (prwire.IsGateAggregatorCheck(res.CheckName) || prwire.IsRoboRevCheck(res.CheckName))
}

// sendOwnedAutoUpdate sends one open-PR auto-update cascade trigger (a main
// push, a CI failure or a gate success) to RepoObject with the rendered
// policy; RepoObject runs the cascade and its update-branch calls.
func (s *RouterServer) sendOwnedAutoUpdate(ctx context.Context, repo string, prNumber int, headSHA, mainSHA, checkName string) error {
	normRepo := strings.TrimSpace(repo)
	return s.sendOwnedRepo(ctx, prwire.RepoHandlerAutoUpdate, prwire.RepoEvent{
		Repo: normRepo, PRNumber: prNumber, HeadSHA: headSHA, MainSHA: mainSHA, CheckName: checkName, AutoUpdate: s.autoUpdatePolicy(normRepo),
	})
}

// sendOwnedCheckStatus records one pending or passed non-gate check_run on
// a known, non-dependabot PR (workflow runs record no ci_checks).
func (s *RouterServer) sendOwnedCheckStatus(ctx context.Context, res *ResolvedGitHubEvent) error {
	if res.Event != ghEventCheckRun || res.PRNumber <= 0 || prwire.IsGateAggregatorCheck(res.CheckName) || model.IsBuildMetadataCheck(res.CheckName) || isDependabotBranch(res.Branch) {
		return nil
	}
	checkState := prwire.ClassifyCheckState(res.Status, res.Conclusion)
	if checkState == "" || checkState == statusFailed {
		return nil
	}
	fact := ownedFact(res, "check_updated", prOpen)
	fact.MergeSHA = ""
	fact.CheckName = res.CheckName
	fact.CheckState = checkState
	return s.sendOwnedPR(ctx, prwire.PRHandlerCheckStatus, prwire.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
		CheckName: res.CheckName, CheckState: checkState, Facts: facts(fact),
	})
}

// ownedCIFailedFact renders the ci_failed fact; PRObject renders the
// rollout state when it publishes.
func (s *RouterServer) ownedCIFailedFact(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) pubsub.PRLifecycleEvent {
	if strings.TrimSpace(details) == "" {
		if prwire.IsRoboRevCheck(res.CheckName) {
			details = github.FormatRoboRevPrompt(res.Repo, res.PRNumber, res.HeadSHA, s.fetchLatestRoboRevComment(ctx, res.Repo, res.PRNumber, res.HeadSHA))
		} else {
			details = github.FormatCheckFailurePrompt(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName, details, checkURL)
		}
	}
	fact := ownedFact(res, prCIFailed, prCIFailed)
	fact.CheckName = res.CheckName
	fact.CheckState = statusFailed
	fact.CheckURL = checkURL
	fact.Details = details
	return fact
}

// processOwnedPush normalizes one push webhook: every main push records the
// main SHA in RepoObject (which owns conflict re-checks and git-push
// dispatch), and a push that resolves to a PR sends the merge to PRObject;
// otherwise the merge rides the RepoObject SHA route.
func (s *RouterServer) processOwnedPush(ctx context.Context, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubPushPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal push payload: %w", err)
	}
	res.Repo = p.Repository.FullName
	res.Branch = p.Ref
	if p.Deleted || prwire.IsZeroSHA(p.After) {
		res.ResolutionSource = "branch_deleted"
		return res, nil
	}
	if !strings.HasPrefix(p.Ref, "refs/heads/") {
		res.ResolutionSource = "non_branch_ref"
		return res, nil
	}
	res.HeadSHA = p.After
	if p.HeadCommit != nil && p.HeadCommit.ID != "" {
		res.HeadSHA = p.HeadCommit.ID
	}
	if p.Ref != "refs/heads/main" {
		res.ResolutionSource = "non_main_branch"
		return res, nil
	}
	if err := s.sendOwnedRepo(ctx, prwire.RepoHandlerMainPush, prwire.RepoEvent{Repo: res.Repo, MainSHA: res.HeadSHA}); err != nil {
		return nil, err
	}
	res.Branch = ""
	if p.HeadCommit != nil {
		if prNum := extractPRNumberFromCommitMessage(p.HeadCommit.Message); prNum > 0 {
			res.PRNumber = prNum
			res.ResolutionSource = "commit_message"
		}
	}
	s.resolveOwnedCommitPR(ctx, res)
	res.MergeSHA = res.HeadSHA
	slog.InfoContext(ctx, "[synapse] [restate] push", "repo", res.Repo, "ref", p.Ref, "head_sha", res.HeadSHA, "resolved_pr", res.PRNumber, "source", res.ResolutionSource, "delivery", res.Delivery)
	if err := s.sendOwnedPROrSHA(ctx, prwire.RepoHandlerPushBySHA, prwire.PRHandlerMerged, res.HeadSHA, prwire.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, MergeSHA: res.MergeSHA, Branch: res.Branch,
		Facts: facts(ownedFact(res, prMerged, prMerged)),
	}); err != nil {
		return nil, err
	}
	// Legacy's main_push auto-update trigger.
	if err := s.sendOwnedAutoUpdate(ctx, res.Repo, 0, "", res.HeadSHA, ""); err != nil {
		return nil, err
	}
	// Git-push dispatch is repo-level: RepoObject forwards the push to the
	// Flux receiver whether or not a PR resolved, so a direct commit to main
	// still deploys. Only the default owner dispatches it; synapse reads no
	// PR targets.
	if s.cfgSrc.Current().DefaultOwner {
		if err := s.sendOwnedRepo(ctx, prwire.RepoHandlerGitPush, prwire.RepoEvent{
			Repo: res.Repo, MainSHA: res.HeadSHA, PRNumber: res.PRNumber, Ref: p.Ref,
		}); err != nil {
			return nil, err
		}
		if res.PRNumber > 0 && buildmeta.MetadataWorkflowPath(res.Repo) == "" {
			if err := s.ownedCompleteNoDeploy(ctx, res.Repo, res.HeadSHA, res.PRNumber, res.Branch); err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}

// restateBuildMetadataStore backs the buildmeta consumer: lifecycle and snapshot reads come from PRObject.GetBuildLifecycle
// and each recorded snapshot is sent as a HandleBuildMetadata event carrying
// the optional fact.
type restateBuildMetadataStore struct {
	srv  *RouterServer
	fact *pubsub.PRLifecycleEvent
}

var _ buildmeta.PRMetadataRegistry = restateBuildMetadataStore{}

func (st restateBuildMetadataStore) lifecycle(ctx context.Context, repo string, prNumber int) (prwire.BuildLifecycle, error) {
	c, err := st.srv.restateClient()
	if err != nil {
		return prwire.BuildLifecycle{}, err
	}
	return c.GetBuildLifecycle(ctx, repo, prNumber)
}

func (st restateBuildMetadataStore) LoadPRBuildLifecycle(ctx context.Context, repo string, prNumber int) (model.PRBuildLifecycle, error) {
	lc, err := st.lifecycle(ctx, repo, prNumber)
	if err != nil || !lc.Found {
		return model.PRBuildLifecycle{}, err
	}
	return model.PRBuildLifecycle{Status: lc.Status, HeadSHA: lc.HeadSHA, MergeSHA: lc.MergeSHA, Started: lc.Started}, nil
}

func (st restateBuildMetadataStore) LoadPRBuildMetadata(ctx context.Context, repo string, prNumber int) (prwire.BuildMetadata, bool, error) {
	lc, err := st.lifecycle(ctx, repo, prNumber)
	if err != nil || !lc.HasBuildMetadata || lc.BuildMetadata == nil {
		return prwire.BuildMetadata{}, false, err
	}
	return *lc.BuildMetadata, true, nil
}

// RecordPRBuildMetadata sends the snapshot; PRObject applies its own
// revision and ordering fence, so a sent snapshot reports updated.
func (st restateBuildMetadataStore) RecordPRBuildMetadata(ctx context.Context, repo string, prNumber int, metadata prwire.BuildMetadata) (bool, error) {
	snapshot := metadata
	ev := prwire.PREvent{Repo: repo, PRNumber: prNumber, HeadSHA: metadata.SourceSHA, BuildMetadata: &snapshot}
	if st.fact != nil {
		ev.Facts = facts(*st.fact)
	}
	if err := st.srv.sendOwnedPR(ctx, prwire.PRHandlerBuildMetadata, ev); err != nil {
		return false, err
	}
	return true, nil
}

// ownedBuildConsumer builds the buildmeta consumer over the Restate store.
func (s *RouterServer) ownedBuildConsumer(fact *pubsub.PRLifecycleEvent) *buildmeta.Consumer {
	return buildmeta.NewConsumer(s, restateBuildMetadataStore{srv: s, fact: fact})
}

// processOwnedBuildMetadataCheck ingests one transport check: the PR comes
// from the payload or the commits API, the snapshot from GitHub, and the
// check_updated fact rides the HandleBuildMetadata event.
func (s *RouterServer) processOwnedBuildMetadataCheck(ctx context.Context, p GitHubCheckRunPayload, res *ResolvedGitHubEvent) error {
	if buildmeta.MetadataWorkflowPath(strings.TrimSpace(res.Repo)) == "" {
		return nil
	}
	resolveOwnedPayloadPR(res, p.CheckRun.PullRequests, "build_metadata")
	s.resolveOwnedCommitPR(ctx, res)
	if res.PRNumber <= 0 {
		return nil
	}
	fact := ownedFact(res, "check_updated", "")
	if _, err := s.ownedBuildConsumer(&fact).RefreshPRBuildMetadata(ctx, res.Repo, res.PRNumber, res.HeadSHA); err != nil {
		return fmt.Errorf("refresh build metadata: %w", err)
	}
	return nil
}

// refreshOwnedWorkflowRunBuildMetadata supersedes a stale attempt and
// refreshes the snapshot for one producer workflow run of a known PR.
func (s *RouterServer) refreshOwnedWorkflowRunBuildMetadata(ctx context.Context, p GitHubWorkflowRunPayload, res *ResolvedGitHubEvent) error {
	repo := strings.TrimSpace(res.Repo)
	if res.PRNumber <= 0 || buildmeta.MetadataWorkflowPath(repo) == "" || (p.WorkflowRun.Path != "" && p.WorkflowRun.Path != buildmeta.MetadataWorkflowPath(repo)) {
		return nil
	}
	consumer := s.ownedBuildConsumer(nil)
	consumer.SupersedeStaleBuildAttempt(ctx, repo, res.PRNumber, workflowRunEvent{ID: p.WorkflowRun.ID, Attempt: p.WorkflowRun.RunAttempt, HeadSHA: p.WorkflowRun.HeadSHA})
	_, err := consumer.RefreshPRBuildMetadata(ctx, repo, res.PRNumber, p.WorkflowRun.HeadSHA)
	return err
}

// checkOutcomeHandler maps a check/workflow conclusion to its PRObject
// handler. Empty means no send: pending, cancelled, and neutral outcomes
// carry no PRObject transition.
func checkOutcomeHandler(res *ResolvedGitHubEvent) string {
	if res == nil {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(res.Conclusion)) {
	case conclusionSuccess:
		return prwire.PRHandlerCheckPassed
	case conclusionFailure, conclusionTimedOut:
		return prwire.PRHandlerCheckFailed
	default:
		return ""
	}
}

// Self-registration: synapse tells PRObject about PRs nobody registered
// through aerial-pr.sh (default-owner dependabot PRs and PRs whose body
// carries bot metadata) with a HandleSelfRegistered event. PRObject fills
// the target and title only when none is stored and seeds the owner only on
// a PR it has not seen, so the stored target always wins.

// prSelfRegistration is one self-registration: the resolved target (always
// a valid snowflake), the title (may be empty) and the owner seed
// (lowercased; empty when unknown).
type prSelfRegistration struct {
	TargetID string
	Title    string
	Agent    string
}

// newSelfRegistration normalizes one registration; ok is false when the
// target is not a valid snowflake.
func newSelfRegistration(targetID, title, agent string) (prSelfRegistration, bool) {
	targetID = model.NormalizeDiscordTargetID(targetID)
	if !model.IsValidDiscordSnowflake(targetID) {
		return prSelfRegistration{}, false
	}
	return prSelfRegistration{
		TargetID: targetID,
		Title:    strings.TrimSpace(title),
		Agent:    strings.ToLower(strings.TrimSpace(agent)),
	}, true
}

// systemChannelTarget is the configured system channel as a target, or ""
// when it is not a valid snowflake.
func (s *RouterServer) systemChannelTarget() string {
	if sys := model.NormalizeDiscordTargetID(s.currentConfig().SystemChannelID); model.IsValidDiscordSnowflake(sys) {
		return sys
	}
	return ""
}

// dependabotSelfRegistration: a default-owner dependabot PR on opened,
// reopened or synchronize registers under the system channel with its title
// and no owner.
func (s *RouterServer) dependabotSelfRegistration(res *ResolvedGitHubEvent, p GitHubPullRequestPayload) (prSelfRegistration, bool) {
	if !s.currentConfig().DefaultOwner || !isDependabotPR(p) || res.PRNumber <= 0 {
		return prSelfRegistration{}, false
	}
	switch res.Action {
	case ghActionOpened, ghActionReopened, ghActionSynchronize:
	default:
		return prSelfRegistration{}, false
	}
	return newSelfRegistration(s.systemChannelTarget(), p.PullRequest.Title, "")
}

// metadataSelfRegistration: a PR
// with bot metadata (or any PR on a default-owner synapse) whose body names
// a target registers on every action except an unmerged close, under the
// body snowflake or, for a headless session key, the system channel, with
// its title and the PR-body owner.
func (s *RouterServer) metadataSelfRegistration(res *ResolvedGitHubEvent, pr GitHubPullRequest) (prSelfRegistration, bool) {
	meta := ResolveAerialPRMetadata(res.Repo, res.Branch, pr.Body)
	if meta.TargetID == "" && ExtractChunkIDFromBranch(res.Repo, res.Branch) != "" && s.currentConfig().DefaultOwner {
		meta.TargetID = s.systemChannelTarget()
	}
	if res.PRNumber <= 0 || s.isForeignBotEvent(meta) || (meta.BotID == "" && !s.currentConfig().DefaultOwner) ||
		(res.Action == ghActionClosed && !pr.Merged) {
		return prSelfRegistration{}, false
	}
	targetID := meta.TargetID
	if model.IsHeadlessSessionKey(targetID) {
		targetID = s.systemChannelTarget()
	}
	return newSelfRegistration(targetID, pr.Title, meta.OwningAgent())
}

// dependabotCIHealRegistration: a CI failure on a default-owner dependabot
// branch registers under the system channel with
// no title and no owner.
func (s *RouterServer) dependabotCIHealRegistration(res *ResolvedGitHubEvent) (prSelfRegistration, bool) {
	if !s.currentConfig().DefaultOwner || !isDependabotBranch(res.Branch) || res.PRNumber <= 0 {
		return prSelfRegistration{}, false
	}
	return newSelfRegistration(s.systemChannelTarget(), "", "")
}

// pullRequestSelfRegistration picks the registration for a pull_request
// event: the dependabot rule runs before the metadata rule.
func (s *RouterServer) pullRequestSelfRegistration(res *ResolvedGitHubEvent, p GitHubPullRequestPayload) (prSelfRegistration, bool) {
	if reg, ok := s.dependabotSelfRegistration(res, p); ok {
		return reg, true
	}
	return s.metadataSelfRegistration(res, p.PullRequest)
}

// selfRegistrationEvent renders the HandleSelfRegistered payload.
func selfRegistrationEvent(res *ResolvedGitHubEvent, reg prSelfRegistration) prwire.PREvent {
	return prwire.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
		Title: reg.Title, TargetID: reg.TargetID, Agent: reg.Agent,
	}
}

// routeSelfRegistration sends one registration to PRObject. The agent is
// the owner seed for a PR PRObject has not seen; it never overwrites a
// stored owner.
func (s *RouterServer) routeSelfRegistration(ctx context.Context, res *ResolvedGitHubEvent, reg prSelfRegistration) error {
	return s.sendOwnedPR(ctx, prwire.PRHandlerSelfRegistered, selfRegistrationEvent(res, reg))
}
