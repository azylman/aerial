package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// Deploy path: the deploy-side twin of restate_owned_github.go. For a
// JetStream deploy or workflow_run job, or one watcher call, synapse stays a
// Postgres-free edge: it observes the cluster and GitHub statelessly
// (Kustomization status,
// workload rollout state, image tags a workflow built, commit history),
// reads PR state only from Restate (PRObject.GetDeployView,
// GetBuildLifecycle, RepoObject.ListRollouts and ResolvePRBySHA), and sends
// one-way observations. PRObject/RepoObject apply the transitions, publish
// the facts, run the Flux/image/GitHub effects and write the registry
// projection.
//
//   - Kustomization watcher: the deploying set comes from the Restate
//     rollout index, each rollout's recorded requests from its deploy view.
//     Gate outcomes go to HandleRolloutProgress (ready jobs, completed
//     Kustomization, rollout_updated fact) and, on completion, to
//     HandleDeploySuccess, which PRObject publishes once per PR when the
//     last recorded Kustomization completes. Failures go to
//     HandleDeployFailed with the deploy_failed fact, and lost reconcile
//     requests to HandleReconcileLost, which re-patches in prstate.
//   - Deploy alerts: watcher deferral records the rollout through
//     HandleKustomizationRequested (PRObject patches the reconciles);
//     started/expected jobs go to HandleDeployObserved; success, sync and
//     failure outcomes go to HandleDeploySuccess / HandleDeployObserved /
//     HandleDeployFailed with their facts. PR-less alerts keep the
//     stateless system-alert path, which never touches Restate.
//   - Main-branch workflow runs: attempt-1 failures rerun failed jobs via
//     RepoObject.HandleWorkflowRerun; later failures report deploy_failed
//     to the PR (by number or through the RepoObject SHA route); successful
//     image builds go to RepoObject.HandleImagePromotion, which promotes,
//     starts the PR rollout and resolves a SHA-only commit to its PR from
//     the durable index.

// ownedDeployView reads PRObject's deploy read model for one PR.
func (s *RouterServer) ownedDeployView(ctx context.Context, repo string, prNumber int) (prwire.DeployView, error) {
	c, err := s.restateClient()
	if err != nil {
		return prwire.DeployView{}, err
	}
	view, err := c.GetDeployView(ctx, repo, prNumber)
	if err != nil {
		return prwire.DeployView{}, fmt.Errorf("restate deploy view %s#%d: %w", strings.TrimSpace(repo), prNumber, err)
	}
	return view, nil
}

// ownedTrackedRollouts lists every rollout the watcher gates from the
// Restate rollout index, with its recorded requests from the PR's deploy
// view. A view that no longer shows a deploying rollout is skipped.
func (s *RouterServer) ownedTrackedRollouts(ctx context.Context) ([]trackedRollout, error) {
	c, err := s.restateClient()
	if err != nil {
		return nil, err
	}
	refs, err := c.ListRollouts(ctx)
	if err != nil {
		return nil, fmt.Errorf("list restate rollouts: %w", err)
	}
	var rollouts []trackedRollout
	for _, ref := range refs {
		view, err := s.ownedDeployView(ctx, ref.Repo, ref.PRNumber)
		if err != nil {
			return nil, err
		}
		if t, ok := rolloutFromView(view); ok {
			rollouts = append(rollouts, t)
		}
	}
	return rollouts, nil
}

// rolloutFromView maps a deploying view with recorded requests to the
// watcher's tracked rollout.
func rolloutFromView(v prwire.DeployView) (trackedRollout, bool) {
	if !v.Found || v.PRNumber <= 0 || v.Status != statusDeploying || len(v.KustomizationRequests) == 0 {
		return trackedRollout{}, false
	}
	return trackedRollout{
		PR:       DeployingPR{Repo: v.Repo, PRNumber: v.PRNumber, Branch: v.Branch, HeadSHA: v.HeadSHA, MergeSHA: v.MergeSHA, TargetID: v.TargetID},
		Requests: v.KustomizationRequests,
		Deployed: v.DeployedKustomizations,
	}, true
}

// ownedRolloutFact renders one watcher lifecycle fact for a tracked PR.
// It carries no rollout state: PRObject renders it from the state the
// handler just committed (ready jobs, deployed jobs) when it publishes.
func ownedRolloutFact(pr DeployingPR, name, event, status string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{
		Event: event, Repo: pr.Repo, PRNumber: pr.PRNumber, HeadSHA: pr.HeadSHA, MergeSHA: pr.MergeSHA,
		TargetID: pr.TargetID, JobName: name, Status: status, Quiet: isDependabotBranch(pr.Branch), OccurredAt: time.Now().UTC(),
	}
}

// sendOwnedRolloutProgress reports one gate outcome in the order the parity
// goldens pin (testdata/prlifecycle/parity/README.md): HandleRolloutProgress
// with the ready jobs, the completed Kustomization and the rollout_updated
// fact, then, for a completed Kustomization, HandleDeploySuccess with the
// deploy_success fact. PRObject drops rollout_updated once the rollout is
// deployed and publishes deploy_success once per PR (DeploySuccessDue), so
// completion fires on whichever Kustomization finishes last.
func (s *RouterServer) sendOwnedRolloutProgress(ctx context.Context, pr DeployingPR, name string, readyJobs []string, done bool) error {
	if len(readyJobs) == 0 && !done {
		return nil
	}
	ev := prwire.PREvent{
		Repo: pr.Repo, PRNumber: pr.PRNumber, HeadSHA: pr.HeadSHA, MergeSHA: pr.MergeSHA, Branch: pr.Branch, JobName: name,
		DeployedJobs: readyJobs, Facts: facts(ownedRolloutFact(pr, name, "rollout_updated", statusDeploying)),
	}
	if done {
		ev.DeployedKustomizations = []string{name}
	}
	if err := s.sendOwnedPR(ctx, prwire.PRHandlerRolloutProgress, ev); err != nil || !done {
		return err
	}
	return s.sendOwnedPR(ctx, prwire.PRHandlerDeploySuccess, prwire.PREvent{
		Repo: pr.Repo, PRNumber: pr.PRNumber, HeadSHA: pr.HeadSHA, MergeSHA: strutil.FirstNonEmpty(pr.MergeSHA, pr.HeadSHA), JobName: name,
		Facts: facts(ownedRolloutFact(pr, name, eventDeploySuccess, statusDeployed)),
	})
}

// sendOwnedReconcileLost asks each PR whose recorded request Flux lost to
// re-patch it; prstate keys the re-patch on the observed reconcile value.
func (s *RouterServer) sendOwnedReconcileLost(ctx context.Context, prs []DeployingPR, obs kustomizationObservation) error {
	errs := make([]error, 0, len(prs))
	for _, pr := range prs {
		slog.InfoContext(ctx, "[synapse] [flux-watch] re-requesting lost reconcile through PRObject", "namespace", obs.Ref.Namespace, "name", obs.Ref.Name, "repo", pr.Repo, "pr", pr.PRNumber)
		errs = append(errs, s.sendOwnedPR(ctx, prwire.PRHandlerReconcileLost, prwire.PREvent{
			Repo: pr.Repo, PRNumber: pr.PRNumber, JobName: obs.Ref.Name, ObservedReconcileAt: obs.LastHandledReconcileAt,
		}))
	}
	return errors.Join(errs...)
}

// ownedDeployFact renders the lifecycle fact for one deploy alert outcome:
// success events without a job name default to
// git-sync, Quiet comes from the branch PRObject recorded, and PRObject
// renders the rollout state from its committed state when it publishes.
func ownedDeployFact(evt DeployEvent, eventName, status, branch string) pubsub.PRLifecycleEvent {
	jobName := evt.JobName
	if (eventName == eventDeploySuccess || eventName == eventSyncSuccess) && jobName == "" {
		jobName = jobGitSync
	}
	return pubsub.PRLifecycleEvent{
		Event: eventName, Repo: evt.Repo, PRNumber: evt.PRNumber, MergeSHA: evt.CommitSHA, JobName: jobName,
		Details: evt.Details, TargetID: evt.TargetID, Status: status, Quiet: isDependabotBranch(branch), OccurredAt: time.Now().UTC(),
	}
}

// sendOwnedDeployFailed reports one PR deploy failure (or rollback) to
// PRObject with the rendered deploy_failed fact.
func (s *RouterServer) sendOwnedDeployFailed(ctx context.Context, evt DeployEvent, branch string) error {
	return s.sendOwnedPR(ctx, prwire.PRHandlerDeployFailed, prwire.PREvent{
		Repo: evt.Repo, PRNumber: evt.PRNumber, MergeSHA: evt.CommitSHA,
		Facts: facts(ownedDeployFact(evt, eventDeployFailed, eventDeployFailed, branch)),
	})
}

// ownedDeferPRToWatcher hands a PR's completion to the watcher: when repo is
// the GitRepository source of a watched Kustomization, it sends the
// rollout (deploying jobs, fresh reconcile requests, the deploy_started
// fact) to PRObject.HandleKustomizationRequested, which records the
// requests before patching them, and reports that the watcher owns the PR.
//
// A PR the deploy view does not find (never seen, or untracked: an
// upstream Flux source commit resolved to a PR nobody registered) is
// handled without tracking: the Kustomizations are patched once and nothing is tracked, so no rollout
// starts that could never complete.
func (s *RouterServer) ownedDeferPRToWatcher(ctx context.Context, repo string, prNum int, view prwire.DeployView) (bool, error) {
	if s.dispatcher == nil || prNum <= 0 {
		return false, nil
	}
	refs, err := s.dispatcher.DiscoverRepoKustomizations(ctx, repo)
	if err != nil {
		return false, fmt.Errorf("discover kustomizations for %s: %w", repo, err)
	}
	if len(refs) == 0 {
		return false, nil
	}
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Name)
	}
	requests := newKustomizationRequests(refs, time.Now().UTC())
	if !view.Found {
		slog.InfoContext(ctx, "[synapse] [deploy] untracked PR: patching kustomizations without tracking a rollout", "repo", repo, "pr", prNum, "kustomizations", strings.Join(names, ", "))
		return true, s.reconcileImageKustomizations(ctx, refs, requests)
	}
	branch := view.Branch
	started := pubsub.PRLifecycleEvent{
		Event: eventDeployStarted, Repo: repo, PRNumber: prNum, Status: statusDeploying,
		Quiet: isDependabotBranch(branch), OccurredAt: time.Now().UTC(),
	}
	if err := s.sendOwnedPR(ctx, prwire.PRHandlerKustomizationRequested, prwire.PREvent{
		Repo: repo, PRNumber: prNum, Branch: branch, Jobs: names,
		KustomizationRequests: requests, Facts: facts(started),
	}); err != nil {
		return false, err
	}
	slog.InfoContext(ctx, "[synapse] [deploy] deferring completion to the kustomization watcher via PRObject", "repo", repo, "pr", prNum, "kustomizations", strings.Join(names, ", "))
	return true, nil
}

// resolveOwnedSHAPR resolves a commit to its PR: the RepoObject durable SHA
// index first, then the stateless GitHub commits-API lookup. A GitHub
// answer that proves a merge PRObject missed (lost closed and main-push
// webhooks) is sent to PRObject before the caller forwards its event.
func (s *RouterServer) resolveOwnedSHAPR(ctx context.Context, repo, sha string) (int, string) {
	sha = strings.TrimSpace(sha)
	if strings.TrimSpace(repo) == "" || sha == "" || prwire.IsZeroSHA(sha) {
		return 0, ""
	}
	if c, err := s.restateClient(); err == nil {
		resp, rErr := c.ResolvePRBySHA(ctx, repo, sha)
		if rErr == nil && resp.Found && resp.PRNumber > 0 {
			return resp.PRNumber, resp.Branch
		}
		if rErr != nil {
			slog.WarnContext(ctx, "[synapse] [restate] RepoObject SHA lookup failed; trying GitHub", "repo", repo, "sha", sha, "error", rErr)
		}
	}
	pr, err := s.resolveCommitPR(ctx, repo, sha)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [github] failed resolving PR for commit", "repo", repo, "sha", sha, "error", err)
		return 0, ""
	}
	s.recoverLostMerge(ctx, mergeRecoveryPathOwnedDeploy, repo, sha, pr)
	return pr.Number, pr.Branch
}

// processOwnedDeployEvent normalizes one deploy alert and sends its PR outcome to PRObject.
func (s *RouterServer) processOwnedDeployEvent(ctx context.Context, evt DeployEvent) (*DeployEvent, error) {
	if evt.PRNumber == 0 && evt.CommitSHA != "" {
		evt.PRNumber, _ = s.resolveOwnedSHAPR(ctx, evt.Repo, evt.CommitSHA)
	}
	var view prwire.DeployView
	if evt.PRNumber > 0 {
		v, err := s.ownedDeployView(ctx, evt.Repo, evt.PRNumber)
		if err != nil {
			return &evt, restateFailure(ctx, "GetDeployView", err)
		}
		view = v
		evt.TargetID = strutil.FirstNonEmpty(evt.TargetID, view.TargetID)
	}
	if evt.TargetID == "" && (evt.Event == eventDeployFailed || evt.Event == eventDeployRollback || evt.Event == eventSyncFailed) {
		evt.TargetID = strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
	}
	slog.InfoContext(ctx, "[synapse] [restate] deploy event", "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber, "commit", evt.CommitSHA, "status", evt.Status)

	s.evaluateFluxEventTriggers(ctx, evt)

	if evt.PRNumber <= 0 {
		// PR-less alerts have no PRObject key: the stateless system-alert
		// path publishes and prompts without touching Restate.
		return s.publishUnownedDeployEvent(ctx, evt)
	}
	waiting := evt.SourceKind == kindKustomization && s.ownedWaitingForImageBuild(ctx, evt.Repo, evt.PRNumber, view)
	if handled, err := s.ownedDeferDeployEventToWatcher(ctx, evt, view, waiting); handled || err != nil {
		return &evt, err
	}
	if evt.Event == eventDeployStarted && evt.JobName != "" && waiting {
		slog.InfoContext(ctx, "[synapse] [deploy] skipping deploy_started (waiting for image build)", "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber)
		return &evt, nil
	}
	expected := model.DedupeTrimmedStrings(evt.ExpectedJobs)
	syncHasExpected, deferred, err := s.ownedSyncExpected(ctx, evt, view, expected)
	if deferred || err != nil {
		return &evt, err
	}
	return &evt, s.publishOwnedDeployEvent(ctx, evt, view.Branch, expected, syncHasExpected, waiting)
}

// ownedSyncExpected handles a sync_success: it records the announced jobs on
// PRObject, reports whether the PR expects deploy jobs, and otherwise
// hands completion to the watcher when the repo sources a Kustomization.
// Other events expect nothing and never defer.
func (s *RouterServer) ownedSyncExpected(ctx context.Context, evt DeployEvent, view prwire.DeployView, expected []string) (hasExpected, deferred bool, err error) {
	if evt.Event != eventSyncSuccess {
		return false, false, nil
	}
	if len(expected) > 0 {
		if sendErr := s.sendOwnedPR(ctx, prwire.PRHandlerDeployObserved, prwire.PREvent{Repo: evt.Repo, PRNumber: evt.PRNumber, MergeSHA: evt.CommitSHA, ExpectedJobs: expected}); sendErr != nil {
			return false, false, sendErr
		}
		return true, false, nil
	}
	if view.HasExpectedDeployJobs || s.ownedWaitingForImageBuild(ctx, evt.Repo, evt.PRNumber, view) {
		return true, false, nil
	}
	deferred, err = s.ownedDeferPRToWatcher(ctx, evt.Repo, evt.PRNumber, view)
	return false, deferred, err
}

// ownedDeferDeployEventToWatcher defers watcher-owned alerts: a watcher-owned
// alert publishes nothing
// itself. Already-tracked alerts defer silently; untracked started/success
// alerts start the rollout through ownedDeferPRToWatcher unless the image
// build is still pending.
func (s *RouterServer) ownedDeferDeployEventToWatcher(ctx context.Context, evt DeployEvent, view prwire.DeployView, waiting bool) (bool, error) {
	tracked := len(view.KustomizationRequests) > 0
	owned, discErr := s.ownedDeployEventWatcherOwnership(ctx, evt, tracked)
	if !owned {
		return false, nil
	}
	if discErr != nil {
		slog.ErrorContext(ctx, "[synapse] [deploy] kustomization discovery failed; suppressing alert without publishing", "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber, "error", discErr)
		return true, nil
	}
	if tracked || (evt.Event != eventDeployStarted && evt.Event != eventDeploySuccess) {
		slog.InfoContext(ctx, "[synapse] [deploy] deferring event to the kustomization watcher", "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber)
		return true, nil
	}
	if waiting {
		slog.InfoContext(ctx, "[synapse] [deploy] watcher-owned event waiting for image build", "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber)
		return false, nil
	}
	return s.ownedDeferPRToWatcher(ctx, evt.Repo, evt.PRNumber, view)
}

// ownedDeployEventWatcherOwnership reports whether the Kustomization watcher
// owns a deploy alert, with the tracked flag read from PRObject.
func (s *RouterServer) ownedDeployEventWatcherOwnership(ctx context.Context, evt DeployEvent, tracked bool) (bool, error) {
	isFluxKind := evt.SourceKind == kindKustomization || evt.SourceKind == kindHelmRelease
	if !isFluxKind && evt.Event != eventDeploySuccess {
		return false, nil
	}
	if tracked {
		return evt.SourceKind == kindKustomization || evt.Event == eventDeploySuccess || (isFluxKind && evt.Event == eventDeployStarted), nil
	}
	if !isFluxKind || (evt.Event != eventDeployStarted && evt.Event != eventDeploySuccess) || s.dispatcher == nil {
		return false, nil
	}
	refs, discErr := s.dispatcher.DiscoverRepoKustomizations(ctx, evt.Repo)
	if discErr != nil {
		return true, discErr
	}
	return len(refs) > 0, nil
}

// ownedWaitingForImageBuild mirrors isWaitingForImageBuild from Restate
// state: an open/merged PR whose image build has not started, that records
// no reconcile requests, and whose final build metadata is missing or still
// selects images. A failed lifecycle read waits.
func (s *RouterServer) ownedWaitingForImageBuild(ctx context.Context, repo string, prNumber int, view prwire.DeployView) bool {
	repo = strings.TrimSpace(repo)
	if repo == "" || prNumber <= 0 || buildmeta.MetadataWorkflowPath(repo) == "" {
		return false
	}
	c, err := s.restateClient()
	if err != nil {
		return true
	}
	lc, err := c.GetBuildLifecycle(ctx, repo, prNumber)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [deploy] error loading restate build lifecycle", "repo", repo, "pr", prNumber, "error", err)
		return true
	}
	switch strings.ToLower(strings.TrimSpace(lc.Status)) {
	case prOpen, prCIFailed, prConflict, prMerged, prSynced:
	default:
		return false
	}
	if lc.Started || len(view.KustomizationRequests) > 0 {
		return false
	}
	m := lc.BuildMetadata
	return !lc.HasBuildMetadata || m == nil || m.Kind != prwire.BuildMetadataFinal || len(m.Selected) > 0
}

// publishOwnedDeployEvent sends the lifecycle outcome of an alert the
// watcher does not own to PRObject, which publishes it.
func (s *RouterServer) publishOwnedDeployEvent(ctx context.Context, evt DeployEvent, branch string, expected []string, syncHasExpected, waiting bool) error {
	if evt.Event == eventDeployStarted && evt.JobName != "" {
		// The transition itself is state: it applies whatever the target,
		// and PRObject publishes deploy_started only when it transitioned.
		ev := prwire.PREvent{Repo: evt.Repo, PRNumber: evt.PRNumber, MergeSHA: evt.CommitSHA, JobName: evt.JobName, ExpectedJobs: expected}
		if s.isValidAlertTarget(evt.TargetID) && s.dispatcher != nil {
			ev.Facts = facts(ownedDeployFact(evt, eventDeployStarted, statusDeploying, branch))
		}
		return s.sendOwnedPR(ctx, prwire.PRHandlerDeployObserved, ev)
	}
	if !s.isValidAlertTarget(evt.TargetID) || s.dispatcher == nil {
		return nil
	}
	observed := func(eventName, status string) error {
		return s.sendOwnedPR(ctx, prwire.PRHandlerDeployObserved, prwire.PREvent{
			Repo: evt.Repo, PRNumber: evt.PRNumber, MergeSHA: evt.CommitSHA, Facts: facts(ownedDeployFact(evt, eventName, status, branch)),
		})
	}
	success := func() error {
		fact := ownedDeployFact(evt, eventDeploySuccess, statusDeployed, branch)
		return s.sendOwnedPR(ctx, prwire.PRHandlerDeploySuccess, prwire.PREvent{
			Repo: evt.Repo, PRNumber: evt.PRNumber, MergeSHA: evt.CommitSHA, JobName: evt.JobName, Facts: facts(fact),
		})
	}
	switch evt.Event {
	case eventDeploySuccess:
		if waiting {
			return observed(eventSyncSuccess, "synced")
		}
		return success()
	case eventDeployRollback, eventDeployFailed:
		return s.sendOwnedDeployFailed(ctx, evt, branch)
	case eventSyncSuccess:
		if syncHasExpected {
			return observed(eventSyncSuccess, "synced")
		}
		return success()
	case eventSyncFailed:
		return observed(eventSyncFailed, eventDeployFailed)
	}
	return nil
}

// resolveOwnedMainPR fills the PR a main-branch workflow run built from its
// commit (the PR's merge commit) without touching res.Branch, which stays
// the run's branch; the PR branch is returned for fact rendering.
func (s *RouterServer) resolveOwnedMainPR(ctx context.Context, res *ResolvedGitHubEvent) string {
	if res.PRNumber > 0 {
		return ""
	}
	prNum, branch := s.resolveOwnedSHAPR(ctx, res.Repo, res.HeadSHA)
	if prNum > 0 {
		res.PRNumber = prNum
		res.ResolutionSource = "restate_or_commit_sha_lookup"
	}
	return branch
}

// processOwnedMainWorkflowRun handles one main-branch workflow run: the
// attempt-1 failed-job rerun, a later failure's deploy_failed, or a
// successful build's image promotion. Only the default owner acts on
// main-branch runs; synapse reads no PR owner.
func (s *RouterServer) processOwnedMainWorkflowRun(ctx context.Context, p GitHubWorkflowRunPayload, res *ResolvedGitHubEvent, prBranch string) error {
	if !s.cfgSrc.Current().DefaultOwner || p.Action != ghStatusCompleted {
		return nil
	}
	switch res.Conclusion {
	case conclusionFailure, conclusionTimedOut:
		if prwire.IsGateAggregatorCheck(res.CheckName) || res.HeadSHA == "" || prwire.IsZeroSHA(res.HeadSHA) {
			return nil
		}
		if p.WorkflowRun.RunAttempt == 1 && p.WorkflowRun.ID > 0 {
			return s.sendOwnedMainRerun(ctx, p, res)
		}
		details := fmt.Sprintf("Workflow %q concluded with %s", p.WorkflowRun.Name, p.WorkflowRun.Conclusion)
		fact := ownedImageDeployFailedFact(res.Repo, res.PRNumber, prBranch, res.HeadSHA, details)
		return s.sendOwnedPROrSHA(ctx, prwire.RepoHandlerWorkflowRunBySHA, prwire.PRHandlerDeployFailed, res.HeadSHA, prwire.PREvent{
			Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, MergeSHA: res.HeadSHA, Facts: facts(fact),
		})
	case conclusionSuccess:
		if p.WorkflowRun.Status != ghStatusCompleted || s.dispatcher == nil {
			return nil
		}
		return s.dispatchOwnedWorkflowRunImages(ctx, workflowRunEvent{ID: p.WorkflowRun.ID, Attempt: p.WorkflowRun.RunAttempt, HeadSHA: res.HeadSHA}, res, prBranch)
	}
	return nil
}

// sendOwnedMainRerun reruns the failed jobs of an attempt-1 main-branch run
// in RepoObject and records the rerun request on the PR it built.
func (s *RouterServer) sendOwnedMainRerun(ctx context.Context, p GitHubWorkflowRunPayload, res *ResolvedGitHubEvent) error {
	if err := s.sendOwnedPR(ctx, prwire.PRHandlerCheckRerunRequested, prwire.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, MergeSHA: res.HeadSHA, CheckName: res.CheckName,
	}); err != nil {
		return err
	}
	return s.sendOwnedRepo(ctx, prwire.RepoHandlerWorkflowRerun, prwire.RepoEvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, CheckName: res.CheckName, RunID: p.WorkflowRun.ID,
	})
}

// ownedImageDeployFailedFact renders the deploy_failed fact for a failed
// image build or promotion.
func ownedImageDeployFailedFact(repo string, prNum int, branch, sha, details string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{
		Event: eventDeployFailed, Repo: repo, PRNumber: prNum, Branch: branch, HeadSHA: sha, MergeSHA: sha,
		Status: eventDeployFailed, Details: details, Quiet: isDependabotBranch(branch), OccurredAt: time.Now().UTC(),
	}
}

// dispatchOwnedWorkflowRunImages resolves the images the run built (a
// stateless GitHub read through the Restate-backed buildmeta store) and
// promotes them through promoteOwnedImages.
func (s *RouterServer) dispatchOwnedWorkflowRunImages(ctx context.Context, run workflowRunEvent, res *ResolvedGitHubEvent, prBranch string) error {
	repo, prNum, headSHA := res.Repo, res.PRNumber, run.HeadSHA
	images, err := s.ownedBuildConsumer(nil).ResolveWorkflowRunImages(ctx, repo, run, prNum)
	if errors.Is(err, errStaleWorkflowEvent) || errors.Is(err, errNotImageWorkflow) {
		slog.InfoContext(ctx, "[synapse] [dispatcher] skipping workflow run image promotion", "run_id", run.ID, "attempt", run.Attempt, "repo", repo, "reason", err)
		return nil
	}
	if err != nil {
		slog.ErrorContext(ctx, "[synapse] [dispatcher] error resolving images for workflow run", "run_id", run.ID, "attempt", run.Attempt, "repo", repo, "error", err)
		if _, rateLimited := githubapi.RateLimitDelay(err); !rateLimited && prNum > 0 {
			fact := ownedImageDeployFailedFact(repo, prNum, prBranch, headSHA, err.Error())
			if rErr := s.sendOwnedPR(ctx, prwire.PRHandlerDeployAttemptFailed, prwire.PREvent{
				Repo: repo, PRNumber: prNum, HeadSHA: headSHA, MergeSHA: headSHA, Banner: fact.Details, Facts: facts(fact),
			}); rErr != nil {
				return rErr
			}
		}
		return fmt.Errorf("resolve workflow run images: %w", err)
	}
	return s.promoteOwnedImages(ctx, repo, prNum, headSHA, prBranch, images)
}

// promoteOwnedImages hands the images one main-branch build produced to
// RepoObject.HandleImagePromotion with the rollout they start: the deploying
// workloads, a fresh reconcile request per owning Kustomization, the
// deploy_started fact and the deploy_failed template RepoObject fills on a
// failed promotion. Images no workload deploys complete through
// ownedCompleteNoDeploy.
func (s *RouterServer) promoteOwnedImages(ctx context.Context, repo string, prNum int, headSHA, prBranch string, images []string) error {
	imageNames, workloads := resolveImagesWorkloadsAndNames(repo, images)
	if len(workloads) == 0 {
		return s.ownedCompleteNoDeploy(ctx, repo, headSHA, prNum, prBranch)
	}
	refs, err := s.dispatcher.DiscoverImageKustomizations(ctx, imageNames)
	if err != nil {
		slog.ErrorContext(ctx, "[synapse] [dispatcher] error discovering kustomizations for images", "repo", repo, "images", imageNames, "error", err)
		return fmt.Errorf("discover image kustomizations: %w", err)
	}
	ev := prwire.RepoEvent{
		Repo: repo, HeadSHA: headSHA, PRNumber: prNum, Branch: prBranch, Images: images, Jobs: workloads,
		DeployFailedFacts: facts(ownedImageDeployFailedFact(repo, prNum, prBranch, headSHA, "")),
	}
	if len(refs) > 0 {
		ev.KustomizationRequests = newKustomizationRequests(refs, time.Now().UTC())
		ev.Facts = facts(pubsub.PRLifecycleEvent{
			Event: eventDeployStarted, Repo: repo, PRNumber: prNum, HeadSHA: headSHA, Status: statusDeploying,
			Quiet: isDependabotBranch(prBranch), OccurredAt: time.Now().UTC(),
		})
	}
	if err := s.sendOwnedRepo(ctx, prwire.RepoHandlerImagePromotion, ev); err != nil {
		return err
	}
	if len(refs) == 0 {
		return s.ownedCompleteNoDeploy(ctx, repo, headSHA, prNum, prBranch)
	}
	return nil
}

// ownedCompleteNoDeploy completes a build with no deploy jobs: a PR that expects no deploy jobs defers to the watcher when its repo
// sources a watched Kustomization and otherwise completes with the
// git-merge deploy_success, which PRObject dedupes and publishes.
func (s *RouterServer) ownedCompleteNoDeploy(ctx context.Context, repo, headSHA string, prNum int, branch string) error {
	if prNum <= 0 {
		return nil
	}
	view, err := s.ownedDeployView(ctx, repo, prNum)
	if err != nil {
		return restateFailure(ctx, "GetDeployView", err)
	}
	if view.HasExpectedDeployJobs {
		return nil
	}
	branch = strutil.FirstNonEmpty(view.Branch, branch)
	view.Branch = branch
	if deferred, err := s.ownedDeferPRToWatcher(ctx, repo, prNum, view); deferred || err != nil {
		return err
	}
	fact := pubsub.PRLifecycleEvent{
		Event: eventDeploySuccess, Repo: repo, PRNumber: prNum, HeadSHA: headSHA, Status: statusDeployed,
		JobName: jobGitMerge, Quiet: isDependabotBranch(branch), OccurredAt: time.Now().UTC(),
	}
	return s.sendOwnedPR(ctx, prwire.PRHandlerDeploySuccess, prwire.PREvent{
		Repo: repo, PRNumber: prNum, HeadSHA: headSHA, JobName: jobGitMerge, Facts: facts(fact),
	})
}
