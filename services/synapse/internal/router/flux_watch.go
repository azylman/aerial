package router

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/flux"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// kustomizationObservation is the live state of one Flux Kustomization as seen by a LIST or WATCH.
type kustomizationObservation = model.KustomizationObservation

// kustomizationRequestHandled reports whether Flux has handled a reconcile request recorded as
// requestedAt: lastHandledReconcileAt at or after it means handled, because a later request (from
// another PR) supersedes an earlier one. Unparseable values fall back to string equality.
func kustomizationRequestHandled(requestedAt, lastHandledReconcileAt string) bool {
	return flux.KustomizationRequestHandled(requestedAt, lastHandledReconcileAt)
}

const (
	kustomizationWait         = flux.KustomizationWait
	kustomizationMarkDeployed = flux.KustomizationMarkDeployed
	kustomizationFail         = flux.KustomizationFail
	kustomizationRepatch      = flux.KustomizationRepatch
)

// kustomizationAction is what a PR rollout must do about one observed Kustomization.
type kustomizationAction = flux.KustomizationAction

// kustomizationRequestAction decides how an observation affects a recorded reconcile request.
func kustomizationRequestAction(recorded kustomizationRequest, obs kustomizationObservation) kustomizationAction {
	return flux.KustomizationRequestAction(recorded, obs)
}

// startKustomizationWatcher runs the Kustomization rollout watchers when PR state is durable and the kube
// API is configured. The returned stop function cancels them and waits for them to exit; it is idempotent.
func (s *RouterServer) startKustomizationWatcher(ctx context.Context, kube *DefaultOutboundDispatcher) func() {
	if s == nil || s.registry == nil {
		// Without a registry there are no tracked rollouts to reconcile:
		// trackedRollouts is empty so every apply iterates nothing and
		// every recheck is a no-op. Skip the watchers instead of burning
		// kube list calls that can never match a rollout.
		return func() {}
	}
	return flux.StartWatcher(ctx, kube, s)
}

// trackedRollout is a deploying PR whose completion is gated on recorded Kustomization requests.
type trackedRollout = flux.TrackedRollout

// recordedKustomizationRequests decodes the PR's kustomization_requests metadata, if any.
func recordedKustomizationRequests(meta map[string]any) map[string]kustomizationRequest {
	return flux.RecordedKustomizationRequests(meta)
}

func isDeploySuccessPending(meta map[string]any) bool {
	return flux.IsDeploySuccessPending(meta)
}

// hasPendingRollouts reports whether any deploying PR still waits on a recorded Kustomization request.
func (s *RouterServer) hasPendingRollouts(ctx context.Context) (bool, error) {
	rollouts, err := s.trackedRollouts(ctx)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(rollouts, trackedRollout.HasPending), nil
}

func (s *RouterServer) trackedRollouts(ctx context.Context) ([]trackedRollout, error) {
	if s.registry == nil {
		return nil, nil
	}
	prs, err := s.registry.ListDeployingPRs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list deploying prs: %w", err)
	}
	var rollouts []trackedRollout
	for _, p := range prs {
		if requests := recordedKustomizationRequests(p.Metadata); p.PRNumber > 0 && len(requests) > 0 {
			rollouts = append(rollouts, trackedRollout{PR: p, Requests: requests, Deployed: extractStringSliceFromMetadata(p.Metadata, deployedKustomizationsKey)})
		}
	}
	return rollouts, nil
}

// ApplyKustomizationList satisfies flux.RolloutSink.
// Each sink call snapshots the effect owner once for everything it applies.
func (s *RouterServer) ApplyKustomizationList(ctx context.Context, namespace string, observations []model.KustomizationObservation) error {
	return s.applyKustomizationList(stampEffectsOwner(ctx), namespace, observations)
}

// ApplyKustomizationStatus satisfies flux.RolloutSink.
func (s *RouterServer) ApplyKustomizationStatus(ctx context.Context, obs model.KustomizationObservation) error {
	return s.applyKustomizationStatus(stampEffectsOwner(ctx), obs)
}

// ApplyKustomizationDeleted satisfies flux.RolloutSink.
func (s *RouterServer) ApplyKustomizationDeleted(ctx context.Context, ref model.FluxKustomizationRef) error {
	return s.applyKustomizationDeleted(stampEffectsOwner(ctx), ref)
}

// HasPendingRollouts satisfies flux.RolloutSink.
func (s *RouterServer) HasPendingRollouts(ctx context.Context) (bool, error) {
	return s.hasPendingRollouts(ctx)
}

// applyKustomizationList reconciles a full LIST: every observation is applied, and any pending request
// for a Kustomization of namespace that no longer exists fails its PR.
func (s *RouterServer) applyKustomizationList(ctx context.Context, namespace string, observations []kustomizationObservation) error {
	rollouts, err := s.trackedRollouts(ctx)
	if err != nil {
		return err
	}
	present := make(map[fluxKustomizationRef]bool, len(observations))
	var errs []error
	for _, obs := range observations {
		present[obs.Ref] = true
		errs = append(errs, s.applyObservation(ctx, rollouts, obs))
	}
	for _, t := range rollouts {
		for _, name := range slices.Sorted(maps.Keys(t.Requests)) {
			ref := fluxKustomizationRef{Namespace: cmp.Or(t.Requests[name].Namespace, fluxSystemNamespace), Name: name}
			if _, pending := t.PendingRequest(ref); pending && ref.Namespace == namespace && !present[ref] {
				errs = append(errs, s.failKustomizationRollout(ctx, t.PR, name, "Kustomization deleted"))
			}
		}
	}
	return errors.Join(errs...)
}

// applyKustomizationStatus applies one ADDED/MODIFIED observation to every PR waiting on it.
func (s *RouterServer) applyKustomizationStatus(ctx context.Context, obs kustomizationObservation) error {
	rollouts, err := s.trackedRollouts(ctx)
	if err != nil {
		return err
	}
	return s.applyObservation(ctx, rollouts, obs)
}

// applyKustomizationDeleted fails every PR still waiting on a deleted Kustomization.
func (s *RouterServer) applyKustomizationDeleted(ctx context.Context, ref fluxKustomizationRef) error {
	rollouts, err := s.trackedRollouts(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, t := range rollouts {
		if _, pending := t.PendingRequest(ref); pending {
			errs = append(errs, s.failKustomizationRollout(ctx, t.PR, ref.Name, "Kustomization deleted"))
		}
	}
	return errors.Join(errs...)
}

// applyObservation acts on each pending request for obs. A request Flux has applied completes only once
// everything the PR changed in the Kustomization is live (see gateKustomizationRequest); its workloads,
// ConfigMaps and source repo are loaded at most once per call. Lost requests are repaired with one PATCH
// carrying the latest recorded value, which covers every earlier request. A returned error means some PR
// is still deploying with work left undone; the watcher then backs off and re-lists, because a
// Kustomization already in its final state emits no further event to retry on.
func (s *RouterServer) applyObservation(ctx context.Context, rollouts []trackedRollout, obs kustomizationObservation) error {
	ctx = restateclient.WithEventIdentity(ctx, kustomizationObservationIdentity(obs))
	repatchAt := ""
	var errs []error
	var state kustomizationRolloutState
	var loadErr error
	loaded := false
	for _, t := range rollouts {
		req, pending := t.PendingRequest(obs.Ref)
		if !pending {
			continue
		}
		switch action := kustomizationRequestAction(req, obs); action.Kind {
		case kustomizationMarkDeployed:
			if !loaded {
				loaded = true
				if state, loadErr = s.loadKustomizationRollout(ctx, obs); loadErr != nil {
					errs = append(errs, loadErr)
				}
			}
			if loadErr == nil {
				errs = append(errs, s.gateKustomizationRequest(ctx, t.PR, obs, state))
			}
		case kustomizationFail:
			errs = append(errs, s.failKustomizationRollout(ctx, t.PR, obs.Ref.Name, action.Reason))
		case kustomizationRepatch:
			if repatchAt == "" || !kustomizationRequestHandled(req.RequestedAt, repatchAt) {
				repatchAt = req.RequestedAt
			}
		case kustomizationWait:
		}
	}
	if repatchAt != "" && s.dispatcher != nil {
		slog.InfoContext(ctx, "[synapse] [flux-watch] re-requesting lost reconcile", "namespace", obs.Ref.Namespace, "name", obs.Ref.Name, "requested_at", repatchAt)
		if err := s.dispatcher.ReconcileKustomization(ctx, obs.Ref, repatchAt); err != nil {
			errs = append(errs, fmt.Errorf("re-request reconcile of %s/%s: %w", obs.Ref.Namespace, obs.Ref.Name, err))
		}
	}
	return errors.Join(errs...)
}

// kustomizationObservationIdentity scopes Restate idempotency keys to one
// observed Kustomization state, so a re-list of an unchanged Kustomization
// re-sends identical events that dedup at the Restate ingress.
func kustomizationObservationIdentity(obs kustomizationObservation) string {
	return fmt.Sprintf("flux:%s/%s:%s:%s", obs.Ref.Namespace, obs.Ref.Name, obs.LastAppliedRevision, obs.LastHandledReconcileAt)
}

// kustomizationRolloutState is the live cluster state of one applied Kustomization that gates PR deploys.
type kustomizationRolloutState struct {
	Workloads  []workloadRollout
	ConfigMaps []configMapChange
	SourceRepo string
}

// loadKustomizationRollout lists obs's workloads and ConfigMaps and resolves the owner/repo of its
// GitRepository source ("" for any other source kind).
func (s *RouterServer) loadKustomizationRollout(ctx context.Context, obs kustomizationObservation) (kustomizationRolloutState, error) {
	workloads, err := s.dispatcher.ListKustomizationWorkloads(ctx, obs.Ref)
	if err != nil {
		return kustomizationRolloutState{}, fmt.Errorf("list workloads of %s/%s: %w", obs.Ref.Namespace, obs.Ref.Name, err)
	}
	configMaps, err := s.dispatcher.ListKustomizationConfigMaps(ctx, obs.Ref)
	if err != nil {
		return kustomizationRolloutState{}, fmt.Errorf("list configmaps of %s/%s: %w", obs.Ref.Namespace, obs.Ref.Name, err)
	}
	sourceRepo, err := s.dispatcher.ResolveFluxSourceRepo(ctx, FluxInvolvedObject{Kind: obs.SourceRef.Kind, Name: obs.SourceRef.Name, Namespace: obs.SourceRef.Namespace})
	if err != nil {
		return kustomizationRolloutState{}, fmt.Errorf("resolve source of %s/%s: %w", obs.Ref.Namespace, obs.Ref.Name, err)
	}
	return kustomizationRolloutState{Workloads: workloads, ConfigMaps: configMaps, SourceRepo: sourceRepo}, nil
}

// gateKustomizationRequest completes an applied Kustomization request once the PR's change is live:
// when the Kustomization's source is the PR's repo, Flux must have applied the merge commit or a
// descendant of it, and every workload the change reached must have rolled out (prRolloutStatus, with
// the merge commit time as the change cutoff). It fails the PR when a rollout failed and otherwise
// leaves it pending for the watchers and the periodic recheck to re-evaluate.
func (s *RouterServer) gateKustomizationRequest(ctx context.Context, pr DeployingPR, obs kustomizationObservation, state kustomizationRolloutState) error {
	name := obs.Ref.Name
	commitSHA := firstNonEmpty(pr.MergeSHA, pr.HeadSHA)
	if commitSHA == "" {
		slog.InfoContext(ctx, "[synapse] [flux-watch] waiting on kustomization: no merge commit recorded", "kustomization", name, "repo", pr.Repo, "pr", pr.PRNumber)
		return nil
	}
	if state.SourceRepo != "" && strings.EqualFold(normalizeRepo(state.SourceRepo), normalizeRepo(pr.Repo)) {
		applied := cleanRevisionSHA(obs.LastAppliedRevision)
		live, err := s.revisionIncludes(ctx, pr.Repo, applied, commitSHA)
		if err != nil {
			return fmt.Errorf("check applied revision of %s for %s#%d: %w", name, pr.Repo, pr.PRNumber, err)
		}
		if !live {
			slog.InfoContext(ctx, "[synapse] [flux-watch] waiting for kustomization to apply commit", "kustomization", name, "commit", commitSHA, "repo", pr.Repo, "pr", pr.PRNumber, "applied", applied)
			return nil
		}
	}
	since, err := s.commitTime(ctx, pr.Repo, commitSHA)
	if err != nil {
		return fmt.Errorf("resolve merge time of %s#%d: %w", pr.Repo, pr.PRNumber, err)
	}
	in := prRolloutInput{Workloads: state.Workloads, ConfigMaps: state.ConfigMaps, CommitSHA: commitSHA, Since: since, Now: time.Now()}
	done, failed, reason := prRolloutStatus(in)
	if failed {
		return s.failKustomizationRollout(ctx, pr, name, reason)
	}
	readyJobs := flux.ReadyForCommitServices(in)
	if done {
		readyJobs = append(readyJobs, name)
	}
	var jobsUpdated bool
	if s.registry != nil && len(readyJobs) > 0 {
		var err error
		jobsUpdated, err = s.registry.MarkJobsDeployed(ctx, pr.Repo, pr.PRNumber, readyJobs)
		if err != nil {
			return fmt.Errorf("mark jobs deployed for %s#%d: %w", pr.Repo, pr.PRNumber, err)
		}
	}
	if done {
		return s.completeKustomizationRequest(ctx, pr, name)
	}
	if jobsUpdated {
		if err := s.routeRolloutProgress(ctx, rolloutEvent(pr, name, readyJobs, nil)); err != nil {
			slog.WarnContext(ctx, "[synapse] [flux-watch] failed to route rollout progress", "kustomization", name, "repo", pr.Repo, "pr", pr.PRNumber, "error", err)
		}
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        "rollout_updated",
			Repo:         pr.Repo,
			PRNumber:     pr.PRNumber,
			HeadSHA:      pr.HeadSHA,
			MergeSHA:     pr.MergeSHA,
			TargetID:     pr.TargetID,
			JobName:      name,
			Status:       statusDeploying,
			RolloutState: s.loadPRRolloutState(ctx, pr.Repo, pr.PRNumber),
			Quiet:        s.isDependabotPRNumber(ctx, pr.Repo, pr.PRNumber),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			slog.WarnContext(ctx, "[synapse] [flux-watch] failed to publish rollout_updated lifecycle event", "kustomization", name, "repo", pr.Repo, "pr", pr.PRNumber, "error", err)
		}
	}
	slog.InfoContext(ctx, "[synapse] [flux-watch] waiting for rollout", "kustomization", name, "repo", pr.Repo, "pr", pr.PRNumber, "reason", reason)
	return nil
}

// commitTime returns the committer date of sha in repo, which for a merge commit is when it merged.
func (s *RouterServer) commitTime(ctx context.Context, repo, sha string) (time.Time, error) {
	repo = normalizeRepo(repo)
	key := repo + "@" + sha
	if at, ok := s.gitHistory.CommitTime(key); ok {
		return at, nil
	}
	var commit struct {
		Committer struct {
			Date time.Time `json:"date"`
		} `json:"committer"`
	}
	if err := s.getGitHubJSON(ctx, "/repos/"+repo+"/git/commits/"+url.PathEscape(sha), 1<<20, &commit); err != nil {
		return time.Time{}, fmt.Errorf("get commit %s: %w", sha, err)
	}
	if commit.Committer.Date.IsZero() {
		return time.Time{}, fmt.Errorf("commit %s has no committer date", sha)
	}
	s.gitHistory.StoreCommitTime(key, commit.Committer.Date)
	return commit.Committer.Date, nil
}

// revisionIncludes reports whether revision is commit or a descendant of it in repo; an empty revision
// includes nothing.
func (s *RouterServer) revisionIncludes(ctx context.Context, repo, revision, commit string) (bool, error) {
	if revision == "" {
		return false, nil
	}
	if revision == commit {
		return true, nil
	}
	repo = normalizeRepo(repo)
	key := repo + "@" + commit + "..." + revision
	if verdict, ok := s.gitHistory.IncludesVerdict(key); ok {
		return verdict, nil
	}
	var comparison struct {
		Status string `json:"status"`
	}
	path := "/repos/" + repo + "/compare/" + url.PathEscape(commit) + "..." + url.PathEscape(revision) + "?per_page=1"
	if err := s.getGitHubJSON(ctx, path, 16<<20, &comparison); err != nil {
		return false, fmt.Errorf("compare %s...%s: %w", commit, revision, err)
	}
	verdict := comparison.Status == "identical" || comparison.Status == "ahead"
	s.gitHistory.StoreIncludesVerdict(key, verdict)
	return verdict, nil
}

// completeKustomizationRequest records one handled Kustomization; only the call that flips the PR to
// deployed dispatches the success wake. Only a failed registry update is returned: once the PR is
// deployed it leaves the deploying set, so a re-list could not retry a failed wake anyway.
func (s *RouterServer) completeKustomizationRequest(ctx context.Context, pr DeployingPR, name string) error {
	if s.restateAuthoritative() {
		return s.completeKustomizationRequestAuthoritative(ctx, pr, name)
	}
	var (
		transitioned bool
		err          error
	)
	if isDeploySuccessPending(pr.Metadata) {
		transitioned = true
	} else {
		transitioned, err = s.registry.MarkKustomizationsDeployed(ctx, pr.Repo, pr.PRNumber, []string{name})
		if err != nil {
			return fmt.Errorf("mark %s deployed for %s#%d: %w", name, pr.Repo, pr.PRNumber, err)
		}
	}
	if !transitioned {
		// Journal before the legacy publish: a journal failure leaves no
		// rollout_updated behind, so the watcher tick retries cleanly.
		if err := s.routeRolloutProgress(ctx, rolloutEvent(pr, name, nil, []string{name})); err != nil {
			return err
		}
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        "rollout_updated",
			Repo:         pr.Repo,
			PRNumber:     pr.PRNumber,
			HeadSHA:      pr.HeadSHA,
			MergeSHA:     pr.MergeSHA,
			JobName:      name,
			Status:       statusDeploying,
			RolloutState: s.loadPRRolloutState(ctx, pr.Repo, pr.PRNumber),
			Quiet:        s.isDependabotPRNumber(ctx, pr.Repo, pr.PRNumber),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			return err
		}
		return nil
	}
	commitSHA := firstNonEmpty(pr.MergeSHA, pr.HeadSHA)
	targetID := pr.TargetID
	if targetID == "" {
		if resolved, rErr := s.ResolveThreadForPR(ctx, pr.Repo, pr.PRNumber); rErr == nil {
			targetID = resolved
		}
	}
	// The transitioned return value above plus the deploy_success_pending
	// flag are the exactly-once guard for this path, so the publish below
	// must not be vetoed by re-reading the deployed projection: the flip
	// just recorded above always reads back as deployed. Publish errors
	// return before pending is cleared so the watcher retries.
	fact := s.watchDeploySuccessFact(ctx, pr, name, targetID)
	if err := s.routeDeployDispatched(ctx, pr.Repo, name, pr.HeadSHA, commitSHA, pr.PRNumber, fact); err != nil {
		return err
	}
	if err := s.routeRolloutProgress(ctx, rolloutEvent(pr, name, nil, []string{name})); err != nil {
		return err
	}
	if err := s.publishWatchDeploySuccess(ctx, fact); err != nil {
		return err
	}
	if err := s.dispatchDeploymentSuccessPrompt(ctx, targetID, name, pr.Repo, commitSHA, pr.PRNumber, ""); err != nil {
		slog.ErrorContext(ctx, "[synapse] [flux-watch] error dispatching deploy_success", "repo", pr.Repo, "pr", pr.PRNumber, "error", err)
		return err
	}
	if s.registry != nil {
		if err := s.registry.UpdatePRStatus(ctx, pr.Repo, pr.PRNumber, statusDeployed, map[string]any{"deploy_success_pending": false}); err != nil {
			slog.WarnContext(ctx, "[synapse] [flux-watch] error clearing deploy_success_pending", "repo", pr.Repo, "pr", pr.PRNumber, "error", err)
			return fmt.Errorf("clear deploy_success_pending for %s#%d: %w", pr.Repo, pr.PRNumber, err)
		}
	}
	return nil
}

// watchDeploySuccessFact renders the deploy_success fact for one
// completed Kustomization request.
func (s *RouterServer) watchDeploySuccessFact(ctx context.Context, pr DeployingPR, name, targetID string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{
		Event:        eventDeploySuccess,
		Repo:         pr.Repo,
		PRNumber:     pr.PRNumber,
		HeadSHA:      pr.HeadSHA,
		MergeSHA:     pr.MergeSHA,
		TargetID:     targetID,
		Status:       statusDeployed,
		JobName:      name,
		RolloutState: s.loadPRRolloutState(ctx, pr.Repo, pr.PRNumber),
		Quiet:        s.isDependabotPRNumber(ctx, pr.Repo, pr.PRNumber),
		OccurredAt:   time.Now().UTC(),
	}
}

// publishWatchDeploySuccess publishes the watcher deploy_success fact on the
// legacy pipeline; a restate-owned event skips it because the routed
// HandleDeploySuccess carries the fact for PRObject to publish.
func (s *RouterServer) publishWatchDeploySuccess(ctx context.Context, fact pubsub.PRLifecycleEvent) error {
	if restateOwnsEffects(ctx) {
		return nil
	}
	return s.publishLegacyFact(ctx, fact)
}

// completeKustomizationRequestAuthoritative publishes one deploy_success
// decided by the Restate PRObject. The Postgres projection is synced after
// the confirmed publish but never decides the publish: no transition return
// value gates it and IsPRDeployed is never consulted here. A deduped or
// stale verdict skips the publish and only converges the projection; any
// failure before the ack surfaces so the watcher retry re-resolves and
// republishes instead of losing the signal.
func (s *RouterServer) completeKustomizationRequestAuthoritative(ctx context.Context, pr DeployingPR, name string) error {
	commitSHA := firstNonEmpty(pr.MergeSHA, pr.HeadSHA)
	targetID := pr.TargetID
	if targetID == "" {
		if resolved, rErr := s.ResolveThreadForPR(ctx, pr.Repo, pr.PRNumber); rErr == nil {
			targetID = resolved
		}
	}
	fact := s.watchDeploySuccessFact(ctx, pr, name, targetID)
	if restateOwnsEffects(ctx) {
		// PRObject decides, commits, and publishes the carried fact.
		if err := s.routeRolloutProgress(ctx, rolloutEvent(pr, name, nil, []string{name})); err != nil {
			return err
		}
		if err := s.routeDeployDispatched(ctx, pr.Repo, name, pr.HeadSHA, commitSHA, pr.PRNumber, fact); err != nil {
			return err
		}
		if err := s.dispatchDeploymentSuccessPrompt(ctx, targetID, name, pr.Repo, commitSHA, pr.PRNumber, ""); err != nil {
			return err
		}
		return s.syncDeployedProjection(ctx, pr, name)
	}
	decision, err := s.resolveDeployDispatch(ctx, pr.Repo, name, pr.HeadSHA, commitSHA, pr.PRNumber)
	if err != nil {
		return err
	}
	if decision.ShouldPublish {
		if err := s.routeRolloutProgress(ctx, rolloutEvent(pr, name, nil, []string{name})); err != nil {
			return err
		}
		if err := s.publishLegacyFact(ctx, fact); err != nil {
			return err
		}
		if err := s.dispatchDeploymentSuccessPrompt(ctx, targetID, name, pr.Repo, commitSHA, pr.PRNumber, ""); err != nil {
			slog.ErrorContext(ctx, "[synapse] [flux-watch] error dispatching deploy_success", "repo", pr.Repo, "pr", pr.PRNumber, "error", err)
			return err
		}
		if err := s.recordDeployPublished(ctx, pr.Repo, name, pr.HeadSHA, commitSHA, pr.PRNumber); err != nil {
			return err
		}
	}
	return s.syncDeployedProjection(ctx, pr, name)
}

// syncDeployedProjection converges the read-only pr_registry projection after
// an authoritative deploy decision: the kustomizations read deployed and any
// pending flag clears. Deduped redeliveries converge a projection that
// lagged an earlier attempt.
func (s *RouterServer) syncDeployedProjection(ctx context.Context, pr DeployingPR, name string) error {
	if s.registry == nil {
		return nil
	}
	if _, err := s.registry.MarkKustomizationsDeployed(ctx, pr.Repo, pr.PRNumber, []string{name}); err != nil {
		return fmt.Errorf("mark %s deployed for %s#%d: %w", name, pr.Repo, pr.PRNumber, err)
	}
	if err := s.registry.UpdatePRStatus(ctx, pr.Repo, pr.PRNumber, statusDeployed, map[string]any{"deploy_success_pending": false}); err != nil {
		slog.WarnContext(ctx, "[synapse] [flux-watch] error clearing deploy_success_pending", "repo", pr.Repo, "pr", pr.PRNumber, "error", err)
		return fmt.Errorf("clear deploy_success_pending for %s#%d: %w", pr.Repo, pr.PRNumber, err)
	}
	return nil
}

// failKustomizationRollout fails a PR rollout on a Kustomization outcome; the registry transition
// guarantees the failure is reported once. An error leaves (or reverts) the PR deploying, so the
// watcher's re-list retries it.
func (s *RouterServer) failKustomizationRollout(ctx context.Context, pr DeployingPR, name, reason string) error {
	evt := DeployEvent{
		Event:      eventDeployFailed,
		Status:     statusFailed,
		SourceKind: kindKustomization,
		JobName:    name,
		Repo:       pr.Repo,
		CommitSHA:  firstNonEmpty(pr.MergeSHA, pr.HeadSHA),
		PRNumber:   pr.PRNumber,
		TargetID:   pr.TargetID,
		Details:    reason,
		Timestamp:  time.Now().UTC(),
	}
	if err := s.handleDeployFailedOrRollback(ctx, evt, formatDirectMessagePrefix(pr.PRNumber, pr.Repo)); err != nil {
		return fmt.Errorf("report %s failure for %s#%d: %w", name, pr.Repo, pr.PRNumber, err)
	}
	return nil
}

// deferSyncToWatcher defers a sync_success with no image build pending to the Kustomization watcher: a
// git fetch is not an apply, so a Flux-sourced repo's PR completes only once its change rolled out.
func (s *RouterServer) deferSyncToWatcher(ctx context.Context, evt DeployEvent, syncHasExpected bool) (bool, error) {
	if evt.Event != eventSyncSuccess || syncHasExpected {
		return false, nil
	}
	return s.deferPRToWatcher(ctx, evt.Repo, evt.PRNumber)
}

// deferPRToWatcher hands completion of a PR without pending image builds to the Kustomization watcher
// when repo is the GitRepository source of any watched Kustomization: neither a git fetch nor CI
// finishing means Flux applied the change or that the workloads it changed rolled out. It marks the PR
// deploying, durably records a reconcile request per Kustomization before patching any of them (as
// dispatchWorkflowRunImages does), and reports whether the watcher now owns the PR. Re-recording is safe:
// requests are replaced wholesale and the watcher gates on the merge commit being applied. Repos with no
// Flux source are not deferred.
func (s *RouterServer) deferPRToWatcher(ctx context.Context, repo string, prNum int) (bool, error) {
	if s.registry == nil || s.dispatcher == nil || prNum <= 0 {
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
	// Journal before recording: a journal failure leaves nothing recorded,
	// so the retry re-journals and records. The event carries the deploying
	// jobs and the exact requests so PRObject records the same row.
	if jErr := s.routePREvent(ctx, "HandleKustomizationRequested", restateclient.PREvent{
		Repo: repo, PRNumber: prNum, Jobs: names, KustomizationRequests: requests,
	}); jErr != nil {
		return false, jErr
	}
	tID, err := s.registry.TransitionDeploying(ctx, repo, prNum, names)
	if err != nil {
		return false, fmt.Errorf("transition pr %s#%d deploying: %w", repo, prNum, err)
	}
	if tID != "" {
		deferHeadSHA, deferMergeSHA := s.shadowDeployStartedSHAs(ctx, repo, prNum)
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        eventDeployStarted,
			Repo:         repo,
			PRNumber:     prNum,
			HeadSHA:      deferHeadSHA,
			MergeSHA:     deferMergeSHA,
			TargetID:     tID,
			Status:       statusDeploying,
			RolloutState: s.loadPRRolloutState(ctx, repo, prNum),
			Quiet:        s.isDependabotPRNumber(ctx, repo, prNum),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			return false, err
		}
	}
	if err := s.registry.RecordKustomizationRequests(ctx, repo, prNum, requests); err != nil {
		return false, fmt.Errorf("record kustomization requests for %s#%d: %w", repo, prNum, err)
	}
	slog.InfoContext(ctx, "[synapse] [deploy] deferring completion to the kustomization watcher", "repo", repo, "pr", prNum, "kustomizations", strings.Join(names, ", "))
	if err := s.reconcileImageKustomizations(ctx, refs, requests); err != nil {
		return true, err
	}
	// The journaled request carried no head SHA, so PRObject keys the
	// reconcile on an empty SHA; the [synapse-effect] key matches it.
	recordLegacyEffect(ctx, restateclient.EffectFluxReconcile,
		restateclient.EffectMatchKey(repo, prNum, "", restateclient.EffectFluxReconcile, "", ""), normalizeRepo(repo), prNum)
	return true, nil
}

// newKustomizationRequests builds the reconcile request recorded for each ref, all requested at now.
func newKustomizationRequests(refs []fluxKustomizationRef, now time.Time) map[string]kustomizationRequest {
	return flux.NewKustomizationRequests(refs, now)
}

// reconcileImageKustomizations patches each ref with its recorded request; one failure never skips the rest.
func (s *RouterServer) reconcileImageKustomizations(ctx context.Context, refs []fluxKustomizationRef, requests map[string]kustomizationRequest) error {
	var errs []error
	for _, ref := range refs {
		if err := s.dispatcher.ReconcileKustomization(ctx, ref, requests[ref.Name].RequestedAt); err != nil {
			slog.ErrorContext(ctx, "[synapse] [dispatcher] error reconciling kustomization", "namespace", ref.Namespace, "name", ref.Name, "error", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
