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

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/synapse/internal/flux"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// kustomizationObservation is the live state of one Flux Kustomization as seen by a LIST or WATCH.
type kustomizationObservation = model.KustomizationObservation

const (
	kustomizationWait         = flux.KustomizationWait
	kustomizationMarkDeployed = flux.KustomizationMarkDeployed
	kustomizationFail         = flux.KustomizationFail
	kustomizationRepatch      = flux.KustomizationRepatch
)

// kustomizationAction is what a PR rollout must do about one observed Kustomization.
type kustomizationAction = flux.KustomizationAction

// kustomizationRequestAction decides how an observation affects a recorded reconcile request.
func kustomizationRequestAction(recorded prwire.KustomizationRequest, obs kustomizationObservation) kustomizationAction {
	return flux.KustomizationRequestAction(recorded, obs)
}

// startKustomizationWatcher runs the Kustomization rollout watchers once the kube API is configured;
// tracked rollouts come from the Restate rollout index. The returned stop function cancels them and
// waits for them to exit; it is idempotent.
func (s *RouterServer) startKustomizationWatcher(ctx context.Context, kube *DefaultOutboundDispatcher) func() {
	if s == nil {
		return func() {}
	}
	return flux.StartWatcher(ctx, kube, s)
}

// trackedRollout is a deploying PR whose completion is gated on recorded Kustomization requests.
type trackedRollout = flux.TrackedRollout

// hasPendingRollouts reports whether any deploying PR still waits on a recorded Kustomization request.
func (s *RouterServer) hasPendingRollouts(ctx context.Context) (bool, error) {
	rollouts, err := s.trackedRollouts(ctx)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(rollouts, trackedRollout.HasPending), nil
}

// trackedRollouts lists the deploying PRs gated on recorded requests from
// the Restate rollout index.
func (s *RouterServer) trackedRollouts(ctx context.Context) ([]trackedRollout, error) {
	return s.ownedTrackedRollouts(ctx)
}

// ApplyKustomizationList satisfies flux.RolloutSink.
func (s *RouterServer) ApplyKustomizationList(ctx context.Context, namespace string, observations []model.KustomizationObservation) error {
	return s.applyKustomizationList(ctx, namespace, observations)
}

// ApplyKustomizationStatus satisfies flux.RolloutSink.
func (s *RouterServer) ApplyKustomizationStatus(ctx context.Context, obs model.KustomizationObservation) error {
	return s.applyKustomizationStatus(ctx, obs)
}

// ApplyKustomizationDeleted satisfies flux.RolloutSink.
func (s *RouterServer) ApplyKustomizationDeleted(ctx context.Context, ref model.FluxKustomizationRef) error {
	return s.applyKustomizationDeleted(ctx, ref)
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
	var lost []DeployingPR
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
			// PRObject re-patches its own recorded request.
			lost = append(lost, t.PR)
		case kustomizationWait:
		}
	}
	if len(lost) > 0 {
		errs = append(errs, s.sendOwnedReconcileLost(ctx, lost, obs))
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
// descendant of it, and every workload the change reached must have rolled out (flux.PRRolloutStatus, with
// the merge commit time as the change cutoff). It fails the PR when a rollout failed and otherwise
// leaves it pending for the watchers and the periodic recheck to re-evaluate.
func (s *RouterServer) gateKustomizationRequest(ctx context.Context, pr DeployingPR, obs kustomizationObservation, state kustomizationRolloutState) error {
	name := obs.Ref.Name
	commitSHA := strutil.FirstNonEmpty(pr.MergeSHA, pr.HeadSHA)
	if commitSHA == "" {
		slog.InfoContext(ctx, "[synapse] [flux-watch] waiting on kustomization: no merge commit recorded", "kustomization", name, "repo", pr.Repo, "pr", pr.PRNumber)
		return nil
	}
	if state.SourceRepo != "" && strings.EqualFold(model.NormalizeRepo(state.SourceRepo), model.NormalizeRepo(pr.Repo)) {
		applied := flux.CleanRevisionSHA(obs.LastAppliedRevision)
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
	done, failed, reason := flux.PRRolloutStatus(in)
	if failed {
		return s.failKustomizationRollout(ctx, pr, name, reason)
	}
	readyJobs := flux.ReadyForCommitServices(in)
	if done {
		readyJobs = append(readyJobs, name)
	}
	if err := s.sendOwnedRolloutProgress(ctx, pr, name, readyJobs, done); err != nil {
		return err
	}
	if !done {
		slog.InfoContext(ctx, "[synapse] [flux-watch] waiting for rollout", "kustomization", name, "repo", pr.Repo, "pr", pr.PRNumber, "reason", reason)
	}
	return nil
}

// commitTime returns the committer date of sha in repo, which for a merge commit is when it merged.
func (s *RouterServer) commitTime(ctx context.Context, repo, sha string) (time.Time, error) {
	repo = model.NormalizeRepo(repo)
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
	repo = model.NormalizeRepo(repo)
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

// failKustomizationRollout reports a PR rollout failure on a Kustomization outcome to PRObject, which
// reports it once. A send error leaves the PR deploying, so the watcher's re-list retries it.
func (s *RouterServer) failKustomizationRollout(ctx context.Context, pr DeployingPR, name, reason string) error {
	// PRObject applies deploy_failed and publishes the carried fact.
	evt := DeployEvent{Repo: pr.Repo, PRNumber: pr.PRNumber, JobName: name, CommitSHA: strutil.FirstNonEmpty(pr.MergeSHA, pr.HeadSHA), TargetID: pr.TargetID, Details: reason}
	if err := s.sendOwnedDeployFailed(ctx, evt, pr.Branch); err != nil {
		return fmt.Errorf("report %s failure for %s#%d: %w", name, pr.Repo, pr.PRNumber, err)
	}
	return nil
}

// newKustomizationRequests builds the reconcile request recorded for each ref, all requested at now.
func newKustomizationRequests(refs []fluxKustomizationRef, now time.Time) map[string]prwire.KustomizationRequest {
	return flux.NewKustomizationRequests(refs, now)
}

// reconcileImageKustomizations patches each ref with its recorded request; one failure never skips the rest.
func (s *RouterServer) reconcileImageKustomizations(ctx context.Context, refs []fluxKustomizationRef, requests map[string]prwire.KustomizationRequest) error {
	var errs []error
	for _, ref := range refs {
		if err := s.dispatcher.ReconcileKustomization(ctx, ref, requests[ref.Name].RequestedAt); err != nil {
			slog.ErrorContext(ctx, "[synapse] [dispatcher] error reconciling kustomization", "namespace", ref.Namespace, "name", ref.Name, "error", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
