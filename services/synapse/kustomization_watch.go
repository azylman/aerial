package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// errWatchExpired signals that the watch resourceVersion is gone (410) and the watcher must re-list.
var errWatchExpired = errors.New("kube watch resourceVersion expired")

// kustomizationObservation is the live state of one Flux Kustomization as seen by a LIST or WATCH.
type kustomizationObservation struct {
	Ref                    fluxKustomizationRef
	Suspended              bool
	RequestedAt            string
	LastHandledReconcileAt string
	LastAppliedRevision    string
	SourceRef              fluxSourceRef
	Conditions             []fluxCondition
}

// kustomizationRequestHandled reports whether Flux has handled a reconcile request recorded as
// requestedAt: lastHandledReconcileAt at or after it means handled, because a later request (from
// another PR) supersedes an earlier one. Unparseable values fall back to string equality.
func kustomizationRequestHandled(requestedAt, lastHandledReconcileAt string) bool {
	requested, rErr := time.Parse(time.RFC3339Nano, requestedAt)
	handled, hErr := time.Parse(time.RFC3339Nano, lastHandledReconcileAt)
	if rErr != nil || hErr != nil {
		return requestedAt == lastHandledReconcileAt
	}
	return !handled.Before(requested)
}

// kustomizationReady classifies the Ready condition: ready on True; failed on False with a terminal
// reason. Unknown, Progressing and DependencyNotReady are neither, since Flux requeues and writes status
// again, producing the next watch event.
func kustomizationReady(conditions []fluxCondition) (ready, failed bool, reason string) {
	i := slices.IndexFunc(conditions, func(c fluxCondition) bool { return c.Type == "Ready" })
	if i == -1 {
		return false, false, ""
	}
	c := conditions[i]
	reason = c.Reason
	if c.Message != "" {
		reason = cmp.Or(c.Reason, "Ready=False") + ": " + c.Message
	}
	switch {
	case c.Status == "True":
		return true, false, reason
	case c.Status != "False", c.Reason == "Progressing", c.Reason == "DependencyNotReady":
		return false, false, reason
	default:
		return false, true, reason
	}
}

type kustomizationActionKind int

const (
	kustomizationWait kustomizationActionKind = iota
	kustomizationMarkDeployed
	kustomizationFail
	kustomizationRepatch
)

// kustomizationAction is what a PR rollout must do about one observed Kustomization.
type kustomizationAction struct {
	Kind   kustomizationActionKind
	Reason string
}

// kustomizationRequestAction decides how an observation affects a recorded reconcile request.
func kustomizationRequestAction(recorded kustomizationRequest, obs kustomizationObservation) kustomizationAction {
	if kustomizationRequestHandled(recorded.RequestedAt, obs.LastHandledReconcileAt) {
		ready, failed, reason := kustomizationReady(obs.Conditions)
		switch {
		case ready:
			return kustomizationAction{Kind: kustomizationMarkDeployed}
		case failed:
			return kustomizationAction{Kind: kustomizationFail, Reason: reason}
		default:
			return kustomizationAction{Kind: kustomizationWait}
		}
	}
	if obs.Suspended {
		return kustomizationAction{Kind: kustomizationFail, Reason: "Kustomization suspended"}
	}
	// The annotation at or after the recorded value means this (or a newer) request is pending in Flux;
	// anything older means the patch was lost and the recorded value must be re-applied.
	if kustomizationRequestHandled(recorded.RequestedAt, obs.RequestedAt) {
		return kustomizationAction{Kind: kustomizationWait}
	}
	return kustomizationAction{Kind: kustomizationRepatch}
}

// decodeKustomizationList decodes a Kustomization LIST into observations and its resourceVersion.
func decodeKustomizationList(body []byte, namespace string) ([]kustomizationObservation, string, error) {
	var list struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Items []fluxKustomizationObject `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, "", fmt.Errorf("decode kustomization list: %w", err)
	}
	observations := make([]kustomizationObservation, 0, len(list.Items))
	for _, item := range list.Items {
		observations = append(observations, item.observation(namespace))
	}
	return observations, list.Metadata.ResourceVersion, nil
}

// rolloutSink receives the level-triggered state of every watched Kustomization, and reports whether
// any PR still waits on a Kustomization so workload changes know when to re-evaluate.
type rolloutSink interface {
	applyKustomizationList(ctx context.Context, namespace string, observations []kustomizationObservation) error
	applyKustomizationStatus(ctx context.Context, obs kustomizationObservation) error
	applyKustomizationDeleted(ctx context.Context, ref fluxKustomizationRef) error
	hasPendingRollouts(ctx context.Context) (bool, error)
}

// kubeWatchSource is one kube collection driven by the informer loop: list applies the full state and
// returns its resourceVersion, watch streams changes from it, and apply handles one change event.
type kubeWatchSource interface {
	String() string
	list(ctx context.Context, kube *DefaultOutboundDispatcher, sink rolloutSink) (string, error)
	watch(ctx context.Context, kube *DefaultOutboundDispatcher, resourceVersion string) (io.ReadCloser, error)
	apply(ctx context.Context, kube *DefaultOutboundDispatcher, sink rolloutSink, eventType string, object json.RawMessage) error
}

// kustomizationSource watches one namespace's Kustomizations and applies each observation.
type kustomizationSource struct {
	namespace string
}

func (k kustomizationSource) String() string { return k.namespace + " kustomizations" }

func (k kustomizationSource) list(ctx context.Context, kube *DefaultOutboundDispatcher, sink rolloutSink) (string, error) {
	observations, resourceVersion, err := kube.ListKustomizations(ctx, k.namespace)
	if err != nil {
		return "", err
	}
	if err := sink.applyKustomizationList(ctx, k.namespace, observations); err != nil {
		return "", fmt.Errorf("apply kustomization list: %w", err)
	}
	return resourceVersion, nil
}

func (k kustomizationSource) watch(ctx context.Context, kube *DefaultOutboundDispatcher, resourceVersion string) (io.ReadCloser, error) {
	return kube.WatchKustomizations(ctx, k.namespace, resourceVersion)
}

func (k kustomizationSource) apply(ctx context.Context, _ *DefaultOutboundDispatcher, sink rolloutSink, eventType string, object json.RawMessage) error {
	var obj fluxKustomizationObject
	if err := json.Unmarshal(object, &obj); err != nil {
		return fmt.Errorf("decode %s kustomization: %w", eventType, err)
	}
	obs := obj.observation(k.namespace)
	if eventType == "DELETED" {
		return sink.applyKustomizationDeleted(ctx, obs.Ref)
	}
	return sink.applyKustomizationStatus(ctx, obs)
}

// workloadSource watches the Flux-managed workloads at path. A Kustomization that is already Ready emits
// no further event, so every workload change re-lists flux-system Kustomizations while a rollout is
// pending, letting a request gated on workload rollout complete once its workloads become available.
type workloadSource struct {
	path string
}

func (w workloadSource) String() string { return w.path }

func (w workloadSource) list(ctx context.Context, kube *DefaultOutboundDispatcher, sink rolloutSink) (string, error) {
	resourceVersion, err := kube.ListFluxWorkloadsVersion(ctx, w.path)
	if err != nil {
		return "", err
	}
	return resourceVersion, recheckPendingRollouts(ctx, kube, sink)
}

func (w workloadSource) watch(ctx context.Context, kube *DefaultOutboundDispatcher, resourceVersion string) (io.ReadCloser, error) {
	return kube.WatchFluxWorkloads(ctx, w.path, resourceVersion)
}

func (w workloadSource) apply(ctx context.Context, kube *DefaultOutboundDispatcher, sink rolloutSink, eventType string, _ json.RawMessage) error {
	if eventType == "DELETED" {
		return nil
	}
	return recheckPendingRollouts(ctx, kube, sink)
}

// recheckPendingRollouts re-lists flux-system Kustomizations while any PR rollout is pending, so requests
// gated on workload rollout re-evaluate against current state.
func recheckPendingRollouts(ctx context.Context, kube *DefaultOutboundDispatcher, sink rolloutSink) error {
	pending, err := sink.hasPendingRollouts(ctx)
	if err != nil || !pending {
		return err
	}
	if _, err := (kustomizationSource{namespace: fluxSystemNamespace}).list(ctx, kube, sink); err != nil {
		return fmt.Errorf("re-evaluate rollouts: %w", err)
	}
	return nil
}

// runRolloutRechecks re-evaluates pending rollouts on every tick until ctx ends or ticks closes, so
// time-based outcomes such as a Reloader restart timeout fire even when no watched object changes.
func runRolloutRechecks(ctx context.Context, kube *DefaultOutboundDispatcher, sink rolloutSink, ticks <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				return
			}
			if err := recheckPendingRollouts(ctx, kube, sink); err != nil && ctx.Err() == nil {
				log.Printf("[synapse] [flux-watch] error re-checking pending rollouts: %v", err)
			}
		}
	}
}

// kubeWatcher runs the informer loop over one source: LIST and apply the full state, WATCH from the
// list resourceVersion, and re-LIST whenever the stream ends or expires.
type kubeWatcher struct {
	kube       *DefaultOutboundDispatcher
	sink       rolloutSink
	source     kubeWatchSource
	retryDelay time.Duration
}

// newKubeWatcher constructs a watcher; retryDelay is the initial backoff after a failure.
func newKubeWatcher(kube *DefaultOutboundDispatcher, sink rolloutSink, source kubeWatchSource, retryDelay time.Duration) *kubeWatcher {
	return &kubeWatcher{kube: kube, sink: sink, source: source, retryDelay: retryDelay}
}

// Run loops until ctx is cancelled. A finished or expired watch re-lists immediately; failures back off.
func (w *kubeWatcher) Run(ctx context.Context) {
	delay := w.retryDelay
	for ctx.Err() == nil {
		err := w.listAndWatch(ctx)
		if err == nil || errors.Is(err, errWatchExpired) {
			delay = w.retryDelay
			continue
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("[synapse] [flux-watch] error watching %s (retrying in %s): %v", w.source, delay, err)
		select {
		case <-time.After(delay):
			delay = advanceBackoffDelay(delay, true)
		case <-ctx.Done():
			return
		}
	}
}

func (w *kubeWatcher) listAndWatch(ctx context.Context) error {
	resourceVersion, err := w.source.list(ctx, w.kube, w.sink)
	if err != nil {
		return err
	}
	stream, err := w.source.watch(ctx, w.kube, resourceVersion)
	if err != nil {
		return err
	}
	defer stream.Close()
	return w.consume(ctx, stream)
}

type kubeWatchEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

// consume applies watch events until the stream ends (nil), expires (errWatchExpired) or fails.
func (w *kubeWatcher) consume(ctx context.Context, stream io.Reader) error {
	dec := json.NewDecoder(stream)
	for {
		var evt kubeWatchEvent
		if err := dec.Decode(&evt); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("decode watch event: %w", err)
		}
		if err := w.applyEvent(ctx, evt); err != nil {
			return err
		}
	}
}

func (w *kubeWatcher) applyEvent(ctx context.Context, evt kubeWatchEvent) error {
	switch evt.Type {
	case "ADDED", "MODIFIED", "DELETED":
		return w.source.apply(ctx, w.kube, w.sink, evt.Type, evt.Object)
	case "ERROR":
		var status struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(evt.Object, &status); err == nil && status.Code == http.StatusGone {
			return errWatchExpired
		}
		return fmt.Errorf("watch error event: %s", string(evt.Object))
	default:
		return nil
	}
}

// rolloutWatchSources are the collections the rollout watcher follows: flux-system Kustomizations and
// every Flux-managed workload kind whose rollout gates a deploy.
func rolloutWatchSources() []kubeWatchSource {
	sources := make([]kubeWatchSource, 0, 1+len(rolloutWorkloadKinds))
	sources = append(sources, kustomizationSource{namespace: fluxSystemNamespace})
	for _, k := range rolloutWorkloadKinds {
		sources = append(sources, workloadSource{path: k.Path})
	}
	return sources
}

// startKustomizationWatcher runs the Kustomization rollout watchers when PR state is durable and the kube
// API is configured. The returned stop function cancels them and waits for them to exit; it is idempotent.
func (s *RouterServer) startKustomizationWatcher(ctx context.Context, kube *DefaultOutboundDispatcher) func() {
	if s.registry == nil || kube == nil || kube.kubeAPIServer == "" {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, source := range rolloutWatchSources() {
		wg.Go(func() { newKubeWatcher(kube, s, source, kube.retryDelay).Run(ctx) })
	}
	ticker := time.NewTicker(rolloutRecheckInterval)
	wg.Go(func() {
		defer ticker.Stop()
		runRolloutRechecks(ctx, kube, s, ticker.C)
	})
	return func() {
		cancel()
		wg.Wait()
	}
}

// trackedRollout is a deploying PR whose completion is gated on recorded Kustomization requests.
type trackedRollout struct {
	PR       DeployingPR
	Requests map[string]kustomizationRequest
	Deployed []string
}

// recordedKustomizationRequests decodes the PR's kustomization_requests metadata, if any.
func recordedKustomizationRequests(meta map[string]any) map[string]kustomizationRequest {
	raw, ok := meta[kustomizationRequestsKey].(map[string]any)
	if !ok {
		return nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var requests map[string]kustomizationRequest
	if err := json.Unmarshal(encoded, &requests); err != nil {
		return nil
	}
	return requests
}

// pendingRequest returns the PR's recorded request for ref while it has not been deployed.
func (t trackedRollout) pendingRequest(ref fluxKustomizationRef) (kustomizationRequest, bool) {
	req, ok := t.Requests[ref.Name]
	if !ok || req.RequestedAt == "" || cmp.Or(req.Namespace, fluxSystemNamespace) != ref.Namespace || slices.Contains(t.Deployed, ref.Name) {
		return kustomizationRequest{}, false
	}
	return req, true
}

// hasPending reports whether any recorded request is still waiting to be deployed.
func (t trackedRollout) hasPending() bool {
	for name, req := range t.Requests {
		if _, pending := t.pendingRequest(fluxKustomizationRef{Namespace: cmp.Or(req.Namespace, fluxSystemNamespace), Name: name}); pending {
			return true
		}
	}
	return false
}

// hasPendingRollouts reports whether any deploying PR still waits on a recorded Kustomization request.
func (s *RouterServer) hasPendingRollouts(ctx context.Context) (bool, error) {
	rollouts, err := s.trackedRollouts(ctx)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(rollouts, trackedRollout.hasPending), nil
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
			if _, pending := t.pendingRequest(ref); pending && ref.Namespace == namespace && !present[ref] {
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
		if _, pending := t.pendingRequest(ref); pending {
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
	repatchAt := ""
	var errs []error
	var state kustomizationRolloutState
	var loadErr error
	loaded := false
	for _, t := range rollouts {
		req, pending := t.pendingRequest(obs.Ref)
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
		log.Printf("[synapse] [flux-watch] re-requesting lost reconcile of %s/%s at %s", obs.Ref.Namespace, obs.Ref.Name, repatchAt)
		if err := s.dispatcher.ReconcileKustomization(ctx, obs.Ref, repatchAt); err != nil {
			errs = append(errs, fmt.Errorf("re-request reconcile of %s/%s: %w", obs.Ref.Namespace, obs.Ref.Name, err))
		}
	}
	return errors.Join(errs...)
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
		log.Printf("[synapse] [flux-watch] waiting on %s for %s#%d: no merge commit recorded", name, pr.Repo, pr.PRNumber)
		return nil
	}
	if state.SourceRepo != "" && strings.EqualFold(normalizeRepo(state.SourceRepo), normalizeRepo(pr.Repo)) {
		applied := cleanRevisionSHA(obs.LastAppliedRevision)
		live, err := s.revisionIncludes(ctx, pr.Repo, applied, commitSHA)
		if err != nil {
			return fmt.Errorf("check applied revision of %s for %s#%d: %w", name, pr.Repo, pr.PRNumber, err)
		}
		if !live {
			log.Printf("[synapse] [flux-watch] waiting for %s to apply %s for %s#%d (applied %q)", name, commitSHA, pr.Repo, pr.PRNumber, applied)
			return nil
		}
	}
	since, err := s.commitTime(ctx, pr.Repo, commitSHA)
	if err != nil {
		return fmt.Errorf("resolve merge time of %s#%d: %w", pr.Repo, pr.PRNumber, err)
	}
	done, failed, reason := prRolloutStatus(prRolloutInput{Workloads: state.Workloads, ConfigMaps: state.ConfigMaps, CommitSHA: commitSHA, Since: since, Now: time.Now()})
	switch {
	case failed:
		return s.failKustomizationRollout(ctx, pr, name, reason)
	case done:
		return s.completeKustomizationRequest(ctx, pr, name)
	default:
		log.Printf("[synapse] [flux-watch] waiting for rollout of %s for %s#%d: %s", name, pr.Repo, pr.PRNumber, reason)
		return nil
	}
}

// gitHistoryCache memoizes the GitHub facts the rollout gate needs. Both are immutable: a commit's
// time never changes, and neither does whether one SHA descends from another.
type gitHistoryCache struct {
	mu          sync.Mutex
	commitTimes map[string]time.Time
	includes    map[string]bool
}

func (c *gitHistoryCache) commitTime(key string) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.commitTimes[key]
	return at, ok
}

func (c *gitHistoryCache) storeCommitTime(key string, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.commitTimes == nil {
		c.commitTimes = make(map[string]time.Time)
	}
	c.commitTimes[key] = at
}

func (c *gitHistoryCache) includesVerdict(key string) (verdict, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	verdict, ok = c.includes[key]
	return verdict, ok
}

func (c *gitHistoryCache) storeIncludesVerdict(key string, verdict bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.includes == nil {
		c.includes = make(map[string]bool)
	}
	c.includes[key] = verdict
}

// commitTime returns the committer date of sha in repo, which for a merge commit is when it merged.
func (s *RouterServer) commitTime(ctx context.Context, repo, sha string) (time.Time, error) {
	repo = normalizeRepo(repo)
	key := repo + "@" + sha
	if at, ok := s.gitHistory.commitTime(key); ok {
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
	s.gitHistory.storeCommitTime(key, commit.Committer.Date)
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
	if verdict, ok := s.gitHistory.includesVerdict(key); ok {
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
	s.gitHistory.storeIncludesVerdict(key, verdict)
	return verdict, nil
}

// completeKustomizationRequest records one handled Kustomization; only the call that flips the PR to
// deployed dispatches the success wake. Only a failed registry update is returned: once the PR is
// deployed it leaves the deploying set, so a re-list could not retry a failed wake anyway.
func (s *RouterServer) completeKustomizationRequest(ctx context.Context, pr DeployingPR, name string) error {
	transitioned, err := s.registry.MarkKustomizationsDeployed(ctx, pr.Repo, pr.PRNumber, []string{name})
	if err != nil {
		return fmt.Errorf("mark %s deployed for %s#%d: %w", name, pr.Repo, pr.PRNumber, err)
	}
	if !transitioned {
		if err := s.SyncPRWidget(ctx, pr.Repo, pr.PRNumber); err != nil {
			log.Printf("[synapse] [widget] warning syncing rollout widget for %s#%d: %v", pr.Repo, pr.PRNumber, err)
		}
		return nil
	}
	commitSHA := firstNonEmpty(pr.MergeSHA, pr.HeadSHA)
	s.recordDeploySuccessDispatched(pr.Repo, name, commitSHA, pr.PRNumber)
	s.setPRWidgetState(ctx, pr.Repo, pr.PRNumber, "deployed", "")
	targetID := pr.TargetID
	if targetID == "" {
		if resolved, rErr := s.ResolveThreadForPR(ctx, pr.Repo, pr.PRNumber); rErr == nil {
			targetID = resolved
		}
	}
	if err := s.dispatchDeploymentSuccessPrompt(ctx, targetID, name, pr.Repo, commitSHA, pr.PRNumber, ""); err != nil {
		log.Printf("[synapse] [flux-watch] error dispatching deploy_success for %s#%d: %v", pr.Repo, pr.PRNumber, err)
	}
	return nil
}

// failKustomizationRollout fails a PR rollout on a Kustomization outcome; the registry transition
// guarantees the failure is reported once. An error leaves (or reverts) the PR deploying, so the
// watcher's re-list retries it.
func (s *RouterServer) failKustomizationRollout(ctx context.Context, pr DeployingPR, name, reason string) error {
	evt := DeployEvent{
		Event:      "deploy_failed",
		Status:     "failed",
		SourceKind: "Kustomization",
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

// formatKustomizationRolloutStatus renders a deploying PR's recorded Kustomizations one per line
// (sorted), done once listed in deployed_kustomizations, else pending. ok is false when the PR is not
// deploying or recorded no requests, so callers fall back to legacy per-job rendering.
func formatKustomizationRolloutStatus(cleanStatus string, meta map[string]any) (string, bool) {
	requests := recordedKustomizationRequests(meta)
	if cleanStatus != "deploying" || len(requests) == 0 {
		return "", false
	}
	deployed := make(map[string]bool)
	for _, name := range extractStringSliceFromMetadata(meta, deployedKustomizationsKey) {
		deployed[strings.TrimSpace(name)] = true
	}
	names := make([]string, 0, len(requests))
	for name := range requests {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(names))
	for _, name := range names {
		state := "🔄 rolling out"
		if deployed[name] {
			state = "✅ rolled out"
		}
		lines = append(lines, fmt.Sprintf("`%s`: %s", name, state))
	}
	return strings.Join(lines, "\n"), true
}
