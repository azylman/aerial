package flux

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/ericlient"
	"github.com/azylman/aerial/services/synapse/internal/kube"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	watchEventDeleted                  = "DELETED"
	fluxSystemNamespace                = "flux-system"
	kustomizationRequestsKey           = "kustomization_requests"
	rolloutRecheckInterval             = 30 * time.Second
	fluxReconcileRequestedAtAnnotation = "reconcile.fluxcd.io/requestedAt"
)

// ErrWatchExpired signals that the watch resourceVersion is gone (410) and the watcher must re-list.
var ErrWatchExpired = kube.ErrWatchExpired

// KustomizationRequestHandled reports whether Flux has handled a reconcile request recorded as
// requestedAt: lastHandledReconcileAt at or after it means handled, because a later request (from
// another PR) supersedes an earlier one. Unparseable values fall back to string equality.
func KustomizationRequestHandled(requestedAt, lastHandledReconcileAt string) bool {
	requested, rErr := time.Parse(time.RFC3339Nano, requestedAt)
	handled, hErr := time.Parse(time.RFC3339Nano, lastHandledReconcileAt)
	if rErr != nil || hErr != nil {
		return requestedAt == lastHandledReconcileAt
	}
	return !handled.Before(requested)
}

// KustomizationReady classifies the Ready condition: ready on True; failed on False with a terminal
// reason. Unknown, Progressing and DependencyNotReady are neither, since Flux requeues and writes status
// again, producing the next watch event.
func KustomizationReady(conditions []model.FluxCondition) (ready, failed bool, reason string) {
	i := slices.IndexFunc(conditions, func(c model.FluxCondition) bool { return c.Type == "Ready" })
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

// KustomizationActionKind represents the action to take for an observed Kustomization.
type KustomizationActionKind int

const (
	KustomizationWait KustomizationActionKind = iota
	KustomizationMarkDeployed
	KustomizationFail
	KustomizationRepatch
)

// KustomizationAction is what a PR rollout must do about one observed Kustomization.
type KustomizationAction struct {
	Kind   KustomizationActionKind
	Reason string
}

// KustomizationRequestAction decides how an observation affects a recorded reconcile request.
func KustomizationRequestAction(recorded prwire.KustomizationRequest, obs model.KustomizationObservation) KustomizationAction {
	if KustomizationRequestHandled(recorded.RequestedAt, obs.LastHandledReconcileAt) {
		ready, failed, reason := KustomizationReady(obs.Conditions)
		switch {
		case ready:
			return KustomizationAction{Kind: KustomizationMarkDeployed}
		case failed:
			return KustomizationAction{Kind: KustomizationFail, Reason: reason}
		default:
			return KustomizationAction{Kind: KustomizationWait}
		}
	}
	if obs.Suspended {
		return KustomizationAction{Kind: KustomizationFail, Reason: "Kustomization suspended"}
	}
	if KustomizationRequestHandled(recorded.RequestedAt, obs.RequestedAt) {
		return KustomizationAction{Kind: KustomizationWait}
	}
	return KustomizationAction{Kind: KustomizationRepatch}
}

// RolloutSink receives the level-triggered state of every watched Kustomization, and reports whether
// any PR still waits on a Kustomization so workload changes know when to re-evaluate.
type RolloutSink interface {
	ApplyKustomizationList(ctx context.Context, namespace string, observations []model.KustomizationObservation) error
	ApplyKustomizationStatus(ctx context.Context, obs model.KustomizationObservation) error
	ApplyKustomizationDeleted(ctx context.Context, ref model.FluxKustomizationRef) error
	HasPendingRollouts(ctx context.Context) (bool, error)
}

// KubeClient abstracts the Kubernetes API client used by KubeWatcher.
type KubeClient interface {
	ListKustomizations(ctx context.Context, namespace string) ([]model.KustomizationObservation, string, error)
	WatchKustomizations(ctx context.Context, namespace, resourceVersion string) (io.ReadCloser, error)
	ListFluxWorkloadsVersion(ctx context.Context, path string) (string, error)
	WatchFluxWorkloads(ctx context.Context, path, resourceVersion string) (io.ReadCloser, error)
	RetryDelay() time.Duration
	APIServer() string
}

// KubeWatchSource is one kube collection driven by the informer loop: list applies the full state and
// returns its resourceVersion, watch streams changes from it, and apply handles one change event.
type KubeWatchSource interface {
	fmt.Stringer
	List(ctx context.Context, client KubeClient, sink RolloutSink) (string, error)
	Watch(ctx context.Context, client KubeClient, resourceVersion string) (io.ReadCloser, error)
	Apply(ctx context.Context, client KubeClient, sink RolloutSink, eventType string, object json.RawMessage) error
}

type fluxKustomizationObject struct {
	Metadata struct {
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace"`
		ResourceVersion string            `json:"resourceVersion"`
		Annotations     map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		Suspend   bool                `json:"suspend"`
		SourceRef model.FluxSourceRef `json:"sourceRef"`
		PostBuild struct {
			SubstituteFrom []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"substituteFrom"`
		} `json:"postBuild"`
	} `json:"spec"`
	Status struct {
		LastHandledReconcileAt string                `json:"lastHandledReconcileAt"`
		LastAppliedRevision    string                `json:"lastAppliedRevision"`
		Conditions             []model.FluxCondition `json:"conditions"`
	} `json:"status"`
}

func (k fluxKustomizationObject) observation(defaultNamespace string) model.KustomizationObservation {
	namespace := cmp.Or(k.Metadata.Namespace, defaultNamespace)
	source := k.Spec.SourceRef
	source.Namespace = cmp.Or(source.Namespace, namespace)
	return model.KustomizationObservation{
		Ref:                    model.FluxKustomizationRef{Namespace: namespace, Name: k.Metadata.Name},
		Suspended:              k.Spec.Suspend,
		RequestedAt:            k.Metadata.Annotations[fluxReconcileRequestedAtAnnotation],
		LastHandledReconcileAt: k.Status.LastHandledReconcileAt,
		LastAppliedRevision:    k.Status.LastAppliedRevision,
		SourceRef:              source,
		Conditions:             k.Status.Conditions,
	}
}

// KustomizationSource watches one namespace's Kustomizations and applies each observation.
type KustomizationSource struct {
	Namespace string
}

func (k KustomizationSource) String() string { return k.Namespace + " kustomizations" }

// List fetches all Kustomizations in the namespace and applies them to sink.
func (k KustomizationSource) List(ctx context.Context, client KubeClient, sink RolloutSink) (string, error) {
	observations, resourceVersion, err := client.ListKustomizations(ctx, k.Namespace)
	if err != nil {
		return "", err
	}
	if err := sink.ApplyKustomizationList(ctx, k.Namespace, observations); err != nil {
		return "", fmt.Errorf("apply kustomization list: %w", err)
	}
	return resourceVersion, nil
}

// Watch opens a streaming watch for Kustomizations starting at resourceVersion.
func (k KustomizationSource) Watch(ctx context.Context, client KubeClient, resourceVersion string) (io.ReadCloser, error) {
	return client.WatchKustomizations(ctx, k.Namespace, resourceVersion)
}

// Apply handles one change event from the watch stream.
func (k KustomizationSource) Apply(ctx context.Context, _ KubeClient, sink RolloutSink, eventType string, object json.RawMessage) error {
	var obj fluxKustomizationObject
	if err := json.Unmarshal(object, &obj); err != nil {
		return fmt.Errorf("decode %s kustomization: %w", eventType, err)
	}
	obs := obj.observation(k.Namespace)
	if eventType == watchEventDeleted {
		return sink.ApplyKustomizationDeleted(ctx, obs.Ref)
	}
	return sink.ApplyKustomizationStatus(ctx, obs)
}

// WorkloadSource watches the Flux-managed workloads at path.
type WorkloadSource struct {
	Path string
}

func (w WorkloadSource) String() string { return w.Path }

// List fetches all workloads at path, checks pending rollouts, and returns resourceVersion.
func (w WorkloadSource) List(ctx context.Context, client KubeClient, sink RolloutSink) (string, error) {
	resourceVersion, err := client.ListFluxWorkloadsVersion(ctx, w.Path)
	if err != nil {
		return "", err
	}
	return resourceVersion, RecheckPendingRollouts(ctx, client, sink)
}

// Watch opens a streaming watch for workloads at path starting at resourceVersion.
func (w WorkloadSource) Watch(ctx context.Context, client KubeClient, resourceVersion string) (io.ReadCloser, error) {
	return client.WatchFluxWorkloads(ctx, w.Path, resourceVersion)
}

// Apply handles one change event from the workload watch stream.
func (WorkloadSource) Apply(ctx context.Context, client KubeClient, sink RolloutSink, eventType string, _ json.RawMessage) error {
	if eventType == watchEventDeleted {
		return nil
	}
	return RecheckPendingRollouts(ctx, client, sink)
}

// RecheckPendingRollouts re-lists flux-system Kustomizations while any PR rollout is pending.
func RecheckPendingRollouts(ctx context.Context, client KubeClient, sink RolloutSink) error {
	pending, err := sink.HasPendingRollouts(ctx)
	if err != nil || !pending {
		return err
	}
	if _, err := (KustomizationSource{Namespace: fluxSystemNamespace}).List(ctx, client, sink); err != nil {
		return fmt.Errorf("re-evaluate rollouts: %w", err)
	}
	return nil
}

// RunRolloutRechecks re-evaluates pending rollouts on every tick until ctx ends or ticks closes.
func RunRolloutRechecks(ctx context.Context, client KubeClient, sink RolloutSink, ticks <-chan time.Time) {
	var rateLimitUntil time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				return
			}
			if time.Now().Before(rateLimitUntil) {
				continue
			}
			if err := RecheckPendingRollouts(ctx, client, sink); err != nil && ctx.Err() == nil {
				if rlDelay, ok := githubapi.RateLimitDelay(err); ok {
					rateLimitUntil = time.Now().Add(rlDelay)
				}
				log.Printf("[synapse] [flux-watch] error re-checking pending rollouts: %v", err)
			}
		}
	}
}

// KubeWatcher runs the informer loop over one source.
type KubeWatcher struct {
	kube       KubeClient
	sink       RolloutSink
	source     KubeWatchSource
	retryDelay time.Duration
}

// NewKubeWatcher constructs a watcher.
func NewKubeWatcher(client KubeClient, sink RolloutSink, source KubeWatchSource, retryDelay time.Duration) *KubeWatcher {
	return &KubeWatcher{kube: client, sink: sink, source: source, retryDelay: retryDelay}
}

// Run loops until ctx is cancelled. A finished or expired watch re-lists immediately; failures back off.
func (w *KubeWatcher) Run(ctx context.Context) {
	delay := w.retryDelay
	for ctx.Err() == nil {
		err := w.listAndWatch(ctx)
		if err == nil || errors.Is(err, kube.ErrWatchExpired) {
			delay = w.retryDelay
			continue
		}
		if ctx.Err() != nil {
			return
		}
		wait := delay
		if rlDelay, ok := githubapi.RateLimitDelay(err); ok && rlDelay > wait {
			wait = rlDelay
		}
		log.Printf("[synapse] [flux-watch] error watching %s (retrying in %s): %v", w.source, wait, err)
		select {
		case <-time.After(wait):
			delay = ericlient.AdvanceBackoffDelay(delay, true)
		case <-ctx.Done():
			return
		}
	}
}

func (w *KubeWatcher) listAndWatch(ctx context.Context) error {
	resourceVersion, err := w.source.List(ctx, w.kube, w.sink)
	if err != nil {
		return err
	}
	stream, err := w.source.Watch(ctx, w.kube, resourceVersion)
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

func (w *KubeWatcher) consume(ctx context.Context, stream io.Reader) error {
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

func (w *KubeWatcher) applyEvent(ctx context.Context, evt kubeWatchEvent) error {
	switch evt.Type {
	case "ADDED", "MODIFIED", watchEventDeleted:
		return w.source.Apply(ctx, w.kube, w.sink, evt.Type, evt.Object)
	case "ERROR":
		var status struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(evt.Object, &status); err == nil && status.Code == http.StatusGone {
			return kube.ErrWatchExpired
		}
		return fmt.Errorf("watch error event: %s", string(evt.Object))
	default:
		return nil
	}
}

var rolloutKustomizationNamespaces = []string{fluxSystemNamespace}

var rolloutWorkloadKinds = []struct{ Kind, Path string }{
	{Kind: "Deployment", Path: "/apis/apps/v1/deployments"},
	{Kind: "StatefulSet", Path: "/apis/apps/v1/statefulsets"},
	{Kind: "DaemonSet", Path: "/apis/apps/v1/daemonsets"},
	{Kind: "RestateDeployment", Path: "/apis/restate.dev/v1beta1/restatedeployments"},
	{Kind: "RestateCluster", Path: "/apis/restate.dev/v1/restateclusters"},
}

// RolloutWatchSources returns the sources followed by the rollout watcher.
func RolloutWatchSources() []KubeWatchSource {
	sources := make([]KubeWatchSource, 0, len(rolloutKustomizationNamespaces)+len(rolloutWorkloadKinds))
	for _, namespace := range rolloutKustomizationNamespaces {
		sources = append(sources, KustomizationSource{Namespace: namespace})
	}
	for _, k := range rolloutWorkloadKinds {
		sources = append(sources, WorkloadSource{Path: k.Path})
	}
	return sources
}

// StartWatcher runs the Kustomization rollout watchers when PR state is durable and the kube API is configured.
func StartWatcher(ctx context.Context, client KubeClient, sink RolloutSink) func() {
	if client == nil || client.APIServer() == "" {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, source := range RolloutWatchSources() {
		src := source
		wg.Go(func() { NewKubeWatcher(client, sink, src, client.RetryDelay()).Run(ctx) })
	}
	ticker := time.NewTicker(rolloutRecheckInterval)
	wg.Go(func() {
		defer ticker.Stop()
		RunRolloutRechecks(ctx, client, sink, ticker.C)
	})
	return func() {
		cancel()
		wg.Wait()
	}
}

// TrackedRollout is a deploying PR whose completion is gated on recorded Kustomization requests.
type TrackedRollout struct {
	PR       model.DeployingPR
	Requests map[string]prwire.KustomizationRequest
	Deployed []string
}

// PendingRequest returns the PR's recorded request for ref while it has not been deployed.
func (t TrackedRollout) PendingRequest(ref model.FluxKustomizationRef) (prwire.KustomizationRequest, bool) {
	req, ok := t.Requests[ref.Name]
	if !ok || req.RequestedAt == "" || cmp.Or(req.Namespace, fluxSystemNamespace) != ref.Namespace {
		return prwire.KustomizationRequest{}, false
	}
	if slices.Contains(t.Deployed, ref.Name) && !IsDeploySuccessPending(t.PR.Metadata) {
		return prwire.KustomizationRequest{}, false
	}
	return req, true
}

// HasPending reports whether any recorded request is still waiting to be deployed.
func (t TrackedRollout) HasPending() bool {
	for name, req := range t.Requests {
		if _, pending := t.PendingRequest(model.FluxKustomizationRef{Namespace: cmp.Or(req.Namespace, fluxSystemNamespace), Name: name}); pending {
			return true
		}
	}
	return false
}

// IsDeploySuccessPending reports whether the deploy_success_pending flag is true in metadata.
func IsDeploySuccessPending(meta map[string]any) bool {
	if meta == nil {
		return false
	}
	v, ok := meta["deploy_success_pending"]
	if !ok {
		return false
	}
	switch b := v.(type) {
	case bool:
		return b
	case string:
		return strings.EqualFold(b, "true")
	default:
		return false
	}
}

// NewKustomizationRequests builds the reconcile request recorded for each ref, all requested at now.
func NewKustomizationRequests(refs []model.FluxKustomizationRef, now time.Time) map[string]prwire.KustomizationRequest {
	requestedAt := now.Format(time.RFC3339Nano)
	requests := make(map[string]prwire.KustomizationRequest, len(refs))
	for _, ref := range refs {
		requests[ref.Name] = prwire.KustomizationRequest{Namespace: ref.Namespace, RequestedAt: requestedAt}
	}
	return requests
}

// GitHistoryCache memoizes the GitHub facts the rollout gate needs.
type GitHistoryCache struct {
	mu          sync.Mutex
	commitTimes map[string]time.Time
	includes    map[string]bool
}

// CommitTime retrieves a cached commit time.
func (c *GitHistoryCache) CommitTime(key string) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.commitTimes[key]
	return at, ok
}

// StoreCommitTime caches a commit time.
func (c *GitHistoryCache) StoreCommitTime(key string, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.commitTimes == nil {
		c.commitTimes = make(map[string]time.Time)
	}
	c.commitTimes[key] = at
}

// IncludesVerdict retrieves a cached includes verdict.
func (c *GitHistoryCache) IncludesVerdict(key string) (verdict, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	verdict, ok = c.includes[key]
	return verdict, ok
}

// StoreIncludesVerdict caches an includes verdict.
func (c *GitHistoryCache) StoreIncludesVerdict(key string, verdict bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.includes == nil {
		c.includes = make(map[string]bool)
	}
	c.includes[key] = verdict
}
