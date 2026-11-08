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
	"slices"
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

// kustomizationSink receives the level-triggered state of every watched Kustomization.
type kustomizationSink interface {
	applyKustomizationList(ctx context.Context, namespace string, observations []kustomizationObservation) error
	applyKustomizationStatus(ctx context.Context, obs kustomizationObservation) error
	applyKustomizationDeleted(ctx context.Context, ref fluxKustomizationRef) error
}

// KustomizationWatcher runs the informer loop over one namespace's Kustomizations: LIST, apply the
// full list, WATCH from the list resourceVersion, and re-LIST whenever the stream ends or expires.
type KustomizationWatcher struct {
	kube       *DefaultOutboundDispatcher
	sink       kustomizationSink
	namespace  string
	retryDelay time.Duration
}

// NewKustomizationWatcher constructs a watcher; retryDelay is the initial backoff after a failure.
func NewKustomizationWatcher(kube *DefaultOutboundDispatcher, sink kustomizationSink, namespace string, retryDelay time.Duration) *KustomizationWatcher {
	return &KustomizationWatcher{kube: kube, sink: sink, namespace: namespace, retryDelay: retryDelay}
}

// Run loops until ctx is cancelled. A finished or expired watch re-lists immediately; failures back off.
func (w *KustomizationWatcher) Run(ctx context.Context) {
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
		log.Printf("[synapse] [flux-watch] error watching %s kustomizations (retrying in %s): %v", w.namespace, delay, err)
		select {
		case <-time.After(delay):
			delay = advanceBackoffDelay(delay, true)
		case <-ctx.Done():
			return
		}
	}
}

func (w *KustomizationWatcher) listAndWatch(ctx context.Context) error {
	observations, resourceVersion, err := w.kube.ListKustomizations(ctx, w.namespace)
	if err != nil {
		return err
	}
	if err := w.sink.applyKustomizationList(ctx, w.namespace, observations); err != nil {
		return fmt.Errorf("apply kustomization list: %w", err)
	}
	stream, err := w.kube.WatchKustomizations(ctx, w.namespace, resourceVersion)
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
func (w *KustomizationWatcher) consume(ctx context.Context, stream io.Reader) error {
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

func (w *KustomizationWatcher) applyEvent(ctx context.Context, evt kubeWatchEvent) error {
	switch evt.Type {
	case "ADDED", "MODIFIED", "DELETED":
		var obj fluxKustomizationObject
		if err := json.Unmarshal(evt.Object, &obj); err != nil {
			return fmt.Errorf("decode %s kustomization: %w", evt.Type, err)
		}
		obs := obj.observation(w.namespace)
		if evt.Type == "DELETED" {
			return w.sink.applyKustomizationDeleted(ctx, obs.Ref)
		}
		return w.sink.applyKustomizationStatus(ctx, obs)
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

// startKustomizationWatcher runs the Kustomization rollout watcher when PR state is durable and the kube
// API is configured. The returned stop function cancels it and waits for it to exit; it is idempotent.
func (s *RouterServer) startKustomizationWatcher(ctx context.Context, kube *DefaultOutboundDispatcher) func() {
	if s.registry == nil || kube == nil || kube.kubeAPIServer == "" {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		NewKustomizationWatcher(kube, s, fluxSystemNamespace, kube.retryDelay).Run(ctx)
	}()
	return func() {
		cancel()
		<-done
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

// applyObservation acts on each pending request for obs. Lost requests are repaired with one PATCH
// carrying the latest recorded value, which covers every earlier request. A returned error means some
// PR is still deploying with work left undone; the watcher then backs off and re-lists, because a
// Kustomization already in its final state emits no further event to retry on.
func (s *RouterServer) applyObservation(ctx context.Context, rollouts []trackedRollout, obs kustomizationObservation) error {
	repatchAt := ""
	var errs []error
	for _, t := range rollouts {
		req, pending := t.pendingRequest(obs.Ref)
		if !pending {
			continue
		}
		switch action := kustomizationRequestAction(req, obs); action.Kind {
		case kustomizationMarkDeployed:
			errs = append(errs, s.completeKustomizationRequest(ctx, t.PR, obs.Ref.Name))
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
