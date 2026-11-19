package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/synapse/internal/kube"
)

const (
	jobGitSync         = "git-sync"
	severityError      = "error"
	kindKustomization  = "Kustomization"
	kindConfigMap      = "ConfigMap"
	kindRestateCluster = "RestateCluster"
	kindStatefulSet    = "StatefulSet"
	reasonProgressing  = "Progressing"
)

// FluxInvolvedObject identifies the resource associated with a Flux notification event.
type FluxInvolvedObject struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// FluxAlertEvent represents an alert event payload emitted by Flux notification-controller.
type FluxAlertEvent struct {
	InvolvedObject FluxInvolvedObject `json:"involvedObject"`
	Severity       string             `json:"severity"`
	Timestamp      time.Time          `json:"timestamp"`
	Message        string             `json:"message"`
	Reason         string             `json:"reason"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	Repo           string             `json:"repo,omitempty"`
}

// cleanRevisionSHA extracts the commit SHA from a Flux revision metadata string.
func cleanRevisionSHA(raw string) string {
	return kube.CleanRevisionSHA(raw)
}

// alertPayloadRepo returns the repo an alert carries in its payload or metadata, if any.
func alertPayloadRepo(alert FluxAlertEvent) string {
	if candidate := strings.TrimSpace(alert.Repo); candidate != "" {
		return candidate
	}
	return strings.TrimSpace(alert.Metadata["repo"])
}

// resolveFluxRepo determines the normalized repo name from an alert event.
func resolveFluxRepo(alert FluxAlertEvent) string {
	if candidate := alertPayloadRepo(alert); candidate != "" {
		return normalizeRepo(candidate)
	}
	return "azylman/aerial"
}

// TranslateFluxAlertToDeployEvent translates a Flux alert event into an equivalent DeployEvent.
// Returns false if the alert kind/reason is not recognized or does not map to an actionable deploy/sync event.
func TranslateFluxAlertToDeployEvent(alert FluxAlertEvent) (DeployEvent, bool) {
	repo := resolveFluxRepo(alert)
	var commitSHA string
	if alert.Metadata != nil {
		commitSHA = cleanRevisionSHA(alert.Metadata["revision"])
	}

	ts := alert.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	kind := strings.TrimSpace(alert.InvolvedObject.Kind)
	reason := strings.TrimSpace(alert.Reason)
	severity := strings.ToLower(strings.TrimSpace(alert.Severity))

	switch kind {
	case "GitRepository":
		switch {
		case reason == "NewArtifact" || reason == "GitOperationSucceeded":
			return DeployEvent{
				Event:      eventSyncSuccess,
				Status:     conclusionSuccess,
				SourceKind: kind,
				JobName:    jobGitSync,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		case severity == severityError || reason == "GitOperationFailed":
			return DeployEvent{
				Event:      eventSyncFailed,
				Status:     statusFailed,
				SourceKind: kind,
				JobName:    jobGitSync,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		default:
			return DeployEvent{}, false
		}

	case kindKustomization, "HelmRelease":
		jobName := strings.TrimSpace(alert.InvolvedObject.Name)
		switch {
		case reason == reasonProgressing:
			return DeployEvent{
				Event:      eventDeployStarted,
				Status:     "started",
				SourceKind: kind,
				JobName:    jobName,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		case reason == "ReconciliationSucceeded":
			return DeployEvent{
				Event:      eventDeploySuccess,
				Status:     conclusionSuccess,
				SourceKind: kind,
				JobName:    jobName,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		case severity == severityError || isFluxFailureReason(reason):
			return DeployEvent{
				Event:      eventDeployFailed,
				Status:     statusFailed,
				SourceKind: kind,
				JobName:    jobName,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		default:
			return DeployEvent{}, false
		}

	default:
		return DeployEvent{}, false
	}
}

func isFluxFailureReason(reason string) bool {
	switch reason {
	case "ReconciliationFailed", "HealthCheckFailed", "BuildFailed", "ArtifactFailed", "ValidationFailed":
		return true
	default:
		return false
	}
}

func (s *RouterServer) handleFluxWebhook(w http.ResponseWriter, r *http.Request) {
	body, ok := readWebhookBody(w, r, 1<<20)
	if !ok {
		return
	}
	if len(s.cfgSrc.Current().WebhookForwardURLs) > 0 {
		reqPath := r.URL.RequestURI()
		if reqPath == "" {
			reqPath = r.URL.Path
		}
		s.forwardRawWebhookToPeers(r.Context(), reqPath, r.Header, body)
	}

	var alert FluxAlertEvent
	if err := json.Unmarshal(body, &alert); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: errMsgInvalidJSONPayload})
		return
	}

	if alertPayloadRepo(alert) == "" {
		alert.Repo = s.resolveFluxAlertSourceRepo(r.Context(), alert)
	}
	deployEvt, ok := TranslateFluxAlertToDeployEvent(alert)
	if !ok {
		slog.InfoContext(r.Context(), "[synapse] [flux] ignoring unhandled flux alert", "kind", alert.InvolvedObject.Kind, "name", alert.InvolvedObject.Name, "reason", alert.Reason, "severity", alert.Severity)
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			fieldStatus: "ignored",
			"message":   "unhandled flux alert event",
		})
		return
	}

	s.enqueueOrProcessDeployEvent(w, r, deployEvt, "flux")
}

// resolveFluxAlertSourceRepo derives the repo of an alert without one from the involved object's Flux
// GitRepository source; "" (the resolveFluxRepo default) when it cannot be resolved.
func (s *RouterServer) resolveFluxAlertSourceRepo(ctx context.Context, alert FluxAlertEvent) string {
	if s.dispatcher == nil {
		return ""
	}
	repo, err := s.dispatcher.ResolveFluxSourceRepo(ctx, alert.InvolvedObject)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [flux] error resolving source repo", "kind", alert.InvolvedObject.Kind, "namespace", alert.InvolvedObject.Namespace, "name", alert.InvolvedObject.Name, "error", err)
		return ""
	}
	return repo
}

const (
	fluxSystemNamespace                = "flux-system"
	imageTagsConfigMapName             = "aerial-image-tags"
	fluxKustomizationNameLabel         = "kustomize.toolkit.fluxcd.io/name"
	fluxKustomizationNamespaceLabel    = "kustomize.toolkit.fluxcd.io/namespace"
	fluxReconcileRequestedAtAnnotation = "reconcile.fluxcd.io/requestedAt"
)

// fluxKustomizationRef identifies a Flux Kustomization object.
type fluxKustomizationRef struct {
	Namespace, Name string
}

type kubeResourceRequirements struct {
	Limits   map[string]string `json:"limits,omitempty"`
	Requests map[string]string `json:"requests,omitempty"`
}

// kubeManagedField is one metadata.managedFields entry; Subresource is set for status and scale writes.
type kubeManagedField struct {
	Time        time.Time `json:"time"`
	Subresource string    `json:"subresource"`
}

// kubeObjectMeta is the object metadata the rollout gate reads from workloads and ConfigMaps.
type kubeObjectMeta struct {
	UID             string               `json:"uid"`
	Labels          map[string]string    `json:"labels"`
	OwnerReferences []kubeOwnerReference `json:"ownerReferences"`
	Name            string               `json:"name"`
	Namespace       string               `json:"namespace"`
	Generation      int64                `json:"generation"`
	Annotations     map[string]string    `json:"annotations"`
	ManagedFields   []kubeManagedField   `json:"managedFields"`
}

// workloadRollout is the rollout state of one Deployment, StatefulSet, DaemonSet, RestateDeployment or RestateCluster.
// Replicas is spec.replicas (1 when unset) and StatusReplicas is status.replicas, which still counts old pods.
// ChangedAt is the last non-status write (see kubeObjectMeta.changedAt) and ConfigMapRefs the ConfigMaps
// its pod template consumes.
const kindRestateDeployment = "RestateDeployment"

type workloadRollout struct {
	UID                                                             string
	Selector                                                        *kubeLabelSelector
	StoppedChildren                                                 *stoppedChildObservation
	Kind, Namespace, Name                                           string
	Images                                                          []string
	ChangedAt                                                       time.Time
	Annotations                                                     map[string]string
	ConfigMapRefs                                                   []string
	Generation, ObservedGeneration                                  int64
	Replicas, UpdatedReplicas, AvailableReplicas, StatusReplicas    int32
	UpdateRevision, CurrentRevision                                 string
	DesiredNumberScheduled, UpdatedNumberScheduled, NumberAvailable int32
	ProgressDeadlineExceeded                                        bool
	ReadyReplicas                                                   int32
	DeploymentID                                                    string
	ReadyConditionTrue                                              bool
	ManagedWorkloads                                                []workloadRollout
	Env                                                             map[string]string
	Resources                                                       kubeResourceRequirements
}

// imageTag returns the tag of an image reference, ignoring any digest and registry port; "" when untagged.
func imageTag(image string) string {
	ref, _, _ := strings.Cut(strings.TrimSpace(image), "@")
	name := ref[strings.LastIndex(ref, "/")+1:]
	_, tag, _ := strings.CutLast(name, ":")
	return tag
}

func (w workloadRollout) String() string {
	return w.Kind + " " + w.Namespace + "/" + w.Name
}

func managedWorkloadMatchesClusterSpec(w, m workloadRollout) (bool, string) {
	if len(w.Images) > 0 && !slices.Contains(m.Images, w.Images[0]) {
		return false, fmt.Sprintf("managed %s images %v do not match cluster image %s", m, m.Images, w.Images[0])
	}
	if m.Replicas != w.Replicas {
		return false, fmt.Sprintf("managed %s replicas %d do not match cluster replicas %d", m, m.Replicas, w.Replicas)
	}
	for k, v := range w.Env {
		if m.Env[k] != v {
			return false, fmt.Sprintf("managed %s env %s=%q does not match cluster %q", m, k, m.Env[k], v)
		}
	}
	for k, v := range w.Resources.Limits {
		if m.Resources.Limits[k] != v {
			return false, fmt.Sprintf("managed %s resource limit %s=%q does not match cluster %q", m, k, m.Resources.Limits[k], v)
		}
	}
	for k, v := range w.Resources.Requests {
		if m.Resources.Requests[k] != v {
			return false, fmt.Sprintf("managed %s resource request %s=%q does not match cluster %q", m, k, m.Resources.Requests[k], v)
		}
	}
	return true, ""
}

// restateClusterRolloutComplete reports whether a RestateCluster and its managed workloads are rolled out.
func restateClusterRolloutComplete(w workloadRollout) (done, failed bool, reason string) {
	if w.Replicas != 0 && !w.ReadyConditionTrue {
		return false, false, "ready=false"
	}
	if len(w.ManagedWorkloads) == 0 {
		return false, false, "waiting for managed statefulset"
	}
	for _, m := range w.ManagedWorkloads {
		if w.Replicas == 0 && m.Kind != kindStatefulSet {
			return false, false, "waiting for managed statefulset"
		}
		if ok, mismatchReason := managedWorkloadMatchesClusterSpec(w, m); !ok {
			return false, false, mismatchReason
		}
		mDone, mFailed, mReason := workloadRolloutComplete(m)
		if mFailed {
			return false, true, fmt.Sprintf("managed %s: %s", m, mReason)
		}
		if !mDone {
			return false, false, fmt.Sprintf("managed %s: %s", m, mReason)
		}
	}
	return true, false, ""
}

// stoppedWorkloadRolloutComplete is called only after the generation guard.
// Inactive controllers need no readiness, registration, or matching revisions.
func stoppedWorkloadRolloutComplete(w workloadRollout) (done, failed bool, reason string) {
	if w.StatusReplicas == 0 && w.ReadyReplicas == 0 && w.AvailableReplicas == 0 && w.UpdatedReplicas == 0 {
		return true, false, ""
	}
	return false, false, "waiting for all replicas to stop"
}

// workloadRolloutComplete reports whether the workload controller has observed its latest spec and
// every desired pod runs the current template and is available, with no old pods left.
func workloadRolloutComplete(w workloadRollout) (done, failed bool, reason string) {
	if w.Kind != kindRestateCluster && w.ObservedGeneration < w.Generation {
		return false, false, fmt.Sprintf("generation %d not yet observed", w.Generation)
	}
	switch w.Kind {
	case "Deployment":
		if w.ProgressDeadlineExceeded {
			return false, true, "progress deadline exceeded"
		}
		done = w.UpdatedReplicas == w.Replicas && w.AvailableReplicas == w.Replicas && w.StatusReplicas == w.Replicas
		reason = fmt.Sprintf("%d/%d updated, %d/%d available, %d total", w.UpdatedReplicas, w.Replicas, w.AvailableReplicas, w.Replicas, w.StatusReplicas)
	case kindStatefulSet:
		if w.Replicas == 0 {
			return stoppedWorkloadRolloutComplete(w)
		}
		done = w.UpdateRevision == w.CurrentRevision && w.AvailableReplicas == w.Replicas
		reason = fmt.Sprintf("revision %s of %s, %d/%d available", w.CurrentRevision, w.UpdateRevision, w.AvailableReplicas, w.Replicas)
	case "DaemonSet":
		done = w.UpdatedNumberScheduled == w.DesiredNumberScheduled && w.NumberAvailable == w.DesiredNumberScheduled
		reason = fmt.Sprintf("%d/%d updated, %d/%d available", w.UpdatedNumberScheduled, w.DesiredNumberScheduled, w.NumberAvailable, w.DesiredNumberScheduled)
	case kindRestateDeployment:
		if w.Replicas == 0 {
			return stoppedRestateDeploymentComplete(w)
		}
		done = w.ReadyConditionTrue && w.ReadyReplicas == w.Replicas && w.AvailableReplicas == w.Replicas && w.DeploymentID != ""
		reason = fmt.Sprintf("%d/%d ready, %d/%d available, deploymentId=%s, ready=%t", w.ReadyReplicas, w.Replicas, w.AvailableReplicas, w.Replicas, w.DeploymentID, w.ReadyConditionTrue)
	case kindRestateCluster:
		return restateClusterRolloutComplete(w)
	default:
		return false, false, "unsupported workload kind"
	}
	if done {
		return true, false, ""
	}
	return false, false, reason
}

const (
	// rolloutChangeSkew tolerates clock skew between GitHub's merge timestamp and the kube API server.
	rolloutChangeSkew = 30 * time.Second
	// reloaderRestartTimeout bounds how long a Reloader-enabled workload may lag a changed ConfigMap.
	reloaderRestartTimeout = 5 * time.Minute
	// rolloutRecheckInterval re-evaluates pending rollouts without kube events, so timeouts fire.
	rolloutRecheckInterval = 30 * time.Second

	reloaderAutoAnnotation          = "reloader.stakater.com/auto"
	reloaderConfigMapAutoAnnotation = "configmap.reloader.stakater.com/auto"
	reloaderConfigMapAnnotation     = "configmap.reloader.stakater.com/reload"
)

// configMapChange is when a Flux-managed ConfigMap was last written (see kubeObjectMeta.changedAt).
type configMapChange struct {
	Namespace, Name string
	ChangedAt       time.Time
}

// prRolloutInput is everything one Kustomization contributes to a PR's deployed decision. Since is the
// merge commit time: anything written at or after it (less rolloutChangeSkew) may carry the PR's change.
type prRolloutInput struct {
	Workloads  []workloadRollout
	ConfigMaps []configMapChange
	CommitSHA  string
	Since, Now time.Time
}

// reloadsOn reports whether Reloader restarts w when ConfigMap name changes: auto mode covers the
// ConfigMaps its pod template consumes, the reload annotation names ConfigMaps explicitly.
func (w workloadRollout) reloadsOn(name string) bool {
	auto := w.Annotations[reloaderAutoAnnotation] == "true" || w.Annotations[reloaderConfigMapAutoAnnotation] == "true"
	if auto && slices.Contains(w.ConfigMapRefs, name) {
		return true
	}
	for listed := range strings.SplitSeq(w.Annotations[reloaderConfigMapAnnotation], ",") {
		if strings.TrimSpace(listed) == name {
			return true
		}
	}
	return false
}

func linkManagedWorkloads(workloads []workloadRollout) {
	for i := range workloads {
		if workloads[i].Kind != kindRestateCluster {
			continue
		}
		targetNS := cmp.Or(workloads[i].Namespace, workloads[i].Name)
		for _, other := range workloads {
			if other.Kind == kindStatefulSet && other.Namespace == targetNS && (other.Name == workloads[i].Name || other.Name == "restate") {
				workloads[i].ManagedWorkloads = append(workloads[i].ManagedWorkloads, other)
			}
		}
	}
}

// prRolloutStatus gates a PR deploy on every workload its change reached: workloads running the
// commit's image, workloads written since the merge (manifest edits), and Reloader-enabled workloads
// consuming a ConfigMap written since the merge, which must restart after that ConfigMap changed. Done
// when all of them have rolled out; failed when one failed or Reloader has not restarted it within
// reloaderRestartTimeout; otherwise pending with the reason naming each workload still rolling out.
// Workloads nothing changed never affect the result.
func prRolloutStatus(in prRolloutInput) (done, failed bool, reason string) {
	linkManagedWorkloads(in.Workloads)
	cutoff := in.Since.Add(-rolloutChangeSkew)
	changed := func(at time.Time) bool { return !at.IsZero() && !at.Before(cutoff) }
	var pending []string
	for _, w := range in.Workloads {
		gated := changed(w.ChangedAt) || (in.CommitSHA != "" && slices.ContainsFunc(w.Images, func(image string) bool { return imageTag(image) == in.CommitSHA }))
		var awaitingRestart []string
		for _, cm := range in.ConfigMaps {
			if cm.Namespace != w.Namespace || !changed(cm.ChangedAt) || !w.reloadsOn(cm.Name) {
				continue
			}
			gated = true
			if !w.ChangedAt.Before(cm.ChangedAt) {
				continue
			}
			if in.Now.Sub(cm.ChangedAt) > reloaderRestartTimeout {
				return false, true, fmt.Sprintf("%s: not restarted by Reloader within %s of ConfigMap %s/%s changing", w, reloaderRestartTimeout, cm.Namespace, cm.Name)
			}
			awaitingRestart = append(awaitingRestart, cm.Namespace+"/"+cm.Name)
		}
		if !gated {
			continue
		}
		if len(awaitingRestart) > 0 {
			pending = append(pending, w.String()+" (awaiting Reloader restart for ConfigMap "+strings.Join(awaitingRestart, ", ")+")")
			continue
		}
		wDone, wFailed, wReason := workloadRolloutComplete(w)
		switch {
		case wFailed:
			return false, true, w.String() + ": " + wReason
		case !wDone:
			pending = append(pending, w.String()+" ("+wReason+")")
		}
	}
	if len(pending) > 0 {
		return false, false, strings.Join(pending, "; ")
	}
	return true, false, ""
}

type fluxCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// fluxSourceRef is a Kustomization spec.sourceRef.
type fluxSourceRef struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type fluxKustomizationObject struct {
	Metadata struct {
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace"`
		ResourceVersion string            `json:"resourceVersion"`
		Annotations     map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		Suspend   bool          `json:"suspend"`
		SourceRef fluxSourceRef `json:"sourceRef"`
		PostBuild struct {
			SubstituteFrom []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"substituteFrom"`
		} `json:"postBuild"`
	} `json:"spec"`
	Status struct {
		LastHandledReconcileAt string          `json:"lastHandledReconcileAt"`
		LastAppliedRevision    string          `json:"lastAppliedRevision"`
		Conditions             []fluxCondition `json:"conditions"`
	} `json:"status"`
}

// observation projects the fields the rollout watcher acts on; namespace defaults to defaultNamespace.
// The source ref namespace defaults to the Kustomization's own, as in Flux.
func (k fluxKustomizationObject) observation(defaultNamespace string) kustomizationObservation {
	namespace := cmp.Or(k.Metadata.Namespace, defaultNamespace)
	source := k.Spec.SourceRef
	source.Namespace = cmp.Or(source.Namespace, namespace)
	return kustomizationObservation{
		Ref:                    fluxKustomizationRef{Namespace: namespace, Name: k.Metadata.Name},
		Suspended:              k.Spec.Suspend,
		RequestedAt:            k.Metadata.Annotations[fluxReconcileRequestedAtAnnotation],
		LastHandledReconcileAt: k.Status.LastHandledReconcileAt,
		LastAppliedRevision:    k.Status.LastAppliedRevision,
		SourceRef:              source,
		Conditions:             k.Status.Conditions,
	}
}
