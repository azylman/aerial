package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/synapse/internal/flux"
	"github.com/azylman/aerial/services/synapse/internal/model"
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
type FluxInvolvedObject = model.FluxInvolvedObject

// FluxAlertEvent represents an alert event payload emitted by Flux notification-controller.
type FluxAlertEvent = model.FluxAlertEvent

// cleanRevisionSHA extracts the commit SHA from a Flux revision metadata string.
func cleanRevisionSHA(raw string) string {
	return flux.CleanRevisionSHA(raw)
}

// alertPayloadRepo returns the repo an alert carries in its payload or metadata, if any.
func alertPayloadRepo(alert FluxAlertEvent) string {
	return flux.AlertPayloadRepo(alert)
}

// TranslateFluxAlertToDeployEvent translates a Flux alert event into an equivalent DeployEvent.
// Returns false if the alert kind/reason is not recognized or does not map to an actionable deploy/sync event.
func TranslateFluxAlertToDeployEvent(alert FluxAlertEvent) (DeployEvent, bool) {
	mEvt, ok := flux.TranslateFluxAlertToDeployEvent(alert)
	if !ok {
		return DeployEvent{}, false
	}
	return DeployEvent{
		Event:        mEvt.Event,
		JobName:      mEvt.JobName,
		SourceKind:   mEvt.SourceKind,
		ExpectedJobs: mEvt.ExpectedJobs,
		Repo:         mEvt.Repo,
		CommitSHA:    mEvt.CommitSHA,
		PRNumber:     mEvt.PRNumber,
		TargetID:     mEvt.TargetID,
		Image:        mEvt.Image,
		Digest:       mEvt.Digest,
		Status:       mEvt.Status,
		DeploymentID: mEvt.DeploymentID,
		Details:      mEvt.Details,
		Timestamp:    mEvt.Timestamp,
	}, true
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
type fluxKustomizationRef = model.FluxKustomizationRef

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
	return flux.ImageTag(image)
}

func (w workloadRollout) String() string {
	return flux.WorkloadString(toModelWorkloadRollout(w))
}

// workloadRolloutComplete reports whether the workload controller has observed its latest spec and
// every desired pod runs the current template and is available, with no old pods left.
func workloadRolloutComplete(w workloadRollout) (done, failed bool, reason string) {
	return flux.WorkloadRolloutComplete(toModelWorkloadRollout(w))
}

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

// prRolloutStatus gates a PR deploy on every workload its change reached: workloads running the
// commit's image, workloads written since the merge (manifest edits), and Reloader-enabled workloads
// consuming a ConfigMap written since the merge, which must restart after that ConfigMap changed. Done
// when all of them have rolled out; failed when one failed or Reloader has not restarted it within
// reloaderRestartTimeout; otherwise pending with the reason naming each workload still rolling out.
// Workloads nothing changed never affect the result.
func prRolloutStatus(in prRolloutInput) (done, failed bool, reason string) {
	cms := make([]model.ConfigMapChange, len(in.ConfigMaps))
	for i, c := range in.ConfigMaps {
		cms[i] = model.ConfigMapChange{Namespace: c.Namespace, Name: c.Name, ChangedAt: c.ChangedAt}
	}
	return flux.PRRolloutStatus(flux.PRRolloutInput{
		Workloads:  toModelWorkloadRollouts(in.Workloads),
		ConfigMaps: cms,
		CommitSHA:  in.CommitSHA,
		Since:      in.Since,
		Now:        in.Now,
	})
}

type fluxCondition = model.FluxCondition
