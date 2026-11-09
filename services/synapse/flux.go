package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
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
// Formats observed from Flux: "branch@sha1:1234abcd...", "sha1:1234abcd...", or bare "1234abcd...".
func cleanRevisionSHA(raw string) string {
	raw = strings.TrimSpace(raw)
	if idx := strings.Index(raw, "@sha1:"); idx != -1 {
		return strings.TrimSpace(raw[idx+len("@sha1:"):])
	}
	if after, found := strings.CutPrefix(raw, "sha1:"); found {
		return strings.TrimSpace(after)
	}
	if idx := strings.Index(raw, "@"); idx != -1 {
		return strings.TrimSpace(raw[idx+1:])
	}
	return raw
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
				Event:      "sync_success",
				Status:     "success",
				SourceKind: kind,
				JobName:    "git-sync",
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		case severity == "error" || reason == "GitOperationFailed":
			return DeployEvent{
				Event:      "sync_failed",
				Status:     "failed",
				SourceKind: kind,
				JobName:    "git-sync",
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		default:
			return DeployEvent{}, false
		}

	case "Kustomization", "HelmRelease":
		jobName := strings.TrimSpace(alert.InvolvedObject.Name)
		switch {
		case reason == "Progressing":
			return DeployEvent{
				Event:      "deploy_started",
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
				Event:      "deploy_success",
				Status:     "success",
				SourceKind: kind,
				JobName:    jobName,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		case severity == "error" || isFluxFailureReason(reason):
			return DeployEvent{
				Event:      "deploy_failed",
				Status:     "failed",
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
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
		return
	}

	if alertPayloadRepo(alert) == "" {
		alert.Repo = s.resolveFluxAlertSourceRepo(r.Context(), alert)
	}
	deployEvt, ok := TranslateFluxAlertToDeployEvent(alert)
	if !ok {
		log.Printf("[synapse] [flux] ignoring unhandled flux alert: kind=%s name=%s reason=%s severity=%s",
			alert.InvolvedObject.Kind, alert.InvolvedObject.Name, alert.Reason, alert.Severity)
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ignored",
			"message": "unhandled flux alert event",
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
		log.Printf("[synapse] [flux] warning resolving source repo for %s %s/%s: %v", alert.InvolvedObject.Kind, alert.InvolvedObject.Namespace, alert.InvolvedObject.Name, err)
		return ""
	}
	return repo
}

// gitRepoSlug extracts "owner/repo" from a git remote URL (https, ssh:// or scp-like), or "" if none.
func gitRepoSlug(rawURL string) string {
	ref := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(rawURL), "/"), ".git")
	if i := strings.Index(ref, "://"); i != -1 {
		ref = ref[i+3:]
		if j := strings.Index(ref, "/"); j != -1 {
			ref = ref[j+1:]
		} else {
			return ""
		}
	} else if i := strings.Index(ref, ":"); i != -1 {
		ref = ref[i+1:]
	}
	parts := strings.Split(strings.Trim(ref, "/"), "/")
	if len(parts) < 2 || parts[len(parts)-2] == "" || parts[len(parts)-1] == "" {
		return ""
	}
	return parts[len(parts)-2] + "/" + parts[len(parts)-1]
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

func compareKustomizationRefs(a, b fluxKustomizationRef) int {
	if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
		return c
	}
	return strings.Compare(a.Name, b.Name)
}

// gitRepositoryPath returns the kube API path of a Flux GitRepository.
func gitRepositoryPath(namespace, name string) string {
	return "/apis/source.toolkit.fluxcd.io/v1/namespaces/" + url.PathEscape(namespace) + "/gitrepositories/" + url.PathEscape(name)
}

// kustomizationPath returns the kube API path of ref, or of its namespace's collection when Name is empty.
func kustomizationPath(ref fluxKustomizationRef) string {
	path := "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/" + url.PathEscape(ref.Namespace) + "/kustomizations"
	if ref.Name != "" {
		path += "/" + url.PathEscape(ref.Name)
	}
	return path
}

// workloadImageRef is the subset of a workload needed to map images to their owning Kustomization.
type workloadImageRef struct {
	Labels map[string]string
	Images []string
}

type kubeContainer struct {
	Image string `json:"image"`
}

type kubePodSpec struct {
	Containers     []kubeContainer `json:"containers"`
	InitContainers []kubeContainer `json:"initContainers"`
}

func (p kubePodSpec) images() []string {
	images := make([]string, 0, len(p.Containers)+len(p.InitContainers))
	for _, c := range slices.Concat(p.Containers, p.InitContainers) {
		images = append(images, c.Image)
	}
	return images
}

// kubeWorkloadList decodes any apps/v1 or batch/v1 workload list; cronjobs carry their pod spec under
// spec.jobTemplate.spec.template.spec, every other kind under spec.template.spec.
type kubeWorkloadList struct {
	Items []struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Spec struct {
			Template struct {
				Spec kubePodSpec `json:"spec"`
			} `json:"template"`
			JobTemplate struct {
				Spec struct {
					Template struct {
						Spec kubePodSpec `json:"spec"`
					} `json:"template"`
				} `json:"spec"`
			} `json:"jobTemplate"`
		} `json:"spec"`
	} `json:"items"`
}

func decodeWorkloadImageRefs(body []byte) ([]workloadImageRef, error) {
	var list kubeWorkloadList
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, err
	}
	refs := make([]workloadImageRef, 0, len(list.Items))
	for _, item := range list.Items {
		refs = append(refs, workloadImageRef{
			Labels: item.Metadata.Labels,
			Images: slices.Concat(item.Spec.Template.Spec.images(), item.Spec.JobTemplate.Spec.Template.Spec.images()),
		})
	}
	return refs, nil
}

// imageRepositoryBasename strips registry, path, tag and digest from an image reference.
func imageRepositoryBasename(image string) string {
	ref := strings.TrimSpace(image)
	if i := strings.Index(ref, "@"); i != -1 {
		ref = ref[:i]
	}
	if i := strings.LastIndex(ref, "/"); i != -1 {
		ref = ref[i+1:]
	}
	if i := strings.Index(ref, ":"); i != -1 {
		ref = ref[:i]
	}
	return ref
}

// kustomizationsForImages returns the sorted, de-duplicated Flux Kustomizations owning any workload
// that runs one of imageNames (matched on image repository basename).
func kustomizationsForImages(workloads []workloadImageRef, imageNames []string) []fluxKustomizationRef {
	seen := make(map[fluxKustomizationRef]struct{})
	var refs []fluxKustomizationRef
	for _, w := range workloads {
		name := w.Labels[fluxKustomizationNameLabel]
		if name == "" || !slices.ContainsFunc(w.Images, func(img string) bool {
			return slices.Contains(imageNames, imageRepositoryBasename(img))
		}) {
			continue
		}
		ref := fluxKustomizationRef{Namespace: cmp.Or(w.Labels[fluxKustomizationNamespaceLabel], fluxSystemNamespace), Name: name}
		if _, dup := seen[ref]; dup {
			continue
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, compareKustomizationRefs)
	return refs
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
		Conditions             []fluxCondition `json:"conditions"`
	} `json:"status"`
}

func (k fluxKustomizationObject) substitutesImageTags() bool {
	for _, src := range k.Spec.PostBuild.SubstituteFrom {
		if src.Kind == "ConfigMap" && src.Name == imageTagsConfigMapName {
			return true
		}
	}
	return false
}

// observation projects the fields the rollout watcher acts on; namespace defaults to defaultNamespace.
func (k fluxKustomizationObject) observation(defaultNamespace string) kustomizationObservation {
	return kustomizationObservation{
		Ref:                    fluxKustomizationRef{Namespace: cmp.Or(k.Metadata.Namespace, defaultNamespace), Name: k.Metadata.Name},
		Suspended:              k.Spec.Suspend,
		RequestedAt:            k.Metadata.Annotations[fluxReconcileRequestedAtAnnotation],
		LastHandledReconcileAt: k.Status.LastHandledReconcileAt,
		Conditions:             k.Status.Conditions,
	}
}

// decodeImageTagKustomizations returns the sorted Kustomizations of a list that substitute from the
// image tag ConfigMap.
func decodeImageTagKustomizations(body []byte) ([]fluxKustomizationRef, error) {
	var list struct {
		Items []fluxKustomizationObject `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("unmarshal kustomization list: %w", err)
	}
	var refs []fluxKustomizationRef
	for _, k := range list.Items {
		if k.substitutesImageTags() {
			refs = append(refs, fluxKustomizationRef{Namespace: cmp.Or(k.Metadata.Namespace, fluxSystemNamespace), Name: k.Metadata.Name})
		}
	}
	slices.SortFunc(refs, compareKustomizationRefs)
	return refs, nil
}
