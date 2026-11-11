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

// kubeNameRef is any kube reference object identified by name (configMapKeyRef, configMapRef, configMap volume).
type kubeNameRef struct {
	Name string `json:"name"`
}

type kubeContainer struct {
	Image string `json:"image"`
	Env   []struct {
		ValueFrom struct {
			ConfigMapKeyRef *kubeNameRef `json:"configMapKeyRef"`
		} `json:"valueFrom"`
	} `json:"env"`
	EnvFrom []struct {
		ConfigMapRef *kubeNameRef `json:"configMapRef"`
	} `json:"envFrom"`
}

type kubeVolume struct {
	ConfigMap *kubeNameRef `json:"configMap"`
	Projected struct {
		Sources []struct {
			ConfigMap *kubeNameRef `json:"configMap"`
		} `json:"sources"`
	} `json:"projected"`
}

type kubePodSpec struct {
	Containers     []kubeContainer `json:"containers"`
	InitContainers []kubeContainer `json:"initContainers"`
	Volumes        []kubeVolume    `json:"volumes"`
}

// configMapRefs returns the sorted, de-duplicated names of the ConfigMaps the pod spec consumes through
// env valueFrom, envFrom, configMap volumes and projected volume sources.
func (p kubePodSpec) configMapRefs() []string {
	var refs []*kubeNameRef
	for _, c := range slices.Concat(p.Containers, p.InitContainers) {
		for _, e := range c.Env {
			refs = append(refs, e.ValueFrom.ConfigMapKeyRef)
		}
		for _, e := range c.EnvFrom {
			refs = append(refs, e.ConfigMapRef)
		}
	}
	for _, v := range p.Volumes {
		refs = append(refs, v.ConfigMap)
		for _, src := range v.Projected.Sources {
			refs = append(refs, src.ConfigMap)
		}
	}
	var names []string
	for _, ref := range refs {
		if ref != nil && ref.Name != "" {
			names = append(names, ref.Name)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
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

// kubeManagedField is one metadata.managedFields entry; Subresource is set for status and scale writes.
type kubeManagedField struct {
	Time        time.Time `json:"time"`
	Subresource string    `json:"subresource"`
}

// kubeObjectMeta is the object metadata the rollout gate reads from workloads and ConfigMaps.
type kubeObjectMeta struct {
	Name          string             `json:"name"`
	Namespace     string             `json:"namespace"`
	Generation    int64              `json:"generation"`
	Annotations   map[string]string  `json:"annotations"`
	ManagedFields []kubeManagedField `json:"managedFields"`
}

// changedAt is the latest time any manager wrote the object itself (not a subresource), so controller
// status updates never count as a change; zero when no entry carries a time.
func (m kubeObjectMeta) changedAt() time.Time {
	var latest time.Time
	for _, f := range m.ManagedFields {
		if f.Subresource == "" && f.Time.After(latest) {
			latest = f.Time
		}
	}
	return latest
}

// workloadRollout is the rollout state of one Deployment, StatefulSet or DaemonSet. Replicas is
// spec.replicas (1 when unset) and StatusReplicas is status.replicas, which still counts old pods.
// ChangedAt is the last non-status write (see kubeObjectMeta.changedAt) and ConfigMapRefs the ConfigMaps
// its pod template consumes.
type workloadRollout struct {
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
}

// kubeRolloutList decodes an apps/v1 Deployment, StatefulSet or DaemonSet list with rollout status.
type kubeRolloutList struct {
	Items []struct {
		Metadata kubeObjectMeta `json:"metadata"`
		Spec     struct {
			Replicas *int32 `json:"replicas"`
			Template struct {
				Spec kubePodSpec `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
		Status struct {
			ObservedGeneration     int64           `json:"observedGeneration"`
			Replicas               int32           `json:"replicas"`
			UpdatedReplicas        int32           `json:"updatedReplicas"`
			AvailableReplicas      int32           `json:"availableReplicas"`
			UpdateRevision         string          `json:"updateRevision"`
			CurrentRevision        string          `json:"currentRevision"`
			DesiredNumberScheduled int32           `json:"desiredNumberScheduled"`
			UpdatedNumberScheduled int32           `json:"updatedNumberScheduled"`
			NumberAvailable        int32           `json:"numberAvailable"`
			Conditions             []fluxCondition `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}

// decodeWorkloadRollouts decodes a workload list of kind (list items carry no kind of their own).
func decodeWorkloadRollouts(kind string, body []byte) ([]workloadRollout, error) {
	var list kubeRolloutList
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, err
	}
	workloads := make([]workloadRollout, 0, len(list.Items))
	for _, item := range list.Items {
		st := item.Status
		workloads = append(workloads, workloadRollout{
			Kind:                   kind,
			Namespace:              item.Metadata.Namespace,
			Name:                   item.Metadata.Name,
			Images:                 item.Spec.Template.Spec.images(),
			ChangedAt:              item.Metadata.changedAt(),
			Annotations:            item.Metadata.Annotations,
			ConfigMapRefs:          item.Spec.Template.Spec.configMapRefs(),
			Generation:             item.Metadata.Generation,
			ObservedGeneration:     st.ObservedGeneration,
			Replicas:               *cmp.Or(item.Spec.Replicas, new(int32(1))),
			UpdatedReplicas:        st.UpdatedReplicas,
			AvailableReplicas:      st.AvailableReplicas,
			StatusReplicas:         st.Replicas,
			UpdateRevision:         st.UpdateRevision,
			CurrentRevision:        st.CurrentRevision,
			DesiredNumberScheduled: st.DesiredNumberScheduled,
			UpdatedNumberScheduled: st.UpdatedNumberScheduled,
			NumberAvailable:        st.NumberAvailable,
			ProgressDeadlineExceeded: slices.ContainsFunc(st.Conditions, func(c fluxCondition) bool {
				return c.Type == "Progressing" && c.Status == "False" && c.Reason == "ProgressDeadlineExceeded"
			}),
		})
	}
	return workloads, nil
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

// workloadRolloutComplete reports whether the workload controller has observed its latest spec and
// every desired pod runs the current template and is available, with no old pods left.
func workloadRolloutComplete(w workloadRollout) (done, failed bool, reason string) {
	if w.ObservedGeneration < w.Generation {
		return false, false, fmt.Sprintf("generation %d not yet observed", w.Generation)
	}
	switch w.Kind {
	case "Deployment":
		if w.ProgressDeadlineExceeded {
			return false, true, "progress deadline exceeded"
		}
		done = w.UpdatedReplicas == w.Replicas && w.AvailableReplicas == w.Replicas && w.StatusReplicas == w.Replicas
		reason = fmt.Sprintf("%d/%d updated, %d/%d available, %d total", w.UpdatedReplicas, w.Replicas, w.AvailableReplicas, w.Replicas, w.StatusReplicas)
	case "StatefulSet":
		done = w.UpdateRevision == w.CurrentRevision && w.AvailableReplicas == w.Replicas
		reason = fmt.Sprintf("revision %s of %s, %d/%d available", w.CurrentRevision, w.UpdateRevision, w.AvailableReplicas, w.Replicas)
	case "DaemonSet":
		done = w.UpdatedNumberScheduled == w.DesiredNumberScheduled && w.NumberAvailable == w.DesiredNumberScheduled
		reason = fmt.Sprintf("%d/%d updated, %d/%d available", w.UpdatedNumberScheduled, w.DesiredNumberScheduled, w.NumberAvailable, w.DesiredNumberScheduled)
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

// decodeConfigMapChanges decodes a ConfigMap list into the last change of each ConfigMap.
func decodeConfigMapChanges(body []byte) ([]configMapChange, error) {
	var list struct {
		Items []struct {
			Metadata kubeObjectMeta `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, err
	}
	changes := make([]configMapChange, 0, len(list.Items))
	for _, item := range list.Items {
		changes = append(changes, configMapChange{Namespace: item.Metadata.Namespace, Name: item.Metadata.Name, ChangedAt: item.Metadata.changedAt()})
	}
	return changes, nil
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

// prRolloutStatus gates a PR deploy on every workload its change reached: workloads running the
// commit's image, workloads written since the merge (manifest edits), and Reloader-enabled workloads
// consuming a ConfigMap written since the merge, which must restart after that ConfigMap changed. Done
// when all of them have rolled out; failed when one failed or Reloader has not restarted it within
// reloaderRestartTimeout; otherwise pending with the reason naming each workload still rolling out.
// Workloads nothing changed never affect the result.
func prRolloutStatus(in prRolloutInput) (done, failed bool, reason string) {
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
		LastAppliedRevision    string          `json:"lastAppliedRevision"`
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
