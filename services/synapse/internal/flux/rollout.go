package flux

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/kube"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	kindStatefulSet       = "StatefulSet"
	kindRestateDeployment = "RestateDeployment"
	kindRestateCluster    = "RestateCluster"

	rolloutChangeSkew      = 30 * time.Second
	reloaderRestartTimeout = 5 * time.Minute

	reloaderAutoAnnotation          = "reloader.stakater.com/auto"
	reloaderConfigMapAutoAnnotation = "configmap.reloader.stakater.com/auto"
	reloaderConfigMapAnnotation     = "configmap.reloader.stakater.com/reload"
)

// ImageTag returns the tag of an image reference, ignoring any digest and registry port; "" when untagged.
func ImageTag(image string) string {
	ref, _, _ := strings.Cut(strings.TrimSpace(image), "@")
	name := ref[strings.LastIndex(ref, "/")+1:]
	_, tag, _ := strings.CutLast(name, ":")
	return tag
}

// WorkloadString formats a workload's kind and namespaced name.
func WorkloadString(w model.WorkloadRollout) string {
	return w.Kind + " " + w.Namespace + "/" + w.Name
}

// ReloadsOn reports whether Reloader restarts w when ConfigMap name changes.
func ReloadsOn(w model.WorkloadRollout, name string) bool {
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

// LinkManagedWorkloads correlates RestateCluster workloads with their managed StatefulSet children.
func LinkManagedWorkloads(workloads []model.WorkloadRollout) {
	for i := range workloads {
		if workloads[i].Kind != kindRestateCluster {
			continue
		}
		workloads[i].ManagedWorkloads = nil
		targetNS := cmp.Or(workloads[i].Namespace, workloads[i].Name)
		for _, other := range workloads {
			if other.Kind == kindStatefulSet && other.Namespace == targetNS && (other.Name == workloads[i].Name || other.Name == "restate") {
				workloads[i].ManagedWorkloads = append(workloads[i].ManagedWorkloads, other)
			}
		}
	}
}

// ImageServiceCandidates extracts candidate service names from an image reference.
func ImageServiceCandidates(image string) []string {
	base := prwire.ExtractImageWorkloadName(image)
	if base == "" {
		return nil
	}
	candidates := []string{base}
	var stripped string
	switch {
	case strings.HasPrefix(base, "aerial-sidecar-"):
		stripped = strings.TrimPrefix(base, "aerial-sidecar-")
	case strings.HasPrefix(base, "aerial-"):
		stripped = strings.TrimPrefix(base, "aerial-")
	case strings.HasPrefix(base, "mirrormere-"):
		stripped = strings.TrimPrefix(base, "mirrormere-")
	}
	if stripped != "" && stripped != base && !slices.Contains(candidates, stripped) {
		candidates = append(candidates, stripped)
	}
	return candidates
}

// ManagedWorkloadMatchesClusterSpec checks whether a managed workload matches the cluster spec.
func ManagedWorkloadMatchesClusterSpec(w, m model.WorkloadRollout) (bool, string) {
	if len(w.Images) > 0 && !slices.Contains(m.Images, w.Images[0]) {
		return false, fmt.Sprintf("managed %s images %v do not match cluster image %s", WorkloadString(m), m.Images, w.Images[0])
	}
	if m.Replicas != w.Replicas {
		return false, fmt.Sprintf("managed %s replicas %d do not match cluster replicas %d", WorkloadString(m), m.Replicas, w.Replicas)
	}
	for k, v := range w.Env {
		if m.Env[k] != v {
			return false, fmt.Sprintf("managed %s env %s=%q does not match cluster %q", WorkloadString(m), k, m.Env[k], v)
		}
	}
	for k, v := range w.Resources.Limits {
		if m.Resources.Limits[k] != v {
			return false, fmt.Sprintf("managed %s resource limit %s=%q does not match cluster %q", WorkloadString(m), k, m.Resources.Limits[k], v)
		}
	}
	for k, v := range w.Resources.Requests {
		if m.Resources.Requests[k] != v {
			return false, fmt.Sprintf("managed %s resource request %s=%q does not match cluster %q", WorkloadString(m), k, m.Resources.Requests[k], v)
		}
	}
	return true, ""
}

// RestateClusterRolloutComplete reports whether a RestateCluster and its managed workloads are rolled out.
func RestateClusterRolloutComplete(w model.WorkloadRollout) (done, failed bool, reason string) {
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
		if ok, mismatchReason := ManagedWorkloadMatchesClusterSpec(w, m); !ok {
			return false, false, mismatchReason
		}
		mDone, mFailed, mReason := WorkloadRolloutComplete(m)
		if mFailed {
			return false, true, fmt.Sprintf("managed %s: %s", WorkloadString(m), mReason)
		}
		if !mDone {
			return false, false, fmt.Sprintf("managed %s: %s", WorkloadString(m), mReason)
		}
	}
	return true, false, ""
}

// StoppedWorkloadRolloutComplete is called only after the generation guard.
// Inactive controllers need no readiness, registration, or matching revisions.
func StoppedWorkloadRolloutComplete(w model.WorkloadRollout) (done, failed bool, reason string) {
	if w.StatusReplicas == 0 && w.ReadyReplicas == 0 && w.AvailableReplicas == 0 && w.UpdatedReplicas == 0 {
		return true, false, ""
	}
	return false, false, "waiting for all replicas to stop"
}

func terminatingWorkloadReason(w model.WorkloadRollout) string {
	ts := strings.TrimSpace(w.DeletionTimestamp)
	if len(w.Finalizers) > 0 {
		return fmt.Sprintf("terminating since %s (blocked by finalizers %v; generation %d not observed)", ts, w.Finalizers, w.Generation)
	}
	return fmt.Sprintf("terminating since %s (generation %d not observed)", ts, w.Generation)
}

func deploymentRolloutComplete(w model.WorkloadRollout) (done, failed bool, reason string) {
	if w.ProgressDeadlineExceeded {
		return false, true, "progress deadline exceeded"
	}
	if w.UpdatedReplicas == w.Replicas && w.AvailableReplicas == w.Replicas && w.StatusReplicas == w.Replicas {
		return true, false, ""
	}
	return false, false, fmt.Sprintf("%d/%d updated, %d/%d available, %d total", w.UpdatedReplicas, w.Replicas, w.AvailableReplicas, w.Replicas, w.StatusReplicas)
}

// WorkloadRolloutComplete reports whether the workload controller has observed its latest spec and
// every desired pod runs the current template and is available, with no old pods left.
func WorkloadRolloutComplete(w model.WorkloadRollout) (done, failed bool, reason string) {
	if w.Kind != kindRestateCluster && w.ObservedGeneration < w.Generation {
		if strings.TrimSpace(w.DeletionTimestamp) != "" {
			return false, true, terminatingWorkloadReason(w)
		}
		return false, false, fmt.Sprintf("generation %d not yet observed", w.Generation)
	}
	switch w.Kind {
	case "Deployment":
		return deploymentRolloutComplete(w)
	case kindStatefulSet:
		if w.Replicas == 0 {
			return StoppedWorkloadRolloutComplete(w)
		}
		done = w.UpdateRevision == w.CurrentRevision && w.AvailableReplicas == w.Replicas
		reason = fmt.Sprintf("revision %s of %s, %d/%d available", w.CurrentRevision, w.UpdateRevision, w.AvailableReplicas, w.Replicas)
	case "DaemonSet":
		done = w.UpdatedNumberScheduled == w.DesiredNumberScheduled && w.NumberAvailable == w.DesiredNumberScheduled
		reason = fmt.Sprintf("%d/%d updated, %d/%d available", w.UpdatedNumberScheduled, w.DesiredNumberScheduled, w.NumberAvailable, w.DesiredNumberScheduled)
	case kindRestateDeployment:
		if w.Replicas == 0 {
			return kube.StoppedRestateDeploymentComplete(w)
		}
		done = w.ReadyConditionTrue && w.ReadyReplicas == w.Replicas && w.AvailableReplicas == w.Replicas && w.DeploymentID != ""
		reason = fmt.Sprintf("%d/%d ready, %d/%d available, deploymentId=%s, ready=%t", w.ReadyReplicas, w.Replicas, w.AvailableReplicas, w.Replicas, w.DeploymentID, w.ReadyConditionTrue)
	case kindRestateCluster:
		return RestateClusterRolloutComplete(w)
	default:
		return false, false, "unsupported workload kind"
	}
	if done {
		return true, false, ""
	}
	return false, false, reason
}

// PRRolloutInput is everything one Kustomization contributes to a PR's deployed decision. Since is the
// merge commit time: anything written at or after it (less rolloutChangeSkew) may carry the PR's change.
type PRRolloutInput struct {
	Workloads       []model.WorkloadRollout
	ConfigMaps      []model.ConfigMapChange
	CommitSHA       string
	IncludedCommits []string
	ExpectedJobs    []string
	Since, Now      time.Time
}

func init() {
	_ = ServiceJobs(nil, nil)
	_ = CandidateDescendantTags(nil, "", nil)
}

// ServiceJobs trims, drops empty, and deduplicates entries in jobs (preserving first-occurrence
// order) that are not keys in requests (matched after strings.TrimSpace). It returns nil when no
// non-Kustomization service jobs remain (e.g., manifest-only rollouts where jobs equals the
// Kustomization names).
func ServiceJobs(jobs []string, requests map[string]prwire.KustomizationRequest) []string {
	reqKeys := make(map[string]bool, len(requests))
	for k := range requests {
		if trimmed := strings.TrimSpace(k); trimmed != "" {
			reqKeys[trimmed] = true
		}
	}
	var (
		res  []string
		seen = make(map[string]bool)
	)
	for _, j := range jobs {
		trimmed := strings.TrimSpace(j)
		if trimmed == "" || reqKeys[trimmed] || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		res = append(res, trimmed)
	}
	return res
}

// WorkloadMatchesJobs reports whether w corresponds to any job in expectedJobs.
// It returns true when len(expectedJobs) == 0; otherwise returns true if w.Name matches
// any entry in expectedJobs (case-insensitively, trimmed), or any image in w.Images has a
// candidate in ImageServiceCandidates matching any entry in expectedJobs, or any managed
// child in w.ManagedWorkloads satisfies WorkloadMatchesJobs.
func WorkloadMatchesJobs(w model.WorkloadRollout, expectedJobs []string) bool {
	if len(expectedJobs) == 0 {
		return true
	}
	wName := strings.TrimSpace(w.Name)
	for _, j := range expectedJobs {
		tj := strings.TrimSpace(j)
		if tj != "" && strings.EqualFold(wName, tj) {
			return true
		}
	}
	for _, img := range w.Images {
		for _, cand := range ImageServiceCandidates(img) {
			candTrimmed := strings.TrimSpace(cand)
			for _, j := range expectedJobs {
				tj := strings.TrimSpace(j)
				if tj != "" && strings.EqualFold(candTrimmed, tj) {
					return true
				}
			}
		}
	}
	for _, m := range w.ManagedWorkloads {
		if WorkloadMatchesJobs(m, expectedJobs) {
			return true
		}
	}
	return false
}

func imageMatchesExpectedJobs(img string, wName string, expectedJobs []string) bool {
	cands := ImageServiceCandidates(img)
	for _, cand := range cands {
		candTrimmed := strings.TrimSpace(cand)
		for _, j := range expectedJobs {
			tj := strings.TrimSpace(j)
			if tj != "" && strings.EqualFold(candTrimmed, tj) {
				return true
			}
		}
	}
	if len(cands) == 0 {
		tName := strings.TrimSpace(wName)
		for _, j := range expectedJobs {
			tj := strings.TrimSpace(j)
			if tj != "" && strings.EqualFold(tName, tj) {
				return true
			}
		}
	}
	return false
}

func appendCandidateTags(tags []string, seen map[string]bool, images []string, names []string, commitSHA string, expectedJobs []string) []string {
	for _, img := range images {
		if !imageMatchesAnyName(img, names, expectedJobs) {
			continue
		}
		tag := ImageTag(img)
		if tag == "" || tag == commitSHA || strings.EqualFold(tag, "latest") || seen[tag] {
			continue
		}
		seen[tag] = true
		tags = append(tags, tag)
	}
	return tags
}

func imageMatchesAnyName(img string, names []string, expectedJobs []string) bool {
	for _, name := range names {
		if imageMatchesExpectedJobs(img, name, expectedJobs) {
			return true
		}
	}
	return false
}

// CandidateDescendantTags returns candidate descendant image tags found on workloads matching expectedJobs.
// It returns nil when commitSHA == "" or len(expectedJobs) == 0. Otherwise it calls LinkManagedWorkloads
// and scans non-deleting workloads where WorkloadMatchesJobs is true; for each image in w.Images (and managed
// child images) whose ImageServiceCandidates matches at least one entry in expectedJobs (or when w.Name
// matches expectedJobs and len(ImageServiceCandidates) == 0), extracts ImageTag and collects unique tags
// where tag != "", tag != commitSHA, and tag != "latest" (case-insensitive) in first-seen order.
func CandidateDescendantTags(workloads []model.WorkloadRollout, commitSHA string, expectedJobs []string) []string {
	if commitSHA == "" || len(expectedJobs) == 0 {
		return nil
	}
	LinkManagedWorkloads(workloads)

	var (
		tags []string
		seen = make(map[string]bool)
	)
	for _, w := range workloads {
		if strings.TrimSpace(w.DeletionTimestamp) != "" || !WorkloadMatchesJobs(w, expectedJobs) {
			continue
		}
		tags = appendCandidateTags(tags, seen, w.Images, []string{w.Name}, commitSHA, expectedJobs)
		for _, m := range w.ManagedWorkloads {
			if strings.TrimSpace(m.DeletionTimestamp) != "" {
				continue
			}
			tags = appendCandidateTags(tags, seen, m.Images, []string{m.Name, w.Name}, commitSHA, expectedJobs)
		}
	}
	return tags
}

func checkReloaderRestart(w model.WorkloadRollout, configMaps []model.ConfigMapChange, changed func(time.Time) bool, now time.Time) (gated bool, awaiting []string, failReason string) {
	wStr := WorkloadString(w)
	for _, cm := range configMaps {
		if cm.Namespace != w.Namespace || !changed(cm.ChangedAt) || !ReloadsOn(w, cm.Name) {
			continue
		}
		gated = true
		if !w.ChangedAt.Before(cm.ChangedAt) {
			continue
		}
		if now.Sub(cm.ChangedAt) > reloaderRestartTimeout {
			return true, nil, fmt.Sprintf("%s: not restarted by Reloader within %s of ConfigMap %s/%s changing", wStr, reloaderRestartTimeout, cm.Namespace, cm.Name)
		}
		awaiting = append(awaiting, cm.Namespace+"/"+cm.Name)
	}
	return gated, awaiting, ""
}

// PRRolloutStatus gates a PR deploy on every workload its change reached: workloads running the
// commit's image, workloads written since the merge (manifest edits), and Reloader-enabled workloads
// consuming a ConfigMap written since the merge, which must restart after that ConfigMap changed. Done
// when all of them have rolled out; failed when one failed or Reloader has not restarted it within
// reloaderRestartTimeout; otherwise pending with the reason naming each workload still rolling out.
// Workloads nothing changed never affect the result.
func PRRolloutStatus(in PRRolloutInput) (done, failed bool, reason string) {
	LinkManagedWorkloads(in.Workloads)
	cutoff := in.Since.Add(-rolloutChangeSkew)
	changed := func(at time.Time) bool { return !at.IsZero() && !at.Before(cutoff) }
	var pending []string
	for _, w := range in.Workloads {
		if len(in.ExpectedJobs) > 0 && !WorkloadMatchesJobs(w, in.ExpectedJobs) {
			continue
		}
		wStr := WorkloadString(w)
		imageMatched := in.CommitSHA != "" && len(commitMatchingImages(w.Images, in.CommitSHA, in.IncludedCommits)) > 0
		cmGated, awaitingRestart, failMsg := checkReloaderRestart(w, in.ConfigMaps, changed, in.Now)
		if failMsg != "" {
			return false, true, failMsg
		}
		gated := changed(w.ChangedAt) || imageMatched || cmGated
		if !gated {
			if len(in.ExpectedJobs) > 0 && strings.TrimSpace(w.DeletionTimestamp) == "" && in.CommitSHA != "" {
				pending = append(pending, wStr+" (awaiting image tag "+in.CommitSHA+")")
			}
			continue
		}
		if len(awaitingRestart) > 0 {
			pending = append(pending, wStr+" (awaiting Reloader restart for ConfigMap "+strings.Join(awaitingRestart, ", ")+")")
			continue
		}
		wDone, wFailed, wReason := WorkloadRolloutComplete(w)
		switch {
		case wFailed:
			return false, true, wStr + ": " + wReason
		case !wDone:
			pending = append(pending, wStr+" ("+wReason+")")
		}
	}
	if len(pending) > 0 {
		return false, false, strings.Join(pending, "; ")
	}
	return true, false, ""
}

func workloadAwaitingConfigMapRestart(w model.WorkloadRollout, configMaps []model.ConfigMapChange, changed func(time.Time) bool) bool {
	for _, cm := range configMaps {
		if cm.Namespace == w.Namespace && changed(cm.ChangedAt) && ReloadsOn(w, cm.Name) && w.ChangedAt.Before(cm.ChangedAt) {
			return true
		}
	}
	return false
}

func commitMatchingImages(images []string, commitSHA string, includedCommits []string) []string {
	var matching []string
	for _, img := range images {
		tag := ImageTag(img)
		if tag != "" && (tag == commitSHA || slices.Contains(includedCommits, tag)) {
			matching = append(matching, img)
		}
	}
	return matching
}

// ReadyForCommitServices identifies candidate service names whose commit-matching workloads have finished rolling out.
func ReadyForCommitServices(in PRRolloutInput) []string {
	if in.CommitSHA == "" || len(in.Workloads) == 0 {
		return nil
	}
	LinkManagedWorkloads(in.Workloads)
	cutoff := in.Since.Add(-rolloutChangeSkew)
	changed := func(at time.Time) bool { return !at.IsZero() && !at.Before(cutoff) }

	var (
		seen    []string
		seenMap = make(map[string]bool)
		unready = make(map[string]bool)
	)

	for _, w := range in.Workloads {
		if strings.TrimSpace(w.DeletionTimestamp) != "" {
			continue
		}
		if len(in.ExpectedJobs) > 0 && !WorkloadMatchesJobs(w, in.ExpectedJobs) {
			continue
		}
		commitImages := commitMatchingImages(w.Images, in.CommitSHA, in.IncludedCommits)
		if len(commitImages) == 0 {
			continue
		}

		awaitingRestart := workloadAwaitingConfigMapRestart(w, in.ConfigMaps, changed)
		wDone, wFailed, _ := WorkloadRolloutComplete(w)
		wReady := !awaitingRestart && wDone && !wFailed

		for _, img := range commitImages {
			for _, cand := range ImageServiceCandidates(img) {
				if !seenMap[cand] {
					seenMap[cand] = true
					seen = append(seen, cand)
				}
				if !wReady {
					unready[cand] = true
				}
			}
		}
	}

	var ready []string
	for _, cand := range seen {
		if !unready[cand] {
			ready = append(ready, cand)
		}
	}
	return ready
}
