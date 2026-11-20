package flux

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

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
		targetNS := cmp.Or(workloads[i].Namespace, workloads[i].Name)
		for _, other := range workloads {
			if other.Kind == kindStatefulSet && other.Namespace == targetNS && (other.Name == workloads[i].Name || other.Name == "restate") {
				workloads[i].ManagedWorkloads = append(workloads[i].ManagedWorkloads, other)
			}
		}
	}
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

// WorkloadRolloutComplete reports whether the workload controller has observed its latest spec and
// every desired pod runs the current template and is available, with no old pods left.
func WorkloadRolloutComplete(w model.WorkloadRollout) (done, failed bool, reason string) {
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
	Workloads  []model.WorkloadRollout
	ConfigMaps []model.ConfigMapChange
	CommitSHA  string
	Since, Now time.Time
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
		wStr := WorkloadString(w)
		gated := changed(w.ChangedAt) || (in.CommitSHA != "" && slices.ContainsFunc(w.Images, func(image string) bool { return ImageTag(image) == in.CommitSHA }))
		var awaitingRestart []string
		for _, cm := range in.ConfigMaps {
			if cm.Namespace != w.Namespace || !changed(cm.ChangedAt) || !ReloadsOn(w, cm.Name) {
				continue
			}
			gated = true
			if !w.ChangedAt.Before(cm.ChangedAt) {
				continue
			}
			if in.Now.Sub(cm.ChangedAt) > reloaderRestartTimeout {
				return false, true, fmt.Sprintf("%s: not restarted by Reloader within %s of ConfigMap %s/%s changing", wStr, reloaderRestartTimeout, cm.Namespace, cm.Name)
			}
			awaitingRestart = append(awaitingRestart, cm.Namespace+"/"+cm.Name)
		}
		if !gated {
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

func workloadMatchingJobs(w model.WorkloadRollout, repo, commitSHA string) []string {
	var jobs []string
	for _, img := range w.Images {
		if ImageTag(img) == commitSHA {
			if _, _, job := model.ResolveImageTag(repo, img); job != "" {
				jobs = append(jobs, job)
			}
		}
	}
	return jobs
}

func workloadReloaderRestartPending(w model.WorkloadRollout, configMaps []model.ConfigMapChange, cutoff time.Time) bool {
	changed := func(at time.Time) bool { return !at.IsZero() && !at.Before(cutoff) }
	for _, cm := range configMaps {
		if cm.Namespace == w.Namespace && changed(cm.ChangedAt) && ReloadsOn(w, cm.Name) {
			if w.ChangedAt.Before(cm.ChangedAt) {
				return true
			}
		}
	}
	return false
}

// PRRolloutJobProgress reports granular per-job rollout readiness for workloads matching in.CommitSHA.
func PRRolloutJobProgress(in PRRolloutInput, repo string) (readyJobs, pendingJobs []string) {
	if in.CommitSHA == "" {
		return nil, nil
	}
	LinkManagedWorkloads(in.Workloads)
	cutoff := in.Since.Add(-rolloutChangeSkew)

	seenReady := make(map[string]bool)
	pending := make(map[string]bool)

	for _, w := range in.Workloads {
		matchingJobs := workloadMatchingJobs(w, repo, in.CommitSHA)
		if len(matchingJobs) == 0 {
			continue
		}

		wDone, wFailed, _ := WorkloadRolloutComplete(w)
		notDone := !wDone || wFailed || workloadReloaderRestartPending(w, in.ConfigMaps, cutoff)

		for _, job := range matchingJobs {
			if notDone {
				pending[job] = true
			} else {
				seenReady[job] = true
			}
		}
	}

	for job := range pending {
		pendingJobs = append(pendingJobs, job)
	}
	slices.Sort(pendingJobs)

	for job := range seenReady {
		if !pending[job] {
			readyJobs = append(readyJobs, job)
		}
	}
	slices.Sort(readyJobs)

	return readyJobs, pendingJobs
}
