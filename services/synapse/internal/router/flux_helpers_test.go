package router

import (
	"time"
)

// deployRollout builds a Deployment rollout state.
func deployRollout(name, image string, gen, observed int64, replicas, updated, available, total int32, deadline bool) workloadRollout {
	return workloadRollout{Kind: "Deployment", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, UpdatedReplicas: updated, AvailableReplicas: available, StatusReplicas: total, ProgressDeadlineExceeded: deadline}
}

// rolloutClock returns 2026-11-08 at hh:mm:ss UTC, the day of the rollout fixtures.
func rolloutClock(hh, mm, ss int) time.Time {
	return time.Date(2026, time.November, 8, hh, mm, ss, 0, time.UTC)
}

// withChange sets when w last changed, its annotations and the ConfigMaps its pod template consumes.
func withChange(w workloadRollout, changedAt time.Time, annotations map[string]string, configMapRefs ...string) workloadRollout {
	w.ChangedAt, w.Annotations, w.ConfigMapRefs = changedAt, annotations, configMapRefs
	return w
}
