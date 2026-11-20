package flux

import (
	"slices"
	"testing"
	"time"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

func TestPRRolloutJobProgress(t *testing.T) {
	t.Parallel()
	rolloutClock := func(hh, mm, ss int) time.Time {
		return time.Date(2026, time.November, 8, hh, mm, ss, 0, time.UTC)
	}
	withChange := func(w model.WorkloadRollout, changedAt time.Time, annotations map[string]string, configMapRefs ...string) model.WorkloadRollout {
		w.ChangedAt, w.Annotations, w.ConfigMapRefs = changedAt, annotations, configMapRefs
		return w
	}
	deployRollout := func(name, image string, gen, observed int64, replicas, updated, available, total int32, deadline bool) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "Deployment", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, UpdatedReplicas: updated, AvailableReplicas: available, StatusReplicas: total, ProgressDeadlineExceeded: deadline}
	}
	stsRollout := func(name, image string, gen, observed int64, replicas, available int32, updateRev, currentRev string) model.WorkloadRollout {
		return model.WorkloadRollout{Kind: "StatefulSet", Namespace: "default", Name: name, Images: []string{image}, Generation: gen, ObservedGeneration: observed, Replicas: replicas, AvailableReplicas: available, UpdateRevision: updateRev, CurrentRevision: currentRev}
	}

	const commitSHA = "0123abcd"
	const eriImg = "ghcr.io/azylman/aerial-eri:" + commitSHA
	const synapseImg = "ghcr.io/azylman/aerial-synapse:" + commitSHA
	const haroImg = "ghcr.io/azylman/aerial-haro:otherSHA"
	auto := map[string]string{"reloader.stakater.com/auto": "true"}
	since := rolloutClock(10, 0, 0)
	now := rolloutClock(10, 30, 0)

	tests := []struct {
		name        string
		input       PRRolloutInput
		repo        string
		wantReady   []string
		wantPending []string
	}{
		{
			name:  "empty commit sha returns nil",
			input: PRRolloutInput{Workloads: []model.WorkloadRollout{deployRollout("eri", eriImg, 1, 1, 1, 1, 1, 1, false)}},
			repo:  "aerial",
		},
		{
			name:  "empty workloads returns nil",
			input: PRRolloutInput{CommitSHA: commitSHA, Since: since, Now: now},
			repo:  "aerial",
		},
		{
			name: "single workload rolled out",
			input: PRRolloutInput{
				Workloads: []model.WorkloadRollout{deployRollout("eri", eriImg, 1, 1, 1, 1, 1, 1, false)},
				CommitSHA: commitSHA, Since: since, Now: now,
			},
			repo:      "aerial",
			wantReady: []string{"aerial-eri"},
		},
		{
			name: "single workload still rolling out",
			input: PRRolloutInput{
				Workloads: []model.WorkloadRollout{deployRollout("eri", eriImg, 1, 1, 1, 1, 0, 1, false)},
				CommitSHA: commitSHA, Since: since, Now: now,
			},
			repo:        "aerial",
			wantPending: []string{"aerial-eri"},
		},
		{
			name: "single workload failed",
			input: PRRolloutInput{
				Workloads: []model.WorkloadRollout{deployRollout("eri", eriImg, 1, 1, 1, 1, 0, 1, true)},
				CommitSHA: commitSHA, Since: since, Now: now,
			},
			repo:        "aerial",
			wantPending: []string{"aerial-eri"},
		},
		{
			name: "single workload awaiting reloader restart",
			input: PRRolloutInput{
				Workloads:  []model.WorkloadRollout{withChange(deployRollout("eri", eriImg, 1, 1, 1, 1, 1, 1, false), rolloutClock(9, 0, 0), auto, "eri-cm")},
				ConfigMaps: []model.ConfigMapChange{{Namespace: "default", Name: "eri-cm", ChangedAt: rolloutClock(10, 5, 0)}},
				CommitSHA:  commitSHA, Since: since, Now: now,
			},
			repo:        "aerial",
			wantPending: []string{"aerial-eri"},
		},
		{
			name: "two workloads for same job: one ready and one pending marks job pending",
			input: PRRolloutInput{
				Workloads: []model.WorkloadRollout{
					deployRollout("eri", eriImg, 1, 1, 1, 1, 1, 1, false),
					deployRollout("eri-secondary", eriImg, 1, 1, 1, 1, 0, 1, false),
				},
				CommitSHA: commitSHA, Since: since, Now: now,
			},
			repo:        "aerial",
			wantPending: []string{"aerial-eri"},
		},
		{
			name: "two workloads for same job: both ready dedupes and sorts",
			input: PRRolloutInput{
				Workloads: []model.WorkloadRollout{
					deployRollout("eri-secondary", eriImg, 1, 1, 1, 1, 1, 1, false),
					deployRollout("eri", eriImg, 1, 1, 1, 1, 1, 1, false),
				},
				CommitSHA: commitSHA, Since: since, Now: now,
			},
			repo:      "aerial",
			wantReady: []string{"aerial-eri"},
		},
		{
			name: "different jobs: one ready and one pending",
			input: PRRolloutInput{
				Workloads: []model.WorkloadRollout{
					deployRollout("eri", eriImg, 1, 1, 1, 1, 1, 1, false),
					deployRollout("synapse", synapseImg, 1, 1, 1, 1, 0, 1, false),
				},
				CommitSHA: commitSHA, Since: since, Now: now,
			},
			repo:        "aerial",
			wantReady:   []string{"aerial-eri"},
			wantPending: []string{"aerial-synapse"},
		},
		{
			name: "workload with non-matching commitSHA is ignored",
			input: PRRolloutInput{
				Workloads: []model.WorkloadRollout{
					deployRollout("haro", haroImg, 1, 1, 1, 1, 1, 1, false),
					stsRollout("db", "ghcr.io/azylman/aerial-db:"+commitSHA, 1, 1, 1, 1, "rev-1", "rev-1"),
				},
				CommitSHA: commitSHA, Since: since, Now: now,
			},
			repo:      "aerial",
			wantReady: []string{"aerial-db"},
		},
		{
			name: "unresolvable image tag ignored",
			input: PRRolloutInput{
				Workloads: []model.WorkloadRollout{
					deployRollout("none-workload", "none:"+commitSHA, 1, 1, 1, 1, 1, 1, false),
				},
				CommitSHA: commitSHA, Since: since, Now: now,
			},
			repo: "aerial",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotReady, gotPending := PRRolloutJobProgress(tc.input, tc.repo)
			if !slices.Equal(gotReady, tc.wantReady) {
				t.Errorf("PRRolloutJobProgress() ready = %v, want %v", gotReady, tc.wantReady)
			}
			if !slices.Equal(gotPending, tc.wantPending) {
				t.Errorf("PRRolloutJobProgress() pending = %v, want %v", gotPending, tc.wantPending)
			}
		})
	}
}
