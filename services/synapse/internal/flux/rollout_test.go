package flux

import (
	"slices"
	"testing"
	"time"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

func TestPRRolloutJobProgress(t *testing.T) {
	t.Parallel()

	clock := func(hh, mm, ss int) time.Time {
		return time.Date(2026, time.November, 20, hh, mm, ss, 0, time.UTC)
	}
	makeWorkload := func(name, image string, replicas, updated, available int32, deadline bool, changedAt time.Time, annotations map[string]string, cms ...string) model.WorkloadRollout {
		return model.WorkloadRollout{
			Kind:                     "Deployment",
			Namespace:                "default",
			Name:                     name,
			Images:                   []string{image},
			Generation:               1,
			ObservedGeneration:       1,
			Replicas:                 replicas,
			UpdatedReplicas:          updated,
			AvailableReplicas:        available,
			StatusReplicas:           replicas,
			ProgressDeadlineExceeded: deadline,
			ChangedAt:                changedAt,
			Annotations:              annotations,
			ConfigMapRefs:            cms,
		}
	}

	const commitSHA = "sha-999"
	const oldSHA = "sha-888"
	const repo = "azylman/aerial"

	eriImage := "ghcr.io/azylman/aerial-eri:" + commitSHA
	synapseImage := "ghcr.io/azylman/aerial-synapse:" + commitSHA
	oldEriImage := "ghcr.io/azylman/aerial-eri:" + oldSHA
	auto := map[string]string{"reloader.stakater.com/auto": "true"}

	since := clock(10, 0, 0)

	tests := []struct {
		name        string
		workloads   []model.WorkloadRollout
		configMaps  []model.ConfigMapChange
		commitSHA   string
		now         time.Time
		wantReady   []string
		wantPending []string
	}{
		{
			name:      "empty commitSHA returns nil",
			commitSHA: "",
			workloads: []model.WorkloadRollout{
				makeWorkload("eri", eriImage, 1, 1, 1, false, clock(10, 5, 0), nil),
			},
		},
		{
			name:      "no matching images returns nil",
			commitSHA: commitSHA,
			workloads: []model.WorkloadRollout{
				makeWorkload("eri", oldEriImage, 1, 1, 1, false, clock(10, 5, 0), nil),
			},
		},
		{
			name:      "single job ready",
			commitSHA: commitSHA,
			workloads: []model.WorkloadRollout{
				makeWorkload("eri", eriImage, 1, 1, 1, false, clock(10, 5, 0), nil),
			},
			wantReady: []string{"aerial-eri"},
		},
		{
			name:      "single job pending unrolled",
			commitSHA: commitSHA,
			workloads: []model.WorkloadRollout{
				makeWorkload("eri", eriImage, 1, 1, 0, false, clock(10, 5, 0), nil),
			},
			wantPending: []string{"aerial-eri"},
		},
		{
			name:      "single job failed",
			commitSHA: commitSHA,
			workloads: []model.WorkloadRollout{
				makeWorkload("eri", eriImage, 1, 1, 0, true, clock(10, 5, 0), nil),
			},
			wantPending: []string{"aerial-eri"},
		},
		{
			name:      "job with multiple workloads: one ready, one pending -> job is pending",
			commitSHA: commitSHA,
			workloads: []model.WorkloadRollout{
				makeWorkload("eri-suletta", eriImage, 1, 1, 1, false, clock(10, 5, 0), nil),
				makeWorkload("eri-miorine", eriImage, 1, 1, 0, false, clock(10, 5, 0), nil),
			},
			wantPending: []string{"aerial-eri"},
		},
		{
			name:      "two different jobs: one ready, one pending",
			commitSHA: commitSHA,
			workloads: []model.WorkloadRollout{
				makeWorkload("eri", eriImage, 1, 1, 1, false, clock(10, 5, 0), nil),
				makeWorkload("synapse", synapseImage, 1, 1, 0, false, clock(10, 5, 0), nil),
			},
			wantReady:   []string{"aerial-eri"},
			wantPending: []string{"aerial-synapse"},
		},
		{
			name:      "reloader awaiting restart marks job pending",
			commitSHA: commitSHA,
			workloads: []model.WorkloadRollout{
				makeWorkload("eri", eriImage, 1, 1, 1, false, clock(9, 0, 0), auto, "eri-cm"),
			},
			configMaps: []model.ConfigMapChange{
				{Namespace: "default", Name: "eri-cm", ChangedAt: clock(10, 1, 0)},
			},
			now:         clock(10, 3, 0),
			wantPending: []string{"aerial-eri"},
		},
		{
			name:      "reloader restarted and ready marks job ready",
			commitSHA: commitSHA,
			workloads: []model.WorkloadRollout{
				makeWorkload("eri", eriImage, 1, 1, 1, false, clock(10, 2, 0), auto, "eri-cm"),
			},
			configMaps: []model.ConfigMapChange{
				{Namespace: "default", Name: "eri-cm", ChangedAt: clock(10, 1, 0)},
			},
			now:       clock(10, 3, 0),
			wantReady: []string{"aerial-eri"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotReady, gotPending := PRRolloutJobProgress(PRRolloutInput{
				Workloads:  tc.workloads,
				ConfigMaps: tc.configMaps,
				CommitSHA:  tc.commitSHA,
				Since:      since,
				Now:        tc.now,
			}, repo)

			if !slices.Equal(gotReady, tc.wantReady) {
				t.Errorf("PRRolloutJobProgress() ready = %v, want %v", gotReady, tc.wantReady)
			}
			if !slices.Equal(gotPending, tc.wantPending) {
				t.Errorf("PRRolloutJobProgress() pending = %v, want %v", gotPending, tc.wantPending)
			}
		})
	}
}
