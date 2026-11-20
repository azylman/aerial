package router

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func makeTestWorkload(name, image string, replicas, updated, available int32, deadline bool) workloadRollout {
	return workloadRollout{
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
	}
}

type gateKustomizationJobProgressCase struct {
	name               string
	pr                 DeployingPR
	obs                kustomizationObservation
	state              kustomizationRolloutState
	kustWorkloads      map[string][]workloadRollout
	kustWorkloadsErr   error
	recordDeployedRes  bool
	recordDeployedErr  error
	busErr             error
	wantRecordCalls    int
	wantRecordJobs     []string
	wantPublishedEvent string
	wantErr            bool
}

func gateKustomizationJobProgressCases() []gateKustomizationJobProgressCase {
	const commitSHA = "sha-42"
	eriReady := makeTestWorkload("eri", "ghcr.io/azylman/aerial-eri:"+commitSHA, 1, 1, 1, false)
	synapseReady := makeTestWorkload("synapse", "ghcr.io/azylman/aerial-synapse:"+commitSHA, 1, 1, 1, false)
	synapsePending := makeTestWorkload("synapse", "ghcr.io/azylman/aerial-synapse:"+commitSHA, 1, 1, 0, false)
	haroPending := makeTestWorkload("haro", "ghcr.io/azylman/aerial-haro:"+commitSHA, 1, 1, 0, false)
	eriPending := makeTestWorkload("eri-miorine", "ghcr.io/azylman/aerial-eri:"+commitSHA, 1, 1, 0, false)
	eriDoneCfg := makeTestWorkload("eri-miorine", "ghcr.io/azylman/aerial-eri:"+commitSHA, 1, 1, 1, false)

	return []gateKustomizationJobProgressCase{
		{
			name: "single kustomization ready job records deployed and publishes rollout_updated when not done",
			pr:   trackedPR(42, map[string]string{"aerial-core": reqT1}),
			obs:  kobs("aerial-core", reqT1, reqT1, false, readyCond("True", "ReconciliationSucceeded", "")),
			state: kustomizationRolloutState{
				Workloads: []workloadRollout{eriReady, synapsePending},
			},
			recordDeployedRes:  true,
			wantRecordCalls:    1,
			wantRecordJobs:     []string{"aerial-eri"},
			wantPublishedEvent: "rollout_updated",
		},
		{
			name: "multi-kustomization filters out job pending in other kustomization",
			pr:   trackedPR(42, map[string]string{"aerial-core": reqT1, "aerial-config": reqT1}),
			obs:  kobs("aerial-core", reqT1, reqT1, false, readyCond("True", "ReconciliationSucceeded", "")),
			state: kustomizationRolloutState{
				Workloads: []workloadRollout{eriReady, synapseReady, haroPending},
			},
			kustWorkloads: map[string][]workloadRollout{
				"aerial-config": {eriPending},
			},
			recordDeployedRes:  true,
			wantRecordCalls:    1,
			wantRecordJobs:     []string{"aerial-synapse"},
			wantPublishedEvent: "rollout_updated",
		},
		{
			name: "multi-kustomization allows job when ready in all kustomizations",
			pr:   trackedPR(42, map[string]string{"aerial-core": reqT1, "aerial-config": reqT1}),
			obs:  kobs("aerial-core", reqT1, reqT1, false, readyCond("True", "ReconciliationSucceeded", "")),
			state: kustomizationRolloutState{
				Workloads: []workloadRollout{eriReady, synapsePending},
			},
			kustWorkloads: map[string][]workloadRollout{
				"aerial-config": {eriDoneCfg},
			},
			recordDeployedRes:  true,
			wantRecordCalls:    1,
			wantRecordJobs:     []string{"aerial-eri"},
			wantPublishedEvent: "rollout_updated",
		},
		{
			name: "failed rollout does not record deployed jobs",
			pr:   trackedPR(42, map[string]string{"aerial-core": reqT1}),
			obs:  kobs("aerial-core", reqT1, reqT1, false, readyCond("True", "ReconciliationSucceeded", "")),
			state: kustomizationRolloutState{
				Workloads: []workloadRollout{makeTestWorkload("eri", "ghcr.io/azylman/aerial-eri:"+commitSHA, 1, 1, 0, true)},
			},
			wantRecordCalls: 0,
		},
		{
			name: "record deployed jobs db error returns error",
			pr:   trackedPR(42, map[string]string{"aerial-core": reqT1}),
			obs:  kobs("aerial-core", reqT1, reqT1, false, readyCond("True", "ReconciliationSucceeded", "")),
			state: kustomizationRolloutState{
				Workloads: []workloadRollout{eriReady, synapsePending},
			},
			recordDeployedErr: errors.New("db connection lost"),
			wantRecordCalls:   1,
			wantRecordJobs:    []string{"aerial-eri"},
			wantErr:           true,
		},
		{
			name: "pending other kustomization error surfaces",
			pr:   trackedPR(42, map[string]string{"aerial-core": reqT1, "aerial-config": reqT1}),
			obs:  kobs("aerial-core", reqT1, reqT1, false, readyCond("True", "ReconciliationSucceeded", "")),
			state: kustomizationRolloutState{
				Workloads: []workloadRollout{eriReady, synapsePending},
			},
			kustWorkloadsErr: errors.New("k8s api failed"),
			wantErr:          true,
		},
		{
			name: "publish event error surfaces when updated",
			pr:   trackedPR(42, map[string]string{"aerial-core": reqT1}),
			obs:  kobs("aerial-core", reqT1, reqT1, false, readyCond("True", "ReconciliationSucceeded", "")),
			state: kustomizationRolloutState{
				Workloads: []workloadRollout{eriReady, synapsePending},
			},
			recordDeployedRes: true,
			busErr:            errors.New("nats down"),
			wantRecordCalls:   1,
			wantRecordJobs:    []string{"aerial-eri"},
			wantErr:           true,
		},
	}
}

func TestGateKustomizationRequest_JobProgress(t *testing.T) {
	t.Parallel()

	for _, tc := range gateKustomizationJobProgressCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			reg := &mockPRRegistry{
				recordDeployedRes: tc.recordDeployedRes,
				recordDeployedErr: tc.recordDeployedErr,
			}
			disp := &mockOutboundDispatcher{
				kustomizationWorkloads: tc.kustWorkloads,
				workloadsErr:           tc.kustWorkloadsErr,
			}
			bus := &mockEventBus{
				err: tc.busErr,
			}

			_, srv := newRolloutRouter(t, reg, disp, nil)
			srv.SetEventBus(bus)

			err := srv.gateKustomizationRequest(ctx, tc.pr, tc.obs, tc.state)
			if (err != nil) != tc.wantErr {
				t.Fatalf("gateKustomizationRequest err = %v, wantErr = %v", err, tc.wantErr)
			}

			if len(reg.recordDeployedCalls) != tc.wantRecordCalls {
				t.Fatalf("recordDeployedCalls = %d, want %d", len(reg.recordDeployedCalls), tc.wantRecordCalls)
			}

			if tc.wantRecordCalls > 0 {
				gotJobs := reg.recordDeployedCalls[0].jobs
				if !slices.Equal(gotJobs, tc.wantRecordJobs) {
					t.Errorf("recorded jobs = %v, want %v", gotJobs, tc.wantRecordJobs)
				}
			}

			if tc.wantPublishedEvent != "" {
				bus.mu.Lock()
				found := false
				for _, evt := range bus.publishedPREvents {
					if evt.Event == tc.wantPublishedEvent {
						found = true
						break
					}
				}
				bus.mu.Unlock()
				if !found {
					t.Errorf("publishedPREvents does not contain %s", tc.wantPublishedEvent)
				}
			}
		})
	}
}

func TestPendingOtherKustomizationJobs(t *testing.T) {
	t.Parallel()

	const commitSHA = "sha-42"
	const oldSHA = "sha-prev"

	eriCommitReady := makeTestWorkload("eri-miorine", "ghcr.io/azylman/aerial-eri:"+commitSHA, 1, 1, 1, false)
	eriCommitPending := makeTestWorkload("eri-miorine", "ghcr.io/azylman/aerial-eri:"+commitSHA, 1, 1, 0, false)
	eriOld := makeTestWorkload("eri-miorine", "ghcr.io/azylman/aerial-eri:"+oldSHA, 1, 1, 1, false)

	tests := []struct {
		name          string
		pr            DeployingPR
		currentRef    fluxKustomizationRef
		kustWorkloads map[string][]workloadRollout
		workloadsErr  error
		wantPending   []string
		wantErr       bool
	}{
		{
			name:        "empty commitSHA returns nil",
			pr:          DeployingPR{Repo: "azylman/aerial", PRNumber: 42},
			currentRef:  fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"},
			wantPending: nil,
		},
		{
			name:        "single request returns nil",
			pr:          trackedPR(42, map[string]string{"aerial-core": reqT1}),
			currentRef:  fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"},
			wantPending: nil,
		},
		{
			name:        "other request already deployed is skipped",
			pr:          trackedPR(42, map[string]string{"aerial-core": reqT1, "aerial-config": reqT1}, "aerial-config"),
			currentRef:  fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"},
			wantPending: nil,
		},
		{
			name:       "other request has workload with old SHA -> marks job pending",
			pr:         trackedPR(42, map[string]string{"aerial-core": reqT1, "aerial-config": reqT1}),
			currentRef: fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"},
			kustWorkloads: map[string][]workloadRollout{
				"aerial-config": {eriOld},
			},
			wantPending: []string{"aerial-eri"},
		},
		{
			name:       "other request has workload with commitSHA not ready -> marks job pending",
			pr:         trackedPR(42, map[string]string{"aerial-core": reqT1, "aerial-config": reqT1}),
			currentRef: fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"},
			kustWorkloads: map[string][]workloadRollout{
				"aerial-config": {eriCommitPending},
			},
			wantPending: []string{"aerial-eri"},
		},
		{
			name:       "other request has workload with commitSHA ready -> not marked pending",
			pr:         trackedPR(42, map[string]string{"aerial-core": reqT1, "aerial-config": reqT1}),
			currentRef: fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"},
			kustWorkloads: map[string][]workloadRollout{
				"aerial-config": {eriCommitReady},
			},
			wantPending: nil,
		},
		{
			name:         "dispatcher error returns error",
			pr:           trackedPR(42, map[string]string{"aerial-core": reqT1, "aerial-config": reqT1}),
			currentRef:   fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"},
			workloadsErr: errors.New("k8s api timeout"),
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			disp := &mockOutboundDispatcher{
				kustomizationWorkloads: tc.kustWorkloads,
				workloadsErr:           tc.workloadsErr,
			}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetDispatcher(disp)

			got, err := srv.pendingOtherKustomizationJobs(ctx, tc.pr, tc.currentRef)
			if (err != nil) != tc.wantErr {
				t.Fatalf("pendingOtherKustomizationJobs err = %v, wantErr = %v", err, tc.wantErr)
			}

			for _, p := range tc.wantPending {
				if !got[p] {
					t.Errorf("pendingOtherKustomizationJobs expected %s in pending, got %v", p, got)
				}
			}
			if len(tc.wantPending) == 0 && len(got) > 0 {
				t.Errorf("pendingOtherKustomizationJobs expected empty, got %v", got)
			}
		})
	}
}

func TestRouterServer_Accessors(t *testing.T) {
	t.Parallel()
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	reg := &mockPRRegistry{}
	disp := &mockOutboundDispatcher{}
	srv.SetRegistry(reg)
	srv.SetDispatcher(disp)
	if srv.Registry() != reg {
		t.Errorf("Registry() mismatch")
	}
	if srv.Dispatcher() != disp {
		t.Errorf("Dispatcher() mismatch")
	}
	var processed string
	srv.SetOnProcessed(func(evt, delivery string) {
		processed = evt + ":" + delivery
	})
	if srv.onProcessed == nil {
		t.Errorf("SetOnProcessed failed")
	}
	srv.onProcessed("evt", "del")
	if processed != "evt:del" {
		t.Errorf("onProcessed call failed")
	}
}
