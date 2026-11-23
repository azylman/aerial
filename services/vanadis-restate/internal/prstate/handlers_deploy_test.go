package prstate

// DeploySuccessDue tests: when a watcher-tracked rollout may publish
// deploy_success.

import (
	"testing"
)

func TestPRStateDeploySuccessDue(t *testing.T) {
	t.Parallel()
	reqs := map[string]KustomizationRequest{"apps": {Namespace: "flux-system"}, "core": {Namespace: "flux-system"}}
	tests := []struct {
		name     string
		requests map[string]KustomizationRequest
		status   string
		deployed []string
		pending  bool
		job      string
		want     bool
	}{
		{name: "untracked rollout is always due", status: RegistryStatusMerged, job: "eri", want: true},
		{name: "first of two kustomizations is not due", requests: reqs, status: RegistryStatusDeploying, job: "apps"},
		{name: "last outstanding kustomization is due", requests: reqs, status: RegistryStatusDeploying, deployed: []string{"apps"}, job: "core", want: true},
		{name: "failed rollout completing is due", requests: reqs, status: RegistryStatusDeployFailed, deployed: []string{"core"}, job: "apps", want: true},
		{name: "flipped row with pending success is due", requests: reqs, status: RegistryStatusDeployed, deployed: []string{"apps", "core"}, pending: true, job: "core", want: true},
		{name: "acknowledged deployed row is not due again", requests: reqs, status: RegistryStatusDeployed, deployed: []string{"apps", "core"}, job: "core"},
		{name: "foreign job never completes the set", requests: reqs, status: RegistryStatusDeploying, deployed: []string{"apps"}, job: "eri"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 7)
			s.KustomizationRequests, s.Status, s.DeployedKustomizations, s.DeploySuccessPending = tc.requests, tc.status, tc.deployed, tc.pending
			if got := s.DeploySuccessDue(tc.job); got != tc.want {
				t.Fatalf("DeploySuccessDue(%q) = %v, want %v", tc.job, got, tc.want)
			}
		})
	}
}
