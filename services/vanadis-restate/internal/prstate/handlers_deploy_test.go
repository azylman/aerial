package prstate

// Deploy-dedup and deploy-failure tests: BOTH WasDispatched keys, the
// deploy-failure deadline, and readiness retry without minting generations.

import (
	"testing"
)

func deployState(repo string, pr int, head string, gen uint64) *PRState {
	s := NewPRState(repo, pr)
	s.HeadSHA = head
	s.GateGeneration = gen
	return s
}

func seedFresh() *PRState { return deployState("azylman/aerial", 7, "aaa111", 2) }

func seedPRSlot() *PRState {
	s := seedFresh()
	s.RecordDeployedPR()
	return s
}

func seedSHAJob(job, sha string) *PRState {
	s := seedFresh()
	s.RecordDeployedSHAJob(job, sha)
	return s
}

func TestWasDispatchedDeployKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		state    *PRState
		repo     string
		job      string
		sha      string
		prNumber int
		want     bool
	}{
		{name: "empty state never dispatched", state: seedFresh(), repo: "azylman/aerial", job: "flux-a", sha: "aaa111", prNumber: 7, want: false},
		{name: "pr slot alone reports dispatched", state: seedPRSlot(), repo: "azylman/aerial", job: "other-job", sha: "other-sha", prNumber: 7, want: true},
		{name: "job plus sha alone reports dispatched", state: seedSHAJob("flux-a", "aaa111"), repo: "azylman/aerial", job: "flux-a", sha: "aaa111", prNumber: 99, want: true},
		{name: "different job not dispatched", state: seedSHAJob("flux-a", "aaa111"), repo: "azylman/aerial", job: "flux-b", sha: "aaa111", prNumber: 99, want: false},
		{name: "empty job never dispatched", state: seedFresh(), repo: "azylman/aerial", job: "", sha: "aaa111", prNumber: 99, want: false},
		{name: "zero sha never dispatched", state: seedFresh(), repo: "azylman/aerial", job: "flux-a", sha: "0000", prNumber: 99, want: false},
		{name: "nil dedup map never dispatched", state: &PRState{Repo: "azylman/aerial", PRNumber: 7}, repo: "azylman/aerial", job: "flux-a", sha: "aaa111", prNumber: 99, want: false},
		{name: "nil state not dispatched", state: nil, repo: "azylman/aerial", job: "flux-a", sha: "aaa111", prNumber: 7, want: false},
		{name: "unscoped repo matches owner form", state: seedSHAJob("flux-a", "aaa111"), repo: "azylman/aerial", job: "flux-a", sha: "aaa111", prNumber: 99, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := WasDispatched(tc.state, tc.repo, tc.job, tc.sha, tc.prNumber); got != tc.want {
				t.Fatalf("WasDispatched = %v, want %v", got, tc.want)
			}
		})
	}
}

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
