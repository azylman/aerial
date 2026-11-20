package router

import (
	"testing"
)

func TestDeployEventIdempotencyKey(t *testing.T) {
	t.Parallel()

	// Nonce uniqueness across empty events
	k1 := deployEventIdempotencyKey(DeployEvent{}, "")
	k2 := deployEventIdempotencyKey(DeployEvent{}, "")
	if k1 == k2 {
		t.Errorf("expected distinct random nonces for empty deploy events, got collision %q", k1)
	}

	base := DeployEvent{
		Event:     "deploy_success",
		Repo:      "azylman/aerial",
		JobName:   "eri",
		CommitSHA: "abcdef123456",
	}

	// Determinism: same input yields identical key
	baseKey := deployEventIdempotencyKey(base, "flux")
	if deployEventIdempotencyKey(base, "flux") != baseKey {
		t.Errorf("expected deterministic idempotency key for identical inputs")
	}

	// Field distinctness: altering any field or prefix must yield a different key
	cases := []struct {
		name string
		evt  DeployEvent
		pfx  string
	}{
		{name: "different prefix", evt: base, pfx: "other"},
		{name: "empty prefix", evt: base, pfx: ""},
		{name: "different event", evt: DeployEvent{Event: "deploy_failed", Repo: base.Repo, JobName: base.JobName, CommitSHA: base.CommitSHA}, pfx: "flux"},
		{name: "different repo", evt: DeployEvent{Event: base.Event, Repo: "azylman/mirrormere", JobName: base.JobName, CommitSHA: base.CommitSHA}, pfx: "flux"},
		{name: "different job", evt: DeployEvent{Event: base.Event, Repo: base.Repo, JobName: "synapse", CommitSHA: base.CommitSHA}, pfx: "flux"},
		{name: "different sha", evt: DeployEvent{Event: base.Event, Repo: base.Repo, JobName: base.JobName, CommitSHA: "fedcba654321"}, pfx: "flux"},
	}

	for _, tc := range cases {
		if deployEventIdempotencyKey(tc.evt, tc.pfx) == baseKey {
			t.Errorf("case %q collided with base idempotency key %q", tc.name, baseKey)
		}
	}
}
