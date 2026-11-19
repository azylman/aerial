package router

import (
	"context"
	"testing"
)

func TestWorkflowRunCIFailureDeduplication(t *testing.T) {
	t.Parallel()

	repo := "azylman/aerial"
	prNum := 42
	headSHA := "1111222233334444555566667777888899990000"

	t.Run("recordNonGateCheckFailureDetailed detects active check runs", func(t *testing.T) {
		t.Parallel()
		srv := NewRouterServer(fixedConfig(Config{}), nil)

		res := &ResolvedGitHubEvent{
			Event:    ghEventWorkflowRun,
			Repo:     repo,
			PRNumber: prNum,
			HeadSHA:  headSHA,
		}

		// Initially, no active failed checks.
		emit, wasActive, hadPrior := srv.recordNonGateCheckFailureDetailed(res)
		if emit || wasActive || hadPrior {
			t.Fatalf("expected all false, got emit=%v, wasActive=%v, hadPrior=%v", emit, wasActive, hadPrior)
		}

		// Record a check run failure.
		srv.ghState.RecordFailedCheckRunDetailed(repo, prNum, headSHA, "unit-tests")

		// Now workflow_run arrives for same repo/PR/SHA.
		emit, wasActive, hadPrior = srv.recordNonGateCheckFailureDetailed(res)
		if emit {
			t.Errorf("emitAdditional = true, want false")
		}
		if !wasActive {
			t.Errorf("wasActive = false, want true")
		}
		if !hadPrior {
			t.Errorf("hadPriorEverFailed = false, want true")
		}
	})

	t.Run("handleCIFailureByPRNumber treats workflow_run as active when check_run failed", func(t *testing.T) {
		t.Parallel()
		mockReg := &mockPRRegistry{
			ciFailedTargetID: "thread-123",
			ciFailedUpdated:  false, // updated = false simulates PR is already in ci_failed state
			buildLifecycle: PRBuildLifecycle{
				Status:  prCIFailed,
				HeadSHA: headSHA,
			},
		}

		srv := NewRouterServer(fixedConfig(Config{}), nil)
		srv.registry = mockReg

		res := &ResolvedGitHubEvent{
			Event:    ghEventWorkflowRun,
			Repo:     repo,
			PRNumber: prNum,
			HeadSHA:  headSHA,
			TargetID: "thread-123",
		}

		// Record an active failed check run in ghState
		srv.ghState.RecordFailedCheckRunDetailed(repo, prNum, headSHA, "build")

		// With active failed check_run, workflow_run promotes wasActive to true and suppresses duplicate completion
		completedWithActive, err := srv.handleCIFailureByPRNumber(context.Background(), res, "workflow failed", "https://ci", false, false)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if completedWithActive {
			t.Errorf("completedWithActive = true, want false (duplicate completion must be suppressed)")
		}
	})
}

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
