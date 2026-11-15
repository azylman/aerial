package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestPRLifecycleEvents_Publish(t *testing.T) {
	t.Parallel()

	t.Run("publishPRLifecycleEvent publishes to aerial.pr.<event> with correct fields", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		srv := &RouterServer{bus: bus}

		ev := pubsub.PRLifecycleEvent{
			Event:    "opened",
			Repo:     "azylman/aerial",
			PRNumber: 42,
			Branch:   "feat/something",
			HeadSHA:  "abcdef123456",
			Status:   "open",
		}
		err := srv.publishPRLifecycleEvent(context.Background(), ev)
		if err != nil {
			t.Fatalf("publishPRLifecycleEvent() err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 {
			t.Fatalf("publishedPREvents len = %d, want 1", len(bus.publishedPREvents))
		}
		got := bus.publishedPREvents[0]
		if got.Event != "opened" || got.Repo != "azylman/aerial" || got.PRNumber != 42 {
			t.Errorf("published event mismatch: %+v", got)
		}
		if got.OccurredAt.IsZero() {
			t.Errorf("expected non-zero OccurredAt")
		}
	})

	t.Run("CI failure publishes ci_failed and suppresses duplicate prompt when bus is present and PR is Vanadis-owned", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678", suppressVanadisRes: true}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		res := &ResolvedGitHubEvent{
			Repo:      "azylman/aerial",
			PRNumber:  42,
			HeadSHA:   "sha-ci-fail",
			TargetID:  "123456789012345678",
			CheckName: "build",
		}
		err := srv.completeCIFailureTransition(context.Background(), res, "compile error", "https://github.com/check/1")
		if err != nil {
			t.Fatalf("completeCIFailureTransition() err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(bus.publishedPREvents))
		}
		if bus.publishedPREvents[0].Event != "ci_failed" {
			t.Errorf("expected event ci_failed, got %s", bus.publishedPREvents[0].Event)
		}
		if len(disp.PromptCalls()) != 0 || len(disp.DispatchCalls()) != 0 {
			t.Errorf("expected prompt/dispatch to be suppressed when bus is active and PR is Vanadis-owned, got prompt=%d dispatch=%d", len(disp.PromptCalls()), len(disp.DispatchCalls()))
		}
	})

	t.Run("CI failure dispatches fallback prompt when publish fails", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{err: errors.New("nats down")}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678", suppressVanadisRes: true}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		res := &ResolvedGitHubEvent{
			Repo:      "azylman/aerial",
			PRNumber:  42,
			HeadSHA:   "sha-ci-fail",
			TargetID:  "123456789012345678",
			CheckName: "build",
		}
		err := srv.completeCIFailureTransition(context.Background(), res, "compile error", "https://github.com/check/1")
		if err != nil {
			t.Fatalf("completeCIFailureTransition() err = %v", err)
		}
		if len(disp.DispatchCalls()) != 1 {
			t.Errorf("expected background dispatch to be dispatched as fallback when publish fails, got %d calls", len(disp.DispatchCalls()))
		}
	})

	t.Run("CI failure dispatches fallback prompt when PR is not Vanadis-owned", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678", suppressVanadisRes: false}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		res := &ResolvedGitHubEvent{
			Repo:      "azylman/aerial",
			PRNumber:  42,
			HeadSHA:   "sha-ci-fail",
			TargetID:  "123456789012345678",
			CheckName: "build",
		}
		err := srv.completeCIFailureTransition(context.Background(), res, "compile error", "https://github.com/check/1")
		if err != nil {
			t.Fatalf("completeCIFailureTransition() err = %v", err)
		}
		if len(disp.DispatchCalls()) != 1 {
			t.Errorf("expected background dispatch to be dispatched when PR is not Vanadis-owned, got %d calls", len(disp.DispatchCalls()))
		}
	})

	t.Run("transitionRegisteredConflict publishes conflict", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{conflictTargetID: "123456789012345678", conflictUpdated: true}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		err := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", 42, "feat/branch", "sha-conflict")
		if err != nil {
			t.Fatalf("transitionRegisteredConflict() err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(bus.publishedPREvents))
		}
		if bus.publishedPREvents[0].Event != "conflict" {
			t.Errorf("expected event conflict, got %s", bus.publishedPREvents[0].Event)
		}
	})

	t.Run("dispatchConflictPrompt suppresses conflict prompt when bus is active and PR is Vanadis-owned", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678", suppressVanadisRes: true}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		err := srv.dispatchConflictPrompt(context.Background(), "123456789012345678", "azylman/aerial", "feat/branch", "sha-conflict", 42)
		if err != nil {
			t.Fatalf("dispatchConflictPrompt() err = %v", err)
		}
		if len(disp.PromptCalls()) != 0 || len(disp.DispatchCalls()) != 0 {
			t.Errorf("expected conflict prompt to be suppressed when bus is active and PR is Vanadis-owned, got prompt=%d dispatch=%d", len(disp.PromptCalls()), len(disp.DispatchCalls()))
		}
	})

	t.Run("dispatchConflictPrompt dispatches prompt when bus is active and PR is not Vanadis-owned", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678", suppressVanadisRes: false}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		err := srv.dispatchConflictPrompt(context.Background(), "123456789012345678", "azylman/aerial", "feat/branch", "sha-conflict", 42)
		if err != nil {
			t.Fatalf("dispatchConflictPrompt() err = %v", err)
		}
		if len(disp.DispatchCalls()) != 1 {
			t.Errorf("expected conflict prompt to be dispatched when PR is not Vanadis-owned, got %d calls", len(disp.DispatchCalls()))
		}
	})
}

func TestPRLifecycleEvents_GitHubEvents(t *testing.T) {
	t.Parallel()

	t.Run("opened event publishes to bus", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		srv := &RouterServer{
			bus:    bus,
			cfgSrc: fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}
		res := &ResolvedGitHubEvent{}
		body := []byte(`{"action":"opened","number":42,"pull_request":{"number":42,"title":"test pr","head":{"ref":"feat/test","sha":"head123"},"user":{"login":"tester"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"tester"}}`)
		_, err := srv.processGitHubPullRequestEvent(context.Background(), "del-open", body, res)
		if err != nil {
			t.Fatalf("processGitHubPullRequestEvent() err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "opened" {
			t.Fatalf("expected opened event, got %v", bus.publishedPREvents)
		}
		ev := bus.publishedPREvents[0]
		if ev.PRNumber != 42 || ev.Title != "test pr" || ev.HeadSHA != "head123" {
			t.Errorf("unexpected opened event fields: %+v", ev)
		}
	})

	t.Run("synchronize event publishes to bus", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678"}
		srv := &RouterServer{
			bus:      bus,
			registry: reg,
			cfgSrc:   fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}
		res := &ResolvedGitHubEvent{
			Repo:     "azylman/aerial",
			PRNumber: 42,
			Branch:   "feat/test",
			HeadSHA:  "newhead456",
			Action:   "synchronize",
		}
		pr := GitHubPullRequest{Title: "synced pr"}
		err := srv.handlePullRequestRegistryAction(context.Background(), res, pr)
		if err != nil {
			t.Fatalf("handlePullRequestRegistryAction() err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "synchronize" {
			t.Fatalf("expected synchronize event, got %v", bus.publishedPREvents)
		}
		if bus.publishedPREvents[0].HeadSHA != "newhead456" {
			t.Errorf("unexpected HeadSHA: %q", bus.publishedPREvents[0].HeadSHA)
		}
	})

	t.Run("merged event publishes to bus", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678"}
		srv := &RouterServer{
			bus:      bus,
			registry: reg,
			cfgSrc:   fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}
		res := &ResolvedGitHubEvent{
			Repo:     "azylman/aerial",
			PRNumber: 42,
			Branch:   "feat/test",
			HeadSHA:  "head123",
			MergeSHA: "mergesha789",
		}
		err := srv.handlePRClosedAction(context.Background(), res, true)
		if err != nil {
			t.Fatalf("handlePRClosedAction(merged=true) err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "merged" {
			t.Fatalf("expected merged event, got %v", bus.publishedPREvents)
		}
		if bus.publishedPREvents[0].MergeSHA != "mergesha789" {
			t.Errorf("unexpected MergeSHA: %q", bus.publishedPREvents[0].MergeSHA)
		}
	})

	t.Run("closed_unmerged event publishes to bus", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678"}
		srv := &RouterServer{
			bus:      bus,
			registry: reg,
			cfgSrc:   fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}
		res := &ResolvedGitHubEvent{
			Repo:     "azylman/aerial",
			PRNumber: 42,
			Branch:   "feat/test",
			HeadSHA:  "head123",
		}
		err := srv.handlePRClosedAction(context.Background(), res, false)
		if err != nil {
			t.Fatalf("handlePRClosedAction(merged=false) err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "closed_unmerged" {
			t.Fatalf("expected closed_unmerged event, got %v", bus.publishedPREvents)
		}
		if bus.publishedPREvents[0].Status != "closed_unmerged" {
			t.Errorf("unexpected status: %q", bus.publishedPREvents[0].Status)
		}
	})

	t.Run("check_updated event publishes to bus", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		reg := &mockPRRegistry{recordCheckUpdated: true}
		srv := &RouterServer{
			bus:      bus,
			registry: reg,
			cfgSrc:   fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}
		res := &ResolvedGitHubEvent{
			Repo:       "azylman/aerial",
			PRNumber:   42,
			Branch:     "feat/test",
			HeadSHA:    "head123",
			CheckName:  "Unit Tests",
			Status:     "completed",
			Conclusion: "success",
		}
		srv.recordCheckRunStatus(context.Background(), res)
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "check_updated" {
			t.Fatalf("expected check_updated event, got %v", bus.publishedPREvents)
		}
		ev := bus.publishedPREvents[0]
		if ev.CheckName != "Unit Tests" || ev.CheckState != "passed" {
			t.Errorf("unexpected check event fields: %+v", ev)
		}
	})
}

func TestPRLifecycleEvents_DeployAndKustomization(t *testing.T) {
	t.Parallel()

	t.Run("Deploy failed publishes deploy_failed when bus is present", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{deployFailedTargetID: "123456789012345678", deployFailedUpdated: true}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		evt := DeployEvent{
			Event:     "deploy_failed",
			Repo:      "azylman/aerial",
			PRNumber:  42,
			JobName:   "eri",
			CommitSHA: "sha-deploy-fail",
			TargetID:  "123456789012345678",
			Details:   "crashloopbackoff",
			Timestamp: time.Now().UTC(),
		}
		err := srv.handleDeployFailedOrRollback(context.Background(), evt, "")
		if err != nil {
			t.Fatalf("handleDeployFailedOrRollback() err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(bus.publishedPREvents))
		}
		if bus.publishedPREvents[0].Event != "deploy_failed" {
			t.Errorf("expected event deploy_failed, got %s", bus.publishedPREvents[0].Event)
		}
	})

	t.Run("Deploy sync success publishes sync_success and deploy_success appropriately", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678"}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		// When syncHasExpected is true
		evt := DeployEvent{
			Event:     "sync_success",
			Repo:      "azylman/aerial",
			PRNumber:  42,
			CommitSHA: "sha-sync",
			TargetID:  "123456789012345678",
			Timestamp: time.Now().UTC(),
		}
		err := srv.handleDeploySyncSuccess(context.Background(), evt, true)
		if err != nil {
			t.Fatalf("handleDeploySyncSuccess(syncHasExpected=true) err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "sync_success" {
			t.Errorf("expected sync_success, got %v", bus.publishedPREvents)
		}

		// When syncHasExpected is false -> deploy_success
		err = srv.handleDeploySyncSuccess(context.Background(), evt, false)
		if err != nil {
			t.Fatalf("handleDeploySyncSuccess(syncHasExpected=false) err = %v", err)
		}
		if len(bus.publishedPREvents) != 2 || bus.publishedPREvents[1].Event != "deploy_success" {
			t.Errorf("expected deploy_success, got %v", bus.publishedPREvents)
		}
	})

	t.Run("ProcessDeployEvent publishes deploy_started and deploy_success when bus is active", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678"}
		disp := &mockOutboundDispatcher{}
		srv := &RouterServer{
			bus:        bus,
			registry:   reg,
			dispatcher: disp,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}
		deployEvt := DeployEvent{
			Event:     "deploy_started",
			Repo:      "azylman/aerial",
			PRNumber:  42,
			CommitSHA: "sha-dep",
			TargetID:  "123456789012345678",
			JobName:   "eri",
		}
		_, err := srv.ProcessDeployEvent(context.Background(), deployEvt)
		if err != nil {
			t.Fatalf("ProcessDeployEvent(deploy_started) err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "deploy_started" {
			t.Fatalf("expected deploy_started event, got %v", bus.publishedPREvents)
		}

		deploySuccessEvt := DeployEvent{
			Event:     "deploy_success",
			Repo:      "azylman/aerial",
			PRNumber:  42,
			CommitSHA: "sha-dep",
			TargetID:  "123456789012345678",
			JobName:   "eri",
		}
		_, err = srv.ProcessDeployEvent(context.Background(), deploySuccessEvt)
		if err != nil {
			t.Fatalf("ProcessDeployEvent(deploy_success) err = %v", err)
		}
		if len(bus.publishedPREvents) != 2 || bus.publishedPREvents[1].Event != "deploy_success" {
			t.Fatalf("expected deploy_success event, got %v", bus.publishedPREvents)
		}
	})

	t.Run("Kustomization watch completes with deploy_success or rollout_updated", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678"}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		// Not yet transitioned -> rollout_updated
		reg.markKustomizationsRes = false
		pr := DeployingPR{Repo: "azylman/aerial", PRNumber: 42, TargetID: "123456789012345678", MergeSHA: "sha-merge"}
		err := srv.completeKustomizationRequest(context.Background(), pr, "aerial-root")
		if err != nil {
			t.Fatalf("completeKustomizationRequest() err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "rollout_updated" {
			t.Errorf("expected rollout_updated, got %v", bus.publishedPREvents)
		}

		// Transitioned -> deploy_success
		reg.markKustomizationsRes = true
		err = srv.completeKustomizationRequest(context.Background(), pr, "aerial-services")
		if err != nil {
			t.Fatalf("completeKustomizationRequest() err = %v", err)
		}
		if len(bus.publishedPREvents) != 2 || bus.publishedPREvents[1].Event != "deploy_success" {
			t.Errorf("expected deploy_success, got %v", bus.publishedPREvents)
		}
	})

	t.Run("completeNoDeployWorkflowRun dispatches prompt for non-Vanadis PR when bus publish succeeds", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678", suppressVanadisRes: false}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		err := srv.completeNoDeployWorkflowRun(context.Background(), "azylman/aerial", "head123", "123456789012345678", 42)
		if err != nil {
			t.Fatalf("completeNoDeployWorkflowRun() err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "deploy_success" {
			t.Fatalf("expected deploy_success published event, got %v", bus.publishedPREvents)
		}
		if len(disp.PromptCalls()) != 1 {
			t.Errorf("expected prompt to be dispatched for non-Vanadis PR on deploy success, got %d calls", len(disp.PromptCalls()))
		}
	})

	t.Run("completeNoDeployWorkflowRun advances Vanadis and suppresses prompt for Vanadis PR when bus publish succeeds", func(t *testing.T) {
		t.Parallel()
		bus := &mockEventBus{}
		disp := &mockOutboundDispatcher{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678", suppressVanadisRes: true}
		srv := &RouterServer{
			bus:        bus,
			dispatcher: disp,
			registry:   reg,
			cfgSrc:     fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}

		err := srv.completeNoDeployWorkflowRun(context.Background(), "azylman/aerial", "head123", "123456789012345678", 42)
		if err != nil {
			t.Fatalf("completeNoDeployWorkflowRun() err = %v", err)
		}
		if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].Event != "deploy_success" {
			t.Fatalf("expected deploy_success published event, got %v", bus.publishedPREvents)
		}
		if len(disp.PromptCalls()) != 0 {
			t.Errorf("expected prompt to be suppressed for Vanadis PR on deploy success, got %d calls", len(disp.PromptCalls()))
		}
		if len(reg.advanceVanadisCalls) != 1 {
			t.Errorf("expected advanceVanadisCalls = 1, got %d", len(reg.advanceVanadisCalls))
		}
	})
}

func TestPRLifecycleEvents_HelpersAndState(t *testing.T) {
	t.Parallel()

	t.Run("buildPRRolloutState extracts all fields from metadata", func(t *testing.T) {
		t.Parallel()
		meta := map[string]any{
			"jobs":          []any{"eri", "coven"},
			"started_jobs":  []any{"eri"},
			"deployed_jobs": []any{"eri"},
			"job_states": map[string]any{
				"eri": "verifying_health",
			},
			"kustomization_statuses": map[string]any{
				"aerial-root": "Ready",
			},
		}
		rs := buildPRRolloutState(meta)
		if rs == nil {
			t.Fatalf("expected non-nil rollout state")
		}
		if len(rs.ExpectedJobs) != 2 || rs.ExpectedJobs[0] != "eri" {
			t.Errorf("unexpected ExpectedJobs: %v", rs.ExpectedJobs)
		}
		if len(rs.StartedJobs) != 1 || rs.StartedJobs[0] != "eri" {
			t.Errorf("unexpected StartedJobs: %v", rs.StartedJobs)
		}
		if len(rs.DeployedJobs) != 1 || rs.DeployedJobs[0] != "eri" {
			t.Errorf("unexpected DeployedJobs: %v", rs.DeployedJobs)
		}
		if rs.JobStates["eri"] != "verifying_health" {
			t.Errorf("unexpected JobStates: %v", rs.JobStates)
		}
		if rs.KustomizationStatuses["aerial-root"] != "Ready" {
			t.Errorf("unexpected KustomizationStatuses: %v", rs.KustomizationStatuses)
		}

		if buildPRRolloutState(nil) != nil {
			t.Errorf("expected nil for nil meta")
		}
		if buildPRRolloutState(map[string]any{}) != nil {
			t.Errorf("expected nil for empty meta")
		}
	})

	t.Run("healDependabotPR branches", func(t *testing.T) {
		t.Parallel()
		reg := &mockPRRegistry{
			resolveTargetID: "123456789012345678",
		}
		srv := &RouterServer{
			registry: reg,
			cfgSrc:   fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}
		resNonDep := &ResolvedGitHubEvent{
			Repo:     "azylman/aerial",
			PRNumber: 14,
			Branch:   "feat/foo",
			HeadSHA:  "sha123",
		}
		srv.healDependabotPR(context.Background(), resNonDep)
		if len(reg.ensureCalls) != 0 {
			t.Errorf("expected 0 ensureCalls for non-dependabot branch, got %d", len(reg.ensureCalls))
		}

		resDep := &ResolvedGitHubEvent{
			Repo:     "azylman/aerial",
			PRNumber: 15,
			Branch:   "dependabot/pip/foo-1.0",
			HeadSHA:  "sha123",
		}
		srv.healDependabotPR(context.Background(), resDep)
		if len(reg.ensureCalls) != 1 {
			t.Errorf("expected 1 ensureCall for dependabot branch, got %d", len(reg.ensureCalls))
		}
	})

	t.Run("isAutoUpdateAuthorAllowed and sanitizeSubjectToken branches", func(t *testing.T) {
		t.Parallel()
		srv := &RouterServer{
			cfgSrc: fixedConfig(Config{AutoUpdatePRAuthors: []string{"app/dependabot", "trusted-dev"}}),
		}
		if !srv.isAutoUpdateAuthorAllowed("app/dependabot") {
			t.Errorf("expected app/dependabot to be allowed")
		}
		if srv.isAutoUpdateAuthorAllowed("random-user") {
			t.Errorf("expected random-user to not be allowed")
		}

		if sanitizeSubjectToken("  ") != "unknown" {
			t.Errorf("expected unknown for empty token")
		}
		if sanitizeSubjectToken("foo/bar:baz") != "foo_bar_baz" {
			t.Errorf("expected foo_bar_baz")
		}
	})
}
