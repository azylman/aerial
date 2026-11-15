package main

import (
	"context"
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

	t.Run("CI failure publishes ci_failed and suppresses duplicate prompt when bus is present", func(t *testing.T) {
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
		if len(disp.PromptCalls()) != 0 {
			t.Errorf("expected prompt to be suppressed when bus is active, got %d calls", len(disp.PromptCalls()))
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
}

func TestPRLifecycleEvents_DeployAndKustomization(t *testing.T) {
	t.Parallel()

	t.Run("Deploy failed publishes deploy_failed and suppresses duplicate prompt when bus is present", func(t *testing.T) {
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

	t.Run("resolveDeployEventTargetID branches", func(t *testing.T) {
		t.Parallel()
		reg := &mockPRRegistry{
			resolveTargetID: "target-12345",
			resolvePRNum:    10,
		}
		srv := &RouterServer{registry: reg}

		evt := DeployEvent{
			Repo:     "azylman/aerial",
			PRNumber: 10,
		}
		srv.resolveDeployEventTargetID(context.Background(), &evt)
		if evt.TargetID != "target-12345" {
			t.Errorf("got targetID %q, want target-12345", evt.TargetID)
		}

		// When PRNumber is 0 but CommitSHA is set, resolveDeployEventPRNumber resolves PR and TargetID
		evt2 := DeployEvent{
			Repo:      "azylman/aerial",
			CommitSHA: "sha123",
		}
		srv.resolveDeployEventPRNumber(context.Background(), &evt2)
		if evt2.PRNumber != 10 || evt2.TargetID != "target-12345" {
			t.Errorf("got PRNumber %d, targetID %q for sha, want 10, target-12345", evt2.PRNumber, evt2.TargetID)
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
		// Non-dependabot branch returns early
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

		// Empty targetID returns early
		srvNoSys := &RouterServer{
			registry: &mockPRRegistry{},
			cfgSrc:   fixedConfig(Config{SystemChannelID: ""}),
		}
		resDep := &ResolvedGitHubEvent{
			Repo:     "azylman/aerial",
			PRNumber: 15,
			Branch:   "dependabot/pip/foo-1.0",
			HeadSHA:  "sha123",
		}
		srvNoSys.healDependabotPR(context.Background(), resDep)

		// Successful self-healing
		srv.healDependabotPR(context.Background(), resDep)
		if len(reg.ensureCalls) != 1 {
			t.Errorf("expected 1 ensureCall, got %d", len(reg.ensureCalls))
		}
	})

	t.Run("isAutoUpdateAuthorAllowed and checkOpenPRConflicts edge branches", func(t *testing.T) {
		t.Parallel()
		srv := &RouterServer{
			cfgSrc: fixedConfig(Config{AutoUpdatePRAuthors: []string{"app/dependabot", "arcane103"}}),
		}
		if !srv.isAutoUpdateAuthorAllowed("app/dependabot") {
			t.Errorf("expected app/dependabot to be allowed")
		}
		if srv.isAutoUpdateAuthorAllowed("random-user") {
			t.Errorf("expected random-user to not be allowed")
		}
		var nilSrv *RouterServer
		if nilSrv.isAutoUpdateAuthorAllowed("app/dependabot") {
			t.Errorf("expected nil server to return false")
		}
		if srv.isAutoUpdateAuthorAllowed("") {
			t.Errorf("expected empty author to return false")
		}

		if sanitizeSubjectToken("  ") != "unknown" {
			t.Errorf("expected unknown for empty token")
		}
		if sanitizeSubjectToken("foo/bar:baz") != "foo_bar_baz" {
			t.Errorf("expected foo_bar_baz")
		}

		// dispatchConflictPrompt with non-PostgresPRRegistry
		mockReg := &mockPRRegistry{}
		srvMock := &RouterServer{
			registry: mockReg,
			cfgSrc:   fixedConfig(Config{SystemChannelID: "123456789012345678"}),
		}
		_ = srvMock.dispatchConflictPrompt(context.Background(), "123456789012345678", "azylman/aerial", "feat/conflict", "sha-conf", 100)

		// Test isDependabotBranch and isZeroSHA helper functions
		if !isDependabotBranch("dependabot/npm/foo-1.0") {
			t.Errorf("expected dependabot branch")
		}
		if isDependabotBranch("feat/foo") {
			t.Errorf("expected non-dependabot branch")
		}
		if !isZeroSHA("0000000000000000000000000000000000000000") {
			t.Errorf("expected zero sha")
		}
		if isZeroSHA("1234567890abcdef") {
			t.Errorf("expected non-zero sha")
		}

		// Test extractPRNumberFromCommitMessage
		if prNum := extractPRNumberFromCommitMessage("feat: cool feature (#123)"); prNum != 123 {
			t.Errorf("expected 123, got %d", prNum)
		}
		if prNum := extractPRNumberFromCommitMessage("Merge pull request #456 from branch"); prNum != 456 {
			t.Errorf("expected 456, got %d", prNum)
		}
		if prNum := extractPRNumberFromCommitMessage("random commit"); prNum != 0 {
			t.Errorf("expected 0, got %d", prNum)
		}

		// Test buildJobService
		if svc, ok := buildJobService("Build & Push Container Image (eri)"); !ok || svc != "eri" {
			t.Errorf("expected eri, true, got %q, %v", svc, ok)
		}
		if _, ok := buildJobService("Build & Push Image (${{ matrix.service }})"); ok {
			t.Errorf("expected unexpanded template to be rejected")
		}
		if _, ok := buildJobService("Lint & Test"); ok {
			t.Errorf("expected Lint & Test to not match build job")
		}

		// Test hasSkippedImagePromotionJob and hasExecutedBuildStep
		if !hasSkippedImagePromotionJob([]GitHubWorkflowJobItem{
			{Name: "Promote latest images", Status: "completed", Conclusion: "skipped"},
		}) {
			t.Errorf("expected skipped promote job to be detected")
		}
		if hasSkippedImagePromotionJob([]GitHubWorkflowJobItem{
			{Name: "Promote latest images", Status: "completed", Conclusion: "success"},
		}) {
			t.Errorf("expected successful promote job not to be considered skipped")
		}
		if !hasExecutedBuildStep([]GitHubWorkflowJobStep{
			{Name: "Build & push docker", Status: "completed", Conclusion: "success"},
		}) {
			t.Errorf("expected successful build step")
		}
		if hasExecutedBuildStep([]GitHubWorkflowJobStep{
			{Name: "Build & push docker", Status: "completed", Conclusion: "skipped"},
		}) {
			t.Errorf("expected skipped build step to be detected as false")
		}

		// Exercise ProcessDeployEvent branches for deploy_started and deploy_success when bus is active
		bus := &mockEventBus{}
		reg := &mockPRRegistry{resolveTargetID: "123456789012345678"}
		disp := &mockOutboundDispatcher{}
		srvDeploy := &RouterServer{
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
		_, _ = srvDeploy.ProcessDeployEvent(context.Background(), deployEvt)

		deploySuccessEvt := DeployEvent{
			Event:     "deploy_success",
			Repo:      "azylman/aerial",
			PRNumber:  42,
			CommitSHA: "sha-dep",
			TargetID:  "123456789012345678",
			JobName:   "eri",
		}
		_, _ = srvDeploy.ProcessDeployEvent(context.Background(), deploySuccessEvt)
	})
}
