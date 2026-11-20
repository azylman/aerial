package router

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestIsWaitingForImageBuild(t *testing.T) {
	t.Parallel()
	engine := []BuildSelection{{Service: "engine", ImageName: "aerial-engine"}}
	tests := []struct {
		name, repo, status, kind string
		started, found, watch    bool
		selected                 []BuildSelection
		lifecycleErr, err        error
		want                     bool
	}{
		{"absent snapshot keeps waiting", "example/aerial", prMerged, "", false, false, false, nil, nil, nil, true},
		{"unreadable snapshot keeps waiting", "example/aerial", prMerged, "", false, false, false, nil, nil, errors.New("invalid"), true},
		{"lifecycle unavailable keeps waiting", "example/aerial", prMerged, "", false, false, false, nil, errors.New("offline"), nil, true},
		{"manifest-only empty selection keeps waiting", "example/aerial", prMerged, buildMetadataManifest, false, true, false, []BuildSelection{}, nil, nil, true},
		{"in-progress selection keeps waiting", "example/aerial", prSynced, buildMetadataProgress, false, true, false, engine, nil, nil, true},
		{"explicit empty final completes", "example/aerial", prMerged, buildMetadataFinal, false, true, false, []BuildSelection{}, nil, nil, false},
		{"explicit empty final on open PR", "example/aerial-sidecars", prOpen, buildMetadataFinal, false, true, false, []BuildSelection{}, nil, nil, false},
		{"selected images wait for promotion", "example/mirrormere", prMerged, buildMetadataFinal, false, true, false, engine, nil, nil, true},
		{"conflicted PR with selection waits", "example/aerial", prConflict, buildMetadataFinal, false, true, false, engine, nil, nil, true},
		{"started deployment ends wait", "example/aerial", prMerged, buildMetadataFinal, true, true, false, engine, nil, nil, false},
		{"watcher-owned rollout ends wait", "example/aerial", prMerged, buildMetadataFinal, false, true, true, engine, nil, nil, false},
		{"deploying is terminal", "example/aerial", statusDeploying, "", false, false, false, nil, nil, nil, false},
		{"deployed is terminal", "example/aerial", "deployed", "", false, false, false, nil, nil, nil, false},
		{"deploy failed is terminal", "example/aerial", "deploy_failed", "", false, false, false, nil, nil, nil, false},
		{"unregistered PR", "example/aerial", "", "", false, false, false, nil, nil, nil, false},
		{"repository without image producer", "example/aerial-config", prMerged, "", false, false, false, nil, nil, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{buildLifecycle: PRBuildLifecycle{Status: tc.status, Started: tc.started}, buildLifecycleErr: tc.lifecycleErr, buildMetadata: BuildMetadata{Kind: tc.kind, Selected: tc.selected}, buildMetadataFound: tc.found, buildMetadataErr: tc.err, hasKustomizationReqs: tc.watch}
			srv := newTestRouter(Config{}, nil, reg, nil)
			if got := srv.isWaitingForImageBuild(context.Background(), tc.repo, 42); got != tc.want {
				t.Fatalf("wait=%v want %v", got, tc.want)
			}
		})
	}
}

// --- 5. Outbound Dispatcher ---

func TestResolveSHA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name               string
		repo               string
		sha                string
		regErr             error
		regResolvePRNum    int
		regResolveTargetID string
		regResolveBranch   string
		regEmptySHAID      bool
		byNumTargetID      string
		ghPRNumber         int
		ghBranch           string
		wantPRNum          int
		wantTargetID       string
		wantBranch         string
		wantBackfillCalls  int
		wantErr            bool
	}{
		{
			name:               "local fast-path with targetID",
			repo:               "azylman/aerial",
			sha:                "sha-fast-1",
			regResolvePRNum:    42,
			regResolveTargetID: "100000000000000001",
			regResolveBranch:   "feat/fast-path",
			wantPRNum:          42,
			wantTargetID:       "100000000000000001",
			wantBranch:         "feat/fast-path",
		},
		{
			name:             "local fast-path with empty targetID resolved via ResolvePRByNumber",
			repo:             "azylman/aerial",
			sha:              "sha-fast-empty-tid",
			regResolvePRNum:  43,
			regResolveBranch: "feat/resolved-by-num",
			regEmptySHAID:    true,
			byNumTargetID:    "100000000000000001",
			wantPRNum:        43,
			wantTargetID:     "100000000000000001",
			wantBranch:       "feat/resolved-by-num",
		},
		{
			name:              "GitHub API fallback with BackfillPushMergeSHA",
			repo:              "azylman/aerial",
			sha:               "sha-gh-3",
			ghPRNumber:        99,
			ghBranch:          "feat/gh-fallback",
			byNumTargetID:     "100000000000000001",
			wantPRNum:         99,
			wantTargetID:      "100000000000000001",
			wantBranch:        "feat/gh-fallback",
			wantBackfillCalls: 1,
		},
		{
			name:       "GitHub API fallback with BackfillPushMergeSHA error",
			repo:       "azylman/aerial",
			sha:        "sha-gh-bf-err",
			ghPRNumber: 99,
			ghBranch:   "feat/gh-fallback-err",
			regErr:     errors.New("bf err"),
			wantPRNum:  99,
			wantBranch: "feat/gh-fallback-err",
		},
		{
			name:      "zero SHA returns zero results",
			repo:      "azylman/aerial",
			sha:       "0000000000000000000000000000000000000000",
			wantPRNum: 0,
		},
		{
			name:      "empty repo and sha returns zero results",
			repo:      "",
			sha:       "",
			wantPRNum: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.ghPRNumber > 0 {
					w.Header().Set("Content-Type", "application/json")
					item := GitHubCommitPRItem{Number: tc.ghPRNumber, State: "closed"}
					item.Head.Ref = tc.ghBranch
					_ = json.NewEncoder(w).Encode([]GitHubCommitPRItem{item})
					return
				}
				http.NotFound(w, r)
			}))
			defer ghServer.Close()

			targetID := tc.regResolveTargetID
			if tc.byNumTargetID != "" {
				targetID = tc.byNumTargetID
			}
			mockReg := &mockPRRegistry{
				err:                     tc.regErr,
				resolvePRNum:            tc.regResolvePRNum,
				resolveTargetID:         targetID,
				resolveBranch:           tc.regResolveBranch,
				resolveBySHATargetEmpty: tc.regEmptySHAID,
				backfillTargetID:        tc.byNumTargetID,
			}
			srv := newTestRouter(Config{GitHubAPIURL: ghServer.URL}, ghServer.Client(), mockReg, nil)

			prNum, targetID, branch, err := srv.ResolveSHA(ctx, tc.repo, tc.sha)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveSHA() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if prNum != tc.wantPRNum {
				t.Errorf("got prNum = %d, want %d", prNum, tc.wantPRNum)
			}
			if targetID != tc.wantTargetID {
				t.Errorf("got targetID = %q, want %q", targetID, tc.wantTargetID)
			}
			if branch != tc.wantBranch {
				t.Errorf("got branch = %q, want %q", branch, tc.wantBranch)
			}
			if len(mockReg.backfillCalls) != tc.wantBackfillCalls {
				t.Errorf("got %d backfill calls, want %d", len(mockReg.backfillCalls), tc.wantBackfillCalls)
			}
		})
	}
}

func TestResolveThreadForPRAndDeploymentSuccessPrompt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validSnowflake := "100000000000000001"

	tests := []resolveThreadCase{
		{
			name:           "ResolveThreadForPR nil registry returns empty",
			useNilRegistry: true,
			prNumber:       123,
			wantTargetID:   "",
		},
		{
			name:         "ResolveThreadForPR zero prNumber returns empty",
			prNumber:     0,
			wantTargetID: "",
		},
		{
			name:       "ResolveThreadForPR registry error returns error",
			prNumber:   456,
			resolveErr: errors.New("db error"),
			wantErr:    true,
		},
		{
			name:         "ResolveThreadForPR valid prNumber returns targetID",
			prNumber:     456,
			targetID:     validSnowflake,
			wantTargetID: validSnowflake,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt normal",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			wantPrompts:         1,
			wantWidgets:         1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job brain on azylman/aerial",
				"Directive: Confirm the deployment and suggest logical next steps, follow-up verification, or related tasks concisely.",
			},
		},
		{
			name:                "dispatchDeploymentSuccessPrompt with prNumber > 0 is suppressed",
			isDeploymentSuccess: true,
			prNumber:            558,
			targetID:            validSnowflake,
			wantPrompts:         0,
			wantWidgets:         0,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt invalid snowflake is no-op",
			isDeploymentSuccess: true,
			targetID:            "invalid-snowflake",
			wantPrompts:         0,
			wantWidgets:         0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runResolveThreadCase(ctx, t, tc)
		})
	}
}

type resolveThreadCase struct {
	name                string
	isDeploymentSuccess bool
	useNilRegistry      bool
	prNumber            int
	targetID            string
	branch              string
	wantTargetID        string
	wantErr             bool
	wantPrompts         int
	wantPromptSubs      []string
	wantDMs             int
	wantWidgets         int
	resolveErr          error
}

func runResolveThreadCase(ctx context.Context, t *testing.T, tc resolveThreadCase) {
	t.Helper()
	var mockReg PRRegistryUpdater
	if !tc.useNilRegistry {
		mockReg = &mockPRRegistry{
			resolveTargetID: tc.targetID,
			resolveBranch:   tc.branch,
			resolveErr:      tc.resolveErr,
		}
	}
	mockDisp := &mockOutboundDispatcher{}
	srv := newTestRouter(Config{}, nil, mockReg, mockDisp)

	if !tc.isDeploymentSuccess {
		gotTarget, err := srv.ResolveThreadForPR(ctx, "azylman/aerial", tc.prNumber)
		if (err != nil) != tc.wantErr {
			t.Fatalf("ResolveThreadForPR() err = %v, wantErr = %v", err, tc.wantErr)
		}
		if gotTarget != tc.wantTargetID {
			t.Errorf("got targetID = %q, want %q", gotTarget, tc.wantTargetID)
		}
		return
	}

	err := srv.dispatchDeploymentSuccessPrompt(ctx, tc.targetID, "brain", "azylman/aerial", "sha123", tc.prNumber, "alloc-1")
	if (err != nil) != tc.wantErr {
		t.Fatalf("dispatchDeploymentSuccessPrompt() err = %v, wantErr = %v", err, tc.wantErr)
	}
	assertResolveThreadDispatcher(t, mockDisp, tc)
}

func assertResolveThreadDispatcher(t *testing.T, mockDisp *mockOutboundDispatcher, tc resolveThreadCase) {
	t.Helper()
	pCalls := mockDisp.PromptCalls()
	if len(pCalls) != tc.wantPrompts {
		t.Errorf("got %d prompts, want %d", len(pCalls), tc.wantPrompts)
	}
	for _, sub := range tc.wantPromptSubs {
		if len(pCalls) == 0 || !strings.Contains(pCalls[0].Prompt, sub) {
			t.Errorf("expected prompt containing %q, got %+v", sub, pCalls)
		}
	}
	if len(mockDisp.DirectMessageCalls()) != tc.wantDMs {
		t.Errorf("got %d DMs, want %d", len(mockDisp.DirectMessageCalls()), tc.wantDMs)
	}
}

func TestDeployRolloutSuppressionAndState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := &RouterServer{}

	// deploy success suppression memory branches
	srv.recordDeploySuccessDispatched("azylman/aerial", "test-job", "sha-deploy-1", 101)
	if !srv.shouldSuppressDuplicateDeploySuccess(ctx, "azylman/aerial", "test-job", "sha-deploy-1", 101) {
		t.Errorf("expected suppression by PR number")
	}
	if !srv.shouldSuppressDuplicateDeploySuccess(ctx, "azylman/aerial", "test-job", "sha-deploy-1", 0) {
		t.Errorf("expected suppression by SHA")
	}
	if srv.shouldSuppressDuplicateDeploySuccess(ctx, "azylman/aerial", "diff-job", "diff-sha", 0) {
		t.Errorf("unexpected suppression for unknown job/sha")
	}

	// loadPRRolloutState branches
	var nilSrv *RouterServer
	if nilSrv.loadPRRolloutState(ctx, "azylman/aerial", 1) != nil {
		t.Errorf("expected nil for nil server")
	}
	if srv.loadPRRolloutState(ctx, "", 1) != nil {
		t.Errorf("expected nil for empty repo")
	}
	if srv.loadPRRolloutState(ctx, "azylman/aerial", 0) != nil {
		t.Errorf("expected nil for pr <= 0")
	}
	srv.registry = &mockPRRegistry{}
	if srv.loadPRRolloutState(ctx, "azylman/aerial", 1) != nil {
		t.Errorf("expected nil for non-postgres registry")
	}
	mockPool := &mockSynapsePgxPool{
		queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return mockSynapsePgxRow{
				scanFn: func(dest ...any) error {
					if len(dest) > 0 {
						if b, ok := dest[0].(*[]byte); ok {
							*b = []byte(`{"jobs":["eri"]}`)
						}
					}
					return nil
				},
			}
		},
	}
	srv.registry = NewPostgresPRRegistry(mockPool)
	if rs := srv.loadPRRolloutState(ctx, "azylman/aerial", 1); rs == nil || len(rs.ExpectedJobs) != 1 {
		t.Errorf("expected rollout state with 1 job, got %v", rs)
	}

	mockDeployedPool := &mockSynapsePgxPool{
		queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return mockSynapsePgxRow{
				scanFn: func(dest ...any) error {
					if len(dest) > 0 {
						if s, ok := dest[0].(*string); ok {
							*s = "deployed"
						}
					}
					return nil
				},
			}
		},
	}
	srvDeployed := &RouterServer{registry: NewPostgresPRRegistry(mockDeployedPool)}
	if !srvDeployed.shouldSuppressDuplicateDeploySuccess(ctx, "azylman/aerial", "new-job", "new-sha", 202) {
		t.Errorf("expected suppression by postgres deployed status")
	}
}

func TestDispatchAlertWithPrompt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validSnowflake := "123456789012345678"

	tests := []struct {
		name          string
		nilDispatcher bool
		targetID      string
		dmMsg         string
		prompt        string
		opt           *alertPromptOpts
		dispatchErr   error
		wantErr       bool
		wantDMs       int
		wantPrompts   int
		wantBg        int
	}{
		{
			name:          "nil dispatcher returns nil",
			nilDispatcher: true,
			targetID:      validSnowflake,
			prompt:        "test prompt",
		},
		{
			name:        "invalid target without silent is no-op",
			targetID:    "invalid-snowflake",
			prompt:      "test prompt",
			wantPrompts: 0,
		},
		{
			name:        "valid target with dm and prompt dispatches both",
			targetID:    validSnowflake,
			dmMsg:       "direct message alert",
			prompt:      "prompt directive",
			wantDMs:     1,
			wantPrompts: 1,
		},
		{
			name:        "prompt dispatch error returns error",
			targetID:    validSnowflake,
			prompt:      "prompt directive",
			dispatchErr: errors.New("prompt dispatch failed"),
			wantErr:     true,
			wantPrompts: 1,
		},
		{
			name:     "silent background dispatch",
			targetID: validSnowflake,
			prompt:   "background prompt",
			opt: &alertPromptOpts{
				silent:       true,
				skipIfActive: true,
				effort:       "high",
				dispatchID:   "disp-1",
			},
			wantBg: 1,
		},
		{
			name:     "silent background dispatch error returns error",
			targetID: validSnowflake,
			prompt:   "background prompt",
			opt: &alertPromptOpts{
				silent: true,
			},
			dispatchErr: errors.New("background dispatch failed"),
			wantErr:     true,
			wantBg:      1,
		},
		{
			name:        "dmMsg dispatch error logs warning and continues to prompt",
			targetID:    validSnowflake,
			dmMsg:       "dm alert",
			prompt:      "prompt directive",
			dispatchErr: errors.New("dm error"),
			wantErr:     true, // prompt also fails when dispatcher returns error
			wantDMs:     1,
			wantPrompts: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			var mockDisp *mockOutboundDispatcher
			if !tc.nilDispatcher {
				mockDisp = &mockOutboundDispatcher{
					promptErr:        tc.dispatchErr,
					dispatchErr:      tc.dispatchErr,
					directMessageErr: tc.dispatchErr,
				}
				srv.SetDispatcher(mockDisp)
			}

			var aOpts []alertPromptOpts
			if tc.opt != nil {
				aOpts = append(aOpts, *tc.opt)
			}
			err := srv.dispatchAlertWithPrompt(ctx, tc.targetID, "test_alert", tc.dmMsg, tc.prompt, aOpts...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("dispatchAlertWithPrompt() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if mockDisp != nil {
				if len(mockDisp.DirectMessageCalls()) != tc.wantDMs {
					t.Errorf("got %d DMs, want %d", len(mockDisp.DirectMessageCalls()), tc.wantDMs)
				}
				if len(mockDisp.PromptCalls()) != tc.wantPrompts {
					t.Errorf("got %d prompts, want %d", len(mockDisp.PromptCalls()), tc.wantPrompts)
				}
				if len(mockDisp.DispatchCalls()) != tc.wantBg {
					t.Errorf("got %d bg calls, want %d", len(mockDisp.DispatchCalls()), tc.wantBg)
				}
			}
		})
	}
}

func TestHandleImagePromotionDeployFailed_RetryAndDeduplication(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name                string
		initialUpdated      bool
		busErr              error
		wantErr             bool
		wantPublishedEvents int
		wantDeployCalls     int
		wantAckCalls        int
	}{
		{
			name:                "first attempt succeeds and publishes event",
			initialUpdated:      true,
			wantPublishedEvents: 1,
			wantDeployCalls:     1,
			wantAckCalls:        1,
		},
		{
			name:                "transient bus failure propagates error and does not mark published",
			initialUpdated:      true,
			busErr:              errors.New("transient bus disconnect"),
			wantErr:             true,
			wantPublishedEvents: 0,
			wantDeployCalls:     1,
			wantAckCalls:        0,
		},
		{
			name:                "deduplicated delivery returns nil without publishing duplicate event",
			initialUpdated:      false,
			wantPublishedEvents: 0,
			wantDeployCalls:     1,
			wantAckCalls:        0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{err: tc.busErr}
			reg := &mockPRRegistry{
				deployFailedTargetID: "100000000000000001",
				deployFailedUpdated:  tc.initialUpdated,
				resolveTargetID:      "100000000000000001",
				resolveBranch:        "feat/test",
				resolveHeadSHA:       "head123",
				resolveMergeSHA:      "merge123",
			}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.bus = bus
			srv.registry = reg

			err := srv.handleImagePromotionDeployFailed(ctx, "azylman/aerial", 42, "feat/test", "head123", "100000000000000001", errors.New("simulated error"))
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleImagePromotionDeployFailed() err = %v, wantErr = %v", err, tc.wantErr)
			}

			if len(bus.publishedPREvents) != tc.wantPublishedEvents {
				t.Errorf("got %d published events, want %d", len(bus.publishedPREvents), tc.wantPublishedEvents)
			}
			if len(reg.deployFailedCalls) != tc.wantDeployCalls {
				t.Errorf("got %d deployFailedCalls, want %d", len(reg.deployFailedCalls), tc.wantDeployCalls)
			}
			if len(reg.ackDeployFailedPublishedCalls) != tc.wantAckCalls {
				t.Errorf("got %d ackDeployFailedPublishedCalls, want %d", len(reg.ackDeployFailedPublishedCalls), tc.wantAckCalls)
			}
		})
	}
}

func TestProcessDeployEvent_LifecycleErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		evt           DeployEvent
		busErr        error
		promptErr     error
		authoritative bool
		wantErrSub    string
	}{
		{
			name: "deploy_started publish error propagates",
			evt: DeployEvent{
				Event:    eventDeployStarted,
				Repo:     "azylman/aerial",
				PRNumber: 42,
				TargetID: "100000000000000001",
			},
			busErr:     errors.New("bus deploy_started fail"),
			wantErrSub: "bus deploy_started fail",
		},
		{
			name: "deploy_success publish error propagates",
			evt: DeployEvent{
				Event:    eventDeploySuccess,
				Repo:     "azylman/aerial",
				PRNumber: 42,
				TargetID: "100000000000000001",
			},
			busErr:     errors.New("bus deploy_success fail"),
			wantErrSub: "bus deploy_success fail",
		},
		{
			name: "deploy_success non-pr prompt error propagates",
			evt: DeployEvent{
				Event:     eventDeploySuccess,
				Repo:      "azylman/aerial",
				PRNumber:  0,
				TargetID:  "100000000000000001",
				JobName:   "brain",
				CommitSHA: "sha123",
			},
			promptErr:  errors.New("prompt fail"),
			wantErrSub: "prompt fail",
		},
		{
			name: "sync_success publish error propagates",
			evt: DeployEvent{
				Event:     eventSyncSuccess,
				Repo:      "azylman/aerial",
				PRNumber:  42,
				TargetID:  "100000000000000001",
				CommitSHA: "sha123",
			},
			busErr:     errors.New("bus sync fail"),
			wantErrSub: "bus sync fail",
		},
		{
			name: "authoritative deploy_success journal failure surfaces",
			evt: DeployEvent{
				Event:     eventDeploySuccess,
				Repo:      "azylman/aerial",
				PRNumber:  42,
				TargetID:  "100000000000000001",
				JobName:   "brain",
				CommitSHA: "sha123",
			},
			authoritative: true,
			wantErrSub:    "restate authoritative",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{err: tc.busErr}
			reg := &mockPRRegistry{resolveTargetID: "100000000000000001"}
			disp := &mockOutboundDispatcher{promptErr: tc.promptErr}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative}), nil)
			srv.bus = bus
			srv.registry = reg
			srv.dispatcher = disp

			_, err := srv.ProcessDeployEvent(ctx, tc.evt)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErrSub)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
		})
	}
}

// watcherAuthorityCase drives one watcher-sole-authority scenario: a sequence of
// deploy alerts through ProcessDeployEvent optionally followed by gated watcher
// completions, asserting the published lifecycle order and dedup state.
type watcherAuthorityCase struct {
	name            string
	repo            string
	pr              int
	mergeSHA        string
	headSHA         string
	kustomization   string
	discoverErr     bool
	hasRequests     bool
	trackAfterDefer bool
	deployFailed    bool
	events          []DeployEvent
	gate            bool
	gateImage       bool
	wantEvents      []wantPREvent
	wantDeploying   int
	wantRequested   []string
	wantReconciles  int
	wantDispatched  bool
}

func watcherAuthorityCases() []watcherAuthorityCase {
	return append(watcherAuthoritySequenceCases(), watcherAuthorityEdgeCases()...)
}

func watcherAuthoritySequenceCases() []watcherAuthorityCase {
	const targetID = "100000000000000001"
	return []watcherAuthorityCase{
		{
			name:          "pre-image success defers to watcher without terminal success",
			repo:          "azylman/aerial",
			pr:            1501,
			mergeSHA:      "mergesha1501",
			headSHA:       "headsha1501",
			kustomization: "aerial-core",
			events: []DeployEvent{
				mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "oldsha1501", 1501, targetID, "success"),
			},
			wantEvents:     []wantPREvent{{Event: "deploy_started", PRNumber: 1501, Status: "deploying"}},
			wantDeploying:  1,
			wantRequested:  []string{"aerial-core"},
			wantReconciles: 1,
			wantDispatched: false,
		},
		{
			name:            "real started after deferral stays deploying without duplicate started",
			repo:            "azylman/aerial",
			pr:              1501,
			mergeSHA:        "mergesha1501",
			headSHA:         "headsha1501",
			kustomization:   "aerial-core",
			trackAfterDefer: true,
			events: []DeployEvent{
				mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "oldsha1501", 1501, targetID, "success"),
				mkKustomizationEvt("deploy_started", "aerial-core", "azylman/aerial", "mergesha1501", 1501, targetID, "started"),
			},
			wantEvents:     []wantPREvent{{Event: "deploy_started", PRNumber: 1501, Status: "deploying"}},
			wantDeploying:  1,
			wantRequested:  []string{"aerial-core"},
			wantReconciles: 1,
			wantDispatched: false,
		},
		{
			name:            "gated completion publishes single success after start",
			repo:            "azylman/aerial",
			pr:              1501,
			mergeSHA:        "mergesha1501",
			headSHA:         "headsha1501",
			kustomization:   "aerial-core",
			trackAfterDefer: true,
			events: []DeployEvent{
				mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "oldsha1501", 1501, targetID, "success"),
				mkKustomizationEvt("deploy_started", "aerial-core", "azylman/aerial", "mergesha1501", 1501, targetID, "started"),
			},
			gate:      true,
			gateImage: true,
			wantEvents: []wantPREvent{
				{Event: "deploy_started", PRNumber: 1501, Status: "deploying"},
				{Event: "deploy_success", PRNumber: 1501, HeadSHA: "headsha1501", MergeSHA: "mergesha1501", Status: "deployed"},
			},
			wantDeploying:  1,
			wantRequested:  []string{"aerial-core"},
			wantReconciles: 1,
			wantDispatched: true,
		},
	}
}

func watcherAuthorityEdgeCases() []watcherAuthorityCase {
	const targetID = "100000000000000001"
	return []watcherAuthorityCase{
		{
			name:          "manifest-only config change completes through watcher gate",
			repo:          "azylman/aerial-config",
			pr:            1505,
			mergeSHA:      "cfgmerge1505",
			headSHA:       "cfghead1505",
			kustomization: "aerial-config",
			events: []DeployEvent{
				mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "cfghead1505", 1505, targetID, "success", ""),
			},
			gate:      true,
			gateImage: false,
			wantEvents: []wantPREvent{
				{Event: "deploy_started", PRNumber: 1505, Status: "deploying"},
				{Event: "deploy_success", PRNumber: 1505, HeadSHA: "cfghead1505", MergeSHA: "cfgmerge1505", Status: "deployed"},
			},
			wantDeploying:  1,
			wantRequested:  []string{"aerial-config"},
			wantReconciles: 1,
			wantDispatched: true,
		},
		{
			name:          "deploy failed keeps direct failure path",
			repo:          "azylman/aerial",
			pr:            1508,
			mergeSHA:      "mergesha1508",
			headSHA:       "headsha1508",
			kustomization: "aerial-core",
			deployFailed:  true,
			events: []DeployEvent{
				mkKustomizationEvt("deploy_failed", "aerial-core", "azylman/aerial", "failsha1508", 1508, targetID, "failed"),
			},
			wantEvents: []wantPREvent{
				{Event: "deploy_failed", PRNumber: 1508, MergeSHA: "failsha1508", Status: "deploy_failed"},
			},
			wantDeploying:  0,
			wantReconciles: 0,
			wantDispatched: false,
		},
		{
			name:          "tracked HelmRelease failure keeps direct failure path",
			repo:          "azylman/aerial",
			pr:            1510,
			mergeSHA:      "mergesha1510",
			headSHA:       "headsha1510",
			kustomization: "aerial-core",
			hasRequests:   true,
			deployFailed:  true,
			events: []DeployEvent{
				{Event: "deploy_failed", JobName: "rel-synapse", SourceKind: "HelmRelease", Repo: "azylman/aerial", CommitSHA: "failsha1510", PRNumber: 1510, TargetID: targetID, Status: "failed"},
			},
			wantEvents: []wantPREvent{
				{Event: "deploy_failed", PRNumber: 1510, MergeSHA: "failsha1510", Status: "deploy_failed"},
			},
			wantDeploying:  0,
			wantReconciles: 0,
			wantDispatched: false,
		},
		{
			name:          "discovery error fails closed without success or dedup",
			repo:          "azylman/aerial",
			pr:            1509,
			mergeSHA:      "mergesha1509",
			headSHA:       "headsha1509",
			kustomization: "aerial-core",
			discoverErr:   true,
			events: []DeployEvent{
				mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "mergesha1509", 1509, targetID, "success"),
			},
			wantEvents:     nil,
			wantDeploying:  0,
			wantReconciles: 0,
			wantDispatched: false,
		},
	}
}

func TestProcessDeployEventWatcherSoleAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range watcherAuthorityCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runWatcherAuthorityCase(t, tc)
		})
	}
}

func runWatcherAuthorityCase(t *testing.T, tc watcherAuthorityCase) {
	t.Helper()
	ctx := context.Background()
	const targetID = "100000000000000001"
	reg := &mockPRRegistry{
		hasKustomizationReqs:  tc.hasRequests,
		deployFailedUpdated:   tc.deployFailed,
		markKustomizationsRes: true,
	}
	disp := &mockOutboundDispatcher{}
	if tc.kustomization != "" {
		disp.repoKustomizations = map[string][]fluxKustomizationRef{
			tc.repo: {{Namespace: "flux-system", Name: tc.kustomization}},
		}
	}
	if tc.discoverErr {
		disp.repoDiscoverErr = errors.New("kube api down")
	}
	bus := &mockEventBus{}
	srv := newTestRouter(Config{SystemChannelID: targetID}, &http.Client{Transport: &zeroCallTransport{}}, reg, disp)
	srv.SetEventBus(bus)
	if tc.gate {
		srv.gitHistory.StoreCommitTime(tc.repo+"@"+tc.mergeSHA, time.Now().UTC())
	}
	for i, evt := range tc.events {
		if _, err := srv.ProcessDeployEvent(ctx, evt); err != nil {
			t.Fatalf("ProcessDeployEvent() step %d err = %v", i, err)
		}
		if tc.trackAfterDefer && i == 0 {
			reg.hasKustomizationReqs = true
		}
	}
	if tc.gate {
		runWatcherAuthorityGate(ctx, t, srv, tc, targetID)
	}
	assertWatcherAuthorityOutcome(t, srv, reg, disp, bus, tc)
}

func runWatcherAuthorityGate(ctx context.Context, t *testing.T, srv *RouterServer, tc watcherAuthorityCase, targetID string) {
	t.Helper()
	applied := tc.mergeSHA
	workload := workloadRollout{
		Kind: "Deployment", Namespace: "flux-system", Name: "synapse",
		Generation: 2, ObservedGeneration: 2,
		Replicas: 2, UpdatedReplicas: 2, AvailableReplicas: 2, StatusReplicas: 2,
	}
	if tc.gateImage {
		workload.Images = []string{"ghcr.io/azylman/synapse:" + tc.mergeSHA}
	} else {
		workload.ChangedAt = time.Now().UTC()
	}
	pr := DeployingPR{
		Repo: tc.repo, PRNumber: tc.pr, Branch: "feat/test",
		HeadSHA: tc.headSHA, MergeSHA: tc.mergeSHA, TargetID: targetID,
	}
	obs := KustomizationObservation{
		Ref:                 fluxKustomizationRef{Namespace: "flux-system", Name: tc.kustomization},
		LastAppliedRevision: applied,
	}
	state := kustomizationRolloutState{Workloads: []workloadRollout{workload}, SourceRepo: tc.repo}
	if err := srv.gateKustomizationRequest(ctx, pr, obs, state); err != nil {
		t.Fatalf("gateKustomizationRequest() err = %v", err)
	}
}

func assertWatcherAuthorityOutcome(t *testing.T, srv *RouterServer, reg *mockPRRegistry, disp *mockOutboundDispatcher, bus *mockEventBus, tc watcherAuthorityCase) {
	t.Helper()
	assertPREvents(t, bus, tc.wantEvents)
	if got := len(reg.deployingCalls); got != tc.wantDeploying {
		t.Errorf("deploying calls = %d, want %d", got, tc.wantDeploying)
	}
	var requested []string
	for _, call := range reg.kustomizationRequestCalls {
		for name := range call {
			requested = append(requested, name)
		}
	}
	slices.Sort(requested)
	if !slices.Equal(requested, tc.wantRequested) {
		t.Errorf("recorded kustomization requests = %v, want %v", requested, tc.wantRequested)
	}
	if got := len(disp.ReconcileCalls()); got != tc.wantReconciles {
		t.Errorf("reconcile calls = %d, want %d", got, tc.wantReconciles)
	}
	if got := srv.wasDeploySuccessDispatched(tc.repo, tc.kustomization, tc.mergeSHA, tc.pr); got != tc.wantDispatched {
		t.Errorf("wasDeploySuccessDispatched = %v, want %v", got, tc.wantDispatched)
	}
}
