package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
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

func TestServerEndpoints_HealthMetricsAndCoverage(t *testing.T) {
	oldCountersFn := httpkit.WriteCountersDirFn
	oldMetaFn := httpkit.WriteMetaDirFn
	t.Cleanup(func() {
		httpkit.WriteCountersDirFn = oldCountersFn
		httpkit.WriteMetaDirFn = oldMetaFn
	})

	RecordJobEnqueued("", "", "")
	RecordJobRetry("", "", "")
	RecordJobDropped("", "", "")
	SetDiscordGatewayLatency(0.042)
	registerCollector(JobsEnqueuedTotal)

	tests := []struct {
		name          string
		method        string
		path          string
		mockMetaErr   error
		mockCountErr  error
		queueStatsErr bool
		wantCode      int
		wantBodySubs  []string
	}{
		{
			name:         "/healthz returns 200 and json",
			method:       http.MethodGet,
			path:         "/healthz",
			wantCode:     http.StatusOK,
			wantBodySubs: []string{`"status":"ok"`, `"service":"synapse"`},
		},
		{
			name:     "/metrics returns 200 and prometheus metrics with queue stats and gateway latency",
			method:   http.MethodGet,
			path:     "/metrics",
			wantCode: http.StatusOK,
			wantBodySubs: []string{
				"synapse_queue_depth",
				"synapse_discord_gateway_latency_seconds 0.042",
			},
		},
		{
			name:          "/metrics with queue stats error logs and still returns 200",
			method:        http.MethodGet,
			path:          "/metrics",
			queueStatsErr: true,
			wantCode:      http.StatusOK,
			wantBodySubs:  []string{"synapse_discord_gateway_latency_seconds 0.042"},
		},
		{
			name:     "/debug/coverage/flush calls flush and returns 200",
			method:   http.MethodGet,
			path:     "/debug/coverage/flush",
			wantCode: http.StatusOK,
		},
		{
			name:        "/debug/coverage/flush meta error still returns 200",
			method:      http.MethodGet,
			path:        "/debug/coverage/flush",
			mockMetaErr: errors.New("simulated meta flush error"),
			wantCode:    http.StatusOK,
		},
		{
			name:         "/debug/coverage/flush counters error returns 500",
			method:       http.MethodGet,
			path:         "/debug/coverage/flush",
			mockCountErr: errors.New("simulated counters flush error"),
			wantCode:     http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			httpkit.WriteMetaDirFn = func(dir string) error {
				return tc.mockMetaErr
			}
			httpkit.WriteCountersDirFn = func(dir string) error {
				return tc.mockCountErr
			}

			srv := NewRouterServer(fixedConfig(Config{Port: "4020"}), nil)
			mockQ := &mockConsumerStatsQuerier{
				stats: map[string][2]int64{
					"AERIAL_JOBS/synapse-github": {4, 1},
				},
			}
			if tc.queueStatsErr {
				mockQ.err = errors.New("simulated queue query error")
			}
			srv.SetQueueStats(mockQ)
			srv.SetGatewayLatencyFn(func() time.Duration {
				return 42 * time.Millisecond
			})

			rec := doHTTPRequest(srv.Routes(), tc.method, tc.path, nil, nil)
			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			for _, sub := range tc.wantBodySubs {
				if !strings.Contains(rec.Body.String(), sub) {
					t.Errorf("body missing %q: %s", sub, rec.Body.String())
				}
			}
		})
	}

	rec := httptest.NewRecorder()
	httpkit.WriteJSON(rec, http.StatusOK, make(chan int))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("writeJSON code = %d, want 500", rec.Code)
	}
}

func TestConsumerStatsQuerier(t *testing.T) {
	t.Parallel()
	var nilSrv *RouterServer
	if nilSrv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil for nil server")
	}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	if srv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil when no stats or bus configured")
	}
	memBus := pubsub.NewMemoryBus()
	srv.SetEventBus(memBus)
	if srv.consumerStatsQuerier() == nil {
		t.Errorf("expected bus stats querier when bus implements consumerStatsQuerier")
	}
	mockQ := &mockConsumerStatsQuerier{}
	srv.SetQueueStats(mockQ)
	if srv.consumerStatsQuerier() != mockQ {
		t.Errorf("expected queueStats override")
	}
}

func newTestReloadable(initial, next Config) *httpkit.Reloadable[Config] {
	return httpkit.NewReloadable(&initial, func() (*Config, error) {
		reloaded := next
		return &reloaded, nil
	}, synapseRestartRequired)
}

func TestRunServerLifecycle(t *testing.T) {
	tests := []runServerLifecycleCase{
		{
			name:             "runServer start, loopback reload, and graceful cancel shutdown",
			config:           Config{Port: "0"},
			wantGraceful:     true,
			wantReloadStatus: httpkit.ReloadUnchanged,
		},
		{
			name:             "runServer live reload applies non-restart field",
			config:           Config{Port: "0"},
			reloadConfig:     &Config{Port: "0", SystemChannelID: "123456789012345678"},
			wantGraceful:     true,
			wantReloadStatus: httpkit.ReloadApplied,
		},
		{
			name:    "runServer invalid port -1 returns error",
			config:  Config{Port: "-1"},
			wantErr: true,
		},
		{
			name:      "runApp pre-canceled context returns clean or server closed",
			config:    Config{Port: "0"},
			preCancel: true,
			isRunApp:  true,
		},
		{
			name:         "runApp exits cleanly when reload requires restart",
			config:       Config{Port: "0"},
			reloadConfig: &Config{Port: "4021"},
			isRunApp:     true,
		},
		{
			name:    "runServer invalid postgres url returns error",
			config:  Config{Port: "0", PostgresURL: "postgres://user:pass@invalid%xx/db"},
			wantErr: true,
		},
		{
			name:     "runApp invalid port returns error",
			config:   Config{Port: "-1"},
			isRunApp: true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next := tc.config
			if tc.reloadConfig != nil {
				next = *tc.reloadConfig
			}
			cfgSrc := newTestReloadable(tc.config, next)
			if tc.wantGraceful {
				runServerGracefulCase(t, tc, cfgSrc, next)
				return
			}
			if tc.isRunApp {
				runServerAppCase(t, tc, cfgSrc)
				return
			}
			err := runServer(context.Background(), cfgSrc, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("runServer() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

type runServerLifecycleCase struct {
	name             string
	config           Config
	reloadConfig     *Config
	preCancel        bool
	isRunApp         bool
	wantErr          bool
	wantGraceful     bool
	wantReloadStatus httpkit.ReloadStatus
}

func runServerGracefulCase(t *testing.T, tc runServerLifecycleCase, cfgSrc *httpkit.Reloadable[Config], next Config) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	done := make(chan error, 1)

	go func() {
		done <- runServer(ctx, cfgSrc, func(addr string) {
			ready <- addr
		})
	}()

	var addr string
	select {
	case addr = <-ready:
		if addr == "" {
			t.Errorf("expected non-empty listening address")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server to start")
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	resp, err := http.Post("http://"+net.JoinHostPort("127.0.0.1", port)+"/-/reload", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /-/reload: %v", err)
	}
	var got struct {
		Status httpkit.ReloadStatus `json:"status"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&got)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || decodeErr != nil || got.Status != tc.wantReloadStatus {
		t.Errorf("POST /-/reload = %d %q (decode err %v), want 200 %q", resp.StatusCode, got.Status, decodeErr, tc.wantReloadStatus)
	}
	if gotSys := cfgSrc.Current().SystemChannelID; gotSys != next.SystemChannelID {
		t.Errorf("SystemChannelID after reload = %q, want %q", gotSys, next.SystemChannelID)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected clean shutdown, got error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server shutdown")
	}
}

func runServerAppCase(t *testing.T, tc runServerLifecycleCase, cfgSrc *httpkit.Reloadable[Config]) {
	t.Helper()
	ctxC, cancelC := context.WithCancel(context.Background())
	if tc.preCancel {
		cancelC()
	} else {
		defer cancelC()
	}
	if tc.reloadConfig != nil {
		if status, err := cfgSrc.Reload(); status != httpkit.ReloadRestarting || err != nil {
			t.Fatalf("Reload() = %q, %v; want %q", status, err, httpkit.ReloadRestarting)
		}
	}
	err := runApp(ctxC, cfgSrc)
	if (err != nil) != tc.wantErr && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("unexpected error from runApp: %v", err)
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

func (s *RouterServer) recordFailedCheckRun(repo string, prNumber int, headSHA, checkName string) bool {
	emit, _, _ := s.recordFailedCheckRunDetailed(repo, prNumber, headSHA, checkName)
	return emit
}

func (s *RouterServer) recordNonGateCheckFailure(res *ResolvedGitHubEvent) bool {
	emit, _, _ := s.recordNonGateCheckFailureDetailed(res)
	return emit
}

func TestRecordNonGateCheckFailure(t *testing.T) {
	t.Parallel()
	srv := &RouterServer{}
	if srv.recordNonGateCheckFailure(nil) {
		t.Errorf("expected false for nil event")
	}
	res1 := &ResolvedGitHubEvent{
		Event:     ghEventCheckRun,
		Repo:      "azylman/aerial",
		PRNumber:  42,
		HeadSHA:   "sha123",
		CheckName: "build-service",
	}
	if srv.recordNonGateCheckFailure(res1) {
		t.Errorf("expected false for initial check failure")
	}
	if sha, ok := srv.nonRoboRevFailedSHAs.Load(prCheckKey("azylman/aerial", 42)); !ok || sha != "sha123" {
		t.Errorf("expected nonRoboRevFailedSHAs to record sha123, got %v", sha)
	}
	res2 := &ResolvedGitHubEvent{
		Event:     ghEventCheckRun,
		Repo:      "azylman/aerial",
		PRNumber:  42,
		HeadSHA:   "sha123",
		CheckName: "test-service",
	}
	if !srv.recordNonGateCheckFailure(res2) {
		t.Errorf("expected true for additional check failure")
	}
}

func assertActiveCheckState(t *testing.T, srv *RouterServer, repo string, prNumber int, headSHA, checkName string, wantActive bool) {
	t.Helper()
	entry := srv.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	if entry == nil {
		t.Fatalf("expected non-nil failedCheckState entry for %s#%d@%s", repo, prNumber, headSHA)
	}
	entry.mu.Lock()
	active := entry.activeFailed[checkName]
	entry.mu.Unlock()
	if active != wantActive {
		t.Errorf("activeFailed[%s] = %v, want %v", checkName, active, wantActive)
	}
}

func TestHandleCIFailureByHeadSHA_FallbackAndRollback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name        string
		busErr      error
		isCIFailed  bool
		resPRNumber int
		regPRNumber int
		updated     bool
		wantErr     bool
	}{
		{
			name:        "resolves PR from head SHA and rolls back on bus error",
			busErr:      errors.New("bus fail"),
			isCIFailed:  true,
			resPRNumber: 0,
			regPRNumber: 42,
			updated:     false,
			wantErr:     true,
		},
		{
			name:        "resolves PR from head SHA and succeeds",
			isCIFailed:  true,
			resPRNumber: 0,
			regPRNumber: 42,
			updated:     false,
			wantErr:     false,
		},
		{
			name:        "not failed in registry returns false without error",
			isCIFailed:  false,
			resPRNumber: 0,
			regPRNumber: 0,
			updated:     false,
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{err: tc.busErr}
			reg := &mockPRRegistry{
				resolveTargetID: "100000000000000001",
				resolveHeadSHA:  "sha-head",
				buildLifecycle: PRBuildLifecycle{
					Status: func() string {
						if tc.isCIFailed {
							return prCIFailed
						}
						return prOpen
					}(),
				},
				ciFailedBySHAUpdated: tc.updated,
				ciFailedBySHAPRNum:   tc.regPRNumber,
				ciFailedBySHATarget:  "",
			}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.bus = bus
			srv.registry = reg

			res := &ResolvedGitHubEvent{
				Event:      ghEventCheckRun,
				Repo:       "azylman/aerial",
				PRNumber:   tc.resPRNumber,
				HeadSHA:    "sha-head",
				CheckName:  "test-check",
				Conclusion: conclusionFailure,
			}
			handled, err := srv.handleCIFailureByHeadSHA(ctx, res, "ci failed", "https://check.url")
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleCIFailureByHeadSHA() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if handled {
					t.Fatalf("expected handled = false on error")
				}
				assertActiveCheckState(t, srv, "azylman/aerial", 42, "sha-head", "test-check", false)
				return
			}
			if tc.isCIFailed && tc.regPRNumber > 0 {
				if !handled {
					t.Fatalf("expected handled = true on success")
				}
				assertActiveCheckState(t, srv, "azylman/aerial", 42, "sha-head", "test-check", true)
			}
		})
	}
}

func TestWorkflowRunPRDeployingAndNoDeploy_LifecycleErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name       string
		busErr     error
		noDeploy   bool
		wantErrSub string
	}{
		{
			name:       "startWorkflowRunPRDeploying publish error propagates",
			busErr:     errors.New("bus deploy_started fail"),
			wantErrSub: "bus deploy_started fail",
		},
		{
			name:       "completeNoDeployWorkflowRun publish error propagates",
			busErr:     errors.New("bus deploy_success fail"),
			noDeploy:   true,
			wantErrSub: "bus deploy_success fail",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{err: tc.busErr}
			reg := &mockPRRegistry{
				resolveTargetID:   "100000000000000001",
				deployingTargetID: "100000000000000001",
			}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.bus = bus
			srv.registry = reg

			var err error
			if tc.noDeploy {
				err = srv.completeNoDeployWorkflowRun(ctx, "azylman/aerial", "sha123", "100000000000000001", 42)
			} else {
				err = srv.startWorkflowRunPRDeploying(ctx, "azylman/aerial", 42, "sha123", "100000000000000001", []string{"eri"})
			}
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

func TestGitHubLifecyclePublishErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name       string
		action     string
		merged     bool
		isPush     bool
		isOpened   bool
		wantErrSub string
	}{
		{
			name:       "handlePRClosedAction merged publish error propagates",
			action:     ghActionClosed,
			merged:     true,
			wantErrSub: "bus fail",
		},
		{
			name:       "handlePRClosedAction unmerged publish error propagates",
			action:     ghActionClosed,
			merged:     false,
			wantErrSub: "bus fail",
		},
		{
			name:       "handlePullRequestRegistryAction synchronize publish error propagates",
			action:     ghActionSynchronize,
			wantErrSub: "bus fail",
		},
		{
			name:       "resolveAndBackfillPushPR publish error propagates",
			isPush:     true,
			wantErrSub: "bus fail",
		},
		{
			name:       "processGitHubPullRequestEvent opened publish error propagates",
			action:     ghActionOpened,
			isOpened:   true,
			wantErrSub: "bus fail",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{err: errors.New("bus fail")}
			reg := &mockPRRegistry{
				resolveTargetID: "100000000000000001",
			}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.bus = bus
			srv.registry = reg

			res := &ResolvedGitHubEvent{
				Event:    ghEventPullRequest,
				Action:   tc.action,
				Repo:     "azylman/aerial",
				PRNumber: 42,
				HeadSHA:  "head123",
				Branch:   "feat/test",
			}
			var err error
			switch {
			case tc.isOpened:
				payload := GitHubPullRequestPayload{
					Action: ghActionOpened,
					Number: 42,
					PullRequest: GitHubPullRequest{
						Number: 42,
						Title:  "PR title",
						Head:   GitHubPRRef{Ref: "feat/test", SHA: "head123"},
					},
				}
				payload.Repository.FullName = "azylman/aerial"
				body, _ := json.Marshal(payload)
				_, err = srv.processGitHubPullRequestEvent(ctx, "delivery-1", body, res)
			case tc.isPush:
				payload := GitHubPushPayload{
					Ref: "refs/heads/main",
					HeadCommit: &GitHubCommit{
						ID:      "head123",
						Message: "merge (#42)",
					},
				}
				err = srv.resolveAndBackfillPushPR(ctx, res, payload)
			case tc.action == ghActionClosed:
				err = srv.handlePRClosedAction(ctx, res, tc.merged)
			case tc.action == ghActionSynchronize:
				err = srv.handlePullRequestRegistryAction(ctx, res, GitHubPullRequest{Title: "PR title"})
			}
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

func TestIsPRRegistryCIFailed_Guards(t *testing.T) {
	t.Parallel()
	var nilSrv *RouterServer
	if nilSrv.isPRRegistryCIFailed(context.Background(), "azylman/aerial", 42) {
		t.Errorf("expected false for nil server")
	}
	srv := &RouterServer{}
	if srv.isPRRegistryCIFailed(context.Background(), "azylman/aerial", 42) {
		t.Errorf("expected false for nil registry")
	}
	srv.registry = &mockPRRegistry{}
	if srv.isPRRegistryCIFailed(context.Background(), "azylman/aerial", 0) {
		t.Errorf("expected false for pr <= 0")
	}
	if srv.isPRRegistryCIFailed(context.Background(), "", 42) {
		t.Errorf("expected false for empty repo")
	}

	regCIFailed := &mockPRRegistry{
		buildLifecycle: PRBuildLifecycle{Status: prCIFailed},
	}
	srvCIFailed := &RouterServer{registry: regCIFailed}
	if !srvCIFailed.isPRRegistryCIFailed(context.Background(), "azylman/aerial", 42) {
		t.Errorf("expected true when status is prCIFailed")
	}

	regOpen := &mockPRRegistry{
		buildLifecycle: PRBuildLifecycle{Status: prOpen},
	}
	srvOpen := &RouterServer{registry: regOpen}
	if srvOpen.isPRRegistryCIFailed(context.Background(), "azylman/aerial", 42) {
		t.Errorf("expected false when status is prOpen")
	}

	regErr := &mockPRRegistry{
		buildLifecycleErr: errors.New("db error"),
	}
	srvErr := &RouterServer{registry: regErr}
	if srvErr.isPRRegistryCIFailed(context.Background(), "azylman/aerial", 42) {
		t.Errorf("expected false when registry returns error")
	}
}

func TestHandleCIFailureByPRNumber_Fallback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reg := &mockPRRegistry{
		resolveTargetID: "100000000000000001",
		buildLifecycle: PRBuildLifecycle{
			Status: prCIFailed,
		},
		ciFailedUpdated: false,
	}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	srv.bus = &mockEventBus{}
	srv.registry = reg

	res := &ResolvedGitHubEvent{
		Event:      ghEventCheckRun,
		Repo:       "azylman/aerial",
		PRNumber:   42,
		HeadSHA:    "sha-head",
		CheckName:  "test-check",
		Conclusion: conclusionFailure,
	}
	handled, err := srv.handleCIFailureByPRNumber(ctx, res, "ci failed", "https://check.url", false, false)
	if err != nil || !handled {
		t.Fatalf("handleCIFailureByPRNumber() = (%v, %v), want (true, nil)", handled, err)
	}
}

func TestRollbackFailedCheckRun(t *testing.T) {
	t.Parallel()
	srv := &RouterServer{}

	// Guards handle invalid inputs without panic
	srv.rollbackFailedCheckRun("azylman/aerial", 0, "sha123", "build", false, false)
	srv.rollbackFailedCheckRun("azylman/aerial", 42, "sha123", "", false, false)
	srv.rollbackFailedCheckRun("azylman/aerial", 42, "sha123", "build", true, false)
	srv.rollbackFailedCheckRun("azylman/aerial", 42, "sha123", "build", false, false)

	// Test roborev check rollback asserts activeFailed and everFailed state
	resRR := &ResolvedGitHubEvent{
		Event:     ghEventCheckRun,
		Repo:      "azylman/aerial",
		PRNumber:  42,
		HeadSHA:   "sha123",
		CheckName: "roborev",
	}
	srv.recordNonGateCheckFailureDetailed(resRR)
	entryRR := srv.loadMatchingFailedCheckState("azylman/aerial", 42, "sha123")
	if entryRR == nil || !entryRR.roboRevActiveFailed || !entryRR.everFailed["roborev"] {
		t.Fatalf("expected roborev activeFailed and everFailed before rollback")
	}
	srv.rollbackFailedCheckRun("azylman/aerial", 42, "sha123", "roborev", false, false)
	if entryRR.roboRevActiveFailed || entryRR.everFailed["roborev"] {
		t.Errorf("expected roborev activeFailed and everFailed cleared after rollback")
	}

	// Test non-roborev check rollback asserts activeFailed, everFailed, and nonRoboRevFailedSHAs state
	resNonRR := &ResolvedGitHubEvent{
		Event:     ghEventCheckRun,
		Repo:      "azylman/aerial",
		PRNumber:  43,
		HeadSHA:   "sha123",
		CheckName: "build",
	}
	srv.recordNonGateCheckFailureDetailed(resNonRR)
	entryNonRR := srv.loadMatchingFailedCheckState("azylman/aerial", 43, "sha123")
	if entryNonRR == nil || !entryNonRR.activeFailed["build"] || !entryNonRR.everFailed["build"] {
		t.Fatalf("expected build activeFailed and everFailed before rollback")
	}
	if sha, ok := srv.nonRoboRevFailedSHAs.Load(prCheckKey("azylman/aerial", 43)); !ok || sha != "sha123" {
		t.Fatalf("expected nonRoboRevFailedSHAs to record sha123 before rollback")
	}
	srv.rollbackFailedCheckRun("azylman/aerial", 43, "sha123", "build", false, false)
	if entryNonRR.activeFailed["build"] || entryNonRR.everFailed["build"] {
		t.Errorf("expected build activeFailed and everFailed cleared after rollback")
	}
	if _, ok := srv.nonRoboRevFailedSHAs.Load(prCheckKey("azylman/aerial", 43)); ok {
		t.Errorf("expected nonRoboRevFailedSHAs to be cleared after rolling back only active failure")
	}

	// Test hadPriorEverFailed preserves everFailed
	srv.recordNonGateCheckFailureDetailed(resNonRR)
	srv.rollbackFailedCheckRun("azylman/aerial", 43, "sha123", "build", false, true)
	if entryNonRR.activeFailed["build"] {
		t.Errorf("expected activeFailed cleared on rollback")
	}
	if !entryNonRR.everFailed["build"] {
		t.Errorf("expected everFailed preserved when hadPriorEverFailed was true")
	}
}

func TestProcessDeployEvent_LifecycleErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name       string
		evt        DeployEvent
		busErr     error
		promptErr  error
		wantErrSub string
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{err: tc.busErr}
			reg := &mockPRRegistry{resolveTargetID: "100000000000000001"}
			disp := &mockOutboundDispatcher{promptErr: tc.promptErr}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
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
