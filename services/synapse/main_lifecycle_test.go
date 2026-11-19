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

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
)

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
	if sha, ok := srv.GHState().GetNonRoboRevFailedSHA("azylman/aerial", 42); !ok || sha != "sha123" {
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
	active := srv.GHState().HasActiveCheckRun(repo, prNumber, headSHA, checkName)
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
	if !srv.GHState().HasActiveRoboRevFailure("azylman/aerial", 42, "sha123") || !srv.GHState().HasEverFailedCheckRun("azylman/aerial", 42, "sha123", "roborev") {
		t.Fatalf("expected roborev activeFailed and everFailed before rollback")
	}
	srv.rollbackFailedCheckRun("azylman/aerial", 42, "sha123", "roborev", false, false)
	if srv.GHState().HasActiveRoboRevFailure("azylman/aerial", 42, "sha123") || srv.GHState().HasEverFailedCheckRun("azylman/aerial", 42, "sha123", "roborev") {
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
	if !srv.GHState().HasActiveCheckRun("azylman/aerial", 43, "sha123", "build") || !srv.GHState().HasEverFailedCheckRun("azylman/aerial", 43, "sha123", "build") {
		t.Fatalf("expected build activeFailed and everFailed before rollback")
	}
	if sha, ok := srv.GHState().GetNonRoboRevFailedSHA("azylman/aerial", 43); !ok || sha != "sha123" {
		t.Fatalf("expected nonRoboRevFailedSHAs to record sha123 before rollback")
	}
	srv.rollbackFailedCheckRun("azylman/aerial", 43, "sha123", "build", false, false)
	if srv.GHState().HasActiveCheckRun("azylman/aerial", 43, "sha123", "build") || srv.GHState().HasEverFailedCheckRun("azylman/aerial", 43, "sha123", "build") {
		t.Errorf("expected build activeFailed and everFailed cleared after rollback")
	}
	if _, ok := srv.GHState().GetNonRoboRevFailedSHA("azylman/aerial", 43); ok {
		t.Errorf("expected nonRoboRevFailedSHAs to be cleared after rolling back only active failure")
	}

	// Test hadPriorEverFailed preserves everFailed
	srv.recordNonGateCheckFailureDetailed(resNonRR)
	srv.rollbackFailedCheckRun("azylman/aerial", 43, "sha123", "build", false, true)
	if srv.GHState().HasActiveCheckRun("azylman/aerial", 43, "sha123", "build") {
		t.Errorf("expected activeFailed cleared on rollback")
	}
	if !srv.GHState().HasEverFailedCheckRun("azylman/aerial", 43, "sha123", "build") {
		t.Errorf("expected everFailed preserved when hadPriorEverFailed was true")
	}
}
