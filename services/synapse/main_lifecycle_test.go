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
	ctx := context.Background()

	tests := []isWaitingForImageBuildCase{
		{
			name:       "nil server returns false",
			serverNil:  true,
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "nil registry returns false",
			nilReg:     true,
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "empty repo returns false",
			repo:       "",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "zero prNumber returns false",
			repo:       "azylman/aerial",
			prNumber:   0,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "non-postgres registry returns false",
			mockReg:    true,
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "nil pool returns false",
			nilPool:    true,
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "query error returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			queryErr:   errors.New("db error"),
			wantResult: false,
		},
		{
			name:       "unsupported status returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "deploying",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "invalid metadata unmarshals to nil and returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{invalid json`,
			wantResult: false,
		},
		{
			name:       "started_jobs non-empty returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"started_jobs":["aerial-synapse"],"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "no build checks in metadata returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Go Unit Tests":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "open status with build checks returns true",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "open",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: true,
		},
		{
			name:       "merged status with build checks returns true",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: true,
		},
		{
			name:       "synced status with build checks returns true",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "synced",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runIsWaitingForImageBuildCase(ctx, t, tc)
		})
	}
}

type isWaitingForImageBuildCase struct {
	name       string
	serverNil  bool
	nilReg     bool
	mockReg    bool
	nilPool    bool
	repo       string
	prNumber   int
	status     string
	metadata   string
	queryErr   error
	wantResult bool
}

func newWaitingImageMockPool(status, metadata string, queryErr error) *mockSynapsePgxPool {
	return &mockSynapsePgxPool{
		queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
			if queryErr != nil {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					return queryErr
				}}
			}
			return mockSynapsePgxRow{scanFn: func(dest ...any) error {
				if len(dest) >= 2 {
					if s, ok := dest[0].(*string); ok {
						*s = status
					}
					if b, ok := dest[1].(*[]byte); ok && metadata != "" {
						*b = []byte(metadata)
					}
				}
				return nil
			}}
		},
	}
}

func runIsWaitingForImageBuildCase(ctx context.Context, t *testing.T, tc isWaitingForImageBuildCase) {
	t.Helper()
	if tc.serverNil {
		var nilSrv *RouterServer
		if got := nilSrv.isWaitingForImageBuild(ctx, tc.repo, tc.prNumber); got != tc.wantResult {
			t.Errorf("isWaitingForImageBuild() = %v, want %v", got, tc.wantResult)
		}
		return
	}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	switch {
	case tc.mockReg:
		srv.SetRegistry(&mockPRRegistry{})
	case tc.nilPool:
		srv.SetRegistry(NewPostgresPRRegistry(nil))
	case !tc.nilReg:
		srv.SetRegistry(NewPostgresPRRegistry(newWaitingImageMockPool(tc.status, tc.metadata, tc.queryErr)))
	}
	got := srv.isWaitingForImageBuild(ctx, tc.repo, tc.prNumber)
	if got != tc.wantResult {
		t.Errorf("isWaitingForImageBuild() = %v, want %v", got, tc.wantResult)
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
		name                 string
		initialUpdated       bool
		busErr               error
		wantErr              bool
		wantPublishedEvents  int
		wantDeployCalls      int
		wantUpdateStatusRuns int
	}{
		{
			name:                 "first attempt succeeds and publishes event",
			initialUpdated:       true,
			wantPublishedEvents:  1,
			wantDeployCalls:      1,
			wantUpdateStatusRuns: 1,
		},
		{
			name:                 "transient bus failure propagates error and does not mark published",
			initialUpdated:       true,
			busErr:               errors.New("transient bus disconnect"),
			wantErr:              true,
			wantPublishedEvents:  0,
			wantDeployCalls:      1,
			wantUpdateStatusRuns: 0,
		},
		{
			name:                 "deduplicated delivery returns nil without publishing duplicate event",
			initialUpdated:       false,
			wantPublishedEvents:  0,
			wantDeployCalls:      1,
			wantUpdateStatusRuns: 0,
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
			if len(reg.updatePRStatusCalls) != tc.wantUpdateStatusRuns {
				t.Errorf("got %d updatePRStatusCalls, want %d", len(reg.updatePRStatusCalls), tc.wantUpdateStatusRuns)
			}
		})
	}
}
