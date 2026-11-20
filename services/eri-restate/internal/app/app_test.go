package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

func WithStore(store db.Store, txStore db.TranscriptStore) AppOption {
	return func(o *appOptions) {
		o.store = store
		o.txStore = txStore
	}
}

func WithDaemonSpawner(spawner runner.DaemonSpawner) AppOption {
	return func(o *appOptions) {
		o.processSpawner = spawner
	}
}

func WithLLMFunc(fn runner.LLMFunc) AppOption {
	return func(o *appOptions) {
		o.llmFn = fn
	}
}

func WithAppConfig(cfg *config.Config) AppOption {
	return func(o *appOptions) {
		o.appConfig = cfg
	}
}

func (t *ReloadTrigger) Done() <-chan struct{} {
	return t.done
}

func TestNewEriRestateServer(t *testing.T) {
	t.Parallel()

	srv := NewEriRestateServer(nil, "aerial")
	require.NotNil(t, srv)
	h, err := srv.Handler()
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/discover", http.NoBody)
	req.Header.Set("Accept", "application/vnd.restate.endpointmanifest.v3+json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ThreadSession")
	assert.Contains(t, rec.Body.String(), "DispatchSession")
	assert.Contains(t, rec.Body.String(), "VoiceSession")
	assert.Contains(t, rec.Body.String(), "SessionManager")
}

func TestDiscoveryTimeoutReconciliationAndH2C(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	reconcileCount := 0
	adminServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reconcileCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer adminServer.Close()

	var h2cProtocols http.Protocols
	h2cProtocols.SetHTTP1(true)
	h2cProtocols.SetUnencryptedHTTP2(true)

	srv := NewEriRestateServer(nil, "aerial")
	rawHandler, err := srv.Handler()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trigger := startDiscoveryTimeoutReconciler(ctx, adminServer.URL, "aerial", adminServer.Client(), 3, 50*time.Millisecond)
	wrappedHandler := wrapDiscoveryTimeoutReconciler(rawHandler, trigger.Trigger)

	server := httptest.NewUnstartedServer(wrappedHandler)
	server.Config.Protocols = &h2cProtocols
	server.Start()
	defer server.Close()

	h2cClient := server.Client()
	if tr, ok := h2cClient.Transport.(*http.Transport); ok {
		var clientProtocols http.Protocols
		clientProtocols.SetUnencryptedHTTP2(true)
		tr.Protocols = &clientProtocols
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/discover", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/vnd.restate.endpointmanifest.v3+json")

	resp, err := h2cClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "HTTP/2.0", resp.Proto)

	// Verify that the /discover request triggered timeout reconciliation against adminServer
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return reconcileCount >= 3 // 3 services reconciled
	}, 3*time.Second, 50*time.Millisecond)
}

type errRoundTripper struct {
	err error
}

func (e *errRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, e.err
}

type timeoutReconcileCase struct {
	name           string
	agent          string
	adminURL       string
	statusCode     int
	returnBody     string
	transportError bool
	wantErrorSub   string
	wantCallCount  int
	wantPaths      []string
}

func runTimeoutReconcileCase(t *testing.T, tc timeoutReconcileCase) {
	t.Helper()
	t.Parallel()

	var mu sync.Mutex
	recordedPaths := make([]string, 0)
	recordedBodies := make([]string, 0)
	recordedMethods := make([]string, 0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		recordedPaths = append(recordedPaths, r.URL.Path)
		recordedMethods = append(recordedMethods, r.Method)
		bodyBytes, _ := io.ReadAll(r.Body)
		recordedBodies = append(recordedBodies, string(bodyBytes))
		mu.Unlock()

		w.WriteHeader(tc.statusCode)
		_, _ = w.Write([]byte(tc.returnBody))
	}))
	defer ts.Close()

	targetURL := ts.URL
	if tc.adminURL != "" {
		targetURL = tc.adminURL
	}
	client := ts.Client()
	if tc.transportError {
		client = &http.Client{
			Transport: &errRoundTripper{err: assert.AnError},
		}
	}

	err := ReconcileRestateServiceTimeouts(context.Background(), targetURL, tc.agent, client)
	if tc.wantErrorSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantErrorSub)
		return
	}

	require.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, tc.wantCallCount, len(recordedPaths))
	if len(tc.wantPaths) > 0 {
		assert.Equal(t, tc.wantPaths, recordedPaths)
	}
	for i := range recordedPaths {
		assert.Equal(t, http.MethodPatch, recordedMethods[i])
		var p struct {
			InactivityTimeout string `json:"inactivity_timeout"`
			AbortTimeout      string `json:"abort_timeout"`
		}
		uErr := json.Unmarshal([]byte(recordedBodies[i]), &p)
		require.NoError(t, uErr)
		assert.Equal(t, "1h", p.InactivityTimeout)
		assert.Equal(t, "1h", p.AbortTimeout)
	}
}

func TestReconcileRestateServiceTimeouts(t *testing.T) {
	t.Parallel()

	tests := []timeoutReconcileCase{
		{
			name:          "successful timeout reconciliation for all 3 services",
			agent:         "aerial",
			statusCode:    http.StatusOK,
			returnBody:    `{"status":"ok"}`,
			wantCallCount: 3,
			wantPaths: []string{
				"/services/ThreadSession",
				"/services/DispatchSession",
				"/services/VoiceSession",
			},
		},
		{
			name:          "successful timeout reconciliation with namespaced agent",
			agent:         "agent-alpha",
			statusCode:    http.StatusOK,
			returnBody:    `{"status":"ok"}`,
			wantCallCount: 3,
			wantPaths: []string{
				"/services/ThreadSession-agent-alpha",
				"/services/DispatchSession-agent-alpha",
				"/services/VoiceSession-agent-alpha",
			},
		},
		{
			name:          "admin returns 500 error",
			agent:         "aerial",
			statusCode:    http.StatusInternalServerError,
			returnBody:    `{"error":"internal"}`,
			wantErrorSub:  "failed with status 500",
			wantCallCount: 1,
		},
		{
			name:           "http transport error",
			agent:          "aerial",
			transportError: true,
			wantErrorSub:   "patch service",
			wantCallCount:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runTimeoutReconcileCase(t, tc)
		})
	}
}

type configEnvCase struct {
	name            string
	env             map[string]string
	wantRestatePort string
	wantHealthPort  string
	wantAdminURL    string
	wantAgentName   string
}

func runConfigEnvCase(t *testing.T, tc configEnvCase) {
	t.Helper()
	for k, v := range tc.env {
		t.Setenv(k, v)
	}
	cfg := LoadConfigFromEnv()
	assert.Equal(t, tc.wantRestatePort, cfg.RestatePort)
	assert.Equal(t, tc.wantHealthPort, cfg.HealthPort)
	assert.Equal(t, tc.wantAdminURL, cfg.AdminURL)
	assert.Equal(t, tc.wantAgentName, cfg.AgentName)
}

func TestLoadConfigFromEnv(t *testing.T) {
	tests := []configEnvCase{
		{
			name:            "default values",
			env:             map[string]string{"RESTATE_PORT": "", "PORT": "", "HEALTH_PORT": "", "RESTATE_ADMIN_URL": "", "AGENT_NAME": ""},
			wantRestatePort: "9080",
			wantHealthPort:  "4028",
			wantAdminURL:    "http://restate.aerial:9070",
			wantAgentName:   "aerial",
		},
		{
			name:            "PORT fallback for restate port",
			env:             map[string]string{"RESTATE_PORT": "", "PORT": "9081", "HEALTH_PORT": "4029", "RESTATE_ADMIN_URL": "http://admin:9070/", "AGENT_NAME": "agent-alpha"},
			wantRestatePort: "9081",
			wantHealthPort:  "4029",
			wantAdminURL:    "http://admin:9070",
			wantAgentName:   "agent-alpha",
		},
		{
			name:            "explicit environment overrides",
			env:             map[string]string{"RESTATE_PORT": "9090", "HEALTH_PORT": "4030", "RESTATE_ADMIN_URL": "http://restate-custom:9070", "AGENT_NAME": "agent-beta"},
			wantRestatePort: "9090",
			wantHealthPort:  "4030",
			wantAdminURL:    "http://restate-custom:9070",
			wantAgentName:   "agent-beta",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runConfigEnvCase(t, tc)
		})
	}
}

type healthEndpointCase struct {
	name          string
	method        string
	path          string
	body          string
	llmResponse   string
	llmError      error
	withNilLLM    bool
	withUsage     bool
	wantStatus    int
	wantBodySub   string
	wantHeaderKey string
	wantHeaderVal string
}

func runHealthEndpointCase(t *testing.T, tc healthEndpointCase) {
	t.Helper()
	t.Parallel()

	if tc.withUsage {
		metrics.RecordClaudeUsageJSON([]byte(`{"five_hour":{"utilization":50,"resets_at":"2026-11-20T12:00:00Z"}}`))
	}

	var llmFn runner.LLMFunc
	if !tc.withNilLLM {
		llmFn = func(ctx context.Context, model, prompt string) (string, error) {
			if tc.llmError != nil {
				return "", tc.llmError
			}
			return tc.llmResponse, nil
		}
	}

	reloadTrigger := NewReloadTrigger(context.Background(), func(source string) {})
	mux := SetupHealthMux(HealthMuxOptions{
		AgentName:     "aerial",
		LLMFunc:       llmFn,
		ReloadTrigger: reloadTrigger,
	})

	req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
	if tc.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, tc.wantStatus, rec.Code)
	if tc.wantBodySub != "" {
		assert.Contains(t, rec.Body.String(), tc.wantBodySub)
	}
	if tc.wantHeaderKey != "" {
		assert.Equal(t, tc.wantHeaderVal, rec.Header().Get(tc.wantHeaderKey))
	}
}

func TestSetupHealthMux(t *testing.T) {
	t.Parallel()

	tests := []healthEndpointCase{
		{
			name:        "GET /health returns 200 ok",
			method:      http.MethodGet,
			path:        "/health",
			wantStatus:  http.StatusOK,
			wantBodySub: `{"status":"ok"}`,
		},
		{
			name:        "GET /healthz returns 200 ok",
			method:      http.MethodGet,
			path:        "/healthz",
			wantStatus:  http.StatusOK,
			wantBodySub: `{"status":"ok"}`,
		},
		{
			name:        "GET /internal/quota returns 200 array",
			method:      http.MethodGet,
			path:        "/internal/quota",
			wantStatus:  http.StatusOK,
			wantBodySub: `[`,
		},
		{
			name:        "GET /internal/quota with usage populated",
			method:      http.MethodGet,
			path:        "/internal/quota",
			withUsage:   true,
			wantStatus:  http.StatusOK,
			wantBodySub: `Claude`,
		},
		{
			name:          "POST /internal/quota returns 405",
			method:        http.MethodPost,
			path:          "/internal/quota",
			wantStatus:    http.StatusMethodNotAllowed,
			wantHeaderKey: "Allow",
			wantHeaderVal: "GET",
		},
		{
			name:        "POST /internal/llm generates response",
			method:      http.MethodPost,
			path:        "/internal/llm",
			body:        `{"prompt":"summarize text"}`,
			llmResponse: "summary result",
			wantStatus:  http.StatusOK,
			wantBodySub: `{"response":"summary result"}`,
		},
		{
			name:          "GET /internal/llm returns 405",
			method:        http.MethodGet,
			path:          "/internal/llm",
			wantStatus:    http.StatusMethodNotAllowed,
			wantHeaderKey: "Allow",
			wantHeaderVal: "POST",
		},
		{
			name:        "POST /internal/llm empty prompt returns 400",
			method:      http.MethodPost,
			path:        "/internal/llm",
			body:        `{"prompt":"   "}`,
			wantStatus:  http.StatusBadRequest,
			wantBodySub: "prompt is required",
		},
		{
			name:        "POST /internal/llm invalid json returns 400",
			method:      http.MethodPost,
			path:        "/internal/llm",
			body:        `not-json`,
			wantStatus:  http.StatusBadRequest,
			wantBodySub: "invalid json payload",
		},
		{
			name:        "POST /internal/llm nil llm returns 503",
			method:      http.MethodPost,
			path:        "/internal/llm",
			body:        `{"prompt":"summarize"}`,
			withNilLLM:  true,
			wantStatus:  http.StatusServiceUnavailable,
			wantBodySub: "llm function not available",
		},
		{
			name:        "POST /internal/llm execution error returns 500",
			method:      http.MethodPost,
			path:        "/internal/llm",
			body:        `{"prompt":"summarize"}`,
			llmError:    errors.New("backend timeout"),
			wantStatus:  http.StatusInternalServerError,
			wantBodySub: "backend timeout",
		},
		{
			name:        "POST /internal/llm body too large returns 413",
			method:      http.MethodPost,
			path:        "/internal/llm",
			body:        strings.Repeat("a", (1<<20)+1024),
			wantStatus:  http.StatusRequestEntityTooLarge,
			wantBodySub: "request body too large",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runHealthEndpointCase(t, tc)
		})
	}
}

type dummyAgentPool struct {
	initErr error
	initRan chan struct{}
}

func (d *dummyAgentPool) Initialize(ctx context.Context) error {
	if d.initRan != nil {
		close(d.initRan)
	}
	return d.initErr
}

func (*dummyAgentPool) AcquireLease(ctx context.Context, targetKey string) (runner.SessionLease, error) {
	return nil, nil
}

func (*dummyAgentPool) Close() error {
	return nil
}

func TestPrewarmProcessPoolsSequentially(t *testing.T) {
	t.Parallel()

	pool1 := &dummyAgentPool{}
	pool2 := &dummyAgentPool{initErr: errors.New("transient error"), initRan: make(chan struct{})}

	ctx := t.Context()

	PrewarmProcessPoolsSequentially(ctx, pool1, nil, pool2)
	select {
	case <-pool2.initRan:
	case <-time.After(2 * time.Second):
		t.Fatal("pre-warming did not finish")
	}
}

func TestRunApp_CanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	tmpDir := t.TempDir()
	appCfg := config.NewFromData(&config.ConfigData{
		DatabaseURL:   "sqlite://" + tmpDir + "/test.db",
		GeminiHomeDir: tmpDir + "/home",
		DataDir:       tmpDir + "/data",
		Port:          "4028",
		BotID:         "bot-1",
		SynapseURL:    "http://synapse.aerial:4020",
		VanadisURL:    "http://vanadis.aerial:4025",
		CredentialSources: config.CredentialSources{
			OAuth: config.CredentialSource{Disabled: true},
			Codex: config.CredentialSource{Disabled: true},
			Muse:  config.CredentialSource{Disabled: true},
		},
	})

	fakeStore := dbtest.NewFakeStore()

	cfg := Config{
		RestatePort: "19080",
		HealthPort:  "14028",
		AdminURL:    "http://127.0.0.1:19070",
		AgentName:   "aerial-test",
	}

	err := RunApp(ctx, cfg,
		WithStore(fakeStore, nil),
		WithAppConfig(appCfg),
		WithDaemonSpawner(&dummySpawner{}),
		WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			return "test", nil
		}),
	)
	assert.NoError(t, err)

	// Test DiscordSession and EventBus injection in RunApp
	bus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = bus.Close() })
	err = RunApp(ctx, cfg,
		WithStore(fakeStore, nil),
		WithAppConfig(appCfg),
		WithDaemonSpawner(&dummySpawner{}),
		WithDiscordSession(&discordgo.Session{}),
		WithEventBus(bus),
	)
	assert.NoError(t, err)

	// Test config DiscordToken and NATSURL in RunApp
	appCfgWithTokens := config.NewFromData(&config.ConfigData{
		DatabaseURL:   "sqlite://" + tmpDir + "/test.db",
		GeminiHomeDir: tmpDir + "/home",
		DataDir:       tmpDir + "/data",
		Port:          "4028",
		DiscordToken:  "test-discord-token",
		NATSURL:       "nats://127.0.0.1:65530",
		CredentialSources: config.CredentialSources{
			OAuth: config.CredentialSource{Disabled: true},
			Codex: config.CredentialSource{Disabled: true},
			Muse:  config.CredentialSource{Disabled: true},
		},
	})
	err = RunApp(ctx, cfg,
		WithStore(fakeStore, nil),
		WithAppConfig(appCfgWithTokens),
		WithDaemonSpawner(&dummySpawner{}),
	)
	assert.NoError(t, err)

	// Test database error branch when store is not injected
	cfgBadDB := Config{
		RestatePort: "19080",
		HealthPort:  "14028",
		AdminURL:    "http://127.0.0.1:19070",
		AgentName:   "aerial-test",
	}
	err = RunApp(ctx, cfgBadDB,
		WithAppConfig(appCfg),
		WithDaemonSpawner(&dummySpawner{}),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to initialize database")

	defaultReloadLogger("test")
}

type dummySpawner struct{}

func (*dummySpawner) Spawn(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
	return nil, nil, nil, nil, errors.New("dummy spawn")
}

func TestReloadTrigger(t *testing.T) {
	t.Parallel()

	called := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())

	trig := NewReloadTrigger(ctx, func(s string) {
		called <- s
	})
	require.NotNil(t, trig)
	assert.NotNil(t, trig.Done())

	trig.Trigger()
	select {
	case s := <-called:
		assert.Equal(t, "git-sync webhook", s)
	case <-time.After(2 * time.Second):
		t.Fatal("reload trigger did not fire")
	}

	cancel()
	trig.Trigger()
	<-trig.Done()

	var nilTrig *ReloadTrigger
	nilTrig.Trigger() // safe no-op
}

type usagePollerCase struct {
	name        string
	cur         *config.ConfigData
	wantStarted bool
}

func TestStartUsagePoller(t *testing.T) {
	tests := []usagePollerCase{
		{
			name:        "nil cur is safe no-op",
			cur:         nil,
			wantStarted: false,
		},
		{
			name:        "agy engine",
			cur:         &config.ConfigData{Engine: config.EngineAgy, AgyBin: "agy"},
			wantStarted: true,
		},
		{
			name:        "claude engine",
			cur:         &config.ConfigData{Engine: config.EngineClaude},
			wantStarted: true,
		},
		{
			name:        "codex engine",
			cur:         &config.ConfigData{Engine: config.EngineCodex},
			wantStarted: true,
		},
		{
			name:        "muse engine",
			cur:         &config.ConfigData{Engine: config.EngineMuse, MuseBin: "muse"},
			wantStarted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var started bool
			var gotInterval time.Duration
			var gotRunner metrics.UsageRunnerFunc
			var gotRecord func([]byte) error

			oldStarter := startUsagePollerFn
			startUsagePollerFn = func(ctx context.Context, interval time.Duration, run metrics.UsageRunnerFunc, record func([]byte) error) {
				started = true
				gotInterval = interval
				gotRunner = run
				gotRecord = record
			}
			defer func() { startUsagePollerFn = oldStarter }()

			tmpDir := t.TempDir()
			cur := tc.cur
			if cur != nil {
				cur.DataDir = tmpDir
				if cur.GetEngine() == config.EngineClaude {
					t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "mock-token")
				}
			}
			startUsagePoller(context.Background(), cur, tmpDir, []string{"A=B"})

			assert.Equal(t, tc.wantStarted, started)
			if tc.wantStarted {
				assert.Equal(t, 10*time.Minute, gotInterval)
				assert.NotNil(t, gotRunner)
				assert.NotNil(t, gotRecord)
			}
		})
	}
}

func TestReconcileRestateServiceTimeouts_DefaultAndInvalidURL(t *testing.T) {
	t.Parallel()

	// 1. Default URL (adminURL == "")
	client := &http.Client{Transport: &errRoundTripper{err: assert.AnError}}
	err := ReconcileRestateServiceTimeouts(context.Background(), "", "aerial", client)
	require.Error(t, err)

	// 2. Invalid URL path leading to request creation error
	err = ReconcileRestateServiceTimeouts(context.Background(), "http://localhost\x7f", "aerial", client)
	require.Error(t, err)
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("read error")
}

func TestSetupHealthMux_LoopbackReloadAndLLMFallback(t *testing.T) {
	t.Parallel()

	activeTurns := runner.NewActiveTurnRegistry()
	mcpRoutes := &env.MCPProxyRoutes{}

	reloadCalled := make(chan bool, 1)
	reloadTrigger := NewReloadTrigger(context.Background(), func(source string) {
		reloadCalled <- true
	})

	mux := SetupHealthMux(HealthMuxOptions{
		AgentName:     "aerial",
		MCPRoutes:     mcpRoutes,
		ActiveTurns:   activeTurns,
		ReloadTrigger: reloadTrigger,
		LLMFunc: func(ctx context.Context, model, prompt string) (string, error) {
			return "ok", nil
		},
	})

	// Test POST /-/reload from 127.0.0.1
	req := httptest.NewRequest(http.MethodPost, "/-/reload", http.NoBody)
	req.RemoteAddr = "127.0.0.1:54321"
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"status":"queued"`)

	// Test POST /internal/llm with errReader
	reqErr := httptest.NewRequest(http.MethodPost, "/internal/llm", errReader{})
	recErr := httptest.NewRecorder()
	mux.ServeHTTP(recErr, reqErr)
	assert.Equal(t, http.StatusBadRequest, recErr.Code)

	// Test with pool LLMFunc fallback
	pool := queue.New(nil, queue.WorkerPoolConfig{
		LLMFunc: func(ctx context.Context, model, prompt string) (string, error) {
			return "from-pool", nil
		},
	})
	muxWithPool := SetupHealthMux(HealthMuxOptions{
		AgentName: "aerial",
		Pool:      pool,
	})
	reqPool := httptest.NewRequest(http.MethodPost, "/internal/llm", strings.NewReader(`{"prompt":"test"}`))
	recPool := httptest.NewRecorder()
	muxWithPool.ServeHTTP(recPool, reqPool)
	assert.Equal(t, http.StatusOK, recPool.Code)
	assert.Contains(t, recPool.Body.String(), "from-pool")
}

func TestRunApp_ConfigLoadError(t *testing.T) {
	oldPaths := config.ConfigSearchPaths
	config.ConfigSearchPaths = []string{filepath.Join(t.TempDir(), "nonexistent.yaml")}
	t.Cleanup(func() { config.ConfigSearchPaths = oldPaths })

	cfg := Config{
		RestatePort: "0",
		HealthPort:  "0",
		AgentName:   "aerial-test",
	}

	err := RunApp(t.Context(), cfg, WithStore(dbtest.NewFakeStore(), nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load configuration")
}

func TestRunApp_ListenFailures(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tmpDir := t.TempDir()
	appCfg := config.NewFromData(&config.ConfigData{
		DatabaseURL:   "sqlite://" + tmpDir + "/test.db",
		GeminiHomeDir: tmpDir + "/home",
		DataDir:       tmpDir + "/data",
		Port:          "4028",
		CredentialSources: config.CredentialSources{
			OAuth: config.CredentialSource{Disabled: true},
			Codex: config.CredentialSource{Disabled: true},
			Muse:  config.CredentialSource{Disabled: true},
		},
	})

	fakeStore := dbtest.NewFakeStore()

	// Invalid RestatePort
	cfgInvalidRestate := Config{
		RestatePort: "-1",
		HealthPort:  "14028",
		AdminURL:    "http://127.0.0.1:19070",
		AgentName:   "aerial-test",
	}
	err := RunApp(ctx, cfgInvalidRestate, WithStore(fakeStore, nil), WithAppConfig(appCfg), WithDaemonSpawner(&dummySpawner{}))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "listen on restate port")

	// Invalid HealthPort
	cfgInvalidHealth := Config{
		RestatePort: "0",
		HealthPort:  "-1",
		AdminURL:    "http://127.0.0.1:19070",
		AgentName:   "aerial-test",
	}
	err = RunApp(ctx, cfgInvalidHealth, WithStore(fakeStore, nil), WithAppConfig(appCfg), WithDaemonSpawner(&dummySpawner{}))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "listen on health port")
}

func TestInitDatabaseAndStores(t *testing.T) {
	tests := []struct {
		name       string
		dbURL      string
		stack      string
		openFn     func(*config.Config) (*sql.DB, error)
		wantErrSub string
	}{
		{
			name:       "DBOpenError",
			dbURL:      "",
			wantErrSub: "failed to initialize database",
		},
		{
			name:  "ResolveTranscriptStackError",
			dbURL: "",
			stack: "",
			openFn: func(*config.Config) (*sql.DB, error) {
				return sql.Open("pgx", "postgres://localhost/test")
			},
			wantErrSub: "failed to resolve transcript stack",
		},
		{
			name:  "Success",
			dbURL: "postgres://localhost/test",
			stack: "aerial-test",
			openFn: func(*config.Config) (*sql.DB, error) {
				return sql.Open("pgx", "postgres://localhost/test")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.openFn != nil {
				oldOpen := openDefaultDB
				openDefaultDB = tc.openFn
				defer func() { openDefaultDB = oldOpen }()
			}
			appCfg := config.NewFromData(&config.ConfigData{
				DatabaseURL: tc.dbURL,
				Transcripts: config.TranscriptsConfig{Stack: tc.stack},
			})
			cur := appCfg.Current()
			dbConn, s, tx, err := initDatabaseAndStores(context.Background(), appCfg, cur, "")
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, dbConn)
			assert.NotNil(t, s)
			assert.NotNil(t, tx)
			_ = dbConn.Close()
		})
	}
}

func TestRunApp_DefaultDatabaseInitializationAndShutdown(t *testing.T) {
	t.Run("InitDBError", func(t *testing.T) {
		tmpDir := t.TempDir()
		appCfg := config.NewFromData(&config.ConfigData{
			DatabaseURL:   "",
			GeminiHomeDir: tmpDir + "/home",
			DataDir:       tmpDir + "/data",
		})

		cfg := Config{
			RestatePort: "0",
			HealthPort:  "0",
			AgentName:   "aerial-test",
		}

		err := RunApp(t.Context(), cfg, WithAppConfig(appCfg))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to initialize database")
	})

	t.Run("GracefulShutdownWithDefaultStore", func(t *testing.T) {
		oldOpen := openDefaultDB
		openDefaultDB = func(*config.Config) (*sql.DB, error) {
			return sql.Open("pgx", "postgres://localhost/test")
		}
		defer func() { openDefaultDB = oldOpen }()

		ctx, cancel := context.WithCancel(t.Context())

		tmpDir := t.TempDir()
		appCfg := config.NewFromData(&config.ConfigData{
			DatabaseURL:   "postgres://localhost/test",
			GeminiHomeDir: tmpDir + "/home",
			DataDir:       tmpDir + "/data",
			Port:          "4028",
			Transcripts:   config.TranscriptsConfig{Stack: "aerial-test"},
			CredentialSources: config.CredentialSources{
				OAuth: config.CredentialSource{Disabled: true},
				Codex: config.CredentialSource{Disabled: true},
				Muse:  config.CredentialSource{Disabled: true},
			},
		})

		cfg := Config{
			RestatePort: "0",
			HealthPort:  "0",
			AdminURL:    "http://127.0.0.1:19070",
			AgentName:   "aerial-test",
		}

		ready := make(chan struct{})
		oldReady := onServerReady
		onServerReady = func(_, _ string) {
			close(ready)
		}
		defer func() { onServerReady = oldReady }()

		go func() {
			<-ready
			cancel()
		}()

		err := RunApp(ctx, cfg, WithAppConfig(appCfg), WithDaemonSpawner(&dummySpawner{}))
		require.NoError(t, err)
	})
}

func TestReloadTrigger_ContextCanceledEarly(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	trigger := NewReloadTrigger(ctx, func(string) {})
	cancel()
	trigger.Trigger()
	select {
	case <-trigger.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for trigger.Done()")
	}
}

func TestWrapDiscoveryTimeoutReconciler_NonDiscoverPath(t *testing.T) {
	t.Parallel()
	called := false
	trigger := func() { called = true }
	handler := wrapDiscoveryTimeoutReconciler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), trigger)

	req := httptest.NewRequest(http.MethodGet, "/other", http.NoBody)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.False(t, called)

	nilWrapped := wrapDiscoveryTimeoutReconciler(handler, nil)
	assert.NotNil(t, nilWrapped)
}

func TestWireOutboundIntegrations(t *testing.T) {
	t.Parallel()

	pool := queue.New(nil, queue.WorkerPoolConfig{})
	cur := &config.ConfigData{
		DiscordToken: "token-1",
		NATSURL:      "nats://127.0.0.1:65530",
	}

	// 1. With explicit discord session and bus
	s := &discordgo.Session{}
	bus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = bus.Close() })
	closeFn := wireOutboundIntegrations(context.Background(), cur, pool, appOptions{
		discordSession: s,
		eventBus:       bus,
	})
	assert.Nil(t, closeFn)
	assert.Equal(t, s, pool.DiscordSession())
	assert.Equal(t, bus, pool.EventBus())

	// 2. With token and NATS URL
	pool2 := queue.New(nil, queue.WorkerPoolConfig{})
	closeFn2 := wireOutboundIntegrations(context.Background(), cur, pool2, appOptions{})
	assert.NotNil(t, pool2.DiscordSession())
	if closeFn2 != nil {
		assert.NotNil(t, pool2.EventBus())
		closeFn2()
	}
}

type reloadFuncTestCase struct {
	name          string
	withAppCfg    bool
	corruptConfig bool
	withProv      bool
	provSyncError bool
	withEphem     bool
	withWorker    bool
}

func TestCreateReloadFunc(t *testing.T) {
	tests := []reloadFuncTestCase{
		{
			name: "all nil components is safe no-op",
		},
		{
			name:          "corrupt config fails and returns early",
			withAppCfg:    true,
			corruptConfig: true,
			withEphem:     true,
			withWorker:    true,
		},
		{
			name:       "successful reload marks pools dirty",
			withAppCfg: true,
			withProv:   true,
			withEphem:  true,
			withWorker: true,
		},
		{
			name:          "provisioner sync error logs warning and continues",
			withAppCfg:    true,
			withProv:      true,
			provSyncError: true,
			withEphem:     true,
			withWorker:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()

			var appCfg *config.Config
			if tc.withAppCfg {
				cfgPath := filepath.Join(tmpDir, "config.yaml")
				switch {
				case tc.corruptConfig:
					_ = os.WriteFile(cfgPath, []byte("invalid: yaml: ["), 0o644)
				case tc.provSyncError:
					invalidCredYAML := "model: test-model\ntimezone: UTC\nchannels:\n  default:\n    mode: threads\ncredential_sources:\n  oauth:\n    path: /fake/oauth\n    disabled: true\n  codex:\n    disabled: true\n  muse:\n    disabled: true\n"
					_ = os.WriteFile(cfgPath, []byte(invalidCredYAML), 0o644)
				default:
					validYAML := "model: test-model\ntimezone: UTC\nchannels:\n  default:\n    mode: threads\ncredential_sources:\n  oauth:\n    disabled: true\n  codex:\n    disabled: true\n  muse:\n    disabled: true\n"
					_ = os.WriteFile(cfgPath, []byte(validYAML), 0o644)
				}
				t.Setenv("CONFIG_PATH", cfgPath)

				appCfg = config.NewFromData(&config.ConfigData{
					DataDir:       tmpDir,
					GeminiHomeDir: tmpDir,
					CredentialSources: config.CredentialSources{
						OAuth: config.CredentialSource{Disabled: true},
						Codex: config.CredentialSource{Disabled: true},
						Muse:  config.CredentialSource{Disabled: true},
					},
				})
			}

			var prov *env.Provisioner
			if tc.withProv && appCfg != nil {
				prov, _ = env.NewFromConfig(appCfg)
			}

			var ephemPool *runner.InterchangeablePool
			if tc.withEphem {
				underlying := runner.NewUnifiedProcessPool(runner.PoolConfig{
					GeminiHomeDir: tmpDir,
					Cwd:           tmpDir,
				}, &dummySpawner{})
				ephemPool = runner.NewInterchangeablePool(underlying, runner.InterchangeablePoolConfig{
					WorkerCount: 1,
					KeyPrefix:   "ephem:test",
				})
				t.Cleanup(func() { _ = ephemPool.Close() })
			}

			var workerPool *queue.WorkerPool
			if tc.withWorker {
				workerPool = queue.New(appCfg, queue.WorkerPoolConfig{
					ParentContext: ctx,
					Store:         dbtest.NewFakeStore(),
				})
			}

			fn := createReloadFunc(ctx, appCfg, prov, ephemPool, workerPool)
			require.NotNil(t, fn)
			fn("test-source")

			if tc.withAppCfg && !tc.corruptConfig {
				assert.Equal(t, "test-model", appCfg.Current().Model)
			}
		})
	}
}

func TestStartDiscoveryTimeoutReconciler_ContextCanceledDuringRetry(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	called := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		select {
		case <-called:
		default:
			close(called)
		}
	}))
	defer ts.Close()

	trigger := startDiscoveryTimeoutReconciler(ctx, ts.URL, "aerial", ts.Client(), 3, 500*time.Millisecond)
	trigger.Trigger()
	<-called
	cancel() // cancel during retry delay
	select {
	case <-trigger.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for trigger.Done()")
	}
}
