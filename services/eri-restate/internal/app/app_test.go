package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/eri/pkg/ambient"
	"github.com/azylman/aerial/services/eri/pkg/bootstrap"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
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

func TestNewEriRestateServer(t *testing.T) {
	t.Parallel()

	srv := NewEriRestateServer(nil, "aerial", nil)
	require.NotNil(t, srv)
	h, err := srv.Handler()
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/discover", http.NoBody)
	req.Header.Set("Accept", "application/vnd.restate.endpointmanifest.v3+json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	for _, svc := range []string{`"ThreadSession"`, `"DispatchSession"`, `"VoiceSession"`, `"SessionManager"`, `"TurnRunner"`, `"LLM"`} {
		assert.Contains(t, rec.Body.String(), svc)
	}
}

// boundHandlers maps "<service> <service type>" to each handler name and kind of defs.
func boundHandlers(defs []restate.ServiceDefinition) map[string]map[string]string {
	out := map[string]map[string]string{}
	for _, def := range defs {
		handlers := map[string]string{}
		for name, h := range def.Handlers() {
			kind := "-"
			if ht := h.HandlerType(); ht != nil {
				kind = fmt.Sprint(*ht)
			}
			handlers[name] = kind
		}
		out[def.Name()+" "+fmt.Sprint(def.Type())] = handlers
	}
	return out
}

func TestEriRestateDefinitions(t *testing.T) {
	t.Parallel()

	// Every revision binds exactly this set, so a deploy never drops a service or changes a
	// handler's name or kind on a deployment with in-flight invocations.
	want := func(suffix string) map[string]map[string]string {
		return map[string]map[string]string{
			"ThreadSession" + suffix + " VIRTUAL_OBJECT": {
				"Enqueue": "SHARED", "Append": "EXCLUSIVE", "OnTurnDone": "EXCLUSIVE", "Kick": "EXCLUSIVE",
				"CancelTurn": "SHARED", "ClearPending": "EXCLUSIVE", "Status": "SHARED",
			},
			"DispatchSession" + suffix + " VIRTUAL_OBJECT": {
				"Enqueue": "SHARED", "Append": "EXCLUSIVE", "OnTurnDone": "EXCLUSIVE", "Cancel": "SHARED",
				"ClearPending": "EXCLUSIVE", "Status": "SHARED",
			},
			"VoiceSession" + suffix + " VIRTUAL_OBJECT": {"Ask": "EXCLUSIVE"},
			"SessionManager" + suffix + " SERVICE":      {"CancelDispatch": "-"},
			"TurnRunner" + suffix + " SERVICE":          {"RunBurst": "-", "RunDispatch": "-"},
			"LLM" + suffix + " SERVICE":                 {"Complete": "-"},
		}
	}

	tests := []struct {
		name  string
		agent string
		want  map[string]map[string]string
	}{
		{name: "aerial", agent: "aerial", want: want("")},
		{name: "agent-alpha", agent: "agent-alpha", want: want("-agent-alpha")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, boundHandlers(eriRestateDefinitions(nil, tc.agent, nil)))
		})
	}
}

func TestDiscoveryManifestTimeoutsAndH2C(t *testing.T) {
	t.Parallel()

	var h2cProtocols http.Protocols
	h2cProtocols.SetHTTP1(true)
	h2cProtocols.SetUnencryptedHTTP2(true)

	srv := NewEriRestateServer(nil, "aerial", nil)
	rawHandler, err := srv.Handler()
	require.NoError(t, err)

	server := httptest.NewUnstartedServer(rawHandler)
	server.Config.Protocols = &h2cProtocols
	server.Start()
	defer server.Close()

	h2cClient := server.Client()
	if tr, ok := h2cClient.Transport.(*http.Transport); ok {
		var clientProtocols http.Protocols
		clientProtocols.SetUnencryptedHTTP2(true)
		tr.Protocols = &clientProtocols
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/discover", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/vnd.restate.endpointmanifest.v3+json")

	resp, err := h2cClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "HTTP/2.0", resp.Proto)

	var manifest struct {
		Services []struct {
			Name              string `json:"name"`
			InactivityTimeout int64  `json:"inactivityTimeout"`
			AbortTimeout      int64  `json:"abortTimeout"`
		} `json:"services"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&manifest))

	type timeouts struct {
		inactivity int64
		abort      int64
	}
	got := make(map[string]timeouts, len(manifest.Services))
	for _, s := range manifest.Services {
		got[s.Name] = timeouts{inactivity: s.InactivityTimeout, abort: s.AbortTimeout}
	}

	wantTurnHoldMs := queue.TurnHoldTimeout.Milliseconds()
	wantHourMs := time.Hour.Milliseconds()

	wantTimeouts := map[string]timeouts{
		"ThreadSession":   {inactivity: wantHourMs, abort: wantHourMs},
		"DispatchSession": {inactivity: wantHourMs, abort: wantHourMs},
		"VoiceSession":    {inactivity: wantHourMs, abort: wantHourMs},
		"SessionManager":  {inactivity: wantHourMs, abort: wantHourMs},
		"TurnRunner":      {inactivity: wantTurnHoldMs, abort: wantTurnHoldMs},
		"LLM":             {inactivity: wantHourMs, abort: wantHourMs},
	}
	assert.Equal(t, wantTimeouts, got)
}

type configEnvCase struct {
	name                   string
	env                    map[string]string
	wantRestatePort        string
	wantHealthPort         string
	wantPostgresSocketPath string
	wantAgentName          string
}

func runConfigEnvCase(t *testing.T, tc configEnvCase) {
	t.Helper()
	for k, v := range tc.env {
		t.Setenv(k, v)
	}
	cfg := LoadConfigFromEnv()
	assert.Equal(t, tc.wantRestatePort, cfg.RestatePort)
	assert.Equal(t, tc.wantHealthPort, cfg.HealthPort)
	assert.Equal(t, tc.wantPostgresSocketPath, cfg.PostgresSocketPath)
	assert.Equal(t, tc.wantAgentName, cfg.AgentName)
}

func TestLoadConfigFromEnv(t *testing.T) {
	tests := []configEnvCase{
		{
			name:                   "default values",
			env:                    map[string]string{"RESTATE_PORT": "", "PORT": "", "HEALTH_PORT": "", "POSTGRES_SOCKET_PATH": "", "AGENT_NAME": ""},
			wantRestatePort:        "9080",
			wantHealthPort:         "4028",
			wantPostgresSocketPath: "/var/run/postgresql/.s.PGSQL.5432",
			wantAgentName:          "aerial",
		},
		{
			name:                   "PORT fallback for restate port",
			env:                    map[string]string{"RESTATE_PORT": "", "PORT": "9081", "HEALTH_PORT": "4029", "POSTGRES_SOCKET_PATH": "", "AGENT_NAME": "agent-alpha"},
			wantRestatePort:        "9081",
			wantHealthPort:         "4029",
			wantPostgresSocketPath: "/var/run/postgresql/.s.PGSQL.5432",
			wantAgentName:          "agent-alpha",
		},
		{
			name:                   "explicit environment overrides",
			env:                    map[string]string{"RESTATE_PORT": "9090", "HEALTH_PORT": "4030", "POSTGRES_SOCKET_PATH": "/tmp/custom/.s.PGSQL.5432", "AGENT_NAME": "agent-beta"},
			wantRestatePort:        "9090",
			wantHealthPort:         "4030",
			wantPostgresSocketPath: "/tmp/custom/.s.PGSQL.5432",
			wantAgentName:          "agent-beta",
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
	withUsage     bool
	remoteAddr    string
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

	reloadTrigger := bootstrap.NewReloadTrigger(context.Background(), func(source string) {})
	mux := SetupHealthMux(HealthMuxOptions{
		AgentName:     "aerial",
		ReloadTrigger: reloadTrigger,
	})

	req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
	if tc.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if tc.remoteAddr != "" {
		req.RemoteAddr = tc.remoteAddr
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
			name:        "GET /internal/quota returns 200 snapshot object",
			method:      http.MethodGet,
			path:        "/internal/quota",
			wantStatus:  http.StatusOK,
			wantBodySub: `"groups":[`,
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
			name:        "GET /metrics serves the eri application registry",
			method:      http.MethodGet,
			path:        "/metrics",
			wantStatus:  http.StatusOK,
			wantBodySub: "aerial_brain_discord_typing_sessions_active",
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
			name:        "loopback GET /internal/github-api/ reaches proxy without PAT",
			method:      http.MethodGet,
			path:        "/internal/github-api/user",
			remoteAddr:  "127.0.0.1:41000",
			wantStatus:  http.StatusServiceUnavailable,
			wantBodySub: "GitHub PAT not configured",
		},
		{
			name:        "loopback POST /internal/github-git/ reaches proxy without PAT",
			method:      http.MethodPost,
			path:        "/internal/github-git/org/repo.git/git-receive-pack",
			remoteAddr:  "127.0.0.1:41000",
			wantStatus:  http.StatusServiceUnavailable,
			wantBodySub: "GitHub PAT not configured",
		},
		{
			name:        "pod-network GET /internal/github-api/ is forbidden",
			method:      http.MethodGet,
			path:        "/internal/github-api/user",
			remoteAddr:  "192.0.2.20:41000",
			wantStatus:  http.StatusForbidden,
			wantBodySub: "forbidden",
		},
		{
			name:        "pod-network POST /internal/github-git/ is forbidden",
			method:      http.MethodPost,
			path:        "/internal/github-git/org/repo.git/git-receive-pack",
			remoteAddr:  "192.0.2.20:41000",
			wantStatus:  http.StatusForbidden,
			wantBodySub: "forbidden",
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
		DatabaseURL:   pgtest.NewDSN(t),
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

	fakeStore := newTestStore(t)

	cfg := Config{
		RestatePort: "19080",
		HealthPort:  "14028",
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

	// Test config DiscordToken and NATSURL in RunApp
	appCfgWithTokens := config.NewFromData(&config.ConfigData{
		DatabaseURL:   pgtest.NewDSN(t),
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

	// Test provisioner sync error branch in RunApp
	appCfgBadSync := config.NewFromData(&config.ConfigData{
		DatabaseURL:   pgtest.NewDSN(t),
		GeminiHomeDir: tmpDir + "/home",
		DataDir:       "/proc/nonexistent-sync-dir/data",
		Port:          "4028",
		CredentialSources: config.CredentialSources{
			OAuth: config.CredentialSource{Disabled: true},
			Codex: config.CredentialSource{Disabled: true},
			Muse:  config.CredentialSource{Disabled: true},
		},
	})
	err = RunApp(ctx, cfg,
		WithStore(fakeStore, nil),
		WithAppConfig(appCfgBadSync),
		WithDaemonSpawner(&dummySpawner{}),
	)
	assert.NoError(t, err)

	// Test nil Current() config fallback and PostgresSocketPath startup + error warning branches
	sockTmp, sErr := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, sErr)
	cfgWithSocket := Config{
		RestatePort:        "19080",
		HealthPort:         "14028",
		PostgresSocketPath: filepath.Join(sockTmp, ".s.PGSQL.5432"),
		AgentName:          "aerial-test",
	}
	err = RunApp(ctx, cfgWithSocket,
		WithStore(fakeStore, nil),
		WithAppConfig(config.NewFromData(nil)),
		WithDaemonSpawner(&dummySpawner{}),
	)
	assert.NoError(t, err)

	badDir := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(badDir, []byte("x"), 0o600))
	cfgWithBadSocket := Config{
		RestatePort:        "19080",
		HealthPort:         "14028",
		PostgresSocketPath: filepath.Join(badDir, "sub", ".s.PGSQL.5432"),
		AgentName:          "aerial-test",
	}

	var logBuf bytes.Buffer
	origLogWriter := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(origLogWriter)

	err = RunApp(ctx, cfgWithBadSocket,
		WithStore(fakeStore, nil),
		WithAppConfig(appCfg),
		WithDaemonSpawner(&dummySpawner{}),
	)
	assert.NoError(t, err)
	assert.Contains(t, logBuf.String(), "Warning starting postgres socket proxy")

	// Test database error branch when store is not injected
	cfgBadDB := Config{
		RestatePort: "19080",
		HealthPort:  "14028",
		AgentName:   "aerial-test",
	}
	appCfgBadDB := config.NewFromData(&config.ConfigData{DatabaseURL: ""})
	err = RunApp(ctx, cfgBadDB,
		WithAppConfig(appCfgBadDB),
		WithDaemonSpawner(&dummySpawner{}),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to initialize database")
}

type dummySpawner struct{}

func (*dummySpawner) Spawn(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
	return nil, nil, nil, nil, errors.New("dummy spawn")
}

type usagePollerCase struct {
	name             string
	cur              *config.ConfigData
	wantStarted      bool
	proveDiscordHome bool
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
			name:             "muse engine",
			cur:              &config.ConfigData{Engine: config.EngineMuse, MuseBin: "muse"},
			wantStarted:      true,
			proveDiscordHome: true,
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

			ephemeralHome := t.TempDir()
			discordHome := t.TempDir()
			recordFile := filepath.Join(t.TempDir(), "home-record.txt")
			cur := tc.cur
			if cur != nil {
				cur.DataDir = ephemeralHome
				if cur.GetEngine() == config.EngineClaude {
					t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "mock-token")
				}
				if tc.proveDiscordHome {
					script := filepath.Join(t.TempDir(), "mock-muse.sh")
					scriptBody := "#!/bin/sh\nprintf 'HOME=%s\\nMUSE_AUTH_PATH=%s\\n' \"$HOME\" \"$MUSE_AUTH_PATH\" > \"" + recordFile + "\"\necho '{\"allowance\":{\"window\":{\"resetsAtMs\":1795037083000,\"usedPercent\":25}}}'\n"
					if err := os.WriteFile(script, []byte(scriptBody), 0o755); err != nil {
						t.Fatal(err)
					}
					cur.MuseBin = script
				}
			}
			startUsagePoller(context.Background(), cur, ephemeralHome, discordHome, []string{"A=B"})

			assert.Equal(t, tc.wantStarted, started)
			if tc.wantStarted {
				assert.Equal(t, 10*time.Minute, gotInterval)
				assert.NotNil(t, gotRunner)
				assert.NotNil(t, gotRecord)
			}
			if tc.proveDiscordHome {
				require.NotNil(t, gotRunner)
				out, err := gotRunner(context.Background())
				require.NoError(t, err)
				require.Contains(t, string(out), "allowance")
				recorded, err := os.ReadFile(recordFile)
				require.NoError(t, err, "fake MuseBin should record the HOME it ran under")
				assert.Contains(t, string(recorded), "HOME="+discordHome+"\n")
				assert.Contains(t, string(recorded), "MUSE_AUTH_PATH="+filepath.Join(discordHome, ".config", "muse", "auth.json"))
				assert.NotContains(t, string(recorded), ephemeralHome)
			}
		})
	}
}

func TestSetupHealthMux_LoopbackReload(t *testing.T) {
	t.Parallel()

	activeTurns := runner.NewActiveTurnRegistry()
	mcpRoutes := &env.MCPProxyRoutes{}

	reloadCalled := make(chan bool, 1)
	reloadTrigger := bootstrap.NewReloadTrigger(context.Background(), func(source string) {
		reloadCalled <- true
	})

	mux := SetupHealthMux(HealthMuxOptions{
		AgentName:     "aerial",
		MCPRoutes:     mcpRoutes,
		ActiveTurns:   activeTurns,
		ReloadTrigger: reloadTrigger,
	})

	// Test POST /-/reload from 127.0.0.1
	req := httptest.NewRequest(http.MethodPost, "/-/reload", http.NoBody)
	req.RemoteAddr = "127.0.0.1:54321"
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"status":"queued"`)
}

func TestRunApp_ConfigLoadError(t *testing.T) {
	// ResolveConfigSearchPaths prepends CONFIG_PATH; clear it so the real config never loads.
	t.Setenv("CONFIG_PATH", "")
	oldPaths := config.ConfigSearchPaths
	config.ConfigSearchPaths = []string{filepath.Join(t.TempDir(), "nonexistent.yaml")}
	t.Cleanup(func() { config.ConfigSearchPaths = oldPaths })

	cfg := Config{
		RestatePort: "0",
		HealthPort:  "0",
		AgentName:   "aerial-test",
	}

	err := RunApp(t.Context(), cfg, WithStore(newTestStore(t), nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load configuration")
}

func TestRunApp_ListenFailures(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tmpDir := t.TempDir()
	appCfg := config.NewFromData(&config.ConfigData{
		DatabaseURL:   pgtest.NewDSN(t),
		GeminiHomeDir: tmpDir + "/home",
		DataDir:       tmpDir + "/data",
		Port:          "4028",
		CredentialSources: config.CredentialSources{
			OAuth: config.CredentialSource{Disabled: true},
			Codex: config.CredentialSource{Disabled: true},
			Muse:  config.CredentialSource{Disabled: true},
		},
	})

	fakeStore := newTestStore(t)

	// Invalid RestatePort
	cfgInvalidRestate := Config{
		RestatePort: "-1",
		HealthPort:  "14028",
		AgentName:   "aerial-test",
	}
	err := RunApp(ctx, cfgInvalidRestate, WithStore(fakeStore, nil), WithAppConfig(appCfg), WithDaemonSpawner(&dummySpawner{}))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "listen on restate port")

	// Invalid HealthPort
	cfgInvalidHealth := Config{
		RestatePort: "0",
		HealthPort:  "-1",
		AgentName:   "aerial-test",
	}
	err = RunApp(ctx, cfgInvalidHealth, WithStore(fakeStore, nil), WithAppConfig(appCfg), WithDaemonSpawner(&dummySpawner{}))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "listen on health port")
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
		ctx, cancel := context.WithCancel(t.Context())

		tmpDir := t.TempDir()
		appCfg := config.NewFromData(&config.ConfigData{
			DatabaseURL:   pgtest.NewDSN(t),
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

		sockPath := filepath.Join(tmpDir, ".s.PGSQL.5432")
		cfg := Config{
			RestatePort:        "0",
			HealthPort:         "0",
			PostgresSocketPath: sockPath,
			AgentName:          "aerial-test",
		}

		ready := make(chan struct{})
		oldReady := onServerReady
		onServerReady = func(_, _ string) {
			close(ready)
		}
		defer func() { onServerReady = oldReady }()

		go func() {
			<-ready
			if c, dErr := (&net.Dialer{}).DialContext(ctx, "unix", sockPath); dErr == nil {
				if _, wErr := c.Write([]byte{0, 0, 0, 16, 0x04, 0xd2, 0x16, 0x2e, 0, 0, 0, 1, 0, 0, 0, 2}); wErr == nil {
					if _, rErr := io.ReadAll(c); rErr != nil {
						return
					}
				}
				if cErr := c.Close(); cErr != nil {
					return
				}
			}
			cancel()
		}()

		err := RunApp(ctx, cfg, WithAppConfig(appCfg), WithDaemonSpawner(&dummySpawner{}))
		require.NoError(t, err)
	})
}

func TestWireOutboundIntegrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cur         *config.ConfigData
		wantSession bool
	}{
		{name: "token and nats url", cur: &config.ConfigData{DiscordToken: "token-1", NATSURL: "nats://127.0.0.1:65530"}, wantSession: true},
		{name: "no token and no nats url", cur: &config.ConfigData{}},
		{name: "malformed nats url skips event bus", cur: &config.ConfigData{NATSURL: "nats://bad\x7fhost:4222"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := queue.New(nil, queue.WorkerPoolConfig{})
			dg, closeFn := wireOutboundIntegrations(context.Background(), tc.cur, pool)
			assert.Equal(t, tc.wantSession, dg != nil)
			assert.Same(t, dg, pool.DiscordSession())
			assert.Equal(t, closeFn != nil, pool.EventBus() != nil)
			if closeFn != nil {
				closeFn()
			}
		})
	}
}

type recordingSpawner struct {
	mu      sync.Mutex
	lastCfg runner.DaemonConfig
}

func (s *recordingSpawner) Spawn(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastCfg = cfg
	return nil, nil, nil, nil, errors.New("recording spawner stop")
}

func TestInitializeAgentPools_VoiceConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		engine           string
		voiceEngine      string
		voiceModel       string
		lowEffortModel   string
		prewarmedTargets []string
		wantEngine       string
		wantModel        string
		wantTargets      []string
	}{
		{
			name:             "explicit voice model, engine, and targets",
			engine:           "agy",
			voiceEngine:      "gemini_api",
			voiceModel:       "gemini-3.8-flash-voice",
			lowEffortModel:   "gemini-3.8-flash-low",
			prewarmedTargets: []string{"kiosk-kitchen", "galaxy-watch7"},
			wantEngine:       "gemini_api",
			wantModel:        "gemini-3.8-flash-voice",
			wantTargets:      []string{"kiosk-kitchen", "galaxy-watch7"},
		},
		{
			name:             "fallback to low effort model and default agy engine when unspecified",
			engine:           "",
			voiceEngine:      "",
			voiceModel:       "",
			lowEffortModel:   "gemini-3.8-flash-low",
			prewarmedTargets: []string{"kiosk-bedroom"},
			wantEngine:       "agy",
			wantModel:        "gemini-3.8-flash-low",
			wantTargets:      []string{"kiosk-bedroom"},
		},
		{
			name:             "global engine claude without voice engine inherits claude in pool and spawned daemon config",
			engine:           "claude",
			voiceEngine:      "",
			voiceModel:       "claude-haiku-voice",
			lowEffortModel:   "claude-haiku-low",
			prewarmedTargets: []string{"kiosk-living"},
			wantEngine:       "claude",
			wantModel:        "claude-haiku-voice",
			wantTargets:      []string{"kiosk-living"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subTmp := t.TempDir()
			cur := &config.ConfigData{
				Engine:         tc.engine,
				DataDir:        subTmp + "/data",
				Model:          "claude-opus-3",
				LowEffortModel: tc.lowEffortModel,
				Voice: config.VoiceConfig{
					Engine:           tc.voiceEngine,
					Model:            tc.voiceModel,
					PrewarmedTargets: tc.prewarmedTargets,
				},
			}
			appCfg := config.NewFromData(cur)
			sessionMgr := session.New(subTmp+"/home", subTmp+"/data")
			ambientResolver := ambient.NewResolver(appCfg)

			cfg := Config{
				HealthPort: "4028",
				AgentName:  "aerial",
			}

			spawner := &recordingSpawner{}
			bundle := initializeAgentPools(appCfg, cur, cfg, sessionMgr, spawner, ambientResolver)
			require.NotNil(t, bundle.voicePool)

			underlying := bundle.voicePool.Underlying()
			require.NotNil(t, underlying)

			assert.Equal(t, tc.wantModel, underlying.Model())
			assert.Equal(t, tc.wantEngine, underlying.Engine())
			assert.Equal(t, tc.wantTargets, underlying.PrewarmedTargets())

			target := "target-test"
			if len(tc.prewarmedTargets) > 0 {
				target = tc.prewarmedTargets[0]
			}
			_, spawnErr := underlying.GetOrCreate(context.Background(), target, "")
			assert.Error(t, spawnErr)
			spawner.mu.Lock()
			spawnedCfg := spawner.lastCfg
			spawner.mu.Unlock()
			assert.Equal(t, tc.wantEngine, spawnedCfg.Engine)
		})
	}
}

func TestMakeVoiceAmbientContextRetriever(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		nilAppCfg     bool
		nilResolver   bool
		voiceAmbient  string
		wantRetriever bool
		wantContent   string
	}{
		{
			name:          "nil appCfg returns nil",
			nilAppCfg:     true,
			wantRetriever: false,
		},
		{
			name:          "nil resolver returns nil",
			nilResolver:   true,
			wantRetriever: false,
		},
		{
			name:          "configured voice ambient context resolves",
			voiceAmbient:  "Current time is 5pm",
			wantRetriever: true,
			wantContent:   "<ambient_context>\nCurrent time is 5pm\n</ambient_context>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var appCfg *config.Config
			if !tc.nilAppCfg {
				var ambientCfg *config.AmbientContextConfig
				if tc.voiceAmbient != "" {
					ambientCfg = &config.AmbientContextConfig{Template: tc.voiceAmbient}
				}
				appCfg = config.NewFromData(&config.ConfigData{
					Voice: config.VoiceConfig{
						AmbientContext: ambientCfg,
					},
				})
			}
			var resolver *ambient.Resolver
			if !tc.nilResolver {
				resolver = ambient.NewResolver(appCfg)
			}

			retriever := makeVoiceAmbientContextRetriever(appCfg, resolver)
			if !tc.wantRetriever {
				assert.Nil(t, retriever)
				return
			}
			require.NotNil(t, retriever)
			content, err := retriever(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, tc.wantContent, content)
		})
	}
}

func TestPrewarmAmbientResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilResolver bool
		cancelCtx   bool
		hasAmbient  bool
	}{
		{
			name:        "nil resolver is a safe no-op",
			nilResolver: true,
			cancelCtx:   false,
			hasAmbient:  false,
		},
		{
			name:        "canceled context is a safe no-op",
			nilResolver: false,
			cancelCtx:   true,
			hasAmbient:  true,
		},
		{
			name:        "active context primes configured ambient resolver",
			nilResolver: false,
			cancelCtx:   false,
			hasAmbient:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelCtx {
				cancel()
			}

			var resolver *ambient.Resolver
			var appCfg *config.Config
			if !tc.nilResolver {
				var voiceAmb *config.AmbientContextConfig
				if tc.hasAmbient {
					voiceAmb = &config.AmbientContextConfig{Template: "Prewarmed Voice Ambient"}
				}
				appCfg = config.NewFromData(&config.ConfigData{
					Voice: config.VoiceConfig{
						AmbientContext: voiceAmb,
					},
				})
				resolver = ambient.NewResolver(appCfg)
			}

			stopPrewarm := PrewarmAmbientResolver(ctx, resolver)
			defer stopPrewarm()

			if !tc.nilResolver && !tc.cancelCtx && tc.hasAmbient {
				vAmb := appCfg.Current().VoiceAmbientContext()
				require.NotNil(t, vAmb)
				out, err := resolver.ResolveVoice(context.Background(), vAmb)
				require.NoError(t, err)
				assert.Contains(t, out, "Prewarmed Voice Ambient")
			}
		})
	}
}
