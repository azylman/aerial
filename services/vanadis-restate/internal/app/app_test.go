package app

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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/build"
	"github.com/azylman/aerial/services/vanadis-restate/internal/prstate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/router"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
	"github.com/azylman/aerial/services/vanadis-restate/internal/widget"
)

func TestHealthEndpoints(t *testing.T) {
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }

	mux := SetupHealthMux()

	for _, path := range []string{"/health", "/healthz", "/metrics", "/debug/coverage/flush"} {
		req := httptest.NewRequest(http.MethodGet, path, http.NoBody)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code, "endpoint %s should return 200 OK", path)
	}
}

func TestRunApp_Shutdown(t *testing.T) {
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	origPool, origBus := newPoolFn, newBusFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
		newPoolFn, newBusFn = origPool, origBus
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }
	newPoolFn = func(ctx context.Context, dsn string, opts pgutil.PoolOptions) (*pgxpool.Pool, error) {
		opts.Ping = false
		return pgutil.NewPool(ctx, dsn, opts)
	}
	newBusFn = func(context.Context, string) (pubsub.Bus, error) { return pubsub.NewMemoryBus(), nil }

	// Find two free ports for health and restate
	lHealth, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthPort := strings.TrimPrefix(lHealth.Addr().String(), "127.0.0.1:")
	_ = lHealth.Close()

	lRestate, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	restatePort := strings.TrimPrefix(lRestate.Addr().String(), "127.0.0.1:")
	_ = lRestate.Close()

	cfg := Config{
		HealthPort:   healthPort,
		RestatePort:  restatePort,
		VanadisURL:   "http://127.0.0.1:4025",
		PostgresURL:  "postgres://mock:mock@localhost:5432/mock",
		NATSURL:      "nats://mock:4222",
		DiscordToken: "test-token",
	}

	ctx, cancel := context.WithCancel(context.Background())
	appDone := make(chan error, 1)
	go func() {
		appDone <- RunApp(ctx, cfg)
	}()

	// Wait for health endpoint to become ready
	healthURL := "http://127.0.0.1:" + healthPort + "/healthz"
	require.Eventually(t, func() bool {
		resp, hErr := http.Get(healthURL)
		if hErr == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return true
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		return false
	}, 3*time.Second, 50*time.Millisecond)

	// Cancel context and verify shutdown
	cancel()
	select {
	case err := <-appDone:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("RunApp failed to stop within 5s")
	}
}

func TestRunApp_ErrorCases(t *testing.T) {
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	origPool, origBus := newPoolFn, newBusFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
		newPoolFn, newBusFn = origPool, origBus
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }
	newPoolFn = func(ctx context.Context, dsn string, opts pgutil.PoolOptions) (*pgxpool.Pool, error) {
		opts.Ping = false
		return pgutil.NewPool(ctx, dsn, opts)
	}
	newBusFn = func(context.Context, string) (pubsub.Bus, error) { return pubsub.NewMemoryBus(), nil }

	// 1. Invalid health port causes listen error
	errHealth := RunApp(context.Background(), Config{
		HealthPort:  "invalid-port",
		RestatePort: "9080",
		VanadisURL:  "http://vanadis.aerial:4025",
		PostgresURL: "postgres://mock:mock@localhost:5432/mock",
		NATSURL:     "nats://mock:4222",
	})
	assert.Error(t, errHealth)

	// 2. Invalid Restate port causes server error
	lHealth, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthPort := strings.TrimPrefix(lHealth.Addr().String(), "127.0.0.1:")
	_ = lHealth.Close()

	errRestate := RunApp(context.Background(), Config{
		HealthPort:  healthPort,
		RestatePort: "invalid-port",
		VanadisURL:  "http://vanadis.aerial:4025",
		PostgresURL: "postgres://mock:mock@localhost:5432/mock",
		NATSURL:     "nats://mock:4222",
	})
	assert.Error(t, errRestate)

	// 3. Widget deliverer constructor error logs warning without failing RunApp
	origDeliverer := newDiscordWidgetDelivererFn
	defer func() { newDiscordWidgetDelivererFn = origDeliverer }()
	newDiscordWidgetDelivererFn = func(string, string, []string, string) (*widget.DiscordWidgetDeliverer, error) {
		return nil, errors.New("mock deliverer fail")
	}

	lHealth2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthPort2 := strings.TrimPrefix(lHealth2.Addr().String(), "127.0.0.1:")
	_ = lHealth2.Close()

	lRestate2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	restatePort2 := strings.TrimPrefix(lRestate2.Addr().String(), "127.0.0.1:")
	_ = lRestate2.Close()

	ctxDeliverer, cancelDeliverer := context.WithCancel(context.Background())
	delivererDone := make(chan error, 1)
	go func() {
		delivererDone <- RunApp(ctxDeliverer, Config{
			HealthPort:  healthPort2,
			RestatePort: restatePort2,
			VanadisURL:  "http://127.0.0.1:4025",
			PostgresURL: "postgres://mock:mock@localhost:5432/mock",
			NATSURL:     "nats://mock:4222",
		})
	}()

	healthURL2 := "http://127.0.0.1:" + healthPort2 + "/healthz"
	require.Eventually(t, func() bool {
		resp, hErr := http.Get(healthURL2)
		if hErr == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return true
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		return false
	}, 3*time.Second, 50*time.Millisecond)

	cancelDeliverer()
	select {
	case err := <-delivererDone:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("RunApp with deliverer failure failed to stop within 5s")
	}
}

// discoveryManifest is the minimal Restate endpoint-manifest shape the
// assembly test decodes: service names with their handler names.
type discoveryManifest struct {
	Services []struct {
		Name     string `json:"name"`
		Ty       string `json:"ty"`
		Handlers []struct {
			Name string `json:"name"`
		} `json:"handlers"`
	} `json:"services"`
}

func TestBindRestateServices(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		agent        string
		wantBuild    string
		wantRouter   string
		wantPRState  bool
		wantServices int
		projection   bool
		noGitHub     bool
	}{
		{name: "aerial agent with projection store owns pr state", agent: "aerial", projection: true, wantBuild: "Build", wantRouter: "BuildRouter", wantPRState: true, wantServices: 4},
		{name: "empty agent owns pr state", agent: "", noGitHub: true, wantBuild: "Build", wantRouter: "BuildRouter", wantPRState: true, wantServices: 4},
		{name: "aerial agent owns pr state", agent: "aerial", wantBuild: "Build", wantRouter: "BuildRouter", wantPRState: true, wantServices: 4},
		{name: "mixed case aerial owns pr state", agent: " Aerial ", wantBuild: "Build", wantRouter: "BuildRouter", wantPRState: true, wantServices: 4},
		{name: "sibling agent never binds pr state", agent: "agent-alpha", wantBuild: "Build-agent-alpha", wantRouter: "BuildRouter-agent-alpha", wantPRState: false, wantServices: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			restateObj := build.NewBuildObjectWithAgent("http://127.0.0.1:4025", tc.agent, nil)
			routerSvc := router.NewBuildRouterServiceWithAgent(nil, tc.agent)
			var projection *prstate.PgProjectionStore
			if tc.projection {
				projection = prstate.NewPgProjectionStore(nil)
			}
			gh := prstate.NewGitHubClient(nil, "", "")
			kube := prstate.NewKubeClient(nil, "", "", "", "")
			if tc.noGitHub {
				gh, kube = nil, nil
			}
			srv := bindRestateServices(tc.agent, restateObj, routerSvc, projection, prstate.NewSessionEriEffector(nil), prstate.NewBusFactPublisher(nil), gh, kube)
			require.NotNil(t, srv)
			handler, err := srv.Handler()
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodGet, "/discover", http.NoBody)
			req.Header.Set("Accept", "application/vnd.restate.endpointmanifest.v2+json")
			rec := httptest.NewRecorder()
			handler(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			var manifest discoveryManifest
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &manifest))
			byName := make(map[string]map[string]bool, len(manifest.Services))
			types := make(map[string]string, len(manifest.Services))
			for _, svc := range manifest.Services {
				handlers := make(map[string]bool, len(svc.Handlers))
				for _, h := range svc.Handlers {
					handlers[h.Name] = true
				}
				byName[svc.Name] = handlers
				types[svc.Name] = svc.Ty
			}
			require.Len(t, byName, tc.wantServices)
			require.Contains(t, byName, tc.wantBuild)
			require.Contains(t, byName, tc.wantRouter)
			require.True(t, byName[tc.wantBuild]["CompleteChunk"])
			require.True(t, byName[tc.wantBuild]["FailChunk"])
			if !tc.wantPRState {
				require.NotContains(t, byName, "PRObject")
				require.NotContains(t, byName, "RepoObject")
				return
			}
			require.Len(t, byName["PRObject"], 44)
			require.True(t, byName["PRObject"]["RetryReadiness"])
			require.True(t, byName["PRObject"]["RetryFluxReconcile"])
			require.True(t, byName["PRObject"]["HandleConflictCheck"])
			require.True(t, byName["PRObject"]["HandleBranchConflict"])
			require.True(t, byName["PRObject"]["HandleHeadRefMissing"])
			require.True(t, byName["PRObject"]["RetryEriEffect"])
			require.True(t, byName["PRObject"]["Hydrate"])
			require.True(t, byName["PRObject"]["Project"])
			require.True(t, byName["PRObject"]["CompareProjection"])
			require.True(t, byName["PRObject"]["HandleCheckFailed"])
			require.True(t, byName["PRObject"]["HandleDeploySuccessPublished"])
			require.True(t, byName["PRObject"]["HandleConflictDetected"])
			require.True(t, byName["PRObject"]["HandleConflictPublished"])
			require.True(t, byName["PRObject"]["HandleCIRecovery"])
			require.Equal(t, "VIRTUAL_OBJECT", types["PRObject"])
			require.True(t, byName["PRObject"]["HandleMainAdvanced"])
			require.True(t, byName["PRObject"]["Register"])
			require.True(t, byName["PRObject"]["GetBuildLifecycle"])
			require.True(t, byName["PRObject"]["HandleSynchronized"])
			require.True(t, byName["PRObject"]["HandleSelfRegistered"])
			require.True(t, byName["PRObject"]["HandleCheckStatus"])
			require.True(t, byName["PRObject"]["HandleDeployFailed"])
			require.True(t, byName["PRObject"]["HandleBuildMetadata"])
			require.True(t, byName["PRObject"]["GetDeployView"])
			require.True(t, byName["PRObject"]["HandleDeployObserved"])
			require.True(t, byName["PRObject"]["HandleReconcileLost"])
			require.True(t, byName["PRObject"]["DropUntracked"])
			require.Len(t, byName["RepoObject"], 17)
			require.True(t, byName["RepoObject"]["HandleRolloutIndex"])
			require.True(t, byName["RepoObject"]["ListRollouts"])
			require.True(t, byName["RepoObject"]["HandleGitPush"])
			require.True(t, byName["RepoObject"]["HandleImagePromotion"])
			require.True(t, byName["RepoObject"]["HandleAutoUpdate"])
			require.True(t, byName["RepoObject"]["HandleWorkflowRerun"])
			require.True(t, byName["RepoObject"]["ResolvePRBySHA"])
			require.True(t, byName["RepoObject"]["HandlePRIndex"])
			require.True(t, byName["RepoObject"]["HandleHeldReplay"])
			require.True(t, byName["RepoObject"]["HandleCheckRunBySHA"])
			require.Equal(t, "VIRTUAL_OBJECT", types["RepoObject"])
		})
	}
}

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	defaults := Config{
		RestatePort:       "9080",
		HealthPort:        "4026",
		VanadisURL:        "http://vanadis.aerial:4025",
		PostgresURL:       "",
		EriURL:            "http://eri.aerial",
		RestateIngressURL: "http://restate.aerial:8080",
		NATSURL:           "nats://nats.aerial:4222",
		ShareRoot:         "/share",
		AgentName:         "aerial",
		ConfigPath:        "/share/aerial-config/config.yaml",
		ServiceConfigPath: DefaultServiceConfigPath,
	}
	tests := []struct {
		name string
		env  map[string]string
		want Config
	}{
		{"empty env uses defaults", map[string]string{}, defaults},
		{"explicit values trimmed and authoritative", map[string]string{
			"RESTATE_PORT":               "9999",
			"PORT":                       "9191",
			"HEALTH_PORT":                "8888",
			"VANADIS_URL":                "http://vanadis.local:4025/",
			"DATABASE_URL":               "postgres://user:pass@localhost:5432/db",
			"POSTGRES_URL":               "postgres://ignored",
			"POSTGRES_USER":              "leaked-user",
			"ERI_URL":                    "http://eri.local/",
			"RESTATE_INGRESS_URL":        "http://restate.local:8080/",
			"NATS_URL":                   "nats://localhost:4222",
			"GITHUB_TOKEN":               "token-xyz",
			"GITHUB_PAT":                 "pat-ignored",
			"DISCORD_TOKEN":              " discord-xyz ",
			"SHARE_ROOT":                 "/tmp/share",
			"AGENT_NAME":                 "agent-alpha",
			"CONFIG_PATH":                "/tmp/share/agent-alpha/config.yaml",
			"KUBERNETES_SERVICE_HOST":    "kube.test",
			"KUBERNETES_SERVICE_PORT":    "443",
			"FLUX_RECEIVER_URL":          " http://receiver.local/hook ",
			"GITHUB_APP_ID":              " 12345 ",
			"GITHUB_APP_INSTALLATION_ID": "678",
			"GITHUB_APP_PRIVATE_KEY":     "\npem-text\n",
		}, Config{
			RestatePort:             "9999",
			HealthPort:              "8888",
			VanadisURL:              "http://vanadis.local:4025",
			PostgresURL:             "postgres://user:pass@localhost:5432/db",
			EriURL:                  "http://eri.local",
			RestateIngressURL:       "http://restate.local:8080",
			NATSURL:                 "nats://localhost:4222",
			GitHubToken:             "token-xyz",
			DiscordToken:            "discord-xyz",
			ShareRoot:               "/tmp/share",
			AgentName:               "agent-alpha",
			ConfigPath:              "/tmp/share/agent-alpha/config.yaml",
			KubeAPIServer:           "https://kube.test:443",
			FluxReceiverURL:         "http://receiver.local/hook",
			GitHubAppID:             "12345",
			GitHubAppInstallationID: "678",
			GitHubAppPrivateKey:     "pem-text",
			ServiceConfigPath:       DefaultServiceConfigPath,
		}},
		{"PORT fallback, POSTGRES_URL and GITHUB_PAT fallbacks", map[string]string{
			"PORT":         "9191",
			"POSTGRES_URL": "postgres://pg-url",
			"GITHUB_PAT":   "pat-xyz",
		}, Config{
			RestatePort:       "9191",
			HealthPort:        "4026",
			VanadisURL:        "http://vanadis.aerial:4025",
			PostgresURL:       "postgres://pg-url",
			EriURL:            "http://eri.aerial",
			RestateIngressURL: "http://restate.aerial:8080",
			NATSURL:           "nats://nats.aerial:4222",
			GitHubToken:       "pat-xyz",
			ShareRoot:         "/share",
			AgentName:         "aerial",
			ConfigPath:        "/share/aerial-config/config.yaml",
			ServiceConfigPath: DefaultServiceConfigPath,
		}},
		{"DSN built from POSTGRES_* parts", map[string]string{
			"POSTGRES_PASSWORD": "secret-pass",
		}, Config{
			RestatePort:       "9080",
			HealthPort:        "4026",
			VanadisURL:        "http://vanadis.aerial:4025",
			PostgresURL:       "postgres://aerial:secret-pass@postgres:5432/aerial?sslmode=disable",
			EriURL:            "http://eri.aerial",
			RestateIngressURL: "http://restate.aerial:8080",
			NATSURL:           "nats://nats.aerial:4222",
			ShareRoot:         "/share",
			AgentName:         "aerial",
			ConfigPath:        "/share/aerial-config/config.yaml",
			ServiceConfigPath: DefaultServiceConfigPath,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := LoadConfig(func(k string) string { return tc.env[k] })
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRunApp_ContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	cfg := Config{
		RestatePort: "0",
		HealthPort:  "0",
	}
	err := RunApp(ctx, cfg)
	// Server should cleanly exit when context is cancelled
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("RunApp returned: %v", err)
	}
}

type mockStartupBus struct {
	pubsub.Bus

	ensureErr error
	closed    bool
}

func (m *mockStartupBus) EnsureCoreStreams(context.Context) error { return m.ensureErr }
func (m *mockStartupBus) Close() error {
	m.closed = true
	return nil
}

func TestRunApp_FailFast(t *testing.T) {
	// Missing Postgres URL
	err := RunApp(context.Background(), Config{PostgresURL: "", NATSURL: "nats://localhost:4222"})
	assert.ErrorContains(t, err, "postgres_url is required")

	// Invalid Postgres URL
	err = RunApp(context.Background(), Config{PostgresURL: "invalid://url", NATSURL: "nats://localhost:4222"})
	assert.ErrorContains(t, err, "connect to postgres")

	// Unreachable NATS via default newBusFn fails fast (RetryOnFailedConnect(false))
	_, natsDialErr := newBusFn(context.Background(), "nats://127.0.0.1:1")
	assert.Error(t, natsDialErr)

	// Missing NATS URL
	oldInitPg := initPostgres
	initPostgres = func(context.Context, string) (*store.Store, func(context.Context), error) {
		return &store.Store{}, func(context.Context) {}, nil
	}
	defer func() { initPostgres = oldInitPg }()

	err = RunApp(context.Background(), Config{PostgresURL: "postgres://mock", NATSURL: ""})
	assert.ErrorContains(t, err, "nats_url is required")

	// NATS connect failure
	oldNewBus := newBusFn
	newBusFn = func(context.Context, string) (pubsub.Bus, error) {
		return nil, errors.New("nats down")
	}
	defer func() { newBusFn = oldNewBus }()

	err = RunApp(context.Background(), Config{PostgresURL: "postgres://mock", NATSURL: "nats://mock"})
	assert.ErrorContains(t, err, "connect to nats")

	// NATS JetStream verification failure closes bus and fails fast
	startupBus := &mockStartupBus{ensureErr: errors.New("jetstream unavailable")}
	newBusFn = func(context.Context, string) (pubsub.Bus, error) {
		return startupBus, nil
	}
	err = RunApp(context.Background(), Config{PostgresURL: "postgres://mock", NATSURL: "nats://mock"})
	assert.ErrorContains(t, err, "verify nats jetstream")
	assert.True(t, startupBus.closed)
}

func TestInitPostgres(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		url        string
		wantErr    bool
		wantSubstr string
	}{
		{name: "invalid url", url: "://invalid-url", wantErr: true, wantSubstr: "parse postgres url"},
		{name: "ping error closes pool", url: "postgres://user:pass@127.0.0.1:1/db", wantErr: true, wantSubstr: "ping postgres"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := initPostgres(ctx, tc.url)
			if (err != nil) != tc.wantErr {
				t.Fatalf("initPostgres() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
			}
		})
	}
}
