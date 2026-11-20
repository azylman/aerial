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
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/build"
	"github.com/azylman/aerial/services/vanadis-restate/internal/router"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
	"github.com/azylman/aerial/services/vanadis-restate/internal/widget"
)

func TestConfig_LoadConfigFromEnv(t *testing.T) {
	t.Setenv("RESTATE_PORT", "9999")
	t.Setenv("HEALTH_PORT", "4444")
	t.Setenv("VANADIS_URL", "http://test-vanadis:4025/")
	t.Setenv("RESTATE_INGRESS_URL", "http://test-restate:8080/")
	t.Setenv("DISCORD_TOKEN", "test-token")
	cfg := LoadConfigFromEnv()
	assert.Equal(t, "9999", cfg.RestatePort)
	assert.Equal(t, "4444", cfg.HealthPort)
	assert.Equal(t, "http://test-vanadis:4025", cfg.VanadisURL)
	assert.Equal(t, "http://test-restate:8080", cfg.RestateIngressURL)
	assert.Equal(t, "test-token", cfg.DiscordToken)

	t.Setenv("RESTATE_PORT", "")
	t.Setenv("PORT", "9191")
	t.Setenv("HEALTH_PORT", "")
	t.Setenv("VANADIS_URL", "")
	t.Setenv("RESTATE_INGRESS_URL", "")
	t.Setenv("DISCORD_TOKEN", "")
	cfgDefault := LoadConfigFromEnv()
	assert.Equal(t, "9191", cfgDefault.RestatePort)
	assert.Equal(t, "4026", cfgDefault.HealthPort)
	assert.Equal(t, "http://vanadis.aerial:4025", cfgDefault.VanadisURL)
	assert.Equal(t, "http://restate.aerial:8080", cfgDefault.RestateIngressURL)
	assert.Equal(t, "", cfgDefault.DiscordToken)

	t.Setenv("PORT", "")
	cfgFallback := LoadConfigFromEnv()
	assert.Equal(t, "9080", cfgFallback.RestatePort)
}

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
	origPing, origBus := pingPgxPoolFn, newBusFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
		pingPgxPoolFn, newBusFn = origPing, origBus
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }
	pingPgxPoolFn = func(context.Context, *pgxpool.Pool) error { return nil }
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
	origPing, origBus := pingPgxPoolFn, newBusFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
		pingPgxPoolFn, newBusFn = origPing, origBus
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }
	pingPgxPoolFn = func(context.Context, *pgxpool.Pool) error { return nil }
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
	newDiscordWidgetDelivererFn = func(string, ...string) (*widget.DiscordWidgetDeliverer, error) {
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
		name       string
		agent      string
		wantBuild  string
		wantRouter string
		wantPR     string
		wantRepo   string
	}{
		{
			name:       "default agent binds dark services",
			agent:      "",
			wantBuild:  "Build",
			wantRouter: "BuildRouter",
			wantPR:     "PRObject",
			wantRepo:   "RepoObject",
		},
		{
			name:       "scoped agent keeps single pr state services",
			agent:      "marida",
			wantBuild:  "Build-marida",
			wantRouter: "BuildRouter-marida",
			wantPR:     "PRObject",
			wantRepo:   "RepoObject",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			restateObj := build.NewBuildObjectWithAgent("http://127.0.0.1:4025", tc.agent, nil)
			routerSvc := router.NewBuildRouterServiceWithAgent(nil, tc.agent)
			srv := bindRestateServices(restateObj, routerSvc)
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
			require.Len(t, byName, 4)
			for _, want := range []string{tc.wantBuild, tc.wantRouter, tc.wantPR, tc.wantRepo} {
				require.Contains(t, byName, want)
			}
			require.Len(t, byName[tc.wantPR], 16)
			require.True(t, byName[tc.wantPR]["HandleCheckFailed"])
			require.True(t, byName[tc.wantPR]["HandleDeploySuccessPublished"])
			require.True(t, byName[tc.wantPR]["HandleConflictDetected"])
			require.True(t, byName[tc.wantPR]["HandleConflictPublished"])
			require.True(t, byName[tc.wantPR]["HandleCIRecovery"])
			require.Equal(t, "VIRTUAL_OBJECT", types[tc.wantPR])
			require.Len(t, byName[tc.wantRepo], 4)
			require.True(t, byName[tc.wantRepo]["ResolvePRBySHA"])
			require.Equal(t, "VIRTUAL_OBJECT", types[tc.wantRepo])
		})
	}
}

func TestLoadConfigFromEnv_Authoritative(t *testing.T) {
	t.Setenv("RESTATE_PORT", "9999")
	t.Setenv("HEALTH_PORT", "8888")
	t.Setenv("VANADIS_URL", "http://vanadis.local:4025")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")
	t.Setenv("ERI_URL", "http://eri.local")
	t.Setenv("RESTATE_INGRESS_URL", "http://restate.local:8080")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("GITHUB_TOKEN", "token-xyz")
	t.Setenv("DISCORD_TOKEN", "discord-xyz")
	t.Setenv("SHARE_ROOT", "/tmp/share")
	t.Setenv("POSTGRES_DB", "")

	cfg := LoadConfigFromEnv()
	assert.Equal(t, "9999", cfg.RestatePort)
	assert.Equal(t, "8888", cfg.HealthPort)
	assert.Equal(t, "http://vanadis.local:4025", cfg.VanadisURL)
	assert.Equal(t, "postgres://user:pass@localhost:5432/db", cfg.PostgresURL)
	assert.Equal(t, "http://eri.local", cfg.EriURL)
	assert.Equal(t, "http://restate.local:8080", cfg.RestateIngressURL)
	assert.Equal(t, "nats://localhost:4222", cfg.NATSURL)
	assert.Equal(t, "token-xyz", cfg.GitHubToken)
	assert.Equal(t, "discord-xyz", cfg.DiscordToken)
	assert.Equal(t, "/tmp/share", cfg.ShareRoot)

	t.Setenv("DATABASE_URL", "")
	t.Setenv("POSTGRES_URL", "")
	t.Setenv("POSTGRES_PASSWORD", "secret-pass")
	cfgDSN := LoadConfigFromEnv()
	assert.Equal(t, "postgres://aerial:secret-pass@postgres:5432/aerial?sslmode=disable", cfgDSN.PostgresURL)
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
		mockFail   bool
		pingFail   bool
		wantErr    bool
		wantSubstr string
	}{
		{name: "invalid url", url: "://invalid-url", wantErr: true, wantSubstr: "parse postgres url"},
		{name: "pool creation error", url: "postgres://user:pass@localhost:5432/db", mockFail: true, wantErr: true, wantSubstr: "create pgxpool"},
		{name: "ping error closes pool", url: "postgres://user:pass@localhost:5432/db", pingFail: true, wantErr: true, wantSubstr: "ping postgres"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			closed := false
			origNew, origPing, origClose := newPgxPoolFn, pingPgxPoolFn, closePgxPoolFn
			defer func() { newPgxPoolFn, pingPgxPoolFn, closePgxPoolFn = origNew, origPing, origClose }()
			closePgxPoolFn = func(pool *pgxpool.Pool) {
				closed = true
				origClose(pool)
			}
			if tc.mockFail {
				newPgxPoolFn = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
					return nil, errors.New("mock pool fail")
				}
			}
			if tc.pingFail {
				pingPgxPoolFn = func(context.Context, *pgxpool.Pool) error {
					return errors.New("ping refused")
				}
			}
			_, _, err := initPostgres(ctx, tc.url)
			if (err != nil) != tc.wantErr {
				t.Fatalf("initPostgres() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
			}
			if tc.pingFail && !closed {
				t.Fatal("expected pool to be closed on ping failure")
			}
		})
	}
}
