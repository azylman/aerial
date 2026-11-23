package app

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/build"
	"github.com/azylman/aerial/services/vanadis-restate/internal/prstate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/router"
	"github.com/azylman/aerial/services/vanadis-restate/internal/widget"
)

// runAppClosedAddr returns a loopback address nothing listens on, so a dial
// fails fast with connection refused and never leaves the host.
func runAppClosedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// runAppFreePort returns a loopback port that was free a moment ago.
func runAppFreePort(t *testing.T) string {
	t.Helper()
	_, port, err := net.SplitHostPort(runAppClosedAddr(t))
	require.NoError(t, err)
	return port
}

// runAppWaitHealthy blocks until the health server on port answers /healthz.
func runAppWaitHealthy(t *testing.T, port string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://127.0.0.1:" + port + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 3*time.Second, 20*time.Millisecond)
}

// runAppSwapSeams installs the RunApp package seams for one row and restores
// them on cleanup. It returns the memory bus behind bus (nil without one).
func runAppSwapSeams(t *testing.T, poolErr error, bus *pubsubtest.FaultBus, busErr, delivererErr error) pubsub.Bus {
	t.Helper()
	origPool, origBus, origDeliverer := newPoolFn, newBusFn, newDiscordWidgetDelivererFn
	t.Cleanup(func() { newPoolFn, newBusFn, newDiscordWidgetDelivererFn = origPool, origBus, origDeliverer })
	newPoolFn = func(ctx context.Context, dsn string, opts pgutil.PoolOptions) (*pgxpool.Pool, error) {
		if poolErr != nil {
			return nil, poolErr
		}
		opts.Ping, opts.MinConns = false, 0
		return pgutil.NewPool(ctx, dsn, opts)
	}
	var mem pubsub.Bus
	if bus != nil {
		mem = pubsub.NewMemoryBus()
		bus.Bus = mem
	}
	if bus != nil || busErr != nil {
		newBusFn = func(context.Context, string) (pubsub.Bus, error) {
			if busErr != nil {
				return nil, busErr
			}
			return bus, nil
		}
	}
	if delivererErr != nil {
		newDiscordWidgetDelivererFn = func(string, string, string, []string, string) (*widget.DiscordWidgetDeliverer, error) {
			return nil, delivererErr
		}
	}
	return mem
}

// runAppRequireErr asserts err is nil when want is empty, else that its text
// contains want.
func runAppRequireErr(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		require.NoError(t, err)
		return
	}
	require.ErrorContains(t, err, want)
}

// runAppCfg is a RunApp config with the fields the TestRunApp rows vary.
func runAppCfg(pgURL, owner, natsURL, healthPort, restatePort, discordToken string) Config {
	return Config{PostgresURL: pgURL, GitHubOwner: owner, NATSURL: natsURL, HealthPort: healthPort, RestatePort: restatePort, DiscordToken: discordToken, VanadisURL: "http://127.0.0.1:4025"}
}

// TestRunApp drives RunApp through every startup branch. Rows run serially
// because they swap the package seams newPoolFn, newBusFn and
// newDiscordWidgetDelivererFn. newPoolFn always returns a lazy pool
// (no ping, no min conns) on a closed loopback port, so no row dials
// Postgres. newBusFn is swapped only when the row sets bus or busErr; the
// remaining row dials NATS on a closed loopback port through the default
// dialer.
func TestRunApp(t *testing.T) {
	closed := runAppClosedAddr(t)
	pgURL := "postgres://user:pass@" + closed + "/db"
	base := runAppCfg(pgURL, "acme", "nats://bus.test:4222", "invalid-port", "", "")
	readyHealth, readyRestate := runAppFreePort(t), runAppFreePort(t)
	warnHealth, warnRestate := runAppFreePort(t), runAppFreePort(t)
	startHealth := runAppFreePort(t)
	tests := []struct {
		name            string
		cfg             Config
		poolErr         error
		bus             *pubsubtest.FaultBus
		busErr          error
		delivererErr    error
		cancelWhenReady bool
		wantErr         string
	}{
		{name: "missing postgres url fails first", cfg: Config{NATSURL: "nats://bus.test:4222"}, wantErr: "postgres_url is required"},
		{
			name: "blank github owner fails before connecting", cfg: runAppCfg(pgURL, "  ", "nats://bus.test:4222", "invalid-port", "", ""),
			wantErr: "github_owner is required (vanadis_restate.github_owner in the vanadis service config)",
		},
		{name: "postgres pool error", cfg: base, poolErr: errors.New("parse postgres url: bad"), wantErr: "connect to postgres: parse postgres url: bad"},
		{name: "missing nats url", cfg: runAppCfg(pgURL, "acme", "", "invalid-port", "", ""), wantErr: "nats_url is required"},
		{name: "nats connect failure", cfg: base, busErr: errors.New("nats down"), wantErr: "connect to nats: nats down"},
		{name: "default dialer fails fast on unreachable nats", cfg: runAppCfg(pgURL, "acme", "nats://"+closed, "invalid-port", "", ""), wantErr: "connect to nats: "},
		{
			name: "jetstream verification failure closes bus", cfg: base, bus: &pubsubtest.FaultBus{EnsureErr: errors.New("jetstream unavailable")},
			wantErr: "verify nats jetstream: jetstream unavailable",
		},
		{
			name: "health listen failure still closes bus", cfg: base, bus: &pubsubtest.FaultBus{CloseErr: errors.New("close failed")},
			wantErr: "listening on health port invalid-port: ",
		},
		{
			name: "restate start failure surfaces", cfg: runAppCfg(pgURL, "acme", "nats://bus.test:4222", startHealth, "invalid-port", ""), bus: &pubsubtest.FaultBus{},
			wantErr: "invalid-port",
		},
		{
			name: "cancel after ready shuts down cleanly with discord auth", cfg: runAppCfg(pgURL, "acme", "nats://bus.test:4222", readyHealth, readyRestate, "test-token"),
			bus: &pubsubtest.FaultBus{}, cancelWhenReady: true,
		},
		{
			name: "widget deliverer failure only warns", cfg: runAppCfg(pgURL, "acme", "nats://bus.test:4222", warnHealth, warnRestate, ""), bus: &pubsubtest.FaultBus{},
			delivererErr: errors.New("deliverer down"), cancelWhenReady: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := runAppSwapSeams(t, tc.poolErr, tc.bus, tc.busErr, tc.delivererErr)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- RunApp(ctx, tc.cfg) }()
			if tc.cancelWhenReady {
				runAppWaitHealthy(t, tc.cfg.HealthPort)
				cancel()
			}
			select {
			case err := <-done:
				runAppRequireErr(t, err, tc.wantErr)
			case <-time.After(10 * time.Second):
				t.Fatal("RunApp did not return within 10s")
			}
			if tc.bus != nil && tc.bus.CloseErr == nil {
				assert.EqualError(t, mem.Publish(context.Background(), "probe", "", "x"), "pubsub: memory bus closed", "RunApp must close the bus")
			}
		})
	}
}

// discoveryManifest is the minimal Restate endpoint-manifest shape the
// assembly test decodes: service names with their handler names.
type discoveryManifest struct {
	Services []struct {
		Name              string `json:"name"`
		Ty                string `json:"ty"`
		InactivityTimeout int    `json:"inactivityTimeout"`
		AbortTimeout      int    `json:"abortTimeout"`
		Handlers          []struct {
			Name string `json:"name"`
		} `json:"handlers"`
	} `json:"services"`
}

// bindPRObjectHandlers and bindRepoObjectHandlers are the complete sorted
// handler sets the default-agent runner registers (37 and 17).
var (
	bindPRObjectHandlers = []string{
		"DropUntracked", "GetBuildLifecycle", "GetDeployView", "HandleBranchConflict", "HandleBuildMetadata", "HandleCIRecovered",
		"HandleCheckFailed", "HandleCheckPassed", "HandleCheckRerunRequested", "HandleCheckStatus", "HandleClosed", "HandleCollaboratorAlert", "HandleConflict",
		"HandleConflictCheck", "HandleConflictResolved", "HandleDeployAttemptFailed",
		"HandleDeployFailed", "HandleDeployFailureDeadline", "HandleDeployObserved", "HandleDeploySuccess", "HandleHeadRefMissing",
		"HandleKustomizationRequested", "HandleMainAdvanced", "HandleMerged", "HandleOpened", "HandleReadinessRetry", "HandleReconcileLost", "HandleReopened",
		"HandleRolloutProgress", "HandleSelfRegistered", "HandleSynchronized", "Project", "Register", "RetryEriEffect", "RetryFactEffect", "RetryFluxReconcile",
		"RetryReadiness",
	}
	bindRepoObjectHandlers = []string{
		"HandleAutoUpdate", "HandleCheckRunBySHA", "HandleFanout", "HandleGitPush", "HandleHeldExpiry", "HandleHeldReplay", "HandleImagePromotion",
		"HandleImageReadyBySHA", "HandleKustomizationStatus", "HandleMainPush", "HandlePRIndex", "HandlePushBySHA", "HandleRolloutIndex", "HandleWorkflowRerun",
		"HandleWorkflowRunBySHA", "ListRollouts", "ResolvePRBySHA",
	}
)

func TestBindRestateServices(t *testing.T) {
	t.Parallel()
	prState := map[string][]string{"PRObject": bindPRObjectHandlers, "RepoObject": bindRepoObjectHandlers}
	expectedTimeouts := map[string][2]int{
		"Build":       {45000, 15000},
		"BuildRouter": {45000, 15000},
	}
	tests := []struct {
		name         string
		agent        string
		nilDeps      bool
		wantServices []string
		wantPRState  map[string][]string
		wantTimeouts map[string][2]int
	}{
		{name: "empty agent owns pr state", agent: "", wantServices: []string{"Build", "BuildRouter", "PRObject", "RepoObject"}, wantPRState: prState, wantTimeouts: expectedTimeouts},
		{name: "aerial agent owns pr state", agent: "aerial", wantServices: []string{"Build", "BuildRouter", "PRObject", "RepoObject"}, wantPRState: prState, wantTimeouts: expectedTimeouts},
		{name: "mixed case aerial owns pr state", agent: " Aerial ", wantServices: []string{"Build", "BuildRouter", "PRObject", "RepoObject"}, wantPRState: prState, wantTimeouts: expectedTimeouts},
		{name: "sibling agent never binds pr state", agent: "agent-alpha", nilDeps: true, wantServices: []string{"Build", "BuildRouter"}, wantPRState: map[string][]string{}, wantTimeouts: expectedTimeouts},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			projection := prstate.NewPgProjectionStore(nil)
			gh := prstate.NewGitHubClient(nil, "", "")
			kube := prstate.NewKubeClient(nil, "", "", "", "")
			if tc.nilDeps {
				projection, gh, kube = nil, nil, nil
			}
			srv, err := bindRestateServices(tc.agent, build.NewBuildObjectWithAgent("http://127.0.0.1:4025", tc.agent, nil), router.NewBuildRouterServiceWithAgent(nil, tc.agent),
				projection, prstate.NewSessionEriEffector(nil), prstate.NewBusFactPublisher(nil), gh, kube)
			require.NoError(t, err)
			handler, err := srv.Handler()
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodGet, "/discover", http.NoBody)
			req.Header.Set("Accept", "application/vnd.restate.endpointmanifest.v3+json")
			rec := httptest.NewRecorder()
			handler(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			var manifest discoveryManifest
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &manifest))

			services := make([]string, 0, len(manifest.Services))
			handlers := map[string][]string{}
			gotPRState := map[string][]string{}
			inactivityTimeouts := map[string]int{}
			abortTimeouts := map[string]int{}
			for _, svc := range manifest.Services {
				services = append(services, svc.Name)
				inactivityTimeouts[svc.Name] = svc.InactivityTimeout
				abortTimeouts[svc.Name] = svc.AbortTimeout
				for _, h := range svc.Handlers {
					handlers[svc.Name] = append(handlers[svc.Name], h.Name)
				}
				slices.Sort(handlers[svc.Name])
				if svc.Name == "PRObject" || svc.Name == "RepoObject" {
					assert.Equal(t, "VIRTUAL_OBJECT", svc.Ty, svc.Name)
					gotPRState[svc.Name] = handlers[svc.Name]
				}
			}
			slices.Sort(services)
			assert.Equal(t, tc.wantServices, services)
			assert.Subset(t, handlers["Build"], []string{"CompleteChunk", "FailChunk"})
			assert.Equal(t, tc.wantPRState, gotPRState)
			for svc, timeouts := range tc.wantTimeouts {
				assert.Equal(t, timeouts[0], inactivityTimeouts[svc], "service %s inactivityTimeout", svc)
				assert.Equal(t, timeouts[1], abortTimeouts[svc], "service %s abortTimeout", svc)
			}
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
			"RESTATE_PORT":            "9999",
			"PORT":                    "9191",
			"HEALTH_PORT":             "8888",
			"VANADIS_URL":             "http://vanadis.local:4025/",
			"DATABASE_URL":            "postgres://user:pass@localhost:5432/db",
			"POSTGRES_URL":            "postgres://ignored",
			"POSTGRES_USER":           "leaked-user",
			"ERI_URL":                 "http://eri.local/",
			"RESTATE_INGRESS_URL":     "http://restate.local:8080/",
			"NATS_URL":                "nats://localhost:4222",
			"GITHUB_TOKEN":            "token-xyz",
			"GITHUB_PAT":              "pat-ignored",
			"DISCORD_TOKEN":           " discord-xyz ",
			"SHARE_ROOT":              "/tmp/share",
			"AGENT_NAME":              "agent-alpha",
			"CONFIG_PATH":             "/tmp/share/agent-alpha/config.yaml",
			"KUBERNETES_SERVICE_HOST": "kube.test",
			"KUBERNETES_SERVICE_PORT": "443",
			"FLUX_RECEIVER_URL":       " http://receiver.local/hook ",
			"GITHUB_APP_PRIVATE_KEY":  "\npem-text\n",
		}, Config{
			RestatePort:         "9999",
			HealthPort:          "8888",
			VanadisURL:          "http://vanadis.local:4025",
			PostgresURL:         "postgres://user:pass@localhost:5432/db",
			EriURL:              "http://eri.local",
			RestateIngressURL:   "http://restate.local:8080",
			NATSURL:             "nats://localhost:4222",
			GitHubToken:         "token-xyz",
			DiscordToken:        "discord-xyz",
			ShareRoot:           "/tmp/share",
			AgentName:           "agent-alpha",
			ConfigPath:          "/tmp/share/agent-alpha/config.yaml",
			KubeAPIServer:       "https://kube.test:443",
			FluxReceiverURL:     "http://receiver.local/hook",
			GitHubAppPrivateKey: "pem-text",
			ServiceConfigPath:   DefaultServiceConfigPath,
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

// TestNewPRStateServices pins that the default-agent prstate wiring needs
// every boundary and names the missing one.
func TestNewPRStateServices(t *testing.T) {
	t.Parallel()
	projection := prstate.NewPgProjectionStore(nil)
	eri := prstate.NewSessionEriEffector(nil)
	facts := prstate.NewBusFactPublisher(nil)
	gh := prstate.NewGitHubClient(nil, "", "")
	kube := prstate.NewKubeClient(nil, "", "", "", "")
	tests := []struct {
		name       string
		projection *prstate.PgProjectionStore
		eri        prstate.EriEffector
		facts      prstate.FactPublisher
		gh         *prstate.GitHubClient
		kube       *prstate.KubeClient
		wantErr    string
	}{
		{name: "every boundary wires", projection: projection, eri: eri, facts: facts, gh: gh, kube: kube},
		{name: "nil projection fails", eri: eri, facts: facts, gh: gh, kube: kube, wantErr: "vanadis-restate: prstate dependency is nil: projection store"},
		{name: "nil eri fails", projection: projection, facts: facts, gh: gh, kube: kube, wantErr: "vanadis-restate: prstate dependency is nil: eri effector"},
		{name: "nil facts fails", projection: projection, eri: eri, gh: gh, kube: kube, wantErr: "vanadis-restate: prstate dependency is nil: fact publisher"},
		{name: "nil github fails", projection: projection, eri: eri, facts: facts, kube: kube, wantErr: "vanadis-restate: prstate dependency is nil: GitHub client"},
		{name: "nil kube fails", projection: projection, eri: eri, facts: facts, gh: gh, wantErr: "vanadis-restate: prstate dependency is nil: Kubernetes client"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pr, repo, err := newPRStateServices(tc.projection, tc.eri, tc.facts, tc.gh, tc.kube)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				require.Nil(t, pr)
				require.Nil(t, repo)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pr)
			require.NotNil(t, repo)
		})
	}
}
