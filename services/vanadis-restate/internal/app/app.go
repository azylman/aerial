package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis-restate/internal/build"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/prstate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/router"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
	"github.com/azylman/aerial/services/vanadis-restate/internal/widget"
)

var (
	newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
		return pubsub.NewJetStreamBus(ctx, url, nats.RetryOnFailedConnect(false), nats.Timeout(5*time.Second))
	}
	newDiscordWidgetDelivererFn = widget.NewDiscordWidgetDeliverer
)

// DefaultConfigPath is the agent config.yaml used when CONFIG_PATH is unset.
const DefaultConfigPath = "/share/aerial-config/config.yaml"

// Config holds runtime configuration for the vanadis-restate worker service.
type Config struct {
	RestatePort       string
	HealthPort        string
	VanadisURL        string
	PostgresURL       string
	EriURL            string
	RestateIngressURL string
	NATSURL           string
	GitHubToken       string
	// GitHubAppID, GitHubAppInstallationID and GitHubAppPrivateKey are the
	// GitHub App secrets (env); all empty keeps every GitHub REST call on
	// GitHubToken exactly as before.
	GitHubAppID             string
	GitHubAppInstallationID string
	GitHubAppPrivateKey     string
	// ServiceConfigPath is the non-secret YAML whose
	// vanadis_restate.github_credential (app|pat, default app) picks the
	// GitHub REST credential, re-read JIT.
	ServiceConfigPath string
	DiscordToken      string
	ShareRoot         string
	AgentName         string
	// ConfigPath is the agent config.yaml read for widget channel policy (shared CONFIG_PATH convention).
	ConfigPath string
	// Environ is the process environment (os.Environ form) the widget deliverer scans for
	// DISCORD_TOKEN_<AGENT> bot tokens; main fills it, tests pass literals.
	Environ []string
	// KubeAPIServer is the in-cluster API server restate-owned Flux and
	// image effects write to; empty outside a cluster.
	KubeAPIServer string
	// FluxReceiverURL is the tokenized Flux receiver webhook (a secret);
	// empty disables receiver forwards exactly like synapse.
	FluxReceiverURL string
}

// LoadConfig loads configuration through getenv (getenv in main) with defaults. Environ is
// left for main to fill from os.Environ.
func LoadConfig(getenv func(string) string) Config {
	restatePort := strings.TrimSpace(getenv("RESTATE_PORT"))
	if restatePort == "" {
		restatePort = strings.TrimSpace(getenv("PORT"))
	}
	if restatePort == "" {
		restatePort = "9080"
	}

	healthPort := strings.TrimSpace(getenv("HEALTH_PORT"))
	if healthPort == "" {
		healthPort = "4026"
	}

	vanadisURL := strings.TrimRight(strings.TrimSpace(getenv("VANADIS_URL")), "/")
	if vanadisURL == "" {
		vanadisURL = "http://vanadis.aerial:4025"
	}

	pgURL := strings.TrimSpace(store.FirstNonEmpty(getenv("DATABASE_URL"), getenv("POSTGRES_URL"), httpkit.BuildPostgresDSN(getenv, "aerial")))
	eriURL := strings.TrimRight(strings.TrimSpace(store.FirstNonEmpty(getenv("ERI_URL"), "http://eri.aerial")), "/")
	restateIngressURL := strings.TrimRight(strings.TrimSpace(store.FirstNonEmpty(getenv("RESTATE_INGRESS_URL"), "http://restate.aerial:8080")), "/")
	natsURL := strings.TrimSpace(store.FirstNonEmpty(getenv("NATS_URL"), "nats://nats.aerial:4222"))
	ghToken := strings.TrimSpace(store.FirstNonEmpty(getenv("GITHUB_TOKEN"), getenv("GITHUB_PAT")))
	discordToken := strings.TrimSpace(getenv("DISCORD_TOKEN"))
	shareRoot := strings.TrimSpace(store.FirstNonEmpty(getenv("SHARE_ROOT"), executor.DefaultShareRoot))
	agentName := strings.TrimSpace(store.FirstNonEmpty(getenv("AGENT_NAME"), "aerial"))
	configPath := strings.TrimSpace(store.FirstNonEmpty(getenv("CONFIG_PATH"), DefaultConfigPath))

	return Config{
		RestatePort:             restatePort,
		HealthPort:              healthPort,
		VanadisURL:              vanadisURL,
		PostgresURL:             pgURL,
		EriURL:                  eriURL,
		RestateIngressURL:       restateIngressURL,
		NATSURL:                 natsURL,
		GitHubToken:             ghToken,
		GitHubAppID:             strings.TrimSpace(getenv("GITHUB_APP_ID")),
		GitHubAppInstallationID: strings.TrimSpace(getenv("GITHUB_APP_INSTALLATION_ID")),
		GitHubAppPrivateKey:     strings.TrimSpace(getenv("GITHUB_APP_PRIVATE_KEY")),
		ServiceConfigPath:       DefaultServiceConfigPath,
		DiscordToken:            discordToken,
		ShareRoot:               shareRoot,
		AgentName:               agentName,
		ConfigPath:              configPath,
		KubeAPIServer:           prstate.InClusterAPIServer(getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT")),
		FluxReceiverURL:         strings.TrimSpace(getenv("FLUX_RECEIVER_URL")),
	}
}

// bindRestateServices assembles the Restate worker server with Build and
// BuildRouter for agent, plus PRObject/RepoObject only on the default-agent
// runner.
//
// Build and BuildRouter stay agent-scoped (their names come from restateObj
// and routerSvc). PRObject/RepoObject are single cluster-wide objects owned by
// the default-agent runner ("" or "aerial", the BuildServiceName rule): a
// sibling runner registering them would steal PR lifecycle traffic from
// whichever deployment registered last.
//
// projection is the pr_registry writer/reader PRObject uses; nil leaves
// PRObject without registry writes or shadow compares. eri is the effect
// boundary for restate-owned eri wakes and DMs; nil leaves them unconfigured.
// facts publishes restate-owned aerial.pr.> lifecycle facts; nil leaves them
// unconfigured. gh is the GitHub client restate-owned GitHub effects
// (auto-update, update-branch, conflict checks, workflow reruns) call; nil
// leaves them unconfigured. kube is the Kubernetes/Flux client restate-owned
// Flux and image effects (git-push dispatch, image promotion, reconcile
// requests) call; nil leaves them unconfigured.
func bindRestateServices(agent string, restateObj *build.BuildObject, routerSvc *router.BuildRouterService, projection *prstate.PgProjectionStore, eri prstate.EriEffector, facts prstate.FactPublisher, gh *prstate.GitHubClient, kube *prstate.KubeClient) *server.Restate {
	srv := server.NewRestate().
		Bind(restate.Reflect(restateObj)).
		Bind(restate.Reflect(routerSvc))
	if ownsPRState(agent) {
		pr, repo := newPRStateServices(projection, eri, facts, gh, kube)
		prstate.RegisterPRStateServices(srv, pr, repo)
	}
	return srv
}

// newPRStateServices wires the PRObject and RepoObject services with every
// configured boundary; a nil boundary stays unconfigured.
func newPRStateServices(projection *prstate.PgProjectionStore, eri prstate.EriEffector, facts prstate.FactPublisher, gh *prstate.GitHubClient, kube *prstate.KubeClient) (*prstate.PRObjectService, *prstate.RepoObjectService) {
	pr := prstate.NewPRObjectService()
	repo := prstate.NewRepoObjectService()
	if projection != nil {
		pr = pr.WithProjection(projection, projection)
	}
	if eri != nil {
		pr = pr.WithEriEffector(eri)
	}
	if facts != nil {
		pr = pr.WithFactPublisher(facts)
	}
	if gh != nil {
		pr = pr.WithGitHub(gh)
		repo = repo.WithGitHub(gh)
	}
	if kube != nil {
		pr = pr.WithKube(kube)
		repo = repo.WithKube(kube)
	}
	return pr, repo
}

// ownsPRState reports whether agent is the default agent that owns prstate.
func ownsPRState(agent string) bool {
	return model.BuildServiceName(agent) == model.BuildServiceName("")
}

// SetupHealthMux creates the ServeMux for health, metrics, and coverage flush.
func SetupHealthMux() *http.ServeMux {
	return httpkit.SetupHealthMux("vanadis-restate", promhttp.Handler())
}

// RunApp runs both the health server and the Restate worker server.
func RunApp(ctx context.Context, cfg Config) error {
	if strings.TrimSpace(cfg.PostgresURL) == "" {
		return errors.New("postgres_url is required")
	}
	pgStore, closeStore, pErr := initPostgres(ctx, cfg.PostgresURL)
	if pErr != nil {
		return fmt.Errorf("connect to postgres: %w", pErr)
	}
	defer closeStore(context.WithoutCancel(ctx))

	if strings.TrimSpace(cfg.NATSURL) == "" {
		return errors.New("nats_url is required")
	}
	bus, bErr := newBusFn(ctx, cfg.NATSURL)
	if bErr != nil {
		return fmt.Errorf("connect to nats: %w", bErr)
	}
	defer func() {
		if cErr := bus.Close(); cErr != nil {
			log.Printf("[vanadis-restate] warning closing nats: %v", cErr)
		}
	}()
	natsCtx, natsCancel := context.WithTimeout(ctx, 5*time.Second)
	defer natsCancel()
	if sErr := bus.EnsureCoreStreams(natsCtx); sErr != nil {
		return fmt.Errorf("verify nats jetstream: %w", sErr)
	}

	healthMux := SetupHealthMux()
	healthServer := &http.Server{
		Handler:      healthMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	var lc net.ListenConfig
	healthLn, err := lc.Listen(ctx, "tcp", ":"+cfg.HealthPort)
	if err != nil {
		return fmt.Errorf("listening on health port %s: %w", cfg.HealthPort, err)
	}

	errChan := make(chan error, 2)
	go func() {
		log.Printf("[vanadis-restate] Health server listening on :%s", cfg.HealthPort)
		if sErr := healthServer.Serve(healthLn); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			errChan <- sErr
		}
	}()

	// Internal clients propagate traceparent; GitHub calls record spans without leaking headers.
	var widgetDeliverer ericlient.StatusWidgetDeliverer
	if deliverer, dErr := newDiscordWidgetDelivererFn(cfg.AgentName, cfg.DiscordToken, cfg.Environ, cfg.ConfigPath); dErr != nil {
		log.Printf("[vanadis-restate] warning: failed to construct discord widget deliverer: %v", dErr)
	} else {
		widgetDeliverer = deliverer
	}

	eriHTTPClient := tracing.Client(&http.Client{Timeout: 30 * time.Second})
	eriClient := ericlient.NewRestateEriClient(cfg.RestateIngressURL, cfg.EriURL, cfg.AgentName, eriHTTPClient, widgetDeliverer)
	if strings.TrimSpace(cfg.DiscordToken) != "" {
		eriClient.SetDiscordAuth(true)
	}
	// Every GitHub REST client shares one credential source; the transport
	// injects the token, so the clients get no token of their own.
	ghSource := newGitHubTokenSource(gitHubAuthValues{
		AppID:          cfg.GitHubAppID,
		InstallationID: cfg.GitHubAppInstallationID,
		PrivateKeyPEM:  cfg.GitHubAppPrivateKey,
		PAT:            cfg.GitHubToken,
	}, newCredentialSetting(cfg.ServiceConfigPath, os.ReadFile, time.Now, slog.Default()).Current,
		tracing.ExternalClient(&http.Client{Timeout: 10 * time.Second}), slog.Default())
	exec := executor.NewExecutor(cfg.ShareRoot, eriClient, bus, "", newGitHubHTTPClient(ghSource, nil, 30*time.Second))

	restateObj := build.NewBuildObjectWithAgent(cfg.VanadisURL, cfg.AgentName, tracing.Client(&http.Client{Timeout: 30 * time.Second})).WithAuthority(pgStore, exec)
	routerSvc := router.NewBuildRouterServiceWithAgent(pgStore, cfg.AgentName)
	restateServer := bindRestateServices(cfg.AgentName, restateObj, routerSvc, prstate.NewPgProjectionStore(pgStore.DB()), prstate.NewSessionEriEffector(eriClient), prstate.NewBusFactPublisher(bus),
		prstate.NewGitHubClient(newGitHubHTTPClient(ghSource, nil, 30*time.Second), "", ""),
		prstate.NewKubeClient(tracing.ExternalClient(&http.Client{Timeout: 15 * time.Second}), cfg.KubeAPIServer, prstate.DefaultKubeTokenPath, prstate.DefaultKubeCAPath, cfg.FluxReceiverURL))

	go func() {
		log.Printf("[vanadis-restate] Restate HTTP/2 server listening on :%s (VanadisURL: %s)", cfg.RestatePort, cfg.VanadisURL)
		if sErr := restateServer.Start(ctx, ":"+cfg.RestatePort); sErr != nil && !errors.Is(sErr, context.Canceled) {
			errChan <- sErr
		}
	}()

	select {
	case <-ctx.Done():
	case rErr := <-errChan:
		err = rErr
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()
	if sErr := healthServer.Shutdown(shutdownCtx); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) && err == nil {
		err = sErr
	}

	log.Println("[vanadis-restate] Server stopped")
	return err
}

var (
	newPgxPoolFn  = pgxpool.NewWithConfig
	pingPgxPoolFn = func(ctx context.Context, pool *pgxpool.Pool) error {
		return pool.Ping(ctx)
	}
	closePgxPoolFn = func(pool *pgxpool.Pool) {
		if pool != nil {
			pool.Close()
		}
	}
)

var initPostgres = func(ctx context.Context, pgURL string) (*store.Store, func(context.Context), error) {
	poolConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse postgres url: %w", err)
	}
	poolConfig.MaxConns, poolConfig.MinConns = 10, 2
	pool, err := newPgxPoolFn(ctx, poolConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("create pgxpool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pingPgxPoolFn(pingCtx, pool); err != nil {
		closePgxPoolFn(pool)
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}
	st := store.New(pool)
	return st, func(context.Context) {
		closePgxPoolFn(pool)
	}, nil
}
