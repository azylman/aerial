package app

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/prstate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/router"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

var newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url, nats.RetryOnFailedConnect(false), nats.Timeout(5*time.Second))
}

// Config holds runtime configuration for the vanadis-restate worker service.
type Config struct {
	RestatePort string
	HealthPort  string
	VanadisURL  string
	PostgresURL string
	EriURL      string
	NATSURL     string
	GitHubToken string
	ShareRoot   string
	AgentName   string
}

// LoadConfigFromEnv loads configuration from environment variables with defaults.
func LoadConfigFromEnv() Config {
	restatePort := strings.TrimSpace(os.Getenv("RESTATE_PORT"))
	if restatePort == "" {
		restatePort = strings.TrimSpace(os.Getenv("PORT"))
	}
	if restatePort == "" {
		restatePort = "9080"
	}

	healthPort := strings.TrimSpace(os.Getenv("HEALTH_PORT"))
	if healthPort == "" {
		healthPort = "4026"
	}

	vanadisURL := strings.TrimRight(strings.TrimSpace(os.Getenv("VANADIS_URL")), "/")
	if vanadisURL == "" {
		vanadisURL = "http://vanadis.aerial:4025"
	}

	pgURL := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("DATABASE_URL"), os.Getenv("POSTGRES_URL"), httpkit.BuildPostgresDSN(os.Getenv, "aerial")))
	eriURL := strings.TrimRight(strings.TrimSpace(store.FirstNonEmpty(os.Getenv("ERI_URL"), "http://eri.aerial")), "/")
	natsURL := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("NATS_URL"), "nats://nats.aerial:4222"))
	ghToken := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("GITHUB_TOKEN"), os.Getenv("GITHUB_PAT")))
	shareRoot := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("SHARE_ROOT"), executor.DefaultShareRoot))
	agentName := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("AGENT_NAME"), "aerial"))

	return Config{
		RestatePort: restatePort,
		HealthPort:  healthPort,
		VanadisURL:  vanadisURL,
		PostgresURL: pgURL,
		EriURL:      eriURL,
		NATSURL:     natsURL,
		GitHubToken: ghToken,
		ShareRoot:   shareRoot,
		AgentName:   agentName,
	}
}

// bindRestateServices assembles the Restate worker server with Build,
// BuildRouter, and the dark PRObject/RepoObject state services. The synapse
// ingress invoker addresses PRObject/RepoObject, but all synapse consumers
// stay on legacy trackers: nothing routes here until the cutover chunk.
func bindRestateServices(restateObj *build.BuildObject, routerSvc *router.BuildRouterService) *server.Restate {
	srv := server.NewRestate().
		Bind(restate.Reflect(restateObj)).
		Bind(restate.Reflect(routerSvc))
	prstate.RegisterPRStateServices(srv, prstate.NewPRObjectService(), prstate.NewRepoObjectService())
	return srv
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
	eriClient := executor.NewHTTPEriClient(cfg.EriURL, tracing.Client(&http.Client{Timeout: 30 * time.Second}))
	exec := executor.NewExecutor(cfg.ShareRoot, eriClient, bus, cfg.GitHubToken, tracing.ExternalClient(&http.Client{Timeout: 30 * time.Second}))

	restateObj := build.NewBuildObjectWithAgent(cfg.VanadisURL, cfg.AgentName, tracing.Client(&http.Client{Timeout: 30 * time.Second})).WithAuthority(pgStore, exec)
	routerSvc := router.NewBuildRouterServiceWithAgent(pgStore, cfg.AgentName)
	restateServer := bindRestateServices(restateObj, routerSvc)

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
