package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/services/vanadis/internal/metrics"
	"github.com/azylman/aerial/services/vanadis/internal/restate"
)

// Client proxies synchronous commands to Restate ingress.
type Client = restate.Client

var (
	// NewClient constructs a Restate ingress Client.
	NewClient = restate.NewClient
	// MapRestateError maps a Restate ingress error status onto a Vanadis sentinel error.
	MapRestateError = restate.MapRestateError
	// ErrRestateTimeout reports an upstream timeout whose acceptance is unknown.
	ErrRestateTimeout = restate.ErrRestateTimeout
	// CallIngress proxies a synchronous command to Restate ingress.
	CallIngress = (*Client).CallIngress
	// CallRetryAction posts a durable action-retry request for a build.
	CallRetryAction = (*Client).CallRetryAction
)

var (
	// BuildsActive tracks active Vanadis builds by queue state.
	BuildsActive = metrics.BuildsActive
	// ChunksActive tracks active Vanadis chunks across non-completed builds by lifecycle status.
	ChunksActive = metrics.ChunksActive
	// BuildsTotal tracks total Vanadis builds by lifecycle or terminal status.
	BuildsTotal = metrics.BuildsTotal
	// ChunksTotal tracks total Vanadis chunks by lifecycle event status.
	ChunksTotal = metrics.ChunksTotal
	// QueueDepth tracks chunk execution jobs in the Vanadis JetStream queue categorized by state.
	QueueDepth = metrics.QueueDepth
	// QueueOldestAgeSeconds tracks the age in seconds of the oldest pending job in the Vanadis queue.
	QueueOldestAgeSeconds = metrics.QueueOldestAgeSeconds
	// RecordBuildTotal increments the vanadis_builds_total counter for a given status.
	RecordBuildTotal = metrics.RecordBuildTotal
	// NewHandler returns the Prometheus metrics HTTP handler.
	NewHandler = metrics.NewHandler
)

// consumerStatsQuerier reports pending and in-flight consumer counts.
type consumerStatsQuerier = metrics.ConsumerStatsQuerier

// CancelBuildRequest represents a cancel build mutation payload.
type CancelBuildRequest struct{}

// RemoveChunkRequest represents a request to remove a chunk from a build.
type RemoveChunkRequest struct {
	ChunkRef string `json:"chunk_ref"`
}

// FailChunkRequest carries the chunk and reason for failing a chunk.
type FailChunkRequest struct {
	DispatchID string `json:"dispatch_id,omitempty"`
	BuildID    string `json:"build_id,omitempty"`
	ChunkID    string `json:"chunk_id"`
	Reason     string `json:"reason"`
}

// FailChunkResponse represents the result of failing a chunk.
type FailChunkResponse struct {
	Failed      bool   `json:"failed"`
	BuildID     string `json:"build_id"`
	ChunkID     string `json:"chunk_id"`
	BuildStatus string `json:"build_status"`
	ChunkStatus string `json:"chunk_status"`
}

// AttachChunkPRResponse represents the result of attaching a PR to a chunk or standalone build.
type AttachChunkPRResponse struct {
	Status   string `json:"status"`
	BuildID  string `json:"build_id"`
	ChunkID  string `json:"chunk_id"`
	ThreadID string `json:"thread_id"`
}

func (s *VanadisServer) restateClient() *Client {
	if s == nil {
		return NewClient(RestateConfig{}, nil)
	}
	return NewClient(s.restateCfg, s.httpClient)
}

func (s *VanadisServer) restateObjectPath(buildID, handler string) string {
	return s.restateClient().ObjectPath(buildID, handler)
}

func (s *VanadisServer) restateRouterPath(handler string) string {
	return s.restateClient().RouterPath(handler)
}

// callRestateIngress proxies a synchronous command to Restate ingress.
func (s *VanadisServer) callRestateIngress(ctx context.Context, path string, payload any, out any) error {
	return s.restateClient().CallIngress(ctx, path, payload, out)
}

func (s *VanadisServer) collectBuildMetrics(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	builds, err := s.store.ListBuilds(ctx, 100)
	if err != nil {
		return
	}
	metrics.CollectBuildMetrics(builds)
}

func (s *VanadisServer) collectQueueMetrics(ctx context.Context) {
	if s == nil {
		metrics.ResetQueueMetrics()
		return
	}
	metrics.CollectQueueMetrics(ctx, s.consumerStatsQuerier())
}

func (s *VanadisServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s != nil {
		s.collectBuildMetrics(r.Context())
		s.collectQueueMetrics(r.Context())
	}
	promhttp.Handler().ServeHTTP(w, r)
}

var newPgxPoolFn = pgxpool.NewWithConfig

// InitPostgres initializes the PostgreSQL store backing the server.
func InitPostgres(ctx context.Context, server *VanadisServer, pgURL string) (func(context.Context), error) {
	poolConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	poolConfig.MaxConns, poolConfig.MinConns = 10, 2
	pool, err := newPgxPoolFn(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pgxpool: %w", err)
	}
	server.SetStore(NewPostgresVanadisStore(pool))
	return func(context.Context) {
		pool.Close()
	}, nil
}

var initPostgres = InitPostgres

func runServer(ctx context.Context, cfg Config, onReady func(addr string)) error {
	server := NewVanadisServer(cfg, nil, nil)
	var cleanupDB func(context.Context)
	if cfg.PostgresURL != "" {
		cleanup, err := initPostgres(ctx, server, cfg.PostgresURL)
		if err != nil {
			return err
		}
		cleanupDB = cleanup
		log.Println("[vanadis] PostgreSQL store initialized")
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+cfg.Port)
	if err != nil {
		if cleanupDB != nil {
			cleanupDB(context.WithoutCancel(ctx))
		}
		return fmt.Errorf("listening on port %s: %w", cfg.Port, err)
	}
	httpServer := &http.Server{Handler: server.Routes(), ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	if onReady != nil {
		onReady(ln.Addr().String())
	}
	errChan := make(chan error, 1)
	go func() {
		log.Printf("[vanadis] Server listening on %s (BrainURL: %s)", ln.Addr().String(), cfg.BrainURL)
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errChan:
	}
	log.Println("[vanadis] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()
	if shutErr := httpServer.Shutdown(shutdownCtx); shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
		log.Printf("[vanadis] HTTP shutdown error: %v", shutErr)
	}
	if cleanupDB != nil {
		cleanupDB(shutdownCtx)
	}
	log.Println("[vanadis] Server stopped")
	return runErr
}

// RunApp runs the Vanadis orchestrator application until the context is canceled.
func RunApp(ctx context.Context, cfg Config) error { return runServer(ctx, cfg, nil) }
