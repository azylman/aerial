package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/pkg/pubsub"
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

// restateObjectPath targets the canonical Build object.
func (s *VanadisServer) restateObjectPath(runnerAgent, buildID, handler string) string {
	return s.restateClient().ObjectPath(buildplan.BuildServiceName(runnerAgent), buildID, handler)
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

// InitPostgres initializes the PostgreSQL store backing the server.
func InitPostgres(ctx context.Context, server *VanadisServer, pgURL string) (func(context.Context), error) {
	pool, err := pgutil.NewPool(ctx, pgURL, pgutil.PoolOptions{MaxConns: 10, MinConns: 2})
	if err != nil {
		return nil, err
	}
	server.SetStore(NewPostgresVanadisStore(pool))
	return func(context.Context) {
		pool.Close()
	}, nil
}

var initPostgres = InitPostgres

// queueStatsBus reads consumer backlog for the queue gauges and is closed on shutdown.
type queueStatsBus interface {
	consumerStatsQuerier
	Close() error
}

var newQueueStatsBusFn = func(ctx context.Context, natsURL string) (queueStatsBus, error) {
	return pubsub.NewJetStreamBus(ctx, natsURL)
}

// initQueueStats connects the JetStream stats reader behind vanadis_queue_* when a
// NATS URL is configured. It is metrics-only, so a failure is logged, not fatal.
func initQueueStats(ctx context.Context, server *VanadisServer, natsURL string, newBus func(context.Context, string) (queueStatsBus, error)) func() {
	if natsURL == "" {
		return func() {}
	}
	bus, err := newBus(ctx, natsURL)
	if err != nil {
		log.Printf("[vanadis] queue metrics disabled: %v", err)
		return func() {}
	}
	server.queueStats = bus
	log.Println("[vanadis] JetStream queue metrics initialized")
	return func() {
		if err := bus.Close(); err != nil {
			log.Printf("[vanadis] JetStream close error: %v", err)
		}
	}
}

func runServer(ctx context.Context, cfg Config, restateCfg RestateConfig, onReady func(addr string)) error {
	server := NewVanadisServer(cfg, restateCfg, nil, nil)
	var cleanupDB func(context.Context)
	if cfg.PostgresURL != "" {
		cleanup, err := initPostgres(ctx, server, cfg.PostgresURL)
		if err != nil {
			return err
		}
		cleanupDB = cleanup
		log.Println("[vanadis] PostgreSQL store initialized")
	}
	closeQueueStats := initQueueStats(ctx, server, cfg.NATSURL, newQueueStatsBusFn)
	defer closeQueueStats()

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

// RunApp runs the Vanadis orchestrator application until the context is
// canceled. It is the only place that resolves Restate config from
// CONFIG_PATH and its env overrides.
func RunApp(ctx context.Context, cfg Config) error {
	return runServer(ctx, cfg, LoadRestateConfig(os.Getenv("CONFIG_PATH")), nil)
}
