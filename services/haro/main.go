package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/server"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/ticker"
	"github.com/azylman/aerial/services/haro/internal/tools"
)

// RunApp bootstraps the scheduler MCP HTTP server with graceful shutdown.
var (
	openDBFn = store.NewDB
	newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
		return pubsub.NewJetStreamBus(ctx, url)
	}
)

func initHaroJetStream(ctx context.Context, srv *server.Server, database *sql.DB, natsURL string) pubsub.Bus {
	return initHaroJetStreamWithFactory(ctx, srv, database, natsURL, newBusFn)
}

// configureServerEventBus wires the JetStream event bus into the server's
// dispatcher via the exported internal/server seam.
func configureServerEventBus(srv *server.Server, b pubsub.Bus) {
	if srv == nil {
		return
	}
	srv.ConfigureEventBus(b)
}

func initHaroJetStreamWithFactory(ctx context.Context, srv *server.Server, database *sql.DB, natsURL string, factory func(ctx context.Context, url string) (pubsub.Bus, error)) pubsub.Bus {
	natsURL = strings.TrimSpace(natsURL)
	if natsURL == "" {
		return nil
	}
	b, err := factory(ctx, natsURL)
	if err != nil {
		log.Printf("[Haro] warning: failed to connect to nats at %s: %v", natsURL, err)
		return nil
	}
	if err := pubsub.EnsureCoreStreams(ctx, b); err != nil {
		log.Printf("[Haro] warning: failed to ensure core streams: %v", err)
	}
	if _, err := b.ConsumeQueue(ctx, "aerial.dispatch.*", "haro-dispatch", func(msgCtx context.Context, msg pubsub.Message) error {
		return ticker.HandleDispatchOutcome(msgCtx, database, msg)
	}); err != nil {
		log.Printf("[Haro] warning: failed to subscribe haro-dispatch: %v", err)
	} else {
		log.Println("[Haro] Subscribed haro-dispatch queue consumer to aerial.dispatch.*")
	}
	if _, err := b.ConsumeQueue(ctx, pubsub.SubjectHaroOneShotJob, "haro-oneshot", func(msgCtx context.Context, msg pubsub.Message) error {
		return server.HandleOneShotJob(msgCtx, srv, database, msg)
	}); err != nil {
		log.Printf("[Haro] warning: failed to subscribe haro-oneshot: %v", err)
	} else {
		log.Println("[Haro] Subscribed haro-oneshot queue consumer to " + pubsub.SubjectHaroOneShotJob)
	}
	configureServerEventBus(srv, b)
	if srv != nil {
		if _, err := b.Subscribe(ctx, "aerial.events.>", "haro-events", func(msgCtx context.Context, msg pubsub.Message) error {
			var ev pubsub.ExternalEvent
			if unmarshalErr := json.Unmarshal(msg.Data(), &ev); unmarshalErr != nil {
				log.Printf("[Haro] warning: failed to unmarshal external event: %v", unmarshalErr)
				return nil
			}
			if err := srv.HandleExternalEvent(msgCtx, ev); err != nil {
				if msg.NumDelivered() < 5 {
					return err
				}
				log.Printf("[Haro] dropping external event %s after %d delivery attempts: %v", ev.EventID, msg.NumDelivered(), err)
				return nil
			}
			return nil
		}); err != nil {
			log.Printf("[Haro] warning: failed to subscribe haro-events: %v", err)
		} else {
			log.Println("[Haro] Subscribed haro-events durable consumer to aerial.events.>")
		}
	}
	return b
}

func RunApp(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("app: config cannot be nil")
	}
	config.RegisterConfigTokens(cfg)
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		return fmt.Errorf("app: port cannot be empty")
	}
	eriURL := strings.TrimSpace(cfg.EriURL)
	if eriURL == "" {
		return fmt.Errorf("app: eri URL cannot be empty")
	}

	database, err := openDBFn(ctx, cfg)
	if err != nil {
		return err
	}
	defer httpkit.CloseWarn(database, "database on shutdown")

	// Invariant: ticker goroutine must be bound to a child context and strictly
	// awaited on all exit paths before closing the database to prevent goroutine leaks
	// and use-after-close on database shutdown.
	dispatcher := dispatch.NewHTTPPromptDispatcher(cfg.EriURL)
	tickerCtx, tickerCancel := context.WithCancel(ctx)
	var tickerWg sync.WaitGroup
	tickerWg.Go(func() {
		t := ticker.NewTicker(database, dispatcher, 30*time.Second)
		t.Start(tickerCtx)
	})
	defer func() {
		tickerCancel()
		tickerWg.Wait()
	}()

	toolHandler := tools.NewToolHandler(cfg, database)
	srv := server.NewServer(toolHandler, dispatcher)

	bus := initHaroJetStream(ctx, srv, database, cfg.NATSURL)
	defer func() {
		if bus != nil {
			if closeErr := bus.Close(); closeErr != nil {
				log.Printf("[Haro] bus close error: %v", closeErr)
			}
		}
	}()

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("Scheduler MCP Server listening on port %s (db: %s)", port, RedactDSN(cfg.DatabaseURL))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case <-ctx.Done():
	case err := <-errChan:
		return err
	}

	log.Println("Shutting down Scheduler MCP server gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
		return err
	}
	log.Println("Scheduler MCP server stopped cleanly")
	return nil
}

var (
	configPath = defaultConfigPath
	runAppFn   = RunApp
	exitFn     = log.Fatalf
)

func main() {
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))))

	resolvedPath := config.ResolveConfigPath(configPath)
	cfg, err := config.LoadConfig(resolvedPath)
	if err != nil {
		exitFn("Fatal configuration error: %v", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := tracing.Init(ctx, "haro", tracing.DefaultConfigPath)

	err = runAppFn(ctx, cfg)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("Server failure: %v", err)
	}
}
