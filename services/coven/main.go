package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

var newCovenBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url)
}

func initCovenJetStream(ctx context.Context, worker *Worker, natsURL string) pubsub.Bus {
	return initCovenJetStreamWithFactory(ctx, worker, natsURL, newCovenBusFn)
}

func subscribeCovenTurns(ctx context.Context, b pubsub.Bus, worker *Worker) error {
	if b == nil || worker == nil {
		return nil
	}
	if err := pubsub.EnsureCoreStreams(ctx, b); err != nil {
		log.Printf("[Coven] warning: failed to ensure core streams: %v", err)
	}
	_, err := b.Subscribe(ctx, "aerial.turn.completed", "coven-turns", func(msgCtx context.Context, msg pubsub.Message) error {
		var ev pubsub.TurnCompletedEvent
		if unmarshalErr := json.Unmarshal(msg.Data(), &ev); unmarshalErr != nil {
			log.Printf("[Coven] warning: failed to unmarshal turn completed event: %v", unmarshalErr)
			return nil
		}
		if err := worker.HandleTurnCompletedEvent(msgCtx, ev); err != nil {
			if msg.NumDelivered() < 5 {
				return err
			}
			log.Printf("[Coven] dropping turn completed event %s after %d delivery attempts: %v", ev.SessionID, msg.NumDelivered(), err)
			return nil
		}
		return nil
	})
	if err != nil {
		log.Printf("[Coven] warning: failed to subscribe coven-turns: %v", err)
		return err
	}
	log.Println("[Coven] Subscribed coven-turns durable consumer to aerial.turn.completed")
	return nil
}

var subscribeRetryIntervalNs atomic.Int64

func init() {
	subscribeRetryIntervalNs.Store(int64(5 * time.Second))
}

func getSubscribeRetryInterval() time.Duration {
	return time.Duration(subscribeRetryIntervalNs.Load())
}

func retrySubscribeCovenTurns(ctx context.Context, b pubsub.Bus, worker *Worker) {
	ticker := time.NewTicker(getSubscribeRetryInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := subscribeCovenTurns(ctx, b, worker); err == nil {
				log.Println("[Coven] Subscribed coven-turns durable consumer to aerial.turn.completed on retry")
				return
			}
		}
	}
}

func initCovenJetStreamWithFactory(ctx context.Context, worker *Worker, natsURL string, factory func(ctx context.Context, url string) (pubsub.Bus, error)) pubsub.Bus {
	natsURL = strings.TrimSpace(natsURL)
	if natsURL == "" {
		return nil
	}
	b, err := factory(ctx, natsURL)
	if err != nil {
		log.Printf("[Coven] warning: failed to connect to nats at %s: %v", natsURL, err)
		return nil
	}
	if worker != nil {
		if subErr := subscribeCovenTurns(ctx, b, worker); subErr != nil {
			log.Printf("[Coven] warning: failed initial coven-turns subscription: %v; will retry in background", subErr)
			go retrySubscribeCovenTurns(ctx, b, worker)
		}
	}
	return b
}

// RunApp bootstraps the coven MCP HTTP server with graceful shutdown.
func RunApp(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("app: config cannot be nil")
	}
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		port = DefaultPort
	}

	database, err := OpenDB(cfg)
	if err != nil {
		return err
	}
	defer httpkit.CloseWarn(database, "database on shutdown")

	store := NewSQLStore(database)
	store.SetBrainURL(cfg.BrainURL)
	ollama := NewOllamaClient(&cfg.Ollama)
	publisher := NewGitHubSkillPublisher(GitHubSkillPublisherConfig{
		Token:              cfg.GitHubPAT,
		Owner:              cfg.ConfigRepoOwner,
		Repo:               cfg.ConfigRepoName,
		BrainURL:           cfg.BrainURL,
		BrainPRRegisterURL: cfg.BrainPRRegisterURL,
		SystemChannelID:    cfg.SystemChannelID,
	})
	toolHandler := NewToolHandler(cfg, store, ollama, publisher)
	server := NewServer(toolHandler)

	worker := NewWorker(WorkerConfig{
		Store:           store,
		Ollama:          ollama,
		LLMClient:       HTTPBrainLLMClient(cfg.BrainLLMURL, nil),
		Publisher:       publisher,
		CustomSkillsDir: cfg.CustomSkillsDir,
		InitialDelay:    DefaultWorkerInitialDelay,
		TickInterval:    DefaultWorkerTickInterval,
	})
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	bus := initCovenJetStream(ctx, worker, cfg.NATSURL)
	defer func() {
		if bus != nil {
			if closeErr := bus.Close(); closeErr != nil {
				log.Printf("[Coven] bus close error: %v", closeErr)
			}
		}
	}()

	go func() {
		if err := worker.Start(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Coven Worker] Background worker exited: %v", err)
		}
	}()

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           server.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("Coven MCP Server listening on port %s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case <-ctx.Done():
	case err := <-errChan:
		return err
	}

	log.Println("Shutting down Coven MCP server gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
		return err
	}
	log.Println("Coven MCP server stopped cleanly")
	return nil
}

var (
	configPath = DefaultConfigPath
	runAppFn   = RunApp
	exitFn     = log.Fatalf
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	resolvedPath := ResolveConfigPath(configPath)
	cfg, err := LoadConfig(resolvedPath)
	if err != nil {
		exitFn("Fatal configuration error: %v", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := tracing.Init(ctx, "coven", tracing.DefaultConfigPath)

	err = runAppFn(ctx, cfg)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("Server failure: %v", err)
	}
}
