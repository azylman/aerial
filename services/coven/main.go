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

func handleTranscriptSyncedEvent(ctx context.Context, msg pubsub.Message, bus pubsub.Bus) error {
	var ev pubsub.TranscriptSyncedEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		log.Printf("[Coven] warning: failed to unmarshal transcript synced event: %v", err)
		return nil
	}
	if strings.TrimSpace(ev.ConversationID) == "" || strings.TrimSpace(ev.SessionID) == "healthcheck-ping" {
		return nil
	}
	msgID := fmt.Sprintf("coven:extract:%s:%d", ev.ConversationID, time.Now().UnixNano())
	args := pubsub.CovenExtractJobArgs{
		ThreadID:  ev.ConversationID,
		SessionID: ev.SessionID,
	}
	return bus.Publish(ctx, pubsub.SubjectCovenExtractCoalesced(ev.ConversationID), msgID, args)
}

func nakBackoffDelay(numDelivered uint64) time.Duration {
	if numDelivered <= 1 {
		return 1 * time.Second
	}
	shift := min(numDelivered-1, 4)
	return time.Duration(1<<shift) * time.Second
}

func handleCovenExtractJob(ctx context.Context, msg pubsub.Message, worker *Worker) error {
	var args pubsub.CovenExtractJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		log.Printf("[Coven] error unmarshaling coven extract job: %v", err)
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[Coven] warning: error terming coven extract job: %v", termErr)
		}
		return nil
	}
	if err := worker.ExtractThread(ctx, args.ThreadID); err != nil {
		if msg.NumDelivered() >= 5 {
			log.Printf("[Coven] dropping coven extract job for thread %s after %d delivery attempts: %v", args.ThreadID, msg.NumDelivered(), err)
			if termErr := msg.Term(); termErr != nil {
				log.Printf("[Coven] warning: error terming coven extract job: %v", termErr)
			}
			return nil
		}
		if nakErr := msg.NakWithDelay(nakBackoffDelay(msg.NumDelivered())); nakErr != nil {
			log.Printf("[Coven] warning: error naking coven extract job: %v", nakErr)
		}
		return err
	}
	return nil
}

func subscribeCovenTranscriptSynced(ctx context.Context, b pubsub.Bus) error {
	if b == nil {
		return nil
	}
	_, err := b.Subscribe(ctx, pubsub.SubjectAerialTurnTranscriptSynced, "coven-transcript-synced", func(msgCtx context.Context, msg pubsub.Message) error {
		if err := handleTranscriptSyncedEvent(msgCtx, msg, b); err != nil {
			if msg.NumDelivered() < 5 {
				return err
			}
			log.Printf("[Coven] dropping transcript synced event after %d delivery attempts: %v", msg.NumDelivered(), err)
			return nil
		}
		return nil
	})
	if err != nil {
		log.Printf("[Coven] warning: failed to subscribe coven-transcript-synced: %v", err)
		return err
	}
	log.Println("[Coven] Subscribed coven-transcript-synced durable consumer to " + pubsub.SubjectAerialTurnTranscriptSynced)
	return nil
}

func subscribeCovenExtract(ctx context.Context, b pubsub.Bus, worker *Worker) error {
	if b == nil || worker == nil {
		return nil
	}
	_, err := b.Subscribe(ctx, "aerial.coalesced.coven_extract.>", "coven-extract", func(msgCtx context.Context, msg pubsub.Message) error {
		return handleCovenExtractJob(msgCtx, msg, worker)
	})
	if err != nil {
		log.Printf("[Coven] warning: failed to subscribe coven-extract: %v", err)
		return err
	}
	log.Println("[Coven] Subscribed coven-extract durable consumer to aerial.coalesced.coven_extract.>")
	return nil
}

type covenConsumers struct {
	subscribedTranscriptSynced bool
	subscribedExtract          bool
}

func (c *covenConsumers) subscribeAll(ctx context.Context, b pubsub.Bus, worker *Worker) error {
	if b == nil || worker == nil {
		return nil
	}
	if err := pubsub.EnsureCoreStreams(ctx, b); err != nil {
		log.Printf("[Coven] warning: failed to ensure core streams: %v", err)
	}
	if !c.subscribedTranscriptSynced {
		if err := subscribeCovenTranscriptSynced(ctx, b); err != nil {
			return err
		}
		c.subscribedTranscriptSynced = true
	}
	if !c.subscribedExtract {
		if err := subscribeCovenExtract(ctx, b, worker); err != nil {
			return err
		}
		c.subscribedExtract = true
	}
	return nil
}

var subscribeRetryIntervalNs atomic.Int64

func init() {
	subscribeRetryIntervalNs.Store(int64(5 * time.Second))
}

func getSubscribeRetryInterval() time.Duration {
	return time.Duration(subscribeRetryIntervalNs.Load())
}

func retrySubscribeCovenConsumers(ctx context.Context, b pubsub.Bus, worker *Worker, consumers *covenConsumers) {
	ticker := time.NewTicker(getSubscribeRetryInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := consumers.subscribeAll(ctx, b, worker); err == nil {
				log.Println("[Coven] Subscribed coven consumers on retry")
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
		consumers := &covenConsumers{}
		if subErr := consumers.subscribeAll(ctx, b, worker); subErr != nil {
			log.Printf("[Coven] warning: failed initial coven consumers subscription: %v; will retry in background", subErr)
			go retrySubscribeCovenConsumers(ctx, b, worker, consumers)
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
	store.SetBrainURL(cfg.EriURL)
	ollama := NewOllamaClient(&cfg.Ollama)
	publisher := NewGitHubSkillPublisher(GitHubSkillPublisherConfig{
		Token:           cfg.GitHubPAT,
		Owner:           cfg.ConfigRepoOwner,
		Repo:            cfg.ConfigRepoName,
		PRRegisterURL:   cfg.PRRegisterURL,
		SystemChannelID: cfg.SystemChannelID,
	})
	toolHandler := NewToolHandler(cfg, store, ollama, publisher)
	server := NewServer(toolHandler)

	worker := NewWorker(WorkerConfig{
		Store:           store,
		Ollama:          ollama,
		LLMClient:       HTTPBrainLLMClient(cfg.EriLLMURL, nil),
		Publisher:       publisher,
		CustomSkillsDir: cfg.CustomSkillsDir,
		InitialDelay:    DefaultWorkerInitialDelay,
		TickInterval:    DefaultWorkerTickInterval,
	})
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	bus := initCovenJetStream(ctx, worker, cfg.NATSURL)
	publisher.SetEventBus(bus)
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
