package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

var (
	openDBFn = pgutil.OpenSQL
	newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
		return pubsub.NewJetStreamBus(ctx, url)
	}
	httpShutdown = (*http.Server).Shutdown
)

// RunApp bootstraps the transcripts indexer: JetStream step-batch consumer plus
// HTTP read API, with graceful shutdown.
func RunApp(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("app: config cannot be nil")
	}
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		return fmt.Errorf("app: port cannot be empty")
	}

	database, err := openDBFn(ctx, cfg.DatabaseURL, pgutil.SQLOptions{})
	if err != nil {
		return fmt.Errorf("db: %w", err)
	}
	defer httpkit.CloseWarn(database, "database on shutdown")

	store := NewStore(database, cfg.Stack)
	ingester := NewIngester(store, nil)

	bus := initTranscriptsJetStream(ctx, ingester, cfg.NATSURL)
	defer func() {
		if bus != nil {
			if closeErr := bus.Close(); closeErr != nil {
				log.Printf("[Transcripts] bus close error: %v", closeErr)
			}
		}
	}()
	if bus != nil {
		ingester.publish = NewBusIngester(store, bus).publish
	}
	stopSummarizer := startSummarizerLoop(ctx, store, cfg)
	defer stopSummarizer()

	var auth *k8sauth.Authenticator
	if cfg.Auth.Enabled {
		auth = k8sauth.NewAuthenticator(cfg.Auth)
	}
	server := NewServer(store, auth)
	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[Transcripts] Indexer listening on port %s (stack: %s)", port, cfg.Stack)
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

	log.Println("[Transcripts] Shutting down indexer gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()
	if err := httpShutdown(httpServer, shutdownCtx); err != nil {
		log.Printf("[Transcripts] HTTP server shutdown error: %v", err)
		return err
	}
	return nil
}

// startSummarizerLoop runs the dark per-stack idle summarizer in the
// background. Disabled configs (the default) return a no-op stop and start
// nothing; enabled configs sweep RunOnce on an interval until ctx ends.
func startSummarizerLoop(ctx context.Context, store *Store, cfg *Config) func() {
	if cfg == nil || store == nil || !cfg.Summarizer.Enabled {
		return func() {}
	}
	llm := NewLLMClient(cfg.Summarizer.EriLLMURL, nil)
	embedder := NewEmbedder(cfg.Summarizer.SensesURL, cfg.Summarizer.SensesModel, nil)
	summarizer := NewSummarizer(store, llm, embedder, cfg.Summarizer)
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		if _, err := summarizer.RunOnce(loopCtx); err != nil {
			log.Printf("[Transcripts] summarizer sweep error: %v", err)
		}
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				if _, err := summarizer.RunOnce(loopCtx); err != nil {
					log.Printf("[Transcripts] summarizer sweep error: %v", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func initTranscriptsJetStream(ctx context.Context, ingester *Ingester, natsURL string) pubsub.Bus {
	return initTranscriptsJetStreamWithFactory(ctx, ingester, natsURL, newBusFn)
}

func initTranscriptsJetStreamWithFactory(ctx context.Context, ingester *Ingester, natsURL string, factory func(ctx context.Context, url string) (pubsub.Bus, error)) pubsub.Bus {
	natsURL = strings.TrimSpace(natsURL)
	if natsURL == "" || ingester == nil {
		return nil
	}
	b, err := factory(ctx, natsURL)
	if err != nil {
		log.Printf("[Transcripts] warning: failed to connect to nats at %s: %v", natsURL, err)
		return nil
	}
	if err := pubsub.EnsureCoreStreams(ctx, b); err != nil {
		log.Printf("[Transcripts] warning: failed to ensure core streams: %v", err)
	}
	if _, err := b.ConsumeQueue(ctx, pubsub.SubjectAerialTranscriptsPrefix+">", "transcripts-indexer", func(msgCtx context.Context, msg pubsub.Message) error {
		return ingester.HandleBatchMessage(msgCtx, msg)
	}); err != nil {
		log.Printf("[Transcripts] warning: failed to subscribe transcripts-indexer: %v", err)
	} else {
		log.Println("[Transcripts] Subscribed transcripts-indexer queue consumer to " + pubsub.SubjectAerialTranscriptsPrefix + ">")
	}
	return b
}

var (
	configPath = DefaultConfigPath
	runAppFn   = RunApp
	exitFn     = log.Fatalf
)

func main() {
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))))

	resolvedPath := ResolveConfigPath(configPath)
	cfg, err := LoadConfig(resolvedPath)
	if err != nil {
		exitFn("Fatal configuration error: %v", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := tracing.Init(ctx, "transcripts", tracing.DefaultConfigPath)

	err = runAppFn(ctx, cfg)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("Server failure: %v", err)
	}
}
