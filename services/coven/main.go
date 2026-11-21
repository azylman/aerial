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

func handleCovenExtractJob(ctx context.Context, msg pubsub.Message, worker *Worker) error {
	var args pubsub.CovenExtractJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		slog.ErrorContext(ctx, "[Coven] error unmarshaling coven extract job", "error", err)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[Coven] error terming coven extract job", "error", termErr)
		}
		return nil
	}
	threadID := strings.TrimSpace(args.ThreadID)
	targetAgent := args.EffectiveAgent()
	if subjectAgent, subjThread, ok := ParseCovenExtractSubject(msg.Subject()); ok {
		targetAgent = subjectAgent
		if threadID == "" {
			threadID = subjThread
		}
	}
	if threadID == "" {
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[Coven] error terming empty thread coven extract job", "error", termErr)
		}
		return nil
	}
	if !IsValidAgentSlug(targetAgent) {
		slog.WarnContext(ctx, "[Coven] invalid agent slug in coven extract job", "agent", targetAgent)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[Coven] error terming invalid agent slug coven extract job", "error", termErr)
		}
		return nil
	}
	if worker != nil {
		isAll := strings.EqualFold(strings.TrimSpace(worker.Role()), "all")
		if !isAll && NormalizeStack(targetAgent) != NormalizeStack(worker.Stack()) {
			return nil
		}
	}
	if err := worker.ExtractThread(ctx, threadID, targetAgent); err != nil {
		if errors.Is(err, ErrAdvisoryLockBusy) {
			if nakErr := msg.NakWithDelay(5 * time.Second); nakErr != nil {
				slog.WarnContext(ctx, "[Coven] error naking coven extract job on advisory lock busy", "error", nakErr)
			}
			return err
		}
		if nakErr := msg.NakWithDelay(covenNakDelay(msg.NumDelivered())); nakErr != nil {
			slog.WarnContext(ctx, "[Coven] error naking coven extract job", "error", nakErr)
		}
		return err
	}
	return nil
}

func covenTranscriptsIndexedDurable(stack string) string {
	s := NormalizeStack(stack)
	if s == DefaultStack {
		return "coven-transcripts-indexed"
	}
	return "coven-transcripts-indexed-" + s
}

func covenExtractDurable(stack string) string {
	s := NormalizeStack(stack)
	if s == DefaultStack {
		return "coven-extract"
	}
	return "coven-extract-" + s
}

func covenExtractSubjectFilter(stack string, role ...string) string {
	if len(role) > 0 && strings.EqualFold(strings.TrimSpace(role[0]), "all") {
		return "aerial.coalesced.coven_extract.>"
	}
	s := NormalizeStack(stack)
	if s == DefaultStack {
		return "aerial.coalesced.coven_extract.>"
	}
	return pubsub.SubjectCovenExtractCoalescedForAgent(s, ">")
}

func subscribeCovenTranscriptsIndexed(ctx context.Context, b pubsub.Bus, stack string, role ...string) error {
	if b == nil {
		return nil
	}
	subject := pubsub.SubjectAerialTurnPrefix + "transcripts_indexed"
	durable := covenTranscriptsIndexedDurable(stack)
	userRole := ""
	if len(role) > 0 {
		userRole = strings.TrimSpace(role[0])
	}
	_, err := b.Subscribe(ctx, subject, durable, newTranscriptsIndexedHandler(b, stack, userRole))
	if err != nil {
		log.Printf("[Coven] warning: failed to subscribe %s: %v", durable, err)
		return err
	}
	log.Printf("[Coven] Subscribed %s durable consumer to %s", durable, subject)
	return nil
}

func subscribeCovenExtract(ctx context.Context, b pubsub.Bus, worker *Worker) error {
	if b == nil || worker == nil {
		return nil
	}
	stack := worker.Stack()
	subject := covenExtractSubjectFilter(stack, worker.Role())
	durable := covenExtractDurable(stack)
	_, err := b.Subscribe(ctx, subject, durable, func(msgCtx context.Context, msg pubsub.Message) error {
		return handleCovenExtractJob(msgCtx, msg, worker)
	})
	if err != nil {
		log.Printf("[Coven] warning: failed to subscribe %s: %v", durable, err)
		return err
	}
	log.Printf("[Coven] Subscribed %s durable consumer to %s", durable, subject)
	return nil
}

type covenConsumers struct {
	subscribedTranscriptsIndexed bool
	subscribedExtract            bool
}

func (c *covenConsumers) subscribeAll(ctx context.Context, b pubsub.Bus, worker *Worker) error {
	if b == nil || worker == nil {
		return nil
	}
	if err := pubsub.EnsureCoreStreams(ctx, b); err != nil {
		log.Printf("[Coven] warning: failed to ensure core streams: %v", err)
	}
	if !c.subscribedTranscriptsIndexed {
		if err := subscribeCovenTranscriptsIndexed(ctx, b, worker.Stack(), worker.Role()); err != nil {
			return err
		}
		c.subscribedTranscriptsIndexed = true
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

	database, err := OpenDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer httpkit.CloseWarn(database, "database on shutdown")

	store := NewSQLStore(database)
	store.SetStack(cfg.EffectiveStack())
	if cfg.TranscriptsURL != "" {
		indexerClient := NewIndexerClient(cfg.TranscriptsURL, cfg.EffectiveStack(), nil)
		indexerClient.SetMaxSteps(cfg.ExtractBatchLimit())
		store.SetIndexerClient(indexerClient)
	}
	ollama := NewOllamaClient(&cfg.Ollama)
	publisher := NewGitHubSkillPublisher(GitHubSkillPublisherConfig{
		Token:           cfg.GitHubPAT,
		Owner:           cfg.ConfigRepoOwner,
		Repo:            cfg.ConfigRepoName,
		PRRegisterURL:   cfg.PRRegisterURL,
		SystemChannelID: cfg.SystemChannelID,
		EriURL:          cfg.EriURL,
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
		Role:            cfg.Role,
		Stack:           cfg.EffectiveStack(),
		ExtractTimeout:  cfg.ExtractTimeout(),
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

	if !strings.EqualFold(cfg.Role, "extractor") {
		go func() {
			if err := worker.Start(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[Coven Worker] Background worker exited: %v", err)
			}
		}()
	}

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
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
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
	_, flushTracing := tracing.Init(ctx, "coven", tracing.DefaultConfigPath)

	err = runAppFn(ctx, cfg)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("Server failure: %v", err)
	}
}
