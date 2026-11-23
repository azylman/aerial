package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	restate "github.com/restatedev/sdk-go"
	restateserver "github.com/restatedev/sdk-go/server"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

var newCovenBusFn = func(ctx context.Context, busURL string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, busURL)
}

func initCovenJetStream(ctx context.Context, worker *Worker, natsURL string) pubsub.Bus {
	return initCovenJetStreamWithFactory(ctx, worker, natsURL, newCovenBusFn)
}

// dispatchCovenExtract forwards thread fact extraction to the CovenThread Virtual Object.
// Invocations are dispatched asynchronously to Restate (/send) with a message-scoped idempotency key
// (coven:extract:<scopedKey>:<msgID>), so slow extractions never cause HTTP client timeouts or duplicate
// in-flight runs on redelivery. When no stable message ID is present, the Idempotency-Key header is omitted
// so distinct events are never collapsed.
func dispatchCovenExtract(ctx context.Context, worker *Worker, targetAgent, threadID string, msgKey ...string) error {
	if worker == nil {
		return nil
	}
	if worker.RestateIngressURL() == "" {
		return worker.ExtractThread(ctx, threadID, targetAgent)
	}

	scopedKey := config.ScopedThreadKey(targetAgent, threadID)
	endpoint := fmt.Sprintf("%s/CovenThread/%s/Extract/send", worker.RestateIngressURL(), url.PathEscape(scopedKey))
	req := ExtractJobRequest{
		ThreadID: threadID,
		Agent:    targetAgent,
	}
	var idempotencyKey string
	if len(msgKey) > 0 && strings.TrimSpace(msgKey[0]) != "" {
		idempotencyKey = fmt.Sprintf("coven:extract:%s:%s", scopedKey, strings.TrimSpace(msgKey[0]))
	}
	_, err := restateingress.Post(ctx, worker.HTTPClient(), endpoint, idempotencyKey, req, 0)
	return err
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
	if !k8sauth.IsValidAgentSlug(strings.TrimSpace(targetAgent)) {
		slog.WarnContext(ctx, "[Coven] invalid agent slug in coven extract job", "agent", targetAgent)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[Coven] error terming invalid agent slug coven extract job", "error", termErr)
		}
		return nil
	}
	if worker != nil {
		isAll := strings.EqualFold(strings.TrimSpace(worker.Role()), "all")
		if !isAll && config.NormalizeStack(targetAgent) != config.NormalizeStack(worker.Stack()) {
			return nil
		}
	}
	var msgKey string
	if msg != nil && msg.Header() != nil {
		msgKey = strings.TrimSpace(msg.Header().Get(pubsub.HeaderMsgID))
	}
	if err := dispatchCovenExtract(ctx, worker, targetAgent, threadID, msgKey); err != nil {
		if msg != nil {
			if nakErr := msg.NakWithDelay(pubsub.NakBackoff(msg.NumDelivered(), covenMaxNakDelay)); nakErr != nil {
				slog.WarnContext(ctx, "[Coven] error naking coven extract job", "error", nakErr)
			}
		}
		return err
	}
	return nil
}

func covenTranscriptsIndexedDurable(stack string) string {
	s := config.NormalizeStack(stack)
	if s == config.DefaultStack {
		return "coven-transcripts-indexed"
	}
	return "coven-transcripts-indexed-" + s
}

func covenExtractDurable(stack string) string {
	s := config.NormalizeStack(stack)
	if s == config.DefaultStack {
		return "coven-extract"
	}
	return "coven-extract-" + s
}

func covenExtractSubjectFilter(stack string, role ...string) string {
	if len(role) > 0 && strings.EqualFold(strings.TrimSpace(role[0]), "all") {
		return "aerial.coalesced.coven_extract.>"
	}
	s := config.NormalizeStack(stack)
	if s == config.DefaultStack {
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

func initCovenJetStreamWithFactory(ctx context.Context, worker *Worker, natsURL string, factory func(ctx context.Context, busURL string) (pubsub.Bus, error)) pubsub.Bus {
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

// NewRestateServer binds the Coven Virtual Objects and returns a Restate server.
func NewRestateServer(thread *CovenThread, dreamer *CovenDreamer) *restateserver.Restate {
	tenMin := []restate.ServiceDefinitionOption{
		restate.WithInactivityTimeout(10 * time.Minute),
		restate.WithAbortTimeout(10 * time.Minute),
	}
	srv := restateserver.NewRestate()
	if thread != nil {
		srv = srv.Bind(restate.Reflect(thread, tenMin...))
	}
	if dreamer != nil {
		srv = srv.Bind(restate.Reflect(dreamer, tenMin...))
	}
	return srv
}

func startRestateServer(ctx context.Context, cfg *config.Config, thread *CovenThread, dreamer *CovenDreamer) (net.Listener, *http.Server, <-chan error, error) {
	restateSrv := NewRestateServer(thread, dreamer)
	restateHandler, err := restateSrv.Handler()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create restate handler: %w", err)
	}

	restatePort := strings.TrimSpace(cfg.RestatePort)
	if restatePort == "" {
		restatePort = config.DefaultRestatePort
	}

	var restateProtocols http.Protocols
	restateProtocols.SetHTTP1(true)
	restateProtocols.SetUnencryptedHTTP2(true)

	restateHTTPServer := &http.Server{
		Addr:              ":" + restatePort,
		Handler:           restateHandler,
		Protocols:         &restateProtocols,
		ReadHeaderTimeout: 10 * time.Second,
	}

	var lc net.ListenConfig
	restateLn, err := lc.Listen(ctx, "tcp", ":"+restatePort)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("listen on restate port %s: %w", restatePort, err)
	}

	restateErrChan := make(chan error, 1)
	go func() {
		log.Printf("Coven Restate Server listening on %s", restateLn.Addr().String())
		if sErr := restateHTTPServer.Serve(restateLn); sErr != nil && sErr != http.ErrServerClosed {
			restateErrChan <- sErr
		}
		close(restateErrChan)
	}()

	return restateLn, restateHTTPServer, restateErrChan, nil
}

const (
	restateRegisterInitialBackoff = time.Second
	restateRegisterMaxBackoff     = 30 * time.Second
	restateRegisterAttemptTimeout = 10 * time.Second
)

type restateDeploymentRequest struct {
	URI   string `json:"uri"`
	Force bool   `json:"force"`
}

// RegisterRestateDeployment registers coven's advertised Restate endpoint (CovenThread, CovenDreamer)
// with the Restate admin API. force=true matches haro and prospera so a redeploy replaces the old revision.
func RegisterRestateDeployment(ctx context.Context, client *http.Client, adminURL, advertiseURL string) error {
	cleanAdmin := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	cleanAdv := strings.TrimSpace(advertiseURL)
	if cleanAdmin == "" || cleanAdv == "" {
		return nil
	}
	if _, err := restateingress.Post(ctx, client, cleanAdmin+"/deployments", "", restateDeploymentRequest{URI: cleanAdv, Force: true}, 1024); err != nil {
		return fmt.Errorf("register restate deployment at %s/deployments: %w", cleanAdmin, err)
	}
	return nil
}

// registerRestateUntilSuccess retries RegisterRestateDeployment with doubling backoff (capped at
// restateRegisterMaxBackoff) until it succeeds or ctx ends. It sets coven_restate_registered on success.
func registerRestateUntilSuccess(ctx context.Context, client *http.Client, adminURL, advertiseURL string, backoff time.Duration) error {
	if strings.TrimSpace(adminURL) == "" || strings.TrimSpace(advertiseURL) == "" {
		log.Printf("[Coven] Restate admin or advertise URL empty; skipping deployment registration")
		return nil
	}
	if backoff <= 0 {
		backoff = restateRegisterInitialBackoff
	}
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, restateRegisterAttemptTimeout)
		err := RegisterRestateDeployment(attemptCtx, client, adminURL, advertiseURL)
		cancel()
		if err == nil {
			RestateRegistered.Set(1)
			log.Printf("[Coven] Registered Restate deployment %s with %s", strings.TrimSpace(advertiseURL), strings.TrimSpace(adminURL))
			return nil
		}
		log.Printf("[Coven] warning: Restate deployment registration failed: %v; retrying in %s", err, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(2*backoff, restateRegisterMaxBackoff)
	}
}

// runWorkerAfterRegistration registers coven with Restate, then (when startWorker) starts the worker,
// which arms CovenDreamer. Arming before registration would hit "service not found" at ingress.
func runWorkerAfterRegistration(ctx context.Context, client *http.Client, adminURL, advertiseURL string, backoff time.Duration, worker *Worker, startWorker bool) error {
	if err := registerRestateUntilSuccess(ctx, client, adminURL, advertiseURL, backoff); err != nil {
		return err
	}
	if !startWorker || worker == nil {
		return nil
	}
	return worker.Start(ctx)
}

func newExtractFn(cfg *config.Config, worker *Worker, toolHandler *ToolHandler) func(context.Context, string) error {
	return func(extractCtx context.Context, threadID string) error {
		if cfg.RestateIngressURL != "" {
			var targetAgent, cleanThread string
			if strings.Contains(threadID, "/") {
				targetAgent, cleanThread = config.ParseScopedThreadKey(threadID)
			} else {
				targetAgent = cfg.EffectiveStack()
				cleanThread = strings.TrimSpace(threadID)
			}
			scopedKey := config.ScopedThreadKey(targetAgent, cleanThread)
			endpoint := fmt.Sprintf("%s/CovenThread/%s/Extract/send", strings.TrimRight(cfg.RestateIngressURL, "/"), url.PathEscape(scopedKey))
			req := ExtractJobRequest{
				ThreadID: cleanThread,
				Agent:    targetAgent,
			}
			var client *http.Client
			if worker != nil {
				client = worker.HTTPClient()
			}
			_, postErr := restateingress.Post(extractCtx, client, endpoint, "", req, 0)
			return postErr
		}
		return toolHandler.Extract(extractCtx, threadID)
	}
}

// RunApp bootstraps the coven MCP HTTP server with graceful shutdown.
func RunApp(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("app: config cannot be nil")
	}
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		port = config.DefaultPort
	}

	database, err := store.OpenDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer httpkit.CloseWarn(database, "database on shutdown")

	sqlStore := store.NewSQLStore(database)
	sqlStore.SetStack(cfg.EffectiveStack())
	if cfg.TranscriptsURL != "" {
		indexerClient := store.NewIndexerClient(cfg.TranscriptsURL, cfg.EffectiveStack(), nil)
		indexerClient.SetMaxSteps(cfg.ExtractBatchLimit())
		sqlStore.SetIndexerClient(indexerClient)
	}
	ollama := store.NewOllamaClient(&cfg.Ollama)
	publisher := NewGitHubSkillPublisher(GitHubSkillPublisherConfig{
		Token:           cfg.GitHubPAT,
		Owner:           cfg.ConfigRepoOwner,
		Repo:            cfg.ConfigRepoName,
		PRRegisterURL:   cfg.PRRegisterURL,
		SystemChannelID: cfg.SystemChannelID,
		EriURL:          cfg.EriURL,
	})
	toolHandler := NewToolHandler(cfg, sqlStore, ollama, publisher)

	worker := NewWorker(WorkerConfig{
		Store:               sqlStore,
		Ollama:              ollama,
		LLMClient:           HTTPBrainLLMClient(cfg.EriLLMURL, nil),
		Publisher:           publisher,
		CustomSkillsDir:     cfg.CustomSkillsDir,
		InitialDelay:        DefaultWorkerInitialDelay,
		TickInterval:        DefaultWorkerTickInterval,
		Role:                cfg.Role,
		Stack:               cfg.EffectiveStack(),
		ExtractTimeout:      cfg.ExtractTimeout(),
		RestateIngressURL:   cfg.RestateIngressURL,
		RestateAdminURL:     cfg.RestateAdminURL,
		RestateAdvertiseURL: cfg.RestateAdvertiseURL,
	})
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	extractFn := newExtractFn(cfg, worker, toolHandler)
	mcpServer := NewServer(toolHandler, extractFn)

	thread := NewCovenThread(worker)
	dreamer := NewCovenDreamer(worker)
	restateLn, restateHTTPServer, restateErrChan, err := startRestateServer(ctx, cfg, thread, dreamer)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := restateLn.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			log.Printf("[Coven] restate listener close error: %v", closeErr)
		}
	}()

	bus := initCovenJetStream(ctx, worker, cfg.NATSURL)
	defer func() {
		if bus != nil {
			if closeErr := bus.Close(); closeErr != nil {
				log.Printf("[Coven] bus close error: %v", closeErr)
			}
		}
	}()

	go func() {
		startWorker := !strings.EqualFold(cfg.Role, "extractor")
		if err := runWorkerAfterRegistration(workerCtx, nil, cfg.RestateAdminURL, cfg.RestateAdvertiseURL, restateRegisterInitialBackoff, worker, startWorker); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Coven Worker] Background worker exited: %v", err)
		}
	}()

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           mcpServer.Routes(),
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
	case err := <-restateErrChan:
		return err
	}

	log.Println("Shutting down Coven MCP and Restate servers gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	if err := restateHTTPServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Restate server shutdown error: %v", err)
	}
	log.Println("Coven MCP server stopped cleanly")
	return nil
}
