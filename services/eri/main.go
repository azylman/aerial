package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/ambient"
	"github.com/azylman/aerial/services/eri/pkg/bootstrap"
	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

// BrainAppOption configures optional runtime dependencies for RunBrainApp.
type BrainAppOption func(*brainAppOptions)

type brainAppOptions struct {
	store          db.Store
	txStore        db.TranscriptStore
	hupChan        <-chan os.Signal
	onReload       func(source string)
	agentName      string
	tracingEnabled bool
	traceContent   bool // record prompt/response text on agent spans (capture_content)
}

func validateBrainStartupConfig(cur *config.ConfigData, hasInjectedStore bool) error {
	if !hasInjectedStore && cur.DatabaseURL == "" {
		return fmt.Errorf("database URL or path is required")
	}
	if cur.GeminiHomeDir == "" {
		return fmt.Errorf("gemini home directory is required")
	}
	if cur.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	if cur.Port == "" {
		return fmt.Errorf("port is required")
	}
	return nil
}

var (
	onServerReady func(addr string)
	openDefaultDB = db.New
)

func setupDiscordFunnel(
	ctx context.Context,
	cfg *config.Config,
	store db.Store,
	pool *queue.WorkerPool,
	discordToken string,
) (*discordgo.Session, func()) {
	SetFunnelConfig(cfg)
	dgSession := connectDiscordFunnel(ctx, store, pool, discordToken)
	if pool != nil && dgSession != nil {
		pool.SetDiscordSession(dgSession)
	}
	cleanup := func() {
		log.Printf("Stopping worker pool...")
		if pool != nil {
			pool.Stop()
		}
		if dgSession != nil {
			log.Printf("Closing Discord gateway session...")
			if err := dgSession.Close(); err != nil {
				log.Printf("[WARN] Failed to close Discord gateway session: %v", err)
			}
		}
	}
	return dgSession, cleanup
}

// registerGitSyncReload mounts the git-sync webhook on mux. The endpoint
// queues a full config reload, so it must stay reachable only from the
// in-pod sidecars over loopback. The method-qualified pattern makes the mux
// answer 405 for anything but POST.
func registerGitSyncReload(mux *http.ServeMux, trigger *bootstrap.ReloadTrigger) {
	mux.Handle("POST /-/reload", httpkit.LoopbackOnly(bootstrap.GitSyncReloadHandler(trigger)))
}

func startSIGHUPReloadLoop(ctx context.Context, appHupChan <-chan os.Signal, reloadConfig func(string)) func() {
	var hupChan <-chan os.Signal
	var stopHup func()
	if appHupChan != nil {
		hupChan = appHupChan
	} else {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		hupChan = ch
		stopHup = func() {
			signal.Stop(ch)
		}
	}

	hupCtx, hupCancel := context.WithCancel(ctx)
	go func() {
		for {
			select {
			case <-hupCtx.Done():
				return
			case sig, ok := <-hupChan:
				if !ok {
					return
				}
				if sig == syscall.SIGHUP {
					reloadConfig("SIGHUP")
				}
			}
		}
	}()

	return func() {
		if stopHup != nil {
			stopHup()
		}
		hupCancel()
	}
}

// brainHandler wraps mux with request metrics and server spans.
func brainHandler(mux http.Handler) http.Handler {
	return tracing.Middleware("eri", metricsMiddleware(mux))
}

func serveBrainHTTP(ctx context.Context, ln net.Listener, mux http.Handler, model string, asyncErrCh ...<-chan error) error {
	srv := &http.Server{
		Handler:           brainHandler(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	if onServerReady != nil {
		onServerReady(ln.Addr().String())
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("Aerial Brain listening on %s (model=%s, timeout=%dm)", ln.Addr().String(), model, queue.DefaultTimeoutMinutes)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	var leaderErrCh <-chan error
	if len(asyncErrCh) > 0 {
		leaderErrCh = asyncErrCh[0]
	}

	select {
	case <-ctx.Done():
		log.Println("Shutting down Aerial Brain gracefully...")
		// ctx is already canceled here; the drain gets its own grace period.
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), httpShutdownDrain)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
		log.Println("Aerial Brain shutdown complete")
		return nil
	case err := <-errChan:
		return err
	case err := <-leaderErrCh:
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), httpShutdownDrain)
		defer shutdownCancel()
		return errors.Join(err, srv.Shutdown(shutdownCtx))
	}
}

func resolveBrainStores(cfg *config.Config, cur *config.ConfigData, opts brainAppOptions) (db.Store, db.TranscriptStore, func(), error) {
	if opts.store != nil {
		return opts.store, opts.txStore, nil, nil
	}
	return bootstrap.InitDefaultStores(openDefaultDB, cfg, cur, opts.agentName)
}

func RunBrainApp(ctx context.Context, cfg *config.Config, opts ...BrainAppOption) error {
	if cfg == nil {
		return fmt.Errorf("brain: config cannot be nil")
	}
	if ctx.Err() != nil {
		return nil
	}

	var appOpts brainAppOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&appOpts)
		}
	}

	cur := cfg.Current()
	if err := validateBrainStartupConfig(cur, appOpts.store != nil); err != nil {
		return err
	}

	config.RegisterConfigTokens(cfg)

	store, txStore, cleanupStores, storeErr := resolveBrainStores(cfg, cur, appOpts)
	if storeErr != nil {
		return storeErr
	}
	if cleanupStores != nil {
		defer cleanupStores()
	}

	sessionMgr := session.New(cfg.GeminiHomeDir(), cfg.DataDir())

	cls := classifier.New(cfg)
	ambientResolver := ambient.NewResolver(cfg).Resolve

	var restateClient *RestateIngressClient
	if ingressURL := strings.TrimSpace(os.Getenv("RESTATE_INGRESS_URL")); ingressURL != "" {
		restateClient = NewRestateIngressClient(ingressURL, appOpts.agentName)
	}
	var remoteLLMFn runner.LLMFunc
	if restateClient != nil {
		origRestate := activeRestateIngressClient.Swap(restateClient)
		defer activeRestateIngressClient.Store(origRestate)
		eriRestateURL := os.Getenv("ERI_RESTATE_URL")
		remoteLLMFn = NewRemoteEphemeralLLMFunc(eriRestateURL)
		remoteQuotaProvider := NewRemoteQuotaProvider(eriRestateURL)
		cls.LLMFunc = remoteLLMFn
		cls.TitleLLMFunc = remoteLLMFn
		SetFunnelQuotaProvider(remoteQuotaProvider.ProviderFunc())
		defer SetFunnelQuotaProvider(nil)
	}

	llmFn := remoteLLMFn

	pool := queue.New(cfg, queue.WorkerPoolConfig{
		ParentContext:          ctx,
		Store:                  store,
		TranscriptStore:        txStore,
		TranscriptsIndexerURL:  cur.Transcripts.IndexerURL,
		Classifier:             cls,
		SessionManager:         sessionMgr,
		LLMFunc:                llmFn,
		AmbientResolver:        ambientResolver,
		NATSURL:                cur.NATSURL,
		AgentName:              appOpts.agentName,
		TraceContent:           appOpts.traceContent,
		ActiveTurns:            runner.NewActiveTurnRegistry(),
		ChunkStepFlushInterval: queue.DefaultChunkStepFlushInterval,
		SATokenPath:            k8sauth.DefaultTokenPath,
	})
	detachEventBus := attachEriEventBus(ctx, pool)
	defer detachEventBus()
	pool.Start()

	dgSession, cleanupFunnel := setupDiscordFunnel(ctx, cfg, store, pool, cur.DiscordToken)
	defer cleanupFunnel()

	reloadConfig := bootstrap.CreateReloadConfigFunc(cfg,
		bootstrap.WithDiscordSession(dgSession),
		bootstrap.WithDirtyPools(pool),
		bootstrap.WithOnReload(appOpts.onReload))

	stopHup := startSIGHUPReloadLoop(ctx, appOpts.hupChan, reloadConfig)
	defer stopHup()

	txEmbedder := transcript.NewEmbedder(cfg, sessionMgr.Roots()...)
	var embedder transcript.EmbedderFunc
	if txEmbedder != nil {
		embedder = func(eCtx context.Context, text string) ([]float32, error) {
			return txEmbedder.GenerateEmbedding(eCtx, text, false, 1)
		}
	}
	mux := SetupBrainMuxWithEmbedder(store, txStore, pool, embedder, sessionMgr.Roots()...)
	reloadCtx, stopReloads := context.WithCancel(ctx)
	gitSyncReloads := bootstrap.NewReloadTrigger(reloadCtx, reloadConfig)
	defer func() {
		stopReloads()
		<-gitSyncReloads.Done()
	}()
	registerGitSyncReload(mux, gitSyncReloads)

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+cur.Port)
	if err != nil {
		return err
	}
	defer func() {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("[WARN] Error closing listener: %v", err)
		}
	}()

	return serveBrainHTTP(ctx, ln, mux, cur.Model)
}

// setupTracing installs the global tracer provider and propagator from the config at path
// (see tracing.Init). It returns a BrainAppOption naming the turn agent after the config's stack
// (the persona recorded as gen_ai.agent.name) and switching on the MCP trace proxy when tracing
// is enabled (plus prompt/response capture when capture_content is also on), and a func that
// flushes pending spans.
func setupTracing(ctx context.Context, path string) (BrainAppOption, func()) {
	tcfg, flush := tracing.Init(ctx, "eri", path)
	return func(o *brainAppOptions) {
		o.agentName, o.tracingEnabled, o.traceContent = tcfg.Stack, tcfg.Enabled, tcfg.Enabled && tcfg.CaptureContent
	}, flush
}

func main() {
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))))

	cfg, err := config.New()
	if err != nil {
		log.Fatalf("Fatal: failed to load configuration: %v", err)
	}

	cur := cfg.Current()
	if cur.GeminiHomeDir == "" {
		cur.GeminiHomeDir = bootstrap.DefaultGeminiHomeDir()
	}
	if cur.DataDir == "" {
		cur.DataDir = bootstrap.DefaultDataDir()
	}
	if cur.Port == "" {
		cur.Port = "8080"
	}
	cfg.Update(cur)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-stopChan
		cancel()
	}()

	withTracing, shutdownTracing := setupTracing(ctx, tracing.DefaultConfigPath)

	err = RunBrainApp(ctx, cfg, withTracing)
	cancel()
	shutdownTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server failed: %v", err)
	}
}

// httpShutdownDrain must stay below the pod terminationGracePeriodSeconds
// (30s default) so in-flight voice SSE streams survive a roll.
const httpShutdownDrain = 25 * time.Second
