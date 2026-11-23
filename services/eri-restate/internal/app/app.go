package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
	"golang.org/x/sync/errgroup"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/ambient"
	"github.com/azylman/aerial/services/eri/pkg/bootstrap"
	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/egressscan"
	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/githubproxy"
	"github.com/azylman/aerial/services/eri/pkg/llmcall"
	"github.com/azylman/aerial/services/eri/pkg/mcp"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

// Config holds runtime configuration for the eri-restate worker service.
type Config struct {
	RestatePort string
	HealthPort  string
	PostgresURL string
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
		healthPort = "4028"
	}

	pgURL := strings.TrimSpace(strutil.FirstNonEmpty(os.Getenv("DATABASE_URL"), os.Getenv("POSTGRES_URL"), httpkit.BuildPostgresDSN(os.Getenv, "aerial")))
	agentName := strings.TrimSpace(strutil.FirstNonEmpty(os.Getenv("AGENT_NAME"), "aerial"))

	return Config{
		RestatePort: restatePort,
		HealthPort:  healthPort,
		PostgresURL: pgURL,
		AgentName:   agentName,
	}
}

// eriRestateDefinitions returns the fixed service set every eri-restate revision binds: the
// thread and dispatch coordinators, the voice session, the dispatch cancel router (under the
// SessionManager service name), TurnRunner and the LLM service on llmFn (the ephemeral
// summarizer pool). Only TurnRunner holds an invocation open across a turn, so it alone takes
// queue.TurnHoldTimeout; every other service keeps 1h.
func eriRestateDefinitions(pool *queue.WorkerPool, agentName string, llmFn runner.LLMFunc) []restate.ServiceDefinition {
	hour := []restate.ServiceDefinitionOption{restate.WithInactivityTimeout(time.Hour), restate.WithAbortTimeout(time.Hour)}
	store := pool.Store()
	return []restate.ServiceDefinition{
		restate.Reflect(queue.NewThreadCoordinator(store, pool, agentName), hour...),
		restate.Reflect(queue.NewDispatchCoordinator(store, agentName), hour...),
		restate.Reflect(queue.NewVoiceSessionObject(pool, agentName), hour...),
		restate.Reflect(queue.NewDispatchCancelRouter(store, agentName), hour...),
		queue.ReflectTurnRunner(queue.NewTurnRunnerService(pool, agentName)),
		restate.Reflect(llmcall.NewService(llmFn, agentName), hour...),
	}
}

// NewEriRestateServer binds the Eri services plus the LLM service on llmFn and returns a Restate
// server handler.
func NewEriRestateServer(pool *queue.WorkerPool, agentName string, llmFn runner.LLMFunc) *server.Restate {
	srv := server.NewRestate()
	for _, def := range eriRestateDefinitions(pool, agentName, llmFn) {
		srv = srv.Bind(def)
	}
	return srv
}

// HealthMuxOptions configures the health and internal endpoint HTTP mux.
type HealthMuxOptions struct {
	AgentName     string
	MCPRoutes     *env.MCPProxyRoutes
	ActiveTurns   *runner.ActiveTurnRegistry
	ReloadTrigger *bootstrap.ReloadTrigger
	// Config supplies the live GitHub PAT for the loopback-only GitHub egress
	// proxy. A nil Config still mounts the routes; they answer 503.
	Config *config.Config
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	httpkit.WriteJSON(w, status, map[string]string{"error": msg})
}

// SetupHealthMux creates the ServeMux for health, metrics, quota, and diagnostics.
func SetupHealthMux(opts HealthMuxOptions) *http.ServeMux {
	mux := http.NewServeMux()

	healthHandler := func(w http.ResponseWriter, _ *http.Request) {
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/healthz", healthHandler)
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("eri-restate"))

	if opts.ReloadTrigger != nil {
		mux.Handle("POST /-/reload", httpkit.LoopbackOnly(bootstrap.GitSyncReloadHandler(opts.ReloadTrigger)))
	}

	// NOTE: Unlike POST /-/reload (which is invoked by the pod-local git-sync sidecar over 127.0.0.1),
	// /internal/quota is a cross-pod cluster endpoint invoked by stateless
	// Deployment/eri pods over the Kubernetes pod network via Service/eri-restate-health:4028
	// (isolated by Kubernetes NetworkPolicy). It must NOT be wrapped in httpkit.LoopbackOnly,
	// which would reject cross-pod requests from Deployment/eri with 403 Forbidden.
	mux.HandleFunc("/internal/quota", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, metrics.LatestQuotaSnapshot())
	})

	if opts.MCPRoutes != nil {
		mcp.NewTraceProxy(mcp.TraceProxyConfig{
			Upstreams: opts.MCPRoutes,
			Turns:     opts.ActiveTurns,
		}).Register(mux)
	}

	mcp.NewOTelLogProxy(mcp.OTelLogProxyConfig{
		Turns: opts.ActiveTurns,
		Stack: opts.AgentName,
	}).Register(mux)

	// Agent turns in this pod push and call the GitHub API through this proxy;
	// it is loopback-only so the PAT-injecting routes never face the pod network.
	githubproxy.Register(mux, opts.Config)
	// The in-pod hook-guard posts blocklisted MCP tool arguments here, where the
	// registered secret values are loaded; loopback-only like the proxy.
	egressscan.Register(mux, opts.Config)

	return mux
}

// PrewarmProcessPoolsSequentially warms up process pool targets sequentially in the background.
func PrewarmProcessPoolsSequentially(ctx context.Context, pools ...runner.AgentPool) {
	go func() {
		prewarmCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		log.Printf("[INIT] Pre-warming process pools sequentially...")
		for _, p := range pools {
			if p == nil {
				continue
			}
			if err := p.Initialize(prewarmCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[WARN] Failed pre-warming pool: %v", err)
			}
			select {
			case <-prewarmCtx.Done():
				return
			default:
			}
		}
		log.Printf("[INIT] Process pool pre-warming completed.")
	}()
}

// AppOption configures optional dependencies for RunApp.
type AppOption func(*appOptions)

type appOptions struct {
	store          db.Store
	txStore        db.TranscriptStore
	processSpawner runner.DaemonSpawner
	llmFn          runner.LLMFunc
	appConfig      *config.Config
}

func startUsagePoller(ctx context.Context, cur *config.ConfigData, ephemeralHome string, envVars []string) {
	if cur == nil {
		return
	}
	switch cur.GetEngine() {
	case config.EngineAgy:
		startUsagePollerFn(ctx, 10*time.Minute, metrics.DefaultUsageRunner(cur.AgyBin, ephemeralHome, envVars, false), metrics.RecordUsageJSON)
	case config.EngineClaude:
		startUsagePollerFn(ctx, 10*time.Minute, metrics.DefaultClaudeUsageRunner(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN"), false), metrics.RecordClaudeUsageJSON)
	case config.EngineCodex:
		startUsagePollerFn(ctx, 10*time.Minute, metrics.DefaultCodexUsageRunner(filepath.Join(cur.DataDir, "codex", "auth.json"), false), metrics.RecordCodexUsageJSON)
	case config.EngineMuse:
		startUsagePollerFn(ctx, 10*time.Minute, metrics.DefaultMuseUsageRunner(cur.MuseBin, ephemeralHome, envVars, false), metrics.RecordMuseUsageJSON)
	}
}

var (
	openDefaultDB      = db.New
	onServerReady      func(restateAddr, healthAddr string)
	startUsagePollerFn = metrics.StartUsagePoller
)

type agentPoolsBundle struct {
	discordPools  map[queue.PoolKey]runner.AgentPool
	ephemeralPool *runner.InterchangeablePool
	voicePool     *runner.DynamicVoicePool
	allPools      []runner.AgentPool
	ephemeralHome string
	poolEnv       []string
}

func initializeAgentPools(cur *config.ConfigData, cfg Config, sessionMgr *session.Manager, spawner runner.DaemonSpawner) agentPoolsBundle {
	runtimeBase := cur.DataDir
	discordHome := filepath.Join(runtimeBase, "runtimes", "discord")
	ephemeralHome := filepath.Join(runtimeBase, "runtimes", "ephemeral")
	voiceHome := filepath.Join(runtimeBase, "runtimes", "voice")

	poolEnv := os.Environ()
	if cur.BotID != "" {
		poolEnv = append(poolEnv, "AERIAL_BOT_ID="+cur.BotID)
	}
	if cur.SynapseURL != "" {
		poolEnv = append(poolEnv, "SYNAPSE_URL="+cur.SynapseURL)
	}
	if cur.VanadisURL != "" {
		poolEnv = append(poolEnv, "VANADIS_URL="+cur.VanadisURL)
	}

	basePoolCfg := func(home, model string, prewarmed []string) runner.PoolConfig {
		return runner.PoolConfig{
			Engine:              cur.GetEngine(),
			ClaudeBin:           cur.ClaudeBin,
			CodexBin:            cur.CodexBin,
			MuseBin:             cur.MuseBin,
			GeminiHomeDir:       home,
			Model:               model,
			AgyBin:              cur.AgyBin,
			Cwd:                 cur.DataDir,
			Env:                 poolEnv,
			Timezone:            cur.Timezone,
			TranscriptRescuer:   sessionMgr.ExtractResponseSinceContext,
			TerminalTurnRescuer: sessionMgr.ExtractDefaultTerminalTurnResponse,
			SessionManager:      sessionMgr,
			PrewarmedTargets:    prewarmed,
			MCPProxyPort:        cfg.HealthPort,
			Stack:               cfg.AgentName,
		}
	}

	lowEffortModel := cur.LowEffortModel
	if lowEffortModel == "" {
		lowEffortModel = "gemini-3.8-flash-low"
	}
	mediumEffortModel := cur.MediumEffortModel
	if mediumEffortModel == "" {
		mediumEffortModel = "gemini-3.8-flash-medium"
	}

	discordPools := make(map[queue.PoolKey]runner.AgentPool)
	dHigh := runner.NewUnifiedProcessPool(basePoolCfg(discordHome, cur.Model, nil), spawner)
	dMedium := runner.NewUnifiedProcessPool(basePoolCfg(discordHome, mediumEffortModel, nil), spawner)
	dLow := runner.NewUnifiedProcessPool(basePoolCfg(discordHome, lowEffortModel, nil), spawner)

	discordPools[queue.PoolKey{Ephemeral: false, Effort: "high"}] = dHigh
	discordPools[queue.PoolKey{Ephemeral: false, Effort: "medium"}] = dMedium
	discordPools[queue.PoolKey{Ephemeral: false, Effort: "low"}] = dLow

	ephemUnderlying := runner.NewUnifiedProcessPool(basePoolCfg(ephemeralHome, lowEffortModel, []string{"ephemeral:worker-0", "ephemeral:worker-1"}), spawner)
	ephemeralPool := runner.NewInterchangeablePool(ephemUnderlying, runner.InterchangeablePoolConfig{
		WorkerCount: len(basePoolCfg(ephemeralHome, lowEffortModel, []string{"ephemeral:worker-0", "ephemeral:worker-1"}).PrewarmedTargets),
		KeyPrefix:   "ephemeral:worker",
	})

	voiceUnderlying := runner.NewUnifiedProcessPool(basePoolCfg(voiceHome, lowEffortModel, nil), spawner)
	voicePool := runner.NewDynamicVoicePool(func() (runner.AgentPool, string, error) {
		return voiceUnderlying, "voice-static", nil
	})

	allPools := []runner.AgentPool{dHigh, dMedium, dLow, ephemeralPool, voicePool}
	return agentPoolsBundle{
		discordPools:  discordPools,
		ephemeralPool: ephemeralPool,
		voicePool:     voicePool,
		allPools:      allPools,
		ephemeralHome: ephemeralHome,
		poolEnv:       poolEnv,
	}
}

// wireOutboundIntegrations attaches the Discord session (returned so config
// reloads can alert the system channel; nil without a token) and the NATS event
// bus (closed by the returned func; nil when unset or unreachable).
func wireOutboundIntegrations(ctx context.Context, cur *config.ConfigData, workerPool *queue.WorkerPool) (*discordgo.Session, func()) {
	var dg *discordgo.Session
	if token := strings.TrimSpace(cur.DiscordToken); token != "" {
		if s, err := discordgo.New("Bot " + token); err == nil {
			dg = s
			workerPool.SetDiscordSession(dg)
		}
	}

	if natsURL := strings.TrimSpace(cur.NATSURL); natsURL != "" {
		bus, err := pubsub.NewJetStreamBus(ctx, natsURL)
		if err != nil {
			log.Printf("[eri-restate] Warning connecting to NATS event bus: %v", err)
			return dg, nil
		}
		workerPool.SetEventBus(bus)
		return dg, func() {
			if cErr := bus.Close(); cErr != nil {
				log.Printf("[eri-restate] Warning closing event bus: %v", cErr)
			}
		}
	}
	return dg, nil
}

// RunApp runs both the Restate worker server and the health/metrics server.
func RunApp(ctx context.Context, cfg Config, opts ...AppOption) error {
	var appOpts appOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&appOpts)
		}
	}

	appCfg := appOpts.appConfig
	if appCfg == nil {
		var err error
		appCfg, err = config.New()
		if err != nil {
			return fmt.Errorf("failed to load configuration: %w", err)
		}
	}

	cur := appCfg.Current()
	config.RegisterConfigTokens(appCfg)

	log.Printf("[eri-restate] turn_queue=coordinator source=builtin")

	store := appOpts.store
	txStore := appOpts.txStore
	if store == nil {
		s, tx, cleanupStores, err := bootstrap.InitDefaultStores(openDefaultDB, appCfg, cur, cfg.AgentName)
		if err != nil {
			return err
		}
		defer cleanupStores()
		store, txStore = s, tx
	}

	activeTurns := runner.NewActiveTurnRegistry()
	mcpRoutes := env.NewMCPProxyRoutes(cfg.HealthPort)

	provisioner, pErr := env.NewFromConfig(appCfg)
	if pErr == nil && provisioner != nil {
		provisioner.WithMCPProxy(mcpRoutes)
		if err := provisioner.Sync(ctx, appCfg); err != nil {
			log.Printf("[eri-restate] Warning: env sync error: %v", err)
		}
	}

	sessionMgr := session.New(appCfg.GeminiHomeDir(), appCfg.DataDir())
	pools := initializeAgentPools(cur, cfg, sessionMgr, appOpts.processSpawner)
	defer func() {
		for _, p := range pools.allPools {
			if cErr := p.Close(); cErr != nil {
				log.Printf("[eri-restate] Warning closing agent pool: %v", cErr)
			}
		}
	}()

	PrewarmProcessPoolsSequentially(ctx, pools.allPools...)
	startUsagePoller(ctx, cur, pools.ephemeralHome, pools.poolEnv)

	cls := classifier.New(appCfg, classifier.WithProcessPool(pools.ephemeralPool))

	llmFn := appOpts.llmFn
	if llmFn == nil {
		llmFn = pools.ephemeralPool.EphemeralLLMFunc("ephemeral:summarizer")
	}

	ambientResolver := ambient.NewResolver(appCfg)

	workerPool := queue.New(appCfg, queue.WorkerPoolConfig{
		ParentContext:          ctx,
		Store:                  store,
		TranscriptStore:        txStore,
		Classifier:             cls,
		SessionManager:         sessionMgr,
		Pools:                  pools.discordPools,
		VoiceProcessPool:       pools.voicePool,
		LLMFunc:                llmFn,
		NATSURL:                cur.NATSURL,
		AgentName:              cfg.AgentName,
		ActiveTurns:            activeTurns,
		ChunkStepFlushInterval: queue.DefaultChunkStepFlushInterval,
		AmbientResolver:        ambientResolver.Resolve,
		TranscriptsIndexerURL:  cur.Transcripts.IndexerURL,
		SATokenPath:            k8sauth.DefaultTokenPath,
	})

	dgSession, closeBus := wireOutboundIntegrations(ctx, cur, workerPool)
	if closeBus != nil {
		defer closeBus()
	}

	workerPool.Start()
	defer workerPool.Stop()

	reloadTrigger := bootstrap.NewReloadTrigger(ctx, bootstrap.CreateReloadConfigFunc(appCfg,
		bootstrap.WithDiscordSession(dgSession),
		bootstrap.WithProvisioner(ctx, provisioner),
		bootstrap.WithDirtyPools(pools.ephemeralPool, workerPool)))

	restateSrv := NewEriRestateServer(workerPool, cfg.AgentName, llmFn)
	restateHandler, err := restateSrv.Handler()
	if err != nil {
		return fmt.Errorf("create restate handler: %w", err)
	}

	healthMux := SetupHealthMux(HealthMuxOptions{
		AgentName:     cfg.AgentName,
		MCPRoutes:     mcpRoutes,
		ActiveTurns:   activeTurns,
		ReloadTrigger: reloadTrigger,
		Config:        appCfg,
	})

	return runServers(ctx, cfg, restateHandler, healthMux)
}

func runServers(ctx context.Context, cfg Config, restateHandler http.Handler, healthMux http.Handler) error {
	var lc net.ListenConfig
	restateLn, err := lc.Listen(ctx, "tcp", ":"+cfg.RestatePort)
	if err != nil {
		return fmt.Errorf("listen on restate port %s: %w", cfg.RestatePort, err)
	}
	defer func() {
		if cErr := restateLn.Close(); cErr != nil && !errors.Is(cErr, net.ErrClosed) {
			log.Printf("[eri-restate] Warning closing restate listener: %v", cErr)
		}
	}()

	healthLn, err := lc.Listen(ctx, "tcp", ":"+cfg.HealthPort)
	if err != nil {
		return fmt.Errorf("listen on health port %s: %w", cfg.HealthPort, err)
	}
	defer func() {
		if cErr := healthLn.Close(); cErr != nil && !errors.Is(cErr, net.ErrClosed) {
			log.Printf("[eri-restate] Warning closing health listener: %v", cErr)
		}
	}()

	if onServerReady != nil {
		onServerReady(restateLn.Addr().String(), healthLn.Addr().String())
	}

	g, gCtx := errgroup.WithContext(ctx)

	var restateProtocols http.Protocols
	restateProtocols.SetHTTP1(true)
	restateProtocols.SetUnencryptedHTTP2(true)

	restateServer := &http.Server{
		Handler:           restateHandler,
		Protocols:         &restateProtocols,
		ReadHeaderTimeout: 10 * time.Second,
	}
	g.Go(func() error {
		log.Printf("[eri-restate] Restate worker listening on :%s", cfg.RestatePort)
		if sErr := restateServer.Serve(restateLn); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			return fmt.Errorf("restate server error: %w", sErr)
		}
		return nil
	})

	healthServer := &http.Server{
		Handler:           tracing.Middleware("eri-restate", healthMux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	g.Go(func() error {
		log.Printf("[eri-restate] Health server listening on :%s", cfg.HealthPort)
		if sErr := healthServer.Serve(healthLn); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			return fmt.Errorf("health server error: %w", sErr)
		}
		return nil
	})

	g.Go(func() error {
		<-gCtx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(gCtx), 5*time.Second)
		defer shutdownCancel()
		if sErr := restateServer.Shutdown(shutdownCtx); sErr != nil {
			log.Printf("[eri-restate] Warning shutting down restate server: %v", sErr)
		}
		if sErr := healthServer.Shutdown(shutdownCtx); sErr != nil {
			log.Printf("[eri-restate] Warning shutting down health server: %v", sErr)
		}
		return nil
	})

	return g.Wait()
}
