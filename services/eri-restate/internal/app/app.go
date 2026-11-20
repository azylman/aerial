package app

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
	"golang.org/x/sync/errgroup"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/ambient"
	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/env"
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
	AdminURL    string
	PostgresURL string
	AgentName   string
}

// FirstNonEmpty returns the first non-blank string after trimming.
func FirstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
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

	adminURL := strings.TrimRight(strings.TrimSpace(os.Getenv("RESTATE_ADMIN_URL")), "/")
	if adminURL == "" {
		adminURL = "http://restate.aerial:9070"
	}

	pgURL := strings.TrimSpace(FirstNonEmpty(os.Getenv("DATABASE_URL"), os.Getenv("POSTGRES_URL"), httpkit.BuildPostgresDSN(os.Getenv, "aerial")))
	agentName := strings.TrimSpace(FirstNonEmpty(os.Getenv("AGENT_NAME"), "aerial"))

	return Config{
		RestatePort: restatePort,
		HealthPort:  healthPort,
		AdminURL:    adminURL,
		PostgresURL: pgURL,
		AgentName:   agentName,
	}
}

// NewEriRestateServer binds the Eri Virtual Objects and returns a Restate server handler.
func NewEriRestateServer(pool *queue.WorkerPool, agentName string) *server.Restate {
	threadSession := queue.NewThreadSessionObject(pool, agentName)
	dispatchSession := queue.NewDispatchSessionObject(pool, agentName)
	voiceSession := queue.NewVoiceSessionObject(pool, agentName)
	sessionManager := queue.NewSessionManagerService(pool, agentName)

	return server.NewRestate().
		Bind(restate.Reflect(threadSession)).
		Bind(restate.Reflect(dispatchSession)).
		Bind(restate.Reflect(voiceSession)).
		Bind(restate.Reflect(sessionManager))
}

var defaultTimeoutPayload = []byte(`{"inactivity_timeout":"1h","abort_timeout":"1h"}`)

func startDiscoveryTimeoutReconciler(
	ctx context.Context,
	adminURL, agent string,
	httpClient *http.Client,
	maxAttempts int,
	retryDelay time.Duration,
) *ReloadTrigger {
	reconcileFn := func(source string) {
		log.Printf("[eri-restate] Service timeout reconciliation triggered via %s", source)
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			recCtx, recCancel := context.WithTimeout(ctx, 30*time.Second)
			err := ReconcileRestateServiceTimeouts(recCtx, adminURL, agent, httpClient)
			recCancel()
			if err == nil {
				log.Printf("[eri-restate] Successfully reconciled service timeouts for agent %q", agent)
				return
			}
			log.Printf("[eri-restate] Service timeout reconciliation attempt %d/%d failed: %v", attempt, maxAttempts, err)
			if attempt == maxAttempts {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
			}
		}
	}

	trigger := NewReloadTrigger(ctx, reconcileFn)
	return trigger
}

func wrapDiscoveryTimeoutReconciler(next http.Handler, triggerFn func()) http.Handler {
	if triggerFn == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL != nil && strings.HasPrefix(r.URL.Path, "/discover") {
			triggerFn()
		}
		next.ServeHTTP(w, r)
	})
}

// ReconcileRestateServiceTimeouts sets 1h timeouts on Eri services via the Restate admin API.
func ReconcileRestateServiceTimeouts(ctx context.Context, adminURL string, agent string, httpClient ...*http.Client) error {
	adminURL = strings.TrimRight(strings.TrimSpace(adminURL), "/")
	if adminURL == "" {
		adminURL = "http://restate.aerial:9070"
	}

	client := &http.Client{Timeout: 10 * time.Second}
	if len(httpClient) > 0 && httpClient[0] != nil {
		client = httpClient[0]
	}

	services := []string{
		queue.ThreadSessionServiceName(agent),
		queue.DispatchSessionServiceName(agent),
		queue.VoiceSessionServiceName(agent),
	}

	for _, svc := range services {
		reqURL := fmt.Sprintf("%s/services/%s", adminURL, url.PathEscape(svc))
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPatch, reqURL, bytes.NewReader(defaultTimeoutPayload))
		if reqErr != nil {
			return fmt.Errorf("create timeout reconcile request for %s: %w", svc, reqErr)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, doErr := client.Do(req)
		if doErr != nil {
			return fmt.Errorf("patch service %s timeouts: %w", svc, doErr)
		}
		resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("patch service %s timeouts failed with status %d", svc, resp.StatusCode)
		}
	}
	return nil
}

// ReloadTrigger coalesces background configuration reloads.
type ReloadTrigger struct {
	pending chan struct{}
	done    chan struct{}
}

// NewReloadTrigger starts the background reload worker.
func NewReloadTrigger(ctx context.Context, reloadFn func(string)) *ReloadTrigger {
	t := &ReloadTrigger{pending: make(chan struct{}, 1), done: make(chan struct{})}
	go func() {
		defer close(t.done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.pending:
				if ctx.Err() != nil {
					return
				}
				if reloadFn != nil {
					reloadFn("git-sync webhook")
				}
			}
		}
	}()
	return t
}

// Trigger queues a reload without blocking.
func (t *ReloadTrigger) Trigger() {
	if t == nil {
		return
	}
	select {
	case t.pending <- struct{}{}:
	default:
	}
}

// HealthMuxOptions configures the health and internal endpoint HTTP mux.
type HealthMuxOptions struct {
	AgentName     string
	Pool          *queue.WorkerPool
	LLMFunc       runner.LLMFunc
	MCPRoutes     *env.MCPProxyRoutes
	ActiveTurns   *runner.ActiveTurnRegistry
	ReloadTrigger *ReloadTrigger
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	httpkit.WriteJSON(w, status, map[string]string{"error": msg})
}

// SetupHealthMux creates the ServeMux for health, metrics, quota, llm, and diagnostics.
func SetupHealthMux(opts HealthMuxOptions) *http.ServeMux {
	mux := http.NewServeMux()

	healthHandler := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, wErr := w.Write([]byte(`{"status":"ok"}`)); wErr != nil {
			log.Printf("[eri-restate] Warning writing health response: %v", wErr)
		}
	}
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/healthz", healthHandler)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("eri-restate"))

	if opts.ReloadTrigger != nil {
		mux.Handle("POST /-/reload", httpkit.LoopbackOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			opts.ReloadTrigger.Trigger()
			httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "queued"})
		})))
	}

	// NOTE: Unlike POST /-/reload (which is invoked by the pod-local git-sync sidecar over 127.0.0.1),
	// /internal/quota and /internal/llm are cross-pod cluster endpoints invoked by stateless
	// Deployment/eri pods over the Kubernetes pod network via Service/eri-restate-health:4028
	// (isolated by Kubernetes NetworkPolicy). They must NOT be wrapped in httpkit.LoopbackOnly,
	// which would reject cross-pod requests from Deployment/eri with 403 Forbidden.
	mux.HandleFunc("/internal/quota", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		groups := metrics.LatestQuotaGroups()
		httpkit.WriteJSON(w, http.StatusOK, groups)
	})

	llmFn := opts.LLMFunc
	if llmFn == nil && opts.Pool != nil {
		llmFn = opts.Pool.LLMFunc()
	}

	mux.HandleFunc("/internal/llm", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if llmFn == nil {
			writeErr(w, http.StatusServiceUnavailable, "llm function not available")
			return
		}
		var req struct {
			Prompt string `json:"prompt"`
			Model  string `json:"model"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
				writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			writeErr(w, http.StatusBadRequest, "failed to read body: "+err.Error())
			return
		}
		if uErr := json.Unmarshal(body, &req); uErr != nil {
			writeErr(w, http.StatusBadRequest, "invalid json payload: "+uErr.Error())
			return
		}
		prompt := strings.TrimSpace(req.Prompt)
		if prompt == "" {
			writeErr(w, http.StatusBadRequest, "prompt is required")
			return
		}
		resp, err := llmFn(r.Context(), req.Model, prompt)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"response": resp})
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
	httpClient     *http.Client
	appConfig      *config.Config
	discordSession *discordgo.Session
	eventBus       pubsub.Bus
}

// WithDiscordSession sets an explicit Discord session for testing or dependency injection.
func WithDiscordSession(s *discordgo.Session) AppOption {
	return func(o *appOptions) {
		o.discordSession = s
	}
}

// WithEventBus sets an explicit pubsub.Bus for testing or dependency injection.
func WithEventBus(bus pubsub.Bus) AppOption {
	return func(o *appOptions) {
		o.eventBus = bus
	}
}

func createReloadFunc(ctx context.Context, appCfg *config.Config, provisioner *env.Provisioner, ephemeralPool *runner.InterchangeablePool, workerPool *queue.WorkerPool) func(string) {
	return func(source string) {
		log.Printf("[eri-restate] Config reload triggered via %s", source)
		if appCfg != nil {
			if err := config.ReloadWithSupplier(appCfg, config.New); err != nil {
				log.Printf("[eri-restate] Error reloading config: %v", err)
				return
			}
			config.RegisterConfigTokens(appCfg)
		}
		if provisioner != nil {
			if pErr := provisioner.Sync(ctx, appCfg); pErr != nil {
				log.Printf("[eri-restate] Warning: env sync error on reload: %v", pErr)
			}
		}
		if ephemeralPool != nil {
			ephemeralPool.MarkDirty()
		}
		if workerPool != nil {
			workerPool.MarkDirty()
		}
	}
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

func defaultReloadLogger(source string) {
	log.Printf("[eri-restate] Config reload triggered via %s", source)
}

var (
	openDefaultDB        = db.New
	onServerReady        func(restateAddr, healthAddr string)
	startUsagePollerFn   = metrics.StartUsagePoller
	reconcileRetryDelay  = 2 * time.Second
	reconcileMaxAttempts = 3
)

func initDatabaseAndStores(_ context.Context, appCfg *config.Config, cur *config.ConfigData) (*sql.DB, db.Store, db.TranscriptStore, error) {
	database, err := openDefaultDB(appCfg)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	store := db.NewSQLStore(database)
	stack, err := db.ResolveTranscriptStack(cur.DatabaseURL, cur.Transcripts.Stack)
	if err != nil {
		_ = database.Close()
		return nil, nil, nil, fmt.Errorf("failed to resolve transcript stack: %w", err)
	}
	txStore := db.NewSQLTranscriptStore(database, stack)
	return database, store, txStore, nil
}

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

func wireOutboundIntegrations(ctx context.Context, cur *config.ConfigData, workerPool *queue.WorkerPool, appOpts appOptions) func() {
	if appOpts.discordSession != nil {
		workerPool.SetDiscordSession(appOpts.discordSession)
	} else if token := strings.TrimSpace(cur.DiscordToken); token != "" {
		if dg, err := discordgo.New("Bot " + token); err == nil {
			workerPool.SetDiscordSession(dg)
		}
	}

	if appOpts.eventBus != nil {
		workerPool.SetEventBus(appOpts.eventBus)
		return nil
	}
	if natsURL := strings.TrimSpace(cur.NATSURL); natsURL != "" {
		bus, err := pubsub.NewJetStreamBus(ctx, natsURL)
		if err != nil {
			log.Printf("[eri-restate] Warning connecting to NATS event bus: %v", err)
			return nil
		}
		workerPool.SetEventBus(bus)
		return func() {
			if cErr := bus.Close(); cErr != nil {
				log.Printf("[eri-restate] Warning closing event bus: %v", cErr)
			}
		}
	}
	return nil
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
	if cur == nil {
		cur = config.DefaultConfigData()
	}
	config.RegisterConfigTokens(appCfg)

	store := appOpts.store
	txStore := appOpts.txStore
	if store == nil {
		database, s, tx, err := initDatabaseAndStores(ctx, appCfg, cur)
		if err != nil {
			return err
		}
		defer func() {
			if cErr := database.Close(); cErr != nil {
				log.Printf("[eri-restate] Warning closing database: %v", cErr)
			}
		}()
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
	})

	if closeBus := wireOutboundIntegrations(ctx, cur, workerPool, appOpts); closeBus != nil {
		defer closeBus()
	}

	workerPool.Start()
	defer workerPool.Stop()

	reloadTrigger := NewReloadTrigger(ctx, createReloadFunc(ctx, appCfg, provisioner, pools.ephemeralPool, workerPool))

	restateSrv := NewEriRestateServer(workerPool, cfg.AgentName)
	restateHandler, err := restateSrv.Handler()
	if err != nil {
		return fmt.Errorf("create restate handler: %w", err)
	}

	healthMux := SetupHealthMux(HealthMuxOptions{
		AgentName:     cfg.AgentName,
		Pool:          workerPool,
		LLMFunc:       llmFn,
		MCPRoutes:     mcpRoutes,
		ActiveTurns:   activeTurns,
		ReloadTrigger: reloadTrigger,
	})

	reconcileTrigger := startDiscoveryTimeoutReconciler(ctx, cfg.AdminURL, cfg.AgentName, appOpts.httpClient, reconcileMaxAttempts, reconcileRetryDelay)
	reconcileTrigger.Trigger()

	return runServers(ctx, cfg, wrapDiscoveryTimeoutReconciler(restateHandler, reconcileTrigger.Trigger), healthMux)
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
