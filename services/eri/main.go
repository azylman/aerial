package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/ambient"
	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/mcp"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

type ReloadOption func(*reloadConfigOptions)

type reloadConfigOptions struct {
	dgSession             *discordgo.Session
	reloadSupplier        func(active *config.Config) error
	skipEnvironmentSync   bool
	workerPool            *queue.WorkerPool
	onReload              func(source string)
	lastDaemonFingerprint string
}

func WithDiscordSession(s *discordgo.Session) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.dgSession = s
	}
}

func WithWorkerPool(pool *queue.WorkerPool) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.workerPool = pool
	}
}

func ensureConfigDefaults(cfg *config.Config) {
	if cfg == nil {
		return
	}
	cur := cfg.Current()
	changed := false
	if cur.GeminiHomeDir == "" {
		cur.GeminiHomeDir = DefaultGeminiHomeDir()
		changed = true
	}
	if cur.DataDir == "" {
		cur.DataDir = DefaultDataDir()
		changed = true
	}
	if changed {
		cfg.Update(cur)
	}
}

func computeDaemonReloadFingerprint(cfg *config.Config) string {
	if cfg == nil || cfg.Current() == nil {
		return ""
	}
	cur := cfg.Current()
	homeDir := cur.GeminiHomeDir
	if homeDir == "" {
		homeDir = DefaultGeminiHomeDir()
	}
	dataDir := cur.DataDir
	if dataDir == "" {
		dataDir = DefaultDataDir()
	}

	var b strings.Builder
	b.WriteString("homeDir:")
	b.WriteString(homeDir)
	b.WriteString("\ndataDir:")
	b.WriteString(dataDir)
	b.WriteString("\nengine:")
	b.WriteString(cur.GetEngine())
	b.WriteString("\nclaudeBin:")
	b.WriteString(cur.ClaudeBin)
	b.WriteString("\ncodexBin:")
	b.WriteString(cur.CodexBin)
	b.WriteString("\nmuseBin:")
	b.WriteString(cur.MuseBin)
	b.WriteString("\nagyBin:")
	b.WriteString(cur.AgyBin)
	b.WriteString("\nmodel:")
	b.WriteString(cur.Model)
	b.WriteString("\nlowEffortModel:")
	b.WriteString(cur.LowEffortModel)
	b.WriteString("\nmediumEffortModel:")
	b.WriteString(cur.MediumEffortModel)
	b.WriteString("\ntimezone:")
	b.WriteString(cur.Timezone)
	b.WriteString("\napiKey:")
	b.WriteString(cur.APIKey)
	b.WriteString("\nharnessApiKey:")
	b.WriteString(cur.HarnessAPIKey)
	b.WriteString("\nsystemPrompt:")
	b.WriteString(cur.SystemPrompt)
	b.WriteString("\nvoice.engine:")
	b.WriteString(cur.Voice.Engine)
	b.WriteString("\nvoice.model:")
	b.WriteString(cur.Voice.Model)
	b.WriteString("\nvoice.prewarmedTargets:")
	b.WriteString(strings.Join(cur.Voice.PrewarmedTargets, ","))
	b.WriteString("\nvoiceAllowedTools:")
	b.WriteString(strings.Join(cur.VoiceAllowedTools(), ","))
	b.WriteString("\nvoiceAmbientContext:")
	if amb := cur.VoiceAmbientContext(); amb != nil {
		if ambBytes, err := json.Marshal(amb); err == nil {
			b.Write(ambBytes)
		}
	} else {
		b.WriteString("nil")
	}
	b.WriteString("\nmcpServers:")
	if mcpServersJSON, err := json.Marshal(cur.McpServers); err == nil {
		b.Write(mcpServersJSON)
	}
	b.WriteString("\nmcps:")
	if mcpsJSON, err := json.Marshal(cur.MCPServers); err == nil {
		b.Write(mcpsJSON)
	}
	b.WriteString("\nmcpConfig:")
	b.WriteString(cur.MCPConfig)
	b.WriteString("\ncredentialSources:")
	if credJSON, err := json.Marshal(cur.CredentialSources); err == nil {
		b.Write(credJSON)
	}
	b.WriteByte('\n')

	h := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(h[:])
}

func markDirtyIfFingerprintChanged(source string, cfg *config.Config, options *reloadConfigOptions) {
	nextFP := computeDaemonReloadFingerprint(cfg)
	if nextFP == options.lastDaemonFingerprint {
		log.Printf("[%s] Daemon configuration and provisioned assets unchanged; preserving active daemon sessions", source)
		return
	}
	options.lastDaemonFingerprint = nextFP
	if options.workerPool != nil {
		options.workerPool.MarkDirty()
	}
}

func executeConfigReload(source string, cfg *config.Config, options *reloadConfigOptions) {
	candidate := config.NewFromData(cfg.Current())
	reloadErr := prepareReloadCandidate(candidate, options)
	if err := reloadErr; err != nil {
		metrics.ConfigReloadsTotal.WithLabelValues(source, "failure").Inc()
		log.Printf("[%s] Warning: Failed to reload config: %v", source, err)
		if options.dgSession != nil {
			alertMsg := fmt.Sprintf("Failed to reload config:\n```\n%v\n```\nAerial has retained the Last Known Good Configuration (LKGC).", err)
			if alertErr := delivery.SendSystemAlert(options.dgSession, cfg.Current().SystemChannel, "Invalid Configuration File", alertMsg); alertErr != nil {
				log.Printf("[%s] Warning: Failed to send system alert for config reload failure: %v", source, alertErr)
			}
		}
		return
	}
	ensureConfigDefaults(candidate)
	cfg.Update(candidate.Current())
	metrics.ConfigReloadsTotal.WithLabelValues(source, "success").Inc()
	config.RegisterConfigTokens(cfg)

	markDirtyIfFingerprintChanged(source, cfg, options)
}

func CreateReloadConfigFunc(cfg *config.Config, opts ...ReloadOption) func(source string) {
	options := &reloadConfigOptions{
		reloadSupplier: func(candidate *config.Config) error {
			// Loading a reload candidate must not publish to ActiveConfig before validation.
			return config.ReloadWithSupplier(candidate, func() (*config.Config, error) {
				return config.LoadConfigFromLookup(config.WithSharedSecretsLookup(os.Getenv, config.SharedSecretsPath),
					config.ResolveConfigSearchPaths(os.Getenv, config.ConfigSearchPaths...)...)
			})
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}

	options.lastDaemonFingerprint = computeDaemonReloadFingerprint(cfg)

	var reloadMu sync.Mutex
	return func(source string) {
		reloadMu.Lock()
		defer reloadMu.Unlock()
		defer func() {
			if options.onReload != nil {
				options.onReload(source)
			}
		}()
		log.Printf("[%s] Changes detected. Reloading configuration, system rules, and skills...", source)
		executeConfigReload(source, cfg, options)
	}
}

// BrainAppOption configures optional runtime dependencies for RunBrainApp.
type BrainAppOption func(*brainAppOptions)

type brainAppOptions struct {
	store          db.Store
	txStore        db.TranscriptStore
	processSpawner runner.DaemonSpawner
	hupChan        <-chan os.Signal
	onReload       func(source string)
	agentName      string
	tracingEnabled bool
	traceContent   bool // record prompt/response text on agent spans (capture_content)
	restateClient  *RestateIngressClient
}

// WithRestateIngressClient sets a custom RestateIngressClient for RunBrainApp.
func WithRestateIngressClient(client *RestateIngressClient) BrainAppOption {
	return func(o *brainAppOptions) {
		o.restateClient = client
	}
}

func extractMCPServers(cur *config.ConfigData, includeDiscord bool) []runner.MCPServerConfig {
	serverMap := make(map[string]runner.MCPServerConfig)
	if cur != nil {
		type srvJSON struct {
			ServerURL string            `json:"serverUrl"`
			Headers   map[string]string `json:"headers,omitempty"`
		}
		parseMap := func(m map[string]json.RawMessage) {
			for name, raw := range m {
				var sj srvJSON
				if err := json.Unmarshal(raw, &sj); err == nil && strings.TrimSpace(sj.ServerURL) != "" {
					serverMap[name] = runner.MCPServerConfig{
						Name:      name,
						ServerURL: strings.TrimSpace(sj.ServerURL),
						Headers:   sj.Headers,
					}
				}
			}
		}
		parseMap(cur.McpServers.Common)
		if includeDiscord {
			parseMap(cur.McpServers.Discord)
		}
		parseMap(cur.McpServers.Voice)
	}
	result := make([]runner.MCPServerConfig, 0, len(serverMap))
	for _, s := range serverMap {
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
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

func newAmbientContextResolver(cfg *config.Config) func(ctx context.Context, ambCfg *config.AmbientContextConfig) (string, error) {
	var ambientProviders sync.Map
	return func(ctx context.Context, ambCfg *config.AmbientContextConfig) (string, error) {
		if ambCfg == nil {
			return "", nil
		}
		toolsJSON, err := json.Marshal(ambCfg.Tools)
		if err != nil {
			log.Printf("[WorkerPool] Warning: failed to marshal ambient tools for cache key: %v", err)
		}
		key := fmt.Sprintf("%s:%s:%s:%s", ambCfg.TemplatePath, ambCfg.Template, string(toolsJSON), ambCfg.CacheTTL)
		val, ok := ambientProviders.Load(key)
		var prov *ambient.Provider
		if ok {
			if p, ok := val.(*ambient.Provider); ok {
				prov = p
			}
		}
		if prov == nil {
			var curData *config.ConfigData
			if cfg != nil {
				curData = cfg.Current()
			}
			servers := extractMCPServers(curData, true)
			disp := mcp.NewDispatcher(servers, nil)
			var pErr error
			prov, pErr = ambient.NewProvider(*ambCfg, disp)
			if pErr != nil {
				return "", fmt.Errorf("failed to create ambient provider: %w", pErr)
			}
			// Priming the shared, cached provider outlives the turn that first created it.
			go func() {
				primeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer cancel()
				if err := prov.Prime(primeCtx); err != nil {
					log.Printf("[WorkerPool] Ambient context prime warning: %v", err)
				}
			}()
			ambientProviders.Store(key, prov)
		}
		return prov.Retrieve(ctx)
	}
}

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

// reloadTrigger runs git-sync webhook reloads on a single background worker,
// coalescing bursts of requests. pending holds at most one queued reload: a
// Trigger that arrives while one is already pending is dropped, because the
// queued reload will read the newest files anyway. A burst of webhook calls
// therefore costs at most one extra reload after the one in flight.
type reloadTrigger struct {
	pending chan struct{}
	done    chan struct{}
}

// newReloadTrigger starts the reload worker. The worker calls reloadConfig for
// each pending request until ctx is cancelled, then closes done. A request
// still pending at cancellation is discarded rather than reloaded mid-shutdown.
func newReloadTrigger(ctx context.Context, reloadConfig func(string)) *reloadTrigger {
	t := &reloadTrigger{pending: make(chan struct{}, 1), done: make(chan struct{})}
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
				reloadConfig("git-sync webhook")
			}
		}
	}()
	return t
}

// Trigger queues a reload without blocking; it is a no-op when one is already
// pending.
func (t *reloadTrigger) Trigger() {
	select {
	case t.pending <- struct{}{}:
	default:
	}
}

// handleGitSyncReload serves the git-sync sidecar webhook mounted by
// registerGitSyncReload. A full reload (config, rules, skills, MCP config,
// daemon pool MarkDirty) takes 1-3s, but git-sync's webhook has a 1s default
// timeout and retries every few seconds until it sees its configured success
// status. Reloading on the request path therefore made every call time out and
// every retry run another reload that recycled warm daemons. The handler only
// queues a coalesced reload on trigger and answers 200 immediately. eri runs
// two git-sync sidecars (aerial rules/skills and aerial-config) that may fire at
// the same moment; the trigger's single worker serializes their reloads, and
// reloadConfig retains the last known good config on failure, so there is no
// error to report back to git-sync.
func handleGitSyncReload(trigger *reloadTrigger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		trigger.Trigger()
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{jsonKeyStatus: "queued"})
	})
}

// registerGitSyncReload mounts the git-sync webhook on mux. The endpoint
// queues a full config reload, so it must stay reachable only from the
// in-pod sidecars over loopback. The method-qualified pattern makes the mux
// answer 405 for anything but POST.
func registerGitSyncReload(mux *http.ServeMux, trigger *reloadTrigger) {
	mux.Handle("POST /-/reload", httpkit.LoopbackOnly(handleGitSyncReload(trigger)))
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
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
		log.Println("Aerial Brain shutdown complete")
		return nil
	case err := <-errChan:
		return err
	case err := <-leaderErrCh:
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer shutdownCancel()
		return errors.Join(err, srv.Shutdown(shutdownCtx))
	}
}

func initDefaultStores(cfg *config.Config, cur *config.ConfigData) (db.Store, db.TranscriptStore, func(), error) {
	database, err := openDefaultDB(cfg)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	stack, err := db.ResolveTranscriptStack(cur.DatabaseURL, cur.Transcripts.Stack)
	if err != nil {
		_ = database.Close()
		return nil, nil, nil, fmt.Errorf("failed to resolve transcript stack: %w", err)
	}
	store := db.NewSQLStore(database)
	txStore := db.NewSQLTranscriptStore(database, stack)
	cleanup := func() {
		for _, s := range []any{txStore, store} {
			if closer, ok := s.(io.Closer); ok {
				_ = closer.Close()
			}
		}
		if closeErr := database.Close(); closeErr != nil {
			log.Printf("Error closing database: %v", closeErr)
		}
	}
	return store, txStore, cleanup, nil
}

func resolveBrainStores(cfg *config.Config, cur *config.ConfigData, opts brainAppOptions) (db.Store, db.TranscriptStore, func(), error) {
	if opts.store != nil {
		return opts.store, opts.txStore, nil, nil
	}
	return initDefaultStores(cfg, cur)
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
	ambientResolver := newAmbientContextResolver(cfg)

	restateClient := appOpts.restateClient
	if restateClient == nil {
		if ingressURL := strings.TrimSpace(os.Getenv("RESTATE_INGRESS_URL")); ingressURL != "" {
			restateClient = NewRestateIngressClient(ingressURL, appOpts.agentName)
		}
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
	})
	detachEventBus := attachEriEventBus(ctx, pool)
	defer detachEventBus()
	pool.Start()

	dgSession, cleanupFunnel := setupDiscordFunnel(ctx, cfg, store, pool, cur.DiscordToken)
	defer cleanupFunnel()

	var reloadOpts []ReloadOption
	reloadOpts = append(reloadOpts, WithDiscordSession(dgSession), WithWorkerPool(pool))
	if appOpts.onReload != nil {
		reloadOpts = append(reloadOpts, func(o *reloadConfigOptions) { o.onReload = appOpts.onReload })
	}
	reloadConfig := CreateReloadConfigFunc(cfg, reloadOpts...)

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
	gitSyncReloads := newReloadTrigger(reloadCtx, reloadConfig)
	defer func() {
		stopReloads()
		<-gitSyncReloads.done
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
		cur.GeminiHomeDir = DefaultGeminiHomeDir()
	}
	if cur.DataDir == "" {
		cur.DataDir = DefaultDataDir()
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

// prepareReloadCandidate loads candidate configuration from supplier without mutating active config.
func prepareReloadCandidate(candidate *config.Config, options *reloadConfigOptions) error {
	return options.reloadSupplier(candidate)
}
