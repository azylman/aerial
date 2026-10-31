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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/azylman/aerial/services/eri/pkg/ambient"
	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/mcp"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
	"github.com/bwmarrin/discordgo"
)

func InitializeBrainEnvironment(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	provisioner := env.NewFromConfig(cfg)
	if err := provisioner.Sync(ctx, cfg); err != nil {
		log.Printf("Warning: env sync error: %v", err)
		return err
	}
	return nil
}

type ReloadOption func(*reloadConfigOptions)

type reloadConfigOptions struct {
	dgSession           *discordgo.Session
	reloadSupplier      func(active *config.Config) error
	skipEnvironmentSync bool
	provisioner         *env.Provisioner
	workerPool          *queue.WorkerPool
	onReload            func(source string)
}

func WithDiscordSession(s *discordgo.Session) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.dgSession = s
	}
}

func WithProvisioner(p *env.Provisioner) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.provisioner = p
	}
}

func WithWorkerPool(pool *queue.WorkerPool) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.workerPool = pool
	}
}

func ensureConfigDefaults(cfg *config.Config) {
	cur := cfg.Current()
	if cur == nil {
		return
	}
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

func executeConfigReload(source string, cfg *config.Config, options *reloadConfigOptions) {
	if cfg == nil {
		return
	}
	if err := options.reloadSupplier(cfg); err != nil {
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
	metrics.ConfigReloadsTotal.WithLabelValues(source, "success").Inc()
	ensureConfigDefaults(cfg)
	config.RegisterConfigTokens(cfg)

	if !options.skipEnvironmentSync && options.provisioner != nil {
		if err := options.provisioner.Sync(context.Background(), cfg); err != nil {
			log.Printf("[%s] Warning: env sync error: %v", source, err)
		}
	}

	if options.workerPool != nil {
		options.workerPool.MarkDirty()
	}
}

func CreateReloadConfigFunc(cfg *config.Config, opts ...ReloadOption) func(source string) {
	options := &reloadConfigOptions{
		reloadSupplier: config.Reload,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}
	if options.provisioner == nil && cfg != nil {
		options.provisioner = env.NewFromConfig(cfg)
	}

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
	processSpawner runner.DaemonSpawner
	hupChan        <-chan os.Signal
	onReload       func(source string)
}

func extractMCPServers(cur *config.ConfigData, includeDiscord bool) []runner.MCPServerConfig {
	serverMap := make(map[string]runner.MCPServerConfig)
	serverMap["scheduler"] = runner.MCPServerConfig{
		Name:      "scheduler",
		ServerURL: "http://haro.aerial/mcp",
	}
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

// buildPoolEnv returns a process environment slice with unified compiler and module
// cache directories under runtimeBase/cache when not already explicitly set.
func buildPoolEnv(baseEnv []string, runtimeBase string, timezone ...string) []string {
	if runtimeBase == "" {
		return baseEnv
	}
	cacheBase := filepath.Join(runtimeBase, "cache")
	for _, dir := range []string{
		filepath.Join(cacheBase, "go-build"),
		filepath.Join(cacheBase, "go", "pkg", "mod"),
		filepath.Join(cacheBase, "golangci-lint"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[WARN] Failed to create cache directory %q: %v", dir, err)
		}
	}

	envMap := make(map[string]bool)
	for _, e := range baseEnv {
		if k, _, ok := strings.Cut(e, "="); ok {
			envMap[k] = true
		}
	}

	var extra []string
	for _, item := range []struct{ key, sub string }{
		{"GOCACHE", "go-build"},
		{"GOPATH", "go"},
		{"GOMODCACHE", filepath.Join("go", "pkg", "mod")},
		{"GOLANGCI_LINT_CACHE", "golangci-lint"},
	} {
		if !envMap[item.key] {
			extra = append(extra, item.key+"="+filepath.Join(cacheBase, item.sub))
		}
	}
	if !envMap["TZ"] {
		var tz string
		if len(timezone) > 0 && strings.TrimSpace(timezone[0]) != "" {
			tz = strings.TrimSpace(timezone[0])
		} else {
			tz = config.GetTimezone()
		}
		if tz != "" {
			extra = append(extra, "TZ="+tz)
		}
	}

	if len(extra) == 0 {
		return baseEnv
	}
	result := make([]string, len(baseEnv)+len(extra))
	copy(result, baseEnv)
	copy(result[len(baseEnv):], extra)
	return result
}

func initVoiceAmbientRetriever(cur *config.ConfigData) runner.AmbientContextRetriever {
	ambCfg := cur.VoiceAmbientContext()
	if ambCfg == nil {
		return nil
	}
	voiceServers := extractMCPServers(cur, false)
	disp := mcp.NewDispatcher(voiceServers, nil)
	p, pErr := ambient.NewProvider(*ambCfg, disp)
	if pErr != nil {
		log.Printf("[INIT] Warning: failed to initialize voice ambient provider: %v", pErr)
		return nil
	}
	go func() {
		primeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := p.Prime(primeCtx); err != nil {
			log.Printf("[INIT] Ambient context prime warning: %v", err)
		}
	}()
	log.Printf("[INIT] Voice ambient context provider initialized (tools=%d, cache_ttl=%s)", len(ambCfg.Tools), ambCfg.CacheTTL)
	return p.Retrieve
}

func buildGeminiVoicePool(cur *config.ConfigData, lowEffortModel string, prewarmedTargets []string, memoryRetriever runner.MemoryRetriever, ambRetriever runner.AmbientContextRetriever) (runner.AgentPool, string, string, []runner.MCPServerConfig, bool) {
	apiKey := cur.HarnessAPIKey
	if apiKey == "" {
		apiKey = cur.APIKey
	}
	if apiKey == "" {
		log.Printf("[WARN] voice.engine is configured as 'gemini_api' but neither harness_api_key nor api_key is set; safely falling back to 'agy'")
		return nil, "", "", nil, false
	}
	geminiModel := lowEffortModel
	if strings.TrimSpace(cur.Voice.Model) != "" {
		geminiModel = strings.TrimSpace(cur.Voice.Model)
	}
	mcpServers := extractMCPServers(cur, false)
	voicePool := runner.NewGeminiAPIPool(runner.GeminiAPIPoolConfig{
		APIKey:                  apiKey,
		Model:                   geminiModel,
		PrewarmedTargets:        prewarmedTargets,
		SystemPrompt:            cur.SystemPrompt,
		MCPServers:              mcpServers,
		AllowedTools:            cur.VoiceAllowedTools(),
		DataDir:                 cur.DataDir,
		MemoryRetriever:         memoryRetriever,
		AmbientContextRetriever: ambRetriever,
	})
	log.Printf("[INIT] Voice engine initialized with 'gemini_api' (model=%s, prewarmed=%v, mcp_servers=%d, allowed_tools=%d)", geminiModel, prewarmedTargets, len(mcpServers), len(cur.VoiceAllowedTools()))
	return voicePool, geminiModel, apiKey, mcpServers, true
}

func buildUnifiedVoicePool(cfg *config.Config, cur *config.ConfigData, voiceHome, lowEffortModel string, prewarmedTargets []string, spawner runner.DaemonSpawner, memoryRetriever runner.MemoryRetriever, ambRetriever runner.AmbientContextRetriever) runner.AgentPool {
	runtimeBase := cur.DataDir
	if runtimeBase == "" && cfg != nil {
		runtimeBase = cfg.DataDir()
	}
	if runtimeBase == "" && cfg != nil {
		runtimeBase = cfg.GeminiHomeDir()
	}
	voiceSessionMgr := session.New(voiceHome, runtimeBase)
	return runner.NewUnifiedProcessPool(runner.PoolConfig{
		GeminiHomeDir:           voiceHome,
		Model:                   lowEffortModel,
		AgyBin:                  cur.AgyBin,
		Cwd:                     cur.DataDir,
		Env:                     buildPoolEnv(os.Environ(), runtimeBase, cur.Timezone),
		Timezone:                cur.Timezone,
		PrewarmedTargets:        prewarmedTargets,
		MemoryRetriever:         memoryRetriever,
		AmbientContextRetriever: ambRetriever,
		SessionManager:          voiceSessionMgr,
	}, spawner)
}

func computeVoiceConfigFingerprint(voiceEngine, effectiveModel, effectiveAPIKey string, cur *config.ConfigData, prewarmedTargets []string, mcpServers []runner.MCPServerConfig) string {
	var b strings.Builder
	b.WriteString("engine:")
	b.WriteString(voiceEngine)
	b.WriteString("\nmodel:")
	b.WriteString(effectiveModel)
	b.WriteString("\n")
	if effectiveAPIKey != "" {
		keyHash := sha256.Sum256([]byte(effectiveAPIKey))
		b.WriteString("apiKeyHash:")
		b.WriteString(hex.EncodeToString(keyHash[:]))
		b.WriteString("\n")
	}
	if ambCfg := cur.VoiceAmbientContext(); ambCfg != nil {
		b.WriteString("ambient_context_tools:")
		fmt.Fprintf(&b, "%d", len(ambCfg.Tools))
		b.WriteString("\nambient_context_ttl:")
		b.WriteString(ambCfg.CacheTTL)
		b.WriteString("\nambient_context_template:")
		b.WriteString(ambCfg.Template)
		b.WriteString("\nambient_context_template_path:")
		b.WriteString(ambCfg.TemplatePath)
		b.WriteString("\n")
	}
	for _, target := range prewarmedTargets {
		b.WriteString("prewarmed:")
		b.WriteString(target)
		b.WriteString("\n")
	}
	for _, s := range mcpServers {
		b.WriteString("mcp:")
		b.WriteString(s.Name)
		b.WriteString("=")
		b.WriteString(s.ServerURL)
		b.WriteString("\n")
	}
	b.WriteString("systemPrompt:")
	b.WriteString(cur.SystemPrompt)
	b.WriteString("\n")

	h := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(h[:])
}

// createVoiceProcessPool constructs a runner.AgentPool based on cfg.VoiceEngine()
// wrapped in runner.DynamicVoicePool with a factory closure evaluating cfg.Current() dynamically.
func createVoiceProcessPool(cfg *config.Config, voiceHome string, lowEffortModel string, spawner runner.DaemonSpawner, memRetriever ...runner.MemoryRetriever) runner.AgentPool {
	var memoryRetriever runner.MemoryRetriever
	if len(memRetriever) > 0 {
		memoryRetriever = memRetriever[0]
	}
	buildPool := func() (runner.AgentPool, string, error) {
		var cur *config.ConfigData
		var voiceEngine string
		var prewarmedTargets []string
		if cfg != nil {
			cur = cfg.Current()
			voiceEngine = cfg.VoiceEngine()
			prewarmedTargets = cfg.VoicePrewarmedTargets()
		} else {
			cur = config.DefaultConfigData()
			voiceEngine = "agy"
			prewarmedTargets = []string{}
		}

		var voicePool runner.AgentPool
		var effectiveModel string
		var effectiveAPIKey string
		var mcpServers []runner.MCPServerConfig

		ambRetriever := initVoiceAmbientRetriever(cur)

		if voiceEngine == "gemini_api" {
			pool, model, apiKey, servers, ok := buildGeminiVoicePool(cur, lowEffortModel, prewarmedTargets, memoryRetriever, ambRetriever)
			if ok {
				voicePool = pool
				effectiveModel = model
				effectiveAPIKey = apiKey
				mcpServers = servers
			} else {
				voiceEngine = "agy"
			}
		}
		if voiceEngine != "gemini_api" {
			effectiveModel = lowEffortModel
			voicePool = buildUnifiedVoicePool(cfg, cur, voiceHome, lowEffortModel, prewarmedTargets, spawner, memoryRetriever, ambRetriever)
		}

		fingerprint := computeVoiceConfigFingerprint(voiceEngine, effectiveModel, effectiveAPIKey, cur, prewarmedTargets, mcpServers)
		return voicePool, fingerprint, nil
	}

	return runner.NewDynamicVoicePool(buildPool)
}

// PrewarmProcessPoolsSequentially pre-warms process pool targets in series in the background,
// ensuring only one daemon is in the CPU-intensive startup phase at a time across all pools.
func newWorkerInterchangeablePool(pCfg runner.PoolConfig, spawner runner.DaemonSpawner, keyPrefix string) *runner.InterchangeablePool {
	underlying := runner.NewUnifiedProcessPool(pCfg, spawner)
	return runner.NewInterchangeablePool(underlying, runner.InterchangeablePoolConfig{
		WorkerCount: len(pCfg.PrewarmedTargets),
		KeyPrefix:   keyPrefix,
	})
}

func PrewarmProcessPoolsSequentially(ctx context.Context, pools ...runner.AgentPool) {
	if ctx == nil {
		ctx = context.Background()
	}
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

var (
	onServerReady func(addr string)
	openDefaultDB = db.New
)

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

func resolveBrainModelDefaults(cur *config.ConfigData) (lowEffortModel, mediumEffortModel string) {
	lowEffortModel = cur.LowEffortModel
	if lowEffortModel == "" {
		lowEffortModel = "gemini-3.8-flash-low"
	}
	mediumEffortModel = cur.MediumEffortModel
	if mediumEffortModel == "" {
		mediumEffortModel = "gemini-3.8-flash-medium"
	}
	return lowEffortModel, mediumEffortModel
}

func buildBrainProcessPools(
	cfg *config.Config,
	cur *config.ConfigData,
	sessionMgr *session.Manager,
	spawner runner.DaemonSpawner,
) (map[queue.PoolKey]runner.AgentPool, *runner.InterchangeablePool, runner.AgentPool, []runner.AgentPool, []string, string) {
	lowEffortModel, mediumEffortModel := resolveBrainModelDefaults(cur)
	runtimeBase := cur.DataDir

	discordHome := filepath.Join(runtimeBase, "runtimes", "discord")
	voiceHome := filepath.Join(runtimeBase, "runtimes", "voice")
	ephemeralHome := filepath.Join(runtimeBase, "runtimes", "ephemeral")

	poolEnv := buildPoolEnv(os.Environ(), runtimeBase, cur.Timezone)
	memoryRetriever := newHTTPMemoryRetriever("")

	basePoolCfg := func(home, model string, prewarmed []string) runner.PoolConfig {
		pcfg := runner.PoolConfig{
			GeminiHomeDir:     home,
			Model:             model,
			AgyBin:            cur.AgyBin,
			Cwd:               cur.DataDir,
			Env:               poolEnv,
			Timezone:          cur.Timezone,
			TranscriptRescuer: sessionMgr.ExtractResponseSince,
			SessionManager:    sessionMgr,
			PrewarmedTargets:  prewarmed,
		}
		if home == discordHome {
			pcfg.MemoryRetriever = memoryRetriever
		}
		return pcfg
	}

	discordPools, discordPoolList := buildDiscordPools(basePoolCfg, discordHome, cur.Model, mediumEffortModel, lowEffortModel, spawner)
	ephemeralPool := newWorkerInterchangeablePool(basePoolCfg(ephemeralHome, lowEffortModel, []string{"ephemeral:worker-0", "ephemeral:worker-1"}), spawner, "ephemeral:worker")
	var voicePool runner.AgentPool = createVoiceProcessPool(cfg, voiceHome, lowEffortModel, spawner, memoryRetriever)
	allPools := append(append([]runner.AgentPool{}, discordPoolList...), ephemeralPool, voicePool)

	return discordPools, ephemeralPool, voicePool, allPools, poolEnv, ephemeralHome
}

func closeAllBrainPools(pools []runner.AgentPool) {
	for _, p := range pools {
		if err := p.Close(); err != nil {
			log.Printf("[WARN] Failed to close process pool: %v", err)
		}
	}
}

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
			go func() {
				primeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func connectAndRecoverDiscordFunnel(
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
		pool.SetWidgetDeliveryFunc(func(req discord.StatusWidgetRequest) (string, error) {
			return discord.DeliverStatusWidget(dgSession, req, time.Now())
		})
	}
	queue.RecoverInterrupted(store, pool)
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

func serveBrainHTTP(ctx context.Context, ln net.Listener, mux http.Handler, model string) error {
	srv := &http.Server{
		Handler: metricsMiddleware(mux),
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

	select {
	case <-ctx.Done():
		log.Println("Shutting down Aerial Brain gracefully...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
		log.Println("Aerial Brain shutdown complete")
		return nil
	case err := <-errChan:
		return err
	}
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

	provisioner := env.NewFromConfig(cfg)
	if err := InitializeBrainEnvironment(ctx, cfg); err != nil {
		log.Printf("Warning initializing brain environment: %v", err)
	}

	var store db.Store
	if appOpts.store != nil {
		store = appOpts.store
	} else {
		database, err := openDefaultDB(cfg)
		if err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
		}
		defer func() {
			if err := database.Close(); err != nil {
				log.Printf("Error closing database: %v", err)
			}
		}()
		store = db.NewSQLStore(database)
		defer func() {
			if closer, ok := store.(io.Closer); ok {
				_ = closer.Close()
			}
		}()
	}

	config.RegisterConfigTokens(cfg)

	sessionMgr := session.New(cfg.GeminiHomeDir(), cfg.DataDir())

	discordPools, ephemeralPool, voicePool, allPools, poolEnv, ephemeralHome := buildBrainProcessPools(cfg, cur, sessionMgr, appOpts.processSpawner)
	defer closeAllBrainPools(allPools)

	PrewarmProcessPoolsSequentially(ctx, allPools...)
	metrics.StartUsagePoller(ctx, 10*time.Minute, metrics.DefaultUsageRunner(cur.AgyBin, ephemeralHome, poolEnv, testing.Testing()))

	cls := classifier.New(cfg, classifier.WithProcessPool(ephemeralPool))
	ambientResolver := newAmbientContextResolver(cfg)

	pool := queue.New(cfg, queue.WorkerPoolConfig{
		Store:            store,
		Classifier:       cls,
		SessionManager:   sessionMgr,
		Pools:            discordPools,
		VoiceProcessPool: voicePool,
		LLMFunc:          ephemeralPool.EphemeralLLMFunc("ephemeral:summarizer"),
		AmbientResolver:  ambientResolver,
		HaroURL:          os.Getenv("HARO_URL"),
	})
	pool.Start()

	dgSession, cleanupFunnel := connectAndRecoverDiscordFunnel(ctx, cfg, store, pool, cur.DiscordToken)
	defer cleanupFunnel()

	var reloadOpts []ReloadOption
	reloadOpts = append(reloadOpts, WithDiscordSession(dgSession), WithProvisioner(provisioner), WithWorkerPool(pool))
	if appOpts.onReload != nil {
		reloadOpts = append(reloadOpts, func(o *reloadConfigOptions) { o.onReload = appOpts.onReload })
	}
	reloadConfig := CreateReloadConfigFunc(cfg, reloadOpts...)

	stopHup := startSIGHUPReloadLoop(ctx, appOpts.hupChan, reloadConfig)
	defer stopHup()

	syncLLM := buildSyncLLM(ephemeralPool.EphemeralLLMFunc("ephemeral:summarizer"), cur.LowEffortModel, cur.Model)
	transcript.StartTranscriptSync(ctx, store, cfg, "", syncLLM, 5*time.Minute)
	txEmbedder := transcript.NewEmbedder(cfg, sessionMgr.Roots()...)
	var embedder transcript.EmbedderFunc
	if txEmbedder != nil {
		embedder = func(eCtx context.Context, text string) ([]float32, error) {
			return txEmbedder.GenerateEmbedding(eCtx, text, false, 1)
		}
	}
	mux := SetupBrainMuxWithEmbedder(store, pool, embedder, sessionMgr.Roots()...)

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

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

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
	defer cancel()

	go func() {
		<-stopChan
		cancel()
	}()

	if err := RunBrainApp(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server failed: %v", err)
	}
}
