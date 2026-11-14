// Package queue provides an asynchronous FIFO-with-bursts message queue, worker pool,
// status updating, and turn execution engine for Aerial.
package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/notifier"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/bwmarrin/discordgo"
	"go.opentelemetry.io/otel/trace"
)

const (
	DefaultMaxQuotaPauseDBBytes = session.DefaultMaxQuotaPauseDBBytes
	DefaultMaxSessionIdleTime   = 24 * time.Hour
	DefaultTimeoutMinutes       = 60
	DefaultMaxRestarts          = 3
	ContinuationPromptTemplate  = "Your previous execution timed out or was interrupted while working. Please inspect where you left off in the conversation transcript and continue the task to completion.\n\nOriginal user request:\n%s"
	MaxYieldTrapAutoResumes     = 2
	YieldTrapResumePrompt       = "[SYSTEM NOTICE]: Your previous turn yielded prematurely while background task(s) were still running, causing the runtime to terminate them. Inspect current status (check git status, PR state, or process logs) and continue your workflow to completion. Do NOT end the turn with waiting text."
	InterruptedByOperatorReason = "interrupted by operator via slash command"
)

func sanitizeErrorText(errStr string) string {
	return sanitizer.SanitizeLog(errStr)
}

// testPoolHook is an optional hook injected by tests to wrap custom test runners without compiling test adapters into production.
var testPoolHook func(cfg WorkerPoolConfig) runner.AgentPool

// PoolKey uniquely identifies one of the 5 Discord execution pools by ephemerality and effort tier.
type PoolKey struct {
	Ephemeral bool
	Effort    string
}

// AllValidPoolKeys returns the 5 valid Discord PoolKey combinations in deterministic order.
func AllValidPoolKeys() []PoolKey {
	return []PoolKey{
		{Ephemeral: false, Effort: "high"},
		{Ephemeral: false, Effort: "medium"},
		{Ephemeral: false, Effort: "low"},
		{Ephemeral: true, Effort: "medium"},
		{Ephemeral: true, Effort: "low"},
	}
}

// ResolvePoolKey normalizes effort and resolves the target PoolKey, rejecting (ephemeral: true, effort: "high").
func ResolvePoolKey(ephemeral *bool, effort string) (PoolKey, error) {
	eff := strings.ToLower(strings.TrimSpace(effort))
	if eff != "low" && eff != "medium" {
		eff = "high"
	}
	isEphem := eff == "low" || eff == "medium"
	if ephemeral != nil {
		isEphem = *ephemeral
	}
	if isEphem && eff == "high" {
		return PoolKey{Ephemeral: true, Effort: "high"}, errors.New("ephemeral: true cannot be combined with high effort")
	}
	return PoolKey{Ephemeral: isEphem, Effort: eff}, nil
}

type WorkerPoolConfig struct {
	ParentContext                                                                context.Context
	Store                                                                        db.Store
	DiscordSession                                                               *discordgo.Session
	AgyBin, APIKey, Model, LowEffortModel, MediumEffortModel, SystemPrompt       string
	TimeoutMinutes, MaxAttempts                                                  int
	BackoffBase, IdleTimeout, RetryDelayOverride, MaintenanceInterval            time.Duration
	SessionManager                                                               *session.Manager
	Pools                                                                        map[PoolKey]runner.AgentPool
	ProcessPool, LowEffortProcessPool, MediumEffortProcessPool, VoiceProcessPool runner.AgentPool
	Classifier                                                                   *classifier.Classifier
	HaroURL, SynapseURL, VanadisURL                                              string

	// Optional hooks for testing/custom overrides
	RunnerFunc                       func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
	RunnerWithOptionsFunc            func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (stdout, stderr string, exitCode int, err error)
	NotifierFunc                     func(agyBin, apiKey, contextDescription string) string
	DeliveryFunc                     func(s *discordgo.Session, channelID, text string) error
	DeliveryWithAttachmentsFunc      func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment) error
	ReplyDeliveryWithAttachmentsFunc func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment, replyToMessageID string) error
	WidgetDeliveryFunc               func(req discord.StatusWidgetRequest) (string, error)
	TypingFunc                       func(s *discordgo.Session, channelID string) (stop func())
	OnMessageCompleted               func(msg db.Message, finalStatus string)
	ResolveChannelPolicy             func(channelID, channelName string) config.ChannelPolicy
	HistoryFetcher                   HistoryFetcherFunc
	SystemAlertFunc                  func(s *discordgo.Session, channelNameOrID, title, alertBody string) error
	LLMFunc                          LLMFunc
	WebhookDispatcher                WebhookDispatcher
	VoiceRunnerFunc                  func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (reply string, convID string, err error)
	VoiceStreamRunnerFunc            func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (reply string, convID string, err error)
	AmbientResolver                  func(ctx context.Context, cfg *config.AmbientContextConfig) (string, error)
	CreateThreadFunc                 func(channelID, title string) (string, error)
	TracerProvider                   trace.TracerProvider       // turn spans; nil uses the global provider
	Logger                           *slog.Logger               // turn-scoped log lines; nil uses slog.Default()
	AgentName                        string                     // stack persona recorded as gen_ai.agent.name
	ActiveTurns                      *runner.ActiveTurnRegistry // daemons' active turn spans for the MCP trace proxy; nil gets a private registry
}

type queueItem struct {
	msg      *db.Message
	dispatch *db.Dispatch
}

type threadWorkerState struct {
	ch                       chan queueItem
	activeEnqueuers          int
	inFlight                 bool
	lastEnqueuedDiscordMsgID string
}

type WorkerPool struct {
	appCfg              *config.Config
	hasExplicitAppCfg   bool
	overrideModel       string
	cfg                 WorkerPoolConfig
	sessionMgr          *session.Manager
	mu                  sync.Mutex
	threadChs           map[string]*threadWorkerState
	activeTurns         map[string]*turnExecution
	wg                  sync.WaitGroup
	ctx                 context.Context
	cancel              context.CancelFunc
	stopped, leaderHold bool
	quotaLockedUntil    atomic.Int64
	scopeLocks          sync.Map
	webhookDispatcher   WebhookDispatcher
	summaryGroup        singleflight.Group
	pools               map[PoolKey]runner.AgentPool
	voiceProcessPool    runner.AgentPool
	createThreadFn      func(channelID, title string) (string, error)
	widgetDeliveryMu    sync.Mutex
	widgetLockHolder    atomic.Pointer[widgetLockPath] // path holding widgetDeliveryMu; nil when free
	widgetMu            sync.Mutex
	widgetsByID         map[string]discord.StatusWidgetRequest
	widgetOrder         []string
	chunkToBuildID      map[string]string
	activeChunkSteps    map[string]string
	activeChunkTurns    map[string]string
	startedAt           time.Time // anchors queue.wait for rows resumed after a restart
}

// SummaryGroup returns the singleflight.Group coordinating thread summarizations for this pool instance.
func (p *WorkerPool) SummaryGroup() *singleflight.Group {
	if p == nil {
		return nil
	}
	return &p.summaryGroup
}

// LLMFunc returns the configured ephemeral LLM function on the pool, or nil.
func (p *WorkerPool) LLMFunc() LLMFunc {
	if p == nil {
		return nil
	}
	return p.cfg.LLMFunc
}

// Store returns the configured db.Store.
func (p *WorkerPool) Store() db.Store {
	if p == nil {
		return nil
	}
	return p.cfg.Store
}

func makeSessionCallbacks(store db.Store) (func(context.Context, string) (runner.SessionRecord, error), func(context.Context, string, string, string) error) {
	getRec := func(ctx context.Context, targetKey string) (runner.SessionRecord, error) {
		sessID, err := store.GetSessionID(ctx, targetKey)
		if err != nil {
			return runner.SessionRecord{}, err
		}
		turns, err := store.GetSessionTurnCount(ctx, targetKey)
		if err != nil {
			turns = 0
		}
		prevID, err := store.GetPreviousSessionID(ctx, targetKey)
		if err != nil {
			prevID = ""
		}
		return runner.SessionRecord{ActiveSessionID: sessID, PreviousSessionID: prevID, TurnCount: turns}, nil
	}
	onRot := func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
		return store.RotateSessionID(ctx, targetKey, newSessionID)
	}
	return getRec, onRot
}

func wirePoolSessionDependencies(pools map[PoolKey]runner.AgentPool, sessionMgr *session.Manager, store db.Store) {
	var getRec func(context.Context, string) (runner.SessionRecord, error)
	var onRot func(context.Context, string, string, string) error
	if store != nil {
		getRec, onRot = makeSessionCallbacks(store)
	}
	for _, k := range AllValidPoolKeys() {
		pool := pools[k]
		if pool == nil {
			continue
		}
		if sessionMgr != nil {
			if tr, ok := pool.(interface {
				SetTranscriptRescuer(fn func(string, time.Time) string)
			}); ok {
				tr.SetTranscriptRescuer(sessionMgr.ExtractResponseSince)
			}
			if sm, ok := pool.(interface{ SetSessionManager(mgr *session.Manager) }); ok {
				sm.SetSessionManager(sessionMgr)
			}
		}
		if store != nil && !k.Ephemeral {
			if sc, ok := pool.(interface {
				SetSessionCallbacks(getFn func(context.Context, string) (runner.SessionRecord, error), saveFn func(context.Context, string, string, string) error)
			}); ok {
				sc.SetSessionCallbacks(getRec, onRot)
			}
		}
	}
}

func initWorkerPools(appCfg *config.Config, isExplicitAppCfg bool, cfg WorkerPoolConfig, sessMgr *session.Manager, store db.Store) map[PoolKey]runner.AgentPool {
	pools := make(map[PoolKey]runner.AgentPool, 5)
	for _, k := range AllValidPoolKeys() {
		if cfg.Pools != nil && cfg.Pools[k] != nil {
			pools[k] = cfg.Pools[k]
		}
	}
	highKey := PoolKey{Ephemeral: false, Effort: "high"}
	lowEphemKey := PoolKey{Ephemeral: true, Effort: "low"}
	medEphemKey := PoolKey{Ephemeral: true, Effort: "medium"}
	if pools[highKey] == nil && cfg.ProcessPool != nil {
		pools[highKey] = cfg.ProcessPool
	}
	if pools[lowEphemKey] == nil && cfg.LowEffortProcessPool != nil {
		pools[lowEphemKey] = cfg.LowEffortProcessPool
	}
	if pools[medEphemKey] == nil && cfg.MediumEffortProcessPool != nil {
		pools[medEphemKey] = cfg.MediumEffortProcessPool
	}
	if pools[highKey] == nil && isExplicitAppCfg {
		pools[highKey] = buildDefaultHighEffortPool(appCfg, cfg, sessMgr, store)
	}
	wirePoolSessionDependencies(pools, sessMgr, store)
	return pools
}

func buildDefaultHighEffortPool(appCfg *config.Config, cfg WorkerPoolConfig, sessMgr *session.Manager, store db.Store) runner.AgentPool {
	cur := appCfg.Current()
	daemonBin, daemonDataDir, defaultModel := cfg.AgyBin, "", cfg.Model
	if cur != nil {
		if daemonBin == "" {
			daemonBin = cur.AgyBin
		}
		daemonDataDir = cur.DataDir
		if defaultModel == "" {
			defaultModel = cur.Model
		}
	}
	poolCfg := runner.PoolConfig{AgyBin: daemonBin, Cwd: daemonDataDir, Env: os.Environ(), DefaultModel: defaultModel}
	if sessMgr != nil {
		poolCfg.TranscriptRescuer = sessMgr.ExtractResponseSince
		poolCfg.SessionManager = sessMgr
	}
	if store != nil {
		poolCfg.GetSessionRecord, poolCfg.OnSessionRotated = makeSessionCallbacks(store)
	}
	return runner.NewUnifiedProcessPool(poolCfg, nil)
}

// resolveDefaultAppConfig resolves appCfg or creates a default configuration from WorkerPoolConfig overrides.
func resolveDefaultAppConfig(appCfg *config.Config, cfg WorkerPoolConfig) (*config.Config, bool) {
	if appCfg != nil {
		return appCfg, true
	}
	def := config.DefaultConfigData()
	if cfg.Model != "" {
		def.Model = cfg.Model
	}
	if cfg.LowEffortModel != "" {
		def.LowEffortModel = cfg.LowEffortModel
	}
	if cfg.MediumEffortModel != "" {
		def.MediumEffortModel = cfg.MediumEffortModel
	}
	if cfg.SystemPrompt != "" {
		def.SystemPrompt = cfg.SystemPrompt
	}
	return config.NewFromData(def), false
}

// applyWorkerPoolTimingDefaults applies default timeouts, retry limits, and intervals to WorkerPoolConfig.
func applyWorkerPoolTimingDefaults(cfg *WorkerPoolConfig) {
	if cfg.TimeoutMinutes <= 0 {
		cfg.TimeoutMinutes = DefaultTimeoutMinutes
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 3 * time.Second
	}
}

// applyWorkerPoolHookDefaults applies default test hooks and delivery/notification functions to WorkerPoolConfig.
func applyWorkerPoolHookDefaults(cfg *WorkerPoolConfig) {
	hasCustomRunner := cfg.RunnerFunc != nil || cfg.RunnerWithOptionsFunc != nil
	if hasCustomRunner && testPoolHook != nil {
		if wrapped := testPoolHook(*cfg); wrapped != nil {
			cfg.ProcessPool = wrapped
		}
	}
	if cfg.NotifierFunc == nil {
		cfg.NotifierFunc = func(agyBin, apiKey, desc string) string { return notifier.StaticFallback(desc) }
	}
	hasCustomDelivery := cfg.DeliveryFunc != nil || cfg.DeliveryWithAttachmentsFunc != nil
	if cfg.DeliveryWithAttachmentsFunc == nil {
		if cfg.DeliveryFunc != nil {
			deliveryFn := cfg.DeliveryFunc
			cfg.DeliveryWithAttachmentsFunc = func(s *discordgo.Session, cID, txt string, atts []*delivery.Attachment) error {
				return deliveryFn(s, cID, txt)
			}
		} else {
			cfg.DeliveryWithAttachmentsFunc = func(s *discordgo.Session, cID, txt string, atts []*delivery.Attachment) error {
				return delivery.SendMessageWithAttachments(s, cID, txt, atts)
			}
		}
	}
	if cfg.ReplyDeliveryWithAttachmentsFunc == nil && !hasCustomDelivery {
		cfg.ReplyDeliveryWithAttachmentsFunc = func(s *discordgo.Session, cID, txt string, atts []*delivery.Attachment, replyToID string) error {
			return delivery.SendMessageWithAttachments(s, cID, txt, atts, replyToID)
		}
	}
	if cfg.DeliveryFunc == nil {
		cfg.DeliveryFunc = delivery.SendMessage
	}
	if cfg.TypingFunc == nil {
		cfg.TypingFunc = delivery.StartTyping
	}
}

// applyWorkerPoolServiceDefaults applies default service dependencies to WorkerPoolConfig.
func applyWorkerPoolServiceDefaults(appCfg *config.Config, cfg *WorkerPoolConfig) {
	if cfg.SessionManager == nil {
		cfg.SessionManager = session.New(appCfg.GeminiHomeDir(), appCfg.DataDir())
	}
	if cfg.Classifier == nil {
		cfg.Classifier = classifier.New(nil)
	}
	if cfg.ResolveChannelPolicy == nil {
		cfg.ResolveChannelPolicy = func(cID, cName string) config.ChannelPolicy {
			return appCfg.ResolveChannelPolicy(cID, cName)
		}
	}
	if cfg.SystemAlertFunc == nil {
		cfg.SystemAlertFunc = delivery.SendSystemAlert
	}
	if cfg.WebhookDispatcher == nil {
		cfg.WebhookDispatcher = NewDefaultWebhookDispatcher()
	}
}

// resolveDefaultSummarizerLLM resolves an EphemeralLLMFunc for summarization from configured agent pools.
func resolveDefaultSummarizerLLM(pools map[PoolKey]runner.AgentPool) LLMFunc {
	for _, candidate := range []runner.AgentPool{
		pools[PoolKey{Ephemeral: true, Effort: "low"}],
		pools[PoolKey{Ephemeral: false, Effort: "low"}],
		pools[PoolKey{Ephemeral: false, Effort: "high"}],
	} {
		if candidate == nil {
			continue
		}
		if ep, ok := candidate.(interface {
			EphemeralLLMFunc(model string) runner.LLMFunc
		}); ok {
			return ep.EphemeralLLMFunc("ephemeral:summarizer")
		}
	}
	return nil
}

// formatClassifierParseAlert formats a diagnostic alert markdown string for classifier JSON parse errors.
func formatClassifierParseAlert(model, raw string, parseErr error) string {
	snippet := raw
	if runes := []rune(snippet); len(runes) > 600 {
		snippet = string(runes[:590]) + "\n... (truncated)"
	}
	snippet = strings.ReplaceAll(snippet, "```", "'''")
	snippet = strings.ReplaceAll(snippet, "@everyone", "@\u200beveryone")
	snippet = strings.ReplaceAll(snippet, "@here", "@\u200bhere")
	return fmt.Sprintf("**Model**: `%s`\n**Parse Error**:\n```\n%v\n```\n**Raw Output**:\n```\n%s\n```", model, parseErr, snippet)
}

// registerClassifierAlertHandlers sets up alert reporting hooks on the configured classifier.
func (p *WorkerPool) registerClassifierAlertHandlers() {
	if p.cfg.Classifier == nil {
		return
	}
	var lastAlertMu sync.Mutex
	var lastClassifierAlertTime, lastSystemAlertTime time.Time

	if p.cfg.Classifier.OnParseError == nil {
		p.cfg.Classifier.OnParseError = func(model, raw string, parseErr error) {
			p.makeDebouncedAlertHandler(&lastAlertMu, &lastClassifierAlertTime, "OnParseError", "Classifier JSON Parse Error", fmt.Sprintf("classifier JSON parse alert: %v", parseErr), func() string {
				return formatClassifierParseAlert(model, raw, parseErr)
			})
		}
	}

	if p.cfg.Classifier.OnSystemAlert == nil {
		p.cfg.Classifier.OnSystemAlert = func(endpoint string, endpointErr error) {
			p.makeDebouncedAlertHandler(&lastAlertMu, &lastSystemAlertTime, "OnSystemAlert", "Classifier Endpoint Failure", fmt.Sprintf("classifier endpoint alert: %v", endpointErr), func() string {
				return fmt.Sprintf("**Endpoint**: `%s`\n**Status**: Failed after 3 retry attempts with exponential backoff (failing closed to strict mention-only mode).\n**Error Trace**:\n```\n%v\n```", endpoint, endpointErr)
			})
		}
	}
}

// New creates a new WorkerPool with pure *config.Config dependency injection.
func New(appCfg *config.Config, cfg WorkerPoolConfig) *WorkerPool {
	resolvedAppCfg, isExplicitAppCfg := resolveDefaultAppConfig(appCfg, cfg)
	applyWorkerPoolTimingDefaults(&cfg)
	applyWorkerPoolHookDefaults(&cfg)
	cfg.ActiveTurns = activeTurnRegistryOrNew(cfg.ActiveTurns)
	applyWorkerPoolServiceDefaults(resolvedAppCfg, &cfg)

	pools := initWorkerPools(resolvedAppCfg, isExplicitAppCfg, cfg, cfg.SessionManager, cfg.Store)
	if cfg.LLMFunc == nil {
		cfg.LLMFunc = resolveDefaultSummarizerLLM(pools)
	}

	parentCtx := cfg.ParentContext
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(parentCtx)
	p := &WorkerPool{
		appCfg: resolvedAppCfg, hasExplicitAppCfg: isExplicitAppCfg, cfg: cfg, pools: pools, voiceProcessPool: cfg.VoiceProcessPool,
		sessionMgr: cfg.SessionManager, threadChs: make(map[string]*threadWorkerState),
		activeTurns: make(map[string]*turnExecution), ctx: ctx, cancel: cancel, startedAt: time.Now().UTC(),
		webhookDispatcher: cfg.WebhookDispatcher, createThreadFn: cfg.CreateThreadFunc,
		widgetsByID:      make(map[string]discord.StatusWidgetRequest),
		chunkToBuildID:   make(map[string]string),
		activeChunkSteps: make(map[string]string),
		activeChunkTurns: make(map[string]string),
	}

	if p.cfg.HistoryFetcher == nil {
		p.cfg.HistoryFetcher = func(ctx context.Context, cID, beforeID string, limit int) ([]HistoryMessage, error) {
			return DefaultHistoryFetcher(p.getDiscordSession())(ctx, cID, beforeID, limit)
		}
	}

	p.registerClassifierAlertHandlers()

	return p
}

func (p *WorkerPool) makeDebouncedAlertHandler(mu *sync.Mutex, lastAlert *time.Time, handlerName, title, logDesc string, buildMsg func() string) {
	mu.Lock()
	if !lastAlert.IsZero() && time.Since(*lastAlert) < 15*time.Second {
		mu.Unlock()
		log.Printf("[WorkerPool] Debouncing rapid %s to prevent system channel flooding", logDesc)
		return
	}
	*lastAlert = time.Now()
	mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[WorkerPool] Panic in classifier %s alert handler: %v", handlerName, r)
			}
		}()
		sess := p.getDiscordSession()
		if sess == nil {
			return
		}
		sysChan := p.systemChannel()
		sanitizedAlert := sanitizeErrorText(buildMsg())
		if err := p.cfg.SystemAlertFunc(sess, sysChan, title, sanitizedAlert); err != nil {
			log.Printf("[WorkerPool] Warning: failed to send %s alert to system channel %q: %v", title, sysChan, err)
		}
	}()
}

func (p *WorkerPool) systemChannel() string {
	if p != nil && p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil && cur.SystemChannel != "" {
			return cur.SystemChannel
		}
	}
	return config.DefaultConfigData().SystemChannel
}

func (p *WorkerPool) SetDiscordSession(s *discordgo.Session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfg.DiscordSession = s
}

func (p *WorkerPool) Classifier() *classifier.Classifier {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg.Classifier
}

func (p *WorkerPool) getDiscordSession() *discordgo.Session {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg.DiscordSession
}

func (p *WorkerPool) DiscordSession() *discordgo.Session { return p.getDiscordSession() }

// AppConfig returns the worker pool's application configuration.
func (p *WorkerPool) AppConfig() *config.Config {
	if p == nil {
		return nil
	}
	return p.appCfg
}

// ResolveChannelID resolves a channel name (e.g. "prospera" or "#prospera") or numeric snowflake ID to a channel ID.
func (p *WorkerPool) ResolveChannelID(nameOrID string) (string, error) {
	trimmed := strings.TrimSpace(nameOrID)
	trimmed = strings.TrimPrefix(trimmed, "#")
	if IsNumericSnowflake(trimmed) {
		return trimmed, nil
	}
	if p == nil {
		return "", errors.New("worker pool is nil")
	}
	return delivery.ResolveChannelByNameOrID(p.DiscordSession(), nameOrID)
}

func (p *WorkerPool) DeliverDirect(channelID, text string) error {
	if p == nil {
		return errors.New("worker pool is nil")
	}
	sess := p.DiscordSession()
	if sess == nil && p.cfg.DeliveryFunc == nil {
		return errors.New("discord session is not connected")
	}
	if p.cfg.DeliveryFunc != nil {
		return p.cfg.DeliveryFunc(sess, channelID, text)
	}
	return delivery.SendMessage(sess, channelID, text)
}

// DeliverDirectWithAttachments delivers text and memory-safe attachments directly to a channel or thread.
func (p *WorkerPool) DeliverDirectWithAttachments(channelID, text string, attachments []*delivery.Attachment) error {
	if len(attachments) == 0 {
		return p.DeliverDirect(channelID, text)
	}
	if p == nil {
		return errors.New("worker pool is nil")
	}
	if p.cfg.DeliveryWithAttachmentsFunc != nil {
		return p.cfg.DeliveryWithAttachmentsFunc(p.DiscordSession(), channelID, text, attachments)
	}
	if p.DiscordSession() == nil {
		return errors.New("discord session is not connected")
	}
	return delivery.SendMessageWithAttachments(p.DiscordSession(), channelID, text, attachments)
}

func (p *WorkerPool) resolveChannelPolicy(channelID, channelName string) config.ChannelPolicy {
	if p.cfg.ResolveChannelPolicy != nil {
		return p.cfg.ResolveChannelPolicy(channelID, channelName)
	}
	if p.appCfg != nil {
		return p.appCfg.ResolveChannelPolicy(channelID, channelName)
	}
	return config.ActiveConfig().ResolveChannelPolicy(channelID, channelName)
}

// WebhookDispatcher returns the assigned webhook dispatcher.
func (p *WorkerPool) WebhookDispatcher() WebhookDispatcher {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.webhookDispatcher
}

func (p *WorkerPool) GetRuntimeConfig() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.overrideModel != "" {
		return p.overrideModel
	}
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil && cur.Model != "" {
			return cur.Model
		}
	}
	return p.cfg.Model
}

func (p *WorkerPool) Start() {
	// WorkerPool starts workers lazily per active thread
	log.Printf("[WorkerPool] Started queue worker pool with max %d attempts per turn", p.cfg.MaxAttempts)
	if p.AgentPool() != nil || p.cfg.Store != nil {
		p.wg.Go(func() {
			interval := p.cfg.MaintenanceInterval
			if interval <= 0 {
				interval = 30 * time.Minute
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			lastReportedTurnCount := make(map[string]int)
			for {
				select {
				case <-p.ctx.Done():
					return
				case <-ticker.C:
					p.checkUnrotatedSessions(p.ctx, lastReportedTurnCount)
				}
			}
		})
	}
}

// checkUnrotatedSessions scans for active sessions exceeding the turn limit without rotating and alerts the system channel.
func (p *WorkerPool) checkUnrotatedSessions(ctx context.Context, lastReportedTurnCount map[string]int) {
	if p == nil || p.cfg.Store == nil {
		return
	}
	parentCtx := context.Background()
	if ctx != nil {
		parentCtx = ctx
	} else if p.ctx != nil {
		parentCtx = p.ctx
	}
	checkCtx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()

	sessions, err := p.cfg.Store.FindUnrotatedSessions(checkCtx, session.DefaultMaxSessionTurns)
	if err != nil {
		log.Printf("[WorkerPool] Warning: failed to query unrotated sessions: %v", err)
		return
	}

	activeUnrotated := make(map[string]struct{}, len(sessions))
	var runawayList []db.SessionInfo
	for _, s := range sessions {
		activeUnrotated[s.ThreadID] = struct{}{}
		if s.TurnCount > lastReportedTurnCount[s.ThreadID] {
			runawayList = append(runawayList, s)
		}
	}
	for threadID := range lastReportedTurnCount {
		if _, exists := activeUnrotated[threadID]; !exists {
			delete(lastReportedTurnCount, threadID)
		}
	}
	if len(runawayList) == 0 {
		return
	}

	sess := p.getDiscordSession()
	var lines []string
	for i, s := range runawayList {
		if i >= 5 {
			lines = append(lines, fmt.Sprintf("- *...and %d more runaway session(s)*", len(runawayList)-5))
			break
		}
		lines = append(lines, formatUnrotatedSessionLine(sess, s))
	}
	for _, s := range runawayList {
		lastReportedTurnCount[s.ThreadID] = s.TurnCount
	}

	sysChan := p.systemChannel()
	alertMsg := fmt.Sprintf("⚠️ **Unrotated Runaway Sessions Detected**\n\nThe following sessions have exceeded the turn limit (%d turns) without rotating:\n%s\n\n*Purely informational watchdog alert. Sessions may require investigation if turn count continues increasing.*",
		session.DefaultMaxSessionTurns, strings.Join(lines, "\n"))
	if p.cfg.SystemAlertFunc != nil {
		if alertErr := p.cfg.SystemAlertFunc(sess, sysChan, "Unrotated Runaway Sessions", sanitizeErrorText(alertMsg)); alertErr != nil {
			log.Printf("[WorkerPool] Warning sending unrotated sessions alert: %v", alertErr)
		}
	}
}

// formatUnrotatedSessionLine formats an unrotated session entry for system alerts.
func formatUnrotatedSessionLine(sess *discordgo.Session, s db.SessionInfo) string {
	rawThreadID := strings.TrimPrefix(s.ThreadID, "discord-")
	if !IsNumericSnowflake(rawThreadID) {
		return fmt.Sprintf("- Thread `%s`: **%d** turns (session: `%s`)", s.ThreadID, s.TurnCount, s.InternalSessionID)
	}
	effID, _, isThread, _ := ResolveChannelAndThread(sess, rawThreadID)
	if isThread {
		if effID != "" && effID != rawThreadID {
			return fmt.Sprintf("- Thread <#%s> (in <#%s>): **%d** turns (session: `%s`)", rawThreadID, effID, s.TurnCount, s.InternalSessionID)
		}
		return fmt.Sprintf("- Thread <#%s>: **%d** turns (session: `%s`)", rawThreadID, s.TurnCount, s.InternalSessionID)
	}
	return fmt.Sprintf("- Channel <#%s>: **%d** turns (session: `%s`)", rawThreadID, s.TurnCount, s.InternalSessionID)
}

// HoldUntilLeader gates worker spawning on EnqueueBurst and EnqueueDispatch
// until ReleaseLeaderHold is called after acquiring the leader lock.
func (p *WorkerPool) HoldUntilLeader() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.leaderHold = true
	p.mu.Unlock()
}

// ReleaseLeaderHold clears the canary leader hold and returns true if the pool was held.
func (p *WorkerPool) ReleaseLeaderHold() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	wasHeld := p.leaderHold
	p.leaderHold = false
	p.mu.Unlock()
	return wasHeld
}

func unwrapUnifiedPool(pool runner.AgentPool) *runner.UnifiedProcessPool {
	if pool == nil {
		return nil
	}
	if up, ok := pool.(*runner.UnifiedProcessPool); ok {
		return up
	}
	if uw, ok := pool.(interface {
		Underlying() *runner.UnifiedProcessPool
	}); ok {
		return uw.Underlying()
	}
	return nil
}

func (p *WorkerPool) collectUniquePoolsLocked() []runner.AgentPool {
	var out []runner.AgentPool
	add := func(ap runner.AgentPool) {
		if ap == nil {
			return
		}
		if slices.Contains(out, ap) {
			return
		}
		out = append(out, ap)
	}
	for _, k := range AllValidPoolKeys() {
		if p.pools != nil {
			add(p.pools[k])
		}
	}
	add(p.voiceProcessPool)
	return out
}

func (p *WorkerPool) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()

	p.cancel()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("[WorkerPool] Workers exited cleanly")
	case <-time.After(2 * time.Second):
		log.Printf("[WorkerPool] Warning: worker exit wait timed out")
	}

	p.mu.Lock()
	pools := p.collectUniquePoolsLocked()
	p.mu.Unlock()
	for _, pool := range pools {
		if closeErr := pool.Close(); closeErr != nil {
			log.Printf("[WorkerPool] Warning: failed to close process pool: %v", closeErr)
		}
	}
	log.Printf("[WorkerPool] Queue worker pool stopped cleanly")
}

// Pool returns the configured runner.AgentPool for the given PoolKey, or nil.
func (p *WorkerPool) Pool(k PoolKey) runner.AgentPool {
	if p == nil || p.pools == nil {
		return nil
	}
	return p.pools[k]
}

// StatefulProcessPools returns non-nil unwrapped *runner.UnifiedProcessPool instances for stateful pool keys.
func (p *WorkerPool) StatefulProcessPools() []*runner.UnifiedProcessPool {
	if p == nil {
		return nil
	}
	var out []*runner.UnifiedProcessPool
	for _, k := range []PoolKey{
		{Ephemeral: false, Effort: "high"},
		{Ephemeral: false, Effort: "medium"},
		{Ephemeral: false, Effort: "low"},
	} {
		if up := unwrapUnifiedPool(p.Pool(k)); up != nil {
			out = append(out, up)
		}
	}
	return out
}

// ProcessPool returns the configured runner.UnifiedProcessPool, or nil if unconfigured or p is nil.
func (p *WorkerPool) ProcessPool() *runner.UnifiedProcessPool {
	return unwrapUnifiedPool(p.AgentPool())
}

// LowEffortProcessPool returns the configured runner.UnifiedProcessPool for low effort turns.
func (p *WorkerPool) LowEffortProcessPool() *runner.UnifiedProcessPool {
	if up := unwrapUnifiedPool(p.Pool(PoolKey{Ephemeral: false, Effort: "low"})); up != nil {
		return up
	}
	return unwrapUnifiedPool(p.Pool(PoolKey{Ephemeral: true, Effort: "low"}))
}

// MediumEffortProcessPool returns the configured runner.UnifiedProcessPool for medium effort turns.
func (p *WorkerPool) MediumEffortProcessPool() *runner.UnifiedProcessPool {
	if up := unwrapUnifiedPool(p.Pool(PoolKey{Ephemeral: false, Effort: "medium"})); up != nil {
		return up
	}
	return unwrapUnifiedPool(p.Pool(PoolKey{Ephemeral: true, Effort: "medium"}))
}

// AgentPool returns the configured primary runner.AgentPool.
func (p *WorkerPool) AgentPool() runner.AgentPool {
	return p.Pool(PoolKey{Ephemeral: false, Effort: "high"})
}

func (p *WorkerPool) registerActiveTurn(scopeKey string, te *turnExecution) {
	p.mu.Lock()
	if p.activeTurns == nil {
		p.activeTurns = make(map[string]*turnExecution)
	}
	p.activeTurns[scopeKey] = te
	p.mu.Unlock()
}

func (p *WorkerPool) unregisterActiveTurn(scopeKey string, te *turnExecution) {
	p.mu.Lock()
	if p.activeTurns[scopeKey] == te {
		delete(p.activeTurns, scopeKey)
	}
	p.mu.Unlock()
}

// ActiveTurnSessionID returns the live session ID for an in-flight message ID if the message
// belongs to an active turn in the worker pool.
func (p *WorkerPool) ActiveTurnSessionID(msgID string) (string, bool) {
	if p == nil || msgID == "" {
		return "", false
	}
	p.mu.Lock()
	turns := make([]*turnExecution, 0, len(p.activeTurns))
	for _, te := range p.activeTurns {
		if te != nil {
			turns = append(turns, te)
		}
	}
	p.mu.Unlock()
	for _, te := range turns {
		if sessID, ok := te.liveSessionIDForMessage(msgID); ok {
			return sessID, true
		}
	}
	return "", false
}

func isScopeKeyForThread(scopeKey, threadID string) bool {
	norm := db.DiscordSessionID(threadID)
	return scopeKey == threadID || scopeKey == norm || strings.HasSuffix(scopeKey, ":"+threadID) || strings.HasSuffix(scopeKey, ":"+norm)
}

func (p *WorkerPool) hasNewerEnqueuedDiscordMessage(threadID, triggerID string) bool {
	if p == nil || threadID == "" || !IsNumericSnowflake(triggerID) {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for scopeKey, state := range p.threadChs {
		if state != nil && isScopeKeyForThread(scopeKey, threadID) && compareSnowflakeIDs(state.lastEnqueuedDiscordMsgID, triggerID) > 0 {
			return true
		}
	}
	return false
}

func logInterruptWarning(op string, err error) {
	if err != nil {
		log.Printf("[WorkerPool] Warning on %s: %v", op, err)
	}
}

// CloseThreadProcesses marks any active turn for threadID as interrupted, cancels its per-turn context,
// and terminates any active StreamingDaemon processes for threadID across all pools.
func (p *WorkerPool) CloseThreadProcesses(threadID, reason string) {
	if p == nil || threadID == "" {
		return
	}
	p.mu.Lock()
	var active []*turnExecution
	for scopeKey, te := range p.activeTurns {
		if te != nil && (te.threadID == threadID || isScopeKeyForThread(scopeKey, threadID)) {
			te.interrupted.Store(true)
			active = append(active, te)
		}
	}
	p.mu.Unlock()

	for _, te := range active {
		te.cancelTurn()
	}

	for _, target := range append(p.StatefulProcessPools(), p.ProcessPool(), p.LowEffortProcessPool(), p.MediumEffortProcessPool()) {
		if target != nil {
			if d, ok := target.Get(db.DiscordSessionID(threadID)); ok && d != nil {
				logInterruptWarning(reason, d.Close())
			}
		}
	}
}

// DrainThreadPendingMessages marks all pending or processing DB messages for targetThreadID as FAILED.
func DrainThreadPendingMessages(ctx context.Context, store db.Store, targetThreadID, reason string) {
	if store == nil || targetThreadID == "" {
		return
	}
	msgs, err := store.GetPendingOrProcessingMessages(ctx, 100)
	if err != nil {
		return
	}
	for _, m := range msgs {
		if m.ThreadID == targetThreadID {
			logInterruptWarning("drain pending message", store.UpdateMessageStatus(ctx, m.ID, db.StatusFailed, reason))
		}
	}
}

func matchesDispatchTarget(d *db.Dispatch, dispatchID, buildID string) bool {
	if d == nil {
		return false
	}
	if dispatchID != "" && d.ID == dispatchID {
		return true
	}
	if buildID != "" && (strings.Contains(d.Prompt, "Build ID: "+buildID+"\n") ||
		strings.HasPrefix(d.SessionID, "vanadis-"+buildID+"-chunk-") ||
		strings.Contains(d.Prompt, "Chunk ID: "+buildID+"-chunk-")) {
		return true
	}
	return false
}

type activeDispatchCancelTarget struct {
	te       *turnExecution
	threadID string
}

func drainMatchingDispatchesFromChannel(ch chan queueItem, dID, bID string) []string {
	var drainedIDs []string
	var nonMatching []queueItem
drainLoop:
	for {
		select {
		case item := <-ch:
			if item.dispatch != nil && matchesDispatchTarget(item.dispatch, dID, bID) {
				metrics.QueueDepth.Dec()
				drainedIDs = append(drainedIDs, item.dispatch.ID)
			} else {
				nonMatching = append(nonMatching, item)
			}
		default:
			break drainLoop
		}
	}
	for _, item := range nonMatching {
		ch <- item
	}
	return drainedIDs
}

func (p *WorkerPool) interruptMatchingActiveDispatchesLocked(dID, bID, reason string) ([]string, []activeDispatchCancelTarget) {
	var cancelledIDs []string
	var activeToCancel []activeDispatchCancelTarget
	for _, te := range p.activeTurns {
		if te != nil && te.isDispatch() && matchesDispatchTarget(te.dispatch, dID, bID) {
			te.interrupted.Store(true)
			te.setInterruptReason(reason)
			if te.dispatch != nil && !slices.Contains(cancelledIDs, te.dispatch.ID) {
				cancelledIDs = append(cancelledIDs, te.dispatch.ID)
			}
			activeToCancel = append(activeToCancel, activeDispatchCancelTarget{te: te, threadID: te.threadID})
		}
	}
	return cancelledIDs, activeToCancel
}

// CancelDispatches drains queued dispatches and interrupts active in-flight dispatch turns
// matching either dispatchID or buildID.
func (p *WorkerPool) CancelDispatches(dispatchID, buildID, reason string) []string {
	if p == nil || (strings.TrimSpace(dispatchID) == "" && strings.TrimSpace(buildID) == "") {
		return nil
	}
	dID := strings.TrimSpace(dispatchID)
	bID := strings.TrimSpace(buildID)
	if strings.TrimSpace(reason) == "" {
		reason = "dispatch cancelled by operator"
	}

	var cancelledIDs []string

	p.mu.Lock()
	for _, state := range p.threadChs {
		if state != nil && state.ch != nil {
			for _, id := range drainMatchingDispatchesFromChannel(state.ch, dID, bID) {
				if !slices.Contains(cancelledIDs, id) {
					cancelledIDs = append(cancelledIDs, id)
				}
			}
		}
	}

	activeIDs, activeTargets := p.interruptMatchingActiveDispatchesLocked(dID, bID, reason)
	for _, id := range activeIDs {
		if !slices.Contains(cancelledIDs, id) {
			cancelledIDs = append(cancelledIDs, id)
		}
	}
	p.mu.Unlock()

	for _, target := range activeTargets {
		target.te.cancelTurn()
		if target.threadID != "" {
			p.CloseThreadProcesses(target.threadID, reason)
		}
	}

	return cancelledIDs
}

func (p *WorkerPool) latchActiveTurnsLocked(threadID string) ([]*turnExecution, []string) {
	var active []*turnExecution
	var burstMsgIDs []string

	for scopeKey, te := range p.activeTurns {
		if te == nil || (te.threadID != threadID && !isScopeKeyForThread(scopeKey, threadID)) {
			continue
		}
		te.interrupted.Store(true)
		active = append(active, te)

		te.activeSessionMu.Lock()
		for i := range te.burst {
			burstMsgIDs = append(burstMsgIDs, te.burst[i].ID)
		}
		te.activeSessionMu.Unlock()
	}
	return active, burstMsgIDs
}

func drainChannelMessages(ch chan queueItem) []db.Message {
	var drainedMsgs []db.Message
	var nonMsgItems []queueItem
drainCh:
	for {
		select {
		case item := <-ch:
			if item.msg == nil {
				nonMsgItems = append(nonMsgItems, item)
				continue
			}
			metrics.QueueDepth.Dec()
			drainedMsgs = append(drainedMsgs, *item.msg)
		default:
			break drainCh
		}
	}
	for _, nmi := range nonMsgItems {
		ch <- nmi
	}
	return drainedMsgs
}

func (p *WorkerPool) drainQueuedThreadMessagesLocked(threadID string) []db.Message {
	var drained []db.Message
	for scopeKey, state := range p.threadChs {
		if !isScopeKeyForThread(scopeKey, threadID) || state == nil {
			continue
		}
		drained = append(drained, drainChannelMessages(state.ch)...)
	}
	return drained
}

func (p *WorkerPool) latchInterruptAndDrainLocked(threadID string, drainQueue bool) ([]*turnExecution, []string, []db.Message) {
	active, burstMsgIDs := p.latchActiveTurnsLocked(threadID)
	var drainedMsgs []db.Message
	if drainQueue {
		drainedMsgs = append(drainedMsgs, p.drainQueuedThreadMessagesLocked(threadID)...)
	}
	return active, burstMsgIDs, drainedMsgs
}

// InterruptThread interrupts an active turn for threadID in 4 ordered steps:
// 1. Latch in-memory interrupt gate on active turns and drain queued channel/trailing messages.
// 2. Commit terminal FAILED status (and optional session rotation) in Postgres.
// 3. Cancel per-turn context to wake any in-flight execution or retry/capacity sleep.
// 4. Terminate active StreamingDaemon processes for threadID.
func (p *WorkerPool) InterruptThread(ctx context.Context, threadID string, drainQueue bool, reset ...bool) {
	if p == nil || threadID == "" {
		return
	}
	shouldReset := len(reset) > 0 && reset[0]

	// Step 1: Latch In-Memory Interrupt Gate & Drain Channel Buffer under p.mu
	p.mu.Lock()
	active, burstMsgIDs, drainedMsgs := p.latchInterruptAndDrainLocked(threadID, drainQueue)
	p.mu.Unlock()

	// Step 2: Commit Terminal FAILED State & Optional Session Rotation in Postgres
	if store := p.Store(); store != nil {
		for _, id := range burstMsgIDs {
			logInterruptWarning("interrupt active message", store.UpdateMessageStatus(ctx, id, db.StatusFailed, InterruptedByOperatorReason))
		}
		for _, m := range drainedMsgs {
			logInterruptWarning("interrupt drained message", store.UpdateMessageStatus(ctx, m.ID, db.StatusFailed, InterruptedByOperatorReason))
			if p.cfg.OnMessageCompleted != nil {
				p.cfg.OnMessageCompleted(m, db.StatusFailed)
			}
		}
		if drainQueue {
			DrainThreadPendingMessages(ctx, store, threadID, InterruptedByOperatorReason)
		}
		if shouldReset {
			logInterruptWarning("interrupt rotate session", store.RotateSessionID(ctx, threadID, uuid.New().String()))
		}
	}

	// Step 3: Cancel Per-Turn Context
	for _, te := range active {
		te.cancelTurn()
	}

	// Step 4: Terminate Daemon Process Group
	p.CloseThreadProcesses(threadID, "interrupt")
}

func (p *WorkerPool) poolModelForEffort(effort string) string {
	switch effort {
	case "low":
		return p.LowEffortModel()
	case "medium":
		return p.MediumEffortModel()
	default:
		return p.GetRuntimeConfig()
	}
}

func updatePoolModel(pool runner.AgentPool, model string) {
	if modelUpdater, ok := pool.(interface{ UpdateModel(model string) }); ok {
		modelUpdater.UpdateModel(model)
	}
}

func (p *WorkerPool) reconcilePoolModelsAndVoiceTargets(poolByKey map[PoolKey]runner.AgentPool, voicePool runner.AgentPool, hasExplicitAppCfg bool, voiceTargets []string, voiceModel string) {
	if hasExplicitAppCfg {
		for _, k := range AllValidPoolKeys() {
			updatePoolModel(poolByKey[k], p.poolModelForEffort(k.Effort))
		}
	}
	if voicePool == nil {
		return
	}
	if targetUpdater, ok := voicePool.(interface{ UpdatePrewarmedTargets(targets []string) }); ok && voiceTargets != nil {
		targetUpdater.UpdatePrewarmedTargets(voiceTargets)
	}
	if hasExplicitAppCfg {
		if voiceModel == "" {
			voiceModel = p.LowEffortModel()
		}
		updatePoolModel(voicePool, voiceModel)
	}
}

// MarkDirty notifies the process pools to mark active daemons dirty and evict idle daemons.
func (p *WorkerPool) MarkDirty() {
	if p == nil {
		return
	}
	p.mu.Lock()
	pools := p.collectUniquePoolsLocked()
	voicePool := p.voiceProcessPool
	hasExplicitAppCfg := p.hasExplicitAppCfg
	poolByKey := maps.Clone(p.pools)
	var voiceTargets []string
	var voiceModel string
	if p.appCfg != nil {
		voiceTargets = p.appCfg.VoicePrewarmedTargets()
		if cur := p.appCfg.Current(); cur != nil {
			voiceModel = strings.TrimSpace(cur.Voice.Model)
		}
	}
	p.mu.Unlock()

	p.reconcilePoolModelsAndVoiceTargets(poolByKey, voicePool, hasExplicitAppCfg, voiceTargets, voiceModel)

	for _, pool := range pools {
		if md, ok := pool.(interface{ MarkDirty() }); ok {
			md.MarkDirty()
		}
	}
}

// LowEffortModel returns the configured low effort reasoning model name.
func (p *WorkerPool) LowEffortModel() string {
	def := config.DefaultConfigData().LowEffortModel
	if p == nil {
		return def
	}
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil && strings.TrimSpace(cur.LowEffortModel) != "" {
			return strings.TrimSpace(cur.LowEffortModel)
		}
	}
	if strings.TrimSpace(p.cfg.LowEffortModel) != "" {
		return strings.TrimSpace(p.cfg.LowEffortModel)
	}
	return def
}

// MediumEffortModel returns the configured medium effort reasoning model name.
func (p *WorkerPool) MediumEffortModel() string {
	def := config.DefaultConfigData().MediumEffortModel
	if p == nil {
		return def
	}
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil && strings.TrimSpace(cur.MediumEffortModel) != "" {
			return strings.TrimSpace(cur.MediumEffortModel)
		}
	}
	if strings.TrimSpace(p.cfg.MediumEffortModel) != "" {
		return strings.TrimSpace(p.cfg.MediumEffortModel)
	}
	return def
}

func (p *WorkerPool) runnerCredentials() (string, string) {
	agyBin, apiKey := p.cfg.AgyBin, p.cfg.APIKey
	if p == nil || p.appCfg == nil {
		return agyBin, apiKey
	}
	cur := p.appCfg.Current()
	if cur == nil {
		return agyBin, apiKey
	}
	if cur.AgyBin != "" {
		agyBin = cur.AgyBin
	}
	if cur.APIKey != "" {
		apiKey = cur.APIKey
	}
	return agyBin, apiKey
}

func (p *WorkerPool) checkVoiceTurnReady(ctx context.Context, sessionID string) (string, error) {
	if p == nil {
		return "", fmt.Errorf("worker pool is uninitialized")
	}
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return "", fmt.Errorf("brain is shutting down")
	}

	select {
	case <-p.ctx.Done():
		return "", fmt.Errorf("brain is shutting down")
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	deviceKey := strings.TrimSpace(sessionID)
	if deviceKey == "" {
		return "", fmt.Errorf("queue: session_id is required for voice turn routing")
	}
	return deviceKey, nil
}

func (p *WorkerPool) executeCustomVoiceRunner(ctx context.Context, turnPrompt, deviceKey string, onStatus func(string), sentenceCb func(string)) (string, string, bool, error) {
	hasCustomStream := p.cfg.VoiceStreamRunnerFunc != nil && sentenceCb != nil
	if !hasCustomStream && p.cfg.VoiceRunnerFunc == nil {
		return "", "", false, nil
	}
	start := time.Now()
	var reply, conv string
	var err error
	if hasCustomStream {
		reply, conv, err = p.cfg.VoiceStreamRunnerFunc(ctx, turnPrompt, deviceKey, onStatus, sentenceCb)
	} else {
		reply, conv, err = p.cfg.VoiceRunnerFunc(ctx, turnPrompt, deviceKey, onStatus)
	}
	runStatus := "success"
	if err != nil {
		runStatus = "error"
	}
	metrics.RecordRunnerExecution(runStatus, p.LowEffortModel(), "voice", time.Since(start))
	return reply, conv, true, err
}

func (p *WorkerPool) resolveVoiceAgentPool() runner.AgentPool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.voiceProcessPool != nil {
		return p.voiceProcessPool
	}
	if p.pools != nil {
		return p.pools[PoolKey{Ephemeral: false, Effort: "high"}]
	}
	return nil
}

func finalizeVoiceTurnOutcome(ctx context.Context, outcome runner.TurnOutcome, turnErr error, activeSession, model string, start time.Time, sink *runner.BufferingTurnSink, detector *SentenceDetector, sentenceCb func(string)) (string, string, error) {
	if outcome.IsSuccess {
		metrics.RecordRunnerExecution("success", model, "voice", time.Since(start))
		if outcome.Usage.TotalTokens > 0 {
			metrics.RecordTokens(model, "voice", outcome.Usage.InputTokens, outcome.Usage.OutputTokens, outcome.Usage.ThinkingTokens, outcome.Usage.CacheReadTokens, outcome.Usage.TotalTokens)
		}
		if !sink.EmittedAny() && sentenceCb != nil && detector != nil && outcome.Response != "" {
			emitSentence := func(s string) {
				sink.MarkEmitted()
				if sentenceCb != nil {
					sentenceCb(s)
				}
			}
			detector.Feed(outcome.Response, emitSentence)
			detector.Flush(emitSentence)
		}
		return outcome.Response, outcome.SessionID, nil
	}

	metrics.RecordRunnerExecution("error", model, "voice", time.Since(start))
	if turnErr != nil {
		return "", activeSession, turnErr
	}
	if ctx.Err() != nil {
		return "", activeSession, ctx.Err()
	}
	return "", activeSession, fmt.Errorf("%s", outcome.ErrorDetail)
}

// ExecuteVoiceTurn synchronously executes a turn for voice queries.
func (p *WorkerPool) ExecuteVoiceTurn(ctx context.Context, prompt, sessionID string, onStatus func(status string), onSentence ...func(sentence string)) (string, string, error) {
	deviceKey, err := p.checkVoiceTurnReady(ctx, sessionID)
	if err != nil {
		return "", "", err
	}
	threadID := "voice-" + deviceKey

	lockVal, _ := p.scopeLocks.LoadOrStore(threadID, &sync.Mutex{})
	scopeLock, ok := lockVal.(*sync.Mutex)
	if !ok {
		scopeLock = &sync.Mutex{}
		p.scopeLocks.Store(threadID, scopeLock)
	}
	scopeLock.Lock()
	defer scopeLock.Unlock()

	var sentenceCb func(string)
	if len(onSentence) > 0 {
		sentenceCb = onSentence[0]
	}

	turnPrompt := prompt
	if reply, conv, handled, customErr := p.executeCustomVoiceRunner(ctx, turnPrompt, deviceKey, onStatus, sentenceCb); handled {
		return reply, conv, customErr
	}

	procPool := p.resolveVoiceAgentPool()
	if procPool == nil {
		return "", deviceKey, fmt.Errorf("voice daemon pool unavailable")
	}

	model := p.LowEffortModel()
	start := time.Now()
	nodeInstructions := config.LoadScopeInstructions(deviceKey)

	var detector *SentenceDetector
	if sentenceCb != nil {
		detector = NewSentenceDetector()
	}
	sink := newVoiceTurnSink(ctx, onStatus, sentenceCb, detector)

	turnCtx := &runner.TurnContext{
		TurnID: uuid.New().String(), SessionID: deviceKey, Model: model,
		Prompt: turnPrompt, ScopeInstructions: nodeInstructions, Sink: sink, CreatedAt: time.Now(), Ctx: ctx,
	}

	lease, lErr := procPool.AcquireLease(ctx, deviceKey)
	if lErr != nil {
		metrics.RecordRunnerExecution("error", model, "voice", time.Since(start))
		return "", deviceKey, fmt.Errorf("failed to acquire voice daemon: %w", lErr)
	}
	defer func() {
		if relErr := lease.Release(); relErr != nil {
			log.Printf("[WorkerPool] Warning releasing voice lease for %s: %v", deviceKey, relErr)
		}
	}()
	activeSession := lease.SessionID()
	if activeSession == "" {
		activeSession = deviceKey
	}
	turnCtx.SessionID = activeSession
	turnRes, turnErr := lease.Execute(ctx, turnCtx)
	if turnErr != nil && !errors.Is(turnErr, context.Canceled) && !errors.Is(turnErr, context.DeadlineExceeded) {
		turnErr = fmt.Errorf("failed sending turn to voice daemon: %w", turnErr)
	}

	outcome := runner.NewTurnResolver().Resolve(ctx, turnRes, turnErr, activeSession)
	return finalizeVoiceTurnOutcome(ctx, outcome, turnErr, activeSession, model, start, sink, detector, sentenceCb)
}

func newVoiceTurnSink(ctx context.Context, onStatus func(string), onSentence func(string), detector *SentenceDetector) *runner.BufferingTurnSink {
	var sink *runner.BufferingTurnSink
	emitSentence := func(sentence string) {
		if sink != nil {
			sink.MarkEmitted()
		}
		if onSentence != nil {
			onSentence(sentence)
		}
	}
	sink = runner.NewBufferingTurnSink(runner.BufferingTurnSinkConfig{
		OnToolCall: func(toolName, commandName string) {
			if onStatus != nil && (toolName != "" || commandName != "") {
				onStatus(FormatToolStatus(toolName, commandName, 0))
			}
		},
		OnToolCompleted: func(toolName, mcpServer string, duration time.Duration, status string) {
			metrics.RecordToolExecution(toolName, mcpServer, status, duration)
		},
		OnSkillActivated: func(skillName, source string) { metrics.RecordSkillActivation(skillName, source) },
		OnTextDelta: func(delta string) {
			if (ctx == nil || ctx.Err() == nil) && onSentence != nil && detector != nil {
				detector.Feed(delta, emitSentence)
			}
		},
		OnComplete: func(res *runner.TurnResult) {
			if onSentence != nil && detector != nil {
				detector.Flush(emitSentence)
				if !sink.EmittedAny() && res != nil && strings.TrimSpace(res.Response) != "" {
					detector.Feed(res.Response, emitSentence)
					detector.Flush(emitSentence)
				}
			}
		},
	})
	return sink
}
