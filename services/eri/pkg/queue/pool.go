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
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/discordgo"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"

	"github.com/azylman/aerial/pkg/discordutil"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/notifier"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

const (
	DefaultMaxQuotaPauseDBBytes = session.DefaultMaxQuotaPauseDBBytes
	DefaultMaxSessionIdleTime   = 24 * time.Hour
	DefaultMaxRestarts          = 3
	ContinuationPromptTemplate  = "Your previous execution timed out or was interrupted while working. Please inspect where you left off in the conversation transcript and continue the task to completion.\n\nOriginal user request:\n%s"
	MaxYieldTrapAutoResumes     = 2
	YieldTrapResumePrompt       = "[SYSTEM NOTICE]: Your previous turn yielded prematurely while background task(s) were still running, causing the runtime to terminate them. Inspect current status (check git status, PR state, or process logs) and continue your workflow to completion. Do NOT end the turn with waiting text."
	InterruptedByOperatorReason = "interrupted by operator via slash command"
)

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
		{Ephemeral: false, Effort: config.EffortHigh},
		{Ephemeral: false, Effort: config.EffortMedium},
		{Ephemeral: false, Effort: config.EffortLow},
		{Ephemeral: true, Effort: config.EffortMedium},
		{Ephemeral: true, Effort: config.EffortLow},
	}
}

// ResolvePoolKey normalizes effort and resolves the target PoolKey, rejecting (ephemeral: true, effort: "high").
func ResolvePoolKey(ephemeral *bool, effort string) (PoolKey, error) {
	eff := strings.ToLower(strings.TrimSpace(effort))
	if eff != config.EffortLow && eff != config.EffortMedium {
		eff = config.EffortHigh
	}
	isEphem := eff == config.EffortLow || eff == config.EffortMedium
	if ephemeral != nil {
		isEphem = *ephemeral
	}
	if isEphem && eff == config.EffortHigh {
		return PoolKey{Ephemeral: true, Effort: config.EffortHigh}, errors.New("ephemeral: true cannot be combined with high effort")
	}
	return PoolKey{Ephemeral: isEphem, Effort: eff}, nil
}

type WorkerPoolConfig struct {
	ParentContext                                                                context.Context
	Store                                                                        db.Store
	DiscordSession                                                               *discordgo.Session
	AgyBin, APIKey, Model, LowEffortModel, MediumEffortModel, SystemPrompt       string
	MaxAttempts                                                                  int
	BackoffBase, IdleTimeout, RetryDelayOverride                                 time.Duration
	SessionManager                                                               *session.Manager
	Pools                                                                        map[PoolKey]runner.AgentPool
	ProcessPool, LowEffortProcessPool, MediumEffortProcessPool, VoiceProcessPool runner.AgentPool
	Classifier                                                                   *classifier.Classifier
	NATSURL                                                                      string
	EventBus                                                                     pubsub.Bus

	// Optional hooks for testing/custom overrides
	RunnerFunc                       func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
	RunnerWithOptionsFunc            func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (stdout, stderr string, exitCode int, err error)
	NotifierFunc                     func(agyBin, apiKey, contextDescription string) string
	DeliveryFunc                     func(s *discordgo.Session, channelID, text string) error
	DeliveryWithAttachmentsFunc      func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment) error
	ReplyDeliveryWithAttachmentsFunc func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment, replyToMessageID string) error
	RestateIngressURL                string
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
	TraceContent                     bool                       // record prompt/response on invoke_agent spans (tracing capture_content)
	ActiveTurns                      *runner.ActiveTurnRegistry // daemons' active turn spans for the MCP trace proxy; nil gets a private registry
	TranscriptStore                  db.TranscriptStore
	TranscriptsIndexerURL            string
	TranscriptRecheckDelay           time.Duration
	ChunkStepFlushInterval           time.Duration
	VanadisURL                       string
	VanadisHTTPClient                *http.Client
	SATokenPath                      string
}

type WorkerPool struct {
	appCfg            *config.Config
	hasExplicitAppCfg bool
	overrideModel     string
	cfg               WorkerPoolConfig
	sessionMgr        *session.Manager
	mu                sync.Mutex
	turns             *TurnRegistry // Restate TurnRunner attempts live on this pod, by turn ID
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	stopped           bool
	quotaLockedUntil  atomic.Int64
	vanadisURLWarned  atomic.Bool // set once the unconfigured-vanadis_url skip has been logged
	scopeLocks        sync.Map
	webhookDispatcher WebhookDispatcher
	summaryGroup      singleflight.Group
	pools             map[PoolKey]runner.AgentPool
	voiceProcessPool  runner.AgentPool
	eventBus          pubsub.Bus
	createThreadFn    func(channelID, title string) (string, error)
	agentName         string

	TranscriptStore         db.TranscriptStore
	TranscriptsIndexerURL   string
	TranscriptRecheckDelay  time.Duration
	TranscriptPublisher     *transcript.Publisher
	TranscriptIndexerClient *transcript.IndexerClient
	transcriptPublishCh     chan TranscriptPublishJob
	transcriptPublishSem    chan struct{}
	transcriptPending       map[string]bool
	transcriptInFlight      *TranscriptPublishJob
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

// SetEventBus updates the event bus attached to the worker pool.
func (p *WorkerPool) SetEventBus(bus pubsub.Bus) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.eventBus = bus
}

// EventBus returns the event bus attached to the worker pool, if any.
func (p *WorkerPool) EventBus() pubsub.Bus {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.eventBus
}

// NATSURL returns the resolved NATS URL configured on the pool.
func (p *WorkerPool) NATSURL() string {
	if p == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil && cur.NATSURL != "" {
			return cur.NATSURL
		}
	}
	return p.cfg.NATSURL
}

func voiceSessionKey(targetKey string) string {
	trimmed := strings.TrimSpace(targetKey)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "voice-") {
		return trimmed
	}
	return "voice-" + trimmed
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
		return store.RotateSessionID(ctx, targetKey, newSessionID, oldSessionID)
	}
	return getRec, onRot
}

func makeVoiceSessionCallbacks(store db.Store) (func(context.Context, string) (runner.SessionRecord, error), func(context.Context, string, string, string) error) {
	getRec := func(ctx context.Context, targetKey string) (runner.SessionRecord, error) {
		key := voiceSessionKey(targetKey)
		sessID, err := store.GetSessionID(ctx, key)
		if err != nil {
			return runner.SessionRecord{}, err
		}
		turns, err := store.GetSessionTurnCount(ctx, key)
		if err != nil {
			turns = 0
		}
		prevID, err := store.GetPreviousSessionID(ctx, key)
		if err != nil {
			prevID = ""
		}
		return runner.SessionRecord{ActiveSessionID: sessID, PreviousSessionID: prevID, TurnCount: turns}, nil
	}
	onRot := func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
		return store.RotateSessionID(ctx, voiceSessionKey(targetKey), newSessionID, oldSessionID)
	}
	return getRec, onRot
}

func wireVoicePoolSessionDependencies(voicePool runner.AgentPool, pools map[PoolKey]runner.AgentPool, sessionMgr *session.Manager, store db.Store) {
	if sessionMgr != nil {
		if tr, ok := voicePool.(interface {
			SetTranscriptRescuer(fn func(context.Context, string, time.Time) string)
		}); ok {
			tr.SetTranscriptRescuer(sessionMgr.ExtractResponseSinceContext)
		}
		if sm, ok := voicePool.(interface{ SetSessionManager(mgr *session.Manager) }); ok {
			sm.SetSessionManager(sessionMgr)
		}
	}
	inDiscordPools := false
	for _, p := range pools {
		if p == voicePool {
			inDiscordPools = true
			break
		}
	}
	if store != nil && !inDiscordPools {
		if sc, ok := voicePool.(interface {
			SetSessionCallbacks(getFn func(context.Context, string) (runner.SessionRecord, error), saveFn func(context.Context, string, string, string) error)
		}); ok {
			getRec, onRot := makeVoiceSessionCallbacks(store)
			sc.SetSessionCallbacks(getRec, onRot)
		}
	}
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
				SetTranscriptRescuer(fn func(context.Context, string, time.Time) string)
			}); ok {
				tr.SetTranscriptRescuer(sessionMgr.ExtractResponseSinceContext)
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
	highKey := PoolKey{Ephemeral: false, Effort: config.EffortHigh}
	lowEphemKey := PoolKey{Ephemeral: true, Effort: config.EffortLow}
	medEphemKey := PoolKey{Ephemeral: true, Effort: config.EffortMedium}
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
		poolCfg.TranscriptRescuer = sessMgr.ExtractResponseSinceContext
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
	if cfg.NATSURL != "" {
		def.NATSURL = cfg.NATSURL
	}
	return config.NewFromData(def), false
}

// applyWorkerPoolTimingDefaults applies default timeouts, retry limits, and intervals to WorkerPoolConfig.
func applyWorkerPoolTimingDefaults(cfg *WorkerPoolConfig) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 3 * time.Second
	}
	if cfg.ChunkStepFlushInterval == 0 {
		cfg.ChunkStepFlushInterval = DefaultChunkStepFlushInterval
	} else if cfg.ChunkStepFlushInterval < 0 {
		cfg.ChunkStepFlushInterval = 0
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
	if cfg.TranscriptsIndexerURL == "" && appCfg != nil && appCfg.Current() != nil {
		cfg.TranscriptsIndexerURL = strings.TrimSpace(appCfg.Current().Transcripts.IndexerURL)
	}
}

// resolveDefaultSummarizerLLM resolves an EphemeralLLMFunc for summarization from configured agent pools.
func resolveDefaultSummarizerLLM(pools map[PoolKey]runner.AgentPool) LLMFunc {
	for _, candidate := range []runner.AgentPool{
		pools[PoolKey{Ephemeral: true, Effort: config.EffortLow}],
		pools[PoolKey{Ephemeral: false, Effort: config.EffortLow}],
		pools[PoolKey{Ephemeral: false, Effort: config.EffortHigh}],
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
	wireVoicePoolSessionDependencies(cfg.VoiceProcessPool, pools, cfg.SessionManager, cfg.Store)
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
		eventBus:   cfg.EventBus,
		sessionMgr: cfg.SessionManager,
		ctx:        ctx, cancel: cancel,
		agentName:         cfg.AgentName,
		webhookDispatcher: cfg.WebhookDispatcher, createThreadFn: cfg.CreateThreadFunc,

		TranscriptStore: cfg.TranscriptStore, TranscriptsIndexerURL: cfg.TranscriptsIndexerURL, TranscriptRecheckDelay: cfg.TranscriptRecheckDelay,
		transcriptPublishCh: make(chan TranscriptPublishJob, 256), transcriptPublishSem: make(chan struct{}, 1), transcriptPending: make(map[string]bool),
	}

	if p.cfg.HistoryFetcher == nil {
		p.cfg.HistoryFetcher = func(ctx context.Context, cID, beforeID string, limit int) ([]HistoryMessage, error) {
			return DefaultHistoryFetcher(p.getDiscordSession())(ctx, cID, beforeID, limit)
		}
	}

	p.turns = NewTurnRegistry(p.closeTurnExecution)
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
		if err := p.cfg.SystemAlertFunc(sess, sysChan, title, buildMsg()); err != nil {
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
	if discordutil.IsSnowflake(trimmed) {
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
	p.wg.Go(p.runTranscriptPublishWorker)
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

//nolint:contextcheck,nolintlint // Stop executes on process teardown after callers contexts are cancelled.
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.drainTranscriptPublisher(ctx); err != nil {
		log.Printf("[WorkerPool] Warning: failed to drain transcript publisher: %v", err)
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
		{Ephemeral: false, Effort: config.EffortHigh},
		{Ephemeral: false, Effort: config.EffortMedium},
		{Ephemeral: false, Effort: config.EffortLow},
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
	if up := unwrapUnifiedPool(p.Pool(PoolKey{Ephemeral: false, Effort: config.EffortLow})); up != nil {
		return up
	}
	return unwrapUnifiedPool(p.Pool(PoolKey{Ephemeral: true, Effort: config.EffortLow}))
}

// MediumEffortProcessPool returns the configured runner.UnifiedProcessPool for medium effort turns.
func (p *WorkerPool) MediumEffortProcessPool() *runner.UnifiedProcessPool {
	if up := unwrapUnifiedPool(p.Pool(PoolKey{Ephemeral: false, Effort: config.EffortMedium})); up != nil {
		return up
	}
	return unwrapUnifiedPool(p.Pool(PoolKey{Ephemeral: true, Effort: config.EffortMedium}))
}

// AgentPool returns the configured primary runner.AgentPool.
func (p *WorkerPool) AgentPool() runner.AgentPool {
	return p.Pool(PoolKey{Ephemeral: false, Effort: config.EffortHigh})
}

func logInterruptWarning(op string, err error) {
	if err != nil {
		log.Printf("[WorkerPool] Warning on %s: %v", op, err)
	}
}

// closeSessionDaemons terminates the StreamingDaemon processes leased under sessionKey across all pools.
func (p *WorkerPool) closeSessionDaemons(sessionKey, reason string) {
	if p == nil || sessionKey == "" {
		return
	}
	for _, target := range append(p.StatefulProcessPools(), p.ProcessPool(), p.LowEffortProcessPool(), p.MediumEffortProcessPool()) {
		if target != nil {
			if d, ok := target.Get(sessionKey); ok && d != nil {
				logInterruptWarning(reason, d.Close())
			}
		}
	}
}

// closeTurnExecution interrupts one execution: it cancels its turn context and terminates the
// daemon leased under its session key. TurnRegistry.Stop calls it for the stopped turn only.
func (p *WorkerPool) closeTurnExecution(te *turnExecution, reason string) {
	if te == nil {
		return
	}
	te.cancelTurn()
	te.activeSessionMu.Lock()
	key := te.leaseKey()
	te.activeSessionMu.Unlock()
	p.closeSessionDaemons(key, reason)
}

func (p *WorkerPool) poolModelForEffort(effort string) string {
	switch effort {
	case config.EffortLow:
		return p.LowEffortModel()
	case config.EffortMedium:
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

func updateVoiceEngine(voicePool runner.AgentPool, appCfg *config.Config) {
	if appCfg == nil {
		return
	}
	cur := appCfg.Current()
	if cur == nil {
		return
	}
	if engineUpdater, ok := voicePool.(interface {
		UpdateEngine(engine, claudeBin string, extraBins ...string)
	}); ok {
		engineUpdater.UpdateEngine(cur.VoiceEngine(), cur.ClaudeBin, cur.CodexBin, cur.MuseBin)
	}
}

func (p *WorkerPool) reconcilePoolModelsAndVoiceTargets(poolByKey map[PoolKey]runner.AgentPool, voicePool runner.AgentPool, hasExplicitAppCfg bool, voiceTargets []string) {
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
		updatePoolModel(voicePool, p.VoiceModel())
		updateVoiceEngine(voicePool, p.appCfg)
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
	if p.appCfg != nil {
		voiceTargets = p.appCfg.VoicePrewarmedTargets()
	}
	p.mu.Unlock()

	p.reconcilePoolModelsAndVoiceTargets(poolByKey, voicePool, hasExplicitAppCfg, voiceTargets)

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

// VoiceModel returns the configured voice model name, falling back to LowEffortModel.
func (p *WorkerPool) VoiceModel() string {
	if p != nil && p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil && strings.TrimSpace(cur.Voice.Model) != "" {
			return strings.TrimSpace(cur.Voice.Model)
		}
	}
	if p != nil && !p.hasExplicitAppCfg && p.voiceProcessPool != nil {
		if m, ok := p.voiceProcessPool.(interface{ Model() string }); ok {
			if mod := strings.TrimSpace(m.Model()); mod != "" {
				return mod
			}
		}
	}
	return p.LowEffortModel()
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
