// Package queue provides an asynchronous FIFO-with-bursts message queue, worker pool,
// status updating, and turn execution engine for Aerial.
package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/notifier"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/bwmarrin/discordgo"
)

const (
	DefaultMaxQuotaPauseDBBytes = session.DefaultMaxQuotaPauseDBBytes
	DefaultMaxSessionIdleTime   = 24 * time.Hour
	DefaultTimeoutMinutes       = 60
	DefaultMaxRestarts          = 3
	MaxMessageAbsoluteAge       = 2 * time.Hour
	ContinuationPromptTemplate  = "Your previous execution timed out or was interrupted while working. Please inspect where you left off in the conversation transcript and continue the task to completion.\n\nOriginal user request:\n%s"
	MaxYieldTrapAutoResumes     = 2
	YieldTrapResumePrompt       = "[SYSTEM NOTICE]: Your previous turn yielded prematurely while background task(s) were still running, causing the runtime to terminate them. Inspect current status (check git status, PR state, or process logs) and continue your workflow to completion. Do NOT end the turn with waiting text."
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
	Store                                                                                         db.Store
	DiscordSession                                                                                *discordgo.Session
	AgyBin, APIKey, Model, LowEffortModel, MediumEffortModel, SystemPrompt                        string
	TimeoutMinutes, MaxAttempts                                                                   int
	BackoffBase, StalenessTTL, IdleTimeout, DrainTimeout, RetryDelayOverride, MaintenanceInterval time.Duration
	SessionManager                                                                                *session.Manager
	Pools                                                                                         map[PoolKey]runner.AgentPool
	ProcessPool, LowEffortProcessPool, MediumEffortProcessPool, VoiceProcessPool                  runner.AgentPool
	Classifier                                                                                    *classifier.Classifier
	HaroURL                                                                                       string

	// Optional hooks for testing/custom overrides
	RunnerFunc                  func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
	RunnerWithOptionsFunc       func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (stdout, stderr string, exitCode int, err error)
	NotifierFunc                func(agyBin, apiKey, contextDescription string) string
	DeliveryFunc                func(s *discordgo.Session, channelID, text string) error
	DeliveryWithAttachmentsFunc func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment) error
	WidgetDeliveryFunc          func(req discord.StatusWidgetRequest) (string, error)
	TypingFunc                  func(s *discordgo.Session, channelID string) (stop func())
	OnMessageCompleted          func(msg db.Message, finalStatus string)
	ResolveChannelPolicy        func(channelID, channelName string) config.ChannelPolicy
	HistoryFetcher              HistoryFetcherFunc
	SystemAlertFunc             func(s *discordgo.Session, channelNameOrID, title, alertBody string) error
	LLMFunc                     LLMFunc
	WebhookDispatcher           WebhookDispatcher
	VoiceRunnerFunc             func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (reply string, convID string, err error)
	VoiceStreamRunnerFunc       func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (reply string, convID string, err error)
	AmbientResolver             func(ctx context.Context, cfg *config.AmbientContextConfig) (string, error)
	CreateThreadFunc            func(channelID, title string) (string, error)
}

type threadWorkerState struct {
	ch              chan db.Message
	activeEnqueuers int
	inFlight        bool
}

type WorkerPool struct {
	appCfg            *config.Config
	overrideModel     string
	cfg               WorkerPoolConfig
	sessionMgr        *session.Manager
	mu                sync.Mutex
	threadChs         map[string]*threadWorkerState
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	stopped           bool
	quotaLockedUntil  atomic.Int64
	scopeLocks        sync.Map
	webhookDispatcher WebhookDispatcher
	summaryGroup      singleflight.Group
	pools             map[PoolKey]runner.AgentPool
	voiceProcessPool  runner.AgentPool
	createThreadFn    func(channelID, title string) (string, error)
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
				SetTranscriptRescuer(func(string, time.Time) string)
			}); ok {
				tr.SetTranscriptRescuer(sessionMgr.ExtractResponseSince)
			}
			if sm, ok := pool.(interface{ SetSessionManager(*session.Manager) }); ok {
				sm.SetSessionManager(sessionMgr)
			}
		}
		if store != nil && !k.Ephemeral {
			if sc, ok := pool.(interface {
				SetSessionCallbacks(func(context.Context, string) (runner.SessionRecord, error), func(context.Context, string, string, string) error)
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
		pools[highKey] = runner.NewUnifiedProcessPool(poolCfg, nil)
	}
	wirePoolSessionDependencies(pools, sessMgr, store)
	return pools
}

// New creates a new WorkerPool with pure *config.Config dependency injection.
func New(appCfg *config.Config, cfg WorkerPoolConfig) *WorkerPool {
	isExplicitAppCfg := (appCfg != nil)
	if appCfg == nil {
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
		appCfg = config.NewFromData(def)
	}
	if cfg.TimeoutMinutes <= 0 {
		cfg.TimeoutMinutes = DefaultTimeoutMinutes
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 3 * time.Second
	}
	if cfg.StalenessTTL <= 0 {
		cfg.StalenessTTL = 30 * time.Minute
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 10 * time.Second
	}
	hasCustomRunner := cfg.RunnerFunc != nil || cfg.RunnerWithOptionsFunc != nil
	if hasCustomRunner && testPoolHook != nil {
		if wrapped := testPoolHook(cfg); wrapped != nil {
			cfg.ProcessPool = wrapped
		}
	}
	if cfg.NotifierFunc == nil {
		cfg.NotifierFunc = func(agyBin, apiKey, desc string) string { return notifier.StaticFallback(desc) }
	}
	if cfg.DeliveryWithAttachmentsFunc == nil {
		if cfg.DeliveryFunc != nil {
			cfg.DeliveryWithAttachmentsFunc = func(s *discordgo.Session, cID, txt string, atts []*delivery.Attachment) error {
				return cfg.DeliveryFunc(s, cID, txt)
			}
		} else {
			cfg.DeliveryWithAttachmentsFunc = delivery.SendMessageWithAttachments
		}
	}
	if cfg.DeliveryFunc == nil {
		cfg.DeliveryFunc = delivery.SendMessage
	}
	if cfg.TypingFunc == nil {
		cfg.TypingFunc = delivery.StartTyping
	}
	sessMgr := cfg.SessionManager
	if sessMgr == nil {
		sessMgr = session.New(appCfg.GeminiHomeDir(), appCfg.DataDir())
	}
	cfg.SessionManager = sessMgr

	if cfg.Classifier == nil {
		cfg.Classifier = classifier.New(nil)
	}
	pools := initWorkerPools(appCfg, isExplicitAppCfg, cfg, sessMgr, cfg.Store)
	if cfg.LLMFunc == nil {
		for _, candidate := range []runner.AgentPool{
			pools[PoolKey{Ephemeral: true, Effort: "low"}],
			pools[PoolKey{Ephemeral: false, Effort: "low"}],
			pools[PoolKey{Ephemeral: false, Effort: "high"}],
		} {
			if candidate == nil {
				continue
			}
			if ep, ok := candidate.(interface{ EphemeralLLMFunc(string) runner.LLMFunc }); ok {
				cfg.LLMFunc = ep.EphemeralLLMFunc("ephemeral:summarizer")
				break
			}
		}
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

	ctx, cancel := context.WithCancel(context.Background())
	p := &WorkerPool{
		appCfg: appCfg, cfg: cfg, pools: pools, voiceProcessPool: cfg.VoiceProcessPool,
		sessionMgr: sessMgr, threadChs: make(map[string]*threadWorkerState), ctx: ctx, cancel: cancel,
		webhookDispatcher: cfg.WebhookDispatcher, createThreadFn: cfg.CreateThreadFunc,
	}

	if p.cfg.HistoryFetcher == nil {
		p.cfg.HistoryFetcher = func(ctx context.Context, cID, beforeID string, limit int) ([]HistoryMessage, error) {
			return DefaultHistoryFetcher(p.getDiscordSession(), p.Store())(ctx, cID, beforeID, limit)
		}
	}

	var lastAlertMu sync.Mutex
	var lastClassifierAlertTime, lastSystemAlertTime time.Time
	if p.cfg.Classifier != nil && p.cfg.Classifier.OnParseError == nil {
		p.cfg.Classifier.OnParseError = func(model, raw string, parseErr error) {
			p.makeDebouncedAlertHandler(&lastAlertMu, &lastClassifierAlertTime, "OnParseError", "Classifier JSON Parse Error", fmt.Sprintf("classifier JSON parse alert: %v", parseErr), func() string {
				snippet := raw
				if runes := []rune(snippet); len(runes) > 600 {
					snippet = string(runes[:590]) + "\n... (truncated)"
				}
				snippet = strings.ReplaceAll(snippet, "```", "'''")
				snippet = strings.ReplaceAll(snippet, "@everyone", "@\u200beveryone")
				snippet = strings.ReplaceAll(snippet, "@here", "@\u200bhere")
				return fmt.Sprintf("**Model**: `%s`\n**Parse Error**:\n```\n%v\n```\n**Raw Output**:\n```\n%s\n```", model, parseErr, snippet)
			})
		}
	}

	if p.cfg.Classifier != nil && p.cfg.Classifier.OnSystemAlert == nil {
		p.cfg.Classifier.OnSystemAlert = func(endpoint string, endpointErr error) {
			p.makeDebouncedAlertHandler(&lastAlertMu, &lastSystemAlertTime, "OnSystemAlert", "Classifier Endpoint Failure", fmt.Sprintf("classifier endpoint alert: %v", endpointErr), func() string {
				return fmt.Sprintf("**Endpoint**: `%s`\n**Status**: Failed after 3 retry attempts with exponential backoff (failing closed to strict mention-only mode).\n**Error Trace**:\n```\n%v\n```", endpoint, endpointErr)
			})
		}
	}

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

func (p *WorkerPool) SetWidgetDeliveryFunc(fn func(req discord.StatusWidgetRequest) (string, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfg.WidgetDeliveryFunc = fn
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

func (p *WorkerPool) DeliverStatusWidget(req discord.StatusWidgetRequest) (string, error) {
	if p == nil {
		return "", errors.New("worker pool is nil")
	}
	if p.cfg.WidgetDeliveryFunc != nil {
		return p.cfg.WidgetDeliveryFunc(req)
	}
	sess := p.DiscordSession()
	if sess == nil {
		return "", errors.New("discord session is not connected")
	}
	return discord.DeliverStatusWidget(sess, req, time.Now())
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
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
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
		}()
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
	if !IsNumericSnowflake(s.ThreadID) {
		return fmt.Sprintf("- Thread `%s`: **%d** turns (session: `%s`)", s.ThreadID, s.TurnCount, s.InternalSessionID)
	}
	effID, _, isThread, _ := ResolveChannelAndThread(sess, s.ThreadID)
	if isThread {
		if effID != "" && effID != s.ThreadID {
			return fmt.Sprintf("- Thread <#%s> (in <#%s>): **%d** turns (session: `%s`)", s.ThreadID, effID, s.TurnCount, s.InternalSessionID)
		}
		return fmt.Sprintf("- Thread <#%s>: **%d** turns (session: `%s`)", s.ThreadID, s.TurnCount, s.InternalSessionID)
	}
	return fmt.Sprintf("- Channel <#%s>: **%d** turns (session: `%s`)", s.ThreadID, s.TurnCount, s.InternalSessionID)
}

// StopWithTimeout initiates a bounded graceful drain of the worker pool.
func (p *WorkerPool) StopWithTimeout(drainTimeout time.Duration) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()

	if drainTimeout <= 0 {
		drainTimeout = p.cfg.DrainTimeout
		if drainTimeout <= 0 {
			drainTimeout = 10 * time.Second
		}
	}

	deadline := time.Now().Add(drainTimeout)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		busy := false
		for _, state := range p.threadChs {
			if len(state.ch) > 0 || state.activeEnqueuers > 0 || state.inFlight {
				busy = true
				break
			}
		}
		p.mu.Unlock()
		if !busy {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	p.cancel()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("[WorkerPool] Graceful drain completed cleanly")
	case <-time.After(2 * time.Second):
		log.Printf("[WorkerPool] Warning: worker exit wait timed out")
	}

	sweepCtx, sweepCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer sweepCancel()
	if store := p.Store(); store != nil {
		if msgs, err := store.GetPendingOrProcessingMessages(sweepCtx, 0); err == nil {
			for _, m := range msgs {
				if m.Status == db.StatusProcessing {
					if err := store.UpdateMessageStatus(sweepCtx, m.ID, db.StatusPending, "deployment_drain"); err != nil {
						log.Printf("[WorkerPool] Warning resetting message %s to pending: %v", m.ID, err)
					}
				}
			}
		}
	}
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
		for _, existing := range out {
			if existing == ap {
				return
			}
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
	p.StopWithTimeout(p.cfg.DrainTimeout)
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

// MarkDirty notifies the process pools to mark active daemons dirty and evict idle daemons.
func (p *WorkerPool) MarkDirty() {
	if p == nil {
		return
	}
	p.mu.Lock()
	pools := p.collectUniquePoolsLocked()
	var voiceTargets []string
	if p.appCfg != nil {
		voiceTargets = p.appCfg.VoicePrewarmedTargets()
	}
	p.mu.Unlock()
	for _, pool := range pools {
		if targetUpdater, ok := pool.(interface{ UpdatePrewarmedTargets([]string) }); ok && voiceTargets != nil {
			targetUpdater.UpdatePrewarmedTargets(voiceTargets)
		}
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

func (p *WorkerPool) runnerCredentials() (string, string) {
	agyBin, apiKey := p.cfg.AgyBin, p.cfg.APIKey
	if p != nil && p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil {
			if cur.AgyBin != "" {
				agyBin = cur.AgyBin
			}
			if cur.APIKey != "" {
				apiKey = cur.APIKey
			}
		}
	}
	return agyBin, apiKey
}

// ExecuteVoiceTurn synchronously executes a turn for voice queries.
func (p *WorkerPool) ExecuteVoiceTurn(ctx context.Context, prompt, sessionID string, onStatus func(status string), onSentence ...func(sentence string)) (string, string, error) {
	if p == nil {
		return "", "", fmt.Errorf("worker pool is uninitialized")
	}
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return "", "", fmt.Errorf("brain is shutting down")
	}
	p.mu.Unlock()

	select {
	case <-p.ctx.Done():
		return "", "", fmt.Errorf("brain is shutting down")
	case <-ctx.Done():
		return "", "", ctx.Err()
	default:
	}

	deviceKey := strings.TrimSpace(sessionID)
	if deviceKey == "" {
		return "", "", fmt.Errorf("queue: session_id is required for voice turn routing")
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
	if (p.cfg.VoiceStreamRunnerFunc != nil && sentenceCb != nil) || p.cfg.VoiceRunnerFunc != nil {
		start := time.Now()
		var reply, conv string
		var err error
		if p.cfg.VoiceStreamRunnerFunc != nil && sentenceCb != nil {
			reply, conv, err = p.cfg.VoiceStreamRunnerFunc(ctx, turnPrompt, deviceKey, onStatus, sentenceCb)
		} else {
			reply, conv, err = p.cfg.VoiceRunnerFunc(ctx, turnPrompt, deviceKey, onStatus)
		}
		runStatus := "success"
		if err != nil {
			runStatus = "error"
		}
		metrics.RecordRunnerExecution(runStatus, p.LowEffortModel(), "voice", time.Since(start))
		return reply, conv, err
	}

	p.mu.Lock()
	var procPool runner.AgentPool
	if p.voiceProcessPool != nil {
		procPool = p.voiceProcessPool
	} else if p.pools != nil {
		procPool = p.pools[PoolKey{Ephemeral: false, Effort: "high"}]
	}
	p.mu.Unlock()
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
