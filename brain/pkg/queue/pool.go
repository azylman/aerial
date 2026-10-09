package queue

import (
	"context"
	"database/sql"
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

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/sanitizer"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/bwmarrin/discordgo"
)

const (
	DefaultMaxQuotaPauseDBBytes = session.DefaultMaxQuotaPauseDBBytes
	DefaultMaxSessionIdleTime  = 24 * time.Hour
	DefaultTimeoutMinutes      = 60
	DefaultMaxRestarts         = 3
	MaxMessageAbsoluteAge      = 2 * time.Hour
	ContinuationPromptTemplate = "Your previous execution timed out or was interrupted while working. Please inspect where you left off in the conversation transcript and continue the task to completion.\n\nOriginal user request:\n%s"
	MaxYieldTrapAutoResumes    = 2
	YieldTrapResumePrompt      = "[SYSTEM NOTICE]: Your previous turn yielded prematurely while background task(s) were still running, causing the runtime to terminate them. Inspect current status (check git status, PR state, or process logs) and continue your workflow to completion. Do NOT end the turn with waiting text."
	TaskCompletionResumePrompt = "[SYSTEM NOTICE]: Background task %s has finished with exit code %d. Logs are available at %s. Continue your workflow to completion. Do NOT end the turn with waiting text."
)

func sanitizeErrorText(errStr string) string {
	return sanitizer.SanitizeLog(errStr)
}

// testPoolHook is an optional hook injected by tests to wrap custom test runners without compiling test adapters into production.
var testPoolHook func(cfg WorkerPoolConfig) runner.AgentPool

type WorkerPoolConfig struct {
	DB             *sql.DB
	Store          db.Store
	DiscordSession *discordgo.Session
	AgyBin         string
	APIKey         string
	Model          string
	LowEffortModel string
	SystemPrompt   string
	TimeoutMinutes int
	BackoffBase    time.Duration
	MaxAttempts    int
	Classifier     *classifier.Classifier
	StalenessTTL   time.Duration
	IdleTimeout    time.Duration
	DrainTimeout       time.Duration
	RetryDelayOverride time.Duration
	SessionManager       *session.Manager
	ProcessPool          runner.AgentPool
	LowEffortProcessPool runner.AgentPool
	VoiceProcessPool     runner.AgentPool
	MaintenanceInterval time.Duration

	// Optional hooks for testing/custom overrides
	RunnerFunc                  func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
	RunnerWithOptionsFunc       func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (stdout, stderr string, exitCode int, err error)
	NotifierRunnerFunc          runner.RunnerFunc
	NotifierFunc                func(agyBin, apiKey, contextDescription string) string
	DeliveryFunc                func(s *discordgo.Session, channelID, text string) error
	DeliveryWithAttachmentsFunc func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment) error
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
	processPool          runner.AgentPool
	lowEffortProcessPool runner.AgentPool
	voiceProcessPool     runner.AgentPool
}

// SummaryGroup returns the singleflight.Group coordinating thread summarizations for this pool instance.
func (p *WorkerPool) SummaryGroup() *singleflight.Group {
	if p == nil {
		return nil
	}
	return &p.summaryGroup
}

// Store returns the configured db.Store, resolving cfg.Store or wrapping cfg.DB if necessary.
func (p *WorkerPool) Store() db.Store {
	if p == nil {
		return nil
	}
	if p.cfg.Store != nil {
		return p.cfg.Store
	}
	if p.cfg.DB != nil {
		p.cfg.Store = db.NewSQLStore(p.cfg.DB)
		return p.cfg.Store
	}
	return nil
}

func resolveStore(dbOrStore any) db.Store {
	if dbOrStore == nil {
		return nil
	}
	switch s := dbOrStore.(type) {
	case *db.FakeStore:
		if s == nil {
			return nil
		}
		return s
	case *db.SQLStore:
		if s == nil {
			return nil
		}
		return s
	case db.Store:
		return s
	case *sql.DB:
		if s == nil {
			return nil
		}
		return db.NewSQLStore(s)
	default:
		return nil
	}
}

// New creates a new WorkerPool with pure *config.Config dependency injection.
func New(appCfg *config.Config, cfg WorkerPoolConfig) *WorkerPool {
	if cfg.Store == nil && cfg.DB != nil {
		cfg.Store = db.NewSQLStore(cfg.DB)
	}
	isExplicitAppCfg := (appCfg != nil)
	if appCfg == nil {
		def := config.DefaultConfigData()
		if cfg.Model != "" {
			def.Model = cfg.Model
		}
		if cfg.LowEffortModel != "" {
			def.LowEffortModel = cfg.LowEffortModel
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
	if cfg.RunnerWithOptionsFunc == nil && cfg.RunnerFunc != nil {
		cfg.RunnerWithOptionsFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			timeoutMins := int(opts.MaxDuration / time.Minute)
			if timeoutMins <= 0 {
				timeoutMins = int(opts.InactivityTimeout / time.Minute)
			}
			if timeoutMins <= 0 {
				timeoutMins = cfg.TimeoutMinutes
			}
			return cfg.RunnerFunc(ctx, agyBin, prompt, sessionID, apiKey, model, timeoutMins)
		}
	} else if cfg.RunnerFunc == nil && cfg.RunnerWithOptionsFunc != nil {
		cfg.RunnerFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return cfg.RunnerWithOptionsFunc(ctx, agyBin, prompt, sessionID, apiKey, model, runner.DefaultWatchdogOptions(timeoutMinutes))
		}
	} else if cfg.RunnerWithOptionsFunc == nil && cfg.RunnerFunc == nil {
		cfg.RunnerFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 1, fmt.Errorf("queue: RunnerFunc not configured on WorkerPool")
		}
		cfg.RunnerWithOptionsFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			return "", "", 1, fmt.Errorf("queue: RunnerWithOptionsFunc not configured on WorkerPool")
		}
	}
	if hasCustomRunner && testPoolHook != nil {
		if wrapped := testPoolHook(cfg); wrapped != nil {
			cfg.ProcessPool = wrapped
		}
	}
	if cfg.NotifierFunc == nil {
		notifierRunner := cfg.NotifierRunnerFunc
		if notifierRunner == nil && isExplicitAppCfg {
			notifierRunner = cfg.RunnerFunc
		}
		if notifierRunner != nil {
			cfg.NotifierFunc = func(agyBin, apiKey, contextDescription string) string {
				return notifier.GenerateDynamicNotification(agyBin, apiKey, contextDescription, notifierRunner)
			}
		} else {
			cfg.NotifierFunc = func(agyBin, apiKey, contextDescription string) string {
				return notifier.StaticFallback(contextDescription)
			}
		}
	}
	if cfg.DeliveryWithAttachmentsFunc == nil {
		if cfg.DeliveryFunc != nil {
			cfg.DeliveryWithAttachmentsFunc = func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment) error {
				return cfg.DeliveryFunc(s, channelID, text)
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
		if appCfg != nil {
			sessMgr = session.New(appCfg.GeminiHomeDir(), appCfg.DataDir())
		} else {
			sessMgr = session.New("", "")
		}
	}
	cfg.SessionManager = sessMgr

	if cfg.Classifier == nil {
		runnerFn := cfg.RunnerFunc
		apiKey := cfg.APIKey
		agyBin := cfg.AgyBin
		if appCfg != nil {
			if cur := appCfg.Current(); cur != nil {
				if cur.APIKey != "" {
					apiKey = cur.APIKey
				}
				if cur.AgyBin != "" {
					agyBin = cur.AgyBin
				}
			}
		}
		cfg.Classifier = classifier.NewClassifier(
			classifier.WithLLMFunc(classifier.NewAgyLLMFunc(agyBin, apiKey, runnerFn)),
		)
	}
	if cfg.LLMFunc == nil && cfg.ProcessPool != nil {
		if ep, ok := cfg.ProcessPool.(interface {
			EphemeralLLMFunc(string) runner.LLMFunc
		}); ok {
			cfg.LLMFunc = ep.EphemeralLLMFunc("ephemeral:summarizer")
		}
	}
	if cfg.ResolveChannelPolicy == nil {
		cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
			if appCfg != nil {
				return appCfg.ResolveChannelPolicy(channelID, channelName)
			}
			return config.ActiveConfig().ResolveChannelPolicy(channelID, channelName)
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
		appCfg:            appCfg,
		cfg:               cfg,
		processPool:          cfg.ProcessPool,
		lowEffortProcessPool: cfg.LowEffortProcessPool,
		voiceProcessPool:     cfg.VoiceProcessPool,
		sessionMgr:        sessMgr,
		threadChs:         make(map[string]*threadWorkerState),
		ctx:               ctx,
		cancel:            cancel,
		webhookDispatcher: cfg.WebhookDispatcher,
	}

	if p.processPool == nil && isExplicitAppCfg {
		daemonBin := cfg.AgyBin
		if daemonBin == "" && appCfg.Current() != nil {
			daemonBin = appCfg.Current().AgyBin
		}
		daemonDataDir := ""
		if appCfg.Current() != nil {
			daemonDataDir = appCfg.Current().DataDir
		}
		defaultModel := cfg.Model
		if defaultModel == "" && appCfg.Current() != nil {
			defaultModel = appCfg.Current().Model
		}
		poolCfg := runner.PoolConfig{
			AgyBin:       daemonBin,
			Cwd:          daemonDataDir,
			Env:          os.Environ(),
			DefaultModel: defaultModel,
		}
		if p.sessionMgr != nil {
			poolCfg.TranscriptRescuer = p.sessionMgr.ExtractResponseSince
			poolCfg.SessionManager = p.sessionMgr
		}
		if store := p.Store(); store != nil {
			poolCfg.OnSessionRotated = func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
				return store.RotateSessionID(ctx, targetKey, newSessionID)
			}
			poolCfg.GetSessionRecord = func(ctx context.Context, targetKey string) (runner.SessionRecord, error) {
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
				return runner.SessionRecord{
					ActiveSessionID:   sessID,
					PreviousSessionID: prevID,
					TurnCount:         turns,
				}, nil
			}
		}
		p.processPool = runner.NewUnifiedProcessPool(poolCfg, nil)
	}

	if p.sessionMgr != nil {
		if tr, ok := p.processPool.(interface {
			SetTranscriptRescuer(func(convID string, since time.Time) string)
		}); ok {
			tr.SetTranscriptRescuer(p.sessionMgr.ExtractResponseSince)
		}
		if tr, ok := p.lowEffortProcessPool.(interface {
			SetTranscriptRescuer(func(convID string, since time.Time) string)
		}); ok {
			tr.SetTranscriptRescuer(p.sessionMgr.ExtractResponseSince)
		}
		if sm, ok := p.processPool.(interface {
			SetSessionManager(*session.Manager)
		}); ok {
			sm.SetSessionManager(p.sessionMgr)
		}
		if sm, ok := p.lowEffortProcessPool.(interface {
			SetSessionManager(*session.Manager)
		}); ok {
			sm.SetSessionManager(p.sessionMgr)
		}
	}

	if store := p.Store(); store != nil {
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
			return runner.SessionRecord{
				ActiveSessionID:   sessID,
				PreviousSessionID: prevID,
				TurnCount:         turns,
			}, nil
		}
		onRot := func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			return store.RotateSessionID(ctx, targetKey, newSessionID)
		}
		if sc, ok := p.processPool.(interface {
			SetSessionCallbacks(func(context.Context, string) (runner.SessionRecord, error), func(context.Context, string, string, string) error)
		}); ok {
			sc.SetSessionCallbacks(getRec, onRot)
		}

	}

	if p.cfg.HistoryFetcher == nil {
		p.cfg.HistoryFetcher = func(ctx context.Context, channelID string, beforeID string, limit int) ([]HistoryMessage, error) {
			fetcher := DefaultHistoryFetcher(p.getDiscordSession(), p.Store())
			return fetcher(ctx, channelID, beforeID, limit)
		}
	}

	var lastAlertMu sync.Mutex
	var lastClassifierAlertTime time.Time
	var lastSystemAlertTime time.Time

	if p.cfg.Classifier != nil && p.cfg.Classifier.OnParseError == nil {
		p.cfg.Classifier.OnParseError = func(model, raw string, parseErr error) {
			lastAlertMu.Lock()
			if !lastClassifierAlertTime.IsZero() && time.Since(lastClassifierAlertTime) < 15*time.Second {
				lastAlertMu.Unlock()
				log.Printf("[WorkerPool] Debouncing rapid classifier JSON parse alert to prevent system channel flooding: %v", parseErr)
				return
			}
			lastClassifierAlertTime = time.Now()
			lastAlertMu.Unlock()

			go func(model, raw string, parseErr error) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[WorkerPool] Panic in classifier OnParseError alert handler: %v", r)
					}
				}()

				sess := p.getDiscordSession()
				if sess == nil {
					return
				}
				sysChan := ""
				if p.appCfg != nil {
					if cur := p.appCfg.Current(); cur != nil {
						sysChan = cur.SystemChannel
					}
				}
				if sysChan == "" {
					sysChan = config.DefaultConfigData().SystemChannel
				}

				// Defensively sanitize snippet: rune-slice to 600 runes, escape code fences, and neutralize mentions
				snippet := raw
				runes := []rune(snippet)
				if len(runes) > 600 {
					snippet = string(runes[:590]) + "\n... (truncated)"
				}
				snippet = strings.ReplaceAll(snippet, "```", "'''")
				snippet = strings.ReplaceAll(snippet, "@everyone", "@\u200beveryone")
				snippet = strings.ReplaceAll(snippet, "@here", "@\u200bhere")

				alertMsg := fmt.Sprintf("**Model**: `%s`\n**Parse Error**:\n```\n%v\n```\n**Raw Output**:\n```\n%s\n```",
					model, parseErr, snippet)
				sanitizedAlert := sanitizeErrorText(alertMsg)
				if err := p.cfg.SystemAlertFunc(sess, sysChan, "Classifier JSON Parse Error", sanitizedAlert); err != nil {
					log.Printf("[WorkerPool] Warning: failed to send JSON parse error alert to system channel %q: %v", sysChan, err)
				}
			}(model, raw, parseErr)
		}
	}

	if p.cfg.Classifier != nil && p.cfg.Classifier.OnSystemAlert == nil {
		p.cfg.Classifier.OnSystemAlert = func(endpoint string, endpointErr error) {
			lastAlertMu.Lock()
			if !lastSystemAlertTime.IsZero() && time.Since(lastSystemAlertTime) < 15*time.Second {
				lastAlertMu.Unlock()
				log.Printf("[WorkerPool] Debouncing rapid classifier endpoint alert to prevent system channel flooding: %v", endpointErr)
				return
			}
			lastSystemAlertTime = time.Now()
			lastAlertMu.Unlock()

			go func(endpoint string, endpointErr error) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[WorkerPool] Panic in classifier OnSystemAlert alert handler: %v", r)
					}
				}()

				sess := p.getDiscordSession()
				if sess == nil {
					return
				}
				sysChan := ""
				if p.appCfg != nil {
					if cur := p.appCfg.Current(); cur != nil {
						sysChan = cur.SystemChannel
					}
				}
				if sysChan == "" {
					sysChan = config.DefaultConfigData().SystemChannel
				}

				alertMsg := fmt.Sprintf("**Endpoint**: `%s`\n**Status**: Failed after 3 retry attempts with exponential backoff (failing closed to strict mention-only mode).\n**Error Trace**:\n```\n%v\n```",
					endpoint, endpointErr)
				sanitizedAlert := sanitizeErrorText(alertMsg)
				if err := p.cfg.SystemAlertFunc(sess, sysChan, "Classifier Endpoint Failure", sanitizedAlert); err != nil {
					log.Printf("[WorkerPool] Warning: failed to send classifier endpoint alert to system channel %q: %v", sysChan, err)
				}
			}(endpoint, endpointErr)
		}
	}

	return p
}

// NewWorkerPool is a compatibility wrapper for New.
func NewWorkerPool(cfg WorkerPoolConfig) *WorkerPool {
	return New(nil, cfg)
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

// DiscordSession returns the currently assigned discordgo session.
func (p *WorkerPool) DiscordSession() *discordgo.Session {
	return p.getDiscordSession()
}

// DeliverDirect delivers a message directly to the target Discord channel or thread,
// completely bypassing the worker queue and LLM agent execution.
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

// WebhookDispatcher returns the assigned webhook dispatcher.
func (p *WorkerPool) WebhookDispatcher() WebhookDispatcher {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.webhookDispatcher
}

// SetWebhookDispatcher updates the webhook dispatcher on the pool.
func (p *WorkerPool) SetWebhookDispatcher(d WebhookDispatcher) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if d == nil {
		d = NewDefaultWebhookDispatcher()
	}
	p.webhookDispatcher = d
	p.cfg.WebhookDispatcher = d
}

func (p *WorkerPool) UpdateRuntimeConfig(model string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if strings.TrimSpace(model) != "" {
		p.overrideModel = model
		p.cfg.Model = model
	}
	log.Printf("[WorkerPool] Runtime config updated: model=%s", model)
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

func (p *WorkerPool) SessionManager() *session.Manager {
	return p.sessionMgr
}

func (p *WorkerPool) Start() {
	// WorkerPool starts workers lazily per active thread
	log.Printf("[WorkerPool] Started queue worker pool with max %d attempts per turn", p.cfg.MaxAttempts)
	if p.processPool != nil || p.cfg.Store != nil {
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
// It deduplicates alerts via lastReportedTurnCount so dormant threads do not produce repetitive notifications.
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
		lastCount := lastReportedTurnCount[s.ThreadID]
		if s.TurnCount > lastCount {
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

	displayCap := 5
	var lines []string
	for i, s := range runawayList {
		if i >= displayCap {
			lines = append(lines, fmt.Sprintf("- *...and %d more runaway session(s)*", len(runawayList)-displayCap))
			break
		}
		lines = append(lines, formatUnrotatedSessionLine(sess, s))
	}

	for _, s := range runawayList {
		lastReportedTurnCount[s.ThreadID] = s.TurnCount
	}

	sysChan := ""
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil {
			sysChan = cur.SystemChannel
		}
	}
	if sysChan == "" {
		sysChan = config.DefaultConfigData().SystemChannel
	}

	alertMsg := fmt.Sprintf("⚠️ **Unrotated Runaway Sessions Detected**\n\nThe following sessions have exceeded the turn limit (%d turns) without rotating:\n%s\n\n*Purely informational watchdog alert. Sessions may require investigation if turn count continues increasing.*",
		session.DefaultMaxSessionTurns, strings.Join(lines, "\n"))
	sanitizedAlert := sanitizeErrorText(alertMsg)

	if p.cfg.SystemAlertFunc != nil {
		if alertErr := p.cfg.SystemAlertFunc(sess, sysChan, "Unrotated Runaway Sessions", sanitizedAlert); alertErr != nil {
			log.Printf("[WorkerPool] Warning: failed to send unrotated sessions alert to system channel %q: %v", sysChan, alertErr)
		}
	}
}

// formatUnrotatedSessionLine formats an unrotated session entry for system alerts, resolving Discord snowflakes
// to readable channel or thread mention links.
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

// StopWithTimeout initiates a bounded graceful drain of the worker pool, allowing inflight turns
// to complete and deliver to Discord before cancelling context.
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

	// Stage 1: Wait up to drainTimeout for pending messages and in-flight bursts to finish
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

	// Stage 2: Queues drained and turns completed (or timeout reached).
	// Cancel context to wake up all idle worker goroutines.
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

	// Stage 3: Atomic safety net: sweep any remaining PROCESSING messages to PENDING
	if store := p.Store(); store != nil {
		sweepCtx, sweepCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if msgs, err := store.GetPendingOrProcessingMessages(sweepCtx, 0); err == nil {
			for _, m := range msgs {
				if m.Status == db.StatusProcessing {
					if err := store.UpdateMessageStatus(sweepCtx, m.ID, db.StatusPending, "deployment_drain"); err != nil {
						log.Printf("[WorkerPool] Warning: failed to reset message %s to pending on drain: %v", m.ID, err)
					}
				}
			}
		}
		sweepCancel()
	} else if p.cfg.DB != nil {
		sweepCtx, sweepCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if _, err := p.cfg.DB.ExecContext(sweepCtx, "UPDATE messages SET status = 'PENDING', error_message = 'deployment_drain' WHERE status = 'PROCESSING'"); err != nil {
			log.Printf("[WorkerPool] Warning: failed to sweep processing messages to pending on drain: %v", err)
		}
		sweepCancel()
	}
}

func (p *WorkerPool) Stop() {
	p.StopWithTimeout(p.cfg.DrainTimeout)
	p.mu.Lock()
	procPool := p.processPool
	lowPool := p.lowEffortProcessPool
	voicePool := p.voiceProcessPool
	p.mu.Unlock()
	if procPool != nil {
		if closeErr := procPool.Close(); closeErr != nil {
			log.Printf("[WorkerPool] Warning: failed to close unified process pool: %v", closeErr)
		}
	}
	if lowPool != nil {
		if closeErr := lowPool.Close(); closeErr != nil {
			log.Printf("[WorkerPool] Warning: failed to close low effort process pool: %v", closeErr)
		}
	}
	if voicePool != nil {
		if closeErr := voicePool.Close(); closeErr != nil {
			log.Printf("[WorkerPool] Warning: failed to close voice process pool: %v", closeErr)
		}
	}
	log.Printf("[WorkerPool] Queue worker pool stopped cleanly")
}

// ProcessPool returns the configured runner.UnifiedProcessPool, or nil if unconfigured or p is nil.
func (p *WorkerPool) ProcessPool() *runner.UnifiedProcessPool {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if up, ok := p.processPool.(*runner.UnifiedProcessPool); ok {
		return up
	}
	if uw, ok := p.processPool.(interface {
		Underlying() *runner.UnifiedProcessPool
	}); ok {
		return uw.Underlying()
	}
	return nil
}

// LowEffortProcessPool returns the configured runner.UnifiedProcessPool for low effort turns, or nil if unconfigured or p is nil.
func (p *WorkerPool) LowEffortProcessPool() *runner.UnifiedProcessPool {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if up, ok := p.lowEffortProcessPool.(*runner.UnifiedProcessPool); ok {
		return up
	}
	if ip, ok := p.lowEffortProcessPool.(interface {
		Underlying() *runner.UnifiedProcessPool
	}); ok {
		return ip.Underlying()
	}
	return nil
}

// VoiceProcessPool returns the configured runner.AgentPool for voice turns, or nil if unconfigured or p is nil.
func (p *WorkerPool) VoiceProcessPool() runner.AgentPool {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.voiceProcessPool
}

// AgentPool returns the configured primary runner.AgentPool.
func (p *WorkerPool) AgentPool() runner.AgentPool {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.processPool
}

// MarkDirty notifies the process pools to mark active daemons dirty and evict idle daemons.
func (p *WorkerPool) MarkDirty() {
	if p == nil {
		return
	}
	p.mu.Lock()
	procPool := p.processPool
	lowPool := p.lowEffortProcessPool
	voicePool := p.voiceProcessPool
	var voiceTargets []string
	if p.appCfg != nil {
		voiceTargets = p.appCfg.VoicePrewarmedTargets()
	}
	p.mu.Unlock()
	if procPool != nil {
		if md, ok := procPool.(interface{ MarkDirty() }); ok {
			md.MarkDirty()
		}
	}
	if lowPool != nil {
		if md, ok := lowPool.(interface{ MarkDirty() }); ok {
			md.MarkDirty()
		}
	}
	if voicePool != nil {
		if targetUpdater, ok := voicePool.(interface{ UpdatePrewarmedTargets([]string) }); ok {
			if voiceTargets != nil {
				targetUpdater.UpdatePrewarmedTargets(voiceTargets)
			}
		}
		if dirtyMarker, ok := voicePool.(interface{ MarkDirty() }); ok {
			dirtyMarker.MarkDirty()
		}
	}
}

// LowEffortModel returns the configured low effort reasoning model name.
func (p *WorkerPool) LowEffortModel() string {
	if p == nil {
		return config.DefaultConfigData().LowEffortModel
	}
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil && strings.TrimSpace(cur.LowEffortModel) != "" {
			return strings.TrimSpace(cur.LowEffortModel)
		}
	}
	if strings.TrimSpace(p.cfg.LowEffortModel) != "" {
		return strings.TrimSpace(p.cfg.LowEffortModel)
	}
	return config.DefaultConfigData().LowEffortModel
}

// ExecuteVoiceTurn synchronously executes a turn for voice queries, enforcing low effort reasoning
// and streaming intermediate tool execution status and sentence callbacks.
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

	// Guard against background pool context cancellation or caller cancellation
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

	// Mutual exclusion on session / threadID to prevent concurrent turn collisions
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

	// Testing hook: If VoiceStreamRunnerFunc is configured and sentence callback provided, delegate directly
	if p.cfg.VoiceStreamRunnerFunc != nil && sentenceCb != nil {
		start := time.Now()
		reply, conv, err := p.cfg.VoiceStreamRunnerFunc(ctx, turnPrompt, deviceKey, onStatus, sentenceCb)
		runStatus := "success"
		if err != nil {
			runStatus = "error"
		}
		metrics.RecordRunnerExecution(runStatus, p.LowEffortModel(), "voice", time.Since(start))
		return reply, conv, err
	}

	// Testing hook: If VoiceRunnerFunc is configured, delegate directly
	if p.cfg.VoiceRunnerFunc != nil {
		start := time.Now()
		reply, conv, err := p.cfg.VoiceRunnerFunc(ctx, turnPrompt, deviceKey, onStatus)
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
	} else if p.processPool != nil {
		procPool = p.processPool
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

	var turnRes *runner.TurnResult
	var turnErr error
	activeSession := deviceKey

	if leasedPool, ok := procPool.(runner.LeasedAgentPool); ok {
		lease, lErr := leasedPool.AcquireLease(ctx, deviceKey)
		if lErr != nil {
			metrics.RecordRunnerExecution("error", model, "voice", time.Since(start))
			return "", deviceKey, fmt.Errorf("failed to acquire voice daemon: %w", lErr)
		}
		defer func() {
			if relErr := lease.Release(); relErr != nil {
				log.Printf("[WorkerPool] Warning releasing voice lease for %s: %v", deviceKey, relErr)
			}
		}()
		activeSession = lease.SessionID()
		if activeSession == "" {
			activeSession = deviceKey
		}
		turnCtx := &runner.TurnContext{
			TurnID:            uuid.New().String(),
			SessionID:         activeSession,
			Model:             model,
			Prompt:            turnPrompt,
			ScopeInstructions: nodeInstructions,
			Sink:              sink,
			CreatedAt:         time.Now(),
			Ctx:               ctx,
		}
		turnRes, turnErr = lease.Execute(ctx, turnCtx)
		if turnErr != nil && !errors.Is(turnErr, context.Canceled) && !errors.Is(turnErr, context.DeadlineExceeded) {
			turnErr = fmt.Errorf("failed sending turn to voice daemon: %w", turnErr)
		}
	} else {
		daemon, dErr := procPool.GetOrCreateSession(ctx, deviceKey, "")
		if dErr != nil {
			metrics.RecordRunnerExecution("error", model, "voice", time.Since(start))
			return "", deviceKey, fmt.Errorf("failed to acquire voice daemon: %w", dErr)
		}
		turnCtx := &runner.TurnContext{
			TurnID:            uuid.New().String(),
			SessionID:         deviceKey,
			Model:             model,
			Prompt:            turnPrompt,
			ScopeInstructions: nodeInstructions,
			Sink:              sink,
			CreatedAt:         time.Now(),
			Ctx:               ctx,
		}
		if sendErr := daemon.Send(turnPrompt, turnCtx); sendErr != nil {
			metrics.RecordRunnerExecution("error", model, "voice", time.Since(start))
			return "", deviceKey, fmt.Errorf("failed sending turn to voice daemon: %w", sendErr)
		}
		turnRes, turnErr = sink.Wait(ctx)
		if daemon.SessionID() != "" {
			activeSession = daemon.SessionID()
		}
	}

	resolver := runner.NewTurnResolver()
	outcome := resolver.Resolve(ctx, turnRes, turnErr, activeSession)

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
		OnSkillActivated: func(skillName, source string) {
			metrics.RecordSkillActivation(skillName, source)
		},
		OnTextDelta: func(delta string) {
			if ctx != nil && ctx.Err() != nil {
				return
			}
			if onSentence != nil && detector != nil {
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


