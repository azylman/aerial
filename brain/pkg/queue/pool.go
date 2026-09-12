package queue

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/sanitizer"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/bwmarrin/discordgo"
)

const (
	DefaultMaxSessionTurns     = session.DefaultMaxSessionTurns
	DefaultMaxSessionIdleTime  = 24 * time.Hour
	DefaultTimeoutMinutes      = 60
	DefaultMaxRestarts         = 3
	MaxMessageAbsoluteAge      = 2 * time.Hour
	ContinuationPromptTemplate = "Your previous execution timed out or was interrupted while working. Please inspect where you left off in the conversation transcript and continue the task to completion.\n\nOriginal user request:\n%s"
)

func sanitizeErrorText(errStr string) string {
	return sanitizer.SanitizeLog(errStr)
}

type MemoryRetrieverFunc func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error)

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
	MemoryClient   *memory.Client
	Classifier     *classifier.Classifier
	StalenessTTL   time.Duration
	IdleTimeout    time.Duration
	DrainTimeout   time.Duration
	SessionManager *session.Manager

	// Optional hooks for testing/custom overrides
	RunnerFunc                  func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
	RunnerWithOptionsFunc       func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (stdout, stderr string, exitCode int, err error)
	NotifierRunnerFunc          runner.RunnerFunc
	NotifierFunc                func(agyBin, apiKey, contextDescription string) string
	DeliveryFunc                func(s *discordgo.Session, channelID, text string) error
	DeliveryWithAttachmentsFunc func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment) error
	TypingFunc                  func(s *discordgo.Session, channelID string) (stop func())
	OnMessageCompleted          func(msg db.Message, finalStatus string)
	MemoryRetrieverFunc         MemoryRetrieverFunc
	ResolveChannelPolicy        func(channelID, channelName string) config.ChannelPolicy
	HistoryFetcher              HistoryFetcherFunc
	SystemAlertFunc             func(s *discordgo.Session, channelNameOrID, title, alertBody string) error
	LLMFunc                     LLMFunc
	WebhookDispatcher           WebhookDispatcher
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

	if cfg.MemoryClient == nil {
		cfg.MemoryClient = memory.New(appCfg, sessMgr.Roots()...)
	}
	// Note: MemoryRetrieverFunc is deliberately NOT defaulted to memory.RetrieveRelevantFacts here.
	// Production callers (e.g. brain/main.go) explicitly inject memory.RetrieveRelevantFacts, ensuring
	// unit tests remain strictly hermetic and never attempt live network calls to Ollama.
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
	if cfg.ResolveChannelPolicy == nil {
		cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
			if appCfg != nil {
				if cur := appCfg.Current(); cur != nil {
					return cur.ResolveChannelPolicy(channelID, channelName)
				}
			}
			return config.GetRuntimeConfig().ResolveChannelPolicy(channelID, channelName)
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
		sessionMgr:        sessMgr,
		threadChs:         make(map[string]*threadWorkerState),
		ctx:               ctx,
		cancel:            cancel,
		webhookDispatcher: cfg.WebhookDispatcher,
	}

	if p.cfg.HistoryFetcher == nil {
		p.cfg.HistoryFetcher = func(ctx context.Context, channelID string, beforeID string, limit int) ([]HistoryMessage, error) {
			fetcher := DefaultHistoryFetcher(p.getDiscordSession(), p.cfg.DB)
			return fetcher(ctx, channelID, beforeID, limit)
		}
	}

	var lastAlertMu sync.Mutex
	var lastClassifierAlertTime time.Time

	if p.cfg.Classifier != nil && p.cfg.Classifier.OnParseError == nil {
		p.cfg.Classifier.OnParseError = func(model, raw string, parseErr error) {
			go func(model, raw string, parseErr error) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[WorkerPool] Panic in classifier OnParseError alert handler: %v", r)
					}
				}()

				lastAlertMu.Lock()
				if !lastClassifierAlertTime.IsZero() && time.Since(lastClassifierAlertTime) < 15*time.Second {
					lastAlertMu.Unlock()
					log.Printf("[WorkerPool] Debouncing rapid classifier JSON parse alert to prevent system channel flooding: %v", parseErr)
					return
				}
				lastClassifierAlertTime = time.Now()
				lastAlertMu.Unlock()

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
	if p.cfg.DB != nil {
		sweepCtx, sweepCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = p.cfg.DB.ExecContext(sweepCtx, "UPDATE messages SET status = 'PENDING', error_message = 'deployment_drain' WHERE status = 'PROCESSING'")
		sweepCancel()
	}
}

func (p *WorkerPool) Stop() {
	p.StopWithTimeout(p.cfg.DrainTimeout)
	log.Printf("[WorkerPool] Queue worker pool stopped cleanly")
}
