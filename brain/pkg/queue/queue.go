package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/sanitizer"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// ChannelSnapshot holds immutable channel metadata to avoid cross-goroutine pointer races.
type ChannelSnapshot struct {
	ID       string
	Name     string
	GuildID  string
	ParentID string
	IsThread bool
}

var (
	channelCacheMu   sync.RWMutex
	channelCache     = make(map[string]ChannelSnapshot)
	restSingleFlight singleflight.Group
)

const (
	DefaultMaxSessionTurns     = 15
	DefaultTimeoutMinutes      = 60
	DefaultMaxRestarts         = 3
	MaxMessageAbsoluteAge      = 2 * time.Hour
	ContinuationPromptTemplate = "Your previous execution timed out or was interrupted while working. Please inspect where you left off in the conversation transcript and continue the task to completion.\n\nOriginal user request:\n%s"
)

// CacheDiscordChannel stores an immutable snapshot of a discordgo.Channel.
func CacheDiscordChannel(ch *discordgo.Channel) {
	if ch == nil || ch.ID == "" {
		return
	}
	channelCacheMu.Lock()
	defer channelCacheMu.Unlock()
	guildID := ch.GuildID
	if guildID == "" && ch.ParentID != "" {
		if parentSnap, ok := channelCache[ch.ParentID]; ok {
			guildID = parentSnap.GuildID
		}
	}
	channelCache[ch.ID] = ChannelSnapshot{
		ID:       ch.ID,
		Name:     ch.Name,
		GuildID:  guildID,
		ParentID: ch.ParentID,
		IsThread: ch.IsThread(),
	}
}

// InvalidateChannelCache removes a channel from the internal cache.
func InvalidateChannelCache(channelID string) {
	channelCacheMu.Lock()
	delete(channelCache, channelID)
	channelCacheMu.Unlock()
}

// GetCachedChannel returns a cached channel snapshot if available.
func GetCachedChannel(channelID string) (ChannelSnapshot, bool) {
	channelCacheMu.RLock()
	snap, ok := channelCache[channelID]
	channelCacheMu.RUnlock()
	return snap, ok
}

// IsNumericSnowflake returns true if id != "" and contains only ASCII digits [0-9].
func IsNumericSnowflake(id string) bool {
	if id == "" {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
}

func resolveChannelSnapshot(s *discordgo.Session, channelID string) (ChannelSnapshot, bool) {
	if channelID == "" {
		return ChannelSnapshot{}, false
	}
	if snap, ok := GetCachedChannel(channelID); ok {
		return snap, true
	}
	if s == nil {
		return ChannelSnapshot{}, false
	}
	if s.State != nil {
		if ch, err := s.State.Channel(channelID); err == nil && ch != nil {
			CacheDiscordChannel(ch)
			if snap, ok := GetCachedChannel(channelID); ok {
				return snap, true
			}
		}
	}
	if s.Token != "" && IsNumericSnowflake(channelID) {
		res, err, _ := restSingleFlight.Do(channelID, func() (interface{}, error) {
			return s.Channel(channelID)
		})
		if err == nil && res != nil {
			if ch, ok := res.(*discordgo.Channel); ok && ch != nil {
				CacheDiscordChannel(ch)
				if s.State != nil {
					_ = s.State.ChannelAdd(ch)
				}
				if snap, ok := GetCachedChannel(channelID); ok {
					return snap, true
				}
			}
		}
	}
	return ChannelSnapshot{}, false
}

// ResolveEffectiveChannel resolves a Discord channel or thread to its effective channel ID and name.
// If channelID is a Discord thread, it resolves the parent channel ID and parent channel name.
// It uses the centralized ChannelSnapshot cache, live Discord State, and singleflight REST queries.
// For non-numeric or synthetic channel IDs (e.g. HTTP client UUIDs), it immediately returns without REST calls.
func ResolveEffectiveChannel(s *discordgo.Session, channelID string) (effectiveID string, effectiveName string, isThread bool) {
	if channelID == "" {
		return "", "", false
	}

	snap, ok := resolveChannelSnapshot(s, channelID)
	if !ok {
		return channelID, "", false
	}

	if snap.IsThread {
		if snap.ParentID != "" {
			parentSnap, parentOk := resolveChannelSnapshot(s, snap.ParentID)
			if parentOk {
				return snap.ParentID, parentSnap.Name, true
			}
			return snap.ParentID, "", true
		}
		return snap.ID, snap.Name, true
	}
	return snap.ID, snap.Name, false
}

var (
	aerialExclusionRegex = regexp.MustCompile(`(?i)\baerial\s+(?:view|photo)s?\b`)
	tier1KeywordRegex    = regexp.MustCompile(`(?i)\b(aerial|gundam)\b`)
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

	// Optional hooks for testing/custom overrides
	RunnerFunc           func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
	RunnerWithOptionsFunc func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (stdout, stderr string, exitCode int, err error)
	NotifierFunc         func(agyBin, apiKey, contextDescription string) string
	DeliveryFunc                func(s *discordgo.Session, channelID, text string) error
	DeliveryWithAttachmentsFunc func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment) error
	TypingFunc           func(s *discordgo.Session, channelID string) (stop func())
	OnMessageCompleted   func(msg db.Message, finalStatus string)
	MemoryRetrieverFunc  MemoryRetrieverFunc
	ResolveChannelPolicy func(channelID, channelName string) config.ChannelPolicy
	HistoryFetcher       HistoryFetcherFunc
	SystemAlertFunc      func(s *discordgo.Session, channelNameOrID, title, alertBody string) error
	LLMFunc              LLMFunc
}

type threadWorkerState struct {
	ch              chan db.Message
	activeEnqueuers int
	inFlight        bool
}

type WorkerPool struct {
	appCfg           *config.Config
	overrideModel    string
	cfg              WorkerPoolConfig
	mu               sync.Mutex
	threadChs        map[string]*threadWorkerState
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
	stopped          bool
	quotaLockedUntil atomic.Int64
	scopeLocks       sync.Map
}

// New creates a new WorkerPool with pure *config.Config dependency injection.
func New(appCfg *config.Config, cfg WorkerPoolConfig) *WorkerPool {
	if cfg.Store == nil && cfg.DB != nil {
		cfg.Store = db.NewSQLStore(cfg.DB)
	}
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
		runnerFn := cfg.RunnerFunc
		cfg.NotifierFunc = func(agyBin, apiKey, contextDescription string) string {
			return notifier.GenerateDynamicNotification(agyBin, apiKey, contextDescription, runnerFn)
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
	if cfg.MemoryClient == nil {
		cfg.MemoryClient = memory.New(appCfg)
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

	ctx, cancel := context.WithCancel(context.Background())

	p := &WorkerPool{
		appCfg:    appCfg,
		cfg:       cfg,
		threadChs: make(map[string]*threadWorkerState),
		ctx:       ctx,
		cancel:    cancel,
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
					sysChan = config.GetSystemChannel()
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
}

func (p *WorkerPool) Stop() {
	p.StopWithTimeout(p.cfg.DrainTimeout)
	log.Printf("[WorkerPool] Queue worker pool stopped cleanly")
}

func (p *WorkerPool) Enqueue(msg db.Message) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		log.Printf("[WorkerPool] Warning: attempted to enqueue message %s to stopped pool", msg.ID)
		return
	}

	state, exists := p.threadChs[msg.ThreadID]
	if !exists {
		state = &threadWorkerState{ch: make(chan db.Message, 100)}
		p.threadChs[msg.ThreadID] = state
		p.wg.Add(1)
		go p.runThreadWorker(msg.ThreadID, state)
	}

	// Fast path: non-blocking send under lock
	select {
	case state.ch <- msg:
		metrics.QueueDepth.Inc()
		p.mu.Unlock()
		return
	default:
	}

	// Buffer full: track active enqueuer to block worker eviction while waiting outside lock
	state.activeEnqueuers++
	ch := state.ch
	p.mu.Unlock()

	select {
	case ch <- msg:
		metrics.QueueDepth.Inc()
	case <-p.ctx.Done():
		log.Printf("[WorkerPool] Context cancelled while enqueuing message %s", msg.ID)
	}

	p.mu.Lock()
	state.activeEnqueuers--
	p.mu.Unlock()
}

func (p *WorkerPool) runThreadWorker(threadID string, state *threadWorkerState) {
	defer p.wg.Done()

	idleTimeout := p.cfg.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}
	idleTimer := time.NewTimer(idleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-state.ch:
			if !ok {
				return
			}
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(idleTimeout)

			metrics.QueueDepth.Dec()
			burst := []db.Message{msg}
		DrainLoop:
			for len(burst) < 5 {
				select {
				case extra := <-state.ch:
					metrics.QueueDepth.Dec()
					burst = append(burst, extra)
				default:
					break DrainLoop
				}
			}

			p.mu.Lock()
			state.inFlight = true
			p.mu.Unlock()

			p.processBurst(burst)

			p.mu.Lock()
			state.inFlight = false
			p.mu.Unlock()

			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(idleTimeout)

		case <-idleTimer.C:
			p.mu.Lock()
			if len(state.ch) == 0 && state.activeEnqueuers == 0 {
				delete(p.threadChs, threadID)
				p.scopeLocks.Delete(threadID)
				p.mu.Unlock()
				return
			}
			p.mu.Unlock()
			idleTimer.Reset(idleTimeout)
		}
	}
}

func extractMessageBody(content string) string {
	trimmed := strings.TrimSpace(content)
	if strings.Contains(trimmed, "<USER_REQUEST>") {
		contentMarker := "- content:"
		idx := strings.Index(trimmed, contentMarker)
		if idx != -1 {
			start := idx + len(contentMarker)
			rest := trimmed[start:]

			// Find boundary of next envelope field
			endIdx := -1
			markers := []string{"\n- timestamp:", "\n- mentions:", "\n- attachments:", "\n- ", "\n</USER_REQUEST>"}
			for _, m := range markers {
				if pos := strings.Index(rest, m); pos != -1 {
					if endIdx == -1 || pos < endIdx {
						endIdx = pos
					}
				}
			}
			var val string
			if endIdx != -1 {
				val = rest[:endIdx]
			} else {
				val = rest
			}
			val = strings.TrimSpace(val)
			val = strings.ReplaceAll(val, "<\\/USER_REQUEST>", "</USER_REQUEST>")
			val = strings.ReplaceAll(val, "<\\USER_REQUEST>", "<USER_REQUEST>")
			return val
		}
		inner := strings.TrimPrefix(trimmed, "<USER_REQUEST>")
		inner = strings.TrimSuffix(inner, "</USER_REQUEST>")
		return strings.TrimSpace(inner)
	}
	return trimmed
}

// ResolveBotRoleIDs returns all role IDs associated with the bot in the given guild.
// This includes roles held by the bot member and any managed integration roles matching the bot name.
func ResolveBotRoleIDs(sess *discordgo.Session, guildID string, botUserID string) []string {
	if sess == nil || sess.State == nil {
		return nil
	}

	roleSet := make(map[string]bool)

	// If guildID is specified, inspect that guild; otherwise inspect all cached guilds
	var guilds []*discordgo.Guild
	if guildID != "" {
		if g, err := sess.State.Guild(guildID); err == nil && g != nil {
			guilds = append(guilds, g)
		}
	} else {
		guilds = sess.State.Guilds
	}

	botUsername := ""
	if sess.State.User != nil {
		botUsername = sess.State.User.Username
	}

	for _, g := range guilds {
		if g == nil {
			continue
		}
		// 1. Roles assigned to the bot member
		if botUserID != "" {
			if member, err := sess.State.Member(g.ID, botUserID); err == nil && member != nil {
				for _, rID := range member.Roles {
					if rID != "" {
						roleSet[rID] = true
					}
				}
			}
		}
		// 2. Roles in guild matching bot name or username
		for _, r := range g.Roles {
			if r == nil {
				continue
			}
			if strings.EqualFold(r.Name, "aerial") || strings.EqualFold(r.Name, "gundam") || (botUsername != "" && strings.EqualFold(r.Name, botUsername)) {
				roleSet[r.ID] = true
			}
		}
	}

	var res []string
	for rID := range roleSet {
		res = append(res, rID)
	}
	sort.Strings(res)
	return res
}

func isTier1Wake(m db.Message, botUserID string, botRoleIDs []string, wakeMode string) bool {
	if m.AuthorID == "http-client" || m.AuthorID == "scheduler" || m.ScheduleRunID != "" {
		return true
	}

	// Direct user mentions: <@botUserID>, <@!botUserID>
	if botUserID != "" {
		if strings.Contains(m.Content, "<@"+botUserID+">") || strings.Contains(m.Content, "<@!"+botUserID+">") {
			return true
		}
	}

	// Direct role mentions: <@&botRoleID> for any role associated with the bot
	for _, rID := range botRoleIDs {
		if rID != "" && strings.Contains(m.Content, "<@&"+rID+">") {
			return true
		}
	}

	// Mentions list in Discord prompt envelope containing bot name, botUserID, or botRoleIDs
	if idx := strings.Index(m.Content, "- mentions: ["); idx != -1 {
		if endIdx := strings.Index(m.Content[idx:], "]"); endIdx != -1 {
			inside := strings.ToLower(m.Content[idx+len("- mentions: [") : idx+endIdx])
			if strings.Contains(inside, "aerial") || (botUserID != "" && strings.Contains(inside, strings.ToLower(botUserID))) {
				return true
			}
			for _, rID := range botRoleIDs {
				if rID != "" && strings.Contains(inside, strings.ToLower(rID)) {
					return true
				}
			}
		}
	}

	// Explicit reply to Aerial: check ONLY the author line under - replying_to:
	if idx := strings.Index(m.Content, "- replying_to:"); idx != -1 {
		lines := strings.Split(m.Content[idx:], "\n")
		for _, line := range lines {
			lineTrimmed := strings.TrimSpace(line)
			if strings.HasPrefix(lineTrimmed, "author:") {
				authorVal := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(lineTrimmed, "author:")))
				if strings.Contains(authorVal, "aerial") || (botUserID != "" && strings.Contains(authorVal, strings.ToLower(botUserID))) {
					return true
				}
				break
			}
			if lineTrimmed != "- replying_to:" && strings.HasPrefix(lineTrimmed, "- ") {
				break
			}
		}
	}

	// Check message body direct mentions
	body := extractMessageBody(m.Content)
	if botUserID != "" && (strings.Contains(body, "<@"+botUserID+">") || strings.Contains(body, "<@!"+botUserID+">")) {
		return true
	}
	for _, rID := range botRoleIDs {
		if rID != "" && strings.Contains(body, "<@&"+rID+">") {
			return true
		}
	}
	bodyLower := strings.ToLower(body)
	if strings.Contains(bodyLower, "<@aerial") || strings.Contains(bodyLower, "<@!aerial") {
		return true
	}

	// If wake_mode is "mention", plaintext keywords / name drops do NOT trigger a wake.
	if strings.ToLower(strings.TrimSpace(wakeMode)) == "mention" {
		return false
	}

	// Keyword trigger matching word boundary regex (?i)\b(aerial|gundam)\b in extractMessageBody(m.Content)
	// excluding "aerial view" and "aerial photo"
	cleanedBody := aerialExclusionRegex.ReplaceAllString(body, "")
	return tier1KeywordRegex.MatchString(cleanedBody)
}

// CoalesceBurstPrompt formats a burst of messages into a single coalesced multi-message prompt turn.
func CoalesceBurstPrompt(burst []db.Message) string {
	if len(burst) == 0 {
		return ""
	}
	if len(burst) == 1 {
		return burst[0].Content
	}

	var sb strings.Builder
	sb.WriteString("<USER_REQUEST>\n[Multiple messages received in channel]\n")
	for i, m := range burst {
		author := m.AuthorName
		if author == "" {
			author = "user"
		}
		if !strings.HasPrefix(author, "@") {
			author = "@" + author
		}
		timeStr := m.CreatedAt.Format("15:04:05")
		if m.CreatedAt.IsZero() {
			timeStr = time.Now().UTC().Format("15:04:05")
		}
		body := extractMessageBody(m.Content)
		sb.WriteString(fmt.Sprintf("--- Message %d (by %s at %s) ---\n%s\n\n", i+1, author, timeStr, body))
	}
	sb.WriteString("</USER_REQUEST>")
	return strings.TrimSpace(sb.String())
}

func parseDBTime(val any) (time.Time, bool) {
	if val == nil {
		return time.Time{}, false
	}
	switch v := val.(type) {
	case time.Time:
		return v, true
	case *time.Time:
		if v != nil {
			return *v, true
		}
		return time.Time{}, false
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return time.Time{}, false
		}
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05 -0700 MST",
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05.999999999Z07:00",
			"2006-01-02 15:04:05-07:00",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
		} {
			if t, err := time.Parse(layout, trimmed); err == nil {
				return t, true
			}
		}
	case []byte:
		return parseDBTime(string(v))
	}
	return time.Time{}, false
}

// GetSessionLastActivity queries the database and on-disk session logs to determine the most
// recent activity for a thread's session.
//
// It inspects:
// 1. The `sessions` table (internal_session_id, turn_count, updated_at).
// 2. The `messages` table for genuine completed turns (filtering out [EXPIRED_STALE], [AMBIENT, [IGNORED).
// 3. On-disk logs and task outputs via session.GetSessionLastActivity(internal_session_id).
//
// A thread is considered cold/new (isColdThread: true) only if completedCount == 0, turnCount == 0,
// and diskActivity.IsZero(). Rotated sessions with turn_count == 0 or empty internal_session_id
// but with prior completed messages are recognized as existing sessions.
// If database queries fail (excluding sql.ErrNoRows), the error is returned to allow fail-open semantics.
func GetSessionLastActivity(database db.DBTX, threadID string) (time.Time, bool, error) {
	if database == nil || strings.TrimSpace(threadID) == "" {
		return time.Time{}, true, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		internalSessionID sql.NullString
		turnCount         sql.NullInt64
		rawSessUpdatedAt  any
	)

	err := database.QueryRowContext(
		ctx,
		`SELECT internal_session_id, turn_count, updated_at FROM sessions WHERE thread_id = $1`,
		threadID,
	).Scan(&internalSessionID, &turnCount, &rawSessUpdatedAt)

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, fmt.Errorf("querying session for thread %s: %w", threadID, err)
	}

	var (
		completedCount int64
		rawMsgUpdated  any
	)

	msgErr := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*), MAX(updated_at) FROM messages 
		 WHERE thread_id = $1 
		   AND status = 'COMPLETED' 
		   AND response_text NOT LIKE '[EXPIRED_STALE]%' 
		   AND response_text NOT LIKE '[AMBIENT%' 
		   AND response_text NOT LIKE '[IGNORED%'`,
		threadID,
	).Scan(&completedCount, &rawMsgUpdated)

	if msgErr != nil && !errors.Is(msgErr, sql.ErrNoRows) {
		return time.Time{}, false, fmt.Errorf("querying completed messages for thread %s: %w", threadID, msgErr)
	}

	var diskActivity time.Time
	if internalSessionID.Valid && strings.TrimSpace(internalSessionID.String) != "" {
		diskActivity, _ = session.GetSessionLastActivity(internalSessionID.String)
	}

	turns := int64(0)
	if turnCount.Valid {
		turns = turnCount.Int64
	}

	// A thread is cold only if it has zero completed messages, zero turns, and no disk activity.
	if completedCount == 0 && turns == 0 && diskActivity.IsZero() {
		return time.Time{}, true, nil
	}

	var latestActivity time.Time
	if t, ok := parseDBTime(rawSessUpdatedAt); ok && t.After(latestActivity) {
		latestActivity = t
	}
	if t, ok := parseDBTime(rawMsgUpdated); ok && t.After(latestActivity) {
		latestActivity = t
	}
	if diskActivity.After(latestActivity) {
		latestActivity = diskActivity
	}

	return latestActivity, false, nil
}

func (p *WorkerPool) processBurst(burst []db.Message) {
	if len(burst) == 0 {
		return
	}
	if p.ctx.Err() != nil {
		return
	}

	var isQuotaPaused bool

	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()

	threadID := burst[0].ThreadID
	log.Printf("[WorkerPool] Processing burst of %d message(s) for thread %s", len(burst), threadID)

	lockVal, _ := p.scopeLocks.LoadOrStore(threadID, &sync.Mutex{})
	scopeLock := lockVal.(*sync.Mutex)
	scopeLock.Lock()
	defer scopeLock.Unlock()

	triggerType := "discord"
	if burst[0].ScheduleRunID != "" {
		triggerType = "schedule"
	} else if burst[0].AuthorID == "http-client" {
		triggerType = "http"
	}

	// 1. Claim messages from PENDING to PROCESSING
	var claimedBurst []db.Message
	for _, m := range burst {
		claimed, claimErr := p.cfg.Store.ClaimPendingMessage(p.ctx, m.ID)
		if claimErr != nil {
			log.Printf("[WorkerPool] Failed to claim message %s: %v", m.ID, claimErr)
			continue
		}
		if !claimed {
			log.Printf("[WorkerPool] Skipping message %s: already claimed or completed", m.ID)
			continue
		}
		claimedBurst = append(claimedBurst, m)
	}
	if len(claimedBurst) == 0 {
		return
	}
	burst = claimedBurst

	// 2. Staleness TTL check (for burst, check latest message, default 30m)
	stalenessTTL := p.cfg.StalenessTTL
	if stalenessTTL <= 0 {
		stalenessTTL = 30 * time.Minute
	}

	latestMsg := burst[0]
	for _, m := range burst[1:] {
		if m.CreatedAt.After(latestMsg.CreatedAt) {
			latestMsg = m
		}
	}

	hasRecovered := false
	for _, m := range burst {
		if m.RetryCount > 0 || m.RestartCount > 0 {
			hasRecovered = true
			break
		}
	}

	dropBurstAsStale := func(reason string) {
		log.Printf("[WorkerPool] Dropping stale message(s) in thread %s (%s). Marked [EXPIRED_STALE].", threadID, reason)
		metrics.RecordTurnCompleted("stale", triggerType, "none", time.Since(latestMsg.CreatedAt))
		for _, m := range burst {
			_ = db.UpdateMessageCompleted(p.cfg.DB, m.ID, "[EXPIRED_STALE]")
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "completed",
					CompletedAt: time.Now().UTC(),
					DurationMs:  0,
					Error:       "[EXPIRED_STALE]",
				})
			}
			if p.cfg.OnMessageCompleted != nil {
				p.cfg.OnMessageCompleted(m, db.StatusCompleted)
			}
		}
	}

	if !latestMsg.CreatedAt.IsZero() {
		msgAge := time.Since(latestMsg.CreatedAt)
		if msgAge > MaxMessageAbsoluteAge {
			dropBurstAsStale(fmt.Sprintf("exceeded hard absolute age cap %v: msg age %v", MaxMessageAbsoluteAge, msgAge))
			return
		}

		if !hasRecovered && msgAge > stalenessTTL {
			lastActivity, isColdThread, err := GetSessionLastActivity(p.cfg.DB, threadID)
			if err != nil {
				// Fail-open: Retain message if DB error occurs during staleness lookup
				log.Printf("[WorkerPool] Warning: failed to query session last activity for thread %s: %v. Retaining message(s) (fail-open).", threadID, err)
			} else if isColdThread {
				// New session: message send time governs staleness
				dropBurstAsStale(fmt.Sprintf("new thread and message age %v > TTL %v", msgAge, stalenessTTL))
				return
			} else {
				// Existing session: session last activity governs staleness
				if time.Since(lastActivity) > stalenessTTL {
					dropBurstAsStale(fmt.Sprintf("existing session inactive for %v > TTL %v", time.Since(lastActivity), stalenessTTL))
					return
				}
				log.Printf("[WorkerPool] Retaining message in thread %s (msg age %v > TTL %v) because session had recent activity %v ago <= TTL.",
					threadID, msgAge, stalenessTTL, time.Since(lastActivity))
			}
		}
	}

	execStart := time.Now().UTC()
	for _, m := range burst {
		if m.ScheduleRunID != "" {
			_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
				RunID:     m.ScheduleRunID,
				MessageID: m.ID,
				Status:    "running",
			})
		}
	}

	// Resolve Channel Policy (inheriting parent channel policy if in a thread)
	var policy config.ChannelPolicy
	effectiveID, effectiveName, isThread := ResolveEffectiveChannel(p.getDiscordSession(), threadID)
	if p.cfg.ResolveChannelPolicy != nil {
		policy = p.cfg.ResolveChannelPolicy(effectiveID, effectiveName)
	} else {
		policy = config.GetRuntimeConfig().ResolveChannelPolicy(effectiveID, effectiveName)
	}

	skipDiscord := true
	for _, m := range burst {
		if m.AuthorID != "http-client" {
			skipDiscord = false
			break
		}
	}

	if !skipDiscord && policy.IsIgnored() {
		log.Printf("[WorkerPool] Channel %s policy is ignored (mode=%s). Marking %d message(s) completed without execution.", threadID, policy.Mode, len(burst))
		metrics.RecordTurnCompleted("ignored", triggerType, "none", time.Since(execStart))
		for _, m := range burst {
			_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusCompleted, fmt.Sprintf("[%s]", strings.ToUpper(strings.TrimSpace(policy.Mode))))
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "completed",
					CompletedAt: time.Now().UTC(),
				})
			}
			if p.cfg.OnMessageCompleted != nil {
				p.cfg.OnMessageCompleted(m, db.StatusCompleted)
			}
		}
		return
	}

	stopTyping := func() {}
	statusUpdater := NewStatusUpdater(p.getDiscordSession(), threadID, isThread && !skipDiscord)
	defer func() {
		stopTyping()
		statusUpdater.Stop()
		statusUpdater.DeleteStatusMessage()
	}()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WorkerPool] Panic in processBurst for thread %s: %v", threadID, r)
			stopTyping()
			statusUpdater.Stop()
			statusUpdater.DeleteStatusMessage()
			metrics.RecordTurnCompleted("panic", triggerType, p.GetRuntimeConfig(), time.Since(execStart))
			errMsg := sanitizeErrorText(fmt.Sprintf("panic: %v", r))
			for _, m := range burst {
				_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusFailed, errMsg)
				if m.ScheduleRunID != "" {
					_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
						RunID:       m.ScheduleRunID,
						MessageID:   m.ID,
						Status:      "failed",
						CompletedAt: time.Now().UTC(),
						DurationMs:  time.Since(execStart).Milliseconds(),
						Error:       errMsg,
					})
				}
				if p.cfg.OnMessageCompleted != nil {
					p.cfg.OnMessageCompleted(m, db.StatusFailed)
				}
			}
		}
	}()

	currentSessionID, _ := db.GetSessionID(p.cfg.DB, threadID)
	var turnCount int

	var turnHistory []HistoryMessage
	var turnHistoryFetched bool
	wakeIdx := -1
	getTurnHistory := func(limit int) []HistoryMessage {
		if turnHistoryFetched {
			if len(turnHistory) > limit {
				return turnHistory[:limit]
			}
			return turnHistory
		}
		fetchCtx, fetchCancel := context.WithTimeout(p.ctx, 3*time.Second)
		defer fetchCancel()
		var err error
		if p.cfg.HistoryFetcher != nil {
			turnHistory, err = p.cfg.HistoryFetcher(fetchCtx, threadID, burst[0].ID, limit)
		} else {
			turnHistory, err = FetchRecentThreadHistory(fetchCtx, p.getDiscordSession(), p.cfg.DB, threadID, limit)
		}
		if err != nil {
			log.Printf("[WorkerPool] Warning: History fetch failed for thread %s: %v", threadID, err)
		}
		turnHistoryFetched = true
		if len(turnHistory) > limit {
			return turnHistory[:limit]
		}
		return turnHistory
	}

	if strings.ToLower(policy.Mode) == "channel" {
		botUserID := ""
		var botRoleIDs []string
		if sess := p.getDiscordSession(); sess != nil && sess.State != nil {
			if sess.State.User != nil {
				botUserID = sess.State.User.ID
			}
			guildID := burst[0].GuildID
			botRoleIDs = ResolveBotRoleIDs(sess, guildID, botUserID)
		}

		wakeMode := policy.GetWakeMode()

		type wakeInfo struct {
			isWake    bool
			score     float64
			threshold float64
			reason    string
		}
		wakeInfos := make([]wakeInfo, len(burst))
		var trailingMsgs []db.Message
		var trailingInfos []wakeInfo
		wakeIdx = -1

		// Safeguard 1: Tier-1 Pre-Scan (Zero-Latency Priority)
		for i, m := range burst {
			if isTier1Wake(m, botUserID, botRoleIDs, wakeMode) {
				wakeInfos[i] = wakeInfo{
					isWake:    true,
					score:     1.0,
					threshold: policy.GetAmbientWakeThreshold(),
					reason:    "direct_address",
				}
				if wakeIdx == -1 {
					wakeIdx = i
				}
			}
		}

		if wakeIdx != -1 {
			// Direct Tier-1 wake found!
			// Any leading ambient messages [0..wakeIdx-1] are marked ambient with zero classifier delay.
			for i := 0; i < wakeIdx; i++ {
				wakeInfos[i] = wakeInfo{
					isWake:    false,
					score:     0.0,
					threshold: policy.GetAmbientWakeThreshold(),
					reason:    "burst_prescan_leading",
				}
			}
			// Trailing messages after wakeIdx:
			// If Tier-1, mark isWake: true so handleTrailing re-enqueues it for the next turn.
			// Otherwise mark ambient (or classify if threshold > 0 and wakeMode == "classifier").
			threshold := policy.GetAmbientWakeThreshold()
			for i := wakeIdx + 1; i < len(burst); i++ {
				m := burst[i]
				if isTier1Wake(m, botUserID, botRoleIDs, wakeMode) {
					wakeInfos[i] = wakeInfo{
						isWake:    true,
						score:     1.0,
						threshold: threshold,
						reason:    "direct_address",
					}
				} else if wakeMode == "mention" || threshold <= 0.0 || classifier.IsHeuristicSkip(extractMessageBody(m.Content)) {
					wakeInfos[i] = wakeInfo{
						isWake:    false,
						score:     0.0,
						threshold: threshold,
						reason:    "heuristic_skip",
					}
				} else {
					var recentContext []db.Message
					if p.cfg.DB != nil {
						recentContext, _ = db.GetRecentThreadMessages(p.cfg.DB, threadID, 10)
					}
					var res classifier.ClassificationResult
					if p.cfg.Classifier != nil {
						res = p.cfg.Classifier.Classify(p.ctx, m, recentContext, policy.GetAmbientWakePrompt())
					} else {
						res = classifier.ClassificationResult{Confidence: 0.0, Reason: "no classifier configured"}
					}
					wakeInfos[i] = wakeInfo{
						isWake:    res.Confidence >= threshold,
						score:     res.Confidence,
						threshold: threshold,
						reason:    res.Reason,
					}
				}
			}
		} else if wakeMode == "mention" {
			// Mention-only mode: all non-mention messages are ambient without running classifier
			for i := range burst {
				wakeInfos[i] = wakeInfo{
					isWake:    false,
					score:     0.0,
					threshold: 0.0,
					reason:    "mention_mode_ambient",
				}
			}
		} else if wakeMode == "all" {
			for i := range burst {
				wakeInfos[i] = wakeInfo{
					isWake:    i == 0,
					score:     1.0,
					threshold: 0.0,
					reason:    "all_wake",
				}
			}
			wakeIdx = 0
		} else {
			// Safeguard 2: Coalesced Burst Ambient Evaluation (classifier mode)
			threshold := policy.GetAmbientWakeThreshold()
			if threshold <= 0.0 {
				for i := range burst {
					wakeInfos[i] = wakeInfo{
						isWake:    false,
						score:     0.0,
						threshold: threshold,
						reason:    "classifier disabled",
					}
				}
			} else {
				// Fast pre-filter: if all messages in burst are banter/emojis/commands, skip without LLM
				allSkip := true
				for _, m := range burst {
					if !classifier.IsHeuristicSkip(extractMessageBody(m.Content)) {
						allSkip = false
						break
					}
				}
				if allSkip {
					for i := range burst {
						wakeInfos[i] = wakeInfo{
							isWake:    false,
							score:     0.0,
							threshold: threshold,
							reason:    "heuristic_skip",
						}
					}
				} else {
					var recentContext []db.Message
					if p.cfg.DB != nil {
						recentContext, _ = db.GetRecentThreadMessages(p.cfg.DB, threadID, 10)
					}
					var res classifier.ClassificationResult
					if p.cfg.Classifier != nil {
						res = p.cfg.Classifier.ClassifyBurst(p.ctx, burst, recentContext, policy.GetAmbientWakePrompt())
					} else {
						res = classifier.ClassificationResult{Confidence: 0.0, Reason: "no classifier configured"}
					}
					isWake := res.Confidence >= threshold
					log.Printf("[AmbientClassifier] Channel %s | BurstSize %d | Score: %.2f (Threshold: %.2f) | Wake: %t | Reason: %s",
						threadID, len(burst), res.Confidence, threshold, isWake, res.Reason)

					for i := range burst {
						wakeInfos[i] = wakeInfo{
							isWake:    i == 0 && isWake,
							score:     res.Confidence,
							threshold: threshold,
							reason:    res.Reason,
						}
					}
					if isWake {
						wakeIdx = 0
					}
				}
			}
		}

		channelName := effectiveName
		if channelName == "" {
			channelName = threadID
		}

		if wakeIdx == -1 {
			if p.ctx.Err() != nil {
				log.Printf("[WorkerPool] Context cancelled during ambient classification for thread %s. Preserving burst in PROCESSING for startup recovery.", threadID)
				metrics.RecordTurnCompleted("cancelled", triggerType, "classifier", time.Since(execStart))
				return
			}

			// ALL messages in burst are ambient
			hasDiskSession := currentSessionID != "" && session.SessionExistsOnDisk(currentSessionID)
			if hasDiskSession {
				_, _ = session.EnsureSessionDir(currentSessionID)
			}

			metrics.RecordTurnCompleted("ambient", triggerType, "classifier", time.Since(execStart))

			for i, m := range burst {
				info := wakeInfos[i]
				metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
				telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.score, info.threshold, info.reason)
				_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusCompleted, telemetry)
				if m.ScheduleRunID != "" {
					_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
						RunID:       m.ScheduleRunID,
						MessageID:   m.ID,
						Status:      "completed",
						CompletedAt: time.Now().UTC(),
					})
				}
				if p.cfg.OnMessageCompleted != nil {
					p.cfg.OnMessageCompleted(m, db.StatusCompleted)
				}
			}
			return
		}

		// wakeIdx >= 0
		// Check session rotation timing before Phase 1:
		currentTurns, _ := db.GetSessionTurnCount(p.cfg.DB, threadID)
		if currentTurns >= DefaultMaxSessionTurns || (wakeIdx > 0 && currentTurns+1 >= DefaultMaxSessionTurns) {
			log.Printf("[Queue] Scope session reached turn limit (%d/%d). Resetting to cold state for fresh session initialization.", currentTurns, DefaultMaxSessionTurns)
			_ = db.RotateSessionID(p.cfg.DB, threadID, "")
			currentSessionID = ""
		}

		if currentSessionID == "" {
			currentSessionID, _ = db.GetSessionID(p.cfg.DB, threadID)
		}
		if currentSessionID != "" && !session.SessionExistsOnDisk(currentSessionID) {
			log.Printf("[Queue] Session %s for thread %s not found on disk. Clearing for fresh Turn 1.", currentSessionID, threadID)
			currentSessionID = ""
		}
		if currentSessionID != "" {
			_, _ = session.EnsureSessionDir(currentSessionID)
		}

		// Phase 1 (Leading ambient messages)
		if wakeIdx > 0 {
			for i := 0; i < wakeIdx; i++ {
				m := burst[i]
				info := wakeInfos[i]
				metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
				telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.score, info.threshold, info.reason)
				_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusCompleted, telemetry)
				if m.ScheduleRunID != "" {
					_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
						RunID:       m.ScheduleRunID,
						MessageID:   m.ID,
						Status:      "completed",
						CompletedAt: time.Now().UTC(),
					})
				}
				if p.cfg.OnMessageCompleted != nil {
					p.cfg.OnMessageCompleted(m, db.StatusCompleted)
				}
			}
		}

		// Phase 2 (Active wake batch)
		// Partition trailing messages after the wake message
		if wakeIdx+1 < len(burst) {
			trailingMsgs = burst[wakeIdx+1:]
			trailingInfos = wakeInfos[wakeIdx+1:]
		}
		burst = []db.Message{burst[wakeIdx]}
		metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Inc()

		var incErr error
		turnCount, incErr = db.IncrementSessionTurnCount(p.cfg.DB, threadID)
		if incErr != nil {
			log.Printf("[Queue] Error incrementing turn count for thread %s: %v", threadID, incErr)
		}

		var handledTrailing bool
		handleTrailing := func() {
			if handledTrailing {
				return
			}
			handledTrailing = true
			for i, m := range trailingMsgs {
				info := trailingInfos[i]
				if isQuotaPaused {
					if info.isWake {
						metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Inc()
					} else {
						metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
					}
					telemetry := "[QUOTA_PAUSED] Trailing message suppressed due to active quota lockout"
					_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusFailed, telemetry)
					if m.ScheduleRunID != "" {
						_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
							RunID:       m.ScheduleRunID,
							MessageID:   m.ID,
							Status:      "failed",
							CompletedAt: time.Now().UTC(),
							Error:       telemetry,
						})
					}
					if p.cfg.OnMessageCompleted != nil {
						p.cfg.OnMessageCompleted(m, db.StatusFailed)
					}
					continue
				}
				if !info.isWake {
					metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
					telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.score, info.threshold, info.reason)
					_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusCompleted, telemetry)
					if m.ScheduleRunID != "" {
						_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
							RunID:       m.ScheduleRunID,
							MessageID:   m.ID,
							Status:      "completed",
							CompletedAt: time.Now().UTC(),
						})
					}
					if p.cfg.OnMessageCompleted != nil {
						p.cfg.OnMessageCompleted(m, db.StatusCompleted)
					}
				} else {
					metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Inc()
					_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusPending, "")
					m.Status = db.StatusPending
					p.Enqueue(m)
				}
			}
		}
		defer handleTrailing()
	}

	if !skipDiscord && p.cfg.TypingFunc != nil {
		if stop := p.cfg.TypingFunc(p.getDiscordSession(), threadID); stop != nil {
			stopTyping = stop
		}
	}

	// Pre-execution turn limit rotation check (applies to both channel and thread modes)
	// Run BEFORE IncrementSessionTurnCount to prevent premature rotation and double-rotation.
	currentTurns, _ := db.GetSessionTurnCount(p.cfg.DB, threadID)
	if currentTurns >= DefaultMaxSessionTurns {
		log.Printf("[Queue] Scope session reached turn limit (%d/%d). Resetting to cold state for fresh session initialization.", currentTurns, DefaultMaxSessionTurns)
		_ = db.RotateSessionID(p.cfg.DB, threadID, "")
		currentSessionID = ""
	}

	if strings.ToLower(policy.Mode) != "channel" {
		var incErr error
		turnCount, incErr = db.IncrementSessionTurnCount(p.cfg.DB, threadID)
		if incErr != nil {
			log.Printf("[Queue] Error incrementing turn count for thread %s: %v", threadID, incErr)
		}
	}

	basePrompt := CoalesceBurstPrompt(burst)
	snap, _ := resolveChannelSnapshot(p.getDiscordSession(), threadID)
	isThreadColdStart := snap.IsThread && snap.ParentID != "" && snap.ParentID != snap.ID && currentSessionID == ""

	isColdStart := currentSessionID == ""
	hasHistoryNeed := isColdStart || wakeIdx > 0 || len(burst) > 1

	if isThreadColdStart {
		var summary string
		cachedSum, lastMsgID, _ := db.GetThreadSummary(p.cfg.DB, threadID)

		histMsgs := getTurnHistory(100)

		if len(histMsgs) > 0 {
			var latestMsgID string
			var maxTime time.Time
			for _, m := range histMsgs {
				if m.CreatedAt.After(maxTime) || latestMsgID == "" {
					maxTime = m.CreatedAt
					latestMsgID = m.ID
				}
			}

			if cachedSum != "" && lastMsgID != "" && lastMsgID == latestMsgID {
				summary = cachedSum
				log.Printf("[WorkerPool] Using cached thread summary for thread %s (watermark msg: %s)", threadID, lastMsgID)
			} else {
				llmFn := p.cfg.LLMFunc
				if llmFn == nil && p.cfg.Classifier != nil && p.cfg.Classifier.LLMFunc != nil {
					llmFn = p.cfg.Classifier.LLMFunc
				}
				if llmFn == nil {
					llmFn = classifier.NewAgyLLMFunc(p.cfg.AgyBin, p.cfg.APIKey, p.cfg.RunnerFunc)
				}

				flashModel := ""
				if p.cfg.Classifier != nil && p.cfg.Classifier.Model != "" {
					flashModel = p.cfg.Classifier.Model
				}
				if flashModel == "" && p.appCfg != nil && p.appCfg.Current() != nil {
					cur := p.appCfg.Current()
					if cur.LowEffortModel != "" {
						flashModel = cur.LowEffortModel
					} else {
						flashModel = cur.ClassifierModel
					}
				}
				if flashModel == "" {
					flashModel = p.cfg.Model
				}

				sumCtx, sumCancel := context.WithTimeout(p.ctx, DefaultThreadSummaryTimeout)
				newSum, sumErr := SummarizeThreadHistory(sumCtx, llmFn, flashModel, threadID, histMsgs)
				sumCancel()

				if sumErr != nil {
					log.Printf("[WorkerPool] Warning: Thread history summarization failed for thread %s: %v. Falling back to raw lookback.", threadID, sumErr)
				} else if newSum != "" {
					summary = newSum
					if err := db.SaveThreadSummary(p.cfg.DB, threadID, summary, latestMsgID); err != nil {
						log.Printf("[WorkerPool] Warning: Failed to save thread summary to DB for thread %s: %v", threadID, err)
					}
				}
			}
		}

		if summary != "" {
			basePrompt = summary + "\n\n" + basePrompt
			log.Printf("[WorkerPool] Injected <THREAD_SUMMARY> into Turn 1 prompt for thread %s", threadID)
		}
	}

	if hasHistoryNeed {
		lookbackMsgs := getTurnHistory(10)
		if formattedHist := FormatChannelHistory(lookbackMsgs); formattedHist != "" {
			basePrompt = formattedHist + "\n\n" + basePrompt
			log.Printf("[WorkerPool] Injected channel history into prompt for thread %s", threadID)
		}
	}
	queryText := memory.ExtractQueryText(basePrompt)
	if p.cfg.MemoryRetrieverFunc != nil && p.cfg.DB != nil && strings.TrimSpace(queryText) != "" {
		maxFacts := 10
		if isThreadColdStart {
			maxFacts = 5
		}
		retrievalCtx, retrievalCancel := context.WithTimeout(p.ctx, 2500*time.Millisecond)
		facts, err := p.cfg.MemoryRetrieverFunc(retrievalCtx, p.cfg.DB, p.cfg.MemoryClient, queryText, maxFacts)
		retrievalCancel()
		if err != nil {
			log.Printf("[WorkerPool] Warning: Semantic memory retrieval failed for thread %s: %v. Proceeding without injected facts.", threadID, err)
		} else if len(facts) > 0 {
			if isThreadColdStart && len(facts) > 5 {
				facts = facts[:5]
			}
			memoryBlock := memory.FormatMemoryContext(facts)
			if memoryBlock != "" {
				basePrompt = memoryBlock + "\n\n" + basePrompt
				log.Printf("[WorkerPool] Injected %d semantic memory fact(s) into prompt for thread %s", len(facts), threadID)
			}
		}
	}

	turnPrompt := basePrompt
	if instructions := config.LoadChannelInstructions(effectiveName); instructions != "" {
		turnPrompt = fmt.Sprintf("<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\n%s\n</CHANNEL_INSTRUCTIONS>\n\n%s", instructions, basePrompt)
		log.Printf("[WorkerPool] Injected channel instructions for #%s into prompt", effectiveName)
	}

	maxAttempts := p.cfg.MaxAttempts
	lastErrDetail := ""
	lastStderr := ""
	currentModel := p.GetRuntimeConfig()

	initialRetryCount := 0
	for _, m := range burst {
		if m.RetryCount > initialRetryCount {
			initialRetryCount = m.RetryCount
		}
	}

	for attempt := initialRetryCount + 1; attempt <= maxAttempts; attempt++ {
		p.mu.Lock()
		currentModel = p.cfg.Model
		lowEffortModel := p.cfg.LowEffortModel
		currentTimeout := p.cfg.TimeoutMinutes
		currentAgyBin := p.cfg.AgyBin
		currentAPIKey := p.cfg.APIKey
		overrideModel := p.overrideModel
		p.mu.Unlock()

		if p.appCfg != nil {
			if cur := p.appCfg.Current(); cur != nil {
				if cur.Model != "" {
					currentModel = cur.Model
				}
				if cur.LowEffortModel != "" {
					lowEffortModel = cur.LowEffortModel
				}
				if cur.APIKey != "" {
					currentAPIKey = cur.APIKey
				}
				if cur.AgyBin != "" {
					currentAgyBin = cur.AgyBin
				}
			}
		}

		if lowEffortModel == "" {
			lowEffortModel = config.GetRuntimeConfig().LowEffortModel
		}

		// Check if any message in the burst requested low effort routing
		isLowEffort := false
		for _, m := range burst {
			if strings.EqualFold(strings.TrimSpace(m.Effort), "low") {
				isLowEffort = true
				break
			}
		}
		if isLowEffort && strings.TrimSpace(lowEffortModel) != "" {
			currentModel = lowEffortModel
		}

		if overrideModel != "" {
			currentModel = overrideModel
		}

		// Global Quota Lockout Pre-check: if a quota pause is active across the pool and running in OAuth mode, fail-fast immediately
		if currentAPIKey == "" {
			if lockedUntilUnix := p.quotaLockedUntil.Load(); lockedUntilUnix > 0 {
				lockedUntil := time.Unix(lockedUntilUnix, 0)
				if time.Now().Before(lockedUntil) {
					isQuotaPaused = true
					remaining := time.Until(lockedUntil)
					log.Printf("[WorkerPool] Global quota pause active for thread %s (%v remaining). Aborting turn.", threadID, remaining)
					stopTyping()
					reason := fmt.Sprintf("[QUOTA_PAUSED reset_in=%v scheduled=false] global quota pause active", remaining)
					metrics.RecordRunnerError("quota_paused", currentModel)
					metrics.RecordTurnCompleted("quota_paused", triggerType, currentModel, time.Since(execStart))
					if !skipDiscord && p.cfg.DeliveryFunc != nil {
						pauseMsg := notifier.FormatQuotaPauseMessage(remaining, lockedUntil, false, false)
						_ = p.cfg.DeliveryFunc(p.getDiscordSession(), threadID, pauseMsg)
					}
					for _, m := range burst {
						_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusFailed, reason)
						if m.ScheduleRunID != "" {
							_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "failed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(execStart).Milliseconds(),
								Error:       reason,
								Model:       currentModel,
							})
						}
						if p.cfg.OnMessageCompleted != nil {
							p.cfg.OnMessageCompleted(m, db.StatusFailed)
						}
					}
					return
				}
			}
		}

		promptToSend := turnPrompt
		if attempt > 1 {
			if (runner.IsInactivityTimeout(lastErrDetail, lastStderr) || strings.Contains(lastErrDetail, "max duration exceeded")) && currentSessionID != "" && session.SessionExistsOnDisk(currentSessionID) {
				promptToSend = fmt.Sprintf(ContinuationPromptTemplate, turnPrompt)
			}
		}

		// Pad Go context by +1 minute relative to runner watchdog ceiling so the runner watchdog always fires cleanly
		runCtx, runCancel := context.WithTimeout(p.ctx, time.Duration(currentTimeout+1)*time.Minute)

		var stdout, stderr string
		var exitCode int
		var err error

		if p.cfg.RunnerWithOptionsFunc != nil {
			watchdogOpts := runner.DefaultWatchdogOptions(currentTimeout)
			if statusUpdater != nil {
				statusUpdater.MarkTurnStarted()
				watchdogOpts.StepUpdateHandler = statusUpdater.HandleStep
			}
			stdout, stderr, exitCode, err = p.cfg.RunnerWithOptionsFunc(
				runCtx,
				currentAgyBin,
				promptToSend,
				currentSessionID,
				currentAPIKey,
				currentModel,
				watchdogOpts,
			)
		} else if p.cfg.RunnerFunc != nil {
			stdout, stderr, exitCode, err = p.cfg.RunnerFunc(
				runCtx,
				currentAgyBin,
				promptToSend,
				currentSessionID,
				currentAPIKey,
				currentModel,
				currentTimeout,
			)
		}
		runCancel()

		isFailure, isTransient, isSessionCorruption, errDetail := runner.ClassifyError(exitCode, stdout, stderr)
		if err != nil && errDetail == "" {
			errDetail = err.Error()
		}

		if isFailure && isSessionCorruption && currentSessionID == "" {
			lowerErr := strings.ToLower(errDetail + " " + stderr + " " + stdout)
			if strings.Contains(lowerErr, "context window") || strings.Contains(lowerErr, "context length") || strings.Contains(lowerErr, "maximum context length") || strings.Contains(lowerErr, "token limit exceeded") || strings.Contains(lowerErr, "prompt is too long") || strings.Contains(lowerErr, "request too large") {
				isSessionCorruption = false
				isTransient = false
				errDetail = "prompt length exceeds maximum model context window (hard failure)"
				log.Printf("[Queue] Cold start context window exceeded for thread %s; converting to non-transient fail-fast", threadID)
			}
		}

		lastErrDetail = errDetail
		lastStderr = stderr

		if isFailure {
			targetSess := currentSessionID
			if targetSess == "" {
				combinedOutput := stdout + "\n" + stderr
				if extSess := runner.ExtractSessionID(combinedOutput, execStart); extSess != "" && session.SessionExistsOnDisk(extSess) {
					targetSess = extSess
				}
			}

			// Transcript Recovery on Exit Code 0:
			// If agy completed with exit code 0 but was flagged as a failure (e.g. empty stdout from buffering,
			// or stream-json reporting an error status despite the model successfully generating a response),
			// check if the session transcript on disk contains a valid PLANNER_RESPONSE turn.
			if exitCode == 0 && targetSess != "" && session.SessionExistsOnDisk(targetSess) {
				if respText, _ := session.ExtractResponseAndError(targetSess); respText != "" && !strings.HasPrefix(respText, "[Tool Call Requested]:") {
					log.Printf("[Queue] Recovered response directly from session %s transcript after runner failure on exit 0", targetSess)
					isFailure = false
					isSessionCorruption = false
					isTransient = false
					if currentSessionID == "" {
						currentSessionID = targetSess
						_ = db.SaveSessionID(p.cfg.DB, threadID, currentSessionID)
					}
					stdout = fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":%q}`, currentSessionID, respText)
				}
			}

			// Cold-Start Dynamic Session Latching:
			// If this was a cold start and remains an unrecovered failure, only latch the active session
			// if the failure was NOT session corruption (so retries won't inherit corrupted state).
			if isFailure && currentSessionID == "" && !isSessionCorruption && targetSess != "" {
				log.Printf("[Queue] Latched active session from output on failure (attempt %d/%d) for thread %s: %s", attempt, maxAttempts, threadID, targetSess)
				currentSessionID = targetSess
				_ = db.SaveSessionID(p.cfg.DB, threadID, currentSessionID)
			}
		}

		if isFailure {
			// Quota Lockout Fail-Fast & Auto-Retry Check
			if runner.IsQuotaPause(errDetail, stderr) {
				isQuotaPaused = true
				stopTyping()
				log.Printf("[WorkerPool] Quota pause detected for thread %s on attempt %d/%d: %s", threadID, attempt, maxAttempts, errDetail)

				// Cold-start dynamic session latching if available
				if currentSessionID == "" && !isSessionCorruption {
					combinedOutput := stdout + "\n" + stderr
					if extSess := runner.ExtractSessionID(combinedOutput, execStart); extSess != "" && session.SessionExistsOnDisk(extSess) {
						currentSessionID = extSess
						_ = db.SaveSessionID(p.cfg.DB, threadID, currentSessionID)
					}
				}

				// Circuit breaker: check if this turn was already a quota auto-retry
				isAlreadyRetry := burst[0].ScheduleRunID != "" || strings.HasPrefix(burst[0].Content, "[QUOTA_RETRY]") || strings.HasPrefix(basePrompt, "[QUOTA_RETRY]")

				resetDur, _ := runner.ExtractQuotaResetDuration(errDetail, stderr)
				jitterSec := 5 + rand.Intn(16) // 5s to 20s jitter
				runAt := time.Now().UTC().Add(resetDur).Add(30 * time.Second).Add(time.Duration(jitterSec) * time.Second)

				p.quotaLockedUntil.Store(runAt.Unix())

				var scheduled bool
				if !isAlreadyRetry {
					retryPrompt := fmt.Sprintf("[QUOTA_RETRY] %s", basePrompt)
					oneShotID := uuid.New().String()
					oneShot := db.OneShotSchedule{
						ID:        oneShotID,
						ThreadID:  threadID,
						Prompt:    retryPrompt,
						RunAt:     runAt,
						CreatedAt: time.Now().UTC(),
					}
					if err := db.CreateOneShotSchedule(p.cfg.DB, oneShot); err != nil {
						log.Printf("[WorkerPool] Failed to create one-shot retry schedule for thread %s: %v", threadID, err)
					} else {
						scheduled = true
						log.Printf("[WorkerPool] Scheduled one-shot retry %s for thread %s at %s (+%v)", oneShotID, threadID, runAt.Format(time.RFC3339), resetDur)
					}
				} else {
					log.Printf("[WorkerPool] Circuit breaker: turn for thread %s was already an auto-retry. Skipping further scheduling.", threadID)
				}

				metrics.RecordRunnerError("quota_paused", currentModel)
				metrics.RecordTurnCompleted("quota_paused", triggerType, currentModel, time.Since(execStart))

				stopTyping()
				statusUpdater.Stop()
				statusUpdater.DeleteStatusMessage()

				if !skipDiscord && p.cfg.DeliveryFunc != nil {
					pauseMsg := notifier.FormatQuotaPauseMessage(resetDur, runAt, scheduled, isAlreadyRetry)
					if err := p.cfg.DeliveryFunc(p.getDiscordSession(), threadID, pauseMsg); err != nil {
						log.Printf("[WorkerPool] Failed to deliver quota pause notice for thread %s: %v", threadID, err)
					}
				}

				reason := fmt.Sprintf("[QUOTA_PAUSED reset_in=%v scheduled=%t] %s", resetDur, scheduled, sanitizeErrorText(errDetail))
				for _, m := range burst {
					_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusFailed, reason)
					if m.ScheduleRunID != "" {
						_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
							RunID:       m.ScheduleRunID,
							MessageID:   m.ID,
							Status:      "failed",
							CompletedAt: time.Now().UTC(),
							DurationMs:  time.Since(execStart).Milliseconds(),
							Error:       reason,
							Model:       currentModel,
						})
					}
					if p.cfg.OnMessageCompleted != nil {
						p.cfg.OnMessageCompleted(m, db.StatusFailed)
					}
				}

				return
			}

			isWatchdog := runner.IsInactivityTimeout(errDetail, stderr) || strings.Contains(errDetail, "[watchdog]") || strings.Contains(errDetail, "inactivity timeout exceeded") || strings.Contains(errDetail, "max duration exceeded")

			errCat := "non_transient"
			if isWatchdog {
				errCat = "watchdog_timeout"
			} else if isTransient {
				errCat = "transient"
			} else if isSessionCorruption {
				errCat = "session_corrupt"
			}
			metrics.RecordRunnerError(errCat, currentModel)

			if isWatchdog {
				for _, m := range burst {
					_ = db.IncrementMessageRetry(p.cfg.DB, m.ID, errDetail)
				}
				if attempt < maxAttempts {
					statusUpdater.Reset()
					backoff := time.Duration(attempt) * p.cfg.BackoffBase
					log.Printf("[WorkerPool] Retrying watchdog timeout in %v (attempt %d/%d, preserving session %s)", backoff, attempt, maxAttempts, currentSessionID)
					select {
					case <-time.After(backoff):
					case <-p.ctx.Done():
						metrics.RecordTurnCompleted("cancelled", triggerType, currentModel, time.Since(execStart))
						for _, m := range burst {
							if m.ScheduleRunID != "" {
								_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
									RunID:       m.ScheduleRunID,
									MessageID:   m.ID,
									Status:      "failed",
									CompletedAt: time.Now().UTC(),
									DurationMs:  time.Since(execStart).Milliseconds(),
									Error:       "context cancelled during execution",
									Model:       currentModel,
								})
							}
						}
						return
					}
					continue
				}

				// Exhausted all attempts on watchdog timeout
				stopTyping()
				_ = db.RotateSessionID(p.cfg.DB, threadID, "")
				metrics.RecordTurnCompleted("watchdog_timeout", triggerType, currentModel, time.Since(execStart))

				if !skipDiscord {
					notif := notifier.StaticFallback("execution terminated by watchdog: " + errDetail)
					if err := p.cfg.DeliveryFunc(p.getDiscordSession(), threadID, notif); err != nil {
						log.Printf("[WorkerPool] Failed to deliver watchdog notice for thread %s: %v", threadID, err)
					}
				}

				sanitizedErr := sanitizeErrorText(errDetail)
				for _, m := range burst {
					_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusFailed, sanitizedErr)
					if m.ScheduleRunID != "" {
						_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
							RunID:       m.ScheduleRunID,
							MessageID:   m.ID,
							Status:      "failed",
							CompletedAt: time.Now().UTC(),
							DurationMs:  time.Since(execStart).Milliseconds(),
							Error:       sanitizedErr,
							Model:       currentModel,
						})
					}
					if p.cfg.OnMessageCompleted != nil {
						p.cfg.OnMessageCompleted(m, db.StatusFailed)
					}
				}
				log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED after exhausting %d attempts on watchdog timeout: %s", len(burst), threadID, maxAttempts, errDetail)
				return
			}
		}

		if !isFailure {
			resp, parseErr := runner.ParseAgyOutput(stdout)
			if parseErr != nil {
				log.Printf("[Queue] Failed to parse runner output despite exit 0: %v", parseErr)
				lastErrDetail = parseErr.Error()
				isFailure = true
			} else {
				extSess := resp.ConversationID
				if extSess == "" {
					extSess = runner.ExtractSessionID(stdout+"\n"+stderr, execStart)
				}
				if currentSessionID == "" && (extSess == "" || !runner.IsValidUUID(extSess)) {
					isFailure = true
					isTransient = false
					lastErrDetail = "failed to latch active session UUID on cold start"
					log.Printf("[Queue] Defensive Failure: %s for thread %s", lastErrDetail, threadID)
				} else {
					if extSess != "" && extSess != currentSessionID {
						log.Printf("[Queue] Active session synchronized for thread %s: %s -> %s", threadID, currentSessionID, extSess)
						currentSessionID = extSess
						_ = db.SaveSessionID(p.cfg.DB, threadID, currentSessionID)
					}
					stopTyping()
					statusUpdater.Stop()
					statusUpdater.DeleteStatusMessage()

				responseText := resp.Response
				baseDir := "/root/.gemini/antigravity-cli/brain"
				if currentSessionID != "" {
					baseDir = filepath.Join(baseDir, currentSessionID)
				}
				cleanText, attachments := delivery.ExtractAndSanitizeMedia(responseText, baseDir)
				attachments = delivery.AutoAttachNewMedia(baseDir, execStart, attachments)

				isSilent := runner.IsSilentSentinel(cleanText) && len(attachments) == 0
				if isSilent {
					log.Printf("[Queue] Output is empty. Skipping Discord delivery.")
				} else {
					if !skipDiscord {
						var deliveryErr error
						if p.cfg.DeliveryWithAttachmentsFunc != nil {
							deliveryErr = p.cfg.DeliveryWithAttachmentsFunc(p.getDiscordSession(), threadID, cleanText, attachments)
						} else if p.cfg.DeliveryFunc != nil {
							deliveryErr = p.cfg.DeliveryFunc(p.getDiscordSession(), threadID, cleanText)
						}
						if deliveryErr != nil {
							log.Printf("[WorkerPool] Failed to deliver response for thread %s: %v", threadID, deliveryErr)
						}
					}
				}

				if turnCount >= DefaultMaxSessionTurns {
					log.Printf("[Queue] Scope session reached turn limit (%d/%d). Resetting to cold state for fresh session initialization.", turnCount, DefaultMaxSessionTurns)
					_ = db.RotateSessionID(p.cfg.DB, threadID, "")
					currentSessionID = ""
				}

				// Mark all messages in the burst as completed with unpacked clean text
				metrics.RecordTurnCompleted("success", triggerType, currentModel, time.Since(execStart))
				metrics.RecordTokens(currentModel, resp.Usage.InputTokens, resp.Usage.OutputTokens, resp.Usage.ThinkingTokens, resp.Usage.CacheReadTokens, resp.Usage.TotalTokens)
				for _, m := range burst {
					_ = db.UpdateMessageCompleted(p.cfg.DB, m.ID, cleanText)
					if m.ScheduleRunID != "" {
						_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
							RunID:       m.ScheduleRunID,
							MessageID:   m.ID,
							Status:      "completed",
							CompletedAt: time.Now().UTC(),
							DurationMs:  time.Since(execStart).Milliseconds(),
							Model:       currentModel,
						})
					}
					if p.cfg.OnMessageCompleted != nil {
						p.cfg.OnMessageCompleted(m, db.StatusCompleted)
					}
				}
				log.Printf("[WorkerPool] %d message(s) in thread %s completed successfully on attempt %d/%d", len(burst), threadID, attempt, maxAttempts)

				return
			}
		}
		}

		// If execution failed because the pool context was cancelled (SIGTERM/shutdown),
		// suppress Discord error notifications and do NOT mark FAILED or increment retries.
		// Leave messages in PROCESSING for RecoverInterrupted on container restart.
		if p.ctx.Err() != nil {
			log.Printf("[WorkerPool] Turn execution cancelled due to pool shutdown (thread: %s, attempt: %d/%d). Preserving PROCESSING state for startup recovery.", threadID, attempt, maxAttempts)
			stopTyping()
			metrics.RecordTurnCompleted("cancelled", triggerType, currentModel, time.Since(execStart))
			return
		}

		log.Printf("[WorkerPool] Burst for thread %s failed on attempt %d/%d (transient=%t, corrupt=%t): %s",
			threadID, attempt, maxAttempts, isTransient, isSessionCorruption, errDetail)

		if isSessionCorruption {
			for _, m := range burst {
				_ = db.IncrementMessageRetry(p.cfg.DB, m.ID, errDetail)
			}
			statusUpdater.Reset()
			notif := p.cfg.NotifierFunc(currentAgyBin, currentAPIKey, "session reset due to context corruption")
			if !skipDiscord {
				if err := p.cfg.DeliveryFunc(p.getDiscordSession(), threadID, notif); err != nil {
					log.Printf("[WorkerPool] Failed to deliver session reset notice for thread %s: %v", threadID, err)
				}
			}
			_ = db.RotateSessionID(p.cfg.DB, threadID, "")
			currentSessionID = ""

			if attempt < maxAttempts {
				backoff := time.Duration(attempt) * p.cfg.BackoffBase
				select {
				case <-time.After(backoff):
				case <-p.ctx.Done():
					metrics.RecordTurnCompleted("cancelled", triggerType, currentModel, time.Since(execStart))
					for _, m := range burst {
						if m.ScheduleRunID != "" {
							_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "failed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(execStart).Milliseconds(),
								Error:       "context cancelled during execution",
								Model:       currentModel,
							})
						}
					}
					return
				}
			}
			continue
		}

		if isTransient {
			for _, m := range burst {
				_ = db.IncrementMessageRetry(p.cfg.DB, m.ID, errDetail)
			}
			if attempt < maxAttempts {
				statusUpdater.Reset()
				backoff := time.Duration(attempt) * p.cfg.BackoffBase
				log.Printf("[WorkerPool] Retrying transient error in %v (preserving session %s)", backoff, currentSessionID)
				select {
				case <-time.After(backoff):
				case <-p.ctx.Done():
					metrics.RecordTurnCompleted("cancelled", triggerType, currentModel, time.Since(execStart))
					for _, m := range burst {
						if m.ScheduleRunID != "" {
							_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "failed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(execStart).Milliseconds(),
								Error:       "context cancelled during execution",
								Model:       currentModel,
							})
						}
					}
					return
				}
			}
			continue
		}

		// Non-transient hard failure: fail fast immediately on Attempt 1 without retries
		stopTyping()

		if currentSessionID != "" {
			_ = db.RotateSessionID(p.cfg.DB, threadID, "")
			currentSessionID = ""
		}

		sanitizedErr := sanitizeErrorText(errDetail)
		metrics.RecordTurnCompleted("failed", triggerType, currentModel, time.Since(execStart))

		var notif string
		if isRateLimitError(errDetail) {
			notif = notifier.ModelUnavailableMessage()
		} else {
			notif = notifier.StaticFallback(fmt.Sprintf("execution failed with non-transient error: %s", errDetail))
		}
		stopTyping()
		statusUpdater.Stop()
		statusUpdater.DeleteStatusMessage()
		if !skipDiscord {
			if err := p.cfg.DeliveryFunc(p.getDiscordSession(), threadID, notif); err != nil {
				log.Printf("[WorkerPool] Failed to deliver non-transient failure notice for thread %s: %v", threadID, err)
			}
		}

		for _, m := range burst {
			_ = db.IncrementMessageRetry(p.cfg.DB, m.ID, errDetail)
			_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusFailed, sanitizedErr)
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "failed",
					CompletedAt: time.Now().UTC(),
					DurationMs:  time.Since(execStart).Milliseconds(),
					Error:       sanitizedErr,
					Model:       currentModel,
				})
			}
			if p.cfg.OnMessageCompleted != nil {
				p.cfg.OnMessageCompleted(m, db.StatusFailed)
			}
		}
		log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED due to non-transient error (attempt %d/%d): %s", len(burst), threadID, attempt, maxAttempts, errDetail)
		return
	}

	// Total exhaustion after all attempts
	stopTyping()

	if p.ctx.Err() != nil {
		log.Printf("[WorkerPool] Pool shutting down during turn for thread %s. Suppressing exhaustion alert and preserving state.", threadID)
		metrics.RecordTurnCompleted("cancelled", triggerType, currentModel, time.Since(execStart))
		return
	}
	var notif string
	if lastErrDetail != "" && isRateLimitError(lastErrDetail) {
		notif = notifier.ModelUnavailableMessage()
	} else {
		notif = notifier.StaticFallback(fmt.Sprintf("execution failed after exhausting %d attempts: %s", maxAttempts, lastErrDetail))
	}
	if !skipDiscord {
		if err := p.cfg.DeliveryFunc(p.getDiscordSession(), threadID, notif); err != nil {
			log.Printf("[WorkerPool] Failed to deliver exhaustion notice for thread %s: %v", threadID, err)
		}
	}
	sanitizedErr := sanitizeErrorText(lastErrDetail)
	metrics.RecordTurnCompleted("failed", triggerType, currentModel, time.Since(execStart))
	for _, m := range burst {
		_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusFailed, sanitizedErr)
		if m.ScheduleRunID != "" {
			_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
				RunID:       m.ScheduleRunID,
				MessageID:   m.ID,
				Status:      "failed",
				CompletedAt: time.Now().UTC(),
				DurationMs:  time.Since(execStart).Milliseconds(),
				Error:       sanitizedErr,
				Model:       currentModel,
			})
		}
		if p.cfg.OnMessageCompleted != nil {
			p.cfg.OnMessageCompleted(m, db.StatusFailed)
		}
	}
	log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED after exhausting all %d attempts", len(burst), threadID, maxAttempts)
}

var rateLimitKeywords = []string{
	"503",
	"high demand",
	"rate limit",
	"resource_exhausted",
	"429",
}

func isRateLimitError(errDetail string) bool {
	lower := strings.ToLower(errDetail)
	for _, kw := range rateLimitKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// RecoverInterrupted resumes all PENDING and PROCESSING messages from the database on startup in chronological order.
// If a message in PROCESSING has restart_count >= DefaultMaxRestarts or retry_count >= maxAttempts (poison pill),
// it is not re-enqueued; instead, a poison pill notification is sent and the message is marked FAILED.
func RecoverInterrupted(database *sql.DB, pool *WorkerPool) {
	if database == nil || pool == nil {
		return
	}

	if reconciled, err := db.ReconcileOrphanedScheduleRuns(database); err != nil {
		log.Printf("[Startup Recovery] Error reconciling orphaned schedule runs: %v", err)
	} else if reconciled > 0 {
		log.Printf("[Startup Recovery] Reconciled %d orphaned schedule run(s) from previous run", reconciled)
	}

	messages, err := db.GetPendingOrProcessingMessages(database)
	if err != nil {
		log.Printf("[Startup Recovery] Error querying pending/processing messages: %v", err)
		return
	}

	if len(messages) == 0 {
		log.Printf("[Startup Recovery] No interrupted messages found. System clean.")
		return
	}

	maxAttempts := pool.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	log.Printf("[Startup Recovery] Resuming %d interrupted message(s) in chronological FIFO order...", len(messages))
	for _, m := range messages {
		if m.Status == db.StatusProcessing && (m.RestartCount >= DefaultMaxRestarts || m.RetryCount >= maxAttempts) {
			reason := "poison pill: exceeded restart limit during crash recovery"
			if m.RestartCount < DefaultMaxRestarts {
				reason = "poison pill: exceeded retry limit during crash recovery"
			}
			log.Printf("[Startup Recovery] Poison pill detected for message %s (restart_count=%d, retry_count=%d): %s. Dropping message.", m.ID, m.RestartCount, m.RetryCount, reason)
			snippet := m.Content
			if len([]rune(snippet)) > 60 {
				snippet = string([]rune(snippet)[:57]) + "..."
			}
			agyBin := pool.cfg.AgyBin
			apiKey := pool.cfg.APIKey
			if pool.appCfg != nil {
				if cur := pool.appCfg.Current(); cur != nil {
					if cur.AgyBin != "" {
						agyBin = cur.AgyBin
					}
					if cur.APIKey != "" {
						apiKey = cur.APIKey
					}
				}
			}
			desc := "a message caused repeated crashes and had to be dropped"
			if strings.TrimSpace(snippet) != "" {
				desc = fmt.Sprintf("a message caused repeated crashes and had to be dropped (message snippet: %q)", snippet)
			}
			notif := pool.cfg.NotifierFunc(agyBin, apiKey, desc)
			if m.AuthorID != "http-client" {
				if err := pool.cfg.DeliveryFunc(pool.getDiscordSession(), m.ThreadID, notif); err != nil {
					log.Printf("[Startup Recovery] Failed to deliver poison pill notice for message %s: %v", m.ID, err)
				}
			}
			_ = db.UpdateMessageStatus(database, m.ID, db.StatusFailed, reason)
			continue
		}

		if m.Status == db.StatusProcessing {
			_ = db.ResetMessageToPendingWithRestart(database, m.ID, "interrupted during restart")
			m.Status = db.StatusPending
			m.RestartCount++
		}

		metrics.InterruptedTurnsRecovered.Inc()
		log.Printf("[Startup Recovery] Enqueuing message %s (thread: %s, status: %s, restart_count: %d, retry_count: %d)",
			m.ID, m.ThreadID, m.Status, m.RestartCount, m.RetryCount)
		pool.Enqueue(m)
	}
}

