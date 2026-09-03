package queue

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// ChannelSnapshot holds immutable channel metadata to avoid cross-goroutine pointer races.
type ChannelSnapshot struct {
	ID       string
	Name     string
	ParentID string
	IsThread bool
}

var (
	channelCacheMu   sync.RWMutex
	channelCache     = make(map[string]ChannelSnapshot)
	restSingleFlight singleflight.Group
)

// CacheDiscordChannel stores an immutable snapshot of a discordgo.Channel.
func CacheDiscordChannel(ch *discordgo.Channel) {
	if ch == nil || ch.ID == "" {
		return
	}
	channelCacheMu.Lock()
	channelCache[ch.ID] = ChannelSnapshot{
		ID:       ch.ID,
		Name:     ch.Name,
		ParentID: ch.ParentID,
		IsThread: ch.IsThread(),
	}
	channelCacheMu.Unlock()
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

var sensitivePattern = regexp.MustCompile(`(?i)(?:bearer\s+[a-zA-Z0-9_\-\.]+|ghp_[a-zA-Z0-9]+|gho_[a-zA-Z0-9]+|ghu_[a-zA-Z0-9]+|github_pat_[a-zA-Z0-9_]+|x-access-token:[^@\s]+|antigravity_[a-zA-Z0-9_\-]+|gemini_[a-zA-Z0-9_\-]+|aiza[0-9a-za-z-_]{35})`)
var (
	aerialExclusionRegex = regexp.MustCompile(`(?i)\baerial\s+(?:view|photo)s?\b`)
	tier1KeywordRegex    = regexp.MustCompile(`(?i)\b(aerial|gundam)\b`)
)

func sanitizeErrorText(errStr string) string {
	return sensitivePattern.ReplaceAllString(errStr, "[REDACTED_TOKEN]")
}

type MemoryRetrieverFunc func(ctx context.Context, database *sql.DB, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error)

type WorkerPoolConfig struct {
	DB             *sql.DB
	DiscordSession *discordgo.Session
	AgyBin         string
	APIKey         string
	Model          string
	SystemPrompt   string
	TimeoutMinutes int
	BackoffBase    time.Duration
	MaxAttempts    int
	MemoryClient   *memory.Client
	Classifier     *classifier.Classifier

	// Optional hooks for testing/custom overrides
	RunnerFunc           func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
	NotifierFunc         func(agyBin, apiKey, contextDescription string) string
	DeliveryFunc         func(s *discordgo.Session, channelID, text string) error
	TypingFunc           func(s *discordgo.Session, channelID string) (stop func())
	OnMessageCompleted   func(msg db.Message, finalStatus string)
	MemoryRetrieverFunc  MemoryRetrieverFunc
	ResolveChannelPolicy func(channelID, channelName string) config.ChannelPolicy
}

type WorkerPool struct {
	cfg       WorkerPoolConfig
	mu        sync.Mutex
	threadChs map[string]chan db.Message
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	stopped   bool
}

func NewWorkerPool(cfg WorkerPoolConfig) *WorkerPool {
	if cfg.TimeoutMinutes <= 0 {
		cfg.TimeoutMinutes = 15
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 3 * time.Second
	}
	if cfg.RunnerFunc == nil {
		cfg.RunnerFunc = runner.RunAgy
	}
	if cfg.NotifierFunc == nil {
		cfg.NotifierFunc = notifier.GenerateDynamicNotification
	}
	if cfg.DeliveryFunc == nil {
		cfg.DeliveryFunc = delivery.SendMessage
	}
	if cfg.TypingFunc == nil {
		cfg.TypingFunc = delivery.StartTyping
	}
	if cfg.MemoryClient == nil {
		cfg.MemoryClient = memory.NewClient("")
	}
	if cfg.MemoryRetrieverFunc == nil {
		cfg.MemoryRetrieverFunc = memory.RetrieveRelevantFacts
	}
	if cfg.Classifier == nil {
		cfg.Classifier = classifier.NewClassifier()
	}
	if cfg.ResolveChannelPolicy == nil {
		cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
			return config.GetRuntimeConfig().ResolveChannelPolicy(channelID, channelName)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		cfg:       cfg,
		threadChs: make(map[string]chan db.Message),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (p *WorkerPool) SetDiscordSession(s *discordgo.Session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfg.DiscordSession = s
}

func (p *WorkerPool) getDiscordSession() *discordgo.Session {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg.DiscordSession
}

func (p *WorkerPool) UpdateRuntimeConfig(model string, timeoutMinutes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if strings.TrimSpace(model) != "" {
		p.cfg.Model = model
	}
	if timeoutMinutes > 0 {
		p.cfg.TimeoutMinutes = timeoutMinutes
	}
	log.Printf("[WorkerPool] Runtime config updated: model=%s, timeout=%dm", p.cfg.Model, p.cfg.TimeoutMinutes)
}

func (p *WorkerPool) GetRuntimeConfig() (model string, timeoutMinutes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg.Model, p.cfg.TimeoutMinutes
}

func (p *WorkerPool) Start() {
	// WorkerPool starts workers lazily per active thread
	log.Printf("[WorkerPool] Started queue worker pool with max %d attempts per turn", p.cfg.MaxAttempts)
}

func (p *WorkerPool) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.cancel()
	p.mu.Unlock()

	p.wg.Wait()
	log.Printf("[WorkerPool] Queue worker pool stopped cleanly")
}

func (p *WorkerPool) Enqueue(msg db.Message) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		log.Printf("[WorkerPool] Warning: attempted to enqueue message %s to stopped pool", msg.ID)
		return
	}

	ch, exists := p.threadChs[msg.ThreadID]
	if !exists {
		ch = make(chan db.Message, 100)
		p.threadChs[msg.ThreadID] = ch
		p.wg.Add(1)
		go p.runThreadWorker(msg.ThreadID, ch)
	}
	p.mu.Unlock()

	select {
	case ch <- msg:
	case <-p.ctx.Done():
		log.Printf("[WorkerPool] Context cancelled while enqueuing message %s", msg.ID)
	}
}

func (p *WorkerPool) runThreadWorker(threadID string, ch chan db.Message) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			burst := []db.Message{msg}
		DrainLoop:
			for len(burst) < 5 {
				select {
				case extra := <-ch:
					burst = append(burst, extra)
				default:
					break DrainLoop
				}
			}
			p.processBurst(burst)
		case <-time.After(30 * time.Second):
			p.mu.Lock()
			if len(ch) == 0 {
				delete(p.threadChs, threadID)
				p.mu.Unlock()

				// Final non-blocking drain check in case a message was pushed right during unlock
				select {
				case msg, ok := <-ch:
					if !ok {
						return
					}
					burst := []db.Message{msg}
				DrainLoop2:
					for len(burst) < 5 {
						select {
						case extra := <-ch:
							burst = append(burst, extra)
						default:
							break DrainLoop2
						}
					}
					p.processBurst(burst)
					p.mu.Lock()
					if _, exists := p.threadChs[threadID]; !exists {
						p.threadChs[threadID] = ch
						p.mu.Unlock()
						continue
					}
					p.mu.Unlock()
				default:
					return
				}
			} else {
				p.mu.Unlock()
			}
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

func isTier1Wake(m db.Message, botUserID string) bool {
	if m.AuthorID == "http-client" {
		return true
	}

	// Direct mentions: <@botUserID>, <@!botUserID>
	if botUserID != "" {
		if strings.Contains(m.Content, "<@"+botUserID+">") || strings.Contains(m.Content, "<@!"+botUserID+">") {
			return true
		}
	}

	// Mentions list in Discord prompt envelope containing bot name or botUserID
	if idx := strings.Index(m.Content, "- mentions: ["); idx != -1 {
		if endIdx := strings.Index(m.Content[idx:], "]"); endIdx != -1 {
			inside := strings.ToLower(m.Content[idx+len("- mentions: [") : idx+endIdx])
			if strings.Contains(inside, "aerial") || (botUserID != "" && strings.Contains(inside, strings.ToLower(botUserID))) {
				return true
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

	// Check message body
	body := extractMessageBody(m.Content)
	if botUserID != "" && (strings.Contains(body, "<@"+botUserID+">") || strings.Contains(body, "<@!"+botUserID+">")) {
		return true
	}
	bodyLower := strings.ToLower(body)
	if strings.Contains(bodyLower, "<@aerial") || strings.Contains(bodyLower, "<@!aerial") {
		return true
	}

	// Keyword trigger matching word boundary regex (?i)\b(aerial|gundam)\b in extractMessageBody(m.Content)
	// excluding "aerial view" and "aerial photo"
	cleanedBody := aerialExclusionRegex.ReplaceAllString(body, "")
	if tier1KeywordRegex.MatchString(cleanedBody) {
		return true
	}

	return false
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

func isMentionOrReply(burst []db.Message) bool {
	for _, m := range burst {
		c := m.Content
		cLower := strings.ToLower(c)
		if strings.Contains(c, "<@") ||
			strings.Contains(cLower, "aerial") ||
			strings.Contains(cLower, "gundam") ||
			strings.Contains(cLower, "brain") ||
			strings.Contains(cLower, "bot") {
			return true
		}
		if idx := strings.Index(c, "- mentions: ["); idx != -1 {
			endIdx := strings.Index(c[idx:], "]")
			if endIdx != -1 {
				inside := strings.TrimSpace(c[idx+len("- mentions: [") : idx+endIdx])
				if inside != "" {
					return true
				}
			}
		}
	}
	return false
}

func resolveTypingStarter(policy config.ChannelPolicy, burst []db.Message, skipDiscord bool, typingFunc func(s *discordgo.Session, channelID string) func(), getSession func() *discordgo.Session, threadID string) func() {
	if skipDiscord || typingFunc == nil {
		return func() {}
	}
	switch policy.TypingIndicator {
	case "never":
		return func() {}
	case "on_mention":
		if isMentionOrReply(burst) {
			return typingFunc(getSession(), threadID)
		}
		return func() {}
	case "always":
		fallthrough
	default:
		return typingFunc(getSession(), threadID)
	}
}

func (p *WorkerPool) processBurst(burst []db.Message) {
	if len(burst) == 0 {
		return
	}

	threadID := burst[0].ThreadID
	log.Printf("[WorkerPool] Processing burst of %d message(s) for thread %s", len(burst), threadID)

	// 1. Claim messages from PENDING to PROCESSING
	var claimedBurst []db.Message
	for _, m := range burst {
		claimed, claimErr := db.ClaimPendingMessage(p.cfg.DB, m.ID)
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

	// 2. 5-Minute Staleness TTL check (for burst, check latest message)
	latestMsg := burst[len(burst)-1]
	if !latestMsg.CreatedAt.IsZero() && time.Since(latestMsg.CreatedAt) > 5*time.Minute {
		log.Printf("[WorkerPool] Dropping stale message(s) in thread %s (age > 5m). Marked [EXPIRED_STALE].", threadID)
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
		return
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
	_ = isThread
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
	defer func() {
		stopTyping()
	}()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WorkerPool] Panic in processBurst for thread %s: %v", threadID, r)
			stopTyping()
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

	if strings.ToLower(policy.Mode) == "channel" {
		botUserID := ""
		if sess := p.getDiscordSession(); sess != nil && sess.State != nil && sess.State.User != nil {
			botUserID = sess.State.User.ID
		}

		type wakeInfo struct {
			isWake    bool
			score     float64
			threshold float64
			reason    string
		}
		wakeInfos := make([]wakeInfo, len(burst))
		for i, m := range burst {
			if isTier1Wake(m, botUserID) {
				wakeInfos[i] = wakeInfo{
					isWake:    true,
					score:     1.0,
					threshold: policy.GetAmbientWakeThreshold(),
					reason:    "direct_address",
				}
			} else {
				threshold := policy.GetAmbientWakeThreshold()
				if threshold <= 0.0 {
					wakeInfos[i] = wakeInfo{
						isWake:    false,
						score:     0.0,
						threshold: threshold,
						reason:    "classifier disabled",
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
					isWake := res.Confidence >= threshold
					log.Printf("[AmbientClassifier] Channel %s | Msg %s | Author %s | Score: %.2f (Threshold: %.2f) | Wake: %t | Reason: %s",
						threadID, m.ID, m.AuthorName, res.Confidence, threshold, isWake, res.Reason)
					wakeInfos[i] = wakeInfo{
						isWake:    isWake,
						score:     res.Confidence,
						threshold: threshold,
						reason:    res.Reason,
					}
				}
			}
		}

		var trailingMsgs []db.Message
		var trailingInfos []wakeInfo

		wakeIdx := -1
		for i, info := range wakeInfos {
			if info.isWake {
				wakeIdx = i
				break
			}
		}

		channelName := effectiveName
		if channelName == "" {
			channelName = threadID
		}

		if wakeIdx == -1 {
			// ALL messages in burst are ambient
			if currentSessionID == "" {
				currentSessionID = uuid.New().String()
				_ = db.SaveSessionID(p.cfg.DB, threadID, currentSessionID)
			}
			_, _ = session.EnsureSessionDir(currentSessionID)

			for i, m := range burst {
				info := wakeInfos[i]
				rawBody := extractMessageBody(m.Content)
				if err := session.AppendAmbientTurn(currentSessionID, channelName, m.AuthorName, rawBody, m.CreatedAt); err != nil {
					log.Printf("[WorkerPool] Failed to append ambient turn for %s: %v", m.ID, err)
				}
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
		// If current turn_count + 1 >= policy.MaxSessionTurns, rotate session ID and ensure dir NOW
		// so leading ambient messages are preserved in the new session transcript.
		if policy.MaxSessionTurns > 0 {
			currentTurns, _ := db.GetSessionTurnCount(p.cfg.DB, threadID)
			if currentTurns+1 >= policy.MaxSessionTurns {
				log.Printf("[Queue] Channel session will reach turn limit (%d+1 >= %d). Rotating session ID before burst ingestion.", currentTurns, policy.MaxSessionTurns)
				newSessID := uuid.New().String()
				_ = db.RotateSessionID(p.cfg.DB, threadID, newSessID)
				_, _ = session.EnsureSessionDir(newSessID)
				currentSessionID = newSessID
			}
		}

		if currentSessionID == "" {
			currentSessionID, _ = db.GetSessionID(p.cfg.DB, threadID)
			if currentSessionID == "" {
				currentSessionID = uuid.New().String()
				_ = db.SaveSessionID(p.cfg.DB, threadID, currentSessionID)
			}
		}
		_, _ = session.EnsureSessionDir(currentSessionID)

		// Phase 1 (Leading ambient messages)
		if wakeIdx > 0 {
			for i := 0; i < wakeIdx; i++ {
				m := burst[i]
				info := wakeInfos[i]
				rawBody := extractMessageBody(m.Content)
				if err := session.AppendAmbientTurn(currentSessionID, channelName, m.AuthorName, rawBody, m.CreatedAt); err != nil {
					log.Printf("[WorkerPool] Failed to append ambient turn for %s: %v", m.ID, err)
				}
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

		turnCount, incErr := db.IncrementSessionTurnCount(p.cfg.DB, threadID)
		if incErr != nil {
			log.Printf("[Queue] Error incrementing turn count for thread %s: %v", threadID, incErr)
		}
		if policy.MaxSessionTurns > 0 && turnCount >= policy.MaxSessionTurns {
			log.Printf("[Queue] Channel session reached turn limit (%d/%d). Rotating session ID.", turnCount, policy.MaxSessionTurns)
			newSessID := uuid.New().String()
			_ = db.RotateSessionID(p.cfg.DB, threadID, newSessID)
			_, _ = session.EnsureSessionDir(newSessID)
			currentSessionID = newSessID
		}

		var handledTrailing bool
		handleTrailing := func() {
			if handledTrailing {
				return
			}
			handledTrailing = true
			for i, m := range trailingMsgs {
				info := trailingInfos[i]
				if !info.isWake {
					rawBody := extractMessageBody(m.Content)
					cName := channelName
					if err := session.AppendAmbientTurn(currentSessionID, cName, m.AuthorName, rawBody, m.CreatedAt); err != nil {
						log.Printf("[WorkerPool] Failed to append ambient turn for trailing message %s: %v", m.ID, err)
					}
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
					_ = db.UpdateMessageStatus(p.cfg.DB, m.ID, db.StatusPending, "")
					m.Status = db.StatusPending
					p.Enqueue(m)
				}
			}
		}
		defer handleTrailing()
	}

	stopTyping = resolveTypingStarter(policy, burst, skipDiscord, p.cfg.TypingFunc, p.getDiscordSession, threadID)

	// Format coalesced prompt
	basePrompt := CoalesceBurstPrompt(burst)
	queryText := memory.ExtractQueryText(basePrompt)
	if p.cfg.MemoryRetrieverFunc != nil && p.cfg.DB != nil && strings.TrimSpace(queryText) != "" {
		retrievalCtx, retrievalCancel := context.WithTimeout(p.ctx, 2500*time.Millisecond)
		facts, err := p.cfg.MemoryRetrieverFunc(retrievalCtx, p.cfg.DB, p.cfg.MemoryClient, queryText, 10)
		retrievalCancel()
		if err != nil {
			log.Printf("[WorkerPool] Warning: Semantic memory retrieval failed for thread %s: %v. Proceeding without injected facts.", threadID, err)
		} else if len(facts) > 0 {
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

	initialRetryCount := burst[0].RetryCount

	for attempt := initialRetryCount + 1; attempt <= maxAttempts; attempt++ {
		p.mu.Lock()
		currentModel := p.cfg.Model
		currentTimeout := p.cfg.TimeoutMinutes
		currentAgyBin := p.cfg.AgyBin
		currentAPIKey := p.cfg.APIKey
		p.mu.Unlock()

		startTime := time.Now().Add(-2 * time.Second)
		runCtx, runCancel := context.WithTimeout(p.ctx, time.Duration(currentTimeout)*time.Minute)

		stdout, stderr, exitCode, err := p.cfg.RunnerFunc(
			runCtx,
			currentAgyBin,
			turnPrompt,
			currentSessionID,
			currentAPIKey,
			currentModel,
			currentTimeout,
		)
		runCancel()

		isFailure, isTransient, isSessionCorruption, errDetail := runner.ClassifyError(exitCode, stdout, stderr)
		if err != nil && errDetail == "" {
			errDetail = err.Error()
		}
		lastErrDetail = errDetail

		if currentSessionID == "" {
			if extSess := runner.ExtractSessionID(stderr, startTime); extSess != "" {
				currentSessionID = extSess
				_ = db.SaveSessionID(p.cfg.DB, threadID, currentSessionID)
			}
		}

		if !isFailure {
			if currentSessionID != "" {
				_ = db.SaveSessionID(p.cfg.DB, threadID, currentSessionID)
			}
			stopTyping()

			isSilent := runner.IsSilentSentinel(stdout)
			if isSilent {
				log.Printf("[Queue] Output is silent sentinel ([NO_REPLY] or empty). Skipping Discord delivery.")
			} else {
				if !skipDiscord {
					if err := p.cfg.DeliveryFunc(p.getDiscordSession(), threadID, stdout); err != nil {
						log.Printf("[WorkerPool] Failed to deliver response for thread %s: %v", threadID, err)
					}
				}
			}

			// Mark all messages in the burst as completed
			for _, m := range burst {
				_ = db.UpdateMessageCompleted(p.cfg.DB, m.ID, stdout)
				if m.ScheduleRunID != "" {
					_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
						RunID:       m.ScheduleRunID,
						MessageID:   m.ID,
						Status:      "completed",
						CompletedAt: time.Now().UTC(),
						DurationMs:  time.Since(execStart).Milliseconds(),
					})
				}
				if p.cfg.OnMessageCompleted != nil {
					p.cfg.OnMessageCompleted(m, db.StatusCompleted)
				}
			}
			log.Printf("[WorkerPool] %d message(s) in thread %s completed successfully on attempt %d/%d", len(burst), threadID, attempt, maxAttempts)

			return
		}

		log.Printf("[WorkerPool] Burst for thread %s failed on attempt %d/%d (transient=%t, corrupt=%t): %s",
			threadID, attempt, maxAttempts, isTransient, isSessionCorruption, errDetail)

		if isSessionCorruption {
			for _, m := range burst {
				_ = db.IncrementMessageRetry(p.cfg.DB, m.ID, errDetail)
			}
			notif := notifier.GenerateSessionResetMessage(p.cfg.AgyBin, p.cfg.APIKey)
			if !skipDiscord {
				if err := p.cfg.DeliveryFunc(p.getDiscordSession(), threadID, notif); err != nil {
					log.Printf("[WorkerPool] Failed to deliver session reset notice for thread %s: %v", threadID, err)
				}
			}
			_ = db.DeleteSessionID(p.cfg.DB, threadID)
			currentSessionID = ""

			if attempt < maxAttempts {
				backoff := time.Duration(attempt) * p.cfg.BackoffBase
				select {
				case <-time.After(backoff):
				case <-p.ctx.Done():
					for _, m := range burst {
						if m.ScheduleRunID != "" {
							_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "failed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(execStart).Milliseconds(),
								Error:       "context cancelled during execution",
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
				backoff := time.Duration(attempt) * p.cfg.BackoffBase
				log.Printf("[WorkerPool] Retrying transient error in %v (preserving session %s)", backoff, currentSessionID)
				select {
				case <-time.After(backoff):
				case <-p.ctx.Done():
					for _, m := range burst {
						if m.ScheduleRunID != "" {
							_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "failed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(execStart).Milliseconds(),
								Error:       "context cancelled during execution",
							})
						}
					}
					return
				}
			}
			continue
		}

		// Non-transient general failure
		for _, m := range burst {
			_ = db.IncrementMessageRetry(p.cfg.DB, m.ID, errDetail)
		}
		currentSessionID = ""
		if attempt < maxAttempts {
			backoff := time.Duration(attempt) * p.cfg.BackoffBase
			select {
			case <-time.After(backoff):
			case <-p.ctx.Done():
				for _, m := range burst {
					if m.ScheduleRunID != "" {
						_ = db.UpdateScheduleRunStatus(p.cfg.DB, db.UpdateRunParams{
							RunID:       m.ScheduleRunID,
							MessageID:   m.ID,
							Status:      "failed",
							CompletedAt: time.Now().UTC(),
							DurationMs:  time.Since(execStart).Milliseconds(),
							Error:       "context cancelled during execution",
						})
					}
				}
				return
			}
		}
	}

	// Total exhaustion after all attempts
	stopTyping()
	var notif string
	if lastErrDetail != "" && (runnerIsTransient(lastErrDetail)) {
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
			})
		}
		if p.cfg.OnMessageCompleted != nil {
			p.cfg.OnMessageCompleted(m, db.StatusFailed)
		}
	}
	log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED after exhausting all %d attempts", len(burst), threadID, maxAttempts)
}

func runnerIsTransient(errDetail string) bool {
	_, isTransient, _, _ := runner.ClassifyError(1, "", errDetail)
	return isTransient
}

// RecoverInterrupted resumes all PENDING and PROCESSING messages from SQLite on startup in chronological order.
// If a message in PROCESSING has retry_count >= 3 (poison pill), it is not re-enqueued;
// instead, a poison pill notification is sent and the message is marked FAILED.
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

	log.Printf("[Startup Recovery] Resuming %d interrupted message(s) in chronological FIFO order...", len(messages))
	for _, m := range messages {
		if m.Status == db.StatusProcessing && m.RetryCount >= 3 {
			log.Printf("[Startup Recovery] Poison pill detected for message %s (retry_count=%d). Dropping message.", m.ID, m.RetryCount)
			snippet := m.Content
			if len([]rune(snippet)) > 60 {
				snippet = string([]rune(snippet)[:57]) + "..."
			}
			notif := notifier.GeneratePoisonPillMessage(pool.cfg.AgyBin, pool.cfg.APIKey, snippet)
			if m.AuthorID != "http-client" {
				if err := pool.cfg.DeliveryFunc(pool.getDiscordSession(), m.ThreadID, notif); err != nil {
					log.Printf("[Startup Recovery] Failed to deliver poison pill notice for message %s: %v", m.ID, err)
				}
			}
			_ = db.UpdateMessageStatus(database, m.ID, db.StatusFailed, "poison pill: exceeded retry limit during crash recovery")
			continue
		}

		if m.Status == db.StatusProcessing {
			_ = db.IncrementMessageRetry(database, m.ID, "interrupted during restart")
			_ = db.UpdateMessageStatus(database, m.ID, db.StatusPending, "interrupted during restart")
			m.Status = db.StatusPending
			m.RetryCount++
		}

		log.Printf("[Startup Recovery] Enqueuing message %s (thread: %s, status: %s, retry_count: %d)",
			m.ID, m.ThreadID, m.Status, m.RetryCount)
		pool.Enqueue(m)
	}
}

