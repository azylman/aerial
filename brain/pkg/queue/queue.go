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

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

var sensitivePattern = regexp.MustCompile(`(?i)(?:bearer\s+[a-zA-Z0-9_\-\.]+|ghp_[a-zA-Z0-9]+|gho_[a-zA-Z0-9]+|ghu_[a-zA-Z0-9]+|github_pat_[a-zA-Z0-9_]+|x-access-token:[^@\s]+|antigravity_[a-zA-Z0-9_\-]+|gemini_[a-zA-Z0-9_\-]+|aiza[0-9a-za-z-_]{35})`)

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
	if strings.HasPrefix(trimmed, "<USER_REQUEST>") && strings.HasSuffix(trimmed, "</USER_REQUEST>") {
		inner := strings.TrimPrefix(trimmed, "<USER_REQUEST>")
		inner = strings.TrimSuffix(inner, "</USER_REQUEST>")
		return strings.TrimSpace(inner)
	}
	return trimmed
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

func (p *WorkerPool) processMessage(msg db.Message) {
	p.processBurst([]db.Message{msg})
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
	if p.cfg.ResolveChannelPolicy != nil {
		var ch *discordgo.Channel
		if sess := p.getDiscordSession(); sess != nil {
			if sess.State != nil {
				ch, _ = sess.State.Channel(threadID)
			}
			if ch == nil && sess.Ratelimiter != nil && sess.Token != "" {
				ch, _ = sess.Channel(threadID)
			}
		}

		if ch != nil && ch.IsThread() && ch.ParentID != "" {
			var parentCh *discordgo.Channel
			if sess := p.getDiscordSession(); sess != nil {
				if sess.State != nil {
					parentCh, _ = sess.State.Channel(ch.ParentID)
				}
				if parentCh == nil && sess.Ratelimiter != nil && sess.Token != "" {
					parentCh, _ = sess.Channel(ch.ParentID)
				}
			}
			parentName := ""
			if parentCh != nil {
				parentName = parentCh.Name
			}
			policy = p.cfg.ResolveChannelPolicy(ch.ParentID, parentName)
		} else {
			name := ""
			if ch != nil {
				name = ch.Name
			}
			policy = p.cfg.ResolveChannelPolicy(threadID, name)
		}
	} else {
		policy = config.GetRuntimeConfig().ResolveChannelPolicy(threadID, "")
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

	stopTyping := resolveTypingStarter(policy, burst, skipDiscord, p.cfg.TypingFunc, p.getDiscordSession, threadID)
	defer stopTyping()

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

	// Format coalesced prompt
	prompt := CoalesceBurstPrompt(burst)

	// Dynamically retrieve relevant semantic memory facts for the incoming prompt
	queryText := memory.ExtractQueryText(prompt)
	if p.cfg.MemoryRetrieverFunc != nil && p.cfg.DB != nil && strings.TrimSpace(queryText) != "" {
		retrievalCtx, retrievalCancel := context.WithTimeout(p.ctx, 2500*time.Millisecond)
		facts, err := p.cfg.MemoryRetrieverFunc(retrievalCtx, p.cfg.DB, p.cfg.MemoryClient, queryText, 10)
		retrievalCancel()

		if err != nil {
			log.Printf("[WorkerPool] Warning: Semantic memory retrieval failed for thread %s: %v. Proceeding without injected facts.", threadID, err)
		} else if len(facts) > 0 {
			memoryBlock := memory.FormatMemoryContext(facts)
			if memoryBlock != "" {
				prompt = memoryBlock + "\n\n" + prompt
				log.Printf("[WorkerPool] Injected %d semantic memory fact(s) into prompt for thread %s", len(facts), threadID)
			}
		}
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
			prompt,
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

			// Turn-Count Session Rotation
			if policy.Mode == "channel" && policy.MaxSessionTurns > 0 {
				newTurns, incErr := db.IncrementSessionTurnCount(p.cfg.DB, threadID)
				if incErr != nil {
					log.Printf("[Queue] Error incrementing turn count for thread %s: %v", threadID, incErr)
				} else if newTurns >= policy.MaxSessionTurns {
					log.Printf("[Queue] Channel session reached turn limit (%d/%d). Rotating session ID.", newTurns, policy.MaxSessionTurns)
					newSessionID := uuid.New().String()
					if rotErr := db.RotateSessionID(p.cfg.DB, threadID, newSessionID); rotErr != nil {
						log.Printf("[Queue] Error rotating session ID for thread %s: %v", threadID, rotErr)
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

