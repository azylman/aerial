package queue

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/bwmarrin/discordgo"
)

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

	// Optional hooks for testing/custom overrides
	RunnerFunc         func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
	NotifierFunc       func(agyBin, apiKey, contextDescription string) string
	DeliveryFunc       func(s *discordgo.Session, channelID, text string) error
	TypingFunc         func(s *discordgo.Session, channelID string) (stop func())
	OnMessageCompleted func(msg db.Message, finalStatus string)
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
			p.processMessage(msg)
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
					p.processMessage(msg)
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

func (p *WorkerPool) processMessage(msg db.Message) {
	log.Printf("[WorkerPool] Processing message %s for thread %s (retry_count=%d)", msg.ID, msg.ThreadID, msg.RetryCount)

	_ = db.UpdateMessageStatus(p.cfg.DB, msg.ID, db.StatusProcessing, "")

	skipDiscord := msg.GuildID == "" || msg.AuthorID == "http-client"

	var stopTyping func() = func() {}
	if !skipDiscord {
		stopTyping = p.cfg.TypingFunc(p.getDiscordSession(), msg.ThreadID)
	}
	defer stopTyping()

	currentSessionID, _ := db.GetSessionID(p.cfg.DB, msg.ThreadID)

	maxAttempts := p.cfg.MaxAttempts
	lastErrDetail := ""

	for attempt := msg.RetryCount + 1; attempt <= maxAttempts; attempt++ {
		startTime := time.Now().Add(-2 * time.Second)
		runCtx, runCancel := context.WithTimeout(p.ctx, time.Duration(p.cfg.TimeoutMinutes)*time.Minute)

		stdout, stderr, exitCode, err := p.cfg.RunnerFunc(
			runCtx,
			p.cfg.AgyBin,
			msg.Content,
			currentSessionID,
			p.cfg.APIKey,
			p.cfg.Model,
			p.cfg.TimeoutMinutes,
		)
		runCancel()

		isFailure, isTransient, isSessionCorruption, errDetail := runner.ClassifyError(exitCode, stdout, stderr)
		if err != nil && errDetail == "" {
			errDetail = err.Error()
		}
		lastErrDetail = errDetail

		if extSess := runner.ExtractSessionID(stderr, startTime); extSess != "" {
			currentSessionID = extSess
		}

		if !isFailure {
			if currentSessionID != "" {
				_ = db.SaveSessionID(p.cfg.DB, msg.ThreadID, currentSessionID)
			}
			stopTyping()
			if !skipDiscord {
				if err := p.cfg.DeliveryFunc(p.getDiscordSession(), msg.ThreadID, stdout); err != nil {
					log.Printf("[WorkerPool] Failed to deliver response for message %s to thread %s: %v", msg.ID, msg.ThreadID, err)
				}
			}
			_ = db.UpdateMessageCompleted(p.cfg.DB, msg.ID, stdout)
			log.Printf("[WorkerPool] Message %s completed successfully on attempt %d/%d", msg.ID, attempt, maxAttempts)
			if p.cfg.OnMessageCompleted != nil {
				p.cfg.OnMessageCompleted(msg, db.StatusCompleted)
			}
			return
		}

		log.Printf("[WorkerPool] Message %s failed on attempt %d/%d (transient=%t, corrupt=%t): %s",
			msg.ID, attempt, maxAttempts, isTransient, isSessionCorruption, errDetail)

		if isSessionCorruption {
			_ = db.IncrementMessageRetry(p.cfg.DB, msg.ID, errDetail)
			notif := notifier.GenerateSessionResetMessage(p.cfg.AgyBin, p.cfg.APIKey)
			if !skipDiscord {
				if err := p.cfg.DeliveryFunc(p.getDiscordSession(), msg.ThreadID, notif); err != nil {
					log.Printf("[WorkerPool] Failed to deliver session reset notice for message %s: %v", msg.ID, err)
				}
			}
			_ = db.DeleteSessionID(p.cfg.DB, msg.ThreadID)
			currentSessionID = ""

			if attempt < maxAttempts {
				backoff := time.Duration(attempt) * p.cfg.BackoffBase
				select {
				case <-time.After(backoff):
				case <-p.ctx.Done():
					return
				}
			}
			continue
		}

		if isTransient {
			_ = db.IncrementMessageRetry(p.cfg.DB, msg.ID, errDetail)
			if attempt < maxAttempts {
				backoff := time.Duration(attempt) * p.cfg.BackoffBase
				log.Printf("[WorkerPool] Retrying transient error in %v (preserving session %s)", backoff, currentSessionID)
				select {
				case <-time.After(backoff):
				case <-p.ctx.Done():
					return
				}
			}
			continue
		}

		// Non-transient general failure
		_ = db.IncrementMessageRetry(p.cfg.DB, msg.ID, errDetail)
		currentSessionID = ""
		if attempt < maxAttempts {
			backoff := time.Duration(attempt) * p.cfg.BackoffBase
			select {
			case <-time.After(backoff):
			case <-p.ctx.Done():
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
		if err := p.cfg.DeliveryFunc(p.getDiscordSession(), msg.ThreadID, notif); err != nil {
			log.Printf("[WorkerPool] Failed to deliver exhaustion notice for message %s: %v", msg.ID, err)
		}
	}
	_ = db.UpdateMessageStatus(p.cfg.DB, msg.ID, db.StatusFailed, lastErrDetail)
	log.Printf("[WorkerPool] Message %s marked FAILED after exhausting all %d attempts", msg.ID, maxAttempts)
	if p.cfg.OnMessageCompleted != nil {
		p.cfg.OnMessageCompleted(msg, db.StatusFailed)
	}
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
			if m.GuildID != "" && m.AuthorID != "http-client" {
				if err := pool.cfg.DeliveryFunc(pool.getDiscordSession(), m.ThreadID, notif); err != nil {
					log.Printf("[Startup Recovery] Failed to deliver poison pill notice for message %s: %v", m.ID, err)
				}
			}
			_ = db.UpdateMessageStatus(database, m.ID, db.StatusFailed, "poison pill: exceeded retry limit during crash recovery")
			continue
		}

		log.Printf("[Startup Recovery] Enqueuing message %s (thread: %s, status: %s, retry_count: %d)",
			m.ID, m.ThreadID, m.Status, m.RetryCount)
		pool.Enqueue(m)
	}
}
