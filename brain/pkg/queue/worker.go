package queue

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

type wakeInfo = WakeInfo

type turnExecution struct {
	pool                                                                             *WorkerPool
	activePool                                                                       runner.AgentPool
	burst, trailingMsgs                                                              []db.Message
	threadID, triggerType, activeSessionID, currentSessionID, previousSessionID      string
	effectiveID, effectiveName, threadName, turnPrompt, turnStatus, turnResponseText string
	turnError, injectedHookContext                                                   string
	execStart                                                                        time.Time
	activeSessionMu                                                                  sync.Mutex
	turnCount, wakeIdx                                                               int
	turnDurationMs                                                                   int64
	lease                                                                            runner.SessionLease
	isCold, isEphemeral, isThread, isQuotaPaused, turnAttempted                      bool
	policy                                                                           config.ChannelPolicy
	wakeInfos, trailingInfos                                                         []wakeInfo
	statusUpdater                                                                    *StatusUpdater
	stopTyping                                                                       func()
	hookMetadata                                                                     map[string]any
	turnTokenUsage                                                                   runner.TokenUsage
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
// If database queries fail, the error is returned to allow fail-open semantics.
func GetSessionLastActivity(dbOrStore any, threadID string, mgr ...*session.Manager) (time.Time, bool, error) {
	if dbOrStore == nil || strings.TrimSpace(threadID) == "" {
		return time.Time{}, true, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := resolveStore(dbOrStore)
	if store == nil {
		return time.Time{}, true, nil
	}
	stats, err := store.GetSessionActivityStats(ctx, threadID)

	if err != nil {
		return time.Time{}, false, fmt.Errorf("querying session activity for thread %s: %w", threadID, err)
	}

	if stats == nil {
		return time.Time{}, true, nil
	}

	var diskActivity time.Time
	if strings.TrimSpace(stats.InternalSessionID) != "" {
		if len(mgr) > 0 && mgr[0] != nil {
			var actErr error
			diskActivity, actErr = mgr[0].GetSessionLastActivity(stats.InternalSessionID)
			if actErr != nil {
				log.Printf("[Worker] Warning getting session last activity for %s: %v", stats.InternalSessionID, actErr)
			}
		}
	}

	// A thread is cold only if it has zero completed messages, zero turns, and no disk activity.
	if stats.CompletedTurns == 0 && stats.TurnCount == 0 && diskActivity.IsZero() {
		return time.Time{}, true, nil
	}

	latestActivity := stats.SessionUpdatedAt
	if stats.LastMessageAt.After(latestActivity) {
		latestActivity = stats.LastMessageAt
	}
	if diskActivity.After(latestActivity) {
		latestActivity = diskActivity
	}

	return latestActivity, false, nil
}

func (te *turnExecution) store() db.Store {
	if te == nil || te.pool == nil {
		return nil
	}
	return te.pool.Store()
}

func (te *turnExecution) updateMessageStatus(id, status string, errorMsg ...string) {
	errMsg := ""
	if len(errorMsg) > 0 {
		errMsg = errorMsg[0]
	}
	if s := te.store(); s != nil {
		if err := s.UpdateMessageStatus(context.Background(), id, status, errMsg); err != nil {
			log.Printf("[Worker] Warning updating message %s status to %s: %v", id, status, err)
		}
	}
}

func (te *turnExecution) updateScheduleRunStatus(params db.UpdateRunParams) {
	if s := te.store(); s != nil {
		if err := s.UpdateScheduleRunStatus(context.Background(), params); err != nil {
			log.Printf("[Worker] Warning updating schedule run %s status: %v", params.RunID, err)
		}
	}
}

func (te *turnExecution) updateMessageCompleted(id, responseText string) {
	if s := te.store(); s != nil {
		if err := s.UpdateMessageCompleted(context.Background(), id, responseText); err != nil {
			log.Printf("[Worker] Warning updating message %s completed: %v", id, err)
		}
	}
}

func (te *turnExecution) incrementMessageRetry(id, errorMsg string) {
	if s := te.store(); s != nil {
		if err := s.IncrementMessageRetry(context.Background(), id, errorMsg); err != nil {
			log.Printf("[Worker] Warning incrementing retry for message %s: %v", id, err)
		}
	}
}

func (te *turnExecution) saveSessionID(threadID, sessionID string) {
	if te == nil || te.isEphemeral {
		return
	}
	if s := te.store(); s != nil {
		if err := s.SaveSessionID(context.Background(), threadID, sessionID); err != nil {
			log.Printf("[Worker] Warning saving session ID for thread %s: %v", threadID, err)
		}
	}
}

func (te *turnExecution) rotateSessionID(threadID, newSessionID string) {
	if te == nil || te.isEphemeral {
		return
	}
	if s := te.store(); s != nil {
		if err := s.RotateSessionID(context.Background(), threadID, newSessionID); err != nil {
			log.Printf("[Worker] Warning rotating session ID for thread %s: %v", threadID, err)
		}
	}
}

func (te *turnExecution) finalizeBurstMessages(msgs []db.Message, msgStatus, msgOutput, runStatus, runErr, model string, notify, isResponse bool) {
	durationMs, now := time.Since(te.execStart).Milliseconds(), time.Now().UTC()
	for _, m := range msgs {
		if isResponse {
			te.updateMessageCompleted(m.ID, msgOutput)
		} else {
			te.updateMessageStatus(m.ID, msgStatus, msgOutput)
		}
		if m.ScheduleRunID != "" && runStatus != "" {
			te.updateScheduleRunStatus(db.UpdateRunParams{
				RunID: m.ScheduleRunID, MessageID: m.ID, Status: runStatus, CompletedAt: now, DurationMs: durationMs, Error: runErr, Model: model,
			})
		}
		if notify && te.pool != nil && te.pool.cfg.OnMessageCompleted != nil {
			te.pool.cfg.OnMessageCompleted(m, msgStatus)
		}
	}
}

func (te *turnExecution) incrementSessionTurnCount(threadID string) (int, error) {
	if te == nil || te.isEphemeral || (te.pool != nil && te.isNonPrimaryPool(te.resolveExecutionPool())) {
		return 0, nil
	}
	if s := te.store(); s != nil {
		return s.IncrementSessionTurnCount(context.Background(), threadID)
	}
	return 0, nil
}

func (te *turnExecution) getSessionID(threadID string) (string, error) {
	if s := te.store(); s != nil {
		return s.GetSessionID(context.Background(), threadID)
	}
	return "", nil
}

func (te *turnExecution) getPreviousSessionID(threadID string) string {
	if s := te.store(); s != nil {
		id, err := s.GetPreviousSessionID(context.Background(), threadID)
		if err != nil {
			log.Printf("[WorkerPool] Warning getting previous session ID for %s: %v", threadID, err)
		}
		return id
	}
	return ""
}

func (te *turnExecution) createOneShotSchedule(s db.OneShotSchedule) error {
	if store := te.store(); store != nil {
		return store.CreateOneShotSchedule(context.Background(), s)
	}
	return nil
}

func (te *turnExecution) getThreadSummary(threadID string) (string, string) {
	if s := te.store(); s != nil {
		sum, msgID, err := s.GetThreadSummary(context.Background(), threadID)
		if err != nil {
			log.Printf("[WorkerPool] Warning getting thread summary for %s: %v", threadID, err)
		}
		return sum, msgID
	}
	return "", ""
}

func (te *turnExecution) saveThreadSummary(threadID, summary, lastMsgID string) {
	if s := te.store(); s != nil {
		if err := s.SaveThreadSummary(context.Background(), threadID, summary, lastMsgID); err != nil {
			log.Printf("[WorkerPool] Warning: Failed to save thread summary to DB for thread %s: %v", threadID, err)
		}
	}
}

func (te *turnExecution) isBurstMessage(id string) bool {
	for _, m := range te.burst {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (te *turnExecution) getRecentThreadMessages(threadID string, limit int) ([]db.Message, error) {
	if limit <= 0 {
		limit = 10
	} else if limit > 100 {
		limit = 100
	}

	// 1. Prefer live Discord messages if session and valid snowflake are available.
	if te.pool != nil {
		if dg := te.pool.getDiscordSession(); dg != nil && threadID != "" && IsNumericSnowflake(threadID) {
			fetchCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			beforeID := ""
			if len(te.burst) > 0 && IsNumericSnowflake(te.burst[0].ID) {
				beforeID = te.burst[0].ID
			}

			discordMsgs, err := dg.ChannelMessages(threadID, limit, beforeID, "", "", discordgo.WithContext(fetchCtx))
			if err == nil {
				msgs := make([]db.Message, 0, len(discordMsgs))
				// ChannelMessages returns newest-first; iterate backwards for chronological order.
				for i := len(discordMsgs) - 1; i >= 0; i-- {
					dm := discordMsgs[i]
					if dm == nil || te.isBurstMessage(dm.ID) {
						continue
					}
					authorID, authorName := "", ""
					if dm.Author != nil {
						authorID, authorName = dm.Author.ID, dm.Author.Username
					} else if dm.WebhookID != "" {
						authorName = "Webhook"
					}
					createdAt := dm.Timestamp
					if createdAt.IsZero() {
						if ts, tsErr := discordgo.SnowflakeTimestamp(dm.ID); tsErr == nil {
							createdAt = ts
						} else {
							createdAt = time.Now().UTC()
						}
					}
					var mentions, mentionUserIDs []string
					for _, u := range dm.Mentions {
						if u != nil {
							mentions = append(mentions, u.Username)
							mentionUserIDs = append(mentionUserIDs, u.ID)
						}
					}
					var replyingToAuthor string
					if dm.ReferencedMessage != nil && dm.ReferencedMessage.Author != nil {
						replyingToAuthor = "@" + dm.ReferencedMessage.Author.Username
					}
					msgs = append(msgs, db.Message{
						ID: dm.ID, ThreadID: threadID, AuthorID: authorID, AuthorName: authorName,
						Content: dm.Content, CreatedAt: createdAt,
						Metadata: db.MessageMetadata{Mentions: mentions, MentionUserIDs: mentionUserIDs, ReplyingToAuthor: replyingToAuthor},
					})
				}
				return msgs, nil
			}
			log.Printf("[Worker] Discord ChannelMessages failed for thread %s (falling back to database): %v", threadID, err)
		}
	}

	// 2. Fallback to database on disk (ignoring internal scheduler prompts to avoid prompt injection).
	if s := te.store(); s != nil {
		dbMsgs, err := s.GetRecentThreadMessages(context.Background(), threadID, limit)
		if err != nil {
			return nil, err
		}
		filtered := make([]db.Message, 0, len(dbMsgs))
		for _, m := range dbMsgs {
			if m.ScheduleRunID != "" || strings.EqualFold(m.AuthorName, "Scheduler") {
				continue
			}
			filtered = append(filtered, m)
		}
		return filtered, nil
	}
	return nil, nil
}

var rateLimitKeywords = []string{
	"503", "high demand", "rate limit", "resource_exhausted", "429", "quota",
	"too many requests", "overloaded", "service unavailable", "resource has been exhausted",
	"individual quota reached", "please upgrade your subscription",
}

func isRateLimitError(errDetail string, extraStrs ...string) bool {
	combined := strings.ToLower(errDetail)
	for _, s := range extraStrs {
		combined += " " + strings.ToLower(s)
	}
	if strings.Contains(combined, "disk quota") {
		return false
	}
	for _, kw := range rateLimitKeywords {
		if strings.Contains(combined, kw) {
			return true
		}
	}
	return false
}

func extractBurstModel(burst []db.Message) string {
	for _, m := range burst {
		if model := strings.TrimSpace(m.Metadata.Model); model != "" {
			return model
		}
	}
	return ""
}

func extractBurstSilent(burst []db.Message) bool {
	if len(burst) == 0 {
		return false
	}
	for _, m := range burst {
		if !m.Metadata.Silent {
			return false
		}
	}
	return true
}

func resolveBurstEffort(burst []db.Message) string {
	hasLow := false
	for _, m := range burst {
		switch resolveMessageEffort(m) {
		case "medium":
			return "medium"
		case "low":
			hasLow = true
		}
	}
	if hasLow {
		return "low"
	}
	return "high"
}

func (te *turnExecution) resolveWebhookDispatcher() (WebhookDispatcher, context.Context) {
	var dispatcher WebhookDispatcher
	var poolCtx context.Context
	if te.pool != nil {
		dispatcher = te.pool.WebhookDispatcher()
		poolCtx = te.pool.ctx
	}
	if dispatcher == nil {
		dispatcher = NewDefaultWebhookDispatcher()
	}
	if poolCtx == nil {
		poolCtx = context.Background()
	}
	return dispatcher, poolCtx
}

func (p *WorkerPool) processBurst(burst []db.Message, scopeKeyOpt ...string) {
	if len(burst) == 0 {
		return
	}
	if p.ctx.Err() != nil {
		return
	}

	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()

	threadID := burst[0].ThreadID
	scopeKey := threadID
	if len(scopeKeyOpt) > 0 && scopeKeyOpt[0] != "" {
		scopeKey = scopeKeyOpt[0]
	} else if p != nil {
		scopeKey = p.resolveScopeKey(burst[0])
	}
	log.Printf("[WorkerPool] Processing burst of %d message(s) for thread %s (scope: %s)", len(burst), threadID, scopeKey)

	lockVal, _ := p.scopeLocks.LoadOrStore(scopeKey, &sync.Mutex{})
	scopeLock, ok := lockVal.(*sync.Mutex)
	if !ok {
		scopeLock = &sync.Mutex{}
		p.scopeLocks.Store(scopeKey, scopeLock)
	}
	scopeLock.Lock()
	defer scopeLock.Unlock()

	triggerType := "discord"
	if burst[0].ScheduleRunID != "" {
		triggerType = "schedule"
	} else if burst[0].AuthorID == "http-client" {
		triggerType = "http"
	}

	te := &turnExecution{
		pool:        p,
		burst:       burst,
		threadID:    threadID,
		triggerType: triggerType,
		execStart:   time.Now().UTC(),
		wakeIdx:     -1,
		stopTyping:  func() {},
	}

	// Synchronous post_turn hook:
	// Go defers execute LIFO. By placing post_turn right after scopeLock.Lock() / defer scopeLock.Unlock(),
	// it executes AFTER handleTrailing and panic recovery, but BEFORE scopeLock.Unlock().
	defer func() {
		if te.turnAttempted && te.policy.Hooks.PostTurn != nil {
			postReq := buildPostTurnRequest(te)
			ctx, cancel := context.WithTimeout(context.Background(), te.policy.Hooks.PostTurn.GetTimeout())
			defer cancel()
			dispatcher, _ := te.resolveWebhookDispatcher()
			if _, err := dispatcher.CallPostTurnHook(ctx, te.policy.Hooks.PostTurn, postReq); err != nil {
				log.Printf("[WorkerPool] Warning: post_turn hook failed for thread %s: %v", te.threadID, err)
			}
		}
	}()

	defer func() {
		te.stopIndicators()
	}()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WorkerPool] Panic in processBurst for thread %s: %v", te.threadID, r)
			te.stopIndicators()
			metrics.RecordTurnCompleted("panic", te.triggerType, te.pool.GetRuntimeConfig(), time.Since(te.execStart))
			te.failBurstWithStatus("failed", fmt.Sprintf("panic: %v", r), "")
		}
	}()

	defer te.handleTrailing()

	if !te.claimAndFilterStale() {
		return
	}
	if !te.resolveTurnPolicy() {
		return
	}
	if te.evaluateAmbientWake() {
		return
	}

	// 1. Resolve active execution pool (primary vs medium vs low effort)
	activePool := te.resolveExecutionPool()
	te.activePool = activePool
	te.isEphemeral = te.isNonPrimaryPool(activePool)

	if activePool == nil {
		te.failBurst(fmt.Errorf("queue: no agent execution pool available for thread %s", te.threadID))
		return
	}
	leasedPool, ok := activePool.(runner.LeasedAgentPool)
	if !ok {
		te.failBurst(fmt.Errorf("queue: active pool does not implement runner.LeasedAgentPool for thread %s", te.threadID))
		return
	}

	// 2. Acquire exclusive session lease before prompt construction
	leaseCtx, leaseCancel := context.WithTimeout(te.pool.ctx, 65*time.Second)
	lease, leaseErr := leasedPool.AcquireLease(leaseCtx, te.threadID)
	leaseCancel()
	if leaseErr != nil {
		te.failBurst(fmt.Errorf("queue: failed to acquire session lease for thread %s: %w", te.threadID, leaseErr))
		return
	}
	te.lease = lease
	defer func() {
		if te.lease != nil {
			if relErr := te.lease.Release(); relErr != nil {
				log.Printf("[WorkerPool] Warning releasing lease for %s: %v", te.threadID, relErr)
			}
		}
	}()

	te.currentSessionID = lease.SessionID()
	te.previousSessionID = lease.PreviousSessionID()
	te.isCold = lease.IsCold()
	te.isEphemeral = lease.IsEphemeral() || te.isNonPrimaryPool(activePool)
	te.turnCount = lease.TurnCount()

	if te.currentSessionID != "" && te.isCold {
		te.saveSessionID(te.threadID, te.currentSessionID)
	}

	if te.policy.Hooks.PreTurn != nil {
		preReq := buildPreTurnRequest(te)
		dispatcher, poolCtx := te.resolveWebhookDispatcher()
		preResp, err := dispatcher.CallPreTurnHook(poolCtx, te.policy.Hooks.PreTurn, preReq)
		if err != nil {
			action := te.policy.Hooks.PreTurn.GetOnTimeout("drop")
			if action == "drop" {
				te.abortBurst("pre_turn_timeout_drop")
				return
			} else if action == "retry" {
				te.rescheduleBurst(5)
				return
			}
		} else if preResp != nil {
			if !preResp.Allow {
				if preResp.Action == "retry" {
					te.rescheduleBurst(preResp.RetryAfterSeconds)
					return
				}
				te.abortBurst("pre_turn_rejected: " + preResp.Reason)
				return
			}
			if strings.TrimSpace(preResp.InjectedContext) != "" {
				te.injectedHookContext = fmt.Sprintf("<COORDINATION_CONTEXT>\n%s\n</COORDINATION_CONTEXT>", strings.TrimSpace(preResp.InjectedContext))
			}
			te.hookMetadata = preResp.Metadata
		}
	}

	te.turnAttempted = true
	te.buildTurnPrompt()
	te.executeWithRetries()
}

func (te *turnExecution) claimAndFilterStale() bool {
	var claimedBurst []db.Message
	for _, m := range te.burst {
		claimed, claimErr := te.pool.cfg.Store.ClaimPendingMessage(te.pool.ctx, m.ID)
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
		return false
	}
	te.burst = claimedBurst

	stalenessTTL := te.pool.cfg.StalenessTTL
	if stalenessTTL <= 0 {
		stalenessTTL = 30 * time.Minute
	}

	latestMsg := te.burst[0]
	hasRecovered := false
	for _, m := range te.burst {
		if m.CreatedAt.After(latestMsg.CreatedAt) {
			latestMsg = m
		}
		if m.RetryCount > 0 || m.RestartCount > 0 {
			hasRecovered = true
		}
	}

	var lastActivity time.Time
	var isColdThread bool
	now := time.Now().UTC()
	msgAge := time.Duration(0)
	if !latestMsg.CreatedAt.IsZero() && now.After(latestMsg.CreatedAt) {
		msgAge = now.Sub(latestMsg.CreatedAt)
	}

	if msgAge > stalenessTTL && !hasRecovered {
		var err error
		lastActivity, isColdThread, err = GetSessionLastActivity(te.store(), te.threadID, te.pool.sessionMgr)
		if err != nil {
			log.Printf("[WorkerPool] Warning: failed to query session last activity for thread %s: %v. Retaining message(s) (fail-open).", te.threadID, err)
		}
	}

	isStale, reason := EvaluateBurstStaleness(te.burst, stalenessTTL, isColdThread, lastActivity, now)
	if isStale {
		log.Printf("[WorkerPool] Dropping stale message(s) in thread %s (%s). Marked [EXPIRED_STALE].", te.threadID, reason)
		metrics.RecordTurnCompleted("stale", te.triggerType, "none", time.Since(latestMsg.CreatedAt))
		te.finalizeBurstMessages(te.burst, db.StatusCompleted, "[EXPIRED_STALE]", "completed", "[EXPIRED_STALE]", "", true, true)
		return false
	}
	if reason != "" && strings.Contains(reason, "recent activity") {
		log.Printf("[WorkerPool] Retaining message in thread %s (msg age %v > TTL %v) because session had recent activity %v ago <= TTL.",
			te.threadID, msgAge, stalenessTTL, now.Sub(lastActivity))
	}

	te.execStart = time.Now().UTC()
	for _, m := range te.burst {
		if m.ScheduleRunID != "" {
			te.updateScheduleRunStatus(db.UpdateRunParams{RunID: m.ScheduleRunID, MessageID: m.ID, Status: "running"})
		}
	}
	return true
}

func isHTTPClientBurst(burst []db.Message) bool {
	for _, m := range burst {
		if m.AuthorID != "http-client" {
			return false
		}
	}
	return len(burst) > 0
}

func (te *turnExecution) resolveTurnPolicy() bool {
	te.effectiveID, te.effectiveName, te.isThread, te.threadName = ResolveChannelAndThread(te.pool.getDiscordSession(), te.threadID)
	if te.pool.cfg.ResolveChannelPolicy != nil {
		te.policy = te.pool.cfg.ResolveChannelPolicy(te.effectiveID, te.effectiveName)
	} else {
		te.policy = config.ActiveConfig().ResolveChannelPolicy(te.effectiveID, te.effectiveName)
	}

	if !isHTTPClientBurst(te.burst) && te.policy.IsIgnored() {
		log.Printf("[WorkerPool] Channel %s policy is ignored (mode=%s). Marking %d message(s) completed without execution.", te.threadID, te.policy.Mode, len(te.burst))
		metrics.RecordTurnCompleted("ignored", te.triggerType, "none", time.Since(te.execStart))
		tag := fmt.Sprintf("[%s]", strings.ToUpper(strings.TrimSpace(te.policy.Mode)))
		te.finalizeBurstMessages(te.burst, db.StatusCompleted, tag, "completed", "", "", true, false)
		return false
	}

	var getSessErr error
	te.currentSessionID, getSessErr = te.getSessionID(te.threadID)
	if getSessErr != nil {
		log.Printf("[Worker] Warning getting session ID for thread %s: %v", te.threadID, getSessErr)
	}
	return true
}

func (te *turnExecution) evaluateAmbientWake() (shouldExit bool) {
	if strings.ToLower(te.policy.Mode) == "channel" {
		botUserID := ""
		var botRoleIDs []string
		if sess := te.pool.getDiscordSession(); sess != nil && sess.State != nil {
			if sess.State.User != nil {
				botUserID = sess.State.User.ID
			}
			botRoleIDs = ResolveBotRoleIDs(sess, te.burst[0].GuildID, botUserID)
		}

		wakeMode := te.policy.GetWakeMode()
		var hookOverride string
		if te.policy.Hooks.OnWake != nil {
			channelID := te.effectiveID
			if channelID == "" {
				channelID = te.threadID
			}
			var firstMsg db.Message
			if len(te.burst) > 0 {
				firstMsg = te.burst[0]
			}
			req := &WakeRequest{ChannelID: channelID, ThreadID: te.threadID, Message: firstMsg, Timestamp: time.Now().UTC()}

			dispatcher, poolCtx := te.resolveWebhookDispatcher()
			wakeResp, err := dispatcher.CallWakeHook(poolCtx, te.policy.Hooks.OnWake, req)
			hookOverride = te.policy.Hooks.OnWake.GetOnTimeout("classify")
			if err == nil && wakeResp != nil && strings.TrimSpace(wakeResp.Override) != "" {
				hookOverride = strings.ToLower(strings.TrimSpace(wakeResp.Override))
			}
		}

		classifierFn := func(msgs []db.Message) (float64, string) {
			var recentContext []db.Message
			if te.pool != nil {
				if r, err := te.getRecentThreadMessages(te.threadID, 10); err == nil {
					recentContext = r
				}
			}
			if te.pool == nil || te.pool.cfg.Classifier == nil {
				return 0.0, "no classifier configured"
			}
			if len(msgs) == 1 {
				res := te.pool.cfg.Classifier.Classify(te.pool.ctx, msgs[0], recentContext)
				return res.Confidence, res.Reason
			}
			res := te.pool.cfg.Classifier.ClassifyBurst(te.pool.ctx, msgs, recentContext)
			log.Printf("[AmbientClassifier] Channel %s | BurstSize %d | Score: %.2f (Threshold: %.2f) | Wake: %t | Reason: %s",
				te.threadID, len(msgs), res.Confidence, te.policy.GetAmbientWakeThreshold(), res.Confidence >= te.policy.GetAmbientWakeThreshold(), res.Reason)
			return res.Confidence, res.Reason
		}

		plan := PlanBurstExecution(BurstPlanInput{
			Burst: te.burst, WakeMode: wakeMode, Threshold: te.policy.GetAmbientWakeThreshold(),
			BotUserID: botUserID, BotRoleIDs: botRoleIDs, HookOverride: hookOverride, ClassifierFn: classifierFn,
		})
		te.wakeIdx, te.wakeInfos = plan.WakeIndex, plan.WakeInfos

		if plan.IsAllAmbient {
			if te.pool != nil && te.pool.ctx != nil && te.pool.ctx.Err() != nil {
				log.Printf("[WorkerPool] Context cancelled during ambient classification for thread %s. Resetting to PENDING for clean deployment recovery.", te.threadID)
				metrics.RecordTurnCompleted("cancelled", te.triggerType, "classifier", time.Since(te.execStart))
				for _, m := range te.burst {
					te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
				}
				return true
			}
			te.markAmbientBurst()
			return true
		}

		if te.currentSessionID != "" && te.pool.sessionMgr != nil && !te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID) {
			log.Printf("[Queue] Session %s for thread %s not found on disk. Clearing for fresh Turn 1.", te.currentSessionID, te.threadID)
			te.currentSessionID = ""
			te.saveSessionID(te.threadID, "")
		}

		for i, m := range plan.LeadingAmbient {
			te.completeAmbientMessage(m, plan.WakeInfos[i])
		}
		te.trailingMsgs, te.trailingInfos = plan.TrailingMessages, plan.TrailingInfos
		te.burst = []db.Message{plan.ActiveMessage}
		metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Inc()
	}

	if !extractBurstSilent(te.burst) {
		te.statusUpdater = NewStatusUpdater(te.pool.getDiscordSession(), te.threadID, te.isThread)
		if te.pool.cfg.TypingFunc != nil {
			if stop := te.pool.cfg.TypingFunc(te.pool.getDiscordSession(), te.threadID); stop != nil {
				te.stopTyping = stop
			}
		}
	}
	var incErr error
	te.turnCount, incErr = te.incrementSessionTurnCount(te.threadID)
	if incErr != nil {
		log.Printf("[Queue] Error incrementing turn count for thread %s: %v", te.threadID, incErr)
	}
	return false
}

func (te *turnExecution) markAmbientBurst() {
	if te.pool == nil {
		return
	}

	metrics.RecordTurnCompleted("ambient", te.triggerType, "classifier", time.Since(te.execStart))

	for i, m := range te.burst {
		var info WakeInfo
		if i < len(te.wakeInfos) {
			info = te.wakeInfos[i]
		}
		te.completeAmbientMessage(m, info)
	}
}

func buildPreTurnRequest(te *turnExecution) *PreTurnRequest {
	if te == nil {
		return &PreTurnRequest{Timestamp: time.Now().UTC()}
	}
	channelID := te.effectiveID
	if channelID == "" {
		channelID = te.threadID
	}
	msgs, maxRetry := make([]*db.Message, len(te.burst)), 0
	for i := range te.burst {
		msgs[i] = &te.burst[i]
		if te.burst[i].RetryCount > maxRetry {
			maxRetry = te.burst[i].RetryCount
		}
	}
	prompt := te.turnPrompt
	if prompt == "" {
		var tz string
		if te.pool != nil && te.pool.appCfg != nil {
			if cur := te.pool.appCfg.Current(); cur != nil {
				tz = cur.Timezone
			}
		}
		prompt = CoalesceBurstPrompt(te.burst, tz)
	}
	return &PreTurnRequest{
		ChannelID: channelID, ThreadID: te.threadID, BurstCount: len(te.burst),
		Messages: msgs, Prompt: prompt, RetryCount: maxRetry, Timestamp: time.Now().UTC(),
	}
}

func buildPostTurnRequest(te *turnExecution) *PostTurnRequest {
	if te == nil {
		return &PostTurnRequest{Timestamp: time.Now().UTC()}
	}
	channelID := te.effectiveID
	if channelID == "" {
		channelID = te.threadID
	}
	durMs := te.turnDurationMs
	if durMs <= 0 && !te.execStart.IsZero() {
		durMs = time.Since(te.execStart).Milliseconds()
	}
	status := te.turnStatus
	if status == "" {
		status = "failed"
	}
	return &PostTurnRequest{
		ChannelID: channelID, ThreadID: te.threadID, Status: status,
		ResponseText: te.turnResponseText, Error: te.turnError,
		DurationMs: durMs, TokenUsage: te.turnTokenUsage, Metadata: te.hookMetadata, Timestamp: time.Now().UTC(),
	}
}

func (te *turnExecution) abortBurst(reason string) {
	te.stopIndicators()
	if te.pool != nil {
		metrics.RecordTurnCompleted("aborted", te.triggerType, "pre_turn", time.Since(te.execStart))
	}
	telemetry := fmt.Sprintf("[SKIPPED %s]", reason)
	te.finalizeBurstMessages(te.burst, db.StatusCompleted, telemetry, "completed", telemetry, "", true, false)
}

func (te *turnExecution) rescheduleBurst(delaySeconds int) {
	if delaySeconds <= 0 {
		delaySeconds = 5
	}
	te.stopIndicators()
	maxAttempts := 3
	if te.pool != nil && te.pool.cfg.MaxAttempts > 0 {
		maxAttempts = te.pool.cfg.MaxAttempts
	}
	for _, m := range te.burst {
		if m.RetryCount+1 >= maxAttempts {
			log.Printf("[WorkerPool] Message %s in thread %s exceeded max retry attempts (%d). Marking FAILED.", m.ID, te.threadID, maxAttempts)
			te.finalizeBurstMessages([]db.Message{m}, db.StatusFailed, "[EXHAUSTED_PRE_TURN_RETRIES]", "failed", "[EXHAUSTED_PRE_TURN_RETRIES]", "", true, false)
			continue
		}
		te.incrementMessageRetry(m.ID, "pre_turn_deferred")
		te.updateMessageStatus(m.ID, db.StatusPending, "pre_turn_deferred")
		m.RetryCount++
		m.Status = db.StatusPending
		mCopy := m
		if te.pool != nil {
			delay := time.Duration(delaySeconds) * time.Second
			if te.pool.cfg.RetryDelayOverride > 0 {
				delay = te.pool.cfg.RetryDelayOverride
			}
			time.AfterFunc(delay, func() {
				te.pool.Enqueue(mCopy)
			})
		}
	}
}

func (te *turnExecution) handleTrailing() {
	if len(te.trailingMsgs) == 0 {
		return
	}
	trailing := te.trailingMsgs
	trailingInfos := te.trailingInfos
	te.trailingMsgs = nil
	te.trailingInfos = nil
	for i, m := range trailing {
		info := trailingInfos[i]
		if te.isQuotaPaused {
			if info.IsWake {
				metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Inc()
			} else {
				metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
			}
			telemetry := "[QUOTA_PAUSED] Trailing message suppressed due to active quota lockout"
			te.updateMessageStatus(m.ID, db.StatusFailed, telemetry)
			if m.ScheduleRunID != "" {
				te.updateScheduleRunStatus(db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "failed",
					CompletedAt: time.Now().UTC(),
					Error:       telemetry,
				})
			}
			if te.pool != nil && te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
			}
			continue
		}
		if !info.IsWake {
			te.completeAmbientMessage(m, info)
		} else {
			metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Inc()
			te.updateMessageStatus(m.ID, db.StatusPending, "")
			m.Status = db.StatusPending
			if te.pool != nil {
				te.pool.Enqueue(m)
			}
		}
	}
}

type attemptConfig struct {
	model             string
	lowEffortModel    string
	mediumEffortModel string
	timeoutMinutes    int
	agyBin            string
	apiKey            string
}

func (te *turnExecution) resolveAttemptConfig(attempt int) attemptConfig {
	var cfg attemptConfig
	if te.pool == nil {
		return cfg
	}
	if attempt > 1 {
		log.Printf("[WorkerPool] Resolving attempt %d config for thread %s", attempt, te.threadID)
	}
	te.pool.mu.Lock()
	cfg.model, cfg.lowEffortModel, cfg.mediumEffortModel = te.pool.cfg.Model, te.pool.cfg.LowEffortModel, te.pool.cfg.MediumEffortModel
	cfg.timeoutMinutes, cfg.agyBin, cfg.apiKey = te.pool.cfg.TimeoutMinutes, te.pool.cfg.AgyBin, te.pool.cfg.APIKey
	overrideModel := te.pool.overrideModel
	te.pool.mu.Unlock()

	if te.pool.appCfg != nil {
		if cur := te.pool.appCfg.Current(); cur != nil {
			if cur.Model != "" {
				cfg.model = cur.Model
			}
			if cur.LowEffortModel != "" {
				cfg.lowEffortModel = cur.LowEffortModel
			}
			if cur.MediumEffortModel != "" {
				cfg.mediumEffortModel = cur.MediumEffortModel
			}
			if cur.APIKey != "" {
				cfg.apiKey = cur.APIKey
			}
			if cur.AgyBin != "" {
				cfg.agyBin = cur.AgyBin
			}
		}
	}

	if cfg.lowEffortModel == "" {
		cfg.lowEffortModel = config.GetRuntimeConfig().LowEffortModel
	}
	if cfg.mediumEffortModel == "" {
		cfg.mediumEffortModel = config.GetRuntimeConfig().MediumEffortModel
	}

	switch resolveBurstEffort(te.burst) {
	case "medium":
		if strings.TrimSpace(cfg.mediumEffortModel) != "" {
			cfg.model = cfg.mediumEffortModel
		}
	case "low":
		if strings.TrimSpace(cfg.lowEffortModel) != "" {
			cfg.model = cfg.lowEffortModel
		}
	}
	if reqModel := extractBurstModel(te.burst); reqModel != "" {
		cfg.model = reqModel
	}
	if overrideModel != "" {
		cfg.model = overrideModel
	}

	return cfg
}

func (te *turnExecution) resolveAndPersistSessionID(candidateID, stdout, stderr string, allowPersist bool) string {
	te.activeSessionMu.Lock()
	defer te.activeSessionMu.Unlock()

	targetSess := candidateID
	if targetSess == "" && te.isNonPrimaryPool(te.activePool) {
		if d := getStreamingDaemon(te.activePool, te.threadID); d != nil && d.SessionID() != "" {
			targetSess = d.SessionID()
		}
	}
	if targetSess == "" && te.lease != nil && te.lease.SessionID() != "" {
		targetSess = te.lease.SessionID()
	}
	if targetSess == "" {
		targetSess = te.currentSessionID
	}
	if targetSess == "" && (stdout != "" || stderr != "") {
		combinedOutput := stdout + "\n" + stderr
		if extSess := runner.ExtractSessionID(combinedOutput, te.execStart); extSess != "" && te.pool != nil && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(extSess) {
			targetSess = extSess
		}
	}
	if targetSess == "" && te.activePool != nil {
		if d := getStreamingDaemon(te.activePool, te.threadID); d != nil && d.SessionID() != "" {
			targetSess = d.SessionID()
		}
	}
	if targetSess == "" && te.pool != nil && te.pool.processPool != nil && te.pool.processPool != te.activePool {
		if d := getStreamingDaemon(te.pool.processPool, te.threadID); d != nil && d.SessionID() != "" {
			targetSess = d.SessionID()
		}
	}

	if targetSess != "" {
		te.activeSessionID = targetSess
		if allowPersist {
			if !te.isEphemeral && targetSess != te.previousSessionID && targetSess != te.currentSessionID {
				log.Printf("[Queue] Synchronizing active session for thread %s: %s -> %s", te.threadID, te.currentSessionID, targetSess)
				te.currentSessionID = targetSess
				te.saveSessionID(te.threadID, targetSess)
			} else if te.isEphemeral && targetSess != te.currentSessionID {
				log.Printf("[Queue] Anti-flapping: preserving primary session %s for thread %s; not overwriting with ephemeral session %s", te.currentSessionID, te.threadID, targetSess)
			}
		}
	}
	return targetSess
}

func (te *turnExecution) buildTurnPrompt() {
	var turnHistory []HistoryMessage
	var turnHistoryFetched bool
	getTurnHistory := func(limit int) []HistoryMessage {
		if turnHistoryFetched {
			if len(turnHistory) > limit {
				return turnHistory[:limit]
			}
			return turnHistory
		}
		baseCtx := context.Background()
		if te.pool != nil && te.pool.ctx != nil {
			baseCtx = te.pool.ctx
		}
		fetchCtx, fetchCancel := context.WithTimeout(baseCtx, 3*time.Second)
		defer fetchCancel()
		var err error
		if te.pool.cfg.HistoryFetcher != nil {
			turnHistory, err = te.pool.cfg.HistoryFetcher(fetchCtx, te.threadID, te.burst[0].ID, limit)
		} else {
			turnHistory, err = FetchRecentThreadHistory(fetchCtx, te.pool.getDiscordSession(), te.store(), te.threadID, limit)
		}
		if err != nil {
			log.Printf("[WorkerPool] Warning: History fetch failed for thread %s: %v", te.threadID, err)
		}
		turnHistoryFetched = true
		if len(turnHistory) > limit {
			return turnHistory[:limit]
		}
		return turnHistory
	}

	snap, _ := resolveChannelSnapshot(te.pool.getDiscordSession(), te.threadID)
	isColdStart := te.isCold && !te.isEphemeral
	isChannelMode := strings.EqualFold(te.policy.Mode, "channel")
	summary, lookbackMsgs := te.buildColdStartLookback(snap, isColdStart, isChannelMode, getTurnHistory)

	var prevID string
	if isColdStart {
		prevID = te.previousSessionID
		if prevID == "" {
			prevID = te.getPreviousSessionID(te.threadID)
		}
		if prevID != "" {
			log.Printf("[WorkerPool] Injected previous session identifier (%s) into prompt for thread %s", prevID, te.threadID)
		}
	}

	instructions := config.LoadScopeInstructions(te.effectiveName)
	if instructions != "" {
		log.Printf("[WorkerPool] Injected channel instructions for #%s into prompt", te.effectiveName)
	}
	if te.injectedHookContext != "" {
		log.Printf("[WorkerPool] Injected coordination context into prompt for thread %s", te.threadID)
	}

	var ambientCtx string
	if te.policy.AmbientContext != nil && te.pool != nil && te.pool.cfg.AmbientResolver != nil {
		ctx := context.Background()
		if te.pool.ctx != nil {
			ctx = te.pool.ctx
		}
		if actx, ambErr := te.pool.cfg.AmbientResolver(ctx, te.policy.AmbientContext); ambErr != nil {
			log.Printf("[WorkerPool] Warning resolving ambient context for channel %s: %v", te.effectiveName, ambErr)
		} else if strings.TrimSpace(actx) != "" {
			ambientCtx = actx
			log.Printf("[WorkerPool] Injected ambient context into prompt for #%s", te.effectiveName)
		}
	}

	for i := range te.burst {
		if te.burst[i].Metadata.ChannelName == "" && te.effectiveName != "" {
			te.burst[i].Metadata.ChannelName = te.effectiveName
		}
		if te.isThread && te.burst[i].Metadata.ThreadName == "" && te.threadName != "" {
			te.burst[i].Metadata.ThreadName = te.threadName
		}
	}

	var tz string
	if te.pool != nil && te.pool.appCfg != nil {
		if cur := te.pool.appCfg.Current(); cur != nil {
			tz = cur.Timezone
		}
	}

	te.turnPrompt = AssembleTurnPrompt(TurnPromptInput{
		AmbientContext:      ambientCtx,
		Burst:               te.burst,
		ThreadSummary:       summary,
		LookbackHistory:     lookbackMsgs,
		PreviousSessionID:   prevID,
		IsColdStart:         isColdStart,
		ChannelInstructions: instructions,
		InjectedHookContext: te.injectedHookContext,
		Timezone:            tz,
	})
}

func (te *turnExecution) buildColdStartLookback(snap ChannelSnapshot, isColdStart, isChannelMode bool, getTurnHistory func(int) []HistoryMessage) (string, []HistoryMessage) {
	var summary string
	isThreadColdStart := snap.IsThread && snap.ParentID != "" && snap.ParentID != snap.ID && isColdStart && !isChannelMode
	if isThreadColdStart {
		cachedSum, lastMsgID := te.getThreadSummary(te.threadID)
		histMsgs := getTurnHistory(100)
		if len(histMsgs) > 0 {
			var latestMsgID string
			var maxTime time.Time
			for _, m := range histMsgs {
				if m.CreatedAt.After(maxTime) || latestMsgID == "" {
					maxTime, latestMsgID = m.CreatedAt, m.ID
				}
			}
			if cachedSum != "" && lastMsgID != "" && lastMsgID == latestMsgID {
				summary = cachedSum
				log.Printf("[WorkerPool] Using cached thread summary for thread %s (watermark msg: %s)", te.threadID, lastMsgID)
			} else {
				cfg := te.resolveAttemptConfig(0)
				llmFn := te.pool.cfg.LLMFunc
				if llmFn == nil && te.pool.cfg.Classifier != nil && te.pool.cfg.Classifier.LLMFunc != nil {
					llmFn = te.pool.cfg.Classifier.LLMFunc
				}
				if llmFn == nil {
					llmFn = classifier.NewAgyLLMFunc(cfg.agyBin, cfg.apiKey, te.pool.cfg.RunnerFunc)
				}
				flashModel := cfg.model
				if te.pool.cfg.Classifier != nil && te.pool.cfg.Classifier.Model != "" {
					flashModel = te.pool.cfg.Classifier.Model
				} else if cfg.lowEffortModel != "" {
					flashModel = cfg.lowEffortModel
				}
				sumCtx, sumCancel := context.WithTimeout(te.pool.ctx, DefaultThreadSummaryTimeout)
				newSum, sumErr := SummarizeThreadHistoryWithGroup(sumCtx, te.pool.SummaryGroup(), llmFn, flashModel, te.threadID, histMsgs)
				sumCancel()
				if sumErr != nil {
					log.Printf("[WorkerPool] Warning: Thread history summarization failed for thread %s: %v. Falling back to raw lookback.", te.threadID, sumErr)
				} else if newSum != "" {
					summary = newSum
					te.saveThreadSummary(te.threadID, summary, latestMsgID)
				}
			}
		}
		if summary != "" {
			log.Printf("[WorkerPool] Injected <THREAD_SUMMARY> into Turn 1 prompt for thread %s", te.threadID)
		}
	}

	var lookbackMsgs []HistoryMessage
	if !te.isEphemeral && (isChannelMode || isColdStart || te.wakeIdx > 0 || len(te.burst) > 1) {
		lookbackMsgs = getTurnHistory(10)
		if isChannelMode && !isColdStart && len(lookbackMsgs) > 0 {
			lookbackMsgs = FilterAssistantMessages(lookbackMsgs)
		}
		if len(lookbackMsgs) > 0 {
			log.Printf("[WorkerPool] Injected channel history into prompt for thread %s", te.threadID)
		}
	}
	return summary, lookbackMsgs
}

func (te *turnExecution) handleRetryBackoff(attempt, maxAttempts int, errDetail, currentModel string) (shutdown bool) {
	for _, m := range te.burst {
		te.incrementMessageRetry(m.ID, errDetail)
	}
	if attempt >= maxAttempts {
		return false
	}
	if te.statusUpdater != nil {
		te.statusUpdater.Reset()
	}
	backoff := time.Duration(attempt) * te.pool.cfg.BackoffBase
	select {
	case <-time.After(backoff):
		return false
	case <-te.pool.ctx.Done():
		metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
		te.cancelBurstOnShutdown("failed", currentModel, true)
		return true
	}
}

func (te *turnExecution) resolveTurnOutcome(runCtx context.Context, turnRes *runner.TurnResult, execErr error) (runner.TurnOutcome, string) {
	targetSess := ""
	if te.lease != nil {
		targetSess = te.lease.SessionID()
	}
	if targetSess == "" {
		targetSess = te.currentSessionID
	}
	outcome := runner.NewTurnResolver().Resolve(runCtx, turnRes, execErr, targetSess)
	var stderr string
	if turnRes != nil {
		stderr = turnRes.Stderr
	}
	if !outcome.IsSuccess && targetSess != "" && te.pool != nil && te.pool.sessionMgr != nil {
		poolCtx := context.Background()
		if te.pool.ctx != nil {
			poolCtx = te.pool.ctx
		}
		if transcriptErr, tErr := te.pool.sessionMgr.ExtractLastTurnError(poolCtx, targetSess, te.execStart); tErr == nil && transcriptErr != "" {
			outcome.ErrorDetail = transcriptErr
			stderr = transcriptErr
		}
	}
	return outcome, stderr
}

func (te *turnExecution) deliverSuccessAndFinalize(resp *runner.AgyResponse, accumulatedUsage runner.TokenUsage, currentModel string, attempt, maxAttempts int) (bool, string) {
	te.stopIndicators()
	baseDir := "/root/.gemini/antigravity-cli/brain"
	if te.currentSessionID != "" {
		baseDir = filepath.Join(baseDir, te.currentSessionID)
	}
	cleanText, attachments := delivery.ExtractAndSanitizeMedia(resp.Response, baseDir)
	attachments = delivery.AutoAttachNewMedia(baseDir, te.execStart, attachments)

	isSilent := extractBurstSilent(te.burst)
	if !isSilent && strings.TrimSpace(cleanText) == "" && len(attachments) == 0 {
		log.Printf("[Queue] Agent produced empty response for thread %s on attempt %d/%d; treating as failure", te.threadID, attempt, maxAttempts)
		return false, "agent produced empty response"
	}

	if !isSilent {
		var deliveryErr error
		if te.pool.cfg.DeliveryWithAttachmentsFunc != nil {
			deliveryErr = te.pool.cfg.DeliveryWithAttachmentsFunc(te.pool.getDiscordSession(), te.threadID, cleanText, attachments)
		} else if te.pool.cfg.DeliveryFunc != nil {
			deliveryErr = te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, cleanText)
		}
		if deliveryErr != nil {
			log.Printf("[WorkerPool] Failed to deliver response for thread %s: %v", te.threadID, deliveryErr)
		}
	}

	resp.Usage.InputTokens += accumulatedUsage.InputTokens
	resp.Usage.OutputTokens += accumulatedUsage.OutputTokens
	resp.Usage.ThinkingTokens += accumulatedUsage.ThinkingTokens
	resp.Usage.CacheReadTokens += accumulatedUsage.CacheReadTokens
	resp.Usage.TotalTokens += accumulatedUsage.TotalTokens

	metrics.RecordTurnCompleted("success", te.triggerType, currentModel, time.Since(te.execStart))
	tokenChannel := te.effectiveName
	if tokenChannel == "" {
		if te.triggerType == "schedule" || te.triggerType == "http" {
			tokenChannel = te.triggerType
		} else if !IsNumericSnowflake(te.threadID) && te.threadID != "" {
			tokenChannel = te.threadID
		}
	}
	metrics.RecordTokens(currentModel, tokenChannel, resp.Usage.InputTokens, resp.Usage.OutputTokens, resp.Usage.ThinkingTokens, resp.Usage.CacheReadTokens, resp.Usage.TotalTokens)
	te.turnStatus, te.turnResponseText, te.turnTokenUsage, te.turnDurationMs = "success", cleanText, resp.Usage, time.Since(te.execStart).Milliseconds()
	te.finalizeBurstMessages(te.burst, db.StatusCompleted, cleanText, "completed", "", currentModel, true, true)
	log.Printf("[WorkerPool] %d message(s) in thread %s completed successfully on attempt %d/%d", len(te.burst), te.threadID, attempt, maxAttempts)
	return true, ""
}

func (te *turnExecution) handleQuotaPause(errDetail, stderr, currentModel, burstEffort string, attempt, maxAttempts int) bool {
	resetDur, _ := runner.ExtractQuotaResetDuration(errDetail, stderr)
	if runner.IsCapacityBlip(errDetail, stderr) || resetDur <= 5*time.Second {
		log.Printf("[WorkerPool] Capacity blip detected for thread %s on attempt %d/%d (reset=%v): %s.", te.threadID, attempt, maxAttempts, resetDur, errDetail)
		if attempt < maxAttempts {
			for _, m := range te.burst {
				te.incrementMessageRetry(m.ID, errDetail)
			}
			delay := calculateCapacityBackoff(attempt, resetDur)
			if te.pool != nil && te.pool.cfg.RetryDelayOverride > 0 {
				delay = te.pool.cfg.RetryDelayOverride
			}
			var cancelChan <-chan struct{}
			if te.pool != nil && te.pool.ctx != nil {
				cancelChan = te.pool.ctx.Done()
			}
			select {
			case <-time.After(delay):
				return true
			case <-cancelChan:
				log.Printf("[WorkerPool] Context cancelled during capacity blip backoff for thread %s", te.threadID)
				te.stopIndicators()
				te.cancelBurstOnShutdown("pending", currentModel, false)
				return false
			}
		}
		log.Printf("[WorkerPool] Capacity blip retry budget exhausted for thread %s after %d attempts", te.threadID, maxAttempts)
		te.stopIndicators()
		reason := fmt.Sprintf("[CAPACITY_EXHAUSTED reset_in=%v] %s", resetDur, sanitizeErrorText(errDetail))
		metrics.RecordRunnerError("capacity_exhausted", currentModel)
		metrics.RecordTurnCompleted("capacity_exhausted", te.triggerType, currentModel, time.Since(te.execStart))
		if te.pool != nil && te.pool.cfg.DeliveryFunc != nil {
			pauseMsg := notifier.FormatQuotaPauseMessage(resetDur, time.Now().UTC().Add(resetDur), false, false)
			if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, pauseMsg); err != nil {
				log.Printf("[WorkerPool] Failed to deliver capacity pause notice for thread %s: %v", te.threadID, err)
			}
		}
		te.failBurstWithStatus("failed", reason, currentModel)
		return false
	}
	te.isQuotaPaused = true
	te.stopTyping()
	log.Printf("[WorkerPool] Quota pause detected for thread %s on attempt %d/%d: %s", te.threadID, attempt, maxAttempts, errDetail)
	isAlreadyRetry := te.burst[0].ScheduleRunID != "" || strings.HasPrefix(te.burst[0].Content, "[QUOTA_RETRY]") || strings.Contains(te.turnPrompt, "[QUOTA_RETRY]")
	jitterSec := 5 + rand.Intn(16)
	runAt := time.Now().UTC().Add(resetDur).Add(30 * time.Second).Add(time.Duration(jitterSec) * time.Second)
	if te.pool != nil {
		te.pool.quotaLockedUntil.Store(runAt.Unix())
	}
	var scheduled bool
	if !isAlreadyRetry {
		oneShotID := uuid.New().String()
		oneShot := db.OneShotSchedule{
			ID: oneShotID, ThreadID: te.threadID, Prompt: fmt.Sprintf("[QUOTA_RETRY] %s", te.turnPrompt),
			Effort: burstEffort, RunAt: runAt, CreatedAt: time.Now().UTC(),
		}
		if err := te.createOneShotSchedule(oneShot); err == nil {
			scheduled = true
			log.Printf("[WorkerPool] Scheduled one-shot retry %s for thread %s", oneShotID, te.threadID)
		}
	}
	metrics.RecordTurnCompleted("quota_paused", te.triggerType, currentModel, time.Since(te.execStart))
	te.stopIndicators()
	if te.pool.cfg.DeliveryFunc != nil {
		pauseMsg := notifier.FormatQuotaPauseMessage(resetDur, runAt, scheduled, isAlreadyRetry)
		if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, pauseMsg); err != nil {
			log.Printf("[WorkerPool] Failed to deliver quota pause notice for thread %s: %v", te.threadID, err)
		}
	}
	reason := fmt.Sprintf("[QUOTA_PAUSED reset_in=%v scheduled=%t] %s", resetDur, scheduled, sanitizeErrorText(errDetail))
	te.failBurstWithStatus("failed", reason, currentModel)
	return false
}

func (te *turnExecution) handleYieldTrap(resp *runner.AgyResponse, activePool runner.AgentPool, exitCode int, stdout, stderr string, autoResumeCount *int, accumulatedUsage *runner.TokenUsage, promptToSend *string, currentModel string, runCtx context.Context) bool {
	isYield, taskCount := runner.IsYieldTrap(exitCode, stdout, stderr)
	var unfinishedTaskID string
	detectionSource := "stderr_signature"
	var trackerDaemon *runner.StreamingDaemon
	if !isYield && te.pool != nil {
		for _, candidatePool := range []runner.AgentPool{activePool, te.pool.processPool, te.pool.mediumEffortProcessPool, te.pool.lowEffortProcessPool} {
			if candidatePool == nil {
				continue
			}
			if d := getStreamingDaemon(candidatePool, te.threadID); d != nil && d.TaskTracker().ActiveCount() > 0 {
				isYield, taskCount, detectionSource, trackerDaemon = true, 1, "daemon_tracker", d
				break
			}
		}
	}
	if !isYield {
		return false
	}
	if *autoResumeCount >= MaxYieldTrapAutoResumes {
		log.Printf("[YieldTrap] Circuit breaker tripped for thread %s in session %s after %d auto-resumptions (source=%s, taskID=%s). Delivering final output.",
			te.threadID, te.currentSessionID, *autoResumeCount, detectionSource, unfinishedTaskID)
		metrics.RecordYieldTrap("circuit_breaker", detectionSource, currentModel)
		metrics.RecordRunnerError("yield_trap_circuit_breaker", currentModel)
		return false
	}
	*autoResumeCount++
	accumulatedUsage.InputTokens += resp.Usage.InputTokens
	accumulatedUsage.OutputTokens += resp.Usage.OutputTokens
	accumulatedUsage.ThinkingTokens += resp.Usage.ThinkingTokens
	accumulatedUsage.CacheReadTokens += resp.Usage.CacheReadTokens
	accumulatedUsage.TotalTokens += resp.Usage.TotalTokens

	if detectionSource == "daemon_tracker" && trackerDaemon != nil {
		tasks := trackerDaemon.TaskTracker().ActiveTasks()
		if len(tasks) > 0 {
			activeTask := tasks[0]
			unfinishedTaskID = activeTask.TaskID
			targetSessID := trackerDaemon.SessionID()
			if targetSessID == "" {
				targetSessID = te.currentSessionID
			}
			if te.pool.sessionMgr != nil && targetSessID != "" {
				if sessDir, sErr := te.pool.sessionMgr.GetSessionDir(targetSessID); sErr == nil && sessDir != "" {
					waitCtx, waitCancel := context.WithTimeout(runCtx, 2*time.Second)
					exitCode, logPath, wErr := watchTaskCompletion(waitCtx, sessDir, activeTask.TaskID)
					waitCancel()
					if wErr == nil {
						trackerDaemon.TaskTracker().Remove(activeTask.TaskID)
						*promptToSend = fmt.Sprintf(TaskCompletionResumePrompt, unfinishedTaskID, exitCode, logPath)
					}
				}
			}
		}
	}
	if *promptToSend == te.turnPrompt || *promptToSend == "" {
		if unfinishedTaskID != "" {
			*promptToSend = fmt.Sprintf(TaskCompletionResumePrompt, unfinishedTaskID, 0, "")
		} else {
			*promptToSend = YieldTrapResumePrompt
		}
	}
	log.Printf("[YieldTrap] Intercepted background command exit for thread %s in session %s (auto-resume %d/%d, source=%s, taskCount=%d, taskID=%s): suppressing intermediate waiting output and auto-resuming turn.",
		te.threadID, te.currentSessionID, *autoResumeCount, MaxYieldTrapAutoResumes, detectionSource, taskCount, unfinishedTaskID)
	metrics.RecordYieldTrap("resumed", detectionSource, currentModel)
	metrics.RecordRunnerError("yield_trap_intercepted", currentModel)
	return true
}

func (te *turnExecution) executeWithRetries() {
	maxAttempts := te.pool.cfg.MaxAttempts
	lastErrDetail := ""
	lastStderr := ""
	currentModel := te.pool.GetRuntimeConfig()

	initialRetryCount := 0
	for _, m := range te.burst {
		if m.RetryCount > initialRetryCount {
			initialRetryCount = m.RetryCount
		}
	}

	attemptCfg := te.resolveAttemptConfig(0)
	currentAgyBin := attemptCfg.agyBin
	currentAPIKey := attemptCfg.apiKey

	var autoResumeCount int
	var accumulatedUsage runner.TokenUsage
	promptToSend := te.turnPrompt

	for attempt := initialRetryCount + 1; attempt <= maxAttempts; attempt++ {
		cfg := te.resolveAttemptConfig(attempt)
		currentModel = cfg.model
		currentTimeout := cfg.timeoutMinutes
		currentAgyBin = cfg.agyBin
		currentAPIKey = cfg.apiKey
		burstEffort := resolveBurstEffort(te.burst)

		// Global Quota Lockout Pre-check: if a quota pause is active across the pool and running in OAuth mode, fail-fast immediately
		if currentAPIKey == "" {
			if lockedUntilUnix := te.pool.quotaLockedUntil.Load(); lockedUntilUnix > 0 {
				lockedUntil := time.Unix(lockedUntilUnix, 0)
				if time.Now().Before(lockedUntil) {
					te.isQuotaPaused = true
					remaining := time.Until(lockedUntil)
					log.Printf("[WorkerPool] Global quota pause active for thread %s (%v remaining). Aborting turn.", te.threadID, remaining)
					te.stopIndicators()
					reason := fmt.Sprintf("[QUOTA_PAUSED reset_in=%v scheduled=false] global quota pause active", remaining)
					metrics.RecordRunnerError("quota_paused", currentModel)
					metrics.RecordTurnCompleted("quota_paused", te.triggerType, currentModel, time.Since(te.execStart))
					if te.pool.cfg.DeliveryFunc != nil {
						pauseMsg := notifier.FormatQuotaPauseMessage(remaining, lockedUntil, false, false)
						if dErr := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, pauseMsg); dErr != nil {
							log.Printf("[WorkerPool] Failed to deliver quota pause message to thread %s: %v", te.threadID, dErr)
						}
					}
					te.failBurstWithStatus("failed", reason, currentModel)
					return
				}
			}
		}

		if promptToSend == "" {
			promptToSend = te.turnPrompt
		}
		if attempt > 1 && promptToSend == te.turnPrompt {
			if (runner.IsInactivityTimeout(lastErrDetail, lastStderr) || strings.Contains(lastErrDetail, "max duration exceeded") || strings.Contains(lastErrDetail, "print timeout") || strings.Contains(lastErrDetail, "empty response after")) && te.currentSessionID != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID) {
				promptToSend = fmt.Sprintf(ContinuationPromptTemplate, te.turnPrompt)
			}
		}

		// Pad Go context by +1 minute relative to runner watchdog ceiling so the runner watchdog always fires cleanly
		runCtx, runCancel := context.WithTimeout(te.pool.ctx, time.Duration(currentTimeout+1)*time.Minute)

		var stdout, stderr string
		var exitCode int
		var err error
		var isFailure, isTransient, isSessionCorruption bool
		var errDetail string
		runStart := time.Now()

		activePool := te.activePool
		if activePool == nil {
			activePool = te.resolveExecutionPool()
			te.activePool = activePool
		}

		var resp *runner.AgyResponse
		var outcome runner.TurnOutcome
		var turnRes *runner.TurnResult

		if te.lease != nil {
			if te.statusUpdater != nil {
				te.statusUpdater.MarkTurnStarted()
			}
			if te.lease.SessionID() != "" && te.lease.SessionID() != te.currentSessionID && !te.isEphemeral {
				te.currentSessionID = te.lease.SessionID()
				te.saveSessionID(te.threadID, te.currentSessionID)
			}
			sink := newDiscordTurnSink(te.statusUpdater)
			sessIDToSend := te.currentSessionID
			if te.lease.SessionID() != "" && te.currentSessionID != "" {
				sessIDToSend = te.lease.SessionID()
			}
			scopeInst := config.LoadScopeInstructions(te.effectiveName)
			turnCtx := &runner.TurnContext{
				TurnID: uuid.New().String(), SessionID: sessIDToSend, Model: currentModel,
				Prompt: promptToSend, ScopeInstructions: scopeInst, ChannelInstructions: scopeInst,
				CoordinationContext: te.injectedHookContext, Sink: sink, CreatedAt: time.Now(), Ctx: runCtx,
			}
			var execErr error
			turnRes, execErr = te.lease.Execute(runCtx, turnCtx)
			if execErr != nil {
				err = execErr
			}
			outcome, stderr = te.resolveTurnOutcome(runCtx, turnRes, execErr)
		} else {
			err = fmt.Errorf("queue: no agent execution pool available for thread %s", te.threadID)
			outcome, stderr = te.resolveTurnOutcome(runCtx, nil, err)
		}
		runCancel()

		if outcome.IsSuccess {
			isFailure, isTransient, isSessionCorruption = false, false, false
			errDetail, lastErrDetail, lastStderr = "", "", ""
			if turnRes != nil && turnRes.Stderr != "" {
				stderr = turnRes.Stderr
			}
			resp = &runner.AgyResponse{ConversationID: outcome.SessionID, Status: "SUCCESS", Response: outcome.Response, Usage: outcome.Usage}
			stdout = outcome.Response
			if outcome.SessionID != "" {
				te.resolveAndPersistSessionID(outcome.SessionID, stdout, stderr, true)
			}
		} else {
			isFailure, isTransient, isSessionCorruption = true, outcome.IsTransient, outcome.IsSessionCorruption
			errDetail, lastErrDetail = outcome.ErrorDetail, outcome.ErrorDetail
			if err != nil {
				lastStderr = err.Error()
			} else {
				lastStderr = outcome.ErrorDetail
			}
		}

		if isFailure {
			promptToSend = te.turnPrompt
			targetSess := te.resolveAndPersistSessionID(outcome.SessionID, stdout, stderr, !isSessionCorruption && te.currentSessionID == "")
			if targetSess != "" && te.pool != nil && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(targetSess) {
				poolCtx := context.Background()
				if te.pool.ctx != nil {
					poolCtx = te.pool.ctx
				}
				if transcriptErr, tErr := te.pool.sessionMgr.ExtractLastTurnError(poolCtx, targetSess, te.execStart); tErr == nil && transcriptErr != "" {
					log.Printf("[Queue] Extracted turn error from session %s transcript: %s", targetSess, transcriptErr)
					errDetail, stderr, lastErrDetail, lastStderr = transcriptErr, transcriptErr, transcriptErr, transcriptErr
				}
			}
		}

		runDur := time.Since(runStart)
		runStatus := "success"
		if isFailure {
			runStatus = "error"
		}
		metrics.RecordRunnerExecution(runStatus, currentModel, te.triggerType, runDur)

		if isFailure {
			// Quota Lockout Fail-Fast & Auto-Retry Check
			if runner.IsQuotaPause(errDetail, stderr) {
				if te.handleQuotaPause(errDetail, stderr, currentModel, burstEffort, attempt, maxAttempts) {
					continue
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
				if attempt < maxAttempts {
					if shutdown := te.handleRetryBackoff(attempt, maxAttempts, errDetail, currentModel); shutdown {
						return
					}
					log.Printf("[WorkerPool] Retrying watchdog timeout (attempt %d/%d, session %s)", attempt, maxAttempts, te.currentSessionID)
					continue
				}

				for _, m := range te.burst {
					te.incrementMessageRetry(m.ID, errDetail)
				}
				te.stopIndicators()

				if te.pool.ctx.Err() != nil {
					log.Printf("[WorkerPool] Pool shutting down on watchdog timeout for thread %s. Resetting to PENDING.", te.threadID)
					metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
					te.cancelBurstOnShutdown("timeout", currentModel, false)
					return
				}

				te.rotateSessionID(te.threadID, "")
				metrics.RecordTurnCompleted("watchdog_timeout", te.triggerType, currentModel, time.Since(te.execStart))

				te.deliverFailureNotice(currentAgyBin, currentAPIKey, errDetail, stderr, "execution timed out while processing the request", "watchdog notice")
				te.failBurstWithStatus("timeout", errDetail, currentModel)
				log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED after exhausting %d attempts on watchdog timeout: %s", len(te.burst), te.threadID, maxAttempts, errDetail)
				return
			}
		}

		if !isFailure {
			if resp == nil {
				var parseErr error
				if resp, parseErr = runner.ParseAgyOutput(stdout); parseErr != nil {
					log.Printf("[Queue] Failed to parse runner output despite exit 0: %v", parseErr)
					lastErrDetail = parseErr.Error()
				}
			}
			if resp != nil {
				extSess := resp.ConversationID
				if extSess == "" {
					extSess = runner.ExtractSessionID(stdout+"\n"+stderr, te.execStart)
				}
				if te.currentSessionID == "" && (extSess == "" || !runner.IsValidUUID(extSess)) {
					isTransient = false
					lastErrDetail = "failed to latch active session UUID on cold start"
					errDetail = lastErrDetail
					log.Printf("[Queue] Defensive Failure: %s for thread %s", lastErrDetail, te.threadID)
				} else {
					te.resolveAndPersistSessionID(extSess, stdout, stderr, true)
					if te.handleYieldTrap(resp, activePool, exitCode, stdout, stderr, &autoResumeCount, &accumulatedUsage, &promptToSend, currentModel, runCtx) {
						attempt--
						continue
					}

					if ok, emptyErr := te.deliverSuccessAndFinalize(resp, accumulatedUsage, currentModel, attempt, maxAttempts); ok {
						return
					} else {
						lastErrDetail = emptyErr
						errDetail = emptyErr
					}
				}
			}
		}

		if te.pool.ctx.Err() != nil {
			log.Printf("[WorkerPool] Turn execution cancelled due to pool shutdown (thread: %s, attempt: %d/%d). Resetting to PENDING for clean deployment recovery.", te.threadID, attempt, maxAttempts)
			te.stopIndicators()
			metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
			te.cancelBurstOnShutdown("failed", currentModel, false)
			return
		}

		log.Printf("[WorkerPool] Burst for thread %s failed on attempt %d/%d (transient=%t, corrupt=%t): %s",
			te.threadID, attempt, maxAttempts, isTransient, isSessionCorruption, errDetail)

		if isSessionCorruption {
			te.deliverFailureNotice(currentAgyBin, currentAPIKey, errDetail, stderr, "session reset due to context corruption", "session reset notice")
			te.rotateSessionID(te.threadID, "")
			te.currentSessionID = ""
			if te.lease != nil {
				if relErr := te.lease.Release(); relErr != nil {
					log.Printf("[WorkerPool] Warning releasing lease on corruption reset for %s: %v", te.threadID, relErr)
				}
				te.lease = nil
			}
			if leasedPool, ok := activePool.(runner.LeasedAgentPool); ok {
				if newLease, lErr := leasedPool.AcquireLease(te.pool.ctx, te.threadID); lErr == nil {
					te.lease, te.isEphemeral, te.currentSessionID = newLease, newLease.IsEphemeral() || te.isNonPrimaryPool(activePool), newLease.SessionID()
				}
			}
			if shutdown := te.handleRetryBackoff(attempt, maxAttempts, errDetail, currentModel); shutdown {
				return
			}
			continue
		}

		if isTransient {
			if te.lease != nil {
				te.lease.MarkDirty()
			}
			if shutdown := te.handleRetryBackoff(attempt, maxAttempts, errDetail, currentModel); shutdown {
				return
			}
			log.Printf("[WorkerPool] Retrying transient error in %v (preserving session %s)", time.Duration(attempt)*te.pool.cfg.BackoffBase, te.currentSessionID)
			continue
		}

		te.stopIndicators()
		if te.pool.ctx.Err() != nil {
			log.Printf("[WorkerPool] Pool shutting down during non-transient error for thread %s. Resetting to PENDING.", te.threadID)
			te.cancelBurstOnShutdown("failed", currentModel, false)
			return
		}

		if te.currentSessionID != "" {
			te.previousSessionID, te.currentSessionID = te.currentSessionID, ""
			te.rotateSessionID(te.threadID, "")
		}
		metrics.RecordTurnCompleted("failed", te.triggerType, currentModel, time.Since(te.execStart))
		te.deliverFailureNotice(currentAgyBin, currentAPIKey, errDetail, stderr, "execution failed with non-transient error", "non-transient failure notice")
		for _, m := range te.burst {
			te.incrementMessageRetry(m.ID, errDetail)
		}
		te.failBurstWithStatus("failed", errDetail, currentModel)
		log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED due to non-transient error (attempt %d/%d): %s", len(te.burst), te.threadID, attempt, maxAttempts, errDetail)
		return
	}

	// Total exhaustion after all attempts
	te.stopIndicators()

	if te.pool.ctx.Err() != nil {
		log.Printf("[WorkerPool] Pool shutting down during turn for thread %s. Suppressing exhaustion alert and resetting to PENDING for deployment recovery.", te.threadID)
		metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
		te.cancelBurstOnShutdown("failed", currentModel, true)
		return
	}
	te.deliverFailureNotice(currentAgyBin, currentAPIKey, lastErrDetail, lastStderr, fmt.Sprintf("execution failed after exhausting %d attempts", maxAttempts), "exhaustion notice")
	metrics.RecordTurnCompleted("failed", te.triggerType, currentModel, time.Since(te.execStart))
	te.failBurstWithStatus("failed", lastErrDetail, currentModel)
	log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED after exhausting all %d attempts", len(te.burst), te.threadID, maxAttempts)
}

// watchTaskCompletion polls for background task completion logs in .system_generated/tasks/<taskID>.log.
func watchTaskCompletion(ctx context.Context, sessionDir, taskID string) (int, string, error) {
	logPath := filepath.Join(sessionDir, ".system_generated", "tasks", taskID+".log")
	if _, err := os.Stat(logPath); err == nil {
		return 0, logPath, nil
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return -1, logPath, ctx.Err()
		case <-ticker.C:
			if _, err := os.Stat(logPath); err == nil {
				return 0, logPath, nil
			}
		}
	}
}

func newDiscordTurnSink(statusUpdater *StatusUpdater) *runner.BufferingTurnSink {
	var respondingNotified atomic.Bool
	return runner.NewBufferingTurnSink(runner.BufferingTurnSinkConfig{
		OnThinking: func() {
			respondingNotified.Store(false)
			if statusUpdater != nil {
				statusUpdater.HandleStep(&runner.StepUpdateEvent{Event: "step_update", StepType: "thinking"})
			}
		},
		OnToolCall: func(toolName, commandName string) {
			respondingNotified.Store(false)
			if statusUpdater != nil && (toolName != "" || commandName != "") {
				statusUpdater.HandleStep(&runner.StepUpdateEvent{
					Event: "step_update", State: "RUNNING", Type: "tool_call", ToolName: toolName,
					ToolInfo: &runner.StepToolInfo{Parameters: runner.StepToolParameters{CommandLine: commandName}},
				})
			}
		},
		OnToolCompleted: func(toolName, mcpServer string, duration time.Duration, status string) {
			metrics.RecordToolExecution(toolName, mcpServer, status, duration)
			if statusUpdater != nil && toolName != "" {
				state := "DONE"
				if status != "ok" {
					state = "ERROR"
				}
				statusUpdater.HandleStep(&runner.StepUpdateEvent{Event: "step_update", State: state, Type: "tool_call", ToolName: toolName})
			}
		},
		OnSkillActivated: func(skillName, source string) { metrics.RecordSkillActivation(skillName, source) },
		OnTextDelta: func(delta string) {
			if statusUpdater != nil && !respondingNotified.Load() && strings.TrimSpace(delta) != "" {
				if respondingNotified.CompareAndSwap(false, true) {
					statusUpdater.HandleStep(&runner.StepUpdateEvent{Event: "step_update", StepType: "agent_response"})
				}
			}
		},
	})
}

// calculateCapacityBackoff computes progressive capacity backoff:
// attempt * 30s floor (or resetDur if larger) + up to 3000ms jitter.
func calculateCapacityBackoff(attempt int, resetDur time.Duration) time.Duration {
	minFloor := time.Duration(attempt) * 30 * time.Second
	delay := resetDur
	if delay < minFloor {
		delay = minFloor
	}
	delay += time.Duration(rand.Intn(3000)) * time.Millisecond
	return delay
}

func (te *turnExecution) failBurst(err error) {
	te.stopIndicators()
	metrics.RecordTurnCompleted("failed", te.triggerType, "system", time.Since(te.execStart))
	te.failBurstWithStatus("failed", err.Error(), "")
}

func (te *turnExecution) stopIndicators() {
	if te.stopTyping != nil {
		te.stopTyping()
	}
	if te.statusUpdater != nil {
		te.statusUpdater.Stop()
		te.statusUpdater.DeleteStatusMessage()
	}
}

func (te *turnExecution) failBurstWithStatus(turnStatus, errText, model string) {
	sanitizedErr := sanitizeErrorText(errText)
	te.turnStatus = turnStatus
	te.turnError = sanitizedErr
	te.turnDurationMs = time.Since(te.execStart).Milliseconds()
	te.finalizeBurstMessages(te.burst, db.StatusFailed, sanitizedErr, "failed", sanitizedErr, model, true, false)
}

func (te *turnExecution) completeAmbientMessage(m db.Message, info WakeInfo) {
	metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
	telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.Score, info.Threshold, info.Reason)
	te.finalizeBurstMessages([]db.Message{m}, db.StatusCompleted, telemetry, "completed", "", "", true, false)
}

func (te *turnExecution) cancelBurstOnShutdown(turnStatus, currentModel string, failScheduleRun bool) {
	te.turnStatus = turnStatus
	te.turnError = "interrupted by graceful deployment"
	runStatus := ""
	runErr := ""
	if failScheduleRun {
		te.turnError = "context cancelled during execution"
		runStatus = "failed"
		runErr = "context cancelled during execution"
	}
	te.turnDurationMs = time.Since(te.execStart).Milliseconds()
	te.finalizeBurstMessages(te.burst, db.StatusPending, "interrupted by graceful deployment", runStatus, runErr, currentModel, false, false)
}

func (te *turnExecution) deliverFailureNotice(agyBin, apiKey, errDetail, stderr, descPrefix, logLabel string) {
	if te.pool == nil || te.pool.cfg.DeliveryFunc == nil {
		return
	}
	var notif string
	if isRateLimitError(errDetail, stderr) {
		notif = notifier.ModelUnavailableMessage()
	} else {
		snippet := sanitizeErrorText(errDetail)
		if len([]rune(snippet)) > 300 {
			snippet = string([]rune(snippet)[:300])
		}
		desc := descPrefix
		if desc == "" {
			desc = snippet
		} else if strings.HasSuffix(desc, ": ") || strings.HasSuffix(desc, " ") {
			desc = desc + snippet
		} else {
			desc = desc + ": " + snippet
		}
		notif = te.pool.cfg.NotifierFunc(agyBin, apiKey, desc)
	}
	if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, notif); err != nil {
		log.Printf("[WorkerPool] Failed to deliver %s for thread %s: %v", logLabel, te.threadID, err)
	}
}

func (te *turnExecution) resolveExecutionPool() runner.AgentPool {
	if te.pool == nil {
		return nil
	}
	switch resolveBurstEffort(te.burst) {
	case "medium":
		if te.pool.mediumEffortProcessPool != nil {
			return te.pool.mediumEffortProcessPool
		}
		log.Printf("[Worker] Notice: mediumEffortProcessPool is nil, falling back to processPool for thread %s", te.threadID)
	case "low":
		if te.pool.lowEffortProcessPool != nil {
			return te.pool.lowEffortProcessPool
		}
		log.Printf("[Worker] Notice: lowEffortProcessPool is nil, falling back to processPool for thread %s", te.threadID)
	}
	return te.pool.processPool
}

func (te *turnExecution) isNonPrimaryPool(activePool runner.AgentPool) bool {
	return te != nil && te.pool != nil && activePool != nil && activePool != te.pool.processPool
}

func getStreamingDaemon(pool runner.AgentPool, threadID string) *runner.StreamingDaemon {
	if pool == nil {
		return nil
	}
	if dPool, ok := pool.(interface {
		Get(string) (*runner.StreamingDaemon, bool)
	}); ok {
		if d, ok := dPool.Get(threadID); ok && d != nil {
			return d
		}
	}
	return nil
}
