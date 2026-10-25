package queue

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/notifier"
	"github.com/azylman/aerial/services/eri/pkg/runner"
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

//nolint:unparam
func (te *turnExecution) finalizeBurstMessages(msgs []db.Message, msgStatus, msgOutput, runStatus, runErr, model string, notify, isResponse bool) {
	for _, m := range msgs {
		if isResponse {
			te.updateMessageCompleted(m.ID, msgOutput)
		} else {
			te.updateMessageStatus(m.ID, msgStatus, msgOutput)
		}
		if notify && te.pool != nil && te.pool.cfg.OnMessageCompleted != nil {
			te.pool.cfg.OnMessageCompleted(m, msgStatus)
		}
	}
}

func (te *turnExecution) incrementSessionTurnCount(threadID string) (int, error) {
	if te == nil || te.isEphemeral || isBurstEphemeral(te.burst) {
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

func (te *turnExecution) poolCtx() context.Context {
	if te != nil && te.pool != nil && te.pool.ctx != nil {
		return te.pool.ctx
	}
	return context.Background()
}

func (te *turnExecution) resolveWebhookDispatcher() (WebhookDispatcher, context.Context) {
	var dispatcher WebhookDispatcher
	if te != nil && te.pool != nil {
		dispatcher = te.pool.WebhookDispatcher()
	}
	if dispatcher == nil {
		dispatcher = NewDefaultWebhookDispatcher()
	}
	return dispatcher, te.poolCtx()
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
		isEphemeral: isBurstEphemeral(burst),
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
	activePool, poolKey, poolErr := te.resolveExecutionPool()
	if poolErr != nil {
		te.failBurst(poolErr)
		return
	}
	te.activePool = activePool
	te.isEphemeral = poolKey.Ephemeral

	if activePool == nil {
		te.failBurst(fmt.Errorf("queue: no agent execution pool available for thread %s", te.threadID))
		return
	}

	// 2. Acquire exclusive session lease before prompt construction
	leaseCtx, leaseCancel := context.WithTimeout(te.pool.ctx, 65*time.Second)
	lease, leaseErr := activePool.AcquireLease(leaseCtx, te.threadID)
	leaseCancel()
	if leaseErr != nil {
		te.failBurst(fmt.Errorf("queue: failed to acquire session lease for thread %s: %w", te.threadID, leaseErr))
		return
	}
	te.lease = lease
	defer func() {
		if relErr := te.lease.Release(); relErr != nil {
			log.Printf("[WorkerPool] Warning releasing lease for %s: %v", te.threadID, relErr)
		}
	}()

	te.currentSessionID = lease.SessionID()
	te.previousSessionID = lease.PreviousSessionID()
	te.isCold = lease.IsCold()
	te.isEphemeral = poolKey.Ephemeral || lease.IsEphemeral()
	te.turnCount = lease.TurnCount()

	if te.currentSessionID != "" && te.isCold && !te.isEphemeral {
		te.saveSessionID(te.threadID, te.currentSessionID)
	}

	if !te.runPreTurnHook() {
		return
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
	if te.effectiveName == "" && len(te.burst) > 0 && te.burst[0].ChannelID != "" {
		effID, effName, isTh, thName := ResolveChannelAndThread(te.pool.getDiscordSession(), te.burst[0].ChannelID)
		if effName != "" || effID != "" {
			te.effectiveID = effID
			te.effectiveName = effName
			te.isThread = isTh
			te.threadName = thName
		}
	}
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

func shouldEvaluateAmbientWake(mode string, isThread, isSilent, isHTTP bool) bool {
	return strings.ToLower(mode) == "channel" && !isThread && !isSilent && !isHTTP
}

func (te *turnExecution) resolveWakeHookOverride() string {
	if te.policy.Hooks.OnWake == nil {
		return ""
	}
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
	hookOverride := te.policy.Hooks.OnWake.GetOnTimeout("classify")
	if err == nil && wakeResp != nil && strings.TrimSpace(wakeResp.Override) != "" {
		hookOverride = strings.ToLower(strings.TrimSpace(wakeResp.Override))
	}
	return hookOverride
}

// deliveryTargetID returns the Discord channel or thread snowflake to target for status updates,
// typing indicators, response delivery, and notices. When te.threadID is an internal non-snowflake
// session key (e.g. from /prompt session_id), it falls back to the message's Discord ChannelID or effectiveID.
func (te *turnExecution) deliveryTargetID() string {
	if IsNumericSnowflake(te.threadID) {
		return te.threadID
	}
	if len(te.burst) > 0 && IsNumericSnowflake(te.burst[0].ChannelID) {
		return te.burst[0].ChannelID
	}
	if IsNumericSnowflake(te.effectiveID) {
		return te.effectiveID
	}
	if len(te.burst) > 0 && te.burst[0].ChannelID != "" {
		return te.burst[0].ChannelID
	}
	return te.threadID
}

func (te *turnExecution) evaluateAmbientWake() (shouldExit bool) {
	if shouldEvaluateAmbientWake(te.policy.Mode, te.isThread, extractBurstSilent(te.burst), isHTTPClientBurst(te.burst)) {
		botUserID := ""
		var botRoleIDs []string
		if sess := te.pool.getDiscordSession(); sess != nil && sess.State != nil {
			if sess.State.User != nil {
				botUserID = sess.State.User.ID
			}
			botRoleIDs = ResolveBotRoleIDs(sess, te.burst[0].GuildID, botUserID)
		}

		wakeMode := te.policy.GetWakeMode()
		hookOverride := te.resolveWakeHookOverride()

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
		deliveryTarget := te.deliveryTargetID()
		te.statusUpdater = NewStatusUpdater(te.pool.getDiscordSession(), deliveryTarget, te.isThread)
		if te.pool.cfg.TypingFunc != nil {
			if stop := te.pool.cfg.TypingFunc(te.pool.getDiscordSession(), deliveryTarget); stop != nil {
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
					pauseMsg := notifier.FormatQuotaPauseMessage(remaining, lockedUntil, false, false)
					te.deliverQuotaNotice(pauseMsg, "quota pause message")
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
			if resolved, _, resolveErr := te.resolveExecutionPool(); resolveErr == nil {
				activePool = resolved
				te.activePool = activePool
			}
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
			var memParts []string
			for _, m := range te.burst {
				if bt := strings.TrimSpace(m.BodyText()); bt != "" {
					memParts = append(memParts, bt)
				}
			}
			turnCtx := &runner.TurnContext{
				TurnID: uuid.New().String(), SessionID: sessIDToSend, Model: currentModel,
				Prompt: promptToSend, MemoryQuery: strings.Join(memParts, "\n"),
				ScopeInstructions: scopeInst, ChannelInstructions: scopeInst,
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
				if transcriptErr, tErr := te.pool.sessionMgr.ExtractLastTurnError(te.poolCtx(), targetSess, te.execStart); tErr == nil && transcriptErr != "" {
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
					if te.handleYieldTrap(resp, exitCode, stdout, stderr, &autoResumeCount, &accumulatedUsage, &promptToSend, currentModel) {
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
			if newLease, lErr := activePool.AcquireLease(te.pool.ctx, te.threadID); lErr == nil {
				te.lease, te.isEphemeral, te.currentSessionID = newLease, te.isEphemeral || newLease.IsEphemeral(), newLease.SessionID()
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
