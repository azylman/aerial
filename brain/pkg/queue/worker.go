package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/google/uuid"
)

type wakeInfo = WakeInfo

type turnExecution struct {
	pool                *WorkerPool
	burst               []db.Message
	threadID            string
	triggerType         string
	execStart           time.Time
	currentSessionID    string
	previousSessionID   string
	turnCount           int
	policy              config.ChannelPolicy
	effectiveID         string
	effectiveName       string
	isThread            bool
	skipDiscord         bool
	wakeIdx             int
	wakeInfos           []wakeInfo
	trailingMsgs        []db.Message
	trailingInfos       []wakeInfo
	turnPrompt          string
	isQuotaPaused       bool
	statusUpdater       *StatusUpdater
	stopTyping          func()
	injectedHookContext string
	hookMetadata        map[string]any
	turnAttempted       bool
	turnStatus          string
	turnResponseText    string
	turnError           string
	turnDurationMs      int64
	turnTokenUsage      runner.TokenUsage
}

func parseDBTime(val any) (time.Time, bool) {
	return db.ParseDBTime(val)
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
func GetSessionLastActivity(database db.DBTX, threadID string, mgr ...*session.Manager) (time.Time, bool, error) {
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
		if len(mgr) > 0 && mgr[0] != nil {
			diskActivity, _ = mgr[0].GetSessionLastActivity(internalSessionID.String)
		}
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

var rateLimitKeywords = []string{
	"503",
	"high demand",
	"rate limit",
	"resource_exhausted",
	"429",
	"quota",
	"too many requests",
	"overloaded",
	"service unavailable",
	"resource has been exhausted",
	"individual quota reached",
	"please upgrade your subscription",
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

func (p *WorkerPool) processBurst(burst []db.Message) {
	if len(burst) == 0 {
		return
	}
	if p.ctx.Err() != nil {
		return
	}

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
			var dispatcher WebhookDispatcher
			if te.pool != nil {
				dispatcher = te.pool.WebhookDispatcher()
			}
			if dispatcher == nil {
				dispatcher = NewDefaultWebhookDispatcher()
			}
			_, err := dispatcher.CallPostTurnHook(ctx, te.policy.Hooks.PostTurn, postReq)
			if err != nil {
				log.Printf("[WorkerPool] Warning: post_turn hook failed for thread %s: %v", te.threadID, err)
			}
		}
	}()

	defer func() {
		if te.stopTyping != nil {
			te.stopTyping()
		}
		if te.statusUpdater != nil {
			te.statusUpdater.Stop()
			te.statusUpdater.DeleteStatusMessage()
		}
	}()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WorkerPool] Panic in processBurst for thread %s: %v", te.threadID, r)
			if te.stopTyping != nil {
				te.stopTyping()
			}
			if te.statusUpdater != nil {
				te.statusUpdater.Stop()
				te.statusUpdater.DeleteStatusMessage()
			}
			metrics.RecordTurnCompleted("panic", te.triggerType, te.pool.GetRuntimeConfig(), time.Since(te.execStart))
			errMsg := sanitizeErrorText(fmt.Sprintf("panic: %v", r))
			te.turnStatus = "failed"
			te.turnError = errMsg
			te.turnDurationMs = time.Since(te.execStart).Milliseconds()
			for _, m := range te.burst {
				_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusFailed, errMsg)
				if m.ScheduleRunID != "" {
					_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
						RunID:       m.ScheduleRunID,
						MessageID:   m.ID,
						Status:      "failed",
						CompletedAt: time.Now().UTC(),
						DurationMs:  time.Since(te.execStart).Milliseconds(),
						Error:       errMsg,
					})
				}
				if te.pool.cfg.OnMessageCompleted != nil {
					te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
				}
			}
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

	if te.policy.Hooks.PreTurn != nil {
		preReq := buildPreTurnRequest(te)
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
	// 1. Claim messages from PENDING to PROCESSING
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

	// 2. Staleness TTL check (for burst, check latest message, default 30m)
	stalenessTTL := te.pool.cfg.StalenessTTL
	if stalenessTTL <= 0 {
		stalenessTTL = 30 * time.Minute
	}

	latestMsg := te.burst[0]
	for _, m := range te.burst[1:] {
		if m.CreatedAt.After(latestMsg.CreatedAt) {
			latestMsg = m
		}
	}

	hasRecovered := false
	for _, m := range te.burst {
		if m.RetryCount > 0 || m.RestartCount > 0 {
			hasRecovered = true
			break
		}
	}

	dropBurstAsStale := func(reason string) {
		log.Printf("[WorkerPool] Dropping stale message(s) in thread %s (%s). Marked [EXPIRED_STALE].", te.threadID, reason)
		metrics.RecordTurnCompleted("stale", te.triggerType, "none", time.Since(latestMsg.CreatedAt))
		for _, m := range te.burst {
			_ = db.UpdateMessageCompleted(te.pool.cfg.DB, m.ID, "[EXPIRED_STALE]")
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "completed",
					CompletedAt: time.Now().UTC(),
					DurationMs:  0,
					Error:       "[EXPIRED_STALE]",
				})
			}
			if te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusCompleted)
			}
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
		lastActivity, isColdThread, err = GetSessionLastActivity(te.pool.cfg.DB, te.threadID, te.pool.sessionMgr)
		if err != nil {
			// Fail-open: Retain message if DB error occurs during staleness lookup
			log.Printf("[WorkerPool] Warning: failed to query session last activity for thread %s: %v. Retaining message(s) (fail-open).", te.threadID, err)
		}
	}

	isStale, reason := EvaluateBurstStaleness(te.burst, stalenessTTL, isColdThread, lastActivity, now)
	if isStale {
		dropBurstAsStale(reason)
		return false
	}
	if reason != "" && strings.Contains(reason, "recent activity") {
		log.Printf("[WorkerPool] Retaining message in thread %s (msg age %v > TTL %v) because session had recent activity %v ago <= TTL.",
			te.threadID, msgAge, stalenessTTL, now.Sub(lastActivity))
	}

	te.execStart = time.Now().UTC()
	for _, m := range te.burst {
		if m.ScheduleRunID != "" {
			_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
				RunID:     m.ScheduleRunID,
				MessageID: m.ID,
				Status:    "running",
			})
		}
	}

	return true
}

func (te *turnExecution) resolveTurnPolicy() bool {
	te.effectiveID, te.effectiveName, te.isThread = ResolveEffectiveChannel(te.pool.getDiscordSession(), te.threadID)
	if te.pool.cfg.ResolveChannelPolicy != nil {
		te.policy = te.pool.cfg.ResolveChannelPolicy(te.effectiveID, te.effectiveName)
	} else {
		te.policy = config.GetRuntimeConfig().ResolveChannelPolicy(te.effectiveID, te.effectiveName)
	}

	te.skipDiscord = true
	for _, m := range te.burst {
		if m.AuthorID != "http-client" {
			te.skipDiscord = false
			break
		}
	}

	if !te.skipDiscord && te.policy.IsIgnored() {
		log.Printf("[WorkerPool] Channel %s policy is ignored (mode=%s). Marking %d message(s) completed without execution.", te.threadID, te.policy.Mode, len(te.burst))
		metrics.RecordTurnCompleted("ignored", te.triggerType, "none", time.Since(te.execStart))
		for _, m := range te.burst {
			_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusCompleted, fmt.Sprintf("[%s]", strings.ToUpper(strings.TrimSpace(te.policy.Mode))))
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "completed",
					CompletedAt: time.Now().UTC(),
				})
			}
			if te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusCompleted)
			}
		}
		return false
	}

	te.statusUpdater = NewStatusUpdater(te.pool.getDiscordSession(), te.threadID, te.isThread && !te.skipDiscord)
	te.currentSessionID, _ = db.GetSessionID(te.pool.cfg.DB, te.threadID)

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
			guildID := te.burst[0].GuildID
			botRoleIDs = ResolveBotRoleIDs(sess, guildID, botUserID)
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
			req := &WakeRequest{
				ChannelID: channelID,
				ThreadID:  te.threadID,
				Message:   firstMsg,
				Timestamp: time.Now().UTC(),
			}

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

			wakeResp, err := dispatcher.CallWakeHook(poolCtx, te.policy.Hooks.OnWake, req)
			hookOverride = te.policy.Hooks.OnWake.GetOnTimeout("classify")
			if err == nil && wakeResp != nil && strings.TrimSpace(wakeResp.Override) != "" {
				hookOverride = strings.ToLower(strings.TrimSpace(wakeResp.Override))
			}
		}

		classifierFn := func(msgs []db.Message) (float64, string) {
			var recentContext []db.Message
			if te.pool != nil && te.pool.cfg.DB != nil {
				recentContext, _ = db.GetRecentThreadMessages(te.pool.cfg.DB, te.threadID, 10)
			}
			if te.pool == nil || te.pool.cfg.Classifier == nil {
				return 0.0, "no classifier configured"
			}
			if len(msgs) == 1 {
				res := te.pool.cfg.Classifier.Classify(te.pool.ctx, msgs[0], recentContext, te.policy.GetAmbientWakePrompt())
				return res.Confidence, res.Reason
			}
			res := te.pool.cfg.Classifier.ClassifyBurst(te.pool.ctx, msgs, recentContext, te.policy.GetAmbientWakePrompt())
			log.Printf("[AmbientClassifier] Channel %s | BurstSize %d | Score: %.2f (Threshold: %.2f) | Wake: %t | Reason: %s",
				te.threadID, len(msgs), res.Confidence, te.policy.GetAmbientWakeThreshold(), res.Confidence >= te.policy.GetAmbientWakeThreshold(), res.Reason)
			return res.Confidence, res.Reason
		}

		plan := PlanBurstExecution(BurstPlanInput{
			Burst:        te.burst,
			WakeMode:     wakeMode,
			Threshold:    te.policy.GetAmbientWakeThreshold(),
			BotUserID:    botUserID,
			BotRoleIDs:   botRoleIDs,
			HookOverride: hookOverride,
			ClassifierFn: classifierFn,
		})

		te.wakeIdx = plan.WakeIndex
		te.wakeInfos = plan.WakeInfos

		if plan.IsAllAmbient {
			if te.pool != nil && te.pool.ctx != nil && te.pool.ctx.Err() != nil {
				log.Printf("[WorkerPool] Context cancelled during ambient classification for thread %s. Resetting to PENDING for clean deployment recovery.", te.threadID)
				metrics.RecordTurnCompleted("cancelled", te.triggerType, "classifier", time.Since(te.execStart))
				for _, m := range te.burst {
					_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "interrupted by graceful deployment")
				}
				return true
			}

			// ALL messages in burst are ambient
			te.markAmbientBurst()
			return true
		}

		// wakeIdx >= 0
		// Check session rotation timing before Phase 1:
		currentTurns, _ := db.GetSessionTurnCount(te.pool.cfg.DB, te.threadID)
		lastActivity, isCold, _ := GetSessionLastActivity(te.pool.cfg.DB, te.threadID, te.pool.sessionMgr)
		isTurnLimit := currentTurns >= DefaultMaxSessionTurns || (plan.WakeIndex > 0 && currentTurns+1 >= DefaultMaxSessionTurns)
		isIdleLimit := te.currentSessionID != "" && !isCold && !lastActivity.IsZero() && time.Since(lastActivity) >= DefaultMaxSessionIdleTime
		if isTurnLimit || isIdleLimit {
			if isIdleLimit {
				log.Printf("[Queue] Scope session reached idle limit (%v >= %v). Resetting to cold state for fresh session initialization.", time.Since(lastActivity).Round(time.Minute), DefaultMaxSessionIdleTime)
			} else {
				log.Printf("[Queue] Scope session reached turn limit (%d/%d). Resetting to cold state for fresh session initialization.", currentTurns, DefaultMaxSessionTurns)
			}
			if te.currentSessionID != "" {
				te.previousSessionID = te.currentSessionID
			}
			_ = db.RotateSessionID(te.pool.cfg.DB, te.threadID, "")
			te.currentSessionID = ""
		}

		if te.currentSessionID == "" {
			te.currentSessionID, _ = db.GetSessionID(te.pool.cfg.DB, te.threadID)
		}
		if te.currentSessionID != "" && te.pool.sessionMgr != nil && !te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID) {
			log.Printf("[Queue] Session %s for thread %s not found on disk. Clearing for fresh Turn 1.", te.currentSessionID, te.threadID)
			te.currentSessionID = ""
		}
		if te.currentSessionID != "" && te.pool.sessionMgr != nil {
			_, _ = te.pool.sessionMgr.EnsureSessionDir(te.currentSessionID)
		}

		// Phase 1 (Leading ambient messages)
		for i, m := range plan.LeadingAmbient {
			info := plan.WakeInfos[i]
			metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
			telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.Score, info.Threshold, info.Reason)
			_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusCompleted, telemetry)
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "completed",
					CompletedAt: time.Now().UTC(),
				})
			}
			if te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusCompleted)
			}
		}

		// Phase 2 (Active wake batch)
		te.trailingMsgs = plan.TrailingMessages
		te.trailingInfos = plan.TrailingInfos
		te.burst = []db.Message{plan.ActiveMessage}
		metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Inc()
	}

	if !te.skipDiscord && te.pool.cfg.TypingFunc != nil {
		if stop := te.pool.cfg.TypingFunc(te.pool.getDiscordSession(), te.threadID); stop != nil {
			te.stopTyping = stop
		}
	}

	// Pre-execution turn limit and idle rotation check (applies to both channel and thread modes)
	// Run BEFORE IncrementSessionTurnCount to prevent premature rotation and double-rotation.
	currentTurns, _ := db.GetSessionTurnCount(te.pool.cfg.DB, te.threadID)
	lastActivity, isCold, _ := GetSessionLastActivity(te.pool.cfg.DB, te.threadID, te.pool.sessionMgr)
	isTurnLimit := currentTurns >= DefaultMaxSessionTurns
	isIdleLimit := te.currentSessionID != "" && !isCold && !lastActivity.IsZero() && time.Since(lastActivity) >= DefaultMaxSessionIdleTime
	if isTurnLimit || isIdleLimit {
		if isIdleLimit {
			log.Printf("[Queue] Scope session reached idle limit (%v >= %v). Resetting to cold state for fresh session initialization.", time.Since(lastActivity).Round(time.Minute), DefaultMaxSessionIdleTime)
		} else {
			log.Printf("[Queue] Scope session reached turn limit (%d/%d). Resetting to cold state for fresh session initialization.", currentTurns, DefaultMaxSessionTurns)
		}
		if te.currentSessionID != "" {
			te.previousSessionID = te.currentSessionID
		}
		_ = db.RotateSessionID(te.pool.cfg.DB, te.threadID, "")
		te.currentSessionID = ""
	}

	var incErr error
	te.turnCount, incErr = db.IncrementSessionTurnCount(te.pool.cfg.DB, te.threadID)
	if incErr != nil {
		log.Printf("[Queue] Error incrementing turn count for thread %s: %v", te.threadID, incErr)
	}

	return false
}

func (te *turnExecution) markAmbientBurst() {
	if te.pool == nil {
		return
	}
	hasDiskSession := te.currentSessionID != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID)
	if hasDiskSession && te.pool.sessionMgr != nil {
		_, _ = te.pool.sessionMgr.EnsureSessionDir(te.currentSessionID)
	}

	metrics.RecordTurnCompleted("ambient", te.triggerType, "classifier", time.Since(te.execStart))

	for i, m := range te.burst {
		var info WakeInfo
		if i < len(te.wakeInfos) {
			info = te.wakeInfos[i]
		}
		metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
		telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.Score, info.Threshold, info.Reason)
		_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusCompleted, telemetry)
		if m.ScheduleRunID != "" {
			_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
				RunID:       m.ScheduleRunID,
				MessageID:   m.ID,
				Status:      "completed",
				CompletedAt: time.Now().UTC(),
			})
		}
		if te.pool.cfg.OnMessageCompleted != nil {
			te.pool.cfg.OnMessageCompleted(m, db.StatusCompleted)
		}
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
	msgs := make([]*db.Message, len(te.burst))
	maxRetry := 0
	for i := range te.burst {
		msgs[i] = &te.burst[i]
		if te.burst[i].RetryCount > maxRetry {
			maxRetry = te.burst[i].RetryCount
		}
	}
	prompt := te.turnPrompt
	if prompt == "" {
		prompt = CoalesceBurstPrompt(te.burst)
	}
	return &PreTurnRequest{
		ChannelID:  channelID,
		ThreadID:   te.threadID,
		BurstCount: len(te.burst),
		Messages:   msgs,
		Prompt:     prompt,
		RetryCount: maxRetry,
		Timestamp:  time.Now().UTC(),
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
		ChannelID:    channelID,
		ThreadID:     te.threadID,
		Status:       status,
		ResponseText: te.turnResponseText,
		Error:        te.turnError,
		DurationMs:   durMs,
		TokenUsage:   te.turnTokenUsage,
		Metadata:     te.hookMetadata,
		Timestamp:    time.Now().UTC(),
	}
}

func (te *turnExecution) abortBurst(reason string) {
	if te.stopTyping != nil {
		te.stopTyping()
	}
	if te.statusUpdater != nil {
		te.statusUpdater.Stop()
		te.statusUpdater.DeleteStatusMessage()
	}
	if te.pool != nil {
		metrics.RecordTurnCompleted("aborted", te.triggerType, "pre_turn", time.Since(te.execStart))
	}
	telemetry := fmt.Sprintf("[SKIPPED %s]", reason)
	for _, m := range te.burst {
		if te.pool != nil && te.pool.cfg.DB != nil {
			_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusCompleted, telemetry)
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "completed",
					CompletedAt: time.Now().UTC(),
					Error:       telemetry,
				})
			}
			if te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusCompleted)
			}
		}
	}
}

func (te *turnExecution) rescheduleBurst(delaySeconds int) {
	if delaySeconds <= 0 {
		delaySeconds = 5
	}
	if te.stopTyping != nil {
		te.stopTyping()
	}
	if te.statusUpdater != nil {
		te.statusUpdater.Stop()
		te.statusUpdater.DeleteStatusMessage()
	}
	maxAttempts := 3
	if te.pool != nil && te.pool.cfg.MaxAttempts > 0 {
		maxAttempts = te.pool.cfg.MaxAttempts
	}
	for _, m := range te.burst {
		if m.RetryCount+1 >= maxAttempts {
			log.Printf("[WorkerPool] Message %s in thread %s exceeded max retry attempts (%d). Marking FAILED.", m.ID, te.threadID, maxAttempts)
			if te.pool != nil && te.pool.cfg.DB != nil {
				_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusFailed, "[EXHAUSTED_PRE_TURN_RETRIES]")
				if m.ScheduleRunID != "" {
					_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
						RunID:       m.ScheduleRunID,
						MessageID:   m.ID,
						Status:      "failed",
						CompletedAt: time.Now().UTC(),
						DurationMs:  time.Since(te.execStart).Milliseconds(),
						Error:       "[EXHAUSTED_PRE_TURN_RETRIES]",
					})
				}
				if te.pool.cfg.OnMessageCompleted != nil {
					te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
				}
			}
			continue
		}
		if te.pool != nil && te.pool.cfg.DB != nil {
			_ = db.IncrementMessageRetry(te.pool.cfg.DB, m.ID, "pre_turn_deferred")
			_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "pre_turn_deferred")
		}
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
			_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusFailed, telemetry)
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "failed",
					CompletedAt: time.Now().UTC(),
					Error:       telemetry,
				})
			}
			if te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
			}
			continue
		}
		if !info.IsWake {
			metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
			telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.Score, info.Threshold, info.Reason)
			_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusCompleted, telemetry)
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "completed",
					CompletedAt: time.Now().UTC(),
				})
			}
			if te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusCompleted)
			}
		} else {
			metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Inc()
			_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "")
			m.Status = db.StatusPending
			te.pool.Enqueue(m)
		}
	}
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
		fetchCtx, fetchCancel := context.WithTimeout(te.pool.ctx, 3*time.Second)
		defer fetchCancel()
		var err error
		if te.pool.cfg.HistoryFetcher != nil {
			turnHistory, err = te.pool.cfg.HistoryFetcher(fetchCtx, te.threadID, te.burst[0].ID, limit)
		} else {
			turnHistory, err = FetchRecentThreadHistory(fetchCtx, te.pool.getDiscordSession(), te.pool.cfg.DB, te.threadID, limit)
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
	isThreadColdStart := snap.IsThread && snap.ParentID != "" && snap.ParentID != snap.ID && te.currentSessionID == ""

	isColdStart := te.currentSessionID == ""
	hasHistoryNeed := isColdStart || te.wakeIdx > 0 || len(te.burst) > 1

	var summary string
	if isThreadColdStart {
		cachedSum, lastMsgID, _ := db.GetThreadSummary(te.pool.cfg.DB, te.threadID)

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
				log.Printf("[WorkerPool] Using cached thread summary for thread %s (watermark msg: %s)", te.threadID, lastMsgID)
			} else {
				llmFn := te.pool.cfg.LLMFunc
				if llmFn == nil && te.pool.cfg.Classifier != nil && te.pool.cfg.Classifier.LLMFunc != nil {
					llmFn = te.pool.cfg.Classifier.LLMFunc
				}
				if llmFn == nil {
					llmFn = classifier.NewAgyLLMFunc(te.pool.cfg.AgyBin, te.pool.cfg.APIKey, te.pool.cfg.RunnerFunc)
				}

				flashModel := ""
				if te.pool.cfg.Classifier != nil && te.pool.cfg.Classifier.Model != "" {
					flashModel = te.pool.cfg.Classifier.Model
				}
				if flashModel == "" && te.pool.appCfg != nil && te.pool.appCfg.Current() != nil {
					cur := te.pool.appCfg.Current()
					if cur.LowEffortModel != "" {
						flashModel = cur.LowEffortModel
					} else {
						flashModel = cur.ClassifierModel
					}
				}
				if flashModel == "" {
					flashModel = te.pool.cfg.Model
				}

				sumCtx, sumCancel := context.WithTimeout(te.pool.ctx, DefaultThreadSummaryTimeout)
				newSum, sumErr := SummarizeThreadHistoryWithGroup(sumCtx, te.pool.SummaryGroup(), llmFn, flashModel, te.threadID, histMsgs)
				sumCancel()

				if sumErr != nil {
					log.Printf("[WorkerPool] Warning: Thread history summarization failed for thread %s: %v. Falling back to raw lookback.", te.threadID, sumErr)
				} else if newSum != "" {
					summary = newSum
					if err := db.SaveThreadSummary(te.pool.cfg.DB, te.threadID, summary, latestMsgID); err != nil {
						log.Printf("[WorkerPool] Warning: Failed to save thread summary to DB for thread %s: %v", te.threadID, err)
					}
				}
			}
		}

		if summary != "" {
			log.Printf("[WorkerPool] Injected <THREAD_SUMMARY> into Turn 1 prompt for thread %s", te.threadID)
		}
	}

	var lookbackMsgs []HistoryMessage
	if hasHistoryNeed {
		lookbackMsgs = getTurnHistory(10)
		if len(lookbackMsgs) > 0 {
			log.Printf("[WorkerPool] Injected channel history into prompt for thread %s", te.threadID)
		}
	}

	var prevID string
	if isColdStart {
		prevID = te.previousSessionID
		if prevID == "" && te.pool.cfg.DB != nil {
			prevID, _ = db.GetPreviousSessionID(te.pool.cfg.DB, te.threadID)
		}
		if prevID != "" {
			log.Printf("[WorkerPool] Injected previous session identifier (%s) into prompt for thread %s", prevID, te.threadID)
		}
	}

	baseCoalesced := CoalesceBurstPrompt(te.burst)
	queryText := memory.ExtractQueryText(baseCoalesced)
	if queryText == "" && summary != "" {
		queryText = memory.ExtractQueryText(summary)
	}

	var facts []db.Fact
	if te.pool.cfg.MemoryRetrieverFunc != nil && te.pool.cfg.DB != nil && strings.TrimSpace(queryText) != "" {
		maxFacts := 10
		if isThreadColdStart {
			maxFacts = 5
		}
		retrievalCtx, retrievalCancel := context.WithTimeout(te.pool.ctx, 2500*time.Millisecond)
		var err error
		facts, err = te.pool.cfg.MemoryRetrieverFunc(retrievalCtx, te.pool.cfg.DB, te.pool.cfg.MemoryClient, queryText, maxFacts)
		retrievalCancel()
		if err != nil {
			log.Printf("[WorkerPool] Warning: Semantic memory retrieval failed for thread %s: %v. Proceeding without injected facts.", te.threadID, err)
		} else if len(facts) > 0 {
			if isThreadColdStart && len(facts) > 5 {
				facts = facts[:5]
			}
			log.Printf("[WorkerPool] Injected %d semantic memory fact(s) into prompt for thread %s", len(facts), te.threadID)
		}
	}

	instructions := config.LoadChannelInstructions(te.effectiveName)
	if instructions != "" {
		log.Printf("[WorkerPool] Injected channel instructions for #%s into prompt", te.effectiveName)
	}
	if te.injectedHookContext != "" {
		log.Printf("[WorkerPool] Injected coordination context into prompt for thread %s", te.threadID)
	}

	te.turnPrompt = AssembleTurnPrompt(TurnPromptInput{
		Burst:               te.burst,
		ThreadSummary:       summary,
		LookbackHistory:     lookbackMsgs,
		PreviousSessionID:   prevID,
		IsColdStart:         isColdStart,
		SemanticMemoryFacts: facts,
		ChannelInstructions: instructions,
		InjectedHookContext: te.injectedHookContext,
	})
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

	var currentAgyBin, currentAPIKey string
	te.pool.mu.Lock()
	currentAgyBin = te.pool.cfg.AgyBin
	currentAPIKey = te.pool.cfg.APIKey
	te.pool.mu.Unlock()
	if te.pool.appCfg != nil {
		if cur := te.pool.appCfg.Current(); cur != nil {
			if cur.AgyBin != "" {
				currentAgyBin = cur.AgyBin
			}
			if cur.APIKey != "" {
				currentAPIKey = cur.APIKey
			}
		}
	}

	for attempt := initialRetryCount + 1; attempt <= maxAttempts; attempt++ {
		te.pool.mu.Lock()
		currentModel = te.pool.cfg.Model
		lowEffortModel := te.pool.cfg.LowEffortModel
		currentTimeout := te.pool.cfg.TimeoutMinutes
		if te.pool.cfg.AgyBin != "" {
			currentAgyBin = te.pool.cfg.AgyBin
		}
		if te.pool.cfg.APIKey != "" {
			currentAPIKey = te.pool.cfg.APIKey
		}
		overrideModel := te.pool.overrideModel
		te.pool.mu.Unlock()

		if te.pool.appCfg != nil {
			if cur := te.pool.appCfg.Current(); cur != nil {
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
		for _, m := range te.burst {
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
			if lockedUntilUnix := te.pool.quotaLockedUntil.Load(); lockedUntilUnix > 0 {
				lockedUntil := time.Unix(lockedUntilUnix, 0)
				if time.Now().Before(lockedUntil) {
					te.isQuotaPaused = true
					remaining := time.Until(lockedUntil)
					log.Printf("[WorkerPool] Global quota pause active for thread %s (%v remaining). Aborting turn.", te.threadID, remaining)
					te.stopTyping()
					reason := fmt.Sprintf("[QUOTA_PAUSED reset_in=%v scheduled=false] global quota pause active", remaining)
					metrics.RecordRunnerError("quota_paused", currentModel)
					metrics.RecordTurnCompleted("quota_paused", te.triggerType, currentModel, time.Since(te.execStart))
					te.turnStatus = "failed"
					te.turnError = reason
					te.turnDurationMs = time.Since(te.execStart).Milliseconds()
					if !te.skipDiscord && te.pool.cfg.DeliveryFunc != nil {
						pauseMsg := notifier.FormatQuotaPauseMessage(remaining, lockedUntil, false, false)
						_ = te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, pauseMsg)
					}
					for _, m := range te.burst {
						_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusFailed, reason)
						if m.ScheduleRunID != "" {
							_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "failed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(te.execStart).Milliseconds(),
								Error:       reason,
								Model:       currentModel,
							})
						}
						if te.pool.cfg.OnMessageCompleted != nil {
							te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
						}
					}
					return
				}
			}
		}

		promptToSend := te.turnPrompt
		if attempt > 1 {
			if (runner.IsInactivityTimeout(lastErrDetail, lastStderr) || strings.Contains(lastErrDetail, "max duration exceeded")) && te.currentSessionID != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID) {
				promptToSend = fmt.Sprintf(ContinuationPromptTemplate, te.turnPrompt)
			}
		}

		// Pad Go context by +1 minute relative to runner watchdog ceiling so the runner watchdog always fires cleanly
		runCtx, runCancel := context.WithTimeout(te.pool.ctx, time.Duration(currentTimeout+1)*time.Minute)

		var stdout, stderr string
		var exitCode int
		var err error

		if te.pool.cfg.RunnerWithOptionsFunc != nil {
			watchdogOpts := runner.DefaultWatchdogOptions(currentTimeout)
			if te.pool.sessionMgr != nil {
				watchdogOpts.TranscriptDirs = te.pool.sessionMgr.Roots()
				watchdogOpts.HomeDir = te.pool.sessionMgr.HomeDir()
			}
			if te.statusUpdater != nil {
				te.statusUpdater.MarkTurnStarted()
				watchdogOpts.StepUpdateHandler = te.statusUpdater.HandleStep
			}
			if strings.TrimSpace(te.threadID) != "" {
				watchdogOpts.TargetID = strings.TrimSpace(te.threadID)
			}
			stdout, stderr, exitCode, err = te.pool.cfg.RunnerWithOptionsFunc(
				runCtx,
				currentAgyBin,
				promptToSend,
				te.currentSessionID,
				currentAPIKey,
				currentModel,
				watchdogOpts,
			)
		} else if te.pool.cfg.RunnerFunc != nil {
			stdout, stderr, exitCode, err = te.pool.cfg.RunnerFunc(
				runCtx,
				currentAgyBin,
				promptToSend,
				te.currentSessionID,
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

		if isFailure && isSessionCorruption && te.currentSessionID == "" {
			lowerErr := strings.ToLower(errDetail + " " + stderr + " " + stdout)
			if strings.Contains(lowerErr, "context window") || strings.Contains(lowerErr, "context length") || strings.Contains(lowerErr, "maximum context length") || strings.Contains(lowerErr, "token limit exceeded") || strings.Contains(lowerErr, "prompt is too long") || strings.Contains(lowerErr, "request too large") {
				isSessionCorruption = false
				isTransient = false
				errDetail = "prompt length exceeds maximum model context window (hard failure)"
				log.Printf("[Queue] Cold start context window exceeded for thread %s; converting to non-transient fail-fast", te.threadID)
			}
		}

		lastErrDetail = errDetail
		lastStderr = stderr

		if isFailure {
			targetSess := te.currentSessionID
			if targetSess == "" {
				combinedOutput := stdout + "\n" + stderr
				if extSess := runner.ExtractSessionID(combinedOutput, te.execStart); extSess != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(extSess) {
					targetSess = extSess
				}
			}

			// Transcript Recovery on Exit Code 0:
			// If agy completed with exit code 0 but was flagged as a failure (e.g. empty stdout from buffering,
			// or stream-json reporting an error status despite the model successfully generating a response),
			// check if the session transcript on disk contains a valid PLANNER_RESPONSE turn.
			if exitCode == 0 && targetSess != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(targetSess) {
				if respText, _ := te.pool.sessionMgr.ExtractResponseAndError(targetSess); respText != "" && !strings.HasPrefix(respText, "[Tool Call Requested]:") {
					log.Printf("[Queue] Recovered response directly from session %s transcript after runner failure on exit 0", targetSess)
					isFailure = false
					isSessionCorruption = false
					isTransient = false
					if te.currentSessionID == "" {
						te.currentSessionID = targetSess
						_ = db.SaveSessionID(te.pool.cfg.DB, te.threadID, te.currentSessionID)
					}
					stdout = fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":%q}`, te.currentSessionID, respText)
				}
			}

			// Cold-Start Dynamic Session Latching:
			// If this was a cold start and remains an unrecovered failure, only latch the active session
			// if the failure was NOT session corruption (so retries won't inherit corrupted state).
			if isFailure && te.currentSessionID == "" && !isSessionCorruption && targetSess != "" {
				log.Printf("[Queue] Latched active session from output on failure (attempt %d/%d) for thread %s: %s", attempt, maxAttempts, te.threadID, targetSess)
				te.currentSessionID = targetSess
				_ = db.SaveSessionID(te.pool.cfg.DB, te.threadID, te.currentSessionID)
			}
		}

		if isFailure {
			// Quota Lockout Fail-Fast & Auto-Retry Check
			if runner.IsQuotaPause(errDetail, stderr) {
				te.isQuotaPaused = true
				te.stopTyping()
				log.Printf("[WorkerPool] Quota pause detected for thread %s on attempt %d/%d: %s", te.threadID, attempt, maxAttempts, errDetail)

				// Cold-start dynamic session latching if available
				if te.currentSessionID == "" && !isSessionCorruption {
					combinedOutput := stdout + "\n" + stderr
					if extSess := runner.ExtractSessionID(combinedOutput, te.execStart); extSess != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(extSess) {
						te.currentSessionID = extSess
						_ = db.SaveSessionID(te.pool.cfg.DB, te.threadID, te.currentSessionID)
					}
				}

				// Circuit breaker: check if this turn was already a quota auto-retry
				isAlreadyRetry := te.burst[0].ScheduleRunID != "" || strings.HasPrefix(te.burst[0].Content, "[QUOTA_RETRY]") || strings.Contains(te.turnPrompt, "[QUOTA_RETRY]")

				resetDur, _ := runner.ExtractQuotaResetDuration(errDetail, stderr)
				jitterSec := 5 + rand.Intn(16) // 5s to 20s jitter
				runAt := time.Now().UTC().Add(resetDur).Add(30 * time.Second).Add(time.Duration(jitterSec) * time.Second)

				te.pool.quotaLockedUntil.Store(runAt.Unix())

				var scheduled bool
				if !isAlreadyRetry {
					retryPrompt := fmt.Sprintf("[QUOTA_RETRY] %s", te.turnPrompt)
					oneShotID := uuid.New().String()
					oneShot := db.OneShotSchedule{
						ID:        oneShotID,
						ThreadID:  te.threadID,
						Prompt:    retryPrompt,
						RunAt:     runAt,
						CreatedAt: time.Now().UTC(),
					}
					if err := db.CreateOneShotSchedule(te.pool.cfg.DB, oneShot); err != nil {
						log.Printf("[WorkerPool] Failed to create one-shot retry schedule for thread %s: %v", te.threadID, err)
					} else {
						scheduled = true
						log.Printf("[WorkerPool] Scheduled one-shot retry %s for thread %s at %s (+%v)", oneShotID, te.threadID, runAt.Format(time.RFC3339), resetDur)
					}
				} else {
					log.Printf("[WorkerPool] Circuit breaker: turn for thread %s was already an auto-retry. Skipping further scheduling.", te.threadID)
				}

				metrics.RecordRunnerError("quota_paused", currentModel)
				metrics.RecordTurnCompleted("quota_paused", te.triggerType, currentModel, time.Since(te.execStart))

				te.stopTyping()
				if te.statusUpdater != nil {
					te.statusUpdater.Stop()
					te.statusUpdater.DeleteStatusMessage()
				}

				if !te.skipDiscord && te.pool.cfg.DeliveryFunc != nil {
					pauseMsg := notifier.FormatQuotaPauseMessage(resetDur, runAt, scheduled, isAlreadyRetry)
					if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, pauseMsg); err != nil {
						log.Printf("[WorkerPool] Failed to deliver quota pause notice for thread %s: %v", te.threadID, err)
					}
				}

				reason := fmt.Sprintf("[QUOTA_PAUSED reset_in=%v scheduled=%t] %s", resetDur, scheduled, sanitizeErrorText(errDetail))
				te.turnStatus = "failed"
				te.turnError = reason
				te.turnDurationMs = time.Since(te.execStart).Milliseconds()
				for _, m := range te.burst {
					_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusFailed, reason)
					if m.ScheduleRunID != "" {
						_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
							RunID:       m.ScheduleRunID,
							MessageID:   m.ID,
							Status:      "failed",
							CompletedAt: time.Now().UTC(),
							DurationMs:  time.Since(te.execStart).Milliseconds(),
							Error:       reason,
							Model:       currentModel,
						})
					}
					if te.pool.cfg.OnMessageCompleted != nil {
						te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
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
				for _, m := range te.burst {
					_ = db.IncrementMessageRetry(te.pool.cfg.DB, m.ID, errDetail)
				}
				if attempt < maxAttempts {
					if te.statusUpdater != nil {
						te.statusUpdater.Reset()
					}
					backoff := time.Duration(attempt) * te.pool.cfg.BackoffBase
					log.Printf("[WorkerPool] Retrying watchdog timeout in %v (attempt %d/%d, preserving session %s)", backoff, attempt, maxAttempts, te.currentSessionID)
					select {
					case <-time.After(backoff):
					case <-te.pool.ctx.Done():
						metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
						te.turnStatus = "failed"
						te.turnError = "context cancelled during execution"
						te.turnDurationMs = time.Since(te.execStart).Milliseconds()
						for _, m := range te.burst {
							_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "interrupted by graceful deployment")
							if m.ScheduleRunID != "" {
								_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
									RunID:       m.ScheduleRunID,
									MessageID:   m.ID,
									Status:      "failed",
									CompletedAt: time.Now().UTC(),
									DurationMs:  time.Since(te.execStart).Milliseconds(),
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
				te.stopTyping()
				if te.statusUpdater != nil {
					te.statusUpdater.Stop()
					te.statusUpdater.DeleteStatusMessage()
				}

				if te.pool.ctx.Err() != nil {
					log.Printf("[WorkerPool] Pool shutting down on watchdog timeout for thread %s. Resetting to PENDING.", te.threadID)
					metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
					te.turnStatus = "timeout"
					te.turnError = "context cancelled during execution"
					te.turnDurationMs = time.Since(te.execStart).Milliseconds()
					for _, m := range te.burst {
						_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "interrupted by graceful deployment")
					}
					return
				}

				_ = db.RotateSessionID(te.pool.cfg.DB, te.threadID, "")
				metrics.RecordTurnCompleted("watchdog_timeout", te.triggerType, currentModel, time.Since(te.execStart))

				if !te.skipDiscord {
					sanitizedSnippet := sanitizeErrorText(errDetail)
					if len([]rune(sanitizedSnippet)) > 300 {
						sanitizedSnippet = string([]rune(sanitizedSnippet)[:300])
					}
					var notif string
					if isRateLimitError(errDetail, stderr) {
						notif = notifier.ModelUnavailableMessage()
					} else {
						notif = te.pool.cfg.NotifierFunc(currentAgyBin, currentAPIKey, "execution timed out while processing the request: "+sanitizedSnippet)
					}
					if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, notif); err != nil {
						log.Printf("[WorkerPool] Failed to deliver watchdog notice for thread %s: %v", te.threadID, err)
					}
				}

				sanitizedErr := sanitizeErrorText(errDetail)
				te.turnStatus = "timeout"
				te.turnError = sanitizedErr
				te.turnDurationMs = time.Since(te.execStart).Milliseconds()
				for _, m := range te.burst {
					_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusFailed, sanitizedErr)
					if m.ScheduleRunID != "" {
						_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
							RunID:       m.ScheduleRunID,
							MessageID:   m.ID,
							Status:      "failed",
							CompletedAt: time.Now().UTC(),
							DurationMs:  time.Since(te.execStart).Milliseconds(),
							Error:       sanitizedErr,
							Model:       currentModel,
						})
					}
					if te.pool.cfg.OnMessageCompleted != nil {
						te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
					}
				}
				log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED after exhausting %d attempts on watchdog timeout: %s", len(te.burst), te.threadID, maxAttempts, errDetail)
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
					extSess = runner.ExtractSessionID(stdout+"\n"+stderr, te.execStart)
				}
				if te.currentSessionID == "" && (extSess == "" || !runner.IsValidUUID(extSess)) {
					isFailure = true
					isTransient = false
					lastErrDetail = "failed to latch active session UUID on cold start"
					log.Printf("[Queue] Defensive Failure: %s for thread %s", lastErrDetail, te.threadID)
				} else {
					if extSess != "" && extSess != te.currentSessionID {
						log.Printf("[Queue] Active session synchronized for thread %s: %s -> %s", te.threadID, te.currentSessionID, extSess)
						te.currentSessionID = extSess
						_ = db.SaveSessionID(te.pool.cfg.DB, te.threadID, te.currentSessionID)
					}
					te.stopTyping()
					if te.statusUpdater != nil {
						te.statusUpdater.Stop()
						te.statusUpdater.DeleteStatusMessage()
					}

					responseText := resp.Response
					baseDir := "/root/.gemini/antigravity-cli/brain"
					if te.currentSessionID != "" {
						baseDir = filepath.Join(baseDir, te.currentSessionID)
					}
					cleanText, attachments := delivery.ExtractAndSanitizeMedia(responseText, baseDir)
					attachments = delivery.AutoAttachNewMedia(baseDir, te.execStart, attachments)

					isSilent := runner.IsSilentSentinel(cleanText) && len(attachments) == 0
					if isSilent {
						log.Printf("[Queue] Output is empty. Skipping Discord delivery.")
					} else {
						if !te.skipDiscord {
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
					}

					if te.turnCount >= DefaultMaxSessionTurns {
						log.Printf("[Queue] Scope session reached turn limit (%d/%d). Resetting to cold state for fresh session initialization.", te.turnCount, DefaultMaxSessionTurns)
						_ = db.RotateSessionID(te.pool.cfg.DB, te.threadID, "")
						te.currentSessionID = ""
					}

					// Mark all messages in the burst as completed with unpacked clean text
					metrics.RecordTurnCompleted("success", te.triggerType, currentModel, time.Since(te.execStart))
					metrics.RecordTokens(currentModel, resp.Usage.InputTokens, resp.Usage.OutputTokens, resp.Usage.ThinkingTokens, resp.Usage.CacheReadTokens, resp.Usage.TotalTokens)
					te.turnStatus = "success"
					te.turnResponseText = resp.Response
					te.turnTokenUsage = resp.Usage
					te.turnDurationMs = time.Since(te.execStart).Milliseconds()
					for _, m := range te.burst {
						_ = db.UpdateMessageCompleted(te.pool.cfg.DB, m.ID, cleanText)
						if m.ScheduleRunID != "" {
							_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "completed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(te.execStart).Milliseconds(),
								Model:       currentModel,
							})
						}
						if te.pool.cfg.OnMessageCompleted != nil {
							te.pool.cfg.OnMessageCompleted(m, db.StatusCompleted)
						}
					}
					log.Printf("[WorkerPool] %d message(s) in thread %s completed successfully on attempt %d/%d", len(te.burst), te.threadID, attempt, maxAttempts)

					return
				}
			}
		}

		// If execution failed because the pool context was cancelled (SIGTERM/shutdown),
		// suppress Discord error notifications and do NOT mark FAILED or increment retries.
		// Reset messages to PENDING for clean deployment recovery on container restart.
		if te.pool.ctx.Err() != nil {
			log.Printf("[WorkerPool] Turn execution cancelled due to pool shutdown (thread: %s, attempt: %d/%d). Resetting to PENDING for clean deployment recovery.", te.threadID, attempt, maxAttempts)
			te.stopTyping()
			metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
			te.turnStatus = "failed"
			te.turnError = "interrupted by graceful deployment"
			te.turnDurationMs = time.Since(te.execStart).Milliseconds()
			for _, m := range te.burst {
				_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "interrupted by graceful deployment")
			}
			return
		}

		log.Printf("[WorkerPool] Burst for thread %s failed on attempt %d/%d (transient=%t, corrupt=%t): %s",
			te.threadID, attempt, maxAttempts, isTransient, isSessionCorruption, errDetail)

		if isSessionCorruption {
			for _, m := range te.burst {
				_ = db.IncrementMessageRetry(te.pool.cfg.DB, m.ID, errDetail)
			}
			if te.statusUpdater != nil {
				te.statusUpdater.Reset()
			}
			var notif string
			if isRateLimitError(errDetail, stderr) {
				notif = notifier.ModelUnavailableMessage()
			} else {
				notif = te.pool.cfg.NotifierFunc(currentAgyBin, currentAPIKey, "session reset due to context corruption")
			}
			if !te.skipDiscord {
				if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, notif); err != nil {
					log.Printf("[WorkerPool] Failed to deliver session reset notice for thread %s: %v", te.threadID, err)
				}
			}
			_ = db.RotateSessionID(te.pool.cfg.DB, te.threadID, "")
			te.currentSessionID = ""

			if attempt < maxAttempts {
				backoff := time.Duration(attempt) * te.pool.cfg.BackoffBase
				select {
				case <-time.After(backoff):
				case <-te.pool.ctx.Done():
					metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
					te.turnStatus = "failed"
					te.turnError = "context cancelled during execution"
					te.turnDurationMs = time.Since(te.execStart).Milliseconds()
					for _, m := range te.burst {
						_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "interrupted by graceful deployment")
						if m.ScheduleRunID != "" {
							_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "failed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(te.execStart).Milliseconds(),
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
			for _, m := range te.burst {
				_ = db.IncrementMessageRetry(te.pool.cfg.DB, m.ID, errDetail)
			}
			if attempt < maxAttempts {
				if te.statusUpdater != nil {
					te.statusUpdater.Reset()
				}
				backoff := time.Duration(attempt) * te.pool.cfg.BackoffBase
				log.Printf("[WorkerPool] Retrying transient error in %v (preserving session %s)", backoff, te.currentSessionID)
				select {
				case <-time.After(backoff):
				case <-te.pool.ctx.Done():
					metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
					te.turnStatus = "failed"
					te.turnError = "context cancelled during execution"
					te.turnDurationMs = time.Since(te.execStart).Milliseconds()
					for _, m := range te.burst {
						_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "interrupted by graceful deployment")
						if m.ScheduleRunID != "" {
							_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "failed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(te.execStart).Milliseconds(),
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
		te.stopTyping()
		if te.statusUpdater != nil {
			te.statusUpdater.Stop()
			te.statusUpdater.DeleteStatusMessage()
		}

		if te.pool.ctx.Err() != nil {
			log.Printf("[WorkerPool] Pool shutting down during non-transient error for thread %s. Resetting to PENDING.", te.threadID)
			te.turnStatus = "failed"
			te.turnError = "interrupted by graceful deployment"
			te.turnDurationMs = time.Since(te.execStart).Milliseconds()
			for _, m := range te.burst {
				_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "interrupted by graceful deployment")
			}
			return
		}

		if te.currentSessionID != "" {
			_ = db.RotateSessionID(te.pool.cfg.DB, te.threadID, "")
			te.currentSessionID = ""
		}

		sanitizedErr := sanitizeErrorText(errDetail)
		te.turnStatus = "failed"
		te.turnError = sanitizedErr
		te.turnDurationMs = time.Since(te.execStart).Milliseconds()
		metrics.RecordTurnCompleted("failed", te.triggerType, currentModel, time.Since(te.execStart))

		var notif string
		if isRateLimitError(errDetail, stderr) {
			notif = notifier.ModelUnavailableMessage()
		} else {
			snippet := sanitizedErr
			if len([]rune(snippet)) > 300 {
				snippet = string([]rune(snippet)[:300])
			}
			notif = te.pool.cfg.NotifierFunc(currentAgyBin, currentAPIKey, fmt.Sprintf("execution failed with non-transient error: %s", snippet))
		}
		if !te.skipDiscord {
			if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, notif); err != nil {
				log.Printf("[WorkerPool] Failed to deliver non-transient failure notice for thread %s: %v", te.threadID, err)
			}
		}

		for _, m := range te.burst {
			_ = db.IncrementMessageRetry(te.pool.cfg.DB, m.ID, errDetail)
			_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusFailed, sanitizedErr)
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "failed",
					CompletedAt: time.Now().UTC(),
					DurationMs:  time.Since(te.execStart).Milliseconds(),
					Error:       sanitizedErr,
					Model:       currentModel,
				})
			}
			if te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
			}
		}
		log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED due to non-transient error (attempt %d/%d): %s", len(te.burst), te.threadID, attempt, maxAttempts, errDetail)
		return
	}

	// Total exhaustion after all attempts
	te.stopTyping()
	if te.statusUpdater != nil {
		te.statusUpdater.Stop()
		te.statusUpdater.DeleteStatusMessage()
	}

	if te.pool.ctx.Err() != nil {
		log.Printf("[WorkerPool] Pool shutting down during turn for thread %s. Suppressing exhaustion alert and resetting to PENDING for deployment recovery.", te.threadID)
		metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
		te.turnStatus = "failed"
		te.turnError = "context cancelled during execution"
		te.turnDurationMs = time.Since(te.execStart).Milliseconds()
		for _, m := range te.burst {
			_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusPending, "interrupted by graceful deployment")
			if m.ScheduleRunID != "" {
				_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "failed",
					CompletedAt: time.Now().UTC(),
					DurationMs:  time.Since(te.execStart).Milliseconds(),
					Error:       "context cancelled during execution",
					Model:       currentModel,
				})
			}
		}
		return
	}
	var notif string
	if lastErrDetail != "" && isRateLimitError(lastErrDetail, lastStderr) {
		notif = notifier.ModelUnavailableMessage()
	} else {
		snippet := sanitizeErrorText(lastErrDetail)
		if len([]rune(snippet)) > 300 {
			snippet = string([]rune(snippet)[:300])
		}
		notif = te.pool.cfg.NotifierFunc(currentAgyBin, currentAPIKey, fmt.Sprintf("execution failed after exhausting %d attempts: %s", maxAttempts, snippet))
	}
	if !te.skipDiscord {
		if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, notif); err != nil {
			log.Printf("[WorkerPool] Failed to deliver exhaustion notice for thread %s: %v", te.threadID, err)
		}
	}
	sanitizedErr := sanitizeErrorText(lastErrDetail)
	te.turnStatus = "failed"
	te.turnError = sanitizedErr
	te.turnDurationMs = time.Since(te.execStart).Milliseconds()
	metrics.RecordTurnCompleted("failed", te.triggerType, currentModel, time.Since(te.execStart))
	for _, m := range te.burst {
		_ = db.UpdateMessageStatus(te.pool.cfg.DB, m.ID, db.StatusFailed, sanitizedErr)
		if m.ScheduleRunID != "" {
			_ = db.UpdateScheduleRunStatus(te.pool.cfg.DB, db.UpdateRunParams{
				RunID:       m.ScheduleRunID,
				MessageID:   m.ID,
				Status:      "failed",
				CompletedAt: time.Now().UTC(),
				DurationMs:  time.Since(te.execStart).Milliseconds(),
				Error:       sanitizedErr,
				Model:       currentModel,
			})
		}
		if te.pool.cfg.OnMessageCompleted != nil {
			te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
		}
	}
	log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED after exhausting all %d attempts", len(te.burst), te.threadID, maxAttempts)
}
