package queue

import (
	"context"
	"errors"
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
	pool                *WorkerPool
	activePool          runner.AgentPool
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
	threadName          string
	wakeIdx             int
	wakeInfos           []wakeInfo
	trailingMsgs        []db.Message
	trailingInfos       []wakeInfo
	turnPrompt          string
	previousTurnActions string
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

	var latestActivity time.Time
	if stats.SessionUpdatedAt.After(latestActivity) {
		latestActivity = stats.SessionUpdatedAt
	}
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
	if s := te.store(); s != nil {
		if err := s.SaveSessionID(context.Background(), threadID, sessionID); err != nil {
			log.Printf("[Worker] Warning saving session ID for thread %s: %v", threadID, err)
		}
	}
}

func (te *turnExecution) evictSessionFromPools() {
	if te == nil {
		return
	}
	if te.activePool != nil {
		if err := te.activePool.EvictSession(te.threadID); err != nil {
			log.Printf("[Queue] Warning evicting session for thread %s from activePool: %v", te.threadID, err)
		}
	}
	if te.pool != nil && te.pool.processPool != nil {
		if err := te.pool.processPool.EvictSession(te.threadID); err != nil {
			log.Printf("[Queue] Warning evicting session for thread %s from processPool: %v", te.threadID, err)
		}
	}
	if te.pool != nil && te.pool.lowEffortProcessPool != nil {
		if err := te.pool.lowEffortProcessPool.EvictSession(te.threadID); err != nil {
			log.Printf("[Queue] Warning evicting session for thread %s from lowEffortProcessPool: %v", te.threadID, err)
		}
	}
}

func (te *turnExecution) rotateSessionID(threadID, newSessionID string) {
	if te == nil {
		return
	}
	if s := te.store(); s != nil {
		if err := s.RotateSessionID(context.Background(), threadID, newSessionID); err != nil {
			log.Printf("[Worker] Warning rotating session ID for thread %s: %v", threadID, err)
		}
	}
}

func (te *turnExecution) getSessionTurnCount(threadID string) (int, error) {
	if s := te.store(); s != nil {
		return s.GetSessionTurnCount(context.Background(), threadID)
	}
	return 0, nil
}

func (te *turnExecution) incrementSessionTurnCount(threadID string) (int, error) {
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

func (te *turnExecution) getPreviousSessionID(threadID string) (string, error) {
	if s := te.store(); s != nil {
		return s.GetPreviousSessionID(context.Background(), threadID)
	}
	return "", nil
}

func (te *turnExecution) createOneShotSchedule(s db.OneShotSchedule) error {
	if store := te.store(); store != nil {
		return store.CreateOneShotSchedule(context.Background(), s)
	}
	return nil
}

func (te *turnExecution) getThreadSummary(threadID string) (string, string, error) {
	if s := te.store(); s != nil {
		return s.GetThreadSummary(context.Background(), threadID)
	}
	return "", "", nil
}

func (te *turnExecution) saveThreadSummary(threadID, summary, lastMsgID string) error {
	if s := te.store(); s != nil {
		return s.SaveThreadSummary(context.Background(), threadID, summary, lastMsgID)
	}
	return nil
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
					if dm == nil {
						continue
					}
					if te.isBurstMessage(dm.ID) {
						continue
					}
					authorID := ""
					authorName := ""
					if dm.Author != nil {
						authorID = dm.Author.ID
						authorName = dm.Author.Username
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
					var mentions []string
					var mentionUserIDs []string
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
						ID:         dm.ID,
						ThreadID:   threadID,
						AuthorID:   authorID,
						AuthorName: authorName,
						Content:    dm.Content,
						CreatedAt:  createdAt,
						Metadata: db.MessageMetadata{
							Mentions:         mentions,
							MentionUserIDs:   mentionUserIDs,
							ReplyingToAuthor: replyingToAuthor,
						},
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
	scopeLock, ok := lockVal.(*sync.Mutex)
	if !ok {
		scopeLock = &sync.Mutex{}
		p.scopeLocks.Store(threadID, scopeLock)
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
				te.updateMessageStatus(m.ID, db.StatusFailed, errMsg)
				if m.ScheduleRunID != "" {
					te.updateScheduleRunStatus(db.UpdateRunParams{
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
			te.updateMessageCompleted(m.ID, "[EXPIRED_STALE]")
			if m.ScheduleRunID != "" {
				te.updateScheduleRunStatus(db.UpdateRunParams{
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
		lastActivity, isColdThread, err = GetSessionLastActivity(te.store(), te.threadID, te.pool.sessionMgr)
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
			te.updateScheduleRunStatus(db.UpdateRunParams{
				RunID:     m.ScheduleRunID,
				MessageID: m.ID,
				Status:    "running",
			})
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
		for _, m := range te.burst {
			te.updateMessageStatus(m.ID, db.StatusCompleted, fmt.Sprintf("[%s]", strings.ToUpper(strings.TrimSpace(te.policy.Mode))))
			if m.ScheduleRunID != "" {
				te.updateScheduleRunStatus(db.UpdateRunParams{
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

	te.statusUpdater = NewStatusUpdater(te.pool.getDiscordSession(), te.threadID, te.isThread)
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
			if te.pool != nil {
				var msgErr error
				recentContext, msgErr = te.getRecentThreadMessages(te.threadID, 10)
				if msgErr != nil {
					log.Printf("[Worker] Warning getting recent messages for thread %s: %v", te.threadID, msgErr)
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
					te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
				}
				return true
			}

			// ALL messages in burst are ambient
			te.markAmbientBurst()
			return true
		}

		// wakeIdx >= 0
		if te.currentSessionID == "" {
			var sessErr error
			te.currentSessionID, sessErr = te.getSessionID(te.threadID)
			if sessErr != nil {
				log.Printf("[Worker] Warning getting session ID for thread %s: %v", te.threadID, sessErr)
			}
		}
		if te.currentSessionID != "" && te.pool.sessionMgr != nil && !te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID) {
			log.Printf("[Queue] Session %s for thread %s not found on disk. Clearing for fresh Turn 1.", te.currentSessionID, te.threadID)
			te.currentSessionID = ""
		}
		if te.currentSessionID != "" && te.pool.sessionMgr != nil {
			if _, dirErr := te.pool.sessionMgr.EnsureSessionDir(te.currentSessionID); dirErr != nil {
				log.Printf("[Worker] Warning ensuring session dir for %s: %v", te.currentSessionID, dirErr)
			}
		}

		// Phase 1 (Leading ambient messages)
		for i, m := range plan.LeadingAmbient {
			info := plan.WakeInfos[i]
			metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
			telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.Score, info.Threshold, info.Reason)
			te.updateMessageStatus(m.ID, db.StatusCompleted, telemetry)
			if m.ScheduleRunID != "" {
				te.updateScheduleRunStatus(db.UpdateRunParams{
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

	if te.pool.cfg.TypingFunc != nil {
		if stop := te.pool.cfg.TypingFunc(te.pool.getDiscordSession(), te.threadID); stop != nil {
			te.stopTyping = stop
		}
	}

	if te.currentSessionID != "" {
		currentTurns, turnErr := te.getSessionTurnCount(te.threadID)
		if turnErr != nil {
			log.Printf("[Queue] Warning querying turn count for thread %s: %v", te.threadID, turnErr)
		}
		var currentSteps int
		var currentBytes int64
		var currentDBBytes int64
		if te.pool != nil && te.pool.sessionMgr != nil {
			currentSteps = te.pool.sessionMgr.CountTranscriptSteps(te.currentSessionID)
			currentBytes = te.pool.sessionMgr.GetTranscriptSize(te.currentSessionID)
			currentDBBytes = te.pool.sessionMgr.GetSessionDBSize(te.currentSessionID)
		}
		isTurnLimit := currentTurns >= DefaultMaxSessionTurns
		isStepLimit := currentSteps >= DefaultMaxSessionSteps
		isTranscriptByteLimit := currentBytes >= DefaultMaxTranscriptBytes
		isDBByteLimit := currentDBBytes >= DefaultMaxSessionDBBytes
		isByteLimit := isTranscriptByteLimit || isDBByteLimit
		if isTurnLimit || isStepLimit || isByteLimit {
			scope := "thread"
			if strings.EqualFold(te.policy.Mode, "channel") {
				scope = "channel"
			}
			if isDBByteLimit {
				log.Printf("[Queue] Scope session reached DB size limit (%d >= %d bytes). Resetting to cold state for fresh session initialization.", currentDBBytes, DefaultMaxSessionDBBytes)
				metrics.RecordSessionRotation("pre_flight", scope, "bytes")
			} else if isTranscriptByteLimit {
				log.Printf("[Queue] Scope session reached transcript size limit (%d >= %d bytes). Resetting to cold state for fresh session initialization.", currentBytes, DefaultMaxTranscriptBytes)
				metrics.RecordSessionRotation("pre_flight", scope, "bytes")
			} else if isStepLimit {
				log.Printf("[Queue] Scope session reached step limit (%d >= %d steps). Resetting to cold state for fresh session initialization.", currentSteps, DefaultMaxSessionSteps)
				metrics.RecordSessionRotation("pre_flight", scope, "steps")
			} else {
				log.Printf("[Queue] Scope session reached turn limit (%d/%d). Resetting to cold state for fresh session initialization.", currentTurns, DefaultMaxSessionTurns)
				metrics.RecordSessionRotation("pre_flight", scope, "turns")
			}
			te.previousSessionID = te.currentSessionID
			te.rotateSessionID(te.threadID, "")
			te.currentSessionID = ""
			te.evictSessionFromPools()
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
	hasDiskSession := te.currentSessionID != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID)
	if hasDiskSession && te.pool.sessionMgr != nil {
		if _, dirErr := te.pool.sessionMgr.EnsureSessionDir(te.currentSessionID); dirErr != nil {
			log.Printf("[Queue] Warning ensuring session dir for %s: %v", te.currentSessionID, dirErr)
		}
	}

	metrics.RecordTurnCompleted("ambient", te.triggerType, "classifier", time.Since(te.execStart))

	for i, m := range te.burst {
		var info WakeInfo
		if i < len(te.wakeInfos) {
			info = te.wakeInfos[i]
		}
		metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
		telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.Score, info.Threshold, info.Reason)
		te.updateMessageStatus(m.ID, db.StatusCompleted, telemetry)
		if m.ScheduleRunID != "" {
			te.updateScheduleRunStatus(db.UpdateRunParams{
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
		te.updateMessageStatus(m.ID, db.StatusCompleted, telemetry)
		if m.ScheduleRunID != "" {
			te.updateScheduleRunStatus(db.UpdateRunParams{
				RunID:       m.ScheduleRunID,
				MessageID:   m.ID,
				Status:      "completed",
				CompletedAt: time.Now().UTC(),
				Error:       telemetry,
			})
		}
		if te.pool != nil && te.pool.cfg.OnMessageCompleted != nil {
			te.pool.cfg.OnMessageCompleted(m, db.StatusCompleted)
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
			te.updateMessageStatus(m.ID, db.StatusFailed, "[EXHAUSTED_PRE_TURN_RETRIES]")
			if m.ScheduleRunID != "" {
				te.updateScheduleRunStatus(db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "failed",
					CompletedAt: time.Now().UTC(),
					DurationMs:  time.Since(te.execStart).Milliseconds(),
					Error:       "[EXHAUSTED_PRE_TURN_RETRIES]",
				})
			}
			if te.pool != nil && te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
			}
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
			metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
			telemetry := fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.Score, info.Threshold, info.Reason)
			te.updateMessageStatus(m.ID, db.StatusCompleted, telemetry)
			if m.ScheduleRunID != "" {
				te.updateScheduleRunStatus(db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "completed",
					CompletedAt: time.Now().UTC(),
				})
			}
			if te.pool != nil && te.pool.cfg.OnMessageCompleted != nil {
				te.pool.cfg.OnMessageCompleted(m, db.StatusCompleted)
			}
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
	isColdStart := te.currentSessionID == ""

	// Policy mode dictates conversational engagement contract:
	// - Channel Mode: Aerial wakes selectively. Ambient chatter bypasses session memory,
	//   so history lookback is needed on EVERY wake turn (cold or warm).
	// - Threads Mode: Aerial participates in every turn in the thread. Session memory already
	//   retains all turns on warm resume. Only cold starts need summary / lookback fallback.
	isChannelMode := strings.EqualFold(te.policy.Mode, "channel")
	isThreadColdStart := snap.IsThread && snap.ParentID != "" && snap.ParentID != snap.ID && isColdStart && !isChannelMode

	var summary string
	if isThreadColdStart {
		cachedSum, lastMsgID, getSumErr := te.getThreadSummary(te.threadID)
		if getSumErr != nil {
			log.Printf("[WorkerPool] Warning getting thread summary for %s: %v", te.threadID, getSumErr)
		}

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
					if err := te.saveThreadSummary(te.threadID, summary, latestMsgID); err != nil {
						log.Printf("[WorkerPool] Warning: Failed to save thread summary to DB for thread %s: %v", te.threadID, err)
					}
				}
			}
		}

		if summary != "" {
			log.Printf("[WorkerPool] Injected <THREAD_SUMMARY> into Turn 1 prompt for thread %s", te.threadID)
		}
	}

	hasHistoryNeed := isChannelMode || isColdStart || te.wakeIdx > 0 || len(te.burst) > 1

	var lookbackMsgs []HistoryMessage
	if hasHistoryNeed {
		lookbackMsgs = getTurnHistory(10)
		// On warm channel turns, filter out Assistant messages so Aerial's own prior turns
		// are not re-injected as third-party chatter, preventing self-echo loops and prompt contradiction.
		if isChannelMode && !isColdStart && len(lookbackMsgs) > 0 {
			lookbackMsgs = FilterAssistantMessages(lookbackMsgs)
		}
		if len(lookbackMsgs) > 0 {
			log.Printf("[WorkerPool] Injected channel history into prompt for thread %s", te.threadID)
		}
	}

	var prevID string
	if isColdStart {
		prevID = te.previousSessionID
		if prevID == "" {
			var prevErr error
			prevID, prevErr = te.getPreviousSessionID(te.threadID)
			if prevErr != nil {
				log.Printf("[WorkerPool] Warning getting previous session ID for %s: %v", te.threadID, prevErr)
			}
		}
		if prevID != "" {
			log.Printf("[WorkerPool] Injected previous session identifier (%s) into prompt for thread %s", prevID, te.threadID)
		}
	}

	instructions := config.LoadChannelInstructions(te.effectiveName)
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
		var ambErr error
		ambientCtx, ambErr = te.pool.cfg.AmbientResolver(ctx, te.policy.AmbientContext)
		if ambErr != nil {
			log.Printf("[WorkerPool] Warning resolving ambient context for channel %s: %v", te.effectiveName, ambErr)
		} else if strings.TrimSpace(ambientCtx) != "" {
			log.Printf("[WorkerPool] Injected ambient context into prompt for #%s", te.effectiveName)
		}
	}

	// Enrich burst messages with channel and thread names if not already set
	for i := range te.burst {
		if te.burst[i].Metadata.ChannelName == "" && te.effectiveName != "" {
			te.burst[i].Metadata.ChannelName = te.effectiveName
		}
		if te.isThread && te.burst[i].Metadata.ThreadName == "" && te.threadName != "" {
			te.burst[i].Metadata.ThreadName = te.threadName
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
	})
	te.turnPrompt = te.preparePrompt(te.turnPrompt)
}

func (te *turnExecution) preparePrompt(prompt string) string {
	if te.previousTurnActions != "" && !strings.Contains(prompt, te.previousTurnActions) {
		prompt = prompt + "\n\n" + te.previousTurnActions
	}
	return prompt
}

func (te *turnExecution) condenseTurnActions(rawActions string) string {
	if len(rawActions) <= 1500 {
		return rawActions
	}

	lowEffortModel := ""
	if te.pool != nil {
		if te.pool.appCfg != nil {
			if cur := te.pool.appCfg.Current(); cur != nil {
				lowEffortModel = cur.LowEffortModel
			}
		}
		if lowEffortModel == "" {
			te.pool.mu.Lock()
			lowEffortModel = te.pool.cfg.LowEffortModel
			te.pool.mu.Unlock()
		}
	}
	if lowEffortModel == "" {
		lowEffortModel = config.GetRuntimeConfig().LowEffortModel
	}

	var llmFn LLMFunc
	if te.pool != nil {
		if te.pool.lowEffortProcessPool != nil {
			if ep, ok := te.pool.lowEffortProcessPool.(interface {
				EphemeralLLMFunc(string) runner.LLMFunc
			}); ok {
				llmFn = ep.EphemeralLLMFunc("ephemeral:summarizer")
			}
		}
		if llmFn == nil && te.pool.cfg.LLMFunc != nil {
			llmFn = te.pool.cfg.LLMFunc
		} else if llmFn == nil && te.pool.processPool != nil {
			if ep, ok := te.pool.processPool.(interface {
				EphemeralLLMFunc(string) runner.LLMFunc
			}); ok {
				llmFn = ep.EphemeralLLMFunc("ephemeral:summarizer")
			}
		}
	}

	if llmFn == nil {
		err := errors.New("summarizer unavailable")
		log.Printf("[WorkerPool] Warning: failed to condense turn actions with low-effort model: %v. Falling back to clamped raw actions.", err)
		return rawActions
	}

	ctx := context.Background()
	if te.pool != nil && te.pool.ctx != nil {
		ctx = te.pool.ctx
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	prompt := fmt.Sprintf("Condense the following tool actions executed in the previous attempt into a compact, deduplicated summary of key findings and actions taken. Preserve file paths, commands, exit codes, and core outputs. Output MUST be wrapped in <PREVIOUS_TURN_ACTIONS> and </PREVIOUS_TURN_ACTIONS> tags.\n\n%s", rawActions)

	result, err := llmFn(timeoutCtx, lowEffortModel, prompt)
	if err != nil {
		log.Printf("[WorkerPool] Warning: failed to condense turn actions with low-effort model: %v. Falling back to clamped raw actions.", err)
		return rawActions
	}

	trimmed := strings.TrimSpace(result)
	if trimmed == "" {
		err := errors.New("empty response from summarizer")
		log.Printf("[WorkerPool] Warning: failed to condense turn actions with low-effort model: %v. Falling back to clamped raw actions.", err)
		return rawActions
	}

	if strings.Contains(trimmed, "<PREVIOUS_TURN_ACTIONS>") && strings.Contains(trimmed, "</PREVIOUS_TURN_ACTIONS>") {
		startIdx := strings.Index(trimmed, "<PREVIOUS_TURN_ACTIONS>")
		endIdx := strings.Index(trimmed, "</PREVIOUS_TURN_ACTIONS>")
		if startIdx <= endIdx {
			trimmed = trimmed[startIdx : endIdx+len("</PREVIOUS_TURN_ACTIONS>")]
		}
	} else {
		trimmed = "<PREVIOUS_TURN_ACTIONS>\n" + trimmed + "\n</PREVIOUS_TURN_ACTIONS>"
	}

	const maxTotalChars = 2000
	runes := []rune(trimmed)
	if len(runes) > maxTotalChars {
		closing := []rune("\n</PREVIOUS_TURN_ACTIONS>")
		trimmed = string(runes[:maxTotalChars-len(closing)]) + string(closing)
	}
	return trimmed
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

	var autoResumeCount int
	var accumulatedUsage runner.TokenUsage
	promptToSend := te.turnPrompt

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
					if te.pool.cfg.DeliveryFunc != nil {
						pauseMsg := notifier.FormatQuotaPauseMessage(remaining, lockedUntil, false, false)
						if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, pauseMsg); err != nil {
							log.Printf("[WorkerPool] Failed to deliver quota pause message to thread %s: %v", te.threadID, err)
						}
					}
					for _, m := range te.burst {
						te.updateMessageStatus(m.ID, db.StatusFailed, reason)
						if m.ScheduleRunID != "" {
							te.updateScheduleRunStatus(db.UpdateRunParams{
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

		if promptToSend == "" {
			promptToSend = te.turnPrompt
		}
		if attempt > 1 && promptToSend == te.turnPrompt {
			if (runner.IsInactivityTimeout(lastErrDetail, lastStderr) || strings.Contains(lastErrDetail, "max duration exceeded") || strings.Contains(lastErrDetail, "print timeout") || strings.Contains(lastErrDetail, "empty response after")) && te.currentSessionID != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID) {
				promptToSend = fmt.Sprintf(ContinuationPromptTemplate, te.turnPrompt)
			}
		}
		promptToSend = te.preparePrompt(promptToSend)

		// Pad Go context by +1 minute relative to runner watchdog ceiling so the runner watchdog always fires cleanly
		runCtx, runCancel := context.WithTimeout(te.pool.ctx, time.Duration(currentTimeout+1)*time.Minute)

		var stdout, stderr string
		var exitCode int
		var err error
		var isFailure, isTransient, isSessionCorruption bool
		var errDetail string
		runStart := time.Now()

		activePool := te.pool.processPool
		if isLowEffort && te.pool.lowEffortProcessPool != nil {
			activePool = te.pool.lowEffortProcessPool
		} else if isLowEffort && te.pool.lowEffortProcessPool == nil {
			log.Printf("[Worker] Notice: lowEffortProcessPool is nil, falling back to processPool for thread %s", te.threadID)
		}
		te.activePool = activePool

		var resp *runner.AgyResponse
		var outcome runner.TurnOutcome
		var turnRes *runner.TurnResult
		var execSession runner.AgentSession

		if activePool != nil {
			if te.statusUpdater != nil {
				te.statusUpdater.MarkTurnStarted()
			}
			var sessionErr error
			execSession, sessionErr = activePool.GetOrCreateSession(runCtx, te.threadID, te.currentSessionID)
			if sessionErr != nil {
				execSession = nil
				err = sessionErr
				outcome = runner.NewTurnResolver().Resolve(runCtx, nil, sessionErr, te.currentSessionID)
			} else {
				if execSession != nil && execSession.SessionID() != "" && te.currentSessionID != "" && execSession.SessionID() != te.currentSessionID {
					if isLowEffort && te.pool.lowEffortProcessPool != nil {
						log.Printf("[Queue] Anti-flapping: preserving primary session %s for thread %s; not overwriting with low effort session %s", te.currentSessionID, te.threadID, execSession.SessionID())
					} else {
						te.currentSessionID = execSession.SessionID()
						te.saveSessionID(te.threadID, te.currentSessionID)
					}
				}
				sink := newDiscordTurnSink(te.statusUpdater)
				sessIDToSend := te.currentSessionID
				if isLowEffort && te.pool.lowEffortProcessPool != nil && execSession != nil && execSession.SessionID() != "" {
					sessIDToSend = execSession.SessionID()
				}
				turnCtx := &runner.TurnContext{
					TurnID:    uuid.New().String(),
					SessionID: sessIDToSend,
					Model:     currentModel,
					Prompt:    promptToSend,
					Sink:      sink,
					CreatedAt: time.Now(),
					Ctx:       runCtx,
				}
				if sendErr := execSession.Send(promptToSend, turnCtx); sendErr != nil {
					err = sendErr
					outcome = runner.NewTurnResolver().Resolve(runCtx, nil, sendErr, execSession.SessionID())
				} else {
					var turnErr error
					turnRes, turnErr = sink.Wait(runCtx)
					err = turnErr
					if turnRes != nil {
						stderr = turnRes.Stderr
					}
					targetSess := execSession.SessionID()
					if targetSess == "" {
						targetSess = te.currentSessionID
					}
					resolver := runner.NewTurnResolver()
					outcome = resolver.Resolve(runCtx, turnRes, turnErr, targetSess)
				}
			}
		} else {
			err = fmt.Errorf("queue: no agent execution pool available for thread %s", te.threadID)
			outcome = runner.NewTurnResolver().Resolve(runCtx, nil, err, te.currentSessionID)
		}
		runCancel()

		if outcome.SessionID != "" && outcome.SessionID != te.currentSessionID && outcome.SessionID != te.previousSessionID {
			if isLowEffort && te.pool.lowEffortProcessPool != nil && te.currentSessionID != "" {
				log.Printf("[Queue] Anti-flapping: preserving primary session %s for thread %s; not overwriting with low effort session %s", te.currentSessionID, te.threadID, outcome.SessionID)
			} else {
				te.currentSessionID = outcome.SessionID
				te.saveSessionID(te.threadID, te.currentSessionID)
			}
		}

		if outcome.IsSuccess {
			isFailure = false
			isTransient = false
			isSessionCorruption = false
			errDetail = ""
			lastErrDetail = ""
			lastStderr = ""
			if turnRes != nil && turnRes.Stderr != "" {
				stderr = turnRes.Stderr
			}
			resp = &runner.AgyResponse{
				ConversationID: outcome.SessionID,
				Status:         "SUCCESS",
				Response:       outcome.Response,
				Usage:          outcome.Usage,
			}
			stdout = outcome.Response
		} else {
			isFailure = true
			isTransient = outcome.IsTransient
			isSessionCorruption = outcome.IsSessionCorruption
			errDetail = outcome.ErrorDetail
			lastErrDetail = errDetail
			if err != nil {
				lastStderr = err.Error()
			} else {
				lastStderr = outcome.ErrorDetail
			}
		}

		if isFailure {
			promptToSend = te.turnPrompt
			targetSess := ""
			if isLowEffort && te.pool != nil && te.pool.lowEffortProcessPool != nil && activePool != nil {
				if dPool, ok := activePool.(interface {
					Get(string) (*runner.StreamingDaemon, bool)
				}); ok {
					if d, ok := dPool.Get(te.threadID); ok && d != nil && d.SessionID() != "" {
						targetSess = d.SessionID()
					}
				}
			}
			if targetSess == "" && outcome.SessionID != "" {
				targetSess = outcome.SessionID
			}
			if targetSess == "" && execSession != nil && execSession.SessionID() != "" {
				targetSess = execSession.SessionID()
			}
			if targetSess == "" {
				targetSess = te.currentSessionID
			}
			if targetSess == "" {
				combinedOutput := stdout + "\n" + stderr
				if extSess := runner.ExtractSessionID(combinedOutput, te.execStart); extSess != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(extSess) {
					targetSess = extSess
				}
			}
			if targetSess == "" && te.pool != nil && activePool != nil {
				if dPool, ok := activePool.(interface {
					Get(string) (*runner.StreamingDaemon, bool)
				}); ok {
					if d, ok := dPool.Get(te.threadID); ok && d != nil && d.SessionID() != "" {
						targetSess = d.SessionID()
					}
				}
			}
			if targetSess == "" && te.pool != nil && te.pool.processPool != nil && te.pool.processPool != activePool {
				if dPool, ok := te.pool.processPool.(interface {
					Get(string) (*runner.StreamingDaemon, bool)
				}); ok {
					if d, ok := dPool.Get(te.threadID); ok && d != nil && d.SessionID() != "" {
						targetSess = d.SessionID()
					}
				}
			}

			// If response was not recovered from transcript, inspect transcript for turn errors (e.g. Gemini 429 quota exhaustion)
			if isFailure && targetSess != "" && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(targetSess) {
				poolCtx := context.Background()
				if te.pool != nil && te.pool.ctx != nil {
					poolCtx = te.pool.ctx
				}
				if transcriptErr, tErr := te.pool.sessionMgr.ExtractLastTurnError(poolCtx, targetSess, te.execStart); tErr == nil && transcriptErr != "" {
					log.Printf("[Queue] Extracted turn error from session %s transcript: %s", targetSess, transcriptErr)
					errDetail = transcriptErr
					stderr = transcriptErr
					lastErrDetail = transcriptErr
					lastStderr = transcriptErr
				}
			}

			// Cold-Start Dynamic Session Latching:
			// If this was a cold start and remains an unrecovered failure, only latch the active session
			// if the failure was NOT session corruption (so retries won't inherit corrupted state)
			// and NOT the previously rotated session (so bloated sessions are never resurrected).
			if isFailure && te.currentSessionID == "" && !isSessionCorruption && targetSess != "" && targetSess != te.previousSessionID {
				log.Printf("[Queue] Latched active session from output on failure (attempt %d/%d) for thread %s: %s", attempt, maxAttempts, te.threadID, targetSess)
				te.currentSessionID = targetSess
				te.saveSessionID(te.threadID, te.currentSessionID)
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
				// Cold-start dynamic session latching if available
				if te.currentSessionID == "" && !isSessionCorruption {
					combinedOutput := stdout + "\n" + stderr
					if extSess := runner.ExtractSessionID(combinedOutput, te.execStart); extSess != "" && extSess != te.previousSessionID && te.pool != nil && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(extSess) {
						te.currentSessionID = extSess
						te.saveSessionID(te.threadID, te.currentSessionID)
					}
				}

				resetDur, _ := runner.ExtractQuotaResetDuration(errDetail, stderr)
				isCapacity := runner.IsCapacityBlip(errDetail, stderr) || resetDur <= 5*time.Second

				if isCapacity {
					// Transient capacity blip (e.g. 0s-5s capacity exhaustions, rate limit spikes).
					// Decouple from the global queue lock: do NOT call SetQuotaLockedUntil so other Discord
					// threads remain completely unblocked and active.
					// Keep the warm daemon alive and typing active, retrying locally with jitter.
					log.Printf("[WorkerPool] Capacity blip detected for thread %s on attempt %d/%d (reset=%v): %s. Retrying locally without global queue lock.",
						te.threadID, attempt, maxAttempts, resetDur, errDetail)

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
							continue
						case <-cancelChan:
							log.Printf("[WorkerPool] Context cancelled during capacity blip backoff for thread %s", te.threadID)
							te.stopTyping()
							te.turnStatus = "pending"
							te.turnError = "interrupted by graceful deployment"
							te.turnDurationMs = time.Since(te.execStart).Milliseconds()
							for _, m := range te.burst {
								te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
							}
							return
						}
					}

					// If maxAttempts is exhausted on capacity blips, do NOT globally lock the queue.
					// Mark failure locally for this turn and notify Discord if needed.
					log.Printf("[WorkerPool] Capacity blip retry budget exhausted for thread %s after %d attempts", te.threadID, maxAttempts)
					te.stopTyping()
					if te.statusUpdater != nil {
						te.statusUpdater.Stop()
						te.statusUpdater.DeleteStatusMessage()
					}
					reason := fmt.Sprintf("[CAPACITY_EXHAUSTED reset_in=%v] %s", resetDur, sanitizeErrorText(errDetail))
					metrics.RecordRunnerError("capacity_exhausted", currentModel)
					metrics.RecordTurnCompleted("capacity_exhausted", te.triggerType, currentModel, time.Since(te.execStart))
					te.turnStatus = "failed"
					te.turnError = reason
					te.turnDurationMs = time.Since(te.execStart).Milliseconds()
					for _, m := range te.burst {
						te.updateMessageStatus(m.ID, db.StatusFailed, reason)
						if m.ScheduleRunID != "" {
							te.updateScheduleRunStatus(db.UpdateRunParams{
								RunID:       m.ScheduleRunID,
								MessageID:   m.ID,
								Status:      "failed",
								CompletedAt: time.Now().UTC(),
								DurationMs:  time.Since(te.execStart).Milliseconds(),
								Error:       reason,
								Model:       currentModel,
							})
						}
						if te.pool != nil && te.pool.cfg.OnMessageCompleted != nil {
							te.pool.cfg.OnMessageCompleted(m, db.StatusFailed)
						}
					}
					if te.pool != nil && te.pool.cfg.DeliveryFunc != nil {
						pauseMsg := notifier.FormatQuotaPauseMessage(resetDur, time.Now().UTC().Add(resetDur), false, false)
						if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, pauseMsg); err != nil {
							log.Printf("[WorkerPool] Failed to deliver capacity pause notice for thread %s: %v", te.threadID, err)
						}
					}
					return
				}

				// True account subscription quota exhaustion: lock pool and schedule one-shot retry
				te.isQuotaPaused = true
				te.stopTyping()
				log.Printf("[WorkerPool] Quota pause detected for thread %s on attempt %d/%d: %s", te.threadID, attempt, maxAttempts, errDetail)

				// Circuit breaker: check if this turn was already a quota auto-retry
				isAlreadyRetry := te.burst[0].ScheduleRunID != "" || strings.HasPrefix(te.burst[0].Content, "[QUOTA_RETRY]") || strings.Contains(te.turnPrompt, "[QUOTA_RETRY]")

				jitterSec := 5 + rand.Intn(16) // 5s to 20s jitter
				runAt := time.Now().UTC().Add(resetDur).Add(30 * time.Second).Add(time.Duration(jitterSec) * time.Second)

				if te.pool != nil {
					te.pool.quotaLockedUntil.Store(runAt.Unix())
				}

				var scheduled bool
				if !isAlreadyRetry {
					retryPrompt := fmt.Sprintf("[QUOTA_RETRY] %s", te.turnPrompt)
					oneShotID := uuid.New().String()
					retryEffort := "high"
					if isLowEffort {
						retryEffort = "low"
					}
					oneShot := db.OneShotSchedule{
						ID:        oneShotID,
						ThreadID:  te.threadID,
						Prompt:    retryPrompt,
						Effort:    retryEffort,
						RunAt:     runAt,
						CreatedAt: time.Now().UTC(),
					}
					if err := te.createOneShotSchedule(oneShot); err != nil {
						log.Printf("[WorkerPool] Failed to create one-shot retry schedule for thread %s: %v", te.threadID, err)
					} else {
						scheduled = true
						log.Printf("[WorkerPool] Scheduled one-shot retry %s for thread %s at %s (+%v)", oneShotID, te.threadID, runAt.Format(time.RFC3339), resetDur)
					}
				} else {
					log.Printf("[WorkerPool] Circuit breaker: turn for thread %s was already an auto-retry. Skipping further scheduling.", te.threadID)
				}

				metrics.RecordTurnCompleted("quota_paused", te.triggerType, currentModel, time.Since(te.execStart))

				te.stopTyping()
				if te.statusUpdater != nil {
					te.statusUpdater.Stop()
					te.statusUpdater.DeleteStatusMessage()
				}

				if te.pool.cfg.DeliveryFunc != nil {
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
					te.updateMessageStatus(m.ID, db.StatusFailed, reason)
					if m.ScheduleRunID != "" {
						te.updateScheduleRunStatus(db.UpdateRunParams{
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
					te.incrementMessageRetry(m.ID, errDetail)
				}
				if attempt < maxAttempts {
					if te.statusUpdater != nil {
						te.statusUpdater.Reset()
					}
					var watchBytes int64
					var watchDBBytes int64
					var watchSteps int
					if te.pool != nil && te.pool.sessionMgr != nil && te.currentSessionID != "" {
						watchBytes = te.pool.sessionMgr.GetTranscriptSize(te.currentSessionID)
						watchDBBytes = te.pool.sessionMgr.GetSessionDBSize(te.currentSessionID)
						watchSteps = te.pool.sessionMgr.CountTranscriptSteps(te.currentSessionID)
					}
					if watchBytes >= DefaultMaxTranscriptBytes || watchDBBytes >= DefaultMaxSessionDBBytes || watchSteps >= DefaultMaxSessionSteps {
						scope := "thread"
						if strings.EqualFold(te.policy.Mode, "channel") {
							scope = "channel"
						}
						reason := "bytes"
						if watchSteps >= DefaultMaxSessionSteps {
							reason = "steps"
						}
						log.Printf("[WorkerPool] Watchdog timeout session %s exceeded guardrails (steps=%d/%d, bytes=%d/%d, db_bytes=%d/%d). Resetting session for cold retry.",
							te.currentSessionID, watchSteps, DefaultMaxSessionSteps, watchBytes, DefaultMaxTranscriptBytes, watchDBBytes, DefaultMaxSessionDBBytes)
						metrics.RecordSessionRotation("watchdog", scope, reason)
						if te.currentSessionID != "" {
							te.previousSessionID = te.currentSessionID
						}
						te.rotateSessionID(te.threadID, "")
						te.currentSessionID = ""
						te.evictSessionFromPools()
					}
					backoff := time.Duration(attempt) * te.pool.cfg.BackoffBase
					log.Printf("[WorkerPool] Retrying watchdog timeout in %v (attempt %d/%d, session %s)", backoff, attempt, maxAttempts, te.currentSessionID)
					select {
					case <-time.After(backoff):
					case <-te.pool.ctx.Done():
						metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
						te.turnStatus = "failed"
						te.turnError = "context cancelled during execution"
						te.turnDurationMs = time.Since(te.execStart).Milliseconds()
						for _, m := range te.burst {
							te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
							if m.ScheduleRunID != "" {
								te.updateScheduleRunStatus(db.UpdateRunParams{
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
						te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
					}
					return
				}

				te.evictSessionFromPools()
				te.rotateSessionID(te.threadID, "")
				metrics.RecordTurnCompleted("watchdog_timeout", te.triggerType, currentModel, time.Since(te.execStart))

				if te.pool.cfg.DeliveryFunc != nil {
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
					te.updateMessageStatus(m.ID, db.StatusFailed, sanitizedErr)
					if m.ScheduleRunID != "" {
						te.updateScheduleRunStatus(db.UpdateRunParams{
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
			if resp == nil {
				var parseErr error
				resp, parseErr = runner.ParseAgyOutput(stdout)
				if parseErr != nil {
					log.Printf("[Queue] Failed to parse runner output despite exit 0: %v", parseErr)
					lastErrDetail = parseErr.Error()
				}
			}
			if resp == nil {
				// Failed to obtain valid response from runner
			} else {
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
					if extSess != "" && extSess != te.currentSessionID {
						if isLowEffort && te.pool.lowEffortProcessPool != nil && te.currentSessionID != "" {
							log.Printf("[Queue] Anti-flapping: preserving primary session %s for thread %s; not overwriting with low effort session %s", te.currentSessionID, te.threadID, extSess)
						} else {
							log.Printf("[Queue] Active session synchronized for thread %s: %s -> %s", te.threadID, te.currentSessionID, extSess)
							te.currentSessionID = extSess
							te.saveSessionID(te.threadID, te.currentSessionID)
						}
					}

					// Option B Background Command Yield Trap Interception:
					isYield, taskCount := runner.IsYieldTrap(exitCode, stdout, stderr)
					var unfinishedTaskID string
					detectionSource := "stderr_signature"
					var trackerDaemon *runner.StreamingDaemon
					if !isYield && te.pool != nil {
						if activePool != nil {
							if dPool, ok := activePool.(interface {
								Get(string) (*runner.StreamingDaemon, bool)
							}); ok {
								if d, ok := dPool.Get(te.threadID); ok && d != nil && d.TaskTracker().ActiveCount() > 0 {
									isYield = true
									taskCount = 1
									detectionSource = "daemon_tracker"
									trackerDaemon = d
								}
							}
						}
						if !isYield {
							var altPool runner.AgentPool
							if activePool == te.pool.lowEffortProcessPool {
								altPool = te.pool.processPool
							} else {
								altPool = te.pool.lowEffortProcessPool
							}
							if altPool != nil {
								if dPool, ok := altPool.(interface {
									Get(string) (*runner.StreamingDaemon, bool)
								}); ok {
									if d, ok := dPool.Get(te.threadID); ok && d != nil && d.TaskTracker().ActiveCount() > 0 {
										isYield = true
										taskCount = 1
										detectionSource = "daemon_tracker"
										trackerDaemon = d
									}
								}
							}
						}
					}

					if isYield {
						if autoResumeCount < MaxYieldTrapAutoResumes {
							autoResumeCount++

							// 1. Accumulate token usage across sub-turns
							accumulatedUsage.InputTokens += resp.Usage.InputTokens
							accumulatedUsage.OutputTokens += resp.Usage.OutputTokens
							accumulatedUsage.ThinkingTokens += resp.Usage.ThinkingTokens
							accumulatedUsage.CacheReadTokens += resp.Usage.CacheReadTokens
							accumulatedUsage.TotalTokens += resp.Usage.TotalTokens

							// 2. Keep Discord typing heartbeat active without resetting status updater (do not call te.stopTyping())

							// 3. Set prompt for immediate in-session auto-resumption
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
												promptToSend = fmt.Sprintf(TaskCompletionResumePrompt, unfinishedTaskID, exitCode, logPath)
											}
										}
									}
								}
							}
							if promptToSend == te.turnPrompt || promptToSend == "" {
								if unfinishedTaskID != "" {
									promptToSend = fmt.Sprintf(TaskCompletionResumePrompt, unfinishedTaskID, 0, "")
								} else {
									promptToSend = YieldTrapResumePrompt
								}
							}

							log.Printf("[YieldTrap] Intercepted background command exit for thread %s in session %s (auto-resume %d/%d, source=%s, taskCount=%d, taskID=%s): suppressing intermediate waiting output and auto-resuming turn.",
								te.threadID, te.currentSessionID, autoResumeCount, MaxYieldTrapAutoResumes, detectionSource, taskCount, unfinishedTaskID)

							// 4. Record metric
							metrics.RecordYieldTrap("resumed", detectionSource, currentModel)
							metrics.RecordRunnerError("yield_trap_intercepted", currentModel)

							// 5. Decrement attempt and re-run runner immediately
							attempt--
							continue
						}

						// Circuit breaker tripped!
						log.Printf("[YieldTrap] Circuit breaker tripped for thread %s in session %s after %d auto-resumptions (source=%s, taskID=%s). Delivering final output.",
							te.threadID, te.currentSessionID, autoResumeCount, detectionSource, unfinishedTaskID)
						metrics.RecordYieldTrap("circuit_breaker", detectionSource, currentModel)
						metrics.RecordRunnerError("yield_trap_circuit_breaker", currentModel)
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

					if strings.TrimSpace(cleanText) == "" && len(attachments) == 0 {
						log.Printf("[Queue] Agent produced empty response for thread %s on attempt %d/%d; treating as failure", te.threadID, attempt, maxAttempts)
						lastErrDetail = "agent produced empty response"
						errDetail = lastErrDetail
					} else {
						var deliveryErr error
						if te.pool.cfg.DeliveryWithAttachmentsFunc != nil {
							deliveryErr = te.pool.cfg.DeliveryWithAttachmentsFunc(te.pool.getDiscordSession(), te.threadID, cleanText, attachments)
						} else if te.pool.cfg.DeliveryFunc != nil {
							deliveryErr = te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, cleanText)
						}
						if deliveryErr != nil {
							log.Printf("[WorkerPool] Failed to deliver response for thread %s: %v", te.threadID, deliveryErr)
						}

						// Combine accumulated sub-turn usage from any intercepted yield traps into final turn usage
						resp.Usage.InputTokens += accumulatedUsage.InputTokens
						resp.Usage.OutputTokens += accumulatedUsage.OutputTokens
						resp.Usage.ThinkingTokens += accumulatedUsage.ThinkingTokens
						resp.Usage.CacheReadTokens += accumulatedUsage.CacheReadTokens
						resp.Usage.TotalTokens += accumulatedUsage.TotalTokens

						// Mark all messages in the burst as completed with unpacked clean text
						metrics.RecordTurnCompleted("success", te.triggerType, currentModel, time.Since(te.execStart))
						tokenChannel := te.effectiveName
						if tokenChannel == "" {
							if te.triggerType == "schedule" {
								tokenChannel = "schedule"
							} else if te.triggerType == "http" {
								tokenChannel = "http"
							} else if !IsNumericSnowflake(te.threadID) && te.threadID != "" {
								tokenChannel = te.threadID
							}
						}
						metrics.RecordTokens(currentModel, tokenChannel, resp.Usage.InputTokens, resp.Usage.OutputTokens, resp.Usage.ThinkingTokens, resp.Usage.CacheReadTokens, resp.Usage.TotalTokens)
						te.turnStatus = "success"
						te.turnResponseText = cleanText
						te.turnTokenUsage = resp.Usage
						te.turnDurationMs = time.Since(te.execStart).Milliseconds()
						te.checkPostExecutionRotation()
						for _, m := range te.burst {
							te.updateMessageCompleted(m.ID, cleanText)
							if m.ScheduleRunID != "" {
								te.updateScheduleRunStatus(db.UpdateRunParams{
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
				te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
			}
			return
		}

		log.Printf("[WorkerPool] Burst for thread %s failed on attempt %d/%d (transient=%t, corrupt=%t): %s",
			te.threadID, attempt, maxAttempts, isTransient, isSessionCorruption, errDetail)

		if isSessionCorruption {
			for _, m := range te.burst {
				te.incrementMessageRetry(m.ID, errDetail)
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
			if te.pool.cfg.DeliveryFunc != nil {
				if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, notif); err != nil {
					log.Printf("[WorkerPool] Failed to deliver session reset notice for thread %s: %v", te.threadID, err)
				}
			}
			te.evictSessionFromPools()
			te.rotateSessionID(te.threadID, "")
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
						te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
						if m.ScheduleRunID != "" {
							te.updateScheduleRunStatus(db.UpdateRunParams{
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
				te.incrementMessageRetry(m.ID, errDetail)
			}
			if attempt < maxAttempts {
				if te.statusUpdater != nil {
					te.statusUpdater.Reset()
				}
				if te.currentSessionID != "" && te.pool != nil && te.pool.sessionMgr != nil {
					transSteps := te.pool.sessionMgr.CountTranscriptSteps(te.currentSessionID)
					transBytes := te.pool.sessionMgr.GetTranscriptSize(te.currentSessionID)
					transDBBytes := te.pool.sessionMgr.GetSessionDBSize(te.currentSessionID)
					if transSteps >= DefaultMaxSessionSteps || transBytes >= DefaultMaxTranscriptBytes || transDBBytes >= DefaultMaxSessionDBBytes {
						scope := "thread"
						if strings.EqualFold(te.policy.Mode, "channel") {
							scope = "channel"
						}
						reason := "bytes"
						if transSteps >= DefaultMaxSessionSteps {
							reason = "steps"
						}
						log.Printf("[WorkerPool] Transient failure session %s exceeded guardrails (steps=%d/%d, bytes=%d/%d, db_bytes=%d/%d). Resetting session for cold retry.",
							te.currentSessionID, transSteps, DefaultMaxSessionSteps, transBytes, DefaultMaxTranscriptBytes, transDBBytes, DefaultMaxSessionDBBytes)
						metrics.RecordSessionRotation("transient_retry", scope, reason)
						if te.currentSessionID != "" {
							te.previousSessionID = te.currentSessionID
						}
						te.rotateSessionID(te.threadID, "")
						te.currentSessionID = ""
						te.evictSessionFromPools()
					}
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
						te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
						if m.ScheduleRunID != "" {
							te.updateScheduleRunStatus(db.UpdateRunParams{
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
				te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
			}
			return
		}

		if te.currentSessionID != "" {
			te.previousSessionID = te.currentSessionID
			te.evictSessionFromPools()
			te.rotateSessionID(te.threadID, "")
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
		if te.pool.cfg.DeliveryFunc != nil {
			if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), te.threadID, notif); err != nil {
				log.Printf("[WorkerPool] Failed to deliver non-transient failure notice for thread %s: %v", te.threadID, err)
			}
		}

		for _, m := range te.burst {
			te.incrementMessageRetry(m.ID, errDetail)
			te.updateMessageStatus(m.ID, db.StatusFailed, sanitizedErr)
			if m.ScheduleRunID != "" {
				te.updateScheduleRunStatus(db.UpdateRunParams{
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
			te.updateMessageStatus(m.ID, db.StatusPending, "interrupted by graceful deployment")
			if m.ScheduleRunID != "" {
				te.updateScheduleRunStatus(db.UpdateRunParams{
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
	if te.pool.cfg.DeliveryFunc != nil {
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
		te.updateMessageStatus(m.ID, db.StatusFailed, sanitizedErr)
		if m.ScheduleRunID != "" {
			te.updateScheduleRunStatus(db.UpdateRunParams{
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
				statusUpdater.HandleStep(&runner.StepUpdateEvent{
					Event:    "step_update",
					StepType: "thinking",
				})
			}
		},
		OnToolCall: func(toolName, commandName string) {
			respondingNotified.Store(false)
			if statusUpdater != nil && (toolName != "" || commandName != "") {
				statusUpdater.HandleStep(&runner.StepUpdateEvent{
					Event:    "step_update",
					State:    "RUNNING",
					Type:     "tool_call",
					ToolName: toolName,
					ToolInfo: &runner.StepToolInfo{
						Parameters: runner.StepToolParameters{
							CommandLine: commandName,
						},
					},
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
				statusUpdater.HandleStep(&runner.StepUpdateEvent{
					Event:    "step_update",
					State:    state,
					Type:     "tool_call",
					ToolName: toolName,
				})
			}
		},
		OnSkillActivated: func(skillName, source string) {
			metrics.RecordSkillActivation(skillName, source)
		},
		OnTextDelta: func(delta string) {
			if statusUpdater != nil && !respondingNotified.Load() && strings.TrimSpace(delta) != "" {
				if respondingNotified.CompareAndSwap(false, true) {
					statusUpdater.HandleStep(&runner.StepUpdateEvent{
						Event:    "step_update",
						StepType: "agent_response",
					})
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




func (te *turnExecution) checkPostExecutionRotation() {
	if te == nil || te.currentSessionID == "" {
		return
	}
	scope := "thread"
	if strings.EqualFold(te.policy.Mode, "channel") {
		scope = "channel"
	}
	var postBytes int64
	var postDBBytes int64
	var postSteps int
	if te.pool != nil && te.pool.sessionMgr != nil {
		postBytes = te.pool.sessionMgr.GetTranscriptSize(te.currentSessionID)
		postDBBytes = te.pool.sessionMgr.GetSessionDBSize(te.currentSessionID)
		postSteps = te.pool.sessionMgr.CountTranscriptSteps(te.currentSessionID)
	}
	isTurnLimit := te.turnCount >= DefaultMaxSessionTurns
	isStepLimit := postSteps >= DefaultMaxSessionSteps
	isTranscriptByteLimit := postBytes >= DefaultMaxTranscriptBytes
	isDBByteLimit := postDBBytes >= DefaultMaxSessionDBBytes
	isByteLimit := isTranscriptByteLimit || isDBByteLimit

	if isTurnLimit || isStepLimit || isByteLimit {
		if isDBByteLimit {
			log.Printf("[Queue] Scope session reached DB size limit (%d >= %d bytes). Resetting to cold state for fresh session initialization.", postDBBytes, DefaultMaxSessionDBBytes)
			metrics.RecordSessionRotation("post_execution", scope, "bytes")
		} else if isTranscriptByteLimit {
			log.Printf("[Queue] Scope session reached transcript size limit (%d >= %d bytes). Resetting to cold state for fresh session initialization.", postBytes, DefaultMaxTranscriptBytes)
			metrics.RecordSessionRotation("post_execution", scope, "bytes")
		} else if isStepLimit {
			log.Printf("[Queue] Scope session reached step limit (%d >= %d steps). Resetting to cold state for fresh session initialization.", postSteps, DefaultMaxSessionSteps)
			metrics.RecordSessionRotation("post_execution", scope, "steps")
		} else {
			log.Printf("[Queue] Scope session reached turn limit (%d/%d). Resetting to cold state for fresh session initialization.", te.turnCount, DefaultMaxSessionTurns)
			metrics.RecordSessionRotation("post_execution", scope, "turns")
		}
		te.previousSessionID = te.currentSessionID
		te.rotateSessionID(te.threadID, "")
		te.currentSessionID = ""
		te.evictSessionFromPools()
	}
}
