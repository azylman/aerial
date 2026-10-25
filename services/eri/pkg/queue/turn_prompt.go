package queue

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/bwmarrin/discordgo"
)

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
func GetSessionLastActivity(store db.Store, threadID string, mgr ...*session.Manager) (time.Time, bool, error) {
	if store == nil || strings.TrimSpace(threadID) == "" {
		return time.Time{}, true, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
						Mentions: mentions, MentionUserIDs: mentionUserIDs, ReplyingToAuthor: replyingToAuthor,
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
	if targetSess == "" && te.isEphemeral {
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
	if primaryPool := te.pool.AgentPool(); targetSess == "" && primaryPool != nil && primaryPool != te.activePool {
		if d := getStreamingDaemon(primaryPool, te.threadID); d != nil && d.SessionID() != "" {
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

func (te *turnExecution) fetchTurnHistory(fetchCtx context.Context, limit int) []HistoryMessage {
	if !IsNumericSnowflake(te.threadID) && (te.pool == nil || te.pool.cfg.HistoryFetcher == nil) {
		return nil
	}
	var err error
	var turnHistory []HistoryMessage
	if te.pool.cfg.HistoryFetcher != nil {
		turnHistory, err = te.pool.cfg.HistoryFetcher(fetchCtx, te.threadID, te.burst[0].ID, limit)
	} else {
		turnHistory, err = FetchRecentThreadHistory(fetchCtx, te.pool.getDiscordSession(), te.store(), te.threadID, limit)
	}
	if err != nil {
		log.Printf("[WorkerPool] Warning: History fetch failed for thread %s: %v", te.threadID, err)
	}
	return turnHistory
}

func (te *turnExecution) buildTurnPrompt() {
	var turnHistory []HistoryMessage
	var turnHistoryFetched bool
	getTurnHistory := func(limit int) []HistoryMessage {
		if !turnHistoryFetched {
			fetchCtx, fetchCancel := context.WithTimeout(te.poolCtx(), 3*time.Second)
			turnHistory = te.fetchTurnHistory(fetchCtx, limit)
			fetchCancel()
			turnHistoryFetched = true
		}
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
		if actx, ambErr := te.pool.cfg.AmbientResolver(te.poolCtx(), te.policy.AmbientContext); ambErr != nil {
			log.Printf("[WorkerPool] Warning resolving ambient context for channel %s: %v", te.effectiveName, ambErr)
		} else if strings.TrimSpace(actx) != "" {
			ambientCtx = actx
			log.Printf("[WorkerPool] Injected ambient context into prompt for #%s", te.effectiveName)
		}
	}

	for i := range te.burst {
		if te.burst[i].ChannelName == "" && te.effectiveName != "" {
			te.burst[i].ChannelName = te.effectiveName
		}
		if te.isThread && te.burst[i].ThreadName == "" && te.threadName != "" {
			te.burst[i].ThreadName = te.threadName
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
