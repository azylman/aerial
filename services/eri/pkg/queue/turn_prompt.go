package queue

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/bwmarrin/discordgo"
	"go.opentelemetry.io/otel/trace"
)

func (te *turnExecution) getThreadSummary(threadID string) (string, string) {
	if s := te.store(); s != nil {
		sum, msgID, err := s.GetThreadSummary(context.Background(), threadID)
		if err != nil {
			te.logf("[WorkerPool] Warning getting thread summary for %s: %v", threadID, err)
		}
		return sum, msgID
	}
	return "", ""
}

func (te *turnExecution) saveThreadSummary(threadID, summary, lastMsgID string) {
	if s := te.store(); s != nil {
		if err := s.SaveThreadSummary(context.Background(), threadID, summary, lastMsgID); err != nil {
			te.logf("[WorkerPool] Warning: Failed to save thread summary to DB for thread %s: %v", threadID, err)
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

func (te *turnExecution) liveSessionIDForMessage(msgID string) (string, bool) {
	te.activeSessionMu.Lock()
	defer te.activeSessionMu.Unlock()
	if !te.ownsTurnItem(msgID) {
		return "", false
	}
	if te.activeSessionID != "" {
		return te.activeSessionID, true
	}
	if te.leaseSessionID != "" {
		return te.leaseSessionID, true
	}
	if !te.isEphemeral && te.currentSessionID != "" {
		return te.currentSessionID, true
	}
	return "", true
}

func convertDiscordMessageToDBMessage(dm *discordgo.Message, threadID string, batch ...*discordgo.Message) db.Message {
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
	var attachments []string
	for _, a := range dm.Attachments {
		if a != nil && a.URL != "" {
			attachments = append(attachments, a.URL)
		}
	}
	replyingToAuthor := resolveDiscordReplyAuthor(dm, batch)
	if replyingToAuthor != "" && !strings.HasPrefix(replyingToAuthor, "@") {
		replyingToAuthor = "@" + replyingToAuthor
	}
	content := dm.Content
	if strings.TrimSpace(content) == "" && len(dm.Components) > 0 {
		content = discord.ExtractMessageComponentsText(dm.Components)
	}
	return db.Message{
		ID:               dm.ID,
		ThreadID:         threadID,
		AuthorID:         authorID,
		AuthorName:       authorName,
		Content:          content,
		CreatedAt:        createdAt,
		Mentions:         mentions,
		MentionUserIDs:   mentionUserIDs,
		ReplyingToAuthor: replyingToAuthor,
		Attachments:      attachments,
	}
}

func (te *turnExecution) getRecentThreadMessages(threadID string, limit int) ([]db.Message, error) {
	if limit <= 0 {
		limit = 10
	} else if limit > 100 {
		limit = 100
	}
	if te == nil || te.pool == nil {
		return nil, nil
	}
	dg := te.pool.getDiscordSession()
	if dg == nil || threadID == "" || !IsNumericSnowflake(threadID) {
		return nil, nil
	}

	beforeID := ""
	if len(te.burst) > 0 && IsNumericSnowflake(te.burst[0].ID) {
		beforeID = te.burst[0].ID
	}

	discordMsgs, err := fetchDiscordChannelMessagesWithRetry(te.poolCtx(), dg, threadID, beforeID, limit)
	if err != nil {
		te.logf("[Worker] Discord ChannelMessages failed for thread %s after %d attempts: %v", threadID, historyFetchMaxAttempts, err)
		return nil, err
	}

	msgs := make([]db.Message, 0, len(discordMsgs))
	// ChannelMessages returns newest-first; iterate backwards for chronological order.
	for _, dm := range slices.Backward(discordMsgs) {
		if dm == nil || te.isBurstMessage(dm.ID) {
			continue
		}
		msgs = append(msgs, convertDiscordMessageToDBMessage(dm, threadID, discordMsgs...))
	}
	return msgs, nil
}

type attemptConfig struct {
	model             string
	lowEffortModel    string
	mediumEffortModel string
	timeoutMinutes    int
	agyBin            string
	apiKey            string
}

func applyAppConfigOverrides(cfg *attemptConfig, appCfg *config.Config) {
	if appCfg == nil {
		return
	}
	cur := appCfg.Current()
	if cur == nil {
		return
	}
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

func (te *turnExecution) resolveAttemptConfig(attempt int) attemptConfig {
	var cfg attemptConfig
	if te.pool == nil {
		return cfg
	}
	if attempt > 1 {
		te.logf("[WorkerPool] Resolving attempt %d config for thread %s", attempt, te.threadID)
	}
	te.pool.mu.Lock()
	cfg.model, cfg.lowEffortModel, cfg.mediumEffortModel = te.pool.cfg.Model, te.pool.cfg.LowEffortModel, te.pool.cfg.MediumEffortModel
	cfg.timeoutMinutes, cfg.agyBin, cfg.apiKey = te.pool.cfg.TimeoutMinutes, te.pool.cfg.AgyBin, te.pool.cfg.APIKey
	overrideModel := te.pool.overrideModel
	te.pool.mu.Unlock()

	applyAppConfigOverrides(&cfg, te.pool.appCfg)

	if cfg.lowEffortModel == "" {
		cfg.lowEffortModel = config.GetRuntimeConfig().LowEffortModel
	}
	if cfg.mediumEffortModel == "" {
		cfg.mediumEffortModel = config.GetRuntimeConfig().MediumEffortModel
	}

	effort, reqModel := te.turnEffortAndModel()

	switch effort {
	case "medium":
		if strings.TrimSpace(cfg.mediumEffortModel) != "" {
			cfg.model = cfg.mediumEffortModel
		}
	case "low":
		if strings.TrimSpace(cfg.lowEffortModel) != "" {
			cfg.model = cfg.lowEffortModel
		}
	}
	if reqModel != "" {
		cfg.model = reqModel
	}
	if overrideModel != "" {
		cfg.model = overrideModel
	}

	return cfg
}

func daemonSessionID(pool runner.AgentPool, threadID string) string {
	if d := getStreamingDaemon(pool, threadID); d != nil {
		return d.SessionID()
	}
	return ""
}

func (te *turnExecution) extractVerifiedDiskSessionID(stdout, stderr string) string {
	if stdout == "" && stderr == "" {
		return ""
	}
	combinedOutput := stdout + "\n" + stderr
	extSess := runner.ExtractSessionID(combinedOutput, te.execStart)
	if extSess != "" && te.pool != nil && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(extSess) {
		return extSess
	}
	return ""
}

func (te *turnExecution) probeCandidateSessionID(candidateID, stdout, stderr string) string {
	if candidateID != "" {
		return candidateID
	}
	leaseKey := te.leaseKey()
	if te.isEphemeral {
		if s := daemonSessionID(te.activePool, leaseKey); s != "" {
			return s
		}
	}
	if te.lease != nil && te.lease.SessionID() != "" {
		return te.lease.SessionID()
	}
	if te.currentSessionID != "" {
		return te.currentSessionID
	}
	if s := te.extractVerifiedDiskSessionID(stdout, stderr); s != "" {
		return s
	}
	if s := daemonSessionID(te.activePool, leaseKey); s != "" {
		return s
	}
	if primaryPool := te.pool.AgentPool(); primaryPool != nil && primaryPool != te.activePool {
		return daemonSessionID(primaryPool, leaseKey)
	}
	return ""
}

func (te *turnExecution) resolveAndPersistSessionID(candidateID, stdout, stderr string, allowPersist bool) string {
	te.activeSessionMu.Lock()
	defer te.activeSessionMu.Unlock()

	targetSess := te.probeCandidateSessionID(candidateID, stdout, stderr)
	if targetSess != "" {
		te.activeSessionID = targetSess
		te.leaseSessionID = targetSess
		if allowPersist {
			if !te.isEphemeral && targetSess != te.previousSessionID && targetSess != te.currentSessionID {
				te.logf("[Queue] Synchronizing active session for thread %s: %s -> %s", te.threadID, te.currentSessionID, targetSess)
				te.currentSessionID = targetSess
				te.saveSessionID(te.threadID, targetSess)
			} else if te.isEphemeral && targetSess != te.currentSessionID {
				te.logf("[Queue] Anti-flapping: preserving primary session %s for thread %s; not overwriting with ephemeral session %s", te.currentSessionID, te.threadID, targetSess)
			}
		}
	}
	return targetSess
}

func (te *turnExecution) fetchTurnHistory(fetchCtx context.Context, limit int) []HistoryMessage {
	var beforeID string
	if len(te.burst) > 0 {
		beforeID = te.burst[0].ID
	}
	return te.fetchThreadHistoryBefore(fetchCtx, beforeID, limit)
}

// fetchLatestThreadHistory fetches the newest thread messages, including the reply this turn delivered.
func (te *turnExecution) fetchLatestThreadHistory(fetchCtx context.Context, limit int) []HistoryMessage {
	return te.fetchThreadHistoryBefore(fetchCtx, "", limit)
}

func (te *turnExecution) fetchThreadHistoryBefore(fetchCtx context.Context, beforeID string, limit int) []HistoryMessage {
	if !IsNumericSnowflake(te.threadID) && (te.pool == nil || te.pool.cfg.HistoryFetcher == nil) {
		return nil
	}
	fetchCtx, span := te.startPromptStep(fetchCtx, spanPromptHistoryFetch, attrPromptHistoryLimit.Int(limit))
	defer span.End()
	var err error
	var turnHistory []HistoryMessage
	if te.pool.cfg.HistoryFetcher != nil {
		turnHistory, err = te.pool.cfg.HistoryFetcher(fetchCtx, te.threadID, beforeID, limit)
	} else {
		turnHistory, err = FetchRecentThreadHistory(fetchCtx, te.pool.getDiscordSession(), te.threadID, limit)
	}
	span.SetAttributes(attrPromptHistoryCount.Int(len(turnHistory)))
	if err != nil {
		recordSpanError(span, err)
		te.logf("[WorkerPool] Warning: History fetch failed for thread %s: %v", te.threadID, err)
	}
	return turnHistory
}

func (te *turnExecution) resolveColdStartPreviousSessionID(ctx context.Context, isColdStart bool) string {
	if !isColdStart {
		return ""
	}
	prevID := te.previousSessionID
	if prevID == "" {
		_, span := te.startPromptStep(ctx, spanPromptPrevSessionID)
		prevID = te.getPreviousSessionID(te.threadID)
		span.End()
	}
	if prevID != "" {
		te.logf("[WorkerPool] Injected previous session identifier (%s) into prompt for thread %s", prevID, te.threadID)
	}
	return prevID
}

func (te *turnExecution) resolveAmbientPromptContext(ctx context.Context) string {
	if te.policy.AmbientContext == nil || te.pool == nil || te.pool.cfg.AmbientResolver == nil {
		return ""
	}
	stepCtx, span := te.startPromptStep(promptStepCtx(te.poolCtx(), ctx), spanPromptAmbientContext)
	defer span.End()
	actx, ambErr := te.pool.cfg.AmbientResolver(stepCtx, te.policy.AmbientContext)
	if ambErr != nil {
		recordSpanError(span, ambErr)
		te.logf("[WorkerPool] Warning resolving ambient context for channel %s: %v", te.effectiveName, ambErr)
		return ""
	}
	trimmed := strings.TrimSpace(actx)
	if trimmed != "" {
		te.logf("[WorkerPool] Injected ambient context into prompt for #%s", te.effectiveName)
		return trimmed
	}
	return ""
}

func (te *turnExecution) backfillBurstChannelAndThreadNames() {
	te.activeSessionMu.Lock()
	defer te.activeSessionMu.Unlock()
	for i := range te.burst {
		if te.burst[i].ChannelName == "" && te.effectiveName != "" {
			te.burst[i].ChannelName = te.effectiveName
		}
		if te.isThread && te.burst[i].ThreadName == "" && te.threadName != "" {
			te.burst[i].ThreadName = te.threadName
		}
	}
}

// buildTurnPrompt assembles te.turnPrompt. ctx carries the prompt.build span and is used only
// to parent step spans; each step keeps its own cancellation and timeout source.
func (te *turnExecution) buildTurnPrompt(ctx context.Context) {
	var turnHistory []HistoryMessage
	var turnHistoryFetched bool
	getTurnHistory := func(spanCtx context.Context, limit int) []HistoryMessage {
		if !turnHistoryFetched {
			fetchCtx, fetchCancel := context.WithTimeout(promptStepCtx(te.poolCtx(), spanCtx), 3*time.Second)
			turnHistory = te.fetchTurnHistory(fetchCtx, limit)
			fetchCancel()
			turnHistoryFetched = true
		}
		if len(turnHistory) > limit {
			return turnHistory[:limit]
		}
		return turnHistory
	}

	_, snapSpan := te.startPromptStep(ctx, spanPromptChannelSnapshot)
	snap, snapOK := resolveChannelSnapshot(te.pool.getDiscordSession(), te.threadID)
	snapSpan.SetAttributes(attrPromptSnapshotOK.Bool(snapOK))
	snapSpan.End()
	isColdStart := te.isCold && !te.isEphemeral
	isChannelMode := strings.EqualFold(te.policy.Mode, "channel")
	te.summarizeThread = summarizesThread(snap, isChannelMode)
	summary, lookbackMsgs := te.buildColdStartLookback(ctx, snap, isColdStart, isChannelMode, getTurnHistory)

	prevID := te.resolveColdStartPreviousSessionID(ctx, isColdStart)
	var prevContext string
	if isColdStart && prevID != "" && te.pool != nil && te.pool.sessionMgr != nil {
		_, prevSpan := te.startPromptStep(ctx, spanPromptPreviousSession)
		prevContext = te.pool.sessionMgr.ExtractResponseSince(prevID, time.Time{})
		prevSpan.SetAttributes(attrPromptPrevSessionChars.Int(len(prevContext)))
		prevSpan.End()
	}

	_, instrSpan := te.startPromptStep(ctx, spanPromptScopeInstr)
	instructions := config.LoadScopeInstructions(te.effectiveName)
	instrSpan.End()
	if instructions != "" {
		te.logf("[WorkerPool] Injected channel instructions for #%s into prompt", te.effectiveName)
	}
	if te.injectedHookContext != "" {
		te.logf("[WorkerPool] Injected coordination context into prompt for thread %s", te.threadID)
	}

	ambientCtx := te.resolveAmbientPromptContext(ctx)
	te.backfillBurstChannelAndThreadNames()

	var tz string
	if te.pool != nil && te.pool.appCfg != nil {
		if cur := te.pool.appCfg.Current(); cur != nil {
			tz = cur.Timezone
		}
	}

	isThread := te.isThread || (snap.IsThread && snap.ParentID != "" && snap.ParentID != snap.ID)

	te.turnPrompt = AssembleTurnPrompt(TurnPromptInput{
		AmbientContext:         ambientCtx,
		Burst:                  te.burst,
		ThreadSummary:          summary,
		LookbackHistory:        lookbackMsgs,
		PreviousSessionID:      prevID,
		PreviousSessionContext: prevContext,
		IsColdStart:            isColdStart,
		IsThread:               isThread,
		ChannelInstructions:    instructions,
		InjectedHookContext:    te.injectedHookContext,
		Timezone:               tz,
	})
	trace.SpanFromContext(ctx).SetAttributes(attrPromptColdStart.Bool(isColdStart), attrPromptChars.Int(len(te.turnPrompt)))
}

func latestHistoryMessageID(histMsgs []HistoryMessage) string {
	var latestMsgID string
	var maxTime time.Time
	for _, m := range histMsgs {
		if m.CreatedAt.IsZero() {
			continue
		}
		if m.CreatedAt.After(maxTime) || latestMsgID == "" {
			maxTime, latestMsgID = m.CreatedAt, m.ID
		}
	}
	return latestMsgID
}

func (te *turnExecution) resolveSummarizerLLMAndModel() (LLMFunc, string) {
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
	return llmFn, flashModel
}

const (
	summaryFreshWindow     = 5   // turn end skips the model call when the watermark is among this many newest messages
	coldStartLookbackLimit = 10  // raw messages injected into a cold-start prompt
	turnEndSummaryFetch    = 100 // messages fetched at turn end to locate the watermark (one Discord request)
)

// Values of aerial.prompt.summary_outcome.
const (
	summaryOutcomeCacheHit    = "cache_hit"    // saved watermark is current (turn end) or inside the lookback (cold start)
	summaryOutcomeFreshEnough = "fresh_enough" // turn end: watermark among the newest summaryFreshWindow messages, no model call
	summaryOutcomeStale       = "stale"        // cold start: saved summary injected, watermark older than the lookback
	summaryOutcomeRefreshed   = "refreshed"
	summaryOutcomeFailed      = "failed"
	summaryOutcomeNoHistory   = "no_history" // nothing saved (cold start) or nothing fetched (turn end)
)

// summarizesThread reports whether the turn's channel is a Discord thread whose summary eri maintains.
func summarizesThread(snap ChannelSnapshot, isChannelMode bool) bool {
	return snap.IsThread && snap.ParentID != "" && snap.ParentID != snap.ID && !isChannelMode
}

func (te *turnExecution) resolveThreadColdStartSummary(ctx context.Context, getTurnHistory func(context.Context, int) []HistoryMessage) string {
	ctx, span := te.startPromptStep(ctx, spanPromptThreadSummary)
	defer span.End()
	summary, outcome := te.loadThreadColdStartSummary(ctx, getTurnHistory)
	span.SetAttributes(attrPromptSummaryOutcome.String(outcome))
	return summary
}

// loadThreadColdStartSummary returns the saved thread summary and how current it is. It never calls the
// summarizer: a stale summary is still injected, and the next successful turn-end refresh catches it up.
func (te *turnExecution) loadThreadColdStartSummary(ctx context.Context, getTurnHistory func(context.Context, int) []HistoryMessage) (string, string) {
	_, loadSpan := te.startPromptStep(ctx, spanPromptSummaryLoad)
	cachedSum, lastMsgID := te.getThreadSummary(te.threadID)
	loadSpan.End()
	if cachedSum == "" {
		return "", summaryOutcomeNoHistory
	}
	for _, m := range getTurnHistory(ctx, coldStartLookbackLimit) {
		if m.ID == lastMsgID {
			return cachedSum, summaryOutcomeCacheHit
		}
	}
	return cachedSum, summaryOutcomeStale
}

// refreshThreadSummaryAtTurnEnd brings the saved thread summary up to date after a successful thread turn
// delivered its reply. It runs synchronously while the session lease is held, so the thread's next burst
// waits behind it, and ctx cancellation (shutdown, /interrupt) aborts it. Failures only leave the previous
// summary in place; the turn outcome is already final.
func (te *turnExecution) refreshThreadSummaryAtTurnEnd(ctx context.Context) {
	if te.turnStatus != "success" || !te.summarizeThread || te.isDispatch() || te.isEphemeral {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, DefaultTurnEndSummaryTimeout)
	defer cancel()
	ctx, span := te.startChildSpan(ctx, spanTurnSummaryRefresh)
	defer span.End()
	outcome, err := te.runTurnEndSummaryRefresh(ctx)
	span.SetAttributes(attrPromptSummaryOutcome.String(outcome))
	recordSpanError(span, err)
	if err != nil {
		te.logf("[WorkerPool] Warning: turn-end thread summary refresh failed for thread %s: %v", te.threadID, err)
	}
}

func (te *turnExecution) runTurnEndSummaryRefresh(ctx context.Context) (string, error) {
	_, loadSpan := te.startPromptStep(ctx, spanPromptSummaryLoad)
	cachedSum, lastMsgID := te.getThreadSummary(te.threadID)
	loadSpan.End()
	histMsgs := dropNextBurstMessages(te.fetchLatestThreadHistory(ctx, turnEndSummaryFetch), newestBurstMessageID(te.burst))
	watermark := latestHistoryMessageID(histMsgs)
	if watermark == "" {
		return summaryOutcomeNoHistory, nil
	}
	hadPrior := cachedSum != "" && lastMsgID != ""
	if hadPrior && lastMsgID == watermark {
		return summaryOutcomeCacheHit, nil
	}
	if hadPrior && slices.Contains(newestHistoryMessageIDs(histMsgs, summaryFreshWindow), lastMsgID) {
		return summaryOutcomeFreshEnough, nil
	}
	var opts []ThreadSummaryOption
	if hadPrior {
		opts = append(opts, WithPriorSummary(cachedSum, lastMsgID))
	}
	trace.SpanFromContext(ctx).SetAttributes(attrPromptSummaryHadPrior.Bool(hadPrior))
	llmFn, flashModel := te.resolveSummarizerLLMAndModel()
	newSum, err := SummarizeThreadHistoryWithGroup(ctx, te.pool.SummaryGroup(), llmFn, flashModel, te.threadID, histMsgs, opts...)
	if err != nil {
		return summaryOutcomeFailed, err
	}
	_, saveSpan := te.startPromptStep(ctx, spanPromptSummarySave)
	te.saveThreadSummary(te.threadID, newSum, watermark)
	saveSpan.End()
	return summaryOutcomeRefreshed, nil
}

// newestBurstMessageID returns the newest Discord snowflake in burst, or "" when none is numeric.
func newestBurstMessageID(burst []db.Message) string {
	var newest string
	for _, m := range burst {
		if IsNumericSnowflake(m.ID) && (newest == "" || compareSnowflakeIDs(m.ID, newest) > 0) {
			newest = m.ID
		}
	}
	return newest
}

// dropNextBurstMessages cuts msgs before the oldest non-Assistant message newer than the turn's newest
// burst message. That message belongs to the next queued burst; it and everything after it (including
// this turn's reply when the follow-up arrived mid-turn) are dropped so the watermark never moves past
// it and a later refresh summarizes the follow-up and its reply in order.
func dropNextBurstMessages(msgs []HistoryMessage, newestBurstID string) []HistoryMessage {
	var cutoff string
	for _, m := range msgs {
		if m.Role != "Assistant" && compareSnowflakeIDs(m.ID, newestBurstID) > 0 && (cutoff == "" || compareSnowflakeIDs(m.ID, cutoff) < 0) {
			cutoff = m.ID
		}
	}
	if cutoff == "" {
		return msgs
	}
	return slices.DeleteFunc(slices.Clone(msgs), func(m HistoryMessage) bool {
		return IsNumericSnowflake(m.ID) && compareSnowflakeIDs(m.ID, cutoff) >= 0
	})
}

// newestHistoryMessageIDs returns the IDs of the n newest timestamped messages, newest first.
func newestHistoryMessageIDs(msgs []HistoryMessage, n int) []string {
	dated := slices.DeleteFunc(slices.Clone(msgs), func(m HistoryMessage) bool { return m.CreatedAt.IsZero() })
	slices.SortStableFunc(dated, func(a, b HistoryMessage) int { return b.CreatedAt.Compare(a.CreatedAt) })
	ids := make([]string, 0, min(n, len(dated)))
	for _, m := range dated[:min(n, len(dated))] {
		ids = append(ids, m.ID)
	}
	return ids
}

func (te *turnExecution) resolveColdStartLookbackMessages(ctx context.Context, isColdStart, isChannelMode bool, getTurnHistory func(context.Context, int) []HistoryMessage) []HistoryMessage {
	if !te.isEphemeral && (isColdStart || isChannelMode) {
		lookbackMsgs := getTurnHistory(ctx, coldStartLookbackLimit)
		if len(lookbackMsgs) > 0 {
			te.logf("[WorkerPool] Injected channel history into prompt for thread %s", te.threadID)
		}
		return lookbackMsgs
	}
	return nil
}

func (te *turnExecution) buildColdStartLookback(ctx context.Context, snap ChannelSnapshot, isColdStart, isChannelMode bool, getTurnHistory func(context.Context, int) []HistoryMessage) (string, []HistoryMessage) {
	var summary string
	if isColdStart && summarizesThread(snap, isChannelMode) {
		summary = te.resolveThreadColdStartSummary(ctx, getTurnHistory)
		if summary != "" {
			te.logf("[WorkerPool] Injected <THREAD_SUMMARY> into Turn 1 prompt for thread %s", te.threadID)
		}
	}

	lookbackMsgs := te.resolveColdStartLookbackMessages(ctx, isColdStart, isChannelMode, getTurnHistory)
	return summary, lookbackMsgs
}

// DispatchPromptInput collects pre-fetched context components for deterministic dispatch prompt assembly.
type DispatchPromptInput struct {
	Dispatch          db.Dispatch
	PreviousSessionID string
	IsColdStart       bool
	RecentDispatches  []db.Dispatch
}

func wrapDispatchDirective(prompt string) string {
	trimmed := strings.TrimSpace(prompt)
	if strings.HasPrefix(trimmed, "<SYSTEM_DIRECTIVE>") && strings.HasSuffix(trimmed, "</SYSTEM_DIRECTIVE>") {
		return trimmed
	}
	return fmt.Sprintf("<SYSTEM_DIRECTIVE>\n%s\n\nOnly formulate and output your final response once all immediate work is complete. This is a headless dispatch turn; your response will not be delivered to Discord.\n</SYSTEM_DIRECTIVE>", trimmed)
}

// FormatDispatchHistory formats recent session dispatches into a <DISPATCH_HISTORY> block.
func FormatDispatchHistory(sessionID string, dispatches []db.Dispatch) string {
	if len(dispatches) == 0 {
		return ""
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "<DISPATCH_HISTORY>\nRecent runs for session %s:\n", sessionID)
	for _, d := range dispatches {
		ts := d.CreatedAt
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		summary := strings.TrimSpace(d.Summary)
		if summary == "" {
			summary = db.CleanTaskSummary(d.Prompt)
		}
		if summary == "" {
			summary = strings.TrimSpace(d.Prompt)
		}
		fmt.Fprintf(&sb, "- [%s] status=%s: %s\n", ts.UTC().Format(time.RFC3339), d.Status, summary)
	}
	sb.WriteString("</DISPATCH_HISTORY>")
	return sb.String()
}

// AssembleDispatchPrompt deterministically composes the prompt for a headless background dispatch.
func AssembleDispatchPrompt(input DispatchPromptInput) string {
	basePrompt := wrapDispatchDirective(input.Dispatch.Prompt)
	if len(input.RecentDispatches) > 0 && strings.TrimSpace(input.Dispatch.SessionID) != "" {
		if hist := FormatDispatchHistory(input.Dispatch.SessionID, input.RecentDispatches); hist != "" {
			basePrompt = prependPromptLayer(basePrompt, hist)
		}
	}
	if input.IsColdStart && strings.TrimSpace(input.PreviousSessionID) != "" {
		if prevBlock := FormatPreviousSession(input.PreviousSessionID); prevBlock != "" {
			basePrompt = prependPromptLayer(basePrompt, prevBlock)
		}
	}
	return strings.TrimSpace(basePrompt)
}

// buildDispatchPrompt assembles the dispatch prompt. ctx carries the prompt.build span and is
// used only to parent step spans; each step keeps its own cancellation source.
func (te *turnExecution) buildDispatchPrompt(ctx context.Context) string {
	if te == nil || te.dispatch == nil {
		return ""
	}
	d := *te.dispatch
	var prevID string
	var recent []db.Dispatch
	isColdStart := !te.isEphemeral && te.isCold
	if isColdStart && strings.TrimSpace(d.SessionID) != "" {
		prevID = te.resolveColdStartPreviousSessionID(ctx, true)
		if s := te.store(); s != nil {
			recent = te.fetchRecentDispatches(ctx, s, d.SessionID)
		}
	}
	prompt := AssembleDispatchPrompt(DispatchPromptInput{
		Dispatch:          d,
		PreviousSessionID: prevID,
		IsColdStart:       isColdStart,
		RecentDispatches:  recent,
	})
	trace.SpanFromContext(ctx).SetAttributes(attrPromptColdStart.Bool(isColdStart), attrPromptChars.Int(len(prompt)))
	return prompt
}

// fetchRecentDispatches loads the session's recent dispatch history under a prompt.recent_dispatches span.
func (te *turnExecution) fetchRecentDispatches(ctx context.Context, s db.Store, sessionID string) []db.Dispatch {
	stepCtx, span := te.startPromptStep(promptStepCtx(te.poolCtx(), ctx), spanPromptRecentDispatches)
	defer span.End()
	rec, err := s.GetRecentSessionDispatches(stepCtx, sessionID, 10)
	if err != nil {
		recordSpanError(span, err)
		te.logf("[WorkerPool] Warning getting recent session dispatches for %s: %v", sessionID, err)
		return nil
	}
	span.SetAttributes(attrPromptDispatchCount.Int(len(rec)))
	return rec
}
