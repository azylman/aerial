package queue

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/notifier"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// httpClientAuthorID marks messages injected over the HTTP prompt endpoint rather than Discord.
const httpClientAuthorID = "http-client"

// triggerSourceDiscord labels turns triggered by Discord messages.
const triggerSourceDiscord = "discord"

type wakeInfo = WakeInfo

type turnExecution struct {
	pool                                                                                        *WorkerPool
	activePool                                                                                  runner.AgentPool
	burst                                                                                       []db.Message
	dispatch                                                                                    *db.Dispatch
	threadID, triggerType, activeSessionID, currentSessionID, previousSessionID, leaseSessionID string
	effectiveID, effectiveName, threadName, turnPrompt, turnStatus, turnResponseText            string
	turnError, injectedHookContext                                                              string
	sentPrompt                                                                                  string // prompt the latest attempt handed to the daemon; captured on the turn span
	execStart                                                                                   time.Time
	activeSessionMu                                                                             sync.Mutex
	turnCount, wakeIdx                                                                          int
	turnDurationMs                                                                              int64
	lease                                                                                       runner.SessionLease
	isCold, isEphemeral, isThread, isQuotaPaused, turnAttempted                                 bool
	summarizeThread                                                                             bool // channel is a summarized Discord thread (set by buildTurnPrompt)
	policy                                                                                      config.ChannelPolicy
	wakeInfos                                                                                   []wakeInfo
	statusUpdater                                                                               *StatusUpdater
	stopTyping                                                                                  func()
	hookMetadata                                                                                map[string]any
	turnTokenUsage                                                                              runner.TokenUsage
	traceSink                                                                                   *runner.TracingTurnSink // latest attempt's sink; reports usage of turns cut short
	turnModel                                                                                   string
	turnSpan                                                                                    trace.Span
	setupSpan                                                                                   trace.Span // turn.setup; nil once the first attempt hands off the prompt
	activeTurnDaemon                                                                            string     // daemon registered in ActiveTurnRegistry for this turn
	turnCtx                                                                                     context.Context
	turnCancel                                                                                  context.CancelFunc
	interrupted                                                                                 atomic.Bool
	finalized                                                                                   atomic.Bool // burst committed as completed; interrupts only cancel post-turn work
	interruptReason                                                                             string
	touchedSessions                                                                             []string
	turnID                                                                                      string
	steerMu                                                                                     sync.Mutex
	steeringClosed                                                                              bool
	drainedIDs                                                                                  []string
	drainedPrompts                                                                              []string
	steeredDispatches                                                                           []db.Dispatch
}

func (te *turnExecution) touchSessionIDLocked(id string) {
	if te == nil {
		return
	}
	s := strings.TrimSpace(id)
	if s == "" {
		return
	}
	if !slices.Contains(te.touchedSessions, s) {
		te.touchedSessions = append(te.touchedSessions, s)
	}
}

func (te *turnExecution) touchedSessionIDs() []string {
	if te == nil {
		return nil
	}
	te.activeSessionMu.Lock()
	defer te.activeSessionMu.Unlock()
	return slices.Clone(te.touchedSessions)
}

func (te *turnExecution) enqueueTranscriptPublish() {
	if te == nil || te.pool == nil {
		return
	}
	for _, sid := range te.touchedSessionIDs() {
		te.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: sid, ThreadID: te.threadID})
	}
}

func (te *turnExecution) isDispatch() bool {
	return te != nil && te.dispatch != nil
}

func (te *turnExecution) leaseKey() string {
	if te == nil {
		return ""
	}
	if te.isDispatch() {
		return te.threadID
	}
	return db.DiscordSessionID(te.threadID)
}

func (te *turnExecution) turnEffortAndModel() (string, string) {
	if te == nil {
		return "", ""
	}
	if te.isDispatch() {
		isEphem := te.isEphemeral || strings.TrimSpace(te.dispatch.SessionID) == ""
		return resolveDispatchEffort(*te.dispatch, isEphem), strings.TrimSpace(te.dispatch.Model)
	}
	return resolveBurstEffort(te.burst), resolveBurstModel(te.burst)
}

func (te *turnExecution) turnCtxValue() context.Context {
	if te != nil && te.turnCtx != nil {
		return te.turnCtx
	}
	return te.poolCtx()
}

func (te *turnExecution) isInterrupted() bool {
	return te != nil && te.interrupted.Load()
}

func (te *turnExecution) setInterruptReason(reason string) {
	if te == nil {
		return
	}
	te.activeSessionMu.Lock()
	te.interruptReason = reason
	te.activeSessionMu.Unlock()
}

func (te *turnExecution) getInterruptReason() string {
	if te == nil {
		return ""
	}
	te.activeSessionMu.Lock()
	defer te.activeSessionMu.Unlock()
	return te.interruptReason
}

func (te *turnExecution) cancelTurn() {
	if te != nil && te.turnCancel != nil {
		te.turnCancel()
	}
}

func (te *turnExecution) finalizeInterruptedTurn(currentModel string) {
	te.stopIndicators()
	metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))

	reason := InterruptedByOperatorReason
	if customReason := te.getInterruptReason(); customReason != "" {
		reason = customReason
	}

	// If this turn was a dispatch that an operator cancel already marked terminal (FAILED),
	// do not overwrite the status/error_message in the database; still record the cancelled outcome.
	if te.isDispatch() && te.store() != nil {
		if d, err := te.store().GetDispatch(context.Background(), te.dispatch.ID); err == nil && d != nil && d.Status == db.StatusFailed {
			te.markTurnCancelled(reason)
			return
		}
	}

	te.failBurstWithStatus("cancelled", reason, currentModel)
}

func (te *turnExecution) store() db.Store {
	if te == nil || te.pool == nil {
		return nil
	}
	return te.pool.Store()
}

func (te *turnExecution) saveSessionID(ctx context.Context, threadID, sessionID string) {
	if te == nil || te.isEphemeral || te.isInterrupted() {
		return
	}
	if s := te.store(); s != nil {
		// A session ID the daemon already adopted must persist even if the turn is canceled meanwhile.
		if err := s.SaveSessionID(context.WithoutCancel(ctx), threadID, sessionID); err != nil {
			te.logCtxf(ctx, "[Worker] Warning saving session ID for thread %s: %v", threadID, err)
		}
	}
}

func (te *turnExecution) rotateSessionID(threadID, newSessionID string) {
	if te == nil || te.isEphemeral || te.isInterrupted() {
		return
	}
	if s := te.store(); s != nil {
		if err := s.RotateSessionID(context.Background(), threadID, newSessionID); err != nil {
			te.logf("[Worker] Warning rotating session ID for thread %s: %v", threadID, err)
		}
	}
}

//nolint:unparam
func (te *turnExecution) finalizeBurstMessages(msgs []db.Message, msgStatus, msgOutput, runStatus, runErr, model string, notify, isResponse bool) {
	telemetry := te.turnTelemetry()
	if te.isDispatch() {
		if isResponse {
			te.updateDispatchCompleted(te.dispatch.ID, msgOutput, telemetry...)
		} else {
			te.updateDispatchStatus(te.dispatch.ID, msgStatus, msgOutput, telemetry...)
		}
		for _, d := range te.steeredDispatches {
			if isResponse {
				te.updateDispatchCompleted(d.ID, msgOutput, telemetry...)
			} else {
				te.updateDispatchStatus(d.ID, msgStatus, msgOutput, telemetry...)
			}
		}
		return
	}
	for _, m := range msgs {
		if isResponse {
			te.updateMessageCompleted(m.ID, msgOutput, telemetry...)
		} else {
			te.updateMessageStatus(m.ID, msgStatus, msgOutput, telemetry...)
		}
		if notify && te.pool != nil && te.pool.cfg.OnMessageCompleted != nil {
			te.pool.cfg.OnMessageCompleted(m, msgStatus)
		}
	}
}

func (te *turnExecution) incrementSessionTurnCount(threadID string) (int, error) {
	if te == nil || te.isEphemeral {
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

func (te *turnExecution) getPreviousSessionID(ctx context.Context, threadID string) string {
	if s := te.store(); s != nil {
		id, err := s.GetPreviousSessionID(ctx, threadID)
		if err != nil {
			te.logCtxf(ctx, "[WorkerPool] Warning getting previous session ID for %s: %v", threadID, err)
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

func resolveBurstTriggerType(firstMsg db.Message) string {
	if firstMsg.AuthorID == httpClientAuthorID {
		return "http"
	}
	return triggerSourceDiscord
}

func (te *turnExecution) runPostTurnHook() {
	if te.turnAttempted && te.policy.Hooks.PostTurn != nil {
		postReq := buildPostTurnRequest(te)
		ctx, cancel := context.WithTimeout(context.Background(), te.policy.Hooks.PostTurn.GetTimeout())
		defer cancel()
		ctx, span := te.startChildSpan(ctx, spanPostTurn)
		defer span.End()
		dispatcher, _ := te.resolveWebhookDispatcher()
		if _, err := dispatcher.CallPostTurnHook(ctx, te.policy.Hooks.PostTurn, postReq); err != nil {
			span.SetStatus(codes.Error, "post_turn hook failed")
			te.logf("[WorkerPool] Warning: post_turn hook failed for thread %s: %v", te.threadID, err)
		}
	}
}

func (te *turnExecution) acquirePoolAndLease() bool {
	poolErr, leaseErr := te.acquireTurnLease()
	if poolErr != nil {
		te.failBurst(poolErr)
		return false
	}
	if te.isInterrupted() {
		te.finalizeInterruptedTurn(te.pool.GetRuntimeConfig())
		return false
	}
	if leaseErr != nil {
		te.logf("[TurnRunner] Warning: prep lease acquisition failed for %s (deferring to attempt runner): %v", te.threadID, leaseErr)
		te.activeSessionMu.Lock()
		te.isCold = !te.isEphemeral && (te.currentSessionID == "" || te.turnCount <= 1)
		te.activeSessionMu.Unlock()
		return true
	}
	return true
}

// errTurnLeaseInterrupted reports a lease taken after the turn was interrupted (and released again).
var errTurnLeaseInterrupted = errors.New("queue: turn interrupted while acquiring the session lease")

// acquireTurnLease resolves the execution pool and takes the session lease onto te without writing
// any row: poolErr reports pool resolution failures, leaseErr lease failures (a lease obtained after
// the turn was interrupted is released and reported as errTurnLeaseInterrupted).
//
//nolint:contextcheck,nolintlint // pool resolution logs on the turn context, as acquirePoolAndLease always did.
func (te *turnExecution) acquireTurnLease() (poolErr, leaseErr error) {
	// 1. Resolve active execution pool (primary vs medium vs low effort)
	activePool, poolKey, err := te.resolveExecutionPool()
	if err != nil {
		return err, nil
	}
	te.activeSessionMu.Lock()
	te.activePool = activePool
	te.isEphemeral = poolKey.Ephemeral
	te.activeSessionMu.Unlock()

	if activePool == nil {
		return fmt.Errorf("queue: no agent execution pool available for thread %s", te.threadID), nil
	}

	// 2. Acquire exclusive session lease before prompt construction
	leaseCtx, leaseCancel := context.WithTimeout(te.turnCtxValue(), 65*time.Second)
	leaseCtx, leaseSpan := te.startSetupChildSpan(leaseCtx, spanLease)
	leaseKey := te.leaseKey()
	lease, leaseErr := activePool.AcquireLease(leaseCtx, leaseKey)
	leaseCancel()
	if leaseErr != nil {
		leaseSpan.SetStatus(codes.Error, "session lease failed")
	} else if lease != nil {
		leaseSpan.SetAttributes(leaseSpanAttributes(lease, poolKey.Ephemeral || lease.IsEphemeral())...)
	}
	leaseSpan.End()
	if leaseErr != nil || te.isInterrupted() {
		if lease != nil {
			logInterruptWarning("release interrupted lease", lease.Release())
		}
		if leaseErr == nil {
			leaseErr = errTurnLeaseInterrupted
		}
		return nil, leaseErr
	}
	te.activeSessionMu.Lock()
	te.lease = lease
	te.leaseSessionID = lease.SessionID()
	te.currentSessionID = te.leaseSessionID
	te.previousSessionID = lease.PreviousSessionID()
	te.isCold = lease.IsCold()
	te.isEphemeral = poolKey.Ephemeral || lease.IsEphemeral()
	te.turnCount = lease.TurnCount()
	te.touchSessionIDLocked(te.leaseSessionID)
	currSess, isCold, isEphem := te.currentSessionID, te.isCold, te.isEphemeral
	te.activeSessionMu.Unlock()

	if currSess != "" && isCold && !isEphem {
		te.saveSessionID(te.turnCtxValue(), te.threadID, currSess)
	}
	return nil, nil
}

// publishTurnCompleted publishes a pubsub.TurnCompletedEvent to aerial.turn.completed on AERIAL_EVENTS.
func (p *WorkerPool) publishTurnCompleted(ctx context.Context, sessionID, conversationID, source, status string, durationMs int64, usage *runner.TokenUsage) error {
	if p == nil {
		return errors.New("worker pool is nil")
	}
	bus := p.EventBus()
	if bus == nil {
		return errors.New("event bus is nil")
	}

	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return errors.New("empty session id")
	}

	source = strings.TrimSpace(source)
	if source == "" {
		source = triggerSourceDiscord
	}

	ev := pubsub.TurnCompletedEvent{
		SessionID:      sessionID,
		ConversationID: strings.TrimSpace(conversationID),
		Source:         source,
		CompletedAt:    time.Now().UTC(),
		Status:         status,
		DurationMs:     durationMs,
	}
	if usage != nil {
		ev.InputTokens = usage.InputTokens
		ev.OutputTokens = usage.OutputTokens
		ev.TotalTokens = usage.TotalTokens
	}

	msgID := fmt.Sprintf("turn:%s:%d", sessionID, time.Now().UnixNano())
	return bus.Publish(ctx, pubsub.SubjectAerialTurnCompleted, msgID, ev)
}

func (te *turnExecution) publishTurnCompleted(status string) {
	if te == nil || te.pool == nil {
		return
	}
	if te.pool.EventBus() == nil {
		return
	}
	sessionID := strings.TrimSpace(te.currentSessionID)
	if sessionID == "" {
		sessionID = strings.TrimSpace(te.activeSessionID)
	}
	if sessionID == "" && te.lease != nil {
		sessionID = strings.TrimSpace(te.lease.SessionID())
	}
	if sessionID == "" {
		sessionID = strings.TrimSpace(te.threadID)
	}
	conversationID := strings.TrimSpace(te.threadID)
	source := strings.TrimSpace(te.triggerType)
	if source == "" || strings.HasPrefix(source, "dispatch:") {
		source = triggerSourceDiscord
	}
	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(te.turnCtxValue()), 5*time.Second)
	defer cancel()
	if err := te.pool.publishTurnCompleted(pubCtx, sessionID, conversationID, source, status, te.turnDurationMs, &te.turnTokenUsage); err != nil {
		te.logf("[WorkerPool] Warning publishing turn completed for %s: %v", sessionID, err)
	}
}

func isHTTPClientBurst(burst []db.Message) bool {
	for _, m := range burst {
		if m.AuthorID != httpClientAuthorID {
			return false
		}
	}
	return len(burst) > 0
}

// resolveChannelPolicy resolves the effective channel and its policy onto te without writing rows.
func (te *turnExecution) resolveChannelPolicy() {
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
}

func (te *turnExecution) resolveTurnPolicy() bool {
	te.resolveChannelPolicy()
	if !isHTTPClientBurst(te.burst) && te.policy.IsIgnored() {
		te.logf("[WorkerPool] Channel %s policy is ignored (mode=%s). Marking %d message(s) completed without execution.", te.threadID, te.policy.Mode, len(te.burst))
		metrics.RecordTurnCompleted("ignored", te.triggerType, "none", time.Since(te.execStart))
		tag := fmt.Sprintf("[%s]", strings.ToUpper(strings.TrimSpace(te.policy.Mode)))
		te.finalizeBurstMessages(te.burst, db.StatusCompleted, tag, "completed", "", "", true, false)
		return false
	}

	sessID, getSessErr := te.getSessionID(te.threadID)
	if getSessErr != nil {
		te.logf("[Worker] Warning getting session ID for thread %s: %v", te.threadID, getSessErr)
	}
	te.activeSessionMu.Lock()
	te.currentSessionID = sessID
	te.activeSessionMu.Unlock()
	return true
}

func shouldEvaluateAmbientWake(mode string, isThread, isHTTP bool) bool {
	return strings.EqualFold(mode, "channel") && !isThread && !isHTTP
}

func (te *turnExecution) resolveWakeHookOverride(ctx context.Context) string {
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

	dispatcher, _ := te.resolveWebhookDispatcher()
	wakeResp, err := dispatcher.CallWakeHook(ctx, te.policy.Hooks.OnWake, req)
	hookOverride := te.policy.Hooks.OnWake.GetOnTimeout("classify")
	if err == nil && wakeResp != nil && strings.TrimSpace(wakeResp.Override) != "" {
		hookOverride = strings.ToLower(strings.TrimSpace(wakeResp.Override))
	}
	return hookOverride
}

func (te *turnExecution) resolveBotIdentity() (string, []string) {
	sess := te.pool.getDiscordSession()
	if sess == nil || sess.State == nil {
		return "", nil
	}
	botUserID := ResolveBotUserID(sess)
	guildID := ""
	if len(te.burst) > 0 {
		guildID = te.burst[0].GuildID
	}
	botRoleIDs := ResolveBotRoleIDs(sess, guildID, botUserID)
	return botUserID, botRoleIDs
}

func (te *turnExecution) classifyAmbientMessages(msgs []db.Message) (float64, string) {
	var recentContext []db.Message
	if te.pool != nil {
		if r, err := te.getRecentThreadMessages(te.threadID, 10); err == nil {
			recentContext = r
		}
	}
	if te.pool == nil || te.pool.cfg.Classifier == nil {
		return 0.0, "no classifier configured"
	}
	var score float64
	var reason string
	if len(msgs) == 1 {
		res := te.pool.cfg.Classifier.Classify(te.turnCtxValue(), msgs[0], recentContext)
		score, reason = res.Confidence, res.Reason
	} else {
		res := te.pool.cfg.Classifier.ClassifyBurst(te.turnCtxValue(), msgs, recentContext)
		score, reason = res.Confidence, res.Reason
	}
	channelName := te.effectiveName
	if channelName == "" {
		channelName = te.threadID
	}
	threshold := te.policy.GetAmbientWakeThreshold()
	te.logf("[AmbientClassifier] Channel %s (%d msgs): score=%.2f threshold=%.2f wake=%v reason=%s",
		channelName, len(msgs), score, threshold, score >= threshold, reason)
	return score, reason
}

func (te *turnExecution) handleAllAmbientPlan() bool {
	if te.pool != nil && te.pool.ctx != nil && te.pool.ctx.Err() != nil {
		te.logf("[WorkerPool] Context cancelled during ambient classification for thread %s. Leaving PROCESSING for startup recovery.", te.threadID)
		metrics.RecordTurnCompleted("cancelled", te.triggerType, "classifier", time.Since(te.execStart))
		return true
	}
	te.markAmbientBurst()
	return true
}

func (te *turnExecution) runChannelAmbientTriage() bool {
	botUserID, botRoleIDs := te.resolveBotIdentity()
	wakeMode := te.policy.GetWakeMode()
	hookOverride := te.resolveWakeHookOverride(te.turnCtxValue())

	plan := PlanBurstExecution(BurstPlanInput{
		Burst: te.burst, WakeMode: wakeMode, Threshold: te.policy.GetAmbientWakeThreshold(),
		BotUserID: botUserID, BotRoleIDs: botRoleIDs, HookOverride: hookOverride, ClassifierFn: te.classifyAmbientMessages,
	})
	te.wakeIdx, te.wakeInfos = plan.WakeIndex, plan.WakeInfos

	if plan.IsAllAmbient {
		return te.handleAllAmbientPlan()
	}

	if te.currentSessionID != "" && te.pool.sessionMgr != nil && !te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID) {
		te.logf("[Queue] Session %s for thread %s not found on disk. Clearing for fresh Turn 1.", te.currentSessionID, te.threadID)
		te.activeSessionMu.Lock()
		te.currentSessionID = ""
		te.activeSessionMu.Unlock()
		te.saveSessionID(te.turnCtxValue(), te.threadID, "")
	}

	metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Add(float64(len(te.burst)))
	return false
}

func (te *turnExecution) startTurnIndicators() {
	deliveryTarget := te.threadID
	te.statusUpdater = NewStatusUpdater(te.pool.getDiscordSession(), deliveryTarget, te.isThread)
	if te.pool.cfg.TypingFunc != nil {
		if stop := te.pool.cfg.TypingFunc(te.pool.getDiscordSession(), deliveryTarget); stop != nil {
			te.stopTyping = stop
		}
	}
}

func (te *turnExecution) evaluateAmbientWake() (shouldExit bool) {
	if shouldEvaluateAmbientWake(te.policy.Mode, te.isThread, isHTTPClientBurst(te.burst)) {
		if te.runChannelAmbientTriage() {
			return true
		}
	}

	te.startTurnIndicators()
	var incErr error
	te.turnCount, incErr = te.incrementSessionTurnCount(te.threadID)
	if incErr != nil {
		te.logf("[Queue] Error incrementing turn count for thread %s: %v", te.threadID, incErr)
	}
	return false
}

// EvaluateIngressWake evaluates whether an incoming Discord message should wake a turn
// or be completed immediately (e.g. ignored channel policy or ambient message).
func (p *WorkerPool) EvaluateIngressWake(ctx context.Context, threadID string, msg db.Message) (shouldEnqueue bool, completedReason string) {
	if p == nil {
		return true, ""
	}
	te := p.newStepTurnExecution(ctx, threadID, []db.Message{msg})
	te.resolveChannelPolicy()
	if !isHTTPClientBurst(te.burst) && te.policy.IsIgnored() {
		metrics.RecordTurnCompleted("ignored", te.triggerType, "none", time.Since(te.execStart))
		if p.cfg.OnMessageCompleted != nil {
			p.cfg.OnMessageCompleted(msg, db.StatusCompleted)
		}
		return false, fmt.Sprintf("[%s]", strings.ToUpper(strings.TrimSpace(te.policy.Mode)))
	}
	if shouldEvaluateAmbientWake(te.policy.Mode, te.isThread, isHTTPClientBurst(te.burst)) {
		return p.evaluateAmbientIngressWake(ctx, te, msg)
	}
	return true, ""
}

func (p *WorkerPool) evaluateAmbientIngressWake(ctx context.Context, te *turnExecution, msg db.Message) (bool, string) {
	botUserID, botRoleIDs := te.resolveBotIdentity()
	wakeMode := te.policy.GetWakeMode()
	hookOverride := te.resolveWakeHookOverride(ctx)
	plan := PlanBurstExecution(BurstPlanInput{
		Burst:        te.burst,
		WakeMode:     wakeMode,
		Threshold:    te.policy.GetAmbientWakeThreshold(),
		BotUserID:    botUserID,
		BotRoleIDs:   botRoleIDs,
		HookOverride: hookOverride,
		ClassifierFn: te.classifyAmbientMessages,
	})
	if !plan.IsAllAmbient {
		return true, ""
	}
	metrics.RecordTurnCompleted("ambient", te.triggerType, "classifier", time.Since(te.execStart))
	metrics.DiscordMessagesProcessedTotal.WithLabelValues("true", "ambient").Inc()
	if p.cfg.OnMessageCompleted != nil {
		p.cfg.OnMessageCompleted(msg, db.StatusCompleted)
	}
	var info WakeInfo
	if len(plan.WakeInfos) > 0 {
		info = plan.WakeInfos[0]
	}
	return false, fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", info.Score, info.Threshold, info.Reason)
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
	skipMsg := fmt.Sprintf("[SKIPPED %s]", reason)
	te.finalizeBurstMessages(te.burst, db.StatusCompleted, skipMsg, "completed", skipMsg, "", true, false)
}

func (te *turnExecution) checkGlobalQuotaLockout(currentAPIKey, currentModel string) bool {
	if currentAPIKey != "" {
		return false
	}
	lockedUntilUnix := te.pool.quotaLockedUntil.Load()
	if lockedUntilUnix <= 0 {
		return false
	}
	lockedUntil := time.Unix(lockedUntilUnix, 0)
	if !time.Now().Before(lockedUntil) {
		return false
	}
	te.isQuotaPaused = true
	remaining := time.Until(lockedUntil)
	te.logf("[WorkerPool] Global quota pause active for thread %s (%v remaining). Aborting turn.", te.threadID, remaining)
	te.stopIndicators()
	reason := fmt.Sprintf("[QUOTA_PAUSED reset_in=%v scheduled=false] global quota pause active", remaining)
	metrics.RecordRunnerError("quota_paused", currentModel)
	metrics.RecordTurnCompleted("quota_paused", te.triggerType, currentModel, time.Since(te.execStart))
	pauseMsg := notifier.FormatQuotaPauseMessage(remaining, lockedUntil, false, false)
	te.deliverQuotaNotice(pauseMsg, "quota pause message")
	te.failBurstWithStatus(outcomeFailed, reason, currentModel)
	return true
}

func (te *turnExecution) isRestartedOrDrainedTurn(lastErrDetail string) bool {
	if lastErrDetail != "" {
		return false
	}
	if te.isDispatch() {
		return te.dispatch != nil && te.dispatch.RestartCount > 0
	}
	for _, m := range te.burst {
		if m.RestartCount > 0 {
			return true
		}
	}
	return false
}

func (te *turnExecution) prepareAttemptPrompt(attempt int, promptToSend, lastErrDetail, lastStderr string) string {
	if promptToSend == "" {
		promptToSend = te.turnPrompt
	}
	if promptToSend == te.turnPrompt && te.currentSessionID != "" && te.pool != nil && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID) {
		isTimeoutRetry := attempt > 1 && (runner.IsInactivityTimeout(lastErrDetail, lastStderr) || strings.Contains(lastErrDetail, "max duration exceeded") || strings.Contains(lastErrDetail, "print timeout") || strings.Contains(lastErrDetail, "empty response") || strings.Contains(lastErrDetail, "context deadline exceeded"))
		if isTimeoutRetry || te.isRestartedOrDrainedTurn(lastErrDetail) {
			return fmt.Sprintf(ContinuationPromptTemplate, te.turnPrompt)
		}
	}
	return promptToSend
}

func (te *turnExecution) buildLeaseTurnContext(runCtx context.Context, currentModel, promptToSend string, sink runner.TurnSink) *runner.TurnContext {
	te.activeSessionMu.Lock()
	leaseSess := te.lease.SessionID()
	te.leaseSessionID = leaseSess
	shouldSave := leaseSess != "" && leaseSess != te.currentSessionID && !te.isEphemeral
	if shouldSave {
		te.currentSessionID = leaseSess
	}
	te.touchSessionIDLocked(leaseSess)
	sessIDToSend := te.currentSessionID
	if leaseSess != "" && te.currentSessionID != "" {
		sessIDToSend = leaseSess
	}
	te.activeSessionMu.Unlock()
	if shouldSave {
		te.saveSessionID(runCtx, te.threadID, leaseSess)
	}
	scopeInst := config.LoadScopeInstructions(te.effectiveName)
	var memParts []string
	for _, m := range te.burst {
		if bt := strings.TrimSpace(m.BodyText()); bt != "" {
			memParts = append(memParts, bt)
		}
	}
	targetID := runner.NormalizeEnvTargetID(te.threadID)
	var vanadisChunkID string
	if te.isDispatch() && te.dispatch != nil {
		if dispTarget := runner.NormalizeEnvTargetID(te.dispatch.TargetID); dispTarget != "" {
			targetID = dispTarget
		}
		chunkID, _ := extractVanadisDispatchInfo(te.dispatch)
		vanadisChunkID = chunkID
	}
	te.turnModel = currentModel
	return &runner.TurnContext{
		TurnID:              uuid.New().String(),
		SessionID:           sessIDToSend,
		Model:               currentModel,
		Prompt:              promptToSend,
		MemoryQuery:         strings.Join(memParts, "\n"),
		ScopeInstructions:   scopeInst,
		ChannelInstructions: scopeInst,
		CoordinationContext: te.injectedHookContext,
		TargetID:            targetID,
		VanadisChunkID:      vanadisChunkID,
		Sink:                sink,
		CreatedAt:           time.Now(),
		Ctx:                 runCtx,
	}
}

// attemptRunTimeout pads the Go context by +1 minute relative to the runner watchdog ceiling so
// the runner watchdog always fires cleanly.
func attemptRunTimeout(timeoutMinutes int) time.Duration {
	return time.Duration(timeoutMinutes+1) * time.Minute
}

// executeLeaseAttemptCtx runs one lease execution under runCtx.
//
//nolint:contextcheck,nolintlint // pool resolution logs on the turn context.
func (te *turnExecution) executeLeaseAttemptCtx(runCtx context.Context, currentModel, promptToSend string) (runner.TurnOutcome, *runner.TurnResult, string, error) {
	activePool := te.activePool
	if activePool == nil {
		if resolved, _, resolveErr := te.resolveExecutionPool(); resolveErr == nil {
			activePool = resolved
			te.activePool = activePool
		}
	}

	if te.lease != nil {
		if te.statusUpdater != nil {
			te.statusUpdater.MarkTurnStarted()
		}
		te.sentPrompt = promptToSend
		sink := runner.NewTracingTurnSink(runCtx, newDiscordTurnSink(te.statusUpdater))
		te.traceSink = sink
		// The loopback MCP proxy claims this attempt's execute_tool spans to nest MCP calls
		// under them; a retry attempt replaces the source.
		te.pool.ActiveTurns().SetToolSpans(te.activeTurnDaemon, sink)
		turnCtx := te.buildLeaseTurnContext(runCtx, currentModel, promptToSend, sink)
		// Setup ends at hand-off: from here the daemon's chat span (and any respawn) takes over.
		te.endSetupSpan()
		turnRes, execErr := te.lease.Execute(runCtx, turnCtx)
		outcome, stderr := te.resolveTurnOutcome(runCtx, turnRes, execErr)
		return outcome, turnRes, stderr, execErr
	}

	err := fmt.Errorf("queue: no agent execution pool available for thread %s", te.threadID)
	outcome, stderr := te.resolveTurnOutcome(runCtx, nil, err)
	return outcome, nil, stderr, err
}

func (te *turnExecution) enrichFailureFromTranscript(outcome runner.TurnOutcome, stdout, stderr string, isSessionCorruption bool) (string, string) {
	errDetail := outcome.ErrorDetail
	targetSess := te.resolveAndPersistSessionID(outcome.SessionID, stdout, stderr, !isSessionCorruption && te.currentSessionID == "")
	if targetSess != "" && te.pool != nil && te.pool.sessionMgr != nil && te.pool.sessionMgr.SessionExistsOnDisk(targetSess) {
		if transcriptErr, tErr := te.pool.sessionMgr.ExtractLastTurnError(te.poolCtx(), targetSess, te.execStart); tErr == nil && transcriptErr != "" {
			if shouldReplaceWithTranscriptError(errDetail, stderr, transcriptErr) {
				te.logf("[Queue] Extracted turn error from session %s transcript: %s", targetSess, transcriptErr)
				return transcriptErr, transcriptErr
			}
		}
	}
	return errDetail, stderr
}

func isWatchdogTimeoutError(errDetail, stderr string) bool {
	return runner.IsInactivityTimeout(errDetail, stderr) ||
		strings.Contains(errDetail, "[watchdog]") ||
		strings.Contains(errDetail, "inactivity timeout exceeded") ||
		strings.Contains(errDetail, "max duration exceeded")
}

type attemptOutcomeState struct {
	isFailure, isTransient, isSessionCorruption bool
	errDetail, stderr, stdout                   string
	resp                                        *runner.AgyResponse
}

func (te *turnExecution) unpackAttemptOutcome(outcome runner.TurnOutcome, turnRes *runner.TurnResult, rawStderr string, execErr error) attemptOutcomeState {
	if outcome.IsSuccess {
		stderr := rawStderr
		if turnRes != nil && turnRes.Stderr != "" {
			stderr = turnRes.Stderr
		}
		if outcome.SessionID != "" {
			te.resolveAndPersistSessionID(outcome.SessionID, outcome.Response, stderr, true)
		}
		return attemptOutcomeState{
			isFailure: false,
			stdout:    outcome.Response,
			stderr:    stderr,
			resp: &runner.AgyResponse{
				ConversationID: outcome.SessionID,
				Status:         "SUCCESS",
				Response:       outcome.Response,
				Usage:          outcome.Usage,
			},
		}
	}

	lastStderr := outcome.ErrorDetail
	if execErr != nil {
		lastStderr = execErr.Error()
	} else if rawStderr != "" {
		lastStderr = rawStderr
	}
	return attemptOutcomeState{
		isFailure:           true,
		isTransient:         outcome.IsTransient,
		isSessionCorruption: outcome.IsSessionCorruption,
		errDetail:           outcome.ErrorDetail,
		stderr:              lastStderr,
	}
}
