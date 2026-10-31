package queue

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	pool                                                                                        *WorkerPool
	activePool                                                                                  runner.AgentPool
	burst, trailingMsgs                                                                         []db.Message
	dispatch                                                                                    *db.Dispatch
	threadID, triggerType, activeSessionID, currentSessionID, previousSessionID, leaseSessionID string
	effectiveID, effectiveName, threadName, turnPrompt, turnStatus, turnResponseText            string
	turnError, injectedHookContext                                                              string
	execStart                                                                                   time.Time
	activeSessionMu                                                                             sync.Mutex
	turnCount, wakeIdx                                                                          int
	turnDurationMs                                                                              int64
	lease                                                                                       runner.SessionLease
	isCold, isEphemeral, isThread, isQuotaPaused, turnAttempted                                 bool
	policy                                                                                      config.ChannelPolicy
	wakeInfos, trailingInfos                                                                    []wakeInfo
	statusUpdater                                                                               *StatusUpdater
	stopTyping                                                                                  func()
	hookMetadata                                                                                map[string]any
	turnTokenUsage                                                                              runner.TokenUsage
	turnCtx                                                                                     context.Context
	turnCancel                                                                                  context.CancelFunc
	interrupted                                                                                 atomic.Bool
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

func (te *turnExecution) incrementTurnRetries(errDetail string) {
	if te == nil {
		return
	}
	if te.isDispatch() {
		te.incrementDispatchRetry(te.dispatch.ID, errDetail)
		return
	}
	for _, m := range te.burst {
		te.incrementMessageRetry(m.ID, errDetail)
	}
}

func (te *turnExecution) initialRetryCount() int {
	if te == nil {
		return 0
	}
	if te.isDispatch() {
		return te.dispatch.RetryCount
	}
	return maxBurstRetryCount(te.burst)
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

func (te *turnExecution) ownsTurnItem(id string) bool {
	if te == nil {
		return false
	}
	return te.isBurstMessage(id) || (te.isDispatch() && te.dispatch.ID == id)
}

func (te *turnExecution) turnCtxValue() context.Context {
	if te != nil && te.turnCtx != nil {
		return te.turnCtx
	}
	return te.poolCtx()
}

func (te *turnExecution) turnDone() <-chan struct{} {
	return te.turnCtxValue().Done()
}

func (te *turnExecution) isInterrupted() bool {
	return te != nil && te.interrupted.Load()
}

func (te *turnExecution) cancelTurn() {
	if te != nil && te.turnCancel != nil {
		te.turnCancel()
	}
}

func (te *turnExecution) finalizeInterruptedTurn(currentModel string) {
	te.stopIndicators()
	metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
	te.failBurstWithStatus("cancelled", InterruptedByOperatorReason, currentModel)
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
		if err := s.UpdateMessageCompleted(context.Background(), id, responseText, te.currentSessionID); err != nil {
			log.Printf("[Worker] Warning updating message %s completed: %v", id, err)
		}
	}
}

func (te *turnExecution) updateDispatchCompleted(id, responseText string) {
	if s := te.store(); s != nil {
		if err := s.UpdateDispatchCompleted(context.Background(), id, responseText, te.currentSessionID); err != nil {
			log.Printf("[Worker] Warning updating dispatch %s completed: %v", id, err)
		}
	}
}

func (te *turnExecution) updateDispatchStatus(id, status string, errorMsg ...string) {
	errMsg := ""
	if len(errorMsg) > 0 {
		errMsg = errorMsg[0]
	}
	if s := te.store(); s != nil {
		if err := s.UpdateDispatchStatus(context.Background(), id, status, errMsg); err != nil {
			log.Printf("[Worker] Warning updating dispatch %s status to %s: %v", id, status, err)
		}
	}
}

func (te *turnExecution) incrementDispatchRetry(id, errorMsg string) {
	if s := te.store(); s != nil {
		if err := s.IncrementDispatchRetry(context.Background(), id, errorMsg); err != nil {
			log.Printf("[Worker] Warning incrementing retry for dispatch %s: %v", id, err)
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
	if te == nil || te.isEphemeral || te.isInterrupted() {
		return
	}
	if s := te.store(); s != nil {
		if err := s.SaveSessionID(context.Background(), threadID, sessionID); err != nil {
			log.Printf("[Worker] Warning saving session ID for thread %s: %v", threadID, err)
		}
	}
}

func (te *turnExecution) rotateSessionID(threadID, newSessionID string) {
	if te == nil || te.isEphemeral || te.isInterrupted() {
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
	if te.isDispatch() {
		if isResponse {
			te.updateDispatchCompleted(te.dispatch.ID, msgOutput)
		} else {
			te.updateDispatchStatus(te.dispatch.ID, msgStatus, msgOutput)
		}
		return
	}
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

func (p *WorkerPool) resolveBurstScopeLock(burst []db.Message, scopeKeyOpt []string) (string, string, *sync.Mutex) {
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
	return threadID, scopeKey, scopeLock
}

func resolveBurstTriggerType(firstMsg db.Message) string {
	if firstMsg.AuthorID == "http-client" {
		return "http"
	}
	return "discord"
}

func (te *turnExecution) runPostTurnHook() {
	if te.turnAttempted && te.policy.Hooks.PostTurn != nil {
		postReq := buildPostTurnRequest(te)
		ctx, cancel := context.WithTimeout(context.Background(), te.policy.Hooks.PostTurn.GetTimeout())
		defer cancel()
		dispatcher, _ := te.resolveWebhookDispatcher()
		if _, err := dispatcher.CallPostTurnHook(ctx, te.policy.Hooks.PostTurn, postReq); err != nil {
			log.Printf("[WorkerPool] Warning: post_turn hook failed for thread %s: %v", te.threadID, err)
		}
	}
}

func (te *turnExecution) handleBurstPanic(r any) {
	log.Printf("[WorkerPool] Panic in processBurst for thread %s: %v", te.threadID, r)
	te.stopIndicators()
	metrics.RecordTurnCompleted("panic", te.triggerType, te.pool.GetRuntimeConfig(), time.Since(te.execStart))
	te.failBurstWithStatus("failed", fmt.Sprintf("panic: %v", r), "")
}

func (te *turnExecution) acquirePoolAndLease() bool {
	// 1. Resolve active execution pool (primary vs medium vs low effort)
	activePool, poolKey, poolErr := te.resolveExecutionPool()
	if poolErr != nil {
		te.failBurst(poolErr)
		return false
	}
	te.activeSessionMu.Lock()
	te.activePool = activePool
	te.isEphemeral = poolKey.Ephemeral
	te.activeSessionMu.Unlock()

	if activePool == nil {
		te.failBurst(fmt.Errorf("queue: no agent execution pool available for thread %s", te.threadID))
		return false
	}

	// 2. Acquire exclusive session lease before prompt construction
	leaseCtx, leaseCancel := context.WithTimeout(te.turnCtxValue(), 65*time.Second)
	leaseKey := te.leaseKey()
	lease, leaseErr := activePool.AcquireLease(leaseCtx, leaseKey)
	leaseCancel()
	if leaseErr != nil || te.isInterrupted() {
		if lease != nil {
			logInterruptWarning("release interrupted lease", lease.Release())
		}
		if te.isInterrupted() {
			te.finalizeInterruptedTurn(te.pool.GetRuntimeConfig())
			return false
		}
		te.failBurst(fmt.Errorf("queue: failed to acquire session lease for thread %s: %w", te.threadID, leaseErr))
		return false
	}
	te.activeSessionMu.Lock()
	te.lease = lease
	te.leaseSessionID = lease.SessionID()
	te.currentSessionID = te.leaseSessionID
	te.previousSessionID = lease.PreviousSessionID()
	te.isCold = lease.IsCold()
	te.isEphemeral = poolKey.Ephemeral || lease.IsEphemeral()
	te.turnCount = lease.TurnCount()
	currSess, isCold, isEphem := te.currentSessionID, te.isCold, te.isEphemeral
	te.activeSessionMu.Unlock()

	if currSess != "" && isCold && !isEphem {
		te.saveSessionID(te.threadID, currSess)
	}
	return true
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

	threadID, scopeKey, scopeLock := p.resolveBurstScopeLock(burst, scopeKeyOpt)
	scopeLock.Lock()
	defer scopeLock.Unlock()

	turnCtx, turnCancel := context.WithCancel(p.ctx)
	defer turnCancel()

	te := &turnExecution{
		pool:        p,
		burst:       burst,
		threadID:    threadID,
		triggerType: resolveBurstTriggerType(burst[0]),
		execStart:   time.Now().UTC(),
		wakeIdx:     -1,
		isEphemeral: false,
		stopTyping:  func() {},
		turnCtx:     turnCtx,
		turnCancel:  turnCancel,
	}
	p.registerActiveTurn(scopeKey, te)
	defer p.unregisterActiveTurn(scopeKey, te)

	// Synchronous post_turn hook:
	// Go defers execute LIFO. By placing post_turn right after scopeLock.Lock() / defer scopeLock.Unlock(),
	// it executes AFTER handleTrailing and panic recovery, but BEFORE scopeLock.Unlock().
	defer te.runPostTurnHook()

	defer te.stopIndicators()

	defer func() {
		if r := recover(); r != nil {
			te.handleBurstPanic(r)
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

	if !te.acquirePoolAndLease() {
		return
	}
	defer func() {
		if te.lease != nil {
			if relErr := te.lease.Release(); relErr != nil {
				log.Printf("[WorkerPool] Warning releasing lease for %s: %v", te.threadID, relErr)
			}
		}
	}()

	if !te.runPreTurnHook() {
		return
	}

	te.turnAttempted = true
	te.buildTurnPrompt()
	te.executeWithRetries()
}

func (p *WorkerPool) processDispatch(d db.Dispatch, scopeKey string) {
	if p.ctx.Err() != nil {
		return
	}

	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()

	lockVal, _ := p.scopeLocks.LoadOrStore(scopeKey, &sync.Mutex{})
	scopeLock, ok := lockVal.(*sync.Mutex)
	if !ok {
		scopeLock = &sync.Mutex{}
		p.scopeLocks.Store(scopeKey, scopeLock)
	}
	scopeLock.Lock()
	defer scopeLock.Unlock()

	if p.cfg.Store == nil {
		log.Printf("[WorkerPool] Store is nil, dropping dispatch %s", d.ID)
		return
	}
	claimed, claimErr := p.cfg.Store.ClaimPendingDispatch(p.ctx, d.ID)
	if claimErr != nil || !claimed {
		if claimErr != nil {
			log.Printf("[WorkerPool] Failed to claim dispatch %s: %v", d.ID, claimErr)
		}
		return
	}

	isEphem := strings.TrimSpace(d.SessionID) == ""
	leaseKey := d.SessionID
	if isEphem {
		leaseKey = "dispatch-" + d.ID
	}

	turnCtx, turnCancel := context.WithCancel(p.ctx)
	defer turnCancel()

	te := &turnExecution{
		pool:        p,
		dispatch:    &d,
		threadID:    leaseKey,
		triggerType: "dispatch:" + d.Source,
		execStart:   time.Now().UTC(),
		wakeIdx:     -1,
		isEphemeral: isEphem,
		stopTyping:  func() {},
		turnCtx:     turnCtx,
		turnCancel:  turnCancel,
	}

	p.registerActiveTurn(scopeKey, te)
	defer p.unregisterActiveTurn(scopeKey, te)

	defer func() {
		if r := recover(); r != nil {
			te.handleBurstPanic(r)
		}
	}()

	if !te.acquirePoolAndLease() {
		return
	}
	defer func() {
		if !te.isEphemeral && te.activePool != nil {
			if evicter, ok := te.activePool.(interface{ EvictSession(string) error }); ok {
				if err := evicter.EvictSession(te.leaseKey()); err != nil {
					log.Printf("[WorkerPool] Warning evicting session for %s: %v", te.leaseKey(), err)
				}
			}
		}
		if te.lease != nil {
			if relErr := te.lease.Release(); relErr != nil {
				log.Printf("[WorkerPool] Warning releasing lease for %s: %v", te.threadID, relErr)
			}
		}
	}()

	chunkID, buildID, chunkIdx := extractVanadisDispatchInfo(d)
	if chunkID != "" {
		p.StartChunkStepTurn(chunkID, buildID, chunkIdx, d.ID)
		p.ensureChunkWidgetHydrated(d.Prompt, chunkID, buildID)
		defer p.FinishChunkStepTurn(chunkID, d.ID)
		te.statusUpdater = NewChunkStepStatusUpdater(chunkID, func(step string) error {
			return p.FlushChunkActiveStep(chunkID, d.ID, step)
		})
		defer func() {
			te.statusUpdater.Stop()
			te.statusUpdater.DeleteStatusMessage()
		}()
	}

	te.turnAttempted = true
	te.turnPrompt = te.buildDispatchPrompt()
	te.executeWithRetries()
}

var (
	vanadisChunkIDPromptRegex     = regexp.MustCompile(`(?m)^Chunk ID:\s*(\S+)`)
	vanadisBuildIDPromptRegex     = regexp.MustCompile(`(?m)^Build ID:\s*(\S+)`)
	vanadisActiveChunkHeaderRegex = regexp.MustCompile(`(?i)===\s*Active Chunk\s*\(Chunk\s+(\d+)\s+of\s+\d+\)\s*===`)
	standalonePRRemediationRegex  = regexp.MustCompile(`PR #(\d+)\s+on\s+([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)|(?:on|for repo)\s+([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)\s+\((?:commit: [^,]+,\s*)?PR #(\d+)`)
)

func extractPRRemediationTarget(prompt string) (repo string, prNum int, ok bool) {
	m := standalonePRRemediationRegex.FindStringSubmatch(prompt)
	if len(m) < 5 {
		return "", 0, false
	}
	prNumStr, repo := m[1], m[2]
	if prNumStr == "" {
		repo, prNumStr = m[3], m[4]
	}
	prNum, err := strconv.Atoi(prNumStr)
	return repo, prNum, err == nil && prNum > 0
}

func extractDispatchWidgetSyncPayload(prompt, buildID string) (string, bool) {
	if repo, prNum, ok := extractPRRemediationTarget(prompt); ok {
		return fmt.Sprintf(`{"repo":%q,"pr_number":%d}`, repo, prNum), true
	}
	if buildID != "" && !strings.HasPrefix(buildID, "pr-") {
		return fmt.Sprintf(`{"build_id":%q}`, buildID), true
	}
	return "", false
}

func extractVanadisDispatchInfo(d db.Dispatch) (chunkID, buildID string, chunkIdx int) {
	chunkIdx = -1
	if strings.HasPrefix(d.SessionID, "vanadis-") {
		chunkID = strings.TrimSpace(strings.TrimPrefix(d.SessionID, "vanadis-"))
	}
	if chunkID == "" {
		if m := vanadisChunkIDPromptRegex.FindStringSubmatch(d.Prompt); len(m) >= 2 {
			chunkID = strings.TrimSpace(m[1])
		}
	}
	if m := vanadisBuildIDPromptRegex.FindStringSubmatch(d.Prompt); len(m) >= 2 {
		buildID = strings.TrimSpace(m[1])
	}
	if m := vanadisActiveChunkHeaderRegex.FindStringSubmatch(d.Prompt); len(m) >= 2 {
		if idx, err := strconv.Atoi(m[1]); err == nil && idx >= 1 {
			chunkIdx = idx - 1
		}
	}
	if buildID == "" && strings.HasPrefix(chunkID, "build-") {
		if idx := strings.LastIndex(chunkID, "-chunk-"); idx > 0 {
			buildID = chunkID[:idx]
		}
	}
	if chunkID == "" {
		if repo, prNum, ok := extractPRRemediationTarget(d.Prompt); ok {
			prID := fmt.Sprintf("pr-%s-%d", repo, prNum)
			return prID, prID, 0
		}
	}
	return chunkID, buildID, chunkIdx
}

func (te *turnExecution) claimPendingBurst() []db.Message {
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
	return claimedBurst
}

func (te *turnExecution) claimAndFilterStale() bool {
	claimedBurst := te.claimPendingBurst()
	if len(claimedBurst) == 0 {
		return false
	}
	te.activeSessionMu.Lock()
	te.burst = claimedBurst
	te.activeSessionMu.Unlock()

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

	sessID, getSessErr := te.getSessionID(te.threadID)
	if getSessErr != nil {
		log.Printf("[Worker] Warning getting session ID for thread %s: %v", te.threadID, getSessErr)
	}
	te.activeSessionMu.Lock()
	te.currentSessionID = sessID
	te.activeSessionMu.Unlock()
	return true
}

func shouldEvaluateAmbientWake(mode string, isThread, isHTTP bool) bool {
	return strings.ToLower(mode) == "channel" && !isThread && !isHTTP
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

func (te *turnExecution) resolveBotIdentity() (string, []string) {
	botUserID := ""
	var botRoleIDs []string
	if sess := te.pool.getDiscordSession(); sess != nil && sess.State != nil {
		if sess.State.User != nil {
			botUserID = sess.State.User.ID
		}
		botRoleIDs = ResolveBotRoleIDs(sess, te.burst[0].GuildID, botUserID)
	}
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
	if len(msgs) == 1 {
		res := te.pool.cfg.Classifier.Classify(te.pool.ctx, msgs[0], recentContext)
		return res.Confidence, res.Reason
	}
	res := te.pool.cfg.Classifier.ClassifyBurst(te.pool.ctx, msgs, recentContext)
	log.Printf("[AmbientClassifier] Channel %s | BurstSize %d | Score: %.2f (Threshold: %.2f) | Wake: %t | Reason: %s",
		te.threadID, len(msgs), res.Confidence, te.policy.GetAmbientWakeThreshold(), res.Confidence >= te.policy.GetAmbientWakeThreshold(), res.Reason)
	return res.Confidence, res.Reason
}

func (te *turnExecution) handleAllAmbientPlan() bool {
	if te.pool != nil && te.pool.ctx != nil && te.pool.ctx.Err() != nil {
		log.Printf("[WorkerPool] Context cancelled during ambient classification for thread %s. Leaving PROCESSING for startup recovery.", te.threadID)
		metrics.RecordTurnCompleted("cancelled", te.triggerType, "classifier", time.Since(te.execStart))
		return true
	}
	te.markAmbientBurst()
	return true
}

func (te *turnExecution) runChannelAmbientTriage() bool {
	botUserID, botRoleIDs := te.resolveBotIdentity()
	wakeMode := te.policy.GetWakeMode()
	hookOverride := te.resolveWakeHookOverride()

	plan := PlanBurstExecution(BurstPlanInput{
		Burst: te.burst, WakeMode: wakeMode, Threshold: te.policy.GetAmbientWakeThreshold(),
		BotUserID: botUserID, BotRoleIDs: botRoleIDs, HookOverride: hookOverride, ClassifierFn: te.classifyAmbientMessages,
	})
	te.wakeIdx, te.wakeInfos = plan.WakeIndex, plan.WakeInfos

	if plan.IsAllAmbient {
		return te.handleAllAmbientPlan()
	}

	if te.currentSessionID != "" && te.pool.sessionMgr != nil && !te.pool.sessionMgr.SessionExistsOnDisk(te.currentSessionID) {
		log.Printf("[Queue] Session %s for thread %s not found on disk. Clearing for fresh Turn 1.", te.currentSessionID, te.threadID)
		te.activeSessionMu.Lock()
		te.currentSessionID = ""
		te.activeSessionMu.Unlock()
		te.saveSessionID(te.threadID, "")
	}

	for i, m := range plan.LeadingAmbient {
		te.completeAmbientMessage(m, plan.WakeInfos[i])
	}
	te.activeSessionMu.Lock()
	if !te.isInterrupted() {
		te.trailingMsgs, te.trailingInfos = plan.TrailingMessages, plan.TrailingInfos
	}
	te.burst = []db.Message{plan.ActiveMessage}
	te.activeSessionMu.Unlock()
	metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "wake").Inc()
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
	te.activeSessionMu.Lock()
	if len(te.trailingMsgs) == 0 {
		te.activeSessionMu.Unlock()
		return
	}
	trailing := te.trailingMsgs
	trailingInfos := te.trailingInfos
	te.trailingMsgs = nil
	te.trailingInfos = nil
	te.activeSessionMu.Unlock()
	if !te.isQuotaPaused && te.pool != nil && te.pool.ctx != nil && te.pool.ctx.Err() != nil {
		return
	}
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

func maxBurstRetryCount(burst []db.Message) int {
	maxRetries := 0
	for _, m := range burst {
		if m.RetryCount > maxRetries {
			maxRetries = m.RetryCount
		}
	}
	return maxRetries
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
	log.Printf("[WorkerPool] Global quota pause active for thread %s (%v remaining). Aborting turn.", te.threadID, remaining)
	te.stopIndicators()
	reason := fmt.Sprintf("[QUOTA_PAUSED reset_in=%v scheduled=false] global quota pause active", remaining)
	metrics.RecordRunnerError("quota_paused", currentModel)
	metrics.RecordTurnCompleted("quota_paused", te.triggerType, currentModel, time.Since(te.execStart))
	pauseMsg := notifier.FormatQuotaPauseMessage(remaining, lockedUntil, false, false)
	te.deliverQuotaNotice(pauseMsg, "quota pause message")
	te.failBurstWithStatus("failed", reason, currentModel)
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
		isTimeoutRetry := attempt > 1 && (runner.IsInactivityTimeout(lastErrDetail, lastStderr) || strings.Contains(lastErrDetail, "max duration exceeded") || strings.Contains(lastErrDetail, "print timeout") || strings.Contains(lastErrDetail, "empty response after") || strings.Contains(lastErrDetail, "context deadline exceeded"))
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
	sessIDToSend := te.currentSessionID
	if leaseSess != "" && te.currentSessionID != "" {
		sessIDToSend = leaseSess
	}
	te.activeSessionMu.Unlock()
	if shouldSave {
		te.saveSessionID(te.threadID, leaseSess)
	}
	scopeInst := config.LoadScopeInstructions(te.effectiveName)
	var memParts []string
	for _, m := range te.burst {
		if bt := strings.TrimSpace(m.BodyText()); bt != "" {
			memParts = append(memParts, bt)
		}
	}
	return &runner.TurnContext{
		TurnID:              uuid.New().String(),
		SessionID:           sessIDToSend,
		Model:               currentModel,
		Prompt:              promptToSend,
		MemoryQuery:         strings.Join(memParts, "\n"),
		ScopeInstructions:   scopeInst,
		ChannelInstructions: scopeInst,
		CoordinationContext: te.injectedHookContext,
		Sink:                sink,
		CreatedAt:           time.Now(),
		Ctx:                 runCtx,
	}
}

func (te *turnExecution) executeLeaseAttempt(currentModel, promptToSend string, currentTimeout int) (runner.TurnOutcome, *runner.TurnResult, string, error) {
	// Pad Go context by +1 minute relative to runner watchdog ceiling so the runner watchdog always fires cleanly
	runCtx, runCancel := context.WithTimeout(te.turnCtxValue(), time.Duration(currentTimeout+1)*time.Minute)
	defer runCancel()

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
		sink := newDiscordTurnSink(te.statusUpdater)
		turnCtx := te.buildLeaseTurnContext(runCtx, currentModel, promptToSend, sink)
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
			log.Printf("[Queue] Extracted turn error from session %s transcript: %s", targetSess, transcriptErr)
			return transcriptErr, transcriptErr
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

func classifyRunnerErrorCategory(isWatchdog, isTransient, isSessionCorruption bool) string {
	if isWatchdog {
		return "watchdog_timeout"
	}
	if isTransient {
		return "transient"
	}
	if isSessionCorruption {
		return "session_corrupt"
	}
	return "non_transient"
}

func (te *turnExecution) handleWatchdogFailure(attempt, maxAttempts int, currentAgyBin, currentAPIKey, errDetail, stderr, currentModel string) (shouldRetry bool) {
	if attempt < maxAttempts {
		if shutdown := te.handleRetryBackoff(attempt, maxAttempts, errDetail, currentModel); shutdown {
			return false
		}
		log.Printf("[WorkerPool] Retrying watchdog timeout (attempt %d/%d, session %s)", attempt, maxAttempts, te.currentSessionID)
		return true
	}

	te.incrementTurnRetries(errDetail)
	te.stopIndicators()

	if te.pool.ctx.Err() != nil {
		log.Printf("[WorkerPool] Pool shutting down on watchdog timeout for thread %s. Resetting to PENDING.", te.threadID)
		metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
		te.cancelBurstOnShutdown("timeout", false)
		return false
	}

	te.rotateSessionID(te.threadID, "")
	metrics.RecordTurnCompleted("watchdog_timeout", te.triggerType, currentModel, time.Since(te.execStart))

	te.deliverFailureNotice(currentAgyBin, currentAPIKey, errDetail, stderr, "execution timed out while processing the request", "watchdog notice")
	te.failBurstWithStatus("timeout", errDetail, currentModel)
	log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED after exhausting %d attempts on watchdog timeout: %s", len(te.burst), te.threadID, maxAttempts, errDetail)
	return false
}

func (te *turnExecution) processSuccessfulAttempt(resp *runner.AgyResponse, exitCode int, stdout, stderr, currentModel string, attempt, maxAttempts int, autoResumeCount *int, accumulatedUsage *runner.TokenUsage, promptToSend *string) (done, yieldRetry bool, errDetail string) {
	if resp == nil {
		return false, false, ""
	}
	extSess := resp.ConversationID
	if extSess == "" {
		extSess = runner.ExtractSessionID(stdout+"\n"+stderr, te.execStart)
	}
	if te.currentSessionID == "" && (extSess == "" || !runner.IsValidUUID(extSess)) {
		errDetail = "failed to latch active session UUID on cold start"
		log.Printf("[Queue] Defensive Failure: %s for thread %s", errDetail, te.threadID)
		return false, false, errDetail
	}

	te.resolveAndPersistSessionID(extSess, stdout, stderr, true)
	if te.handleYieldTrap(resp, exitCode, stdout, stderr, autoResumeCount, accumulatedUsage, promptToSend, currentModel) {
		return false, true, ""
	}

	var usage runner.TokenUsage
	if accumulatedUsage != nil {
		usage = *accumulatedUsage
	}
	if ok, emptyErr := te.deliverSuccessAndFinalize(resp, usage, currentModel, attempt, maxAttempts); ok {
		return true, false, ""
	} else {
		return false, false, emptyErr
	}
}

func (te *turnExecution) handleSessionCorruptionRetry(attempt, maxAttempts int, currentAgyBin, currentAPIKey, errDetail, stderr, currentModel string) (shutdown bool) {
	te.deliverFailureNotice(currentAgyBin, currentAPIKey, errDetail, stderr, "session reset due to context corruption", "session reset notice")
	te.rotateSessionID(te.threadID, "")
	te.activeSessionMu.Lock()
	te.currentSessionID = ""
	te.activeSessionID = ""
	te.leaseSessionID = ""
	oldLease := te.lease
	te.lease = nil
	te.activeSessionMu.Unlock()
	if oldLease != nil {
		if relErr := oldLease.Release(); relErr != nil {
			log.Printf("[WorkerPool] Warning releasing lease on corruption reset for %s: %v", te.threadID, relErr)
		}
	}
	activePool := te.activePool
	if activePool == nil {
		if resolved, _, resolveErr := te.resolveExecutionPool(); resolveErr == nil {
			activePool = resolved
			te.activeSessionMu.Lock()
			te.activePool = activePool
			te.activeSessionMu.Unlock()
		}
	}
	if activePool != nil {
		// Acquire replacement lease using te.leaseKey() so Discord threads retain "discord-<id>" pool scoping.
		if newLease, lErr := activePool.AcquireLease(te.pool.ctx, te.leaseKey()); lErr == nil {
			te.activeSessionMu.Lock()
			te.lease, te.leaseSessionID, te.isEphemeral, te.currentSessionID = newLease, newLease.SessionID(), te.isEphemeral || newLease.IsEphemeral(), newLease.SessionID()
			te.activeSessionMu.Unlock()
		}
	}
	return te.handleRetryBackoff(attempt, maxAttempts, errDetail, currentModel)
}

func (te *turnExecution) handleNonTransientFailure(attempt, maxAttempts int, currentAgyBin, currentAPIKey, errDetail, stderr, currentModel string) {
	te.stopIndicators()
	if te.pool.ctx.Err() != nil {
		log.Printf("[WorkerPool] Pool shutting down during non-transient error for thread %s. Resetting to PENDING.", te.threadID)
		te.cancelBurstOnShutdown("failed", false)
		return
	}

	te.activeSessionMu.Lock()
	hasCurr := te.currentSessionID != ""
	if hasCurr {
		te.previousSessionID, te.currentSessionID = te.currentSessionID, ""
	}
	te.activeSessionID = ""
	te.leaseSessionID = ""
	te.activeSessionMu.Unlock()
	if hasCurr {
		te.rotateSessionID(te.threadID, "")
	}
	metrics.RecordTurnCompleted("failed", te.triggerType, currentModel, time.Since(te.execStart))
	te.deliverFailureNotice(currentAgyBin, currentAPIKey, errDetail, stderr, "execution failed with non-transient error", "non-transient failure notice")
	te.incrementTurnRetries(errDetail)
	te.failBurstWithStatus("failed", errDetail, currentModel)
	log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED due to non-transient error (attempt %d/%d): %s", len(te.burst), te.threadID, attempt, maxAttempts, errDetail)
}

func (te *turnExecution) handleAttemptsExhausted(maxAttempts int, currentAgyBin, currentAPIKey, lastErrDetail, lastStderr, currentModel string) {
	te.stopIndicators()

	if te.pool.ctx.Err() != nil {
		log.Printf("[WorkerPool] Pool shutting down during turn for thread %s. Suppressing exhaustion alert and resetting to PENDING for deployment recovery.", te.threadID)
		metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
		te.cancelBurstOnShutdown("failed", true)
		return
	}
	te.deliverFailureNotice(currentAgyBin, currentAPIKey, lastErrDetail, lastStderr, fmt.Sprintf("execution failed after exhausting %d attempts", maxAttempts), "exhaustion notice")
	metrics.RecordTurnCompleted("failed", te.triggerType, currentModel, time.Since(te.execStart))
	te.failBurstWithStatus("failed", lastErrDetail, currentModel)
	log.Printf("[WorkerPool] %d message(s) in thread %s marked FAILED after exhausting all %d attempts", len(te.burst), te.threadID, maxAttempts)
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

func (te *turnExecution) handleTransientRetry(attempt, maxAttempts int, errDetail, currentModel string) (shutdown bool) {
	if te.lease != nil {
		te.lease.MarkDirty()
	}
	if shutdown := te.handleRetryBackoff(attempt, maxAttempts, errDetail, currentModel); shutdown {
		return true
	}
	log.Printf("[WorkerPool] Retrying transient error in %v (preserving session %s)", time.Duration(attempt)*te.pool.cfg.BackoffBase, te.currentSessionID)
	return false
}

func (te *turnExecution) dispatchFailedAttempt(attempt, maxAttempts int, isTransient, isSessionCorruption bool, currentAgyBin, currentAPIKey, errDetail, stderr, currentModel string) bool {
	if te.pool.ctx.Err() != nil {
		log.Printf("[WorkerPool] Turn execution cancelled due to pool shutdown (thread: %s, attempt: %d/%d). Resetting to PENDING for clean deployment recovery.", te.threadID, attempt, maxAttempts)
		te.stopIndicators()
		metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
		te.cancelBurstOnShutdown("failed", false)
		return false
	}

	log.Printf("[WorkerPool] Burst for thread %s failed on attempt %d/%d (transient=%t, corrupt=%t): %s",
		te.threadID, attempt, maxAttempts, isTransient, isSessionCorruption, errDetail)

	if isSessionCorruption {
		return !te.handleSessionCorruptionRetry(attempt, maxAttempts, currentAgyBin, currentAPIKey, errDetail, stderr, currentModel)
	}
	if isTransient {
		return !te.handleTransientRetry(attempt, maxAttempts, errDetail, currentModel)
	}
	te.handleNonTransientFailure(attempt, maxAttempts, currentAgyBin, currentAPIKey, errDetail, stderr, currentModel)
	return false
}

type attemptFailureAction int

const (
	actionStop attemptFailureAction = iota
	actionContinue
	actionProceedToDispatch
)

func (te *turnExecution) handleAttemptFailureCheck(errDetail, stderr, currentModel, burstEffort string, isTransient, isSessionCorruption bool, attempt, maxAttempts int, currentAgyBin, currentAPIKey string) attemptFailureAction {
	if runner.IsQuotaPause(errDetail, stderr) {
		if te.handleQuotaPause(errDetail, stderr, currentModel, burstEffort, attempt, maxAttempts) {
			return actionContinue
		}
		return actionStop
	}

	isWatchdog := isWatchdogTimeoutError(errDetail, stderr)
	errCat := classifyRunnerErrorCategory(isWatchdog, isTransient, isSessionCorruption)
	metrics.RecordRunnerError(errCat, currentModel)

	if isWatchdog {
		if te.handleWatchdogFailure(attempt, maxAttempts, currentAgyBin, currentAPIKey, errDetail, stderr, currentModel) {
			return actionContinue
		}
		return actionStop
	}
	return actionProceedToDispatch
}

type attemptStepResult struct {
	shouldReturn     bool
	shouldContinue   bool
	decrementAttempt bool
	lastErrDetail    string
}

func (te *turnExecution) processAttemptResult(
	isFailure bool,
	errDetail, stderr, currentModel, burstEffort string,
	isTransient, isSessionCorruption bool,
	attempt, maxAttempts int,
	currentAgyBin, currentAPIKey string,
	resp *runner.AgyResponse,
	stdout string,
	autoResumeCount *int,
	accumulatedUsage *runner.TokenUsage,
	promptToSend *string,
) attemptStepResult {
	if isFailure {
		action := te.handleAttemptFailureCheck(errDetail, stderr, currentModel, burstEffort, isTransient, isSessionCorruption, attempt, maxAttempts, currentAgyBin, currentAPIKey)
		return attemptStepResult{
			shouldReturn:   action == actionStop,
			shouldContinue: action == actionContinue,
		}
	}

	done, yieldRetry, stepErr := te.processSuccessfulAttempt(resp, 0, stdout, stderr, currentModel, attempt, maxAttempts, autoResumeCount, accumulatedUsage, promptToSend)
	if done {
		return attemptStepResult{shouldReturn: true}
	}
	if yieldRetry {
		return attemptStepResult{shouldContinue: true, decrementAttempt: true}
	}
	return attemptStepResult{lastErrDetail: stepErr}
}

func (te *turnExecution) executeWithRetries() {
	maxAttempts := te.pool.cfg.MaxAttempts
	lastErrDetail := ""
	lastStderr := ""
	currentModel := te.pool.GetRuntimeConfig()

	initialRetryCount := te.initialRetryCount()

	attemptCfg := te.resolveAttemptConfig(0)
	currentAgyBin := attemptCfg.agyBin
	currentAPIKey := attemptCfg.apiKey

	var autoResumeCount int
	var accumulatedUsage runner.TokenUsage
	promptToSend := te.turnPrompt

	for attempt := initialRetryCount + 1; attempt <= maxAttempts; attempt++ {
		if te.isInterrupted() {
			te.finalizeInterruptedTurn(currentModel)
			return
		}
		cfg := te.resolveAttemptConfig(attempt)
		currentModel = cfg.model
		currentTimeout := cfg.timeoutMinutes
		currentAgyBin = cfg.agyBin
		currentAPIKey = cfg.apiKey
		burstEffort, _ := te.turnEffortAndModel()

		if te.checkGlobalQuotaLockout(currentAPIKey, currentModel) {
			return
		}

		promptToSend = te.prepareAttemptPrompt(attempt, promptToSend, lastErrDetail, lastStderr)

		runStart := time.Now()
		outcome, turnRes, leaseStderr, err := te.executeLeaseAttempt(currentModel, promptToSend, currentTimeout)
		runDur := time.Since(runStart)

		if te.isInterrupted() {
			metrics.RecordRunnerExecution("error", currentModel, te.triggerType, runDur)
			log.Printf("[WorkerPool] Turn execution interrupted by operator (thread: %s, attempt: %d/%d). Aborting retries.", te.threadID, attempt, maxAttempts)
			te.finalizeInterruptedTurn(currentModel)
			return
		}

		st := te.unpackAttemptOutcome(outcome, turnRes, leaseStderr, err)
		isFailure, isTransient, isSessionCorruption := st.isFailure, st.isTransient, st.isSessionCorruption
		errDetail, stdout, resp, stderr := st.errDetail, st.stdout, st.resp, st.stderr
		lastErrDetail, lastStderr = "", ""
		runStatus := "success"
		if isFailure {
			promptToSend = te.turnPrompt
			errDetail, stderr = te.enrichFailureFromTranscript(outcome, stdout, stderr, isSessionCorruption)
			lastErrDetail, lastStderr = errDetail, stderr
			runStatus = "error"
		}
		metrics.RecordRunnerExecution(runStatus, currentModel, te.triggerType, runDur)

		stepRes := te.processAttemptResult(
			isFailure, errDetail, stderr, currentModel, burstEffort,
			isTransient, isSessionCorruption, attempt, maxAttempts,
			currentAgyBin, currentAPIKey, resp, stdout,
			&autoResumeCount, &accumulatedUsage, &promptToSend,
		)
		if stepRes.shouldReturn {
			return
		}
		if stepRes.shouldContinue {
			if stepRes.decrementAttempt {
				attempt--
			}
			continue
		}
		if stepRes.lastErrDetail != "" {
			errDetail = stepRes.lastErrDetail
			lastErrDetail = stepRes.lastErrDetail
		}

		if !te.dispatchFailedAttempt(attempt, maxAttempts, isTransient, isSessionCorruption, currentAgyBin, currentAPIKey, errDetail, stderr, currentModel) {
			return
		}
	}

	te.handleAttemptsExhausted(maxAttempts, currentAgyBin, currentAPIKey, lastErrDetail, lastStderr, currentModel)
}
