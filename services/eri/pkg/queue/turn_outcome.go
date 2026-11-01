package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/notifier"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/google/uuid"
)

type oneShotScheduleRequest struct {
	ID       string    `json:"id"`
	ThreadID string    `json:"thread_id"`
	Prompt   string    `json:"prompt"`
	Effort   string    `json:"effort,omitempty"`
	RunAt    time.Time `json:"run_at"`
}

func (te *turnExecution) createOneShotSchedule(s oneShotScheduleRequest) error {
	if te == nil || te.pool == nil {
		return errors.New("turnExecution or pool is nil")
	}

	haroURL := strings.TrimSpace(te.pool.cfg.HaroURL)
	if haroURL == "" {
		haroURL = strings.TrimSpace(os.Getenv("HARO_URL"))
	}
	if haroURL == "" {
		haroURL = "http://haro.aerial"
	}
	haroURL = strings.TrimRight(haroURL, "/")

	bodyBytes, err := json.Marshal(s)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, haroURL+"/schedules/oneshot", bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("haro returned status code %d", resp.StatusCode)
	}
	return nil
}

var rateLimitKeywords = []string{
	"503", "high demand", "rate limit", "resource_exhausted", "429", "quota",
	"too many requests", "overloaded", "service unavailable", "resource has been exhausted",
	"individual quota reached", "please upgrade your subscription",
}

func isRateLimitError(errDetail string, extraStrs ...string) bool {
	var sb strings.Builder
	sb.WriteString(strings.ToLower(errDetail))
	for _, s := range extraStrs {
		sb.WriteByte(' ')
		sb.WriteString(strings.ToLower(s))
	}
	combined := sb.String()
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

func (te *turnExecution) handleRetryBackoff(attempt, maxAttempts int, errDetail, currentModel string) (shutdown bool) {
	te.incrementTurnRetries(errDetail)
	if attempt >= maxAttempts {
		return false
	}
	backoff := time.Duration(attempt) * te.pool.cfg.BackoffBase
	if te.statusUpdater != nil {
		if isRateLimitError(errDetail) {
			te.statusUpdater.SetCapacityBackoff(backoff)
		} else {
			te.statusUpdater.Reset()
		}
	}
	select {
	case <-time.After(backoff):
		return false
	case <-te.turnDone():
		if te.isInterrupted() {
			te.finalizeInterruptedTurn(currentModel)
			return true
		}
		metrics.RecordTurnCompleted("cancelled", te.triggerType, currentModel, time.Since(te.execStart))
		te.cancelBurstOnShutdown("failed", true)
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
		if transcriptErr, tErr := te.pool.sessionMgr.ExtractLastTurnError(te.poolCtx(), targetSess, te.execStart); tErr == nil && transcriptErr != "" {
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

	isSilent := te.isDispatch()
	if !isSilent && strings.TrimSpace(cleanText) == "" && len(attachments) == 0 {
		log.Printf("[Queue] Agent produced empty response for thread %s on attempt %d/%d; treating as failure", te.threadID, attempt, maxAttempts)
		return false, "agent produced empty response"
	}

	if !isSilent {
		te.deliverDiscordSuccess(cleanText, attachments)
	}

	resp.Usage.InputTokens += accumulatedUsage.InputTokens
	resp.Usage.OutputTokens += accumulatedUsage.OutputTokens
	resp.Usage.ThinkingTokens += accumulatedUsage.ThinkingTokens
	resp.Usage.CacheReadTokens += accumulatedUsage.CacheReadTokens
	resp.Usage.TotalTokens += accumulatedUsage.TotalTokens

	metrics.RecordTurnCompleted("success", te.triggerType, currentModel, time.Since(te.execStart))
	tokenChannel := te.resolveTokenChannel()
	metrics.RecordTokens(currentModel, tokenChannel, resp.Usage.InputTokens, resp.Usage.OutputTokens, resp.Usage.ThinkingTokens, resp.Usage.CacheReadTokens, resp.Usage.TotalTokens)
	te.turnStatus, te.turnResponseText, te.turnTokenUsage, te.turnDurationMs = "success", cleanText, resp.Usage, time.Since(te.execStart).Milliseconds()
	te.finalizeBurstMessages(te.burst, db.StatusCompleted, cleanText, "completed", "", currentModel, true, true)

	te.handleDispatchSessionRotation()

	if te.isDispatch() {
		log.Printf("[WorkerPool] Dispatch %s (session: %s) completed successfully on attempt %d/%d", te.dispatch.ID, te.threadID, attempt, maxAttempts)
	} else {
		log.Printf("[WorkerPool] %d message(s) in thread %s completed successfully on attempt %d/%d", len(te.burst), te.threadID, attempt, maxAttempts)
	}
	return true, ""
}

func (te *turnExecution) deliverDiscordSuccess(cleanText string, attachments []*delivery.Attachment) {
	deliveryTarget := te.threadID
	var deliveryErr error
	if te.pool.cfg.DeliveryWithAttachmentsFunc != nil {
		deliveryErr = te.pool.cfg.DeliveryWithAttachmentsFunc(te.pool.getDiscordSession(), deliveryTarget, cleanText, attachments)
	} else if te.pool.cfg.DeliveryFunc != nil {
		deliveryErr = te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), deliveryTarget, cleanText)
	}
	if deliveryErr != nil {
		log.Printf("[WorkerPool] Failed to deliver response for thread %s: %v", te.threadID, deliveryErr)
	}
}

func (te *turnExecution) resolveTokenChannel() string {
	tokenChannel := te.effectiveName
	if tokenChannel == "" && len(te.burst) > 0 {
		tokenChannel = te.burst[0].ChannelName
	}
	if tokenChannel == "" {
		if te.triggerType == "http" || strings.HasPrefix(te.triggerType, "dispatch:") {
			tokenChannel = te.triggerType
		} else if !IsNumericSnowflake(te.threadID) && te.threadID != "" {
			tokenChannel = te.threadID
		}
	}
	return tokenChannel
}

func (te *turnExecution) handleDispatchSessionRotation() {
	if !te.isDispatch() || te.isEphemeral {
		return
	}
	newTurns, err := te.incrementSessionTurnCount(te.threadID)
	if err == nil && newTurns >= session.DefaultMaxSessionTurns {
		log.Printf("[WorkerPool] Dispatch session %s reached %d turns >= max %d. Rotating session.", te.threadID, newTurns, session.DefaultMaxSessionTurns)
		te.rotateSessionID(te.threadID, "")
	}
}

func (te *turnExecution) deliverQuotaNotice(pauseMsg string, logPrefix string) {
	if te.isDispatch() || te.pool == nil || te.pool.cfg.DeliveryFunc == nil {
		return
	}
	deliveryTarget := te.threadID
	if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), deliveryTarget, pauseMsg); err != nil {
		log.Printf("[WorkerPool] Failed to deliver %s for thread %s: %v", logPrefix, te.threadID, err)
	}
}

func (te *turnExecution) isQuotaRetry() bool {
	if len(te.burst) > 0 {
		return strings.HasPrefix(te.burst[0].Content, "[QUOTA_RETRY]") || strings.Contains(te.turnPrompt, "[QUOTA_RETRY]")
	}
	if te.isDispatch() {
		return strings.Contains(te.turnPrompt, "[QUOTA_RETRY]") || strings.Contains(te.dispatch.Prompt, "[QUOTA_RETRY]")
	}
	return strings.Contains(te.turnPrompt, "[QUOTA_RETRY]")
}

func (te *turnExecution) retryCapacityBlip(errDetail string, resetDur time.Duration, currentModel string, attempt int) bool {
	te.incrementTurnRetries(errDetail)
	delay := calculateCapacityBackoff(attempt, resetDur)
	if te.pool != nil && te.pool.cfg.RetryDelayOverride > 0 {
		delay = te.pool.cfg.RetryDelayOverride
	}
	if te.statusUpdater != nil {
		te.statusUpdater.SetCapacityBackoff(delay)
	}
	cancelChan := te.turnDone()
	select {
	case <-time.After(delay):
		return true
	case <-cancelChan:
		if te.isInterrupted() {
			te.finalizeInterruptedTurn(currentModel)
			return false
		}
		log.Printf("[WorkerPool] Context cancelled during capacity blip backoff for thread %s", te.threadID)
		te.stopIndicators()
		te.cancelBurstOnShutdown("pending", false)
		return false
	}
}

func (te *turnExecution) handleCapacityBlip(errDetail string, resetDur time.Duration, currentModel string, attempt, maxAttempts int) bool {
	log.Printf("[WorkerPool] Capacity blip detected for thread %s on attempt %d/%d (reset=%v): %s.", te.threadID, attempt, maxAttempts, resetDur, errDetail)
	if attempt < maxAttempts {
		return te.retryCapacityBlip(errDetail, resetDur, currentModel, attempt)
	}
	log.Printf("[WorkerPool] Capacity blip retry budget exhausted for thread %s after %d attempts", te.threadID, maxAttempts)
	te.stopIndicators()
	reason := fmt.Sprintf("[CAPACITY_EXHAUSTED reset_in=%v] %s", resetDur, sanitizeErrorText(errDetail))
	metrics.RecordRunnerError("capacity_exhausted", currentModel)
	metrics.RecordTurnCompleted("capacity_exhausted", te.triggerType, currentModel, time.Since(te.execStart))
	pauseMsg := notifier.FormatQuotaPauseMessage(resetDur, time.Now().UTC().Add(resetDur), false, false)
	te.deliverQuotaNotice(pauseMsg, "capacity pause notice")
	te.failBurstWithStatus("failed", reason, currentModel)
	return false
}

func (te *turnExecution) handleQuotaPause(errDetail, stderr, currentModel, burstEffort string, attempt, maxAttempts int) bool {
	resetDur, _ := runner.ExtractQuotaResetDuration(errDetail, stderr)
	if runner.IsCapacityBlip(errDetail, stderr) || resetDur <= 5*time.Second {
		return te.handleCapacityBlip(errDetail, resetDur, currentModel, attempt, maxAttempts)
	}
	te.isQuotaPaused = true
	if te.stopTyping != nil {
		te.stopTyping()
	}
	log.Printf("[WorkerPool] Quota pause detected for thread %s on attempt %d/%d: %s", te.threadID, attempt, maxAttempts, errDetail)
	isAlreadyRetry := te.isQuotaRetry()
	jitterSec := 5 + rand.Intn(16)
	runAt := time.Now().UTC().Add(resetDur).Add(30 * time.Second).Add(time.Duration(jitterSec) * time.Second)
	if te.pool != nil {
		te.pool.quotaLockedUntil.Store(runAt.Unix())
	}
	var scheduled bool
	if !isAlreadyRetry && !te.isDispatch() {
		oneShotID := uuid.New().String()
		oneShot := oneShotScheduleRequest{
			ID:       oneShotID,
			ThreadID: te.threadID,
			Prompt:   "[QUOTA_RETRY] " + te.turnPrompt,
			Effort:   burstEffort,
			RunAt:    runAt,
		}
		if err := te.createOneShotSchedule(oneShot); err == nil {
			scheduled = true
			log.Printf("[WorkerPool] Scheduled one-shot retry %s for thread %s", oneShotID, te.threadID)
		}
	}
	metrics.RecordTurnCompleted("quota_paused", te.triggerType, currentModel, time.Since(te.execStart))
	te.stopIndicators()
	pauseMsg := notifier.FormatQuotaPauseMessage(resetDur, runAt, scheduled, isAlreadyRetry)
	te.deliverQuotaNotice(pauseMsg, "quota pause notice")
	reason := fmt.Sprintf("[QUOTA_PAUSED reset_in=%v scheduled=%t] %s", resetDur, scheduled, sanitizeErrorText(errDetail))
	te.failBurstWithStatus("failed", reason, currentModel)
	return false
}

func (te *turnExecution) handleYieldTrap(resp *runner.AgyResponse, exitCode int, stdout, stderr string, autoResumeCount *int, accumulatedUsage *runner.TokenUsage, promptToSend *string, currentModel string) bool {
	isYield, taskCount := runner.IsYieldTrap(exitCode, stdout, stderr)
	if !isYield {
		return false
	}
	detectionSource := "stderr_signature"
	if *autoResumeCount >= MaxYieldTrapAutoResumes {
		log.Printf("[YieldTrap] Circuit breaker tripped for thread %s in session %s after %d auto-resumptions (source=%s). Delivering final output.",
			te.threadID, te.currentSessionID, *autoResumeCount, detectionSource)
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

	if *promptToSend != YieldTrapResumePrompt && !strings.Contains(*promptToSend, "<SYSTEM_MESSAGE>") {
		*promptToSend = YieldTrapResumePrompt
	}
	log.Printf("[YieldTrap] Intercepted background command exit for thread %s in session %s (auto-resume %d/%d, source=%s, taskCount=%d): suppressing intermediate waiting output and auto-resuming turn.",
		te.threadID, te.currentSessionID, *autoResumeCount, MaxYieldTrapAutoResumes, detectionSource, taskCount)
	metrics.RecordYieldTrap("resumed", detectionSource, currentModel)
	metrics.RecordRunnerError("yield_trap_intercepted", currentModel)
	return true
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
					ToolDetail: commandName,
					ToolInfo: &runner.StepToolInfo{
						Detail:     commandName,
						Parameters: runner.StepToolParameters{CommandLine: commandName},
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
	delay := max(resetDur, minFloor)
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

func (te *turnExecution) cancelBurstOnShutdown(turnStatus string, failScheduleRun bool) {
	te.turnStatus = turnStatus
	te.turnError = "interrupted by shutdown"
	if failScheduleRun {
		te.turnError = "context cancelled during execution"
	}
	te.turnDurationMs = time.Since(te.execStart).Milliseconds()
}

func buildFailureNotice(cfg WorkerPoolConfig, agyBin, apiKey, errDetail, stderr, descPrefix string) string {
	if isRateLimitError(errDetail, stderr) {
		return notifier.ModelUnavailableMessage()
	}
	snippet := sanitizeErrorText(errDetail)
	if len([]rune(snippet)) > 300 {
		snippet = string([]rune(snippet)[:300])
	}
	desc := descPrefix
	switch {
	case desc == "":
		desc = snippet
	case strings.HasSuffix(desc, ": ") || strings.HasSuffix(desc, " "):
		desc += snippet
	default:
		desc += ": " + snippet
	}
	return cfg.NotifierFunc(agyBin, apiKey, desc)
}

func (te *turnExecution) deliverFailureNotice(agyBin, apiKey, errDetail, stderr, descPrefix, logLabel string) {
	if te.isDispatch() || te.pool == nil || te.pool.cfg.DeliveryFunc == nil {
		return
	}
	notif := buildFailureNotice(te.pool.cfg, agyBin, apiKey, errDetail, stderr, descPrefix)
	deliveryTarget := te.threadID
	if err := te.pool.cfg.DeliveryFunc(te.pool.getDiscordSession(), deliveryTarget, notif); err != nil {
		log.Printf("[WorkerPool] Failed to deliver %s for thread %s: %v", logLabel, te.threadID, err)
	}
}
