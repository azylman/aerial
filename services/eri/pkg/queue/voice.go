package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

func (p *WorkerPool) checkVoiceTurnReady(ctx context.Context, sessionID string) (string, error) {
	if p == nil {
		return "", fmt.Errorf("worker pool is uninitialized")
	}
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return "", fmt.Errorf("brain is shutting down")
	}

	select {
	case <-p.ctx.Done():
		return "", fmt.Errorf("brain is shutting down")
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	deviceKey := strings.TrimSpace(sessionID)
	if deviceKey == "" {
		return "", fmt.Errorf("queue: session_id is required for voice turn routing")
	}
	return deviceKey, nil
}

func (p *WorkerPool) executeCustomVoiceRunner(ctx context.Context, turnPrompt, deviceKey string, onStatus func(string), sentenceCb func(string)) (string, string, bool, error) {
	hasCustomStream := p.cfg.VoiceStreamRunnerFunc != nil && sentenceCb != nil
	if !hasCustomStream && p.cfg.VoiceRunnerFunc == nil {
		return "", "", false, nil
	}
	start := time.Now()
	var reply, conv string
	var err error
	if hasCustomStream {
		reply, conv, err = p.cfg.VoiceStreamRunnerFunc(ctx, turnPrompt, deviceKey, onStatus, sentenceCb)
	} else {
		reply, conv, err = p.cfg.VoiceRunnerFunc(ctx, turnPrompt, deviceKey, onStatus)
	}
	runStatus := "success"
	if err != nil {
		runStatus = "error"
	}
	metrics.RecordRunnerExecution(runStatus, p.LowEffortModel(), "voice", time.Since(start))
	return reply, conv, true, err
}

func (p *WorkerPool) resolveVoiceAgentPool() runner.AgentPool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.voiceProcessPool != nil {
		return p.voiceProcessPool
	}
	if p.pools != nil {
		return p.pools[PoolKey{Ephemeral: false, Effort: "high"}]
	}
	return nil
}

func finalizeVoiceTurnOutcome(ctx context.Context, outcome runner.TurnOutcome, turnErr error, activeSession, model string, start time.Time, sink *runner.BufferingTurnSink, detector *SentenceDetector, sentenceCb func(string)) (string, string, error) {
	if outcome.IsSuccess {
		metrics.RecordRunnerExecution("success", model, "voice", time.Since(start))
		if outcome.Usage.TotalTokens > 0 {
			metrics.RecordTokens(model, "voice", outcome.Usage.InputTokens, outcome.Usage.OutputTokens, outcome.Usage.ThinkingTokens, outcome.Usage.CacheReadTokens, outcome.Usage.TotalTokens)
		}
		if !sink.EmittedAny() && sentenceCb != nil && detector != nil && outcome.Response != "" {
			emitSentence := func(s string) {
				sink.MarkEmitted()
				if sentenceCb != nil {
					sentenceCb(s)
				}
			}
			detector.Feed(outcome.Response, emitSentence)
			detector.Flush(emitSentence)
		}
		return outcome.Response, outcome.SessionID, nil
	}

	metrics.RecordRunnerExecution("error", model, "voice", time.Since(start))
	if turnErr != nil {
		return "", activeSession, turnErr
	}
	if ctx.Err() != nil {
		return "", activeSession, ctx.Err()
	}
	return "", activeSession, fmt.Errorf("%s", outcome.ErrorDetail)
}

// ExecuteVoiceTurn synchronously executes a turn for voice queries.
func (p *WorkerPool) ExecuteVoiceTurn(ctx context.Context, prompt, sessionID string, onStatus func(status string), onSentence ...func(sentence string)) (string, string, error) {
	deviceKey, err := p.checkVoiceTurnReady(ctx, sessionID)
	if err != nil {
		return "", "", err
	}
	threadID := "voice-" + deviceKey

	lockVal, _ := p.scopeLocks.LoadOrStore(threadID, &sync.Mutex{})
	scopeLock, ok := lockVal.(*sync.Mutex)
	if !ok {
		scopeLock = &sync.Mutex{}
		p.scopeLocks.Store(threadID, scopeLock)
	}
	scopeLock.Lock()
	defer scopeLock.Unlock()

	var sentenceCb func(string)
	if len(onSentence) > 0 {
		sentenceCb = onSentence[0]
	}

	turnPrompt := prompt
	if reply, conv, handled, customErr := p.executeCustomVoiceRunner(ctx, turnPrompt, deviceKey, onStatus, sentenceCb); handled {
		return reply, conv, customErr
	}

	model := p.LowEffortModel()
	start := time.Now()
	// The invoke_agent span nests under the caller's (the /voice/ask server span) so a voice
	// turn gets the same invoke_agent -> chat/execute_tool -> mcp tree as a Discord turn.
	spanCtx, span := startTurnSpan(ctx, p.tracer(), turnSpanSpec{
		conversationID: deviceKey, agentName: p.cfg.AgentName, triggerType: voiceTriggerType, pickup: start,
	})
	turnStatus, turnUsage := "failed", runner.TokenUsage{}
	defer func() { finishTurnSpan(span, model, turnUsage, turnStatus) }()

	procPool := p.resolveVoiceAgentPool()
	if procPool == nil {
		return "", deviceKey, fmt.Errorf("voice daemon pool unavailable")
	}

	nodeInstructions := config.LoadScopeInstructions(deviceKey)

	var detector *SentenceDetector
	if sentenceCb != nil {
		detector = NewSentenceDetector()
	}
	sink := newVoiceTurnSink(ctx, onStatus, sentenceCb, detector)
	traceSink := runner.NewTracingTurnSink(spanCtx, sink)

	turnCtx := &runner.TurnContext{
		TurnID: uuid.New().String(), SessionID: deviceKey, Model: model,
		Prompt: turnPrompt, ScopeInstructions: nodeInstructions, Sink: traceSink, CreatedAt: time.Now(), Ctx: spanCtx,
	}

	lease, lErr := procPool.AcquireLease(spanCtx, deviceKey)
	if lErr != nil {
		metrics.RecordRunnerExecution("error", model, "voice", time.Since(start))
		return "", deviceKey, fmt.Errorf("failed to acquire voice daemon: %w", lErr)
	}
	defer func() {
		if relErr := lease.Release(); relErr != nil {
			log.Printf("[WorkerPool] Warning releasing voice lease for %s: %v", deviceKey, relErr)
		}
	}()
	// Registered after the release defer so the daemon's entry is cleared before the lease
	// (and the daemon) can be handed to the next turn.
	defer p.markVoiceTurnActive(lease, span.SpanContext(), traceSink)()
	activeSession := lease.SessionID()
	if activeSession == "" {
		activeSession = deviceKey
	}
	turnCtx.SessionID = activeSession
	turnRes, turnErr := lease.Execute(spanCtx, turnCtx)
	if turnErr != nil && !errors.Is(turnErr, context.Canceled) && !errors.Is(turnErr, context.DeadlineExceeded) {
		turnErr = fmt.Errorf("failed sending turn to voice daemon: %w", turnErr)
	}

	outcome := runner.NewTurnResolver().Resolve(ctx, turnRes, turnErr, activeSession)
	turnStatus, turnUsage = voiceTurnStatus(ctx, outcome), outcome.Usage
	return finalizeVoiceTurnOutcome(ctx, outcome, turnErr, activeSession, model, start, sink, detector, sentenceCb)
}

// voiceTriggerType is the aerial.turn.trigger of a voice turn's invoke_agent span.
const voiceTriggerType = "voice"

// voiceTurnStatus maps a voice turn's outcome onto the aerial.turn.status values Discord turns
// record: success, cancelled when the caller went away, failed otherwise.
func voiceTurnStatus(ctx context.Context, outcome runner.TurnOutcome) string {
	switch {
	case outcome.IsSuccess:
		return "success"
	case ctx.Err() != nil:
		return "cancelled"
	default:
		return "failed"
	}
}

// markVoiceTurnActive registers the voice lease's daemon under the turn span and attaches the
// turn's tool spans, so the loopback MCP proxy nests the daemon's MCP calls under them. It
// returns the func that clears the registration; leases without a single daemon are skipped.
func (p *WorkerPool) markVoiceTurnActive(lease runner.SessionLease, sc trace.SpanContext, tools runner.ToolSpanSource) func() {
	owner, ok := lease.(runner.ActiveTurnOwner)
	if !ok {
		return func() {}
	}
	turn := owner.ActiveTurn()
	registry := p.ActiveTurns()
	registry.Set(turn, sc)
	registry.SetToolSpans(turn.DaemonID, tools)
	return func() { registry.Clear(turn.DaemonID) }
}

func newVoiceTurnSink(ctx context.Context, onStatus func(string), onSentence func(string), detector *SentenceDetector) *runner.BufferingTurnSink {
	var sink *runner.BufferingTurnSink
	emitSentence := func(sentence string) {
		if sink != nil {
			sink.MarkEmitted()
		}
		if onSentence != nil {
			onSentence(sentence)
		}
	}
	sink = runner.NewBufferingTurnSink(runner.BufferingTurnSinkConfig{
		OnToolCall: func(toolName, commandName string) {
			if onStatus != nil && (toolName != "" || commandName != "") {
				onStatus(FormatToolStatus(toolName, commandName, 0))
			}
		},
		OnToolCompleted: func(toolName, mcpServer string, duration time.Duration, status string) {
			metrics.RecordToolExecution(toolName, mcpServer, status, duration)
		},
		OnSkillActivated: func(skillName, source string) { metrics.RecordSkillActivation(skillName, source) },
		OnTextDelta: func(delta string) {
			if (ctx == nil || ctx.Err() == nil) && onSentence != nil && detector != nil {
				detector.Feed(delta, emitSentence)
			}
		},
		OnComplete: func(res *runner.TurnResult) {
			if onSentence != nil && detector != nil {
				detector.Flush(emitSentence)
				if !sink.EmittedAny() && res != nil && strings.TrimSpace(res.Response) != "" {
					detector.Feed(res.Response, emitSentence)
					detector.Flush(emitSentence)
				}
			}
		},
	})
	return sink
}
