package runner

import (
	"context"
	"log"
	"strings"
	"time"
)

// SetTerminalTurnRescuer updates the terminal turn rescuer callback on the daemon.
func (d *StreamingDaemon) SetTerminalTurnRescuer(r func(context.Context, string, time.Time) string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cfg.TerminalTurnRescuer = r
}

//nolint:contextcheck,nolintlint // Watchdog operates under the daemon lifetime, while turn rescue operates under TurnContext.Ctx.
func (d *StreamingDaemon) watchdogLoop(lifetime context.Context) {
	defer d.watchdogWg.Done()

	interval := d.cfg.WatchdogInterval
	if interval <= 0 {
		interval = DefaultWatchdogInterval
	}
	idleTimeout := d.cfg.WatchdogIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = DefaultWatchdogIdleTimeout
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-d.closeCh:
			return
		case <-lifetime.Done():
			return
		case <-ticker.C:
			d.checkWatchdog(idleTimeout)
			if d.closed.Load() {
				return
			}
		}
	}
}

//nolint:contextcheck,nolintlint // Watchdog turn rescue operates under TurnContext.Ctx.
func (d *StreamingDaemon) checkWatchdog(idleTimeout time.Duration) {
	if d.closed.Load() {
		return
	}
	lastStdout := time.Unix(0, d.lastStdoutNano.Load())
	if time.Since(lastStdout) < idleTimeout {
		return
	}

	d.inflightMu.Lock()
	if len(d.inflight) == 0 {
		d.inflightMu.Unlock()
		return
	}
	activeTurn := d.inflight[0]
	d.inflightMu.Unlock()

	if activeTurn == nil || activeTurn.Sink == nil {
		return
	}

	rescued := d.rescueTerminalTurn(activeTurn)
	if !IsSubstantiveResponse(rescued) || strings.HasPrefix(rescued, "[Tool Call Requested]:") {
		return
	}

	d.rescueWedgedTurn(activeTurn, rescued)
}

func (d *StreamingDaemon) rescueWedgedTurn(activeTurn *TurnContext, rescued string) {
	d.inflightMu.Lock()
	if len(d.inflight) == 0 || d.inflight[0] != activeTurn {
		d.inflightMu.Unlock()
		return
	}
	d.inflight = d.inflight[1:]
	dangling := d.drainInFlightToolsLocked()
	d.inflightMu.Unlock()

	for _, rec := range dangling {
		activeTurn.Sink.OnToolCompleted(rec.completion(), rec.mcpServer, time.Since(rec.startedAt), "aborted")
	}

	d.mu.Lock()
	d.dirty = true
	d.turnCount++
	d.mu.Unlock()

	log.Printf("[StreamingDaemon] Rescuing wedged turn in session %s via quiescent terminal transcript response (%d bytes)", d.SessionID(), len(rescued))

	if err := d.closeInternal(); err != nil {
		log.Printf("[StreamingDaemon] Warning: closeInternal failed during wedged turn rescue: %v", err)
	}

	res := &TurnResult{
		ConversationID: d.SessionID(),
		Response:       strings.TrimSpace(rescued),
		Duration:       time.Since(activeTurn.CreatedAt),
	}
	d.fillStreamUsage(activeTurn, res)
	activeTurn.Sink.OnResult(res)
}

func (d *StreamingDaemon) drainInFlightToolsLocked() []inFlightToolCall {
	if len(d.inFlightTools) == 0 {
		return nil
	}
	dangling := make([]inFlightToolCall, 0, len(d.inFlightTools))
	for _, rec := range d.inFlightTools {
		dangling = append(dangling, rec)
	}
	d.inFlightTools = make(map[int]inFlightToolCall)
	return dangling
}
