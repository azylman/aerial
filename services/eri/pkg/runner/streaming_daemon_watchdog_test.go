package runner

import (
	"context"
	"testing"
	"time"
)

func TestStreamingDaemon_WatchdogRescue(t *testing.T) {
	tests := []struct {
		name          string
		rescuedText   string
		startTool     bool
		closeStdout   bool
		wantResp      string
		wantErrSubstr string
		wantAborted   bool
	}{
		{
			name:        "QuiescentTerminalRescue",
			rescuedText: "Rescued substantive response via watchdog",
			wantResp:    "Rescued substantive response via watchdog",
		},
		{
			name:        "InflightToolAbortedOnRescue",
			rescuedText: "Rescued turn after hanging tool",
			startTool:   true,
			wantResp:    "Rescued turn after hanging tool",
			wantAborted: true,
		},
		{name: "NonSubstantiveIgnoredFallsBackToEOF", rescuedText: "", closeStdout: true, wantErrSubstr: "unexpected EOF"},
	}
	for _, tc := range tests {
		rescuerChecked := make(chan struct{}, 1)
		cfg := DaemonConfig{
			SessionID:           "00000000-0000-0000-0000-000000000105",
			WatchdogInterval:    5 * time.Millisecond,
			WatchdogIdleTimeout: 15 * time.Millisecond,
			TerminalTurnRescuer: func(ctx context.Context, convID string, since time.Time) string {
				select {
				case rescuerChecked <- struct{}{}:
				default:
				}
				return tc.rescuedText
			},
		}
		h := startTestStreamingDaemon(t, cfg, "", true)
		sink := newMockTurnSink()
		turn := &TurnContext{
			TurnID:    "turn-" + tc.name,
			Prompt:    "prompt",
			Sink:      sink,
			CreatedAt: time.Now().Add(-20 * time.Millisecond),
		}
		if err := h.daemon.Send(turn.Prompt, turn); err != nil {
			t.Fatalf("%s: send failed: %v", tc.name, err)
		}
		if tc.startTool {
			h.daemon.dispatchNDJSONLine(`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","step_index":1,"tool_name":"stalled_tool"}}`)
		}
		if tc.closeStdout {
			select {
			case <-rescuerChecked:
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("%s: timed out waiting for rescuer check", tc.name)
			}
			_ = h.outW.Close()
		}
		waitAndAssertSinkResult(t, tc.name, sink, tc.wantResp, "", tc.wantErrSubstr)
		if tc.wantAborted {
			sink.mu.Lock()
			details := sink.completedToolDetails
			sink.mu.Unlock()
			if len(details) == 0 || details[0].ToolName != "stalled_tool" || details[0].Status != "aborted" {
				t.Errorf("%s: expected stalled_tool aborted, got %+v", tc.name, details)
			}
		}
		if tc.wantResp != "" {
			if !h.daemon.IsDirty() {
				t.Errorf("%s: expected daemon to be dirty after watchdog rescue", tc.name)
			}
			if h.daemon.TurnCount() != 1 {
				t.Errorf("%s: expected turn count 1, got %d", tc.name, h.daemon.TurnCount())
			}
			if h.daemon.State() != StateClosed {
				t.Errorf("%s: expected StateClosed, got %s", tc.name, h.daemon.State())
			}
		}
	}
}

func TestStreamingDaemon_RescuerEdgeCases(t *testing.T) {
	d := &StreamingDaemon{sessionID: "sess-res"}
	if got := d.rescueTranscript(nil); got != "" {
		t.Errorf("expected empty string for nil turn, got %q", got)
	}
	if got := d.rescueTerminalTurn(nil); got != "" {
		t.Errorf("expected empty string for nil turn, got %q", got)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	d.cfg.TranscriptRescuer = func(ctx context.Context, convID string, since time.Time) string {
		return "recovered"
	}
	d.cfg.TerminalTurnRescuer = func(ctx context.Context, convID string, since time.Time) string {
		return "recovered"
	}

	if got := d.rescueTranscript(&TurnContext{Ctx: canceledCtx}); got != "" {
		t.Errorf("expected empty string for canceled context, got %q", got)
	}
	if got := d.rescueTerminalTurn(&TurnContext{Ctx: canceledCtx}); got != "" {
		t.Errorf("expected empty string for canceled context, got %q", got)
	}

	dEmpty := &StreamingDaemon{}
	if got := dEmpty.rescueTranscript(&TurnContext{}); got != "" {
		t.Errorf("expected empty string for empty session ID, got %q", got)
	}
	if got := dEmpty.rescueTerminalTurn(&TurnContext{}); got != "" {
		t.Errorf("expected empty string for empty session ID, got %q", got)
	}
}

func TestStreamingDaemon_CheckWatchdogBranches(t *testing.T) {
	d := &StreamingDaemon{sessionID: "sess-branches"}
	d.closed.Store(true)
	d.checkWatchdog(time.Millisecond)
	if d.IsDirty() || d.TurnCount() != 0 {
		t.Fatalf("expected closed daemon to skip watchdog without mutating state")
	}

	d.closed.Store(false)
	d.lastStdoutNano.Store(time.Now().UnixNano())
	d.checkWatchdog(time.Hour)
	if d.IsDirty() || d.TurnCount() != 0 {
		t.Fatalf("expected recent stdout to skip watchdog without mutating state")
	}

	d.lastStdoutNano.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	d.checkWatchdog(time.Hour)
	if d.IsDirty() || d.TurnCount() != 0 {
		t.Fatalf("expected empty inflight to skip watchdog without mutating state")
	}

	enqueueInflightTurn(d, "turn-nil-sink", nil)
	d.checkWatchdog(time.Hour)
	if d.IsDirty() || d.TurnCount() != 0 {
		t.Fatalf("expected turn with nil sink to skip watchdog without mutating state")
	}
	if len(d.inflight) != 1 {
		t.Fatalf("expected inflight queue to remain intact with nil sink, got %d", len(d.inflight))
	}

	d.inflightMu.Lock()
	d.inflight = nil
	d.inflightMu.Unlock()

	sink := newMockTurnSink()
	enqueueInflightTurn(d, "turn-tool-req", sink)
	d.cfg.TerminalTurnRescuer = func(ctx context.Context, convID string, since time.Time) string {
		return "[Tool Call Requested]: invoke_subagent"
	}
	d.checkWatchdog(time.Hour)
	sink.mu.Lock()
	called := sink.result != nil
	sink.mu.Unlock()
	if called {
		t.Fatalf("expected sink.OnResult NOT called for [Tool Call Requested] prefix")
	}
	if d.IsDirty() || d.TurnCount() != 0 {
		t.Fatalf("expected [Tool Call Requested] to skip rescue without marking dirty")
	}
	if len(d.inflight) != 1 {
		t.Fatalf("expected inflight queue to remain intact when tool call requested, got %d", len(d.inflight))
	}

	otherTurn := &TurnContext{TurnID: "turn-other"}
	d.rescueWedgedTurn(otherTurn, "substantive response")
	if d.IsDirty() || d.TurnCount() != 0 {
		t.Fatalf("expected non-matching turn to not be rescued")
	}
	if len(d.inflight) != 1 || d.inflight[0].TurnID != "turn-tool-req" {
		t.Fatalf("expected inflight turn-tool-req untouched by rescueWedgedTurn for other turn")
	}

	if got := d.drainInFlightToolsLocked(); got != nil {
		t.Errorf("expected nil from drainInFlightToolsLocked on empty map")
	}
}
