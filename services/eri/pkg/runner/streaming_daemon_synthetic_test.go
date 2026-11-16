package runner

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestStreamingDaemon_Claude_SyntheticReplies(t *testing.T) {
	t.Parallel()
	const (
		placeholder = `{"type":"assistant","isApiErrorMessage":false,"error":null,"message":{"id":"msg_s1","model":"<synthetic>","role":"assistant","stop_reason":"stop_sequence","usage":{"output_tokens":0},"content":[{"type":"text","text":"No response requested."}]}}`
		rateLimit   = `{"type":"assistant","isApiErrorMessage":true,"error":"rate_limit","message":{"id":"msg_s2","model":"<synthetic>","role":"assistant","stop_reason":"stop_sequence","usage":{"output_tokens":0},"content":[{"type":"text","text":"You've hit your 5-hour limit · resets 12:10pm (America/Los_Angeles)"}]}}`
		realReply   = `{"type":"assistant","message":{"id":"msg_r1","model":"claude-opus-5-5","role":"assistant","content":[{"type":"text","text":"Deploy is green and every check passed."}]}}`
	)
	tests := []struct {
		name          string
		lines         []string
		rescuedText   string
		wantResp      string
		wantErrSubstr string
		wantQuota     bool
	}{
		{
			name:          "PlaceholderOnlyFails",
			lines:         []string{placeholder, `{"type":"result","subtype":"success","is_error":false,"result":"No response requested."}`},
			rescuedText:   "No response requested.",
			wantErrSubstr: "claude synthetic reply (error=none): No response requested.",
		},
		{
			name:          "RateLimitOnlyFailsAsQuotaPause",
			lines:         []string{rateLimit, `{"type":"result","subtype":"success","is_error":true,"result":"You've hit your 5-hour limit · resets 12:10pm (America/Los_Angeles)"}`},
			rescuedText:   "You've hit your 5-hour limit · resets 12:10pm (America/Los_Angeles)",
			wantErrSubstr: "claude synthetic reply (error=rate_limit): You've hit your 5-hour limit · resets 12:10pm (America/Los_Angeles)",
			wantQuota:     true,
		},
		{
			name:     "RealReplySucceeds",
			lines:    []string{realReply, `{"type":"result","subtype":"success","is_error":false,"result":"Deploy is green and every check passed."}`},
			wantResp: "Deploy is green and every check passed.",
		},
		{
			name:     "RealReplyThenPlaceholderSucceeds",
			lines:    []string{realReply, placeholder, `{"type":"result","subtype":"success","is_error":false,"result":"Deploy is green and every check passed."}`},
			wantResp: "Deploy is green and every check passed.",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := DaemonConfig{
				Engine:            "claude",
				SessionID:         "11111111-2222-3333-4444-666666666666",
				TranscriptRescuer: func(context.Context, string, time.Time) string { return tc.rescuedText },
			}
			daemon := &StreamingDaemon{cfg: cfg, sessionID: cfg.SessionID, claudeToolIDs: make(map[string]int)}
			sink := newMockTurnSink()
			enqueueInflightTurn(daemon, "turn-"+tc.name, sink)
			for _, line := range tc.lines {
				daemon.dispatchNDJSONLine(line)
			}
			waitAndAssertSinkResult(t, tc.name, sink, tc.wantResp, "", tc.wantErrSubstr)
			sink.mu.Lock()
			defer sink.mu.Unlock()
			if gotQuota := sink.err != nil && IsQuotaPause(sink.err.Error(), ""); gotQuota != tc.wantQuota {
				t.Errorf("IsQuotaPause(%v) = %v, want %v", sink.err, gotQuota, tc.wantQuota)
			}
			if sink.err != nil && IsCapacityBlip(sink.err.Error(), "") {
				t.Errorf("IsCapacityBlip(%v) = true, want false", sink.err)
			}
		})
	}
}

func TestStreamingDaemon_TranscriptRescue_Cancellation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                          string
		preCancel, duringCancel, want bool
	}{
		{"live", false, false, true}, {"canceled", true, false, false}, {"scan canceled", false, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			type contextKey struct{}
			parent := context.WithValue(context.Background(), contextKey{}, "parent")
			ctx, cancel := context.WithTimeout(parent, time.Minute)
			defer cancel()
			if tc.preCancel {
				cancel()
			}
			entered, proceed := make(chan struct{}), make(chan struct{})
			d := &StreamingDaemon{sessionID: "session", cfg: DaemonConfig{TranscriptRescuer: func(scanCtx context.Context, _ string, _ time.Time) string {
				if scanCtx.Value(contextKey{}) != "parent" {
					t.Error("parent value lost")
				}
				if deadline, ok := scanCtx.Deadline(); !ok || deadline.IsZero() {
					t.Error("parent deadline lost")
				}
				close(entered)
				<-proceed
				return "A substantive recovered answer."
			}}}
			turn := &TurnContext{Ctx: ctx, CreatedAt: time.Now(), Sink: newMockTurnSink()}
			done := make(chan bool, 1)
			go func() { done <- d.tryRescueTurnOnEOF(turn) }()
			if !tc.preCancel {
				<-entered
				if tc.duringCancel {
					cancel()
				}
				close(proceed)
			}
			if got := <-done; got != tc.want {
				t.Fatalf("EOF rescue = %v, want %v", got, tc.want)
			}
			// The result path must enforce the same cancellation boundary.
			if tc.preCancel {
				if d.tryRescueTurnOnResult(turn, nil, "", "") {
					t.Fatal("result rescued canceled turn")
				}
			}
		})
	}
}

type cancelOnRead struct {
	io.ReadCloser

	cancel context.CancelFunc
}

func (r *cancelOnRead) Read(p []byte) (int, error) {
	r.cancel()
	return r.ReadCloser.Read(p)
}

func TestStreamingDaemon_StartupBudgetDuringHandshake(t *testing.T) {
	t.Parallel()
	lifetime := t.Context()
	startup, cancel := context.WithCancel(context.Background())
	defer cancel()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	defer inR.Close()
	defer inW.Close()
	defer outR.Close()
	defer outW.Close()
	handle := &MockProcessHandle{pid: 999}
	spawner := &MockDaemonSpawner{SpawnFn: func(_ context.Context, _ DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		return inW, &cancelOnRead{ReadCloser: outR, cancel: cancel}, nil, handle, nil
	}}
	_, err := StartStreamingDaemon(lifetime, startup, DaemonConfig{Timeout: time.Hour}, spawner)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startup error = %v", err)
	}
	handle.mu.Lock()
	killed := handle.killed
	handle.mu.Unlock()
	if !killed {
		t.Fatal("incomplete startup process was not terminated")
	}
	if lifetime.Err() != nil {
		t.Fatal("startup cancellation reached process lifetime")
	}
}
