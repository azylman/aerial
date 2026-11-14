package runner

import (
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
				TranscriptRescuer: func(string, time.Time) string { return tc.rescuedText },
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
