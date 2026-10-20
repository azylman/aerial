package runner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTurnResolver_Resolve(t *testing.T) {
	resolver := NewTurnResolver()
	tests := []struct {
		name            string
		res             *TurnResult
		err             error
		sessionID       string
		wantSuccess     bool
		wantTransient   bool
		wantCorruption  bool
		wantDetail      string
		wantTotalTokens int
	}{
		{name: "NominalSuccess", res: &TurnResult{ConversationID: "conv-nominal", Response: "Nominal response from daemon.", Usage: TokenUsage{InputTokens: 10, OutputTokens: 20, TotalTokens: 30}, Duration: 150 * time.Millisecond}, sessionID: "conv-nominal", wantSuccess: true, wantTotalTokens: 30}, {name: "ContextWindowColdStart", err: errors.New("maximum context length exceeded: prompt too large"), sessionID: "conv-ctx-limit", wantDetail: "prompt length exceeds maximum model context window (hard failure)"},
		{name: "SessionCorruption", err: errors.New("corrupted session state: failed to parse session json"), sessionID: "conv-corrupt", wantCorruption: true}, {name: "QuotaPause", err: errors.New("API error: 429 RESOURCE_EXHAUSTED rate limit exceeded"), sessionID: "conv-quota", wantTransient: true}, {name: "FatalStderr", err: errors.New("fatal: unrecoverable crash in runtime"), sessionID: "conv-fatal"},
		{name: "EmptyResult", res: &TurnResult{Response: ""}, sessionID: "conv-empty", wantTransient: true, wantDetail: "daemon turn completed with empty response"}, {name: "WhitespaceOnly", res: &TurnResult{Response: "   \n\t  "}, sessionID: "conv-ws", wantTransient: true, wantDetail: "daemon turn completed with empty response"}, {name: "DaemonError", err: errors.New("daemon connection dropped unexpectedly"), sessionID: "conv-crash", wantTransient: true, wantDetail: "daemon connection dropped unexpectedly"},
	}

	for _, tc := range tests {
		outcome := resolver.Resolve(context.Background(), tc.res, tc.err, tc.sessionID)
		if outcome.IsSuccess != tc.wantSuccess || outcome.IsTransient != tc.wantTransient || outcome.IsSessionCorruption != tc.wantCorruption || (tc.wantDetail != "" && outcome.ErrorDetail != tc.wantDetail) || (tc.wantTotalTokens != 0 && outcome.Usage.TotalTokens != tc.wantTotalTokens) || outcome.SessionID != tc.sessionID || (tc.res != nil && outcome.IsSuccess && outcome.Response != tc.res.Response) {
			t.Errorf("%s: outcome mismatch = %+v", tc.name, outcome)
		}
	}
}
