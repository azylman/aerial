package pubsub

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNakBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		numDelivered uint64
		maxDelay     time.Duration
		want         time.Duration
	}{
		{name: "zero deliveries", numDelivered: 0, maxDelay: 16 * time.Second, want: time.Second},
		{name: "first delivery", numDelivered: 1, maxDelay: 16 * time.Second, want: time.Second},
		{name: "second delivery doubles", numDelivered: 2, maxDelay: 16 * time.Second, want: 2 * time.Second},
		{name: "fifth delivery reaches cap", numDelivered: 5, maxDelay: 16 * time.Second, want: 16 * time.Second},
		{name: "sixth delivery clamped", numDelivered: 6, maxDelay: 16 * time.Second, want: 16 * time.Second},
		{name: "huge count clamped without overflow", numDelivered: 1 << 40, maxDelay: time.Hour, want: time.Hour},
		{name: "huge count under huge cap uses 2^30s", numDelivered: 1 << 40, maxDelay: 1 << 62, want: (1 << 30) * time.Second},
		{name: "synapse webhook forty-fifth delivery", numDelivered: 45, maxDelay: 16 * time.Second, want: 16 * time.Second},
		{name: "synapse restate first delivery", numDelivered: 1, maxDelay: time.Minute, want: time.Second},
		{name: "synapse restate fifth delivery", numDelivered: 5, maxDelay: time.Minute, want: 16 * time.Second},
		{name: "synapse restate sixth delivery", numDelivered: 6, maxDelay: time.Minute, want: 32 * time.Second},
		{name: "synapse restate seventh delivery caps", numDelivered: 7, maxDelay: time.Minute, want: time.Minute},
		{name: "synapse restate forty-fifth delivery capped", numDelivered: 45, maxDelay: time.Minute, want: time.Minute},
		{name: "cap below one second", numDelivered: 1, maxDelay: 500 * time.Millisecond, want: 500 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NakBackoff(tc.numDelivered, tc.maxDelay); got != tc.want {
				t.Errorf("NakBackoff(%d, %v) = %v, want %v", tc.numDelivered, tc.maxDelay, got, tc.want)
			}
		})
	}
}

func TestSanitizeSubjectToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		token string
		want  string
	}{
		{name: "empty becomes unknown", token: "", want: "unknown"},
		{name: "blank becomes unknown", token: "   ", want: "unknown"},
		{name: "only symbols becomes unknown", token: "!!!", want: "unknown"},
		{name: "only underscores becomes unknown", token: "__", want: "unknown"},
		{name: "plain token unchanged", token: "aerial", want: "aerial"},
		{name: "hyphen underscore and digits kept", token: "Agent-Alpha_v1", want: "Agent-Alpha_v1"},
		{name: "surrounding space trimmed", token: "  valid_token  ", want: "valid_token"},
		{name: "dot and hash replaced", token: "my.topic#name", want: "my_topic_name"},
		{name: "slash and colon replaced", token: "foo/bar:baz", want: "foo_bar_baz"},
		{name: "inner spaces replaced", token: "Mixed Case 123", want: "Mixed_Case_123"},
		{name: "wildcards replaced", token: "a*b>c", want: "a_b_c"},
		{name: "non-ascii rune replaced once", token: "café", want: "caf_"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SanitizeSubjectToken(tc.token); got != tc.want {
				t.Errorf("SanitizeSubjectToken(%q) = %q, want %q", tc.token, got, tc.want)
			}
		})
	}
}

func TestSubjectTranscriptsStepsForAgent_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		agent     string
		sessionID string
		want      string
	}{
		{"clean agent and session", "aerial", "sess-123", "aerial.transcripts.steps.aerial.sess-123"},
		{"sibling agent-alpha", "agent-alpha", "sess-456", "aerial.transcripts.steps.agent-alpha.sess-456"},
		{"sibling agent-beta", "agent-beta", "sess-789", "aerial.transcripts.steps.agent-beta.sess-789"},
		{"empty agent defaults to aerial", "", "sess-abc", "aerial.transcripts.steps.aerial.sess-abc"},
		{"whitespace agent defaults to aerial", "   ", "sess-xyz", "aerial.transcripts.steps.aerial.sess-xyz"},
		{"dots in tokens sanitized to hyphens", "agent.test", "session.id.with.dots", "aerial.transcripts.steps.agent-test.session-id-with-dots"},
		{"wildcards and whitespace sanitized", "agent*with>wild card", "sess*>123", "aerial.transcripts.steps.agent-with-wild-card.sess-123"},
		{"special characters sanitized to hyphens", "agent$foo@bar!", "sess#456", "aerial.transcripts.steps.agent-foo-bar.sess-456"},
		{"empty sessionID maps to unknown", "aerial", "", "aerial.transcripts.steps.aerial.unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SubjectTranscriptsStepsForAgent(tc.agent, tc.sessionID)
			if got != tc.want {
				t.Errorf("SubjectTranscriptsStepsForAgent(%q, %q) = %q, want %q", tc.agent, tc.sessionID, got, tc.want)
			}
		})
	}
}

func TestParseTranscriptsStepsSubject_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		subject       string
		wantAgent     string
		wantSessionID string
		wantOK        bool
	}{
		{name: "5-token subject with agent", subject: "aerial.transcripts.steps.alpha.sess-1", wantAgent: "alpha", wantSessionID: "sess-1", wantOK: true},
		{name: "5-token subject aerial", subject: "aerial.transcripts.steps.aerial.sess-abc", wantAgent: "aerial", wantSessionID: "sess-abc", wantOK: true},
		{name: "legacy 4-token subject without agent", subject: "aerial.transcripts.steps.session-123", wantSessionID: "session-123", wantOK: true},
		{name: "wrong prefix 5 tokens", subject: "other.transcripts.steps.aerial.session-123"},
		{name: "wrong prefix 4 tokens", subject: "aerial.other.steps.session-123"},
		{name: "empty agent token in 5-token subject", subject: "aerial.transcripts.steps..session-123"},
		{name: "empty session token in 5-token subject", subject: "aerial.transcripts.steps.agent-alpha."},
		{name: "empty session token in 4-token subject", subject: "aerial.transcripts.steps."},
		{name: "too few tokens", subject: "aerial.transcripts"},
		{name: "too many tokens", subject: "aerial.transcripts.steps.extra.agent.session"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotAgent, gotSessionID, gotOK := ParseTranscriptsStepsSubject(tc.subject)
			if gotOK != tc.wantOK || gotAgent != tc.wantAgent || gotSessionID != tc.wantSessionID {
				t.Errorf("ParseTranscriptsStepsSubject(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tc.subject, gotAgent, gotSessionID, gotOK, tc.wantAgent, tc.wantSessionID, tc.wantOK)
			}
		})
	}
}

func TestSubjectCovenExtractCoalescedForAgent_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		agent    string
		threadID string
		want     string
	}{
		{"valid agent and threadID", "agent-beta", "th-456", "aerial.coalesced.coven_extract.agent-beta.th-456"},
		{"empty agent defaults to aerial", "", "th-123", "aerial.coalesced.coven_extract.aerial.th-123"},
		{"whitespace agent defaults to aerial", "   ", "th-789", "aerial.coalesced.coven_extract.aerial.th-789"},
		{"dots in tokens replaced with hyphens", "agent-alpha.san", "thread.id.dots", "aerial.coalesced.coven_extract.agent-alpha-san.thread-id-dots"},
		{"wildcards and whitespace in agent sanitized", "agent*with>wild card", "th-456", "aerial.coalesced.coven_extract.agent-with-wild-card.th-456"},
		{"wildcard threadID preserved for subscription", "agent-alpha", ">", "aerial.coalesced.coven_extract.agent-alpha.>"},
		{"empty threadID maps to unknown", "aerial", "", "aerial.coalesced.coven_extract.aerial.unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SubjectCovenExtractCoalescedForAgent(tc.agent, tc.threadID)
			if got != tc.want {
				t.Errorf("SubjectCovenExtractCoalescedForAgent(%q, %q) = %q, want %q", tc.agent, tc.threadID, got, tc.want)
			}
		})
	}
}

func TestEffectiveAgent_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		eventAgent string
		eventStack string
		want       string
	}{
		{"agent populated wins", "agent-alpha", "aerial", "agent-alpha"},
		{"agent empty falls back to stack", "", "agent-beta", "agent-beta"},
		{"agent and stack empty default to aerial", "", "", "aerial"},
		{"whitespace trimmed properly", "   ", "  agent-alpha  ", "agent-alpha"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ev := TranscriptsIndexedEvent{
				Agent: tc.eventAgent,
				Stack: tc.eventStack,
			}
			if got := ev.EffectiveAgent(); got != tc.want {
				t.Errorf("TranscriptsIndexedEvent.EffectiveAgent() = %q, want %q", got, tc.want)
			}
			job := CovenExtractJobArgs{
				Agent: tc.eventAgent,
				Stack: tc.eventStack,
			}
			if got := job.EffectiveAgent(); got != tc.want {
				t.Errorf("CovenExtractJobArgs.EffectiveAgent() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestContracts_JSONWireFormat(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 11, 14, 16, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		payload  any
		wantJSON string
	}{
		{
			name: "TurnCompletedEvent_telemetry",
			payload: TurnCompletedEvent{
				SessionID: "s1", CompletedAt: fixedTime, Status: "ok", DurationMs: 10, TotalTokens: 100,
			},
			wantJSON: `{"session_id":"s1","source":"","completed_at":"2026-11-14T16:00:00Z","status":"ok","duration_ms":10,"total_tokens":100}`,
		},
		{
			name:     "TurnCompletedEvent_minimal",
			payload:  TurnCompletedEvent{SessionID: "s2", Source: "src", CompletedAt: fixedTime},
			wantJSON: `{"session_id":"s2","source":"src","completed_at":"2026-11-14T16:00:00Z"}`,
		},
		{
			name:     "CovenExtractJobArgs",
			payload:  CovenExtractJobArgs{ThreadID: "t1", SessionID: "s1"},
			wantJSON: `{"thread_id":"t1","session_id":"s1"}`,
		},
		{
			name:     "TranscriptsIndexedEvent",
			payload:  TranscriptsIndexedEvent{Stack: "aerial", SessionID: "s1", ThreadID: "t1", FileMtime: fixedTime, FirstStep: 0, LastStep: 9, StepsIndexed: 10, Final: true, IndexedAt: fixedTime},
			wantJSON: `{"stack":"aerial","session_id":"s1","thread_id":"t1","file_mtime":"2026-11-14T16:00:00Z","first_step":0,"last_step":9,"steps_indexed":10,"final":true,"indexed_at":"2026-11-14T16:00:00Z"}`,
		},
		{
			name:     "ProsperaReportJobArgs",
			payload:  ProsperaReportJobArgs{CallID: "c1"},
			wantJSON: `{"call_id":"c1"}`,
		},
		{
			name:     "ProsperaVoicemailJobArgs",
			payload:  ProsperaVoicemailJobArgs{CallID: "c1", RecordingURL: "http://u", DurationSec: 30},
			wantJSON: `{"call_id":"c1","recording_url":"http://u","duration_sec":30}`,
		},
		{
			name:     "ProsperaSMSJobArgs",
			payload:  ProsperaSMSJobArgs{MessageSID: "m1", FromNumber: "+1", ToNumber: "+2", Body: "hi"},
			wantJSON: `{"message_sid":"m1","from_number":"+1","to_number":"+2","body":"hi"}`,
		},
		{
			name:     "HaroOneShotJobArgs",
			payload:  HaroOneShotJobArgs{ThreadID: "t1", Prompt: "p1", RunAt: fixedTime},
			wantJSON: `{"thread_id":"t1","prompt":"p1","run_at":"2026-11-14T16:00:00Z"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tc.payload)
			if err != nil {
				t.Fatalf("Marshal(%s): %v", tc.name, err)
			}
			if string(b) != tc.wantJSON {
				t.Errorf("%s wire JSON mismatch:\ngot:  %s\nwant: %s", tc.name, string(b), tc.wantJSON)
			}
		})
	}
}
