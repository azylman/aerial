package pubsub

import (
	"testing"

	"github.com/nats-io/nats.go"
)

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

func TestBuildConnectOptions_UserCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		user         string
		pass         string
		url          string
		wantUser     string
		wantPassword string
	}{
		{name: "credentials injected without userinfo", user: "u1", pass: "p1", url: "nats://127.0.0.1:4222", wantUser: "u1", wantPassword: "p1"},
		{name: "url userinfo kept without nats.UserInfo", user: "u1", pass: "p1", url: "nats://existing:pass@127.0.0.1:4222"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := buildConnectOptions(tc.url, tc.user, tc.pass)
			var natsOpts nats.Options
			for _, opt := range opts {
				if err := opt(&natsOpts); err != nil {
					t.Fatalf("unexpected option error: %v", err)
				}
			}
			if natsOpts.User != tc.wantUser {
				t.Errorf("natsOpts.User = %q, want %q", natsOpts.User, tc.wantUser)
			}
			if natsOpts.Password != tc.wantPassword {
				t.Errorf("natsOpts.Password = %q, want %q", natsOpts.Password, tc.wantPassword)
			}
		})
	}
}
