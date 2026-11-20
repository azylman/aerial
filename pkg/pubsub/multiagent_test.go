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
		{
			name:      "clean agent and session",
			agent:     "aerial",
			sessionID: "sess-123",
			want:      "aerial.transcripts.steps.aerial.sess-123",
		},
		{
			name:      "sibling agent-alpha",
			agent:     "agent-alpha",
			sessionID: "sess-456",
			want:      "aerial.transcripts.steps.agent-alpha.sess-456",
		},
		{
			name:      "sibling agent-beta",
			agent:     "agent-beta",
			sessionID: "sess-789",
			want:      "aerial.transcripts.steps.agent-beta.sess-789",
		},
		{
			name:      "empty agent defaults to aerial",
			agent:     "",
			sessionID: "sess-abc",
			want:      "aerial.transcripts.steps.aerial.sess-abc",
		},
		{
			name:      "whitespace agent defaults to aerial",
			agent:     "   ",
			sessionID: "sess-xyz",
			want:      "aerial.transcripts.steps.aerial.sess-xyz",
		},
		{
			name:      "dots in tokens sanitized to hyphens",
			agent:     "agent.test",
			sessionID: "session.id.with.dots",
			want:      "aerial.transcripts.steps.agent-test.session-id-with-dots",
		},
		{
			name:      "wildcards and whitespace stripped and sanitized",
			agent:     "agent*with>wildcard and spaces",
			sessionID: "sess*>123",
			want:      "aerial.transcripts.steps.agent-with-wildcard-and-spaces.sess-123",
		},
		{
			name:      "special characters sanitized to hyphens",
			agent:     "agent$foo@bar!",
			sessionID: "sess#456",
			want:      "aerial.transcripts.steps.agent-foo-bar.sess-456",
		},
		{
			name:      "empty sessionID maps to unknown",
			agent:     "aerial",
			sessionID: "",
			want:      "aerial.transcripts.steps.aerial.unknown",
		},
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
		{
			name:          "standard 5-token subject with agent",
			subject:       "aerial.transcripts.steps.agent-alpha.session-123",
			wantAgent:     "agent-alpha",
			wantSessionID: "session-123",
			wantOK:        true,
		},
		{
			name:          "standard 5-token subject aerial",
			subject:       "aerial.transcripts.steps.aerial.sess-abc",
			wantAgent:     "aerial",
			wantSessionID: "sess-abc",
			wantOK:        true,
		},
		{
			name:          "legacy 4-token subject without agent",
			subject:       "aerial.transcripts.steps.session-123",
			wantAgent:     "",
			wantSessionID: "session-123",
			wantOK:        true,
		},
		{
			name:          "wrong prefix 5 tokens",
			subject:       "other.transcripts.steps.aerial.session-123",
			wantAgent:     "",
			wantSessionID: "",
			wantOK:        false,
		},
		{
			name:          "wrong prefix 4 tokens",
			subject:       "aerial.other.steps.session-123",
			wantAgent:     "",
			wantSessionID: "",
			wantOK:        false,
		},
		{
			name:          "empty agent token in 5-token subject",
			subject:       "aerial.transcripts.steps..session-123",
			wantAgent:     "",
			wantSessionID: "",
			wantOK:        false,
		},
		{
			name:          "empty session token in 5-token subject",
			subject:       "aerial.transcripts.steps.agent-alpha.",
			wantAgent:     "",
			wantSessionID: "",
			wantOK:        false,
		},
		{
			name:          "empty session token in 4-token subject",
			subject:       "aerial.transcripts.steps.",
			wantAgent:     "",
			wantSessionID: "",
			wantOK:        false,
		},
		{
			name:          "too few tokens",
			subject:       "aerial.transcripts",
			wantAgent:     "",
			wantSessionID: "",
			wantOK:        false,
		},
		{
			name:          "too many tokens",
			subject:       "aerial.transcripts.steps.extra.agent.session",
			wantAgent:     "",
			wantSessionID: "",
			wantOK:        false,
		},
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
		{
			name:     "valid agent and threadID",
			agent:    "agent-beta",
			threadID: "th-456",
			want:     "aerial.coalesced.coven_extract.agent-beta.th-456",
		},
		{
			name:     "empty agent defaults to aerial",
			agent:    "",
			threadID: "th-123",
			want:     "aerial.coalesced.coven_extract.aerial.th-123",
		},
		{
			name:     "whitespace agent defaults to aerial",
			agent:    "   ",
			threadID: "th-789",
			want:     "aerial.coalesced.coven_extract.aerial.th-789",
		},
		{
			name:     "dots in tokens replaced with hyphens",
			agent:    "agent-alpha.san",
			threadID: "thread.id.dots",
			want:     "aerial.coalesced.coven_extract.agent-alpha-san.thread-id-dots",
		},
		{
			name:     "wildcards and whitespace in agent sanitized",
			agent:    "agent*with>wildcard and spaces",
			threadID: "th-456",
			want:     "aerial.coalesced.coven_extract.agent-with-wildcard-and-spaces.th-456",
		},
		{
			name:     "wildcard threadID preserved for subscription",
			agent:    "agent-alpha",
			threadID: ">",
			want:     "aerial.coalesced.coven_extract.agent-alpha.>",
		},
		{
			name:     "empty threadID maps to unknown",
			agent:    "aerial",
			threadID: "",
			want:     "aerial.coalesced.coven_extract.aerial.unknown",
		},
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
		{
			name:       "agent populated wins",
			eventAgent: "agent-alpha",
			eventStack: "aerial",
			want:       "agent-alpha",
		},
		{
			name:       "agent empty falls back to stack",
			eventAgent: "",
			eventStack: "agent-beta",
			want:       "agent-beta",
		},
		{
			name:       "agent and stack empty default to aerial",
			eventAgent: "",
			eventStack: "",
			want:       "aerial",
		},
		{
			name:       "whitespace trimmed properly",
			eventAgent: "   ",
			eventStack: "  agent-alpha  ",
			want:       "agent-alpha",
		},
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
		{
			name:         "no credentials",
			user:         "",
			pass:         "",
			url:          "nats://127.0.0.1:4222",
			wantUser:     "",
			wantPassword: "",
		},
		{
			name:         "credentials injected when url has no userinfo",
			user:         "agent-user",
			pass:         "agent-pass",
			url:          "nats://127.0.0.1:4222",
			wantUser:     "agent-user",
			wantPassword: "agent-pass",
		},
		{
			name:         "url with userinfo preserved without injecting nats.UserInfo",
			user:         "agent-user",
			pass:         "agent-pass",
			url:          "nats://existing:pass@127.0.0.1:4222",
			wantUser:     "",
			wantPassword: "",
		},
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
