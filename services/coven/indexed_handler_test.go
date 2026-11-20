package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

func indexedTestEvent(t *testing.T, stack, threadID string) []byte {
	t.Helper()
	ev := pubsub.TranscriptsIndexedEvent{
		Stack:        stack,
		SessionID:    "sess-1",
		ThreadID:     threadID,
		FirstStep:    0,
		LastStep:     9,
		StepsIndexed: 10,
		Final:        true,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return data
}

func TestCovenNakDelay(t *testing.T) {
	tests := []struct {
		name      string
		delivered uint64
		want      time.Duration
	}{
		{name: "zero delivers one second", delivered: 0, want: 1 * time.Second},
		{name: "first delivers one second", delivered: 1, want: 1 * time.Second},
		{name: "second delivers two seconds", delivered: 2, want: 2 * time.Second},
		{name: "third delivers four seconds", delivered: 3, want: 4 * time.Second},
		{name: "seventh delivers sixty four seconds", delivered: 7, want: 64 * time.Second},
		{name: "twelfth delivers thirty four minutes", delivered: 12, want: 2048 * time.Second},
		{name: "caps at one hour", delivered: 13, want: 1 * time.Hour},
		{name: "large deliveries stay capped", delivered: 40, want: 1 * time.Hour},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := covenNakDelay(tc.delivered); got != tc.want {
				t.Errorf("covenNakDelay(%d) = %v, want %v", tc.delivered, got, tc.want)
			}
		})
	}
}

func TestHandleTranscriptsIndexedEvent(t *testing.T) {
	tests := []struct {
		name           string
		data           string
		dataFromHelper bool
		eventStack     string
		eventThread    string
		stack          string
		delivered      uint64
		extractErr     error
		termErr        error
		nakErr         error
		wantErr        bool
		wantTermed     bool
		wantNaked      bool
		wantNakDelay   time.Duration
		wantCalls      int
	}{
		{
			name:       "undecodable terms without retry",
			data:       `{not json`,
			stack:      "aerial",
			wantTermed: true,
			wantCalls:  0,
		},
		{
			name:       "undecodable term error handled cleanly",
			data:       `{not json`,
			stack:      "aerial",
			termErr:    errors.New("term failed"),
			wantTermed: true,
			wantCalls:  0,
		},
		{
			name:           "empty thread acks without extract",
			dataFromHelper: true,
			eventStack:     "aerial",
			eventThread:    "   ",
			stack:          "aerial",
			wantCalls:      0,
		},
		{
			name:           "foreign stack skips without extract",
			dataFromHelper: true,
			eventStack:     "other",
			eventThread:    "th-1",
			stack:          "aerial",
			wantCalls:      0,
		},
		{
			name:           "matching stack extracts",
			dataFromHelper: true,
			eventStack:     "aerial",
			eventThread:    "th-1",
			stack:          "aerial",
			wantCalls:      1,
		},
		{
			name:           "empty event stack matches configured stack",
			dataFromHelper: true,
			eventStack:     "",
			eventThread:    "th-1",
			stack:          "aerial",
			wantCalls:      1,
		},
		{
			name:           "extract error naks forever without term",
			dataFromHelper: true,
			eventStack:     "aerial",
			eventThread:    "th-1",
			stack:          "aerial",
			delivered:      3,
			extractErr:     errors.New("boom"),
			wantErr:        true,
			wantNaked:      true,
			wantNakDelay:   4 * time.Second,
			wantCalls:      1,
		},
		{
			name:           "extract error with nak failure returns extract error",
			dataFromHelper: true,
			eventStack:     "aerial",
			eventThread:    "th-1",
			stack:          "aerial",
			delivered:      1,
			extractErr:     errors.New("boom"),
			nakErr:         errors.New("nak failed"),
			wantErr:        true,
			wantNaked:      true,
			wantNakDelay:   1 * time.Second,
			wantCalls:      1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(tc.data)
			if tc.dataFromHelper {
				data = indexedTestEvent(t, tc.eventStack, tc.eventThread)
			}
			msg := &mockPubsubMsg{
				data:      data,
				delivered: tc.delivered,
				termErr:   tc.termErr,
				nakErr:    tc.nakErr,
			}
			calls := 0
			retErr := tc.extractErr
			extract := func(_ context.Context, _, _ string) error {
				calls++
				return retErr
			}
			err := HandleTranscriptsIndexed(context.Background(), msg, tc.stack, "", extract)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleTranscriptsIndexed() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if msg.termed != tc.wantTermed {
				t.Errorf("termed = %v, want %v", msg.termed, tc.wantTermed)
			}
			if msg.naked != tc.wantNaked {
				t.Errorf("naked = %v, want %v", msg.naked, tc.wantNaked)
			}
			if tc.wantNaked && msg.nakDelay != tc.wantNakDelay {
				t.Errorf("nakDelay = %v, want %v", msg.nakDelay, tc.wantNakDelay)
			}
			if calls != tc.wantCalls {
				t.Errorf("extract calls = %d, want %d", calls, tc.wantCalls)
			}
		})
	}
}

type mockRecordedPublishBus struct {
	pubsub.Bus

	publishedSubject string
	publishedData    any
	publishCalls     int
}

func (m *mockRecordedPublishBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.publishCalls++
	m.publishedSubject = subject
	m.publishedData = data
	return m.Bus.Publish(ctx, subject, msgID, data)
}

func TestNewTranscriptsIndexedHandler(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		eventAgent  string
		eventStack  string
		threadID    string
		stack       string
		wantPub     bool
		wantSubject string
	}{
		{
			name:        "matching agent publishes coalesced job",
			eventAgent:  "agent-alpha",
			threadID:    "th-1",
			stack:       "agent-alpha",
			wantPub:     true,
			wantSubject: "aerial.coalesced.coven_extract.agent-alpha.th-1",
		},
		{
			name:        "fallback to event stack matches",
			eventAgent:  "",
			eventStack:  "agent-beta",
			threadID:    "th-2",
			stack:       "agent-beta",
			wantPub:     true,
			wantSubject: "aerial.coalesced.coven_extract.agent-beta.th-2",
		},
		{
			name:       "mismatched agent skips publishing",
			eventAgent: "aerial",
			threadID:   "th-3",
			stack:      "agent-alpha",
			wantPub:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()

			bus := &mockRecordedPublishBus{Bus: memBus}
			handler := newTranscriptsIndexedHandler(bus, tc.stack)
			ev := pubsub.TranscriptsIndexedEvent{
				Agent:    tc.eventAgent,
				Stack:    tc.eventStack,
				ThreadID: tc.threadID,
			}
			data, err := json.Marshal(ev)
			if err != nil {
				t.Fatalf("marshal error: %v", err)
			}
			msg := &mockPubsubMsg{data: data}

			err = handler(context.Background(), msg)
			if err != nil {
				t.Fatalf("handler returned unexpected error: %v", err)
			}
			if (bus.publishCalls > 0) != tc.wantPub {
				t.Fatalf("publishCalls = %d, wantPub = %v", bus.publishCalls, tc.wantPub)
			}
			if tc.wantPub {
				if bus.publishedSubject != tc.wantSubject {
					t.Fatalf("published subject = %q, want %q", bus.publishedSubject, tc.wantSubject)
				}
				args, ok := bus.publishedData.(pubsub.CovenExtractJobArgs)
				if !ok {
					t.Fatalf("expected pubsub.CovenExtractJobArgs, got %T", bus.publishedData)
				}
				if args.Agent != tc.stack || args.ThreadID != tc.threadID {
					t.Fatalf("published args = %+v, want Agent=%q ThreadID=%q", args, tc.stack, tc.threadID)
				}
			}
		})
	}
}

func TestIsValidAgentSlug(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		slug string
		want bool
	}{
		{name: "standard alpha slug", slug: "agent-alpha", want: true},
		{name: "standard beta slug", slug: "agent-beta", want: true},
		{name: "single word slug", slug: "aerial", want: true},
		{name: "slug with numbers", slug: "agent-42", want: true},
		{name: "empty slug", slug: "", want: false},
		{name: "spaces only", slug: "   ", want: false},
		{name: "uppercase disallowed", slug: "Agent-Alpha", want: false},
		{name: "underscores disallowed", slug: "agent_alpha", want: false},
		{name: "special chars disallowed", slug: "agent@alpha", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsValidAgentSlug(tc.slug); got != tc.want {
				t.Errorf("IsValidAgentSlug(%q) = %v, want %v", tc.slug, got, tc.want)
			}
		})
	}
}

func TestParseCovenExtractSubject(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		subject      string
		wantAgent    string
		wantThreadID string
		wantOK       bool
	}{
		{
			name:         "valid 5-token subject alpha",
			subject:      "aerial.coalesced.coven_extract.agent-alpha.th-100",
			wantAgent:    "agent-alpha",
			wantThreadID: "th-100",
			wantOK:       true,
		},
		{
			name:         "valid 5-token subject beta",
			subject:      "aerial.coalesced.coven_extract.agent-beta.th-200",
			wantAgent:    "agent-beta",
			wantThreadID: "th-200",
			wantOK:       true,
		},
		{
			name:    "too few tokens",
			subject: "aerial.coalesced.coven_extract.agent-alpha",
			wantOK:  false,
		},
		{
			name:    "too many tokens",
			subject: "aerial.coalesced.coven_extract.agent-alpha.th-100.extra",
			wantOK:  false,
		},
		{
			name:    "wrong prefix",
			subject: "other.coalesced.coven_extract.agent-alpha.th-100",
			wantOK:  false,
		},
		{
			name:    "invalid agent slug",
			subject: "aerial.coalesced.coven_extract.Agent_Alpha.th-100",
			wantOK:  false,
		},
		{
			name:    "empty thread",
			subject: "aerial.coalesced.coven_extract.agent-alpha.   ",
			wantOK:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotAgent, gotThread, ok := ParseCovenExtractSubject(tc.subject)
			if ok != tc.wantOK {
				t.Fatalf("ParseCovenExtractSubject(%q) ok = %v, want %v", tc.subject, ok, tc.wantOK)
			}
			if ok {
				if gotAgent != tc.wantAgent || gotThread != tc.wantThreadID {
					t.Errorf("ParseCovenExtractSubject(%q) = (%q, %q), want (%q, %q)", tc.subject, gotAgent, gotThread, tc.wantAgent, tc.wantThreadID)
				}
			}
		})
	}
}

func TestNewTranscriptsIndexedHandler_RoleAll(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		eventAgent  string
		threadID    string
		role        string
		wantPub     bool
		wantSubject string
	}{
		{
			name:        "role all accepts agent-alpha",
			eventAgent:  "agent-alpha",
			threadID:    "th-alpha-1",
			role:        "all",
			wantPub:     true,
			wantSubject: "aerial.coalesced.coven_extract.agent-alpha.th-alpha-1",
		},
		{
			name:        "role all accepts agent-beta",
			eventAgent:  "agent-beta",
			threadID:    "th-beta-1",
			role:        "all",
			wantPub:     true,
			wantSubject: "aerial.coalesced.coven_extract.agent-beta.th-beta-1",
		},
		{
			name:       "role all rejects invalid slug",
			eventAgent: "INVALID_SLUG!",
			threadID:   "th-bad",
			role:       "all",
			wantPub:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()

			bus := &mockRecordedPublishBus{Bus: memBus}
			handler := newTranscriptsIndexedHandler(bus, "aerial", tc.role)
			ev := pubsub.TranscriptsIndexedEvent{
				Agent:    tc.eventAgent,
				ThreadID: tc.threadID,
			}
			data, err := json.Marshal(ev)
			if err != nil {
				t.Fatalf("marshal error: %v", err)
			}
			msg := &mockPubsubMsg{data: data}

			err = handler(context.Background(), msg)
			if err != nil {
				t.Fatalf("handler returned unexpected error: %v", err)
			}
			if (bus.publishCalls > 0) != tc.wantPub {
				t.Fatalf("publishCalls = %d, wantPub = %v", bus.publishCalls, tc.wantPub)
			}
			if tc.wantPub && bus.publishedSubject != tc.wantSubject {
				t.Fatalf("published subject = %q, want %q", bus.publishedSubject, tc.wantSubject)
			}
		})
	}
}
