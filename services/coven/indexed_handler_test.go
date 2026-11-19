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
			extract := func(_ context.Context, _ string) error {
				calls++
				return retErr
			}
			err := handleTranscriptsIndexedEvent(context.Background(), msg, tc.stack, extract)
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleTranscriptsIndexedEvent() err = %v, wantErr = %v", err, tc.wantErr)
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
