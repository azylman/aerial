package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestDarkAnchorMessage(t *testing.T) {
	msg := darkAnchorMessage{}
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "data returns empty event", got: string(msg.Data()), want: `{}`},
		{name: "subject empty", got: msg.Subject(), want: ""},
		{name: "header nil", got: fmt.Sprintf("%v", msg.Header()), want: "map[]"},
		{name: "delivered zero", got: strconv.FormatUint(msg.NumDelivered(), 10), want: "0"},
		{name: "ack nil", got: fmt.Sprintf("%v", msg.Ack()), want: "<nil>"},
		{name: "nak nil", got: fmt.Sprintf("%v", msg.NakWithDelay(time.Second)), want: "<nil>"},
		{name: "term nil", got: fmt.Sprintf("%v", msg.Term()), want: "<nil>"},
		{name: "snooze nil", got: fmt.Sprintf("%v", msg.Snooze(time.Second)), want: "<nil>"},
		{name: "in progress nil", got: fmt.Sprintf("%v", msg.InProgress()), want: "<nil>"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}

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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(tc.data)
			if tc.dataFromHelper {
				data = indexedTestEvent(t, tc.eventStack, tc.eventThread)
			}
			msg := &mockPubsubMsg{data: data, delivered: tc.delivered}
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
