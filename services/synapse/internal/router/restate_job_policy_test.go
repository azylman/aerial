package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// policyJobMsg records the ack decision handleJetStreamJob makes.
type policyJobMsg struct {
	numDelivered uint64
	termed       bool
	nakDelay     time.Duration
	naked        bool
}

func (*policyJobMsg) Data() []byte           { return []byte(`{}`) }
func (*policyJobMsg) Subject() string        { return "aerial.jobs.synapse.github" }
func (*policyJobMsg) Header() nats.Header    { return nil }
func (m *policyJobMsg) NumDelivered() uint64 { return m.numDelivered }
func (*policyJobMsg) Ack() error             { return nil }
func (m *policyJobMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return nil
}
func (m *policyJobMsg) Term() error              { m.termed = true; return nil }
func (*policyJobMsg) Snooze(time.Duration) error { return nil }
func (*policyJobMsg) InProgress() error          { return nil }

func TestRestateBackoffDelay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		numDelivered uint64
		want         time.Duration
	}{
		{name: "first delivery", numDelivered: 1, want: time.Second},
		{name: "zero delivery", numDelivered: 0, want: time.Second},
		{name: "fourth delivery doubles", numDelivered: 4, want: 8 * time.Second},
		{name: "sixth delivery", numDelivered: 6, want: 32 * time.Second},
		{name: "seventh delivery caps", numDelivered: 7, want: time.Minute},
		{name: "late delivery stays capped", numDelivered: 44, want: time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := restateBackoffDelay(tc.numDelivered); got != tc.want {
				t.Errorf("restateBackoffDelay(%d) = %v, want %v", tc.numDelivered, got, tc.want)
			}
		})
	}
}

func TestHandleJetStreamJobRestatePolicy(t *testing.T) {
	t.Parallel()
	retryable := &restateSendError{handler: "HandleMerged", err: errors.New("status 503")}
	terminal := &restateSendError{handler: "HandleMerged", terminal: true, err: errors.New("status 404")}
	tests := []struct {
		name         string
		numDelivered uint64
		processErr   error
		wantErr      bool
		wantTerm     bool
		wantNak      bool
		wantNakDelay time.Duration
	}{
		{name: "success acks", numDelivered: 1, processErr: nil},
		{name: "non-restate error naks on short backoff", numDelivered: 2, processErr: errors.New("db down"), wantErr: true, wantNak: true, wantNakDelay: 2 * time.Second},
		{name: "non-restate error terms at short budget", numDelivered: 5, processErr: errors.New("db down"), wantTerm: true},
		{name: "restate retryable outlives short budget", numDelivered: 5, processErr: retryable, wantErr: true, wantNak: true, wantNakDelay: 16 * time.Second},
		{name: "restate retryable caps backoff", numDelivered: 20, processErr: retryable, wantErr: true, wantNak: true, wantNakDelay: time.Minute},
		{name: "restate retryable terms at restate budget", numDelivered: 45, processErr: retryable, wantTerm: true},
		{name: "wrapped restate terminal drops job", numDelivered: 1, processErr: errors.Join(errors.New("other"), terminal), wantTerm: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := &policyJobMsg{numDelivered: tc.numDelivered}
			err := handleJetStreamJob(context.Background(), msg, maxDeliverGitHubWebhook, "github", func(struct{}) error { return tc.processErr })
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleJetStreamJob() err = %v, wantErr %v", err, tc.wantErr)
			}
			if msg.termed != tc.wantTerm {
				t.Errorf("termed = %v, want %v", msg.termed, tc.wantTerm)
			}
			if msg.naked != tc.wantNak || msg.nakDelay != tc.wantNakDelay {
				t.Errorf("nak = %v/%v, want %v/%v", msg.naked, msg.nakDelay, tc.wantNak, tc.wantNakDelay)
			}
		})
	}
}

func TestEventIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		source string
		id     string
	}{
		{name: "empty id has no identity", source: "github", id: ""},
		{name: "blank whitespace id has no identity", source: "github", id: "   "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := eventIdentity(tc.source, tc.id); got != "" {
				t.Errorf("eventIdentity(%q, %q) = %q, want empty", tc.source, tc.id, got)
			}
		})
	}
}
