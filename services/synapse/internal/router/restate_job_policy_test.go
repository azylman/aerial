package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsubtest"
)

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
			msg := &pubsubtest.Message{Payload: []byte(`{}`), Subj: "aerial.jobs.synapse.github", Delivered: tc.numDelivered}
			err := handleJetStreamJob(context.Background(), msg, maxDeliverGitHubWebhook, "github", func(struct{}) error { return tc.processErr })
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleJetStreamJob() err = %v, wantErr %v", err, tc.wantErr)
			}
			if msg.Termed() != tc.wantTerm {
				t.Errorf("termed = %v, want %v", msg.Termed(), tc.wantTerm)
			}
			if nakDelay, naked := msg.NakDelay(); naked != tc.wantNak || nakDelay != tc.wantNakDelay {
				t.Errorf("nak = %v/%v, want %v/%v", naked, nakDelay, tc.wantNak, tc.wantNakDelay)
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
