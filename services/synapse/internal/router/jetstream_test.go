package router

import (
	"context"
	"errors"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
)

func TestInitSynapseJetStream(t *testing.T) {
	origNewBus := newBusFn
	defer func() { newBusFn = origNewBus }()

	const natsURL = "nats://127.0.0.1:4222"
	errStream, errSub, errClose := errors.New("stream err"), errors.New("sub err"), errors.New("close err")
	tests := []struct {
		name          string
		natsURL       string
		busErr        error
		ensureErr     error
		consumeErr    error
		closeErr      error
		wantErr       bool
		wantBusCalled bool
	}{
		{name: "empty nats url returns noop without connecting", natsURL: ""},
		{name: "connect error returns error", natsURL: natsURL, busErr: errors.New("connection refused"), wantBusCalled: true, wantErr: true},
		{name: "ensure core streams error returns error", natsURL: natsURL, ensureErr: errStream, wantBusCalled: true, wantErr: true},
		{name: "start job consumers error returns error", natsURL: natsURL, consumeErr: errSub, wantBusCalled: true, wantErr: true},
		{name: "ensure core streams error with close error", natsURL: natsURL, ensureErr: errStream, closeErr: errClose, wantBusCalled: true, wantErr: true},
		{name: "start job consumers error with close error", natsURL: natsURL, consumeErr: errSub, closeErr: errClose, wantBusCalled: true, wantErr: true},
		{name: "successful connection with close error on teardown", natsURL: natsURL, closeErr: errClose, wantBusCalled: true},
		{name: "successful connection configures bus and starts consumers", natsURL: natsURL, wantBusCalled: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
				called = true
				if tc.busErr != nil {
					return nil, tc.busErr
				}
				return &pubsubtest.FaultBus{Bus: pubsub.NewMemoryBus(), EnsureErr: tc.ensureErr, ConsumeErr: tc.consumeErr, CloseErr: tc.closeErr}, nil
			}
			srv := &RouterServer{}
			stop, err := initSynapseJetStream(context.Background(), srv, tc.natsURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("initSynapseJetStream() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if called != tc.wantBusCalled {
				t.Errorf("newBusFn called = %v, want %v", called, tc.wantBusCalled)
			}
			if tc.wantErr {
				if stop != nil {
					t.Error("expected nil stop func on error")
				}
				return
			}
			if stop == nil {
				t.Fatal("expected non-nil stop func")
			}
			stop()
			if tc.wantBusCalled && srv.EventBus() == nil {
				t.Errorf("expected non-nil event bus on server")
			}
		})
	}
}
