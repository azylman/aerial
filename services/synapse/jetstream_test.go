package main

import (
	"context"
	"errors"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestInitSynapseJetStream(t *testing.T) {
	origNewBus := newBusFn
	defer func() { newBusFn = origNewBus }()

	tests := []struct {
		name          string
		natsURL       string
		busErr        error
		wantBusCalled bool
		wantBusSet    bool
	}{
		{name: "empty nats url returns noop without connecting", natsURL: "", wantBusCalled: false},
		{name: "connect error returns noop", natsURL: "nats://127.0.0.1:4222", busErr: errors.New("connection refused"), wantBusCalled: true},
		{name: "successful connection configures bus and starts consumers", natsURL: "nats://127.0.0.1:4222", wantBusCalled: true, wantBusSet: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
				called = true
				if tc.wantBusSet {
					return pubsub.NewMemoryBus(), nil
				}
				return nil, tc.busErr
			}
			srv := &RouterServer{}
			stop := initSynapseJetStream(context.Background(), srv, tc.natsURL)
			if stop == nil {
				t.Fatal("expected non-nil stop func")
			}
			stop()
			if called != tc.wantBusCalled {
				t.Errorf("newBusFn called = %v, want %v", called, tc.wantBusCalled)
			}
			if tc.wantBusSet && srv.EventBus() == nil {
				t.Errorf("expected non-nil event bus on server")
			}
		})
	}
}
