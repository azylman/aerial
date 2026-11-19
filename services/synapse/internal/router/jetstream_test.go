package router

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
		customBus     func() pubsub.Bus
		wantErr       bool
		wantBusCalled bool
		wantBusSet    bool
	}{
		{name: "empty nats url returns noop without connecting", natsURL: "", wantBusCalled: false},
		{name: "connect error returns error", natsURL: "nats://127.0.0.1:4222", busErr: errors.New("connection refused"), wantBusCalled: true, wantErr: true},
		{name: "ensure core streams error returns error", natsURL: "nats://127.0.0.1:4222", wantBusCalled: true, wantErr: true, customBus: func() pubsub.Bus {
			return &errCoreStreamsBus{Bus: pubsub.NewMemoryBus(), err: errors.New("stream err")}
		}},
		{name: "start job consumers error returns error", natsURL: "nats://127.0.0.1:4222", wantBusCalled: true, wantErr: true, customBus: func() pubsub.Bus {
			return &errSubscribeBus{Bus: pubsub.NewMemoryBus(), err: errors.New("sub err")}
		}},
		{name: "ensure core streams error with close error", natsURL: "nats://127.0.0.1:4222", wantBusCalled: true, wantErr: true, customBus: func() pubsub.Bus {
			return &errCoreStreamsAndCloseBus{Bus: pubsub.NewMemoryBus(), streamErr: errors.New("stream err"), closeErr: errors.New("close err")}
		}},
		{name: "start job consumers error with close error", natsURL: "nats://127.0.0.1:4222", wantBusCalled: true, wantErr: true, customBus: func() pubsub.Bus {
			return &errSubscribeAndCloseBus{Bus: pubsub.NewMemoryBus(), subErr: errors.New("sub err"), closeErr: errors.New("close err")}
		}},
		{name: "successful connection with close error on teardown", natsURL: "nats://127.0.0.1:4222", wantBusCalled: true, wantBusSet: true, customBus: func() pubsub.Bus {
			return &errCloseBus{Bus: pubsub.NewMemoryBus(), closeErr: errors.New("close err")}
		}},
		{name: "successful connection configures bus and starts consumers", natsURL: "nats://127.0.0.1:4222", wantBusCalled: true, wantBusSet: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
				called = true
				if tc.customBus != nil {
					return tc.customBus(), nil
				}
				if tc.wantBusSet {
					return pubsub.NewMemoryBus(), nil
				}
				return nil, tc.busErr
			}
			srv := &RouterServer{}
			stop, err := initSynapseJetStream(context.Background(), srv, tc.natsURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("initSynapseJetStream() err = %v, wantErr = %v", err, tc.wantErr)
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
			if called != tc.wantBusCalled {
				t.Errorf("newBusFn called = %v, want %v", called, tc.wantBusCalled)
			}
			if tc.wantBusSet && srv.EventBus() == nil {
				t.Errorf("expected non-nil event bus on server")
			}
		})
	}
}

type errCoreStreamsBus struct {
	pubsub.Bus

	err error
}

func (b *errCoreStreamsBus) EnsureCoreStreams(ctx context.Context) error {
	return b.err
}

type errSubscribeBus struct {
	pubsub.Bus

	err error
}

func (b *errSubscribeBus) ConsumeQueue(ctx context.Context, subject, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return nil, b.err
}

type errCloseBus struct {
	pubsub.Bus

	closeErr error
}

func (b *errCloseBus) Close() error { return b.closeErr }

type errCoreStreamsAndCloseBus struct {
	pubsub.Bus

	streamErr error
	closeErr  error
}

func (b *errCoreStreamsAndCloseBus) EnsureCoreStreams(ctx context.Context) error {
	return b.streamErr
}

func (b *errCoreStreamsAndCloseBus) Close() error { return b.closeErr }

type errSubscribeAndCloseBus struct {
	pubsub.Bus

	subErr   error
	closeErr error
}

func (b *errSubscribeAndCloseBus) ConsumeQueue(ctx context.Context, subject, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return nil, b.subErr
}

func (b *errSubscribeAndCloseBus) Close() error { return b.closeErr }
