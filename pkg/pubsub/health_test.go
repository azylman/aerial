package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamBusOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		extra                []nats.Option
		wantMaxReconnect     int
		wantRetryOnFailed    bool
		wantIgnoreAuthAbort  bool
		wantClosedHandlerSet bool
	}{
		{"defaults", nil, -1, true, true, true},
		{"caller overrides retry on failed connect", []nats.Option{nats.RetryOnFailedConnect(false)}, -1, false, true, true},
		{"caller overrides max reconnects", []nats.Option{nats.MaxReconnects(5)}, 5, true, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := nats.GetDefaultOptions()
			for _, opt := range jetStreamBusOptions(tc.extra...) {
				if err := opt(&o); err != nil {
					t.Fatalf("apply option: %v", err)
				}
			}
			if o.MaxReconnect != tc.wantMaxReconnect {
				t.Errorf("MaxReconnect = %d, want %d", o.MaxReconnect, tc.wantMaxReconnect)
			}
			if o.RetryOnFailedConnect != tc.wantRetryOnFailed {
				t.Errorf("RetryOnFailedConnect = %v, want %v", o.RetryOnFailedConnect, tc.wantRetryOnFailed)
			}
			if o.IgnoreAuthErrorAbort != tc.wantIgnoreAuthAbort {
				t.Errorf("IgnoreAuthErrorAbort = %v, want %v", o.IgnoreAuthErrorAbort, tc.wantIgnoreAuthAbort)
			}
			if (o.ClosedCB != nil) != tc.wantClosedHandlerSet {
				t.Errorf("ClosedCB set = %v, want %v", o.ClosedCB != nil, tc.wantClosedHandlerSet)
			}
		})
	}
}

func newMockDialedBus(t *testing.T) *JetStreamBus {
	t.Helper()
	bus, err := NewJetStreamBus(context.Background(), "nats://127.0.0.1:4222",
		nats.SetCustomDialer(&mockNatsDialer{}), nats.Timeout(1*time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatalf("NewJetStreamBus with mock dialer: %v", err)
	}
	return bus
}

func TestJetStreamBus_Healthy(t *testing.T) {
	t.Parallel()

	openBus := newMockDialedBus(t)
	t.Cleanup(func() { _ = openBus.Close() })
	closedBus := newMockDialedBus(t)
	_ = closedBus.Close()

	tests := []struct {
		name       string
		bus        Bus
		wantErr    bool
		wantClosed bool
	}{
		{"nil bus", nil, true, false},
		{"nil jetstream bus", (*JetStreamBus)(nil), true, false},
		{"bus without connection", newJetStreamBusWithContext(nil, &mockJetStream{}), true, false},
		{"open connection", openBus, false, false},
		{"permanently closed connection", closedBus, true, true},
		{"memory bus without health checker", NewMemoryBus(), false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := CheckHealth(tc.bus)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CheckHealth err = %v, wantErr %v", err, tc.wantErr)
			}
			if errors.Is(err, ErrConnectionClosed) != tc.wantClosed {
				t.Errorf("errors.Is(err, ErrConnectionClosed) = %v, want %v (err %v)", errors.Is(err, ErrConnectionClosed), tc.wantClosed, err)
			}
		})
	}
}
