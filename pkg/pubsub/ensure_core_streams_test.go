package pubsub_test

import (
	"context"
	"errors"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
)

func TestEnsureCoreStreamsBus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		bus     pubsub.Bus
		wantErr string
	}{
		{name: "nil bus", bus: nil, wantErr: "pubsub: bus is nil"},
		{name: "bus error propagated", bus: &pubsubtest.FaultBus{Bus: pubsub.NewMemoryBus(), EnsureErr: errors.New("ensure failed")}, wantErr: "ensure failed"},
		{name: "memory bus ok", bus: pubsub.NewMemoryBus()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.bus != nil {
				t.Cleanup(func() { _ = tc.bus.Close() })
			}
			err := pubsub.EnsureCoreStreams(context.Background(), tc.bus)
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if gotErr != tc.wantErr {
				t.Errorf("EnsureCoreStreams() err = %q, want %q", gotErr, tc.wantErr)
			}
		})
	}
}
