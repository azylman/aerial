package main

import (
	"context"
	"errors"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

func TestEriJetStreamInit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	errFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return nil, errors.New("nats connect error")
	}

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	successFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return memBus, nil
	}

	// attachEriEventBusWithFactory error branch
	pool := queue.New(nil, queue.WorkerPoolConfig{})
	cleanupErr := attachEriEventBusWithFactory(ctx, pool, errFactory)
	cleanupErr()
	if pool.EventBus() != nil {
		t.Error("expected nil event bus on pool when factory fails")
	}

	// attachEriEventBusWithFactory success branch
	cleanupSuccess := attachEriEventBusWithFactory(ctx, pool, successFactory)
	if pool.EventBus() == nil {
		t.Error("expected non-nil event bus on pool when factory succeeds")
	}
	cleanupSuccess()
}

type natsURLResolutionTestCase struct {
	name        string
	envURL      string
	poolURL     string
	expectedURL string
}

func TestAttachEriEventBus_URLResolution(t *testing.T) {
	tests := []natsURLResolutionTestCase{
		{
			name:        "default fallback when env and pool empty",
			envURL:      "",
			poolURL:     "",
			expectedURL: "nats://nats.aerial:4222",
		},
		{
			name:        "pool config fallback when env empty",
			envURL:      "",
			poolURL:     "nats://pool-nats:4222",
			expectedURL: "nats://pool-nats:4222",
		},
		{
			name:        "env takes precedence over pool config",
			envURL:      "nats://env-nats:4222",
			poolURL:     "nats://pool-nats:4222",
			expectedURL: "nats://env-nats:4222",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NATS_URL", tc.envURL)
			var capturedURL string
			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			capturingFactory := func(_ context.Context, u string) (pubsub.Bus, error) {
				capturedURL = u
				return memBus, nil
			}
			pool := queue.New(nil, queue.WorkerPoolConfig{
				NATSURL: tc.poolURL,
			})
			cleanup := attachEriEventBusWithFactory(context.Background(), pool, capturingFactory)
			cleanup()
			if capturedURL != tc.expectedURL {
				t.Errorf("capturedURL = %q, want %q", capturedURL, tc.expectedURL)
			}
		})
	}
}

func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		inputs   []string
		expected string
	}{
		{
			name:     "all empty",
			inputs:   []string{"", "   ", "\t"},
			expected: "",
		},
		{
			name:     "first non empty selected",
			inputs:   []string{"", "  hello  ", "world"},
			expected: "hello",
		},
		{
			name:     "no inputs",
			inputs:   nil,
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := firstNonEmpty(tc.inputs...); got != tc.expected {
				t.Errorf("firstNonEmpty(%v) = %q, want %q", tc.inputs, got, tc.expected)
			}
		})
	}
}
