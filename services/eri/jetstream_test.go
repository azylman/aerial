package main

import (
	"context"
	"errors"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

type errCloseBus struct {
	pubsub.Bus
}

func (e *errCloseBus) Close() error {
	_ = e.Bus.Close()
	return errors.New("close err")
}

func TestEriJetStreamInit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Empty URL returns nil
	if b := initEriJetStreamWithFactory(ctx, "   ", nil); b != nil {
		t.Error("expected nil bus when url is empty")
	}

	// Nil pool returns no-op cleanup
	nilPoolCleanup := attachEriEventBusWithFactory(ctx, nil, nil)
	nilPoolCleanup()

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

	// Close error branch
	closeErrPool := queue.New(nil, queue.WorkerPoolConfig{})
	closeErrFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return &errCloseBus{Bus: pubsub.NewMemoryBus()}, nil
	}
	cleanupCloseErr := attachEriEventBusWithFactory(ctx, closeErrPool, closeErrFactory)
	cleanupCloseErr()

	// Default attachEriEventBus with err factory replacement
	origFactory := newBusFn
	defer func() { newBusFn = origFactory }()
	newBusFn = errFactory
	defCleanup := attachEriEventBus(ctx, pool)
	defCleanup()
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
