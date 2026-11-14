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
	pool := queue.New(nil, queue.WorkerPoolConfig{
		NATSURL: "nats://127.0.0.1:4222",
	})
	cleanupErr := attachEriEventBusWithFactory(ctx, pool, errFactory)
	if cleanupErr != nil {
		cleanupErr()
	}
	if pool.EventBus() != nil {
		t.Error("expected nil event bus on pool when factory fails")
	}

	// attachEriEventBusWithFactory success branch
	cleanupSuccess := attachEriEventBusWithFactory(ctx, pool, successFactory)
	if pool.EventBus() == nil {
		t.Error("expected non-nil event bus on pool when factory succeeds")
	}
	if cleanupSuccess != nil {
		cleanupSuccess()
	}
}

type natsURLResolutionTestCase struct {
	name          string
	envURL        string
	poolURL       string
	expectedURL   string
	expectCleanup bool
}

func TestAttachEriEventBus_DefaultAndEnvURL(t *testing.T) {
	tests := []natsURLResolutionTestCase{
		{
			name:          "returns nil when env and pool empty",
			envURL:        "",
			poolURL:       "",
			expectedURL:   "",
			expectCleanup: false,
		},
		{
			name:          "pool config fallback when env empty",
			envURL:        "",
			poolURL:       "nats://pool-nats:4222",
			expectedURL:   "nats://pool-nats:4222",
			expectCleanup: true,
		},
		{
			name:          "env takes precedence over pool config",
			envURL:        "nats://env-nats:4222",
			poolURL:       "nats://pool-nats:4222",
			expectedURL:   "nats://env-nats:4222",
			expectCleanup: true,
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
			if tc.expectCleanup {
				if cleanup == nil {
					t.Fatalf("expected non-nil cleanup function")
				}
				cleanup()
			} else if cleanup != nil {
				t.Errorf("expected nil cleanup function, got non-nil")
			}
			if capturedURL != tc.expectedURL {
				t.Errorf("capturedURL = %q, want %q", capturedURL, tc.expectedURL)
			}
		})
	}
}

func TestAttachEriEventBus_URLResolution(t *testing.T) {
	TestAttachEriEventBus_DefaultAndEnvURL(t)
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
