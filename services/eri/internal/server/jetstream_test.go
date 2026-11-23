package server

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

	// attachEriEventBus with nil pool
	nilCleanup := attachEriEventBus(ctx, nil)
	if nilCleanup == nil {
		t.Error("expected non-nil no-op cleanup for nil pool")
	} else {
		nilCleanup()
	}

	// attachEriEventBus with empty pool
	emptyPool := queue.New(nil, queue.WorkerPoolConfig{NATSURL: ""})
	emptyCleanup := attachEriEventBus(ctx, emptyPool)
	if emptyCleanup == nil {
		t.Error("expected non-nil no-op cleanup for unconfigured pool")
	} else {
		emptyCleanup()
	}

	// attachEriEventBus with the default factory and an unparseable URL: the
	// connect fails before any dial, leaving the pool without a bus.
	badURLPool := queue.New(nil, queue.WorkerPoolConfig{NATSURL: "nats://bad%zz"})
	badURLCleanup := attachEriEventBus(ctx, badURLPool)
	if badURLCleanup == nil {
		t.Error("expected non-nil no-op cleanup when the default factory fails")
	} else {
		badURLCleanup()
	}
	if badURLPool.EventBus() != nil {
		t.Error("expected nil event bus when the default factory fails")
	}

	// attachEriEventBusWithFactory cleanup still closes a bus whose Close errors
	closeErrPool := queue.New(nil, queue.WorkerPoolConfig{NATSURL: "nats://close-err"})
	closeBus := &closeErrBus{Bus: pubsub.NewMemoryBus()}
	closeErrCleanup := attachEriEventBusWithFactory(ctx, closeErrPool, func(_ context.Context, _ string) (pubsub.Bus, error) {
		return closeBus, nil
	})
	if closeErrCleanup == nil {
		t.Fatal("expected cleanup for an attached bus")
	}
	closeErrCleanup()
	if !closeBus.closed {
		t.Error("expected cleanup to call Close on the attached bus")
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

	// initEriJetStreamWithFactory empty url
	if b := initEriJetStreamWithFactory(ctx, "", nil); b != nil {
		t.Error("expected nil bus for empty natsURL")
	}

	// initEriJetStreamWithFactory with EnsureCoreStreams failure
	failBus := &failEnsureBus{Bus: memBus}
	if b := initEriJetStreamWithFactory(ctx, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return failBus, nil
	}); b == nil {
		t.Error("expected non-nil bus despite EnsureCoreStreams error")
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

type closeErrBus struct {
	pubsub.Bus

	closed bool
}

func (b *closeErrBus) Close() error {
	b.closed = true
	_ = b.Bus.Close()
	return errors.New("simulated close error")
}

type failEnsureBus struct {
	pubsub.Bus
}

func (*failEnsureBus) EnsureCoreStreams(_ context.Context) error {
	return errors.New("simulated ensure error")
}

type natsURLResolutionTestCase struct {
	name          string
	poolURL       string
	expectedURL   string
	expectCleanup bool
}

func TestAttachEriEventBus_DefaultAndEnvURL(t *testing.T) {
	t.Parallel()

	tests := []natsURLResolutionTestCase{
		{
			name:          "returns nil when pool url empty",
			poolURL:       "",
			expectedURL:   "",
			expectCleanup: false,
		},
		{
			name:          "returns nil when pool url whitespace",
			poolURL:       "   ",
			expectedURL:   "",
			expectCleanup: false,
		},
		{
			name:          "pool config url connects",
			poolURL:       "nats://pool-nats:4222",
			expectedURL:   "nats://pool-nats:4222",
			expectCleanup: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
