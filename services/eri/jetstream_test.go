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
