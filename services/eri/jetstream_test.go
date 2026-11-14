package main

import (
	"context"
	"errors"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestEriJetStreamInit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// initEriJetStream empty natsURL
	if bus := initEriJetStream(ctx, ""); bus != nil {
		t.Error("expected nil bus for empty natsURL")
	}

	// initEriJetStreamWithFactory error branch
	errFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return nil, errors.New("nats connect error")
	}
	if bus := initEriJetStreamWithFactory(ctx, "nats://mock", errFactory); bus != nil {
		t.Error("expected nil bus when factory returns error")
	}

	// initEriJetStreamWithFactory success branch
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	successFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return memBus, nil
	}
	if bus := initEriJetStreamWithFactory(ctx, "nats://mock", successFactory); bus == nil {
		t.Error("expected non-nil bus when factory succeeds")
	}
}
