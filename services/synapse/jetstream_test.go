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

	t.Run("empty nats url returns noop", func(t *testing.T) {
		stop := initSynapseJetStream(context.Background(), &RouterServer{}, "")
		if stop == nil {
			t.Fatal("expected non-nil stop func")
		}
		stop()
	})

	t.Run("connect error returns noop", func(t *testing.T) {
		newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
			return nil, errors.New("connection refused")
		}
		stop := initSynapseJetStream(context.Background(), &RouterServer{}, "nats://127.0.0.1:4222")
		if stop == nil {
			t.Fatal("expected non-nil stop func")
		}
		stop()
	})
}
