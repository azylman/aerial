package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

var newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url)
}

func initEriJetStreamWithFactory(ctx context.Context, natsURL string, factory func(ctx context.Context, url string) (pubsub.Bus, error)) pubsub.Bus {
	natsURL = strings.TrimSpace(natsURL)
	if natsURL == "" {
		return nil
	}
	b, err := factory(ctx, natsURL)
	if err != nil {
		log.Printf("[WorkerPool] Warning: failed to connect to nats at %s: %v", natsURL, err)
		return nil
	}
	if err := pubsub.EnsureCoreStreams(ctx, b); err != nil {
		log.Printf("[WorkerPool] Warning: failed to ensure core streams: %v", err)
	}
	return b
}

func attachEriEventBus(ctx context.Context, pool *queue.WorkerPool) func() {
	return attachEriEventBusWithFactory(ctx, pool, newBusFn)
}

func attachEriEventBusWithFactory(ctx context.Context, pool *queue.WorkerPool, factory func(ctx context.Context, url string) (pubsub.Bus, error)) func() {
	if pool == nil {
		return func() {}
	}
	natsURL := firstNonEmpty(os.Getenv("NATS_URL"), pool.NATSURL(), "nats://nats.aerial:4222")
	bus := initEriJetStreamWithFactory(ctx, natsURL, factory)
	if bus == nil {
		return func() {}
	}
	pool.SetEventBus(bus)
	return func() {
		if closeErr := bus.Close(); closeErr != nil {
			log.Printf("[WorkerPool] Warning closing event bus: %v", closeErr)
		}
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
