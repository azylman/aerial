package main

import (
	"context"
	"fmt"
	"log"

	"github.com/azylman/aerial/pkg/pubsub"
)

var newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url)
}

func initSynapseJetStream(ctx context.Context, server *RouterServer, natsURL string) (func(), error) {
	if natsURL == "" {
		return func() {}, nil
	}
	bus, busErr := newBusFn(ctx, natsURL)
	if busErr != nil {
		return nil, fmt.Errorf("connect to nats at %s: %w", natsURL, busErr)
	}
	if streamErr := pubsub.EnsureCoreStreams(ctx, bus); streamErr != nil {
		if closeErr := bus.Close(); closeErr != nil {
			log.Printf("[synapse] Warning closing event bus: %v", closeErr)
		}
		return nil, fmt.Errorf("ensure core streams: %w", streamErr)
	}
	server.SetEventBus(bus)
	if err := server.StartJobConsumers(ctx); err != nil {
		if closeErr := bus.Close(); closeErr != nil {
			log.Printf("[synapse] Warning closing event bus: %v", closeErr)
		}
		server.SetEventBus(nil)
		return nil, fmt.Errorf("start job consumers: %w", err)
	}
	return func() {
		if closeErr := bus.Close(); closeErr != nil {
			log.Printf("[synapse] Warning closing event bus: %v", closeErr)
		}
	}, nil
}
