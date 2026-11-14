package main

import (
	"context"
	"log"

	"github.com/azylman/aerial/pkg/pubsub"
)

var newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url)
}

func initSynapseJetStream(ctx context.Context, server *RouterServer, natsURL string) func() {
	if natsURL == "" {
		return func() {}
	}
	bus, busErr := newBusFn(ctx, natsURL)
	if busErr != nil {
		log.Printf("[synapse] Warning: failed to connect to nats at %s: %v", natsURL, busErr)
		return func() {}
	}
	if streamErr := pubsub.EnsureCoreStreams(ctx, bus); streamErr != nil {
		log.Printf("[synapse] Warning: failed to ensure core streams: %v", streamErr)
	}
	server.SetEventBus(bus)
	return func() {
		if closeErr := bus.Close(); closeErr != nil {
			log.Printf("[synapse] Warning closing event bus: %v", closeErr)
		}
	}
}
