package main

import (
	"net/http"

	"github.com/azylman/aerial/pkg/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracedIngress wraps a webhook or internal ingress handler in a server span that starts (or
// continues) a trace. Probes and metrics scrapes stay untraced.
func tracedIngress(h http.HandlerFunc) http.Handler {
	return tracing.Middleware("synapse", h)
}

// gatewaySpanOptions configure the root span of a turn's trace, opened when synapse receives a
// Discord gateway event. They carry only the channel id, never message content.
func gatewaySpanOptions(channelID string) []trace.SpanStartOption {
	return []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "discord"),
			attribute.String("discord.channel.id", channelID),
		),
	}
}

// discordJobSpanOptions configure the consumer span a Discord job opens under the gateway trace
// persisted on its args. It covers River pickup through every retry until eri accepts the event,
// so queue wait shows as the gap between the gateway span and this one.
func discordJobSpanOptions(queue string) []trace.SpanStartOption {
	return []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "river"),
			attribute.String("messaging.destination.name", queue),
		),
	}
}

// markDiscordJobSpan records the job outcome without error text, which can echo eri response bodies.
func markDiscordJobSpan(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "eri dispatch failed")
	}
}
