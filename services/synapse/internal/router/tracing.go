package router

import (
	"net/http"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/discord"
)

// tracedIngress wraps a webhook or internal ingress handler in a server span that starts (or
// continues) a trace. Probes and metrics scrapes stay untraced.
func tracedIngress(h http.HandlerFunc) http.Handler {
	return tracing.Middleware("synapse", h)
}

// discordJobSpanOptions configure the consumer span a Discord job opens under the gateway trace
// persisted on its args.
func discordJobSpanOptions(queue string) []trace.SpanStartOption {
	return discord.DiscordJobSpanOptions(queue)
}

// markDiscordJobSpan records the job outcome without error text.
func markDiscordJobSpan(span trace.Span, err error) {
	discord.MarkDiscordJobSpan(span, err)
}
