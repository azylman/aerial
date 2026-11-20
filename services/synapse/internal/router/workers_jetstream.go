package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

func resolveJetStreamJobCoords(args any) (string, string) {
	return metrics.ResolveJetStreamJobCoords(args)
}

const (
	maxDeliverGitHubWebhook      uint64 = 5
	maxDeliverDeployWebhook      uint64 = 5
	maxDeliverDiscordIngress     uint64 = 30
	maxDeliverDiscordInteraction uint64 = 1
)

func nakBackoffDelay(numDelivered uint64) time.Duration {
	if numDelivered <= 1 {
		return 1 * time.Second
	}
	shift := min(numDelivered-1, 4)
	return time.Duration(1<<shift) * time.Second
}

func termJobIfExhausted(ctx context.Context, msg pubsub.Message, maxDeliver uint64, jobName string) bool {
	if msg.NumDelivered() >= maxDeliver {
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[synapse] [jetstream] error terming job", "job", jobName, "error", termErr)
		}
		return true
	}
	return false
}

func handleJetStreamJob[T any](ctx context.Context, msg pubsub.Message, maxDeliver uint64, jobName string, process func(args T) error) error {
	var args T
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		slog.ErrorContext(ctx, "[synapse] [jetstream] error unmarshaling job", "job", jobName, "error", err)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[synapse] [jetstream] error terming job", "job", jobName, "error", termErr)
		}
		return nil
	}
	if err := process(args); err != nil {
		slog.ErrorContext(ctx, "[synapse] [jetstream] error processing job", "job", jobName, "error", err)
		if termJobIfExhausted(ctx, msg, maxDeliver, jobName) {
			return nil
		}
		if nakErr := msg.NakWithDelay(nakBackoffDelay(msg.NumDelivered())); nakErr != nil {
			slog.WarnContext(ctx, "[synapse] [jetstream] error naking job", "job", jobName, "error", nakErr)
		}
		return err
	}
	return nil
}

type finalAttemptKey struct{}

func withFinalAttempt(ctx context.Context, isFinal bool) context.Context {
	return context.WithValue(ctx, finalAttemptKey{}, isFinal)
}

func isFinalAttemptFromContext(ctx context.Context) bool {
	v, ok := ctx.Value(finalAttemptKey{}).(bool)
	return ok && v
}

// HandleGitHubJob processes an aerial.jobs.synapse.github job message from JetStream,
// marking final delivery attempts so terminal image promotion failures transition PRs to deploy_failed.
func (s *RouterServer) HandleGitHubJob(ctx context.Context, msg pubsub.Message) error {
	isFinal := msg.NumDelivered() >= maxDeliverGitHubWebhook
	jobCtx := withFinalAttempt(ctx, isFinal)
	return handleJetStreamJob(ctx, msg, maxDeliverGitHubWebhook, "github", func(args GitHubWebhookArgs) error {
		slog.InfoContext(ctx, "[synapse] [jetstream] processing github job", "event", args.Event, "delivery", args.Delivery, "final", isFinal)
		_, err := s.ProcessGitHubEvent(jobCtx, args.Event, args.Delivery, args.Body)
		return err
	})
}

// HandleDeployJob processes an aerial.jobs.synapse.deploy job message from JetStream.
func (s *RouterServer) HandleDeployJob(ctx context.Context, msg pubsub.Message) error {
	return handleJetStreamJob(ctx, msg, maxDeliverDeployWebhook, "deploy", func(args DeployWebhookArgs) error {
		slog.InfoContext(ctx, "[synapse] [jetstream] processing deploy job", "event", args.Event.Event, "job_name", args.Event.JobName)
		_, err := s.ProcessDeployEvent(ctx, args.Event)
		return err
	})
}

// HandleDiscordIngressJob processes an aerial.jobs.synapse.discord_ingress job message from JetStream.
// ctx carries the trace context extracted from the NATS message headers; a legacy
// traceparent key left in older job bodies is an unknown JSON field and is ignored.
func (s *RouterServer) HandleDiscordIngressJob(ctx context.Context, msg pubsub.Message) (err error) {
	var args DiscordIngressArgs
	if uErr := json.Unmarshal(msg.Data(), &args); uErr != nil {
		slog.ErrorContext(ctx, "[synapse] [jetstream] error unmarshaling discord ingress job", "error", uErr)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[synapse] [jetstream] error terming discord ingress job", "error", termErr)
		}
		return nil
	}

	ctx, span := tracing.Tracer().Start(ctx, "process "+QueueDiscordIngress, discordJobSpanOptions(QueueDiscordIngress)...)
	defer span.End()
	defer func() { markDiscordJobSpan(span, err) }()

	if termJobIfExhausted(ctx, msg, maxDeliverDiscordIngress, "discord ingress") {
		RecordJobDropped(QueueDiscordIngress, "discord_ingress", "max_retries_exhausted")
		return nil
	}

	start := time.Now()
	delay := s.discordIngressRetryDelay
	if delay <= 0 {
		delay = 2 * time.Second
	} else if delay > 5*time.Second {
		delay = 5 * time.Second
	}
	maxAge := s.discordIngressMaxAge
	if maxAge <= 0 {
		maxAge = 30 * time.Minute
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		isStale := time.Since(start) >= maxAge
		if args.Message != nil && !args.Message.Timestamp.IsZero() && time.Since(args.Message.Timestamp) >= maxAge {
			isStale = true
		}
		if isStale {
			RecordJobDropped(QueueDiscordIngress, "discord_ingress", "ttl_expired")
			if termErr := msg.Term(); termErr != nil {
				slog.WarnContext(ctx, "[synapse] [jetstream] error terming expired discord ingress job", "error", termErr)
			}
			return nil
		}
		pErr := s.ProcessDiscordIngress(ctx, args)
		if pErr == nil {
			return nil
		}
		if is4xxClientError(pErr) {
			RecordJobDropped(QueueDiscordIngress, "discord_ingress", "client_4xx")
			if termErr := msg.Term(); termErr != nil {
				slog.WarnContext(ctx, "[synapse] [jetstream] error terming client 4xx discord ingress job", "error", termErr)
			}
			return nil
		}
		RecordJobRetry(QueueDiscordIngress, "discord_ingress", "discord_ingress_backoff")
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// HandleDiscordInteractionJob processes an aerial.jobs.synapse.discord_interaction job message from JetStream.
// ctx carries the trace context extracted from the NATS message headers.
func (s *RouterServer) HandleDiscordInteractionJob(ctx context.Context, msg pubsub.Message) error {
	var args DiscordInteractionArgs
	if uErr := json.Unmarshal(msg.Data(), &args); uErr != nil {
		slog.ErrorContext(ctx, "[synapse] [jetstream] error unmarshaling discord interaction job", "error", uErr)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[synapse] [jetstream] error terming discord interaction job", "error", termErr)
		}
		return nil
	}
	ctx, span := tracing.Tracer().Start(ctx, "process "+QueueDiscordInteraction, discordJobSpanOptions(QueueDiscordInteraction)...)
	defer span.End()

	var procErr error
	defer func() { markDiscordJobSpan(span, procErr) }()

	if pErr := s.ProcessDiscordInteraction(ctx, args); pErr != nil {
		slog.ErrorContext(ctx, "[synapse] [jetstream] error processing discord interaction job", "error", pErr)
		procErr = pErr
		_ = termJobIfExhausted(ctx, msg, maxDeliverDiscordInteraction, "discord interaction")
		return nil
	}
	return nil
}

// StartJobConsumers registers the durable consumers for Synapse on AERIAL_JOBS.
func (s *RouterServer) StartJobConsumers(ctx context.Context) error {
	if s == nil || s.bus == nil {
		return nil
	}
	type consumerSpec struct {
		subject string
		queue   string
		handler pubsub.Handler
	}
	specs := []consumerSpec{
		{"aerial.jobs.synapse.github", "synapse-github", s.HandleGitHubJob},
		{"aerial.jobs.synapse.deploy", "synapse-deploy", s.HandleDeployJob},
		{"aerial.jobs.synapse.discord_ingress", "synapse-discord-ingress", s.HandleDiscordIngressJob},
		{"aerial.jobs.synapse.discord_interaction", "synapse-discord-interaction", s.HandleDiscordInteractionJob},
	}
	for _, spec := range specs {
		if _, err := s.bus.ConsumeQueue(ctx, spec.subject, spec.queue, spec.handler); err != nil {
			slog.WarnContext(ctx, "[synapse] failed to subscribe", "queue", spec.queue, "error", err)
			return fmt.Errorf("subscribe %s: %w", spec.queue, err)
		}
		slog.InfoContext(ctx, "[synapse] subscribed queue consumer", "queue", spec.queue, "subject", spec.subject)
	}
	return nil
}
