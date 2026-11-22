package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// discordRoutingFor derives per-agent Discord subject routing from cfg's
// configured bots and default owner agent. A nil cfg routes everything to the
// default owner fallback.
func discordRoutingFor(cfg *Config) metrics.DiscordRouting {
	var agents []string
	if cfg != nil {
		for agent := range cfg.Bots {
			agents = append(agents, agent)
		}
	}
	return metrics.NewDiscordRouting(agents, cfg.DefaultOwnerAgent())
}

// discordRouting returns routing for the current config snapshot. Changing
// bots forces a synapse restart, so the snapshot used at consumer startup and
// at publish time agree for the lifetime of the process.
func (s *RouterServer) discordRouting() metrics.DiscordRouting {
	return discordRoutingFor(s.Config())
}

type consumerSpec struct {
	subject string
	queue   string
	handler pubsub.Handler
}

// jobConsumerSpecs lists every AERIAL_JOBS durable synapse subscribes: github,
// deploy, then one ingress and
// one interaction durable per agent in sorted order. Filter subjects are exact
// (no wildcards), so they never overlap on the WorkQueue stream.
func (s *RouterServer) jobConsumerSpecs(routing metrics.DiscordRouting) []consumerSpec {
	agents := routing.Agents()
	specs := make([]consumerSpec, 0, 2+2*len(agents))
	specs = append(specs,
		consumerSpec{"aerial.jobs.synapse.github", "synapse-github", s.HandleGitHubJob},
		consumerSpec{"aerial.jobs.synapse.deploy", "synapse-deploy", s.HandleDeployJob},
	)
	for _, agent := range agents {
		specs = append(specs,
			consumerSpec{metrics.DiscordIngressSubject(agent), metrics.DiscordIngressDurable(agent), s.HandleDiscordIngressJob},
			consumerSpec{metrics.DiscordInteractionSubject(agent), metrics.DiscordInteractionDurable(agent), s.HandleDiscordInteractionJob},
		)
	}
	return specs
}

const (
	maxDeliverGitHubWebhook      uint64 = 5
	maxDeliverDeployWebhook      uint64 = 5
	maxDeliverDiscordIngress     uint64 = 30
	maxDeliverDiscordInteraction uint64 = 1
)

// Restate send retry budget: prstate owns every event's effects, so a
// Restate outage must not lose one. 45 deliveries on a backoff capped at one
// minute span about 40 minutes (1+2+4+8+16+32 s, then 39 x 60 s), outlasting
// a 30-minute outage. Jobs failing for any other reason keep the short
// budget above.
const (
	maxDeliverRestate         uint64 = 45
	maxRestateBackoff                = time.Minute
	maxNakBackoff                    = 16 * time.Second
	restateTerminalDropReason        = "restate_terminal"
)

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
		metrics.RecordJobDropped(metrics.QueueDefault, jobName+"_webhook", "malformed")
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[synapse] [jetstream] error terming job", "job", jobName, "error", termErr)
		}
		return nil
	}
	if err := process(args); err != nil {
		slog.ErrorContext(ctx, "[synapse] [jetstream] error processing job", "job", jobName, "error", err)
		delay := pubsub.NakBackoff(msg.NumDelivered(), maxNakBackoff)
		switch restateFailed, terminal := restateSendFailure(err); {
		case terminal:
			metrics.RecordJobDropped(metrics.QueueDefault, jobName+"_webhook", restateTerminalDropReason)
			slog.ErrorContext(ctx, "[synapse] [jetstream] dropping job after terminal restate rejection", "job", jobName, "error", err)
			if termErr := msg.Term(); termErr != nil {
				slog.WarnContext(ctx, "[synapse] [jetstream] error terming job", "job", jobName, "error", termErr)
			}
			return nil
		case restateFailed:
			maxDeliver = maxDeliverRestate
			delay = pubsub.NakBackoff(msg.NumDelivered(), maxRestateBackoff)
		}
		if termJobIfExhausted(ctx, msg, maxDeliver, jobName) {
			metrics.RecordJobDropped(metrics.QueueDefault, jobName+"_webhook", "max_retries_exhausted")
			return nil
		}
		if nakErr := msg.NakWithDelay(delay); nakErr != nil {
			slog.WarnContext(ctx, "[synapse] [jetstream] error naking job", "job", jobName, "error", nakErr)
		}
		return err
	}
	return nil
}

// HandleGitHubJob processes an aerial.jobs.synapse.github job message from JetStream.
func (s *RouterServer) HandleGitHubJob(ctx context.Context, msg pubsub.Message) error {
	return handleJetStreamJob(ctx, msg, maxDeliverGitHubWebhook, "github", func(args GitHubWebhookArgs) error {
		jobCtx := restateclient.WithEventIdentity(ctx, eventIdentity("github", args.Delivery))
		slog.InfoContext(ctx, "[synapse] [jetstream] processing github job", "event", args.Event, "delivery", args.Delivery)
		_, err := s.ProcessGitHubEvent(jobCtx, args.Event, args.Delivery, args.Body)
		return err
	})
}

// HandleDeployJob processes an aerial.jobs.synapse.deploy job message from JetStream.
func (s *RouterServer) HandleDeployJob(ctx context.Context, msg pubsub.Message) error {
	return handleJetStreamJob(ctx, msg, maxDeliverDeployWebhook, "deploy", func(args DeployWebhookArgs) error {
		slog.InfoContext(ctx, "[synapse] [jetstream] processing deploy job", "event", args.Event.Event, "job_name", args.Event.JobName)
		_, err := s.ProcessDeployEvent(restateclient.WithEventIdentity(ctx, eventIdentity("deploy", args.IdempotencyKey)), args.Event)
		return err
	})
}

// eventIdentity scopes Restate idempotency keys to one inbound event of a
// source (a GitHub delivery ID or a deploy idempotency key). An empty id
// yields no identity, so sends fall back to payload-digest keys.
func eventIdentity(source, id string) string {
	if id = strings.TrimSpace(id); id == "" {
		return ""
	}
	return source + ":" + id
}

// HandleDiscordIngressJob processes an aerial.jobs.synapse.discord_ingress[.<agent>] job message from JetStream.
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
		metrics.RecordJobDropped(QueueDiscordIngress, "discord_ingress", "max_retries_exhausted")
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
			metrics.RecordJobDropped(QueueDiscordIngress, "discord_ingress", "ttl_expired")
			if termErr := msg.Term(); termErr != nil {
				slog.WarnContext(ctx, "[synapse] [jetstream] error terming expired discord ingress job", "error", termErr)
			}
			return nil
		}
		pErr := s.ProcessDiscordIngress(ctx, args)
		if pErr == nil {
			return nil
		}
		if model.Is4xxClientError(pErr) {
			metrics.RecordJobDropped(QueueDiscordIngress, "discord_ingress", "client_4xx")
			if termErr := msg.Term(); termErr != nil {
				slog.WarnContext(ctx, "[synapse] [jetstream] error terming client 4xx discord ingress job", "error", termErr)
			}
			return nil
		}
		metrics.RecordJobRetry(QueueDiscordIngress, "discord_ingress", "discord_ingress_backoff")
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// HandleDiscordInteractionJob processes an aerial.jobs.synapse.discord_interaction[.<agent>] job message from JetStream.
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
	for _, spec := range s.jobConsumerSpecs(s.discordRouting()) {
		if _, err := s.bus.ConsumeQueue(ctx, spec.subject, spec.queue, spec.handler); err != nil {
			slog.WarnContext(ctx, "[synapse] failed to subscribe", "queue", spec.queue, "error", err)
			return fmt.Errorf("subscribe %s: %w", spec.queue, err)
		}
		slog.InfoContext(ctx, "[synapse] subscribed queue consumer", "queue", spec.queue, "subject", spec.subject)
	}
	return nil
}
