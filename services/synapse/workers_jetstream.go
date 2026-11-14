package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

func resolveJetStreamJobCoords(args any) (string, string) {
	switch a := args.(type) {
	case GitHubWebhookArgs:
		return "aerial.jobs.synapse.github", "github:" + a.Delivery
	case *GitHubWebhookArgs:
		return "aerial.jobs.synapse.github", "github:" + a.Delivery
	case DeployWebhookArgs:
		return "aerial.jobs.synapse.deploy", "deploy:" + a.IdempotencyKey
	case *DeployWebhookArgs:
		return "aerial.jobs.synapse.deploy", "deploy:" + a.IdempotencyKey
	case MergeConflictArgs:
		bucket := time.Now().Unix() / 900
		return "aerial.jobs.synapse.merge_conflict", fmt.Sprintf("conflict:%s:%d:%d", a.Repo, a.PRNumber, bucket)
	case *MergeConflictArgs:
		bucket := time.Now().Unix() / 900
		return "aerial.jobs.synapse.merge_conflict", fmt.Sprintf("conflict:%s:%d:%d", a.Repo, a.PRNumber, bucket)
	case DiscordIngressArgs:
		return "aerial.jobs.synapse.discord_ingress", "discord:msg:" + a.MessageID
	case *DiscordIngressArgs:
		return "aerial.jobs.synapse.discord_ingress", "discord:msg:" + a.MessageID
	case DiscordInteractionArgs:
		return "aerial.jobs.synapse.discord_interaction", "discord:ix:" + a.InteractionID
	case *DiscordInteractionArgs:
		return "aerial.jobs.synapse.discord_interaction", "discord:ix:" + a.InteractionID
	case WidgetSyncArgs:
		if strings.TrimSpace(a.BuildID) != "" {
			return "aerial.coalesced.widget_sync.build." + strings.TrimSpace(a.BuildID),
				fmt.Sprintf("widget:build:%s:%d", strings.TrimSpace(a.BuildID), time.Now().UnixNano())
		}
		repo := strings.ReplaceAll(normalizeRepo(a.Repo), "/", ".")
		return fmt.Sprintf("aerial.coalesced.widget_sync.pr.%s.%d", repo, a.PRNumber),
			fmt.Sprintf("widget:pr:%s:%d:%d", normalizeRepo(a.Repo), a.PRNumber, time.Now().UnixNano())
	case *WidgetSyncArgs:
		if strings.TrimSpace(a.BuildID) != "" {
			return "aerial.coalesced.widget_sync.build." + strings.TrimSpace(a.BuildID),
				fmt.Sprintf("widget:build:%s:%d", strings.TrimSpace(a.BuildID), time.Now().UnixNano())
		}
		repo := strings.ReplaceAll(normalizeRepo(a.Repo), "/", ".")
		return fmt.Sprintf("aerial.coalesced.widget_sync.pr.%s.%d", repo, a.PRNumber),
			fmt.Sprintf("widget:pr:%s:%d:%d", normalizeRepo(a.Repo), a.PRNumber, time.Now().UnixNano())
	default:
		return "", ""
	}
}

const (
	maxDeliverGitHubWebhook      uint64 = 5
	maxDeliverDeployWebhook      uint64 = 5
	maxDeliverMergeConflict      uint64 = 5
	maxDeliverDiscordIngress     uint64 = 30
	maxDeliverDiscordInteraction uint64 = 1
	maxDeliverWidgetSync         uint64 = 5
)

func nakBackoffDelay(numDelivered uint64) time.Duration {
	if numDelivered <= 1 {
		return 1 * time.Second
	}
	shift := min(numDelivered-1, 4)
	return time.Duration(1<<shift) * time.Second
}

func termJobIfExhausted(msg pubsub.Message, maxDeliver uint64, jobName string) bool {
	if msg.NumDelivered() >= maxDeliver {
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[synapse] [jetstream] warning: error terming %s job: %v", jobName, termErr)
		}
		return true
	}
	return false
}

func handleJetStreamJob[T any](msg pubsub.Message, maxDeliver uint64, jobName string, process func(args T) error) error {
	var args T
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		log.Printf("[synapse] [jetstream] error unmarshaling %s job: %v", jobName, err)
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[synapse] [jetstream] warning: error terming %s job: %v", jobName, termErr)
		}
		return nil
	}
	if err := process(args); err != nil {
		log.Printf("[synapse] [jetstream] error processing %s job: %v", jobName, err)
		if termJobIfExhausted(msg, maxDeliver, jobName) {
			return nil
		}
		if nakErr := msg.NakWithDelay(nakBackoffDelay(msg.NumDelivered())); nakErr != nil {
			log.Printf("[synapse] [jetstream] warning: error naking %s job: %v", jobName, nakErr)
		}
		return err
	}
	return nil
}

// HandleGitHubJob processes an aerial.jobs.synapse.github job message from JetStream.
func (s *RouterServer) HandleGitHubJob(ctx context.Context, msg pubsub.Message) error {
	return handleJetStreamJob(msg, maxDeliverGitHubWebhook, "github", func(args GitHubWebhookArgs) error {
		log.Printf("[synapse] [jetstream] processing github job event=%s delivery=%s", args.Event, args.Delivery)
		_, err := s.ProcessGitHubEvent(ctx, args.Event, args.Delivery, args.Body)
		return err
	})
}

// HandleDeployJob processes an aerial.jobs.synapse.deploy job message from JetStream.
func (s *RouterServer) HandleDeployJob(ctx context.Context, msg pubsub.Message) error {
	return handleJetStreamJob(msg, maxDeliverDeployWebhook, "deploy", func(args DeployWebhookArgs) error {
		log.Printf("[synapse] [jetstream] processing deploy job event=%s job_name=%s", args.Event.Event, args.Event.JobName)
		_, err := s.ProcessDeployEvent(ctx, args.Event)
		return err
	})
}

// HandleMergeConflictJob processes an aerial.jobs.synapse.merge_conflict job message from JetStream.
func (s *RouterServer) HandleMergeConflictJob(ctx context.Context, msg pubsub.Message) error {
	return handleJetStreamJob(msg, maxDeliverMergeConflict, "merge conflict", func(args MergeConflictArgs) error {
		log.Printf("[synapse] [jetstream] processing merge conflict job repo=%s pr=%d", args.Repo, args.PRNumber)
		return s.ProcessMergeConflict(ctx, args)
	})
}

// HandleDiscordIngressJob processes an aerial.jobs.synapse.discord_ingress job message from JetStream.
func (s *RouterServer) HandleDiscordIngressJob(ctx context.Context, msg pubsub.Message) (err error) {
	var args DiscordIngressArgs
	if uErr := json.Unmarshal(msg.Data(), &args); uErr != nil {
		log.Printf("[synapse] [jetstream] error unmarshaling discord ingress job: %v", uErr)
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[synapse] [jetstream] warning: error terming discord ingress job: %v", termErr)
		}
		return nil
	}

	ctx, span := tracing.Tracer().Start(tracing.ContextWithTraceparent(ctx, args.Traceparent), "process "+QueueDiscordIngress, discordJobSpanOptions(QueueDiscordIngress)...)
	defer span.End()
	defer func() { markDiscordJobSpan(span, err) }()

	if termJobIfExhausted(msg, maxDeliverDiscordIngress, "discord ingress") {
		RecordJobDropped(QueueDiscordIngress, DiscordIngressArgs{}.Kind(), "max_retries_exhausted")
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
			RecordJobDropped(QueueDiscordIngress, DiscordIngressArgs{}.Kind(), "ttl_expired")
			if termErr := msg.Term(); termErr != nil {
				log.Printf("[synapse] [jetstream] warning: error terming expired discord ingress job: %v", termErr)
			}
			return nil
		}
		pErr := s.ProcessDiscordIngress(ctx, args)
		if pErr == nil {
			return nil
		}
		if is4xxClientError(pErr) {
			RecordJobDropped(QueueDiscordIngress, DiscordIngressArgs{}.Kind(), "client_4xx")
			if termErr := msg.Term(); termErr != nil {
				log.Printf("[synapse] [jetstream] warning: error terming client 4xx discord ingress job: %v", termErr)
			}
			return nil
		}
		RecordJobRetry(QueueDiscordIngress, DiscordIngressArgs{}.Kind(), "discord_ingress_backoff")
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// HandleDiscordInteractionJob processes an aerial.jobs.synapse.discord_interaction job message from JetStream.
func (s *RouterServer) HandleDiscordInteractionJob(ctx context.Context, msg pubsub.Message) error {
	var args DiscordInteractionArgs
	if uErr := json.Unmarshal(msg.Data(), &args); uErr != nil {
		log.Printf("[synapse] [jetstream] error unmarshaling discord interaction job: %v", uErr)
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[synapse] [jetstream] warning: error terming discord interaction job: %v", termErr)
		}
		return nil
	}
	ctx, span := tracing.Tracer().Start(tracing.ContextWithTraceparent(ctx, args.Traceparent), "process "+QueueDiscordInteraction, discordJobSpanOptions(QueueDiscordInteraction)...)
	defer span.End()

	var procErr error
	defer func() { markDiscordJobSpan(span, procErr) }()

	if pErr := s.ProcessDiscordInteraction(ctx, args); pErr != nil {
		log.Printf("[synapse] [jetstream] error processing discord interaction job: %v", pErr)
		procErr = pErr
		_ = termJobIfExhausted(msg, maxDeliverDiscordInteraction, "discord interaction")
		return nil
	}
	return nil
}

// HandleWidgetSyncJob processes an aerial.coalesced.widget_sync.> job message from JetStream.
func (s *RouterServer) HandleWidgetSyncJob(ctx context.Context, msg pubsub.Message) error {
	return handleJetStreamJob(msg, maxDeliverWidgetSync, "widget sync", func(args WidgetSyncArgs) error {
		log.Printf("[synapse] [jetstream] processing widget sync job repo=%s pr=%d build_id=%s", args.Repo, args.PRNumber, args.BuildID)
		return s.ProcessWidgetSync(ctx, args)
	})
}

// StartJobConsumers registers the 6 durable consumers for Synapse on AERIAL_JOBS and AERIAL_COALESCED.
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
		{"aerial.jobs.synapse.merge_conflict", "synapse-merge-conflict", s.HandleMergeConflictJob},
		{"aerial.jobs.synapse.discord_ingress", "synapse-discord-ingress", s.HandleDiscordIngressJob},
		{"aerial.jobs.synapse.discord_interaction", "synapse-discord-interaction", s.HandleDiscordInteractionJob},
		{"aerial.coalesced.widget_sync.>", "synapse-widget-sync", s.HandleWidgetSyncJob},
	}
	for _, spec := range specs {
		if _, err := s.bus.ConsumeQueue(ctx, spec.subject, spec.queue, spec.handler); err != nil {
			log.Printf("[synapse] warning: failed to subscribe %s: %v", spec.queue, err)
			return fmt.Errorf("subscribe %s: %w", spec.queue, err)
		}
		log.Printf("[synapse] Subscribed %s queue consumer to %s", spec.queue, spec.subject)
	}
	return nil
}
