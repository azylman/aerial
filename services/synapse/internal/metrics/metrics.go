package metrics

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	unknownLabel = "unknown"
	labelQueue   = "queue"
	fieldKind    = "kind"
	fieldStatus  = "status"
	fieldReason  = "reason"

	statusPending = "pending"

	QueueDefault            = "default"
	QueueDiscordIngress     = "discord_ingress"
	QueueDiscordInteraction = "discord_interaction"
)

// Metric collectors for JetStream queues and worker execution telemetry.
var (
	JobsEnqueuedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_jobs_enqueued_total",
			Help: "Total number of jobs enqueued, skipped as duplicate, or failed on enqueue.",
		},
		[]string{labelQueue, fieldKind, fieldStatus},
	)

	JobsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_jobs_processed_total",
			Help: "Total number of jobs processed by worker execution status.",
		},
		[]string{labelQueue, fieldKind, fieldStatus},
	)

	JobDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "synapse_job_duration_seconds",
			Help:    "Execution duration of jobs in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{labelQueue, fieldKind},
	)

	JobRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_job_retries_total",
			Help: "Total number of job retry attempts by reason.",
		},
		[]string{labelQueue, fieldKind, fieldReason},
	)

	JobsDroppedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_jobs_dropped_total",
			Help: "Total number of jobs dropped or cancelled by reason.",
		},
		[]string{labelQueue, fieldKind, fieldReason},
	)

	QueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "synapse_queue_depth",
			Help: "Current depth of queues partitioned by state.",
		},
		[]string{labelQueue, "state"},
	)

	DiscordGatewayLatencySeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "synapse_discord_gateway_latency_seconds",
			Help: "Heartbeat latency of the Discord Gateway connection in seconds.",
		},
	)

	// RestateShadowSendsTotal counts dark shadow requests mirrored at the
	// Restate PRObject/RepoObject ingress, partitioned by handler and outcome.
	// Shadow failures always fail closed; the legacy path stays authoritative.
	RestateShadowSendsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_restate_shadow_sends_total",
			Help: "Total number of dark shadow requests mirrored at Restate ingress by outcome.",
		},
		[]string{"handler", fieldStatus},
	)

	// RestateShadowDivergenceTotal counts first-touch hydration mismatches
	// between live events and the pr_registry projection. The cutover gate
	// requires this to stay zero.
	RestateShadowDivergenceTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_restate_shadow_divergence_total",
			Help: "Total number of dark shadow hydration divergences from the registry projection by reason.",
		},
		[]string{fieldReason},
	)
)

var (
	knownQueues = []string{
		QueueDefault,
		QueueDiscordIngress,
		QueueDiscordInteraction,
	}
	knownStates = []string{statusPending, "in_flight"}
)

func registerCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(err)
		}
	}
}

func init() {
	registerCollector(RestateShadowSendsTotal)
	registerCollector(RestateShadowDivergenceTotal)
	registerCollector(JobsEnqueuedTotal)
	registerCollector(JobsProcessedTotal)
	registerCollector(JobDurationSeconds)
	registerCollector(JobRetriesTotal)
	registerCollector(JobsDroppedTotal)
	registerCollector(QueueDepth)
	registerCollector(DiscordGatewayLatencySeconds)

	for _, q := range knownQueues {
		for _, s := range knownStates {
			QueueDepth.WithLabelValues(q, s).Set(0)
		}
	}
	DiscordGatewayLatencySeconds.Set(0)
}

// RecordJobEnqueued increments the JobsEnqueuedTotal counter for the specified queue, kind, and status.
func RecordJobEnqueued(queue, kind, status string) {
	if queue == "" {
		queue = QueueDefault
	}
	if kind == "" {
		kind = unknownLabel
	}
	if status == "" {
		status = unknownLabel
	}
	JobsEnqueuedTotal.WithLabelValues(queue, kind, status).Inc()
}

// RecordJobRetry increments the JobRetriesTotal counter for the specified queue, kind, and reason.
func RecordJobRetry(queue, kind, reason string) {
	if queue == "" {
		queue = QueueDefault
	}
	if kind == "" {
		kind = unknownLabel
	}
	if reason == "" {
		reason = unknownLabel
	}
	JobRetriesTotal.WithLabelValues(queue, kind, reason).Inc()
}

// RecordJobDropped increments the JobsDroppedTotal counter for the specified queue, kind, and reason.
func RecordJobDropped(queue, kind, reason string) {
	if queue == "" {
		queue = QueueDefault
	}
	if kind == "" {
		kind = unknownLabel
	}
	if reason == "" {
		reason = unknownLabel
	}
	JobsDroppedTotal.WithLabelValues(queue, kind, reason).Inc()
}

// RecordRestateShadowSend increments the RestateShadowSendsTotal counter for
// the specified frozen handler and outcome ("sent" or "error").
func RecordRestateShadowSend(handler, status string) {
	if handler == "" {
		handler = unknownLabel
	}
	if status == "" {
		status = unknownLabel
	}
	RestateShadowSendsTotal.WithLabelValues(handler, status).Inc()
}

// RecordRestateShadowDivergence increments the RestateShadowDivergenceTotal
// counter for the specified mismatch reason.
func RecordRestateShadowDivergence(reason string) {
	if reason == "" {
		reason = unknownLabel
	}
	RestateShadowDivergenceTotal.WithLabelValues(reason).Inc()
}

// SetDiscordGatewayLatency sets the DiscordGatewayLatencySeconds gauge.
func SetDiscordGatewayLatency(seconds float64) {
	DiscordGatewayLatencySeconds.Set(seconds)
}

// ConsumerStatsQuerier abstracts JetStream consumer statistics queries.
type ConsumerStatsQuerier interface {
	ConsumerStats(ctx context.Context, stream, consumer string) (pending int64, inFlight int64, err error)
}

type synapseConsumerTarget struct {
	stream   string
	consumer string
	queue    string
}

var synapseConsumerTargets = []synapseConsumerTarget{
	{stream: pubsub.StreamAerialJobs, consumer: "synapse-github", queue: QueueDefault},
	{stream: pubsub.StreamAerialJobs, consumer: "synapse-deploy", queue: QueueDefault},
	{stream: pubsub.StreamAerialJobs, consumer: "synapse-discord-ingress", queue: QueueDiscordIngress},
	{stream: pubsub.StreamAerialJobs, consumer: "synapse-discord-interaction", queue: QueueDiscordInteraction},
}

// CollectJetStreamQueueMetrics reads consumer pending and in-flight counts and updates QueueDepth gauges.
func CollectJetStreamQueueMetrics(ctx context.Context, q ConsumerStatsQuerier) error {
	for _, queue := range knownQueues {
		QueueDepth.WithLabelValues(queue, "pending").Set(0)
		QueueDepth.WithLabelValues(queue, "in_flight").Set(0)
	}
	if q == nil {
		return nil
	}

	queuePending := make(map[string]int64)
	queueInFlight := make(map[string]int64)
	var firstErr error
	for _, target := range synapseConsumerTargets {
		pending, inFlight, err := q.ConsumerStats(ctx, target.stream, target.consumer)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("consumer stats %s/%s: %w", target.stream, target.consumer, err)
			}
			log.Printf("[synapse] [metrics] warning querying consumer stats for %s/%s: %v", target.stream, target.consumer, err)
			continue
		}
		queuePending[target.queue] += pending
		queueInFlight[target.queue] += inFlight
	}
	for queue, p := range queuePending {
		QueueDepth.WithLabelValues(queue, "pending").Set(float64(p))
	}
	for queue, f := range queueInFlight {
		QueueDepth.WithLabelValues(queue, "in_flight").Set(float64(f))
	}
	return firstErr
}

// NormalizeJobArgs unwraps pointer wrappers of known webhook/ingress payload types.
func NormalizeJobArgs(args any) any {
	switch a := args.(type) {
	case *model.GitHubWebhookArgs:
		if a != nil {
			return *a
		}
	case *model.DeployWebhookArgs:
		if a != nil {
			return *a
		}
	case *model.DiscordIngressArgs:
		if a != nil {
			return *a
		}
	case *model.DiscordInteractionArgs:
		if a != nil {
			return *a
		}
	}
	return args
}

// ResolveJobQueueAndKind determines the queue name and metric kind label for a job payload.
func ResolveJobQueueAndKind(args any) (string, string) {
	switch NormalizeJobArgs(args).(type) {
	case model.GitHubWebhookArgs:
		return QueueDefault, "github_webhook"
	case model.DeployWebhookArgs:
		return QueueDefault, "deploy_webhook"
	case model.DiscordIngressArgs:
		return QueueDiscordIngress, "discord_ingress"
	case model.DiscordInteractionArgs:
		return QueueDiscordInteraction, "discord_interaction"
	default:
		return QueueDefault, unknownLabel
	}
}

func discordDedupID(prefix, agent, id string) string {
	agent = strings.TrimSpace(agent)
	if agent != "" {
		return prefix + agent + ":" + id
	}
	return prefix + id
}

// ResolveJetStreamJobCoords returns the JetStream subject and deduplication message ID for a job payload.
func ResolveJetStreamJobCoords(args any) (string, string) {
	switch a := NormalizeJobArgs(args).(type) {
	case model.GitHubWebhookArgs:
		return "aerial.jobs.synapse.github", "github:" + a.Delivery
	case model.DeployWebhookArgs:
		return "aerial.jobs.synapse.deploy", "deploy:" + a.IdempotencyKey
	case model.DiscordIngressArgs:
		return "aerial.jobs.synapse.discord_ingress", discordDedupID("discord:msg:", a.Agent, a.MessageID)
	case model.DiscordInteractionArgs:
		return "aerial.jobs.synapse.discord_interaction", discordDedupID("discord:ix:", a.Agent, a.InteractionID)
	default:
		return "", ""
	}
}
