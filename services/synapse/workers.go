package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// Metric collectors for River queues and worker execution telemetry.
var (
	// UniqueSkippedTotal records the total number of River jobs dropped due to unique constraint debouncing.
	UniqueSkippedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_unique_skipped_total",
			Help: "Total number of River jobs skipped as duplicates due to unique constraints.",
		},
		[]string{"kind"},
	)

	JobsEnqueuedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_jobs_enqueued_total",
			Help: "Total number of River jobs enqueued, skipped as duplicate, or failed on enqueue.",
		},
		[]string{"queue", "kind", "status"},
	)

	JobsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_jobs_processed_total",
			Help: "Total number of River jobs processed by worker execution status.",
		},
		[]string{"queue", "kind", "status"},
	)

	JobDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "synapse_job_duration_seconds",
			Help:    "Execution duration of River jobs in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"queue", "kind"},
	)

	JobRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_job_retries_total",
			Help: "Total number of River job retry attempts by reason.",
		},
		[]string{"queue", "kind", "reason"},
	)

	JobsDroppedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "synapse_jobs_dropped_total",
			Help: "Total number of River jobs dropped or cancelled by reason.",
		},
		[]string{"queue", "kind", "reason"},
	)

	QueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "synapse_queue_depth",
			Help: "Current depth of River queues partitioned by state.",
		},
		[]string{"queue", "state"},
	)

	QueueOldestAgeSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "synapse_queue_oldest_age_seconds",
			Help: "Oldest pending job age in seconds across available, retryable, and scheduled states.",
		},
		[]string{"queue"},
	)

	DiscordGatewayLatencySeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "synapse_discord_gateway_latency_seconds",
			Help: "Heartbeat latency of the Discord Gateway connection in seconds.",
		},
	)
)

var (
	knownRiverQueues = []string{
		river.QueueDefault,
		QueueMergeConflict,
		QueueWidgetSync,
		QueueDiscordIngress,
		QueueDiscordInteraction,
	}
	knownRiverStates = []string{"available", "running", "retryable", "scheduled"}
)

func registerCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(err)
		}
	}
}

func init() {
	registerCollector(UniqueSkippedTotal)
	registerCollector(JobsEnqueuedTotal)
	registerCollector(JobsProcessedTotal)
	registerCollector(JobDurationSeconds)
	registerCollector(JobRetriesTotal)
	registerCollector(JobsDroppedTotal)
	registerCollector(QueueDepth)
	registerCollector(QueueOldestAgeSeconds)
	registerCollector(DiscordGatewayLatencySeconds)

	for _, q := range knownRiverQueues {
		for _, s := range knownRiverStates {
			QueueDepth.WithLabelValues(q, s).Set(0)
		}
		QueueOldestAgeSeconds.WithLabelValues(q).Set(0)
	}
	DiscordGatewayLatencySeconds.Set(0)
}

// RecordUniqueSkipped increments the UniqueSkippedTotal counter for the specified kind.
func RecordUniqueSkipped(kind string) {
	if kind == "" {
		return
	}
	UniqueSkippedTotal.WithLabelValues(kind).Inc()
}

// RecordJobEnqueued increments the JobsEnqueuedTotal counter for the specified queue, kind, and status.
func RecordJobEnqueued(queue, kind, status string) {
	if queue == "" {
		queue = river.QueueDefault
	}
	if kind == "" {
		kind = "unknown"
	}
	if status == "" {
		status = "unknown"
	}
	JobsEnqueuedTotal.WithLabelValues(queue, kind, status).Inc()
}

// RecordJobRetry increments the JobRetriesTotal counter for the specified queue, kind, and reason.
func RecordJobRetry(queue, kind, reason string) {
	if queue == "" {
		queue = river.QueueDefault
	}
	if kind == "" {
		kind = "unknown"
	}
	if reason == "" {
		reason = "unknown"
	}
	JobRetriesTotal.WithLabelValues(queue, kind, reason).Inc()
}

// RecordJobDropped increments the JobsDroppedTotal counter for the specified queue, kind, and reason.
func RecordJobDropped(queue, kind, reason string) {
	if queue == "" {
		queue = river.QueueDefault
	}
	if kind == "" {
		kind = "unknown"
	}
	if reason == "" {
		reason = "unknown"
	}
	JobsDroppedTotal.WithLabelValues(queue, kind, reason).Inc()
}

// SetDiscordGatewayLatency sets the DiscordGatewayLatencySeconds gauge.
func SetDiscordGatewayLatency(seconds float64) {
	DiscordGatewayLatencySeconds.Set(seconds)
}

func resolveRiverJobMeta[T river.JobArgs](job *river.Job[T], defaultQueue string) (string, string) {
	queue := defaultQueue
	if queue == "" {
		queue = river.QueueDefault
	}
	if job == nil {
		var zero T
		return queue, zero.Kind()
	}
	if job.JobRow != nil {
		if job.Queue != "" {
			queue = job.Queue
		}
		if job.Attempt > 1 {
			RecordJobRetry(queue, job.Args.Kind(), "river_retry")
		}
	}
	return queue, job.Args.Kind()
}

func observeRiverJob[T river.JobArgs](job *river.Job[T], defaultQueue string, fn func() error) error {
	queue, kind := resolveRiverJobMeta(job, defaultQueue)
	start := time.Now()
	err := fn()
	duration := time.Since(start).Seconds()
	JobDurationSeconds.WithLabelValues(queue, kind).Observe(duration)

	var status string
	if err == nil {
		status = "success"
	} else if _, ok := errors.AsType[*rivertype.JobCancelError](err); ok {
		status = "cancelled"
	} else if job != nil && job.JobRow != nil && job.MaxAttempts > 0 && job.Attempt >= job.MaxAttempts {
		status = "discarded"
		RecordJobDropped(queue, kind, "max_attempts_exhausted")
	} else {
		status = "error"
	}
	JobsProcessedTotal.WithLabelValues(queue, kind, status).Inc()
	return err
}

type queueStatsQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const riverQueueStatsQuery = `SELECT queue, state::text, COUNT(*), COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::float8, 0) FROM river_job WHERE state IN ('available', 'running', 'retryable', 'scheduled') AND queue IN ('default', 'merge_conflict', 'widget_sync', 'discord_ingress', 'discord_interaction') GROUP BY queue, state`

func collectRiverQueueMetrics(ctx context.Context, q queueStatsQuerier) error {
	if q == nil {
		return nil
	}
	for _, queue := range knownRiverQueues {
		for _, state := range knownRiverStates {
			QueueDepth.WithLabelValues(queue, state).Set(0)
		}
		QueueOldestAgeSeconds.WithLabelValues(queue).Set(0)
	}

	rows, err := q.Query(ctx, riverQueueStatsQuery)
	if err != nil {
		return fmt.Errorf("query river queue stats: %w", err)
	}
	defer rows.Close()

	maxPendingAge := make(map[string]float64)
	for rows.Next() {
		var (
			queue     string
			state     string
			count     int64
			oldestAge float64
		)
		if err := rows.Scan(&queue, &state, &count, &oldestAge); err != nil {
			return fmt.Errorf("scan river queue stats: %w", err)
		}
		QueueDepth.WithLabelValues(queue, state).Set(float64(count))
		if state == "available" || state == "retryable" || state == "scheduled" {
			if oldestAge > maxPendingAge[queue] {
				maxPendingAge[queue] = oldestAge
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate river queue stats: %w", err)
	}

	for queue, age := range maxPendingAge {
		QueueOldestAgeSeconds.WithLabelValues(queue).Set(age)
	}
	return nil
}

// RiverInserter defines the interface for inserting jobs into River.
type RiverInserter interface {
	Insert(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error)
}

type riverJobArgsWithOpts interface {
	river.JobArgs
	InsertOpts() river.InsertOpts
}

func defaultRiverInsertOpts() river.InsertOpts {
	return river.InsertOpts{
		MaxAttempts: 5,
		UniqueOpts:  river.UniqueOpts{ByArgs: true, ByPeriod: 15 * time.Minute},
	}
}

func (s *RouterServer) insertRiverJob(ctx context.Context, args riverJobArgsWithOpts) error {
	opts := args.InsertOpts()
	queue := opts.Queue
	if queue == "" {
		queue = river.QueueDefault
	}
	res, err := s.riverClient.Insert(ctx, args, &opts)
	if err != nil {
		RecordJobEnqueued(queue, args.Kind(), "error")
		return err
	}
	if res != nil && res.UniqueSkippedAsDuplicate {
		RecordUniqueSkipped(args.Kind())
		RecordJobEnqueued(queue, args.Kind(), "skipped_duplicate")
		return nil
	}
	RecordJobEnqueued(queue, args.Kind(), "enqueued")
	return nil
}

// GitHubWebhookArgs contains payload data for durable background processing of GitHub webhooks.
type GitHubWebhookArgs struct {
	Event    string `json:"event"`
	Delivery string `json:"delivery" river:"unique"`
	Body     []byte `json:"body"`
}

func (GitHubWebhookArgs) Kind() string                 { return "github_webhook" }
func (GitHubWebhookArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

// GitHubWebhookWorker processes GitHub webhook events asynchronously from River.
type GitHubWebhookWorker struct {
	river.WorkerDefaults[GitHubWebhookArgs]

	server *RouterServer
}

func (w *GitHubWebhookWorker) Work(ctx context.Context, job *river.Job[GitHubWebhookArgs]) error {
	return observeRiverJob(job, river.QueueDefault, func() error {
		if w.server == nil {
			return errors.New("router server not configured on worker")
		}
		var jobID int64
		if job != nil && job.JobRow != nil {
			jobID = job.ID
		}
		log.Printf("[synapse] [river] processing github job id=%d event=%s delivery=%s", jobID, job.Args.Event, job.Args.Delivery)
		if _, err := w.server.ProcessGitHubEvent(ctx, job.Args.Event, job.Args.Delivery, job.Args.Body); err != nil {
			log.Printf("[synapse] [river] error processing github job id=%d event=%s delivery=%s: %v", jobID, job.Args.Event, job.Args.Delivery, err)
			return err
		}
		return nil
	})
}

// DeployWebhookArgs contains payload data for durable background processing of deployment webhooks.
type DeployWebhookArgs struct {
	IdempotencyKey string      `json:"idempotency_key,omitempty" river:"unique"`
	Event          DeployEvent `json:"event"`
}

func (DeployWebhookArgs) Kind() string                 { return "deploy_webhook" }
func (DeployWebhookArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

// DeployWebhookWorker processes deployment and sync webhook events from River.
type DeployWebhookWorker struct {
	river.WorkerDefaults[DeployWebhookArgs]

	server *RouterServer
}

func (w *DeployWebhookWorker) Work(ctx context.Context, job *river.Job[DeployWebhookArgs]) error {
	return observeRiverJob(job, river.QueueDefault, func() error {
		if w.server == nil {
			return errors.New("router server not configured on worker")
		}
		var jobID int64
		if job != nil && job.JobRow != nil {
			jobID = job.ID
		}
		log.Printf("[synapse] [river] processing deploy job id=%d event=%s job_name=%s", jobID, job.Args.Event.Event, job.Args.Event.JobName)
		if _, err := w.server.ProcessDeployEvent(ctx, job.Args.Event); err != nil {
			log.Printf("[synapse] [river] error processing deploy job id=%d: %v", jobID, err)
			return err
		}
		return nil
	})
}

// QueueWidgetSync is the dedicated River queue for Discord widget sync jobs.
const QueueWidgetSync = "widget_sync"

// WidgetSyncArgs contains payload data for durable background processing of Discord PR and build widget updates.
type WidgetSyncArgs struct {
	Repo     string `json:"repo,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
	BuildID  string `json:"build_id,omitempty"`
	Banner   string `json:"banner,omitempty"`
}

func (WidgetSyncArgs) Kind() string { return "widget_sync" }
func (WidgetSyncArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueWidgetSync, MaxAttempts: 5}
}

// WidgetSyncWorker processes Discord PR and build status widget updates from River.
type WidgetSyncWorker struct {
	river.WorkerDefaults[WidgetSyncArgs]

	server *RouterServer
}

func (w *WidgetSyncWorker) Work(ctx context.Context, job *river.Job[WidgetSyncArgs]) error {
	return observeRiverJob(job, QueueWidgetSync, func() error {
		if w.server == nil {
			return errors.New("router server not configured on worker")
		}
		var jobID int64
		if job != nil && job.JobRow != nil {
			jobID = job.ID
		}
		log.Printf("[synapse] [river] processing widget sync job id=%d repo=%s pr=%d build_id=%s", jobID, job.Args.Repo, job.Args.PRNumber, job.Args.BuildID)
		if err := w.server.ProcessWidgetSync(ctx, job.Args); err != nil {
			log.Printf("[synapse] [river] error processing widget sync job id=%d: %v", jobID, err)
			return err
		}
		return nil
	})
}

// EventTriggerWebhookArgs contains payload data for durable background firing of generic event triggers via Haro.
type EventTriggerWebhookArgs struct {
	Source        string   `json:"source"`
	Targets       []string `json:"targets"`
	EventType     string   `json:"event_type"`
	ContextHeader string   `json:"context_header,omitempty"`
}

func (EventTriggerWebhookArgs) Kind() string { return "event_trigger_webhook" }
func (EventTriggerWebhookArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5}
}

// EventTriggerWebhookWorker processes enqueued external event triggers asynchronously from River.
type EventTriggerWebhookWorker struct {
	river.WorkerDefaults[EventTriggerWebhookArgs]

	server *RouterServer
}

func (w *EventTriggerWebhookWorker) Work(ctx context.Context, job *river.Job[EventTriggerWebhookArgs]) error {
	return observeRiverJob(job, river.QueueDefault, func() error {
		if w.server == nil {
			return errors.New("router server not configured on worker")
		}
		var jobID int64
		if job != nil && job.JobRow != nil {
			jobID = job.ID
		}
		log.Printf("[synapse] [river] processing event trigger job id=%d source=%s event_type=%s targets=%v", jobID, job.Args.Source, job.Args.EventType, job.Args.Targets)
		if err := w.server.ProcessEventTriggerWebhook(ctx, job.Args); err != nil {
			log.Printf("[synapse] [river] error processing event trigger job id=%d: %v", jobID, err)
			return err
		}
		return nil
	})
}

// QueueMergeConflict is the dedicated River queue for PR merge conflict remediation dispatches.
const QueueMergeConflict = "merge_conflict"

// MergeConflictArgs contains payload data for durable background processing of PR merge conflicts.
type MergeConflictArgs struct {
	Repo     string `json:"repo" river:"unique"`
	PRNumber int    `json:"pr_number" river:"unique"`
	Branch   string `json:"branch"`
	HeadSHA  string `json:"head_sha"`
	TargetID string `json:"target_id"`
}

func (MergeConflictArgs) Kind() string { return "merge_conflict" }
func (MergeConflictArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       QueueMergeConflict,
		MaxAttempts: 5,
		UniqueOpts:  river.UniqueOpts{ByArgs: true, ByPeriod: 15 * time.Minute},
	}
}

// MergeConflictWorker processes PR merge conflict remediation dispatches from River.
type MergeConflictWorker struct {
	river.WorkerDefaults[MergeConflictArgs]

	server *RouterServer
}

func (w *MergeConflictWorker) Work(ctx context.Context, job *river.Job[MergeConflictArgs]) error {
	return observeRiverJob(job, QueueMergeConflict, func() error {
		if w.server == nil {
			return errors.New("router server not configured on worker")
		}
		var jobID int64
		if job != nil && job.JobRow != nil {
			jobID = job.ID
		}
		log.Printf("[synapse] [river] processing merge conflict job id=%d repo=%s pr=%d", jobID, job.Args.Repo, job.Args.PRNumber)
		if err := w.server.ProcessMergeConflict(ctx, job.Args); err != nil {
			log.Printf("[synapse] [river] error processing merge conflict job id=%d: %v", jobID, err)
			return err
		}
		return nil
	})
}

// QueueDiscordIngress is the dedicated River queue for single-consumer sequential Discord message ingress.
const QueueDiscordIngress = "discord_ingress"

// QueueDiscordInteraction is the dedicated River queue for Discord application command interaction dispatch.
const QueueDiscordInteraction = "discord_interaction"

// DiscordMessageIngressRequest represents the payload delivered to Eri for message ingress.
type DiscordMessageIngressRequest struct {
	BotUserID string             `json:"bot_user_id,omitempty"`
	RoleNames map[string]string  `json:"role_names,omitempty"`
	Message   *discordgo.Message `json:"message"`
}

// DiscordIngressArgs contains payload data for River queueing of Discord message ingress events.
type DiscordIngressArgs struct {
	MessageID string             `json:"message_id" river:"unique"`
	BotUserID string             `json:"bot_user_id,omitempty"`
	RoleNames map[string]string  `json:"role_names,omitempty"`
	Message   *discordgo.Message `json:"message"`
}

func (DiscordIngressArgs) Kind() string { return "discord_ingress" }
func (DiscordIngressArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       QueueDiscordIngress,
		MaxAttempts: 30,
		UniqueOpts: river.UniqueOpts{
			ByArgs:   true,
			ByPeriod: 30 * time.Minute,
		},
	}
}

// DiscordIngressWorker processes Discord message ingress events with single-consumer head-of-line backpressure.
type DiscordIngressWorker struct {
	river.WorkerDefaults[DiscordIngressArgs]

	server     *RouterServer
	retryDelay time.Duration
	maxAge     time.Duration
}

func (w *DiscordIngressWorker) Timeout(*river.Job[DiscordIngressArgs]) time.Duration {
	return 30 * time.Minute
}

func (w *DiscordIngressWorker) NextRetry(*river.Job[DiscordIngressArgs]) time.Time {
	return time.Now().Add(5 * time.Second)
}

func (w *DiscordIngressWorker) Work(ctx context.Context, job *river.Job[DiscordIngressArgs]) error {
	return observeRiverJob(job, QueueDiscordIngress, func() error {
		if w.server == nil {
			return errors.New("router server not configured on worker")
		}
		delay := w.retryDelay
		if delay <= 0 {
			delay = 2 * time.Second
		} else if delay > 5*time.Second {
			delay = 5 * time.Second
		}
		maxAge := w.maxAge
		if maxAge <= 0 {
			maxAge = 30 * time.Minute
		}

		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			err := w.server.ProcessDiscordIngress(ctx, job.Args)
			if err == nil {
				return nil
			}
			if is4xxClientError(err) {
				RecordJobDropped(QueueDiscordIngress, DiscordIngressArgs{}.Kind(), "client_4xx")
				return river.JobCancel(err)
			}
			if !job.CreatedAt.IsZero() && time.Since(job.CreatedAt) >= maxAge {
				RecordJobDropped(QueueDiscordIngress, DiscordIngressArgs{}.Kind(), "ttl_expired")
				return river.JobCancel(fmt.Errorf("discord message %s exceeded 30m TTL: %w", job.Args.MessageID, err))
			}
			RecordJobRetry(QueueDiscordIngress, DiscordIngressArgs{}.Kind(), "discord_ingress_backoff")
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
}

// DiscordInteractionArgs contains payload data for River queueing of Discord interaction events.
type DiscordInteractionArgs struct {
	InteractionID string                 `json:"interaction_id" river:"unique"`
	Interaction   *discordgo.Interaction `json:"interaction"`
}

func (DiscordInteractionArgs) Kind() string { return "discord_interaction" }
func (DiscordInteractionArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       QueueDiscordInteraction,
		MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{
			ByArgs:   true,
			ByPeriod: 15 * time.Minute,
		},
	}
}

// DiscordInteractionWorker processes Discord interactions asynchronously from River.
type DiscordInteractionWorker struct {
	river.WorkerDefaults[DiscordInteractionArgs]

	server *RouterServer
}

func (w *DiscordInteractionWorker) Timeout(*river.Job[DiscordInteractionArgs]) time.Duration {
	return 5 * time.Second
}

func (w *DiscordInteractionWorker) Work(ctx context.Context, job *river.Job[DiscordInteractionArgs]) error {
	return observeRiverJob(job, QueueDiscordInteraction, func() error {
		if w.server == nil {
			return errors.New("router server not configured on worker")
		}
		return w.server.ProcessDiscordInteraction(ctx, job.Args)
	})
}

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task.
// Note: eri's BuildStatusWidgetComponents (services/eri/pkg/discord/widget.go) renders
// chunk status badges, repo tags, titles, PR links, and runtime durations only; it does
// not display per-job deploying_jobs lists.
type StatusWidgetChunk struct {
	Index       int       `json:"index"`
	Repo        string    `json:"repo"`
	Title       string    `json:"title"`
	Status      string    `json:"status"`
	PRNumber    int       `json:"pr_number,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
	ActiveStep  string    `json:"active_step,omitempty"`
}

// StatusWidgetRequest represents a request to deliver or update a Discord status tracker widget.
type StatusWidgetRequest struct {
	ChannelID string              `json:"channel_id"`
	MessageID string              `json:"message_id,omitempty"`
	ID        string              `json:"id"`
	Tier      int                 `json:"tier,omitempty"`
	Title     string              `json:"title"`
	Status    string              `json:"status"`
	Banner    string              `json:"banner,omitempty"`
	Chunks    []StatusWidgetChunk `json:"chunks"`
	SkipSink  bool                `json:"skip_sink,omitempty"`
	StartedAt time.Time           `json:"started_at,omitempty"`
}

type GitPushEventRequest struct {
	Repo     string `json:"repo"`
	Ref      string `json:"ref"`
	Commit   string `json:"commit"`
	TargetID string `json:"target_id,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
}

type ImageReadyEventRequest struct {
	Image     string `json:"image"`
	Digest    string `json:"digest,omitempty"`
	Repo      string `json:"repo,omitempty"`
	CommitSHA string `json:"commit_sha,omitempty"`
	PRNumber  int    `json:"pr_number,omitempty"`
	TargetID  string `json:"target_id,omitempty"`
}

type PromptRequest struct {
	ChannelID string `json:"channel_id"`
	Prompt    string `json:"prompt"`
	Effort    string `json:"effort,omitempty"`
}

type DispatchRequest struct {
	Source    string `json:"source"`
	SessionID string `json:"session_id,omitempty"`
	Prompt    string `json:"prompt"`
	Effort    string `json:"effort,omitempty"`
}

type DirectMessageRequest struct {
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
}

type OutboundDispatcher interface {
	DispatchGitPush(ctx context.Context, req GitPushEventRequest) error
	DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error)
	DispatchPrompt(ctx context.Context, req PromptRequest) error
	DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error
	DispatchWidget(ctx context.Context, payload StatusWidgetRequest) (string, error)
	DispatchBackground(ctx context.Context, req DispatchRequest) error
	DispatchDiscordMessage(ctx context.Context, req DiscordMessageIngressRequest) error
	DispatchDiscordInteraction(ctx context.Context, interaction *discordgo.Interaction) error
}

type DefaultOutboundDispatcher struct {
	fluxReceiverURL          string
	brainURL                 string
	kubeAPIServer            string
	kubeToken                string
	httpClient               *http.Client
	discordMsgClient         *http.Client
	discordInteractionClient *http.Client
	retryDelay               time.Duration
	maxAttempts              int
}

func NewDefaultOutboundDispatcher(brainURL string, client *http.Client) *DefaultOutboundDispatcher {
	discordMsgClient := client
	discordInteractionClient := client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
		discordMsgClient = &http.Client{Timeout: 20 * time.Second}
		discordInteractionClient = &http.Client{Timeout: 2500 * time.Millisecond}
	}
	return &DefaultOutboundDispatcher{
		brainURL:                 strings.TrimRight(brainURL, "/"),
		httpClient:               client,
		discordMsgClient:         discordMsgClient,
		discordInteractionClient: discordInteractionClient,
		retryDelay:               1500 * time.Millisecond,
		maxAttempts:              6,
	}
}

func (d *DefaultOutboundDispatcher) SetFluxReceiverURL(url string) {
	d.fluxReceiverURL = strings.TrimRight(url, "/")
}

func (d *DefaultOutboundDispatcher) SetKubeConfig(apiServer, token string) {
	d.kubeAPIServer = strings.TrimRight(apiServer, "/")
	d.kubeToken = strings.TrimSpace(token)
}

func (d *DefaultOutboundDispatcher) getKubeToken() string {
	return getKubeServiceAccountToken(d.kubeToken)
}

func (d *DefaultOutboundDispatcher) forwardFluxReceiver(ctx context.Context, payload any) error {
	if d.fluxReceiverURL == "" {
		return nil
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal flux receiver payload: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, d.fluxReceiverURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create flux receiver request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("flux receiver request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr != nil {
			return fmt.Errorf("flux receiver returned status %d", resp.StatusCode)
		}
		return fmt.Errorf("flux receiver returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func extractImageWorkloadName(imageRef string) string {
	ref := strings.TrimSpace(imageRef)
	if lastSlash := strings.LastIndex(ref, "/"); lastSlash != -1 {
		ref = ref[lastSlash+1:]
	}
	if colonIdx := strings.Index(ref, ":"); colonIdx != -1 {
		ref = ref[:colonIdx]
	}
	return strings.TrimSpace(ref)
}

func resolveImageTagAndKustomization(repo, imageRef string) (tagKey, kustomizationName, workloadName string) {
	workloadName = extractImageWorkloadName(imageRef)
	if workloadName == "" || strings.EqualFold(workloadName, "none") {
		return "", "", ""
	}
	cleanRepo := cleanRepoName(repo)
	var imageName string
	switch cleanRepo {
	case "aerial":
		base := strings.TrimPrefix(workloadName, "aerial-")
		imageName = "aerial-" + base
		kustomizationName = "aerial-core"
	case "aerial-sidecars":
		base := strings.TrimPrefix(workloadName, "aerial-sidecar-")
		imageName = "aerial-sidecar-" + base
		kustomizationName = "aerial-config"
	case "mirrormere":
		if strings.HasPrefix(workloadName, "mirrormere") {
			imageName = workloadName
		} else {
			imageName = "mirrormere-" + workloadName
		}
		kustomizationName = "aerial-config"
	default:
		imageName = workloadName
		if strings.HasPrefix(imageName, "aerial-") && !strings.HasPrefix(imageName, "aerial-sidecar-") {
			kustomizationName = "aerial-core"
		} else {
			kustomizationName = "aerial-config"
		}
	}
	tagKey = "TAG_" + strings.ToUpper(strings.ReplaceAll(imageName, "-", "_"))
	return tagKey, kustomizationName, workloadName
}

func (d *DefaultOutboundDispatcher) createOrRetryImageTagConfigMap(ctx context.Context, client *http.Client, token, tagKey, commitSHA string, doPatch func() (int, []byte, error)) error {
	postBody, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]string{
			"name":      "aerial-image-tags",
			"namespace": "flux-system",
		},
		"data": map[string]string{
			tagKey: strings.TrimSpace(commitSHA),
		},
	})
	if err != nil {
		return fmt.Errorf("marshal configmap post body: %w", err)
	}
	postURL := d.kubeAPIServer + "/api/v1/namespaces/flux-system/configmaps"
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewReader(postBody))
	if err != nil {
		return fmt.Errorf("create configmap post request: %w", err)
	}
	postReq.Header.Set("Content-Type", "application/json")
	if token != "" {
		postReq.Header.Set("Authorization", "Bearer "+token)
	}
	postResp, err := client.Do(postReq)
	if err != nil {
		return fmt.Errorf("configmap post request failed: %w", err)
	}
	defer postResp.Body.Close()
	postRespBody, readErr := io.ReadAll(io.LimitReader(postResp.Body, 64<<10))
	if readErr != nil {
		return fmt.Errorf("read configmap post response: %w", readErr)
	}
	if postResp.StatusCode >= 200 && postResp.StatusCode < 300 {
		return nil
	}
	if postResp.StatusCode == http.StatusConflict {
		retryCode, retryBody, retryErr := doPatch()
		if retryErr != nil {
			return retryErr
		}
		if retryCode >= 200 && retryCode < 300 {
			return nil
		}
		return fmt.Errorf("configmap retry patch returned status %d: %s", retryCode, string(retryBody))
	}
	return fmt.Errorf("configmap post returned status %d: %s", postResp.StatusCode, string(postRespBody))
}

func (d *DefaultOutboundDispatcher) upsertImageTagConfigMap(ctx context.Context, tagKey, commitSHA string) error {
	apiServer := d.kubeAPIServer
	token := d.getKubeToken()
	client := getK8sHTTPClient(apiServer, d.httpClient)

	patchBody, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]string{
			"name":      "aerial-image-tags",
			"namespace": "flux-system",
		},
		"data": map[string]string{
			tagKey: strings.TrimSpace(commitSHA),
		},
	})
	if err != nil {
		return fmt.Errorf("marshal configmap patch body: %w", err)
	}

	patchURL := apiServer + "/api/v1/namespaces/flux-system/configmaps/aerial-image-tags"
	doPatch := func() (int, []byte, error) {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPatch, patchURL, bytes.NewReader(patchBody))
		if reqErr != nil {
			return 0, nil, fmt.Errorf("create configmap patch request: %w", reqErr)
		}
		req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, respErr := client.Do(req)
		if respErr != nil {
			return 0, nil, fmt.Errorf("configmap patch request failed: %w", respErr)
		}
		defer resp.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr != nil {
			return resp.StatusCode, nil, fmt.Errorf("read configmap patch response: %w", readErr)
		}
		return resp.StatusCode, body, nil
	}

	statusCode, body, err := doPatch()
	if err != nil {
		return err
	}
	if statusCode >= 200 && statusCode < 300 {
		return nil
	}
	if statusCode == http.StatusNotFound {
		return d.createOrRetryImageTagConfigMap(ctx, client, token, tagKey, commitSHA, doPatch)
	}
	return fmt.Errorf("configmap patch returned status %d: %s", statusCode, string(body))
}

func (d *DefaultOutboundDispatcher) triggerFluxKustomizationReconcile(ctx context.Context, kustomizationName string) error {
	apiServer := d.kubeAPIServer
	token := d.getKubeToken()
	client := getK8sHTTPClient(apiServer, d.httpClient)

	patchBody, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				"reconcile.fluxcd.io/requestedAt": time.Now().UTC().Format(time.RFC3339Nano),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal kustomization patch body: %w", err)
	}

	patchURL := fmt.Sprintf("%s/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/%s", apiServer, kustomizationName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, patchURL, bytes.NewReader(patchBody))
	if err != nil {
		return fmt.Errorf("create kustomization patch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/merge-patch+json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("kustomization patch request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if readErr != nil {
		return fmt.Errorf("kustomization patch returned status %d", resp.StatusCode)
	}
	return fmt.Errorf("kustomization patch returned status %d: %s", resp.StatusCode, string(respBody))
}

func (d *DefaultOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	if d.fluxReceiverURL != "" {
		return d.forwardFluxReceiver(ctx, req)
	}
	return nil
}

func (d *DefaultOutboundDispatcher) DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error) {
	tagKey, kustomizationName, workloadName := resolveImageTagAndKustomization(req.Repo, req.Image)
	if workloadName == "" {
		return nil, nil
	}

	var kubeSuccess, fluxSuccess bool
	var kubeErr, fluxErr error

	if d.kubeAPIServer != "" && req.CommitSHA != "" && !isZeroSHA(req.CommitSHA) && tagKey != "" {
		if err := d.upsertImageTagConfigMap(ctx, tagKey, req.CommitSHA); err != nil {
			kubeErr = fmt.Errorf("upsert image tag configmap: %w", err)
		} else if err := d.triggerFluxKustomizationReconcile(ctx, kustomizationName); err != nil {
			kubeErr = fmt.Errorf("trigger flux kustomization reconcile: %w", err)
		} else {
			kubeSuccess = true
		}
	}

	if d.fluxReceiverURL != "" {
		if err := d.forwardFluxReceiver(ctx, req); err != nil {
			fluxErr = fmt.Errorf("forward flux receiver: %w", err)
		} else {
			fluxSuccess = true
		}
	}

	if kubeErr != nil && fluxErr != nil {
		return nil, errors.Join(kubeErr, fluxErr)
	}
	if kubeErr != nil && !fluxSuccess {
		return nil, kubeErr
	}
	if fluxErr != nil && !kubeSuccess {
		return nil, fluxErr
	}

	if kubeSuccess || fluxSuccess {
		return []string{workloadName}, nil
	}
	return nil, nil
}

func validateBrainChannel(channelID, endpointPath string) error {
	if channelID != "" || endpointPath != "/dispatch" {
		if !IsValidDiscordChannel(channelID) {
			return fmt.Errorf("invalid discord snowflake channel id: %q", channelID)
		}
	}
	return nil
}

func advanceBackoffDelay(delay time.Duration, expBackoff bool) time.Duration {
	if expBackoff {
		delay = min(time.Duration(float64(delay)*1.5), 5*time.Second)
	}
	return delay
}

func (d *DefaultOutboundDispatcher) postBrainWithRetry(ctx context.Context, endpointPath, reqLabel, statusLabel, channelID string, maxAttempts int, defaultDelay time.Duration, expBackoff bool, payload any) error {
	if err := validateBrainChannel(channelID, endpointPath); err != nil {
		return err
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", reqLabel, err)
	}
	delay := d.retryDelay
	if delay <= 0 {
		delay = defaultDelay
	}
	if maxAttempts <= 0 {
		maxAttempts = 6
	}
	endpoint := d.brainURL + endpointPath
	var lastErr error

	for attempt := range maxAttempts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt > 0 {
			RecordJobRetry(river.QueueDefault, "brain_http_dispatch", "brain_http_retry")
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create %s request: %w", reqLabel, err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := d.httpClient.Do(httpReq)
		if err != nil {
			lastErr = fmt.Errorf("%s request failed (attempt %d/%d): %w", reqLabel, attempt+1, maxAttempts, err)
		} else {
			respBody, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				respBody = []byte(fmt.Sprintf("error reading body: %v", readErr))
			}
			resp.Body.Close()

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if resp.StatusCode < 500 {
				return fmt.Errorf("%s returned status %d: %s", statusLabel, resp.StatusCode, string(respBody))
			}
			lastErr = fmt.Errorf("%s returned status %d (attempt %d/%d): %s", statusLabel, resp.StatusCode, attempt+1, maxAttempts, string(respBody))
		}
		if attempt+1 < maxAttempts {
			select {
			case <-time.After(delay):
				delay = advanceBackoffDelay(delay, expBackoff)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return lastErr
}

func (d *DefaultOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	return d.postBrainWithRetry(ctx, "/prompt", "prompt", "brain prompt", req.ChannelID, d.maxAttempts, 1500*time.Millisecond, true, req)
}

func (d *DefaultOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	return d.postBrainWithRetry(ctx, "/discord/message", "direct message", "brain internal message", req.ChannelID, 3, 500*time.Millisecond, false, req)
}

func (d *DefaultOutboundDispatcher) DispatchBackground(ctx context.Context, req DispatchRequest) error {
	return d.postBrainWithRetry(ctx, "/dispatch", "dispatch", "brain dispatch", "", d.maxAttempts, 1500*time.Millisecond, true, req)
}

func (d *DefaultOutboundDispatcher) DispatchWidget(ctx context.Context, payload StatusWidgetRequest) (string, error) {
	if !IsValidDiscordSnowflake(payload.ChannelID) {
		return "", fmt.Errorf("invalid discord snowflake channel id: %q", payload.ChannelID)
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal widget request: %w", err)
	}
	endpoint := d.brainURL + "/discord/widget"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create widget request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("dispatch widget request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read widget response body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("brain widget returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var res struct {
		Status    string `json:"status"`
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal(respBody, &res); err != nil {
		return "", fmt.Errorf("decode widget response: %w", err)
	}
	return res.MessageID, nil
}

func (d *DefaultOutboundDispatcher) postDiscordInternalJSON(ctx context.Context, client *http.Client, timeout time.Duration, path, reqLabel, statusLabel string, payload any) error {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", reqLabel, err)
	}
	endpoint := d.brainURL + path
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create %s request: %w", reqLabel, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient := client
	if httpClient == nil {
		httpClient = d.httpClient
	}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("%s request failed: %w", reqLabel, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("read %s response: %w", reqLabel, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s returned status %d: %s", statusLabel, resp.StatusCode, string(respBody))
	}
	return nil
}

func (d *DefaultOutboundDispatcher) DispatchDiscordMessage(ctx context.Context, req DiscordMessageIngressRequest) error {
	return d.postDiscordInternalJSON(ctx, d.discordMsgClient, 20*time.Second, "/internal/discord/message", "discord message ingress", "discord ingress", req)
}

func (d *DefaultOutboundDispatcher) DispatchDiscordInteraction(ctx context.Context, interaction *discordgo.Interaction) error {
	return d.postDiscordInternalJSON(ctx, d.discordInteractionClient, 2500*time.Millisecond, "/internal/discord/interaction", "discord interaction", "discord interaction", interaction)
}
