package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus"
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

const (
	QueueDefault            = "default"
	QueueMergeConflict      = "merge_conflict"
	QueueWidgetSync         = "widget_sync"
	QueueDiscordIngress     = "discord_ingress"
	QueueDiscordInteraction = "discord_interaction"
)

var (
	knownQueues = []string{
		QueueDefault,
		QueueMergeConflict,
		QueueWidgetSync,
		QueueDiscordIngress,
		QueueDiscordInteraction,
	}
	knownStates = []string{"pending", "in_flight"}
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

	for _, q := range knownQueues {
		for _, s := range knownStates {
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
		queue = QueueDefault
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
		queue = QueueDefault
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
		queue = QueueDefault
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

type consumerStatsQuerier interface {
	ConsumerStats(ctx context.Context, stream, consumer string) (pending int64, inFlight int64, err error)
}

type synapseConsumerTarget struct {
	stream   string
	consumer string
	queue    string
}

var synapseConsumerTargets = []synapseConsumerTarget{
	{stream: "AERIAL_JOBS", consumer: "synapse-github", queue: QueueDefault},
	{stream: "AERIAL_JOBS", consumer: "synapse-deploy", queue: QueueDefault},
	{stream: "AERIAL_JOBS", consumer: "synapse-merge-conflict", queue: QueueMergeConflict},
	{stream: "AERIAL_JOBS", consumer: "synapse-discord-ingress", queue: QueueDiscordIngress},
	{stream: "AERIAL_JOBS", consumer: "synapse-discord-interaction", queue: QueueDiscordInteraction},
	{stream: "AERIAL_COALESCED", consumer: "synapse-widget-sync", queue: QueueWidgetSync},
}

func collectJetStreamQueueMetrics(ctx context.Context, q consumerStatsQuerier) error {
	for _, queue := range knownQueues {
		QueueDepth.WithLabelValues(queue, "pending").Set(0)
		QueueDepth.WithLabelValues(queue, "in_flight").Set(0)
		QueueOldestAgeSeconds.WithLabelValues(queue).Set(0)
	}
	if q == nil {
		return nil
	}

	queuePending := make(map[string]int64)
	queueInFlight := make(map[string]int64)
	for _, target := range synapseConsumerTargets {
		pending, inFlight, err := q.ConsumerStats(ctx, target.stream, target.consumer)
		if err != nil {
			return fmt.Errorf("consumer stats %s/%s: %w", target.stream, target.consumer, err)
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
	return nil
}

func resolveJobQueueAndKind(args any) (string, string) {
	switch args.(type) {
	case GitHubWebhookArgs, *GitHubWebhookArgs:
		return QueueDefault, "github_webhook"
	case DeployWebhookArgs, *DeployWebhookArgs:
		return QueueDefault, "deploy_webhook"
	case MergeConflictArgs, *MergeConflictArgs:
		return QueueMergeConflict, "merge_conflict"
	case DiscordIngressArgs, *DiscordIngressArgs:
		return QueueDiscordIngress, "discord_ingress"
	case DiscordInteractionArgs, *DiscordInteractionArgs:
		return QueueDiscordInteraction, "discord_interaction"
	case WidgetSyncArgs, *WidgetSyncArgs:
		return QueueWidgetSync, "widget_sync"
	default:
		return QueueDefault, "unknown"
	}
}

func (s *RouterServer) publishJetStreamJob(ctx context.Context, args any) error {
	if s == nil || s.bus == nil {
		return errors.New("event bus not configured")
	}
	subject, msgID := resolveJetStreamJobCoords(args)
	if subject == "" {
		return fmt.Errorf("unsupported job args type %T", args)
	}
	queue, kind := resolveJobQueueAndKind(args)
	if err := s.bus.Publish(ctx, subject, msgID, args); err != nil {
		RecordJobEnqueued(queue, kind, "error")
		return err
	}
	RecordJobEnqueued(queue, kind, "enqueued")
	return nil
}

// GitHubWebhookArgs contains payload data for durable background processing of GitHub webhooks.
type GitHubWebhookArgs struct {
	Event    string `json:"event"`
	Delivery string `json:"delivery"`
	Body     []byte `json:"body"`
}

func (GitHubWebhookArgs) Kind() string { return "github_webhook" }

// DeployWebhookArgs contains payload data for durable background processing of deployment webhooks.
type DeployWebhookArgs struct {
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	Event          DeployEvent `json:"event"`
}

func (DeployWebhookArgs) Kind() string { return "deploy_webhook" }

// WidgetSyncArgs contains payload data for Discord PR and build widget updates.
type WidgetSyncArgs struct {
	Repo     string `json:"repo,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
	BuildID  string `json:"build_id,omitempty"`
	Banner   string `json:"banner,omitempty"`
	Status   string `json:"status,omitempty"`
}

func (WidgetSyncArgs) Kind() string { return "widget_sync" }

// MergeConflictArgs contains payload data for durable background processing of PR merge conflicts.
type MergeConflictArgs struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	HeadSHA  string `json:"head_sha"`
	TargetID string `json:"target_id"`
}

func (MergeConflictArgs) Kind() string { return "merge_conflict" }

// DiscordMessageIngressRequest represents the payload delivered to Eri for message ingress.
type DiscordMessageIngressRequest struct {
	BotUserID string             `json:"bot_user_id,omitempty"`
	RoleNames map[string]string  `json:"role_names,omitempty"`
	Message   *discordgo.Message `json:"message"`
}

// DiscordIngressArgs contains payload data for JetStream queueing of Discord message ingress events.
type DiscordIngressArgs struct {
	MessageID   string             `json:"message_id"`
	BotUserID   string             `json:"bot_user_id,omitempty"`
	RoleNames   map[string]string  `json:"role_names,omitempty"`
	Message     *discordgo.Message `json:"message"`
	Traceparent string             `json:"traceparent,omitempty"`
}

func (DiscordIngressArgs) Kind() string { return "discord_ingress" }

// DiscordInteractionArgs contains payload data for JetStream queueing of Discord interaction events.
type DiscordInteractionArgs struct {
	InteractionID string                 `json:"interaction_id"`
	Interaction   *discordgo.Interaction `json:"interaction"`
	Traceparent   string                 `json:"traceparent,omitempty"`
}

func (DiscordInteractionArgs) Kind() string { return "discord_interaction" }

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task.
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
	ID             string `json:"id,omitempty"`
	Source         string `json:"source"`
	SessionID      string `json:"session_id,omitempty"`
	TargetID       string `json:"target_id,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
	Prompt         string `json:"prompt"`
	Effort         string `json:"effort,omitempty"`
	SkipIfActive   bool   `json:"skip_if_active,omitempty"`
}

type DirectMessageRequest struct {
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
}

type OutboundDispatcher interface {
	DispatchGitPush(ctx context.Context, req GitPushEventRequest) error
	DiscoverImageKustomizations(ctx context.Context, imageNames []string) ([]fluxKustomizationRef, error)
	PromoteImageTag(ctx context.Context, req ImageReadyEventRequest) (workloadName string, err error)
	ReconcileKustomization(ctx context.Context, ref fluxKustomizationRef, requestedAt string) error
	ListKustomizationWorkloads(ctx context.Context, ref fluxKustomizationRef) ([]workloadRollout, error)
	ListKustomizationConfigMaps(ctx context.Context, ref fluxKustomizationRef) ([]configMapChange, error)
	ResolveFluxSourceRepo(ctx context.Context, obj FluxInvolvedObject) (string, error)
	DiscoverRepoKustomizations(ctx context.Context, repo string) ([]fluxKustomizationRef, error)
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
	// Outbound eri calls carry traceparent so turn joins gateway or webhook traces.
	return &DefaultOutboundDispatcher{
		brainURL:                 strings.TrimRight(brainURL, "/"),
		httpClient:               tracing.Client(client),
		discordMsgClient:         tracing.Client(discordMsgClient),
		discordInteractionClient: tracing.Client(discordInteractionClient),
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

func resolveImageTag(repo, imageRef string) (tagKey, imageName, workloadName string) {
	workloadName = extractImageWorkloadName(imageRef)
	if workloadName == "" || strings.EqualFold(workloadName, "none") {
		return "", "", ""
	}
	switch cleanRepoName(repo) {
	case "aerial":
		imageName = "aerial-" + strings.TrimPrefix(workloadName, "aerial-")
	case "aerial-sidecars":
		imageName = "aerial-sidecar-" + strings.TrimPrefix(workloadName, "aerial-sidecar-")
	case "mirrormere":
		if strings.HasPrefix(workloadName, "mirrormere") {
			imageName = workloadName
		} else {
			imageName = "mirrormere-" + workloadName
		}
	default:
		imageName = workloadName
	}
	tagKey = "TAG_" + strings.ToUpper(strings.ReplaceAll(imageName, "-", "_"))
	return tagKey, imageName, workloadName
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

func (d *DefaultOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	if d.fluxReceiverURL != "" {
		return d.forwardFluxReceiver(ctx, req)
	}
	return nil
}

// PromoteImageTag writes the image's TAG_ key into ConfigMap and forwards to Flux receiver.
func (d *DefaultOutboundDispatcher) PromoteImageTag(ctx context.Context, req ImageReadyEventRequest) (string, error) {
	tagKey, _, workloadName := resolveImageTag(req.Repo, req.Image)
	if workloadName == "" {
		return "", nil
	}

	var kubeSuccess, fluxSuccess bool
	var kubeErr, fluxErr error

	if d.kubeAPIServer != "" && req.CommitSHA != "" && !isZeroSHA(req.CommitSHA) {
		if err := d.upsertImageTagConfigMap(ctx, tagKey, req.CommitSHA); err != nil {
			kubeErr = fmt.Errorf("upsert image tag configmap: %w", err)
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
		return "", errors.Join(kubeErr, fluxErr)
	}
	if kubeErr != nil && !fluxSuccess {
		return "", kubeErr
	}
	if fluxErr != nil && !kubeSuccess {
		return "", fluxErr
	}
	if kubeSuccess || fluxSuccess {
		return workloadName, nil
	}
	return "", nil
}

// kubeJSON performs a raw kube API request and returns the response body, failing on non-2xx statuses.
func (d *DefaultOutboundDispatcher) kubeJSON(ctx context.Context, method, path, contentType string, body []byte) ([]byte, error) {
	if d.kubeAPIServer == "" {
		return nil, errors.New("kube api server not configured")
	}
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, d.kubeAPIServer+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create kube %s %s request: %w", method, path, err)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if token := d.getKubeToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := getK8sHTTPClient(d.kubeAPIServer, d.httpClient).Do(req)
	if err != nil {
		return nil, fmt.Errorf("kube %s %s request failed: %w", method, path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("read kube %s %s response: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("kube %s %s returned status %d: %s", method, path, resp.StatusCode, truncateKubeBody(respBody))
	}
	return respBody, nil
}

func truncateKubeBody(body []byte) string {
	const maxLen = 512
	if len(body) > maxLen {
		return string(body[:maxLen]) + "..."
	}
	return string(body)
}

// workloadListPaths are the cluster-wide list endpoints for every workload kind that can run an image.
var workloadListPaths = []string{
	"/apis/apps/v1/deployments",
	"/apis/apps/v1/statefulsets",
	"/apis/apps/v1/daemonsets",
	"/apis/batch/v1/jobs",
	"/apis/batch/v1/cronjobs",
}

// rolloutWorkloadKinds are the workload kinds whose rollout gates a deploy.
var rolloutWorkloadKinds = []struct{ Kind, Path string }{
	{Kind: "Deployment", Path: "/apis/apps/v1/deployments"},
	{Kind: "StatefulSet", Path: "/apis/apps/v1/statefulsets"},
	{Kind: "DaemonSet", Path: "/apis/apps/v1/daemonsets"},
}

// fluxManagedSelector selects every object applied by any Flux Kustomization.
const fluxManagedSelector = fluxKustomizationNameLabel

// kustomizationSelector selects every object applied by ref.
func kustomizationSelector(ref fluxKustomizationRef) url.Values {
	return url.Values{"labelSelector": {fluxKustomizationNameLabel + "=" + ref.Name + "," + fluxKustomizationNamespaceLabel + "=" + ref.Namespace}}
}

// ListKustomizationConfigMaps lists when each ConfigMap applied by ref last changed.
func (d *DefaultOutboundDispatcher) ListKustomizationConfigMaps(ctx context.Context, ref fluxKustomizationRef) ([]configMapChange, error) {
	body, err := d.kubeJSON(ctx, http.MethodGet, "/api/v1/configmaps?"+kustomizationSelector(ref).Encode(), "", nil)
	if err != nil {
		return nil, fmt.Errorf("list %s configmaps: %w", ref.Name, err)
	}
	changes, err := decodeConfigMapChanges(body)
	if err != nil {
		return nil, fmt.Errorf("decode %s configmaps: %w", ref.Name, err)
	}
	return changes, nil
}

// ListKustomizationWorkloads lists the Deployments, StatefulSets and DaemonSets applied by ref.
func (d *DefaultOutboundDispatcher) ListKustomizationWorkloads(ctx context.Context, ref fluxKustomizationRef) ([]workloadRollout, error) {
	query := kustomizationSelector(ref)
	var workloads []workloadRollout
	for _, k := range rolloutWorkloadKinds {
		body, err := d.kubeJSON(ctx, http.MethodGet, k.Path+"?"+query.Encode(), "", nil)
		if err != nil {
			return nil, fmt.Errorf("list %s workloads: %w", ref.Name, err)
		}
		decoded, err := decodeWorkloadRollouts(k.Kind, body)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", k.Path, err)
		}
		workloads = append(workloads, decoded...)
	}
	return workloads, nil
}

// ListFluxWorkloadsVersion lists workloads at path and returns list resourceVersion for watching.
func (d *DefaultOutboundDispatcher) ListFluxWorkloadsVersion(ctx context.Context, path string) (string, error) {
	body, err := d.kubeJSON(ctx, http.MethodGet, path+"?"+url.Values{"labelSelector": {fluxManagedSelector}}.Encode(), "", nil)
	if err != nil {
		return "", fmt.Errorf("list flux workloads: %w", err)
	}
	var list struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return "", fmt.Errorf("decode %s: %w", path, err)
	}
	return list.Metadata.ResourceVersion, nil
}

// WatchFluxWorkloads opens a kube watch stream of the Flux-managed workloads at path from resourceVersion.
func (d *DefaultOutboundDispatcher) WatchFluxWorkloads(ctx context.Context, path, resourceVersion string) (io.ReadCloser, error) {
	return d.watchKube(ctx, path, url.Values{"labelSelector": {fluxManagedSelector}}, resourceVersion)
}

func (d *DefaultOutboundDispatcher) listWorkloadImages(ctx context.Context) ([]workloadImageRef, error) {
	var workloads []workloadImageRef
	for _, path := range workloadListPaths {
		body, err := d.kubeJSON(ctx, http.MethodGet, path, "", nil)
		if err != nil {
			return nil, fmt.Errorf("list workloads: %w", err)
		}
		refs, err := decodeWorkloadImageRefs(body)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", path, err)
		}
		workloads = append(workloads, refs...)
	}
	return workloads, nil
}

// DiscoverImageKustomizations returns every Flux Kustomization owning a workload that runs one of
// imageNames. When no labeled workload matches, it falls back to every flux-system Kustomization that
// substitutes from the image tag ConfigMap, so a promoted tag is never left un-reconciled.
func (d *DefaultOutboundDispatcher) DiscoverImageKustomizations(ctx context.Context, imageNames []string) ([]fluxKustomizationRef, error) {
	if d.kubeAPIServer == "" || len(imageNames) == 0 {
		return nil, nil
	}
	workloads, err := d.listWorkloadImages(ctx)
	if err != nil {
		return nil, err
	}
	if refs := kustomizationsForImages(workloads, imageNames); len(refs) > 0 {
		return refs, nil
	}
	body, err := d.kubeJSON(ctx, http.MethodGet, kustomizationPath(fluxKustomizationRef{Namespace: fluxSystemNamespace}), "", nil)
	if err != nil {
		return nil, fmt.Errorf("list kustomizations: %w", err)
	}
	refs, err := decodeImageTagKustomizations(body)
	if err != nil {
		return nil, fmt.Errorf("decode kustomizations: %w", err)
	}
	return refs, nil
}

// ReconcileKustomization requests a Flux reconcile by patching the requestedAt annotation to the
// caller-recorded value; Flux echoes it back in status.lastHandledReconcileAt once handled. Re-patching
// the same value is idempotent.
func (d *DefaultOutboundDispatcher) ReconcileKustomization(ctx context.Context, ref fluxKustomizationRef, requestedAt string) error {
	patchBody, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{fluxReconcileRequestedAtAnnotation: requestedAt},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal kustomization patch body: %w", err)
	}
	if _, err := d.kubeJSON(ctx, http.MethodPatch, kustomizationPath(ref), "application/merge-patch+json", patchBody); err != nil {
		return fmt.Errorf("reconcile kustomization %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	return nil
}

// ResolveFluxSourceRepo returns the owner/repo of the GitRepository behind a Flux object: the object
// itself for a GitRepository, or its spec.sourceRef for a Kustomization. Other kinds resolve to "".
func (d *DefaultOutboundDispatcher) ResolveFluxSourceRepo(ctx context.Context, obj FluxInvolvedObject) (string, error) {
	namespace := cmp.Or(strings.TrimSpace(obj.Namespace), fluxSystemNamespace)
	source := fluxSourceRef{Kind: obj.Kind, Name: strings.TrimSpace(obj.Name), Namespace: namespace}
	if obj.Kind == "Kustomization" {
		body, err := d.kubeJSON(ctx, http.MethodGet, kustomizationPath(fluxKustomizationRef{Namespace: namespace, Name: source.Name}), "", nil)
		if err != nil {
			return "", fmt.Errorf("get kustomization %s/%s: %w", namespace, source.Name, err)
		}
		var k fluxKustomizationObject
		if err := json.Unmarshal(body, &k); err != nil {
			return "", fmt.Errorf("decode kustomization %s/%s: %w", namespace, source.Name, err)
		}
		source = k.Spec.SourceRef
		source.Namespace = cmp.Or(source.Namespace, namespace)
	}
	return d.gitRepositorySlug(ctx, source)
}

// gitRepositorySlug GETs the GitRepository behind source and returns the owner/repo of its spec.url;
// any other source kind resolves to "".
func (d *DefaultOutboundDispatcher) gitRepositorySlug(ctx context.Context, source fluxSourceRef) (string, error) {
	if source.Kind != "GitRepository" || source.Name == "" {
		return "", nil
	}
	body, err := d.kubeJSON(ctx, http.MethodGet, gitRepositoryPath(source.Namespace, source.Name), "", nil)
	if err != nil {
		return "", fmt.Errorf("get gitrepository %s/%s: %w", source.Namespace, source.Name, err)
	}
	var repo struct {
		Spec struct {
			URL string `json:"url"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &repo); err != nil {
		return "", fmt.Errorf("decode gitrepository %s/%s: %w", source.Namespace, source.Name, err)
	}
	return gitRepoSlug(repo.Spec.URL), nil
}

// DiscoverRepoKustomizations returns the sorted Kustomizations, in the namespaces the rollout watcher
// follows, whose GitRepository source is repo. Each distinct source is fetched once. Suspended
// Kustomizations are skipped: they apply nothing, and the watcher would fail a PR waiting on one.
func (d *DefaultOutboundDispatcher) DiscoverRepoKustomizations(ctx context.Context, repo string) ([]fluxKustomizationRef, error) {
	repo = normalizeRepo(repo)
	if d.kubeAPIServer == "" || repo == "" {
		return nil, nil
	}
	slugs := make(map[fluxSourceRef]string)
	var refs []fluxKustomizationRef
	for _, namespace := range rolloutKustomizationNamespaces {
		observations, _, err := d.ListKustomizations(ctx, namespace)
		if err != nil {
			return nil, err
		}
		for _, obs := range observations {
			if obs.Suspended {
				continue
			}
			slug, ok := slugs[obs.SourceRef]
			if !ok {
				if slug, err = d.gitRepositorySlug(ctx, obs.SourceRef); err != nil {
					return nil, err
				}
				slugs[obs.SourceRef] = slug
			}
			if slug != "" && strings.EqualFold(normalizeRepo(slug), repo) {
				refs = append(refs, obs.Ref)
			}
		}
	}
	slices.SortFunc(refs, compareKustomizationRefs)
	return refs, nil
}

// ListKustomizations lists the Kustomizations of namespace and returns them with the list resourceVersion.
func (d *DefaultOutboundDispatcher) ListKustomizations(ctx context.Context, namespace string) ([]kustomizationObservation, string, error) {
	body, err := d.kubeJSON(ctx, http.MethodGet, kustomizationPath(fluxKustomizationRef{Namespace: namespace}), "", nil)
	if err != nil {
		return nil, "", fmt.Errorf("list kustomizations: %w", err)
	}
	return decodeKustomizationList(body, namespace)
}

// WatchKustomizations opens a kube watch stream of namespace's Kustomizations from resourceVersion.
func (d *DefaultOutboundDispatcher) WatchKustomizations(ctx context.Context, namespace, resourceVersion string) (io.ReadCloser, error) {
	return d.watchKube(ctx, kustomizationPath(fluxKustomizationRef{Namespace: namespace}), url.Values{}, resourceVersion)
}

// watchKube opens a kube watch stream at path filtered by query from resourceVersion.
func (d *DefaultOutboundDispatcher) watchKube(ctx context.Context, path string, query url.Values, resourceVersion string) (io.ReadCloser, error) {
	if d.kubeAPIServer == "" {
		return nil, errors.New("kube api server not configured")
	}
	query.Set("watch", "1")
	query.Set("allowWatchBookmarks", "true")
	query.Set("resourceVersion", resourceVersion)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.kubeAPIServer+path+"?"+query.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("create kube watch %s request: %w", path, err)
	}
	req.Header.Set("Accept", "application/json")
	if token := d.getKubeToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	streamClient := *getK8sHTTPClient(d.kubeAPIServer, d.httpClient)
	streamClient.Timeout = 0
	resp, err := streamClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kube watch %s request failed: %w", path, err)
	}
	if resp.StatusCode == http.StatusGone {
		resp.Body.Close()
		return nil, errWatchExpired
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return nil, fmt.Errorf("kube watch %s returned status %d (read error: %w)", path, resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("kube watch %s returned status %d: %s", path, resp.StatusCode, truncateKubeBody(body))
	}
	return resp.Body, nil
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
	if strings.TrimSpace(d.brainURL) == "" {
		return errors.New("brain URL is not configured")
	}
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
			RecordJobRetry(QueueDefault, "brain_http_dispatch", "brain_http_retry")
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
	if strings.TrimSpace(d.brainURL) == "" {
		return "", errors.New("brain URL is not configured")
	}
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
	if strings.TrimSpace(d.brainURL) == "" {
		return errors.New("brain URL is not configured")
	}
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
