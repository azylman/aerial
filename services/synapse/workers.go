package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/ericlient"
	"github.com/azylman/aerial/services/synapse/internal/kube"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// Metric collectors for JetStream queues and worker execution telemetry.
var (
	JobsEnqueuedTotal            = metrics.JobsEnqueuedTotal
	JobsProcessedTotal           = metrics.JobsProcessedTotal
	JobDurationSeconds           = metrics.JobDurationSeconds
	JobRetriesTotal              = metrics.JobRetriesTotal
	JobsDroppedTotal             = metrics.JobsDroppedTotal
	QueueDepth                   = metrics.QueueDepth
	DiscordGatewayLatencySeconds = metrics.DiscordGatewayLatencySeconds
)

const (
	QueueDefault            = metrics.QueueDefault
	QueueDiscordIngress     = metrics.QueueDiscordIngress
	QueueDiscordInteraction = metrics.QueueDiscordInteraction
)

// RecordJobEnqueued increments the JobsEnqueuedTotal counter for the specified queue, kind, and status.
func RecordJobEnqueued(queue, kind, status string) {
	metrics.RecordJobEnqueued(queue, kind, status)
}

// RecordJobRetry increments the JobRetriesTotal counter for the specified queue, kind, and reason.
func RecordJobRetry(queue, kind, reason string) {
	metrics.RecordJobRetry(queue, kind, reason)
}

// RecordJobDropped increments the JobsDroppedTotal counter for the specified queue, kind, and reason.
func RecordJobDropped(queue, kind, reason string) {
	metrics.RecordJobDropped(queue, kind, reason)
}

// SetDiscordGatewayLatency sets the DiscordGatewayLatencySeconds gauge.
func SetDiscordGatewayLatency(seconds float64) {
	metrics.SetDiscordGatewayLatency(seconds)
}

type consumerStatsQuerier = metrics.ConsumerStatsQuerier

func collectJetStreamQueueMetrics(ctx context.Context, q consumerStatsQuerier) error {
	return metrics.CollectJetStreamQueueMetrics(ctx, q)
}

func normalizeJobArgs(args any) any {
	switch a := args.(type) {
	case *GitHubWebhookArgs:
		if a != nil {
			return *a
		}
	case *DeployWebhookArgs:
		if a != nil {
			return *a
		}
	case *DiscordIngressArgs:
		if a != nil {
			return *a
		}
	case *DiscordInteractionArgs:
		if a != nil {
			return *a
		}
	}
	return args
}

func resolveJobQueueAndKind(args any) (string, string) {
	switch a := normalizeJobArgs(args).(type) {
	case DeployWebhookArgs:
		return QueueDefault, "deploy_webhook"
	case *DeployWebhookArgs:
		if a != nil {
			return QueueDefault, "deploy_webhook"
		}
	}
	return metrics.ResolveJobQueueAndKind(args)
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
type GitHubWebhookArgs = model.GitHubWebhookArgs

// DeployWebhookArgs contains payload data for durable background processing of deployment webhooks.
type DeployWebhookArgs struct {
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	Event          DeployEvent `json:"event"`
}

// Type aliases to model domain types.
type (
	DiscordMessageIngressRequest = model.DiscordMessageIngressRequest
	DiscordIngressArgs           = model.DiscordIngressArgs
	DiscordInteractionArgs       = model.DiscordInteractionArgs
	GitPushEventRequest          = model.GitPushEventRequest
	ImageReadyEventRequest       = model.ImageReadyEventRequest
	PromptRequest                = model.PromptRequest
	DispatchRequest              = model.DispatchRequest
	DirectMessageRequest         = model.DirectMessageRequest
)

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

	kubeClient *kube.Client
	eriClient  *ericlient.Client
}

func NewDefaultOutboundDispatcher(brainURL string, client *http.Client) *DefaultOutboundDispatcher {
	discordMsgClient := client
	discordInteractionClient := client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
		discordMsgClient = &http.Client{Timeout: 20 * time.Second}
		discordInteractionClient = &http.Client{Timeout: 2500 * time.Millisecond}
	}
	d := &DefaultOutboundDispatcher{
		brainURL:                 strings.TrimRight(brainURL, "/"),
		httpClient:               tracing.Client(client),
		discordMsgClient:         tracing.Client(discordMsgClient),
		discordInteractionClient: tracing.Client(discordInteractionClient),
		retryDelay:               1500 * time.Millisecond,
		maxAttempts:              6,
	}
	d.ensureClients()
	return d
}

func (d *DefaultOutboundDispatcher) ensureClients() {
	if d.kubeClient == nil {
		d.kubeClient = kube.NewClient(d.httpClient)
		d.kubeClient.SetFluxReceiverURL(d.fluxReceiverURL)
		d.kubeClient.SetKubeConfig(d.kubeAPIServer, d.kubeToken)
		d.kubeClient.SetRetryConfig(d.retryDelay, d.maxAttempts)
	}
	if d.eriClient == nil {
		d.eriClient = ericlient.NewClient(d.brainURL, d.httpClient)
		d.eriClient.SetDiscordMsgClient(d.discordMsgClient)
		d.eriClient.SetDiscordInteractionClient(d.discordInteractionClient)
		d.eriClient.SetRetryConfig(d.retryDelay, d.maxAttempts)
	}
}

func isFluxEventServerRootURL(rawURL string) bool {
	_, rest, ok := strings.Cut(strings.ToLower(strings.TrimSpace(rawURL)), "://")
	if !ok {
		return false
	}
	hostPort, path, _ := strings.Cut(rest, "/")
	host, _, _ := strings.Cut(hostPort, ":")
	return (host == "notification-controller" || strings.HasPrefix(host, "notification-controller.")) && strings.Trim(path, "/") == ""
}

func (d *DefaultOutboundDispatcher) SetFluxReceiverURL(receiverURL string) {
	trimmed := strings.TrimRight(strings.TrimSpace(receiverURL), "/")
	if isFluxEventServerRootURL(trimmed) {
		d.fluxReceiverURL = ""
	} else {
		d.fluxReceiverURL = trimmed
	}
	if d.kubeClient != nil {
		d.kubeClient.SetFluxReceiverURL(d.fluxReceiverURL)
	}
}

func (d *DefaultOutboundDispatcher) SetKubeConfig(apiServer, token string) {
	d.kubeAPIServer = strings.TrimRight(apiServer, "/")
	d.kubeToken = strings.TrimSpace(token)
	if d.kubeClient != nil {
		d.kubeClient.SetKubeConfig(d.kubeAPIServer, d.kubeToken)
	}
}

// APIServer returns the configured Kubernetes API server URL.
func (d *DefaultOutboundDispatcher) APIServer() string {
	return d.kubeAPIServer
}

// RetryDelay returns the base delay between informer retries.
func (d *DefaultOutboundDispatcher) RetryDelay() time.Duration {
	return d.retryDelay
}

func resolveImageTag(repo, imageRef string) (tagKey, imageName, workloadName string) {
	return kube.ResolveImageTag(repo, imageRef)
}

func (d *DefaultOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	d.ensureClients()
	return d.kubeClient.DispatchGitPush(ctx, req)
}

func (d *DefaultOutboundDispatcher) PromoteImageTag(ctx context.Context, req ImageReadyEventRequest) (string, error) {
	d.ensureClients()
	return d.kubeClient.PromoteImageTag(ctx, req)
}

func toFluxKustomizationRefs(refs []model.FluxKustomizationRef) []fluxKustomizationRef {
	if refs == nil {
		return nil
	}
	res := make([]fluxKustomizationRef, len(refs))
	copy(res, refs)
	return res
}

func (d *DefaultOutboundDispatcher) ListKustomizationConfigMaps(ctx context.Context, ref fluxKustomizationRef) ([]configMapChange, error) {
	d.ensureClients()
	changes, err := d.kubeClient.ListKustomizationConfigMaps(ctx, ref)
	if err != nil {
		return nil, err
	}
	res := make([]configMapChange, len(changes))
	for i, c := range changes {
		res[i] = configMapChange{Namespace: c.Namespace, Name: c.Name, ChangedAt: c.ChangedAt}
	}
	return res, nil
}

func (d *DefaultOutboundDispatcher) ListKustomizationWorkloads(ctx context.Context, ref fluxKustomizationRef) ([]workloadRollout, error) {
	d.ensureClients()
	workloads, err := d.kubeClient.ListKustomizationWorkloads(ctx, ref)
	if err != nil {
		return nil, err
	}
	return fromModelWorkloadRollouts(workloads), nil
}

func (d *DefaultOutboundDispatcher) ListFluxWorkloadsVersion(ctx context.Context, path string) (string, error) {
	d.ensureClients()
	return d.kubeClient.ListFluxWorkloadsVersion(ctx, path)
}

func (d *DefaultOutboundDispatcher) WatchFluxWorkloads(ctx context.Context, path, resourceVersion string) (io.ReadCloser, error) {
	d.ensureClients()
	return d.kubeClient.WatchFluxWorkloads(ctx, path, resourceVersion)
}

func (d *DefaultOutboundDispatcher) DiscoverImageKustomizations(ctx context.Context, imageNames []string) ([]fluxKustomizationRef, error) {
	d.ensureClients()
	refs, err := d.kubeClient.DiscoverImageKustomizations(ctx, imageNames)
	if err != nil {
		return nil, err
	}
	return toFluxKustomizationRefs(refs), nil
}

func (d *DefaultOutboundDispatcher) ReconcileKustomization(ctx context.Context, ref fluxKustomizationRef, requestedAt string) error {
	d.ensureClients()
	return d.kubeClient.ReconcileKustomization(ctx, ref, requestedAt)
}

func (d *DefaultOutboundDispatcher) ResolveFluxSourceRepo(ctx context.Context, obj FluxInvolvedObject) (string, error) {
	d.ensureClients()
	return d.kubeClient.ResolveFluxSourceRepo(ctx, obj)
}

func (d *DefaultOutboundDispatcher) DiscoverRepoKustomizations(ctx context.Context, repo string) ([]fluxKustomizationRef, error) {
	d.ensureClients()
	refs, err := d.kubeClient.DiscoverRepoKustomizations(ctx, repo)
	if err != nil {
		return nil, err
	}
	return toFluxKustomizationRefs(refs), nil
}

func (d *DefaultOutboundDispatcher) ListKustomizations(ctx context.Context, namespace string) ([]kustomizationObservation, string, error) {
	d.ensureClients()
	obs, rv, err := d.kubeClient.ListKustomizations(ctx, namespace)
	if err != nil {
		return nil, "", err
	}
	res := make([]kustomizationObservation, len(obs))
	for i, o := range obs {
		conds := make([]fluxCondition, len(o.Conditions))
		copy(conds, o.Conditions)
		res[i] = kustomizationObservation{
			Ref:                    o.Ref,
			Suspended:              o.Suspended,
			RequestedAt:            o.RequestedAt,
			LastHandledReconcileAt: o.LastHandledReconcileAt,
			LastAppliedRevision:    o.LastAppliedRevision,
			SourceRef:              o.SourceRef,
			Conditions:             conds,
		}
	}
	return res, rv, nil
}

func (d *DefaultOutboundDispatcher) WatchKustomizations(ctx context.Context, namespace, resourceVersion string) (io.ReadCloser, error) {
	d.ensureClients()
	return d.kubeClient.WatchKustomizations(ctx, namespace, resourceVersion)
}

func (d *DefaultOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	d.ensureClients()
	return d.eriClient.DispatchPrompt(ctx, req)
}

func (d *DefaultOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	d.ensureClients()
	return d.eriClient.DispatchDirectMessage(ctx, req)
}

func (d *DefaultOutboundDispatcher) DispatchBackground(ctx context.Context, req DispatchRequest) error {
	d.ensureClients()
	return d.eriClient.DispatchBackground(ctx, req)
}

func (d *DefaultOutboundDispatcher) DispatchDiscordMessage(ctx context.Context, req DiscordMessageIngressRequest) error {
	d.ensureClients()
	return d.eriClient.DispatchDiscordMessage(ctx, req)
}

func (d *DefaultOutboundDispatcher) DispatchDiscordInteraction(ctx context.Context, interaction *discordgo.Interaction) error {
	d.ensureClients()
	return d.eriClient.DispatchDiscordInteraction(ctx, interaction)
}
