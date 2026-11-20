package router

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/ericlient"
	"github.com/azylman/aerial/services/synapse/internal/kube"
)

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

func (d *DefaultOutboundDispatcher) SetRetryConfig(delay time.Duration, maxAttempts int) {
	d.retryDelay = delay
	d.maxAttempts = maxAttempts
	if d.kubeClient != nil {
		d.kubeClient.SetRetryConfig(delay, maxAttempts)
	}
	if d.eriClient != nil {
		d.eriClient.SetRetryConfig(delay, maxAttempts)
	}
}

func (d *DefaultOutboundDispatcher) APIServer() string {
	return d.kubeAPIServer
}

func (d *DefaultOutboundDispatcher) RetryDelay() time.Duration {
	return d.retryDelay
}

func (d *DefaultOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	d.ensureClients()
	return d.kubeClient.DispatchGitPush(ctx, req)
}

func (d *DefaultOutboundDispatcher) PromoteImageTag(ctx context.Context, req ImageReadyEventRequest) (string, error) {
	d.ensureClients()
	return d.kubeClient.PromoteImageTag(ctx, req)
}

func (d *DefaultOutboundDispatcher) ListKustomizationConfigMaps(ctx context.Context, ref FluxKustomizationRef) ([]ConfigMapChange, error) {
	d.ensureClients()
	return d.kubeClient.ListKustomizationConfigMaps(ctx, ref)
}

func (d *DefaultOutboundDispatcher) ListKustomizationWorkloads(ctx context.Context, ref FluxKustomizationRef) ([]WorkloadRollout, error) {
	d.ensureClients()
	return d.kubeClient.ListKustomizationWorkloads(ctx, ref)
}

func (d *DefaultOutboundDispatcher) ListFluxWorkloadsVersion(ctx context.Context, path string) (string, error) {
	d.ensureClients()
	return d.kubeClient.ListFluxWorkloadsVersion(ctx, path)
}

func (d *DefaultOutboundDispatcher) WatchFluxWorkloads(ctx context.Context, path, resourceVersion string) (io.ReadCloser, error) {
	d.ensureClients()
	return d.kubeClient.WatchFluxWorkloads(ctx, path, resourceVersion)
}

func (d *DefaultOutboundDispatcher) DiscoverImageKustomizations(ctx context.Context, imageNames []string) ([]FluxKustomizationRef, error) {
	d.ensureClients()
	return d.kubeClient.DiscoverImageKustomizations(ctx, imageNames)
}

func (d *DefaultOutboundDispatcher) ReconcileKustomization(ctx context.Context, ref FluxKustomizationRef, requestedAt string) error {
	d.ensureClients()
	return d.kubeClient.ReconcileKustomization(ctx, ref, requestedAt)
}

func (d *DefaultOutboundDispatcher) ResolveFluxSourceRepo(ctx context.Context, obj FluxInvolvedObject) (string, error) {
	d.ensureClients()
	return d.kubeClient.ResolveFluxSourceRepo(ctx, obj)
}

func (d *DefaultOutboundDispatcher) DiscoverRepoKustomizations(ctx context.Context, repo string) ([]FluxKustomizationRef, error) {
	d.ensureClients()
	return d.kubeClient.DiscoverRepoKustomizations(ctx, repo)
}

func (d *DefaultOutboundDispatcher) ListKustomizations(ctx context.Context, namespace string) ([]KustomizationObservation, string, error) {
	d.ensureClients()
	return d.kubeClient.ListKustomizations(ctx, namespace)
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

func (d *DefaultOutboundDispatcher) DispatchDiscordMessageTo(ctx context.Context, eriURL string, req DiscordMessageIngressRequest) error {
	d.ensureClients()
	return d.eriClient.DispatchDiscordMessageTo(ctx, eriURL, req)
}

func (d *DefaultOutboundDispatcher) DispatchDiscordInteraction(ctx context.Context, interaction *discordgo.Interaction) error {
	d.ensureClients()
	return d.eriClient.DispatchDiscordInteraction(ctx, interaction)
}

func (d *DefaultOutboundDispatcher) DispatchDiscordInteractionTo(ctx context.Context, eriURL string, interaction *discordgo.Interaction) error {
	d.ensureClients()
	return d.eriClient.DispatchDiscordInteractionTo(ctx, eriURL, interaction)
}
