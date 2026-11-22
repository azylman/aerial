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

// DefaultOutboundDispatcher routes outbound work to the Kubernetes/Flux
// client and to the one ericlient.Client that owns eri dispatch, its HTTP
// clients and its retry bounds.
type DefaultOutboundDispatcher struct {
	kubeAPIServer string
	kubeToken     string
	httpClient    *http.Client
	cfgSrc        ConfigSource

	kubeClient *kube.Client
	eriClient  *ericlient.Client
}

// NewDefaultOutboundDispatcher builds a dispatcher whose eri client comes
// from ericlient.NewClient(brainURL, client); a nil client gets ericlient's
// per-endpoint default timeouts and a 15s client for Kubernetes calls.
func NewDefaultOutboundDispatcher(brainURL string, client *http.Client) *DefaultOutboundDispatcher {
	kubeHTTP := client
	if kubeHTTP == nil {
		kubeHTTP = &http.Client{Timeout: 15 * time.Second}
	}
	d := &DefaultOutboundDispatcher{
		httpClient: tracing.Client(kubeHTTP),
		eriClient:  ericlient.NewClient(brainURL, client),
	}
	d.ensureClients()
	return d
}

func (d *DefaultOutboundDispatcher) SetConfigSource(cfgSrc ConfigSource) {
	d.cfgSrc = cfgSrc
}

func (d *DefaultOutboundDispatcher) resolveEriURL(agent string) string {
	if d == nil || d.cfgSrc == nil || agent == "" {
		return ""
	}
	cfg := d.cfgSrc.Current()
	if cfg == nil {
		return ""
	}
	if bot, ok := cfg.Bots[agent]; ok && strings.TrimSpace(bot.EriURL) != "" {
		return strings.TrimSpace(bot.EriURL)
	}
	for k, bot := range cfg.Bots {
		if strings.EqualFold(k, agent) && strings.TrimSpace(bot.EriURL) != "" {
			return strings.TrimSpace(bot.EriURL)
		}
	}
	return ""
}

func (d *DefaultOutboundDispatcher) ensureClients() {
	if d.kubeClient == nil {
		d.kubeClient = kube.NewClient(d.httpClient)
		d.kubeClient.SetKubeConfig(d.kubeAPIServer, d.kubeToken)
	}
	if d.eriClient == nil {
		d.eriClient = ericlient.NewClient("", d.httpClient)
	}
}

func (d *DefaultOutboundDispatcher) SetKubeConfig(apiServer, token string) {
	d.kubeAPIServer = strings.TrimRight(apiServer, "/")
	d.kubeToken = strings.TrimSpace(token)
	if d.kubeClient != nil {
		d.kubeClient.SetKubeConfig(d.kubeAPIServer, d.kubeToken)
	}
}

// SetRetryConfig applies one retry delay and attempt bound to both the
// Kubernetes client and the eri client.
func (d *DefaultOutboundDispatcher) SetRetryConfig(delay time.Duration, maxAttempts int) {
	d.ensureClients()
	d.kubeClient.SetRetryConfig(delay, maxAttempts)
	d.eriClient.SetRetryConfig(delay, maxAttempts)
}

func (d *DefaultOutboundDispatcher) APIServer() string {
	return d.kubeAPIServer
}

func (d *DefaultOutboundDispatcher) RetryDelay() time.Duration {
	d.ensureClients()
	return d.kubeClient.RetryDelay()
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
	if eriURL := d.resolveEriURL(req.Agent); eriURL != "" {
		return d.eriClient.DispatchDiscordMessageToURL(ctx, eriURL, req)
	}
	return d.eriClient.DispatchDiscordMessage(ctx, req)
}

func (d *DefaultOutboundDispatcher) DispatchDiscordInteraction(ctx context.Context, interaction *discordgo.Interaction) error {
	d.ensureClients()
	return d.eriClient.DispatchDiscordInteraction(ctx, interaction)
}

func (d *DefaultOutboundDispatcher) DispatchDiscordInteractionForAgent(ctx context.Context, agent string, interaction *discordgo.Interaction) error {
	d.ensureClients()
	if eriURL := d.resolveEriURL(agent); eriURL != "" {
		return d.eriClient.DispatchDiscordInteractionToURL(ctx, eriURL, interaction)
	}
	return d.eriClient.DispatchDiscordInteraction(ctx, interaction)
}
