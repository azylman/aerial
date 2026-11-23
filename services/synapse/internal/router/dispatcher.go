package router

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/ericlient"
	"github.com/azylman/aerial/services/synapse/internal/kube"
)

// kubeClient and eriClient alias the two embedded clients so their
// promoted fields get distinct names instead of colliding on Client.
type (
	kubeClient = kube.Client
	eriClient  = ericlient.Client
)

// DefaultOutboundDispatcher routes outbound work to the Kubernetes/Flux
// client and to the one ericlient.Client that owns eri dispatch, its HTTP
// clients and its retry bounds. Both clients' methods are promoted; only the
// agent-routed Discord dispatches are defined here.
type DefaultOutboundDispatcher struct {
	*kubeClient
	*eriClient

	cfgSrc ConfigSource
}

// NewDefaultOutboundDispatcher builds a dispatcher whose eri client comes
// from ericlient.NewClient(brainURL, client); a nil client gets ericlient's
// per-endpoint default timeouts and a 15s client for Kubernetes calls.
func NewDefaultOutboundDispatcher(brainURL string, client *http.Client) *DefaultOutboundDispatcher {
	kubeHTTP := client
	if kubeHTTP == nil {
		kubeHTTP = &http.Client{Timeout: 15 * time.Second}
	}
	return &DefaultOutboundDispatcher{
		kubeClient: kube.NewClient(tracing.Client(kubeHTTP)),
		eriClient:  ericlient.NewClient(brainURL, client),
	}
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

func (d *DefaultOutboundDispatcher) DispatchDiscordMessage(ctx context.Context, req DiscordMessageIngressRequest) error {
	if eriURL := d.resolveEriURL(req.Agent); eriURL != "" {
		return d.DispatchDiscordMessageToURL(ctx, eriURL, req)
	}
	return d.eriClient.DispatchDiscordMessage(ctx, req)
}

func (d *DefaultOutboundDispatcher) DispatchDiscordInteractionForAgent(ctx context.Context, agent string, interaction *discordgo.Interaction) error {
	if eriURL := d.resolveEriURL(agent); eriURL != "" {
		return d.DispatchDiscordInteractionToURL(ctx, eriURL, interaction)
	}
	return d.DispatchDiscordInteraction(ctx, interaction)
}
