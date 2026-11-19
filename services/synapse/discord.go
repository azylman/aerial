package main

import (
	"context"
	"errors"

	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/discord"
)

var _ discord.IngressServer = (*RouterServer)(nil)

// Config returns the current configuration snapshot, implementing discord.IngressServer.
func (s *RouterServer) Config() *config.Config {
	if s == nil || s.cfgSrc == nil {
		return nil
	}
	return s.cfgSrc.Current()
}

// PublishJob publishes a job to JetStream, implementing discord.IngressServer.
func (s *RouterServer) PublishJob(ctx context.Context, args any) error {
	return s.publishJetStreamJob(ctx, args)
}

// ProcessDiscordIngress dispatches a Discord message ingress request to Eri.
func (s *RouterServer) ProcessDiscordIngress(ctx context.Context, args DiscordIngressArgs) error {
	if s.dispatcher == nil {
		return errors.New("dispatcher not configured on server")
	}
	req := DiscordMessageIngressRequest{
		BotUserID: args.BotUserID,
		RoleNames: args.RoleNames,
		Message:   args.Message,
	}
	return s.dispatcher.DispatchDiscordMessage(ctx, req)
}

// ProcessDiscordInteraction dispatches an application command interaction to Eri.
func (s *RouterServer) ProcessDiscordInteraction(ctx context.Context, args DiscordInteractionArgs) error {
	if s.dispatcher == nil {
		return errors.New("dispatcher not configured on server")
	}
	return s.dispatcher.DispatchDiscordInteraction(ctx, args.Interaction)
}

// startDiscordGateway delegates gateway startup to the internal discord subpackage.
func startDiscordGateway(ctx context.Context, server *RouterServer, token string) (func(), error) {
	return discord.StartGateway(ctx, server, token)
}
