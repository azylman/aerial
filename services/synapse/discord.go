package main

import (
	"context"
	"errors"
	"time"

	"github.com/bwmarrin/discordgo"

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

// runDiscordCatchUpSweep delegates catch-up sweep to the internal discord subpackage.
func runDiscordCatchUpSweep(ctx context.Context, dg *discordgo.Session, server *RouterServer, now time.Time) {
	discord.RunCatchUpSweep(ctx, dg, server, now)
}

// getChannelWatermark delegates to internal/discord.
func getChannelWatermark(channelID string) string {
	return discord.GetChannelWatermark(channelID)
}

// advanceChannelWatermark delegates to internal/discord.
func advanceChannelWatermark(channelID, messageID string) {
	discord.AdvanceChannelWatermark(channelID, messageID)
}

// parseSnowflakeUint64 delegates to internal/discord.
func parseSnowflakeUint64(s string) uint64 {
	return discord.ParseSnowflakeUint64(s)
}

// discordSnowflakeFromTime delegates to internal/discord.
func discordSnowflakeFromTime(t time.Time) string {
	return discord.DiscordSnowflakeFromTime(t)
}

// compareDiscordSnowflakes delegates to internal/discord.
func compareDiscordSnowflakes(a, b string) int {
	return discord.CompareDiscordSnowflakes(a, b)
}

// maxDiscordSnowflake delegates to internal/discord.
func maxDiscordSnowflake(a, b string) string {
	return discord.MaxDiscordSnowflake(a, b)
}
