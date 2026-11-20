package discord

import (
	"context"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
)

// SetGatewayOpenerForTest overrides the session opener function for testing and returns a cleanup func.
func SetGatewayOpenerForTest(fn func(*discordgo.Session) error) func() {
	gatewayOpenerMu.Lock()
	orig := gatewayOpener
	gatewayOpener = fn
	gatewayOpenerMu.Unlock()
	return func() {
		gatewayOpenerMu.Lock()
		gatewayOpener = orig
		gatewayOpenerMu.Unlock()
	}
}

// ResetWatermarksForTest clears all tracked channel watermarks.
func ResetWatermarksForTest() {
	channelWatermarksMu.Lock()
	defer channelWatermarksMu.Unlock()
	channelWatermarks = make(map[string]string)
}

// SetSweepRunningForTest forces the sweep running state for testing concurrent sweep prevention.
func SetSweepRunningForTest(running bool) func() {
	discordSweepMu.Lock()
	prev := discordSweepsRunning[""]
	discordSweepsRunning[""] = running
	discordSweepMu.Unlock()
	return func() {
		discordSweepMu.Lock()
		discordSweepsRunning[""] = prev
		discordSweepMu.Unlock()
	}
}

// GetChannelWatermark returns the highest message snowflake processed for a channel.
func GetChannelWatermark(channelID string) string {
	return GetAgentChannelWatermark("", channelID)
}

// AdvanceChannelWatermark advances the tracked watermark for a channel if messageID is newer.
func AdvanceChannelWatermark(channelID, messageID string) {
	AdvanceAgentChannelWatermark("", channelID, messageID)
}

// HandleGatewayReady updates session state and triggers a background catch-up sweep.
func HandleGatewayReady(ctx context.Context, server IngressServer, s *discordgo.Session, r *discordgo.Ready, wg *sync.WaitGroup) {
	HandleAgentGatewayReady(ctx, server, s, r, "", wg)
}

// EnqueueDiscordMessage prepares and publishes or dispatches an inbound Discord message.
func EnqueueDiscordMessage(ctx context.Context, server IngressServer, s *discordgo.Session, m *discordgo.Message, botUserID string) {
	EnqueueAgentDiscordMessage(ctx, server, s, m, botUserID, "")
}

// CatchUpChannelMessages processes missed messages in a single Discord channel.
func CatchUpChannelMessages(ctx context.Context, dg *discordgo.Session, server IngressServer, ch *discordgo.Channel, guildID, botUserID, minSnowflake string, now time.Time) {
	CatchUpAgentChannelMessages(ctx, dg, server, ch, guildID, botUserID, minSnowflake, "", now)
}

// RunCatchUpSweep performs a startup catch-up sweep across all available guilds and channels for the default agent.
func RunCatchUpSweep(ctx context.Context, dg *discordgo.Session, server IngressServer, now time.Time) {
	RunStartupCatchup(ctx, dg, server, "", now)
}
