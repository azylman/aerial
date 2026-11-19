package discord

import (
	"context"
	"log"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const discordCatchUpStaleTTL = 30 * time.Minute

var discordSweepRunning atomic.Bool

// FetchActiveGuildThreads retrieves active threads for a guild using dg.GuildThreadsActive.
func FetchActiveGuildThreads(dg *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
	if dg == nil || guildID == "" {
		return nil, nil
	}
	activeThreads, err := dg.GuildThreadsActive(guildID)
	if err != nil {
		return nil, err
	}
	if activeThreads == nil {
		return nil, nil
	}
	return activeThreads.Threads, nil
}

func snapshotGuildState(g *discordgo.Guild) *discordgo.Guild {
	if g == nil || g.ID == "" {
		return nil
	}
	snap := &discordgo.Guild{ID: g.ID}
	for _, ch := range g.Channels {
		if ch != nil {
			snap.Channels = append(snap.Channels, &discordgo.Channel{
				ID:            ch.ID,
				GuildID:       ch.GuildID,
				LastMessageID: ch.LastMessageID,
			})
		}
	}
	for _, th := range g.Threads {
		if th != nil {
			snap.Threads = append(snap.Threads, &discordgo.Channel{
				ID:            th.ID,
				GuildID:       th.GuildID,
				LastMessageID: th.LastMessageID,
			})
		}
	}
	return snap
}

func fetchFallbackUserGuilds(dg *discordgo.Session) []*discordgo.Guild {
	if dg == nil || dg.Token == "" {
		return nil
	}
	ugs, err := dg.UserGuilds(100, "", "", false)
	if err != nil {
		log.Printf("[synapse] [discord] failed to fetch user guilds: %v", err)
		return nil
	}
	var guilds []*discordgo.Guild
	for _, ug := range ugs {
		if ug != nil && ug.ID != "" {
			guilds = append(guilds, &discordgo.Guild{ID: ug.ID})
		}
	}
	return guilds
}

func resolveCatchUpGuilds(dg *discordgo.Session) []*discordgo.Guild {
	if dg == nil {
		return nil
	}
	var guilds []*discordgo.Guild
	if dg.State != nil {
		dg.State.RLock()
		for _, g := range dg.State.Guilds {
			if snap := snapshotGuildState(g); snap != nil {
				guilds = append(guilds, snap)
			}
		}
		dg.State.RUnlock()
	}
	if len(guilds) == 0 {
		guilds = fetchFallbackUserGuilds(dg)
	}
	return guilds
}

func collectCandidateChannels(dg *discordgo.Session, g *discordgo.Guild) []*discordgo.Channel {
	if g == nil || g.ID == "" {
		return nil
	}
	var candidates []*discordgo.Channel
	seenChannels := make(map[string]bool)
	addCandidate := func(ch *discordgo.Channel) {
		if ch == nil || ch.ID == "" || seenChannels[ch.ID] {
			return
		}
		seenChannels[ch.ID] = true
		candidates = append(candidates, ch)
	}

	if dg != nil {
		if restChannels, err := dg.GuildChannels(g.ID); err == nil {
			for _, ch := range restChannels {
				addCandidate(ch)
			}
		} else {
			log.Printf("[synapse] [discord] failed to fetch guild channels for guild %s: %v", g.ID, err)
		}

		if activeThreads, err := FetchActiveGuildThreads(dg, g.ID); err == nil {
			for _, th := range activeThreads {
				addCandidate(th)
			}
		} else {
			log.Printf("[synapse] [discord] failed to fetch active threads for guild %s: %v", g.ID, err)
		}
	}

	for _, ch := range g.Channels {
		addCandidate(ch)
	}
	for _, th := range g.Threads {
		addCandidate(th)
	}
	return candidates
}

func isCatchUpMessageEligible(m *discordgo.Message, botUserID, minSnowflake string, now time.Time) bool {
	if m == nil || m.ID == "" || m.Author == nil {
		return false
	}
	if botUserID != "" && m.Author.ID == botUserID {
		return false
	}
	if !m.Timestamp.IsZero() && (m.Timestamp.Before(now.Add(-discordCatchUpStaleTTL)) || now.Sub(m.Timestamp) > discordCatchUpStaleTTL) {
		metrics.RecordJobDropped(metrics.QueueDiscordIngress, "discord_ingress", "catchup_stale_ttl")
		return false
	}
	if CompareDiscordSnowflakes(m.ID, minSnowflake) <= 0 {
		return false
	}
	return true
}

// CatchUpChannelMessages processes missed messages in a single Discord channel.
func CatchUpChannelMessages(ctx context.Context, dg *discordgo.Session, server IngressServer, ch *discordgo.Channel, guildID, botUserID, minSnowflake string, now time.Time) {
	if ch == nil || ch.ID == "" || dg == nil || isNilServer(server) {
		return
	}
	afterID := MaxDiscordSnowflake(GetChannelWatermark(ch.ID), minSnowflake)
	if ch.LastMessageID == "" || CompareDiscordSnowflakes(ch.LastMessageID, afterID) <= 0 {
		return
	}

	fetched, err := dg.ChannelMessages(ch.ID, 50, "", afterID, "")
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [discord] failed to fetch channel messages", "channel_id", ch.ID, "error", err)
		return
	}

	for _, f := range slices.Backward(fetched) {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		m := f
		if m == nil || m.ID == "" {
			continue
		}
		AdvanceChannelWatermark(ch.ID, m.ID)
		if !isCatchUpMessageEligible(m, botUserID, minSnowflake, now) {
			continue
		}
		if m.GuildID == "" {
			m.GuildID = model.FirstNonEmpty(ch.GuildID, guildID)
		}
		if m.ReferencedMessage != nil && m.ReferencedMessage.GuildID == "" {
			m.ReferencedMessage.GuildID = m.GuildID
		}
		EnqueueDiscordMessage(ctx, server, dg, m, botUserID)
	}

	AdvanceChannelWatermark(ch.ID, ch.LastMessageID)
}

// RunCatchUpSweep performs a startup catch-up sweep across all available guilds and channels.
func RunCatchUpSweep(ctx context.Context, dg *discordgo.Session, server IngressServer, now time.Time) {
	if !discordSweepRunning.CompareAndSwap(false, true) {
		return
	}
	defer discordSweepRunning.Store(false)

	if dg == nil || isNilServer(server) {
		return
	}
	if ctx != nil && ctx.Err() != nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	cutoff := now.UTC().Add(-discordCatchUpStaleTTL)
	minSnowflake := DiscordSnowflakeFromTime(cutoff)
	botUserID := SessionBotUserID(dg)
	guilds := resolveCatchUpGuilds(dg)

	for _, g := range guilds {
		if g == nil || g.ID == "" {
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			return
		}
		candidates := collectCandidateChannels(dg, g)
		for _, ch := range candidates {
			if ctx != nil && ctx.Err() != nil {
				return
			}
			CatchUpChannelMessages(ctx, dg, server, ch, g.ID, botUserID, minSnowflake, now)
		}
	}
}
