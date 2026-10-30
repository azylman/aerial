package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/discordgo"
)

const (
	discordEpochMillis     int64 = 1420070400000
	discordCatchUpStaleTTL       = 30 * time.Minute
)

var (
	discordSweepRunning atomic.Bool

	channelWatermarksMu sync.RWMutex
	channelWatermarks   = make(map[string]string)
)

func parseSnowflakeUint64(s string) uint64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func discordSnowflakeFromTime(t time.Time) string {
	ms := t.UTC().UnixMilli() - discordEpochMillis
	if ms <= 0 {
		return "0"
	}
	return strconv.FormatInt(ms<<22, 10)
}

func compareDiscordSnowflakes(a, b string) int {
	valA := parseSnowflakeUint64(a)
	valB := parseSnowflakeUint64(b)
	if valA < valB {
		return -1
	}
	if valA > valB {
		return 1
	}
	return 0
}

func maxDiscordSnowflake(a, b string) string {
	cmp := compareDiscordSnowflakes(a, b)
	if cmp > 0 {
		return a
	}
	if cmp < 0 {
		return b
	}
	if a != "" {
		return a
	}
	return b
}

func getChannelWatermark(channelID string) string {
	channelWatermarksMu.RLock()
	defer channelWatermarksMu.RUnlock()
	return channelWatermarks[channelID]
}

func advanceChannelWatermark(channelID, messageID string) {
	if channelID == "" || messageID == "" {
		return
	}
	channelWatermarksMu.Lock()
	defer channelWatermarksMu.Unlock()
	curr := channelWatermarks[channelID]
	if compareDiscordSnowflakes(messageID, curr) > 0 {
		channelWatermarks[channelID] = messageID
	}
}

var discordGatewayOpener = func(dg *discordgo.Session) error {
	return dg.Open()
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

// startDiscordGateway initializes and starts the Discord Gateway WebSocket connection.
func startDiscordGateway(ctx context.Context, server *RouterServer, token string) (func(), error) {
	cleanToken := strings.TrimSpace(token)
	if cleanToken == "" {
		log.Println("[webhooks-router] [discord] DISCORD_TOKEN is empty; Discord Gateway disabled")
		return func() {}, nil
	}

	botToken := cleanToken
	if !strings.HasPrefix(botToken, "Bot ") {
		botToken = "Bot " + botToken
	}

	dg, err := discordgo.New(botToken)
	if err != nil {
		return nil, fmt.Errorf("create discordgo session: %w", err)
	}

	dg.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
	dg.SyncEvents = false

	dg.AddHandler(newDiscordGatewayMessageCreateHandler(ctx, server))
	dg.AddHandler(newDiscordGatewayInteractionCreateHandler(ctx, server))
	dg.AddHandler(newDiscordGatewayReadyHandler(ctx, server))

	if err := discordGatewayOpener(dg); err != nil {
		return nil, fmt.Errorf("open discord gateway: %w", err)
	}
	log.Println("[webhooks-router] [discord] Discord Gateway connected successfully")

	var stopOnce sync.Once
	stopFn := func() {
		stopOnce.Do(func() {
			if err := dg.Close(); err != nil {
				log.Printf("[webhooks-router] [discord] error closing discord gateway session: %v", err)
			}
		})
	}
	return stopFn, nil
}

func newDiscordGatewayMessageCreateHandler(ctx context.Context, server *RouterServer) func(s *discordgo.Session, m *discordgo.MessageCreate) {
	return func(s *discordgo.Session, m *discordgo.MessageCreate) {
		handleGatewayMessageCreate(ctx, server, s, m)
	}
}

func newDiscordGatewayInteractionCreateHandler(ctx context.Context, server *RouterServer) func(s *discordgo.Session, i *discordgo.InteractionCreate) {
	return func(s *discordgo.Session, i *discordgo.InteractionCreate) {
		handleGatewayInteractionCreate(ctx, server, i)
	}
}

func newDiscordGatewayReadyHandler(ctx context.Context, server *RouterServer) func(s *discordgo.Session, r *discordgo.Ready) {
	return func(s *discordgo.Session, r *discordgo.Ready) {
		handleGatewayReady(ctx, server, s, r)
	}
}

func handleGatewayReady(ctx context.Context, server *RouterServer, s *discordgo.Session, r *discordgo.Ready) {
	if s != nil && r != nil && r.User != nil {
		if s.State != nil {
			s.State.Lock()
			if s.State.User == nil || s.State.User.ID == "" {
				s.State.User = r.User
			}
			s.State.Unlock()
		}
	}
	go runDiscordCatchUpSweep(ctx, s, server, time.Now().UTC())
}

func sessionBotUserID(s *discordgo.Session) string {
	if s == nil || s.State == nil {
		return ""
	}
	s.State.RLock()
	defer s.State.RUnlock()
	if s.State.User == nil {
		return ""
	}
	return s.State.User.ID
}

func extractMentionRoleNames(s *discordgo.Session, guildID string, roleGroups ...[]string) map[string]string {
	if s == nil || s.State == nil || guildID == "" {
		return nil
	}
	names := make(map[string]string)
	for _, group := range roleGroups {
		for _, roleID := range group {
			if r, err := s.State.Role(guildID, roleID); err == nil && r != nil && r.Name != "" {
				names[roleID] = r.Name
			}
		}
	}
	return names
}

func handleGatewayMessageCreate(ctx context.Context, server *RouterServer, s *discordgo.Session, m *discordgo.MessageCreate) {
	if m != nil && m.ChannelID != "" && m.ID != "" {
		advanceChannelWatermark(m.ChannelID, m.ID)
	}
	if server == nil || m == nil || m.Message == nil || m.Author == nil {
		return
	}
	botUserID := sessionBotUserID(s)
	if botUserID != "" && m.Author.ID == botUserID {
		return
	}
	enqueueDiscordMessage(ctx, server, s, m.Message, botUserID)
}

func enqueueDiscordMessage(ctx context.Context, server *RouterServer, s *discordgo.Session, m *discordgo.Message, botUserID string) {
	if server == nil || m == nil || m.Author == nil {
		return
	}
	if botUserID != "" && m.Author.ID == botUserID {
		return
	}

	if m.ReferencedMessage != nil && m.ReferencedMessage.GuildID == "" {
		m.ReferencedMessage.GuildID = m.GuildID
	}

	var refRoles []string
	if m.ReferencedMessage != nil {
		refRoles = m.ReferencedMessage.MentionRoles
	}
	roleNames := extractMentionRoleNames(s, m.GuildID, m.MentionRoles, refRoles)

	args := DiscordIngressArgs{
		MessageID: m.ID,
		BotUserID: botUserID,
		RoleNames: roleNames,
		Message:   m,
	}

	enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if server.riverClient != nil {
		if err := server.insertRiverJob(enqueueCtx, args); err != nil {
			log.Printf("[webhooks-router] [discord] failed to enqueue message %s to river: %v", m.ID, err)
		}
	} else {
		if err := server.ProcessDiscordIngress(enqueueCtx, args); err != nil {
			log.Printf("[webhooks-router] [discord] failed to process message %s directly: %v", m.ID, err)
		}
	}
}

func resolveCatchUpGuilds(dg *discordgo.Session) []*discordgo.Guild {
	if dg == nil {
		return nil
	}
	var guilds []*discordgo.Guild
	if dg.State != nil {
		dg.State.RLock()
		for _, g := range dg.State.Guilds {
			if g == nil || g.ID == "" {
				continue
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
			guilds = append(guilds, snap)
		}
		dg.State.RUnlock()
	}
	if len(guilds) == 0 && dg.Token != "" {
		if ugs, err := dg.UserGuilds(100, "", "", false); err == nil {
			for _, ug := range ugs {
				if ug != nil && ug.ID != "" {
					guilds = append(guilds, &discordgo.Guild{ID: ug.ID})
				}
			}
		} else {
			log.Printf("[webhooks-router] [discord] failed to fetch user guilds: %v", err)
		}
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
			log.Printf("[webhooks-router] [discord] failed to fetch guild channels for guild %s: %v", g.ID, err)
		}

		if activeThreads, err := dg.GuildThreadsActive(g.ID); err == nil && activeThreads != nil {
			for _, th := range activeThreads.Threads {
				addCandidate(th)
			}
		} else if err != nil {
			log.Printf("[webhooks-router] [discord] failed to fetch active threads for guild %s: %v", g.ID, err)
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
	if (!m.Timestamp.IsZero() && now.Sub(m.Timestamp) > discordCatchUpStaleTTL) || compareDiscordSnowflakes(m.ID, minSnowflake) <= 0 {
		return false
	}
	return true
}

func catchUpChannelMessages(ctx context.Context, dg *discordgo.Session, server *RouterServer, ch *discordgo.Channel, guildID, botUserID, minSnowflake string, now time.Time) {
	if ch == nil || ch.ID == "" || dg == nil || server == nil {
		return
	}
	afterID := maxDiscordSnowflake(getChannelWatermark(ch.ID), minSnowflake)
	if ch.LastMessageID == "" || compareDiscordSnowflakes(ch.LastMessageID, afterID) <= 0 {
		return
	}

	fetched, err := dg.ChannelMessages(ch.ID, 50, "", afterID, "")
	if err != nil {
		log.Printf("[webhooks-router] [discord] failed to fetch channel messages for channel %s: %v", ch.ID, err)
		return
	}

	for i := len(fetched) - 1; i >= 0; i-- {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		m := fetched[i]
		if m == nil || m.ID == "" {
			continue
		}
		advanceChannelWatermark(ch.ID, m.ID)
		if !isCatchUpMessageEligible(m, botUserID, minSnowflake, now) {
			continue
		}
		if m.GuildID == "" {
			m.GuildID = firstNonEmpty(ch.GuildID, guildID)
		}
		if m.ReferencedMessage != nil && m.ReferencedMessage.GuildID == "" {
			m.ReferencedMessage.GuildID = m.GuildID
		}
		enqueueDiscordMessage(ctx, server, dg, m, botUserID)
	}

	advanceChannelWatermark(ch.ID, ch.LastMessageID)
}

func runDiscordCatchUpSweep(ctx context.Context, dg *discordgo.Session, server *RouterServer, now time.Time) {
	if !discordSweepRunning.CompareAndSwap(false, true) {
		return
	}
	defer discordSweepRunning.Store(false)

	if dg == nil || server == nil {
		return
	}
	if ctx != nil && ctx.Err() != nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	cutoff := now.UTC().Add(-discordCatchUpStaleTTL)
	minSnowflake := discordSnowflakeFromTime(cutoff)
	botUserID := sessionBotUserID(dg)
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
			catchUpChannelMessages(ctx, dg, server, ch, g.ID, botUserID, minSnowflake, now)
		}
	}
}

func handleGatewayInteractionCreate(ctx context.Context, server *RouterServer, i *discordgo.InteractionCreate) {
	if server == nil || i == nil || i.Interaction == nil || i.Type != discordgo.InteractionApplicationCommand {
		return
	}

	args := DiscordInteractionArgs{
		InteractionID: i.ID,
		Interaction:   i.Interaction,
	}

	enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()

	if server.riverClient != nil {
		if err := server.insertRiverJob(enqueueCtx, args); err != nil {
			log.Printf("[webhooks-router] [discord] failed to enqueue interaction %s to river: %v", i.ID, err)
		}
	} else {
		if err := server.ProcessDiscordInteraction(enqueueCtx, args); err != nil {
			log.Printf("[webhooks-router] [discord] failed to process interaction %s directly: %v", i.ID, err)
		}
	}
}
