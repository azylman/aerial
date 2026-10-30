package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
)

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

func sessionBotUserID(s *discordgo.Session) string {
	if s == nil || s.State == nil || s.State.User == nil {
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
	if server == nil || m == nil || m.Message == nil || m.Author == nil {
		return
	}
	botUserID := sessionBotUserID(s)
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
		Message:   m.Message,
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
