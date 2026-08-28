package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/bwmarrin/discordgo"
)

func getFunnelConversationID(s *discordgo.Session, m *discordgo.Message) (string, bool) {
	if m.GuildID == "" {
		return m.ChannelID, false
	}
	if ch, err := s.State.Channel(m.ChannelID); err == nil && ch != nil && ch.IsThread() {
		return m.ChannelID, true
	}
	if ch, err := s.Channel(m.ChannelID); err == nil && ch != nil && ch.IsThread() {
		return m.ChannelID, true
	}
	return m.ID, false
}

func buildDiscordPrompt(m *discordgo.Message, isThread bool) string {
	var sb strings.Builder
	sb.WriteString("<USER_REQUEST>\nHere's a message someone sent you from Discord:\n\n")
	sb.WriteString(fmt.Sprintf("- id: %s\n", m.ID))
	sb.WriteString(fmt.Sprintf("- channel_id: %s\n", m.ChannelID))
	sb.WriteString(fmt.Sprintf("- guild_id: %s\n", m.GuildID))
	if m.Author != nil {
		sb.WriteString(fmt.Sprintf("- author_id: %s\n", m.Author.ID))
		sb.WriteString(fmt.Sprintf("- author_username: %s\n", m.Author.Username))
		sb.WriteString(fmt.Sprintf("- author_global_name: %s\n", m.Author.GlobalName))
		sb.WriteString(fmt.Sprintf("- author_bot: %t\n", m.Author.Bot))
	}
	sb.WriteString(fmt.Sprintf("- content: %s\n", m.Content))
	sb.WriteString(fmt.Sprintf("- timestamp: %s\n", m.Timestamp.Format(time.RFC3339)))

	var mentions []string
	for _, u := range m.Mentions {
		if u != nil {
			mentions = append(mentions, u.Username)
		}
	}
	sb.WriteString(fmt.Sprintf("- mentions: %v\n", mentions))

	var attachments []string
	for _, a := range m.Attachments {
		if a != nil {
			attachments = append(attachments, a.URL)
		}
	}
	sb.WriteString(fmt.Sprintf("- attachments: %v\n\n", attachments))

	sb.WriteString("CRITICAL REQUIREMENT: You MUST send your response back to Discord using the Discord MCP tool via `call_mcp_tool` with ServerName: \"discord\":\n")
	if isThread {
		sb.WriteString(fmt.Sprintf("1. This message is ALREADY inside a thread: Execute `call_mcp_tool` with ServerName: \"discord\", ToolName: \"discord_send\", and Arguments: {\"channelId\": %q, \"message\": \"<your response>\"}.\n", m.ChannelID))
	} else {
		sb.WriteString(fmt.Sprintf("1. This message is a new inquiry (NOT inside a thread): Execute `call_mcp_tool` with ServerName: \"discord\", ToolName: \"discord_create_thread\", and Arguments: {\"channelId\": %q, \"messageId\": %q, \"name\": \"<concise thread title>\", \"message\": \"<your response>\"}.\n", m.ChannelID, m.ID))
	}
	sb.WriteString("Execute the tool call now.\n</USER_REQUEST>")
	return sb.String()
}

func isFunnelBotTargeted(s *discordgo.Session, m *discordgo.MessageCreate) bool {
	if m.GuildID == "" {
		return true
	}
	if len(m.Mentions) > 0 {
		return true
	}
	contentLower := strings.ToLower(m.Content)
	if strings.Contains(m.Content, "<@") ||
		strings.Contains(contentLower, "aerial") ||
		strings.Contains(contentLower, "gundam") ||
		strings.Contains(contentLower, "brain") ||
		strings.Contains(contentLower, "bot") {
		return true
	}
	if m.ReferencedMessage != nil || m.MessageReference != nil {
		return true
	}
	if len(m.MentionRoles) > 0 {
		return true
	}
	if ch, err := s.State.Channel(m.ChannelID); err == nil && ch != nil {
		if ch.IsThread() {
			return true
		}
	} else if ch, err := s.Channel(m.ChannelID); err == nil && ch != nil {
		if ch.IsThread() {
			return true
		}
	}
	return false
}

func startDiscordFunnel(database *sql.DB, agyBin, apiKey, model, systemPrompt string, timeoutMinutes int, mcpConfig json.RawMessage) {
	token := config.GetEnv("DISCORD_TOKEN", config.GetEnv("DISCORD_BOT_TOKEN", ""))
	if token == "" {
		log.Println("Discord funnel disabled: DISCORD_BOT_TOKEN/DISCORD_TOKEN not configured")
		return
	}

	mentionsOnly := os.Getenv("MENTIONS_ONLY") == "true"

	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		dg, err := discordgo.New("Bot " + token)
		if err != nil {
			log.Printf("Discord funnel failed to create session: %v. Retrying in %v...", err, backoff)
			time.Sleep(backoff)
			backoff = backoff * 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		dg.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
			log.Printf("Discord funnel gateway session ready as %s#%s (user ID %s)", r.User.Username, r.User.Discriminator, r.User.ID)
			backoff = 1 * time.Second
		})

		dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
			if m.Author == nil || (s.State.User != nil && m.Author.ID == s.State.User.ID) {
				return
			}

			if mentionsOnly && !isFunnelBotTargeted(s, m) {
				log.Printf("Discord funnel ignoring message %s from %s: no trigger matched", m.ID, m.Author.Username)
				return
			}

			convID, isThread := getFunnelConversationID(s, m.Message)
			prompt := buildDiscordPrompt(m.Message, isThread)

			req := PromptRequest{
				ConversationID: convID,
				Prompt:         prompt,
			}

			log.Printf("Discord funnel received message %s from %s (channel %s, is_thread: %t, conversation_id: %s)",
				m.ID, m.Author.Username, m.ChannelID, isThread, convID)

			stopTyping := make(chan struct{})
			var once sync.Once
			onComplete := func() {
				once.Do(func() {
					close(stopTyping)
				})
			}

			go func(channelID string) {
				_ = s.ChannelTyping(channelID)
				ticker := time.NewTicker(5 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						_ = s.ChannelTyping(channelID)
					case <-stopTyping:
						return
					}
				}
			}(m.ChannelID)

			executePrompt(database, req, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig, onComplete)
		})

		dg.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
		dg.SyncEvents = false

		disconnectCh := make(chan struct{}, 1)
		dg.AddHandler(func(s *discordgo.Session, d *discordgo.Disconnect) {
			log.Printf("Discord funnel disconnected from gateway. Reconnecting...")
			select {
			case disconnectCh <- struct{}{}:
			default:
			}
		})

		if err := dg.Open(); err != nil {
			log.Printf("Discord funnel failed to open session: %v. Retrying in %v...", err, backoff)
			time.Sleep(backoff)
			backoff = backoff * 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		log.Printf("Discord funnel worker started successfully inside Brain")
		<-disconnectCh
		_ = dg.Close()

		log.Printf("Discord funnel session closed. Attempting reconnect in %v...", backoff)
		time.Sleep(backoff)
		backoff = backoff * 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}