package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

func deriveThreadTitle(content string) string {
	re := regexp.MustCompile(`<@!?[0-9]+>`)
	cleaned := re.ReplaceAllString(content, "")
	cleaned = strings.TrimSpace(cleaned)
	lines := strings.Split(cleaned, "\n")
	firstLine := strings.TrimSpace(lines[0])
	if firstLine == "" && len(lines) > 1 {
		firstLine = strings.TrimSpace(lines[1])
	}
	if firstLine == "" {
		firstLine = "Aerial Discussion"
	}
	runes := []rune(firstLine)
	if len(runes) > 60 {
		return string(runes[:57]) + "..."
	}
	return string(runes)
}

func getOrCreateThreadID(s *discordgo.Session, m *discordgo.Message) (string, bool) {
	if m.GuildID == "" {
		return m.ChannelID, false
	}
	if ch, err := s.State.Channel(m.ChannelID); err == nil && ch != nil && ch.IsThread() {
		return m.ChannelID, true
	}
	if ch, err := s.Channel(m.ChannelID); err == nil && ch != nil && ch.IsThread() {
		return m.ChannelID, true
	}

	title := deriveThreadTitle(m.Content)
	thread, err := s.MessageThreadStart(m.ChannelID, m.ID, title, 1440)
	if err != nil {
		log.Printf("Failed to create Discord thread for message %s (channel %s): %v", m.ID, m.ChannelID, err)
		return m.ChannelID, false
	}
	log.Printf("Created new Discord thread %q (ID: %s) for message %s in channel %s", title, thread.ID, m.ID, m.ChannelID)
	return thread.ID, true
}

func buildDiscordPrompt(m *discordgo.Message, targetThreadID string) string {
	var sb strings.Builder
	sb.WriteString("<USER_REQUEST>\nHere's a message someone sent you from Discord:\n\n")
	sb.WriteString(fmt.Sprintf("- id: %s\n", m.ID))
	sb.WriteString(fmt.Sprintf("- channel_id: %s\n", m.ChannelID))
	sb.WriteString(fmt.Sprintf("- thread_id: %s\n", targetThreadID))
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
	sb.WriteString("Please formulate your response and output it clearly. It will be delivered directly to the Discord thread.\n</USER_REQUEST>")
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

func connectDiscordFunnel(database *sql.DB, pool *queue.WorkerPool) *discordgo.Session {
	token := config.GetEnv("DISCORD_TOKEN", config.GetEnv("DISCORD_BOT_TOKEN", ""))
	if token == "" {
		log.Println("Discord funnel disabled: DISCORD_BOT_TOKEN/DISCORD_TOKEN not configured")
		return nil
	}

	mentionsOnly := os.Getenv("MENTIONS_ONLY") == "true"

	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		log.Printf("Discord funnel failed to create session: %v", err)
		return nil
	}
	setGlobalDiscordSession(dg)
	if pool != nil {
		pool.SetDiscordSession(dg)
	}

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		log.Printf("Discord funnel gateway session ready as %s#%s (user ID %s)", r.User.Username, r.User.Discriminator, r.User.ID)
	})

	dg.AddHandler(func(s *discordgo.Session, d *discordgo.Disconnect) {
		log.Printf("Discord funnel disconnected from gateway (discordgo will reconnect automatically)")
	})

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Resumed) {
		log.Printf("Discord funnel gateway connection resumed successfully")
	})

	dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		if m.Author == nil || (s.State.User != nil && m.Author.ID == s.State.User.ID) {
			return
		}

		if mentionsOnly && !isFunnelBotTargeted(s, m) {
			log.Printf("Discord funnel ignoring message %s from %s: no trigger matched", m.ID, m.Author.Username)
			return
		}

		targetThreadID, isThread := getOrCreateThreadID(s, m.Message)
		prompt := buildDiscordPrompt(m.Message, targetThreadID)

		authorID := ""
		authorName := "Discord User"
		if m.Author != nil {
			authorID = m.Author.ID
			authorName = m.Author.Username
		}

		msg := db.Message{
			ID:         m.ID,
			ThreadID:   targetThreadID,
			GuildID:    m.GuildID,
			AuthorID:   authorID,
			AuthorName: authorName,
			Content:    prompt,
			Status:     db.StatusPending,
			CreatedAt:  time.Now().UTC(),
			UpdatedAt:  time.Now().UTC(),
		}

		if err := db.InsertMessage(database, msg); err != nil {
			log.Printf("Failed to persist Discord message %s to SQLite: %v", m.ID, err)
		}

		log.Printf("Discord funnel received message %s from %s (channel %s, target_thread: %s, is_thread: %t). Enqueued to worker pool.",
			m.ID, authorName, m.ChannelID, targetThreadID, isThread)

		if pool != nil {
			pool.Enqueue(msg)
		}
	})

	dg.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
	dg.SyncEvents = false

	if err := dg.Open(); err != nil {
		log.Printf("Warning: Discord funnel failed to open initial session: %v. Retrying in background...", err)
		go func() {
			backoff := 2 * time.Second
			maxBackoff := 60 * time.Second
			for {
				if err := dg.Open(); err != nil {
					log.Printf("Discord funnel retry failed: %v. Retrying in %v...", err, backoff)
					time.Sleep(backoff)
					backoff = backoff * 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				}
				log.Printf("Discord funnel worker started successfully inside Brain")
				break
			}
		}()
	} else {
		log.Printf("Discord funnel worker connected successfully inside Brain")
	}

	return dg
}
