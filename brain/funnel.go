package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
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

func resolveGuildID(s *discordgo.Session, m *discordgo.Message) string {
	if m == nil {
		return ""
	}
	if m.GuildID != "" {
		return m.GuildID
	}
	if s != nil {
		if s.State != nil {
			if ch, err := s.State.Channel(m.ChannelID); err == nil && ch != nil && ch.GuildID != "" {
				return ch.GuildID
			}
		}
		if s.Ratelimiter != nil && s.Token != "" {
			if ch, err := s.Channel(m.ChannelID); err == nil && ch != nil && ch.GuildID != "" {
				return ch.GuildID
			}
		}
	}
	return ""
}

func getOrCreateThreadID(s *discordgo.Session, m *discordgo.Message) (string, bool) {
	if m == nil {
		return "", false
	}
	guildID := resolveGuildID(s, m)
	if guildID == "" {
		return m.ChannelID, false
	}
	if s != nil && s.State != nil {
		if ch, err := s.State.Channel(m.ChannelID); err == nil && ch != nil && ch.IsThread() {
			return m.ChannelID, true
		}
	}
	if s != nil && s.Ratelimiter != nil && s.Token != "" {
		if ch, err := s.Channel(m.ChannelID); err == nil && ch != nil && ch.IsThread() {
			return m.ChannelID, true
		}
	}

	title := deriveThreadTitle(m.Content)
	if s != nil && s.Ratelimiter != nil && s.Token != "" {
		thread, err := s.MessageThreadStart(m.ChannelID, m.ID, title, 1440)
		if err != nil {
			log.Printf("Failed to create Discord thread for message %s (channel %s): %v", m.ID, m.ChannelID, err)
			return m.ChannelID, false
		}
		log.Printf("Created new Discord thread %q (ID: %s) for message %s in channel %s", title, thread.ID, m.ID, m.ChannelID)
		return thread.ID, true
	}
	return m.ChannelID, false
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
	if m == nil || m.Message == nil {
		return false
	}
	guildID := resolveGuildID(s, m.Message)
	if guildID == "" {
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
	if s != nil {
		if s.State != nil {
			if ch, err := s.State.Channel(m.ChannelID); err == nil && ch != nil {
				if ch.IsThread() {
					return true
				}
			}
		}
		if s.Ratelimiter != nil && s.Token != "" {
			if ch, err := s.Channel(m.ChannelID); err == nil && ch != nil {
				if ch.IsThread() {
					return true
				}
			}
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
	if pool != nil {
		pool.SetDiscordSession(dg)
	}

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		log.Printf("Discord funnel gateway session ready as %s#%s (user ID %s)", r.User.Username, r.User.Discriminator, r.User.ID)
		go RunStartupCatchUpSweep(context.Background(), database, pool, s)
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
			Summary:    db.CleanTaskSummary(m.Content),
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

var (
	sweepMu     sync.Mutex
	isSweeping  atomic.Bool
	lastSweepAt time.Time
)

func isMessageableChannel(chType discordgo.ChannelType) bool {
	switch chType {
	case discordgo.ChannelTypeGuildText,
		discordgo.ChannelTypeGuildNews,
		discordgo.ChannelTypeGuildNewsThread,
		discordgo.ChannelTypeGuildPublicThread,
		discordgo.ChannelTypeGuildPrivateThread:
		return true
	default:
		return false
	}
}

// RunStartupCatchUpSweep safely sweeps active channels and threads for missed messages during downtime.
func RunStartupCatchUpSweep(ctx context.Context, database *sql.DB, pool *queue.WorkerPool, s *discordgo.Session) {
	if s == nil || database == nil || pool == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	sweepMu.Lock()
	if !isSweeping.CompareAndSwap(false, true) {
		sweepMu.Unlock()
		log.Println("[CatchUpSweep] Sweep already in progress. Skipping duplicate run.")
		return
	}
	if time.Since(lastSweepAt) < 2*time.Minute {
		isSweeping.Store(false)
		sweepMu.Unlock()
		log.Printf("[CatchUpSweep] Sweep executed recently (%v ago). Skipping.", time.Since(lastSweepAt))
		return
	}
	lastSweepAt = time.Now()
	sweepMu.Unlock()

	defer isSweeping.Store(false)
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[CatchUpSweep] Panic recovered during sweep: %v", r)
		}
	}()

	sweepCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	lookbackHours := 2
	lookbackCutoff := time.Now().UTC().Add(-time.Duration(lookbackHours) * time.Hour)
	log.Printf("[CatchUpSweep] Starting startup message catch-up sweep (cutoff=%s)...", lookbackCutoff.Format(time.RFC3339))

	botUserID := ""
	if s.State != nil && s.State.User != nil {
		botUserID = s.State.User.ID
	}

	// 1. Gather target candidate channels:
	// Prioritize: recent active threads from SQLite DB, active guild threads, and top-level guild text channels.
	targetMap := make(map[string]bool)

	// A. Recent threads from DB (last 48 hours)
	if recentThreadIDs, err := db.GetActiveRecentThreadIDs(database, 48*time.Hour); err == nil {
		for _, thID := range recentThreadIDs {
			if thID != "" {
				targetMap[thID] = true
			}
		}
	}

	// B. Active Guild Threads & Guild Channels from state / REST
	var guilds []*discordgo.Guild
	if s.State != nil {
		guilds = s.State.Guilds
	}
	if len(guilds) == 0 {
		if userGuilds, err := s.UserGuilds(100, "", "", false); err == nil {
			for _, ug := range userGuilds {
				guilds = append(guilds, &discordgo.Guild{ID: ug.ID})
			}
		}
	}

	for _, g := range guilds {
		// Active threads in guild
		if activeThreads, err := s.GuildThreadsActive(g.ID); err == nil && activeThreads != nil {
			for _, th := range activeThreads.Threads {
				if th != nil && isMessageableChannel(th.Type) {
					targetMap[th.ID] = true
				}
			}
		}

		// Top-level guild channels
		if channels, err := s.GuildChannels(g.ID); err == nil {
			for _, ch := range channels {
				if ch != nil && isMessageableChannel(ch.Type) {
					targetMap[ch.ID] = true
				}
			}
		}
	}

	var targetChannels []string
	for chID := range targetMap {
		targetChannels = append(targetChannels, chID)
	}

	log.Printf("[CatchUpSweep] Found %d candidate channels/threads to check.", len(targetChannels))

	recoveredCount := 0
	skippedCount := 0
	consecutiveErrors := 0

	for _, chID := range targetChannels {
		select {
		case <-sweepCtx.Done():
			log.Println("[CatchUpSweep] Sweep aborted: context deadline exceeded.")
			return
		default:
		}

		// Pre-flight permission check if available in state
		if botUserID != "" && s.State != nil {
			if perms, err := s.State.UserChannelPermissions(botUserID, chID); err == nil {
				hasView := (perms & discordgo.PermissionViewChannel) != 0
				hasHistory := (perms & discordgo.PermissionReadMessageHistory) != 0
				if !hasView || !hasHistory {
					continue
				}
			}
		}

		// Fetch latest messages from Discord REST API
		fetched, err := s.ChannelMessages(chID, 50, "", "", "")
		if err != nil {
			consecutiveErrors++
			if consecutiveErrors >= 5 {
				log.Printf("[CatchUpSweep] Circuit breaker tripped (5 consecutive errors). Aborting sweep.")
				break
			}
			continue
		}
		consecutiveErrors = 0

		if len(fetched) == 0 {
			continue
		}

		// Reverse slice to restore chronological FIFO order (oldest -> newest)
		for i, j := 0, len(fetched)-1; i < j; i, j = i+1, j-1 {
			fetched[i], fetched[j] = fetched[j], fetched[i]
		}

		for _, m := range fetched {
			if m == nil || m.Author == nil || m.Author.Bot {
				continue
			}
			if botUserID != "" && m.Author.ID == botUserID {
				continue
			}

			msgTime := m.Timestamp
			if msgTime.Before(lookbackCutoff) {
				skippedCount++
				continue
			}

			// Check if message is targeted at the bot
			if !isFunnelBotTargeted(s, &discordgo.MessageCreate{Message: m}) {
				skippedCount++
				continue
			}

			// Check if already in SQLite DB
			exists, _ := db.MessageExists(database, m.ID)
			if exists {
				skippedCount++
				continue
			}

			targetThreadID, isThread := getOrCreateThreadID(s, m)
			prompt := buildDiscordPrompt(m, targetThreadID)

			authorID := m.Author.ID
			authorName := m.Author.Username
			resolvedGuildID := resolveGuildID(s, m)

			msg := db.Message{
				ID:         m.ID,
				ThreadID:   targetThreadID,
				GuildID:    resolvedGuildID,
				AuthorID:   authorID,
				AuthorName: authorName,
				Content:    prompt,
				Status:     db.StatusPending,
				CreatedAt:  msgTime,
				UpdatedAt:  time.Now().UTC(),
			}

			if err := db.InsertMessage(database, msg); err != nil {
				log.Printf("[CatchUpSweep] Failed to insert missed message %s: %v", m.ID, err)
				continue
			}

			log.Printf("[CatchUpSweep] Recovered missed message %s from %s (channel %s, target_thread: %s, is_thread: %t). Enqueued to worker pool.",
				m.ID, authorName, m.ChannelID, targetThreadID, isThread)

			pool.Enqueue(msg)
			recoveredCount++
		}
	}

	log.Printf("[CatchUpSweep] Catch-up sweep completed: scanned %d channels, recovered %d messages, skipped %d.",
		len(targetChannels), recoveredCount, skippedCount)
}
