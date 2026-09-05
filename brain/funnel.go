package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

const discordErrCodeThreadAlreadyCreated = 160004

func isThreadAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	var restErr *discordgo.RESTError
	if errors.As(err, &restErr) && restErr != nil {
		if restErr.Message != nil && restErr.Message.Code == discordErrCodeThreadAlreadyCreated {
			return true
		}
		if len(restErr.ResponseBody) > 0 {
			bodyStr := strings.ToLower(string(restErr.ResponseBody))
			if strings.Contains(bodyStr, "160004") || strings.Contains(bodyStr, "already been created") {
				return true
			}
		}
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "160004") || strings.Contains(errStr, "already been created")
}

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

func getDiscordChannel(s *discordgo.Session, channelID string) *discordgo.Channel {
	if channelID == "" {
		return nil
	}
	if s != nil && s.State != nil {
		if ch, err := s.State.Channel(channelID); err == nil && ch != nil {
			queue.CacheDiscordChannel(ch)
			return ch
		}
	}
	if snap, ok := queue.GetCachedChannel(channelID); ok {
		chType := discordgo.ChannelTypeGuildText
		if snap.IsThread {
			chType = discordgo.ChannelTypeGuildPublicThread
		}
		return &discordgo.Channel{
			ID:       snap.ID,
			Name:     snap.Name,
			ParentID: snap.ParentID,
			Type:     chType,
		}
	}
	if s != nil && s.Token != "" && queue.IsNumericSnowflake(channelID) {
		if ch, err := s.Channel(channelID); err == nil && ch != nil {
			queue.CacheDiscordChannel(ch)
			if s.State != nil {
				_ = s.State.ChannelAdd(ch)
			}
			return ch
		}
	}
	return nil
}

func resolveGuildID(s *discordgo.Session, m *discordgo.Message) string {
	if m == nil {
		return ""
	}
	if m.GuildID != "" {
		return m.GuildID
	}
	if ch := getDiscordChannel(s, m.ChannelID); ch != nil && ch.GuildID != "" {
		return ch.GuildID
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

	var channelName string
	var isAlreadyThread bool

	if ch := getDiscordChannel(s, m.ChannelID); ch != nil {
		channelName = ch.Name
		if ch.IsThread() {
			isAlreadyThread = true
		}
	}

	if isAlreadyThread {
		return m.ChannelID, true
	}

	policy := config.GetRuntimeConfig().ResolveChannelPolicy(m.ChannelID, channelName)
	if policy.Mode == "channel" {
		return m.ChannelID, false
	}

	// Fast in-memory cache check: if this message ID was already cached or stored in session state as a thread
	if snap, ok := queue.GetCachedChannel(m.ID); ok && snap.IsThread {
		return m.ID, true
	}
	if s != nil && s.State != nil {
		if ch, err := s.State.Channel(m.ID); err == nil && ch != nil && ch.IsThread() {
			queue.CacheDiscordChannel(ch)
			return m.ID, true
		}
	}

	title := deriveThreadTitle(m.Content)
	if s != nil && s.Token != "" {
		thread, err := s.MessageThreadStart(m.ChannelID, m.ID, title, 1440)
		if err != nil {
			if isThreadAlreadyExistsError(err) && queue.IsNumericSnowflake(m.ID) {
				metrics.RecordThreadCreated("already_exists")
				log.Printf("Thread already exists for message %s in channel %s (code 160004); resolving thread ID %s", m.ID, m.ChannelID, m.ID)
				var existingThread *discordgo.Channel
				if ch, fetchErr := s.Channel(m.ID); fetchErr == nil && ch != nil {
					existingThread = ch
				} else {
					existingThread = &discordgo.Channel{
						ID:       m.ID,
						GuildID:  guildID,
						ParentID: m.ChannelID,
						Type:     discordgo.ChannelTypeGuildPublicThread,
					}
				}
				if existingThread.ParentID == "" {
					existingThread.ParentID = m.ChannelID
				}
				if existingThread.GuildID == "" {
					existingThread.GuildID = guildID
				}
				queue.CacheDiscordChannel(existingThread)
				if s.State != nil {
					_ = s.State.ChannelAdd(existingThread)
				}
				return m.ID, true
			}
			metrics.RecordThreadCreated("error")
			log.Printf("Failed to create Discord thread for message %s (channel %s): %v", m.ID, m.ChannelID, err)
			return m.ChannelID, false
		} else if thread != nil {
			metrics.RecordThreadCreated("created")
			if thread.ParentID == "" {
				thread.ParentID = m.ChannelID
			}
			if thread.GuildID == "" {
				thread.GuildID = guildID
			}
			queue.CacheDiscordChannel(thread)
			if s.State != nil {
				_ = s.State.ChannelAdd(thread)
			}
		}
		log.Printf("Created new Discord thread %q (ID: %s) for message %s in channel %s", title, thread.ID, m.ID, m.ChannelID)
		return thread.ID, true
	}
	return m.ChannelID, false
}

func buildDiscordPrompt(m *discordgo.Message, targetThreadID string, policy config.ChannelPolicy) string {
	var sb strings.Builder
	sb.WriteString("<USER_REQUEST>\nHere's a message someone sent you from Discord:\n\n")
	sb.WriteString(fmt.Sprintf("- id: %s\n", m.ID))
	sb.WriteString(fmt.Sprintf("- channel_id: %s\n", m.ChannelID))
	sb.WriteString(fmt.Sprintf("- thread_id: %s\n", targetThreadID))
	sb.WriteString(fmt.Sprintf("- guild_id: %s\n", m.GuildID))

	isAdmin := false
	if m.Author != nil {
		isAdmin = config.GetRuntimeConfig().IsAdmin(m.Author.ID, m.Author.Username, m.Author.GlobalName)
		sb.WriteString(fmt.Sprintf("- author_id: %s\n", m.Author.ID))
		sb.WriteString(fmt.Sprintf("- author_username: %s\n", m.Author.Username))
		sb.WriteString(fmt.Sprintf("- author_global_name: %s\n", m.Author.GlobalName))
		sb.WriteString(fmt.Sprintf("- author_bot: %t\n", m.Author.Bot))
		sb.WriteString(fmt.Sprintf("- is_admin: %t\n", isAdmin))
	} else {
		sb.WriteString(fmt.Sprintf("- is_admin: %t\n", false))
	}

	if m.ReferencedMessage != nil && m.ReferencedMessage.Author != nil {
		sb.WriteString("- replying_to:\n")
		sb.WriteString(fmt.Sprintf("    author: \"@%s\"\n", m.ReferencedMessage.Author.Username))
		sb.WriteString(fmt.Sprintf("    content: %q\n", m.ReferencedMessage.Content))
	}

	sanitizedContent := strings.ReplaceAll(m.Content, "</USER_REQUEST>", "<\\/USER_REQUEST>")
	sanitizedContent = strings.ReplaceAll(sanitizedContent, "<USER_REQUEST>", "<\\USER_REQUEST>")
	sb.WriteString(fmt.Sprintf("- content: %s\n", sanitizedContent))
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

	if policy.Mode == "channel" {
		sb.WriteString("Please formulate your response and output it clearly. It will be delivered directly to the Discord channel.\n")
	} else {
		sb.WriteString("Please formulate your response and output it clearly. It will be delivered directly to the Discord thread.\n")
	}
	sb.WriteString("</USER_REQUEST>")
	return sb.String()
}

// resolveEffectiveChannelPolicy resolves the ChannelPolicy for a channel or thread.
// If channelID is a Discord thread, it resolves the parent channel's policy so threads
// inherit ignore/whitelisting rules from their parent channel.
func resolveEffectiveChannelPolicy(s *discordgo.Session, channelID string) (config.ChannelPolicy, bool) {
	effID, effName, isThread := queue.ResolveEffectiveChannel(s, channelID)
	return config.GetRuntimeConfig().ResolveChannelPolicy(effID, effName), isThread
}

// isFunnelBotTargeted returns true if the message should trigger the funnel worker pool.
func isFunnelBotTargeted(s *discordgo.Session, m *discordgo.MessageCreate) bool {
	if m == nil || m.Message == nil || m.Author == nil {
		return false
	}

	botUserID := ""
	if s != nil && s.State != nil && s.State.User != nil {
		botUserID = s.State.User.ID
	}
	if botUserID != "" && m.Author.ID == botUserID {
		return false
	}

	policy, isThread := resolveEffectiveChannelPolicy(s, m.ChannelID)
	if policy.IsIgnored() {
		return false
	}
	if policy.IsBotIgnored() && m.Author.Bot {
		return false
	}

	guildID := resolveGuildID(s, m.Message)
	if guildID == "" {
		return true
	}

	if policy.Mode == "channel" {
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
	if isThread {
		return true
	}
	return false
}

func connectDiscordFunnel(database *sql.DB, pool *queue.WorkerPool) *discordgo.Session {
	token := config.GetEnv("DISCORD_TOKEN", config.GetEnv("DISCORD_BOT_TOKEN", ""))
	if token == "" {
		log.Println("Discord funnel disabled: DISCORD_BOT_TOKEN/DISCORD_TOKEN not configured")
		return nil
	}

	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		log.Printf("Discord funnel failed to create session: %v", err)
		return nil
	}
	if pool != nil {
		pool.SetDiscordSession(dg)
	}

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		metrics.DiscordEventsTotal.WithLabelValues("ready").Inc()
		log.Printf("Discord funnel gateway session ready as %s#%s (user ID %s)", r.User.Username, r.User.Discriminator, r.User.ID)
		if s.State != nil {
			for _, g := range s.State.Guilds {
				if g != nil {
					for _, ch := range g.Channels {
						queue.CacheDiscordChannel(ch)
					}
					for _, th := range g.Threads {
						queue.CacheDiscordChannel(th)
					}
				}
			}
		}
		go RunStartupCatchUpSweep(context.Background(), database, pool, s)
	})

	dg.AddHandler(func(s *discordgo.Session, g *discordgo.GuildCreate) {
		metrics.DiscordEventsTotal.WithLabelValues("guild_create").Inc()
		if g != nil && g.Guild != nil {
			for _, ch := range g.Channels {
				queue.CacheDiscordChannel(ch)
			}
			for _, th := range g.Threads {
				queue.CacheDiscordChannel(th)
			}
		}
	})

	dg.AddHandler(func(s *discordgo.Session, c *discordgo.ChannelCreate) {
		metrics.DiscordEventsTotal.WithLabelValues("channel_create").Inc()
		if c != nil && c.Channel != nil {
			queue.CacheDiscordChannel(c.Channel)
		}
	})

	dg.AddHandler(func(s *discordgo.Session, c *discordgo.ChannelUpdate) {
		metrics.DiscordEventsTotal.WithLabelValues("channel_update").Inc()
		if c != nil && c.Channel != nil {
			queue.CacheDiscordChannel(c.Channel)
		}
	})

	dg.AddHandler(func(s *discordgo.Session, c *discordgo.ChannelDelete) {
		metrics.DiscordEventsTotal.WithLabelValues("channel_delete").Inc()
		if c != nil && c.Channel != nil {
			queue.InvalidateChannelCache(c.Channel.ID)
		}
	})

	dg.AddHandler(func(s *discordgo.Session, t *discordgo.ThreadCreate) {
		metrics.DiscordEventsTotal.WithLabelValues("thread_create").Inc()
		if t != nil && t.Channel != nil {
			queue.CacheDiscordChannel(t.Channel)
		}
	})

	dg.AddHandler(func(s *discordgo.Session, t *discordgo.ThreadUpdate) {
		metrics.DiscordEventsTotal.WithLabelValues("thread_update").Inc()
		if t != nil && t.Channel != nil {
			queue.CacheDiscordChannel(t.Channel)
		}
	})

	dg.AddHandler(func(s *discordgo.Session, t *discordgo.ThreadDelete) {
		metrics.DiscordEventsTotal.WithLabelValues("thread_delete").Inc()
		if t != nil && t.Channel != nil {
			queue.InvalidateChannelCache(t.Channel.ID)
		}
	})

	dg.AddHandler(func(s *discordgo.Session, d *discordgo.Disconnect) {
		metrics.DiscordEventsTotal.WithLabelValues("disconnect").Inc()
		metrics.RecordGatewayReconnect()
		log.Printf("Discord funnel disconnected from gateway (discordgo will reconnect automatically)")
	})

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Resumed) {
		metrics.DiscordEventsTotal.WithLabelValues("resumed").Inc()
		metrics.RecordGatewayReconnect()
		log.Printf("Discord funnel gateway connection resumed successfully")
	})

	dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		if m.Author == nil || (s.State != nil && s.State.User != nil && m.Author.ID == s.State.User.ID) {
			return
		}
		metrics.DiscordEventsTotal.WithLabelValues("message_create").Inc()

		go func() {
			if !isFunnelBotTargeted(s, m) {
				metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "ignored").Inc()
				log.Printf("Discord funnel ignoring message %s from %s: no trigger matched", m.ID, m.Author.Username)
				return
			}

			targetThreadID, isThread := getOrCreateThreadID(s, m.Message)
			policy, _ := resolveEffectiveChannelPolicy(s, m.ChannelID)
			prompt := buildDiscordPrompt(m.Message, targetThreadID, policy)

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
				CreatedAt:  m.Timestamp,
				UpdatedAt:  time.Now().UTC(),
			}

			if err := db.InsertMessage(database, msg); err != nil {
				log.Printf("Failed to insert message %s: %v", m.ID, err)
				return
			}

			metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "enqueued").Inc()
			log.Printf("Discord funnel enqueued message %s from %s (thread: %s, is_thread: %t)", m.ID, authorName, targetThreadID, isThread)
			if pool != nil {
				pool.Enqueue(msg)
			}
		}()
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

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if dg != nil {
				metrics.RecordGatewayLatency(dg.HeartbeatLatency())
			}
		}
	}()

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
	// Prioritize: recent active threads from DB, active guild threads, and top-level guild text channels.
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
	if len(guilds) == 0 && s.Token != "" {
		if userGuilds, err := s.UserGuilds(100, "", "", false); err == nil {
			for _, ug := range userGuilds {
				guilds = append(guilds, &discordgo.Guild{ID: ug.ID})
			}
		}
	}

	for _, g := range guilds {
		if s.Token == "" {
			break
		}
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

		policy, isThread := resolveEffectiveChannelPolicy(s, chID)
		if policy.IsIgnored() {
			log.Printf("[CatchUpSweep] Skipping ignored channel/thread %s (is_thread=%t)", chID, isThread)
			continue
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
			if m == nil || m.Author == nil {
				continue
			}
			if m.Author.Bot && policy.IsBotIgnored() {
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

			// Check if already in DB
			exists, _ := db.MessageExists(database, m.ID)
			if exists {
				skippedCount++
				continue
			}

			targetThreadID, isThread := getOrCreateThreadID(s, m)
			sweepPolicy, _ := resolveEffectiveChannelPolicy(s, m.ChannelID)
			prompt := buildDiscordPrompt(m, targetThreadID, sweepPolicy)

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
