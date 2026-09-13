package main

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

var funnelCfg atomic.Pointer[config.Config]
var funnelPool atomic.Pointer[queue.WorkerPool]
var titleSummarizeSem = make(chan struct{}, 2)
var discordSessionOpener = func(dg *discordgo.Session) error {
	return dg.Open()
}
var funnelRetryBackoff = 2 * time.Second

// SetFunnelConfig sets the active *config.Config for the Discord funnel.
func SetFunnelConfig(cfg *config.Config) {
	funnelCfg.Store(cfg)
}

func currentFunnelConfig() *config.Config {
	if c := funnelCfg.Load(); c != nil {
		return c
	}
	return config.ActiveConfig()
}

// SetFunnelPool sets the active *queue.WorkerPool for the Discord funnel.
func SetFunnelPool(pool *queue.WorkerPool) {
	funnelPool.Store(pool)
}

func currentFunnelClassifier() *classifier.Classifier {
	if pool := funnelPool.Load(); pool != nil {
		return pool.Classifier()
	}
	return nil
}

func isThreadAlreadyExistsError(err error) bool {
	return IsThreadAlreadyExistsError(err)
}

func deriveThreadTitle(content string) string {
	return DeriveThreadTitle(content)
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
			GuildID:  snap.GuildID,
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
	cachedGuildID := ""
	if snap, ok := queue.GetCachedChannel(m.ChannelID); ok && snap.GuildID != "" {
		cachedGuildID = snap.GuildID
	}
	channelGuildID := ""
	if m.GuildID == "" && cachedGuildID == "" {
		if ch := getDiscordChannel(s, m.ChannelID); ch != nil && ch.GuildID != "" {
			channelGuildID = ch.GuildID
		}
	}
	return ResolveGuildID(m.GuildID, cachedGuildID, channelGuildID)
}

func getOrCreateThreadID(s *discordgo.Session, m *discordgo.Message, allowSummarize ...bool) (string, bool) {
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

	policy := config.ResolveChannelPolicy(currentFunnelConfig().Current().Channels, m.ChannelID, channelName)
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

	shouldSummarize := true
	if len(allowSummarize) > 0 {
		shouldSummarize = allowSummarize[0]
	}

	title := ""
	if shouldSummarize {
		cls := currentFunnelClassifier()
		if cls != nil {
			select {
			case titleSummarizeSem <- struct{}{}:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				var sumErr error
				title, sumErr = cls.SummarizeThreadTitle(ctx, m.Content)
				cancel()
				<-titleSummarizeSem
				if sumErr != nil {
					log.Printf("Thread title summarization skipped/failed for message %s: %v", m.ID, sumErr)
					title = ""
				}
			default:
				log.Printf("Thread title summarization semaphore full, falling back to deriveThreadTitle for message %s", m.ID)
			}
		}
	}
	if title == "" {
		title = deriveThreadTitle(m.Content)
	}

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

func buildDiscordPrompt(s *discordgo.Session, m *discordgo.Message, targetThreadID string, policy config.ChannelPolicy) string {
	if m == nil {
		return ""
	}
	roleNames := make(map[string]string)
	if s != nil && s.State != nil && m.GuildID != "" {
		for _, roleID := range m.MentionRoles {
			if r, err := s.State.Role(m.GuildID, roleID); err == nil && r != nil && r.Name != "" {
				roleNames[roleID] = r.Name
			}
		}
	}
	var adminUsers []string
	var tz string
	if cfg := currentFunnelConfig(); cfg != nil {
		cur := cfg.Current()
		adminUsers = cur.AdminUsers
		tz = cur.Timezone
	}
	input := DiscordPromptInput{
		Message:        m,
		TargetThreadID: targetThreadID,
		Policy:         policy,
		AdminUsers:     adminUsers,
		Timezone:       tz,
		RoleNames:      roleNames,
	}
	return BuildDiscordPrompt(input)
}

// resolveEffectiveChannelPolicy resolves the ChannelPolicy for a channel or thread.
// If channelID is a Discord thread, it resolves the parent channel's policy so threads
// inherit ignore/whitelisting rules from their parent channel.
func resolveEffectiveChannelPolicy(s *discordgo.Session, channelID string) (config.ChannelPolicy, bool) {
	effID, effName, isThread := queue.ResolveEffectiveChannel(s, channelID)
	return config.ResolveChannelPolicy(currentFunnelConfig().Current().Channels, effID, effName), isThread
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

func connectDiscordFunnel(ctx context.Context, database *sql.DB, pool *queue.WorkerPool, token string) *discordgo.Session {
	if token == "" {
		log.Println("Discord funnel disabled: DISCORD_BOT_TOKEN/DISCORD_TOKEN not configured")
		return nil
	}

	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
	}

	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		log.Printf("Discord funnel failed to create session: %v", err)
		return nil
	}
	if pool != nil {
		pool.SetDiscordSession(dg)
		SetFunnelPool(pool)
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
		go RunStartupCatchUpSweep(ctx, database, pool, s)
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
			resolvedGuildID := resolveGuildID(s, m.Message)
			m.GuildID = resolvedGuildID
			if m.Message != nil {
				m.Message.GuildID = resolvedGuildID
			}
			prompt := buildDiscordPrompt(s, m.Message, targetThreadID, policy)

			authorID := ""
			authorName := "Discord User"
			if m.Author != nil {
				authorID = m.Author.ID
				authorName = m.Author.Username
			}

			msg := db.Message{
				ID:         m.ID,
				ThreadID:   targetThreadID,
				GuildID:    resolvedGuildID,
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

	if err := discordSessionOpener(dg); err != nil {
		log.Printf("Warning: Discord funnel failed to open initial session: %v. Retrying in background...", err)
		go func() {
			backoff := funnelRetryBackoff
			maxBackoff := 60 * time.Second
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if err := discordSessionOpener(dg); err != nil {
					log.Printf("Discord funnel retry failed: %v. Retrying in %v...", err, backoff)
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
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if dg != nil && dg.DataReady {
					metrics.RecordGatewayLatency(dg.HeartbeatLatency())
				}
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
	return IsMessageableChannel(chType)
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

			targetThreadID, isThread := getOrCreateThreadID(s, m, false)
			sweepPolicy, _ := resolveEffectiveChannelPolicy(s, m.ChannelID)
			resolvedGuildID := resolveGuildID(s, m)
			m.GuildID = resolvedGuildID
			prompt := buildDiscordPrompt(s, m, targetThreadID, sweepPolicy)

			authorID := m.Author.ID
			authorName := m.Author.Username

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
