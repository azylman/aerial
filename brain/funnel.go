package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/interactions"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

var (
	funnelCfg               atomic.Pointer[config.Config]
	funnelPool              atomic.Pointer[queue.WorkerPool]
	activeInteractionRouter atomic.Pointer[interactions.Router]
	titleSummarizeSem       = make(chan struct{}, 2)
	discordSessionOpener    = func(dg *discordgo.Session) error { return dg.Open() }
	funnelRetryBackoff      = 2 * time.Second
	funnelMemberFetcher     = func(s *discordgo.Session, guildID, userID string) (*discordgo.Member, error) {
		if s == nil {
			return nil, errors.New("nil session")
		}
		return s.GuildMember(guildID, userID)
	}
)

func syncBotMemberRoles(ctx context.Context, s *discordgo.Session, guildID, botID string) {
	if s == nil || s.State == nil || guildID == "" || botID == "" || (ctx != nil && ctx.Err() != nil) {
		return
	}
	if m, err := s.State.Member(guildID, botID); err == nil && m != nil {
		return
	}
	m, err := funnelMemberFetcher(s, guildID, botID)
	if err != nil || m == nil {
		log.Printf("Warning: failed to fetch bot member roles for guild %s: %v", guildID, err)
		return
	}
	if m.GuildID == "" {
		m.GuildID = guildID
	}
	if m.User != nil {
		if err := s.State.MemberAdd(m); err != nil {
			log.Printf("[WARN] Failed to add bot member %s to session state: %v", botID, err)
		}
	}
	log.Printf("Discord funnel cached %d bot role(s) for guild %s", len(m.Roles), guildID)
}

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
		return &discordgo.Channel{ID: snap.ID, Name: snap.Name, GuildID: snap.GuildID, ParentID: snap.ParentID, Type: chType}
	}
	if s != nil && s.Token != "" && queue.IsNumericSnowflake(channelID) {
		if ch, err := s.Channel(channelID); err == nil && ch != nil {
			queue.CacheDiscordChannel(ch)
			if s.State != nil {
				if err := s.State.ChannelAdd(ch); err != nil {
					log.Printf("[WARN] Failed to add channel %s to session state: %v", ch.ID, err)
				}
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
		isAlreadyThread = ch.IsThread()
	}
	if isAlreadyThread {
		return m.ChannelID, true
	}

	policy := config.ResolveChannelPolicy(currentFunnelConfig().Current().Channels, m.ChannelID, channelName)
	if !policy.IsThread() {
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

	shouldSummarize := len(allowSummarize) == 0 || allowSummarize[0]
	title := ""
	if shouldSummarize {
		if cls := currentFunnelClassifier(); cls != nil {
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
		title = DeriveThreadTitle(m.Content)
	}

	if s == nil || s.Token == "" {
		return m.ChannelID, false
	}

	cacheAndRegisterThread := func(th *discordgo.Channel) {
		if th.ParentID == "" {
			th.ParentID = m.ChannelID
		}
		if th.GuildID == "" {
			th.GuildID = guildID
		}
		queue.CacheDiscordChannel(th)
		if s.State != nil {
			if err := s.State.ChannelAdd(th); err != nil {
				log.Printf("[WARN] Failed to add thread %s to session state: %v", th.ID, err)
			}
		}
	}

	thread, err := s.MessageThreadStart(m.ChannelID, m.ID, title, 1440)
	if err != nil {
		if IsThreadAlreadyExistsError(err) && queue.IsNumericSnowflake(m.ID) {
			metrics.RecordThreadCreated("already_exists")
			log.Printf("Thread already exists for message %s in channel %s (code 160004); resolving thread ID %s", m.ID, m.ChannelID, m.ID)
			existingThread, fetchErr := s.Channel(m.ID)
			if fetchErr != nil || existingThread == nil {
				existingThread = &discordgo.Channel{
					ID:       m.ID,
					GuildID:  guildID,
					ParentID: m.ChannelID,
					Type:     discordgo.ChannelTypeGuildPublicThread,
				}
			}
			cacheAndRegisterThread(existingThread)
			return m.ID, true
		}
		metrics.RecordThreadCreated("error")
		log.Printf("Failed to create Discord thread for message %s (channel %s): %v", m.ID, m.ChannelID, err)
		return m.ChannelID, false
	}
	metrics.RecordThreadCreated("created")
	cacheAndRegisterThread(thread)
	log.Printf("Created new Discord thread %q (ID: %s) for message %s in channel %s", title, thread.ID, m.ID, m.ChannelID)
	return thread.ID, true
}

// resolveMessageContent resolves Discord mention snowflakes (<@ID>, <@!ID>, <@&ID>, <#ID>)
// to human-readable names using cached session state and incoming payload mentions.
func resolveMessageContent(s *discordgo.Session, m *discordgo.Message) string {
	if m == nil {
		return ""
	}
	content := m.Content
	if s != nil {
		if resolved, err := m.ContentWithMoreMentionsReplaced(s); err == nil && resolved != "" {
			content = resolved
		} else if fallback := m.ContentWithMentionsReplaced(); fallback != "" {
			content = fallback
		}
		if s.State != nil && m.GuildID != "" && len(m.MentionRoles) > 0 {
			for _, roleID := range m.MentionRoles {
				if r, err := s.State.Role(m.GuildID, roleID); err == nil && r != nil && r.Name != "" {
					content = strings.ReplaceAll(content, "<@&"+roleID+">", "@"+r.Name)
				}
			}
		}
		return content
	}
	if resolved := m.ContentWithMentionsReplaced(); resolved != "" {
		return resolved
	}
	return m.Content
}

// extractDiscordMetadata extracts structured metadata from a discordgo.Message.
func extractDiscordMetadata(s *discordgo.Session, m *discordgo.Message, targetThreadID string) db.MessageMetadata {
	if m == nil {
		return db.MessageMetadata{}
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
	if cfg := currentFunnelConfig(); cfg != nil {
		adminUsers = cfg.Current().AdminUsers
	}
	isAdmin, username, globalName, authorBot := false, "", "", false
	if m.Author != nil {
		isAdmin = config.IsAdmin(adminUsers, m.Author.ID, m.Author.Username, m.Author.GlobalName)
		username, globalName, authorBot = m.Author.Username, m.Author.GlobalName, m.Author.Bot
	}

	var replyingToAuthor, replyingToContent string
	if m.ReferencedMessage != nil {
		replyingToAuthor = "Unknown"
		if m.ReferencedMessage.Author != nil {
			replyingToAuthor = "@" + strings.ReplaceAll(strings.ReplaceAll(m.ReferencedMessage.Author.Username, "\n", " "), "\r", "")
		}
		replyingToContent = resolveMessageContent(s, m.ReferencedMessage)
	}

	cleanStr := func(str string) string { return strings.ReplaceAll(strings.ReplaceAll(str, "\n", " "), "\r", "") }
	var mentions, mentionUserIDs []string
	for _, u := range m.Mentions {
		if u != nil {
			mentions = append(mentions, cleanStr(u.Username))
			if u.ID != "" {
				mentionUserIDs = append(mentionUserIDs, u.ID)
			}
		}
	}
	var mentionRoleIDs []string
	for _, roleID := range m.MentionRoles {
		name := roleID
		if rName, ok := roleNames[roleID]; ok && rName != "" {
			name = rName
		}
		mentions = append(mentions, cleanStr(name))
		if roleID != "" {
			mentionRoleIDs = append(mentionRoleIDs, roleID)
		}
	}
	var attachments []string
	for _, a := range m.Attachments {
		if a != nil && a.URL != "" {
			attachments = append(attachments, a.URL)
		}
	}
	_, effName, _, threadName := queue.ResolveChannelAndThread(s, m.ChannelID)
	return db.MessageMetadata{
		ChannelID:         m.ChannelID,
		ChannelName:       effName,
		TargetThreadID:    targetThreadID,
		ThreadName:        threadName,
		GuildID:           m.GuildID,
		AuthorUsername:    username,
		AuthorGlobalName:  globalName,
		AuthorBot:         authorBot,
		IsAdmin:           isAdmin,
		Mentions:          mentions,
		MentionUserIDs:    mentionUserIDs,
		MentionRoleIDs:    mentionRoleIDs,
		ReplyingToAuthor:  replyingToAuthor,
		ReplyingToContent: replyingToContent,
		Attachments:       attachments,
	}
}

// resolveEffectiveChannelPolicy resolves the ChannelPolicy for a channel or thread.
func resolveEffectiveChannelPolicy(s *discordgo.Session, channelID string) (config.ChannelPolicy, bool) {
	effID, effName, isThread := queue.ResolveEffectiveChannel(s, channelID)
	return config.ResolveChannelPolicy(currentFunnelConfig().Current().Channels, effID, effName), isThread
}

// isFunnelBotTargeted returns true if the message should trigger the funnel worker pool.
func isFunnelBotTargeted(s *discordgo.Session, m *discordgo.MessageCreate) bool {
	if m == nil || m.Message == nil || m.Author == nil {
		return false
	}
	if s != nil && s.State != nil && s.State.User != nil && m.Author.ID == s.State.User.ID {
		return false
	}
	policy, isThread := resolveEffectiveChannelPolicy(s, m.ChannelID)
	if policy.IsIgnored() || (policy.IsBotIgnored() && m.Author.Bot) {
		return false
	}
	if resolveGuildID(s, m.Message) == "" || !policy.IsThread() || len(m.Mentions) > 0 || m.ReferencedMessage != nil || m.MessageReference != nil || len(m.MentionRoles) > 0 || isThread {
		return true
	}
	contentLower := strings.ToLower(m.Content)
	return strings.Contains(contentLower, "aerial") || strings.Contains(contentLower, "gundam") ||
		strings.Contains(contentLower, "brain") || strings.Contains(contentLower, "bot")
}

func handleDiscordReady(ctx context.Context, s *discordgo.Session, r *discordgo.Ready, store db.Store, pool *queue.WorkerPool) {
	if r == nil {
		return
	}
	metrics.DiscordEventsTotal.WithLabelValues("ready").Inc()
	userStr := ""
	if r.User != nil {
		userStr = fmt.Sprintf("%s#%s (user ID %s)", r.User.Username, r.User.Discriminator, r.User.ID)
	}
	log.Printf("Discord funnel gateway session ready as %s", userStr)
	if s != nil && s.State != nil {
		botUserID := ""
		if r.User != nil {
			botUserID = r.User.ID
		}
		for _, g := range s.State.Guilds {
			cacheGuildChannelsAndRegisterCommands(s, g, botUserID)
		}
	}
	if ctx != nil && store != nil && pool != nil && s != nil {
		go RunStartupCatchUpSweep(ctx, store, pool, s)
	}
}

func cacheGuildChannelsAndRegisterCommands(s *discordgo.Session, g *discordgo.Guild, botUserID string) {
	if g == nil {
		return
	}
	for _, ch := range g.Channels {
		queue.CacheDiscordChannel(ch)
	}
	for _, th := range g.Threads {
		queue.CacheDiscordChannel(th)
	}
	if botUserID != "" && g.ID != "" && s != nil {
		go func(guildID, appID string) {
			if _, err := interactions.RegisterGuildCommandsOnce(s, appID, guildID); err != nil {
				log.Printf("[Interactions] Failed to register guild commands for guild %s: %v", guildID, err)
			}
		}(g.ID, botUserID)
	}
}

func handleGuildMemberUpdate(s *discordgo.Session, m *discordgo.GuildMemberUpdate) {
	metrics.DiscordEventsTotal.WithLabelValues("guild_member_update").Inc()
	if m == nil || m.Member == nil || s == nil || s.State == nil || s.State.User == nil {
		return
	}
	userID := ""
	if m.User != nil {
		userID = m.User.ID
	}
	if userID != "" && userID == s.State.User.ID {
		if err := s.State.MemberAdd(m.Member); err != nil {
			log.Printf("[WARN] Failed to update member %s in session state: %v", userID, err)
		}
		log.Printf("Discord funnel updated bot roles (%d roles) in real-time for guild %s", len(m.Member.Roles), m.Member.GuildID)
	}
}

func buildAndInsertFunnelMessage(ctx context.Context, s *discordgo.Session, store db.Store, m *discordgo.Message, targetThreadID string, createdAt time.Time) (db.Message, error) {
	resolvedGuildID := resolveGuildID(s, m)
	if m != nil {
		m.GuildID = resolvedGuildID
	}
	metadata := extractDiscordMetadata(s, m, targetThreadID)
	authorID, authorName, rawContent, msgID := "", "Discord User", "", ""
	if m != nil {
		msgID = m.ID
		if m.Author != nil {
			authorID = m.Author.ID
			authorName = m.Author.Username
		}
		rawContent = resolveMessageContent(s, m)
	}
	msg := db.Message{
		ID:         msgID,
		ThreadID:   targetThreadID,
		GuildID:    resolvedGuildID,
		AuthorID:   authorID,
		AuthorName: authorName,
		Content:    rawContent,
		Metadata:   metadata,
		Status:     db.StatusPending,
		CreatedAt:  createdAt,
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.InsertMessage(ctx, msg); err != nil {
		return msg, err
	}
	return msg, nil
}

func connectDiscordFunnel(ctx context.Context, store db.Store, pool *queue.WorkerPool, token string) *discordgo.Session {
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

	interactionRouter := interactions.NewRouter(interactions.InteractionDeps{
		Store:          store,
		Pool:           pool,
		PoolProvider:   funnelPool.Load,
		Config:         currentFunnelConfig(),
		ConfigProvider: currentFunnelConfig,
	})
	activeInteractionRouter.Store(interactionRouter)

	dg.AddHandler(func(s *discordgo.Session, i *discordgo.InteractionCreate) {
		if r := activeInteractionRouter.Load(); r != nil {
			r.Handle(s, i)
		}
	})

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		handleDiscordReady(ctx, s, r, store, pool)
	})

	dg.AddHandler(func(s *discordgo.Session, g *discordgo.GuildCreate) {
		metrics.DiscordEventsTotal.WithLabelValues("guild_create").Inc()
		if g != nil && g.Guild != nil {
			botUserID := ""
			if s != nil && s.State != nil && s.State.User != nil {
				botUserID = s.State.User.ID
			}
			cacheGuildChannelsAndRegisterCommands(s, g.Guild, botUserID)
			if s != nil && s.State != nil && s.State.User != nil && botUserID != "" && g.ID != "" {
				go syncBotMemberRoles(ctx, s, g.ID, botUserID)
			}
		}
	})

	handleCacheEvent := func(evt string, ch *discordgo.Channel, del bool) {
		metrics.DiscordEventsTotal.WithLabelValues(evt).Inc()
		if ch != nil {
			if del {
				queue.InvalidateChannelCache(ch.ID)
			} else {
				queue.CacheDiscordChannel(ch)
			}
		}
	}
	dg.AddHandler(func(_ *discordgo.Session, c *discordgo.ChannelCreate) {
		if c != nil {
			handleCacheEvent("channel_create", c.Channel, false)
		}
	})
	dg.AddHandler(func(_ *discordgo.Session, c *discordgo.ChannelUpdate) {
		if c != nil {
			handleCacheEvent("channel_update", c.Channel, false)
		}
	})
	dg.AddHandler(func(_ *discordgo.Session, c *discordgo.ChannelDelete) {
		if c != nil {
			handleCacheEvent("channel_delete", c.Channel, true)
		}
	})
	dg.AddHandler(func(_ *discordgo.Session, t *discordgo.ThreadCreate) {
		if t != nil {
			handleCacheEvent("thread_create", t.Channel, false)
		}
	})
	dg.AddHandler(func(_ *discordgo.Session, t *discordgo.ThreadUpdate) {
		if t != nil {
			handleCacheEvent("thread_update", t.Channel, false)
		}
	})
	dg.AddHandler(func(_ *discordgo.Session, t *discordgo.ThreadDelete) {
		if t != nil {
			handleCacheEvent("thread_delete", t.Channel, true)
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

	dg.AddHandler(func(s *discordgo.Session, m *discordgo.GuildMemberUpdate) {
		handleGuildMemberUpdate(s, m)
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
			insertCtx, insertCancel := context.WithTimeout(context.Background(), 5*time.Second)
			msg, err := buildAndInsertFunnelMessage(insertCtx, s, store, m.Message, targetThreadID, m.Timestamp)
			insertCancel()
			if err != nil {
				log.Printf("Failed to insert message %s: %v", m.ID, err)
				return
			}

			metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "enqueued").Inc()
			log.Printf("Discord funnel enqueued message %s from %s (thread: %s, is_thread: %t)", m.ID, msg.AuthorName, targetThreadID, isThread)
			if pool != nil {
				pool.Enqueue(msg)
			}
		}()
	})

	dg.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
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

// RunStartupCatchUpSweep safely sweeps active channels and threads for missed messages during downtime.
func RunStartupCatchUpSweep(ctx context.Context, store db.Store, pool *queue.WorkerPool, s *discordgo.Session) {
	if s == nil || store == nil || pool == nil {
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

	targetMap := make(map[string]bool)
	if recentThreadIDs, err := store.GetActiveRecentThreadIDs(sweepCtx, 48*time.Hour); err == nil {
		for _, thID := range recentThreadIDs {
			if thID != "" {
				targetMap[thID] = true
			}
		}
	}

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
		if activeThreads, err := s.GuildThreadsActive(g.ID); err == nil && activeThreads != nil {
			for _, th := range activeThreads.Threads {
				if th != nil && IsMessageableChannel(th.Type) {
					targetMap[th.ID] = true
				}
			}
		}
		if channels, err := s.GuildChannels(g.ID); err == nil {
			for _, ch := range channels {
				if ch != nil && IsMessageableChannel(ch.Type) {
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

		if botUserID != "" && s.State != nil {
			if perms, err := s.State.UserChannelPermissions(botUserID, chID); err == nil {
				hasView := (perms & discordgo.PermissionViewChannel) != 0
				hasHistory := (perms & discordgo.PermissionReadMessageHistory) != 0
				if !hasView || !hasHistory {
					continue
				}
			}
		}

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

		for i, j := 0, len(fetched)-1; i < j; i, j = i+1, j-1 {
			fetched[i], fetched[j] = fetched[j], fetched[i]
		}

		for _, m := range fetched {
			if m == nil || m.Author == nil || (m.Author.Bot && policy.IsBotIgnored()) || (botUserID != "" && m.Author.ID == botUserID) {
				continue
			}

			msgTime := m.Timestamp
			if msgTime.Before(lookbackCutoff) {
				skippedCount++
				continue
			}

			if !isFunnelBotTargeted(s, &discordgo.MessageCreate{Message: m}) {
				skippedCount++
				continue
			}

			exists, err := store.MessageExists(sweepCtx, m.ID)
			if err != nil {
				log.Printf("[Startup Recovery] Warning checking message existence for %s: %v", m.ID, err)
			}
			if exists {
				skippedCount++
				continue
			}

			targetThreadID, isThread := getOrCreateThreadID(s, m, false)
			stats, statsErr := store.GetSessionActivityStats(sweepCtx, targetThreadID)
			if statsErr == nil && stats != nil && !stats.LastCompletedMessageCreatedAt.IsZero() && stats.LastCompletedMessageCreatedAt.After(msgTime) {
				log.Printf("[CatchUpSweep] Skipping missed message %s in thread %s: newer completed message exists in thread (completed created_at %s > msg %s).",
					m.ID, targetThreadID, stats.LastCompletedMessageCreatedAt.Format(time.RFC3339), msgTime.Format(time.RFC3339))
				skippedCount++
				continue
			}

			msg, err := buildAndInsertFunnelMessage(sweepCtx, s, store, m, targetThreadID, msgTime)
			if err != nil {
				log.Printf("[CatchUpSweep] Failed to insert missed message %s: %v", m.ID, err)
				continue
			}

			log.Printf("[CatchUpSweep] Recovered missed message %s from %s (channel %s, target_thread: %s, is_thread: %t). Enqueued to worker pool.",
				m.ID, msg.AuthorName, m.ChannelID, targetThreadID, isThread)

			pool.Enqueue(msg)
			recoveredCount++
		}
	}

	log.Printf("[CatchUpSweep] Catch-up sweep completed: scanned %d channels, recovered %d messages, skipped %d.",
		len(targetChannels), recoveredCount, skippedCount)
}
