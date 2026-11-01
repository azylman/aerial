package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/interactions"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

var (
	funnelCfg               atomic.Pointer[config.Config]
	funnelPool              atomic.Pointer[queue.WorkerPool]
	activeInteractionRouter atomic.Pointer[interactions.Router]
	titleSummarizeSem       = make(chan struct{}, 2)
	discordSessionOpener    = func(dg *discordgo.Session) error { return hydrateDiscordSessionREST(dg) }
	funnelRetryBackoff      = 2 * time.Second
	funnelMemberFetcher     = func(s *discordgo.Session, guildID, userID string) (*discordgo.Member, error) {
		if s == nil {
			return nil, errors.New("nil session")
		}
		return s.GuildMember(guildID, userID)
	}
)

type DiscordMessageIngressRequest struct {
	BotUserID string             `json:"bot_user_id,omitempty"`
	RoleNames map[string]string  `json:"role_names,omitempty"`
	Message   *discordgo.Message `json:"message"`
}

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

func fetchAndCacheDiscordChannel(s *discordgo.Session, channelID string) *discordgo.Channel {
	if s == nil || s.Token == "" || !queue.IsNumericSnowflake(channelID) {
		return nil
	}
	ch, err := s.Channel(channelID)
	if err != nil || ch == nil {
		queue.InvalidateChannelCache(channelID)
		return nil
	}
	queue.CacheDiscordChannel(ch)
	if s.State != nil {
		if err := s.State.ChannelAdd(ch); err != nil {
			log.Printf("[WARN] Failed to add channel %s to session state: %v", ch.ID, err)
		}
	}
	return ch
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
	return fetchAndCacheDiscordChannel(s, channelID)
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

func checkCachedThreadForMessage(s *discordgo.Session, msgID string) (string, bool) {
	if snap, ok := queue.GetCachedChannel(msgID); ok && snap.IsThread {
		return msgID, true
	}
	if s != nil && s.State != nil {
		if ch, err := s.State.Channel(msgID); err == nil && ch != nil && ch.IsThread() {
			queue.CacheDiscordChannel(ch)
			return msgID, true
		}
	}
	return "", false
}

func resolveNewThreadTitle(content, msgID string, shouldSummarize bool) string {
	title := ""
	if shouldSummarize {
		if cls := currentFunnelClassifier(); cls != nil {
			select {
			case titleSummarizeSem <- struct{}{}:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				var sumErr error
				title, sumErr = cls.SummarizeThreadTitle(ctx, content)
				cancel()
				<-titleSummarizeSem
				if sumErr != nil {
					log.Printf("Thread title summarization skipped/failed for message %s: %v", msgID, sumErr)
					title = ""
				}
			default:
				log.Printf("Thread title summarization semaphore full, falling back to deriveThreadTitle for message %s", msgID)
			}
		}
	}
	if title == "" {
		title = DeriveThreadTitle(content)
	}
	return title
}

func cacheAndRegisterThread(s *discordgo.Session, th *discordgo.Channel, parentID, guildID string) {
	if th.ParentID == "" {
		th.ParentID = parentID
	}
	if th.GuildID == "" {
		th.GuildID = guildID
	}
	queue.CacheDiscordChannel(th)
	if s != nil && s.State != nil {
		if err := s.State.ChannelAdd(th); err != nil {
			log.Printf("[WARN] Failed to add thread %s to session state: %v", th.ID, err)
		}
	}
}

func recoverExistingThreadOnConflict(s *discordgo.Session, msgID, channelID, guildID string) (string, bool) {
	metrics.RecordThreadCreated("already_exists")
	log.Printf("Thread already exists for message %s in channel %s (code 160004); resolving thread ID %s", msgID, channelID, msgID)
	existingThread, fetchErr := s.Channel(msgID)
	if fetchErr != nil || existingThread == nil {
		existingThread = &discordgo.Channel{
			ID:       msgID,
			GuildID:  guildID,
			ParentID: channelID,
			Type:     discordgo.ChannelTypeGuildPublicThread,
		}
	}
	cacheAndRegisterThread(s, existingThread, channelID, guildID)
	return msgID, true
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

	if thID, ok := checkCachedThreadForMessage(s, m.ID); ok {
		return thID, true
	}

	shouldSummarize := len(allowSummarize) == 0 || allowSummarize[0]
	title := resolveNewThreadTitle(m.Content, m.ID, shouldSummarize)

	if s == nil || s.Token == "" {
		return m.ChannelID, false
	}

	thread, err := s.MessageThreadStart(m.ChannelID, m.ID, title, 1440)
	if err != nil {
		if IsThreadAlreadyExistsError(err) && queue.IsNumericSnowflake(m.ID) {
			return recoverExistingThreadOnConflict(s, m.ID, m.ChannelID, guildID)
		}
		metrics.RecordThreadCreated("error")
		log.Printf("Failed to create Discord thread for message %s (channel %s): %v", m.ID, m.ChannelID, err)
		return m.ChannelID, false
	}
	metrics.RecordThreadCreated("created")
	cacheAndRegisterThread(s, thread, m.ChannelID, guildID)
	log.Printf("Created new Discord thread %q (ID: %s) for message %s in channel %s", title, thread.ID, m.ID, m.ChannelID)
	return thread.ID, true
}

func replaceMentionRoles(content string, s *discordgo.Session, guildID string, mentionRoles []string, extras map[string]string) string {
	for _, roleID := range mentionRoles {
		roleName := ""
		if s != nil && s.State != nil && guildID != "" {
			if r, err := s.State.Role(guildID, roleID); err == nil && r != nil && r.Name != "" {
				roleName = r.Name
			}
		}
		if roleName == "" && extras != nil {
			if name, ok := extras[roleID]; ok && name != "" {
				roleName = name
			}
		}
		if roleName != "" {
			content = strings.ReplaceAll(content, "<@&"+roleID+">", "@"+roleName)
		}
	}
	return content
}

// resolveMessageContent resolves Discord mention snowflakes (<@ID>, <@!ID>, <@&ID>, <#ID>)
// to human-readable names using cached session state and incoming payload mentions.
func resolveMessageContent(s *discordgo.Session, m *discordgo.Message, extraRoleNames ...map[string]string) string {
	if m == nil {
		return ""
	}
	var extras map[string]string
	if len(extraRoleNames) > 0 {
		extras = extraRoleNames[0]
	}
	content := m.Content
	if s != nil {
		if resolved, err := m.ContentWithMoreMentionsReplaced(s); err == nil && resolved != "" {
			content = resolved
		} else if fallback := m.ContentWithMentionsReplaced(); fallback != "" {
			content = fallback
		}
	} else if resolved := m.ContentWithMentionsReplaced(); resolved != "" {
		content = resolved
	}
	if len(m.MentionRoles) > 0 {
		content = replaceMentionRoles(content, s, m.GuildID, m.MentionRoles, extras)
	}
	return content
}

func resolveMentionRoleNames(s *discordgo.Session, m *discordgo.Message, extraRoleNames ...map[string]string) map[string]string {
	roleNames := make(map[string]string)
	if m == nil {
		return roleNames
	}
	var extras map[string]string
	if len(extraRoleNames) > 0 {
		extras = extraRoleNames[0]
	}
	for _, roleID := range m.MentionRoles {
		if s != nil && s.State != nil && m.GuildID != "" {
			if r, err := s.State.Role(m.GuildID, roleID); err == nil && r != nil && r.Name != "" {
				roleNames[roleID] = r.Name
				continue
			}
		}
		if extras != nil {
			if name, ok := extras[roleID]; ok && name != "" {
				roleNames[roleID] = name
			}
		}
	}
	return roleNames
}

func resolveReplyContext(s *discordgo.Session, m *discordgo.Message, extraRoleNames ...map[string]string) (replyingToAuthor, replyingToContent string) {
	if m == nil || m.ReferencedMessage == nil {
		return "", ""
	}
	if m.ReferencedMessage.GuildID == "" && m.GuildID != "" {
		m.ReferencedMessage.GuildID = m.GuildID
	}
	replyingToAuthor = "Unknown"
	if m.ReferencedMessage.Author != nil {
		replyingToAuthor = "@" + strings.ReplaceAll(strings.ReplaceAll(m.ReferencedMessage.Author.Username, "\n", " "), "\r", "")
	}
	replyingToContent = resolveMessageContent(s, m.ReferencedMessage, extraRoleNames...)
	if len(m.ReferencedMessage.Attachments) > 0 {
		var refAtts []string
		for _, a := range m.ReferencedMessage.Attachments {
			if a != nil && a.URL != "" {
				refAtts = append(refAtts, a.URL)
			}
		}
		if len(refAtts) > 0 {
			if replyingToContent != "" {
				replyingToContent += " "
			}
			replyingToContent += fmt.Sprintf("[attachments: %s]", strings.Join(refAtts, ", "))
		}
	}
	return replyingToAuthor, replyingToContent
}

func resolveMentionsAndRoles(m *discordgo.Message, roleNames map[string]string) (mentions, userIDs, roleIDs []string) {
	cleanStr := func(str string) string { return strings.ReplaceAll(strings.ReplaceAll(str, "\n", " "), "\r", "") }
	for _, u := range m.Mentions {
		if u != nil {
			mentions = append(mentions, cleanStr(u.Username))
			if u.ID != "" {
				userIDs = append(userIDs, u.ID)
			}
		}
	}
	for _, roleID := range m.MentionRoles {
		name := roleID
		if rName, ok := roleNames[roleID]; ok && rName != "" {
			name = rName
		}
		mentions = append(mentions, cleanStr(name))
		if roleID != "" {
			roleIDs = append(roleIDs, roleID)
		}
	}
	return mentions, userIDs, roleIDs
}

func resolveAttachments(m *discordgo.Message) []string {
	var attachments []string
	for _, a := range m.Attachments {
		if a != nil && a.URL != "" {
			attachments = append(attachments, a.URL)
		}
	}
	return attachments
}

// populateDiscordMessageFields populates structured Discord fields onto a db.Message.
func populateDiscordMessageFields(s *discordgo.Session, m *discordgo.Message, msg *db.Message, extraRoleNames ...map[string]string) {
	if m == nil || msg == nil {
		return
	}
	if m.ReferencedMessage != nil && m.ReferencedMessage.GuildID == "" && m.GuildID != "" {
		m.ReferencedMessage.GuildID = m.GuildID
	}
	roleNames := resolveMentionRoleNames(s, m, extraRoleNames...)
	var adminUsers []string
	if cfg := currentFunnelConfig(); cfg != nil {
		adminUsers = cfg.Current().AdminUsers
	}
	if m.Author != nil {
		msg.IsAdmin = config.IsAdmin(adminUsers, m.Author.ID, m.Author.Username, m.Author.GlobalName)
		msg.AuthorGlobalName, msg.AuthorBot = m.Author.GlobalName, m.Author.Bot
	}

	msg.ReplyingToAuthor, msg.ReplyingToContent = resolveReplyContext(s, m, extraRoleNames...)
	msg.Mentions, msg.MentionUserIDs, msg.MentionRoleIDs = resolveMentionsAndRoles(m, roleNames)
	msg.Attachments = resolveAttachments(m)

	_, effName, _, threadName := queue.ResolveChannelAndThread(s, m.ChannelID)
	msg.ChannelID = m.ChannelID
	msg.ChannelName = effName
	msg.ThreadName = threadName
}

// resolveEffectiveChannelPolicy resolves the ChannelPolicy for a channel or thread.
func resolveEffectiveChannelPolicy(s *discordgo.Session, channelID string) (config.ChannelPolicy, bool) {
	effID, effName, isThread := queue.ResolveEffectiveChannel(s, channelID)
	return config.ResolveChannelPolicy(currentFunnelConfig().Current().Channels, effID, effName), isThread
}

func hasDirectFunnelTrigger(s *discordgo.Session, m *discordgo.MessageCreate, policy config.ChannelPolicy, isThread bool) bool {
	return resolveGuildID(s, m.Message) == "" || !policy.IsThread() || len(m.Mentions) > 0 || m.ReferencedMessage != nil || m.MessageReference != nil || len(m.MentionRoles) > 0 || isThread
}

func containsFunnelKeyword(content string) bool {
	contentLower := strings.ToLower(content)
	return strings.Contains(contentLower, "aerial") || strings.Contains(contentLower, "gundam") ||
		strings.Contains(contentLower, "brain") || strings.Contains(contentLower, "bot")
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
	if hasDirectFunnelTrigger(s, m, policy, isThread) {
		return true
	}
	return containsFunnelKeyword(m.Content)
}

func buildAndInsertFunnelMessage(ctx context.Context, s *discordgo.Session, store db.Store, m *discordgo.Message, targetThreadID string, createdAt time.Time, extraRoleNames ...map[string]string) (db.Message, error) {
	resolvedGuildID := resolveGuildID(s, m)
	if m != nil {
		m.GuildID = resolvedGuildID
		if m.ReferencedMessage != nil && m.ReferencedMessage.GuildID == "" {
			m.ReferencedMessage.GuildID = resolvedGuildID
		}
	}
	authorID, authorName, rawContent, msgID, effort := "", "Discord User", "", "", ""
	if m != nil {
		msgID = m.ID
		if m.Author != nil {
			authorID = m.Author.ID
			authorName = m.Author.Username
		}
		rawContent = resolveMessageContent(s, m, extraRoleNames...)
		policy, _ := resolveEffectiveChannelPolicy(s, m.ChannelID)
		effort = policy.GetEffort()
	}
	msg := db.Message{
		ID:         msgID,
		ThreadID:   targetThreadID,
		GuildID:    resolvedGuildID,
		AuthorID:   authorID,
		AuthorName: authorName,
		Content:    rawContent,
		Effort:     effort,
		Status:     db.StatusPending,
		CreatedAt:  createdAt,
		UpdatedAt:  time.Now().UTC(),
	}
	populateDiscordMessageFields(s, m, &msg, extraRoleNames...)
	if store != nil {
		if err := store.InsertMessage(ctx, msg); err != nil {
			return msg, err
		}
	}
	return msg, nil
}

func seedSessionStateForIngress(s *discordgo.Session, botUserID string, m *discordgo.Message) {
	if botUserID == "" || s == nil || s.State == nil {
		return
	}
	s.State.Lock()
	if s.State.User == nil {
		s.State.User = &discordgo.User{ID: botUserID, Username: "Aerial"}
	}
	s.State.Unlock()
	if m != nil && m.GuildID != "" {
		if _, err := s.State.Guild(m.GuildID); err != nil {
			if addErr := s.State.GuildAdd(&discordgo.Guild{ID: m.GuildID}); addErr != nil {
				log.Printf("[Internal Ingress] Warning: failed to add guild %s to state: %v", m.GuildID, addErr)
			}
		}
	}
}

func isSelfOrNilMessage(s *discordgo.Session, m *discordgo.Message, botUserID string) bool {
	if m == nil || m.Author == nil || (botUserID != "" && m.Author.ID == botUserID) {
		return true
	}
	if s != nil && s.State != nil {
		s.State.RLock()
		u := s.State.User
		s.State.RUnlock()
		if u != nil && m.Author.ID == u.ID {
			return true
		}
	}
	return false
}

func processFunnelMessageIngress(ctx context.Context, s *discordgo.Session, store db.Store, pool *queue.WorkerPool, m *discordgo.Message, botUserID string, extraRoleNames map[string]string) (string, error) {
	seedSessionStateForIngress(s, botUserID, m)

	if isSelfOrNilMessage(s, m, botUserID) {
		return "ignored_self", nil
	}

	if !isFunnelBotTargeted(s, &discordgo.MessageCreate{Message: m}) {
		metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "ignored").Inc()
		return "ignored", nil
	}

	if store != nil {
		exists, err := store.MessageExists(ctx, m.ID)
		if err != nil {
			return "", err
		}
		if exists {
			return "duplicate", nil
		}
	}

	targetThreadID, _ := getOrCreateThreadID(s, m)
	msg, err := buildAndInsertFunnelMessage(ctx, s, store, m, targetThreadID, m.Timestamp, extraRoleNames)
	if err != nil {
		return "", err
	}

	metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "enqueued").Inc()
	if pool != nil {
		pool.Enqueue(msg)
	}
	return "enqueued", nil
}

func hydrateGuildRoles(dg *discordgo.Session, guildID string) {
	if dg == nil {
		return
	}
	roles, err := dg.GuildRoles(guildID)
	if err != nil {
		return
	}
	for _, role := range roles {
		if role != nil {
			if err := dg.State.RoleAdd(guildID, role); err != nil {
				log.Printf("[WARN] Failed to add role %s to session state: %v", role.ID, err)
			}
		}
	}
}

func hydrateGuildChannels(dg *discordgo.Session, guildID string) {
	if dg == nil {
		return
	}
	channels, err := dg.GuildChannels(guildID)
	if err != nil {
		return
	}
	for _, ch := range channels {
		if ch == nil {
			continue
		}
		ch.GuildID = guildID
		queue.CacheDiscordChannel(ch)
		if err := dg.State.ChannelAdd(ch); err != nil {
			log.Printf("[WARN] Failed to add channel %s to session state: %v", ch.ID, err)
		}
	}
}

func hydrateGuildThreads(dg *discordgo.Session, guildID string) {
	if dg == nil {
		return
	}
	activeThreads, err := dg.GuildThreadsActive(guildID)
	if err != nil || activeThreads == nil {
		return
	}
	for _, th := range activeThreads.Threads {
		if th == nil {
			continue
		}
		th.GuildID = guildID
		queue.CacheDiscordChannel(th)
		if err := dg.State.ChannelAdd(th); err != nil {
			log.Printf("[WARN] Failed to add thread %s to session state: %v", th.ID, err)
		}
	}
}

func hydrateGuildState(dg *discordgo.Session, ug *discordgo.UserGuild, botUserID string) {
	if ug == nil || ug.ID == "" {
		return
	}
	if err := dg.State.GuildAdd(&discordgo.Guild{ID: ug.ID, Name: ug.Name}); err != nil {
		log.Printf("[WARN] Failed to add guild %s to session state: %v", ug.ID, err)
	}

	hydrateGuildRoles(dg, ug.ID)
	hydrateGuildChannels(dg, ug.ID)
	hydrateGuildThreads(dg, ug.ID)

	syncBotMemberRoles(context.Background(), dg, ug.ID, botUserID)
	go func(appID, guildID string) {
		if _, err := interactions.RegisterGuildCommandsOnce(dg, appID, guildID); err != nil {
			log.Printf("[Interactions] Failed to register guild commands for guild %s: %v", guildID, err)
		}
	}(botUserID, ug.ID)
}

func hydrateDiscordSessionREST(dg *discordgo.Session) error {
	if dg == nil || dg.Token == "" {
		return nil
	}
	u, err := dg.User("@me")
	if err != nil {
		return err
	}
	if dg.State == nil {
		dg.State = discordgo.NewState()
	}
	dg.State.Lock()
	dg.State.User = u
	dg.State.Unlock()

	userGuilds, err := dg.UserGuilds(100, "", "", false)
	if err != nil {
		return err
	}

	for _, ug := range userGuilds {
		hydrateGuildState(dg, ug, u.ID)
	}
	return nil
}

func openDiscordSessionWithRetry(ctx context.Context, dg *discordgo.Session) {
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
					backoff = min(backoff*2, maxBackoff)
					continue
				}
				log.Printf("Discord funnel worker started successfully inside Brain")
				break
			}
		}()
	} else {
		log.Printf("Discord funnel worker connected successfully inside Brain")
	}
}

func connectDiscordFunnel(ctx context.Context, store db.Store, pool *queue.WorkerPool, token string) *discordgo.Session {
	if token == "" {
		log.Println("Discord funnel disabled: DISCORD_BOT_TOKEN/DISCORD_TOKEN not configured")
		return nil
	}

	if ctx == nil {
		cCtx, cancel := context.WithCancel(context.Background())
		cancel()
		ctx = cCtx
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
		Store: store, Pool: pool, PoolProvider: funnelPool.Load,
		Config: currentFunnelConfig(), ConfigProvider: currentFunnelConfig, HaroURL: os.Getenv("HARO_URL"),
	})
	activeInteractionRouter.Store(interactionRouter)

	openDiscordSessionWithRetry(ctx, dg)
	return dg
}
