package queue

import (
	"fmt"
	"log"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/bwmarrin/discordgo"
	"golang.org/x/sync/singleflight"
)

// ChannelSnapshot holds immutable channel metadata to avoid cross-goroutine pointer races.
type ChannelSnapshot struct {
	ID       string
	GuildID  string
	Name     string
	ParentID string
	IsThread bool
}

var (
	channelCacheMu   sync.RWMutex
	channelCache     = make(map[string]ChannelSnapshot)
	restSingleFlight singleflight.Group
	restCooldownMu   sync.RWMutex
	restCooldown     = make(map[string]time.Time)
)

func isRESTInCooldown(key string) bool {
	restCooldownMu.RLock()
	expiry, exists := restCooldown[key]
	restCooldownMu.RUnlock()
	return exists && time.Now().Before(expiry)
}

func setRESTCooldown(key string, duration time.Duration) {
	restCooldownMu.Lock()
	defer restCooldownMu.Unlock()
	if len(restCooldown) > 128 {
		now := time.Now()
		for k, exp := range restCooldown {
			if now.After(exp) {
				delete(restCooldown, k)
			}
		}
	}
	restCooldown[key] = time.Now().Add(duration)
}

// CacheDiscordChannel stores an immutable snapshot of a discordgo.Channel.
func CacheDiscordChannel(ch *discordgo.Channel) {
	if ch == nil || ch.ID == "" {
		return
	}
	channelCacheMu.Lock()
	defer channelCacheMu.Unlock()
	guildID := ch.GuildID
	if guildID == "" && ch.ParentID != "" {
		if parentSnap, ok := channelCache[ch.ParentID]; ok {
			guildID = parentSnap.GuildID
		}
	}
	channelCache[ch.ID] = ChannelSnapshot{ID: ch.ID, GuildID: guildID, Name: ch.Name, ParentID: ch.ParentID, IsThread: ch.IsThread()}
}

// InvalidateChannelCache removes a channel from the internal cache.
func InvalidateChannelCache(channelID string) {
	channelCacheMu.Lock()
	delete(channelCache, channelID)
	channelCacheMu.Unlock()
}

// GetCachedChannel returns a cached channel snapshot if available.
func GetCachedChannel(channelID string) (ChannelSnapshot, bool) {
	channelCacheMu.RLock()
	snap, ok := channelCache[channelID]
	channelCacheMu.RUnlock()
	return snap, ok
}

// IsNumericSnowflake returns true if id != "" and contains only ASCII digits [0-9].
func IsNumericSnowflake(id string) bool {
	if id == "" {
		return false
	}
	for i := range len(id) {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
}

func fetchAndCacheRESTChannel(s *discordgo.Session, channelID string) (ChannelSnapshot, bool) {
	if s.Token == "" || !IsNumericSnowflake(channelID) {
		return ChannelSnapshot{}, false
	}
	res, err, _ := restSingleFlight.Do(channelID, func() (any, error) { return s.Channel(channelID) })
	if err != nil {
		return ChannelSnapshot{}, false
	}
	ch, ok := res.(*discordgo.Channel)
	if !ok || ch == nil {
		return ChannelSnapshot{}, false
	}
	CacheDiscordChannel(ch)
	if s.State != nil {
		if err := s.State.ChannelAdd(ch); err != nil {
			log.Printf("[Queue] Notice: channel state cache update: %v", err)
		}
	}
	return GetCachedChannel(channelID)
}

func resolveChannelSnapshot(s *discordgo.Session, channelID string) (ChannelSnapshot, bool) {
	if channelID == "" {
		return ChannelSnapshot{}, false
	}
	if snap, ok := GetCachedChannel(channelID); ok {
		return snap, true
	}
	if s == nil {
		return ChannelSnapshot{}, false
	}
	if s.State != nil {
		if ch, err := s.State.Channel(channelID); err == nil && ch != nil {
			CacheDiscordChannel(ch)
			return GetCachedChannel(channelID)
		}
	}
	return fetchAndCacheRESTChannel(s, channelID)
}

// ResolveChannelAndThread resolves a Discord channel or thread to its effective channel ID,
// effective channel name, thread boolean flag, and thread name (if applicable).
func ResolveChannelAndThread(s *discordgo.Session, channelID string) (string, string, bool, string) {
	if channelID == "" {
		return "", "", false, ""
	}
	snap, ok := resolveChannelSnapshot(s, channelID)
	if !ok {
		return channelID, "", false, ""
	}
	if snap.IsThread {
		if snap.ParentID != "" {
			parentSnap, _ := resolveChannelSnapshot(s, snap.ParentID)
			return snap.ParentID, parentSnap.Name, true, snap.Name
		}
		return snap.ID, snap.Name, true, snap.Name
	}
	return snap.ID, snap.Name, false, ""
}

// ResolveEffectiveChannel resolves a Discord channel or thread to its effective channel ID and name.
func ResolveEffectiveChannel(s *discordgo.Session, channelID string) (string, string, bool) {
	effID, effName, isThread, _ := ResolveChannelAndThread(s, channelID)
	return effID, effName, isThread
}

var guildMemberFetcher = func(sess *discordgo.Session, guildID string, userID string) (*discordgo.Member, error) {
	if sess == nil || (sess.Client == nil && sess.Token == "") {
		return nil, fmt.Errorf("uninitialized test session")
	}
	return sess.GuildMember(guildID, userID)
}

func addNonEmptyRoles(roleSet map[string]bool, roles []string) {
	for _, rID := range roles {
		if rID != "" {
			roleSet[rID] = true
		}
	}
}

func isBotNamedRole(r *discordgo.Role, botUsername string) bool {
	if r == nil {
		return false
	}
	return strings.EqualFold(r.Name, "aerial") || strings.EqualFold(r.Name, "gundam") ||
		(botUsername != "" && strings.EqualFold(r.Name, botUsername))
}

func collectStateBotRoles(sess *discordgo.Session, guilds []*discordgo.Guild, botUserID, botUsername string, roleSet map[string]bool) bool {
	foundMemberInState := false
	for _, g := range guilds {
		if g == nil {
			continue
		}
		if botUserID != "" {
			if member, err := sess.State.Member(g.ID, botUserID); err == nil && member != nil {
				foundMemberInState = true
				addNonEmptyRoles(roleSet, member.Roles)
			}
		}
		for _, r := range g.Roles {
			if isBotNamedRole(r, botUsername) {
				roleSet[r.ID] = true
			}
		}
	}
	return foundMemberInState
}

func fetchAndCacheBotMemberRoles(sess *discordgo.Session, guildID, botUserID string, roleSet map[string]bool) {
	if botUserID == "" || guildID == "" || sess == nil {
		return
	}
	sfKey := fmt.Sprintf("bot_member:%s:%s", guildID, botUserID)
	if isRESTInCooldown(sfKey) {
		return
	}
	res, sfErr, _ := restSingleFlight.Do(sfKey, func() (any, error) {
		m, err := guildMemberFetcher(sess, guildID, botUserID)
		if err != nil || m == nil {
			return nil, err
		}
		if m.GuildID == "" {
			m.GuildID = guildID
		}
		if m.User != nil {
			if err := sess.State.MemberAdd(m); err != nil {
				// best effort state cache update
				_ = err
			}
		}
		return append([]string(nil), m.Roles...), nil
	})
	if sfErr != nil {
		setRESTCooldown(sfKey, 30*time.Second)
		return
	}
	if fetchedRoles, ok := res.([]string); ok {
		addNonEmptyRoles(roleSet, fetchedRoles)
	}
}

// ResolveBotRoleIDs returns all role IDs associated with the bot in the given guild.
func ResolveBotRoleIDs(sess *discordgo.Session, guildID string, botUserID string) []string {
	if sess == nil || sess.State == nil {
		return nil
	}
	roleSet := make(map[string]bool)
	guilds := sess.State.Guilds
	if guildID != "" {
		if g, err := sess.State.Guild(guildID); err == nil && g != nil {
			guilds = []*discordgo.Guild{g}
		}
	}
	botUsername := ""
	if sess.State.User != nil {
		botUsername = sess.State.User.Username
	}
	foundMemberInState := collectStateBotRoles(sess, guilds, botUserID, botUsername, roleSet)
	if !foundMemberInState {
		fetchAndCacheBotMemberRoles(sess, guildID, botUserID, roleSet)
	}

	res := make([]string, 0, len(roleSet))
	for rID := range roleSet {
		res = append(res, rID)
	}
	sort.Strings(res)
	return res
}

func isTier1Wake(m db.Message, botUserID string, botRoleIDs []string) bool {
	if m.AuthorID == "http-client" {
		return true
	}
	aliases := []string{"aerial"}
	var mentionIDs, candidates []string
	var rawContent string

	if m.HasStructuredFields() {
		mentionIDs = append(slices.Clone(m.MentionUserIDs), m.MentionRoleIDs...)
		candidates = append(slices.Clone(m.Mentions), m.ReplyingToAuthor)
	} else {
		rawContent = m.Content
		mentionIDs = append(extractHeaderTokens(m.Content, "- mention_user_ids: ["), extractHeaderTokens(m.Content, "- mention_role_ids: [")...)
		candidates = append(extractHeaderTokens(m.Content, "- mentions: ["), extractReplyAuthor(m.Content))
	}
	if matchesWakeIdentity(rawContent, mentionIDs, botRoleIDs, botUserID, aliases) {
		return true
	}
	for _, text := range candidates {
		if matchesMentionOrAuthor(text, botUserID, botRoleIDs, aliases) {
			return true
		}
	}
	return false
}

func extractHeaderTokens(content, prefix string) []string {
	idx := strings.Index(content, prefix)
	if idx == -1 {
		return nil
	}
	endIdx := strings.Index(content[idx:], "]")
	if endIdx == -1 {
		return nil
	}
	return strings.Fields(content[idx+len(prefix) : idx+endIdx])
}

func extractReplyAuthor(content string) string {
	idx := strings.Index(content, "- replying_to:")
	if idx == -1 {
		return ""
	}
	for line := range strings.SplitSeq(content[idx:], "\n") {
		trimmed := strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(trimmed, "author:"); ok {
			return strings.TrimSpace(after)
		}
		if trimmed != "- replying_to:" && strings.HasPrefix(trimmed, "- ") {
			break
		}
	}
	return ""
}

func matchesMentionOrAuthor(text, botID string, roleIDs, aliases []string) bool {
	if text == "" {
		return false
	}
	lower := strings.ToLower(text)
	for _, a := range aliases {
		if a != "" && strings.Contains(lower, strings.ToLower(a)) {
			return true
		}
	}
	if botID != "" && strings.Contains(lower, strings.ToLower(botID)) {
		return true
	}
	for _, rID := range roleIDs {
		if rID != "" && strings.Contains(lower, strings.ToLower(rID)) {
			return true
		}
	}
	return false
}

func hasRoleOrAliasOverlap(mentionIDs, roleIDs, aliases []string) bool {
	for _, rID := range roleIDs {
		if rID != "" && (slices.Contains(mentionIDs, rID) || slices.Contains(aliases, rID)) {
			return true
		}
	}
	for _, a := range aliases {
		if a != "" && (slices.Contains(roleIDs, a) || slices.Contains(mentionIDs, a)) {
			return true
		}
	}
	return false
}

func matchesRawContentMentions(content string, roleIDs []string, botID string, aliases []string) bool {
	if content == "" || strings.Contains(content, "<USER_REQUEST>") {
		return false
	}
	if botID != "" && (strings.Contains(content, "<@"+botID+">") || strings.Contains(content, "<@!"+botID+">")) {
		return true
	}
	for _, rID := range roleIDs {
		if rID != "" && strings.Contains(content, "<@&"+rID+">") {
			return true
		}
	}
	contentLower := strings.ToLower(content)
	for _, a := range aliases {
		if strings.Contains(contentLower, "<@"+a) || strings.Contains(contentLower, "<@!"+a) {
			return true
		}
	}
	return false
}

func matchesWakeIdentity(content string, mentionIDs, roleIDs []string, botID string, aliases []string) bool {
	if botID != "" && slices.Contains(mentionIDs, botID) {
		return true
	}
	if hasRoleOrAliasOverlap(mentionIDs, roleIDs, aliases) {
		return true
	}
	return matchesRawContentMentions(content, roleIDs, botID, aliases)
}

// FormatThreadTitle formats the thread title for a thread trigger, clamped to at most 100 runes.
func FormatThreadTitle(titlePrefix, prompt string, now time.Time) string {
	dateStr := now.Format("Jan 02, 2006")
	trimmed := strings.TrimSpace(titlePrefix)
	var title string
	switch {
	case trimmed != "":
		title = fmt.Sprintf("%s – %s", trimmed, dateStr)
	case strings.TrimSpace(prompt) != "":
		clean := db.CleanTaskSummary(prompt)
		title = fmt.Sprintf("%s – %s", clean, dateStr)
	default:
		title = "Scheduled Routine – " + dateStr
	}
	runes := []rune(title)
	if len(runes) > 100 {
		runes = append(runes[:97], []rune("...")...)
	}
	return string(runes)
}

// ShouldCreateThread determines whether a new public Discord thread should be created.
func ShouldCreateThread(isAlreadyThread bool, channelMode string) bool {
	return !isAlreadyThread && strings.ToLower(strings.TrimSpace(channelMode)) != "channel"
}

// IsValidThreadKey returns true if 1 <= len(id) <= 128, !strings.Contains(id, ":"),
// and all bytes are ASCII [a-zA-Z0-9_-].
func IsValidThreadKey(id string) bool {
	if len(id) < 1 || len(id) > 128 || strings.Contains(id, ":") {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

// ResolveTargetThread resolves a target Discord thread for a prompt, auto-creating a thread
// if channel policy permits and the target is not already a thread.
func (p *WorkerPool) ResolveTargetThread(channelID, title, prompt string, now time.Time) string {
	if p == nil || channelID == "" {
		return channelID
	}
	sess := p.getDiscordSession()
	snap, ok := resolveChannelSnapshot(sess, channelID)
	var policy config.ChannelPolicy
	if p.cfg.ResolveChannelPolicy != nil {
		policy = p.cfg.ResolveChannelPolicy(channelID, snap.Name)
	} else {
		policy = config.ActiveConfig().ResolveChannelPolicy(channelID, snap.Name)
	}
	if !ShouldCreateThread(ok && snap.IsThread, policy.Mode) {
		return channelID
	}
	threadTitle := FormatThreadTitle(title, prompt, now)
	if p.createThreadFn != nil {
		if tid, err := p.createThreadFn(channelID, threadTitle); err == nil && tid != "" {
			return tid
		}
		return channelID
	}
	if sess != nil && sess.Token != "" {
		if ch, err := sess.ThreadStart(channelID, threadTitle, discordgo.ChannelTypeGuildPublicThread, 1440); err == nil && ch != nil && ch.ID != "" {
			CacheDiscordChannel(ch)
			return ch.ID
		}
	}
	return channelID
}

var _ = func() bool {
	_ = FormatThreadTitle("", "", time.Time{})
	_ = ShouldCreateThread(false, "")
	return true
}()
