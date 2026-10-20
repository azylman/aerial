package queue

import (
	"fmt"
	"log"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
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
	channelCache[ch.ID] = ChannelSnapshot{
		ID:       ch.ID,
		GuildID:  guildID,
		Name:     ch.Name,
		ParentID: ch.ParentID,
		IsThread: ch.IsThread(),
	}
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
	for i := 0; i < len(id); i++ {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
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
	if s.Token != "" && IsNumericSnowflake(channelID) {
		res, err, _ := restSingleFlight.Do(channelID, func() (interface{}, error) {
			return s.Channel(channelID)
		})
		if err == nil {
			if ch, ok := res.(*discordgo.Channel); ok && ch != nil {
				CacheDiscordChannel(ch)
				if s.State != nil {
					if err := s.State.ChannelAdd(ch); err != nil {
						log.Printf("[WARN] Failed to add channel %s to state: %v", ch.ID, err)
					}
				}
				return GetCachedChannel(channelID)
			}
		}
	}
	return ChannelSnapshot{}, false
}

// ResolveChannelAndThread resolves a Discord channel or thread to its effective channel ID,
// effective channel name, thread boolean flag, and thread name (if applicable).
// For channels that are not threads, threadName is empty.
func ResolveChannelAndThread(s *discordgo.Session, channelID string) (effectiveID string, effectiveName string, isThread bool, threadName string) {
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
// If channelID is a Discord thread, it resolves the parent channel ID and parent channel name.
// It uses the centralized ChannelSnapshot cache, live Discord State, and singleflight REST queries.
// For non-numeric or synthetic channel IDs (e.g. HTTP client UUIDs), it immediately returns without REST calls.
func ResolveEffectiveChannel(s *discordgo.Session, channelID string) (effectiveID string, effectiveName string, isThread bool) {
	effectiveID, effectiveName, isThread, _ = ResolveChannelAndThread(s, channelID)
	return effectiveID, effectiveName, isThread
}

func extractMessageBody(content string) string {
	return db.ExtractMessageBody(content)
}

var guildMemberFetcher = func(sess *discordgo.Session, guildID string, userID string) (*discordgo.Member, error) {
	if sess == nil || (sess.Client == nil && sess.Token == "") {
		return nil, fmt.Errorf("uninitialized test session")
	}
	return sess.GuildMember(guildID, userID)
}

// ResolveBotRoleIDs returns all role IDs associated with the bot in the given guild.
// This includes roles held by the bot member and any managed integration roles matching the bot name.
func ResolveBotRoleIDs(sess *discordgo.Session, guildID string, botUserID string) []string {
	if sess == nil || sess.State == nil {
		return nil
	}
	roleSet := make(map[string]bool)
	var guilds []*discordgo.Guild
	if guildID != "" {
		if g, err := sess.State.Guild(guildID); err == nil && g != nil {
			guilds = []*discordgo.Guild{g}
		}
	} else {
		guilds = sess.State.Guilds
	}
	botUsername := ""
	if sess.State.User != nil {
		botUsername = sess.State.User.Username
	}
	foundMemberInState := false
	for _, g := range guilds {
		if g == nil {
			continue
		}
		if botUserID != "" {
			if member, err := sess.State.Member(g.ID, botUserID); err == nil && member != nil {
				foundMemberInState = true
				for _, rID := range member.Roles {
					if rID != "" {
						roleSet[rID] = true
					}
				}
			}
		}
		for _, r := range g.Roles {
			if r != nil && (strings.EqualFold(r.Name, "aerial") || strings.EqualFold(r.Name, "gundam") ||
				(botUsername != "" && strings.EqualFold(r.Name, botUsername))) {
				roleSet[r.ID] = true
			}
		}
	}

	if botUserID != "" && guildID != "" && !foundMemberInState && sess != nil {
		sfKey := fmt.Sprintf("bot_member:%s:%s", guildID, botUserID)
		if !isRESTInCooldown(sfKey) {
			res, sfErr, _ := restSingleFlight.Do(sfKey, func() (interface{}, error) {
				m, err := guildMemberFetcher(sess, guildID, botUserID)
				if err != nil || m == nil {
					return nil, err
				}
				if m.GuildID == "" {
					m.GuildID = guildID
				}
				if m.User != nil {
					if err := sess.State.MemberAdd(m); err != nil {
						log.Printf("[WARN] Failed to add member %s to state: %v", botUserID, err)
					}
				}
				return append([]string(nil), m.Roles...), nil
			})
			if sfErr != nil {
				setRESTCooldown(sfKey, 30*time.Second)
			} else if fetchedRoles, ok := res.([]string); ok {
				for _, rID := range fetchedRoles {
					if rID != "" {
						roleSet[rID] = true
					}
				}
			}
		}
	}

	var res []string
	for rID := range roleSet {
		res = append(res, rID)
	}
	sort.Strings(res)
	return res
}

func isTier1Wake(m db.Message, botUserID string, botRoleIDs []string) bool {
	if m.AuthorID == "http-client" || m.AuthorID == "scheduler" || m.ScheduleRunID != "" {
		return true
	}

	aliases := []string{"aerial"}
	var mentionIDs, candidates []string
	var rawContent string

	if !m.Metadata.IsEmpty() {
		mentionIDs = append(slices.Clone(m.Metadata.MentionUserIDs), m.Metadata.MentionRoleIDs...)
		candidates = append(slices.Clone(m.Metadata.Mentions), m.Metadata.ReplyingToAuthor)
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
	for _, line := range strings.Split(content[idx:], "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "author:") {
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "author:"))
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

func matchesWakeIdentity(content string, mentionIDs, roleIDs []string, botID string, aliases []string) bool {
	if botID != "" && slices.Contains(mentionIDs, botID) {
		return true
	}
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
	if content != "" && !strings.Contains(content, "<USER_REQUEST>") {
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
	}
	return false
}
