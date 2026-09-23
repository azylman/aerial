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
			if snap, ok := GetCachedChannel(channelID); ok {
				return snap, true
			}
		}
	}
	if s.Token != "" && IsNumericSnowflake(channelID) {
		res, err, _ := restSingleFlight.Do(channelID, func() (interface{}, error) {
			return s.Channel(channelID)
		})
		if err == nil && res != nil {
			if ch, ok := res.(*discordgo.Channel); ok && ch != nil {
				CacheDiscordChannel(ch)
				if s.State != nil {
					if err := s.State.ChannelAdd(ch); err != nil {
						log.Printf("[WARN] Failed to add channel %s to session state: %v", ch.ID, err)
					}
				}
				if snap, ok := GetCachedChannel(channelID); ok {
					return snap, true
				}
			}
		}
	}
	return ChannelSnapshot{}, false
}

// ResolveEffectiveChannel resolves a Discord channel or thread to its effective channel ID and name.
// If channelID is a Discord thread, it resolves the parent channel ID and parent channel name.
// It uses the centralized ChannelSnapshot cache, live Discord State, and singleflight REST queries.
// For non-numeric or synthetic channel IDs (e.g. HTTP client UUIDs), it immediately returns without REST calls.
func ResolveEffectiveChannel(s *discordgo.Session, channelID string) (effectiveID string, effectiveName string, isThread bool) {
	if channelID == "" {
		return "", "", false
	}

	snap, ok := resolveChannelSnapshot(s, channelID)
	if !ok {
		return channelID, "", false
	}

	if snap.IsThread {
		if snap.ParentID != "" {
			parentSnap, parentOk := resolveChannelSnapshot(s, snap.ParentID)
			if parentOk {
				return snap.ParentID, parentSnap.Name, true
			}
			return snap.ParentID, "", true
		}
		return snap.ID, snap.Name, true
	}
	return snap.ID, snap.Name, false
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

	// If guildID is specified, inspect that guild; otherwise inspect all cached guilds
	var guilds []*discordgo.Guild
	if guildID != "" {
		if g, err := sess.State.Guild(guildID); err == nil && g != nil {
			guilds = append(guilds, g)
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
		// 1. Roles assigned to the bot member from state cache
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
		// 2. Roles in guild matching bot name or username
		for _, r := range g.Roles {
			if r == nil {
				continue
			}
			if strings.EqualFold(r.Name, "aerial") || strings.EqualFold(r.Name, "gundam") ||
				(botUsername != "" && strings.EqualFold(r.Name, botUsername)) {
				roleSet[r.ID] = true
			}
		}
	}

	// 3. Fallback: If guildID was specified and bot member wasn't in state cache, perform singleflight REST fetch
	if botUserID != "" && guildID != "" && !foundMemberInState && sess != nil {
		sfKey := fmt.Sprintf("bot_member:%s:%s", guildID, botUserID)
		if !isRESTInCooldown(sfKey) {
			res, sfErr, _ := restSingleFlight.Do(sfKey, func() (interface{}, error) {
				fetchedMember, fetchErr := guildMemberFetcher(sess, guildID, botUserID)
				if fetchErr != nil || fetchedMember == nil {
					return nil, fetchErr
				}
				if fetchedMember.GuildID == "" {
					fetchedMember.GuildID = guildID
				}
				if fetchedMember.User != nil {
					if err := sess.State.MemberAdd(fetchedMember); err != nil {
						log.Printf("[WARN] Failed to add member %s to session state: %v", botUserID, err)
					}
				}
				rolesCopy := append([]string(nil), fetchedMember.Roles...)
				return rolesCopy, nil
			})
			if sfErr != nil {
				setRESTCooldown(sfKey, 30*time.Second)
			} else if res != nil {
				if fetchedRoles, ok := res.([]string); ok {
					for _, rID := range fetchedRoles {
						if rID != "" {
							roleSet[rID] = true
						}
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

func containsToken(s, token string) bool {
	if token == "" {
		return false
	}
	for _, f := range strings.Fields(s) {
		if f == token {
			return true
		}
	}
	return false
}

func isTier1Wake(m db.Message, botUserID string, botRoleIDs []string) bool {
	if m.AuthorID == "http-client" || m.AuthorID == "scheduler" || m.ScheduleRunID != "" {
		return true
	}

	if !m.Metadata.IsEmpty() {
		// 1. Direct slice lookup for User IDs
		if botUserID != "" && slices.Contains(m.Metadata.MentionUserIDs, botUserID) {
			return true
		}
		// 2. Direct slice lookup for Role IDs
		for _, rID := range botRoleIDs {
			if rID != "" && slices.Contains(m.Metadata.MentionRoleIDs, rID) {
				return true
			}
		}
		// 3. Mentions check (username or role name)
		for _, mention := range m.Metadata.Mentions {
			mentionLower := strings.ToLower(mention)
			if strings.Contains(mentionLower, "aerial") || (botUserID != "" && strings.Contains(mentionLower, strings.ToLower(botUserID))) {
				return true
			}
			for _, rID := range botRoleIDs {
				if rID != "" && strings.Contains(mentionLower, strings.ToLower(rID)) {
					return true
				}
			}
		}
		// 4. Replying to author
		if m.Metadata.ReplyingToAuthor != "" {
			authorLower := strings.ToLower(m.Metadata.ReplyingToAuthor)
			if strings.Contains(authorLower, "aerial") || (botUserID != "" && strings.Contains(authorLower, strings.ToLower(botUserID))) {
				return true
			}
		}
		return false
	}

	// 1. Structured Discord Gateway User Mentions from Prompt Envelope:
	// - mention_user_ids: [12345 67890]
	if idx := strings.Index(m.Content, "- mention_user_ids: ["); idx != -1 {
		if endIdx := strings.Index(m.Content[idx:], "]"); endIdx != -1 {
			inside := m.Content[idx+len("- mention_user_ids: [") : idx+endIdx]
			if botUserID != "" && containsToken(inside, botUserID) {
				return true
			}
		}
	}

	// 2. Structured Discord Gateway Role Mentions from Prompt Envelope:
	// - mention_role_ids: [role1 role2]
	if idx := strings.Index(m.Content, "- mention_role_ids: ["); idx != -1 {
		if endIdx := strings.Index(m.Content[idx:], "]"); endIdx != -1 {
			inside := m.Content[idx+len("- mention_role_ids: [") : idx+endIdx]
			for _, rID := range botRoleIDs {
				if rID != "" && containsToken(inside, rID) {
					return true
				}
			}
		}
	}

	// 3. Mentions list in Discord prompt envelope containing bot name, botUserID, or botRoleIDs
	if idx := strings.Index(m.Content, "- mentions: ["); idx != -1 {
		if endIdx := strings.Index(m.Content[idx:], "]"); endIdx != -1 {
			inside := strings.ToLower(m.Content[idx+len("- mentions: [") : idx+endIdx])
			if strings.Contains(inside, "aerial") || (botUserID != "" && strings.Contains(inside, strings.ToLower(botUserID))) {
				return true
			}
			for _, rID := range botRoleIDs {
				if rID != "" && strings.Contains(inside, strings.ToLower(rID)) {
					return true
				}
			}
		}
	}

	// 4. Explicit reply to Aerial: check ONLY the author line under - replying_to:
	if idx := strings.Index(m.Content, "- replying_to:"); idx != -1 {
		lines := strings.Split(m.Content[idx:], "\n")
		for _, line := range lines {
			lineTrimmed := strings.TrimSpace(line)
			if strings.HasPrefix(lineTrimmed, "author:") {
				authorVal := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(lineTrimmed, "author:")))
				if strings.Contains(authorVal, "aerial") || (botUserID != "" && strings.Contains(authorVal, strings.ToLower(botUserID))) {
					return true
				}
				break
			}
			if lineTrimmed != "- replying_to:" && strings.HasPrefix(lineTrimmed, "- ") {
				break
			}
		}
	}

	// 5. Fallback for un-enveloped messages (e.g. ad-hoc unit test fixtures without <USER_REQUEST>):
	if !strings.Contains(m.Content, "<USER_REQUEST>") {
		if botUserID != "" && (strings.Contains(m.Content, "<@"+botUserID+">") || strings.Contains(m.Content, "<@!"+botUserID+">")) {
			return true
		}
		for _, rID := range botRoleIDs {
			if rID != "" && strings.Contains(m.Content, "<@&"+rID+">") {
				return true
			}
		}
		contentLower := strings.ToLower(m.Content)
		if strings.Contains(contentLower, "<@aerial") || strings.Contains(contentLower, "<@!aerial") {
			return true
		}
	}
	return false
}
