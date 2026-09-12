package queue

import (
	"regexp"
	"sort"
	"strings"
	"sync"

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

	aerialExclusionRegex = regexp.MustCompile(`(?i)\baerial\s+(?:view|photo)s?\b`)
	tier1KeywordRegex    = regexp.MustCompile(`(?i)\b(aerial|gundam)\b`)
)

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
					_ = s.State.ChannelAdd(ch)
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
	trimmed := strings.TrimSpace(content)
	if strings.Contains(trimmed, "<USER_REQUEST>") {
		contentMarker := "- content:"
		idx := strings.Index(trimmed, contentMarker)
		if idx != -1 {
			start := idx + len(contentMarker)
			rest := trimmed[start:]

			// Find boundary of next envelope field
			endIdx := -1
			markers := []string{"\n- timestamp:", "\n- mentions:", "\n- attachments:", "\n- ", "\n</USER_REQUEST>"}
			for _, m := range markers {
				if pos := strings.Index(rest, m); pos != -1 {
					if endIdx == -1 || pos < endIdx {
						endIdx = pos
					}
				}
			}
			var val string
			if endIdx != -1 {
				val = rest[:endIdx]
			} else {
				val = rest
			}
			val = strings.TrimSpace(val)
			val = strings.ReplaceAll(val, "<\\/USER_REQUEST>", "</USER_REQUEST>")
			val = strings.ReplaceAll(val, "<\\USER_REQUEST>", "<USER_REQUEST>")
			return val
		}
		inner := strings.TrimPrefix(trimmed, "<USER_REQUEST>")
		inner = strings.TrimSuffix(inner, "</USER_REQUEST>")
		return strings.TrimSpace(inner)
	}
	return trimmed
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

	for _, g := range guilds {
		if g == nil {
			continue
		}
		// 1. Roles assigned to the bot member
		if botUserID != "" {
			if member, err := sess.State.Member(g.ID, botUserID); err == nil && member != nil {
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
			if strings.EqualFold(r.Name, "aerial") || strings.EqualFold(r.Name, "gundam") || (botUsername != "" && strings.EqualFold(r.Name, botUsername)) {
				roleSet[r.ID] = true
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

func isTier1Wake(m db.Message, botUserID string, botRoleIDs []string, wakeMode string) bool {
	if m.AuthorID == "http-client" || m.AuthorID == "scheduler" || m.ScheduleRunID != "" {
		return true
	}

	// Direct user mentions: <@botUserID>, <@!botUserID>
	if botUserID != "" {
		if strings.Contains(m.Content, "<@"+botUserID+">") || strings.Contains(m.Content, "<@!"+botUserID+">") {
			return true
		}
	}

	// Direct role mentions: <@&botRoleID> for any role associated with the bot
	for _, rID := range botRoleIDs {
		if rID != "" && strings.Contains(m.Content, "<@&"+rID+">") {
			return true
		}
	}

	// Mentions list in Discord prompt envelope containing bot name, botUserID, or botRoleIDs
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

	// Explicit reply to Aerial: check ONLY the author line under - replying_to:
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

	// Check message body direct mentions
	body := extractMessageBody(m.Content)
	if botUserID != "" && (strings.Contains(body, "<@"+botUserID+">") || strings.Contains(body, "<@!"+botUserID+">")) {
		return true
	}
	for _, rID := range botRoleIDs {
		if rID != "" && strings.Contains(body, "<@&"+rID+">") {
			return true
		}
	}
	bodyLower := strings.ToLower(body)
	if strings.Contains(bodyLower, "<@aerial") || strings.Contains(bodyLower, "<@!aerial") {
		return true
	}

	// If wake_mode is "mention", plaintext keywords / name drops do NOT trigger a wake.
	if strings.ToLower(strings.TrimSpace(wakeMode)) == "mention" {
		return false
	}

	// Keyword trigger matching word boundary regex (?i)\b(aerial|gundam)\b in extractMessageBody(m.Content)
	// excluding "aerial view" and "aerial photo"
	cleanedBody := aerialExclusionRegex.ReplaceAllString(body, "")
	return tier1KeywordRegex.MatchString(cleanedBody)
}
