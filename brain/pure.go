package main

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/bwmarrin/discordgo"
)

const discordErrCodeThreadAlreadyCreated = 160004

var mentionAndEmojiRegex = regexp.MustCompile(`(?i)<(@!?|@&|#|a?:[a-z0-9_]+:)[0-9]+>`)

var cronMonthNames = map[int]string{
	1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
	7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

var cronDayNames = map[int]string{
	0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday",
}

var cronDayShortNames = map[int]string{
	0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun",
}

// Ordinal formats an integer into its English ordinal string (e.g., 1st, 2nd, 3rd, 4th, 11th, 111th).
func Ordinal(n int) string {
	abs := n
	if abs < 0 {
		abs = -abs
	}
	rem100 := abs % 100
	if rem100 >= 11 && rem100 <= 13 {
		return fmt.Sprintf("%dth", n)
	}
	switch abs % 10 {
	case 1:
		return fmt.Sprintf("%dst", n)
	case 2:
		return fmt.Sprintf("%dnd", n)
	case 3:
		return fmt.Sprintf("%drd", n)
	default:
		return fmt.Sprintf("%dth", n)
	}
}

// FormatCronDescription converts a standard 5-field cron expression or descriptor into human-readable English.
func FormatCronDescription(cronExpr string) string {
	expr := strings.TrimSpace(cronExpr)
	if expr == "" {
		return ""
	}

	switch strings.ToLower(expr) {
	case "@yearly", "@annually":
		return "Every year on Jan 1st at 00:00"
	case "@monthly":
		return "1st of every month at 00:00"
	case "@weekly":
		return "Every week on Sunday at 00:00"
	case "@daily", "@midnight":
		return "Every day at 00:00"
	case "@hourly":
		return "Every hour"
	}

	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return expr
	}

	minStr, hourStr, domStr, monStr, dowStr := fields[0], fields[1], fields[2], fields[3], fields[4]

	// Case: Every minute (* * * * *)
	if minStr == "*" && hourStr == "*" && domStr == "*" && monStr == "*" && dowStr == "*" {
		return "Every minute"
	}

	// Case: Every X minutes (*/N * * * *)
	if strings.HasPrefix(minStr, "*/") && hourStr == "*" && domStr == "*" && monStr == "*" && dowStr == "*" {
		interval := strings.TrimPrefix(minStr, "*/")
		return fmt.Sprintf("Every %s minutes", interval)
	}

	// Case: Every X hours (0 */N * * *)
	if minStr == "0" && strings.HasPrefix(hourStr, "*/") && domStr == "*" && monStr == "*" && dowStr == "*" {
		interval := strings.TrimPrefix(hourStr, "*/")
		return fmt.Sprintf("Every %s hours", interval)
	}

	// Try to parse hour and minute as integers
	m, minErr := strconv.Atoi(minStr)
	h, hourErr := strconv.Atoi(hourStr)

	if minErr == nil && hourErr == nil {
		timeStr := fmt.Sprintf("%02d:%02d", h, m)

		// 1. Every day at HH:MM (0 9 * * *)
		if domStr == "*" && monStr == "*" && dowStr == "*" {
			return fmt.Sprintf("Every day at %s", timeStr)
		}

		// 2. Specific day of week (0 9 * * 1-5, 0 9 * * 0, etc.)
		if domStr == "*" && monStr == "*" && dowStr != "*" {
			dowUpper := strings.ToUpper(dowStr)
			if dowUpper == "1-5" || dowUpper == "MON-FRI" {
				return fmt.Sprintf("Weekdays (Mon–Fri) at %s", timeStr)
			}
			if dowUpper == "0,6" || dowUpper == "6,0" || dowUpper == "SAT,SUN" || dowUpper == "SUN,SAT" {
				return fmt.Sprintf("Weekends (Sat–Sun) at %s", timeStr)
			}

			// Single number day of week
			if dowNum, err := strconv.Atoi(dowStr); err == nil && dowNum >= 0 && dowNum <= 7 {
				return fmt.Sprintf("Every %s at %s", cronDayNames[dowNum], timeStr)
			}

			// Comma-separated list of days (e.g. 1,3,5 or Mon,Wed,Fri)
			parts := strings.Split(dowStr, ",")
			var names []string
			for _, p := range parts {
				pTrim := strings.TrimSpace(p)
				if dNum, err := strconv.Atoi(pTrim); err == nil && dNum >= 0 && dNum <= 7 {
					names = append(names, cronDayShortNames[dNum])
				} else {
					names = append(names, pTrim)
				}
			}
			if len(names) > 0 {
				return fmt.Sprintf("%s at %s", strings.Join(names, ", "), timeStr)
			}
		}

		// 3. Specific day of month (0 12 1 * *)
		if domStr != "*" && monStr == "*" && dowStr == "*" {
			if domNum, err := strconv.Atoi(domStr); err == nil && domNum >= 1 && domNum <= 31 {
				return fmt.Sprintf("%s of every month at %s", Ordinal(domNum), timeStr)
			}
		}

		// 4. Specific month and day (0 0 1 1 *)
		if domStr != "*" && monStr != "*" && dowStr == "*" {
			domNum, errDom := strconv.Atoi(domStr)
			monNum, errMon := strconv.Atoi(monStr)
			if errDom == nil && errMon == nil && monNum >= 1 && monNum <= 12 {
				return fmt.Sprintf("Every year on %s %s at %s", cronMonthNames[monNum], Ordinal(domNum), timeStr)
			}
		}

		return fmt.Sprintf("At %s (cron: %s)", timeStr, expr)
	}

	return expr
}

// DeriveThreadTitle extracts a clean, readable thread title from a message content string.
func DeriveThreadTitle(content string) string {
	cleaned := mentionAndEmojiRegex.ReplaceAllString(content, "")
	cleaned = strings.TrimSpace(cleaned)

	var firstLine string
	for _, line := range strings.Split(cleaned, "\n") {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimLeft(trimmed, "#> \t")
		trimmed = strings.TrimSpace(trimmed)
		if trimmed != "" {
			firstLine = trimmed
			break
		}
	}

	if firstLine == "" {
		firstLine = "Aerial Discussion"
	}
	runes := []rune(firstLine)
	if len(runes) > 60 {
		cut := runes[:57]
		for len(cut) > 0 {
			last := cut[len(cut)-1]
			if last == '\u200D' || last == '\u200B' || last == '\uFEFF' {
				cut = cut[:len(cut)-1]
				continue
			}
			break
		}
		return string(cut) + "..."
	}
	return string(runes)
}

// DiscordPromptInput contains all pre-resolved inputs needed to build a Discord prompt purely without I/O.
type DiscordPromptInput struct {
	Message        *discordgo.Message
	TargetThreadID string
	Policy         config.ChannelPolicy
	AdminUsers     []string
	Timezone       string
	RoleNames      map[string]string
}

// BuildDiscordPrompt constructs a deterministic user prompt from message metadata without session I/O.
func BuildDiscordPrompt(input DiscordPromptInput) string {
	m := input.Message
	if m == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("<USER_REQUEST>\nHere's a message someone sent you from Discord:\n\n")
	sb.WriteString(fmt.Sprintf("- id: %s\n", m.ID))
	sb.WriteString(fmt.Sprintf("- channel_id: %s\n", m.ChannelID))
	sb.WriteString(fmt.Sprintf("- thread_id: %s\n", input.TargetThreadID))
	sb.WriteString(fmt.Sprintf("- guild_id: %s\n", m.GuildID))

	isAdmin := false
	if m.Author != nil {
		isAdmin = config.IsAdmin(input.AdminUsers, m.Author.ID, m.Author.Username, m.Author.GlobalName)
		cleanUsername := strings.ReplaceAll(strings.ReplaceAll(m.Author.Username, "\n", " "), "\r", "")
		cleanGlobalName := strings.ReplaceAll(strings.ReplaceAll(m.Author.GlobalName, "\n", " "), "\r", "")
		sb.WriteString(fmt.Sprintf("- author_id: %s\n", m.Author.ID))
		sb.WriteString(fmt.Sprintf("- author_username: %s\n", cleanUsername))
		sb.WriteString(fmt.Sprintf("- author_global_name: %s\n", cleanGlobalName))
		sb.WriteString(fmt.Sprintf("- author_bot: %t\n", m.Author.Bot))
		sb.WriteString(fmt.Sprintf("- is_admin: %t\n", isAdmin))
	} else {
		sb.WriteString(fmt.Sprintf("- is_admin: %t\n", false))
	}

	if m.ReferencedMessage != nil {
		authorName := "Unknown"
		if m.ReferencedMessage.Author != nil {
			authorName = strings.ReplaceAll(strings.ReplaceAll(m.ReferencedMessage.Author.Username, "\n", " "), "\r", "")
		}
		sb.WriteString("- replying_to:\n")
		sb.WriteString(fmt.Sprintf("    author: \"@%s\"\n", authorName))
		cleanRefContent := strings.ReplaceAll(m.ReferencedMessage.Content, "</USER_REQUEST>", "<\\/USER_REQUEST>")
		cleanRefContent = strings.ReplaceAll(cleanRefContent, "<USER_REQUEST>", "<\\USER_REQUEST>")
		sb.WriteString(fmt.Sprintf("    content: %q\n", cleanRefContent))
	}

	sanitizedContent := strings.ReplaceAll(m.Content, "</USER_REQUEST>", "<\\/USER_REQUEST>")
	sanitizedContent = strings.ReplaceAll(sanitizedContent, "<USER_REQUEST>", "<\\USER_REQUEST>")
	sb.WriteString(fmt.Sprintf("- content: %s\n", sanitizedContent))
	sb.WriteString(fmt.Sprintf("- timestamp: %s\n", m.Timestamp.Format(time.RFC3339)))

	var mentions []string
	for _, u := range m.Mentions {
		if u != nil {
			cleanU := strings.ReplaceAll(strings.ReplaceAll(u.Username, "\n", " "), "\r", "")
			mentions = append(mentions, cleanU)
		}
	}
	for _, roleID := range m.MentionRoles {
		roleName := roleID
		if input.RoleNames != nil {
			if rName, ok := input.RoleNames[roleID]; ok && rName != "" {
				roleName = rName
			}
		}
		cleanR := strings.ReplaceAll(strings.ReplaceAll(roleName, "\n", " "), "\r", "")
		mentions = append(mentions, cleanR)
	}
	sb.WriteString(fmt.Sprintf("- mentions: %v\n", mentions))

	var attachments []string
	for _, a := range m.Attachments {
		if a != nil {
			attachments = append(attachments, a.URL)
		}
	}
	sb.WriteString(fmt.Sprintf("- attachments: %v\n\n", attachments))

	sb.WriteString("Fulfill the user's request. If the user asks for a plan, design, proposal, or investigation, draft the plan, run review gates, and present it for review without modifying source code or opening PRs. If the user asks to implement, build, or fix something, execute all necessary tools, subagents, tests, and code modifications. Only formulate and output your final response once all immediate work is complete. It will be delivered directly to Discord.\n")
	sb.WriteString("</USER_REQUEST>")
	return sb.String()
}

// IsThreadAlreadyExistsError returns true if the error indicates a Discord thread already exists for a message (code 160004).
func IsThreadAlreadyExistsError(err error) bool {
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

// IsMessageableChannel returns true if the given channel type can receive chat messages.
func IsMessageableChannel(chType discordgo.ChannelType) bool {
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

// ResolveGuildID resolves the Discord Guild ID using strict precedence: message -> cached -> channel.
func ResolveGuildID(msgGuildID, cachedGuildID, channelGuildID string) string {
	if msgGuildID != "" {
		return msgGuildID
	}
	if cachedGuildID != "" {
		return cachedGuildID
	}
	return channelGuildID
}
