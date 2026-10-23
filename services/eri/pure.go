package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
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
	switch strings.ToLower(expr) {
	case "":
		return ""
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
	if minStr == "*" && hourStr == "*" && domStr == "*" && monStr == "*" && dowStr == "*" {
		return "Every minute"
	}
	if strings.HasPrefix(minStr, "*/") && hourStr == "*" && domStr == "*" && monStr == "*" && dowStr == "*" {
		return fmt.Sprintf("Every %s minutes", strings.TrimPrefix(minStr, "*/"))
	}
	if minStr == "0" && strings.HasPrefix(hourStr, "*/") && domStr == "*" && monStr == "*" && dowStr == "*" {
		return fmt.Sprintf("Every %s hours", strings.TrimPrefix(hourStr, "*/"))
	}

	m, minErr := strconv.Atoi(minStr)
	h, hourErr := strconv.Atoi(hourStr)
	if minErr != nil || hourErr != nil {
		return expr
	}

	timeStr := fmt.Sprintf("%02d:%02d", h, m)
	if domStr == "*" && monStr == "*" && dowStr == "*" {
		return fmt.Sprintf("Every day at %s", timeStr)
	}
	if domStr == "*" && monStr == "*" && dowStr != "*" {
		switch strings.ToUpper(dowStr) {
		case "1-5", "MON-FRI":
			return fmt.Sprintf("Weekdays (Mon–Fri) at %s", timeStr)
		case "0,6", "6,0", "SAT,SUN", "SUN,SAT":
			return fmt.Sprintf("Weekends (Sat–Sun) at %s", timeStr)
		}
		if dowNum, err := strconv.Atoi(dowStr); err == nil && dowNum >= 0 && dowNum <= 7 {
			return fmt.Sprintf("Every %s at %s", cronDayNames[dowNum], timeStr)
		}
		var names []string
		for _, p := range strings.Split(dowStr, ",") {
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
	if domStr != "*" && monStr == "*" && dowStr == "*" {
		if domNum, err := strconv.Atoi(domStr); err == nil && domNum >= 1 && domNum <= 31 {
			return fmt.Sprintf("%s of every month at %s", Ordinal(domNum), timeStr)
		}
	}
	if domStr != "*" && monStr != "*" && dowStr == "*" {
		domNum, errDom := strconv.Atoi(domStr)
		monNum, errMon := strconv.Atoi(monStr)
		if errDom == nil && errMon == nil && monNum >= 1 && monNum <= 12 {
			return fmt.Sprintf("Every year on %s %s at %s", cronMonthNames[monNum], Ordinal(domNum), timeStr)
		}
	}
	return fmt.Sprintf("At %s (cron: %s)", timeStr, expr)
}

// DeriveThreadTitle extracts a clean, readable thread title from a message content string.
func DeriveThreadTitle(content string) string {
	cleaned := strings.TrimSpace(mentionAndEmojiRegex.ReplaceAllString(content, ""))
	var firstLine string
	for _, line := range strings.Split(cleaned, "\n") {
		if trimmed := strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "#> \t")); trimmed != "" {
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
			switch cut[len(cut)-1] {
			case '\u200D', '\u200B', '\uFEFF':
				cut = cut[:len(cut)-1]
				continue
			}
			break
		}
		return string(cut) + "..."
	}
	return string(runes)
}

// IsThreadAlreadyExistsError returns true if the error indicates a Discord thread already exists for a message (code 160004).
func IsThreadAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	var restErr *discordgo.RESTError
	if !errors.As(err, &restErr) {
		var restErrVal discordgo.RESTError
		if errors.As(err, &restErrVal) {
			restErr = &restErrVal
		}
	}
	if restErr != nil {
		if restErr.Message != nil && restErr.Message.Code == discordErrCodeThreadAlreadyCreated {
			return true
		}
		bodyStr := strings.ToLower(string(restErr.ResponseBody))
		return strings.Contains(bodyStr, "160004") || strings.Contains(bodyStr, "already been created")
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

// buildSyncLLM adapts a runner.LLMFunc into a transcript.LLMClientFunc using the preferred low-effort model.
func buildSyncLLM(rawLLM runner.LLMFunc, lowEffortModel, defaultModel string) transcript.LLMClientFunc {
	if rawLLM == nil {
		return nil
	}
	model := strings.TrimSpace(lowEffortModel)
	if model == "" {
		model = strings.TrimSpace(defaultModel)
	}
	return func(ctx context.Context, prompt string) (string, error) {
		return rawLLM(ctx, model, prompt)
	}
}
