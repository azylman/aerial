package interactions

import (
	"fmt"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
)

func getBoolOption(options []*discordgo.ApplicationCommandInteractionDataOption, name string, defaultVal bool) bool {
	for _, opt := range options {
		if opt != nil && opt.Name == name {
			return opt.BoolValue()
		}
	}
	return defaultVal
}

func getStringOption(options []*discordgo.ApplicationCommandInteractionDataOption, name string, defaultVal string) string {
	for _, opt := range options {
		if opt != nil && opt.Name == name {
			return strings.TrimSpace(opt.StringValue())
		}
	}
	return defaultVal
}

func getChannelOption(options []*discordgo.ApplicationCommandInteractionDataOption, name string, defaultVal string) string {
	for _, opt := range options {
		if opt != nil && opt.Name == name {
			if ch := opt.ChannelValue(nil); ch != nil && ch.ID != "" {
				return ch.ID
			}
			if opt.Value != nil {
				return fmt.Sprintf("%v", opt.Value)
			}
		}
	}
	return defaultVal
}

func hasOption(options []*discordgo.ApplicationCommandInteractionDataOption, name string) bool {
	for _, opt := range options {
		if opt != nil && opt.Name == name {
			return true
		}
	}
	return false
}

func formatRelativeDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	return fmt.Sprintf("%dd %dh", days, hours)
}

func formatRelativeTime(d time.Duration) string {
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func truncateRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "..."
}
