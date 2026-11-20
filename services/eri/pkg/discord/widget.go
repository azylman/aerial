package discord

import (
	"strings"

	"github.com/bwmarrin/discordgo"
)

// ExtractMessageComponentsText extracts readable markdown text and button links from Discord Components V2 components.
func ExtractMessageComponentsText(components []discordgo.MessageComponent) string {
	var lines []string
	for _, comp := range components {
		if comp == nil {
			continue
		}
		if text := extractSingleComponentText(comp); text != "" {
			lines = append(lines, text)
		}
	}
	return strings.Join(lines, "\n")
}

func extractSingleComponentText(comp discordgo.MessageComponent) string {
	if comp == nil {
		return ""
	}
	switch c := comp.(type) {
	case *discordgo.TextDisplay:
		if c == nil {
			return ""
		}
		return strings.TrimSpace(c.Content)
	case discordgo.TextDisplay:
		return strings.TrimSpace(c.Content)
	case *discordgo.Button:
		if c == nil {
			return ""
		}
		return formatButtonText(c.Label, c.URL)
	case discordgo.Button:
		return formatButtonText(c.Label, c.URL)
	case *discordgo.Section:
		if c == nil {
			return ""
		}
		return extractSectionText(c.Components, c.Accessory)
	case discordgo.Section:
		return extractSectionText(c.Components, c.Accessory)
	case *discordgo.Container:
		if c == nil {
			return ""
		}
		return ExtractMessageComponentsText(c.Components)
	case discordgo.Container:
		return ExtractMessageComponentsText(c.Components)
	case *discordgo.ActionsRow:
		if c == nil {
			return ""
		}
		return ExtractMessageComponentsText(c.Components)
	case discordgo.ActionsRow:
		return ExtractMessageComponentsText(c.Components)
	default:
		return ""
	}
}

func formatButtonText(label, url string) string {
	label = strings.TrimSpace(label)
	url = strings.TrimSpace(url)
	switch {
	case label != "" && url != "":
		return "[" + label + "](" + url + ")"
	case label != "":
		return "[" + label + "]"
	case url != "":
		return url
	default:
		return ""
	}
}

func extractSectionText(components []discordgo.MessageComponent, accessory discordgo.MessageComponent) string {
	childText := ExtractMessageComponentsText(components)
	var btnLabel, btnURL string
	hasButton := false
	if accessory != nil {
		switch acc := accessory.(type) {
		case *discordgo.Button:
			if acc != nil {
				btnLabel = strings.TrimSpace(acc.Label)
				btnURL = strings.TrimSpace(acc.URL)
				hasButton = true
			}
		case discordgo.Button:
			btnLabel = strings.TrimSpace(acc.Label)
			btnURL = strings.TrimSpace(acc.URL)
			hasButton = true
		}
	}
	var btnSuffix string
	if hasButton {
		switch {
		case btnLabel != "" && btnURL != "":
			btnSuffix = " [" + btnLabel + "](" + btnURL + ")"
		case btnLabel != "":
			btnSuffix = " [" + btnLabel + "]"
		case btnURL != "":
			btnSuffix = " " + btnURL
		}
	}
	if childText != "" {
		return childText + btnSuffix
	}
	return strings.TrimSpace(btnSuffix)
}
