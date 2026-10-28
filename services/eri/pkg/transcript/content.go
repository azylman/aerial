package transcript

import (
	"strings"
)

const (
	// DefaultHeadLimit is the character limit for the beginning of oversized step content.
	DefaultHeadLimit = 15000
	// DefaultTailLimit is the character limit for the end of oversized step content.
	DefaultTailLimit = 35000
	// DefaultSummaryMaxRunes is the character limit for session summaries in search output.
	DefaultSummaryMaxRunes = 400
	// TruncationMarker indicates omitted middle content in oversized steps.
	TruncationMarker = "\n... [truncated] ...\n"
)

var promptBlockTags = [][2]string{
	{"<retrieved_memory>", "</retrieved_memory>"},
	{"<CHANNEL_INSTRUCTIONS>", "</CHANNEL_INSTRUCTIONS>"},
	{"<CHANNEL_HISTORY>", "</CHANNEL_HISTORY>"},
	{"<ambient_context>", "</ambient_context>"},
	{"<ADDITIONAL_METADATA>", "</ADDITIONAL_METADATA>"},
	{"<USER_SETTINGS_CHANGE>", "</USER_SETTINGS_CHANGE>"},
}

// CleanPromptEnvelope strips injected runtime metadata blocks and wrapper tags from prompts.
func CleanPromptEnvelope(raw string) string {
	s := raw
	for _, pair := range promptBlockTags {
		for {
			start := strings.Index(s, pair[0])
			if start == -1 {
				break
			}
			end := strings.Index(s[start:], pair[1])
			if end == -1 {
				break
			}
			s = s[:start] + s[start+end+len(pair[1]):]
		}
	}
	if lastReq := strings.LastIndex(s, "<USER_REQUEST>"); lastReq != -1 {
		s = s[lastReq+len("<USER_REQUEST>"):]
	}
	s = strings.ReplaceAll(s, "</USER_REQUEST>", "")
	if idx := strings.Index(s, "Only formulate and output your final response once all immediate work is complete."); idx != -1 {
		s = s[:idx]
	}
	cleaned := strings.TrimSpace(s)
	if cleaned == "" {
		return strings.TrimSpace(raw)
	}
	return cleaned
}

// CleanSessionSummary strips runtime prompt envelopes and bounds summary length to maxRunes.
func CleanSessionSummary(raw string, maxRunes int) string {
	if maxRunes <= 0 {
		maxRunes = DefaultSummaryMaxRunes
	}
	cleaned := CleanPromptEnvelope(raw)
	runes := []rune(cleaned)
	if len(runes) <= maxRunes {
		return cleaned
	}
	return strings.TrimSpace(string(runes[:maxRunes])) + "... [truncated]"
}

// ClipContent preserves the head and tail of long content while omitting the repetitive middle.
// If headLimit and tailLimit are both <= 0, DefaultHeadLimit and DefaultTailLimit are used.
func ClipContent(content string, headLimit, tailLimit int) string {
	if headLimit <= 0 && tailLimit <= 0 {
		headLimit = DefaultHeadLimit
		tailLimit = DefaultTailLimit
	} else {
		if headLimit < 0 {
			headLimit = 0
		}
		if tailLimit < 0 {
			tailLimit = 0
		}
	}

	runes := []rune(content)
	totalLimit := headLimit + tailLimit
	if len(runes) <= totalLimit {
		return content
	}

	head := string(runes[:headLimit])
	tail := string(runes[len(runes)-tailLimit:])
	return head + TruncationMarker + tail
}

// IsSubstantiveStep returns false for trivial polling (e.g. manage_task status),
// empty outputs, or heartbeat steps.
func IsSubstantiveStep(stepType, toolName, content string) bool {
	trimmedContent := strings.TrimSpace(content)
	if trimmedContent == "" {
		return false
	}

	trimmedType := strings.ToLower(strings.TrimSpace(stepType))
	trimmedTool := strings.ToLower(strings.TrimSpace(toolName))

	// Heartbeat step detection
	if trimmedType == "heartbeat" || trimmedTool == "heartbeat" {
		return false
	}
	if strings.Contains(strings.ToLower(trimmedContent), "heartbeat") && len(trimmedContent) < 80 {
		return false
	}

	// Trivial polling detection (e.g. manage_task status)
	lowerContent := strings.ToLower(trimmedContent)
	if trimmedTool == "manage_task status" {
		return false
	}
	if trimmedTool == "manage_task" {
		if strings.Contains(lowerContent, `"action":"status"`) ||
			strings.Contains(lowerContent, `"action": "status"`) ||
			(strings.Contains(lowerContent, `"status"`) && len(trimmedContent) < 250) {
			return false
		}
	}
	if strings.Contains(lowerContent, "manage_task") && strings.Contains(lowerContent, "status") && len(trimmedContent) < 250 {
		return false
	}

	return true
}
