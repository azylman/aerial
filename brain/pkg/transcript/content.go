package transcript

import (
	"strings"
)

const (
	// DefaultHeadLimit is the character limit for the beginning of oversized step content.
	DefaultHeadLimit = 15000
	// DefaultTailLimit is the character limit for the end of oversized step content.
	DefaultTailLimit = 35000
	// TruncationMarker indicates omitted middle content in oversized steps.
	TruncationMarker = "\n... [truncated] ...\n"
)

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
