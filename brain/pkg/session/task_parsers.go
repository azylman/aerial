package session

import (
	"strings"
)

func foldByte(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - 32
	}
	if b >= 'A' && b <= 'Z' {
		return b + 32
	}
	return b
}

// indexFold finds the first case-insensitive match of substr in s
// without allocating heap memory or copying s.
func indexFold(s, substr string) int {
	n := len(substr)
	if n == 0 {
		return 0
	}
	if len(s) < n {
		return -1
	}
	b0 := substr[0]
	b0Alt := foldByte(b0)

	for i := 0; i <= len(s)-n; i++ {
		c := s[i]
		if (c == b0 || c == b0Alt) && strings.EqualFold(s[i:i+n], substr) {
			return i
		}
	}
	return -1
}

// lastIndexFold finds the last case-insensitive match of substr in s
// without allocating heap memory or copying s.
func lastIndexFold(s, substr string) int {
	n := len(substr)
	if n == 0 {
		return len(s)
	}
	if len(s) < n {
		return -1
	}
	b0 := substr[0]
	b0Alt := foldByte(b0)

	for i := len(s) - n; i >= 0; i-- {
		c := s[i]
		if (c == b0 || c == b0Alt) && strings.EqualFold(s[i:i+n], substr) {
			return i
		}
	}
	return -1
}

func parseTokenAfterMarker(s, marker string) string {
	idx := indexFold(s, marker)
	if idx == -1 {
		return ""
	}

	rest := strings.TrimSpace(s[idx+len(marker):])
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return ""
	}

	token := strings.Trim(fields[0], `"'\,;.:`)
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	return strings.Clone(token)
}

// ParseBackgroundTaskStarted extracts the background task ID from tool execution output.
// It searches for the case-insensitive phrase "tool is running as a background task with task id:"
// and extracts the clean single-word task token, cloned to avoid retaining large string buffers.
func ParseBackgroundTaskStarted(s string) string {
	return parseTokenAfterMarker(s, "tool is running as a background task with task id:")
}

// ParseTaskMessageSender extracts the sender ID from a background notification message header.
// It searches for the case-insensitive attribute "sender=" and extracts the clean single-word token,
// cloned to prevent memory retention of large message payloads.
func ParseTaskMessageSender(s string) string {
	return parseTokenAfterMarker(s, "sender=")
}

// ParseTaskFinishedContent extracts the completed task ID from a completion notice.
// It finds the completion marker "finished with result:" and identifies the closest
// preceding "task id" marker, ensuring the candidate between them is a single non-whitespace
// token (rejecting false positives with interior spaces or multiple tasks in the text).
func ParseTaskFinishedContent(s string) string {
	const startMarker = "task id"
	const endMarker = "finished with result:"

	endIdx := indexFold(s, endMarker)
	if endIdx == -1 {
		return ""
	}

	startIdx := lastIndexFold(s[:endIdx], startMarker)
	if startIdx == -1 {
		return ""
	}

	between := s[startIdx+len(startMarker) : endIdx]
	fields := strings.Fields(between)
	if len(fields) != 1 {
		return ""
	}

	token := strings.Trim(fields[0], `"'\,;.: `)
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	return strings.Clone(token)
}
