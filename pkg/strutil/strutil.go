// Package strutil holds small string helpers shared across Aerial services.
package strutil

import "strings"

// FirstNonEmpty returns the first value that is non-empty after trimming
// whitespace, trimmed. It returns "" when every value is blank.
func FirstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// TruncateRunes returns s unchanged when it has at most maxRunes runes;
// otherwise it returns the first maxRunes runes followed by "... (truncated)".
func TruncateRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "... (truncated)"
}
