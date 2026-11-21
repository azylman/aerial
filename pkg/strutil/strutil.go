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
