// Package discordutil holds small Discord identifier helpers shared across services.
package discordutil

import "strings"

// IsSnowflake reports whether id is a non-empty string of ASCII digits.
// It does not trim; callers that accept padded input trim first.
func IsSnowflake(id string) bool {
	if id == "" {
		return false
	}
	for i := range len(id) {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
}

// NormalizeThreadID extracts a bare Discord thread ID from a possibly prefixed
// reference: it trims, keeps the text after the last ':', strips a "discord-"
// prefix and trims again. It does not validate the result.
func NormalizeThreadID(s string) string {
	clean := strings.TrimSpace(s)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

// IsHeadlessSessionKey returns true if id is a Model 2 headless dispatch, heartbeat,
// or pooled worker session key.
func IsHeadlessSessionKey(id string) bool {
	clean := strings.TrimSpace(id)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.HasPrefix(clean, "dispatch-") ||
		strings.HasPrefix(clean, "heartbeat-") ||
		strings.HasPrefix(clean, "worker-") ||
		strings.Contains(clean, "-worker-")
}
