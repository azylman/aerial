package model

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	snowflakeRegex   = regexp.MustCompile(`^\d{17,20}$`)
	channelNameRegex = regexp.MustCompile(`^#?[a-z0-9_-]{1,100}$`)
	re4xx            = regexp.MustCompile(`\bstatus 4\d{2}\b`)
)

func RandomNonce() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("nonce-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// CleanRepoName strips any owner prefix ("owner/name" -> "name") for
// concise Discord notifications.
func CleanRepoName(repo string) string {
	repo = strings.TrimSpace(repo)
	if idx := strings.LastIndex(repo, "/"); idx >= 0 {
		return repo[idx+1:]
	}
	return repo
}

func DedupeTrimmedStrings(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	var out []string
	for _, item := range items {
		if clean := strings.TrimSpace(item); clean != "" {
			if _, exists := seen[clean]; !exists {
				seen[clean] = struct{}{}
				out = append(out, clean)
			}
		}
	}
	return out
}

// NormalizeDiscordTargetID strips Model 2 session prefixes ("discord-" and optional
// colon-delimited pool scopes like "stateful_medium:discord-") from a target identifier.
func NormalizeDiscordTargetID(id string) string {
	clean := strings.TrimSpace(id)
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

func IsValidDiscordSnowflake(id string) bool {
	return snowflakeRegex.MatchString(strings.TrimSpace(id))
}

func IsValidDiscordChannel(id string) bool {
	clean := strings.TrimSpace(id)
	return IsValidDiscordSnowflake(clean) || channelNameRegex.MatchString(clean)
}

// Is4xxClientError returns true if the error indicates a 4xx HTTP client error or invalid Discord snowflake.
func Is4xxClientError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return re4xx.MatchString(msg) || strings.Contains(msg, "invalid discord snowflake")
}
