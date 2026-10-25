// Package sanitizer provides high-performance, centralized secret, token,
// credential, and environment variable redaction utilities for Aerial.
package sanitizer

import (
	"net/url"

	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

// RegisterSensitiveTokens safely registers secret strings into the global redaction pool.
func RegisterSensitiveTokens(tokens ...string) {
	sanitizer.RegisterSensitiveTokens(tokens...)
}

// RegisterConfigTokens extracts secrets from the injected *config.Config and registers them.
func RegisterConfigTokens(cfg *config.Config) {
	if cfg == nil {
		return
	}
	cur := cfg.Current()
	if cur == nil {
		return
	}

	var tokens []string
	if cur.APIKey != "" {
		tokens = append(tokens, cur.APIKey)
	}
	if cur.HarnessAPIKey != "" {
		tokens = append(tokens, cur.HarnessAPIKey)
	}
	if cur.DiscordToken != "" {
		tokens = append(tokens, cur.DiscordToken)
	}
	if cur.GitHubPAT != "" {
		tokens = append(tokens, cur.GitHubPAT)
	}
	if cur.DatabaseURL != "" {
		if u, err := url.Parse(cur.DatabaseURL); err == nil && u.User != nil {
			if pass, ok := u.User.Password(); ok && pass != "" {
				tokens = append(tokens, pass)
			}
		}
	}

	RegisterSensitiveTokens(tokens...)
}

// SanitizeString scrubs sensitive tokens, PATs, passwords, and credentials from text,
// replacing them with "[REDACTED]". Ideal for user-facing prompts, tasks, and API payloads.
func SanitizeString(input string) string {
	return sanitizer.SanitizeString(input)
}

// SanitizeLog scrubs sensitive tokens and credentials from logs, error traces, and subprocess output,
// replacing them with "[REDACTED_TOKEN]".
func SanitizeLog(input string) string {
	return sanitizer.SanitizeLog(input)
}

// SanitizeMentions strips Discord user mentions (<@123>, <@!123>), role mentions (<@&123>),
// channel mentions (<#123>), and broad broadcast mentions (@everyone, @here).
func SanitizeMentions(input string) string {
	return sanitizer.SanitizeMentions(input)
}

// NormalizeWhitespace collapses consecutive whitespace runes (spaces, tabs, newlines)
// into a single space and trims leading and trailing whitespace.
func NormalizeWhitespace(input string) string {
	return sanitizer.NormalizeWhitespace(input)
}

// SanitizePromptTags escapes XML prompt tags to prevent prompt injection or envelope breakouts.
// Closing (or self-closing) tags are escaped to <\/TAG> and opening tags to <\TAG>.
func SanitizePromptTags(input string) string {
	return sanitizer.SanitizePromptTags(input)
}

// SanitizeAuthor cleans untrusted author strings by stripping newlines, carriage returns,
// and angle brackets, and trimming whitespace.
func SanitizeAuthor(name string) string {
	return sanitizer.SanitizeAuthor(name)
}
