package sanitizer

import (
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// GitHub Tokens (classic, fine-grained, OAuth, user/server tokens)
	reGitHub = regexp.MustCompile(`(?i)\b(?:gh[pousr]_[a-zA-Z0-9_]+|github_pat_[a-zA-Z0-9_]+)`)

	// Google / Gemini / Antigravity API keys
	reGoogleAndAgy = regexp.MustCompile(`(?i)\b(?:AIza[0-9A-Za-z-_]{35,}|(?:antigravity|gemini)_[a-zA-Z0-9_\-]{16,})`)

	// OpenAI & Anthropic API Keys (sk-, sk-proj-, sk-ant-, sk-svcacct-)
	reOpenAIAndAnthropic = regexp.MustCompile(`\bsk-(?:proj-|ant-|svcacct-)?[a-zA-Z0-9_-]{16,}`)

	// Discord Bot, MFA & Webhook Tokens
	reDiscord        = regexp.MustCompile(`\b(?:mfa\.[a-zA-Z0-9_-]{20,}|[a-zA-Z0-9_-]{24,28}\.[a-zA-Z0-9_-]{6}\.[a-zA-Z0-9_-]{27,38})`)
	reDiscordWebhook = regexp.MustCompile(`(?i)https://(?:canary\.|ptb\.)?discord(?:app)?\.com/api/webhooks/\d+/[a-zA-Z0-9_-]+`)

	// JSON Web Tokens (JWTs) and Home Assistant Long-Lived Access Tokens
	reJWT = regexp.MustCompile(`\beyJ[a-zA-Z0-9_-]{10,}\.eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}`)

	// HTTP Basic & Bearer Auth Headers and x-access-token
	reHTTPAuth = regexp.MustCompile(`(?i)\b(?:basic\s+[a-zA-Z0-9+/]{8,}={1,2}|basic\s+[a-zA-Z0-9+/]{10,}|basic\s+[a-zA-Z0-9+/=]+|bearer\s+[a-zA-Z0-9_\-\.]{12,})`)
	reXAccess  = regexp.MustCompile(`(?i)x-access-token:[^@\s]+`)

	// Universal Database & Network URIs with embedded credentials (non-greedy password capture)
	reURIWithPassword = regexp.MustCompile(`(?i)([a-zA-Z][a-zA-Z0-9+.-]+://[^:\s/@]*:)([^@\s]+)(@[a-zA-Z0-9_.-]+(?::[0-9]+)?(?:/[^\s]*)?)`)

	// Key-Value Configuration / CLI assignments (preserves label)
	reKeyValue = regexp.MustCompile(`(?i)\b(GEMINI_HARNESS_API_KEY|AERIAL_GEMINI_API_KEY|AGY_GEMINI_API_KEY|GEMINI_API_KEY|ANTIGRAVITY_API_KEY|DISCORD_BOT_TOKEN|DISCORD_TOKEN|GITHUB_PAT|HA_TOKEN|OPENAI_API_KEY|ANTHROPIC_API_KEY|POSTGRES_PASSWORD|GRAFANA_ADMIN_PASSWORD|DATABASE_URL|PASSWORD|SECRET|TOKEN|KEY)\s*([:=]\s*)([^\s,;]+)`)

	// Extended secret key-value assignments
	reSecretKeyValue = regexp.MustCompile(`(?i)\b([A-Za-z0-9_-]*(?:KEY|SECRET|TOKEN|PAT|PASSWORD|DATABASE_URL))\s*([:=]\s*)([^\s,;]+)`)

	// Discord Mentions (user, role, channel, broadcast)
	reDiscordMention = regexp.MustCompile(`<@[!&]?[0-9]+>|<#[0-9]+>|@everyone|@here`)

	// Prompt Delimiter Tags (case-insensitive opening, closing, and self-closing system prompt tags)
	rePromptTag = regexp.MustCompile(`(?i)<\s*(/)?\s*(channel_history|user_request|channel_instructions|raw_thread_transcript|thread_summary|previous_session|previous_turn_actions|target_message|target_burst|coordination_context|retrieved_memory|heartbeat_instructions)\s*(/)?\s*>`)
)

var (
	sensitiveTokensMu  sync.Mutex
	sensitiveTokensPtr atomic.Pointer[[]string]
)

func init() {
	empty := make([]string, 0)
	sensitiveTokensPtr.Store(&empty)
}

func getSensitiveTokens() []string {
	p := sensitiveTokensPtr.Load()
	if p == nil {
		return nil
	}
	return *p
}

// ResetSensitiveTokens clears all registered sensitive tokens (primarily for test isolation).
func ResetSensitiveTokens() {
	sensitiveTokensMu.Lock()
	defer sensitiveTokensMu.Unlock()
	empty := make([]string, 0)
	sensitiveTokensPtr.Store(&empty)
}

// RegisterSensitiveTokens safely registers secret strings into the global redaction pool.
// Uses a copy-on-write atomic swap so reads remain completely lock-free.
func RegisterSensitiveTokens(tokens ...string) {
	sensitiveTokensMu.Lock()
	defer sensitiveTokensMu.Unlock()

	current := getSensitiveTokens()
	seen := make(map[string]bool, len(current)+len(tokens))
	for _, tok := range current {
		seen[tok] = true
	}

	updated := make([]string, len(current), len(current)+len(tokens))
	copy(updated, current)

	for _, tok := range tokens {
		trimmed := strings.TrimSpace(tok)
		if len(trimmed) >= 4 && !seen[trimmed] {
			seen[trimmed] = true
			updated = append(updated, trimmed)
		}
	}

	sort.Slice(updated, func(i, j int) bool {
		return len(updated[i]) > len(updated[j])
	})

	sensitiveTokensPtr.Store(&updated)
}

// sanitizeInternal executes the unified sanitization pipeline with a designated replacement placeholder.
func sanitizeInternal(input string, placeholder string) string {
	if input == "" {
		return ""
	}

	out := input

	// 1. Redact registered sensitive tokens
	sensitiveTokens := getSensitiveTokens()
	for _, secret := range sensitiveTokens {
		if strings.Contains(out, secret) {
			out = strings.ReplaceAll(out, secret, placeholder)
		}
	}

	// 2. Redact Discord Webhook URLs
	out = reDiscordWebhook.ReplaceAllString(out, placeholder)

	// 3. Redact x-access-token URL authentication
	out = reXAccess.ReplaceAllString(out, placeholder)

	// 4. Redact multi-protocol database and network URIs with embedded passwords ($1[REDACTED]$3)
	out = reURIWithPassword.ReplaceAllString(out, "${1}"+placeholder+"${3}")

	// 5. Redact key-value configuration assignments ($1$2[REDACTED])
	out = reSecretKeyValue.ReplaceAllString(out, "${1}${2}"+placeholder)
	out = reKeyValue.ReplaceAllString(out, "${1}${2}"+placeholder)

	// 6. Redact specialized token patterns
	out = reGitHub.ReplaceAllString(out, placeholder)
	out = reGoogleAndAgy.ReplaceAllString(out, placeholder)
	out = reOpenAIAndAnthropic.ReplaceAllString(out, placeholder)
	out = reDiscord.ReplaceAllString(out, placeholder)
	out = reJWT.ReplaceAllString(out, placeholder)
	out = reHTTPAuth.ReplaceAllString(out, placeholder)

	return out
}

// SanitizeString scrubs sensitive tokens, PATs, passwords, and credentials from text,
// replacing them with "[REDACTED]".
func SanitizeString(input string) string {
	return sanitizeInternal(input, "[REDACTED]")
}

// SanitizeLog scrubs sensitive tokens and credentials from logs, error traces, and subprocess output,
// replacing them with "[REDACTED_TOKEN]".
func SanitizeLog(input string) string {
	return sanitizeInternal(input, "[REDACTED_TOKEN]")
}

// SanitizeMentions strips Discord user mentions (<@123>, <@!123>), role mentions (<@&123>),
// channel mentions (<#123>), and broadcast mentions (@everyone, @here).
func SanitizeMentions(input string) string {
	if input == "" {
		return ""
	}
	return reDiscordMention.ReplaceAllString(input, "")
}

// NormalizeWhitespace collapses consecutive whitespace runes (spaces, tabs, newlines)
// into a single space and trims leading and trailing whitespace.
func NormalizeWhitespace(input string) string {
	if input == "" {
		return ""
	}
	return strings.Join(strings.Fields(input), " ")
}

// SanitizePromptTags escapes XML prompt tags to prevent prompt injection or envelope breakouts.
// Closing (or self-closing) tags are escaped to <\/TAG> and opening tags to <\TAG>.
func SanitizePromptTags(input string) string {
	if input == "" {
		return ""
	}
	return rePromptTag.ReplaceAllStringFunc(input, func(m string) string {
		tagUpper := strings.ToUpper(strings.Trim(m, "<>/ \t\r\n"))
		if strings.Contains(m, "/") {
			return "<\\/" + tagUpper + ">"
		}
		return "<\\" + tagUpper + ">"
	})
}

// SanitizeAuthor cleans untrusted author strings by stripping newlines, carriage returns,
// and angle brackets, and trimming whitespace.
func SanitizeAuthor(name string) string {
	if name == "" {
		return ""
	}
	name = strings.ReplaceAll(name, "\n", "")
	name = strings.ReplaceAll(name, "\r", "")
	name = strings.ReplaceAll(name, "<", "")
	name = strings.ReplaceAll(name, ">", "")
	return strings.TrimSpace(name)
}
