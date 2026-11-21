package sanitizer

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// Discord Mentions (user, role, channel, broadcast)
	reDiscordMention = regexp.MustCompile(`<@[!&]?\d+>|<#\d+>|@everyone|@here`)

	// Prompt Delimiter Tags (case-insensitive opening, closing, and self-closing system prompt tags)
	rePromptTag = regexp.MustCompile(`(?i)<\s*(/)?\s*(channel_history|user_request|channel_instructions|raw_thread_transcript|thread_summary|previous_session|previous_turn_actions|target_message|target_burst|coordination_context|retrieved_memory|heartbeat_instructions)\s*(/)?\s*>`)

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

	// x-access-token URL authentication
	reXAccess = regexp.MustCompile(`(?i)x-access-token:[^@\s]+`)
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

// ResetSensitiveTokensForTest clears registered sensitive tokens under lock. Strictly for testing.
func ResetSensitiveTokensForTest() {
	sensitiveTokensMu.Lock()
	defer sensitiveTokensMu.Unlock()
	empty := make([]string, 0)
	sensitiveTokensPtr.Store(&empty)
}

// MaskTokenSample masks a token string for safe diagnostic display.
// Whitespace is trimmed; tokens shorter than 6 characters return "[REDACTED]",
// while longer tokens preserve the first 2 and last 2 characters surrounding "***".
func MaskTokenSample(tok string) string {
	trimmed := strings.TrimSpace(tok)
	if len(trimmed) < 6 {
		return "[REDACTED]"
	}
	return trimmed[:2] + "***" + trimmed[len(trimmed)-2:]
}

// MatchSensitiveTokens returns masked diagnostic samples of any registered sensitive tokens
// (including their Base64 and URL-escaped variants) found in input, deduplicated and ordered longest-first.
// Returns nil if input is empty or no tokens match.
func MatchSensitiveTokens(input string) []string {
	if input == "" {
		return nil
	}
	tokens := getSensitiveTokens()
	if len(tokens) == 0 {
		return nil
	}
	var matches []string
	seen := make(map[string]bool)
	remaining := input
	for _, tok := range tokens {
		if tok != "" && strings.Contains(remaining, tok) {
			remaining = strings.ReplaceAll(remaining, tok, "")
			sample := MaskTokenSample(tok)
			if !seen[sample] {
				seen[sample] = true
				matches = append(matches, sample)
			}
		}
	}
	return matches
}

func appendSingleTokenVariants(out []string, tok string) []string {
	b := []byte(tok)
	out = append(out,
		tok,
		base64.StdEncoding.EncodeToString(b),
		base64.RawStdEncoding.EncodeToString(b),
		base64.URLEncoding.EncodeToString(b),
		base64.RawURLEncoding.EncodeToString(b),
	)
	gitBasic := []byte("x-access-token:" + tok)
	out = append(out,
		base64.StdEncoding.EncodeToString(gitBasic),
		base64.RawStdEncoding.EncodeToString(gitBasic),
	)
	if escaped := url.QueryEscape(tok); len(escaped) >= 4 && escaped != tok {
		out = append(out, escaped)
	}
	if uInfo := url.UserPassword("", tok); uInfo != nil {
		if uPass := strings.TrimPrefix(uInfo.String(), ":"); len(uPass) >= 4 && uPass != tok && uPass != url.QueryEscape(tok) {
			out = append(out, uPass)
		}
	}
	return out
}

func extractEmbeddedURIPassword(tok string) string {
	if !strings.Contains(tok, "://") {
		return ""
	}
	u, err := url.Parse(tok)
	if err != nil || u.User == nil {
		return ""
	}
	pass, ok := u.User.Password()
	if !ok {
		return ""
	}
	return strings.TrimSpace(pass)
}

func expandTokenVariants(tok string) []string {
	variants := appendSingleTokenVariants(nil, tok)
	if pass := extractEmbeddedURIPassword(tok); len(pass) >= 4 {
		variants = appendSingleTokenVariants(variants, pass)
	}
	return variants
}

func buildSensitiveTokenSlice(existing, tokens []string) []string {
	seen := make(map[string]bool, len(existing)+len(tokens)*8)
	for _, tok := range existing {
		seen[tok] = true
	}

	updated := make([]string, len(existing), len(existing)+len(tokens)*8)
	copy(updated, existing)

	for _, tok := range tokens {
		trimmed := strings.TrimSpace(tok)
		if len(trimmed) < 4 {
			continue
		}
		for _, variant := range expandTokenVariants(trimmed) {
			if len(variant) >= 4 && !seen[variant] {
				seen[variant] = true
				updated = append(updated, variant)
			}
		}
	}

	sort.Slice(updated, func(i, j int) bool {
		return len(updated[i]) > len(updated[j])
	})

	return updated
}

// ReplaceSensitiveTokens atomically replaces the global sensitive token pool with the provided tokens
// and their Base64/URL-encoded variants. Uses a copy-on-write atomic swap so reads remain completely lock-free.
func ReplaceSensitiveTokens(tokens ...string) {
	sensitiveTokensMu.Lock()
	defer sensitiveTokensMu.Unlock()

	updated := buildSensitiveTokenSlice(nil, tokens)
	sensitiveTokensPtr.Store(&updated)
}

// DefaultSharedSecretsPath is the canonical in-container mount point for shared secrets.
var DefaultSharedSecretsPath = "/secrets"

func loadSharedSecretsDir(dirPath string, dst map[string]string) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") || entry.IsDir() {
			continue
		}
		filePath := filepath.Join(dirPath, name)
		info, err := os.Stat(filePath)
		if err != nil || info.IsDir() {
			continue
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}
		trimmed := strings.TrimSpace(string(data))
		if trimmed != "" {
			dst[name] = trimmed
		}
	}
}

// LoadSharedSecrets reads shared secrets from path, which can be either a Kubernetes
// secret directory mount (e.g. /secrets) or a JSON file (e.g. /secrets/shared-secrets.json).
// If path is empty, missing, or invalid, an empty map is returned.
// Values are trimmed, and alias fallbacks are populated when the primary key is empty.
func LoadSharedSecrets(path string) map[string]string {
	result := make(map[string]string)
	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return result
	}
	fi, err := os.Stat(cleanPath)
	if err != nil {
		return result
	}

	if fi.IsDir() {
		loadSharedSecretsDir(cleanPath, result)
	} else {
		data, err := os.ReadFile(cleanPath)
		if err != nil {
			return result
		}
		var raw map[string]string
		if err := json.Unmarshal(data, &raw); err != nil {
			return result
		}
		for k, v := range raw {
			result[k] = strings.TrimSpace(v)
		}
	}

	aliases := [][2]string{
		{"DISCORD_TOKEN", "DISCORD_BOT_TOKEN"},
		{"OP_SERVICE_ACCOUNT_TOKEN", "1PASSWORD_SERVICE_ACCOUNT_TOKEN"},
		{"UNIFI_PASSWORD", "UNIFI_PASS"},
	}
	for _, pair := range aliases {
		primary, fallback := pair[0], pair[1]
		if result[primary] == "" && result[fallback] != "" {
			result[primary] = result[fallback]
		}
	}
	return result
}

// RegisterSharedSecrets loads secrets from a Kubernetes secrets directory or JSON file
// and registers their values into the sensitive token pool.
func RegisterSharedSecrets(path string) {
	shared := LoadSharedSecrets(path)
	if len(shared) == 0 {
		return
	}
	keys := make([]string, 0, len(shared))
	for k := range shared {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	tokens := make([]string, 0, len(shared))
	for _, k := range keys {
		if v := strings.TrimSpace(shared[k]); v != "" {
			tokens = append(tokens, v)
		}
	}
	if len(tokens) > 0 {
		RegisterSensitiveTokens(tokens...)
	}
}

// RegisterSensitiveTokens safely registers secret strings and their Base64/URL-encoded variants
// into the global redaction pool. Uses a copy-on-write atomic swap so reads remain completely lock-free.
func RegisterSensitiveTokens(tokens ...string) {
	sensitiveTokensMu.Lock()
	defer sensitiveTokensMu.Unlock()

	updated := buildSensitiveTokenSlice(getSensitiveTokens(), tokens)
	sensitiveTokensPtr.Store(&updated)
}

// sanitizeInternal executes the unified sanitization pipeline with a designated replacement placeholder.
func sanitizeInternal(input string, placeholder string) string {
	if input == "" {
		return ""
	}

	out := input
	for _, secret := range getSensitiveTokens() {
		if strings.Contains(out, secret) {
			out = strings.ReplaceAll(out, secret, placeholder)
		}
	}

	out = reDiscordWebhook.ReplaceAllString(out, placeholder)
	out = reXAccess.ReplaceAllString(out, placeholder)
	out = reGitHub.ReplaceAllString(out, placeholder)
	out = reGoogleAndAgy.ReplaceAllString(out, placeholder)
	out = reOpenAIAndAnthropic.ReplaceAllString(out, placeholder)
	out = reDiscord.ReplaceAllString(out, placeholder)
	out = reJWT.ReplaceAllString(out, placeholder)

	return out
}

// SanitizeString scrubs registered sensitive tokens and their encoded variants from text,
// replacing them with "[REDACTED]".
func SanitizeString(input string) string {
	return sanitizeInternal(input, "[REDACTED]")
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

// matchNothingPattern is an RE2 pattern that matches no input, including the empty string.
const matchNothingPattern = `[^\s\S]`

// ValuePattern returns an RE2 alternation matching every registered-style variant of values
// (raw, Base64 alignments, URL-encoded, git basic auth) as literal text. Values and variants
// shorter than the sanitizer's minimum token length are skipped, duplicates are dropped, and
// alternatives are ordered longest first so the longest variant wins at each position.
// With no usable values it returns a pattern that matches nothing.
func ValuePattern(values []string) string {
	variants := buildSensitiveTokenSlice(nil, values)
	if len(variants) == 0 {
		return matchNothingPattern
	}
	sort.SliceStable(variants, func(i, j int) bool {
		if len(variants[i]) != len(variants[j]) {
			return len(variants[i]) > len(variants[j])
		}
		return variants[i] < variants[j]
	})
	quoted := make([]string, len(variants))
	for i, v := range variants {
		quoted[i] = regexp.QuoteMeta(v)
	}
	return strings.Join(quoted, "|")
}

// OTTLStringBody escapes backslashes and double quotes so pattern can be embedded
// inside an OTTL double-quoted string literal.
func OTTLStringBody(pattern string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(pattern)
}
