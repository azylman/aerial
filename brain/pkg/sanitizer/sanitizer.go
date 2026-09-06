// Package sanitizer provides high-performance, centralized secret, token,
// credential, and environment variable redaction utilities for Aerial.
package sanitizer

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
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
	reHTTPAuth = regexp.MustCompile(`(?i)\b(?:basic\s+[a-zA-Z0-9+/]{8,}={1,2}|basic\s+[a-zA-Z0-9+/]{10,}|bearer\s+[a-zA-Z0-9_\-\.]{12,})`)
	reXAccess  = regexp.MustCompile(`(?i)x-access-token:[^@\s]+`)

	// Universal Database & Network URIs with embedded credentials (non-greedy password capture)
	reURIWithPassword = regexp.MustCompile(`(?i)([a-zA-Z][a-zA-Z0-9+.-]+://[^:\s/@]*:)([^@\s]+)(@[a-zA-Z0-9_.-]+(?::[0-9]+)?(?:/[^\s]*)?)`)

	// Key-Value Configuration / CLI assignments (preserves label)
	reKeyValue = regexp.MustCompile(`(?i)\b(GEMINI_API_KEY|ANTIGRAVITY_API_KEY|DISCORD_BOT_TOKEN|DISCORD_TOKEN|GITHUB_PAT|HA_TOKEN|OPENAI_API_KEY|ANTHROPIC_API_KEY|POSTGRES_PASSWORD|GRAFANA_ADMIN_PASSWORD|DATABASE_URL|PASSWORD|SECRET|TOKEN|KEY)\s*([:=]\s*)([^\s,;]+)`)
)

var (
	envSecretsCache []string
	envOnce         sync.Once
	envMu           sync.RWMutex

	// Known explicit sensitive environment variable keys
	explicitSensitiveEnvKeys = []string{
		"GEMINI_API_KEY",
		"ANTIGRAVITY_API_KEY",
		"DISCORD_BOT_TOKEN",
		"DISCORD_TOKEN",
		"GITHUB_PAT",
		"GITHUB_PERSONAL_ACCESS_TOKEN",
		"HA_TOKEN",
		"OPENAI_API_KEY",
		"ANTHROPIC_API_KEY",
		"DATABASE_URL",
		"POSTGRES_PASSWORD",
		"GRAFANA_ADMIN_PASSWORD",
	}

	// Common non-secret words to prevent false-positive over-redaction in dynamic scanning
	ignoredDynamicEnvValues = map[string]bool{
		"production":  true,
		"development": true,
		"staging":     true,
		"default":     true,
		"enabled":     true,
		"disabled":    true,
		"true":        true,
		"false":       true,
		"latest":      true,
		"bearer":      true,
		"oauth2":      true,
		"postgres":    true,
		"aerial":      true,
		"localhost":   true,
		"127.0.0.1":   true,
	}
)

// isSensitiveEnvKey strictly verifies whether an environment variable key represents a secret,
// preventing accidental collisions with system variables like PATH, COMPAT, PATTERN, etc.
func isSensitiveEnvKey(key string) bool {
	k := strings.ToUpper(strings.TrimSpace(key))
	if k == "" {
		return false
	}

	// Exact matches
	if k == "TOKEN" || k == "KEY" || k == "SECRET" || k == "PASSWORD" || k == "PAT" ||
		k == "AUTH" || k == "CREDENTIAL" || k == "CREDENTIALS" {
		return true
	}

	// Strict Suffix matches
	if strings.HasSuffix(k, "_TOKEN") || strings.HasSuffix(k, "_KEY") ||
		strings.HasSuffix(k, "_SECRET") || strings.HasSuffix(k, "_PASSWORD") ||
		strings.HasSuffix(k, "_PAT") || strings.HasSuffix(k, "_AUTH") ||
		strings.HasSuffix(k, "_CREDENTIAL") || strings.HasSuffix(k, "_CREDENTIALS") {
		return true
	}

	// Strict Prefix matches
	if strings.HasPrefix(k, "TOKEN_") || strings.HasPrefix(k, "KEY_") ||
		strings.HasPrefix(k, "SECRET_") || strings.HasPrefix(k, "PASSWORD_") ||
		strings.HasPrefix(k, "PAT_") || strings.HasPrefix(k, "AUTH_") {
		return true
	}

	// Strict delimited Substrings
	if strings.Contains(k, "_TOKEN_") || strings.Contains(k, "_KEY_") ||
		strings.Contains(k, "_SECRET_") || strings.Contains(k, "_PASSWORD_") ||
		strings.Contains(k, "_PAT_") || strings.Contains(k, "_AUTH_") {
		return true
	}

	return false
}

// buildEnvSecrets extracts and sorts sensitive values from the current environment.
func buildEnvSecrets() []string {
	seen := make(map[string]bool)
	var secrets []string

	// 1. Collect explicit known sensitive keys
	for _, k := range explicitSensitiveEnvKeys {
		val := strings.TrimSpace(os.Getenv(k))
		if len(val) >= 4 && !seen[val] {
			// Don't redact boolean true/false or generic mode names even if explicitly set
			lower := strings.ToLower(val)
			if lower != "true" && lower != "false" && lower != "enabled" && lower != "disabled" {
				seen[val] = true
				secrets = append(secrets, val)
			}
		}
	}

	// 2. Scan dynamic environment variables with strict key delimiter and length checks
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		val := strings.TrimSpace(parts[1])

		if len(val) < 8 || ignoredDynamicEnvValues[strings.ToLower(val)] || seen[val] {
			continue
		}

		if isSensitiveEnvKey(key) {
			seen[val] = true
			secrets = append(secrets, val)
		}
	}

	// Sort secrets in descending order of length to replace longer tokens before shorter substrings
	sort.Slice(secrets, func(i, j int) bool {
		return len(secrets[i]) > len(secrets[j])
	})

	return secrets
}

// getCachedEnvSecrets returns the thread-safe cached slice of active environment secrets.
func getCachedEnvSecrets() []string {
	envOnce.Do(func() {
		envSecretsCache = buildEnvSecrets()
	})

	envMu.RLock()
	defer envMu.RUnlock()
	return envSecretsCache
}

// RefreshEnvironmentSecrets recomputes the cached list of environment secrets.
// Useful when environment variables are dynamically updated during runtime or tests.
func RefreshEnvironmentSecrets() {
	envMu.Lock()
	defer envMu.Unlock()
	envSecretsCache = buildEnvSecrets()
}

// sanitizeInternal executes the unified sanitization pipeline with a designated replacement placeholder.
func sanitizeInternal(input string, placeholder string) string {
	if input == "" {
		return ""
	}

	out := input

	// 1. Redact cached environment secrets
	envSecrets := getCachedEnvSecrets()
	for _, secret := range envSecrets {
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
// replacing them with "[REDACTED]". Ideal for user-facing prompts, tasks, and API payloads.
func SanitizeString(input string) string {
	return sanitizeInternal(input, "[REDACTED]")
}

// SanitizeLog scrubs sensitive tokens and credentials from logs, error traces, and subprocess output,
// replacing them with "[REDACTED_TOKEN]".
func SanitizeLog(input string) string {
	return sanitizeInternal(input, "[REDACTED_TOKEN]")
}

// SanitizeEnvVars takes a slice of "KEY=VALUE" environment strings and redacts sensitive values.
// Strictly guards system variables like PATH, COMPAT, PATTERN against false-positive redaction.
func SanitizeEnvVars(envVars []string) []string {
	if envVars == nil {
		return nil
	}
	cleaned := make([]string, 0, len(envVars))

	for _, env := range envVars {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			cleaned = append(cleaned, env)
			continue
		}
		key := parts[0]

		if isSensitiveEnvKey(key) {
			cleaned = append(cleaned, fmt.Sprintf("%s=[REDACTED]", key))
		} else {
			cleaned = append(cleaned, env)
		}
	}

	return cleaned
}
