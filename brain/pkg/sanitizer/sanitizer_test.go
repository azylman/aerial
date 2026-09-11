package sanitizer

import (
	"strings"
	"testing"

	"github.com/azylman/aerial/brain/pkg/config"
)

func TestSanitizer_RegisterConfigTokens(t *testing.T) {
	ResetSensitiveTokens()
	cfg := config.NewFromData(&config.ConfigData{
		APIKey:       "gemini_secret_api_key_12345",
		DiscordToken: "discord_token_secret_abcdef",
		GitHubPAT:    "ghp_testpersonalaccesstoken123456",
		DatabaseURL:  "postgres://aerial:supersecretpass@localhost:5432/aerial",
	})
	RegisterConfigTokens(cfg)

	input := "Error calling Gemini with key gemini_secret_api_key_12345 and db pass supersecretpass"
	sanitized := SanitizeString(input)
	if strings.Contains(sanitized, "gemini_secret_api_key_12345") {
		t.Errorf("APIKey was not sanitized: %s", sanitized)
	}
	if strings.Contains(sanitized, "supersecretpass") {
		t.Errorf("Database password was not sanitized: %s", sanitized)
	}
}

func TestSanitizeString_Tokens(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "clean text without secrets",
			input:    "Hello world, this is a normal sentence about basic setup and risk-assessment.",
			expected: "Hello world, this is a normal sentence about basic setup and risk-assessment.",
		},
		{
			name:     "classic github pat",
			input:    "Error using token " + "ghp_" + "1234567890abcdefghijklmnopqrstuvwxyzAB" + " to access repo",
			expected: "Error using token [REDACTED] to access repo",
		},
		{
			name:     "short github pat fixture",
			input:    "Failed auth with token " + "ghp_" + "errorToken99999999",
			expected: "Failed auth with token [REDACTED]",
		},
		{
			name:     "fine-grained github pat",
			input:    "Pushing with " + "github_pat_" + "11AABCDEF0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" + " to origin",
			expected: "Pushing with [REDACTED] to origin",
		},
		{
			name:     "short github fine-grained pat fixture",
			input:    "Reminder with secret key " + "github_pat_" + "1234567890abcdef",
			expected: "Reminder with secret key [REDACTED]",
		},
		{
			name:     "github oauth tokens",
			input:    "Tokens " + "gho_" + "1234567890abcdefghijklmnopqrstuvwxyzAB" + " and " + "ghu_" + "1234567890abcdefghijklmnopqrstuvwxyzAB",
			expected: "Tokens [REDACTED] and [REDACTED]",
		},
		{
			name:     "short github oauth token fixtures",
			input:    "Tokens " + "gho_" + "OAuthSecret123" + " and " + "ghu_" + "UserSecret456",
			expected: "Tokens [REDACTED] and [REDACTED]",
		},
		{
			name:     "google api key",
			input:    "API key " + "AIza" + "SyD1234567890abcdefghijklmnopqrstuvw" + " configured",
			expected: "API key [REDACTED] configured",
		},
		{
			name:     "antigravity and gemini tokens",
			input:    "Using " + "antigravity_" + "secret_token_12345" + " and " + "gemini_" + "api_key_6789012345",
			expected: "Using [REDACTED] and [REDACTED]",
		},
		{
			name:     "openai and anthropic keys",
			input:    "Keys " + "sk-proj-" + "1234567890abcdefghijklmnop" + " and " + "sk-ant-api03-" + "1234567890abcdefghijklmnop",
			expected: "Keys [REDACTED] and [REDACTED]",
		},
		{
			name:     "token ending with hyphen",
			input:    "Bearer token " + "sk-proj-" + "1234567890abcdef-" + " and key " + "AIza" + "SyD1234567890abcdefghijklmnopqrstuv-" + " in trace",
			expected: "Bearer token [REDACTED] and key [REDACTED] in trace",
		},
		{
			name:     "discord bot token (modern 26-char base64 id)",
			input:    "Bot token " + "MTIzNDU2Nzg5MDEyMzQ1Njc4OQ" + "." + "ABCDEF" + "." + "abcdefghijklmnopqrstuvwxyz12345" + " connected",
			expected: "Bot token [REDACTED] connected",
		},
		{
			name:     "discord mfa token",
			input:    "Auth with " + "mfa." + "1234567890abcdefghijklmnopqrstuvwxyz",
			expected: "Auth with [REDACTED]",
		},
		{
			name:     "discord webhook url",
			input:    "Delivering notification to " + "https://discord.com/api/webhooks/" + "123456789012345678/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_-" + " for alert",
			expected: "Delivering notification to [REDACTED] for alert",
		},
		{
			name:     "jwt token",
			input:    "Token " + "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" + "." + "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ" + "." + "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
			expected: "Token [REDACTED]",
		},
		{
			name:     "http basic auth header",
			input:    "-c http.extraHeader=AUTHORIZATION: basic " + "eC1hY2Nlc3MtdG9rZW46Z2hwXzEyMzQ1Njc4OTA=",
			expected: "-c http.extraHeader=AUTHORIZATION: [REDACTED]",
		},
		{
			name:     "http bearer auth header",
			input:    "Authorization: Bearer " + "1234567890abcdefghijklmnopqrstuvwxyz",
			expected: "Authorization: [REDACTED]",
		},
		{
			name:     "x-access-token header in URL",
			input:    "Connecting to https://" + "x-access-token:" + "secret1234567890" + "@github.com/org/repo.git",
			expected: "Connecting to https://[REDACTED]@github.com/org/repo.git",
		},
		{
			name:     "postgresql uri with embedded password",
			input:    "Connecting to postgresql://aerial_user:super_secret_p%40ss%23word@db.internal.net:5432/aerial_db?sslmode=disable",
			expected: "Connecting to postgresql://aerial_user:[REDACTED]@db.internal.net:5432/aerial_db?sslmode=disable",
		},
		{
			name:     "multiple postgresql uris on single line (non-greedy verification)",
			input:    "Syncing from postgresql://user1:pass1@host1:5432/db1 to postgresql://user2:pass2@host2:5432/db2",
			expected: "Syncing from postgresql://user1:[REDACTED]@host1:5432/db1 to postgresql://user2:[REDACTED]@host2:5432/db2",
		},
		{
			name:     "uri with email and discord mention on same line",
			input:    "Database postgresql://aerial:secret@db.lan/aerial deployed by alex@company.com (<@123456789>)",
			expected: "Database postgresql://aerial:[REDACTED]@db.lan/aerial deployed by alex@company.com (<@123456789>)",
		},
		{
			name:     "redis uri with password",
			input:    "Cache at redis://default:my_redis_password_123@10.0.0.5:6379/0",
			expected: "Cache at redis://default:[REDACTED]@10.0.0.5:6379/0",
		},
		{
			name:     "key-value assignment preserving label",
			input:    "GEMINI_API_KEY=" + "AIza" + "SyD1234567890abcdefghijklmnopqrstuvw and HA_TOKEN: custom_long_token_value_123",
			expected: "GEMINI_API_KEY=[REDACTED] and HA_TOKEN: [REDACTED]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeString(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizeString(%q)\n got:  %q\n want: %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestSanitizeString_RegisteredTokens(t *testing.T) {
	ResetSensitiveTokens()
	defer ResetSensitiveTokens()

	RegisterSensitiveTokens("custom_gemini_secret_key_999", "super_secret_dynamic_token_xyz")

	input := "Request failed with key custom_gemini_secret_key_999 and token super_secret_dynamic_token_xyz in production mode"
	got := SanitizeString(input)

	if strings.Contains(got, "custom_gemini_secret_key_999") {
		t.Errorf("expected custom_gemini_secret_key_999 to be redacted, got: %s", got)
	}
	if strings.Contains(got, "super_secret_dynamic_token_xyz") {
		t.Errorf("expected super_secret_dynamic_token_xyz to be redacted, got: %s", got)
	}
	if !strings.Contains(got, "production") {
		t.Errorf("expected production word to NOT be redacted, got: %s", got)
	}
}

func TestSanitizeLog_UsesRedactedToken(t *testing.T) {
	input := "fatal: authentication failed for https://x-access-token:" + "ghp_" + "1234567890abcdefghijklmnopqrstuvwxyzAB" + "@github.com"
	got := SanitizeLog(input)

	if !strings.Contains(got, "[REDACTED_TOKEN]") {
		t.Errorf("expected [REDACTED_TOKEN] in SanitizeLog output, got: %s", got)
	}
	if strings.Contains(got, "ghp_") {
		t.Errorf("raw token still present in log output: %s", got)
	}
}

func TestSanitizeEnvVars(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "nil slice",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: []string{},
		},
		{
			name: "system variables must NOT be redacted",
			input: []string{
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
				"COMPAT_MODE=1",
				"SPATIAL_INDEX=true",
				"PATTERN=*.go",
				"HOTKEY=ctrl+c",
				"TELEMETRY_DISPATCHER=enabled",
			},
			expected: []string{
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
				"COMPAT_MODE=1",
				"SPATIAL_INDEX=true",
				"PATTERN=*.go",
				"HOTKEY=ctrl+c",
				"TELEMETRY_DISPATCHER=enabled",
			},
		},
		{
			name: "sensitive variables must be redacted",
			input: []string{
				"PORT=8080",
				"GEMINI_API_KEY=secret_key_value",
				"DISCORD_BOT_TOKEN=bot_token_value",
				"GITHUB_PAT=pat_token_value",
				"CUSTOM_AUTH=bearer_token",
				"DATABASE_PASSWORD=super_secret_db_pass",
				"AERIAL_PROJECT_DIR=/share/aerial",
				"MALFORMED_LINE",
			},
			expected: []string{
				"PORT=8080",
				"GEMINI_API_KEY=[REDACTED]",
				"DISCORD_BOT_TOKEN=[REDACTED]",
				"GITHUB_PAT=[REDACTED]",
				"CUSTOM_AUTH=[REDACTED]",
				"DATABASE_PASSWORD=[REDACTED]",
				"AERIAL_PROJECT_DIR=/share/aerial",
				"MALFORMED_LINE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeEnvVars(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("length mismatch: got %d, want %d", len(got), len(tt.expected))
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("item %d mismatch: got %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestRegisterConfigTokens_EdgeCases(t *testing.T) {
	// Nil config
	RegisterConfigTokens(nil)

	// Config with nil data
	emptyCfg := &config.Config{}
	RegisterConfigTokens(emptyCfg)
}

func TestIsSensitiveEnvKey_Direct(t *testing.T) {
	cases := []struct {
		key      string
		expected bool
	}{
		{"", false},
		{"   ", false},
		{"AUTH", true},
		{"CREDENTIALS", true},
		{"AUTH_HEADER", true},
		{"MY_AUTH_TOKEN", true},
		{"PAT_VALUE", true},
		{"DB_PAT_TOKEN", true},
		{"NORMAL_VAR", false},
	}
	for _, tc := range cases {
		if res := isSensitiveEnvKey(tc.key); res != tc.expected {
			t.Errorf("isSensitiveEnvKey(%q) = %v; want %v", tc.key, res, tc.expected)
		}
	}
}

func BenchmarkSanitizeString_Clean(b *testing.B) {
	text := "This is a clean prompt message from the user requesting a political and AI technology news summary."
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SanitizeString(text)
	}
}

func BenchmarkSanitizeString_WithTokens(b *testing.B) {
	text := "Prompt containing token " + "ghp_" + "1234567890abcdefghijklmnopqrstuvwxyzAB" + " and URI postgresql://user:pass@host:5432/db"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SanitizeString(text)
	}
}
