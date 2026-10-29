package sanitizer

import (
	"strings"
	"testing"
)

func TestSanitizeString_Tokens_TableDriven(t *testing.T) {
	t.Parallel()

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
			input:    "Hello world, normal text about setup.",
			expected: "Hello world, normal text about setup.",
		},
		{
			name:     "classic github pat",
			input:    "Error using token " + "ghp_" + "1234567890abcdefghijklmnopqrstuvwxyzAB to access repo",
			expected: "Error using token [REDACTED] to access repo",
		},
		{
			name:     "fine-grained github pat",
			input:    "Pushing with " + "github_pat_" + "11AABCDEF0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 to origin",
			expected: "Pushing with [REDACTED] to origin",
		},
		{
			name:     "github oauth tokens",
			input:    "Tokens " + "gho_" + "1234567890abcdefghijklmnopqrstuvwxyzAB and " + "ghu_" + "1234567890abcdefghijklmnopqrstuvwxyzAB",
			expected: "Tokens [REDACTED] and [REDACTED]",
		},
		{
			name:     "google api key",
			input:    "API key " + "AIza" + "SyD1234567890abcdefghijklmnopqrstuvw configured",
			expected: "API key [REDACTED] configured",
		},
		{
			name:     "antigravity and gemini tokens",
			input:    "Using " + "antigravity_" + "secret_token_12345 and " + "gemini_" + "api_key_6789012345",
			expected: "Using [REDACTED] and [REDACTED]",
		},
		{
			name:     "openai and anthropic keys",
			input:    "Keys " + "sk-proj-" + "1234567890abcdefghijklmnop and " + "sk-ant-api03-" + "1234567890abcdefghijklmnop",
			expected: "Keys [REDACTED] and [REDACTED]",
		},
		{
			name:     "discord bot token",
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
			input:    "Delivering notification to " + "https://discord.com/api/webhooks/" + "123456789012345678/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_- for alert",
			expected: "Delivering notification to [REDACTED] for alert",
		},
		{
			name:     "jwt token",
			input:    "Token " + "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" + "." + "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ" + "." + "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
			expected: "Token [REDACTED]",
		},
		{
			name:     "http basic auth header",
			input:    "-c http.extraHeader=AUTHORIZATION: " + "basic " + "eC1hY2Nlc3MtdG9rZW46Z2hwXzEyMzQ1Njc4OTA=",
			expected: "-c http.extraHeader=AUTHORIZATION: [REDACTED]",
		},
		{
			name:     "http bearer auth header",
			input:    "Authorization: " + "Bearer " + "1234567890abcdefghijklmnopqrstuvwxyz",
			expected: "Authorization: [REDACTED]",
		},
		{
			name:     "x-access-token header in URL",
			input:    "Connecting to https://" + "x-access-token:" + "secret1234567890@github.com/org/repo.git",
			expected: "Connecting to https://[REDACTED]@github.com/org/repo.git",
		},
		{
			name:     "postgresql uri with embedded password",
			input:    "Connecting to postgresql://aerial_user:" + "super_secret_p%40ss%23word" + "@db.internal.net:5432/aerial_db?sslmode=disable",
			expected: "Connecting to postgresql://aerial_user:[REDACTED]@db.internal.net:5432/aerial_db?sslmode=disable",
		},
		{
			name:     "redis uri with password",
			input:    "Cache at redis://default:" + "my_redis_password_123" + "@10.0.0.5:6379/0",
			expected: "Cache at redis://default:[REDACTED]@10.0.0.5:6379/0",
		},
		{
			name:     "key-value assignment preserving label",
			input:    "GEMINI_API_KEY=" + "AIza" + "SyD1234567890abcdefghijklmnopqrstuvw and HA_TOKEN: " + "custom_long_token_value_123",
			expected: "GEMINI_API_KEY=[REDACTED] and HA_TOKEN: [REDACTED]",
		},
		{
			name:     "extended secret key-value assignment",
			input:    "SERVICE_SECRET=super_secret_val and DB_PASSWORD: pass_val_123",
			expected: "SERVICE_SECRET=[REDACTED] and DB_PASSWORD: [REDACTED]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := SanitizeString(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeString(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSanitizeLog_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty log",
			input:    "",
			expected: "",
		},
		{
			name:     "github pat in log",
			input:    "fatal: could not read from remote repo " + "ghp_" + "abcdefghijklmnopqrstuvwxyz123456",
			expected: "fatal: could not read from remote repo [REDACTED_TOKEN]",
		},
		{
			name:     "secret key-value in log",
			input:    "failed command with PAT: my_special_pat_token",
			expected: "failed command with PAT: [REDACTED_TOKEN]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := SanitizeLog(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeLog(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSanitizeMentions_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "user and role mentions",
			input:    "Hello <@12345678> and <@!87654321> and <@&11223344>!",
			expected: "Hello  and  and !",
		},
		{
			name:     "channel and broadcast mentions",
			input:    "Check <#99887766> @everyone and @here right now",
			expected: "Check   and  right now",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := SanitizeMentions(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeMentions(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestNormalizeWhitespace_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "tabs, newlines and multiple spaces",
			input:    "   hello \t\t world \n\n this is \r a   test   ",
			expected: "hello world this is a test",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := NormalizeWhitespace(tc.input)
			if got != tc.expected {
				t.Errorf("NormalizeWhitespace(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSanitizePromptTags_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "opening and closing user request",
			input:    "<user_request>execute task</user_request>",
			expected: "<\\USER_REQUEST>execute task<\\/USER_REQUEST>",
		},
		{
			name:     "channel instructions self-closing",
			input:    "<channel_instructions />",
			expected: "<\\/CHANNEL_INSTRUCTIONS>",
		},
		{
			name:     "unknown tag left intact",
			input:    "<custom_tag>hello</custom_tag>",
			expected: "<custom_tag>hello</custom_tag>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := SanitizePromptTags(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizePromptTags(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSanitizeAuthor_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "author with newlines and angle brackets",
			input:    "  <Evil\nAuthor\r>  ",
			expected: "EvilAuthor",
		},
		{
			name:     "clean author",
			input:    "NormalUser#1234",
			expected: "NormalUser#1234",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := SanitizeAuthor(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeAuthor(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func resetSensitiveTokens() {
	sensitiveTokensMu.Lock()
	defer sensitiveTokensMu.Unlock()
	empty := make([]string, 0)
	sensitiveTokensPtr.Store(&empty)
}

func TestRegisterSensitiveTokens_TableDriven(t *testing.T) {
	// Not t.Parallel() because it mutates global sensitive tokens state
	tests := []struct {
		name         string
		tokens       []string
		input        string
		shouldRedact []string
	}{
		{
			name:         "short token ignored and valid token registered",
			tokens:       []string{"abc", "   ", "my_super_secret_dynamic_token_123"},
			input:        "failed with my_super_secret_dynamic_token_123 and abc",
			shouldRedact: []string{"my_super_secret_dynamic_token_123"},
		},
		{
			name:         "multiple tokens sorted by length",
			tokens:       []string{"secret_token_long", "secret_token_longer_value"},
			input:        "values: secret_token_longer_value and secret_token_long",
			shouldRedact: []string{"secret_token_longer_value", "secret_token_long"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetSensitiveTokens()
			defer resetSensitiveTokens()

			RegisterSensitiveTokens(tc.tokens...)
			got := SanitizeString(tc.input)

			for _, redacted := range tc.shouldRedact {
				if strings.Contains(got, redacted) {
					t.Errorf("expected %q to be redacted from %q", redacted, got)
				}
			}
		})
	}
}
