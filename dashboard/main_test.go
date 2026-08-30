package main

import (
	"testing"
)

func TestSanitizeEnvVars(t *testing.T) {
	input := []string{
		"GEMINI_API_KEY=secret_key_123",
		"DISCORD_TOKEN=bot_token_456",
		"DISCORD_BOT_TOKEN=bot_token_789",
		"GITHUB_PAT=pat_xyz",
		"HA_TOKEN=ha_abc",
		"PORT=8080",
		"AGY_MODEL=Gemini 3.6 Flash",
	}

	sanitized := SanitizeEnvVars(input)

	for _, env := range sanitized {
		if env == "GEMINI_API_KEY=secret_key_123" ||
			env == "DISCORD_TOKEN=bot_token_456" ||
			env == "DISCORD_BOT_TOKEN=bot_token_789" ||
			env == "GITHUB_PAT=pat_xyz" ||
			env == "HA_TOKEN=ha_abc" {
			t.Errorf("found unsanitized secret in output: %s", env)
		}
	}
}
