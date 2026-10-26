package main

import (
	"testing"
)

func TestNormalizeE164(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "already e164", raw: "+15105550199", want: "+15105550199"},
		{name: "us 10 digit formatted", raw: "(510) 555-0199", want: "+15105550199"},
		{name: "us 11 digit leading 1", raw: "1-510-555-0199", want: "+15105550199"},
		{name: "international with spaces", raw: "+44 20 7183 8750", want: "+442071838750"},
		{name: "with dots", raw: "1.510.555.0199", want: "+15105550199"},
		{name: "with slashes", raw: "510/555/0199", want: "+15105550199"},
		{name: "invalid short digits", raw: "123", wantErr: true},
		{name: "invalid empty", raw: "", wantErr: true},
		{name: "invalid letters", raw: "+1510555abcd", wantErr: true},
		{name: "invalid letters 10 digit", raw: "510-555-abcd", wantErr: true},
		{name: "invalid letters 11 digit", raw: "1-510-555-abcd", wantErr: true},
		{name: "invalid zero country code", raw: "+0123456789", wantErr: true},
		{name: "invalid too long", raw: "+1234567890123456", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeE164(tc.raw)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("NormalizeE164(%q) = (%q, %v), want (%q, err=%v)", tc.raw, got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                   string
		env                    map[string]string
		wantErr                bool
		wantPort               string
		wantDatabaseURL        string
		wantTwilioAccountSID   string
		wantTwilioAuthToken    string
		wantTwilioFromNumber   string
		wantPublicWebhookBase  string
		wantGeminiAPIKey       string
		wantGeminiLiveModel    string
		wantGeminiFallback     string
		wantBrainURL           string
		wantGoogleMCPURL       string
		wantCovenURL           string
		wantWhisperURL         string
		wantDiscordChannel     string
		wantCallbackWindowMins int
		wantMaxConcurrent      int
		wantAllowFromLen       int
		wantFirstAllowFrom     string
	}{
		{
			name: "default minimal configuration",
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			wantErr:                false,
			wantPort:               "4019",
			wantDatabaseURL:        "postgres://user:pass@localhost:5432/testdb",
			wantGeminiLiveModel:    "gemini-3.8-live",
			wantGeminiFallback:     "gemini-3.1-flash-live-preview",
			wantBrainURL:           "http://eri.aerial",
			wantGoogleMCPURL:       "http://google-mcp.aerial/mcp",
			wantCovenURL:           "http://coven.aerial",
			wantWhisperURL:         "http://orin-voice.aerial",
			wantDiscordChannel:     "prospera",
			wantCallbackWindowMins: 10080,
			wantMaxConcurrent:      2,
			wantAllowFromLen:       0,
		},
		{
			name: "custom complete configuration with gemini fallback key and allow_from",
			env: map[string]string{
				"PORT":                    "5050",
				"DATABASE_URL":            "postgres://user:pass@db:5432/prospera",
				"TWILIO_ACCOUNT_SID":      "AC1234567890abcdef",
				"TWILIO_AUTH_TOKEN":       "auth_token_xyz",
				"TWILIO_FROM_NUMBER":      "(510) 555-0100",
				"PUBLIC_WEBHOOK_BASE_URL": "https://custom.hooks.example.com",
				"GEMINI_HARNESS_API_KEY":  "harness_key_123",
				"GEMINI_LIVE_MODEL":       "gemini-custom-live",
				"GEMINI_FALLBACK_MODEL":   "gemini-custom-fallback",
				"BRAIN_URL":               "http://eri.custom",
				"GOOGLE_MCP_URL":          "http://google-mcp.custom/mcp",
				"COVEN_URL":               "http://coven.custom",
				"WHISPER_URL":             "http://whisper.custom",
				"DEFAULT_DISCORD_CHANNEL": "voice-ops",
				"CALLBACK_WINDOW_MINUTES": "1440",
				"MAX_CONCURRENT_CALLS":    "5",
				"ALLOW_FROM":              "(510) 555-0101, +15105550102",
			},
			wantErr:                false,
			wantPort:               "5050",
			wantDatabaseURL:        "postgres://user:pass@db:5432/prospera",
			wantTwilioAccountSID:   "AC1234567890abcdef",
			wantTwilioAuthToken:    "auth_token_xyz",
			wantTwilioFromNumber:   "+15105550100",
			wantPublicWebhookBase:  "https://custom.hooks.example.com",
			wantGeminiAPIKey:       "harness_key_123",
			wantGeminiLiveModel:    "gemini-custom-live",
			wantGeminiFallback:     "gemini-custom-fallback",
			wantBrainURL:           "http://eri.custom",
			wantGoogleMCPURL:       "http://google-mcp.custom/mcp",
			wantCovenURL:           "http://coven.custom",
			wantWhisperURL:         "http://whisper.custom",
			wantDiscordChannel:     "voice-ops",
			wantCallbackWindowMins: 1440,
			wantMaxConcurrent:      5,
			wantAllowFromLen:       2,
			wantFirstAllowFrom:     "+15105550101",
		},
		{
			name: "primary gemini key takes precedence and discord_channel_id fallback",
			env: map[string]string{
				"DATABASE_URL":           "postgres://user:pass@localhost:5432/testdb",
				"GEMINI_API_KEY":         "primary_gemini_key",
				"GEMINI_HARNESS_API_KEY": "fallback_key",
				"DISCORD_CHANNEL_ID":     "channel-from-id",
			},
			wantErr:            false,
			wantPort:           "4019",
			wantGeminiAPIKey:   "primary_gemini_key",
			wantDiscordChannel: "channel-from-id",
		},
		{
			name: "missing database url",
			env: map[string]string{
				"PORT": "4019",
			},
			wantErr: true,
		},
		{
			name: "invalid callback window integer",
			env: map[string]string{
				"DATABASE_URL":            "postgres://user:pass@localhost:5432/testdb",
				"CALLBACK_WINDOW_MINUTES": "not-an-int",
			},
			wantErr: true,
		},
		{
			name: "invalid max concurrent calls integer",
			env: map[string]string{
				"DATABASE_URL":         "postgres://user:pass@localhost:5432/testdb",
				"MAX_CONCURRENT_CALLS": "xyz",
			},
			wantErr: true,
		},
		{
			name: "invalid allow from phone number",
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
				"ALLOW_FROM":   "invalid-phone",
			},
			wantErr: true,
		},
		{
			name: "invalid twilio from number",
			env: map[string]string{
				"DATABASE_URL":       "postgres://user:pass@localhost:5432/testdb",
				"TWILIO_FROM_NUMBER": "bad-phone",
			},
			wantErr: true,
		},
		{
			name: "allow from with empty comma entries",
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
				"ALLOW_FROM":   " , +15105550199 ,  ",
			},
			wantErr:          false,
			wantPort:         "4019",
			wantAllowFromLen: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			getenv := func(key string) string {
				return tc.env[key]
			}
			cfg, err := LoadConfig(getenv)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfig() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if cfg.Port != tc.wantPort {
				t.Errorf("cfg.Port = %q, want %q", cfg.Port, tc.wantPort)
			}
			if tc.wantDatabaseURL != "" && cfg.DatabaseURL != tc.wantDatabaseURL {
				t.Errorf("cfg.DatabaseURL = %q, want %q", cfg.DatabaseURL, tc.wantDatabaseURL)
			}
			if tc.wantTwilioAccountSID != "" && cfg.TwilioAccountSID != tc.wantTwilioAccountSID {
				t.Errorf("cfg.TwilioAccountSID = %q, want %q", cfg.TwilioAccountSID, tc.wantTwilioAccountSID)
			}
			if tc.wantTwilioAuthToken != "" && cfg.TwilioAuthToken != tc.wantTwilioAuthToken {
				t.Errorf("cfg.TwilioAuthToken = %q, want %q", cfg.TwilioAuthToken, tc.wantTwilioAuthToken)
			}
			if tc.wantTwilioFromNumber != "" && cfg.TwilioFromNumber != tc.wantTwilioFromNumber {
				t.Errorf("cfg.TwilioFromNumber = %q, want %q", cfg.TwilioFromNumber, tc.wantTwilioFromNumber)
			}
			if tc.wantPublicWebhookBase != "" && cfg.PublicWebhookBaseURL != tc.wantPublicWebhookBase {
				t.Errorf("cfg.PublicWebhookBaseURL = %q, want %q", cfg.PublicWebhookBaseURL, tc.wantPublicWebhookBase)
			}
			if tc.wantGeminiAPIKey != "" && cfg.GeminiAPIKey != tc.wantGeminiAPIKey {
				t.Errorf("cfg.GeminiAPIKey = %q, want %q", cfg.GeminiAPIKey, tc.wantGeminiAPIKey)
			}
			if tc.wantGeminiLiveModel != "" && cfg.GeminiLiveModel != tc.wantGeminiLiveModel {
				t.Errorf("cfg.GeminiLiveModel = %q, want %q", cfg.GeminiLiveModel, tc.wantGeminiLiveModel)
			}
			if tc.wantGeminiFallback != "" && cfg.GeminiFallbackModel != tc.wantGeminiFallback {
				t.Errorf("cfg.GeminiFallbackModel = %q, want %q", cfg.GeminiFallbackModel, tc.wantGeminiFallback)
			}
			if tc.wantBrainURL != "" && cfg.BrainURL != tc.wantBrainURL {
				t.Errorf("cfg.BrainURL = %q, want %q", cfg.BrainURL, tc.wantBrainURL)
			}
			if tc.wantGoogleMCPURL != "" && cfg.GoogleMCPURL != tc.wantGoogleMCPURL {
				t.Errorf("cfg.GoogleMCPURL = %q, want %q", cfg.GoogleMCPURL, tc.wantGoogleMCPURL)
			}
			if tc.wantCovenURL != "" && cfg.CovenURL != tc.wantCovenURL {
				t.Errorf("cfg.CovenURL = %q, want %q", cfg.CovenURL, tc.wantCovenURL)
			}
			if tc.wantWhisperURL != "" && cfg.WhisperURL != tc.wantWhisperURL {
				t.Errorf("cfg.WhisperURL = %q, want %q", cfg.WhisperURL, tc.wantWhisperURL)
			}
			if tc.wantDiscordChannel != "" && cfg.DefaultDiscordChannel != tc.wantDiscordChannel {
				t.Errorf("cfg.DefaultDiscordChannel = %q, want %q", cfg.DefaultDiscordChannel, tc.wantDiscordChannel)
			}
			if tc.wantCallbackWindowMins != 0 && cfg.CallbackWindowMinutes != tc.wantCallbackWindowMins {
				t.Errorf("cfg.CallbackWindowMinutes = %d, want %d", cfg.CallbackWindowMinutes, tc.wantCallbackWindowMins)
			}
			if tc.wantMaxConcurrent != 0 && cfg.MaxConcurrentCalls != tc.wantMaxConcurrent {
				t.Errorf("cfg.MaxConcurrentCalls = %d, want %d", cfg.MaxConcurrentCalls, tc.wantMaxConcurrent)
			}
			if len(cfg.AllowFrom) != tc.wantAllowFromLen {
				t.Errorf("len(cfg.AllowFrom) = %d, want %d", len(cfg.AllowFrom), tc.wantAllowFromLen)
			}
			if tc.wantFirstAllowFrom != "" && len(cfg.AllowFrom) > 0 && cfg.AllowFrom[0] != tc.wantFirstAllowFrom {
				t.Errorf("cfg.AllowFrom[0] = %q, want %q", cfg.AllowFrom[0], tc.wantFirstAllowFrom)
			}
		})
	}
}

func TestLoadConfig_NilGetEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	cfg, err := LoadConfig(nil)
	if err != nil {
		t.Fatalf("LoadConfig(nil) error = %v", err)
	}
	if cfg.DatabaseURL != "postgres://localhost/test" {
		t.Errorf("cfg.DatabaseURL = %q, want postgres://localhost/test", cfg.DatabaseURL)
	}
}
