package main

import (
	"os"
	"path/filepath"
	"strings"
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

func TestResolveConfigPath(t *testing.T) {
	tmpDir := t.TempDir()
	validLocal := filepath.Join(tmpDir, "local_valid.yaml")
	_ = os.WriteFile(validLocal, []byte("port: '4019'\n"), 0o644)
	emptyLocal := filepath.Join(tmpDir, "local_empty.yaml")
	_ = os.WriteFile(emptyLocal, []byte(""), 0o644)
	nonExistent := filepath.Join(tmpDir, "nonexistent.yaml")
	fallbackFile := filepath.Join(tmpDir, "fallback.yaml")
	_ = os.WriteFile(fallbackFile, []byte("port: '4019'\n"), 0o644)

	fallback := "/config/prospera.yaml"
	tests := []struct {
		name         string
		envVal       string
		localPath    string
		fallbackPath string
		want         string
	}{
		{"env override takes precedence", "/custom/override.yaml", validLocal, fallback, "/custom/override.yaml"},
		{"local config non-empty exists", "", validLocal, fallback, validLocal},
		{"local config empty falls back to fallbackFile", "", emptyLocal, fallbackFile, fallbackFile},
		{"local config missing falls back to fallbackFile", "", nonExistent, fallbackFile, fallbackFile},
		{"fallback missing falls back to default", "", nonExistent, fallback, fallback},
		{"fallback empty falls back to DefaultConfigPath", "", nonExistent, "", DefaultConfigPath},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", tc.envVal)
			oldLocal := localConfigPath
			localConfigPath = tc.localPath
			defer func() {
				localConfigPath = oldLocal
			}()

			if got := ResolveConfigPath(tc.fallbackPath); got != tc.want {
				t.Errorf("ResolveConfigPath(%q) = %q, want %q", tc.fallbackPath, got, tc.want)
			}
		})
	}
}

func TestLoadConfigFile_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()

	validYAML := `
port: "4019"
eri_url: "http://eri.aerial"
google_mcp_url: "http://google-mcp.aerial/mcp"
coven_url: "http://coven.aerial"
whisper_url: "http://voice.aerial"
default_discord_channel: "prospera"
public_webhook_base_url: "https://webhooks.example.com/"
gemini_live_model: "gemini-3.8-live"
gemini_fallback_model: "gemini-3.1-flash-live-preview"
gemini_voice_name: "Kore"
callback_window_minutes: 10080
max_concurrent_calls: 2
`
	validPath := filepath.Join(tmpDir, "valid.yaml")
	if err := os.WriteFile(validPath, []byte(validYAML), 0o644); err != nil {
		t.Fatalf("failed to write valid.yaml: %v", err)
	}

	badYAML := `port: [unclosed`
	badPath := filepath.Join(tmpDir, "bad.yaml")
	if err := os.WriteFile(badPath, []byte(badYAML), 0o644); err != nil {
		t.Fatalf("failed to write bad.yaml: %v", err)
	}

	missingPortYAML := strings.Replace(validYAML, `port: "4019"`, `port: ""`, 1)
	missingPortPath := filepath.Join(tmpDir, "missing_port.yaml")
	if err := os.WriteFile(missingPortPath, []byte(missingPortYAML), 0o644); err != nil {
		t.Fatalf("failed to write missing_port.yaml: %v", err)
	}

	missingBrainYAML := strings.Replace(validYAML, `eri_url: "http://eri.aerial"`, `eri_url: ""`, 1)
	missingBrainPath := filepath.Join(tmpDir, "missing_brain.yaml")
	if err := os.WriteFile(missingBrainPath, []byte(missingBrainYAML), 0o644); err != nil {
		t.Fatalf("failed to write missing_brain.yaml: %v", err)
	}

	eriYAML := strings.Replace(validYAML, `eri_url: "http://eri.aerial"`, `eri_url: "http://eri-prospera.aerial"`, 1)
	eriPath := filepath.Join(tmpDir, "eri.yaml")
	if err := os.WriteFile(eriPath, []byte(eriYAML), 0o644); err != nil {
		t.Fatalf("failed to write eri.yaml: %v", err)
	}

	missingGoogleMCPYAML := strings.Replace(validYAML, `google_mcp_url: "http://google-mcp.aerial/mcp"`, `google_mcp_url: ""`, 1)
	missingGoogleMCPPath := filepath.Join(tmpDir, "missing_google.yaml")
	if err := os.WriteFile(missingGoogleMCPPath, []byte(missingGoogleMCPYAML), 0o644); err != nil {
		t.Fatalf("failed to write missing_google.yaml: %v", err)
	}

	missingCovenYAML := strings.Replace(validYAML, `coven_url: "http://coven.aerial"`, `coven_url: ""`, 1)
	missingCovenPath := filepath.Join(tmpDir, "missing_coven.yaml")
	if err := os.WriteFile(missingCovenPath, []byte(missingCovenYAML), 0o644); err != nil {
		t.Fatalf("failed to write missing_coven.yaml: %v", err)
	}

	missingWhisperYAML := strings.Replace(validYAML, `whisper_url: "http://voice.aerial"`, `whisper_url: ""`, 1)
	missingWhisperPath := filepath.Join(tmpDir, "missing_whisper.yaml")
	if err := os.WriteFile(missingWhisperPath, []byte(missingWhisperYAML), 0o644); err != nil {
		t.Fatalf("failed to write missing_whisper.yaml: %v", err)
	}

	missingChannelYAML := strings.Replace(validYAML, `default_discord_channel: "prospera"`, `default_discord_channel: ""`, 1)
	missingChannelPath := filepath.Join(tmpDir, "missing_channel.yaml")
	if err := os.WriteFile(missingChannelPath, []byte(missingChannelYAML), 0o644); err != nil {
		t.Fatalf("failed to write missing_channel.yaml: %v", err)
	}

	missingLiveModelYAML := strings.Replace(validYAML, `gemini_live_model: "gemini-3.8-live"`, `gemini_live_model: ""`, 1)
	missingLiveModelPath := filepath.Join(tmpDir, "missing_live_model.yaml")
	if err := os.WriteFile(missingLiveModelPath, []byte(missingLiveModelYAML), 0o644); err != nil {
		t.Fatalf("failed to write missing_live_model.yaml: %v", err)
	}

	missingFallbackModelYAML := strings.Replace(validYAML, `gemini_fallback_model: "gemini-3.1-flash-live-preview"`, `gemini_fallback_model: ""`, 1)
	missingFallbackModelPath := filepath.Join(tmpDir, "missing_fallback_model.yaml")
	if err := os.WriteFile(missingFallbackModelPath, []byte(missingFallbackModelYAML), 0o644); err != nil {
		t.Fatalf("failed to write missing_fallback_model.yaml: %v", err)
	}

	missingVoiceNameYAML := strings.Replace(validYAML, `gemini_voice_name: "Kore"`, `gemini_voice_name: ""`, 1)
	missingVoiceNamePath := filepath.Join(tmpDir, "missing_voice_name.yaml")
	if err := os.WriteFile(missingVoiceNamePath, []byte(missingVoiceNameYAML), 0o644); err != nil {
		t.Fatalf("failed to write missing_voice_name.yaml: %v", err)
	}

	badWindowYAML := strings.Replace(validYAML, `callback_window_minutes: 10080`, `callback_window_minutes: 0`, 1)
	badWindowPath := filepath.Join(tmpDir, "bad_window.yaml")
	if err := os.WriteFile(badWindowPath, []byte(badWindowYAML), 0o644); err != nil {
		t.Fatalf("failed to write bad_window.yaml: %v", err)
	}

	badCallsYAML := strings.Replace(validYAML, `max_concurrent_calls: 2`, `max_concurrent_calls: 0`, 1)
	badCallsPath := filepath.Join(tmpDir, "bad_calls.yaml")
	if err := os.WriteFile(badCallsPath, []byte(badCallsYAML), 0o644); err != nil {
		t.Fatalf("failed to write bad_calls.yaml: %v", err)
	}

	tests := []struct {
		name                   string
		path                   string
		env                    map[string]string
		nilEnv                 bool
		callLoadConfig         bool
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
		wantGeminiVoiceName    string
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
			name: "valid complete configuration",
			path: validPath,
			env: map[string]string{
				"DATABASE_URL":           "postgres://user:pass@localhost:5432/testdb",
				"GEMINI_HARNESS_API_KEY": "key-123",
				"TWILIO_ACCOUNT_SID":     "AC123",
				"TWILIO_AUTH_TOKEN":      "tok-123",
				"TWILIO_FROM_NUMBER":     "(510) 555-0100",
				"ALLOW_FROM":             "+15105550101, +15105550102",
			},
			wantErr:                false,
			wantPort:               "4019",
			wantDatabaseURL:        "postgres://user:pass@localhost:5432/testdb",
			wantTwilioAccountSID:   "AC123",
			wantTwilioAuthToken:    "tok-123",
			wantTwilioFromNumber:   "+15105550100",
			wantPublicWebhookBase:  "https://webhooks.example.com",
			wantGeminiAPIKey:       "key-123",
			wantGeminiLiveModel:    "gemini-3.8-live",
			wantGeminiFallback:     "gemini-3.1-flash-live-preview",
			wantGeminiVoiceName:    "Kore",
			wantBrainURL:           "http://eri.aerial",
			wantGoogleMCPURL:       "http://google-mcp.aerial/mcp",
			wantCovenURL:           "http://coven.aerial",
			wantWhisperURL:         "http://voice.aerial",
			wantDiscordChannel:     "prospera",
			wantCallbackWindowMins: 10080,
			wantMaxConcurrent:      2,
			wantAllowFromLen:       2,
			wantFirstAllowFrom:     "+15105550101",
		},
		{
			name: "gemini harness fallback and allow_from with spaces",
			path: validPath,
			env: map[string]string{
				"DATABASE_URL":           "postgres://user:pass@localhost:5432/testdb",
				"GEMINI_HARNESS_API_KEY": "harness-key-456",
				"ALLOW_FROM":             " , +15105550199 , ",
			},
			wantErr:              false,
			wantPort:             "4019",
			wantGeminiAPIKey:     "harness-key-456",
			wantAllowFromLen:     1,
			wantFirstAllowFrom:   "+15105550199",
			wantTwilioFromNumber: "",
		},
		{
			name:           "load config wrapper with env",
			path:           validPath,
			callLoadConfig: true,
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			wantErr:  false,
			wantPort: "4019",
		},
		{
			name: "eri_url in yaml",
			path: eriPath,
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			wantErr:      false,
			wantPort:     "4019",
			wantBrainURL: "http://eri-prospera.aerial",
		},
		{
			name: "ERI_URL env fallback when absent from yaml",
			path: missingBrainPath,
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
				"ERI_URL":      "http://env-eri.aerial:8088",
			},
			wantErr:      false,
			wantPort:     "4019",
			wantBrainURL: "http://env-eri.aerial:8088",
		},
		{
			name:    "empty path error",
			path:    "",
			wantErr: true,
		},
		{
			name:    "nonexistent file error",
			path:    filepath.Join(tmpDir, "nonexistent.yaml"),
			wantErr: true,
		},
		{
			name:    "bad yaml syntax error",
			path:    badPath,
			wantErr: true,
		},
		{
			name:    "missing port error",
			path:    missingPortPath,
			wantErr: true,
		},
		{
			name:    "missing brain url error",
			path:    missingBrainPath,
			wantErr: true,
		},
		{
			name:    "missing google mcp url error",
			path:    missingGoogleMCPPath,
			wantErr: true,
		},
		{
			name:    "missing coven url error",
			path:    missingCovenPath,
			wantErr: true,
		},
		{
			name:    "missing whisper url error",
			path:    missingWhisperPath,
			wantErr: true,
		},
		{
			name:    "missing default discord channel error",
			path:    missingChannelPath,
			wantErr: true,
		},
		{
			name:    "missing live model error",
			path:    missingLiveModelPath,
			wantErr: true,
		},
		{
			name:    "missing fallback model error",
			path:    missingFallbackModelPath,
			wantErr: true,
		},
		{
			name:    "missing voice name error",
			path:    missingVoiceNamePath,
			wantErr: true,
		},
		{
			name:    "callback window <= 0 error",
			path:    badWindowPath,
			wantErr: true,
		},
		{
			name:    "max concurrent calls <= 0 error",
			path:    badCallsPath,
			wantErr: true,
		},
		{
			name:    "missing database url error",
			path:    validPath,
			env:     map[string]string{},
			wantErr: true,
		},
		{
			name: "invalid twilio from number error",
			path: validPath,
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/test",
				"TWILIO_FROM_NUMBER": "not-a-number",
			},
			wantErr: true,
		},
		{
			name: "invalid allow from error",
			path: validPath,
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/test",
				"ALLOW_FROM":   "invalid",
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var getenv func(string) string
			if tc.callLoadConfig {
				for k, v := range tc.env {
					t.Setenv(k, v)
				}
				cfg, err := LoadConfig(tc.path)
				if (err != nil) != tc.wantErr {
					t.Fatalf("LoadConfig() error = %v, wantErr = %v", err, tc.wantErr)
				}
				if tc.wantErr {
					return
				}
				if cfg.Port != tc.wantPort {
					t.Errorf("cfg.Port = %q, want %q", cfg.Port, tc.wantPort)
				}
				return
			}

			if tc.nilEnv {
				getenv = nil
			} else {
				getenv = func(key string) string {
					if tc.env == nil {
						return ""
					}
					return tc.env[key]
				}
			}

			cfg, err := LoadConfigFile(tc.path, getenv)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() error = %v, wantErr = %v", err, tc.wantErr)
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
			if tc.wantGeminiVoiceName != "" && cfg.GeminiVoiceName != tc.wantGeminiVoiceName {
				t.Errorf("cfg.GeminiVoiceName = %q, want %q", cfg.GeminiVoiceName, tc.wantGeminiVoiceName)
			}
			if tc.wantBrainURL != "" && (cfg.BrainURL != tc.wantBrainURL || cfg.EriURL != tc.wantBrainURL) {
				t.Errorf("cfg.BrainURL = %q, cfg.EriURL = %q, want %q", cfg.BrainURL, cfg.EriURL, tc.wantBrainURL)
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
