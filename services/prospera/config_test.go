package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/httpkit"
)

// staticConfig is a fixed ConfigSource for tests.
type staticConfig struct {
	cfg *Config
}

func (s staticConfig) Current() *Config { return s.cfg }

// newTestConfigSource wraps cfg in a real Reloadable whose loader always
// yields cfg, so reload requests report "unchanged".
func newTestConfigSource(cfg *Config) *httpkit.Reloadable[Config] {
	return httpkit.NewReloadable(cfg, func() (*Config, error) { return cfg, nil }, prosperaRestartRequired)
}

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

type loadConfigFileTestCase struct {
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
}

func TestLoadConfigFile_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	paths := setupLoadConfigFileFixtures(t, tmpDir)
	tests := loadConfigFileTestCases(paths, tmpDir)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runLoadConfigFileCase(t, tc)
		})
	}
}

func setupLoadConfigFileFixtures(t *testing.T, tmpDir string) map[string]string {
	t.Helper()
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
	files := map[string]string{
		"valid":            validYAML,
		"bad":              `port: [unclosed`,
		"missing_port":     strings.Replace(validYAML, `port: "4019"`, `port: ""`, 1),
		"missing_brain":    strings.Replace(validYAML, `eri_url: "http://eri.aerial"`, `eri_url: ""`, 1),
		"eri":              strings.Replace(validYAML, `eri_url: "http://eri.aerial"`, `eri_url: "http://eri-prospera.aerial"`, 1),
		"missing_google":   strings.Replace(validYAML, `google_mcp_url: "http://google-mcp.aerial/mcp"`, `google_mcp_url: ""`, 1),
		"missing_coven":    strings.Replace(validYAML, `coven_url: "http://coven.aerial"`, `coven_url: ""`, 1),
		"missing_whisper":  strings.Replace(validYAML, `whisper_url: "http://voice.aerial"`, `whisper_url: ""`, 1),
		"missing_channel":  strings.Replace(validYAML, `default_discord_channel: "prospera"`, `default_discord_channel: ""`, 1),
		"missing_live":     strings.Replace(validYAML, `gemini_live_model: "gemini-3.8-live"`, `gemini_live_model: ""`, 1),
		"missing_fallback": strings.Replace(validYAML, `gemini_fallback_model: "gemini-3.1-flash-live-preview"`, `gemini_fallback_model: ""`, 1),
		"missing_voice":    strings.Replace(validYAML, `gemini_voice_name: "Kore"`, `gemini_voice_name: ""`, 1),
		"bad_window":       strings.Replace(validYAML, `callback_window_minutes: 10080`, `callback_window_minutes: 0`, 1),
		"bad_calls":        strings.Replace(validYAML, `max_concurrent_calls: 2`, `max_concurrent_calls: 0`, 1),
		"twilio": validYAML + `twilio_account_sid: "AC_from_yaml"
twilio_from_number: "(510) 555-0999"
`,
	}

	paths := make(map[string]string, len(files))
	for name, content := range files {
		path := filepath.Join(tmpDir, name+".yaml")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("failed to write %s.yaml: %v", name, err)
		}
		paths[name] = path
	}
	return paths
}

func loadConfigFileTestCases(paths map[string]string, tmpDir string) []loadConfigFileTestCase {
	return append(loadConfigValidCases(paths), loadConfigErrorCases(paths, tmpDir)...)
}

func loadConfigValidCases(paths map[string]string) []loadConfigFileTestCase {
	return []loadConfigFileTestCase{
		{
			name: "valid complete configuration",
			path: paths["valid"],
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
			path: paths["valid"],
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
			path:           paths["valid"],
			callLoadConfig: true,
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			wantErr:  false,
			wantPort: "4019",
		},
		{
			name: "eri_url in yaml",
			path: paths["eri"],
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			wantErr:      false,
			wantPort:     "4019",
			wantBrainURL: "http://eri-prospera.aerial",
		},
		{
			name: "decomposed POSTGRES_PASSWORD resolution",
			path: paths["valid"],
			env: map[string]string{
				"POSTGRES_PASSWORD": "secretpassword",
			},
			wantErr:         false,
			wantPort:        "4019",
			wantDatabaseURL: "postgres://aerial:secretpassword@postgres:5432/aerial?sslmode=disable",
		},
		{
			name: "ERI_URL env fallback when absent from yaml",
			path: paths["missing_brain"],
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
				"ERI_URL":      "http://env-eri.aerial:8088",
			},
			wantErr:      false,
			wantPort:     "4019",
			wantBrainURL: "http://env-eri.aerial:8088",
		},
		{
			name: "twilio fields loaded from yaml when env unset",
			path: paths["twilio"],
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			wantErr:              false,
			wantPort:             "4019",
			wantTwilioAccountSID: "AC_from_yaml",
			wantTwilioFromNumber: "+15105550999",
		},
		{
			name: "twilio fields in env override yaml",
			path: paths["twilio"],
			env: map[string]string{
				"DATABASE_URL":       "postgres://user:pass@localhost:5432/testdb",
				"TWILIO_ACCOUNT_SID": "AC_override_env",
				"TWILIO_FROM_NUMBER": "+15105550111",
			},
			wantErr:              false,
			wantPort:             "4019",
			wantTwilioAccountSID: "AC_override_env",
			wantTwilioFromNumber: "+15105550111",
		},
	}
}

func loadConfigErrorCases(paths map[string]string, tmpDir string) []loadConfigFileTestCase {
	return []loadConfigFileTestCase{
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
			path:    paths["bad"],
			wantErr: true,
		},
		{
			name:    "missing port error",
			path:    paths["missing_port"],
			wantErr: true,
		},
		{
			name:    "missing brain url error",
			path:    paths["missing_brain"],
			wantErr: true,
		},
		{
			name:    "missing google mcp url error",
			path:    paths["missing_google"],
			wantErr: true,
		},
		{
			name:    "missing coven url error",
			path:    paths["missing_coven"],
			wantErr: true,
		},
		{
			name:    "missing whisper url error",
			path:    paths["missing_whisper"],
			wantErr: true,
		},
		{
			name:    "missing default discord channel error",
			path:    paths["missing_channel"],
			wantErr: true,
		},
		{
			name:    "missing live model error",
			path:    paths["missing_live"],
			wantErr: true,
		},
		{
			name:    "missing fallback model error",
			path:    paths["missing_fallback"],
			wantErr: true,
		},
		{
			name:    "missing voice name error",
			path:    paths["missing_voice"],
			wantErr: true,
		},
		{
			name:    "callback window <= 0 error",
			path:    paths["bad_window"],
			wantErr: true,
		},
		{
			name:    "max concurrent calls <= 0 error",
			path:    paths["bad_calls"],
			wantErr: true,
		},
		{
			name:    "missing database url error",
			path:    paths["valid"],
			env:     map[string]string{},
			wantErr: true,
		},
		{
			name: "invalid twilio from number error",
			path: paths["valid"],
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/test",
				"TWILIO_FROM_NUMBER": "not-a-number",
			},
			wantErr: true,
		},
		{
			name: "invalid allow from error",
			path: paths["valid"],
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/test",
				"ALLOW_FROM":   "invalid",
			},
			wantErr: true,
		},
	}
}

func runLoadConfigFileCase(t *testing.T, tc loadConfigFileTestCase) {
	t.Helper()
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

	var getenv func(string) string
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
	assertLoadedConfig(t, tc, cfg)
}

func assertLoadedConfig(t *testing.T, tc loadConfigFileTestCase, cfg *Config) {
	t.Helper()
	strChecks := []struct {
		got, want, field string
	}{
		{cfg.Port, tc.wantPort, "Port"},
		{cfg.DatabaseURL, tc.wantDatabaseURL, "DatabaseURL"},
		{cfg.TwilioAccountSID, tc.wantTwilioAccountSID, "TwilioAccountSID"},
		{cfg.TwilioAuthToken, tc.wantTwilioAuthToken, "TwilioAuthToken"},
		{cfg.TwilioFromNumber, tc.wantTwilioFromNumber, "TwilioFromNumber"},
		{cfg.PublicWebhookBaseURL, tc.wantPublicWebhookBase, "PublicWebhookBaseURL"},
		{cfg.GeminiAPIKey, tc.wantGeminiAPIKey, "GeminiAPIKey"},
		{cfg.GeminiLiveModel, tc.wantGeminiLiveModel, "GeminiLiveModel"},
		{cfg.GeminiFallbackModel, tc.wantGeminiFallback, "GeminiFallbackModel"},
		{cfg.GeminiVoiceName, tc.wantGeminiVoiceName, "GeminiVoiceName"},
		{cfg.GoogleMCPURL, tc.wantGoogleMCPURL, "GoogleMCPURL"},
		{cfg.CovenURL, tc.wantCovenURL, "CovenURL"},
		{cfg.WhisperURL, tc.wantWhisperURL, "WhisperURL"},
		{cfg.DefaultDiscordChannel, tc.wantDiscordChannel, "DefaultDiscordChannel"},
	}
	for _, c := range strChecks {
		if c.want != "" && c.got != c.want {
			t.Errorf("cfg.%s = %q, want %q", c.field, c.got, c.want)
		}
	}
	if tc.wantBrainURL != "" && (cfg.BrainURL != tc.wantBrainURL || cfg.EriURL != tc.wantBrainURL) {
		t.Errorf("cfg.BrainURL = %q, cfg.EriURL = %q, want %q", cfg.BrainURL, cfg.EriURL, tc.wantBrainURL)
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
}
