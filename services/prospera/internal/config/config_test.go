package config

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
	if err := os.WriteFile(validLocal, []byte("port: '4019'\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(validLocal): %v", err)
	}
	emptyLocal := filepath.Join(tmpDir, "local_empty.yaml")
	if err := os.WriteFile(emptyLocal, []byte(""), 0o644); err != nil {
		t.Fatalf("WriteFile(emptyLocal): %v", err)
	}
	nonExistent := filepath.Join(tmpDir, "nonexistent.yaml")
	fallbackFile := filepath.Join(tmpDir, "fallback.yaml")
	if err := os.WriteFile(fallbackFile, []byte("port: '4019'\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(fallbackFile): %v", err)
	}

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

type loadConfigFileValidCase struct {
	name   string
	path   string
	env    map[string]string
	nilEnv bool
	want   Config
}

func TestLoadConfigFile_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	paths := setupLoadConfigFileFixtures(t, tmpDir)
	tests := append(loadConfigValidBaseCases(paths), loadConfigValidOverrideCases(paths)...)

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
		"nats": validYAML + `nats_url: "nats://nats.aerial:4222"
`,
		"restate": validYAML + `restate_port: "9081"
restate_ingress_url: "http://restate-custom.aerial:8080"
restate_admin_url: "http://restate-custom.aerial:9070"
restate_advertise_url: "http://prospera-custom:9080"
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

func loadConfigValidBaseCases(paths map[string]string) []loadConfigFileValidCase {
	return []loadConfigFileValidCase{
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
			want: Config{
				Port:                  "4019",
				DatabaseURL:           "postgres://user:pass@localhost:5432/testdb",
				TwilioAccountSID:      "AC123",
				TwilioAuthToken:       "tok-123",
				TwilioFromNumber:      "+15105550100",
				PublicWebhookBaseURL:  "https://webhooks.example.com",
				GeminiAPIKey:          "key-123",
				GeminiLiveModel:       "gemini-3.8-live",
				GeminiFallbackModel:   "gemini-3.1-flash-live-preview",
				GeminiVoiceName:       "Kore",
				BrainURL:              "http://eri.aerial",
				EriURL:                "http://eri.aerial",
				GoogleMCPURL:          "http://google-mcp.aerial/mcp",
				CovenURL:              "http://coven.aerial",
				WhisperURL:            "http://voice.aerial",
				DefaultDiscordChannel: "prospera",
				CallbackWindowMinutes: 10080,
				MaxConcurrentCalls:    2,
				AllowFrom:             []string{"+15105550101", "+15105550102"},
				RestatePort:           "9080",
				RestateIngressURL:     "http://restate.aerial:8080",
				RestateAdminURL:       "http://restate.aerial:9070",
				RestateAdvertiseURL:   "http://prospera.default.svc.cluster.local:9080",
			},
		},
		{
			name: "gemini harness fallback and allow_from with spaces",
			path: paths["valid"],
			env: map[string]string{
				"DATABASE_URL":           "postgres://user:pass@localhost:5432/testdb",
				"GEMINI_HARNESS_API_KEY": "harness-key-456",
				"ALLOW_FROM":             " , +15105550199 , ",
			},
			want: Config{
				Port:         "4019",
				GeminiAPIKey: "harness-key-456",
				AllowFrom:    []string{"+15105550199"},
			},
		},
		{
			name:   "nil getenv falls back to process env",
			path:   paths["valid"],
			nilEnv: true,
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			want: Config{
				Port: "4019",
			},
		},
		{
			name: "eri_url in yaml",
			path: paths["eri"],
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			want: Config{
				Port:     "4019",
				BrainURL: "http://eri-prospera.aerial",
				EriURL:   "http://eri-prospera.aerial",
			},
		},
		{
			name: "decomposed POSTGRES_PASSWORD resolution",
			path: paths["valid"],
			env: map[string]string{
				"POSTGRES_PASSWORD": "secretpassword",
			},
			want: Config{
				Port:        "4019",
				DatabaseURL: "postgres://aerial:secretpassword@postgres:5432/aerial?sslmode=disable",
			},
		},
		{
			name: "ERI_URL env fallback when absent from yaml",
			path: paths["missing_brain"],
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
				"ERI_URL":      "http://env-eri.aerial:8088",
			},
			want: Config{
				Port:     "4019",
				BrainURL: "http://env-eri.aerial:8088",
				EriURL:   "http://env-eri.aerial:8088",
			},
		},
	}
}

func loadConfigValidOverrideCases(paths map[string]string) []loadConfigFileValidCase {
	return []loadConfigFileValidCase{
		{
			name: "twilio fields loaded from yaml when env unset",
			path: paths["twilio"],
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			want: Config{
				Port:             "4019",
				TwilioAccountSID: "AC_from_yaml",
				TwilioFromNumber: "+15105550999",
			},
		},
		{
			name: "twilio fields in env override yaml",
			path: paths["twilio"],
			env: map[string]string{
				"DATABASE_URL":       "postgres://user:pass@localhost:5432/testdb",
				"TWILIO_ACCOUNT_SID": "AC_override_env",
				"TWILIO_FROM_NUMBER": "+15105550111",
			},
			want: Config{
				Port:             "4019",
				TwilioAccountSID: "AC_override_env",
				TwilioFromNumber: "+15105550111",
			},
		},
		{
			name: "nats_url loaded from yaml",
			path: paths["nats"],
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			want: Config{
				Port:    "4019",
				NATSURL: "nats://nats.aerial:4222",
			},
		},
		{
			name: "nats_url in env overrides yaml",
			path: paths["nats"],
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
				"NATS_URL":     "nats://override.aerial:4222",
			},
			want: Config{
				Port:    "4019",
				NATSURL: "nats://override.aerial:4222",
			},
		},
		{
			name: "restate settings loaded from yaml",
			path: paths["restate"],
			env: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb",
			},
			want: Config{
				Port:                "4019",
				RestatePort:         "9081",
				RestateIngressURL:   "http://restate-custom.aerial:8080",
				RestateAdminURL:     "http://restate-custom.aerial:9070",
				RestateAdvertiseURL: "http://prospera-custom:9080",
			},
		},
		{
			name: "restate settings in env override yaml",
			path: paths["restate"],
			env: map[string]string{
				"DATABASE_URL":          "postgres://user:pass@localhost:5432/testdb",
				"RESTATE_PORT":          "9082",
				"RESTATE_INGRESS_URL":   "http://restate-env.aerial:8080/",
				"RESTATE_ADMIN_URL":     "http://restate-env.aerial:9070/",
				"RESTATE_ADVERTISE_URL": "http://prospera-env:9080/",
			},
			want: Config{
				Port:                "4019",
				RestatePort:         "9082",
				RestateIngressURL:   "http://restate-env.aerial:8080",
				RestateAdminURL:     "http://restate-env.aerial:9070",
				RestateAdvertiseURL: "http://prospera-env:9080",
			},
		},
	}
}

func TestLoadConfigFile_Errors(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	paths := setupLoadConfigFileFixtures(t, tmpDir)
	tests := []struct {
		name    string
		path    string
		env     map[string]string
		wantErr bool
	}{
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			getenv := func(key string) string {
				if tc.env == nil {
					return ""
				}
				return tc.env[key]
			}
			_, err := LoadConfigFile(tc.path, getenv)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func runLoadConfigFileCase(t *testing.T, tc loadConfigFileValidCase) {
	t.Helper()
	var getenv func(string) string
	if tc.nilEnv {
		for k, v := range tc.env {
			t.Setenv(k, v)
		}
	} else {
		getenv = func(key string) string {
			if tc.env == nil {
				return ""
			}
			return tc.env[key]
		}
	}

	cfg, err := LoadConfigFile(tc.path, getenv)
	if err != nil {
		t.Fatalf("LoadConfigFile() unexpected error = %v", err)
	}
	assertLoadedConfig(t, tc.want, cfg)
}

func assertLoadedConfig(t *testing.T, want Config, cfg *Config) {
	t.Helper()
	strChecks := []struct {
		got, want, field string
	}{
		{cfg.Port, want.Port, "Port"},
		{cfg.DatabaseURL, want.DatabaseURL, "DatabaseURL"},
		{cfg.TwilioAccountSID, want.TwilioAccountSID, "TwilioAccountSID"},
		{cfg.TwilioAuthToken, want.TwilioAuthToken, "TwilioAuthToken"},
		{cfg.TwilioFromNumber, want.TwilioFromNumber, "TwilioFromNumber"},
		{cfg.PublicWebhookBaseURL, want.PublicWebhookBaseURL, "PublicWebhookBaseURL"},
		{cfg.GeminiAPIKey, want.GeminiAPIKey, "GeminiAPIKey"},
		{cfg.GeminiLiveModel, want.GeminiLiveModel, "GeminiLiveModel"},
		{cfg.GeminiFallbackModel, want.GeminiFallbackModel, "GeminiFallbackModel"},
		{cfg.GeminiVoiceName, want.GeminiVoiceName, "GeminiVoiceName"},
		{cfg.GoogleMCPURL, want.GoogleMCPURL, "GoogleMCPURL"},
		{cfg.CovenURL, want.CovenURL, "CovenURL"},
		{cfg.WhisperURL, want.WhisperURL, "WhisperURL"},
		{cfg.DefaultDiscordChannel, want.DefaultDiscordChannel, "DefaultDiscordChannel"},
		{cfg.NATSURL, want.NATSURL, "NATSURL"},
		{cfg.RestatePort, want.RestatePort, "RestatePort"},
		{cfg.RestateIngressURL, want.RestateIngressURL, "RestateIngressURL"},
		{cfg.RestateAdminURL, want.RestateAdminURL, "RestateAdminURL"},
		{cfg.RestateAdvertiseURL, want.RestateAdvertiseURL, "RestateAdvertiseURL"},
	}
	for _, c := range strChecks {
		if c.want != "" && c.got != c.want {
			t.Errorf("cfg.%s = %q, want %q", c.field, c.got, c.want)
		}
	}
	if want.BrainURL != "" && (cfg.BrainURL != want.BrainURL || cfg.EriURL != want.BrainURL) {
		t.Errorf("cfg.BrainURL = %q, cfg.EriURL = %q, want %q", cfg.BrainURL, cfg.EriURL, want.BrainURL)
	}
	if want.CallbackWindowMinutes != 0 && cfg.CallbackWindowMinutes != want.CallbackWindowMinutes {
		t.Errorf("cfg.CallbackWindowMinutes = %d, want %d", cfg.CallbackWindowMinutes, want.CallbackWindowMinutes)
	}
	if want.MaxConcurrentCalls != 0 && cfg.MaxConcurrentCalls != want.MaxConcurrentCalls {
		t.Errorf("cfg.MaxConcurrentCalls = %d, want %d", cfg.MaxConcurrentCalls, want.MaxConcurrentCalls)
	}
	if want.AllowFrom != nil {
		if len(cfg.AllowFrom) != len(want.AllowFrom) {
			t.Errorf("len(cfg.AllowFrom) = %d, want %d", len(cfg.AllowFrom), len(want.AllowFrom))
		}
		for i := 0; i < len(cfg.AllowFrom) && i < len(want.AllowFrom); i++ {
			if cfg.AllowFrom[i] != want.AllowFrom[i] {
				t.Errorf("cfg.AllowFrom[%d] = %q, want %q", i, cfg.AllowFrom[i], want.AllowFrom[i])
			}
		}
	}
}

func TestRestartRequired(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		next Config
		want bool
	}{
		{"identical", Config{}, false},
		{"live fields change", Config{GeminiLiveModel: "live-2", DefaultDiscordChannel: "chan", MaxConcurrentCalls: 9}, false},
		{"port change", Config{Port: "9090"}, true},
		{"restate port change", Config{RestatePort: "9081"}, true},
		{"database change", Config{DatabaseURL: "postgres://db2/app"}, true},
		{"twilio account change", Config{TwilioAccountSID: "AC2"}, true},
		{"twilio token change", Config{TwilioAuthToken: "tok2"}, true},
		{"google mcp change", Config{GoogleMCPURL: "http://google-mcp-2"}, true},
		{"coven change", Config{CovenURL: "http://coven-2"}, true},
		{"nats url change", Config{NATSURL: "nats://nats.example.com:4222"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RestartRequired(&Config{}, &tc.next); got != tc.want {
				t.Fatalf("RestartRequired() = %v, want %v", got, tc.want)
			}
		})
	}
}
