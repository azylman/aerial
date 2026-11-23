package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/sanitizer"
)

func TestResolveConfigPath(t *testing.T) {
	tmpDir := t.TempDir()
	validLocal, emptyLocal, nonExistent := filepath.Join(tmpDir, "valid.yaml"), filepath.Join(tmpDir, "empty.yaml"), filepath.Join(tmpDir, "missing.yaml")
	fallbackFile := filepath.Join(tmpDir, "haro.yaml")
	if err := os.WriteFile(validLocal, []byte("port: '4004'\n"), 0o644); err != nil {
		t.Fatalf("write valid.yaml: %v", err)
	}
	if err := os.WriteFile(emptyLocal, []byte{}, 0o644); err != nil {
		t.Fatalf("write empty.yaml: %v", err)
	}
	if err := os.WriteFile(fallbackFile, []byte("port: '4004'\n"), 0o644); err != nil {
		t.Fatalf("write haro.yaml: %v", err)
	}

	fallback := "/config/haro.yaml"
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
		{"fallback empty falls back to defaultConfigPath", "", nonExistent, "", defaultConfigPath},
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

func TestLoadConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	validPath, noPortPath, noTZPath, malformedPath := filepath.Join(tmpDir, "valid.yaml"), filepath.Join(tmpDir, "no_port.yaml"), filepath.Join(tmpDir, "no_tz.yaml"), filepath.Join(tmpDir, "malformed.yaml")
	extraPath := filepath.Join(tmpDir, "extra.yaml")
	if err := os.WriteFile(validPath, []byte("port: '4005'\ntimezone: 'America/Chicago'\neri_url: 'http://eri:8080'\nvanadis_url: 'http://vanadis:8080'\n"), 0o644); err != nil {
		t.Fatalf("write valid.yaml: %v", err)
	}
	if err := os.WriteFile(extraPath, []byte("port: '4005'\ntimezone: 'America/Chicago'\neri_url: 'http://eri:8080'\nvanadis_url: 'http://vanadis:8080'\nextra_event_sources:\n  - ' Custom_Source '\n  - ''\n  - 'custom_source'\n"), 0o644); err != nil {
		t.Fatalf("write extra.yaml: %v", err)
	}
	if err := os.WriteFile(noPortPath, []byte("timezone: 'America/Chicago'\neri_url: 'http://eri:8080'\nvanadis_url: 'http://vanadis:8080'\n"), 0o644); err != nil {
		t.Fatalf("write no_port.yaml: %v", err)
	}
	if err := os.WriteFile(noTZPath, []byte("port: '4005'\neri_url: 'http://eri:8080'\nvanadis_url: 'http://vanadis:8080'\n"), 0o644); err != nil {
		t.Fatalf("write no_tz.yaml: %v", err)
	}
	if err := os.WriteFile(malformedPath, []byte("port: [unclosed"), 0o644); err != nil {
		t.Fatalf("write malformed.yaml: %v", err)
	}

	tests := []struct {
		name        string
		path        string
		dbURL       string
		envPassword string
		envAuth     string
		wantErr     bool
		wantPort    string
		wantTZ      string
		wantDB      string
		wantSources int
		wantAuth    bool
	}{
		{"valid config", validPath, "postgres://user:pass@host:5432/db", "", "", false, "4005", "America/Chicago", "postgres://user:pass@host:5432/db", 0, false},
		{"valid config with extra_event_sources", extraPath, "postgres://user:pass@host:5432/db", "", "", false, "4005", "America/Chicago", "postgres://user:pass@host:5432/db", 1, false},
		{"empty dbURL falls back to decomposed POSTGRES_PASSWORD", validPath, "", "secretpass", "", false, "4005", "America/Chicago", "postgres://aerial:secretpass@postgres:5432/aerial?sslmode=disable", 0, false},
		{"valid K8S_AUTH_ENABLED true overrides yaml", validPath, "postgres://user:pass@host:5432/db", "", "true", false, "4005", "America/Chicago", "postgres://user:pass@host:5432/db", 0, true},
		{"invalid K8S_AUTH_ENABLED returns error", validPath, "postgres://user:pass@host:5432/db", "", "not-a-bool", true, "", "", "", 0, false},
		{"missing file", filepath.Join(tmpDir, "missing.yaml"), "postgres://localhost/db", "", "", true, "", "", "", 0, false},
		{"malformed yaml", malformedPath, "postgres://localhost/db", "", "", true, "", "", "", 0, false},
		{"missing port", noPortPath, "postgres://localhost/db", "", "", true, "", "", "", 0, false},
		{"missing timezone", noTZPath, "postgres://localhost/db", "", "", true, "", "", "", 0, false},
		{"empty dbURL", validPath, "", "", "", true, "", "", "", 0, false},
		{"empty path", "", "postgres://localhost/db", "", "", true, "", "", "", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ERI_URL", "")
			t.Setenv("VANADIS_URL", "")
			t.Setenv("DATABASE_URL", "")
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", tc.envPassword)
			t.Setenv("POSTGRES_DB", "")
			t.Setenv("K8S_AUTH_ENABLED", tc.envAuth)

			cfg, err := LoadConfigFile(tc.path, tc.dbURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (cfg.Port != tc.wantPort || cfg.Timezone != tc.wantTZ || cfg.DatabaseURL != tc.wantDB || len(cfg.ExtraEventSources) != tc.wantSources || cfg.Auth.Enabled != tc.wantAuth) {
				t.Fatalf("unexpected config: %+v", cfg)
			}
		})
	}
}

func writeTestConfigs(t *testing.T, tmpDir string) (pathWithBoth, pathWithoutEri, pathWithoutVanadis, pathWithRestate string) {
	t.Helper()
	pathWithBoth = filepath.Join(tmpDir, "with_both.yaml")
	pathWithoutEri = filepath.Join(tmpDir, "without_eri.yaml")
	pathWithoutVanadis = filepath.Join(tmpDir, "without_vanadis.yaml")
	pathWithRestate = filepath.Join(tmpDir, "with_restate.yaml")
	for _, f := range []struct {
		path    string
		content string
	}{
		{pathWithBoth, "port: '4005'\ntimezone: 'UTC'\neri_url: 'http://yaml.eri:8088/'\nvanadis_url: 'http://yaml.vanadis:4025/'\n"},
		{pathWithoutEri, "port: '4005'\ntimezone: 'UTC'\nvanadis_url: 'http://yaml.vanadis:8080'\n"},
		{pathWithoutVanadis, "port: '4005'\ntimezone: 'UTC'\neri_url: 'http://yaml.eri:8080'\n"},
		{pathWithRestate, "port: '4005'\ntimezone: 'UTC'\neri_url: 'http://yaml.eri:8088/'\nvanadis_url: 'http://yaml.vanadis:4025/'\nrestate_url: 'http://yaml.restate:8080/'\nagent_name: 'agent-alpha'\n"},
	} {
		if err := os.WriteFile(f.path, []byte(f.content), 0o644); err != nil {
			t.Fatalf("write %s: %v", f.path, err)
		}
	}
	return
}

func TestLoadConfig_URLResolution(t *testing.T) {
	tmpDir := t.TempDir()
	pathWithBoth, pathWithoutEri, pathWithoutVanadis, pathWithRestate := writeTestConfigs(t, tmpDir)

	tests := []struct {
		name             string
		path             string
		envEri           string
		envVanadis       string
		envRestate       string
		envRestatePort   string
		envRestateAdmin  string
		envRestateAdv    string
		envAgent         string
		envDB            string
		wantErr          bool
		wantEriURL       string
		wantVanadisURL   string
		wantRestateURL   string
		wantRestatePort  string
		wantRestateAdmin string
		wantRestateAdv   string
		wantAgentName    string
	}{
		{
			name:             "env eri url takes precedence over yaml",
			path:             pathWithBoth,
			envEri:           "http://eri.aerial:9001/",
			envVanadis:       "",
			envDB:            "postgres://localhost/db",
			wantEriURL:       "http://eri.aerial:9001",
			wantVanadisURL:   "http://yaml.vanadis:4025",
			wantRestateURL:   "http://restate.aerial:8080",
			wantRestatePort:  "9080",
			wantRestateAdmin: "http://restate.aerial:9070",
			wantRestateAdv:   "http://haro.default.svc.cluster.local:9080",
			wantAgentName:    "aerial",
		},
		{
			name:             "env vanadis url takes precedence over yaml",
			path:             pathWithBoth,
			envEri:           "",
			envVanadis:       "http://env.vanadis:4025/",
			envDB:            "postgres://localhost/db",
			wantEriURL:       "http://yaml.eri:8088",
			wantVanadisURL:   "http://env.vanadis:4025",
			wantRestateURL:   "http://restate.aerial:8080",
			wantRestatePort:  "9080",
			wantRestateAdmin: "http://restate.aerial:9070",
			wantRestateAdv:   "http://haro.default.svc.cluster.local:9080",
			wantAgentName:    "aerial",
		},
		{
			name:             "yaml urls used when env empty",
			path:             pathWithBoth,
			envEri:           "",
			envVanadis:       "",
			envDB:            "postgres://localhost/db",
			wantEriURL:       "http://yaml.eri:8088",
			wantVanadisURL:   "http://yaml.vanadis:4025",
			wantRestateURL:   "http://restate.aerial:8080",
			wantRestatePort:  "9080",
			wantRestateAdmin: "http://restate.aerial:9070",
			wantRestateAdv:   "http://haro.default.svc.cluster.local:9080",
			wantAgentName:    "aerial",
		},
		{
			name:             "restate url and agent name loaded from yaml",
			path:             pathWithRestate,
			envEri:           "",
			envVanadis:       "",
			envDB:            "postgres://localhost/db",
			wantEriURL:       "http://yaml.eri:8088",
			wantVanadisURL:   "http://yaml.vanadis:4025",
			wantRestateURL:   "http://yaml.restate:8080",
			wantRestatePort:  "9080",
			wantRestateAdmin: "http://restate.aerial:9070",
			wantRestateAdv:   "http://haro.default.svc.cluster.local:9080",
			wantAgentName:    "agent-alpha",
		},
		{
			name:             "env restate url and agent name take precedence over yaml",
			path:             pathWithRestate,
			envEri:           "",
			envVanadis:       "",
			envRestate:       "http://env.restate:8080/",
			envRestatePort:   "9099",
			envRestateAdmin:  "http://admin.restate:9070/",
			envRestateAdv:    "http://adv.restate:9080/",
			envAgent:         "agent-beta",
			envDB:            "postgres://localhost/db",
			wantEriURL:       "http://yaml.eri:8088",
			wantVanadisURL:   "http://yaml.vanadis:4025",
			wantRestateURL:   "http://env.restate:8080",
			wantRestatePort:  "9099",
			wantRestateAdmin: "http://admin.restate:9070",
			wantRestateAdv:   "http://adv.restate:9080",
			wantAgentName:    "agent-beta",
		},
		{
			name:       "missing eri url returns error",
			path:       pathWithoutEri,
			envEri:     "",
			envVanadis: "",
			envDB:      "postgres://localhost/db",
			wantErr:    true,
		},
		{
			name:       "missing vanadis url returns error",
			path:       pathWithoutVanadis,
			envEri:     "",
			envVanadis: "",
			envDB:      "postgres://localhost/db",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ERI_URL", tc.envEri)
			t.Setenv("VANADIS_URL", tc.envVanadis)
			t.Setenv("RESTATE_INGRESS_URL", tc.envRestate)
			t.Setenv("RESTATE_PORT", tc.envRestatePort)
			t.Setenv("RESTATE_ADMIN_URL", tc.envRestateAdmin)
			t.Setenv("RESTATE_ADVERTISE_URL", tc.envRestateAdv)
			t.Setenv("AGENT_NAME", tc.envAgent)
			t.Setenv("DATABASE_URL", tc.envDB)
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", "")
			t.Setenv("POSTGRES_DB", "")

			cfg, err := LoadConfig(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfig() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (cfg.EriURL != tc.wantEriURL || cfg.VanadisURL != tc.wantVanadisURL || cfg.RestateURL != tc.wantRestateURL || cfg.RestatePort != tc.wantRestatePort || cfg.RestateAdminURL != tc.wantRestateAdmin || cfg.RestateAdvertiseURL != tc.wantRestateAdv || cfg.AgentName != tc.wantAgentName) {
				t.Errorf("cfg.EriURL = %q (want %q), cfg.VanadisURL = %q (want %q), cfg.RestateURL = %q (want %q), cfg.RestatePort = %q (want %q), cfg.RestateAdminURL = %q (want %q), cfg.RestateAdvertiseURL = %q (want %q), cfg.AgentName = %q (want %q)",
					cfg.EriURL, tc.wantEriURL, cfg.VanadisURL, tc.wantVanadisURL, cfg.RestateURL, tc.wantRestateURL, cfg.RestatePort, tc.wantRestatePort, cfg.RestateAdminURL, tc.wantRestateAdmin, cfg.RestateAdvertiseURL, tc.wantRestateAdv, cfg.AgentName, tc.wantAgentName)
			}
		})
	}
}

func TestRegisterConfigTokens_TableDriven(t *testing.T) {
	tempDir := t.TempDir()
	secretsDir := filepath.Join(tempDir, "shared_secrets")
	if err := os.MkdirAll(secretsDir, 0o755); err != nil {
		t.Fatalf("failed to create secrets dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(secretsDir, "DISCORD_TOKEN"), []byte("shared_bot_token_secret"), 0o600); err != nil {
		t.Fatalf("write DISCORD_TOKEN: %v", err)
	}

	oldPath := sanitizer.DefaultSharedSecretsPath
	sanitizer.DefaultSharedSecretsPath = secretsDir
	t.Cleanup(func() { sanitizer.DefaultSharedSecretsPath = oldPath })

	tests := []struct {
		name         string
		cfg          *Config
		envVars      map[string]string
		input        string
		shouldRedact []string
	}{
		{
			name: "database and nats passwords registered from config and env",
			cfg: &Config{
				DatabaseURL: "postgres://user:db_cfg_pass@host:5432/db",
				NATSURL:     "nats://natsuser:nats_cfg_pass@nats:4222",
			},
			envVars: map[string]string{
				"POSTGRES_PASSWORD": "env_pg_password_99",
				"GITHUB_PAT":        "ghp_test_token_from_env_123",
			},
			input: "Connecting with db_cfg_pass and nats_cfg_pass and env_pg_password_99 and ghp_test_token_from_env_123 and shared_bot_token_secret",
			shouldRedact: []string{
				"db_cfg_pass",
				"nats_cfg_pass",
				"env_pg_password_99",
				"ghp_test_token_from_env_123",
				"shared_bot_token_secret",
			},
		},
		{
			name: "nil config registers env and shared secrets safely",
			cfg:  nil,
			envVars: map[string]string{
				"POSTGRES_PASSWORD": "standalone_pg_pass",
			},
			input: "Leak standalone_pg_pass and shared_bot_token_secret",
			shouldRedact: []string{
				"standalone_pg_pass",
				"shared_bot_token_secret",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer.ResetSensitiveTokensForTest()
			defer sanitizer.ResetSensitiveTokensForTest()

			for k, v := range tc.envVars {
				t.Setenv(k, v)
			}

			RegisterConfigTokens(tc.cfg)
			got := sanitizer.SanitizeString(tc.input)

			for _, redacted := range tc.shouldRedact {
				if strings.Contains(got, redacted) {
					t.Errorf("expected %q to be redacted from %q", redacted, got)
				}
			}
		})
	}
}
