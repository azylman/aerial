package config

import (
	"os"
	"path/filepath"
	"testing"
)

var configEnvKeys = []string{
	"PORT", "DATABASE_URL", "POSTGRES_URL", "POSTGRES_HOST", "POSTGRES_PORT",
	"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "ERI_URL", "NATS_URL",
	"CONFIG_PATH", "GITHUB_API_URL", "GITHUB_TOKEN", "GITHUB_PAT",
	"RESTATE_INGRESS_URL", "RESTATE_ENABLED", "VANADIS_RESTATE_ENABLED", "AGENT_NAME", "K8S_AUTH_ENABLED", "AUTH_ENABLED",
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, k := range configEnvKeys {
		t.Setenv(k, "")
	}
}

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "vanadis.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}
	return p
}

type loadConfigCase struct {
	name          string
	fallbackYAML  string // FallbackConfigPath content; "" = file absent
	configYAML    string // explicit config path content; "" = configPath used as given
	configPath    string // explicit path when configYAML is empty; a bare name resolves to a missing file in t.TempDir()
	env           map[string]string
	wantPort      string
	wantPostgres  string
	wantBrain     string
	wantEri       string
	wantNATS      string
	wantGitHubAPI string
	wantGitHubTok string
	wantDeployUI  string
}

// setupConfigPaths points FallbackConfigPath at fallbackYAML (or a missing
// file) and returns the explicit config path to load.
func setupConfigPaths(t *testing.T, fallbackYAML, configYAML, configPath string) string {
	t.Helper()
	tmpDir := t.TempDir()
	FallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")
	if fallbackYAML != "" {
		FallbackConfigPath = writeTempYAML(t, fallbackYAML)
	}
	switch {
	case configYAML != "":
		return writeTempYAML(t, configYAML)
	case configPath != "":
		return filepath.Join(tmpDir, configPath)
	default:
		return ""
	}
}

func checkLoadedConfig(t *testing.T, got Config, tc loadConfigCase, wantConfigPath string) {
	t.Helper()
	if got.Port != tc.wantPort || got.PostgresURL != tc.wantPostgres {
		t.Errorf("port/postgres = %q/%q, want %q/%q", got.Port, got.PostgresURL, tc.wantPort, tc.wantPostgres)
	}
	if got.BrainURL != tc.wantBrain || got.EriURL != tc.wantEri {
		t.Errorf("brain/eri = %q/%q, want %q/%q", got.BrainURL, got.EriURL, tc.wantBrain, tc.wantEri)
	}
	if got.NATSURL != tc.wantNATS {
		t.Errorf("NATSURL = %q, want %q", got.NATSURL, tc.wantNATS)
	}
	if got.GitHubAPIURL != tc.wantGitHubAPI || got.GitHubToken != tc.wantGitHubTok {
		t.Errorf("github api/token = %q/%q, want %q/%q", got.GitHubAPIURL, got.GitHubToken, tc.wantGitHubAPI, tc.wantGitHubTok)
	}
	if got.DeployUIURL != tc.wantDeployUI {
		t.Errorf("DeployUIURL = %q, want %q", got.DeployUIURL, tc.wantDeployUI)
	}
	if got.ConfigPath != wantConfigPath {
		t.Errorf("ConfigPath = %q, want %q", got.ConfigPath, wantConfigPath)
	}
}

func TestLoadConfig(t *testing.T) {
	origFallback := FallbackConfigPath
	t.Cleanup(func() { FallbackConfigPath = origFallback })
	const fileYAML = "port: \"4099\"\npostgres_url: \"postgres://u:p@db:5432/vanadis\"\neri_url: \"http://eri.aerial:8088/\"\n" +
		"github_api_url: \"https://api.github.internal/\"\ngithub_token: \"yaml-pat\"\n"
	const ghAPI = "https://api.github.com"
	tests := []loadConfigCase{
		{name: "unconfigured defaults", wantPort: "4025", wantGitHubAPI: ghAPI},
		{
			name: "decomposed postgres password", env: map[string]string{"POSTGRES_PASSWORD": "custom_vanadis_pass"}, wantPort: "4025",
			wantPostgres: "postgres://aerial:custom_vanadis_pass@postgres:5432/aerial?sslmode=disable", wantGitHubAPI: ghAPI,
		},
		{
			name: "fallback file loaded when explicit path missing", fallbackYAML: fileYAML + "deploy_ui_url: \"https://deploy.example.com/\"\n", configPath: "missing.yaml",
			wantPort: "4099", wantPostgres: "postgres://u:p@db:5432/vanadis", wantBrain: "http://eri.aerial:8088", wantEri: "http://eri.aerial:8088",
			wantGitHubAPI: "https://api.github.internal", wantGitHubTok: "yaml-pat", wantDeployUI: "https://deploy.example.com",
		},
		{name: "explicit config path wins", fallbackYAML: "port: \"4099\"\n", configYAML: "port: \"4101\"\n", wantPort: "4101", wantGitHubAPI: ghAPI},
		{name: "bad yaml warns and falls back", fallbackYAML: ":::invalid_yaml", wantPort: "4025", wantGitHubAPI: ghAPI},
		{
			name: "env overrides file", fallbackYAML: fileYAML, configPath: "missing.yaml",
			env: map[string]string{
				"PORT": "4030", "DATABASE_URL": "postgres://env:5432/db", "ERI_URL": "http://env-eri:8088/",
				"GITHUB_API_URL": "https://env-api.github.com/", "GITHUB_TOKEN": "env-token", "NATS_URL": "nats://env-nats:4222",
			},
			wantPort: "4030", wantPostgres: "postgres://env:5432/db", wantBrain: "http://env-eri:8088", wantEri: "http://env-eri:8088",
			wantNATS: "nats://env-nats:4222", wantGitHubAPI: "https://env-api.github.com", wantGitHubTok: "env-token",
		},
		{name: "github pat fallback", env: map[string]string{"GITHUB_PAT": "pat-token-fallback"}, wantPort: "4025", wantGitHubAPI: ghAPI, wantGitHubTok: "pat-token-fallback"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			configPath := setupConfigPaths(t, tc.fallbackYAML, tc.configYAML, tc.configPath)
			checkLoadedConfig(t, LoadConfig(configPath), tc, configPath)
		})
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	origFallback := FallbackConfigPath
	t.Cleanup(func() { FallbackConfigPath = origFallback })
	tests := []struct {
		name         string
		configYAML   string
		env          map[string]string
		wantPort     string
		wantPostgres string
	}{
		{name: "empty env defaults", wantPort: "4025"},
		{
			name:         "config path file loaded",
			configYAML:   "port: \"4111\"\npostgres_url: \"postgres://u:p@db:5432/vanadis\"\n",
			wantPort:     "4111",
			wantPostgres: "postgres://u:p@db:5432/vanadis",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			FallbackConfigPath = filepath.Join(t.TempDir(), "nonexistent.yaml")
			if tc.configYAML != "" {
				t.Setenv("CONFIG_PATH", writeTempYAML(t, tc.configYAML))
			}
			got := LoadConfigFromEnv()
			if got.Port != tc.wantPort || got.PostgresURL != tc.wantPostgres {
				t.Errorf("port/postgres = %q/%q, want %q/%q", got.Port, got.PostgresURL, tc.wantPort, tc.wantPostgres)
			}
		})
	}
}

func TestLoadRestateConfig(t *testing.T) {
	origFallback := FallbackConfigPath
	t.Cleanup(func() { FallbackConfigPath = origFallback })
	const fileYAML = "enabled: true\nrestate_ingress_url: \"http://restate.aerial:8080/\"\nagent_name: \"yaml-agent\"\n"
	tests := []struct {
		name         string
		fallbackYAML string // FallbackConfigPath content; "" = file absent
		configYAML   string // explicit config path content; "" = no explicit path
		env          map[string]string
		wantEnabled  bool
		wantURL      string
		wantAgent    string
	}{
		{name: "defaults unconfigured", wantAgent: "aerial"},
		{name: "env ingress url implies enabled", env: map[string]string{"RESTATE_INGRESS_URL": "http://env-restate:8080/"}, wantEnabled: true, wantURL: "http://env-restate:8080", wantAgent: "aerial"},
		{name: "env restate enabled true", env: map[string]string{"RESTATE_ENABLED": "true"}, wantEnabled: true, wantAgent: "aerial"},
		{name: "env vanadis restate enabled true", env: map[string]string{"VANADIS_RESTATE_ENABLED": "true"}, wantEnabled: true, wantAgent: "aerial"},
		{
			name: "env disabled overrides ingress url", env: map[string]string{"RESTATE_INGRESS_URL": "http://env-restate:8080", "RESTATE_ENABLED": "false"},
			wantURL: "http://env-restate:8080", wantAgent: "aerial",
		},
		{
			name: "vanadis disabled overrides ingress url", env: map[string]string{"RESTATE_INGRESS_URL": "http://env-restate:8080", "VANADIS_RESTATE_ENABLED": "false"},
			wantURL: "http://env-restate:8080", wantAgent: "aerial",
		},
		{name: "agent name from env", env: map[string]string{"AGENT_NAME": "  Agent-Beta  "}, wantAgent: "Agent-Beta"},
		{name: "config path yaml loaded", configYAML: fileYAML, wantEnabled: true, wantURL: "http://restate.aerial:8080", wantAgent: "yaml-agent"},
		{name: "fallback config path loaded", fallbackYAML: fileYAML, wantEnabled: true, wantURL: "http://restate.aerial:8080", wantAgent: "yaml-agent"},
		{name: "bad yaml falls back to defaults", fallbackYAML: ":::bad_yaml", wantAgent: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			got := LoadRestateConfig(setupConfigPaths(t, tc.fallbackYAML, tc.configYAML, ""))
			if got.Enabled != tc.wantEnabled {
				t.Errorf("Enabled = %v, want %v", got.Enabled, tc.wantEnabled)
			}
			if got.RestateIngressURL != tc.wantURL {
				t.Errorf("RestateIngressURL = %q, want %q", got.RestateIngressURL, tc.wantURL)
			}
			if got.AgentName != tc.wantAgent {
				t.Errorf("AgentName = %q, want %q", got.AgentName, tc.wantAgent)
			}
		})
	}
}
