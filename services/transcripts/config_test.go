package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigPath(t *testing.T) {
	tmpDir := t.TempDir()
	validLocal, emptyLocal, nonExistent := filepath.Join(tmpDir, "valid.yaml"), filepath.Join(tmpDir, "empty.yaml"), filepath.Join(tmpDir, "missing.yaml")
	fallbackFile := filepath.Join(tmpDir, "transcripts.yaml")
	_ = os.WriteFile(validLocal, []byte("port: '4017'\n"), 0o644)
	_ = os.WriteFile(emptyLocal, []byte{}, 0o644)
	_ = os.WriteFile(fallbackFile, []byte("port: '4017'\n"), 0o644)

	fallback := "/config/transcripts.yaml"
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
		{"fallback empty falls back to defaultConfigPath", "", nonExistent, "", DefaultConfigPath},
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
	writeCfg := func(name, body string) string {
		p := filepath.Join(tmpDir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		return p
	}
	fullPath := writeCfg("full.yaml", "port: '4017'\nstack: 'aerial'\nnats_url: 'nats://nats:4222'\n")
	minimalPath := writeCfg("minimal.yaml", "stack: 'agent-alpha'\n")
	malformedPath := writeCfg("malformed.yaml", "port: [unclosed")
	deployedPath := writeCfg("deployed.yaml", "port: '4030'\nstack: 'aerial'\neri_llm_url: 'http://eri.aerial/internal/llm'\nollama:\n  base_url: 'http://senses.aerial'\n  model: 'bge-small-en-v1.5'\n")
	embedderPath := writeCfg("embedder.yaml", "port: '4030'\nstack: 'aerial'\nembedder:\n  base_url: 'http://embedder.aerial'\n  model: 'bge-custom'\n")

	tests := []struct {
		name            string
		path            string
		dbURL           string
		wantErr         bool
		wantPort        string
		wantStack       string
		wantDB          string
		wantNATS        string
		wantEriLLM      string
		wantSensesURL   string
		wantSensesModel string
	}{
		{"full config", fullPath, "postgres://user:pass@host:5432/db", false, "4017", "aerial", "postgres://user:pass@host:5432/db", "nats://nats:4222", "", "", ""},
		{"defaults fill port stack and nats", minimalPath, "postgres://user:pass@host:5432/db", false, DefaultPort, "agent-alpha", "postgres://user:pass@host:5432/db", "", "", "", ""},
		{"deployed format with ollama", deployedPath, "postgres://user:pass@host:5432/db", false, "4030", "aerial", "postgres://user:pass@host:5432/db", "", "http://eri.aerial/internal/llm", "http://senses.aerial", "bge-small-en-v1.5"},
		{"embedder format populated", embedderPath, "postgres://user:pass@host:5432/db", false, "4030", "aerial", "postgres://user:pass@host:5432/db", "", "", "http://embedder.aerial", "bge-custom"},
		{"yaml database url wins when arg empty", minimalPath, "", true, "", "", "", "", "", "", ""},
		{"missing file", filepath.Join(tmpDir, "missing.yaml"), "postgres://localhost/db", true, "", "", "", "", "", "", ""},
		{"malformed yaml", malformedPath, "postgres://localhost/db", true, "", "", "", "", "", "", ""},
		{"empty path", "", "postgres://localhost/db", true, "", "", "", "", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "")
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", "")
			t.Setenv("POSTGRES_DB", "")
			t.Setenv("NATS_URL", "")

			cfg, err := LoadConfigFile(tc.path, tc.dbURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if cfg.Port != tc.wantPort || cfg.Stack != tc.wantStack || cfg.DatabaseURL != tc.wantDB || cfg.NATSURL != tc.wantNATS {
				t.Fatalf("unexpected config: %+v", cfg)
			}
			if cfg.Summarizer.EriLLMURL != tc.wantEriLLM || cfg.Summarizer.SensesURL != tc.wantSensesURL || cfg.Summarizer.SensesModel != tc.wantSensesModel {
				t.Fatalf("unexpected summarizer in config: %+v", cfg.Summarizer)
			}
		})
	}
}

func TestLoadConfigFileDatabaseURLFromYAML(t *testing.T) {
	tmpDir := t.TempDir()
	p := filepath.Join(tmpDir, "db.yaml")
	_ = os.WriteFile(p, []byte("port: '4017'\nstack: 'aerial'\ndatabase_url: 'postgres://yaml:pass@host:5432/db'\n"), 0o644)
	t.Setenv("DATABASE_URL", "")
	t.Setenv("POSTGRES_URL", "")
	t.Setenv("POSTGRES_HOST", "")
	t.Setenv("POSTGRES_PORT", "")
	t.Setenv("POSTGRES_USER", "")
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_DB", "")

	cfg, err := LoadConfigFile(p, "")
	if err != nil {
		t.Fatalf("LoadConfigFile() err = %v", err)
	}
	if cfg.DatabaseURL != "postgres://yaml:pass@host:5432/db" {
		t.Fatalf("DatabaseURL = %q, want yaml value", cfg.DatabaseURL)
	}
}

func TestLoadConfigFileEnvFallbacks(t *testing.T) {
	tmpDir := t.TempDir()
	p := filepath.Join(tmpDir, "env.yaml")
	if err := os.WriteFile(p, []byte("stack: 'aerial'\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	t.Setenv("DATABASE_URL", "")
	t.Setenv("POSTGRES_URL", "")
	t.Setenv("POSTGRES_HOST", "")
	t.Setenv("POSTGRES_PORT", "")
	t.Setenv("POSTGRES_USER", "")
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_DB", "")
	t.Setenv("NATS_URL", "nats://env:4222")

	cfg, err := LoadConfigFile(p, "postgres://arg:pass@host:5432/db")
	if err != nil {
		t.Fatalf("LoadConfigFile() err = %v", err)
	}
	if cfg.DatabaseURL != "postgres://arg:pass@host:5432/db" {
		t.Fatalf("DatabaseURL = %q, want arg value", cfg.DatabaseURL)
	}
	if cfg.NATSURL != "nats://env:4222" {
		t.Fatalf("NATSURL = %q, want env value", cfg.NATSURL)
	}
	if cfg.Port != DefaultPort || cfg.Stack != "aerial" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadConfigFileDefaultStack(t *testing.T) {
	tmpDir := t.TempDir()
	p := filepath.Join(tmpDir, "nostack.yaml")
	if err := os.WriteFile(p, []byte("port: '4017'\ndatabase_url: 'postgres://x'\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	t.Setenv("DATABASE_URL", "")
	t.Setenv("POSTGRES_URL", "")
	t.Setenv("POSTGRES_HOST", "")
	t.Setenv("POSTGRES_PORT", "")
	t.Setenv("POSTGRES_USER", "")
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_DB", "")
	t.Setenv("NATS_URL", "")
	cfg, err := LoadConfigFile(p, "")
	if err != nil {
		t.Fatalf("LoadConfigFile() err = %v", err)
	}
	if cfg.Stack != DefaultStack {
		t.Fatalf("Stack = %q, want default %q", cfg.Stack, DefaultStack)
	}
}

func TestLoadConfigFileSummarizer(t *testing.T) {
	tmpDir := t.TempDir()
	writeCfg := func(name, body string) string {
		p := filepath.Join(tmpDir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		return p
	}
	absentPath := writeCfg("absent.yaml", "port: '4017'\nstack: 'aerial'\ndatabase_url: 'postgres://x'\n")
	enabledPath := writeCfg("enabled.yaml", "port: '4017'\nstack: 'aerial'\ndatabase_url: 'postgres://x'\nsummarizer:\n  enabled: true\n  batch_limit: 3\n  max_steps: 7\n  eri_llm_url: 'http://eri:8080/internal/llm'\n  senses_url: 'http://senses:8880'\n")
	zeroLimitsPath := writeCfg("zerolimits.yaml", "port: '4017'\nstack: 'aerial'\ndatabase_url: 'postgres://x'\nsummarizer:\n  enabled: true\n")
	tests := []struct {
		name        string
		path        string
		wantEnabled bool
		wantBatch   int
		wantMax     int
		wantLLMURL  string
	}{
		{"absent section stays dark with sweep defaults", absentPath, false, DefaultSummarizerBatchLimit, DefaultSummarizerMaxSteps, ""},
		{"enabled keeps explicit bounds and urls", enabledPath, true, 3, 7, "http://eri:8080/internal/llm"},
		{"zero limits fill defaults", zeroLimitsPath, true, DefaultSummarizerBatchLimit, DefaultSummarizerMaxSteps, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "")
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", "")
			t.Setenv("POSTGRES_DB", "")
			t.Setenv("NATS_URL", "")
			cfg, err := LoadConfigFile(tc.path, "")
			if err != nil {
				t.Fatalf("LoadConfigFile() err = %v", err)
			}
			if cfg.Summarizer.Enabled != tc.wantEnabled {
				t.Errorf("Enabled = %v, want %v", cfg.Summarizer.Enabled, tc.wantEnabled)
			}
			if cfg.Summarizer.BatchLimit != tc.wantBatch {
				t.Errorf("BatchLimit = %d, want %d", cfg.Summarizer.BatchLimit, tc.wantBatch)
			}
			if cfg.Summarizer.MaxSteps != tc.wantMax {
				t.Errorf("MaxSteps = %d, want %d", cfg.Summarizer.MaxSteps, tc.wantMax)
			}
			if cfg.Summarizer.EriLLMURL != tc.wantLLMURL {
				t.Errorf("EriLLMURL = %q, want %q", cfg.Summarizer.EriLLMURL, tc.wantLLMURL)
			}
		})
	}
}

func TestLoadConfigFileAuth(t *testing.T) {
	tmpDir := t.TempDir()
	writeCfg := func(name, body string) string {
		p := filepath.Join(tmpDir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		return p
	}
	defaultAuthPath := writeCfg("default_auth.yaml", "port: '4017'\nstack: 'aerial'\ndatabase_url: 'postgres://x'\n")
	enabledAuthPath := writeCfg("enabled_auth.yaml", "port: '4017'\nstack: 'aerial'\ndatabase_url: 'postgres://x'\nauth:\n  enabled: true\n")

	tests := []struct {
		name        string
		path        string
		k8sAuthEnv  string
		wantEnabled bool
		wantErr     bool
	}{
		{
			name:        "auth disabled by default",
			path:        defaultAuthPath,
			k8sAuthEnv:  "",
			wantEnabled: false,
		},
		{
			name:        "k8s auth enabled via env override",
			path:        defaultAuthPath,
			k8sAuthEnv:  "true",
			wantEnabled: true,
		},
		{
			name:        "auth enabled in yaml",
			path:        enabledAuthPath,
			k8sAuthEnv:  "",
			wantEnabled: true,
		},
		{
			name:        "env 1 enables via ParseBool",
			path:        defaultAuthPath,
			k8sAuthEnv:  "1",
			wantEnabled: true,
		},
		{
			name:        "env false disables yaml true",
			path:        enabledAuthPath,
			k8sAuthEnv:  "false",
			wantEnabled: false,
		},
		{
			name:       "invalid env value fails config load",
			path:       defaultAuthPath,
			k8sAuthEnv: "maybe",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "")
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", "")
			t.Setenv("POSTGRES_DB", "")
			t.Setenv("NATS_URL", "")
			t.Setenv("K8S_AUTH_ENABLED", tc.k8sAuthEnv)

			cfg, err := LoadConfigFile(tc.path, "")
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if cfg.Auth.Enabled != tc.wantEnabled {
				t.Errorf("Auth.Enabled = %v, want %v", cfg.Auth.Enabled, tc.wantEnabled)
			}
		})
	}
}
