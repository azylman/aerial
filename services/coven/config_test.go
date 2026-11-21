package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeCovenDarkConfig(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "coven-dark.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

const covenDarkBaseConfig = `port: '4011'
database_url: 'postgres://localhost/coven_test'
eri_url: 'http://eri.aerial'
eri_llm_url: 'http://eri.aerial/internal/llm'
pr_register_url: 'http://synapse.aerial/internal/pr/register'
custom_skills_dir: '/custom/skills'
rules_dir: '/custom/rules'
config_repo_owner: 'azylman'
config_repo_name: 'aerial-config'
ollama:
  base_url: 'http://ollama:11434'
  model: 'all-minilm'
  num_ctx: 512
`

func TestLoadConfigFile_DarkStackDefaults(t *testing.T) {
	env := map[string]string{}
	env["DATABASE_URL"] = "postgres://localhost/coven_test"
	env["POSTGRES_PASSWORD"] = ""
	env["COVEN_STACK"] = ""
	env["STACK"] = ""
	env["COVEN_ROLE"] = ""
	env["TRANSCRIPTS_URL"] = ""
	env["LLM_EFFORT"] = ""
	env["EXTRACT_TIMEOUT_SECONDS"] = ""
	env["EXTRACT_MAX_STEPS"] = ""

	tests := []struct {
		name            string
		extraYAML       string
		wantStack       string
		wantRole        string
		wantTranscripts string
		wantLLMEffort   string
		wantTimeout     time.Duration
		wantMaxSteps    int
	}{
		{
			name:            "empty stack defaults to aerial per K8s N2",
			extraYAML:       "",
			wantStack:       "aerial",
			wantTranscripts: "http://transcripts.aerial",
			wantTimeout:     120 * time.Second,
			wantMaxSteps:    50,
		},
		{
			name:            "explicit dark stack fields load",
			extraYAML:       "stack: 'canary'\nrole: 'coven_aerial'\ntranscripts_url: 'http://indexer:4020'\nllm_effort: ''\nextract_timeout_seconds: 30\nextract_max_steps: 25\n",
			wantStack:       "canary",
			wantRole:        "coven_aerial",
			wantTranscripts: "http://indexer:4020",
			wantLLMEffort:   "",
			wantTimeout:     30 * time.Second,
			wantMaxSteps:    25,
		},
		{
			name:            "indexer_url acts as alias for transcripts_url",
			extraYAML:       "stack: 'canary'\nindexer_url: 'http://indexer-alias:4020'\n",
			wantStack:       "canary",
			wantTranscripts: "http://indexer-alias:4020",
			wantTimeout:     120 * time.Second,
			wantMaxSteps:    50,
		},
		{
			name:            "non-aerial stack with empty transcripts_url does not default",
			extraYAML:       "stack: 'canary'\n",
			wantStack:       "canary",
			wantTranscripts: "",
			wantTimeout:     120 * time.Second,
			wantMaxSteps:    50,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCovenDarkConfig(t, covenDarkBaseConfig+tc.extraYAML)
			cfg, err := LoadConfigFile(path, "postgres://localhost/coven_test", mapEnv(env))
			if err != nil {
				t.Fatalf("LoadConfigFile() error = %v", err)
			}
			if got := cfg.EffectiveStack(); got != tc.wantStack {
				t.Errorf("EffectiveStack() = %q, want %q", got, tc.wantStack)
			}
			if cfg.Role != tc.wantRole {
				t.Errorf("Role = %q, want %q", cfg.Role, tc.wantRole)
			}
			if cfg.TranscriptsURL != tc.wantTranscripts {
				t.Errorf("TranscriptsURL = %q, want %q", cfg.TranscriptsURL, tc.wantTranscripts)
			}
			if cfg.LLMEffort != tc.wantLLMEffort {
				t.Errorf("LLMEffort = %q, want %q (must stay unset per Q2)", cfg.LLMEffort, tc.wantLLMEffort)
			}
			if got := cfg.ExtractTimeout(); got != tc.wantTimeout {
				t.Errorf("ExtractTimeout() = %v, want %v", got, tc.wantTimeout)
			}
			if got := cfg.ExtractBatchLimit(); got != tc.wantMaxSteps {
				t.Errorf("ExtractBatchLimit() = %d, want %d", got, tc.wantMaxSteps)
			}
		})
	}
}

func TestLoadConfigFile_DarkEnvOverrides(t *testing.T) {
	tests := []struct {
		name         string
		extraYAML    string
		envTimeout   string
		envMaxSteps  string
		envEffort    string
		wantTimeout  time.Duration
		wantMaxSteps int
		wantEffort   string
	}{
		{
			name:         "positive env overrides yaml",
			extraYAML:    "extract_timeout_seconds: 30\nextract_max_steps: 25\n",
			envTimeout:   "45",
			envMaxSteps:  "10",
			wantTimeout:  45 * time.Second,
			wantMaxSteps: 10,
		},
		{
			name:         "invalid env falls back to yaml",
			extraYAML:    "extract_timeout_seconds: 30\nextract_max_steps: 25\n",
			envTimeout:   "not-a-number",
			envMaxSteps:  "0",
			wantTimeout:  30 * time.Second,
			wantMaxSteps: 25,
		},
		{
			name:         "unset env keeps yaml values",
			extraYAML:    "extract_timeout_seconds: 30\nextract_max_steps: 25\n",
			wantTimeout:  30 * time.Second,
			wantMaxSteps: 25,
		},
		{
			name:         "llm effort stays unset unless configured",
			extraYAML:    "llm_effort: 'high'\n",
			envEffort:    "",
			wantEffort:   "high",
			wantTimeout:  120 * time.Second,
			wantMaxSteps: 50,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{}
			env["DATABASE_URL"] = "postgres://localhost/coven_test"
			env["POSTGRES_PASSWORD"] = ""
			env["COVEN_STACK"] = ""
			env["STACK"] = ""
			env["EXTRACT_TIMEOUT_SECONDS"] = tc.envTimeout
			env["EXTRACT_MAX_STEPS"] = tc.envMaxSteps
			env["LLM_EFFORT"] = tc.envEffort
			path := writeCovenDarkConfig(t, covenDarkBaseConfig+tc.extraYAML)
			cfg, err := LoadConfigFile(path, "postgres://localhost/coven_test", mapEnv(env))
			if err != nil {
				t.Fatalf("LoadConfigFile() error = %v", err)
			}
			if got := cfg.ExtractTimeout(); got != tc.wantTimeout {
				t.Errorf("ExtractTimeout() = %v, want %v", got, tc.wantTimeout)
			}
			if got := cfg.ExtractBatchLimit(); got != tc.wantMaxSteps {
				t.Errorf("ExtractBatchLimit() = %d, want %d", got, tc.wantMaxSteps)
			}
			if cfg.LLMEffort != tc.wantEffort {
				t.Errorf("LLMEffort = %q, want %q", cfg.LLMEffort, tc.wantEffort)
			}
		})
	}
}

func TestNilConfigDarkDefaults(t *testing.T) {
	var cfg *Config
	if got := cfg.EffectiveStack(); got != DefaultStack {
		t.Errorf("nil EffectiveStack() = %q, want %q", got, DefaultStack)
	}
	if got := cfg.ExtractTimeout(); got != DefaultExtractTimeout {
		t.Errorf("nil ExtractTimeout() = %v, want %v", got, DefaultExtractTimeout)
	}
	if got := cfg.ExtractBatchLimit(); got != DefaultExtractMaxSteps {
		t.Errorf("nil ExtractBatchLimit() = %d, want %d", got, DefaultExtractMaxSteps)
	}
}

func TestNormalizeStack(t *testing.T) {
	tests := []struct {
		name  string
		stack string
		want  string
	}{
		{name: "empty defaults to aerial", stack: "", want: "aerial"},
		{name: "whitespace defaults to aerial", stack: "   ", want: "aerial"},
		{name: "keeps explicit stack", stack: "canary", want: "canary"},
		{name: "trims padding", stack: "  aerial  ", want: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeStack(tc.stack); got != tc.want {
				t.Errorf("NormalizeStack(%q) = %q, want %q", tc.stack, got, tc.want)
			}
		})
	}
}

func TestLoadConfigFile_ExtractorRole(t *testing.T) {
	env := map[string]string{}
	extractorMinimalYAML := `port: '4011'
database_url: 'postgres://localhost/coven_test'
eri_url: 'http://eri.aerial'
eri_llm_url: 'http://eri.aerial/internal/llm'
role: 'extractor'
stack: 'agent-alpha'
ollama:
  base_url: 'http://ollama:11434'
  model: 'all-minilm'
  num_ctx: 512
`
	allRoleMinimalYAML := `port: '4011'
database_url: 'postgres://localhost/coven_test'
eri_url: 'http://eri.aerial'
eri_llm_url: 'http://eri.aerial/internal/llm'
role: 'all'
stack: 'aerial'
ollama:
  base_url: 'http://ollama:11434'
  model: 'all-minilm'
  num_ctx: 512
`

	tests := []struct {
		name      string
		yaml      string
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "extractor role succeeds without pr and config repo keys",
			yaml:    extractorMinimalYAML,
			wantErr: false,
		},
		{
			name:      "all role requires pr_register_url",
			yaml:      allRoleMinimalYAML,
			wantErr:   true,
			errSubstr: "pr_register_url is required",
		},
		{
			name:      "malformed yaml returns error",
			yaml:      "port: [bad yaml",
			wantErr:   true,
			errSubstr: "failed to parse YAML",
		},
		{
			name:      "missing port returns error",
			yaml:      strings.Replace(extractorMinimalYAML, "port: '4011'", "port: ''", 1),
			wantErr:   true,
			errSubstr: "port is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCovenDarkConfig(t, tc.yaml)
			cfg, err := LoadConfigFile(path, "postgres://localhost/coven_test", mapEnv(env))
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && !strings.Contains(err.Error(), tc.errSubstr) {
				t.Errorf("error %q missing expected substring %q", err.Error(), tc.errSubstr)
			}
			if !tc.wantErr && cfg.Role != "extractor" {
				t.Errorf("Role = %q, want 'extractor'", cfg.Role)
			}
		})
	}
}

func TestLoadConfigFile_Auth(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		envEnabled  string
		wantEnabled bool
	}{
		{
			name:        "auth enabled from yaml",
			yaml:        "auth:\n  enabled: true\n",
			wantEnabled: true,
		},
		{
			name:        "auth disabled by default",
			yaml:        "",
			wantEnabled: false,
		},
		{
			name:        "env overrides yaml enabled",
			yaml:        "auth:\n  enabled: false\n",
			envEnabled:  "true",
			wantEnabled: true,
		},
		{
			name:        "env disables yaml enabled",
			yaml:        "auth:\n  enabled: true\n",
			envEnabled:  "false",
			wantEnabled: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"K8S_AUTH_ENABLED": tc.envEnabled}
			path := writeCovenDarkConfig(t, covenDarkBaseConfig+tc.yaml)
			cfg, err := LoadConfigFile(path, "postgres://localhost/coven_test", mapEnv(env))
			if err != nil {
				t.Fatalf("LoadConfigFile() error = %v", err)
			}
			if cfg.Auth.Enabled != tc.wantEnabled {
				t.Errorf("Auth.Enabled = %v, want %v", cfg.Auth.Enabled, tc.wantEnabled)
			}
		})
	}
}

// mapEnv returns a getenv backed by m so loaders never read the process env.
func mapEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}
