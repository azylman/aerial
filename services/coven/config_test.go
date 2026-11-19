package main

import (
	"os"
	"path/filepath"
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
	t.Setenv("DATABASE_URL", "postgres://localhost/coven_test")
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("COVEN_STACK", "")
	t.Setenv("STACK", "")
	t.Setenv("COVEN_ROLE", "")
	t.Setenv("TRANSCRIPTS_URL", "")
	t.Setenv("LLM_EFFORT", "")
	t.Setenv("EXTRACT_TIMEOUT_SECONDS", "")
	t.Setenv("EXTRACT_MAX_STEPS", "")

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
			name:         "empty stack defaults to aerial per K8s N2",
			extraYAML:    "",
			wantStack:    "aerial",
			wantTimeout:  120 * time.Second,
			wantMaxSteps: 50,
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCovenDarkConfig(t, covenDarkBaseConfig+tc.extraYAML)
			cfg, err := LoadConfigFile(path, "postgres://localhost/coven_test")
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
			t.Setenv("DATABASE_URL", "postgres://localhost/coven_test")
			t.Setenv("POSTGRES_PASSWORD", "")
			t.Setenv("COVEN_STACK", "")
			t.Setenv("STACK", "")
			t.Setenv("EXTRACT_TIMEOUT_SECONDS", tc.envTimeout)
			t.Setenv("EXTRACT_MAX_STEPS", tc.envMaxSteps)
			t.Setenv("LLM_EFFORT", tc.envEffort)
			path := writeCovenDarkConfig(t, covenDarkBaseConfig+tc.extraYAML)
			cfg, err := LoadConfigFile(path, "postgres://localhost/coven_test")
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
