package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigFile_EriKeysAndEnv(t *testing.T) {
	tmpDir := t.TempDir()

	legacyYAML := `port: '4008'
brain_url: 'http://legacy.brain:8080'
brain_llm_url: 'http://legacy.brain:8080/internal/llm'
brain_pr_register_url: 'http://legacy.synapse:4020/internal/pr/register'
custom_skills_dir: '/custom/skills'
rules_dir: '/custom/rules'
config_repo_owner: 'azylman'
config_repo_name: 'aerial-config'
ollama:
  base_url: 'http://ollama:11434'
  model: 'all-minilm'
  num_ctx: 512
`
	legacyFile := filepath.Join(tmpDir, "legacy.yaml")
	if err := os.WriteFile(legacyFile, []byte(legacyYAML), 0o644); err != nil {
		t.Fatalf("failed to write legacy config: %v", err)
	}

	eriYAML := `port: '4008'
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
	eriFile := filepath.Join(tmpDir, "eri.yaml")
	if err := os.WriteFile(eriFile, []byte(eriYAML), 0o644); err != nil {
		t.Fatalf("failed to write eri config: %v", err)
	}

	tests := []struct {
		name          string
		path          string
		envEriURL     string
		envEriLLMURL  string
		envPRRegURL   string
		wantEriURL    string
		wantEriLLMURL string
		wantPRRegURL  string
	}{
		{
			name:          "legacy yaml loaded and copied to eri fields",
			path:          legacyFile,
			wantEriURL:    "http://legacy.brain:8080",
			wantEriLLMURL: "http://legacy.brain:8080/internal/llm",
			wantPRRegURL:  "http://legacy.synapse:4020/internal/pr/register",
		},
		{
			name:          "eri yaml loaded and copied to both eri and brain fields",
			path:          eriFile,
			wantEriURL:    "http://eri.aerial",
			wantEriLLMURL: "http://eri.aerial/internal/llm",
			wantPRRegURL:  "http://synapse.aerial/internal/pr/register",
		},
		{
			name:          "env overrides take precedence over yaml",
			path:          eriFile,
			envEriURL:     "http://env.eri:8088",
			envEriLLMURL:  "http://env.eri:8088/internal/llm",
			envPRRegURL:   "http://env.synapse:4020/internal/pr/register",
			wantEriURL:    "http://env.eri:8088",
			wantEriLLMURL: "http://env.eri:8088/internal/llm",
			wantPRRegURL:  "http://env.synapse:4020/internal/pr/register",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://localhost/testdb")
			t.Setenv("ERI_URL", tc.envEriURL)
			t.Setenv("ERI_LLM_URL", tc.envEriLLMURL)
			t.Setenv("PR_REGISTER_URL", tc.envPRRegURL)

			cfg, err := LoadConfig(tc.path)
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if cfg.EriURL != tc.wantEriURL || cfg.BrainURL != tc.wantEriURL {
				t.Errorf("EriURL = %q, BrainURL = %q, want %q", cfg.EriURL, cfg.BrainURL, tc.wantEriURL)
			}
			if cfg.EriLLMURL != tc.wantEriLLMURL || cfg.BrainLLMURL != tc.wantEriLLMURL {
				t.Errorf("EriLLMURL = %q, BrainLLMURL = %q, want %q", cfg.EriLLMURL, cfg.BrainLLMURL, tc.wantEriLLMURL)
			}
			if cfg.PRRegisterURL != tc.wantPRRegURL || cfg.BrainPRRegisterURL != tc.wantPRRegURL {
				t.Errorf("PRRegisterURL = %q, BrainPRRegisterURL = %q, want %q", cfg.PRRegisterURL, cfg.BrainPRRegisterURL, tc.wantPRRegURL)
			}
		})
	}
}
