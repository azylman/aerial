package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetEnv(t *testing.T) {
	if val := GetEnv("NON_EXISTENT_VAR_12345", "default_val"); val != "default_val" {
		t.Errorf("Expected default_val, got %s", val)
	}

	_ = os.Setenv("TEST_VAR_12345", "actual_val")
	defer func() { _ = os.Unsetenv("TEST_VAR_12345") }()

	if val := GetEnv("TEST_VAR_12345", "default_val"); val != "actual_val" {
		t.Errorf("Expected actual_val, got %s", val)
	}
}

func TestEnsureAgySettingsAndRules(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	// Test with API key
	if err := EnsureAgySettings("test_key", "test_model"); err != nil {
		t.Fatalf("EnsureAgySettings failed: %v", err)
	}

	settingsFile := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "settings.json")
	data, err := os.ReadFile(settingsFile)
	if err != nil {
		t.Fatalf("Expected settings.json to exist at %s: %v", settingsFile, err)
	}
	var parsedSettings map[string]interface{}
	if err := json.Unmarshal(data, &parsedSettings); err != nil {
		t.Fatalf("Failed to parse settings: %v", err)
	}
	if parsedSettings["modelProvider"] != "gemini" {
		t.Errorf("Expected modelProvider=gemini with apiKey, got %v", parsedSettings["modelProvider"])
	}

	// Test without API key (OAuth mode)
	if err := EnsureAgySettings("", "test_model_oauth"); err != nil {
		t.Fatalf("EnsureAgySettings failed without apiKey: %v", err)
	}
	data, err = os.ReadFile(settingsFile)
	if err != nil {
		t.Fatalf("Failed to read settings: %v", err)
	}
	parsedSettings = nil
	if err := json.Unmarshal(data, &parsedSettings); err != nil {
		t.Fatalf("Failed to parse settings: %v", err)
	}
	if _, ok := parsedSettings["modelProvider"]; ok {
		t.Errorf("Expected modelProvider to be deleted when apiKey is empty, but found: %v", parsedSettings["modelProvider"])
	}
	if parsedSettings["model"] != "test_model_oauth" {
		t.Errorf("Expected model=test_model_oauth, got %v", parsedSettings["model"])
	}

	if err := EnsureSystemRules("test custom prompt"); err != nil {
		t.Fatalf("EnsureSystemRules failed: %v", err)
	}

	rulesFile := filepath.Join(tmpDir, ".gemini", "rules", "system_instructions.md")
	if _, err := os.Stat(rulesFile); err != nil {
		t.Errorf("Expected system_instructions.md to exist at %s", rulesFile)
	}
}

func TestEnsureMcpConfigAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	rawJSON := json.RawMessage(`{"mcpServers": {"test": {"serverUrl": "http://localhost:1234"}}}`)
	if err := EnsureMcpConfig(rawJSON); err != nil {
		t.Fatalf("EnsureMcpConfig failed: %v", err)
	}

	cfgFile := filepath.Join(tmpDir, ".gemini", "config", "mcp_config.json")
	if _, err := os.Stat(cfgFile); err != nil {
		t.Errorf("Expected mcp_config.json to exist at %s", cfgFile)
	}

	mcpCfg := LoadMCPConfig()
	if len(mcpCfg) == 0 {
		t.Error("Expected non-empty MCP config")
	}
}

func TestEnsureConfigErrorCases(t *testing.T) {
	// Test empty or nil configs return nil without error
	if err := EnsureAgySettings("", ""); err != nil {
		t.Errorf("Expected nil error for empty apiKey, got %v", err)
	}
	if err := EnsureSystemRules(""); err != nil {
		t.Errorf("Expected nil error for empty customPrompt, got %v", err)
	}
	if err := EnsureMcpConfig(nil); err != nil {
		t.Errorf("Expected nil error for nil mcpConfig, got %v", err)
	}
	if err := EnsureMcpConfig(json.RawMessage(`""`)); err != nil {
		t.Errorf("Expected nil error for empty raw string, got %v", err)
	}

	// Test unwritable directory
	_ = os.Setenv("HOME", "/proc/unwritable_dir")
	if err := EnsureAgySettings("key", "model"); err == nil {
		t.Error("Expected error when writing settings to uncreated directory, got nil")
	}
	if err := EnsureSystemRules("prompt"); err == nil {
		t.Error("Expected error when writing rules to uncreated directory, got nil")
	}
	if err := EnsureMcpConfig(json.RawMessage(`{"test":1}`)); err == nil {
		t.Error("Expected error when writing mcp config to uncreated directory, got nil")
	}
}

func TestEnsureSystemRules_LKGCAndOrdering(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	oldSys := SystemGuidelinesSearchPaths
	oldAgent := AgentInstructionsSearchPaths
	SystemGuidelinesSearchPaths = []string{filepath.Join(tmpDir, "non_existent_system.md")}
	AgentInstructionsSearchPaths = []string{filepath.Join(tmpDir, "non_existent_agents.md")}
	defer func() {
		SystemGuidelinesSearchPaths = oldSys
		AgentInstructionsSearchPaths = oldAgent
	}()

	// Test writing initial instructions
	if err := EnsureSystemRules("initial instructions prompt"); err != nil {
		t.Fatalf("EnsureSystemRules failed: %v", err)
	}

	rulesFile := filepath.Join(tmpDir, ".gemini", "rules", "system_instructions.md")
	data, err := os.ReadFile(rulesFile)
	if err != nil {
		t.Fatalf("Failed to read generated rules: %v", err)
	}
	if !strings.Contains(string(data), "initial instructions prompt") {
		t.Errorf("Expected rules to contain 'initial instructions prompt', got: %s", string(data))
	}

	// Test LKGC fallback when calling with empty prompt and no files
	if err := EnsureSystemRules(""); err != nil {
		t.Fatalf("EnsureSystemRules with LKGC failed: %v", err)
	}

	dataAfter, err := os.ReadFile(rulesFile)
	if err != nil {
		t.Fatalf("Failed to read rules after LKGC fallback: %v", err)
	}
	if !strings.Contains(string(dataAfter), "initial instructions prompt") {
		t.Errorf("Expected rules to retain LKGC 'initial instructions prompt', got: %s", string(dataAfter))
	}
}

func TestWriteAtomic(t *testing.T) {
	tmpDir := t.TempDir()
	targetFile := filepath.Join(tmpDir, "sub", "config.json")

	content1 := `{"version": 1}`
	if err := writeAtomic(targetFile, content1); err != nil {
		t.Fatalf("writeAtomic failed: %v", err)
	}

	data, err := os.ReadFile(targetFile)
	if err != nil {
		t.Fatalf("Failed to read written atomic file: %v", err)
	}
	if string(data) != content1 {
		t.Errorf("Expected content %s, got %s", content1, string(data))
	}

	// Test overwriting
	content2 := `{"version": 2}`
	if err := writeAtomic(targetFile, content2); err != nil {
		t.Fatalf("writeAtomic overwrite failed: %v", err)
	}

	data2, err := os.ReadFile(targetFile)
	if err != nil {
		t.Fatalf("Failed to read overwritten atomic file: %v", err)
	}
	if string(data2) != content2 {
		t.Errorf("Expected content %s, got %s", content2, string(data2))
	}

	// Verify no temporary files remain in the directory
	entries, err := os.ReadDir(filepath.Dir(targetFile))
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp.") {
			t.Errorf("Found dangling tempfile: %s", entry.Name())
		}
	}
}

func TestLoadConfigValidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")

	yamlContent := `
model: "gemini-1.5-pro"
timeout_minutes: 45
timezone: "America/New_York"
system_channel: "my-alerts"
git_sync:
  enabled: true
  interval: "30s"
  config_repo_url: "https://github.com/example/repo.git"
  repositories:
    - "/custom/path1"
    - "/custom/path2"
mcp_servers:
  weather:
    serverUrl: "http://weather:8080/mcp"
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test yaml: %v", err)
	}

	cfg, err := LoadConfigFromPaths(yamlPath)
	if err != nil {
		t.Fatalf("LoadConfigFromPaths failed: %v", err)
	}

	if cfg.Model != "gemini-1.5-pro" {
		t.Errorf("Expected model 'gemini-1.5-pro', got %q", cfg.Model)
	}
	if cfg.TimeoutMinutes != 45 {
		t.Errorf("Expected timeout 45, got %d", cfg.TimeoutMinutes)
	}
	if cfg.Timezone != "America/New_York" {
		t.Errorf("Expected timezone 'America/New_York', got %q", cfg.Timezone)
	}
	if cfg.SystemChannel != "my-alerts" {
		t.Errorf("Expected system channel 'my-alerts', got %q", cfg.SystemChannel)
	}
	if !cfg.GitSync.Enabled || cfg.GitSync.Interval != "30s" || cfg.GitSync.ConfigRepoUrl != "https://github.com/example/repo.git" {
		t.Errorf("Unexpected GitSyncConfig: %+v", cfg.GitSync)
	}
	if len(cfg.GitSync.Repositories) != 2 || cfg.GitSync.Repositories[0] != "/custom/path1" {
		t.Errorf("Unexpected GitSync Repositories: %v", cfg.GitSync.Repositories)
	}
	if len(cfg.McpServers) != 1 || cfg.McpServers["weather"] == nil {
		t.Errorf("Unexpected McpServers: %v", cfg.McpServers)
	}

	// Verify getters
	if GetTimezone() != "America/New_York" {
		t.Errorf("GetTimezone expected 'America/New_York', got %q", GetTimezone())
	}
	if GetSystemChannel() != "my-alerts" {
		t.Errorf("GetSystemChannel expected 'my-alerts', got %q", GetSystemChannel())
	}
	rtCfg := GetRuntimeConfig()
	if rtCfg.Model != "gemini-1.5-pro" || rtCfg.TimeoutMinutes != 45 {
		t.Errorf("GetRuntimeConfig returned unexpected struct: %+v", rtCfg)
	}
}

func TestLoadConfigCorruptedYAML_LKGCFallback(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")

	// 1. Write initial valid YAML
	validYAML := `
model: "gemini-2.5-flash"
timeout_minutes: 20
system_channel: "dev-channel-1"
`
	if err := os.WriteFile(yamlPath, []byte(validYAML), 0644); err != nil {
		t.Fatalf("Failed to write valid yaml: %v", err)
	}

	cfg, err := LoadConfigFromPaths(yamlPath)
	if err != nil {
		t.Fatalf("Initial LoadConfigFromPaths failed: %v", err)
	}
	if cfg.Model != "gemini-2.5-flash" || cfg.TimeoutMinutes != 20 {
		t.Fatalf("Unexpected initial config: %+v", cfg)
	}

	// 2. Corrupt YAML with invalid syntax
	corruptYAML := `
model: [broken yaml invalid syntax: ::: {
timeout_minutes:
`
	if err := os.WriteFile(yamlPath, []byte(corruptYAML), 0644); err != nil {
		t.Fatalf("Failed to overwrite with corrupt yaml: %v", err)
	}

	cfgAfter, errAfter := LoadConfigFromPaths(yamlPath)
	if errAfter == nil {
		t.Fatal("Expected error on corrupted YAML, got nil")
	}

	// Verify LKGC is retained
	if cfgAfter.Model != "gemini-2.5-flash" || cfgAfter.TimeoutMinutes != 20 {
		t.Errorf("Expected retained LKGC model 'gemini-2.5-flash' and timeout 20, got model=%q timeout=%d",
			cfgAfter.Model, cfgAfter.TimeoutMinutes)
	}
	rtCfg := GetRuntimeConfig()
	if rtCfg.Model != "gemini-2.5-flash" || rtCfg.SystemChannel != "dev-channel-1" {
		t.Errorf("Expected GetRuntimeConfig() to retain LKGC, got %+v", rtCfg)
	}
}

func TestLoadConfigEnvInterpolation(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")

	t.Setenv("TEST_EXPAND_MODEL", "interpolated-gemini-model")
	t.Setenv("TEST_EXPAND_CHAN", "interpolated-channel")
	t.Setenv("TEST_EXPAND_REPO", "https://github.com/interpolated/repo.git")

	yamlContent := `
model: "${TEST_EXPAND_MODEL}"
system_channel: "${TEST_EXPAND_CHAN}"
git_sync:
  config_repo_url: "${TEST_EXPAND_REPO}"
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}

	cfg, err := LoadConfigFromPaths(yamlPath)
	if err != nil {
		t.Fatalf("LoadConfigFromPaths failed: %v", err)
	}

	if cfg.Model != "interpolated-gemini-model" {
		t.Errorf("Expected interpolated model 'interpolated-gemini-model', got %q", cfg.Model)
	}
	if cfg.SystemChannel != "interpolated-channel" {
		t.Errorf("Expected interpolated system_channel 'interpolated-channel', got %q", cfg.SystemChannel)
	}
	if cfg.GitSync.ConfigRepoUrl != "https://github.com/interpolated/repo.git" {
		t.Errorf("Expected interpolated repo url, got %q", cfg.GitSync.ConfigRepoUrl)
	}
}

func TestLoadConfigMissingFileFallbacks(t *testing.T) {
	// Reset runtime config to clean defaults
	runtimeConfigMu.Lock()
	currentRuntimeConfig = Config{}
	runtimeConfigMu.Unlock()

	t.Setenv("AGY_MODEL", "env-model-fallback")
	t.Setenv("TIMEOUT_MINUTES", "28")
	t.Setenv("DEFAULT_TIMEZONE", "Europe/London")
	t.Setenv("SYSTEM_CHANNEL", "env-system-chan")

	cfg, err := LoadConfigFromPaths("/non/existent/file/for/sure/config.yaml")
	if err != nil {
		t.Fatalf("LoadConfigFromPaths failed for missing file: %v", err)
	}

	if cfg.Model != "env-model-fallback" {
		t.Errorf("Expected fallback to env model 'env-model-fallback', got %q", cfg.Model)
	}
	if cfg.TimeoutMinutes != 28 {
		t.Errorf("Expected fallback timeout 28, got %d", cfg.TimeoutMinutes)
	}
	if cfg.Timezone != "Europe/London" {
		t.Errorf("Expected fallback timezone 'Europe/London', got %q", cfg.Timezone)
	}
	if cfg.SystemChannel != "env-system-chan" {
		t.Errorf("Expected fallback channel 'env-system-chan', got %q", cfg.SystemChannel)
	}
}

func TestLoadMCPConfig_MergeCustomWithDefaults(t *testing.T) {
	// Set custom mcp_servers in runtime config
	runtimeConfigMu.Lock()
	currentRuntimeConfig = Config{
		Model: "Gemini 3.6 Flash (Low)",
		McpServers: map[string]json.RawMessage{
			"brave-search": json.RawMessage(`{"serverUrl":"http://brave-mcp:4005/mcp"}`),
			"custom-api":   json.RawMessage(`{"serverUrl":"https://mcp.example.com/mcp","headers":{"Authorization":"Bearer secret123"}}`),
		},
	}
	runtimeConfigMu.Unlock()

	raw := LoadMCPConfig()
	var res struct {
		McpServers map[string]interface{} `json:"mcpServers"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("Failed to unmarshal LoadMCPConfig output: %v", err)
	}

	// Must contain default servers
	if _, ok := res.McpServers["discord"]; !ok {
		t.Errorf("Expected built-in 'discord' server in merged config")
	}
	if _, ok := res.McpServers["scheduler"]; !ok {
		t.Errorf("Expected built-in 'scheduler' server in merged config")
	}
	if _, ok := res.McpServers["docker"]; !ok {
		t.Errorf("Expected built-in 'docker' server in merged config")
	}

	// Must contain custom servers
	if _, ok := res.McpServers["brave-search"]; !ok {
		t.Errorf("Expected custom 'brave-search' server in merged config")
	}
	if _, ok := res.McpServers["custom-api"]; !ok {
		t.Errorf("Expected custom 'custom-api' server in merged config")
	}
}

