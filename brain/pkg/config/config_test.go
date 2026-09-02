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
channels:
  default:
    mode: "threads"
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
channels:
  default:
    mode: "threads"
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
channels:
  default:
    mode: "threads"
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

func TestChannelPolicy_Parsing(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")

	yamlContent := `
model: "gemini-2.5-flash"
admin_users:
  - "169260920550195200"
  - "999888777666"
channels:
  default:
    mode: "threads"
    typing_indicator: "always"
    ignore_bots: true
    allow_system_ops: false
    max_session_turns: 0
  aerial-dev:
    mode: "threads"
    typing_indicator: "always"
    ignore_bots: true
    allow_system_ops: true
  general:
    mode: "channel"
    typing_indicator: "on_mention"
    ignore_bots: true
    allow_system_ops: false
    max_session_turns: 50
  "123456789012345678":
    mode: "channel"
    typing_indicator: "never"
    ignore_bots: false
    allow_system_ops: true
    max_session_turns: 20
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}

	cfg, err := LoadConfigFromPaths(yamlPath)
	if err != nil {
		t.Fatalf("LoadConfigFromPaths failed: %v", err)
	}

	if len(cfg.AdminUsers) != 2 || cfg.AdminUsers[0] != "169260920550195200" || cfg.AdminUsers[1] != "999888777666" {
		t.Errorf("Unexpected AdminUsers: %v", cfg.AdminUsers)
	}

	if len(cfg.Channels) != 4 {
		t.Fatalf("Expected 4 channel policies, got %d", len(cfg.Channels))
	}

	def := cfg.Channels["default"]
	if def.Mode != "threads" || def.TypingIndicator != "always" || !def.IgnoreBots || def.AllowSystemOps || def.MaxSessionTurns != 0 {
		t.Errorf("Unexpected default policy: %+v", def)
	}

	dev := cfg.Channels["aerial-dev"]
	if dev.Mode != "threads" || dev.TypingIndicator != "always" || !dev.AllowSystemOps {
		t.Errorf("Unexpected aerial-dev policy: %+v", dev)
	}

	gen := cfg.Channels["general"]
	if gen.Mode != "channel" || gen.TypingIndicator != "on_mention" || gen.MaxSessionTurns != 50 {
		t.Errorf("Unexpected general policy: %+v", gen)
	}

	sn := cfg.Channels["123456789012345678"]
	if sn.Mode != "channel" || sn.TypingIndicator != "never" || sn.IgnoreBots || !sn.AllowSystemOps || sn.MaxSessionTurns != 20 {
		t.Errorf("Unexpected snowflake channel policy: %+v", sn)
	}
}

func TestChannelPolicy_Validation_MissingDefault(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Missing channels entirely
	yamlPath1 := filepath.Join(tmpDir, "config1.yaml")
	yamlNoChannels := `
model: "gemini-2.5-flash"
`
	if err := os.WriteFile(yamlPath1, []byte(yamlNoChannels), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}
	_, err := LoadConfigFromPaths(yamlPath1)
	if err == nil {
		t.Error("Expected error when channels.default is missing, got nil")
	}

	// 2. channels present but no default
	yamlPath2 := filepath.Join(tmpDir, "config2.yaml")
	yamlNoDefault := `
model: "gemini-2.5-flash"
channels:
  general:
    mode: "channel"
`
	if err := os.WriteFile(yamlPath2, []byte(yamlNoDefault), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}
	_, err = LoadConfigFromPaths(yamlPath2)
	if err == nil {
		t.Error("Expected error when channels.default is missing from channels map, got nil")
	}
}

func TestChannelPolicy_Validation_InvalidMode(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")

	yamlInvalidMode := `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "unsupported_mode"
`
	if err := os.WriteFile(yamlPath, []byte(yamlInvalidMode), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}
	_, err := LoadConfigFromPaths(yamlPath)
	if err == nil {
		t.Error("Expected error when channels.default has invalid mode, got nil")
	}
}

func TestChannelPolicy_DefaultsAndNormalization(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Test default in threads mode gets typing_indicator: "always"
	yamlPath1 := filepath.Join(tmpDir, "config_threads.yaml")
	yamlThreads := `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`
	if err := os.WriteFile(yamlPath1, []byte(yamlThreads), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}
	cfg, err := LoadConfigFromPaths(yamlPath1)
	if err != nil {
		t.Fatalf("LoadConfigFromPaths failed: %v", err)
	}
	if cfg.Channels["default"].TypingIndicator != "always" {
		t.Errorf("Expected default typing_indicator='always' for threads mode, got %q", cfg.Channels["default"].TypingIndicator)
	}

	// 2. Test default in channel mode gets typing_indicator: "on_mention" and max_session_turns: 50
	yamlPath2 := filepath.Join(tmpDir, "config_channel.yaml")
	yamlChannel := `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "channel"
`
	if err := os.WriteFile(yamlPath2, []byte(yamlChannel), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}
	cfg, err = LoadConfigFromPaths(yamlPath2)
	if err != nil {
		t.Fatalf("LoadConfigFromPaths failed: %v", err)
	}
	if cfg.Channels["default"].TypingIndicator != "on_mention" {
		t.Errorf("Expected default typing_indicator='on_mention' for channel mode, got %q", cfg.Channels["default"].TypingIndicator)
	}
	if cfg.Channels["default"].MaxSessionTurns != 50 {
		t.Errorf("Expected default max_session_turns=50 for channel mode, got %d", cfg.Channels["default"].MaxSessionTurns)
	}
}

func TestResolveChannelPolicy(t *testing.T) {
	cfg := Config{
		Channels: map[string]ChannelPolicy{
			"default": {
				Mode:            "threads",
				TypingIndicator: "always",
				IgnoreBots:      true,
				AllowSystemOps:  false,
				MaxSessionTurns: 0,
			},
			"general": {
				Mode: "channel",
			},
			"aerial-dev": {
				AllowSystemOps: true,
			},
			"1543668253363150928": {
				Mode:            "channel",
				TypingIndicator: "never",
				AllowSystemOps:  true,
				MaxSessionTurns: 100,
			},
		},
	}

	// 1. Match by Snowflake ID
	p1 := cfg.ResolveChannelPolicy("1543668253363150928", "aerial-dev")
	if p1.Mode != "channel" || p1.TypingIndicator != "never" || !p1.AllowSystemOps || p1.MaxSessionTurns != 100 || !p1.IgnoreBots {
		t.Errorf("Unexpected policy for snowflake ID match: %+v", p1)
	}

	// 2. Match by Channel Name with leading '#' and uppercase
	p2 := cfg.ResolveChannelPolicy("999999", "#General")
	if p2.Mode != "channel" || p2.TypingIndicator != "on_mention" || p2.MaxSessionTurns != 50 || !p2.IgnoreBots || p2.AllowSystemOps {
		t.Errorf("Unexpected policy for #General match: %+v", p2)
	}

	// 3. Match by Channel Name without '#'
	p3 := cfg.ResolveChannelPolicy("999999", "aerial-dev")
	if p3.Mode != "threads" || p3.TypingIndicator != "always" || !p3.AllowSystemOps || !p3.IgnoreBots {
		t.Errorf("Unexpected policy for aerial-dev match with default inheritance: %+v", p3)
	}

	// 4. Fallback to default when neither ID nor name match
	p4 := cfg.ResolveChannelPolicy("999999", "unknown-channel")
	if p4.Mode != "threads" || p4.TypingIndicator != "always" || p4.AllowSystemOps || !p4.IgnoreBots {
		t.Errorf("Unexpected policy for fallback: %+v", p4)
	}
}

func TestIsAdmin(t *testing.T) {
	cfg := Config{
		AdminUsers: []string{"1542035925603713086", "arcane103", "@Alex"},
	}

	// 1. Exact snowflake ID
	if !cfg.IsAdmin("1542035925603713086") {
		t.Errorf("Expected snowflake ID to be admin")
	}

	// 2. Exact username
	if !cfg.IsAdmin("arcane103") {
		t.Errorf("Expected arcane103 to be admin")
	}

	// 3. Username with @ prefix and case variations
	if !cfg.IsAdmin("@Arcane103") {
		t.Errorf("Expected @Arcane103 to be admin")
	}
	if !cfg.IsAdmin("alex") {
		t.Errorf("Expected alex to be admin matching @Alex")
	}
	if !cfg.IsAdmin("@ALEX") {
		t.Errorf("Expected @ALEX to be admin")
	}

	// 4. Variadic check (ID, Username, GlobalName) where one matches
	if !cfg.IsAdmin("some-random-id", "arcane103", "Alex") {
		t.Errorf("Expected variadic check with matching username to be admin")
	}

	// 5. Non-admins and empty/whitespace
	if cfg.IsAdmin("regular-user") {
		t.Errorf("Expected regular-user NOT to be admin")
	}
	if cfg.IsAdmin("", "  ", "@") {
		t.Errorf("Expected empty/whitespace/bare @ NOT to be admin")
	}
	if cfg.IsAdmin() {
		t.Errorf("Expected empty variadic call NOT to be admin")
	}
}

func TestChannelPolicy_IsIgnored(t *testing.T) {
	cases := []struct {
		mode     string
		expected bool
	}{
		{"ignore", true},
		{"disabled", true},
		{"IGNORE", true},
		{"DISABLED", true},
		{" Ignore ", true},
		{" Disabled ", true},
		{"threads", false},
		{"channel", false},
		{"", false},
	}

	for _, tc := range cases {
		p := ChannelPolicy{Mode: tc.mode}
		if p.IsIgnored() != tc.expected {
			t.Errorf("ChannelPolicy{Mode: %q}.IsIgnored() = %v, expected %v", tc.mode, p.IsIgnored(), tc.expected)
		}
	}
}

func TestChannelPolicy_IgnoredMode_YAML(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config_ignored_channels.yaml")

	yamlContent := `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
  random:
    mode: "ignore"
  "123456789":
    mode: "ignore"
  disabled-channel:
    mode: "disabled"
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}

	cfg, err := LoadConfigFromPaths(yamlPath)
	if err != nil {
		t.Fatalf("LoadConfigFromPaths failed: %v", err)
	}

	// Verify ignored channels with mode "ignore"
	pRandom := cfg.ResolveChannelPolicy("99999", "random")
	if !pRandom.IsIgnored() || pRandom.Mode != "ignore" {
		t.Errorf("Expected 'random' channel to be ignored, got %+v", pRandom)
	}

	pSnowflake := cfg.ResolveChannelPolicy("123456789", "some-name")
	if !pSnowflake.IsIgnored() || pSnowflake.Mode != "ignore" {
		t.Errorf("Expected snowflake '123456789' to be ignored, got %+v", pSnowflake)
	}

	pDisabled := cfg.ResolveChannelPolicy("77777", "disabled-channel")
	if !pDisabled.IsIgnored() || pDisabled.Mode != "disabled" {
		t.Errorf("Expected 'disabled-channel' to be ignored, got %+v", pDisabled)
	}
	pOther := cfg.ResolveChannelPolicy("66666", "general")
	if pOther.IsIgnored() || pOther.Mode != "threads" {
		t.Errorf("Expected 'general' channel to not be ignored, got %+v", pOther)
	}
}

func TestChannelPolicy_DefaultDeny_IgnoreMode(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config_default_ignore.yaml")

	yamlContent := `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "ignore"
  aerial-dev:
    mode: "threads"
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}

	cfg, err := LoadConfigFromPaths(yamlPath)
	if err != nil {
		t.Fatalf("LoadConfigFromPaths failed with default mode: ignore: %v", err)
	}

	// Default channel should be ignored
	defPolicy := cfg.Channels["default"]
	if !defPolicy.IsIgnored() || defPolicy.Mode != "ignore" {
		t.Errorf("Expected default policy mode=ignore, got %+v", defPolicy)
	}

	// Unmatched channel should inherit default deny / ignore
	pUnmatched := cfg.ResolveChannelPolicy("11111", "random-general")
	if !pUnmatched.IsIgnored() {
		t.Errorf("Expected unmatched channel to be ignored under default-deny, got %+v", pUnmatched)
	}

	// Explicit override should work
	pDev := cfg.ResolveChannelPolicy("22222", "aerial-dev")
	if pDev.IsIgnored() || pDev.Mode != "threads" {
		t.Errorf("Expected aerial-dev to be threads mode and not ignored, got %+v", pDev)
	}
}

func TestChannelPolicy_Validation_InvalidNonDefaultMode(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config_invalid_nondefault_mode.yaml")

	yamlContent := `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
  some-channel:
    mode: "invalid_mode_name"
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}

	_, err := LoadConfigFromPaths(yamlPath)
	if err == nil {
		t.Fatalf("Expected error for invalid channel mode, got nil")
	}
	if !strings.Contains(err.Error(), "mode must be 'threads', 'channel', 'ignore', or 'disabled'") {
		t.Errorf("Expected descriptive mode validation error, got: %v", err)
	}
}





