package config

import (
	"encoding/json"
	"os"
	"path/filepath"
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

	if err := EnsureAgySettings("test_key", "test_model"); err != nil {
		t.Fatalf("EnsureAgySettings failed: %v", err)
	}

	settingsFile := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "settings.json")
	if _, err := os.Stat(settingsFile); err != nil {
		t.Errorf("Expected settings.json to exist at %s", settingsFile)
	}

	if err := EnsureSystemRules("test custom prompt"); err != nil {
		t.Fatalf("EnsureSystemRules failed: %v", err)
	}

	rulesFile := filepath.Join(tmpDir, ".gemini", "rules", "user_override.md")
	if _, err := os.Stat(rulesFile); err != nil {
		t.Errorf("Expected user_override.md to exist at %s", rulesFile)
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
