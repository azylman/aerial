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


