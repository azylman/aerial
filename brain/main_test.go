package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestParseSkillFrontmatter(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Create a skill directory with valid frontmatter
	skillDir := filepath.Join(tmpDir, "test-skill")
	if err := os.MkdirAll(skillDir, 0755); err != nil {
		t.Fatalf("Failed to create temp skill dir: %v", err)
	}

	skillContent := `---
name: custom-test-skill
description: Useful operational procedures for automated testing.
---

# Title
Some skill body content.
`
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte(skillContent), 0644); err != nil {
		t.Fatalf("Failed to write SKILL.md: %v", err)
	}

	info, err := parseSkillFrontmatter(skillDir)
	if err != nil {
		t.Fatalf("parseSkillFrontmatter returned unexpected error: %v", err)
	}

	if info.Name != "custom-test-skill" {
		t.Errorf("Expected skill name 'custom-test-skill', got: '%s'", info.Name)
	}
	if info.Description != "Useful operational procedures for automated testing." {
		t.Errorf("Expected skill description 'Useful operational procedures for automated testing.', got: '%s'", info.Description)
	}

	// 2. Test missing SKILL.md error handling
	_, err = parseSkillFrontmatter(filepath.Join(tmpDir, "nonexistent"))
	if err == nil {
		t.Errorf("Expected error when parsing non-existent skill path, got nil")
	}
}

func TestGetEnv(t *testing.T) {
	t.Setenv("AERIAL_TEST_VAR", "hello_world")
	if val := getEnv("AERIAL_TEST_VAR", "default"); val != "hello_world" {
		t.Errorf("Expected 'hello_world', got '%s'", val)
	}

	if val := getEnv("AERIAL_NONEXISTENT_VAR", "default_val"); val != "default_val" {
		t.Errorf("Expected 'default_val', got '%s'", val)
	}
}

func TestLoadMCPConfigEnvExpansion(t *testing.T) {
	t.Setenv("TEST_DISCORD_URL", "http://custom-discord:4001/mcp")

	rawConfig := []byte(`{
		"mcpServers": {
			"discord": {
				"serverUrl": "${TEST_DISCORD_URL}"
			}
		}
	}`)

	// Test os.ExpandEnv directly on config string
	expanded := os.ExpandEnv(string(rawConfig))
	var configStruct struct {
		MCPServers map[string]struct {
			ServerURL string `json:"serverUrl"`
		} `json:"mcpServers"`
	}

	if err := json.Unmarshal([]byte(expanded), &configStruct); err != nil {
		t.Fatalf("Failed to unmarshal expanded JSON: %v", err)
	}

	gotURL := configStruct.MCPServers["discord"].ServerURL
	expectedURL := "http://custom-discord:4001/mcp"
	if gotURL != expectedURL {
		t.Errorf("Expected expanded URL '%s', got '%s'", expectedURL, gotURL)
	}
}
