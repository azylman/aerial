package env

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/brain/pkg/config"
)

func TestSyncSettings_APIKeyAndOAuth(t *testing.T) {
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())

	// 1. API Key mode
	if err := p.SyncSettings("test-api-key", "gemini-2.5-flash"); err != nil {
		t.Fatalf("SyncSettings with API key failed: %v", err)
	}

	settingsPath := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "settings.json")
	data, err := os.ReadFile(settingsPath)
	if err != nil {
		t.Fatalf("Failed to read settings.json: %v", err)
	}

	var s map[string]interface{}
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("Failed to parse settings.json: %v", err)
	}

	if s["apiKey"] != "test-api-key" {
		t.Errorf("Expected apiKey 'test-api-key', got '%v'", s["apiKey"])
	}
	if s["modelProvider"] != "gemini" {
		t.Errorf("Expected modelProvider 'gemini', got '%v'", s["modelProvider"])
	}
	if s["model"] != "gemini-2.5-flash" {
		t.Errorf("Expected model 'gemini-2.5-flash', got '%v'", s["model"])
	}

	// 2. OAuth mode (empty API key): modelProvider must be purged
	if err := p.SyncSettings("", "gemini-2.5-pro"); err != nil {
		t.Fatalf("SyncSettings in OAuth mode failed: %v", err)
	}

	data, err = os.ReadFile(settingsPath)
	if err != nil {
		t.Fatalf("Failed to read settings.json after OAuth sync: %v", err)
	}
	s = make(map[string]interface{})
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("Failed to parse settings.json after OAuth sync: %v", err)
	}

	if _, exists := s["apiKey"]; exists {
		t.Errorf("Expected apiKey to be omitted in OAuth mode, got '%v'", s["apiKey"])
	}
	if _, exists := s["modelProvider"]; exists {
		t.Errorf("Expected modelProvider to be purged in OAuth mode, got '%v'", s["modelProvider"])
	}
	if s["model"] != "gemini-2.5-pro" {
		t.Errorf("Expected model 'gemini-2.5-pro', got '%v'", s["model"])
	}
}

func TestSyncSettings_CorruptedFileRecovery(t *testing.T) {
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())

	settingsDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli")
	_ = os.MkdirAll(settingsDir, 0755)
	settingsPath := filepath.Join(settingsDir, "settings.json")
	_ = os.WriteFile(settingsPath, []byte("NOT_JSON_AT_ALL"), 0644)

	if err := p.SyncSettings("key123", "model123"); err != nil {
		t.Fatalf("SyncSettings failed to heal corrupted settings: %v", err)
	}

	data, err := os.ReadFile(settingsPath)
	if err != nil {
		t.Fatalf("Failed to read settings.json: %v", err)
	}
	var s map[string]interface{}
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("Failed to parse healed settings.json: %v", err)
	}
	if s["apiKey"] != "key123" {
		t.Errorf("Expected apiKey 'key123', got '%v'", s["apiKey"])
	}
}

func TestSyncRules_AndLKGC(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	agentsDir := t.TempDir()
	agentsFile := filepath.Join(agentsDir, "AGENTS.md")
	_ = os.WriteFile(agentsFile, []byte("Tone: Direct and technical"), 0644)
	p.SetAgentInstructionsSearchPaths([]string{agentsFile})

	// 1. Initial sync with persona and custom prompt
	if err := p.SyncRules("Custom System Prompt"); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}

	ruleFile := filepath.Join(tmpHome, ".gemini", "rules", "user_persona.md")
	content, err := os.ReadFile(ruleFile)
	if err != nil {
		t.Fatalf("Failed to read user_persona.md: %v", err)
	}
	ruleStr := string(content)
	if !strings.Contains(ruleStr, "Tone: Direct and technical") {
		t.Errorf("Expected persona content in user_persona.md, got: %s", ruleStr)
	}
	if !strings.Contains(ruleStr, "Custom System Prompt") {
		t.Errorf("Expected custom prompt in user_persona.md, got: %s", ruleStr)
	}

	// Also verify config rules directory copy
	configRuleFile := filepath.Join(tmpHome, ".gemini", "config", "rules", "user_persona.md")
	if _, err := os.Stat(configRuleFile); err != nil {
		t.Errorf("Expected config rules copy at %s: %v", configRuleFile, err)
	}

	// 2. Torn read: empty file engages LKGC
	_ = os.WriteFile(agentsFile, []byte(""), 0644)
	if err := p.SyncRules("Updated Prompt"); err != nil {
		t.Fatalf("SyncRules failed during torn read: %v", err)
	}

	content, err = os.ReadFile(ruleFile)
	if err != nil {
		t.Fatalf("Failed to read user_persona.md after torn read: %v", err)
	}
	ruleStr = string(content)
	if !strings.Contains(ruleStr, "Tone: Direct and technical") {
		t.Errorf("Expected LKGC persona to persist across torn read, got: %s", ruleStr)
	}

	// 3. Stale rule file cleanup
	staleFile := filepath.Join(tmpHome, ".gemini", "rules", "system.md")
	_ = os.WriteFile(staleFile, []byte("stale"), 0644)
	if err := p.SyncRules("Prompt"); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}
	if _, err := os.Stat(staleFile); !os.IsNotExist(err) {
		t.Errorf("Expected stale rule file %s to be deleted", staleFile)
	}
}

func TestSyncMCP(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	customServers := map[string]json.RawMessage{
		"my-custom-mcp": json.RawMessage(`{"serverUrl":"http://my-host:9000/sse"}`),
	}
	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.McpServers = customServers
	})

	if err := p.SyncMCP(context.Background(), cfg); err != nil {
		t.Fatalf("SyncMCP failed: %v", err)
	}

	mcpPath := filepath.Join(tmpHome, ".gemini", "config", "mcp_config.json")
	data, err := os.ReadFile(mcpPath)
	if err != nil {
		t.Fatalf("Failed to read mcp_config.json: %v", err)
	}

	var root struct {
		McpServers map[string]struct {
			ServerURL string `json:"serverUrl"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("Failed to parse mcp_config.json: %v", err)
	}

	// Verify built-ins
	if _, ok := root.McpServers["scheduler"]; !ok {
		t.Errorf("Missing built-in 'scheduler' MCP server")
	}
	if _, ok := root.McpServers["discord"]; !ok {
		t.Errorf("Missing built-in 'discord' MCP server")
	}

	// Verify custom server was merged and /sse normalized to /mcp
	custom, ok := root.McpServers["my-custom-mcp"]
	if !ok {
		t.Fatalf("Missing custom MCP server 'my-custom-mcp'")
	}
	if custom.ServerURL != "http://my-host:9000/sse" {
		t.Errorf("Expected URL 'http://my-host:9000/sse', got '%s'", custom.ServerURL)
	}
}

func TestSyncSkills(t *testing.T) {
	tmpHome := t.TempDir()
	tmpCustomSkills := t.TempDir()
	tmpSuperpowers := t.TempDir()

	p := New(tmpHome, t.TempDir())
	p.SetCustomSkillsDir(tmpCustomSkills)
	p.SetSuperpowersDir(tmpSuperpowers)

	// Create a test skill
	customSkillDir := filepath.Join(tmpCustomSkills, "test-skill")
	_ = os.MkdirAll(customSkillDir, 0755)
	_ = os.WriteFile(filepath.Join(customSkillDir, "SKILL.md"), []byte("# Test Skill"), 0644)

	if err := p.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills failed: %v", err)
	}

	targetDir := filepath.Join(tmpHome, ".gemini", "skills", "test-skill")
	targetSkillMD := filepath.Join(targetDir, "SKILL.md")
	if _, err := os.Stat(targetSkillMD); err != nil {
		t.Fatalf("Expected symlinked skill at %s: %v", targetSkillMD, err)
	}

	// Test orphaned symlink cleanup
	_ = os.RemoveAll(customSkillDir)
	if err := p.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills after skill deletion failed: %v", err)
	}

	if _, err := os.Lstat(targetDir); !os.IsNotExist(err) {
		t.Errorf("Expected orphaned symlink %s to be removed by sweeper", targetDir)
	}
}

func TestSyncRules_Concurrent(t *testing.T) {
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())

	agentsFile := filepath.Join(t.TempDir(), "AGENTS.md")
	_ = os.WriteFile(agentsFile, []byte("Concurrent persona instructions"), 0644)
	p.SetAgentInstructionsSearchPaths([]string{agentsFile})

	var wg sync.WaitGroup
	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := p.SyncRules("iteration prompt"); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("Concurrent SyncRules error: %v", err)
	}

	ruleFile := filepath.Join(tmpHome, ".gemini", "rules", "user_persona.md")
	data, err := os.ReadFile(ruleFile)
	if err != nil {
		t.Fatalf("Failed to read user_persona.md: %v", err)
	}
	if !strings.Contains(string(data), "Concurrent persona instructions") {
		t.Errorf("Expected persona in user_persona.md, got: %s", string(data))
	}
}

func TestSyncMCP_EdgeCases(t *testing.T) {
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())

	// 1. Nil, empty, and string raw configs return nil
	if err := p.EnsureMcpConfig(nil); err != nil {
		t.Errorf("Expected nil error for nil config, got: %v", err)
	}
	if err := p.EnsureMcpConfig(json.RawMessage("")); err != nil {
		t.Errorf("Expected nil error for empty config, got: %v", err)
	}
	if err := p.EnsureMcpConfig(json.RawMessage(`""`)); err != nil {
		t.Errorf("Expected nil error for empty string, got: %v", err)
	}
	if err := p.EnsureMcpConfig(json.RawMessage("null")); err != nil {
		t.Errorf("Expected nil error for null config, got: %v", err)
	}

	// 2. Raw string-encoded JSON
	strJSON := json.RawMessage(`"{\"mcpServers\":{\"from-str\":{\"serverUrl\":\"http://localhost:5000\"}}}"`)
	if err := p.EnsureMcpConfig(strJSON); err != nil {
		t.Fatalf("EnsureMcpConfig failed with string-encoded JSON: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(tmpHome, ".gemini", "config", "mcp_config.json"))
	if err != nil {
		t.Fatalf("Failed to read mcp_config.json: %v", err)
	}
	if !strings.Contains(string(data), "from-str") {
		t.Errorf("Expected 'from-str' in mcp_config.json, got: %s", string(data))
	}
}

func TestProvisioner_ZeroAmbientDefaultsAndNoOps(t *testing.T) {
	// 1. Verify New strictly sets given values without ambient fallbacks
	pEmpty := New("", "")
	if pEmpty.HomeDir() != "" {
		t.Errorf("Expected empty HomeDir, got %q", pEmpty.HomeDir())
	}
	if pEmpty.DataDir() != "" {
		t.Errorf("Expected empty DataDir, got %q", pEmpty.DataDir())
	}

	// 2. Verify all operations return nil when homeDir == ""
	if err := pEmpty.Sync(context.Background(), nil); err != nil {
		t.Errorf("Expected nil error from Sync on empty homeDir, got: %v", err)
	}
	if err := pEmpty.SyncSettings("key", "model"); err != nil {
		t.Errorf("Expected nil error from SyncSettings on empty homeDir, got: %v", err)
	}
	if err := pEmpty.SyncRules("prompt"); err != nil {
		t.Errorf("Expected nil error from SyncRules on empty homeDir, got: %v", err)
	}
	if err := pEmpty.SyncMCP(context.Background(), nil); err != nil {
		t.Errorf("Expected nil error from SyncMCP on empty homeDir, got: %v", err)
	}
	if err := pEmpty.EnsureMcpConfig(json.RawMessage(`{"mcpServers":{}}`)); err != nil {
		t.Errorf("Expected nil error from EnsureMcpConfig on empty homeDir, got: %v", err)
	}
	if err := pEmpty.SyncSkills(); err != nil {
		t.Errorf("Expected nil error from SyncSkills on empty homeDir, got: %v", err)
	}

	// 3. Verify nil receiver safety
	var pNil *Provisioner
	if err := pNil.Sync(context.Background(), nil); err != nil {
		t.Errorf("Expected nil error from Sync on nil Provisioner, got: %v", err)
	}
	if err := pNil.SyncSettings("key", "model"); err != nil {
		t.Errorf("Expected nil error from SyncSettings on nil Provisioner, got: %v", err)
	}
	if err := pNil.SyncRules("prompt"); err != nil {
		t.Errorf("Expected nil error from SyncRules on nil Provisioner, got: %v", err)
	}
	if err := pNil.SyncMCP(context.Background(), nil); err != nil {
		t.Errorf("Expected nil error from SyncMCP on nil Provisioner, got: %v", err)
	}
	if err := pNil.EnsureMcpConfig(json.RawMessage(`{"mcpServers":{}}`)); err != nil {
		t.Errorf("Expected nil error from EnsureMcpConfig on nil Provisioner, got: %v", err)
	}
	if err := pNil.SyncSkills(); err != nil {
		t.Errorf("Expected nil error from SyncSkills on nil Provisioner, got: %v", err)
	}

	// 4. EnsureAgySettings and EnsureAgySettingsForHome with empty homeDir
	if err := EnsureAgySettings("key", "model"); err != nil {
		t.Errorf("Expected nil error from EnsureAgySettings, got: %v", err)
	}
	if err := EnsureAgySettingsForHome("", "key", "model"); err != nil {
		t.Errorf("Expected nil error from EnsureAgySettingsForHome(\"\"), got: %v", err)
	}

	// 5. NewFromConfig resolves via getters
	tmpHome := t.TempDir()
	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = tmpHome
		d.DataDir = "/custom/data"
	})
	pFromCfg := NewFromConfig(cfg)
	if pFromCfg.HomeDir() != tmpHome {
		t.Errorf("Expected HomeDir %q from config, got %q", tmpHome, pFromCfg.HomeDir())
	}
	if pFromCfg.DataDir() != "/custom/data" {
		t.Errorf("Expected resolved DataDir '/custom/data' from config, got %q", pFromCfg.DataDir())
	}

	// Nil config in NewFromConfig
	pNilCfg := NewFromConfig(nil)
	if pNilCfg.HomeDir() != "" || pNilCfg.DataDir() != "" {
		t.Errorf("Expected empty paths from nil config, got home=%q data=%q", pNilCfg.HomeDir(), pNilCfg.DataDir())
	}
}

func TestLoadMCPConfig_ConfigDataPropagation(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.GitHubPAT = "ghp_mock_token_123"
		d.MCPConfig = `{"mcpServers":{"custom":{"serverUrl":"http://custom:5000/mcp"}}}`
	})

	raw := p.LoadMCPConfig(cfg)
	var parsed map[string]interface{}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("failed to unmarshal LoadMCPConfig output: %v", err)
	}

	servers, ok := parsed["mcpServers"].(map[string]interface{})
	if !ok {
		t.Fatalf("mcpServers mapping missing from output: %s", string(raw))
	}

	// 1. Built-in defaults must be present
	for _, name := range []string{"scheduler", "discord", "docker", "victoriametrics"} {
		if _, exists := servers[name]; !exists {
			t.Errorf("expected built-in server %q to be present", name)
		}
	}

	// 2. GitHub server enabled via cfg.Current().GitHubPAT
	if _, exists := servers["github"]; !exists {
		t.Errorf("expected github server to be enabled when GitHubPAT is set in config")
	}

	// 3. Custom server enabled via cfg.Current().MCPConfig
	if customSrv, exists := servers["custom"]; !exists {
		t.Errorf("expected custom server to be enabled from cfg.MCPConfig")
	} else if customMap, ok := customSrv.(map[string]interface{}); !ok || customMap["serverUrl"] != "http://custom:5000/mcp" {
		t.Errorf("expected custom serverUrl 'http://custom:5000/mcp', got %+v", customSrv)
	}

	// 4. Test without GitHubPAT and without MCPConfig
	cfgEmpty := config.NewTestConfig(func(d *config.ConfigData) {
		d.GitHubPAT = ""
		d.MCPConfig = ""
	})
	rawEmpty := p.LoadMCPConfig(cfgEmpty)
	var parsedEmpty map[string]interface{}
	_ = json.Unmarshal(rawEmpty, &parsedEmpty)
	serversEmpty := parsedEmpty["mcpServers"].(map[string]interface{})
	if _, exists := serversEmpty["github"]; exists {
		t.Errorf("expected github server to be absent when GitHubPAT is empty")
	}
	if _, exists := serversEmpty["custom"]; exists {
		t.Errorf("expected custom server to be absent when MCPConfig is empty")
	}
}
