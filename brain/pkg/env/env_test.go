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

	targetDir := filepath.Join(tmpHome, ".gemini", "config", "skills", "test-skill")
	targetSkillMD := filepath.Join(targetDir, "SKILL.md")
	if _, err := os.Stat(targetSkillMD); err != nil {
		t.Fatalf("Expected symlinked skill at %s: %v", targetSkillMD, err)
	}

	// Verify legacy ~/.gemini/skills directory is purged
	legacyDir := filepath.Join(tmpHome, ".gemini", "skills")
	_ = os.MkdirAll(legacyDir, 0755)
	if err := p.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills with legacy dir failed: %v", err)
	}
	if _, err := os.Stat(legacyDir); !os.IsNotExist(err) {
		t.Errorf("Expected legacy skills directory %s to be purged", legacyDir)
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

func TestSyncSkills_LegacyPluginDirPruned(t *testing.T) {
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())
	p.SetCustomSkillsDir(t.TempDir())
	p.SetSuperpowersDir(t.TempDir())
	p.SetAgentsSkillsDir(t.TempDir())

	pluginDir := filepath.Join(tmpHome, ".gemini", "config", "plugins", "superpowers")
	_ = os.MkdirAll(pluginDir, 0755)
	_ = os.WriteFile(filepath.Join(pluginDir, "SKILL.md"), []byte("# Legacy Plugin"), 0644)

	if err := p.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills failed: %v", err)
	}

	if _, err := os.Lstat(pluginDir); !os.IsNotExist(err) {
		t.Errorf("Expected legacy plugin directory %s to be pruned", pluginDir)
	}
}

func TestSyncSkills_CuratedSkills(t *testing.T) {
	tmpHome := t.TempDir()
	tmpCustomSkills := t.TempDir()
	tmpSuperpowers := t.TempDir()

	p := New(tmpHome, t.TempDir())
	p.SetCustomSkillsDir(tmpCustomSkills)
	p.SetSuperpowersDir(tmpSuperpowers)
	p.SetAgentsSkillsDir(t.TempDir())

	// Custom skill
	customDir := filepath.Join(tmpCustomSkills, "my-custom-skill")
	_ = os.MkdirAll(customDir, 0755)
	_ = os.WriteFile(filepath.Join(customDir, "SKILL.md"), []byte("# My Custom Skill"), 0644)

	// Curated methodology skill
	tddDir := filepath.Join(tmpSuperpowers, "test-driven-development")
	_ = os.MkdirAll(tddDir, 0755)
	_ = os.WriteFile(filepath.Join(tddDir, "SKILL.md"), []byte("# TDD"), 0644)

	if err := p.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills failed: %v", err)
	}

	skillsRoot := filepath.Join(tmpHome, ".gemini", "config", "skills")
	for _, name := range []string{"my-custom-skill", "test-driven-development"} {
		path := filepath.Join(skillsRoot, name, "SKILL.md")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("Expected skill %s at %s, but stat failed: %v", name, path, err)
		}
	}
}

func TestLinkSkills_Branches(t *testing.T) {
	targetDir := t.TempDir()
	srcDir1 := t.TempDir()
	srcDir2 := t.TempDir()

	// 1. Skill in srcDir1 with SKILL.md
	s1 := filepath.Join(srcDir1, "skill-one")
	_ = os.MkdirAll(s1, 0755)
	_ = os.WriteFile(filepath.Join(s1, "SKILL.md"), []byte("# S1"), 0644)

	// 2. Duplicate skill in srcDir2 (should be shadowed by srcDir1)
	s1Dup := filepath.Join(srcDir2, "skill-one")
	_ = os.MkdirAll(s1Dup, 0755)
	_ = os.WriteFile(filepath.Join(s1Dup, "SKILL.md"), []byte("# S1 Dup"), 0644)

	// 3. Skill in srcDir2 missing SKILL.md (should be ignored)
	sNoSkillMD := filepath.Join(srcDir2, "no-md")
	_ = os.MkdirAll(sNoSkillMD, 0755)

	// 4. Regular non-directory file in srcDir1 (should be ignored)
	_ = os.WriteFile(filepath.Join(srcDir1, "regular-file.txt"), []byte("hello"), 0644)

	// 5. Symlink to a directory with SKILL.md in srcDir2
	realTarget := filepath.Join(t.TempDir(), "symlinked-skill")
	_ = os.MkdirAll(realTarget, 0755)
	_ = os.WriteFile(filepath.Join(realTarget, "SKILL.md"), []byte("# Symlinked"), 0644)
	_ = os.Symlink(realTarget, filepath.Join(srcDir2, "symlinked-skill"))

	// 6. Symlink to a file in srcDir2 (not a dir, should be ignored)
	realFile := filepath.Join(t.TempDir(), "some-file")
	_ = os.WriteFile(realFile, []byte("file"), 0644)
	_ = os.Symlink(realFile, filepath.Join(srcDir2, "file-symlink"))

	// 7. Non-existent source dir in sourceDirs list
	count := LinkSkills([]string{targetDir}, []string{srcDir1, srcDir2, filepath.Join(t.TempDir(), "nonexistent")})
	if count != 2 {
		t.Errorf("Expected 2 skills linked (skill-one and symlinked-skill), got %d", count)
	}

	// 8. Re-run LinkSkills when target already has destinations and stale tmp files
	staleTmp := filepath.Join(targetDir, "skill-one.tmp")
	_ = os.WriteFile(staleTmp, []byte("stale tmp"), 0644)
	count2 := LinkSkills([]string{targetDir}, []string{srcDir1})
	if count2 != 1 {
		t.Errorf("Expected 1 skill linked on re-run, got %d", count2)
	}
}

func TestSweepOrphanedSymlinks_Branches(t *testing.T) {
	// 1. Non-existent target directory
	sweepOrphanedSymlinks([]string{filepath.Join(t.TempDir(), "nonexistent")})

	// 2. Real directory, regular file, valid symlink, and broken symlink
	targetDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(targetDir, "normal-dir"), 0755)
	_ = os.WriteFile(filepath.Join(targetDir, "normal-file"), []byte("data"), 0644)

	validTarget := filepath.Join(t.TempDir(), "valid-target")
	_ = os.MkdirAll(validTarget, 0755)
	validLink := filepath.Join(targetDir, "valid-link")
	_ = os.Symlink(validTarget, validLink)

	brokenTarget := filepath.Join(t.TempDir(), "broken-target")
	_ = os.MkdirAll(brokenTarget, 0755)
	brokenLink := filepath.Join(targetDir, "broken-link")
	_ = os.Symlink(brokenTarget, brokenLink)
	_ = os.RemoveAll(brokenTarget) // break the symlink

	sweepOrphanedSymlinks([]string{targetDir})

	if _, err := os.Lstat(validLink); err != nil {
		t.Errorf("Expected valid link to remain: %v", err)
	}
	if _, err := os.Lstat(brokenLink); !os.IsNotExist(err) {
		t.Errorf("Expected broken link to be removed")
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
		d.OpenObserveUser = "admin@aerial.local"
		d.OpenObservePassword = "secure_password"
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

	// 3. OpenObserve server enabled via OpenObserveUser / OpenObservePassword
	if ooSrv, exists := servers["openobserve"]; !exists {
		t.Errorf("expected openobserve server to be enabled when credentials are set")
	} else if ooMap, ok := ooSrv.(map[string]interface{}); !ok {
		t.Errorf("expected openobserve to be a map, got %+v", ooSrv)
	} else {
		if ooMap["serverUrl"] != "http://openobserve:5080/openobserve/api/default/mcp" {
			t.Errorf("expected serverUrl 'http://openobserve:5080/openobserve/api/default/mcp', got %v", ooMap["serverUrl"])
		}
		if headers, ok := ooMap["headers"].(map[string]interface{}); !ok || headers["Authorization"] == "" {
			t.Errorf("expected non-empty Authorization header, got %+v", ooMap["headers"])
		}
	}

	// 4. Custom server enabled via cfg.Current().MCPConfig
	if customSrv, exists := servers["custom"]; !exists {
		t.Errorf("expected custom server to be enabled from cfg.MCPConfig")
	} else if customMap, ok := customSrv.(map[string]interface{}); !ok || customMap["serverUrl"] != "http://custom:5000/mcp" {
		t.Errorf("expected custom serverUrl 'http://custom:5000/mcp', got %+v", customSrv)
	}

	// 5. Test without GitHubPAT, without OpenObserve, and without MCPConfig
	cfgEmpty := config.NewTestConfig(func(d *config.ConfigData) {
		d.GitHubPAT = ""
		d.MCPConfig = ""
		d.OpenObserveUser = ""
		d.OpenObservePassword = ""
	})
	rawEmpty := p.LoadMCPConfig(cfgEmpty)
	var parsedEmpty map[string]interface{}
	_ = json.Unmarshal(rawEmpty, &parsedEmpty)
	serversEmpty := parsedEmpty["mcpServers"].(map[string]interface{})
	if _, exists := serversEmpty["github"]; exists {
		t.Errorf("expected github server to be absent when GitHubPAT is empty")
	}
	if _, exists := serversEmpty["openobserve"]; exists {
		t.Errorf("expected openobserve server to be absent when credentials are empty")
	}
	if _, exists := serversEmpty["custom"]; exists {
		t.Errorf("expected custom server to be absent when MCPConfig is empty")
	}
}

func TestProvisioner_SettersAndGetters(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	if p.HomeDir() != tmpHome {
		t.Errorf("HomeDir mismatch: expected %q, got %q", tmpHome, p.HomeDir())
	}
	if p.DataDir() != tmpData {
		t.Errorf("DataDir mismatch: expected %q, got %q", tmpData, p.DataDir())
	}

	p.SetCustomSkillsDir("/custom/skills")
	if p.customSkillsDir != "/custom/skills" {
		t.Errorf("customSkillsDir mismatch: %q", p.customSkillsDir)
	}

	p.SetSuperpowersDir("/super/powers")
	if p.superpowersDir != "/super/powers" {
		t.Errorf("superpowersDir mismatch: %q", p.superpowersDir)
	}

	p.SetAgentsSkillsDir("/agents/skills")
	if p.agentsSkillsDir != "/agents/skills" {
		t.Errorf("agentsSkillsDir mismatch: %q", p.agentsSkillsDir)
	}

	p.SetAgentInstructionsSearchPaths([]string{"/path/1", "/path/2"})
	if len(p.agentInstructionsPaths) != 2 || p.agentInstructionsPaths[0] != "/path/1" {
		t.Errorf("agentInstructionsPaths mismatch: %+v", p.agentInstructionsPaths)
	}
}

func TestProvisioner_Sync_FlowAndErrors(t *testing.T) {
	// 1. Full successful sync with cfg != nil
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)
	p.SetAgentsSkillsDir(t.TempDir())
	p.SetCustomSkillsDir(t.TempDir())
	p.SetSuperpowersDir(t.TempDir())

	agentsFile := filepath.Join(t.TempDir(), "AGENTS.md")
	_ = os.WriteFile(agentsFile, []byte("Sync test instructions"), 0644)
	p.SetAgentInstructionsSearchPaths([]string{agentsFile})

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.APIKey = "sync-key"
		d.Model = "gemini-2.5-flash"
		d.SystemPrompt = "custom prompt"
	})

	ctx := context.Background()
	if err := p.Sync(ctx, cfg); err != nil {
		t.Fatalf("p.Sync(ctx, cfg) failed: %v", err)
	}

	// 2. Successful sync with cfg == nil (uses default config)
	tmpHomeNilCfg := t.TempDir()
	pNilCfg := New(tmpHomeNilCfg, t.TempDir())
	pNilCfg.SetAgentsSkillsDir(t.TempDir())
	pNilCfg.SetCustomSkillsDir(t.TempDir())
	pNilCfg.SetSuperpowersDir(t.TempDir())
	pNilCfg.SetAgentInstructionsSearchPaths([]string{agentsFile})

	if err := pNilCfg.Sync(ctx, nil); err != nil {
		t.Fatalf("pNilCfg.Sync(ctx, nil) failed: %v", err)
	}

	// 3. Error case: SyncSettings failure
	tmpHomeErrSettings := t.TempDir()
	pErrSettings := New(tmpHomeErrSettings, t.TempDir())
	// Block .gemini/antigravity-cli with a regular file
	_ = os.MkdirAll(filepath.Join(tmpHomeErrSettings, ".gemini"), 0755)
	_ = os.WriteFile(filepath.Join(tmpHomeErrSettings, ".gemini", "antigravity-cli"), []byte("file-blocking-dir"), 0644)
	if err := pErrSettings.Sync(ctx, cfg); err == nil || !strings.Contains(err.Error(), "failed to sync settings") {
		t.Errorf("Expected 'failed to sync settings' error, got: %v", err)
	}

	// 4. Error case: SyncRules failure
	tmpHomeErrRules := t.TempDir()
	pErrRules := New(tmpHomeErrRules, t.TempDir())
	pErrRules.SetAgentInstructionsSearchPaths([]string{agentsFile})
	// Block .gemini/rules with a regular file
	_ = os.MkdirAll(filepath.Join(tmpHomeErrRules, ".gemini"), 0755)
	_ = os.WriteFile(filepath.Join(tmpHomeErrRules, ".gemini", "rules"), []byte("file-blocking-dir"), 0644)
	if err := pErrRules.Sync(ctx, cfg); err == nil || !strings.Contains(err.Error(), "failed to sync rules") {
		t.Errorf("Expected 'failed to sync rules' error, got: %v", err)
	}

	// 5. Error case: SyncMCP failure
	tmpHomeErrMCP := t.TempDir()
	pErrMCP := New(tmpHomeErrMCP, t.TempDir())
	pErrMCP.SetAgentInstructionsSearchPaths([]string{agentsFile})
	// Block .gemini/config/mcp_config.json with a non-empty directory
	mcpFileBlock := filepath.Join(tmpHomeErrMCP, ".gemini", "config", "mcp_config.json")
	_ = os.MkdirAll(mcpFileBlock, 0755)
	_ = os.WriteFile(filepath.Join(mcpFileBlock, "blocker"), []byte("b"), 0644)
	if err := pErrMCP.Sync(ctx, cfg); err == nil || !strings.Contains(err.Error(), "failed to sync mcp config") {
		t.Errorf("Expected 'failed to sync mcp config' error, got: %v", err)
	}
}

func TestProvisioner_writeAtomic_FallbackAndErrors(t *testing.T) {
	tmpDir := t.TempDir()
	p := New(tmpDir, tmpDir)

	// 1. MkdirAll error: parent directory path contains a regular file
	regularFilePath := filepath.Join(tmpDir, "blocking_file")
	_ = os.WriteFile(regularFilePath, []byte("blocker"), 0644)
	err := p.writeAtomic(filepath.Join(regularFilePath, "sub", "target.txt"), "data")
	if err == nil {
		t.Errorf("Expected writeAtomic error when MkdirAll fails, got nil")
	}

	// 2. Rename fallback success: targetPath exists as an empty directory
	emptyDirPath := filepath.Join(tmpDir, "empty_dir")
	if err := os.Mkdir(emptyDirPath, 0755); err != nil {
		t.Fatalf("Failed to create empty dir: %v", err)
	}
	if err := p.writeAtomic(emptyDirPath, "replacement data"); err != nil {
		t.Fatalf("Expected writeAtomic to succeed replacing empty dir, got: %v", err)
	}
	readData, err := os.ReadFile(emptyDirPath)
	if err != nil || string(readData) != "replacement data" {
		t.Errorf("writeAtomic file content mismatch: %q, err: %v", string(readData), err)
	}

	// 3. Rename fallback failure: targetPath exists as a non-empty directory
	nonEmptyDirPath := filepath.Join(tmpDir, "non_empty_dir")
	if err := os.Mkdir(nonEmptyDirPath, 0755); err != nil {
		t.Fatalf("Failed to create non-empty dir: %v", err)
	}
	_ = os.WriteFile(filepath.Join(nonEmptyDirPath, "child.txt"), []byte("child"), 0644)
	if err := p.writeAtomic(nonEmptyDirPath, "fails"); err == nil {
		t.Errorf("Expected writeAtomic error when replacing non-empty dir, got nil")
	}
}

func TestLoadMCPConfig_FileOverridesAndNormalizations(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	// 1. Load from dataDir/mcp.config.json
	fileConfig := `{"mcpServers":{"from-file":{"serverUrl":"http://from-file:8000/mcp"}}}`
	_ = os.WriteFile(filepath.Join(tmpData, "mcp.config.json"), []byte(fileConfig), 0644)
	raw := p.LoadMCPConfig(nil)
	if !strings.Contains(string(raw), "from-file") {
		t.Errorf("Expected 'from-file' in raw config, got: %s", string(raw))
	}

	// Remove mcp.config.json so cfg.MCPConfig fallback can be tested
	_ = os.Remove(filepath.Join(tmpData, "mcp.config.json"))

	// 2. Load from cfg.Current().MCPConfig
	cfgWithMCP := config.NewTestConfig(func(d *config.ConfigData) {
		d.MCPConfig = `{"mcpServers":{"from-cfg":{"serverUrl":"http://from-cfg:8001/mcp"}}}`
	})
	raw = p.LoadMCPConfig(cfgWithMCP)
	if !strings.Contains(string(raw), "from-cfg") {
		t.Errorf("Expected 'from-cfg' in raw config, got: %s", string(raw))
	}

	// 4. cur.McpServers with unmarshalable JSON value (exercises `mergedServers[k] = v` and json.Marshal error branch)
	cfgInvalid := config.NewTestConfig(func(d *config.ConfigData) {
		d.McpServers = map[string]json.RawMessage{
			"raw-invalid": json.RawMessage(`{not-json`),
		}
	})
	raw = p.LoadMCPConfig(cfgInvalid)
	if string(raw) != `{"mcpServers":{}}` {
		t.Errorf("Expected fallback empty mcpServers on marshal error, got: %s", string(raw))
	}

	// 5. Normalization of legacy /sse endpoints to /mcp for docker, github, victoriametrics
	cfgSSE := config.NewTestConfig(func(d *config.ConfigData) {
		d.GitHubPAT = "dummy-pat"
		d.McpServers = map[string]json.RawMessage{
			"docker":          json.RawMessage(`{"serverUrl":"http://docker-mcp:4002/sse"}`),
			"github":          json.RawMessage(`{"serverUrl":"http://github-mcp:4003/sse"}`),
			"victoriametrics": json.RawMessage(`{"serverUrl":"http://victoriametrics-mcp:4004/sse"}`),
		}
	})
	raw = p.LoadMCPConfig(cfgSSE)
	rawStr := string(raw)
	for _, svc := range []string{"docker-mcp:4002/mcp", "github-mcp:4003/mcp", "victoriametrics-mcp:4004/mcp"} {
		if !strings.Contains(rawStr, svc) {
			t.Errorf("Expected normalized endpoint %q in config, got: %s", svc, rawStr)
		}
	}
	if strings.Contains(rawStr, "/sse") {
		t.Errorf("Expected no /sse endpoints in config, got: %s", rawStr)
	}
}

func TestEnsureMcpConfig_ErrorBranches(t *testing.T) {
	// 1. MkdirAll error: parent directory has a regular file blocking it
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())
	_ = os.WriteFile(filepath.Join(tmpHome, ".gemini"), []byte("blocking-file"), 0644)

	err := p.EnsureMcpConfig(json.RawMessage(`{"mcpServers":{}}`))
	if err == nil || !strings.Contains(err.Error(), "failed to create config directory") {
		t.Errorf("Expected 'failed to create config directory' error, got: %v", err)
	}

	// 2. writeAtomic error: target file cannot be written (exists as a non-empty dir)
	tmpHome2 := t.TempDir()
	p2 := New(tmpHome2, t.TempDir())
	configDir := filepath.Join(tmpHome2, ".gemini", "config")
	_ = os.MkdirAll(configDir, 0755)
	targetDir := filepath.Join(configDir, "mcp_config.json")
	_ = os.Mkdir(targetDir, 0755)
	_ = os.WriteFile(filepath.Join(targetDir, "child"), []byte("child"), 0644)

	err = p2.EnsureMcpConfig(json.RawMessage(`{"mcpServers":{}}`))
	if err == nil || !strings.Contains(err.Error(), "failed to write mcp config") {
		t.Errorf("Expected 'failed to write mcp config' error, got: %v", err)
	}
}

func TestSyncSettings_EnsureAgySettingsForHome_AndErrors(t *testing.T) {
	// 1. EnsureAgySettingsForHome with valid home
	tmpHome := t.TempDir()
	if err := EnsureAgySettingsForHome(tmpHome, "home-key", "home-model"); err != nil {
		t.Fatalf("EnsureAgySettingsForHome failed: %v", err)
	}
	settingsPath := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "settings.json")
	data, err := os.ReadFile(settingsPath)
	if err != nil {
		t.Fatalf("Failed to read settings.json: %v", err)
	}
	if !strings.Contains(string(data), "home-key") {
		t.Errorf("Expected 'home-key' in settings.json, got: %s", string(data))
	}

	// 2. MkdirAll error in SyncSettings
	tmpHomeErr := t.TempDir()
	pErr := New(tmpHomeErr, t.TempDir())
	_ = os.WriteFile(filepath.Join(tmpHomeErr, ".gemini"), []byte("blocking-file"), 0644)
	err = pErr.SyncSettings("key", "model")
	if err == nil || !strings.Contains(err.Error(), "failed to create config directory") {
		t.Errorf("Expected 'failed to create config directory' error, got: %v", err)
	}
}

func TestSyncRules_LKGCAndErrorBranches(t *testing.T) {
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())

	// 1. Empty agentInstructionsPaths defaults to DefaultAgentInstructionsSearchPaths
	origDefaults := DefaultAgentInstructionsSearchPaths
	defer func() { DefaultAgentInstructionsSearchPaths = origDefaults }()

	fallbackAgentsDir := t.TempDir()
	fallbackFile := filepath.Join(fallbackAgentsDir, "AGENTS.md")
	_ = os.WriteFile(fallbackFile, []byte("Default fallback persona"), 0644)
	DefaultAgentInstructionsSearchPaths = []string{fallbackFile}
	p.SetAgentInstructionsSearchPaths(nil) // explicitly nil to trigger fallback

	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules with default search paths failed: %v", err)
	}
	rulePath := filepath.Join(tmpHome, ".gemini", "rules", "user_persona.md")
	data, err := os.ReadFile(rulePath)
	if err != nil || !strings.Contains(string(data), "Default fallback persona") {
		t.Errorf("Expected default fallback persona, got: %s (err: %v)", string(data), err)
	}

	// 2. Missing file in searchPaths (exercises `if err != nil { continue }`)
	validAgentsDir := t.TempDir()
	validFile := filepath.Join(validAgentsDir, "AGENTS.md")
	_ = os.WriteFile(validFile, []byte("Valid second path"), 0644)
	p.SetAgentInstructionsSearchPaths([]string{
		filepath.Join(validAgentsDir, "non_existent.md"),
		validFile,
	})
	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules with missing first path failed: %v", err)
	}
	data, _ = os.ReadFile(rulePath)
	if !strings.Contains(string(data), "Valid second path") {
		t.Errorf("Expected 'Valid second path' in rules, got: %s", string(data))
	}

	// 3. First torn read where personaSource is empty
	pFresh := New(t.TempDir(), t.TempDir())
	tornFile := filepath.Join(t.TempDir(), "EMPTY_AGENTS.md")
	_ = os.WriteFile(tornFile, []byte("   \n\t"), 0644) // whitespace only = torn read
	pFresh.SetAgentInstructionsSearchPaths([]string{tornFile})
	if err := pFresh.SyncRules("Custom Prompt Only"); err != nil {
		t.Fatalf("SyncRules fresh torn read failed: %v", err)
	}

	// 4. No instructions and no custom prompt
	// Subcase 4a: Fresh provisioner -> returns nil
	pEmpty := New(t.TempDir(), t.TempDir())
	pEmpty.SetAgentInstructionsSearchPaths([]string{filepath.Join(t.TempDir(), "missing.md")})
	if err := pEmpty.SyncRules(""); err != nil {
		t.Errorf("Expected nil error for empty rules and no LKGC, got: %v", err)
	}

	// Subcase 4b: Existing LKGC used when instructions not found
	pWithLKGC := New(t.TempDir(), t.TempDir())
	pWithLKGC.SetAgentInstructionsSearchPaths([]string{validFile})
	if err := pWithLKGC.SyncRules("Initial Prompt"); err != nil {
		t.Fatalf("Initial SyncRules failed: %v", err)
	}
	// Now remove search paths so no persona or custom prompt is found
	pWithLKGC.SetAgentInstructionsSearchPaths([]string{filepath.Join(t.TempDir(), "missing.md")})
	if err := pWithLKGC.SyncRules(""); err != nil {
		t.Fatalf("LKGC fallback SyncRules failed: %v", err)
	}

	// 5. MkdirAll error for primaryRulesDir
	tmpHomeErrPrimary := t.TempDir()
	pErrPrimary := New(tmpHomeErrPrimary, t.TempDir())
	pErrPrimary.SetAgentInstructionsSearchPaths([]string{validFile})
	_ = os.WriteFile(filepath.Join(tmpHomeErrPrimary, ".gemini"), []byte("blocker"), 0644)
	if err := pErrPrimary.SyncRules("prompt"); err == nil || !strings.Contains(err.Error(), "failed to create primary rules directory") {
		t.Errorf("Expected 'failed to create primary rules directory' error, got: %v", err)
	}

	// 6. MkdirAll error for configRulesDir
	tmpHomeErrConfig := t.TempDir()
	pErrConfig := New(tmpHomeErrConfig, t.TempDir())
	pErrConfig.SetAgentInstructionsSearchPaths([]string{validFile})
	_ = os.MkdirAll(filepath.Join(tmpHomeErrConfig, ".gemini"), 0755)
	_ = os.WriteFile(filepath.Join(tmpHomeErrConfig, ".gemini", "config"), []byte("blocker"), 0644)
	if err := pErrConfig.SyncRules("prompt"); err == nil || !strings.Contains(err.Error(), "failed to create config rules directory") {
		t.Errorf("Expected 'failed to create config rules directory' error, got: %v", err)
	}

	// 7. writeAtomic error for primaryRuleFile
	tmpHomeErrWrite := t.TempDir()
	pErrWrite := New(tmpHomeErrWrite, t.TempDir())
	pErrWrite.SetAgentInstructionsSearchPaths([]string{validFile})
	rulesDir := filepath.Join(tmpHomeErrWrite, ".gemini", "rules")
	_ = os.MkdirAll(rulesDir, 0755)
	blockingDir := filepath.Join(rulesDir, "user_persona.md")
	_ = os.Mkdir(blockingDir, 0755)
	_ = os.WriteFile(filepath.Join(blockingDir, "child"), []byte("child"), 0644)
	if err := pErrWrite.SyncRules("prompt"); err == nil || !strings.Contains(err.Error(), "failed to write primary system rules") {
		t.Errorf("Expected 'failed to write primary system rules' error, got: %v", err)
	}
}

func TestSyncSkills_AndLinkSkills_Branches(t *testing.T) {
	// 1. SyncSkills MkdirAll warning branch
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())
	// Block .gemini/config/skills with a regular file
	_ = os.MkdirAll(filepath.Join(tmpHome, ".gemini", "config"), 0755)
	_ = os.WriteFile(filepath.Join(tmpHome, ".gemini", "config", "skills"), []byte("blocking-file"), 0644)
	if err := p.SyncSkills(); err != nil {
		t.Errorf("Expected SyncSkills to handle MkdirAll warning gracefully, got: %v", err)
	}

	// 2. LinkSkills: non-existent source directory
	tmpTarget := t.TempDir()
	count := LinkSkills([]string{tmpTarget}, []string{filepath.Join(t.TempDir(), "nonexistent")})
	if count != 0 {
		t.Errorf("Expected 0 skills linked from nonexistent dir, got %d", count)
	}

	// 3. LinkSkills: directory filtering branches
	srcDir := t.TempDir()
	// Regular file (not a dir) -> skipped
	_ = os.WriteFile(filepath.Join(srcDir, "regular_file.txt"), []byte("file"), 0644)

	// Directory without SKILL.md -> skipped
	noSkillMD := filepath.Join(srcDir, "no-skill")
	_ = os.Mkdir(noSkillMD, 0755)

	// Directory that is a symlink pointing to a real directory with SKILL.md
	realDir := filepath.Join(t.TempDir(), "real-skill")
	_ = os.Mkdir(realDir, 0755)
	_ = os.WriteFile(filepath.Join(realDir, "SKILL.md"), []byte("# Real Skill"), 0644)
	symlinkSkill := filepath.Join(srcDir, "symlink-skill")
	if err := os.Symlink(realDir, symlinkSkill); err != nil {
		t.Fatalf("Failed to create symlink skill: %v", err)
	}

	count = LinkSkills([]string{tmpTarget}, []string{srcDir})
	if count != 1 {
		t.Errorf("Expected 1 skill linked from symlink, got %d", count)
	}

	// 4. LinkSkills: rename tmpDest over existing empty directory
	targetDirForRename := t.TempDir()
	destDir := filepath.Join(targetDirForRename, "symlink-skill")
	_ = os.Mkdir(destDir, 0755) // existing empty directory
	count = LinkSkills([]string{targetDirForRename}, []string{srcDir})
	if count != 1 {
		t.Errorf("Expected 1 skill linked replacing empty dir, got %d", count)
	}

	// 5. LinkSkills: symlink to tmpDest fails (tmpDest is non-empty dir)
	// Case 5a: fallback to destPath succeeds
	targetDirTmpBlock := t.TempDir()
	tmpDestPath := filepath.Join(targetDirTmpBlock, "symlink-skill.tmp")
	_ = os.Mkdir(tmpDestPath, 0755)
	_ = os.WriteFile(filepath.Join(tmpDestPath, "child"), []byte("c"), 0644)
	count = LinkSkills([]string{targetDirTmpBlock}, []string{srcDir})
	if count != 1 {
		t.Errorf("Expected fallback symlink to succeed, got count %d", count)
	}

	// Case 5b: fallback to destPath also fails (destPath is also non-empty dir)
	targetDirBothBlock := t.TempDir()
	tmpDestPath2 := filepath.Join(targetDirBothBlock, "symlink-skill.tmp")
	_ = os.Mkdir(tmpDestPath2, 0755)
	_ = os.WriteFile(filepath.Join(tmpDestPath2, "child"), []byte("c"), 0644)
	destPath2 := filepath.Join(targetDirBothBlock, "symlink-skill")
	_ = os.Mkdir(destPath2, 0755)
	_ = os.WriteFile(filepath.Join(destPath2, "child"), []byte("c"), 0644)
	count = LinkSkills([]string{targetDirBothBlock}, []string{srcDir})
	if count != 0 {
		t.Errorf("Expected 0 skills linked when both dests blocked, got %d", count)
	}

	// 6. sweepOrphanedSymlinks branches
	// Non-existent target directory
	sweepOrphanedSymlinks([]string{filepath.Join(t.TempDir(), "nonexistent")})

	// Directory with regular file and valid symlink (should not be deleted)
	sweepDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(sweepDir, "file.txt"), []byte("regular"), 0644)
	validTarget := filepath.Join(t.TempDir(), "valid.txt")
	_ = os.WriteFile(validTarget, []byte("valid"), 0644)
	_ = os.Symlink(validTarget, filepath.Join(sweepDir, "valid_symlink"))

	sweepOrphanedSymlinks([]string{sweepDir})
	if _, err := os.Lstat(filepath.Join(sweepDir, "file.txt")); err != nil {
		t.Errorf("Regular file should not be removed by sweeper: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(sweepDir, "valid_symlink")); err != nil {
		t.Errorf("Valid symlink should not be removed by sweeper: %v", err)
	}
}

func TestEnv_SwallowedErrorsRemediationCoverage(t *testing.T) {
	// 1. writeAtomic with targetPath being a non-empty directory (triggers rename error, remove error, warning log)
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "blocking_dir")
	if err := os.MkdirAll(filepath.Join(targetDir, "child"), 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	p := New(tmpDir, tmpDir)
	if err := p.writeAtomic(targetDir, "content"); err == nil {
		t.Errorf("expected writeAtomic to fail when target is a directory")
	}

	// 2. SyncRules with bad dataDir so LKGC write fails
	blockFile := filepath.Join(tmpDir, "block_file")
	if err := os.WriteFile(blockFile, []byte("x"), 0644); err != nil {
		t.Fatalf("write blocker failed: %v", err)
	}
	badDataDir := filepath.Join(blockFile, "sub")
	pBadData := New(tmpDir, badDataDir)
	agentsFile := filepath.Join(tmpDir, "AGENTS.md")
	if err := os.WriteFile(agentsFile, []byte("test persona"), 0644); err != nil {
		t.Fatalf("write agents file failed: %v", err)
	}
	pBadData.SetAgentInstructionsSearchPaths([]string{agentsFile})
	if err := pBadData.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}

	// 3. sweepOrphanedSymlinks with an orphaned symlink that gets removed
	sweepDir := t.TempDir()
	nonExistentTarget := filepath.Join(t.TempDir(), "ghost.txt")
	brokenLink := filepath.Join(sweepDir, "broken_link")
	if err := os.Symlink(nonExistentTarget, brokenLink); err != nil {
		t.Fatalf("symlink failed: %v", err)
	}
	sweepOrphanedSymlinks([]string{sweepDir})
	if _, err := os.Lstat(brokenLink); !os.IsNotExist(err) {
		t.Errorf("expected broken symlink to be swept, err=%v", err)
	}
}

func TestSyncRules_SeparatePersonaAndSystemInvariants(t *testing.T) {
	t.Parallel()

	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	srcDir := t.TempDir()
	agentsPath := filepath.Join(srcDir, "AGENTS.md")
	geminiPath := filepath.Join(srcDir, "GEMINI.md")

	if err := os.WriteFile(agentsPath, []byte("ABG Baddie Persona"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(geminiPath, []byte("Core Invariant 5: No Option B"), 0644); err != nil {
		t.Fatal(err)
	}

	p.SetAgentInstructionsSearchPaths([]string{agentsPath})
	p.SetSystemInstructionsSearchPaths([]string{geminiPath})

	// 1. Verify separate generation
	if err := p.SyncRules("Custom Prompt"); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}

	personaRuleFile := filepath.Join(tmpHome, ".gemini", "rules", "user_persona.md")
	personaData, err := os.ReadFile(personaRuleFile)
	if err != nil {
		t.Fatalf("Failed to read user_persona.md: %v", err)
	}
	personaContent := string(personaData)

	if !strings.Contains(personaContent, "# User Persona Overrides (AGENTS.md)") {
		t.Errorf("expected persona header, got:\n%s", personaContent)
	}
	if !strings.Contains(personaContent, "ABG Baddie Persona") {
		t.Errorf("expected persona content, got:\n%s", personaContent)
	}
	if !strings.Contains(personaContent, "# Environment Prompt Override") {
		t.Errorf("expected env prompt header, got:\n%s", personaContent)
	}
	if !strings.Contains(personaContent, "Custom Prompt") {
		t.Errorf("expected custom prompt, got:\n%s", personaContent)
	}
	if strings.Contains(personaContent, "# Base System Architecture & Operational Rules") {
		t.Errorf("user_persona.md must NOT contain gemini system rules, got:\n%s", personaContent)
	}
	if len(personaData) > MaxRuleFileSizeBytes {
		t.Errorf("user_persona.md must be <= %d-byte ceiling, got %d bytes", MaxRuleFileSizeBytes, len(personaData))
	}

	geminiRuleFile := filepath.Join(tmpHome, ".gemini", "rules", "system_invariants.md")
	geminiData, err := os.ReadFile(geminiRuleFile)
	if err != nil {
		t.Fatalf("Failed to read system_invariants.md: %v", err)
	}
	geminiContent := string(geminiData)
	if len(geminiData) > MaxRuleFileSizeBytes {
		t.Errorf("system_invariants.md must be <= %d-byte ceiling, got %d bytes", MaxRuleFileSizeBytes, len(geminiData))
	}

	if !strings.Contains(geminiContent, "# Base System Architecture & Operational Rules (GEMINI.md)") {
		t.Errorf("expected gemini header, got:\n%s", geminiContent)
	}
	if !strings.Contains(geminiContent, "Core Invariant 5: No Option B") {
		t.Errorf("expected gemini content, got:\n%s", geminiContent)
	}
	if strings.Contains(geminiContent, "# User Persona Overrides") {
		t.Errorf("system_invariants.md must NOT contain persona overrides, got:\n%s", geminiContent)
	}

	// Verify copies in config rules directory
	configPersona := filepath.Join(tmpHome, ".gemini", "config", "rules", "user_persona.md")
	if _, err := os.Stat(configPersona); err != nil {
		t.Errorf("expected config user_persona.md copy to exist: %v", err)
	}
	configGemini := filepath.Join(tmpHome, ".gemini", "config", "rules", "system_invariants.md")
	if _, err := os.Stat(configGemini); err != nil {
		t.Errorf("expected config system_invariants.md copy to exist: %v", err)
	}

	// 2. Verify torn read fallback for GEMINI.md
	if err := os.WriteFile(geminiPath, []byte(""), 0644); err != nil {
		t.Fatal(err)
	}
	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed on torn read: %v", err)
	}
	geminiDataAfterTorn, err := os.ReadFile(geminiRuleFile)
	if err != nil {
		t.Fatal(err)
	}
	geminiContentAfterTorn := string(geminiDataAfterTorn)
	if !strings.Contains(geminiContentAfterTorn, "Core Invariant 5: No Option B") {
		t.Errorf("expected LKGC gemini content to persist after torn read, got:\n%s", geminiContentAfterTorn)
	}

	// 3. Verify torn read fallback for AGENTS.md
	if err := os.WriteFile(agentsPath, []byte(""), 0644); err != nil {
		t.Fatal(err)
	}
	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed on torn read for persona: %v", err)
	}
	personaDataAfterTorn, err := os.ReadFile(personaRuleFile)
	if err != nil {
		t.Fatal(err)
	}
	personaContentAfterTorn := string(personaDataAfterTorn)
	if !strings.Contains(personaContentAfterTorn, "ABG Baddie Persona") {
		t.Errorf("expected LKGC persona content to persist after torn read, got:\n%s", personaContentAfterTorn)
	}

	// 4. Verify LKGC fallback for system_invariants.md when search path produces no file at all
	p.SetSystemInstructionsSearchPaths([]string{filepath.Join(srcDir, "nonexistent_gemini.md")})
	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed when gemini instructions path missing: %v", err)
	}
	geminiDataAfterMissing, err := os.ReadFile(geminiRuleFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(geminiDataAfterMissing), "Core Invariant 5: No Option B") {
		t.Errorf("expected lkgcGeminiRule to persist when file not found, got:\n%s", string(geminiDataAfterMissing))
	}
}

func TestSyncRules_EdgeCasesAndCoverage(t *testing.T) {
	if len(DefaultSystemInstructionsSearchPaths) == 0 {
		t.Error("expected non-empty DefaultSystemInstructionsSearchPaths")
	}

	// 1. Nil provisioner and empty home dir
	var nilP *Provisioner
	if err := nilP.SyncRules("prompt"); err != nil {
		t.Errorf("expected nil provisioner SyncRules to return nil, got %v", err)
	}
	emptyP := New("", t.TempDir())
	if err := emptyP.SyncRules("prompt"); err != nil {
		t.Errorf("expected empty homeDir SyncRules to return nil, got %v", err)
	}
	if err := nilP.Sync(context.Background(), nil); err != nil {
		t.Errorf("expected nil provisioner Sync to return nil, got %v", err)
	}
	if err := emptyP.Sync(context.Background(), nil); err != nil {
		t.Errorf("expected empty homeDir Sync to return nil, got %v", err)
	}

	// 2. Default SystemInstructions search paths fallback (when slice is empty)
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)
	p.SetSystemInstructionsSearchPaths(nil)
	p.SetAgentInstructionsSearchPaths([]string{filepath.Join(t.TempDir(), "nonexistent.md")})
	// Sync with custom prompt -> verifies DefaultSystemInstructionsSearchPaths loaded
	if err := p.SyncRules("Test Prompt"); err != nil {
		t.Fatalf("expected SyncRules to succeed with default system paths, got %v", err)
	}

	// 3. No instructions found anywhere: first sync returns nil, subsequent uses LKGC
	tmpHome2 := t.TempDir()
	p2 := New(tmpHome2, t.TempDir())
	nonexistent := filepath.Join(t.TempDir(), "nonexistent.md")
	p2.SetAgentInstructionsSearchPaths([]string{nonexistent})
	p2.SetSystemInstructionsSearchPaths([]string{nonexistent})

	if err := p2.SyncRules(""); err != nil {
		t.Fatalf("expected SyncRules to return nil when no rules found and no LKGC, got %v", err)
	}

	if err := p2.SyncRules("Initial Prompt Setting LKGC"); err != nil {
		t.Fatalf("failed to set initial LKGC rules: %v", err)
	}
	if err := p2.SyncRules(""); err != nil {
		t.Fatalf("failed to sync with LKGC rules: %v", err)
	}
	ruleFile := filepath.Join(tmpHome2, ".gemini", "rules", "user_persona.md")
	data, err := os.ReadFile(ruleFile)
	if err != nil {
		t.Fatalf("failed to read user_persona.md: %v", err)
	}
	if !strings.Contains(string(data), "Initial Prompt Setting LKGC") {
		t.Errorf("expected LKGC rules content, got %s", string(data))
	}

	// 4. Torn read of GEMINI on initial attempt when p.lkgcGeminiSource is empty
	pTorn := New(t.TempDir(), t.TempDir())
	emptyGemini := filepath.Join(t.TempDir(), "GEMINI.md")
	if err := os.WriteFile(emptyGemini, []byte("   "), 0644); err != nil {
		t.Fatal(err)
	}
	pTorn.SetSystemInstructionsSearchPaths([]string{emptyGemini})
	pTorn.SetAgentInstructionsSearchPaths([]string{filepath.Join(t.TempDir(), "nonexistent.md")})
	if err := pTorn.SyncRules("Custom Prompt"); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}

	// 5. GEMINI LKGC file persistence and LKGC read path
	tmpHome3 := t.TempDir()
	tmpData3 := t.TempDir()
	p3 := New(tmpHome3, tmpData3)
	geminiSrc := filepath.Join(t.TempDir(), "GEMINI.md")
	if err := os.WriteFile(geminiSrc, []byte("Gemini Rules To Persist"), 0644); err != nil {
		t.Fatal(err)
	}
	p3.SetAgentInstructionsSearchPaths([]string{filepath.Join(t.TempDir(), "nonexistent.md")})
	p3.SetSystemInstructionsSearchPaths([]string{geminiSrc})
	if err := p3.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}
	lkgcFile := filepath.Join(tmpData3, ".GEMINI.md.lkgc")
	if lkgcData, err := os.ReadFile(lkgcFile); err != nil || !strings.Contains(string(lkgcData), "Gemini Rules To Persist") {
		t.Errorf("expected .GEMINI.md.lkgc to be written with content, got %s (err: %v)", string(lkgcData), err)
	}

	// Now read from .GEMINI.md.lkgc directly to cover personaSource == ".GEMINI.md.lkgc" bypass
	p4 := New(t.TempDir(), tmpData3)
	p4.SetAgentInstructionsSearchPaths([]string{filepath.Join(t.TempDir(), "nonexistent.md")})
	p4.SetSystemInstructionsSearchPaths([]string{lkgcFile})
	if err := p4.SyncRules(""); err != nil {
		t.Fatalf("SyncRules with .GEMINI.md.lkgc failed: %v", err)
	}
}

func TestProvisioner_NilAndEmptyEdgeCases(t *testing.T) {
	var pNil *Provisioner
	if err := pNil.Sync(context.Background(), nil); err != nil {
		t.Errorf("Expected nil error for nil provisioner Sync, got %v", err)
	}
	if err := pNil.SyncSkills(); err != nil {
		t.Errorf("Expected nil error for nil provisioner SyncSkills, got %v", err)
	}
	pEmpty := &Provisioner{}
	if err := pEmpty.Sync(context.Background(), nil); err != nil {
		t.Errorf("Expected nil error for empty homeDir provisioner Sync, got %v", err)
	}
	if err := pEmpty.SyncSkills(); err != nil {
		t.Errorf("Expected nil error for empty homeDir provisioner SyncSkills, got %v", err)
	}
	if pEmpty.HomeDir() != "" {
		t.Errorf("Expected empty HomeDir, got %s", pEmpty.HomeDir())
	}
	if pEmpty.DataDir() != "" {
		t.Errorf("Expected empty DataDir, got %s", pEmpty.DataDir())
	}

	// Test legacy directories cleanup in SyncSkills
	tmpHome := t.TempDir()
	pLegacy := New(tmpHome, t.TempDir())
	legacySkills := filepath.Join(tmpHome, ".gemini", "skills")
	legacyPlugins := filepath.Join(tmpHome, ".gemini", "config", "plugins", "superpowers")
	_ = os.MkdirAll(legacySkills, 0755)
	_ = os.MkdirAll(legacyPlugins, 0755)
	if err := pLegacy.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills failed with legacy dirs: %v", err)
	}
	if _, err := os.Stat(legacySkills); !os.IsNotExist(err) {
		t.Errorf("expected legacy skills dir to be removed")
	}
	if _, err := os.Stat(legacyPlugins); !os.IsNotExist(err) {
		t.Errorf("expected legacy plugins dir to be removed")
	}
}

func TestSyncRules_ConfigRuleWriteFailure(t *testing.T) {
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())
	configRulesDir := filepath.Join(tmpHome, ".gemini", "config", "rules")
	_ = os.MkdirAll(configRulesDir, 0755)
	// Create user_persona.md and system_invariants.md as non-empty directories so writeAtomic fails
	blockedPersona := filepath.Join(configRulesDir, "user_persona.md")
	_ = os.MkdirAll(filepath.Join(blockedPersona, "sub"), 0755)
	blockedGemini := filepath.Join(configRulesDir, "system_invariants.md")
	_ = os.MkdirAll(filepath.Join(blockedGemini, "sub"), 0755)

	srcDir := t.TempDir()
	agentsPath := filepath.Join(srcDir, "AGENTS.md")
	_ = os.WriteFile(agentsPath, []byte("Content"), 0644)
	geminiPath := filepath.Join(srcDir, "GEMINI.md")
	_ = os.WriteFile(geminiPath, []byte("Gemini Content"), 0644)
	p.SetAgentInstructionsSearchPaths([]string{agentsPath})
	p.SetSystemInstructionsSearchPaths([]string{geminiPath})

	// SyncRules should succeed overall and log warnings for config rules
	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}
}

func TestSyncRules_PrimaryWriteFailure(t *testing.T) {
	t.Parallel()
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())
	primaryRulesDir := filepath.Join(tmpHome, ".gemini", "rules")
	_ = os.MkdirAll(primaryRulesDir, 0755)

	srcDir := t.TempDir()
	agentsPath := filepath.Join(srcDir, "AGENTS.md")
	_ = os.WriteFile(agentsPath, []byte("Persona Content"), 0644)
	geminiPath := filepath.Join(srcDir, "GEMINI.md")
	_ = os.WriteFile(geminiPath, []byte("Gemini Content"), 0644)
	p.SetAgentInstructionsSearchPaths([]string{agentsPath})
	p.SetSystemInstructionsSearchPaths([]string{geminiPath})

	// 1. Fail persona write
	blockedPersona := filepath.Join(primaryRulesDir, "user_persona.md")
	_ = os.MkdirAll(filepath.Join(blockedPersona, "sub"), 0755)
	if err := p.SyncRules(""); err == nil {
		t.Errorf("expected SyncRules to fail when primary user_persona.md cannot be written")
	}
	_ = os.RemoveAll(blockedPersona)

	// 2. Fail gemini write
	blockedGemini := filepath.Join(primaryRulesDir, "system_invariants.md")
	_ = os.MkdirAll(filepath.Join(blockedGemini, "sub"), 0755)
	if err := p.SyncRules(""); err == nil {
		t.Errorf("expected SyncRules to fail when primary system_invariants.md cannot be written")
	}
}

func TestProvisioner_WriteAtomicAndDirErrors(t *testing.T) {
	t.Parallel()
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())

	// Test writeAtomic failure when parent directory cannot be created (blocked by file)
	blockFile := filepath.Join(tmpHome, "block_file")
	if err := os.WriteFile(blockFile, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := p.writeAtomic(filepath.Join(blockFile, "sub", "impossible.txt"), "data"); err == nil {
		t.Errorf("expected writeAtomic to fail when parent dir creation fails")
	}

	// Test primaryRulesDir creation error
	pBadHome := New(filepath.Join(blockFile, "subhome"), t.TempDir())
	if err := pBadHome.SyncRules("prompt"); err == nil {
		t.Errorf("expected SyncRules to fail when primaryRulesDir creation fails")
	}
}

func TestSyncRules_RuleFileSizeCeiling(t *testing.T) {
	t.Parallel()
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	dir := t.TempDir()
	agentsPath := filepath.Join(dir, "AGENTS.md")
	geminiPath := filepath.Join(dir, "GEMINI.md")

	// 1. Valid compliant content under ceiling
	validContent := strings.Repeat("A", 1000)
	if err := os.WriteFile(agentsPath, []byte(validContent), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(geminiPath, []byte(validContent), 0644); err != nil {
		t.Fatal(err)
	}
	p.SetAgentInstructionsSearchPaths([]string{agentsPath})
	p.SetSystemInstructionsSearchPaths([]string{geminiPath})

	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed with compliant files: %v", err)
	}

	personaFile := filepath.Join(tmpHome, ".gemini", "rules", "user_persona.md")
	personaBytes, err := os.ReadFile(personaFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(personaBytes) > MaxRuleFileSizeBytes {
		t.Errorf("persona rule should be <= %d, got %d", MaxRuleFileSizeBytes, len(personaBytes))
	}

	geminiFile := filepath.Join(tmpHome, ".gemini", "rules", "system_invariants.md")
	geminiBytes, err := os.ReadFile(geminiFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(geminiBytes) > MaxRuleFileSizeBytes {
		t.Errorf("gemini rule should be <= %d, got %d", MaxRuleFileSizeBytes, len(geminiBytes))
	}

	// 2. Oversized content (> 23 KB): verify LKGC fallback prevents prompt truncation
	oversizedContent := strings.Repeat("B", MaxRuleFileSizeBytes+100)
	if err := os.WriteFile(geminiPath, []byte(oversizedContent), 0644); err != nil {
		t.Fatal(err)
	}

	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules should not fail on oversized rule, got error: %v", err)
	}

	geminiBytesAfter, err := os.ReadFile(geminiFile)
	if err != nil {
		t.Fatal(err)
	}
	// Should fall back to LKGC version (which is <= MaxRuleFileSizeBytes) rather than the oversized version
	if len(geminiBytesAfter) > MaxRuleFileSizeBytes {
		t.Errorf("expected LKGC fallback under %d bytes, got %d bytes", MaxRuleFileSizeBytes, len(geminiBytesAfter))
	}
	if strings.Contains(string(geminiBytesAfter), strings.Repeat("B", 100)) {
		t.Errorf("oversized content should not have overwritten compliant LKGC rule")
	}

	// Verify disk LKGC for GEMINI was not poisoned with oversized content
	diskGeminiLKGC, err := os.ReadFile(filepath.Join(tmpData, ".GEMINI.md.lkgc"))
	if err == nil && strings.Contains(string(diskGeminiLKGC), strings.Repeat("B", 100)) {
		t.Errorf("disk LKGC .GEMINI.md.lkgc should not be poisoned by oversized rule")
	}

	// 3. Oversized persona content: verify persona LKGC fallback
	oversizedPersona := strings.Repeat("C", MaxRuleFileSizeBytes+100)
	if err := os.WriteFile(agentsPath, []byte(oversizedPersona), 0644); err != nil {
		t.Fatal(err)
	}

	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules should not fail on oversized persona, got error: %v", err)
	}

	personaBytesAfter, err := os.ReadFile(personaFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(personaBytesAfter) > MaxRuleFileSizeBytes {
		t.Errorf("expected persona LKGC fallback under %d bytes, got %d bytes", MaxRuleFileSizeBytes, len(personaBytesAfter))
	}
	if strings.Contains(string(personaBytesAfter), strings.Repeat("C", 100)) {
		t.Errorf("oversized persona should not have overwritten compliant LKGC rule")
	}

	// Verify disk LKGC for AGENTS was not poisoned with oversized persona
	diskPersonaLKGC, err := os.ReadFile(filepath.Join(tmpData, ".AGENTS.md.lkgc"))
	if err == nil && strings.Contains(string(diskPersonaLKGC), strings.Repeat("C", 100)) {
		t.Errorf("disk LKGC .AGENTS.md.lkgc should not be poisoned by oversized persona")
	}

	// 4. Verify repo GEMINI.md stays strictly under MaxSourceRuleFileSizeBytes
	repoGemini := filepath.Join("..", "..", "GEMINI.md")
	if data, err := os.ReadFile(repoGemini); err == nil {
		if len(data) > MaxSourceRuleFileSizeBytes {
			t.Errorf("Repository GEMINI.md (%d bytes) exceeds MaxSourceRuleFileSizeBytes (%d bytes)", len(data), MaxSourceRuleFileSizeBytes)
		}
	}
}





