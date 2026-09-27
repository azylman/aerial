package env

import (
	"context"
	"encoding/json"
	"errors"
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

func TestSyncRules_ModularDiscoveryAndTornReadProtection(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	// Create aerial rules
	aerialDir := t.TempDir()
	p.SetAerialRulesDir(aerialDir)
	_ = os.MkdirAll(filepath.Join(aerialDir, "common"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialDir, "discord"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialDir, "voice"), 0755)

	_ = os.WriteFile(filepath.Join(aerialDir, "common", "01-invariants.md"), []byte("Common Invariants"), 0644)
	_ = os.WriteFile(filepath.Join(aerialDir, "discord", "01-chat.md"), []byte("Discord Chat"), 0644)
	_ = os.WriteFile(filepath.Join(aerialDir, "voice", "01-speech.md"), []byte("Voice Speech"), 0644)

	// Create user config rules
	configDir := t.TempDir()
	p.SetConfigRulesDir(configDir)
	_ = os.MkdirAll(filepath.Join(configDir, "common"), 0755)
	_ = os.MkdirAll(filepath.Join(configDir, "discord"), 0755)
	_ = os.MkdirAll(filepath.Join(configDir, "voice"), 0755)

	_ = os.WriteFile(filepath.Join(configDir, "common", "01-identity.md"), []byte("User Alex"), 0644)
	_ = os.WriteFile(filepath.Join(configDir, "discord", "01-persona.md"), []byte("ABG Persona"), 0644)
	_ = os.WriteFile(filepath.Join(configDir, "voice", "01-persona.md"), []byte("Chill Persona"), 0644)

	// 1. Initial sync with custom prompt
	if err := p.SyncRules("Custom System Prompt"); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}

	primaryRules := filepath.Join(tmpHome, ".gemini", "rules")
	entries, err := os.ReadDir(primaryRules)
	if err != nil {
		t.Fatalf("Failed to read primary rules dir: %v", err)
	}

	expectedFiles := []string{
		"00_aerial_common_01-invariants.md",
		"10_user_common_01-identity.md",
		"20_aerial_discord_01-chat.md",
		"30_user_discord_01-persona.md",
		"99_custom_prompt.md",
	}
	var fileNames []string
	for _, e := range entries {
		fileNames = append(fileNames, e.Name())
	}
	for _, expected := range expectedFiles {
		found := false
		for _, name := range fileNames {
			if name == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected rule file %s in primary rules, got files: %v", expected, fileNames)
		}
	}

	// Verify target isolation in runtimes
	discordRulesDir := filepath.Join(tmpData, "runtimes", "discord", ".gemini", "rules")
	if _, err := os.Stat(filepath.Join(discordRulesDir, "20_aerial_discord_01-chat.md")); err != nil {
		t.Errorf("Expected discord rule in discord runtime: %v", err)
	}
	if _, err := os.Stat(filepath.Join(discordRulesDir, "20_aerial_voice_01-speech.md")); !os.IsNotExist(err) {
		t.Errorf("Voice rule must not exist in discord runtime")
	}

	voiceRulesDir := filepath.Join(tmpData, "runtimes", "voice", ".gemini", "rules")
	if _, err := os.Stat(filepath.Join(voiceRulesDir, "20_aerial_voice_01-speech.md")); err != nil {
		t.Errorf("Expected voice rule in voice runtime: %v", err)
	}
	if _, err := os.Stat(filepath.Join(voiceRulesDir, "20_aerial_discord_01-chat.md")); !os.IsNotExist(err) {
		t.Errorf("Discord rule must not exist in voice runtime")
	}
	if _, err := os.Stat(filepath.Join(voiceRulesDir, "99_custom_prompt.md")); !os.IsNotExist(err) {
		t.Errorf("Custom prompt must not exist in voice runtime")
	}

	// 2. Torn read: empty file in rules aborts compilation and retains existing rules
	_ = os.WriteFile(filepath.Join(aerialDir, "discord", "01-chat.md"), []byte("   \n\t"), 0644)
	if err := p.SyncRules("Updated Prompt"); err == nil {
		t.Fatalf("Expected SyncRules to fail on torn read (empty file)")
	}

	// Verify previous rules were retained
	content, err := os.ReadFile(filepath.Join(primaryRules, "20_aerial_discord_01-chat.md"))
	if err != nil {
		t.Fatalf("Failed to read retained rule: %v", err)
	}
	if !strings.Contains(string(content), "Discord Chat") {
		t.Errorf("Expected active rule to be retained after torn read, got: %s", string(content))
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

	// Verify custom server was merged
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
	tmpAgentsSkills := t.TempDir()

	p := New(tmpHome, t.TempDir())
	p.SetCustomSkillsDir(tmpCustomSkills)
	p.SetSuperpowersDir(tmpSuperpowers)
	p.SetAgentsSkillsDir(tmpAgentsSkills)

	// Custom skill
	customDir := filepath.Join(tmpCustomSkills, "my-custom-skill")
	_ = os.MkdirAll(customDir, 0755)
	_ = os.WriteFile(filepath.Join(customDir, "SKILL.md"), []byte("# My Custom Skill"), 0644)

	// Curated methodology skill
	tddDir := filepath.Join(tmpSuperpowers, "test-driven-development")
	_ = os.MkdirAll(tddDir, 0755)
	_ = os.WriteFile(filepath.Join(tddDir, "SKILL.md"), []byte("# TDD"), 0644)

	// Core built-in skill
	incidentDir := filepath.Join(tmpAgentsSkills, "incident-triage")
	_ = os.MkdirAll(incidentDir, 0755)
	_ = os.WriteFile(filepath.Join(incidentDir, "SKILL.md"), []byte("# Incident Triage"), 0644)

	if err := p.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills failed: %v", err)
	}

	skillsRoot := filepath.Join(tmpHome, ".gemini", "config", "skills")
	for _, name := range []string{"my-custom-skill", "test-driven-development", "incident-triage"} {
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

	aerialDir := t.TempDir()
	p.SetAerialRulesDir(aerialDir)
	_ = os.MkdirAll(filepath.Join(aerialDir, "common"), 0755)
	_ = os.WriteFile(filepath.Join(aerialDir, "common", "01.md"), []byte("Common instructions"), 0644)

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

	ruleFile := filepath.Join(tmpHome, ".gemini", "rules", "99_custom_prompt.md")
	data, err := os.ReadFile(ruleFile)
	if err != nil {
		t.Fatalf("Failed to read 99_custom_prompt.md: %v", err)
	}
	if !strings.Contains(string(data), "iteration prompt") {
		t.Errorf("Expected prompt in 99_custom_prompt.md, got: %s", string(data))
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

	// 4. EnsureAgySettingsForHome with empty homeDir
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

	// 5. Custom MCP servers preserve declared serverUrl verbatim without rewrite
	cfgCustom := config.NewTestConfig(func(d *config.ConfigData) {
		d.GitHubPAT = "dummy-pat"
		d.McpServers = map[string]json.RawMessage{
			"docker":          json.RawMessage(`{"serverUrl":"http://docker-mcp:4002/mcp"}`),
			"custom-sse":      json.RawMessage(`{"serverUrl":"http://custom-server:9000/sse"}`),
			"victoriametrics": json.RawMessage(`{"serverUrl":"http://victoriametrics-mcp:4004/mcp"}`),
		}
	})
	raw = p.LoadMCPConfig(cfgCustom)
	rawStr := string(raw)
	for _, svc := range []string{"docker-mcp:4002/mcp", "custom-server:9000/sse", "victoriametrics-mcp:4004/mcp"} {
		if !strings.Contains(rawStr, svc) {
			t.Errorf("Expected endpoint %q preserved in config, got: %s", svc, rawStr)
		}
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

func TestSyncRules_LobotomyAndErrorBranches(t *testing.T) {
	// 1. Lobotomy guardrail: 0 common files aborts compilation
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())
	emptyAerial := t.TempDir()
	p.SetAerialRulesDir(emptyAerial)
	emptyConfig := t.TempDir()
	p.SetConfigRulesDir(emptyConfig)

	err := p.SyncRules("")
	if err == nil || !strings.Contains(err.Error(), "common rules directory has 0 files") {
		t.Errorf("Expected lobotomy guardrail error, got: %v", err)
	}

	// 2. Setup valid common rules and test successful sync
	_ = os.MkdirAll(filepath.Join(emptyAerial, "common"), 0755)
	_ = os.WriteFile(filepath.Join(emptyAerial, "common", "01-invariants.md"), []byte("Invariants"), 0644)
	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed with valid common rules: %v", err)
	}

	// 3. Torn read: empty file in common aborts
	tornFile := filepath.Join(emptyAerial, "common", "02-torn.md")
	_ = os.WriteFile(tornFile, []byte("   \n\t"), 0644)
	if err := p.SyncRules(""); err == nil || !strings.Contains(err.Error(), "possible git sync torn read") {
		t.Errorf("Expected torn read error, got: %v", err)
	}
	_ = os.Remove(tornFile)

	// 4. MkdirAll error for primary home
	tmpHomeErr := t.TempDir()
	pErr := New(tmpHomeErr, t.TempDir())
	pErr.SetAerialRulesDir(emptyAerial)
	_ = os.WriteFile(filepath.Join(tmpHomeErr, ".gemini"), []byte("blocking-file"), 0644)
	if err := pErr.SyncRules(""); err == nil {
		t.Errorf("Expected error when .gemini cannot be created as directory")
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

func TestSyncRules_ModularDirectoryCompilationAndIsolation(t *testing.T) {
	t.Parallel()

	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	aerialDir := t.TempDir()
	p.SetAerialRulesDir(aerialDir)
	_ = os.MkdirAll(filepath.Join(aerialDir, "common"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialDir, "discord"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialDir, "voice"), 0755)

	configDir := t.TempDir()
	p.SetConfigRulesDir(configDir)
	_ = os.MkdirAll(filepath.Join(configDir, "common"), 0755)
	_ = os.MkdirAll(filepath.Join(configDir, "discord"), 0755)
	_ = os.MkdirAll(filepath.Join(configDir, "voice"), 0755)

	// Write rules without frontmatter to test auto-wrapping
	_ = os.WriteFile(filepath.Join(aerialDir, "common", "01-arch.md"), []byte("# Core Architecture"), 0644)
	_ = os.WriteFile(filepath.Join(configDir, "common", "01-identity.md"), []byte("# Identity Alex"), 0644)
	_ = os.WriteFile(filepath.Join(aerialDir, "discord", "01-msg.md"), []byte("# Discord Messaging"), 0644)
	_ = os.WriteFile(filepath.Join(configDir, "discord", "01-slang.md"), []byte("# ABG Slang"), 0644)
	_ = os.WriteFile(filepath.Join(aerialDir, "voice", "01-audio.md"), []byte("# Voice Output"), 0644)
	_ = os.WriteFile(filepath.Join(configDir, "voice", "01-cadence.md"), []byte("# Chill Cadence"), 0644)

	if err := p.SyncRules("Custom Prompt Override"); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}

	// 1. Primary rules check (default target: discord)
	primaryRulesDir := filepath.Join(tmpHome, ".gemini", "rules")
	expectedDiscordRules := []string{
		"00_aerial_common_01-arch.md",
		"10_user_common_01-identity.md",
		"20_aerial_discord_01-msg.md",
		"30_user_discord_01-slang.md",
		"99_custom_prompt.md",
	}
	for _, expected := range expectedDiscordRules {
		filePath := filepath.Join(primaryRulesDir, expected)
		data, err := os.ReadFile(filePath)
		if err != nil {
			t.Errorf("Expected primary rule file %s: %v", expected, err)
			continue
		}
		if len(data) > MaxRuleFileSizeBytes {
			t.Errorf("Rule %s exceeds %d bytes (%d bytes)", expected, MaxRuleFileSizeBytes, len(data))
		}
		content := string(data)
		if !strings.HasPrefix(content, "---") || !strings.Contains(content, "trigger: always_on") {
			t.Errorf("Rule %s missing valid YAML frontmatter:\n%s", expected, content)
		}
	}

	// Verify voice rules are NOT in primary (discord) rules
	unexpectedInDiscord := []string{"20_aerial_voice_01-audio.md", "30_user_voice_01-cadence.md"}
	for _, unexpected := range unexpectedInDiscord {
		if _, err := os.Stat(filepath.Join(primaryRulesDir, unexpected)); !os.IsNotExist(err) {
			t.Errorf("Voice rule %s should not exist in primary discord rules", unexpected)
		}
	}

	// 2. Runtime target isolation
	runtimesRoot := filepath.Join(tmpData, "runtimes")
	discordRuntimeDir := filepath.Join(runtimesRoot, "discord", ".gemini", "rules")
	voiceRuntimeDir := filepath.Join(runtimesRoot, "voice", ".gemini", "rules")

	if _, err := os.Stat(filepath.Join(discordRuntimeDir, "20_aerial_discord_01-msg.md")); err != nil {
		t.Errorf("Expected discord rule in discord runtime: %v", err)
	}
	if _, err := os.Stat(filepath.Join(voiceRuntimeDir, "20_aerial_voice_01-audio.md")); err != nil {
		t.Errorf("Expected voice rule in voice runtime: %v", err)
	}
	if _, err := os.Stat(filepath.Join(voiceRuntimeDir, "20_aerial_discord_01-msg.md")); !os.IsNotExist(err) {
		t.Errorf("Discord rule must not exist in voice runtime")
	}
	if _, err := os.Stat(filepath.Join(voiceRuntimeDir, "99_custom_prompt.md")); !os.IsNotExist(err) {
		t.Errorf("Custom prompt must not exist in voice runtime")
	}

	// 3. Verify shared asset provisioning in runtime
	voiceSettings := filepath.Join(runtimesRoot, "voice", ".gemini", "antigravity-cli", "settings.json")
	if _, err := os.Stat(filepath.Join(tmpHome, ".gemini", "antigravity-cli", "settings.json")); err == nil {
		if _, err := os.Stat(voiceSettings); err != nil {
			t.Errorf("Expected settings.json copied to voice runtime: %v", err)
		}
	}
}

func TestSyncRules_EdgeCasesAndCoverage(t *testing.T) {
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

	// 2. Missing target directory in aerial or config does not error if common rules exist
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())
	aerialDir := t.TempDir()
	p.SetAerialRulesDir(aerialDir)
	_ = os.MkdirAll(filepath.Join(aerialDir, "common"), 0755)
	_ = os.WriteFile(filepath.Join(aerialDir, "common", "01.md"), []byte("common"), 0644)
	// Empty config dir with no subdirectories
	p.SetConfigRulesDir(t.TempDir())

	if err := p.SyncRules(""); err != nil {
		t.Fatalf("expected SyncRules to succeed when target directory missing: %v", err)
	}
}

func TestSyncRules_ConfigRuleWriteFailure(t *testing.T) {
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())
	aerialDir := t.TempDir()
	p.SetAerialRulesDir(aerialDir)
	_ = os.MkdirAll(filepath.Join(aerialDir, "common"), 0755)
	_ = os.WriteFile(filepath.Join(aerialDir, "common", "01.md"), []byte("common"), 0644)

	// Block .gemini/config with a regular file
	_ = os.MkdirAll(filepath.Join(tmpHome, ".gemini"), 0755)
	_ = os.WriteFile(filepath.Join(tmpHome, ".gemini", "config"), []byte("blocker"), 0644)

	// SyncRules should succeed overall and log warnings for config rules
	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed: %v", err)
	}
}

func TestSyncRules_PrimaryWriteFailure(t *testing.T) {
	t.Parallel()
	tmpHome := t.TempDir()
	p := New(tmpHome, t.TempDir())

	aerialDir := t.TempDir()
	p.SetAerialRulesDir(aerialDir)
	_ = os.MkdirAll(filepath.Join(aerialDir, "common"), 0755)
	_ = os.WriteFile(filepath.Join(aerialDir, "common", "01.md"), []byte("common"), 0644)

	// Block .gemini with a non-directory file so primary rules directory cannot be created
	_ = os.WriteFile(filepath.Join(tmpHome, ".gemini"), []byte("blocking-file"), 0644)
	if err := p.SyncRules(""); err == nil {
		t.Errorf("expected SyncRules to fail when .gemini cannot be created as directory")
	}
}

func TestSyncRules_RuleFileSizeCeiling(t *testing.T) {
	t.Parallel()
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	dir := t.TempDir()
	p.SetAerialRulesDir(dir)
	_ = os.MkdirAll(filepath.Join(dir, "common"), 0755)

	validContent := strings.Repeat("A", 1000)
	rulePath := filepath.Join(dir, "common", "01.md")
	if err := os.WriteFile(rulePath, []byte(validContent), 0644); err != nil {
		t.Fatal(err)
	}

	if err := p.SyncRules(""); err != nil {
		t.Fatalf("SyncRules failed with compliant files: %v", err)
	}

	// Oversized source rule file (> 23040 bytes): compilation must fail
	oversizedContent := strings.Repeat("B", MaxSourceRuleFileSizeBytes+100)
	if err := os.WriteFile(rulePath, []byte(oversizedContent), 0644); err != nil {
		t.Fatal(err)
	}

	if err := p.SyncRules(""); err == nil {
		t.Fatalf("SyncRules should fail on oversized source rule")
	}

	// Verify all repo rule files stay strictly under MaxSourceRuleFileSizeBytes
	var repoRulesDir string
	for _, candidate := range []string{"rules", "../rules", "../../rules", "../../../rules", "/share/aerial/rules"} {
		if fi, err := os.Stat(candidate); err == nil && fi.IsDir() {
			repoRulesDir = candidate
			break
		}
	}
	if repoRulesDir != "" {
		err := filepath.Walk(repoRulesDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasSuffix(info.Name(), ".md") {
				if info.Size() > MaxSourceRuleFileSizeBytes {
					t.Errorf("Rule file %s (%d bytes) exceeds MaxSourceRuleFileSizeBytes (%d bytes)", path, info.Size(), MaxSourceRuleFileSizeBytes)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to walk repo rules: %v", err)
		}
	} else {
		t.Log("Warning: repo rules directory not found in candidate paths")
	}
}

func TestProvisionRuntimeSharedAssets_Comprehensive(t *testing.T) {
	primaryHome := t.TempDir()
	runtimeHome := t.TempDir()

	p := New(primaryHome, t.TempDir())

	// 1. Setup primary files
	primaryGemini := filepath.Join(primaryHome, ".gemini")
	_ = os.MkdirAll(filepath.Join(primaryGemini, "antigravity-cli"), 0755)
	_ = os.MkdirAll(filepath.Join(primaryGemini, "config", "skills"), 0755)
	_ = os.MkdirAll(filepath.Join(primaryGemini, "skills"), 0755)

	_ = os.WriteFile(filepath.Join(primaryGemini, "antigravity-cli", "settings.json"), []byte(`{"model":"gemini-test"}`), 0644)
	_ = os.WriteFile(filepath.Join(primaryGemini, "config", "mcp_config.json"), []byte(`{"mcpServers":{}}`), 0644)
	_ = os.WriteFile(filepath.Join(primaryGemini, "config", "skills", "test.txt"), []byte("skill-data"), 0644)
	_ = os.WriteFile(filepath.Join(primaryGemini, "skills", "legacy.txt"), []byte("legacy-data"), 0644)
	_ = os.WriteFile(filepath.Join(primaryGemini, "antigravity-cli", "antigravity-oauth-token"), []byte(`{"token":"secret"}`), 0644)
	_ = os.WriteFile(filepath.Join(primaryGemini, "antigravity-cli", "mcp_oauth_tokens.json"), []byte(`{"mcp":"secret"}`), 0644)
	_ = os.WriteFile(filepath.Join(primaryGemini, "antigravity-cli", "installation_id"), []byte("uuid-1234"), 0644)
	_ = os.WriteFile(filepath.Join(primaryGemini, "antigravity-cli", "jetski_state.pbtxt"), []byte("jetski: true"), 0644)
	_ = os.WriteFile(filepath.Join(primaryGemini, "mcp_oauth_tokens.json"), []byte(`{"root_mcp":"secret"}`), 0644)

	// Call provisionRuntimeSharedAssets - covers settings, mcp, skills, legacy skills copying/symlinks, and shared credentials
	p.provisionRuntimeSharedAssets(runtimeHome)

	targetGemini := filepath.Join(runtimeHome, ".gemini")
	if data, err := os.ReadFile(filepath.Join(targetGemini, "antigravity-cli", "settings.json")); err != nil || string(data) != `{"model":"gemini-test"}` {
		t.Errorf("settings.json not copied correctly: %v, %s", err, string(data))
	}
	if data, err := os.ReadFile(filepath.Join(targetGemini, "config", "mcp_config.json")); err != nil || string(data) != `{"mcpServers":{}}` {
		t.Errorf("mcp_config.json not copied correctly: %v, %s", err, string(data))
	}
	if data, err := os.ReadFile(filepath.Join(targetGemini, "antigravity-cli", "antigravity-oauth-token")); err != nil || string(data) != `{"token":"secret"}` {
		t.Errorf("antigravity-oauth-token not provisioned correctly: %v, %s", err, string(data))
	}
	if data, err := os.ReadFile(filepath.Join(targetGemini, "antigravity-cli", "mcp_oauth_tokens.json")); err != nil || string(data) != `{"mcp":"secret"}` {
		t.Errorf("mcp_oauth_tokens.json not provisioned correctly: %v, %s", err, string(data))
	}
	if data, err := os.ReadFile(filepath.Join(targetGemini, "antigravity-cli", "installation_id")); err != nil || string(data) != "uuid-1234" {
		t.Errorf("installation_id not provisioned correctly: %v, %s", err, string(data))
	}
	if data, err := os.ReadFile(filepath.Join(targetGemini, "antigravity-cli", "jetski_state.pbtxt")); err != nil || string(data) != "jetski: true" {
		t.Errorf("jetski_state.pbtxt not provisioned correctly: %v, %s", err, string(data))
	}
	if data, err := os.ReadFile(filepath.Join(targetGemini, "mcp_oauth_tokens.json")); err != nil || string(data) != `{"root_mcp":"secret"}` {
		t.Errorf("root mcp_oauth_tokens.json not provisioned correctly: %v, %s", err, string(data))
	}

	// 2. Call again to test removing existing symlinks/files and replacing them
	p.provisionRuntimeSharedAssets(runtimeHome)

	// 3. Error branches: blocked mkdir
	blockedRuntime := t.TempDir()
	_ = os.WriteFile(filepath.Join(blockedRuntime, ".gemini"), []byte("blocker"), 0644)
	p.provisionRuntimeSharedAssets(blockedRuntime)

	// 4. Blocked write for settings, mcp, skills, and credentials
	blockedFilesRuntime := t.TempDir()
	_ = os.MkdirAll(filepath.Join(blockedFilesRuntime, ".gemini", "antigravity-cli", "settings.json"), 0755)
	_ = os.MkdirAll(filepath.Join(blockedFilesRuntime, ".gemini", "config", "mcp_config.json"), 0755)
	_ = os.MkdirAll(filepath.Join(blockedFilesRuntime, ".gemini", "config", "skills", "blocker"), 0755)
	_ = os.MkdirAll(filepath.Join(blockedFilesRuntime, ".gemini", "skills", "blocker"), 0755)
	_ = os.MkdirAll(filepath.Join(blockedFilesRuntime, ".gemini", "antigravity-cli", "antigravity-oauth-token", "blocker"), 0755)
	_ = os.MkdirAll(filepath.Join(blockedFilesRuntime, ".gemini", "mcp_oauth_tokens.json", "blocker"), 0755)
	p.provisionRuntimeSharedAssets(blockedFilesRuntime)

	// 5. Test forced symlink failure on platforms where symlinks succeed (e.g. Linux CI)
	// to deterministically exercise the fallback atomic copy across all shared assets.
	origSymlink := symlinkRuntimeAsset
	defer func() { symlinkRuntimeAsset = origSymlink }()

	symlinkRuntimeAsset = func(src, dst string) error {
		return errors.New("simulated symlink error")
	}

	fallbackRuntime := t.TempDir()
	p.provisionRuntimeSharedAssets(fallbackRuntime)

	fallbackGemini := filepath.Join(fallbackRuntime, ".gemini")
	if data, err := os.ReadFile(filepath.Join(fallbackGemini, "antigravity-cli", "antigravity-oauth-token")); err != nil || string(data) != `{"token":"secret"}` {
		t.Errorf("antigravity-oauth-token not copied in fallback: %v, %s", err, string(data))
	}
	if data, err := os.ReadFile(filepath.Join(fallbackGemini, "mcp_oauth_tokens.json")); err != nil || string(data) != `{"root_mcp":"secret"}` {
		t.Errorf("mcp_oauth_tokens.json not copied in fallback: %v, %s", err, string(data))
	}

	// 6. Test fallback when primary files are unreadable (e.g. directory instead of file)
	unreadableRuntime := t.TempDir()
	badPrimaryHome := t.TempDir()
	pBad := New(badPrimaryHome, t.TempDir())
	badGemini := filepath.Join(badPrimaryHome, ".gemini")
	_ = os.MkdirAll(filepath.Join(badGemini, "antigravity-cli", "antigravity-oauth-token"), 0755)
	_ = os.MkdirAll(filepath.Join(badGemini, "mcp_oauth_tokens.json"), 0755)
	pBad.provisionRuntimeSharedAssets(unreadableRuntime)
}

func TestAtomicSwapRulesDir_RollbackAndErrors(t *testing.T) {
	tmpDir := t.TempDir()
	p := New(tmpDir, tmpDir)

	// 1. Invalid parentDir (cannot mkdir staging)
	blocker := filepath.Join(tmpDir, "file_parent")
	_ = os.WriteFile(blocker, []byte("blocker"), 0644)
	err := p.atomicSwapRulesDir([]ruleSourceFile{{filename: "01.md", content: "test"}}, filepath.Join(blocker, "sub", "rules"))
	if err == nil {
		t.Errorf("expected error when staging directory cannot be created")
	}

	// 2. Empty rules slice (staging directory is empty)
	targetDir := filepath.Join(tmpDir, "target_rules")
	err = p.atomicSwapRulesDir(nil, targetDir)
	if err == nil || !strings.Contains(err.Error(), "staging rules directory is empty") {
		t.Errorf("expected staging rules directory is empty error, got: %v", err)
	}

	// 3. Successful swap followed by swap over active directory
	validRules := []ruleSourceFile{
		{filename: "01-invariants.md", content: "# Invariants\n", prefix: "00_"},
	}
	if err := p.atomicSwapRulesDir(validRules, targetDir); err != nil {
		t.Fatalf("first swap failed: %v", err)
	}
	validRules2 := []ruleSourceFile{
		{filename: "01-invariants.md", content: "# Invariants V2\n", prefix: "00_"},
	}
	if err := p.atomicSwapRulesDir(validRules2, targetDir); err != nil {
		t.Fatalf("second swap failed: %v", err)
	}
}

func TestWriteAtomic_Errors(t *testing.T) {
	p := New("", "")
	tmpDir := t.TempDir()
	blocker := filepath.Join(tmpDir, "blocker")
	_ = os.WriteFile(blocker, []byte("file"), 0644)
	err := p.writeAtomic(filepath.Join(blocker, "sub", "file.txt"), "data")
	if err == nil {
		t.Errorf("expected error when directory creation fails")
	}

	targetDir := filepath.Join(tmpDir, "target_dir")
	_ = os.MkdirAll(targetDir, 0755)
	_ = os.WriteFile(filepath.Join(targetDir, "keep.txt"), []byte("keep"), 0644)
	err = p.writeAtomic(targetDir, "data")
	if err == nil {
		t.Errorf("expected error when target is non-empty directory")
	}
}

func TestNew_SkillsDirFallback(t *testing.T) {
	p := New("/custom/home", "/custom/data")
	if p.HomeDir() != "/custom/home" || p.DataDir() != "/custom/data" {
		t.Errorf("unexpected provisioner dirs: %s, %s", p.HomeDir(), p.DataDir())
	}
	pNil := NewFromConfig(nil)
	if pNil.HomeDir() != "" {
		t.Errorf("expected empty home for nil config")
	}
}

func TestCompileTargetRules_VoiceErrorAndDiscoveryBranches(t *testing.T) {
	// 1. resolveDir fallback to configured when nothing exists
	got := resolveDir("/nonexistent/custom", []string{"/nonexistent/c1", "/nonexistent/c2"})
	if got != "/nonexistent/custom" {
		t.Errorf("expected fallback to configured, got: %s", got)
	}

	// 2. discoverRuleFiles with empty dir
	res, err := discoverRuleFiles("", "common", "00_")
	if res != nil || err != nil {
		t.Errorf("expected nil, nil for empty dir, got %v, %v", res, err)
	}

	// 3. discoverRuleFiles with subdirs and non-md files
	tmpDir := t.TempDir()
	commonDir := filepath.Join(tmpDir, "common")
	_ = os.MkdirAll(filepath.Join(commonDir, "nested_dir"), 0755)
	_ = os.WriteFile(filepath.Join(commonDir, "notes.txt"), []byte("not a rule"), 0644)
	_ = os.WriteFile(filepath.Join(commonDir, "01.md"), []byte("valid rule"), 0644)
	files, err := discoverRuleFiles(tmpDir, "common", "00_")
	if err != nil || len(files) != 1 {
		t.Errorf("expected 1 file, got %d files, err: %v", len(files), err)
	}

	// 4. Voice target compilation error (oversized rule in rules/voice)
	p := New(t.TempDir(), t.TempDir())
	p.SetAerialRulesDir(tmpDir)
	_ = os.MkdirAll(filepath.Join(tmpDir, "voice"), 0755)
	_ = os.WriteFile(filepath.Join(tmpDir, "voice", "01.md"), []byte(strings.Repeat("X", MaxSourceRuleFileSizeBytes+100)), 0644)
	if err := p.SyncRules(""); err == nil {
		t.Errorf("expected SyncRules to fail when voice rules has oversized rule")
	}

	// 5. User target rules discovery error (oversized rule in user rules/voice)
	userRulesDir := t.TempDir()
	p.SetConfigRulesDir(userRulesDir)
	_ = os.MkdirAll(filepath.Join(userRulesDir, "voice"), 0755)
	_ = os.WriteFile(filepath.Join(userRulesDir, "voice", "01.md"), []byte(strings.Repeat("Y", MaxSourceRuleFileSizeBytes+100)), 0644)
	_ = os.Remove(filepath.Join(tmpDir, "voice", "01.md"))
	_ = os.WriteFile(filepath.Join(tmpDir, "voice", "01.md"), []byte("valid voice"), 0644)
	if err := p.SyncRules(""); err == nil {
		t.Errorf("expected SyncRules to fail when user config voice rules has oversized rule")
	}
}

func TestSyncSkills_LegacyDirectoriesAndOrphanedSymlinks(t *testing.T) {
	homeDir := t.TempDir()
	p := New(homeDir, t.TempDir())

	// 1. Create legacy ~/.gemini/skills and legacy plugin dir
	legacySkillsDir := filepath.Join(homeDir, ".gemini", "skills")
	_ = os.MkdirAll(legacySkillsDir, 0755)
	legacyPluginDir := filepath.Join(homeDir, ".gemini", "config", "plugins", "superpowers")
	_ = os.MkdirAll(legacyPluginDir, 0755)

	// 2. Create an orphaned symlink in target skills directory
	targetSkillsDir := filepath.Join(homeDir, ".gemini", "config", "skills")
	_ = os.MkdirAll(targetSkillsDir, 0755)
	orphanedLink := filepath.Join(targetSkillsDir, "orphaned-skill")
	_ = os.Symlink(filepath.Join(homeDir, "nonexistent-target"), orphanedLink)

	// SyncSkills removes legacy dirs and sweeps orphaned symlink
	if err := p.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills failed: %v", err)
	}

	if _, err := os.Stat(legacySkillsDir); err == nil {
		t.Errorf("legacy skills dir should have been removed")
	}
	if _, err := os.Stat(legacyPluginDir); err == nil {
		t.Errorf("legacy plugin dir should have been removed")
	}
	if _, err := os.Lstat(orphanedLink); err == nil {
		t.Errorf("orphaned symlink should have been removed")
	}
}

func TestProvisioner_NilReceiverCoverage(t *testing.T) {
	var nilP *Provisioner
	if err := nilP.SyncSkills(); err != nil {
		t.Errorf("expected nil from nilP.SyncSkills()")
	}
	if err := nilP.SyncRules(""); err != nil {
		t.Errorf("expected nil from nilP.SyncRules()")
	}
	if err := nilP.SyncSettings("", ""); err != nil {
		t.Errorf("expected nil from nilP.SyncSettings()")
	}
	if err := nilP.Sync(context.Background(), nil); err != nil {
		t.Errorf("expected nil from nilP.Sync()")
	}
}

func TestAtomicSwapRulesDir_WriteFailureInStaging(t *testing.T) {
	tmpDir := t.TempDir()
	p := New(tmpDir, tmpDir)
	invalidRules := []ruleSourceFile{
		{filename: "nonexistent_sub/rule.md", content: "data", prefix: "00_"},
	}
	err := p.atomicSwapRulesDir(invalidRules, filepath.Join(tmpDir, "target"))
	if err == nil || !strings.Contains(err.Error(), "failed to write rule file") {
		t.Errorf("expected failed to write rule file error, got: %v", err)
	}
}

func TestDiscoverRuleFiles_NotADirectory(t *testing.T) {
	tmpDir := t.TempDir()
	// create a regular file instead of subfolder directory to trigger ReadDir ENOTDIR
	_ = os.WriteFile(filepath.Join(tmpDir, "common"), []byte("file-not-dir"), 0644)
	res, err := discoverRuleFiles(tmpDir, "common", "00_")
	if err == nil || !strings.Contains(err.Error(), "failed to read rules directory") {
		t.Errorf("expected failed to read rules directory error, got: %v, %v", res, err)
	}

	// Test compileTargetRules when user target rules returns an error (user target is a file)
	p := New(t.TempDir(), t.TempDir())
	validAerial := t.TempDir()
	_ = os.MkdirAll(filepath.Join(validAerial, "common"), 0755)
	_ = os.WriteFile(filepath.Join(validAerial, "common", "01.md"), []byte("aerial common"), 0644)
	p.SetAerialRulesDir(validAerial)

	badConfig := t.TempDir()
	_ = os.WriteFile(filepath.Join(badConfig, "discord"), []byte("file-not-dir"), 0644)
	p.SetConfigRulesDir(badConfig)

	_, err = p.compileTargetRules(TargetDiscord, "")
	if err == nil || !strings.Contains(err.Error(), "failed to read rules directory") {
		t.Errorf("expected error when user target rules dir is a file, got: %v", err)
	}

	// Test compileTargetRules when aerial target rules dir is a file
	_ = os.WriteFile(filepath.Join(validAerial, "discord"), []byte("file-not-dir"), 0644)
	p.SetConfigRulesDir(t.TempDir())
	_, err = p.compileTargetRules(TargetDiscord, "")
	if err == nil || !strings.Contains(err.Error(), "failed to read rules directory") {
		t.Errorf("expected error when aerial target rules dir is a file, got: %v", err)
	}
}

func TestLinkSkills_DestinationCollision(t *testing.T) {
	targetDir := t.TempDir()
	srcDir := t.TempDir()
	skillDir := filepath.Join(srcDir, "my-skill")
	_ = os.MkdirAll(skillDir, 0755)
	_ = os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte("# Skill"), 0644)

	// Block destPath with a non-empty directory so os.Remove(destPath) fails
	destPath := filepath.Join(targetDir, "my-skill")
	_ = os.MkdirAll(filepath.Join(destPath, "nested"), 0755)
	_ = os.WriteFile(filepath.Join(destPath, "nested", "file"), []byte("data"), 0644)

	LinkSkills([]string{targetDir}, []string{srcDir})
}

func TestSync_AdditionalErrorBranches(t *testing.T) {
	// 1. SyncSettings failure (blocked path in primaryHome)
	badPrimary := t.TempDir()
	_ = os.WriteFile(filepath.Join(badPrimary, ".gemini"), []byte("not-a-dir"), 0644)
	p := New(badPrimary, t.TempDir())
	err := p.Sync(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "failed to sync settings") {
		t.Errorf("expected failed to sync settings error, got: %v", err)
	}

	// 2. SyncRules failure (file exceeding size ceiling)
	goodPrimary := t.TempDir()
	rulesDir := t.TempDir()
	commonDir := filepath.Join(rulesDir, "common")
	_ = os.MkdirAll(commonDir, 0755)
	hugeContent := strings.Repeat("A", MaxSourceRuleFileSizeBytes+100)
	_ = os.WriteFile(filepath.Join(commonDir, "01.md"), []byte(hugeContent), 0644)

	pRules := New(goodPrimary, t.TempDir())
	pRules.SetAerialRulesDir(rulesDir)
	err = pRules.Sync(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "failed to sync rules") {
		t.Errorf("expected failed to sync rules error, got: %v", err)
	}
}

func TestSweepOrphanedSymlinks_Coverage(t *testing.T) {
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "skills")
	_ = os.MkdirAll(targetDir, 0755)

	// Valid file, not a symlink
	_ = os.WriteFile(filepath.Join(targetDir, "regular.txt"), []byte("file"), 0644)

	// Non-existent directory handling
	sweepOrphanedSymlinks([]string{filepath.Join(tmpDir, "nonexistent"), targetDir})
}

func TestAtomicSwapRulesDir_RollbackAndErrors_Extended(t *testing.T) {
	tmpDir := t.TempDir()
	p := New(tmpDir, tmpDir)
	rules := []ruleSourceFile{{filename: "01.md", content: "data", prefix: "00_"}}
	targetDir := filepath.Join(tmpDir, "swap_target")

	// 1. First swap succeeds, creates targetDir
	if err := p.atomicSwapRulesDir(rules, targetDir); err != nil {
		t.Fatalf("setup swap failed: %v", err)
	}

	origRename := renameRulesDir
	defer func() { renameRulesDir = origRename }()

	// 2. Backup failure: renameRulesDir fails on targetDir -> backupDir
	renameRulesDir = func(src, dst string) error {
		if strings.Contains(dst, ".rules.backup.") {
			return errors.New("simulated backup error")
		}
		return os.Rename(src, dst)
	}
	err := p.atomicSwapRulesDir(rules, targetDir)
	if err == nil || !strings.Contains(err.Error(), "failed to backup existing rules dir") {
		t.Errorf("expected backup failure error, got: %v", err)
	}

	// 3. Staging rename failure with successful restore
	renameRulesDir = func(src, dst string) error {
		if strings.Contains(src, ".rules.tmp.") && dst == targetDir {
			return errors.New("simulated move error")
		}
		return os.Rename(src, dst)
	}
	err = p.atomicSwapRulesDir(rules, targetDir)
	if err == nil || !strings.Contains(err.Error(), "failed to move staging rules to target dir") {
		t.Errorf("expected move failure error, got: %v", err)
	}

	// 4. Staging rename failure with restore failure
	renameRulesDir = func(src, dst string) error {
		if strings.Contains(src, ".rules.tmp.") && dst == targetDir {
			return errors.New("simulated move error")
		}
		if strings.Contains(src, ".rules.backup.") && dst == targetDir {
			return errors.New("simulated restore error")
		}
		return os.Rename(src, dst)
	}
	err = p.atomicSwapRulesDir(rules, targetDir)
	if err == nil || !strings.Contains(err.Error(), "failed to move staging rules to target dir") {
		t.Errorf("expected move failure error with restore error log, got: %v", err)
	}
}

func TestLinkSharedSessionStorage_Comprehensive(t *testing.T) {
	tmpDir := t.TempDir()

	canonicalBrain := filepath.Join(tmpDir, "data", "brain")
	canonicalConv := filepath.Join(tmpDir, "data", "conversations")
	if err := os.MkdirAll(canonicalBrain, 0755); err != nil {
		t.Fatalf("mkdir canonicalBrain failed: %v", err)
	}
	if err := os.MkdirAll(canonicalConv, 0755); err != nil {
		t.Fatalf("mkdir canonicalConv failed: %v", err)
	}

	// 1. Pre-create a file in canonicalBrain
	if err := os.WriteFile(filepath.Join(canonicalBrain, "existing_canon.txt"), []byte("canon"), 0644); err != nil {
		t.Fatalf("write existing_canon failed: %v", err)
	}

	targetGemini := filepath.Join(tmpDir, "runtime", ".gemini")

	// Pre-create an existing directory at targetGemini/antigravity-cli/brain with a file to migrate
	oldTargetDir := filepath.Join(targetGemini, "antigravity-cli", "brain")
	if err := os.MkdirAll(oldTargetDir, 0755); err != nil {
		t.Fatalf("mkdir oldTargetDir failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(oldTargetDir, "migrated.txt"), []byte("migrated_content"), 0644); err != nil {
		t.Fatalf("write migrated.txt failed: %v", err)
	}
	// Also put existing_canon.txt in oldTargetDir to hit the dErr == nil (already exists) branch
	if err := os.WriteFile(filepath.Join(oldTargetDir, "existing_canon.txt"), []byte("old_canon"), 0644); err != nil {
		t.Fatalf("write dup file failed: %v", err)
	}

	// Pre-create a regular file where brain symlink should be
	regFile := filepath.Join(targetGemini, "brain")
	if err := os.WriteFile(regFile, []byte("im a file"), 0644); err != nil {
		t.Fatalf("write regFile failed: %v", err)
	}

	// Pre-create an outdated symlink for conversations
	outdatedSymlink := filepath.Join(targetGemini, "antigravity-cli", "conversations")
	bogusTarget := filepath.Join(tmpDir, "bogus")
	_ = os.MkdirAll(bogusTarget, 0755)
	_ = symlinkRuntimeAsset(bogusTarget, outdatedSymlink)

	p := New(filepath.Join(tmpDir, "home"), filepath.Join(tmpDir, "data"))

	// First run: exercises migration, regular file removal, outdated symlink removal, and symlink creation
	p.linkSharedSessionStorage(targetGemini, canonicalBrain, canonicalConv)

	// Verify migrated file landed in canonicalBrain
	migratedContent, err := os.ReadFile(filepath.Join(canonicalBrain, "migrated.txt"))
	if err != nil || string(migratedContent) != "migrated_content" {
		t.Errorf("expected migrated.txt in canonicalBrain, got %v (content: %s)", err, string(migratedContent))
	}
	// Verify existing_canon was preserved
	canonContent, err := os.ReadFile(filepath.Join(canonicalBrain, "existing_canon.txt"))
	if err != nil || string(canonContent) != "canon" {
		t.Errorf("expected canon in canonicalBrain, got %v", err)
	}

	// Second run: exercises linkTarget == cleanCanonical (idempotent skip)
	p.linkSharedSessionStorage(targetGemini, canonicalBrain, canonicalConv)

	// Third run: pass targetGemini where filepath.Join(targetGemini, "brain") == canonicalBrain to hit cleanTarget == cleanCanonical
	p.linkSharedSessionStorage(filepath.Join(tmpDir, "data"), canonicalBrain, canonicalConv)

	// 4. Test writeAtomic error when target directory is blocked by a file
	blockerFile := filepath.Join(tmpDir, "blocker")
	if err := os.WriteFile(blockerFile, []byte("blocker"), 0644); err != nil {
		t.Fatalf("write blocker failed: %v", err)
	}
	invalidPath := filepath.Join(blockerFile, "subfile.txt")
	if err := p.writeAtomic(invalidPath, "content"); err == nil {
		t.Errorf("expected writeAtomic to fail when dir is a file")
	}

	// 5. Test provisionRuntimeSharedAssets when p.dataDir is empty
	pNoData := New(filepath.Join(tmpDir, "home_nodata"), "")
	pNoData.provisionRuntimeSharedAssets(filepath.Join(tmpDir, "runtime_nodata"))

	// 6. Test SyncRules with configured dataDir to exercise lines 498-507 (canonical brain linking)
	syncHome := t.TempDir()
	syncData := t.TempDir()
	rulesDir := filepath.Join(syncHome, "rules", "common")
	if err := os.MkdirAll(rulesDir, 0755); err != nil {
		t.Fatalf("mkdir rules failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rulesDir, "00_test.md"), []byte("# Test Rule"), 0644); err != nil {
		t.Fatalf("write rule failed: %v", err)
	}
	pSync := New(syncHome, syncData)
	pSync.SetAerialRulesDir(filepath.Join(syncHome, "rules"))
	pSync.SetConfigRulesDir(filepath.Join(syncHome, "config-rules"))
	if err := pSync.SyncRules(""); err != nil {
		t.Errorf("SyncRules with dataDir failed: %v", err)
	}
}





