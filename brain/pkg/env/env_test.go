package env

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/brain/pkg/config"
)

func newTestProvisioner(t *testing.T, aerialFiles, configFiles map[string]string) (*Provisioner, string, string) {
	t.Helper()
	aerialDir := filepath.Join(t.TempDir(), "aerial")
	configDir := filepath.Join(t.TempDir(), "config")
	for relPath, content := range aerialFiles {
		fullPath := filepath.Join(aerialDir, relPath)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatalf("newTestProvisioner aerial mkdir: %v", err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("newTestProvisioner aerial write: %v", err)
		}
	}
	for relPath, content := range configFiles {
		fullPath := filepath.Join(configDir, relPath)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatalf("newTestProvisioner config mkdir: %v", err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("newTestProvisioner config write: %v", err)
		}
	}
	p := New(t.TempDir(), t.TempDir())
	p.SetAerialRulesDir(aerialDir)
	p.SetConfigRulesDir(configDir)
	return p, aerialDir, configDir
}

func writeSkillFixture(t *testing.T, baseDir, relPath, content string) string {
	t.Helper()
	p := filepath.Join(baseDir, relPath)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		t.Fatalf("writeSkillFixture mkdir: %v", err)
	}
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatalf("writeSkillFixture write: %v", err)
	}
	return p
}

func TestProvisioner_InitAndGetters(t *testing.T) {
	tests := []struct {
		name              string
		optSkillsErr      bool
		optSuperpowersOk  bool
		agentsSkillsApp   bool
		nilConfig         bool
		testSetters       bool
		testNilReceiver   bool
		expectedHome      string
		expectedData      string
		expectedSkillsDir string
	}{
		{name: "standard initialization with existing opt skills", expectedHome: "/tmp/home", expectedData: "/tmp/data", expectedSkillsDir: "/opt/skills"},
		{name: "fallback to superpowers skills directory", optSkillsErr: true, optSuperpowersOk: true, expectedHome: "/tmp/home", expectedData: "/tmp/data", expectedSkillsDir: "/opt/superpowers/skills"},
		{name: "fallback agents skills to app directory", agentsSkillsApp: true, expectedHome: "/tmp/home", expectedData: "/tmp/data"},
		{name: "NewFromConfig with nil config", nilConfig: true},
		{name: "NewFromConfig with active config", expectedHome: "/custom/gemini", expectedData: "/custom/data"},
		{name: "all setters and getters", testSetters: true},
		{name: "nil receiver safety", testNilReceiver: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origOpt := statOptSkills
			origAgents := statAgentsSkills
			defer func() {
				statOptSkills = origOpt
				statAgentsSkills = origAgents
			}()

			if tc.optSkillsErr {
				statOptSkills = func(name string) (os.FileInfo, error) {
					if name == "/opt/skills" {
						return nil, os.ErrNotExist
					}
					if name == "/opt/superpowers/skills" && tc.optSuperpowersOk {
						return nil, nil
					}
					return nil, os.ErrNotExist
				}
			}

			if tc.agentsSkillsApp {
				statAgentsSkills = func(name string) (os.FileInfo, error) {
					if strings.Contains(name, "/share/aerial") {
						return nil, os.ErrNotExist
					}
					return nil, nil
				}
			}

			if tc.testNilReceiver {
				var pNil *Provisioner
				if pNil.HomeDir() != "" {
					t.Errorf("expected empty HomeDir on nil receiver")
				}
				if pNil.DataDir() != "" {
					t.Errorf("expected empty DataDir on nil receiver")
				}
				if err := pNil.Sync(context.Background(), nil); err != nil {
					t.Errorf("expected nil error on Sync with nil receiver")
				}
				if err := pNil.SyncRules(""); err != nil {
					t.Errorf("expected nil error on SyncRules with nil receiver")
				}
				if err := pNil.SyncMCP(context.Background(), nil); err != nil {
					t.Errorf("expected nil error on SyncMCP with nil receiver")
				}
				if err := pNil.SyncSkills(); err != nil {
					t.Errorf("expected nil error on SyncSkills with nil receiver")
				}
				if err := pNil.SyncSettings("", ""); err != nil {
					t.Errorf("expected nil error on SyncSettings with nil receiver")
				}
				if pNil.LinkTargetSkills("/tmp", TargetDiscord) != 0 {
					t.Errorf("expected 0 skills linked on nil receiver")
				}
				return
			}

			if tc.testSetters {
				p := New("/h", "/d")
				p.SetCustomSkillsDir("/custom/skills")
				p.SetSuperpowersDir("/super/skills")
				p.SetAgentsSkillsDir("/agents/skills")
				p.SetAerialRulesDir("/aerial/rules")
				p.SetConfigRulesDir("/config/rules")
				p.SetHooksDir("/hooks/dir")

				if p.customSkillsDir != "/custom/skills" || p.superpowersDir != "/super/skills" ||
					p.agentsSkillsDir != "/agents/skills" || p.aerialRulesDir != "/aerial/rules" ||
					p.configRulesDir != "/config/rules" || p.hooksDir != "/hooks/dir" {
					t.Fatalf("setters failed to update Provisioner fields")
				}
				if p.HomeDir() != "/h" || p.DataDir() != "/d" {
					t.Fatalf("unexpected HomeDir or DataDir: %s, %s", p.HomeDir(), p.DataDir())
				}
				return
			}

			if tc.nilConfig {
				p := NewFromConfig(nil)
				if p.HomeDir() != "" || p.DataDir() != "" {
					t.Fatalf("expected empty dirs from nil config, got %q, %q", p.HomeDir(), p.DataDir())
				}
				return
			}

			if tc.expectedHome == "/custom/gemini" {
				cfg := config.NewTestConfig(func(c *config.ConfigData) {
					c.GeminiHomeDir = "/custom/gemini"
					c.DataDir = "/custom/data"
				})
				p := NewFromConfig(cfg)
				if p.HomeDir() != "/custom/gemini" || p.DataDir() != "/custom/data" {
					t.Fatalf("unexpected paths from config: %q, %q", p.HomeDir(), p.DataDir())
				}
				return
			}

			p := New(tc.expectedHome, tc.expectedData)
			if p.HomeDir() != tc.expectedHome || p.DataDir() != tc.expectedData {
				t.Fatalf("unexpected paths: got home=%q, data=%q", p.HomeDir(), p.DataDir())
			}
			if tc.expectedSkillsDir != "" && p.superpowersDir != tc.expectedSkillsDir {
				t.Fatalf("unexpected superpowersDir: got %q, want %q", p.superpowersDir, tc.expectedSkillsDir)
			}
		})
	}
}

func TestSync_EndToEndAndIsolation(t *testing.T) {
	tests := []struct {
		name          string
		emptyHome     bool
		withActiveCfg bool
		failSettings  bool
		failRules     bool
		failMCP       bool
		wantErr       bool
	}{
		{name: "empty home returns nil", emptyHome: true, wantErr: false},
		{name: "successful default sync with default config data", withActiveCfg: false, wantErr: false},
		{name: "successful sync with active config and partitioned runtimes", withActiveCfg: true, wantErr: false},
		{name: "sync failure propagated from settings error", failSettings: true, wantErr: true},
		{name: "sync failure propagated from rules error", failRules: true, wantErr: true},
		{name: "sync failure propagated from mcp error", failMCP: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.emptyHome {
				p := New("", "")
				if err := p.Sync(context.Background(), nil); err != nil {
					t.Fatalf("expected nil error on empty home, got: %v", err)
				}
				return
			}

			p, aerialDir, _ := newTestProvisioner(t, map[string]string{
				"common/01_base.md":  "Base rule content",
				"discord/01_flow.md": "Discord flow content",
				"voice/01_flow.md":   "Voice flow content",
			}, nil)

			if tc.failSettings {
				// Block .gemini/antigravity-cli with a regular file so MkdirAll fails
				blocker := filepath.Join(p.HomeDir(), ".gemini", "antigravity-cli")
				_ = os.MkdirAll(filepath.Dir(blocker), 0755)
				_ = os.WriteFile(blocker, []byte("file-blocking-dir"), 0644)
			}
			if tc.failRules {
				// Block .gemini/rules with a regular file
				blocker := filepath.Join(p.HomeDir(), ".gemini", "rules")
				_ = os.MkdirAll(filepath.Dir(blocker), 0755)
				_ = os.WriteFile(blocker, []byte("file-blocking-dir"), 0644)
			}
			if tc.failMCP {
				// Block .gemini/config/mcp_config.json with a non-empty directory
				mcpBlock := filepath.Join(p.HomeDir(), ".gemini", "config", "mcp_config.json")
				_ = os.MkdirAll(mcpBlock, 0755)
				_ = os.WriteFile(filepath.Join(mcpBlock, "blocker"), []byte("b"), 0644)
			}

			var cfg *config.Config
			if tc.withActiveCfg {
				cfg = config.NewTestConfig(func(c *config.ConfigData) {
					c.APIKey = "test-key"
					c.Model = "gemini-2.5-pro"
					c.SystemPrompt = "Custom instructions"
				})
			}

			err := p.Sync(context.Background(), cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Sync() error = %v, wantErr %v", err, tc.wantErr)
			}

			if !tc.wantErr && tc.withActiveCfg {
				// Verify runtime isolation: discord, voice, ephemeral
				for _, target := range allRuntimeTargets {
					targetGemini := filepath.Join(p.DataDir(), "runtimes", string(target), ".gemini")
					if _, err := os.Stat(filepath.Join(targetGemini, "rules", "00_aerial_common_01_base.md")); err != nil {
						t.Errorf("expected common rule in %s runtime: %v", target, err)
					}
					if _, err := os.Stat(filepath.Join(targetGemini, "config", "mcp_config.json")); err != nil {
						t.Errorf("expected mcp config in %s runtime: %v", target, err)
					}
				}

				// Ephemeral MCP servers must be empty
				ephemeralMCP := filepath.Join(p.DataDir(), "runtimes", "ephemeral", ".gemini", "config", "mcp_config.json")
				data, _ := os.ReadFile(ephemeralMCP)
				if strings.Contains(string(data), "scheduler") {
					t.Errorf("ephemeral runtime should not include scheduler: %s", string(data))
				}
			}
			_ = aerialDir
		})
	}
}

func TestSyncSettings(t *testing.T) {
	tests := []struct {
		name              string
		apiKey            string
		model             string
		preSeedOAuth      bool
		corruptedInput    bool
		helperHome        string
		blockFile         bool
		createTempFail    bool
		atomicEmptyDir    bool
		atomicNonEmptyDir bool
		wantErr           bool
	}{
		{name: "api key and custom model", apiKey: "api-secret-123", model: "gemini-2.5-pro"},
		{name: "oauth mode without api key removes apiKey and provider", apiKey: "", model: "gemini-flash", preSeedOAuth: true},
		{name: "recovers gracefully from corrupted json file", apiKey: "rec-key", model: "rec-model", corruptedInput: true},
		{name: "EnsureAgySettingsForHome with empty home is no-op", helperHome: ""},
		{name: "EnsureAgySettingsForHome provisions settings", helperHome: "valid", apiKey: "helper-key", model: "helper-model"},
		{name: "write failure returns error when parent dir blocked", blockFile: true, wantErr: true},
		{name: "writeAtomic fails on parent directory blocker", createTempFail: true, wantErr: true},
		{name: "writeAtomic replaces empty directory fallback", atomicEmptyDir: true},
		{name: "writeAtomic fails replacing non-empty directory", atomicNonEmptyDir: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.createTempFail {
				tmpDir := t.TempDir()
				p := New(tmpDir, tmpDir)
				blocker := filepath.Join(tmpDir, "blocker")
				_ = os.WriteFile(blocker, []byte("file"), 0644)
				err := p.writeAtomic(filepath.Join(blocker, "sub", "test.txt"), "data")
				if err == nil {
					t.Errorf("expected error when directory creation fails")
				}
				return
			}

			if tc.atomicEmptyDir || tc.atomicNonEmptyDir {
				tmpDir := t.TempDir()
				p := New(tmpDir, tmpDir)
				targetPath := filepath.Join(tmpDir, "replace_dir")
				if err := os.Mkdir(targetPath, 0755); err != nil {
					t.Fatalf("mkdir replace_dir: %v", err)
				}
				if tc.atomicNonEmptyDir {
					_ = os.WriteFile(filepath.Join(targetPath, "child.txt"), []byte("child"), 0644)
				}
				err := p.writeAtomic(targetPath, "new content")
				if (err != nil) != tc.wantErr {
					t.Fatalf("writeAtomic dir replacement error=%v, wantErr=%v", err, tc.wantErr)
				}
				if !tc.wantErr {
					data, _ := os.ReadFile(targetPath)
					if string(data) != "new content" {
						t.Errorf("writeAtomic content mismatch: %q", string(data))
					}
				}
				return
			}

			if tc.helperHome != "" {
				home := t.TempDir()
				err := EnsureAgySettingsForHome(home, tc.apiKey, tc.model)
				if err != nil {
					t.Fatalf("EnsureAgySettingsForHome failed: %v", err)
				}
				settingsPath := filepath.Join(home, ".gemini", "antigravity-cli", "settings.json")
				data, err := os.ReadFile(settingsPath)
				if err != nil || !strings.Contains(string(data), tc.apiKey) {
					t.Fatalf("expected api key in settings: %v, data: %s", err, string(data))
				}
				return
			}
			if tc.name == "EnsureAgySettingsForHome with empty home is no-op" {
				if err := EnsureAgySettingsForHome("   ", "k", "m"); err != nil {
					t.Fatalf("expected nil on whitespace home: %v", err)
				}
				return
			}

			tmpHome := t.TempDir()
			p := New(tmpHome, "")
			settingsPath := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "settings.json")

			if tc.preSeedOAuth {
				_ = os.MkdirAll(filepath.Dir(settingsPath), 0755)
				_ = os.WriteFile(settingsPath, []byte(`{"apiKey":"old","modelProvider":"gemini"}`), 0644)
			}
			if tc.corruptedInput {
				_ = os.MkdirAll(filepath.Dir(settingsPath), 0755)
				_ = os.WriteFile(settingsPath, []byte("{broken json:"), 0644)
			}
			if tc.blockFile {
				// Block parent dir so MkdirAll fails
				_ = os.MkdirAll(filepath.Join(tmpHome, ".gemini"), 0755)
				_ = os.WriteFile(filepath.Join(tmpHome, ".gemini", "antigravity-cli"), []byte("file-blocker"), 0644)
			}

			err := p.SyncSettings(tc.apiKey, tc.model)
			if (err != nil) != tc.wantErr {
				t.Fatalf("SyncSettings() error = %v, wantErr %v", err, tc.wantErr)
			}

			if !tc.wantErr {
				data, err := os.ReadFile(settingsPath)
				if err != nil {
					t.Fatalf("failed reading settings: %v", err)
				}
				var parsed map[string]interface{}
				if err := json.Unmarshal(data, &parsed); err != nil {
					t.Fatalf("settings is not valid json: %v", err)
				}

				if tc.apiKey != "" {
					if parsed["apiKey"] != tc.apiKey || parsed["modelProvider"] != "gemini" {
						t.Errorf("expected apiKey and gemini provider: %v", parsed)
					}
				} else {
					if _, hasKey := parsed["apiKey"]; hasKey {
						t.Errorf("expected apiKey deleted in OAuth mode")
					}
					if _, hasProvider := parsed["modelProvider"]; hasProvider {
						t.Errorf("expected modelProvider deleted in OAuth mode")
					}
				}
				if tc.model != "" && parsed["model"] != tc.model {
					t.Errorf("expected model %q, got %v", tc.model, parsed["model"])
				}
			}
		})
	}
}

func TestCompileAndDiscoverRules(t *testing.T) {
	tests := []struct {
		name                string
		target              RuleTarget
		prompt              string
		fileSizeLimit       bool
		tornZeroByte        bool
		dirIsFile           bool
		userSubIsFile       bool
		aerialPersonaIsFile bool
		configPersonaIsFile bool
		statErrorSubfolder  bool
		emptyDirTest        bool
		testResolve         bool
		wantErr             bool
		wantPromptMD        bool
		wantPersona         bool
	}{
		{name: "discord target with custom prompt", target: TargetDiscord, prompt: "Do not compromise invariants", wantPromptMD: true, wantPersona: true},
		{name: "voice target includes persona and target rules", target: TargetVoice, wantPersona: true},
		{name: "ephemeral target compiles isolated rules and filters out persona", target: TargetEphemeral, wantPersona: false},
		{name: "rule file exceeding size limit triggers error", target: TargetDiscord, fileSizeLimit: true, wantErr: true},
		{name: "torn zero byte rule file triggers discovery error", target: TargetDiscord, tornZeroByte: true, wantErr: true},
		{name: "aerial common subfolder is regular file triggers discovery error", target: TargetDiscord, dirIsFile: true, wantErr: true},
		{name: "user target subfolder is regular file triggers discovery error", target: TargetDiscord, userSubIsFile: true, wantErr: true},
		{name: "aerial persona subfolder is regular file triggers error", target: TargetDiscord, aerialPersonaIsFile: true, wantErr: true},
		{name: "config persona subfolder is regular file triggers error", target: TargetDiscord, configPersonaIsFile: true, wantErr: true},
		{name: "statErr ENOTDIR on subfolder triggers discovery error", statErrorSubfolder: true, wantErr: true},
		{name: "discoverRuleFiles empty dir and non-md skipping", emptyDirTest: true},
		{name: "resolveDir candidate fallback and defaults", testResolve: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.statErrorSubfolder {
				tmpDir := t.TempDir()
				blocker := filepath.Join(tmpDir, "file_blocker")
				_ = os.WriteFile(blocker, []byte("f"), 0644)
				_, err := discoverRuleFiles(blocker, "subfolder", "00_")
				if err == nil {
					t.Fatalf("expected error from ENOTDIR stat on subfolder")
				}
				return
			}

			if tc.emptyDirTest {
				// 1. discoverRuleFiles with empty dir
				res, err := discoverRuleFiles("", "common", "00_")
				if res != nil || err != nil {
					t.Errorf("expected nil, nil for empty dir, got %v, %v", res, err)
				}

				// 2. discoverRuleFiles with subdirs and non-md files
				tmpDir := t.TempDir()
				commonDir := filepath.Join(tmpDir, "common")
				_ = os.MkdirAll(filepath.Join(commonDir, "nested_dir"), 0755)
				_ = os.WriteFile(filepath.Join(commonDir, "notes.txt"), []byte("not a rule"), 0644)
				_ = os.WriteFile(filepath.Join(commonDir, "01.md"), []byte("valid rule"), 0644)
				files, err := discoverRuleFiles(tmpDir, "common", "00_")
				if err != nil || len(files) != 1 {
					t.Errorf("expected 1 file discovered, got %d, err: %v", len(files), err)
				}
				return
			}

			if tc.testResolve {
				tmpDir := t.TempDir()
				candDir := filepath.Join(tmpDir, "cand")
				_ = os.Mkdir(candDir, 0755)

				// 1. Configured exists
				if r := resolveDir(candDir, nil); r != candDir {
					t.Errorf("resolveDir configured mismatch: %q", r)
				}
				// 2. Fallback to candidate
				if r := resolveDir(filepath.Join(tmpDir, "missing"), []string{candDir}); r != candDir {
					t.Errorf("resolveDir candidate fallback mismatch: %q", r)
				}
				// 3. Neither exists returns configured
				missing := filepath.Join(tmpDir, "nonexistent")
				if r := resolveDir(missing, []string{filepath.Join(tmpDir, "nonexistent2")}); r != missing {
					t.Errorf("resolveDir default mismatch: %q", r)
				}
				return
			}

			content := "Standard rule body"
			if tc.fileSizeLimit {
				content = strings.Repeat("A", MaxSourceRuleFileSizeBytes+100)
			} else if tc.tornZeroByte {
				content = ""
			}

			p, aerialDir, configDir := newTestProvisioner(t, map[string]string{
				"common/01_identity.md": "Aerial core identity",
				"discord/01_chat.md":    "Discord chat handling",
				"voice/01_audio.md":     "Voice audio handling",
				"ephemeral/01_temp.md":  "Ephemeral worker rules",
			}, map[string]string{
				"common/10_persona_style.md": "Config persona override",
				"common/20_facts.md":         content,
				"discord/30_user_chat.md":    "Config discord settings",
			})

			if tc.dirIsFile {
				// Make aerialDir/common a regular file so ReadDir fails with ENOTDIR
				_ = os.RemoveAll(filepath.Join(aerialDir, "common"))
				_ = os.WriteFile(filepath.Join(aerialDir, "common"), []byte("file-not-dir"), 0644)
			}

			if tc.userSubIsFile {
				// Make configDir/discord a regular file
				_ = os.RemoveAll(filepath.Join(configDir, "discord"))
				_ = os.WriteFile(filepath.Join(configDir, "discord"), []byte("file-not-dir"), 0644)
			}

			if tc.aerialPersonaIsFile {
				_ = os.RemoveAll(filepath.Join(aerialDir, "persona"))
				_ = os.WriteFile(filepath.Join(aerialDir, "persona"), []byte("file-not-dir"), 0644)
			}

			if tc.configPersonaIsFile {
				_ = os.RemoveAll(filepath.Join(configDir, "persona"))
				_ = os.WriteFile(filepath.Join(configDir, "persona"), []byte("file-not-dir"), 0644)
			}

			rules, err := p.compileTargetRules(tc.target, tc.prompt)
			if (err != nil) != tc.wantErr {
				t.Fatalf("compileTargetRules() error = %v, wantErr %v", err, tc.wantErr)
			}

			if !tc.wantErr {
				hasPrompt := false
				hasPersona := false
				for _, r := range rules {
					if r.filename == "99_custom_prompt.md" {
						hasPrompt = true
					}
					if strings.Contains(r.filename, "persona") {
						hasPersona = true
					}
				}
				if hasPrompt != tc.wantPromptMD {
					t.Errorf("prompt rule existence=%v, want=%v", hasPrompt, tc.wantPromptMD)
				}
				if hasPersona != tc.wantPersona {
					t.Errorf("target %s persona existence=%v, want=%v", tc.target, hasPersona, tc.wantPersona)
				}
			}
		})
	}
}

func TestAtomicSwapRulesDir(t *testing.T) {
	tests := []struct {
		name              string
		emptyStaging      bool
		targetIsFile      bool
		parentIsFile      bool
		stagingWriteFail  bool
		backupFail        bool
		stagingMoveFail   bool
		restoreFail       bool
		wantErr           bool
	}{
		{name: "successful directory swap"},
		{name: "abort when staging has zero rules", emptyStaging: true, wantErr: true},
		{name: "error when target path is a file", targetIsFile: true, wantErr: true},
		{name: "error when parent dir creation fails", parentIsFile: true, wantErr: true},
		{name: "staging write failure on invalid relative path", stagingWriteFail: true, wantErr: true},
		{name: "backup failure when target backup rename fails", backupFail: true, wantErr: true},
		{name: "rollback restores backup when staging move fails", stagingMoveFail: true, wantErr: true},
		{name: "staging move failure and restore failure", stagingMoveFail: true, restoreFail: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origRename := renameRulesDir
			defer func() { renameRulesDir = origRename }()

			tmpParent := t.TempDir()
			targetDir := filepath.Join(tmpParent, "rules")
			_ = os.MkdirAll(targetDir, 0755)
			origFile := filepath.Join(targetDir, "original.md")
			_ = os.WriteFile(origFile, []byte("active rule"), 0644)

			if tc.parentIsFile {
				blocker := filepath.Join(tmpParent, "blocked_parent")
				_ = os.WriteFile(blocker, []byte("file"), 0644)
				targetDir = filepath.Join(blocker, "rules")
			}

			if tc.targetIsFile {
				_ = os.RemoveAll(targetDir)
				_ = os.WriteFile(targetDir, []byte("not a dir"), 0644)
			}

			var rules []ruleSourceFile
			if tc.stagingWriteFail {
				rules = []ruleSourceFile{
					{filename: "nonexistent_sub/01.md", content: "data", prefix: "00_"},
				}
			} else if !tc.emptyStaging {
				rules = []ruleSourceFile{
					{filename: "new.md", prefix: "00_", content: "new rule"},
				}
			}

			if tc.backupFail {
				renameRulesDir = func(src, dst string) error {
					if strings.Contains(dst, ".rules.backup.") {
						return errors.New("simulated backup error")
					}
					return origRename(src, dst)
				}
			}

			if tc.stagingMoveFail {
				callCount := 0
				renameRulesDir = func(oldpath, newpath string) error {
					callCount++
					if callCount == 1 {
						return origRename(oldpath, newpath) // backup succeeds
					}
					if callCount == 2 {
						return errors.New("simulated staging move failure")
					}
					if tc.restoreFail {
						return errors.New("simulated restore failure")
					}
					return origRename(oldpath, newpath) // restore succeeds
				}
			}

			p := New("", "")
			err := p.atomicSwapRulesDir(rules, targetDir)
			if (err != nil) != tc.wantErr {
				t.Fatalf("atomicSwapRulesDir() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.stagingMoveFail && !tc.restoreFail {
				// Verify original was restored
				data, rErr := os.ReadFile(origFile)
				if rErr != nil || string(data) != "active rule" {
					t.Fatalf("backup was not restored after failed swap: %v, data=%q", rErr, string(data))
				}
			} else if !tc.wantErr {
				newFile := filepath.Join(targetDir, "00_new.md")
				data, rErr := os.ReadFile(newFile)
				if rErr != nil || string(data) != "new rule" {
					t.Fatalf("target rules dir does not contain new file: %v", rErr)
				}
			}
		})
	}
}

func TestSyncRules(t *testing.T) {
	tests := []struct {
		name          string
		emptyHome     bool
		zeroRules     bool
		primaryFail   bool
		configFail    bool
		voiceFail     bool
		ephemeralFail bool
		runtimeFail   bool
		concurrent    bool
		wantErr       bool
	}{
		{name: "empty home returns nil", emptyHome: true, wantErr: false},
		{name: "successful sync compiles and provisions all runtimes", wantErr: false},
		{name: "zero rules aborts swap under lobotomy protection", zeroRules: true, wantErr: true},
		{name: "voice rules compilation failure returns error", voiceFail: true, wantErr: true},
		{name: "ephemeral rules compilation failure returns error", ephemeralFail: true, wantErr: true},
		{name: "primary rules write failure returns error", primaryFail: true, wantErr: true},
		{name: "config rules write failure logs warning but succeeds", configFail: true, wantErr: false},
		{name: "runtime rules write failure logs warning and continues", runtimeFail: true, wantErr: false},
		{name: "concurrent sync execution safe under systemRulesMu", concurrent: true, wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.emptyHome {
				p := New("", "")
				if err := p.SyncRules(""); err != nil {
					t.Fatalf("expected nil error on empty home: %v", err)
				}
				return
			}

			p, aerialDir, _ := newTestProvisioner(t, map[string]string{
				"common/01_rule.md":  "Common content",
				"discord/01_rule.md": "Discord content",
				"voice/01_rule.md":   "Voice content",
			}, nil)

			if tc.zeroRules {
				p.SetAerialRulesDir(t.TempDir())
				p.SetConfigRulesDir(t.TempDir())
			}

			if tc.voiceFail {
				// Voice compilation fails if voice subfolder has a torn 0-byte file
				_ = os.WriteFile(filepath.Join(aerialDir, "voice", "torn.md"), []byte(""), 0644)
			}

			if tc.ephemeralFail {
				// Ephemeral compilation fails if ephemeral subfolder has a torn 0-byte file
				_ = os.MkdirAll(filepath.Join(aerialDir, "ephemeral"), 0755)
				_ = os.WriteFile(filepath.Join(aerialDir, "ephemeral", "torn.md"), []byte(""), 0644)
			}

			if tc.primaryFail {
				blocker := filepath.Join(p.HomeDir(), ".gemini", "rules")
				_ = os.MkdirAll(filepath.Dir(blocker), 0755)
				_ = os.WriteFile(blocker, []byte("file blocker"), 0644)
			}

			if tc.configFail {
				blocker := filepath.Join(p.HomeDir(), ".gemini", "config", "rules")
				_ = os.MkdirAll(filepath.Dir(blocker), 0755)
				_ = os.WriteFile(blocker, []byte("file blocker"), 0644)
			}

			if tc.runtimeFail {
				rtBlocker := filepath.Join(p.DataDir(), "runtimes", "discord", ".gemini", "rules")
				_ = os.MkdirAll(filepath.Dir(rtBlocker), 0755)
				_ = os.WriteFile(rtBlocker, []byte("file blocker"), 0644)
			}

			if tc.concurrent {
				var wg sync.WaitGroup
				errCh := make(chan error, 5)
				for i := 0; i < 5; i++ {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						errCh <- p.SyncRules(fmt.Sprintf("Prompt %d", idx))
					}(i)
				}
				wg.Wait()
				close(errCh)
				for err := range errCh {
					if err != nil {
						t.Fatalf("concurrent SyncRules failed: %v", err)
					}
				}
				return
			}

			err := p.SyncRules("Custom System Prompt")
			if (err != nil) != tc.wantErr {
				t.Fatalf("SyncRules() error = %v, wantErr %v", err, tc.wantErr)
			}

			if !tc.wantErr {
				rulesDir := filepath.Join(p.HomeDir(), ".gemini", "rules")
				entries, rErr := os.ReadDir(rulesDir)
				if rErr != nil || len(entries) == 0 {
					t.Fatalf("expected rules in %s: %v", rulesDir, rErr)
				}
			}
		})
	}
}

func TestProvisionRuntimeSharedAssetsAndStorage(t *testing.T) {
	tests := []struct {
		name              string
		fallbackCopy      bool
		linkAssetFail     bool
		existingSymlink   bool
		existingDirFiles  bool
		destinationExists bool
		existingPlainFile bool
		emptyDataDir      bool
	}{
		{name: "standard symlinks and shared storage"},
		{name: "fallback copy when symlink fails", fallbackCopy: true},
		{name: "linkOrCopyAsset destination remove and write failures", linkAssetFail: true},
		{name: "migrate existing directory contents to canonical storage", existingDirFiles: true},
		{name: "migrate skips file when destination already exists", existingDirFiles: true, destinationExists: true},
		{name: "replace outdated symlinks and regular files", existingSymlink: true, existingPlainFile: true},
		{name: "empty dataDir falls back to primary gemini", emptyDataDir: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origSymlink := symlinkRuntimeAsset
			defer func() { symlinkRuntimeAsset = origSymlink }()

			if tc.fallbackCopy {
				symlinkRuntimeAsset = func(oldname, newname string) error {
					return errors.New("simulated symlink failure")
				}
			}

			tmpHome := t.TempDir()
			var tmpData string
			if !tc.emptyDataDir {
				tmpData = t.TempDir()
			}
			p := New(tmpHome, tmpData)

			primaryGemini := filepath.Join(tmpHome, ".gemini")
			primaryCli := filepath.Join(primaryGemini, "antigravity-cli")
			_ = os.MkdirAll(primaryCli, 0755)
			_ = os.WriteFile(filepath.Join(primaryCli, "settings.json"), []byte(`{"apiKey":"test"}`), 0644)
			_ = os.WriteFile(filepath.Join(primaryCli, "antigravity-oauth-token"), []byte("tok-123"), 0644)
			_ = os.WriteFile(filepath.Join(primaryCli, "installation_id"), []byte("inst-456"), 0644)
			_ = os.WriteFile(filepath.Join(primaryGemini, "mcp_oauth_tokens.json"), []byte(`{"oauth":"data"}`), 0644)

			runtimeHome := filepath.Join(t.TempDir(), "runtime")
			runtimeGemini := filepath.Join(runtimeHome, ".gemini")

			if tc.linkAssetFail {
				srcFile := filepath.Join(tmpHome, "src.txt")
				_ = os.WriteFile(srcFile, []byte("src-data"), 0644)
				// dst as non-empty directory triggers remove failure
				dstDir := filepath.Join(tmpHome, "dst_dir")
				_ = os.Mkdir(dstDir, 0755)
				_ = os.WriteFile(filepath.Join(dstDir, "c"), []byte("c"), 0644)
				p.linkOrCopyAsset(srcFile, dstDir)

				// blocked dst parent triggers writeAtomic failure on copy fallback
				symlinkRuntimeAsset = func(oldname, newname string) error {
					return errors.New("symlink fail")
				}
				blockedDst := filepath.Join(tmpHome, "file_blocker", "target.txt")
				_ = os.WriteFile(filepath.Join(tmpHome, "file_blocker"), []byte("f"), 0644)
				p.linkOrCopyAsset(srcFile, blockedDst)
				return
			}

			if tc.destinationExists && !tc.emptyDataDir {
				canonBrain := filepath.Join(tmpData, "brain")
				_ = os.MkdirAll(canonBrain, 0755)
				_ = os.WriteFile(filepath.Join(canonBrain, "migrated_turn.jsonl"), []byte("canon copy"), 0644)
			}

			if tc.existingDirFiles {
				targetBrain := filepath.Join(runtimeGemini, "brain")
				_ = os.MkdirAll(targetBrain, 0755)
				_ = os.WriteFile(filepath.Join(targetBrain, "migrated_turn.jsonl"), []byte("transcript content"), 0644)
			}
			if tc.existingPlainFile {
				targetConv := filepath.Join(runtimeGemini, "conversations")
				_ = os.MkdirAll(filepath.Dir(targetConv), 0755)
				_ = os.WriteFile(targetConv, []byte("plain file blocker"), 0644)
			}
			if tc.existingSymlink {
				targetCliBrain := filepath.Join(runtimeGemini, "antigravity-cli", "brain")
				_ = os.MkdirAll(filepath.Dir(targetCliBrain), 0755)
				_ = os.Symlink(t.TempDir(), targetCliBrain)
			}

			p.provisionRuntimeSharedAssets(runtimeHome)

			// Idempotent second call exercises already-valid symlinks
			p.provisionRuntimeSharedAssets(runtimeHome)

			// Exercise cleanTarget == cleanCanonical
			if !tc.emptyDataDir {
				p.linkSharedSessionStorage(tmpData, filepath.Join(tmpData, "brain"), filepath.Join(tmpData, "conversations"))
			}

			// Verify settings copied
			settingsDst := filepath.Join(runtimeGemini, "antigravity-cli", "settings.json")
			if data, err := os.ReadFile(settingsDst); err != nil || !strings.Contains(string(data), "test") {
				t.Fatalf("settings.json missing or incorrect in runtime: %v", err)
			}

			// Verify shared asset presence
			tokenDst := filepath.Join(runtimeGemini, "antigravity-cli", "antigravity-oauth-token")
			if _, err := os.Lstat(tokenDst); err != nil {
				t.Fatalf("shared token asset missing: %v", err)
			}

			// Verify migration of existing directory contents to canonical storage
			if tc.existingDirFiles && !tc.emptyDataDir {
				migratedFile := filepath.Join(tmpData, "brain", "migrated_turn.jsonl")
				if data, err := os.ReadFile(migratedFile); err != nil {
					t.Fatalf("failed migrating directory contents to canonical brain: %v", err)
				} else if tc.destinationExists && string(data) != "canon copy" {
					t.Fatalf("expected canon copy preserved: %s", string(data))
				}
			}
		})
	}
}

func TestMCPProvisioning(t *testing.T) {
	tests := []struct {
		name            string
		target          RuleTarget
		withPAT         bool
		withOpenObs     bool
		defaultOOUrl    bool
		withFileConf    bool
		withCustomSrv   bool
		withRawNonObj   bool
		withMCPCfgStr   bool
		emptyRaw        bool
		strRaw          bool
		blockDir        bool
		syncFail        bool
		syncRuntimeFail bool
		wantScheduler   bool
		wantDiscord     bool
		wantGitHub      bool
		wantOpenObs     bool
	}{
		{name: "discord target with github and openobserve", target: TargetDiscord, withPAT: true, withOpenObs: true, wantScheduler: true, wantDiscord: true, wantGitHub: true, wantOpenObs: true},
		{name: "voice target isolates scheduler and excludes discord services", target: TargetVoice, withPAT: true, withOpenObs: true, wantScheduler: true},
		{name: "ephemeral target excludes scheduler and discord services", target: TargetEphemeral},
		{name: "discord target overlays file-based mcp configuration", target: TargetDiscord, withFileConf: true, wantScheduler: true, wantDiscord: true},
		{name: "overlay custom mcp servers across targets", target: TargetVoice, withCustomSrv: true, wantScheduler: true},
		{name: "overlay custom mcp server with raw non-object value", target: TargetDiscord, withRawNonObj: true, wantScheduler: true, wantDiscord: true},
		{name: "openobserve with default url and org", target: TargetDiscord, withOpenObs: true, defaultOOUrl: true, wantScheduler: true, wantDiscord: true, wantOpenObs: true},
		{name: "cfg.Current().MCPConfig string fallback", target: TargetDiscord, withMCPCfgStr: true, wantScheduler: true, wantDiscord: true},
		{name: "EnsureTargetMcpConfig handles raw JSON string and invalid paths", strRaw: true, blockDir: true},
		{name: "SyncMCP error bubbles up when primary config write fails", syncFail: true},
		{name: "SyncMCP error bubbles up when runtime config write fails", syncRuntimeFail: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpHome := t.TempDir()
			tmpData := t.TempDir()
			p := New(tmpHome, tmpData)

			cfg := config.NewTestConfig(func(c *config.ConfigData) {
				if tc.withPAT {
					c.GitHubPAT = "ghp_12345678901234567890"
				}
				if tc.withOpenObs {
					c.OpenObserveUser = "admin"
					c.OpenObservePassword = "password"
					if tc.defaultOOUrl {
						c.OpenObserveURL = ""
						c.OpenObserveOrg = ""
					} else {
						c.OpenObserveOrg = "custom_org"
						c.OpenObserveURL = "http://openobserve:5080/openobserve/" // test slash trim
					}
				}
				if tc.withCustomSrv {
					c.McpServers.Common = map[string]json.RawMessage{
						"custom_common": json.RawMessage(`{"serverUrl":"http://common:8080"}`),
					}
					c.McpServers.Voice = map[string]json.RawMessage{
						"custom_voice": json.RawMessage(`{"serverUrl":"http://voice:8080"}`),
					}
					c.McpServers.Ephemeral = map[string]json.RawMessage{
						"custom_eph": json.RawMessage(`{"serverUrl":"http://eph:8080"}`),
					}
				}
				if tc.withRawNonObj {
					c.McpServers.Discord = map[string]json.RawMessage{
						"raw_srv": json.RawMessage(`"string_only_value"`),
					}
				}
				if tc.withMCPCfgStr {
					c.MCPConfig = `{"mcpServers":{"fallback_srv":{"serverUrl":"http://fallback:80"}}}`
				}
			})

			if tc.withFileConf {
				overridePath := filepath.Join(tmpData, "mcp.config.json")
				_ = os.WriteFile(overridePath, []byte(`{"mcpServers":{"custom_override":{"serverUrl":"http://override:80"}}}`), 0644)
			}

			if tc.syncFail {
				blocker := filepath.Join(tmpHome, ".gemini", "config")
				_ = os.MkdirAll(filepath.Dir(blocker), 0755)
				_ = os.WriteFile(blocker, []byte("blocker"), 0644)
				if err := p.SyncMCP(context.Background(), cfg); err == nil {
					t.Fatalf("expected error on blocked config directory in SyncMCP")
				}
				return
			}

			if tc.syncRuntimeFail {
				rtBlocker := filepath.Join(tmpData, "runtimes", "discord", ".gemini", "config")
				_ = os.MkdirAll(filepath.Dir(rtBlocker), 0755)
				_ = os.WriteFile(rtBlocker, []byte("blocker"), 0644)
				if err := p.SyncMCP(context.Background(), cfg); err == nil {
					t.Fatalf("expected error on blocked runtime config directory in SyncMCP")
				}
				return
			}

			if tc.strRaw || tc.blockDir {
				var pNil *Provisioner
				if err := pNil.EnsureMcpConfig(json.RawMessage(`{}`)); err != nil {
					t.Errorf("expected nil on nil receiver")
				}
				if err := p.EnsureTargetMcpConfig("", json.RawMessage(`{}`)); err != nil {
					t.Errorf("expected nil on empty target dir")
				}
				if err := p.EnsureTargetMcpConfig(tmpHome, json.RawMessage(``)); err != nil {
					t.Errorf("expected nil on empty rawConfig")
				}
				if err := p.EnsureTargetMcpConfig(tmpHome, json.RawMessage(`""`)); err != nil {
					t.Errorf("expected nil on empty quote rawConfig")
				}

				// Plain string rawConfig unmarshal
				strJSON := json.RawMessage(`"{\"mcpServers\":{\"str_srv\":{}}}"`)
				if err := p.EnsureTargetMcpConfig(tmpHome, strJSON); err != nil {
					t.Fatalf("EnsureTargetMcpConfig failed on json string: %v", err)
				}
				return
			}

			raw := p.LoadTargetMCPConfig(cfg, tc.target)
			var parsed map[string]map[string]interface{}
			if err := json.Unmarshal(raw, &parsed); err != nil {
				t.Fatalf("failed parsing MCP config: %v", err)
			}
			servers := parsed["mcpServers"]

			if (servers["scheduler"] != nil) != tc.wantScheduler {
				t.Errorf("scheduler presence = %v, want %v", servers["scheduler"] != nil, tc.wantScheduler)
			}
			if (servers["discord"] != nil) != tc.wantDiscord {
				t.Errorf("discord presence = %v, want %v", servers["discord"] != nil, tc.wantDiscord)
			}
			if (servers["github"] != nil) != tc.wantGitHub {
				t.Errorf("github presence = %v, want %v", servers["github"] != nil, tc.wantGitHub)
			}
			if (servers["openobserve"] != nil) != tc.wantOpenObs {
				t.Errorf("openobserve presence = %v, want %v", servers["openobserve"] != nil, tc.wantOpenObs)
			}
			if tc.withOpenObs && tc.wantOpenObs {
				oo := servers["openobserve"].(map[string]interface{})
				headers := oo["headers"].(map[string]interface{})
				auth := headers["Authorization"].(string)
				expectedVal := "Basic " + base64.StdEncoding.EncodeToString([]byte("admin:password"))
				if auth != expectedVal {
					t.Errorf("unexpected openobserve auth header: got %q, want %q", auth, expectedVal)
				}
			}
			if tc.withFileConf && servers["custom_override"] == nil {
				t.Errorf("expected custom_override from file-based config")
			}
			if tc.withCustomSrv {
				if servers["custom_common"] == nil || servers["custom_voice"] == nil {
					t.Errorf("expected custom servers from McpServers struct: %+v", servers)
				}
			}
			if tc.withMCPCfgStr && servers["fallback_srv"] == nil {
				t.Errorf("expected fallback_srv from MCPConfig string")
			}

			// Verify LoadMCPConfig helper defaults to Discord target
			defRaw := p.LoadMCPConfig(cfg)
			if string(defRaw) != string(p.LoadTargetMCPConfig(cfg, TargetDiscord)) {
				t.Fatalf("LoadMCPConfig did not match TargetDiscord config")
			}

			// Verify EnsureMcpConfig and SyncMCP
			if err := p.EnsureMcpConfig(raw); err != nil {
				t.Fatalf("EnsureMcpConfig failed: %v", err)
			}
			if err := p.SyncMCP(context.Background(), cfg); err != nil {
				t.Fatalf("SyncMCP failed: %v", err)
			}
		})
	}
}

func TestSkillsProvisioning(t *testing.T) {
	tests := []struct {
		name               string
		target             RuleTarget
		superpowers        bool
		tornRead           bool
		flatSkillWarning   bool
		pruneUnauthorized  bool
		legacySymlinkDir   bool
		legacyCleanup      bool
		legacyCleanupErr   bool
		targetBlocked      bool
		symlinkTmpFallback bool
		symlinkTotalFail   bool
		renameFailFallback bool
		orphanSymlinkSweep bool
	}{
		{name: "discord target receives common and target skills and superpowers", target: TargetDiscord, superpowers: true},
		{name: "voice target excludes superpowers and discord skills", target: TargetVoice, superpowers: true},
		{name: "ephemeral target links exclusively ephemeral skills", target: TargetEphemeral, superpowers: true},
		{name: "torn read skips zero-byte SKILL.md", target: TargetDiscord, tornRead: true},
		{name: "flat root skill emits warning and is ignored", target: TargetDiscord, flatSkillWarning: true},
		{name: "active reconciliation prunes unauthorized skills", target: TargetDiscord, pruneUnauthorized: true},
		{name: "target skills directory is existing legacy symlink", target: TargetDiscord, legacySymlinkDir: true},
		{name: "cleans up legacy skills and plugins directories", target: TargetDiscord, legacyCleanup: true},
		{name: "logs warning and handles legacy remove errors", target: TargetDiscord, legacyCleanupErr: true},
		{name: "target skills directory creation blocked by file", target: TargetDiscord, targetBlocked: true},
		{name: "symlink tmp fails and triggers direct symlink fallback", target: TargetDiscord, symlinkTmpFallback: true},
		{name: "both tmp and direct symlink fail gracefully", target: TargetDiscord, symlinkTotalFail: true},
		{name: "rename fails and triggers destination removal fallback", target: TargetDiscord, renameFailFallback: true},
		{name: "sweepOrphanedSymlinks removes broken links and retains valid links", orphanSymlinkSweep: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origRemoveAll := removeAllSkills
			origSymlink := symlinkSkill
			origRename := renameSkill
			origRemove := removeSkill
			defer func() {
				removeAllSkills = origRemoveAll
				symlinkSkill = origSymlink
				renameSkill = origRename
				removeSkill = origRemove
			}()

			if tc.orphanSymlinkSweep {
				tmpTarget := t.TempDir()
				validSrc := t.TempDir()
				_ = os.WriteFile(filepath.Join(validSrc, "file.txt"), []byte("ok"), 0644)
				validLink := filepath.Join(tmpTarget, "valid")
				_ = os.Symlink(validSrc, validLink)

				brokenLink := filepath.Join(tmpTarget, "broken")
				_ = os.Symlink(filepath.Join(t.TempDir(), "nonexistent"), brokenLink)

				plainFile := filepath.Join(tmpTarget, "regular.txt")
				_ = os.WriteFile(plainFile, []byte("keep"), 0644)

				// Test sweep with nonexistent directory and remove error
				sweepOrphanedSymlinks([]string{filepath.Join(tmpTarget, "missing_dir"), tmpTarget})

				removeSkill = func(path string) error {
					return errors.New("simulated sweep remove error")
				}
				sweepOrphanedSymlinks([]string{tmpTarget})
				removeSkill = origRemove

				sweepOrphanedSymlinks([]string{tmpTarget})

				if _, err := os.Lstat(brokenLink); !os.IsNotExist(err) {
					t.Fatalf("broken symlink was not removed: %v", err)
				}
				if _, err := os.Lstat(validLink); err != nil {
					t.Fatalf("valid symlink was improperly removed: %v", err)
				}
				if _, err := os.Stat(plainFile); err != nil {
					t.Fatalf("plain file was improperly removed: %v", err)
				}
				return
			}

			tmpHome := t.TempDir()
			tmpData := t.TempDir()
			p := New(tmpHome, tmpData)

			customDir := t.TempDir()
			// Put a regular file at customDir root to exercise non-dir scanning
			_ = os.WriteFile(filepath.Join(customDir, "README.txt"), []byte("readme"), 0644)

			writeSkillFixture(t, customDir, "common/custom-skill/SKILL.md", "---\nname: custom-skill\n---\nCustom skill body")
			writeSkillFixture(t, customDir, "discord/discord-skill/SKILL.md", "---\nname: discord-skill\n---\nDiscord only body")
			writeSkillFixture(t, customDir, "voice/voice-skill/SKILL.md", "---\nname: voice-skill\n---\nVoice only body")
			writeSkillFixture(t, customDir, "ephemeral/eph-skill/SKILL.md", "---\nname: eph-skill\n---\nEphemeral only body")
			p.SetCustomSkillsDir(customDir)

			if tc.flatSkillWarning {
				writeSkillFixture(t, customDir, "flat-root-skill/SKILL.md", "---\nname: flat\n---\nFlat body")
			}

			if tc.superpowers {
				spDir := t.TempDir()
				writeSkillFixture(t, spDir, "brainstorming/SKILL.md", "---\nname: brainstorming\n---\nSuperpower body")
				p.SetSuperpowersDir(spDir)
			}

			if tc.tornRead {
				writeSkillFixture(t, customDir, "common/torn-skill/SKILL.md", "") // 0 bytes
			}

			targetSkillsDir := filepath.Join(tmpHome, ".gemini", "config", "skills")

			if tc.targetBlocked {
				_ = os.MkdirAll(filepath.Dir(targetSkillsDir), 0755)
				_ = os.WriteFile(targetSkillsDir, []byte("file blocker"), 0644)
			}

			if tc.legacySymlinkDir {
				_ = os.MkdirAll(filepath.Dir(targetSkillsDir), 0755)
				_ = os.Symlink(t.TempDir(), targetSkillsDir)
			}

			if tc.pruneUnauthorized {
				_ = os.MkdirAll(targetSkillsDir, 0755)
				_ = os.WriteFile(filepath.Join(targetSkillsDir, "unauthorized-rogue-skill"), []byte("rogue"), 0644)
				_ = os.WriteFile(filepath.Join(targetSkillsDir, "stale-skill.tmp"), []byte("tmp"), 0644)
			}

			if tc.legacyCleanup {
				legacySkills := filepath.Join(tmpHome, ".gemini", "skills")
				legacyPlugins := filepath.Join(tmpHome, ".gemini", "config", "plugins", "superpowers")
				_ = os.MkdirAll(legacySkills, 0755)
				_ = os.MkdirAll(legacyPlugins, 0755)
			}

			if tc.legacyCleanupErr {
				removeAllSkills = func(path string) error {
					return errors.New("simulated removeAll error")
				}
				legacySkills := filepath.Join(tmpHome, ".gemini", "skills")
				_ = os.MkdirAll(legacySkills, 0755)
			}

			if tc.symlinkTmpFallback {
				symlinkSkill = func(oldname, newname string) error {
					if strings.HasSuffix(newname, ".tmp") {
						return errors.New("simulated tmp symlink error")
					}
					return origSymlink(oldname, newname)
				}
			}

			if tc.symlinkTotalFail {
				symlinkSkill = func(oldname, newname string) error {
					return errors.New("simulated total symlink error")
				}
			}

			if tc.renameFailFallback {
				// Pre-create destination file to test removal prior to fallback rename
				_ = os.MkdirAll(targetSkillsDir, 0755)
				_ = os.WriteFile(filepath.Join(targetSkillsDir, "custom-skill"), []byte("existing"), 0644)
				renameCalls := 0
				renameSkill = func(oldpath, newpath string) error {
					renameCalls++
					if renameCalls == 1 {
						return errors.New("simulated initial rename error")
					}
					return origRename(oldpath, newpath)
				}
			}

			if err := p.SyncSkills(); err != nil {
				t.Fatalf("SyncSkills failed: %v", err)
			}

			installed := p.LinkTargetSkills(targetSkillsDir, tc.target)
			if installed == 0 && tc.target != TargetEphemeral && !tc.symlinkTotalFail && !tc.targetBlocked {
				t.Fatalf("expected installed skills > 0 for target %s", tc.target)
			}

			found := make(map[string]bool)
			entries, _ := os.ReadDir(targetSkillsDir)
			for _, e := range entries {
				found[e.Name()] = true
			}

			if tc.flatSkillWarning && found["flat-root-skill"] {
				t.Errorf("flat un-nested root skill should not be linked")
			}
			if tc.tornRead && found["torn-skill"] {
				t.Errorf("torn 0-byte skill should not be linked")
			}
			if tc.pruneUnauthorized && found["unauthorized-rogue-skill"] {
				t.Errorf("unauthorized skill was not pruned")
			}

			if !tc.symlinkTotalFail && !tc.targetBlocked {
				if tc.target == TargetDiscord {
					if !found["custom-skill"] || !found["discord-skill"] || !found["brainstorming"] || found["voice-skill"] {
						t.Errorf("unexpected skills for discord target: %v", found)
					}
				} else if tc.target == TargetVoice {
					if !found["custom-skill"] || !found["voice-skill"] || found["discord-skill"] || found["brainstorming"] {
						t.Errorf("unexpected skills for voice target: %v", found)
					}
				} else if tc.target == TargetEphemeral {
					if !found["eph-skill"] || found["custom-skill"] || found["discord-skill"] || found["voice-skill"] || found["brainstorming"] {
						t.Errorf("unexpected skills for ephemeral target: %v", found)
					}
				}
			}
		})
	}
}
