package env

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func (p *Provisioner) SetCustomSkillsDir(dir string) { p.customSkillsDir = dir }
func (p *Provisioner) SetSuperpowersDir(dir string)  { p.superpowersDir = dir }
func (p *Provisioner) SetAgentsSkillsDir(dir string) { p.agentsSkillsDir = dir }
func (p *Provisioner) SetAerialRulesDir(dir string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.aerialRulesDir = dir
}

func (p *Provisioner) SetConfigRulesDir(dir string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.configRulesDir = dir
}

func (p *Provisioner) SetHooksDir(dir string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hooksDir = dir
}

func (p *Provisioner) SetOAuthTokenSourcePath(path string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.oauthTokenSourcePath = path
}

func newTestConfig(mutators ...func(*config.ConfigData)) *config.Config {
	data := config.DefaultConfigData()
	data.DatabaseURL, data.Port = ":memory:", "0"
	data.DiscordToken, data.APIKey, data.HarnessAPIKey, data.GitHubPAT = "", "", "", ""
	data.GeminiHomeDir = filepath.Join(os.TempDir(), "aerial-test-gemini")
	data.DataDir = filepath.Join(os.TempDir(), "aerial-test-data")
	for _, fn := range mutators {
		if fn != nil {
			fn(data)
		}
	}
	return config.NewFromData(data)
}

func writeTestFile(t *testing.T, path, content string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func newTestProvisioner(t *testing.T, aerialFiles, configFiles map[string]string) (*Provisioner, string, string) {
	t.Helper()
	aerialDir := filepath.Join(t.TempDir(), "aerial")
	configDir := filepath.Join(t.TempDir(), "config")
	for rel, c := range aerialFiles {
		writeTestFile(t, filepath.Join(aerialDir, rel), c)
	}
	for rel, c := range configFiles {
		writeTestFile(t, filepath.Join(configDir, rel), c)
	}
	p := New(t.TempDir(), t.TempDir())
	p.SetAerialRulesDir(aerialDir)
	p.SetConfigRulesDir(configDir)
	return p, aerialDir, configDir
}

func writeSkillFixture(t *testing.T, baseDir, relPath, content string) string {
	t.Helper()
	return writeTestFile(t, filepath.Join(baseDir, relPath), content)
}

type provisionerInitCase struct {
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
}

func assertNilReceiver(t *testing.T) {
	t.Helper()
	var pNil *Provisioner
	pNil.SetOAuthTokenSourcePath("/ignored")

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
}

func assertProvisionerSetters(t *testing.T) {
	t.Helper()
	p := New("/h", "/d")
	if p.oauthTokenSourcePath != "/local/antigravity-oauth-token" {
		t.Fatalf("unexpected default oauthTokenSourcePath: %q", p.oauthTokenSourcePath)
	}
	p.SetCustomSkillsDir("/custom/skills")
	p.SetSuperpowersDir("/super/skills")
	p.SetAgentsSkillsDir("/agents/skills")
	p.SetAerialRulesDir("/aerial/rules")
	p.SetConfigRulesDir("/config/rules")
	p.SetHooksDir("/hooks/dir")
	p.SetOAuthTokenSourcePath("/custom/oauth")

	if p.customSkillsDir != "/custom/skills" || p.superpowersDir != "/super/skills" ||
		p.agentsSkillsDir != "/agents/skills" || p.aerialRulesDir != "/aerial/rules" ||
		p.configRulesDir != "/config/rules" || p.hooksDir != "/hooks/dir" ||
		p.oauthTokenSourcePath != "/custom/oauth" {
		t.Fatalf("setters failed to update Provisioner fields")
	}
	if p.homeDir != "/h" || p.dataDir != "/d" {
		t.Fatalf("unexpected HomeDir or DataDir: %s, %s", p.homeDir, p.dataDir)
	}
}

func runProvisionerInitCase(t *testing.T, tc provisionerInitCase) {
	t.Helper()
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
		assertNilReceiver(t)
		return
	}

	if tc.testSetters {
		assertProvisionerSetters(t)
		return
	}

	if tc.nilConfig {
		p := NewFromConfig(nil)
		if p.homeDir != "" || p.dataDir != "" {
			t.Fatalf("expected empty dirs from nil config, got %q, %q", p.homeDir, p.dataDir)
		}
		return
	}

	if tc.expectedHome == "/custom/gemini" {
		cfg := newTestConfig(func(c *config.ConfigData) {
			c.GeminiHomeDir = "/custom/gemini"
			c.DataDir = "/custom/data"
		})
		p := NewFromConfig(cfg)
		if p.homeDir != "/custom/gemini" || p.dataDir != "/custom/data" {
			t.Fatalf("unexpected paths from config: %q, %q", p.homeDir, p.dataDir)
		}
		return
	}

	p := New(tc.expectedHome, tc.expectedData)
	if p.homeDir != tc.expectedHome || p.dataDir != tc.expectedData {
		t.Fatalf("unexpected paths: got home=%q, data=%q", p.homeDir, p.dataDir)
	}
	if tc.expectedSkillsDir != "" && p.superpowersDir != tc.expectedSkillsDir {
		t.Fatalf("unexpected superpowersDir: got %q, want %q", p.superpowersDir, tc.expectedSkillsDir)
	}
}

func TestProvisioner_InitAndGetters(t *testing.T) {
	tests := []provisionerInitCase{
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
			runProvisionerInitCase(t, tc)
		})
	}
}

type syncCase struct {
	name          string
	emptyHome     bool
	withActiveCfg bool
	failSettings  bool
	failRules     bool
	failMCP       bool
	mockRoot      bool
	homeIsRoot    bool
	wantErr       bool
}

func assertChownPaths(t *testing.T, chowned []string) {
	t.Helper()
	if len(chowned) == 0 {
		t.Errorf("expected chownPath to be called when getEuid returns 0")
	}
	var foundRtHome, foundBrain bool
	for _, cp := range chowned {
		if strings.Contains(cp, filepath.Join("runtimes", "discord")) {
			foundRtHome = true
		}
		if strings.Contains(cp, "brain") {
			foundBrain = true
		}
	}
	if !foundRtHome {
		t.Errorf("expected runtime home directory to be chowned")
	}
	if !foundBrain {
		t.Errorf("expected brain storage to be chowned")
	}
	for _, cp := range chowned {
		if strings.Contains(cp, "target-skill") || strings.Contains(cp, "symlink-test") {
			t.Errorf("symlink or symlink target was unexpectedly chowned: %s", cp)
		}
	}
}

func assertRuntimeIsolation(t *testing.T, p *Provisioner) {
	t.Helper()
	for _, target := range allRuntimeTargets {
		targetGemini := filepath.Join(p.dataDir, "runtimes", string(target), ".gemini")
		if _, err := os.Stat(filepath.Join(targetGemini, "rules", "00_aerial_common_01_base.md")); err != nil {
			t.Errorf("expected common rule in %s runtime: %v", target, err)
		}
		if _, err := os.Stat(filepath.Join(targetGemini, "config", "mcp_config.json")); err != nil {
			t.Errorf("expected mcp config in %s runtime: %v", target, err)
		}
	}

	ephemeralMCP := filepath.Join(p.dataDir, "runtimes", "ephemeral", ".gemini", "config", "mcp_config.json")
	data, _ := os.ReadFile(ephemeralMCP)
	if strings.Contains(string(data), "scheduler") {
		t.Errorf("ephemeral runtime should not include scheduler: %s", string(data))
	}
}

func runSyncCase(t *testing.T, tc syncCase) {
	t.Helper()
	if tc.emptyHome {
		p := New("", "")
		if err := p.Sync(context.Background(), nil); err != nil {
			t.Fatalf("expected nil error on empty home, got: %v", err)
		}
		return
	}

	origGetEuid := getEuid
	origChownPath := chownPath
	origChmodPath := chmodPath
	origRootHome := rootHomeDir
	defer func() {
		getEuid = origGetEuid
		chownPath = origChownPath
		chmodPath = origChmodPath
		rootHomeDir = origRootHome
	}()

	var chowned []string
	var chmodded []string
	if tc.mockRoot {
		getEuid = func() int { return 0 }
		chownPath = func(path string, uid, gid int) error {
			chowned = append(chowned, path)
			return nil
		}
		chmodPath = func(path string, mode os.FileMode) error {
			chmodded = append(chmodded, path)
			return nil
		}
	} else {
		getEuid = func() int { return 1000 }
	}

	p, _, _ := newTestProvisioner(t, map[string]string{
		"common/01_base.md":  "Base rule content",
		"discord/01_flow.md": "Discord flow content",
		"voice/01_flow.md":   "Voice flow content",
	}, nil)

	if tc.homeIsRoot {
		mockRootDir := filepath.Join(t.TempDir(), "mock-root")
		_ = os.MkdirAll(filepath.Join(mockRootDir, ".gemini"), 0o755)
		rootHomeDir = mockRootDir
		origHome := p.homeDir
		p.homeDir = mockRootDir
		p.chownProvisionedPaths()
		p.homeDir = origHome
		if len(chmodded) == 0 || chmodded[0] != mockRootDir {
			t.Errorf("expected chmodPath to be called for %s, got %v", mockRootDir, chmodded)
		}
	}

	if tc.mockRoot && tc.withActiveCfg {
		symlinkTarget := filepath.Join(t.TempDir(), "target-skill")
		writeTestFile(t, filepath.Join(symlinkTarget, "SKILL.md"), "skill body")
		symlinkPath := filepath.Join(p.homeDir, ".gemini", "symlink-test")
		_ = os.Symlink(symlinkTarget, symlinkPath)
	}

	if tc.failSettings {
		writeTestFile(t, filepath.Join(p.homeDir, ".gemini", "antigravity-cli"), "file-blocking-dir")
	}
	if tc.failRules {
		writeTestFile(t, filepath.Join(p.homeDir, ".gemini", "rules"), "file-blocking-dir")
	}
	if tc.failMCP {
		writeTestFile(t, filepath.Join(p.homeDir, ".gemini", "config", "mcp_config.json", "blocker"), "b")
	}

	var cfg *config.Config
	if tc.withActiveCfg {
		cfg = newTestConfig(func(c *config.ConfigData) {
			c.APIKey = "test-key"
			c.Model = "gemini-2.5-pro"
			c.SystemPrompt = "Custom instructions"
		})
	}

	err := p.Sync(context.Background(), cfg)
	if (err != nil) != tc.wantErr {
		t.Fatalf("Sync() error = %v, wantErr %v", err, tc.wantErr)
	}

	if tc.mockRoot && !tc.wantErr && tc.withActiveCfg {
		assertChownPaths(t, chowned)
	}

	if !tc.wantErr && tc.withActiveCfg {
		assertRuntimeIsolation(t, p)
	}
}

func TestSync_EndToEndAndIsolation(t *testing.T) {
	tests := []syncCase{
		{name: "empty home returns nil", emptyHome: true, wantErr: false},
		{name: "successful default sync with default config data", withActiveCfg: false, wantErr: false},
		{name: "successful sync with active config and partitioned runtimes", withActiveCfg: true, wantErr: false},
		{name: "sync failure propagated from settings error", failSettings: true, wantErr: true},
		{name: "sync failure propagated from rules error", failRules: true, wantErr: true},
		{name: "sync failure propagated from mcp error", failMCP: true, wantErr: true},
		{name: "sync with mock root triggers chown", withActiveCfg: true, mockRoot: true, wantErr: false},
		{name: "chown provisioned paths with root home", withActiveCfg: false, mockRoot: true, homeIsRoot: true, wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSyncCase(t, tc)
		})
	}
}

type syncSettingsCase struct {
	name                  string
	apiKey                string
	model                 string
	preSeedOAuth          bool
	seedOAuthSource       bool
	preExistingOAuthFiles bool
	blockOAuthToken       bool
	blockInstallID        bool
	blockJetskiState      bool
	corruptedInput        bool
	blockFile             bool
	createTempFail        bool
	atomicEmptyDir        bool
	atomicNonEmptyDir     bool
	wantErr               bool
}

func testAtomicDirReplacement(t *testing.T, tc syncSettingsCase) {
	t.Helper()
	tmpDir := t.TempDir()
	p := New(tmpDir, tmpDir)
	targetPath := filepath.Join(tmpDir, "replace_dir")
	if tc.atomicNonEmptyDir {
		writeTestFile(t, filepath.Join(targetPath, "child.txt"), "child")
	} else if err := os.Mkdir(targetPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
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
}

func assertSyncSettingsOAuth(t *testing.T, tc syncSettingsCase, configDir string, parsed map[string]any) {
	t.Helper()
	if _, hasKey := parsed["apiKey"]; hasKey {
		t.Errorf("expected apiKey deleted in OAuth mode")
	}
	if _, hasProvider := parsed["modelProvider"]; hasProvider {
		t.Errorf("expected modelProvider deleted in OAuth mode")
	}
	if parsed["useG1Credits"] != true {
		t.Errorf("expected useG1Credits=true in OAuth mode, got %v", parsed["useG1Credits"])
	}
	if _, hasTW := parsed["trustedWorkspaces"]; !hasTW {
		t.Errorf("expected trustedWorkspaces in OAuth mode, got %v", parsed["trustedWorkspaces"])
	}
	installBytes, err := os.ReadFile(filepath.Join(configDir, "installation_id"))
	if err != nil || len(strings.TrimSpace(string(installBytes))) == 0 {
		t.Fatalf("expected non-empty installation_id, err=%v", err)
	}
	jetskiBytes, err := os.ReadFile(filepath.Join(configDir, "jetski_state.pbtxt"))
	if err != nil || len(strings.TrimSpace(string(jetskiBytes))) == 0 {
		t.Fatalf("expected non-empty jetski_state.pbtxt, err=%v", err)
	}
	if tc.seedOAuthSource {
		tokBytes, err := os.ReadFile(filepath.Join(configDir, "antigravity-oauth-token"))
		if err != nil {
			t.Fatalf("expected antigravity-oauth-token: %v", err)
		}
		wantSub := "seeded-tok"
		if tc.preExistingOAuthFiles {
			wantSub = "refreshed-tok"
		}
		if !strings.Contains(string(tokBytes), wantSub) {
			t.Errorf("unexpected token content %q, want substring %q", string(tokBytes), wantSub)
		}
	}
}

func assertSyncSettingsOutput(t *testing.T, tc syncSettingsCase, configDir, settingsPath string) {
	t.Helper()
	data, err := os.ReadFile(settingsPath)
	if err != nil {
		t.Fatalf("failed reading settings: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("settings is not valid json: %v", err)
	}

	if tc.apiKey != "" {
		if parsed["apiKey"] != tc.apiKey || parsed["modelProvider"] != "gemini" {
			t.Errorf("expected apiKey and gemini provider: %v", parsed)
		}
	} else {
		assertSyncSettingsOAuth(t, tc, configDir, parsed)
	}
	if tc.model != "" && parsed["model"] != tc.model {
		t.Errorf("expected model %q, got %v", tc.model, parsed["model"])
	}
}

func runSyncSettingsCase(t *testing.T, tc syncSettingsCase) {
	t.Helper()
	if tc.createTempFail {
		tmpDir := t.TempDir()
		p := New(tmpDir, tmpDir)
		writeTestFile(t, filepath.Join(tmpDir, "blocker"), "file")
		if err := p.writeAtomic(filepath.Join(tmpDir, "blocker", "sub", "test.txt"), "data"); err == nil {
			t.Errorf("expected error when directory creation fails")
		}
		return
	}

	if tc.atomicEmptyDir || tc.atomicNonEmptyDir {
		testAtomicDirReplacement(t, tc)
		return
	}

	tmpHome := t.TempDir()
	p := New(tmpHome, "")
	p.SetOAuthTokenSourcePath(filepath.Join(tmpHome, "nonexistent-oauth-source"))
	configDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli")
	settingsPath := filepath.Join(configDir, "settings.json")

	if tc.seedOAuthSource {
		srcTokenPath := writeTestFile(t, filepath.Join(tmpHome, "secret-oauth-token"), `{"refresh_token":"seeded-tok"}`)
		p.SetOAuthTokenSourcePath(srcTokenPath)
	}
	if tc.preExistingOAuthFiles {
		writeTestFile(t, filepath.Join(configDir, "antigravity-oauth-token"), `{"access_token":"refreshed-tok"}`)
		writeTestFile(t, filepath.Join(configDir, "installation_id"), "existing-uuid-123\n")
		writeTestFile(t, filepath.Join(configDir, "jetski_state.pbtxt"), "existing-jetski-state\n")
	}
	if tc.blockOAuthToken {
		writeTestFile(t, filepath.Join(configDir, "antigravity-oauth-token", "blocker"), "b")
	}
	if tc.blockInstallID {
		writeTestFile(t, filepath.Join(configDir, "installation_id", "blocker"), "b")
	}
	if tc.blockJetskiState {
		writeTestFile(t, filepath.Join(configDir, "jetski_state.pbtxt", "blocker"), "b")
	}

	if tc.preSeedOAuth {
		_ = os.MkdirAll(filepath.Dir(settingsPath), 0o755)
		_ = os.WriteFile(settingsPath, []byte(`{"apiKey":"old","modelProvider":"gemini"}`), 0o644)
	}
	if tc.corruptedInput {
		_ = os.MkdirAll(filepath.Dir(settingsPath), 0o755)
		_ = os.WriteFile(settingsPath, []byte("{broken json:"), 0o644)
	}
	if tc.blockFile {
		_ = os.MkdirAll(filepath.Join(tmpHome, ".gemini"), 0o755)
		_ = os.WriteFile(filepath.Join(tmpHome, ".gemini", "antigravity-cli"), []byte("file-blocker"), 0o644)
	}

	err := p.SyncSettings(tc.apiKey, tc.model)
	if (err != nil) != tc.wantErr {
		t.Fatalf("SyncSettings() error = %v, wantErr %v", err, tc.wantErr)
	}

	if !tc.wantErr {
		assertSyncSettingsOutput(t, tc, configDir, settingsPath)
	}
}

func TestSyncSettings(t *testing.T) {
	tests := []syncSettingsCase{
		{name: "api key and custom model", apiKey: "api-secret-123", model: "gemini-2.5-pro"},
		{name: "oauth mode without api key removes apiKey and provider", apiKey: "", model: "gemini-flash", preSeedOAuth: true},
		{name: "oauth mode seeds token from source and generates installation_id and jetski_state", apiKey: "", model: "gemini-flash", seedOAuthSource: true},
		{name: "oauth mode preserves existing token and installation_id across reloads", apiKey: "", model: "gemini-flash", seedOAuthSource: true, preExistingOAuthFiles: true},
		{name: "oauth mode fails when token destination is blocked by directory", apiKey: "", seedOAuthSource: true, blockOAuthToken: true, wantErr: true},
		{name: "oauth mode fails when installation_id destination is blocked by directory", apiKey: "", blockInstallID: true, wantErr: true},
		{name: "oauth mode fails when jetski_state destination is blocked by directory", apiKey: "", blockJetskiState: true, wantErr: true},
		{name: "recovers gracefully from corrupted json file", apiKey: "rec-key", model: "rec-model", corruptedInput: true},
		{name: "write failure returns error when parent dir blocked", blockFile: true, wantErr: true},
		{name: "writeAtomic fails on parent directory blocker", createTempFail: true, wantErr: true},
		{name: "writeAtomic replaces empty directory fallback", atomicEmptyDir: true},
		{name: "writeAtomic fails replacing non-empty directory", atomicNonEmptyDir: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSyncSettingsCase(t, tc)
		})
	}
}

type compileAndDiscoverRulesCase struct {
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
}

func testResolveDir(t *testing.T) {
	t.Helper()
	tmpDir := t.TempDir()
	candDir := filepath.Join(tmpDir, "cand")
	_ = os.Mkdir(candDir, 0o755)

	if r := resolveDir(candDir, nil); r != candDir {
		t.Errorf("resolveDir configured mismatch: %q", r)
	}
	if r := resolveDir(filepath.Join(tmpDir, "missing"), []string{candDir}); r != candDir {
		t.Errorf("resolveDir candidate fallback mismatch: %q", r)
	}
	missing := filepath.Join(tmpDir, "nonexistent")
	if r := resolveDir(missing, []string{filepath.Join(tmpDir, "nonexistent2")}); r != missing {
		t.Errorf("resolveDir default mismatch: %q", r)
	}
}

func testEmptyDirDiscovery(t *testing.T) {
	t.Helper()
	res, err := discoverRuleFiles("", "common", "00_")
	if res != nil || err != nil {
		t.Errorf("expected nil, nil for empty dir, got %v, %v", res, err)
	}

	tmpDir := t.TempDir()
	commonDir := filepath.Join(tmpDir, "common")
	_ = os.MkdirAll(filepath.Join(commonDir, "nested_dir"), 0o755)
	_ = os.WriteFile(filepath.Join(commonDir, "notes.txt"), []byte("not a rule"), 0o644)
	_ = os.WriteFile(filepath.Join(commonDir, "01.md"), []byte("valid rule"), 0o644)
	files, err := discoverRuleFiles(tmpDir, "common", "00_")
	if err != nil || len(files) != 1 {
		t.Errorf("expected 1 file discovered, got %d, err: %v", len(files), err)
	}
}

func runCompileAndDiscoverRulesCase(t *testing.T, tc compileAndDiscoverRulesCase) {
	t.Helper()
	if tc.statErrorSubfolder {
		tmpDir := t.TempDir()
		blocker := filepath.Join(tmpDir, "file_blocker")
		_ = os.WriteFile(blocker, []byte("f"), 0o644)
		_, err := discoverRuleFiles(blocker, "subfolder", "00_")
		if err == nil {
			t.Fatalf("expected error from ENOTDIR stat on subfolder")
		}
		return
	}

	if tc.emptyDirTest {
		testEmptyDirDiscovery(t)
		return
	}

	if tc.testResolve {
		testResolveDir(t)
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

	for _, pt := range []struct {
		cond bool
		path string
	}{
		{tc.dirIsFile, filepath.Join(aerialDir, "common")},
		{tc.userSubIsFile, filepath.Join(configDir, "discord")},
		{tc.aerialPersonaIsFile, filepath.Join(aerialDir, "persona")},
		{tc.configPersonaIsFile, filepath.Join(configDir, "persona")},
	} {
		if pt.cond {
			_ = os.RemoveAll(pt.path)
			_ = os.WriteFile(pt.path, []byte("file-not-dir"), 0o644)
		}
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
}

func TestCompileAndDiscoverRules(t *testing.T) {
	tests := []compileAndDiscoverRulesCase{
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
			runCompileAndDiscoverRulesCase(t, tc)
		})
	}
}

type atomicSwapRulesCase struct {
	name             string
	emptyStaging     bool
	targetIsFile     bool
	parentIsFile     bool
	stagingWriteFail bool
	backupFail       bool
	stagingMoveFail  bool
	restoreFail      bool
	wantErr          bool
}

func runAtomicSwapRulesCase(t *testing.T, tc atomicSwapRulesCase) {
	t.Helper()
	origRename := renameRulesDir
	defer func() { renameRulesDir = origRename }()

	tmpParent := t.TempDir()
	targetDir := filepath.Join(tmpParent, "rules")
	origFile := writeTestFile(t, filepath.Join(targetDir, "original.md"), "active rule")

	if tc.parentIsFile {
		writeTestFile(t, filepath.Join(tmpParent, "blocked_parent"), "file")
		targetDir = filepath.Join(tmpParent, "blocked_parent", "rules")
	}
	if tc.targetIsFile {
		_ = os.RemoveAll(targetDir)
		writeTestFile(t, targetDir, "not a dir")
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
}

func TestAtomicSwapRulesDir(t *testing.T) {
	tests := []atomicSwapRulesCase{
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
			runAtomicSwapRulesCase(t, tc)
		})
	}
}

type syncRulesCase struct {
	name          string
	emptyHome     bool
	zeroRules     bool
	primaryFail   bool
	configFail    bool
	voiceFail     bool
	ephemeralFail bool
	runtimeFail   bool
	concurrent    bool
	noDataDir     bool
	staleClaudeMD bool
	wantErr       bool
	wantPrimaryMD bool
}

func assertSyncRulesOutput(t *testing.T, tc syncRulesCase, p *Provisioner) {
	t.Helper()
	rulesDir := filepath.Join(p.homeDir, ".gemini", "rules")
	entries, rErr := os.ReadDir(rulesDir)
	if rErr != nil || len(entries) == 0 {
		t.Fatalf("expected rules in %s: %v", rulesDir, rErr)
	}
	claudeMD := filepath.Join(p.homeDir, ".claude", "CLAUDE.md")
	claudeData, cErr := os.ReadFile(claudeMD)
	if tc.wantPrimaryMD {
		if cErr != nil || len(claudeData) == 0 {
			t.Fatalf("expected CLAUDE.md in %s: %v", claudeMD, cErr)
		}
		if strings.Contains(string(claudeData), "---") {
			t.Errorf("CLAUDE.md should not contain YAML frontmatter: %s", string(claudeData))
		}
	} else if !os.IsNotExist(cErr) {
		t.Errorf("expected no primary CLAUDE.md in %s, got err=%v", claudeMD, cErr)
	}
	if tc.noDataDir {
		return
	}
	for _, target := range allRuntimeTargets {
		rtClaudeMD := filepath.Join(p.runtimeHomeDir(target), ".claude", "CLAUDE.md")
		if _, err := os.Stat(rtClaudeMD); err != nil {
			t.Errorf("expected CLAUDE.md in %s runtime: %v", target, err)
		}
		rtClaudeProjects := filepath.Join(p.runtimeHomeDir(target), ".claude", "projects")
		if fi, err := os.Lstat(rtClaudeProjects); err != nil || fi.Mode()&os.ModeSymlink == 0 {
			t.Errorf("expected projects symlink in %s runtime: %v", target, err)
		}
	}
}

func runSyncRulesCase(t *testing.T, tc syncRulesCase) {
	t.Helper()
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
	if tc.noDataDir {
		np := New(t.TempDir(), "")
		np.SetAerialRulesDir(p.aerialRulesDir)
		np.SetConfigRulesDir(p.configRulesDir)
		p = np
	}
	if tc.staleClaudeMD {
		writeTestFile(t, filepath.Join(p.homeDir, ".claude", "CLAUDE.md"), "stale")
	}

	if tc.zeroRules {
		p.SetAerialRulesDir(t.TempDir())
		p.SetConfigRulesDir(t.TempDir())
	}

	if tc.voiceFail {
		writeTestFile(t, filepath.Join(aerialDir, "voice", "torn.md"), "")
	}
	if tc.ephemeralFail {
		writeTestFile(t, filepath.Join(aerialDir, "ephemeral", "torn.md"), "")
	}
	if tc.primaryFail {
		writeTestFile(t, filepath.Join(p.homeDir, ".gemini", "rules"), "file blocker")
	}
	if tc.configFail {
		writeTestFile(t, filepath.Join(p.homeDir, ".gemini", "config", "rules"), "file blocker")
	}
	if tc.runtimeFail {
		writeTestFile(t, filepath.Join(p.dataDir, "runtimes", "discord", ".gemini", "rules"), "file blocker")
	}

	if tc.concurrent {
		var wg sync.WaitGroup
		errCh := make(chan error, 5)
		for i := range 5 {
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
		assertSyncRulesOutput(t, tc, p)
	}
}

func TestSyncRules(t *testing.T) {
	tests := []syncRulesCase{
		{name: "empty home returns nil", emptyHome: true, wantErr: false},
		{name: "successful sync compiles and provisions all runtimes", wantErr: false},
		{name: "stale primary CLAUDE.md removed when runtimes exist", staleClaudeMD: true, wantErr: false},
		{name: "no dataDir writes primary CLAUDE.md", noDataDir: true, wantErr: false, wantPrimaryMD: true},
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
			runSyncRulesCase(t, tc)
		})
	}
}

func TestClaudeTargetMcpAndSettingsProvisioning(t *testing.T) {
	tests := []struct {
		name          string
		nilProv       bool
		emptyDir      bool
		emptyRaw      json.RawMessage
		invalidJSON   json.RawMessage
		noMCPServers  json.RawMessage
		blockedClaude bool
	}{
		{name: "nil provisioner", nilProv: true},
		{name: "empty dir", emptyDir: true},
		{name: "empty raw config", emptyRaw: json.RawMessage("")},
		{name: "null raw config", emptyRaw: json.RawMessage("null")},
		{name: "quotes raw config", emptyRaw: json.RawMessage(`""`)},
		{name: "invalid json returns raw", invalidJSON: json.RawMessage(`{not valid`), emptyRaw: json.RawMessage(`{not valid`)},
		{name: "no mcpServers in json", noMCPServers: json.RawMessage(`{"other": 1}`)},
		{name: "blocked claude dir", blockedClaude: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var p *Provisioner
			if !tc.nilProv {
				p = New(tmp, tmp)
			}
			targetDir := filepath.Join(tmp, ".claude")
			if tc.emptyDir {
				targetDir = ""
			}
			if tc.blockedClaude {
				targetDir = filepath.Join(tmp, "blocked_claude")
				writeTestFile(t, targetDir, "blocking file")
			}

			raw := json.RawMessage(`{"mcpServers":{"srv":{"server_url":"http://test:8080"}}}`)
			if len(tc.emptyRaw) > 0 {
				raw = tc.emptyRaw
			}
			if len(tc.invalidJSON) > 0 {
				converted := ConvertMCPConfigForClaude(tc.invalidJSON)
				if string(converted) != string(tc.invalidJSON) {
					t.Fatalf("expected raw return on invalid JSON: %s", string(converted))
				}
			}
			if len(tc.noMCPServers) > 0 {
				converted := ConvertMCPConfigForClaude(tc.noMCPServers)
				if string(converted) != string(tc.noMCPServers) {
					t.Fatalf("expected raw return when no mcpServers: %s", string(converted))
				}
			}

			if p != nil {
				_ = p.EnsureTargetClaudeMcpConfig(targetDir, raw)
				p.provisionTargetClaudeSettings(targetDir)
			}
		})
	}
}
