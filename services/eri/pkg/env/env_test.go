package env

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

func (p *Provisioner) SetCodexAuthSourcePath(path string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.codexAuthSourcePath = path
}

func (p *Provisioner) SetMuseAuthSourcePath(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.museAuthSourcePath = path
}

func newTestConfig(mutators ...func(*config.ConfigData)) *config.Config {
	data := config.DefaultConfigData()
	data.CredentialSources = config.CredentialSources{OAuth: config.CredentialSource{Disabled: true}, Codex: config.CredentialSource{Disabled: true}}
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
	p := newDisabledProvisioner(t, t.TempDir(), t.TempDir())
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
	pNil.SetCodexAuthSourcePath("/ignored")

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
	if err := pNil.EnsureTargetCodexConfigTOML("/tmp", nil); err != nil {
		t.Errorf("expected nil error on EnsureTargetCodexConfigTOML with nil receiver")
	}
	if err := pNil.EnsureTargetMuseSettingsJSON("/tmp", nil); err != nil {
		t.Errorf("expected nil error on EnsureTargetMuseSettingsJSON with nil receiver")
	}
}

func assertProvisionerSetters(t *testing.T) {
	t.Helper()
	p := newDisabledProvisioner(t, "/h", "/d")
	if p.oauthTokenSourcePath != "" {
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
		_, err := NewFromConfig(nil)
		if err == nil {
			t.Fatal("expected nil configuration error")
		}
		return
	}

	if tc.expectedHome == "/custom/gemini" {
		cfg := newTestConfig(func(c *config.ConfigData) {
			c.GeminiHomeDir = "/custom/gemini"
			c.DataDir = "/custom/data"
		})
		p, err := NewFromConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if p.homeDir != "/custom/gemini" || p.dataDir != "/custom/data" {
			t.Fatalf("unexpected paths from config: %q, %q", p.homeDir, p.dataDir)
		}
		return
	}

	p := newDisabledProvisioner(t, tc.expectedHome, tc.expectedData)
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
	failCodexAuth bool
	failMuseAuth  bool
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
	var foundRtHome, foundBrain, foundCodex, foundAtomicTmp, foundRtConfigParent bool
	for _, cp := range chowned {
		if strings.Contains(cp, filepath.Join("runtimes", "discord")) {
			foundRtHome = true
		}
		if strings.Contains(cp, "brain") {
			foundBrain = true
		}
		if strings.Contains(cp, ".codex") || strings.Contains(cp, "codex") {
			foundCodex = true
		}
		if strings.Contains(cp, ".settings.json.tmp.") {
			foundAtomicTmp = true
		}
		if strings.HasSuffix(cp, filepath.Join("runtimes", "ephemeral", ".config")) {
			foundRtConfigParent = true
		}
	}
	if !foundRtHome {
		t.Errorf("expected runtime home directory to be chowned")
	}
	if !foundBrain {
		t.Errorf("expected brain storage to be chowned")
	}
	if !foundCodex {
		t.Errorf("expected codex directory to be chowned")
	}
	if !foundAtomicTmp {
		t.Errorf("expected writeAtomic temp file to be chowned before rename")
	}
	if !foundRtConfigParent {
		t.Errorf("expected runtime .config parent directory to be chowned")
	}
	for _, cp := range chowned {
		if strings.Contains(cp, "target-skill") || strings.Contains(cp, "symlink-test") {
			t.Errorf("symlink or symlink target was unexpectedly chowned: %s", cp)
		}
		if strings.Contains(cp, filepath.Join("cache", "go-build")) || strings.Contains(cp, "artifact.bin") {
			t.Errorf("cache file was unexpectedly chowned during Sync: %s", cp)
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
		targetCodex := filepath.Join(p.dataDir, "runtimes", string(target), ".codex")
		if _, err := os.Stat(filepath.Join(targetCodex, "AGENTS.md")); err != nil {
			t.Errorf("expected AGENTS.md in %s runtime: %v", target, err)
		}
		if _, err := os.Stat(filepath.Join(targetCodex, "config.toml")); err != nil {
			t.Errorf("expected config.toml in %s runtime: %v", target, err)
		}
	}

	ephemeralMCP := filepath.Join(p.dataDir, "runtimes", "ephemeral", ".gemini", "config", "mcp_config.json")
	data, _ := os.ReadFile(ephemeralMCP)
	if strings.Contains(string(data), "scheduler") {
		t.Errorf("ephemeral runtime should not include scheduler: %s", string(data))
	}
}

func injectSyncFailure(t *testing.T, tc syncCase, p *Provisioner) {
	t.Helper()
	if tc.failSettings {
		writeTestFile(t, filepath.Join(p.homeDir, ".gemini", "antigravity-cli"), "file-blocking-dir")
	}
	if tc.failCodexAuth {
		writeTestFile(t, filepath.Join(p.dataDir, "codex"), "file-blocking-dir")
		p.SetCodexAuthSourcePath(writeTestFile(t, filepath.Join(t.TempDir(), "auth.json"), `{"seed":1}`))
	}
	if tc.failMuseAuth {
		writeTestFile(t, filepath.Join(p.dataDir, "muse"), "file-blocking-dir")
		p.SetMuseAuthSourcePath(writeTestFile(t, filepath.Join(p.homeDir, "source-auth.json"), `{"seed":1}`))
	}
	if tc.failRules {
		writeTestFile(t, filepath.Join(p.homeDir, ".gemini", "rules"), "file-blocking-dir")
	}
	if tc.failMCP {
		writeTestFile(t, filepath.Join(p.homeDir, ".gemini", "config", "mcp_config.json", "blocker"), "b")
	}
}

func setupMockRootHome(t *testing.T, p *Provisioner, chmodded *[]string) {
	t.Helper()
	mockRootDir := filepath.Join(t.TempDir(), "mock-root")
	_ = os.MkdirAll(filepath.Join(mockRootDir, ".gemini"), 0o755)
	rootHomeDir = mockRootDir
	origHome := p.homeDir
	p.homeDir = mockRootDir
	p.chownProvisionedPaths()
	p.homeDir = origHome
	if len(*chmodded) == 0 || (*chmodded)[0] != mockRootDir {
		t.Errorf("expected chmodPath to be called for %s, got %v", mockRootDir, *chmodded)
	}
}

func assertSyncAuthFailure(t *testing.T, tc syncCase, err error) {
	t.Helper()
	if tc.failCodexAuth && (err == nil || !strings.Contains(err.Error(), "codex auth")) {
		t.Fatalf("expected error to mention 'codex auth', got: %v", err)
	}
	if tc.failMuseAuth && (err == nil || !strings.Contains(err.Error(), "muse auth")) {
		t.Fatalf("expected error to mention 'muse auth', got: %v", err)
	}
}

func runSyncCase(t *testing.T, tc syncCase) {
	t.Helper()
	if tc.emptyHome {
		p := newDisabledProvisioner(t, "", "")
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
		setupMockRootHome(t, p, &chmodded)
	}

	if tc.mockRoot && tc.withActiveCfg {
		symlinkTarget := filepath.Join(t.TempDir(), "target-skill")
		writeTestFile(t, filepath.Join(symlinkTarget, "SKILL.md"), "skill body")
		symlinkPath := filepath.Join(p.homeDir, ".gemini", "symlink-test")
		_ = os.Symlink(symlinkTarget, symlinkPath)
		writeTestFile(t, filepath.Join(p.dataDir, "cache", "go-build", "artifact.bin"), "bin")
	}

	injectSyncFailure(t, tc, p)

	var cfg *config.Config
	if tc.withActiveCfg {
		cfg = newTestConfig(func(c *config.ConfigData) {
			c.APIKey = "test-key"
			c.Model = "gemini-2.5-pro"
			c.SystemPrompt = "Custom instructions"
			if tc.failMuseAuth {
				c.CredentialSources.Muse = config.CredentialSource{Path: filepath.Join(p.homeDir, "source-auth.json")}
			}
		})
	}

	err := p.Sync(context.Background(), cfg)
	if (err != nil) != tc.wantErr {
		t.Fatalf("Sync() error = %v, wantErr %v", err, tc.wantErr)
	}
	assertSyncAuthFailure(t, tc, err)

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
		{name: "sync failure propagated from codex auth error", failCodexAuth: true, wantErr: true},
		{name: "sync failure propagated from muse auth error", withActiveCfg: true, failMuseAuth: true, wantErr: true},
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
	p := newDisabledProvisioner(t, tmpDir, tmpDir)
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
	if err != nil || strings.TrimSpace(string(installBytes)) == "" {
		t.Fatalf("expected non-empty installation_id, err=%v", err)
	}
	jetskiBytes, err := os.ReadFile(filepath.Join(configDir, "jetski_state.pbtxt"))
	if err != nil || strings.TrimSpace(string(jetskiBytes)) == "" {
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
		p := newDisabledProvisioner(t, tmpDir, tmpDir)
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
	p := newDisabledProvisioner(t, tmpHome, "")
	p.SetOAuthTokenSourcePath("")
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
				p = newDisabledProvisioner(t, tmp, tmp)
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

type codexTargetCase struct {
	name        string
	nilProv     bool
	emptyDir    bool
	rawConfig   json.RawMessage
	blockedDir  bool
	wantErr     bool
	wantTOML    bool
	wantContent string
}

func assertTargetConfigFileExists(t *testing.T, targetDir, filename string, expectFile bool, wantContent string) {
	t.Helper()
	if targetDir == "" {
		return
	}
	filePath := filepath.Join(targetDir, filename)
	data, readErr := os.ReadFile(filePath)
	if expectFile && readErr != nil {
		t.Fatalf("expected %s at %s, but read failed: %v", filename, filePath, readErr)
	}
	if !expectFile && readErr == nil {
		t.Fatalf("did not expect %s at %s, but file exists", filename, filePath)
	}
	if wantContent != "" && !strings.Contains(string(data), wantContent) {
		t.Fatalf("%s missing expected substring %q: %s", filename, wantContent, string(data))
	}
}

func assertCodexTargetTOML(t *testing.T, targetDir string, tc codexTargetCase) {
	t.Helper()
	assertTargetConfigFileExists(t, targetDir, "config.toml", tc.wantTOML, tc.wantContent)
}

func TestCodexTargetMcpAndHooksProvisioning(t *testing.T) {
	tests := []codexTargetCase{
		{
			name:        "valid target config written",
			rawConfig:   json.RawMessage(`{"mcpServers":{"srv":{"server_url":"http://test:8080"}}}`),
			wantErr:     false,
			wantTOML:    true,
			wantContent: `url = "http://test:8080"`,
		},
		{
			name:       "blocked codex dir returns error",
			rawConfig:  json.RawMessage(`{"mcpServers":{"srv":{"server_url":"http://test:8080"}}}`),
			blockedDir: true,
			wantErr:    true,
			wantTOML:   false,
		},
		{
			name:      "empty raw config does not write toml",
			rawConfig: json.RawMessage(""),
			wantErr:   false,
			wantTOML:  false,
		},
		{
			name:      "null raw config does not write toml",
			rawConfig: json.RawMessage("null"),
			wantErr:   false,
			wantTOML:  false,
		},
		{
			name:      "quoted empty raw config does not write toml",
			rawConfig: json.RawMessage(`""`),
			wantErr:   false,
			wantTOML:  false,
		},
		{
			name:      "empty dir returns nil without writing",
			emptyDir:  true,
			rawConfig: json.RawMessage(`{"mcpServers":{"srv":{"server_url":"http://test:8080"}}}`),
			wantErr:   false,
			wantTOML:  false,
		},
		{
			name:      "nil provisioner returns nil",
			nilProv:   true,
			rawConfig: json.RawMessage(`{"mcpServers":{"srv":{"server_url":"http://test:8080"}}}`),
			wantErr:   false,
			wantTOML:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var p *Provisioner
			if !tc.nilProv {
				p = newDisabledProvisioner(t, tmp, tmp)
			}
			targetDir := filepath.Join(tmp, ".codex")
			if tc.emptyDir {
				targetDir = ""
			}
			if tc.blockedDir {
				targetDir = filepath.Join(tmp, "blocked_codex")
				writeTestFile(t, targetDir, "blocking file")
			}

			err := p.EnsureTargetCodexConfigTOML(targetDir, tc.rawConfig)
			if (err != nil) != tc.wantErr {
				t.Fatalf("EnsureTargetCodexConfigTOML() error = %v, wantErr %v", err, tc.wantErr)
			}

			assertCodexTargetTOML(t, targetDir, tc)
		})
	}
}

type museSettingsWriteCase struct {
	label       string
	skippedDir  string
	jsonInput   json.RawMessage
	isBlocked   bool
	isNil       bool
	expectError bool
	expectFile  bool
	matchSubstr string
}

func assertMuseSettingsFile(t *testing.T, targetDir string, tc museSettingsWriteCase) {
	t.Helper()
	assertTargetConfigFileExists(t, targetDir, "settings.json", tc.expectFile, tc.matchSubstr)
}

func TestMuseTargetMcpAndHooksProvisioning(t *testing.T) {
	tests := []museSettingsWriteCase{
		{
			label:       "write valid settings.json with streamable http server",
			jsonInput:   json.RawMessage(`{"mcpServers":{"primary":{"serverUrl":"http://127.0.0.1:4040/mcp"}}}`),
			expectFile:  true,
			matchSubstr: `"type": "streamable-http"`,
		},
		{
			label:       "error when target config directory is obstructed by file",
			jsonInput:   json.RawMessage(`{"mcpServers":{"srv":{"url":"http://localhost:8080"}}}`),
			isBlocked:   true,
			expectError: true,
		},
		{
			label:     "do not write on empty raw json config payload",
			jsonInput: json.RawMessage(""),
		},
		{
			label:     "do not write on null json string literal",
			jsonInput: json.RawMessage(nullLiteral),
		},
		{
			label:     "do not write on empty json string literal",
			jsonInput: json.RawMessage(`""`),
		},
		{
			label:      "skip write when target directory argument is blank",
			skippedDir: "blank",
			jsonInput:  json.RawMessage(`{"mcpServers":{"srv":{"url":"http://test"}}}`),
		},
		{
			label:     "nil provisioner receiver gracefully succeeds without writing",
			isNil:     true,
			jsonInput: json.RawMessage(`{"mcpServers":{"srv":{"url":"http://test"}}}`),
		},
	}
	for _, tc := range tests {
		t.Run(tc.label, func(t *testing.T) {
			tmp := t.TempDir()
			var p *Provisioner
			if !tc.isNil {
				p = newDisabledProvisioner(t, tmp, tmp)
			}
			targetDir := filepath.Join(tmp, ".config", "muse")
			if tc.skippedDir != "" {
				targetDir = ""
			}
			if tc.isBlocked {
				targetDir = filepath.Join(tmp, "blocked_path")
				writeTestFile(t, targetDir, "conflict")
			}

			err := p.EnsureTargetMuseSettingsJSON(targetDir, tc.jsonInput)
			if (err != nil) != tc.expectError {
				t.Fatalf("EnsureTargetMuseSettingsJSON() error = %v, expectError %v", err, tc.expectError)
			}
			assertMuseSettingsFile(t, targetDir, tc)
		})
	}
}

func TestOverlayConfigMCPServers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		nilCfg  bool
		raw     string
		target  RuleTarget
		wantKey string
	}{
		{name: "nil config returns immediately", nilCfg: true},
		{name: "invalid json preserved verbatim", raw: "invalid-json", target: TargetDiscord, wantKey: "raw_server"},
		{name: "ephemeral target without servers leaves merged empty", target: TargetEphemeral},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cfg *config.Config
			if !tc.nilCfg {
				cfg = newTestConfig(func(c *config.ConfigData) {
					if tc.wantKey != "" {
						c.McpServers.Discord = map[string]json.RawMessage{
							tc.wantKey: json.RawMessage(tc.raw),
						}
					}
				})
			}
			target := tc.target
			if target == "" {
				target = TargetDiscord
			}
			merged := make(map[string]any)
			overlayConfigMCPServers(cfg, target, merged)
			if tc.wantKey != "" {
				if _, ok := merged[tc.wantKey]; !ok {
					t.Fatalf("expected key %q in merged", tc.wantKey)
				}
			}
		})
	}
}
