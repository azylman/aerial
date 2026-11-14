package env

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

type sharedAssetsCase struct {
	name              string
	fallbackCopy      bool
	linkAssetFail     bool
	existingSymlink   bool
	existingDirFiles  bool
	destinationExists bool
	existingPlainFile bool
	emptyDataDir      bool
}

func runSharedAssetsLinkFail(t *testing.T, p *Provisioner, tmpHome string) {
	t.Helper()
	srcFile := writeTestFile(t, filepath.Join(tmpHome, "src.txt"), "src-data")
	dstDir := filepath.Join(tmpHome, "dst_dir")
	writeTestFile(t, filepath.Join(dstDir, "c"), "c")
	p.linkOrCopyAsset(srcFile, dstDir)

	symlinkRuntimeAsset = func(oldname, newname string) error { return errors.New("symlink fail") }
	writeTestFile(t, filepath.Join(tmpHome, "file_blocker"), "f")
	p.linkOrCopyAsset(srcFile, filepath.Join(tmpHome, "file_blocker", "target.txt"))
}

func runProvisionRuntimeSharedAssetsCase(t *testing.T, tc sharedAssetsCase) {
	t.Helper()
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
	writeTestFile(t, filepath.Join(primaryCli, "settings.json"), `{"apiKey":"test"}`)
	writeTestFile(t, filepath.Join(primaryCli, "antigravity-oauth-token"), "tok-123")
	writeTestFile(t, filepath.Join(primaryCli, "installation_id"), "inst-456")
	writeTestFile(t, filepath.Join(primaryGemini, "mcp_oauth_tokens.json"), `{"oauth":"data"}`)

	runtimeHome := filepath.Join(t.TempDir(), "runtime")
	runtimeGemini := filepath.Join(runtimeHome, ".gemini")

	if tc.linkAssetFail {
		runSharedAssetsLinkFail(t, p, tmpHome)
		return
	}

	if tc.destinationExists && !tc.emptyDataDir {
		writeTestFile(t, filepath.Join(tmpData, "brain", "migrated_turn.jsonl"), "canon copy")
	}
	if tc.existingDirFiles {
		writeTestFile(t, filepath.Join(runtimeGemini, "brain", "migrated_turn.jsonl"), "transcript content")
	}
	if tc.existingPlainFile {
		writeTestFile(t, filepath.Join(runtimeGemini, "conversations"), "plain file blocker")
	}
	if tc.existingSymlink {
		targetCliBrain := filepath.Join(runtimeGemini, "antigravity-cli", "brain")
		_ = os.MkdirAll(filepath.Dir(targetCliBrain), 0o755)
		_ = os.Symlink(t.TempDir(), targetCliBrain)
	}

	p.provisionRuntimeSharedAssets(runtimeHome)

	// Idempotent second call exercises already-valid symlinks
	p.provisionRuntimeSharedAssets(runtimeHome)

	if !tc.emptyDataDir {
		p.linkSharedSessionStorage(tmpData, filepath.Join(tmpData, "brain"), filepath.Join(tmpData, "conversations"))
	}

	settingsDst := filepath.Join(runtimeGemini, "antigravity-cli", "settings.json")
	if data, err := os.ReadFile(settingsDst); err != nil || !strings.Contains(string(data), "test") {
		t.Fatalf("settings.json missing or incorrect in runtime: %v", err)
	}

	tokenDst := filepath.Join(runtimeGemini, "antigravity-cli", "antigravity-oauth-token")
	if _, err := os.Lstat(tokenDst); err != nil {
		t.Fatalf("shared token asset missing: %v", err)
	}

	if tc.existingDirFiles && !tc.emptyDataDir {
		migratedFile := filepath.Join(tmpData, "brain", "migrated_turn.jsonl")
		if data, err := os.ReadFile(migratedFile); err != nil {
			t.Fatalf("failed migrating directory contents to canonical brain: %v", err)
		} else if tc.destinationExists && string(data) != "canon copy" {
			t.Fatalf("expected canon copy preserved: %s", string(data))
		}
	}
}

func TestProvisionRuntimeSharedAssetsAndStorage(t *testing.T) {
	tests := []sharedAssetsCase{
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
			runProvisionRuntimeSharedAssetsCase(t, tc)
		})
	}
}

type mcpProvisioningCase struct {
	name               string
	target             RuleTarget
	withPAT            bool
	withFileConf       bool
	withCustomSrv      bool
	withRawNonObj      bool
	withRawInvalidJSON bool
	withEphemeralSrv   bool
	withMCPCfgStr      bool
	withHeaderSrv      bool
	strRaw             bool
	blockDir           bool
	syncFail           bool
	syncRuntimeFail    bool
	wantScheduler      bool
	wantDiscord        bool
	wantGitHub         bool
	wantURL            map[string]string
	wantHeaders        map[string]map[string]any
}

func testEnsureTargetMcpConfigEdgeCases(t *testing.T, p *Provisioner, tmpHome string) {
	t.Helper()
	var pNil *Provisioner
	if err := pNil.EnsureTargetMcpConfig(tmpHome, json.RawMessage(`{}`)); err != nil {
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

	strJSON := json.RawMessage(`"{\"mcpServers\":{\"str_srv\":{}}}"`)
	if err := p.EnsureTargetMcpConfig(tmpHome, strJSON); err != nil {
		t.Fatalf("EnsureTargetMcpConfig failed on json string: %v", err)
	}
}

func buildTestMCPConfig(tc mcpProvisioningCase) *config.Config {
	return newTestConfig(func(c *config.ConfigData) {
		if !tc.withRawInvalidJSON {
			c.McpServers.Common = map[string]json.RawMessage{
				"scheduler": json.RawMessage(`{"serverUrl":"http://haro.aerial/mcp"}`),
				"memory":    json.RawMessage(`{"serverUrl":"http://coven.aerial/mcp"}`),
				"github":    json.RawMessage(`{"serverUrl":"http://github-mcp:4003/mcp"}`),
			}
			c.McpServers.Discord = map[string]json.RawMessage{
				"discord": json.RawMessage(`{"serverUrl":"http://discord-mcp:4001/mcp"}`),
				"vanadis": json.RawMessage(`{"serverUrl":"http://vanadis.aerial/mcp"}`),
			}
		}
		if tc.withPAT {
			c.GitHubPAT = "ghp_" + "12345678901234567890"
		}
		if tc.withCustomSrv {
			if c.McpServers.Common == nil {
				c.McpServers.Common = make(map[string]json.RawMessage)
			}
			c.McpServers.Common["custom_common"] = json.RawMessage(`{"serverUrl":"http://common:8080"}`)
			c.McpServers.Voice = map[string]json.RawMessage{
				"custom_voice": json.RawMessage(`{"serverUrl":"http://voice:8080"}`),
			}
			c.McpServers.Ephemeral = map[string]json.RawMessage{
				"custom_eph": json.RawMessage(`{"serverUrl":"http://eph:8080"}`),
			}
		}
		if tc.withEphemeralSrv {
			c.McpServers.Ephemeral = map[string]json.RawMessage{
				"custom_eph": json.RawMessage(`{"serverUrl":"http://eph:8080"}`),
			}
		}
		if tc.withRawNonObj {
			if c.McpServers.Discord == nil {
				c.McpServers.Discord = make(map[string]json.RawMessage)
			}
			c.McpServers.Discord["raw_srv"] = json.RawMessage(`"string_only_value"`)
		}
		if tc.withRawInvalidJSON {
			if c.McpServers.Discord == nil {
				c.McpServers.Discord = make(map[string]json.RawMessage)
			}
			c.McpServers.Discord["invalid_srv"] = json.RawMessage(`{not-valid-json`)
		}
		if tc.withHeaderSrv {
			c.McpServers.Common["ubereats"] = json.RawMessage(`{"serverUrl":"https://mcp.example.com/mcp?key=abc","headers":{"Authorization":"Bearer tok-123","X-Tenant":"home"}}`)
		}
		if tc.withMCPCfgStr {
			c.MCPConfig = `{"mcpServers":{"fallback_srv":{"serverUrl":"http://fallback:80"}}}`
		}
	})
}

func assertTargetStandardServers(t *testing.T, tc mcpProvisioningCase, servers map[string]any) {
	t.Helper()
	if (servers["scheduler"] != nil) != tc.wantScheduler {
		t.Errorf("scheduler presence = %v, want %v", servers["scheduler"] != nil, tc.wantScheduler)
	}
	if tc.wantScheduler && servers["scheduler"] != nil {
		if sMap, ok := servers["scheduler"].(map[string]any); ok {
			if sMap["serverUrl"] != "http://haro.aerial/mcp" {
				t.Errorf("scheduler serverUrl = %v, want http://haro.aerial/mcp", sMap["serverUrl"])
			}
		}
	}
	if (servers["memory"] != nil) != tc.wantScheduler {
		t.Errorf("memory presence = %v, want %v", servers["memory"] != nil, tc.wantScheduler)
	}
	if (servers["discord"] != nil) != tc.wantDiscord {
		t.Errorf("discord presence = %v, want %v", servers["discord"] != nil, tc.wantDiscord)
	}
	if (servers["vanadis"] != nil) != tc.wantDiscord {
		t.Errorf("vanadis presence = %v, want %v", servers["vanadis"] != nil, tc.wantDiscord)
	}
	if tc.wantDiscord && servers["vanadis"] != nil {
		if vMap, ok := servers["vanadis"].(map[string]any); ok {
			if vMap["serverUrl"] != "http://vanadis.aerial/mcp" {
				t.Errorf("vanadis serverUrl = %v, want http://vanadis.aerial/mcp", vMap["serverUrl"])
			}
		} else {
			t.Errorf("expected servers[\"vanadis\"] to be map[string]interface{}, got %T", servers["vanadis"])
		}
	}
	if (servers["github"] != nil) != tc.wantGitHub {
		t.Errorf("github presence = %v, want %v", servers["github"] != nil, tc.wantGitHub)
	}
}

func assertTargetCustomServers(t *testing.T, tc mcpProvisioningCase, servers map[string]any) {
	t.Helper()
	for name, want := range tc.wantURL {
		srv, _ := servers[name].(map[string]any)
		if srv["serverUrl"] != want {
			t.Errorf("%s serverUrl = %v, want %q", name, srv["serverUrl"], want)
		}
		assertMCPHeaders(t, name, srv, tc.wantHeaders[name])
	}
	if tc.withFileConf && servers["custom_override"] == nil {
		t.Errorf("expected custom_override from file-based config")
	}
	if tc.withEphemeralSrv && servers["custom_eph"] == nil {
		t.Errorf("expected custom_eph server for ephemeral target")
	}
	if tc.withRawInvalidJSON && len(servers) != 0 {
		t.Errorf("expected empty servers when marshal fails on invalid raw JSON, got %v", servers)
	}
	if tc.withCustomSrv {
		if servers["custom_common"] == nil || servers["custom_voice"] == nil {
			t.Errorf("expected custom servers from McpServers struct: %+v", servers)
		}
	}
	if tc.withMCPCfgStr && servers["fallback_srv"] == nil {
		t.Errorf("expected fallback_srv from MCPConfig string")
	}
}

func assertTargetMCPServers(t *testing.T, tc mcpProvisioningCase, servers map[string]any) {
	t.Helper()
	assertTargetStandardServers(t, tc, servers)
	assertTargetCustomServers(t, tc, servers)
}

func assertClaudeMCPServers(t *testing.T, tc mcpProvisioningCase, p *Provisioner, cfg *config.Config) {
	t.Helper()
	if err := p.SyncMCP(context.Background(), cfg); err != nil {
		t.Fatalf("SyncMCP failed: %v", err)
	}
	cData, err := os.ReadFile(filepath.Join(p.runtimeHomeDir(tc.target), ".claude", "mcp_config.json"))
	if err != nil {
		t.Fatalf("reading claude mcp config: %v", err)
	}
	var cParsed map[string]map[string]any
	if err := json.Unmarshal(cData, &cParsed); err != nil {
		t.Fatalf("parsing claude mcp config: %v", err)
	}
	cServers := cParsed["mcpServers"]
	if tc.wantScheduler {
		sMap, _ := cServers["scheduler"].(map[string]any)
		if sMap["type"] != "http" || sMap["url"] != "http://haro.aerial/mcp" {
			t.Errorf("claude scheduler config mismatch: %+v", sMap)
		}
	}
	for name, want := range tc.wantURL {
		srv, _ := cServers[name].(map[string]any)
		if srv["type"] != "http" || srv["url"] != want {
			t.Errorf("claude %s = %+v, want type http url %q", name, srv, want)
		}
		if _, has := srv["serverUrl"]; has {
			t.Errorf("claude %s retains serverUrl: %+v", name, srv)
		}
		assertMCPHeaders(t, "claude "+name, srv, tc.wantHeaders[name])
	}
}

func runMCPProvisioningCase(t *testing.T, tc mcpProvisioningCase) {
	t.Helper()
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	cfg := buildTestMCPConfig(tc)

	if tc.withFileConf {
		writeTestFile(t, filepath.Join(tmpData, "mcp.config.json"), `{"mcpServers":{"custom_override":{"serverUrl":"http://override:80"}}}`)
	}
	if tc.syncFail {
		writeTestFile(t, filepath.Join(tmpHome, ".gemini", "config"), "blocker")
		if err := p.SyncMCP(context.Background(), cfg); err == nil {
			t.Fatalf("expected error on blocked config directory in SyncMCP")
		}
		return
	}
	if tc.syncRuntimeFail {
		writeTestFile(t, filepath.Join(tmpData, "runtimes", "discord", ".gemini", "config"), "blocker")
		if err := p.SyncMCP(context.Background(), cfg); err == nil {
			t.Fatalf("expected error on blocked runtime config directory in SyncMCP")
		}
		return
	}

	if tc.strRaw || tc.blockDir {
		testEnsureTargetMcpConfigEdgeCases(t, p, tmpHome)
		return
	}

	raw := p.LoadTargetMCPConfig(cfg, tc.target)
	var parsed map[string]map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("failed parsing MCP config: %v", err)
	}
	assertTargetMCPServers(t, tc, parsed["mcpServers"])
	assertClaudeMCPServers(t, tc, p, cfg)
}

func TestMCPProvisioning(t *testing.T) {
	tests := []mcpProvisioningCase{
		{
			name: "discord target with github", target: TargetDiscord, withPAT: true, wantScheduler: true, wantDiscord: true, wantGitHub: true,
			wantURL:     map[string]string{"github": "http://github-mcp:4003/mcp"},
			wantHeaders: map[string]map[string]any{"github": nil},
		},
		{
			name: "config server headers pass through verbatim", target: TargetVoice, withHeaderSrv: true, wantScheduler: true, wantGitHub: true,
			wantURL:     map[string]string{"ubereats": "https://mcp.example.com/mcp?key=abc"},
			wantHeaders: map[string]map[string]any{"ubereats": {"Authorization": "Bearer tok-123", "X-Tenant": "home"}},
		},
		{name: "voice target isolates scheduler and excludes discord services", target: TargetVoice, withPAT: true, wantScheduler: true, wantGitHub: true},
		{name: "ephemeral target excludes scheduler and discord services", target: TargetEphemeral},
		{name: "ephemeral target includes ephemeral servers", target: TargetEphemeral, withEphemeralSrv: true},
		{name: "discord target overlays file-based mcp configuration", target: TargetDiscord, withFileConf: true, wantScheduler: true, wantDiscord: true, wantGitHub: true},
		{name: "overlay custom mcp servers across targets", target: TargetVoice, withCustomSrv: true, wantScheduler: true, wantGitHub: true},
		{name: "overlay custom mcp server with raw non-obj value", target: TargetDiscord, withRawNonObj: true, wantScheduler: true, wantDiscord: true, wantGitHub: true},
		{name: "overlay custom mcp server with invalid json raw value", target: TargetDiscord, withRawInvalidJSON: true},
		{name: "cfg.Current().MCPConfig string fallback", target: TargetDiscord, withMCPCfgStr: true, wantScheduler: true, wantDiscord: true, wantGitHub: true},
		{name: "EnsureTargetMcpConfig handles raw JSON string and invalid paths", strRaw: true, blockDir: true},
		{name: "SyncMCP error bubbles up when primary config write fails", syncFail: true},
		{name: "SyncMCP error bubbles up when runtime config write fails", syncRuntimeFail: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runMCPProvisioningCase(t, tc)
		})
	}
}

func assertMCPHeaders(t *testing.T, name string, srv map[string]any, want map[string]any) {
	t.Helper()
	got, has := srv["headers"]
	if want == nil {
		if has {
			t.Errorf("%s headers = %+v, want no headers key", name, got)
		}
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s headers = %+v, want %+v", name, got, want)
	}
}

type skillsProvisioningCase struct {
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
	replaceExistingDir bool
	renameTotalFail    bool
	orphanSymlinkSweep bool
}

func testOrphanSymlinkSweep(t *testing.T, origRemove func(string) error) {
	t.Helper()
	tmpTarget := t.TempDir()
	validSrc := t.TempDir()
	writeTestFile(t, filepath.Join(validSrc, "file.txt"), "ok")
	validLink := filepath.Join(tmpTarget, "valid")
	_ = os.Symlink(validSrc, validLink)
	brokenLink := filepath.Join(tmpTarget, "broken")
	_ = os.Symlink(filepath.Join(t.TempDir(), "nonexistent"), brokenLink)
	plainFile := writeTestFile(t, filepath.Join(tmpTarget, "regular.txt"), "keep")

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
}

func setupSkillTestMocks(t *testing.T, tc skillsProvisioningCase, targetSkillsDir, tmpHome string, origRename, origSymlink func(string, string) error) {
	t.Helper()
	if tc.targetBlocked {
		writeTestFile(t, targetSkillsDir, "file blocker")
	}
	if tc.legacySymlinkDir {
		_ = os.MkdirAll(filepath.Dir(targetSkillsDir), 0o755)
		_ = os.Symlink(t.TempDir(), targetSkillsDir)
	}
	if tc.pruneUnauthorized {
		writeTestFile(t, filepath.Join(targetSkillsDir, "unauthorized-rogue-skill"), "rogue")
		writeTestFile(t, filepath.Join(targetSkillsDir, "stale-skill.tmp"), "tmp")
	}
	if tc.legacyCleanup {
		_ = os.MkdirAll(filepath.Join(tmpHome, ".gemini", "skills"), 0o755)
		_ = os.MkdirAll(filepath.Join(tmpHome, ".gemini", "config", "plugins", "superpowers"), 0o755)
	}
	if tc.legacyCleanupErr {
		removeAllSkills = func(path string) error { return errors.New("simulated removeAll error") }
		_ = os.MkdirAll(filepath.Join(tmpHome, ".gemini", "skills"), 0o755)
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
		symlinkSkill = func(oldname, newname string) error { return errors.New("simulated total symlink error") }
	}
	if tc.renameFailFallback {
		writeTestFile(t, filepath.Join(targetSkillsDir, "custom-skill"), "existing")
		renameCalls := 0
		renameSkill = func(oldpath, newpath string) error {
			renameCalls++
			if renameCalls == 1 {
				return errors.New("simulated initial rename error")
			}
			return origRename(oldpath, newpath)
		}
	}
	if tc.replaceExistingDir {
		existingDir := filepath.Join(targetSkillsDir, "custom-skill")
		if err := os.MkdirAll(existingDir, 0o755); err != nil {
			t.Fatalf("failed to create existing skill directory: %v", err)
		}
		writeTestFile(t, filepath.Join(existingDir, "stale.txt"), "old content")
	}
	if tc.renameTotalFail {
		renameSkill = func(oldpath, newpath string) error {
			return errors.New("simulated fatal rename error")
		}
	}
}

func assertSkillRenameAndReplace(t *testing.T, tc skillsProvisioningCase, targetSkillsDir string) {
	t.Helper()
	if tc.replaceExistingDir {
		tmpLink := filepath.Join(targetSkillsDir, "custom-skill.tmp")
		if _, err := os.Lstat(tmpLink); !os.IsNotExist(err) {
			t.Fatalf("expected tmp symlink %s to be cleaned up, got err: %v", tmpLink, err)
		}
		fi, err := os.Lstat(filepath.Join(targetSkillsDir, "custom-skill"))
		if err != nil {
			t.Fatalf("expected custom-skill to exist: %v", err)
		}
		if fi.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("expected custom-skill to be symlink, got mode: %v", fi.Mode())
		}
	}
	if tc.renameTotalFail {
		tmpLink := filepath.Join(targetSkillsDir, "custom-skill.tmp")
		if _, err := os.Lstat(tmpLink); !os.IsNotExist(err) {
			t.Fatalf("expected tmp symlink %s to be removed on failure, got err: %v", tmpLink, err)
		}
	}
}

func assertSkillTargetRules(t *testing.T, target RuleTarget, superpowers bool, found map[string]bool) {
	t.Helper()
	switch target {
	case TargetDiscord:
		if !found["custom-skill"] || !found["discord-skill"] || (superpowers != found["brainstorming"]) || found["voice-skill"] {
			t.Errorf("unexpected skills for discord target: %v", found)
		}
	case TargetVoice:
		if !found["custom-skill"] || !found["voice-skill"] || found["discord-skill"] || found["brainstorming"] {
			t.Errorf("unexpected skills for voice target: %v", found)
		}
	case TargetEphemeral:
		if !found["eph-skill"] || found["custom-skill"] || found["discord-skill"] || found["voice-skill"] || found["brainstorming"] {
			t.Errorf("unexpected skills for ephemeral target: %v", found)
		}
	}
}

func assertSkillEntries(t *testing.T, tc skillsProvisioningCase, found map[string]bool, targetSkillsDir string) {
	t.Helper()
	assertSkillRenameAndReplace(t, tc, targetSkillsDir)

	if tc.flatSkillWarning && found["flat-root-skill"] {
		t.Errorf("flat un-nested root skill should not be linked")
	}
	if tc.tornRead && found["torn-skill"] {
		t.Errorf("torn 0-byte skill should not be linked")
	}
	if tc.pruneUnauthorized && found["unauthorized-rogue-skill"] {
		t.Errorf("unauthorized skill was not pruned")
	}

	if !tc.symlinkTotalFail && !tc.targetBlocked && !tc.renameTotalFail {
		assertSkillTargetRules(t, tc.target, tc.superpowers, found)
	}
}

func runSkillsProvisioningCase(t *testing.T, tc skillsProvisioningCase) {
	t.Helper()
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
		testOrphanSymlinkSweep(t, origRemove)
		return
	}

	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := New(tmpHome, tmpData)

	customDir := t.TempDir()
	writeTestFile(t, filepath.Join(customDir, "README.txt"), "readme")

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
	} else {
		p.SetSuperpowersDir("")
	}

	if tc.tornRead {
		writeSkillFixture(t, customDir, "common/torn-skill/SKILL.md", "")
	}

	targetSkillsDir := filepath.Join(tmpHome, ".gemini", "config", "skills")
	setupSkillTestMocks(t, tc, targetSkillsDir, tmpHome, origRename, origSymlink)

	if err := p.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills failed: %v", err)
	}

	installed := p.LinkTargetSkills(targetSkillsDir, tc.target)
	if installed == 0 && tc.target != TargetEphemeral && !tc.symlinkTotalFail && !tc.targetBlocked && !tc.renameTotalFail {
		t.Fatalf("expected installed skills > 0 for target %s", tc.target)
	}

	found := make(map[string]bool)
	entries, _ := os.ReadDir(targetSkillsDir)
	for _, e := range entries {
		found[e.Name()] = true
	}

	assertSkillEntries(t, tc, found, targetSkillsDir)
}

func TestSkillsProvisioning(t *testing.T) {
	tests := []skillsProvisioningCase{
		{name: "discord target receives common and target skills and superpowers", target: TargetDiscord, superpowers: true},
		{name: "voice target excludes superpowers and discord skills", target: TargetVoice, superpowers: true},
		{name: "ephemeral target links exclusively ephemeral skills", target: TargetEphemeral, superpowers: true},
		{name: "torn read skips zero-byte SKILL.md", target: TargetDiscord, tornRead: true},
		{name: "flat skill warning on un-nested root skill", target: TargetDiscord, flatSkillWarning: true},
		{name: "active reconciliation prunes unauthorized skills", target: TargetDiscord, pruneUnauthorized: true},
		{name: "target skills directory is existing legacy symlink", target: TargetDiscord, legacySymlinkDir: true},
		{name: "cleans up legacy skills and plugins directories", target: TargetDiscord, legacyCleanup: true},
		{name: "logs warning and handles legacy remove errors", target: TargetDiscord, legacyCleanupErr: true},
		{name: "target skills directory creation blocked by file", target: TargetDiscord, targetBlocked: true},
		{name: "symlink tmp fails and triggers direct symlink fallback", target: TargetDiscord, symlinkTmpFallback: true},
		{name: "both tmp and direct symlink fail gracefully", target: TargetDiscord, symlinkTotalFail: true},
		{name: "rename fails and triggers destination removal fallback", target: TargetDiscord, renameFailFallback: true},
		{name: "replacing existing skill directory does not leave tmp symlink", target: TargetDiscord, replaceExistingDir: true},
		{name: "rename total failure cleans up tmp symlink", target: TargetDiscord, renameTotalFail: true},
		{name: "sweepOrphanedSymlinks removes broken links and retains valid links", orphanSymlinkSweep: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSkillsProvisioningCase(t, tc)
		})
	}
}

type syncHooksCase struct {
	name             string
	nilProv          bool
	emptyHome        bool
	withDataDir      bool
	withCustomAssets bool
	withCandidates   bool
	hooksDirIsFile   bool
	blockConfig      bool
	blockHooksDir    bool
}

func assertSyncHooksOutputs(t *testing.T, tc syncHooksCase, p *Provisioner, tmpHome, tmpData string) {
	t.Helper()
	if tc.withDataDir {
		for _, target := range allRuntimeTargets {
			rtHooksJSON := filepath.Join(tmpData, "runtimes", string(target), ".gemini", "config", "hooks.json")
			if _, err := os.Stat(rtHooksJSON); err != nil {
				t.Errorf("expected hooks.json in runtime %s: %v", target, err)
			}
			rtClaudeSettings := filepath.Join(tmpData, "runtimes", string(target), ".claude", "settings.json")
			if _, err := os.Stat(rtClaudeSettings); err != nil {
				t.Errorf("expected settings.json in claude runtime %s: %v", target, err)
			}
		}
		primaryClaudeSettings := filepath.Join(tmpHome, ".claude", "settings.json")
		if _, err := os.Stat(primaryClaudeSettings); err != nil {
			t.Errorf("expected settings.json in primary claude home: %v", err)
		}
	}

	if tc.withCustomAssets && !tc.blockHooksDir && !tc.blockConfig {
		scriptPath := filepath.Join(tmpHome, ".gemini", "config", "hooks", "custom_guard.py")
		fi, err := os.Stat(scriptPath)
		if err != nil {
			t.Fatalf("expected copied script at %s: %v", scriptPath, err)
		}
		if fi.Mode().Perm()&0o111 == 0 {
			t.Errorf("expected executable script permissions, got %v", fi.Mode().Perm())
		}
		if err := p.SyncHooks(); err != nil {
			t.Errorf("second SyncHooks call failed: %v", err)
		}
	}
}

func runSyncHooksCase(t *testing.T, tc syncHooksCase) {
	t.Helper()
	if tc.nilProv {
		var pNil *Provisioner
		if err := pNil.SyncHooks(); err != nil {
			t.Fatalf("expected nil error on nil provisioner, got: %v", err)
		}
		return
	}
	if tc.emptyHome {
		pEmpty := New("", "")
		if err := pEmpty.SyncHooks(); err != nil {
			t.Fatalf("expected nil error on empty homeDir, got: %v", err)
		}
		return
	}

	tmpHome := t.TempDir()
	var tmpData string
	if tc.withDataDir {
		tmpData = t.TempDir()
	}

	p := New(tmpHome, tmpData)

	if tc.blockConfig {
		writeTestFile(t, filepath.Join(tmpHome, ".gemini", "config"), "file blocker")
	}
	if tc.blockHooksDir {
		writeTestFile(t, filepath.Join(tmpHome, ".gemini", "config", "hooks"), "file blocker")
	}
	if tc.withCustomAssets {
		sourceDir := t.TempDir()
		writeTestFile(t, filepath.Join(sourceDir, "hooks.json"), `{"custom-guard":{"PreToolUse":[]}}`)
		writeTestFile(t, filepath.Join(sourceDir, "custom_guard.py"), "#!/usr/bin/env python3\nprint('ok')\n")
		p.SetHooksDir(sourceDir)
	}
	if tc.withCandidates {
		sourceDir := t.TempDir()
		origPaths := DefaultHooksSearchPaths
		defer func() { DefaultHooksSearchPaths = origPaths }()
		DefaultHooksSearchPaths = []string{sourceDir}
		_ = os.MkdirAll(filepath.Join(sourceDir, "subfolder"), 0o755)
		_ = os.Symlink(filepath.Join(sourceDir, "nonexistent"), filepath.Join(sourceDir, "broken.sh"))
		p.SetHooksDir("")
	}
	if tc.hooksDirIsFile {
		p.SetHooksDir(writeTestFile(t, filepath.Join(tmpHome, "file.txt"), "not a dir"))
	}

	if err := p.SyncHooks(); err != nil {
		t.Fatalf("SyncHooks failed: %v", err)
	}

	assertSyncHooksOutputs(t, tc, p, tmpHome, tmpData)
}

func TestSyncHooks(t *testing.T) {
	tests := []syncHooksCase{
		{name: "nil provisioner", nilProv: true},
		{name: "empty home", emptyHome: true},
		{name: "default canonical across runtimes", withDataDir: true},
		{name: "custom hooks and executable scripts", withCustomAssets: true},
		{name: "candidate search with subfolder and broken symlink", withCandidates: true},
		{name: "hooksDir points to plain file", hooksDirIsFile: true},
		{name: "config directory blocked by file", blockConfig: true},
		{name: "hooks scripts directory blocked by file", withCustomAssets: true, blockHooksDir: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSyncHooksCase(t, tc)
		})
	}
}
