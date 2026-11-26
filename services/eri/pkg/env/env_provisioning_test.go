package env

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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
	existingSymlink   bool
	existingDirFiles  bool
	destinationExists bool
	existingPlainFile bool
	emptyDataDir      bool
}

func setupPreexistingSessionSymlinks(t *testing.T, homeDir, tmpData string) (string, string) {
	t.Helper()
	cliBrain := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain")
	if err := os.MkdirAll(filepath.Dir(cliBrain), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.Symlink(filepath.Join(tmpData, "brain"), cliBrain); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	codexSessions := filepath.Join(homeDir, ".codex", "sessions")
	if err := os.MkdirAll(filepath.Dir(codexSessions), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.Symlink(filepath.Join(tmpData, "codex", "sessions"), codexSessions); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	return cliBrain, codexSessions
}

func assertEphemeralStorageIsolated(t *testing.T, p *Provisioner, tmpData string) {
	t.Helper()
	ephHome := p.runtimeHomeDir(TargetEphemeral)
	ephCliBrain, ephCodexSessions := setupPreexistingSessionSymlinks(t, ephHome, tmpData)
	p.provisionRuntimeSharedAssets(ephHome)
	for _, dir := range []struct {
		path  string
		label string
	}{
		{ephCliBrain, "brain"},
		{ephCodexSessions, "codex sessions"},
	} {
		fi, err := os.Lstat(dir.path)
		if err != nil || fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
			t.Fatalf("expected %s %s to be isolated directory, got fi=%v err=%v", TargetEphemeral, dir.label, fi, err)
		}
	}
}

func assertVoiceStorageIsolated(t *testing.T, p *Provisioner, tmpData string) {
	t.Helper()
	voiceHome := p.runtimeHomeDir(TargetVoice)
	voiceCliBrain, voiceCodexSessions := setupPreexistingSessionSymlinks(t, voiceHome, tmpData)
	p.provisionRuntimeSharedAssets(voiceHome)

	wantVoiceData := filepath.Join(tmpData, "voice-sessions")
	targets := []struct {
		symlinkPath string
		wantDst     string
		sharedDst   string
		label       string
	}{
		{voiceCliBrain, filepath.Join(wantVoiceData, ".gemini", "antigravity-cli", "brain"), filepath.Join(tmpData, "brain"), "brain"},
		{voiceCodexSessions, filepath.Join(wantVoiceData, "codex", "sessions"), filepath.Join(tmpData, "codex", "sessions"), "codex sessions"},
	}
	for _, tc := range targets {
		fi, err := os.Lstat(tc.symlinkPath)
		if err != nil || fi.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("expected %s %s to be symlink, got fi=%v err=%v", TargetVoice, tc.label, fi, err)
		}
		linkTarget, err := os.Readlink(tc.symlinkPath)
		if err != nil || filepath.Clean(linkTarget) != filepath.Clean(tc.wantDst) {
			t.Fatalf("expected %s %s to link to %s, got %s (err=%v)", TargetVoice, tc.label, tc.wantDst, linkTarget, err)
		}
		if filepath.Clean(linkTarget) == filepath.Clean(tc.sharedDst) {
			t.Fatalf("expected %s %s to be distinct from shared storage", TargetVoice, tc.label)
		}
	}
}

func assertIsolatedEphemeralStorage(t *testing.T, p *Provisioner, tmpData string) {
	t.Helper()
	origSymlink := symlinkRuntimeAsset
	symlinkRuntimeAsset = os.Symlink
	defer func() { symlinkRuntimeAsset = origSymlink }()

	assertEphemeralStorageIsolated(t, p, tmpData)
	assertVoiceStorageIsolated(t, p, tmpData)
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
	p := newDisabledProvisioner(t, tmpHome, tmpData)

	primaryGemini := filepath.Join(tmpHome, ".gemini")
	primaryCli := filepath.Join(primaryGemini, "antigravity-cli")
	writeTestFile(t, filepath.Join(primaryCli, "settings.json"), `{"apiKey":"test"}`)
	writeTestFile(t, filepath.Join(primaryCli, "antigravity-oauth-token"), "tok-123")
	writeTestFile(t, filepath.Join(primaryCli, "installation_id"), "inst-456")
	writeTestFile(t, filepath.Join(primaryGemini, "mcp_oauth_tokens.json"), `{"oauth":"data"}`)

	runtimeHome := filepath.Join(t.TempDir(), "runtime")
	runtimeGemini := filepath.Join(runtimeHome, ".gemini")

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
		if err := os.MkdirAll(filepath.Dir(targetCliBrain), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.Symlink(t.TempDir(), targetCliBrain); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
	}

	p.provisionRuntimeSharedAssets(runtimeHome)

	// Idempotent second call exercises already-valid symlinks
	p.provisionRuntimeSharedAssets(runtimeHome)

	if !tc.emptyDataDir {
		assertIsolatedEphemeralStorage(t, p, tmpData)
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

func TestLinkOrCopyAsset(t *testing.T) {
	tests := []struct {
		name       string
		files      map[string]string // written under the temp root; src is always src.txt
		dst        string
		symlinkErr error
	}{
		{name: "non-empty destination dir defeats remove, link and copy", files: map[string]string{"dst_dir/c": "c"}, dst: "dst_dir"},
		{
			name:       "symlink failure falls back to copy that fails under a file blocker",
			files:      map[string]string{"file_blocker": "f"},
			dst:        "file_blocker/target.txt",
			symlinkErr: errors.New("symlink fail"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origSymlink := symlinkRuntimeAsset
			t.Cleanup(func() { symlinkRuntimeAsset = origSymlink })
			if tc.symlinkErr != nil {
				symlinkRuntimeAsset = func(string, string) error { return tc.symlinkErr }
			}
			tmpHome := t.TempDir()
			src := writeTestFile(t, filepath.Join(tmpHome, "src.txt"), "src-data")
			for rel, content := range tc.files {
				writeTestFile(t, filepath.Join(tmpHome, rel), content)
			}
			dst := filepath.Join(tmpHome, tc.dst)
			newDisabledProvisioner(t, tmpHome, "").linkOrCopyAsset(src, dst)
			if fi, err := os.Lstat(dst); err == nil && fi.Mode()&os.ModeSymlink != 0 {
				t.Errorf("linkOrCopyAsset() linked %s despite the blocked destination", dst)
			}
		})
	}
}

type mcpProvisioningCase struct {
	name          string
	target        RuleTarget
	withPAT       bool
	servers       config.TargetMcpConfig // merged over the defaults
	voiceTools    []string               // voice.allowed_tools
	wantScheduler bool
	wantDiscord   bool
	wantGitHub    bool
	wantPresent   []string
	wantAbsent    []string
	wantURL       map[string]string
	wantHeaders   map[string]map[string]any
}

type mcpFallbacksAndErrorsCase struct {
	name          string
	target        RuleTarget
	nilCfg        bool
	noDefaults    bool
	servers       config.TargetMcpConfig
	mcpConfig     string
	fileConf      string
	blocker       string
	wantSyncErr   bool
	wantEmpty     bool
	wantScheduler bool
	wantDiscord   bool
	wantGitHub    bool
	wantPresent   []string
}

// mcpRaw builds a server map from name, raw-JSON pairs.
func mcpRaw(kv ...string) map[string]json.RawMessage {
	m := make(map[string]json.RawMessage, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = json.RawMessage(kv[i+1])
	}
	return m
}

func newMCPTestConfig(withPAT, noDefaults bool, mcpConfig string, extra config.TargetMcpConfig, voiceTools []string) *config.Config {
	return newTestConfig(func(c *config.ConfigData) {
		if !noDefaults {
			c.McpServers.Common = mcpRaw("scheduler", `{"serverUrl":"http://haro.aerial/mcp"}`, "memory", `{"serverUrl":"http://coven.aerial/mcp"}`,
				"github", `{"serverUrl":"http://github-mcp:4003/mcp"}`)
			c.McpServers.Discord = mcpRaw("discord", `{"serverUrl":"http://discord-mcp:4001/mcp"}`, "vanadis", `{"serverUrl":"http://vanadis.aerial/mcp"}`)
		}
		for _, sec := range []struct {
			dst *map[string]json.RawMessage
			src map[string]json.RawMessage
		}{
			{&c.McpServers.Common, extra.Common},
			{&c.McpServers.Discord, extra.Discord},
			{&c.McpServers.Voice, extra.Voice},
			{&c.McpServers.Ephemeral, extra.Ephemeral},
		} {
			for k, v := range sec.src {
				if *sec.dst == nil {
					*sec.dst = make(map[string]json.RawMessage)
				}
				(*sec.dst)[k] = v
			}
		}
		if withPAT {
			c.GitHubPAT = "ghp_" + "12345678901234567890"
		}
		c.MCPConfig = mcpConfig
		c.Voice.AllowedTools = voiceTools
	})
}

func TestEnsureTargetMcpConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		nilProv     bool
		emptyDir    bool
		raw         string
		wantWritten string // substring of config/mcp_config.json; "" = no file written
	}{
		{name: "nil receiver is a no-op", nilProv: true, raw: `{}`},
		{name: "empty target dir is a no-op", emptyDir: true, raw: `{}`},
		{name: "empty raw config is a no-op", raw: ``},
		{name: "empty quoted raw config is a no-op", raw: `""`},
		{name: "json string payload is unwrapped and written", raw: `"{\"mcpServers\":{\"str_srv\":{}}}"`, wantWritten: `"str_srv"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpHome := t.TempDir()
			var p *Provisioner
			if !tc.nilProv {
				p = newDisabledProvisioner(t, tmpHome, t.TempDir())
			}
			targetDir := tmpHome
			if tc.emptyDir {
				targetDir = ""
			}
			if err := p.EnsureTargetMcpConfig(targetDir, json.RawMessage(tc.raw)); err != nil {
				t.Fatalf("EnsureTargetMcpConfig() error = %v", err)
			}
			data, err := os.ReadFile(filepath.Join(tmpHome, "config", "mcp_config.json"))
			if tc.wantWritten == "" {
				if !os.IsNotExist(err) {
					t.Errorf("expected no mcp_config.json, got %q, err=%v", string(data), err)
				}
				return
			}
			if !strings.Contains(string(data), tc.wantWritten) {
				t.Errorf("mcp_config.json = %q, %v; want substring %s", string(data), err, tc.wantWritten)
			}
		})
	}
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
	for _, name := range tc.wantPresent {
		if servers[name] == nil {
			t.Errorf("expected server %q in %s config: %+v", name, tc.target, servers)
		}
	}
	for _, name := range tc.wantAbsent {
		if servers[name] != nil {
			t.Errorf("expected server %q to be absent in %s config: %+v", name, tc.target, servers)
		}
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
	for _, name := range tc.wantAbsent {
		if cServers[name] != nil {
			t.Errorf("expected server %q to be absent in claude %s config: %+v", name, tc.target, cServers)
		}
	}
}

func runMCPProvisioningCase(t *testing.T, tc mcpProvisioningCase) {
	t.Helper()
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := newDisabledProvisioner(t, tmpHome, tmpData)

	cfg := newMCPTestConfig(tc.withPAT, false, "", tc.servers, tc.voiceTools)

	raw := p.LoadTargetMCPConfig(cfg, tc.target)
	var parsed map[string]map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("failed parsing MCP config: %v", err)
	}
	assertTargetMCPServers(t, tc, parsed["mcpServers"])
	assertClaudeMCPServers(t, tc, p, cfg)
}

func runMCPFallbacksAndErrorsCase(t *testing.T, tc mcpFallbacksAndErrorsCase) {
	t.Helper()
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := newDisabledProvisioner(t, tmpHome, tmpData)

	var cfg *config.Config
	if !tc.nilCfg {
		cfg = newMCPTestConfig(false, tc.noDefaults, tc.mcpConfig, tc.servers, nil)
	}
	if tc.fileConf != "" {
		writeTestFile(t, filepath.Join(tmpData, "mcp.config.json"), tc.fileConf)
	}
	if rel, ok := strings.CutPrefix(tc.blocker, "home/"); ok {
		writeTestFile(t, filepath.Join(tmpHome, rel), "blocker")
	}
	if rel, ok := strings.CutPrefix(tc.blocker, "data/"); ok {
		writeTestFile(t, filepath.Join(tmpData, rel), "blocker")
	}
	if tc.wantSyncErr {
		if err := p.SyncMCP(context.Background(), cfg); err == nil {
			t.Fatalf("expected SyncMCP error with blocker %s", tc.blocker)
		}
		return
	}

	raw := p.LoadTargetMCPConfig(cfg, tc.target)
	var parsed map[string]map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("failed parsing MCP config: %v", err)
	}
	servers := parsed["mcpServers"]
	provCase := mcpProvisioningCase{
		target:        tc.target,
		wantScheduler: tc.wantScheduler,
		wantDiscord:   tc.wantDiscord,
		wantGitHub:    tc.wantGitHub,
	}
	assertTargetStandardServers(t, provCase, servers)
	for _, name := range tc.wantPresent {
		if servers[name] == nil {
			t.Errorf("expected server %q in %s config: %+v", name, tc.target, servers)
		}
	}
	if tc.wantEmpty && len(servers) != 0 {
		t.Errorf("expected empty servers when marshal fails on invalid raw JSON, got %v", servers)
	}
	assertClaudeMCPServers(t, provCase, p, cfg)
}

func TestMCPProvisioning(t *testing.T) {
	tests := []mcpProvisioningCase{
		{
			name: "discord target with github", target: TargetDiscord, withPAT: true, wantScheduler: true, wantDiscord: true, wantGitHub: true,
			wantURL:     map[string]string{"github": "http://github-mcp:4003/mcp"},
			wantHeaders: map[string]map[string]any{"github": nil},
		},
		{
			name: "config server headers pass through verbatim", target: TargetVoice, wantScheduler: true, wantGitHub: true,
			servers: config.TargetMcpConfig{Common: mcpRaw("ubereats",
				`{"serverUrl":"https://mcp.example.com/mcp?key=abc","headers":{"Authorization":"Bearer tok-123","X-Tenant":"home"}}`)},
			wantURL:     map[string]string{"ubereats": "https://mcp.example.com/mcp?key=abc"},
			wantHeaders: map[string]map[string]any{"ubereats": {"Authorization": "Bearer tok-123", "X-Tenant": "home"}},
		},
		{
			name: "voice target isolates scheduler and excludes discord services", target: TargetVoice, withPAT: true, wantScheduler: true,
			wantGitHub: true,
		},
		{name: "ephemeral target excludes scheduler and discord services", target: TargetEphemeral},
		{
			name: "ephemeral target includes ephemeral servers", target: TargetEphemeral,
			servers:     config.TargetMcpConfig{Ephemeral: mcpRaw("custom_eph", `{"serverUrl":"http://eph:8080"}`)},
			wantPresent: []string{"custom_eph"},
		},
		{
			name: "overlay custom mcp servers across targets", target: TargetVoice, wantScheduler: true, wantGitHub: true,
			servers: config.TargetMcpConfig{
				Common:    mcpRaw("custom_common", `{"serverUrl":"http://common:8080"}`),
				Voice:     mcpRaw("custom_voice", `{"serverUrl":"http://voice:8080"}`),
				Ephemeral: mcpRaw("custom_eph", `{"serverUrl":"http://eph:8080"}`),
			},
			wantPresent: []string{"custom_common", "custom_voice"},
		},
		{
			name: "overlay custom mcp server with raw non-obj value", target: TargetDiscord, wantScheduler: true, wantDiscord: true, wantGitHub: true,
			servers: config.TargetMcpConfig{Discord: mcpRaw("raw_srv", `"string_only_value"`)},
		},
		{
			name: "voice target with allowed tools filters and prunes servers", target: TargetVoice, withPAT: true,
			voiceTools: []string{"read_tool", "WRITE_TOOL"},
			servers: config.TargetMcpConfig{
				Common: mcpRaw(
					"keep_server", `{"serverUrl":"http://keep:8080","enabled_tools":["read_tool","other_tool"]}`,
					"prune_server", `{"serverUrl":"http://prune:8080","enabledTools":["other_tool"]}`,
					"stdio_server", `{"command":"npx","args":["-y","stdio"]}`,
					"sse_server", `{"url":"http://sse:8080","type":"sse"}`,
				),
				Voice: mcpRaw(
					"unconstrained_server", `{"serverUrl":"http://unconstrained:8080"}`,
				),
			},
			wantScheduler: true, wantGitHub: true,
			wantPresent: []string{"keep_server", "unconstrained_server"},
			wantAbsent:  []string{"prune_server", "stdio_server", "sse_server"},
		},
		{
			name: "voice target with empty allowed tools clears all servers", target: TargetVoice, withPAT: true,
			voiceTools:    []string{},
			wantScheduler: false, wantDiscord: false, wantGitHub: false,
			wantAbsent: []string{"scheduler", "memory", "github"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runMCPProvisioningCase(t, tc)
		})
	}
}

func assertVoiceAllowedPresent(t *testing.T, servers map[string]any, wantPresent []string, wantTools map[string][]string) {
	t.Helper()
	for _, name := range wantPresent {
		entry, ok := servers[name].(map[string]any)
		if !ok {
			t.Fatalf("expected server %q to be present", name)
		}
		if want, ok := wantTools[name]; ok {
			got, _ := entry["enabledTools"].([]string)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("server %q enabledTools = %v, want %v", name, entry["enabledTools"], want)
			}
		}
		if _, has := entry["enabled_tools"]; has {
			t.Errorf("server %q retained enabled_tools key", name)
		}
	}
}

func assertVoiceAllowedAbsent(t *testing.T, servers map[string]any, wantAbsent []string) {
	t.Helper()
	for _, name := range wantAbsent {
		if servers[name] != nil {
			t.Errorf("expected server %q to be absent, got %v", name, servers[name])
		}
	}
}

func TestApplyVoiceAllowedTools(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		allowedTools []string
		servers      map[string]any
		wantPresent  []string
		wantAbsent   []string
		wantTools    map[string][]string
	}{
		{
			name:         "nil allowed tools leaves servers untouched",
			allowedTools: nil,
			servers: map[string]any{
				"s1":    map[string]any{"enabledTools": []string{"t1"}},
				"stdio": map[string]any{"command": "npx"},
				"sse":   map[string]any{"url": "http://sse", "type": "sse"},
			},
			wantPresent: []string{"s1", "stdio", "sse"},
			wantTools:   map[string][]string{"s1": {"t1"}},
		},
		{
			name:         "only whitespace tool names clears all servers",
			allowedTools: []string{"  ", ""},
			servers: map[string]any{
				"s1": map[string]any{"serverUrl": "http://s1"},
			},
			wantAbsent: []string{"s1"},
		},
		{
			name:         "normalizes lowercase trims and deduplicates allowed tools",
			allowedTools: []string{"  TOOL_A ", "tool_a", "Tool_B"},
			servers: map[string]any{
				"unconstrained": map[string]any{"serverUrl": "http://u"},
				"intersected":   map[string]any{"serverUrl": "http://i", "enabled_tools": []string{"tool_b", "tool_c"}},
				"disjoint":      map[string]any{"serverUrl": "http://d", "enabledTools": []string{"tool_c"}},
			},
			wantPresent: []string{"unconstrained", "intersected"},
			wantAbsent:  []string{"disjoint"},
			wantTools: map[string][]string{
				"unconstrained": {"tool_a", "tool_b"},
				"intersected":   {"tool_b"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			applyVoiceAllowedTools(tc.servers, tc.allowedTools)
			assertVoiceAllowedPresent(t, tc.servers, tc.wantPresent, tc.wantTools)
			assertVoiceAllowedAbsent(t, tc.servers, tc.wantAbsent)
		})
	}
}

func TestMCPProvisioning_FallbacksAndErrors(t *testing.T) {
	tests := []mcpFallbacksAndErrorsCase{
		{name: "nil config yields no servers", target: TargetVoice, nilCfg: true},
		{
			name: "discord target overlays file-based mcp configuration", target: TargetDiscord,
			wantScheduler: true, wantDiscord: true, wantGitHub: true,
			fileConf:    `{"mcpServers":{"custom_override":{"serverUrl":"http://override:80"}}}`,
			wantPresent: []string{"custom_override"},
		},
		{
			name: "invalid json raw value is preserved verbatim and fails the marshal", target: TargetDiscord,
			noDefaults: true,
			servers:    config.TargetMcpConfig{Discord: mcpRaw("invalid_srv", `{not-valid-json`)}, wantEmpty: true,
		},
		{
			name: "cfg.Current().MCPConfig string fallback", target: TargetDiscord,
			wantScheduler: true, wantDiscord: true, wantGitHub: true,
			mcpConfig:   `{"mcpServers":{"fallback_srv":{"serverUrl":"http://fallback:80"}}}`,
			wantPresent: []string{"fallback_srv"},
		},
		{name: "SyncMCP error bubbles up when primary config write fails", blocker: "home/.gemini/config", wantSyncErr: true},
		{name: "SyncMCP error bubbles up when runtime config write fails", blocker: "data/runtimes/discord/.gemini/config", wantSyncErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runMCPFallbacksAndErrorsCase(t, tc)
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
	name             string
	target           RuleTarget
	superpowers      bool
	customFiles      map[string]string // extra files in the custom skills dir
	targetFiles      map[string]string // files pre-seeded in the target skills dir
	homeDirs         []string          // legacy dirs pre-created under home
	blockTarget      bool              // target skills dir is a regular file
	legacySymlinkDir bool              // target skills dir is a legacy symlink
	symlinkFail      string            // symlinkSkill fails for links with this suffix; "*" fails all
	renameFail       int               // renameSkill fails the first N calls; -1 fails all
	removeAllErr     bool
	wantLinked       bool // the target's skill set is linked
	wantAbsent       []string
	wantSymlink      []string
}

// installSkillFaults swaps the skill filesystem seams for failing fakes and restores them on cleanup.
func installSkillFaults(t *testing.T, symlinkFail string, renameFail int, removeAllErr bool) {
	t.Helper()
	origRemoveAll, origSymlink, origRename := removeAllSkills, symlinkSkill, renameSkill
	t.Cleanup(func() { removeAllSkills, symlinkSkill, renameSkill = origRemoveAll, origSymlink, origRename })
	if removeAllErr {
		removeAllSkills = func(string) error { return errors.New("simulated removeAll error") }
	}
	if symlinkFail != "" {
		symlinkSkill = func(oldname, newname string) error {
			if symlinkFail == "*" || strings.HasSuffix(newname, symlinkFail) {
				return errors.New("simulated symlink error")
			}
			return origSymlink(oldname, newname)
		}
	}
	if renameFail != 0 {
		calls := 0
		renameSkill = func(oldpath, newpath string) error {
			calls++
			if renameFail < 0 || calls <= renameFail {
				return errors.New("simulated rename error")
			}
			return origRename(oldpath, newpath)
		}
	}
}

func TestSweepOrphanedSymlinks(t *testing.T) {
	tests := []struct {
		name           string
		removeErr      error
		wantBrokenGone bool
	}{
		{name: "removes broken links, keeps valid links and files, skips missing dirs", wantBrokenGone: true},
		{name: "remove error leaves broken link in place", removeErr: errors.New("simulated sweep remove error")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origRemove := removeSkill
			t.Cleanup(func() { removeSkill = origRemove })
			if tc.removeErr != nil {
				removeSkill = func(string) error { return tc.removeErr }
			}
			tmpTarget := t.TempDir()
			validSrc := t.TempDir()
			writeTestFile(t, filepath.Join(validSrc, "file.txt"), "ok")
			validLink := filepath.Join(tmpTarget, "valid")
			brokenLink := filepath.Join(tmpTarget, "broken")
			if err := os.Symlink(validSrc, validLink); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(t.TempDir(), "nonexistent"), brokenLink); err != nil {
				t.Fatal(err)
			}
			plainFile := writeTestFile(t, filepath.Join(tmpTarget, "regular.txt"), "keep")

			sweepOrphanedSymlinks([]string{filepath.Join(tmpTarget, "missing_dir"), tmpTarget})

			if _, err := os.Lstat(brokenLink); os.IsNotExist(err) != tc.wantBrokenGone {
				t.Errorf("broken symlink removed = %v, want %v", os.IsNotExist(err), tc.wantBrokenGone)
			}
			if _, err := os.Lstat(validLink); err != nil {
				t.Errorf("valid symlink was improperly removed: %v", err)
			}
			if _, err := os.Stat(plainFile); err != nil {
				t.Errorf("plain file was improperly removed: %v", err)
			}
		})
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

func assertSkillEntries(t *testing.T, tc skillsProvisioningCase, installed int, targetSkillsDir string) {
	t.Helper()
	if tc.blockTarget {
		return
	}
	if tc.wantLinked && installed == 0 && tc.target != TargetEphemeral {
		t.Fatalf("expected installed skills > 0 for target %s", tc.target)
	}
	found := make(map[string]bool)
	entries, err := os.ReadDir(targetSkillsDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", targetSkillsDir, err)
	}
	for _, e := range entries {
		found[e.Name()] = true
	}
	for _, name := range tc.wantAbsent {
		if found[name] {
			t.Errorf("expected %s absent from target skills dir: %v", name, found)
		}
	}
	for _, name := range tc.wantSymlink {
		if fi, err := os.Lstat(filepath.Join(targetSkillsDir, name)); err != nil || fi.Mode()&os.ModeSymlink == 0 {
			t.Errorf("expected %s to be a symlink: fi=%v err=%v", name, fi, err)
		}
	}
	if tc.wantLinked {
		assertSkillTargetRules(t, tc.target, tc.superpowers, found)
	}
}

func runSkillsProvisioningCase(t *testing.T, tc skillsProvisioningCase) {
	t.Helper()
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	p := newDisabledProvisioner(t, tmpHome, tmpData)

	customDir := t.TempDir()
	writeTestFile(t, filepath.Join(customDir, "README.txt"), "readme")
	writeSkillFixture(t, customDir, "common/custom-skill/SKILL.md", "---\nname: custom-skill\n---\nCustom skill body")
	writeSkillFixture(t, customDir, "discord/discord-skill/SKILL.md", "---\nname: discord-skill\n---\nDiscord only body")
	writeSkillFixture(t, customDir, "voice/voice-skill/SKILL.md", "---\nname: voice-skill\n---\nVoice only body")
	writeSkillFixture(t, customDir, "ephemeral/eph-skill/SKILL.md", "---\nname: eph-skill\n---\nEphemeral only body")
	for rel, content := range tc.customFiles {
		writeSkillFixture(t, customDir, rel, content)
	}
	p.SetCustomSkillsDir(customDir)

	p.SetSuperpowersDir("")
	if tc.superpowers {
		spDir := t.TempDir()
		writeSkillFixture(t, spDir, "brainstorming/SKILL.md", "---\nname: brainstorming\n---\nSuperpower body")
		p.SetSuperpowersDir(spDir)
	}

	targetSkillsDir := filepath.Join(tmpHome, ".gemini", "config", "skills")
	if tc.blockTarget {
		writeTestFile(t, targetSkillsDir, "file blocker")
	}
	if tc.legacySymlinkDir {
		if err := os.MkdirAll(filepath.Dir(targetSkillsDir), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), targetSkillsDir); err != nil {
			t.Fatal(err)
		}
	}
	for rel, content := range tc.targetFiles {
		writeTestFile(t, filepath.Join(targetSkillsDir, rel), content)
	}
	for _, rel := range tc.homeDirs {
		if err := os.MkdirAll(filepath.Join(tmpHome, rel), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	installSkillFaults(t, tc.symlinkFail, tc.renameFail, tc.removeAllErr)

	if err := p.SyncSkills(); err != nil {
		t.Fatalf("SyncSkills failed: %v", err)
	}
	installed := p.LinkTargetSkills(targetSkillsDir, tc.target)
	assertSkillEntries(t, tc, installed, targetSkillsDir)
}

func TestSkillsProvisioning(t *testing.T) {
	tests := []skillsProvisioningCase{
		{name: "discord target receives common and target skills and superpowers", target: TargetDiscord, superpowers: true, wantLinked: true},
		{name: "voice target excludes superpowers and discord skills", target: TargetVoice, superpowers: true, wantLinked: true},
		{name: "ephemeral target links exclusively ephemeral skills", target: TargetEphemeral, superpowers: true, wantLinked: true},
		{
			name: "torn read skips zero-byte SKILL.md", target: TargetDiscord, wantLinked: true,
			customFiles: map[string]string{"common/torn-skill/SKILL.md": ""}, wantAbsent: []string{"torn-skill"},
		},
		{
			name: "flat skill warning on un-nested root skill", target: TargetDiscord, wantLinked: true,
			customFiles: map[string]string{"flat-root-skill/SKILL.md": "---\nname: flat\n---\nFlat body"}, wantAbsent: []string{"flat-root-skill"},
		},
		{
			name: "active reconciliation prunes unauthorized skills", target: TargetDiscord, wantLinked: true,
			targetFiles: map[string]string{"unauthorized-rogue-skill": "rogue", "stale-skill.tmp": "tmp"}, wantAbsent: []string{"unauthorized-rogue-skill"},
		},
		{name: "target skills directory is existing legacy symlink", target: TargetDiscord, legacySymlinkDir: true, wantLinked: true},
		{
			name: "cleans up legacy skills and plugins directories", target: TargetDiscord, wantLinked: true,
			homeDirs: []string{".gemini/skills", ".gemini/config/plugins/superpowers"},
		},
		{
			name: "logs warning and handles legacy remove errors", target: TargetDiscord, homeDirs: []string{".gemini/skills"}, removeAllErr: true,
			wantLinked: true,
		},
		{name: "target skills directory creation blocked by file", target: TargetDiscord, blockTarget: true},
		{name: "symlink tmp fails and triggers direct symlink fallback", target: TargetDiscord, symlinkFail: ".tmp", wantLinked: true},
		{name: "both tmp and direct symlink fail gracefully", target: TargetDiscord, symlinkFail: "*"},
		{
			name: "rename fails and triggers destination removal fallback", target: TargetDiscord, wantLinked: true,
			targetFiles: map[string]string{"custom-skill": "existing"}, renameFail: 1,
		},
		{
			name: "replacing existing skill directory does not leave tmp symlink", target: TargetDiscord, wantLinked: true,
			targetFiles: map[string]string{"custom-skill/stale.txt": "old content"},
			wantAbsent:  []string{"custom-skill.tmp"}, wantSymlink: []string{"custom-skill"},
		},
		{name: "rename total failure cleans up tmp symlink", target: TargetDiscord, renameFail: -1, wantAbsent: []string{"custom-skill.tmp"}},
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
	blockCodexDir    bool
}

func assertRuntimeHooksOutputs(t *testing.T, tc syncHooksCase, tmpHome, tmpData string) {
	t.Helper()
	for _, target := range allRuntimeTargets {
		rtHooksJSON := filepath.Join(tmpData, "runtimes", string(target), ".gemini", "config", "hooks.json")
		if _, err := os.Stat(rtHooksJSON); err != nil {
			t.Errorf("expected hooks.json in runtime %s: %v", target, err)
		}
		rtClaudeSettings := filepath.Join(tmpData, "runtimes", string(target), ".claude", "settings.json")
		if _, err := os.Stat(rtClaudeSettings); err != nil {
			t.Errorf("expected settings.json in claude runtime %s: %v", target, err)
		}
		rtCodexHooks := filepath.Join(tmpData, "runtimes", string(target), ".codex", "hooks.json")
		if _, err := os.Stat(rtCodexHooks); err != nil {
			t.Errorf("expected hooks.json in codex runtime %s: %v", target, err)
		}
		rtMuseSettings := filepath.Join(tmpData, "runtimes", string(target), ".config", "muse", "settings.json")
		if _, err := os.Stat(rtMuseSettings); err != nil {
			t.Errorf("expected settings.json in muse runtime %s: %v", target, err)
		}
	}
	primaryClaudeSettings := filepath.Join(tmpHome, ".claude", "settings.json")
	if _, err := os.Stat(primaryClaudeSettings); err != nil {
		t.Errorf("expected settings.json in primary claude home: %v", err)
	}
	primaryMuseSettings := filepath.Join(tmpHome, ".config", "muse", "settings.json")
	if _, err := os.Stat(primaryMuseSettings); err != nil {
		t.Errorf("expected settings.json in primary muse home: %v", err)
	}
	if !tc.blockCodexDir {
		primaryCodexHooks := filepath.Join(tmpHome, ".codex", "hooks.json")
		if _, err := os.Stat(primaryCodexHooks); err != nil {
			t.Errorf("expected hooks.json in primary codex home: %v", err)
		}
	}
}

func assertCustomHooksOutputs(t *testing.T, p *Provisioner, tmpHome string) {
	t.Helper()
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

func assertSyncHooksOutputs(t *testing.T, tc syncHooksCase, p *Provisioner, tmpHome, tmpData string) {
	t.Helper()
	if tc.withDataDir {
		assertRuntimeHooksOutputs(t, tc, tmpHome, tmpData)
	}
	if tc.withCustomAssets && !tc.blockHooksDir && !tc.blockConfig {
		assertCustomHooksOutputs(t, p, tmpHome)
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
		pEmpty := newDisabledProvisioner(t, "", "")
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

	p := newDisabledProvisioner(t, tmpHome, tmpData)

	if tc.blockConfig {
		writeTestFile(t, filepath.Join(tmpHome, ".gemini", "config"), "file blocker")
	}
	if tc.blockHooksDir {
		writeTestFile(t, filepath.Join(tmpHome, ".gemini", "config", "hooks"), "file blocker")
	}
	if tc.blockCodexDir {
		writeTestFile(t, filepath.Join(tmpHome, ".codex"), "file blocker")
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
		if err := os.MkdirAll(filepath.Join(sourceDir, "subfolder"), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.Symlink(filepath.Join(sourceDir, "nonexistent"), filepath.Join(sourceDir, "broken.sh")); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
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
		{name: "codex directory blocked by file", blockCodexDir: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSyncHooksCase(t, tc)
		})
	}
}

// authEngineLayout is the per-engine credential layout shared by the codex and muse seeding contract.
type authEngineLayout struct {
	dataSub string // canonical dir under dataDir
	homeSub string // canonical dir under home (no dataDir) and the primary link dir
}

var authEngineLayouts = map[string]authEngineLayout{
	config.EngineCodex: {"codex", ".codex"},
	config.EngineMuse:  {"muse", filepath.Join(".config", "muse")},
}

func setEngineAuthSource(p *Provisioner, engine, path string) {
	if p == nil {
		return
	}
	if engine == config.EngineCodex {
		p.SetCodexAuthSourcePath(path)
	} else {
		p.SetMuseAuthSourcePath(path)
	}
}

func provisionEngineAuth(p *Provisioner, engine string) error {
	if engine == config.EngineCodex {
		return p.provisionCodexAuthState()
	}
	return p.provisionMuseAuthState()
}

type authStateCase struct {
	name         string
	engine       string
	nilProv      bool
	emptyHome    bool
	noDataDir    bool
	source       string
	existingAuth string
	existingSHA  string // "match_source" = sha256 of source
	blockDir     bool
	wantErr      bool
	wantAuth     string
	wantSHA      string // "match_source" = sha256 of source
}

func resolveAuthSHA(sha, source string) string {
	if sha == "match_source" {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(source)))
	}
	return sha
}

func assertAuthStateOutput(t *testing.T, tc authStateCase, layout authEngineLayout, canonicalDir, tmpHome, tmpData string, p *Provisioner) {
	t.Helper()
	data, rErr := os.ReadFile(filepath.Join(canonicalDir, "auth.json"))
	if rErr != nil || string(data) != tc.wantAuth {
		t.Fatalf("auth.json = %q, %v; want %q", string(data), rErr, tc.wantAuth)
	}
	wantSHA := resolveAuthSHA(tc.wantSHA, tc.source)
	shaData, sErr := os.ReadFile(filepath.Join(canonicalDir, ".auth.seed-sha256"))
	if sErr != nil || strings.TrimSpace(string(shaData)) != wantSHA {
		t.Fatalf(".auth.seed-sha256 = %q, %v; want %q", string(shaData), sErr, wantSHA)
	}
	if tmpData == "" {
		return
	}
	links := []string{filepath.Join(tmpHome, layout.homeSub, "auth.json")}
	for _, target := range allRuntimeTargets {
		links = append(links, filepath.Join(p.runtimeHomeDir(target), layout.homeSub, "auth.json"))
	}
	for _, link := range links {
		if fi, lErr := os.Lstat(link); lErr != nil || fi.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("expected %s auth symlink at %s: %v", tc.engine, link, lErr)
		}
	}
}

func runAuthStateCase(t *testing.T, tc authStateCase) {
	t.Helper()
	layout := authEngineLayouts[tc.engine]
	if tc.nilProv || tc.emptyHome {
		var p *Provisioner
		if tc.emptyHome {
			p = newDisabledProvisioner(t, "", "")
		}
		if err := provisionEngineAuth(p, tc.engine); err != nil {
			t.Fatalf("provision %s auth: expected nil error, got %v", tc.engine, err)
		}
		return
	}

	tmpHome := t.TempDir()
	var tmpData string
	canonicalDir := filepath.Join(tmpHome, layout.homeSub)
	if !tc.noDataDir {
		tmpData = t.TempDir()
		canonicalDir = filepath.Join(tmpData, layout.dataSub)
	}
	p := newDisabledProvisioner(t, tmpHome, tmpData)

	if tc.blockDir {
		writeTestFile(t, canonicalDir, "blocker")
	}
	if tc.existingAuth != "" {
		writeTestFile(t, filepath.Join(canonicalDir, "auth.json"), tc.existingAuth)
	}
	if tc.existingSHA != "" {
		writeTestFile(t, filepath.Join(canonicalDir, ".auth.seed-sha256"), resolveAuthSHA(tc.existingSHA, tc.source))
	}
	setEngineAuthSource(p, tc.engine, "")
	if tc.source != "" {
		setEngineAuthSource(p, tc.engine, writeTestFile(t, filepath.Join(tmpHome, "source-auth.json"), tc.source))
	}

	err := provisionEngineAuth(p, tc.engine)
	if (err != nil) != tc.wantErr {
		t.Fatalf("provision %s auth error = %v, wantErr %v", tc.engine, err, tc.wantErr)
	}
	if !tc.wantErr && tc.wantAuth != "" {
		assertAuthStateOutput(t, tc, layout, canonicalDir, tmpHome, tmpData, p)
	}
}

func TestProvisionAuthState(t *testing.T) {
	tests := []authStateCase{
		{name: "codex nil provisioner", engine: config.EngineCodex, nilProv: true},
		{name: "codex empty home", engine: config.EngineCodex, emptyHome: true},
		{
			name: "codex missing auth seeds from source", engine: config.EngineCodex, source: `{"tokens":"seed1"}`, wantAuth: `{"tokens":"seed1"}`,
			wantSHA: "match_source",
		},
		{
			name: "codex preserves live rotated refresh tokens when seed sha matches", engine: config.EngineCodex, source: `{"tokens":"seed1"}`,
			existingAuth: `{"tokens":"refreshed"}`, existingSHA: "match_source", wantAuth: `{"tokens":"refreshed"}`, wantSHA: "match_source",
		},
		{
			name: "codex overwrites auth when source seed sha differs", engine: config.EngineCodex, source: `{"tokens":"seed2"}`,
			existingAuth: `{"tokens":"old"}`, existingSHA: "old-sha", wantAuth: `{"tokens":"seed2"}`, wantSHA: "match_source",
		},
		{
			name: "codex seeds into home .codex when dataDir is empty", engine: config.EngineCodex, noDataDir: true,
			source: `{"tokens":"home-seed"}`, wantAuth: `{"tokens":"home-seed"}`, wantSHA: "match_source",
		},
		{name: "codex empty source content is skipped", engine: config.EngineCodex, source: "   "},
		{
			name: "codex mkdir failure returns error when dir is file blocker", engine: config.EngineCodex, source: `{"tokens":"seed"}`,
			blockDir: true, wantErr: true,
		},
		{name: "muse nil provisioner", engine: config.EngineMuse, nilProv: true},
		{name: "muse empty home", engine: config.EngineMuse, emptyHome: true},
		{
			name: "muse missing auth seeds from source", engine: config.EngineMuse, source: `{"jwt":"muse1"}`, wantAuth: `{"jwt":"muse1"}`,
			wantSHA: "match_source",
		},
		{
			name: "muse overwrites auth when source seed sha differs", engine: config.EngineMuse, source: `{"jwt":"muse2"}`,
			existingAuth: `{"jwt":"stale"}`, existingSHA: "stale-digest", wantAuth: `{"jwt":"muse2"}`, wantSHA: "match_source",
		},
		{
			name: "muse seeds into home config when dataDir is empty", engine: config.EngineMuse, noDataDir: true,
			source: `{"jwt":"home-token"}`, wantAuth: `{"jwt":"home-token"}`, wantSHA: "match_source",
		},
		{
			name: "muse mkdir failure returns error when dir is file blocker", engine: config.EngineMuse, source: `{"jwt":"token"}`, blockDir: true,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runAuthStateCase(t, tc)
		})
	}
}

func setupPruneTestFiles(baseDir string, setupFiles []string) {
	for _, spec := range setupFiles {
		if rel, ok := strings.CutPrefix(spec, "file:"); ok {
			full := filepath.Join(baseDir, rel)
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				panic(err)
			}
			if err := os.WriteFile(full, []byte("data"), 0o644); err != nil {
				panic(err)
			}
			continue
		}
		if rel, ok := strings.CutPrefix(spec, "dir:"); ok {
			full := filepath.Join(baseDir, rel)
			if err := os.MkdirAll(full, 0o755); err != nil {
				panic(err)
			}
			continue
		}
		if rest, ok := strings.CutPrefix(spec, "symlink:"); ok {
			parts := strings.Split(rest, "->")
			target := filepath.Join(baseDir, parts[0])
			link := filepath.Join(baseDir, parts[1])
			if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
				panic(err)
			}
			if err := os.Symlink(target, link); err != nil {
				panic(err)
			}
		}
	}
}

func TestPruneAgyLogDirs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		nilProv       bool
		withDataDir   bool
		setupFiles    []string
		wantRemoved   []string
		wantPreserved []string
	}{
		{name: "nil provisioner is safe", nilProv: true},
		{
			name:        "removes cli-*.log and preserves other entries across primary and runtimes",
			withDataDir: true,
			setupFiles: []string{
				"file:primary/.gemini/antigravity-cli/log/cli-12345.log",
				"file:primary/.gemini/antigravity-cli/log/cli-debug.log",
				"file:primary/.gemini/antigravity-cli/log/other.log",
				"file:primary/.gemini/antigravity-cli/log/cli-info.txt",
				"dir:primary/.gemini/antigravity-cli/log/cli-sub.log",
				"file:runtimes/discord/.gemini/antigravity-cli/log/cli-999.log",
				"file:runtimes/discord/.gemini/antigravity-cli/log/keep.log",
				"file:runtimes/voice/.gemini/antigravity-cli/log/cli-voice.log",
				"file:runtimes/ephemeral/.gemini/antigravity-cli/log/cli-eph.log",
			},
			wantRemoved: []string{
				"primary/.gemini/antigravity-cli/log/cli-12345.log",
				"primary/.gemini/antigravity-cli/log/cli-debug.log",
				"runtimes/discord/.gemini/antigravity-cli/log/cli-999.log",
				"runtimes/voice/.gemini/antigravity-cli/log/cli-voice.log",
				"runtimes/ephemeral/.gemini/antigravity-cli/log/cli-eph.log",
			},
			wantPreserved: []string{
				"primary/.gemini/antigravity-cli/log/other.log",
				"primary/.gemini/antigravity-cli/log/cli-info.txt",
				"primary/.gemini/antigravity-cli/log/cli-sub.log",
				"runtimes/discord/.gemini/antigravity-cli/log/keep.log",
			},
		},
		{
			name:        "preserves symlink log file",
			withDataDir: false,
			setupFiles: []string{
				"file:primary/target.txt",
				"symlink:primary/target.txt->primary/.gemini/antigravity-cli/log/cli-symlink.log",
			},
			wantPreserved: []string{
				"primary/.gemini/antigravity-cli/log/cli-symlink.log",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilProv {
				var p *Provisioner
				p.pruneAgyLogDirs()
				return
			}

			baseDir := t.TempDir()
			primaryHome := filepath.Join(baseDir, "primary")
			var dataDir string
			if tc.withDataDir {
				dataDir = baseDir
			}

			setupPruneTestFiles(baseDir, tc.setupFiles)

			p := newDisabledProvisioner(t, primaryHome, dataDir)
			p.pruneAgyLogDirs()

			for _, rel := range tc.wantRemoved {
				full := filepath.Join(baseDir, rel)
				if _, err := os.Lstat(full); !os.IsNotExist(err) {
					t.Errorf("expected %s to be removed, but it exists", rel)
				}
			}
			for _, rel := range tc.wantPreserved {
				full := filepath.Join(baseDir, rel)
				if _, err := os.Lstat(full); err != nil {
					t.Errorf("expected %s to be preserved, got err: %v", rel, err)
				}
			}
		})
	}
}
