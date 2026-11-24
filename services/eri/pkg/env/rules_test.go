package env

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

type compileAndDiscoverRulesCase struct {
	name               string
	target             RuleTarget
	engine             string
	prompt             string
	fault              string
	wantErr            bool
	wantPromptMD       bool
	wantPersona        bool
	wantPrefixOrder    []string
	wantExcludedPrefix string
}

func TestResolveDir(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		configured string
		candidates []string
		want       string
	}{
		{"existing configured dir wins", "cand", nil, "cand"},
		{"missing configured falls back to existing candidate", "missing", []string{"cand"}, "cand"},
		{"no existing dir returns configured default", "nonexistent", []string{"nonexistent2"}, "nonexistent"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			if err := os.Mkdir(filepath.Join(tmpDir, "cand"), 0o755); err != nil {
				t.Fatal(err)
			}
			var candidates []string
			for _, c := range tc.candidates {
				candidates = append(candidates, filepath.Join(tmpDir, c))
			}
			if got := resolveDir(filepath.Join(tmpDir, tc.configured), candidates); got != filepath.Join(tmpDir, tc.want) {
				t.Errorf("resolveDir() = %q, want %q", got, filepath.Join(tmpDir, tc.want))
			}
		})
	}
}

func TestDiscoverRuleFiles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		files     map[string]string // written under a temp root
		dir       string            // relative to the temp root; "-" passes an empty dir
		subfolder string
		wantFiles int
		wantErr   bool
	}{
		{name: "empty dir returns nil", dir: "-", subfolder: "common"},
		{
			name:      "skips nested dirs and non-md files",
			files:     map[string]string{"common/nested_dir/02.md": "nested", "common/notes.txt": "not a rule", "common/01.md": "valid rule"},
			subfolder: "common",
			wantFiles: 1,
		},
		{
			name: "ENOTDIR stat on subfolder errors", files: map[string]string{"file_blocker": "f"}, dir: "file_blocker", subfolder: "subfolder",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for rel, content := range tc.files {
				writeTestFile(t, filepath.Join(root, rel), content)
			}
			dir := filepath.Join(root, tc.dir)
			if tc.dir == "-" {
				dir = ""
			}
			files, err := discoverRuleFiles(dir, tc.subfolder, "00_")
			if (err != nil) != tc.wantErr {
				t.Fatalf("discoverRuleFiles() error = %v, wantErr %v", err, tc.wantErr)
			}
			if len(files) != tc.wantFiles {
				t.Errorf("discoverRuleFiles() returned %d files, want %d", len(files), tc.wantFiles)
			}
		})
	}
}

func setupCompileRuleBlockers(t *testing.T, aerialDir, configDir string, tc compileAndDiscoverRulesCase) {
	t.Helper()
	switch tc.fault {
	case "aerial_engine_is_file":
		path := filepath.Join(aerialDir, "discord", "claude")
		_ = os.RemoveAll(path)
		if err := os.WriteFile(path, []byte("file-not-dir"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	case "user_engine_is_file":
		path := filepath.Join(configDir, "discord", "claude")
		_ = os.RemoveAll(path)
		if err := os.WriteFile(path, []byte("file-not-dir"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	case "aerial_engine_torn":
		writeTestFile(t, filepath.Join(aerialDir, "discord", "claude", "torn.md"), "")
	case "user_engine_oversized":
		writeTestFile(t, filepath.Join(configDir, "discord", "claude", "oversized.md"), strings.Repeat("B", MaxSourceRuleFileSizeBytes+200))
	case "dir_is_file":
		path := filepath.Join(aerialDir, "common")
		_ = os.RemoveAll(path)
		if err := os.WriteFile(path, []byte("file-not-dir"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	case "user_sub_is_file":
		path := filepath.Join(configDir, "discord")
		_ = os.RemoveAll(path)
		if err := os.WriteFile(path, []byte("file-not-dir"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	case "aerial_persona_is_file":
		path := filepath.Join(aerialDir, "persona")
		_ = os.RemoveAll(path)
		if err := os.WriteFile(path, []byte("file-not-dir"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	case "config_persona_is_file":
		path := filepath.Join(configDir, "persona")
		_ = os.RemoveAll(path)
		if err := os.WriteFile(path, []byte("file-not-dir"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
}

func assertPrefixOrder(t *testing.T, wantPrefixOrder []string, rules []ruleSourceFile) {
	t.Helper()
	var actualPrefixes []string
	for _, r := range rules {
		if r.prefix != "" {
			actualPrefixes = append(actualPrefixes, r.prefix)
		}
	}
	orderIdx := 0
	for _, pfx := range actualPrefixes {
		if orderIdx < len(wantPrefixOrder) && strings.HasPrefix(pfx, wantPrefixOrder[orderIdx]) {
			orderIdx++
		}
	}
	if orderIdx != len(wantPrefixOrder) {
		t.Errorf("prefix order check failed: matched %d of %d expected prefixes (%v), got prefixes %v",
			orderIdx, len(wantPrefixOrder), wantPrefixOrder, actualPrefixes)
	}
}

func assertCompileDiscoveryOutput(t *testing.T, tc compileAndDiscoverRulesCase, rules []ruleSourceFile) {
	t.Helper()
	hasPrompt := false
	hasPersona := false
	for _, r := range rules {
		if r.filename == "99_custom_prompt.md" {
			hasPrompt = true
		}
		if strings.Contains(r.filename, "persona") {
			hasPersona = true
		}
		if tc.wantExcludedPrefix != "" && strings.HasPrefix(r.prefix+r.filename, tc.wantExcludedPrefix) {
			t.Errorf("found excluded prefix %s in compiled rule file %s", tc.wantExcludedPrefix, r.prefix+r.filename)
		}
	}
	if hasPrompt != tc.wantPromptMD {
		t.Errorf("prompt rule existence=%v, want=%v", hasPrompt, tc.wantPromptMD)
	}
	if hasPersona != tc.wantPersona {
		t.Errorf("target %s persona existence=%v, want=%v", tc.target, hasPersona, tc.wantPersona)
	}
	if len(tc.wantPrefixOrder) > 0 {
		assertPrefixOrder(t, tc.wantPrefixOrder, rules)
	}
}

func runCompileAndDiscoverRulesCase(t *testing.T, tc compileAndDiscoverRulesCase) {
	t.Helper()
	content := "Standard rule body"
	switch tc.fault {
	case "file_size_limit":
		content = strings.Repeat("A", MaxSourceRuleFileSizeBytes+100)
	case "torn_zero_byte":
		content = ""
	}

	p := newTestProvisioner(t, map[string]string{
		"common/01_identity.md":     "Aerial core identity",
		"discord/01_chat.md":        "Discord chat handling",
		"discord/claude/01_exec.md": "Claude discord instructions",
		"discord/codex/01_exec.md":  "Codex discord instructions",
		"voice/01_audio.md":         "Voice audio handling",
		"ephemeral/01_temp.md":      "Ephemeral worker rules",
	}, map[string]string{
		"common/10_persona_style.md":  "Config persona override",
		"common/20_facts.md":          content,
		"discord/30_user_chat.md":     "Config discord settings",
		"discord/claude/10_custom.md": "User claude customizations",
		"discord/codex/10_custom.md":  "User codex customizations",
	})

	setupCompileRuleBlockers(t, p.aerialRulesDir, p.configRulesDir, tc)

	var rules []ruleSourceFile
	var err error
	if tc.engine != "" {
		rules, err = p.compileTargetRules(tc.target, tc.prompt, tc.engine)
	} else {
		rules, err = p.compileTargetRules(tc.target, tc.prompt)
	}

	if (err != nil) != tc.wantErr {
		t.Fatalf("compileTargetRules() error = %v, wantErr %v", err, tc.wantErr)
	}
	if !tc.wantErr {
		assertCompileDiscoveryOutput(t, tc, rules)
	}
}

func TestCompileAndDiscoverRules(t *testing.T) {
	tests := []compileAndDiscoverRulesCase{
		{name: "discord target with custom prompt", target: TargetDiscord, prompt: "Do not compromise invariants", wantPromptMD: true, wantPersona: true},
		{name: "voice target includes persona and target rules", target: TargetVoice, wantPersona: true},
		{name: "ephemeral target compiles isolated rules and filters out persona", target: TargetEphemeral, wantPersona: false},
		{name: "rule file exceeding size limit triggers error", target: TargetDiscord, fault: "file_size_limit", wantErr: true},
		{name: "torn zero byte rule file triggers discovery error", target: TargetDiscord, fault: "torn_zero_byte", wantErr: true},
		{name: "aerial common subfolder is regular file triggers discovery error", target: TargetDiscord, fault: "dir_is_file", wantErr: true},
		{name: "user target subfolder is regular file triggers discovery error", target: TargetDiscord, fault: "user_sub_is_file", wantErr: true},
		{name: "aerial persona subfolder is regular file triggers error", target: TargetDiscord, fault: "aerial_persona_is_file", wantErr: true},
		{name: "config persona subfolder is regular file triggers error", target: TargetDiscord, fault: "config_persona_is_file", wantErr: true},
		{
			name:               "discord target with claude engine compiles engine rules in prefix order",
			target:             TargetDiscord,
			engine:             config.EngineClaude,
			wantPersona:        true,
			wantPrefixOrder:    []string{"00_aerial_common_", "10_user_common_", "20_aerial_discord_", "25_aerial_discord_claude_", "30_user_discord_", "35_user_discord_claude_"},
			wantExcludedPrefix: "25_aerial_discord_codex_",
		},
		{
			name:               "discord target with codex engine compiles codex rules and excludes claude",
			target:             TargetDiscord,
			engine:             config.EngineCodex,
			wantPersona:        true,
			wantPrefixOrder:    []string{"00_aerial_common_", "10_user_common_", "20_aerial_discord_", "25_aerial_discord_codex_", "30_user_discord_", "35_user_discord_codex_"},
			wantExcludedPrefix: "25_aerial_discord_claude_",
		},
		{
			name:               "voice target ignores engine parameter",
			target:             TargetVoice,
			engine:             config.EngineClaude,
			wantPersona:        true,
			wantExcludedPrefix: "25_aerial_discord_claude_",
		},
		{
			name:               "ephemeral target ignores engine parameter",
			target:             TargetEphemeral,
			engine:             config.EngineClaude,
			wantPersona:        false,
			wantExcludedPrefix: "25_aerial_discord_claude_",
		},
		{
			name:    "aerial engine subfolder is regular file triggers discovery error",
			target:  TargetDiscord,
			engine:  config.EngineClaude,
			fault:   "aerial_engine_is_file",
			wantErr: true,
		},
		{
			name:    "user engine subfolder is regular file triggers discovery error",
			target:  TargetDiscord,
			engine:  config.EngineClaude,
			fault:   "user_engine_is_file",
			wantErr: true,
		},
		{
			name:    "aerial engine torn zero byte file triggers error",
			target:  TargetDiscord,
			engine:  config.EngineClaude,
			fault:   "aerial_engine_torn",
			wantErr: true,
		},
		{
			name:    "user engine oversized rule file triggers error",
			target:  TargetDiscord,
			engine:  config.EngineClaude,
			fault:   "user_engine_oversized",
			wantErr: true,
		},
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

	p := newDisabledProvisioner(t, "", "")
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
	name              string
	emptyHome         bool
	zeroRules         bool
	primaryFail       bool
	configFail        bool
	voiceFail         bool
	ephemeralFail     bool
	runtimeFail       bool
	concurrent        bool
	noDataDir         bool
	staleClaudeMD     bool
	claudeDiscordFail bool
	codexDiscordFail  bool
	checkIsolation    bool
	wantErr           bool
	wantPrimaryMD     bool
}

func assertNonEmptyWithoutFrontmatter(t *testing.T, path, label string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		t.Fatalf("expected %s in %s: %v", label, path, err)
	}
	if strings.Contains(string(data), "---") {
		t.Errorf("%s should not contain YAML frontmatter: %s", label, string(data))
	}
}

func assertFileNotExist(t *testing.T, path, label string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected no %s in %s, got err=%v", label, path, err)
	}
}

func assertTargetDirectoryOrSymlink(t *testing.T, path, label string, isEphemeral bool) {
	t.Helper()
	fi, err := os.Lstat(path)
	if isEphemeral {
		if err != nil || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			t.Errorf("expected isolated %s dir: %v", label, err)
		}
		return
	}
	if err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("expected %s symlink: %v", label, err)
	}
}

func assertPrimaryRulesMD(t *testing.T, p *Provisioner, tc syncRulesCase) {
	t.Helper()
	claudeMD := filepath.Join(p.homeDir, ".claude", "CLAUDE.md")
	codexMD := filepath.Join(p.homeDir, ".codex", "AGENTS.md")
	museMD := filepath.Join(p.homeDir, ".config", "muse", "AGENTS.md")
	if !tc.wantPrimaryMD {
		assertFileNotExist(t, claudeMD, "CLAUDE.md")
		assertFileNotExist(t, codexMD, "AGENTS.md")
		assertFileNotExist(t, museMD, "muse AGENTS.md")
		return
	}
	assertNonEmptyWithoutFrontmatter(t, claudeMD, "CLAUDE.md")
	assertNonEmptyWithoutFrontmatter(t, codexMD, "AGENTS.md")
	assertNonEmptyWithoutFrontmatter(t, museMD, "muse AGENTS.md")

	if tc.checkIsolation {
		assertPrimaryRulesIsolation(t, claudeMD, codexMD, museMD)
	}
}

func assertPrimaryRulesIsolation(t *testing.T, claudeMD, codexMD, museMD string) {
	t.Helper()
	cData, err := os.ReadFile(claudeMD)
	if err != nil {
		t.Fatalf("ReadFile(claudeMD): %v", err)
	}
	if !strings.Contains(string(cData), "Claude Discord Content") || strings.Contains(string(cData), "Codex Discord Content") {
		t.Errorf("primary CLAUDE.md isolation failure: %s", string(cData))
	}
	codexData, err := os.ReadFile(codexMD)
	if err != nil {
		t.Fatalf("ReadFile(codexMD): %v", err)
	}
	if !strings.Contains(string(codexData), "Codex Discord Content") || strings.Contains(string(codexData), "Claude Discord Content") {
		t.Errorf("primary AGENTS.md isolation failure: %s", string(codexData))
	}
	museData, err := os.ReadFile(museMD)
	if err != nil {
		t.Fatalf("ReadFile(museMD): %v", err)
	}
	if !strings.Contains(string(museData), "Muse Discord Content") || strings.Contains(string(museData), "Claude Discord Content") {
		t.Errorf("primary muse AGENTS.md isolation failure: %s", string(museData))
	}
}

func assertRuntimeRulesMD(t *testing.T, p *Provisioner) {
	t.Helper()
	for _, target := range allRuntimeTargets {
		rtHome := p.runtimeHomeDir(target)
		isEph := target == TargetEphemeral
		for _, mdRel := range []string{
			filepath.Join(".claude", "CLAUDE.md"),
			filepath.Join(".codex", "AGENTS.md"),
			filepath.Join(".config", "muse", "AGENTS.md"),
		} {
			if _, err := os.Stat(filepath.Join(rtHome, mdRel)); err != nil {
				t.Errorf("expected %s in %s runtime: %v", mdRel, target, err)
			}
		}
		assertTargetDirectoryOrSymlink(t, filepath.Join(rtHome, ".claude", "projects"), "claude projects", isEph)
		assertTargetDirectoryOrSymlink(t, filepath.Join(rtHome, ".codex", "sessions"), "codex sessions", isEph)
		assertTargetDirectoryOrSymlink(t, filepath.Join(rtHome, ".local", "share", "muse", "sessions"), "muse sessions", isEph)
	}
}

func assertDiscordGeminiIsolation(t *testing.T, p *Provisioner) {
	t.Helper()
	discordGeminiRules := filepath.Join(p.runtimeHomeDir(TargetDiscord), ".gemini", "rules")
	entries, err := os.ReadDir(discordGeminiRules)
	if err != nil {
		t.Fatalf("failed to read discord .gemini/rules: %v", err)
	}
	hasAgy := false
	for _, e := range entries {
		if strings.Contains(e.Name(), "25_aerial_discord_agy_") || strings.Contains(e.Name(), "35_user_discord_agy_") {
			hasAgy = true
		}
		if strings.Contains(e.Name(), "claude") || strings.Contains(e.Name(), "codex") || strings.Contains(e.Name(), "muse") {
			t.Errorf("discord .gemini/rules leaked non-agy engine file: %s", e.Name())
		}
	}
	if !hasAgy {
		t.Errorf("discord .gemini/rules missing agy engine rules")
	}
}

func assertDiscordEngineMDIsolation(t *testing.T, p *Provisioner) {
	t.Helper()
	discordClaudeMD, err := os.ReadFile(filepath.Join(p.runtimeHomeDir(TargetDiscord), ".claude", "CLAUDE.md"))
	if err != nil {
		t.Fatalf("ReadFile(discordClaudeMD): %v", err)
	}
	if !strings.Contains(string(discordClaudeMD), "Claude Discord Content") {
		t.Errorf("discord .claude/CLAUDE.md missing Claude content: %s", string(discordClaudeMD))
	}
	if strings.Contains(string(discordClaudeMD), "Codex Discord Content") || strings.Contains(string(discordClaudeMD), "Muse Discord Content") {
		t.Errorf("discord .claude/CLAUDE.md leaked codex/muse content: %s", string(discordClaudeMD))
	}

	discordCodexMD, err := os.ReadFile(filepath.Join(p.runtimeHomeDir(TargetDiscord), ".codex", "AGENTS.md"))
	if err != nil {
		t.Fatalf("ReadFile(discordCodexMD): %v", err)
	}
	if !strings.Contains(string(discordCodexMD), "Codex Discord Content") {
		t.Errorf("discord .codex/AGENTS.md missing Codex content: %s", string(discordCodexMD))
	}
	if strings.Contains(string(discordCodexMD), "Claude Discord Content") || strings.Contains(string(discordCodexMD), "Muse Discord Content") {
		t.Errorf("discord .codex/AGENTS.md leaked claude/muse content: %s", string(discordCodexMD))
	}

	discordMuseMD, err := os.ReadFile(filepath.Join(p.runtimeHomeDir(TargetDiscord), ".config", "muse", "AGENTS.md"))
	if err != nil {
		t.Fatalf("ReadFile(discordMuseMD): %v", err)
	}
	if !strings.Contains(string(discordMuseMD), "Muse Discord Content") {
		t.Errorf("discord muse AGENTS.md missing Muse content: %s", string(discordMuseMD))
	}
	if strings.Contains(string(discordMuseMD), "Claude Discord Content") || strings.Contains(string(discordMuseMD), "Codex Discord Content") {
		t.Errorf("discord muse AGENTS.md leaked claude/codex content: %s", string(discordMuseMD))
	}
}

func assertVoiceAndEphemeralIsolation(t *testing.T, p *Provisioner) {
	t.Helper()
	voiceClaudeMD, err := os.ReadFile(filepath.Join(p.runtimeHomeDir(TargetVoice), ".claude", "CLAUDE.md"))
	if err != nil {
		t.Fatalf("ReadFile(voiceClaudeMD): %v", err)
	}
	if strings.Contains(string(voiceClaudeMD), "Claude Discord Content") || strings.Contains(string(voiceClaudeMD), "Discord content") {
		t.Errorf("voice runtime leaked discord content: %s", string(voiceClaudeMD))
	}
	if !strings.Contains(string(voiceClaudeMD), "Voice content") {
		t.Errorf("voice runtime missing voice content: %s", string(voiceClaudeMD))
	}

	ephClaudeMD, err := os.ReadFile(filepath.Join(p.runtimeHomeDir(TargetEphemeral), ".claude", "CLAUDE.md"))
	if err != nil {
		t.Fatalf("ReadFile(ephClaudeMD): %v", err)
	}
	if strings.Contains(string(ephClaudeMD), "Claude Discord Content") || strings.Contains(string(ephClaudeMD), "Discord content") {
		t.Errorf("ephemeral runtime leaked discord content: %s", string(ephClaudeMD))
	}
}

func assertRuleEngineIsolation(t *testing.T, p *Provisioner) {
	t.Helper()
	assertDiscordGeminiIsolation(t, p)
	assertDiscordEngineMDIsolation(t, p)
	assertVoiceAndEphemeralIsolation(t, p)
}

func assertSyncRulesOutput(t *testing.T, tc syncRulesCase, p *Provisioner) {
	t.Helper()
	rulesDir := filepath.Join(p.homeDir, ".gemini", "rules")
	entries, rErr := os.ReadDir(rulesDir)
	if rErr != nil || len(entries) == 0 {
		t.Fatalf("expected rules in %s: %v", rulesDir, rErr)
	}
	assertPrimaryRulesMD(t, p, tc)
	if !tc.noDataDir {
		assertRuntimeRulesMD(t, p)
		if tc.checkIsolation {
			assertRuleEngineIsolation(t, p)
		}
	}
}

func runSyncRulesCase(t *testing.T, tc syncRulesCase) {
	t.Helper()
	if tc.emptyHome {
		p := newDisabledProvisioner(t, "", "")
		if err := p.SyncRules(""); err != nil {
			t.Fatalf("expected nil error on empty home: %v", err)
		}
		return
	}

	p := newTestProvisioner(t, map[string]string{
		"common/01_rule.md":           "Common content",
		"discord/01_rule.md":          "Discord content",
		"discord/agy/01_agy.md":       "Agy Discord Content",
		"discord/claude/01_claude.md": "Claude Discord Content",
		"discord/codex/01_codex.md":   "Codex Discord Content",
		"discord/muse/01_muse.md":     "Muse Discord Content",
		"voice/01_rule.md":            "Voice content",
		"ephemeral/01_rule.md":        "Ephemeral content",
	}, nil)
	if tc.noDataDir {
		np := newDisabledProvisioner(t, t.TempDir(), "")
		np.SetAerialRulesDir(p.aerialRulesDir)
		np.SetConfigRulesDir(p.configRulesDir)
		p = np
	}
	if tc.staleClaudeMD {
		writeTestFile(t, filepath.Join(p.homeDir, ".claude", "CLAUDE.md"), "stale")
		writeTestFile(t, filepath.Join(p.homeDir, ".codex", "AGENTS.md"), "stale")
		writeTestFile(t, filepath.Join(p.homeDir, ".config", "muse", "AGENTS.md"), "stale")
	}

	if tc.zeroRules {
		p.SetAerialRulesDir(t.TempDir())
		p.SetConfigRulesDir(t.TempDir())
	}

	if tc.voiceFail {
		writeTestFile(t, filepath.Join(p.aerialRulesDir, "voice", "torn.md"), "")
	}
	if tc.ephemeralFail {
		writeTestFile(t, filepath.Join(p.aerialRulesDir, "ephemeral", "torn.md"), "")
	}
	if tc.claudeDiscordFail {
		writeTestFile(t, filepath.Join(p.aerialRulesDir, "discord", "claude", "torn.md"), "")
	}
	if tc.codexDiscordFail {
		writeTestFile(t, filepath.Join(p.aerialRulesDir, "discord", "codex", "torn.md"), "")
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
		{name: "successful sync compiles and provisions all runtimes with strict engine isolation", checkIsolation: true, wantErr: false},
		{name: "stale primary CLAUDE.md removed when runtimes exist", staleClaudeMD: true, wantErr: false},
		{name: "no dataDir writes primary CLAUDE.md", noDataDir: true, wantErr: false, wantPrimaryMD: true},
		{name: "no dataDir writes primary CLAUDE.md and AGENTS.md with per-engine content", noDataDir: true, checkIsolation: true, wantErr: false, wantPrimaryMD: true},
		{name: "zero rules aborts swap under lobotomy protection", zeroRules: true, wantErr: true},
		{name: "voice rules compilation failure returns error", voiceFail: true, wantErr: true},
		{name: "ephemeral rules compilation failure returns error", ephemeralFail: true, wantErr: true},
		{name: "claude discord rules compilation failure returns error", claudeDiscordFail: true, wantErr: true},
		{name: "codex discord rules compilation failure returns error", codexDiscordFail: true, wantErr: true},
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

func TestStripRuleFrontmatter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "with frontmatter", input: "---\nkey: val\n---\nbody text", want: "body text"},
		{name: "without frontmatter", input: "plain rule content", want: "plain rule content"},
		{name: "unterminated frontmatter", input: "---\nkey: val without closing", want: "---\nkey: val without closing"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := stripRuleFrontmatter(tc.input)
			if got != tc.want {
				t.Fatalf("stripRuleFrontmatter(%q) = %q; want %q", tc.input, got, tc.want)
			}
		})
	}
}
