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

type compileRulesCase struct {
	name               string
	target             RuleTarget
	engine             string
	prompt             string
	wantPromptMD       bool
	wantPersona        bool
	wantPrefixOrder    []string
	wantExcludedPrefix string
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

func assertCompileDiscoveryOutput(t *testing.T, tc compileRulesCase, rules []ruleSourceFile) {
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

func runCompileRulesCase(t *testing.T, tc compileRulesCase) {
	t.Helper()
	p := newTestProvisioner(t, map[string]string{
		"common/01_identity.md":     "Aerial core identity",
		"discord/01_chat.md":        "Discord chat handling",
		"discord/claude/01_exec.md": "Claude discord instructions",
		"discord/codex/01_exec.md":  "Codex discord instructions",
		"voice/01_audio.md":         "Voice audio handling",
		"ephemeral/01_temp.md":      "Ephemeral worker rules",
	}, map[string]string{
		"common/10_persona_style.md":  "Config persona override",
		"common/20_facts.md":          "Standard rule body",
		"discord/30_user_chat.md":     "Config discord settings",
		"discord/claude/10_custom.md": "User claude customizations",
		"discord/codex/10_custom.md":  "User codex customizations",
	})

	var rules []ruleSourceFile
	var err error
	if tc.engine != "" {
		rules, err = p.compileTargetRules(tc.target, tc.prompt, tc.engine)
	} else {
		rules, err = p.compileTargetRules(tc.target, tc.prompt)
	}

	if err != nil {
		t.Fatalf("compileTargetRules() error = %v", err)
	}
	assertCompileDiscoveryOutput(t, tc, rules)
}

func TestCompileAndDiscoverRules(t *testing.T) {
	tests := []compileRulesCase{
		{
			name:         "discord target with custom prompt",
			target:       TargetDiscord,
			prompt:       "Do not compromise invariants",
			wantPromptMD: true,
			wantPersona:  true,
		},
		{
			name:        "voice target includes persona and target rules",
			target:      TargetVoice,
			wantPersona: true,
		},
		{
			name:        "ephemeral target compiles isolated rules and filters out persona",
			target:      TargetEphemeral,
			wantPersona: false,
		},
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCompileRulesCase(t, tc)
		})
	}
}

type compileRulesFaultCase struct {
	name             string
	target           RuleTarget
	engine           string
	aerialBlockerRel string
	configBlockerRel string
	fileBody         string
	oversized        bool
}

func runCompileRulesFaultCase(t *testing.T, tc compileRulesFaultCase) {
	t.Helper()
	p := newTestProvisioner(t, map[string]string{
		"common/01_identity.md":     "Aerial core identity",
		"discord/01_chat.md":        "Discord chat handling",
		"discord/claude/01_exec.md": "Claude discord instructions",
		"discord/codex/01_exec.md":  "Codex discord instructions",
		"voice/01_audio.md":         "Voice audio handling",
		"ephemeral/01_temp.md":      "Ephemeral worker rules",
	}, map[string]string{
		"common/10_persona_style.md":  "Config persona override",
		"common/20_facts.md":          "Standard rule body",
		"discord/30_user_chat.md":     "Config discord settings",
		"discord/claude/10_custom.md": "User claude customizations",
		"discord/codex/10_custom.md":  "User codex customizations",
	})

	content := tc.fileBody
	if tc.oversized {
		content = strings.Repeat("B", MaxSourceRuleFileSizeBytes+200)
	}

	if tc.aerialBlockerRel != "" {
		path := filepath.Join(p.aerialRulesDir, tc.aerialBlockerRel)
		_ = os.RemoveAll(path)
		writeTestFile(t, path, content)
	}
	if tc.configBlockerRel != "" {
		path := filepath.Join(p.configRulesDir, tc.configBlockerRel)
		_ = os.RemoveAll(path)
		writeTestFile(t, path, content)
	}

	var err error
	if tc.engine != "" {
		_, err = p.compileTargetRules(tc.target, "", tc.engine)
	} else {
		_, err = p.compileTargetRules(tc.target, "")
	}

	if err == nil {
		t.Fatalf("compileTargetRules() error = nil, wantErr true")
	}
}

func TestCompileAndDiscoverRules_DiscoveryFaults(t *testing.T) {
	tests := []compileRulesFaultCase{
		{
			name:             "rule file exceeding size limit triggers error",
			target:           TargetDiscord,
			configBlockerRel: "common/20_facts.md",
			oversized:        true,
		},
		{
			name:             "torn zero byte rule file triggers discovery error",
			target:           TargetDiscord,
			configBlockerRel: "common/20_facts.md",
			fileBody:         "",
		},
		{
			name:             "aerial common subfolder is regular file triggers discovery error",
			target:           TargetDiscord,
			aerialBlockerRel: "common",
			fileBody:         "file-not-dir",
		},
		{
			name:             "user target subfolder is regular file triggers discovery error",
			target:           TargetDiscord,
			configBlockerRel: "discord",
			fileBody:         "file-not-dir",
		},
		{
			name:             "aerial persona subfolder is regular file triggers error",
			target:           TargetDiscord,
			aerialBlockerRel: "persona",
			fileBody:         "file-not-dir",
		},
		{
			name:             "config persona subfolder is regular file triggers error",
			target:           TargetDiscord,
			configBlockerRel: "persona",
			fileBody:         "file-not-dir",
		},
		{
			name:             "aerial engine subfolder is regular file triggers discovery error",
			target:           TargetDiscord,
			engine:           config.EngineClaude,
			aerialBlockerRel: "discord/claude",
			fileBody:         "file-not-dir",
		},
		{
			name:             "user engine subfolder is regular file triggers discovery error",
			target:           TargetDiscord,
			engine:           config.EngineClaude,
			configBlockerRel: "discord/claude",
			fileBody:         "file-not-dir",
		},
		{
			name:             "aerial engine torn zero byte file triggers error",
			target:           TargetDiscord,
			engine:           config.EngineClaude,
			aerialBlockerRel: "discord/claude/torn.md",
			fileBody:         "",
		},
		{
			name:             "user engine oversized rule file triggers error",
			target:           TargetDiscord,
			engine:           config.EngineClaude,
			configBlockerRel: "discord/claude/oversized.md",
			oversized:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCompileRulesFaultCase(t, tc)
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

type syncRulesProvisionCase struct {
	name           string
	emptyHome      bool
	noDataDir      bool
	staleClaudeMD  bool
	concurrent     bool
	checkIsolation bool
	wantPrimaryMD  bool
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

func assertPrimaryRulesMD(t *testing.T, p *Provisioner, wantPrimaryMD, checkIsolation bool) {
	t.Helper()
	claudeMD := filepath.Join(p.homeDir, ".claude", "CLAUDE.md")
	codexMD := filepath.Join(p.homeDir, ".codex", "AGENTS.md")
	museMD := filepath.Join(p.homeDir, ".config", "muse", "AGENTS.md")
	if !wantPrimaryMD {
		assertFileNotExist(t, claudeMD, "CLAUDE.md")
		assertFileNotExist(t, codexMD, "AGENTS.md")
		assertFileNotExist(t, museMD, "muse AGENTS.md")
		return
	}
	assertNonEmptyWithoutFrontmatter(t, claudeMD, "CLAUDE.md")
	assertNonEmptyWithoutFrontmatter(t, codexMD, "AGENTS.md")
	assertNonEmptyWithoutFrontmatter(t, museMD, "muse AGENTS.md")

	if checkIsolation {
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
		isEphemeral := target == TargetEphemeral
		for _, mdRel := range []string{
			filepath.Join(".claude", "CLAUDE.md"),
			filepath.Join(".codex", "AGENTS.md"),
			filepath.Join(".config", "muse", "AGENTS.md"),
		} {
			if _, err := os.Stat(filepath.Join(rtHome, mdRel)); err != nil {
				t.Errorf("expected %s in %s runtime: %v", mdRel, target, err)
			}
		}
		assertTargetDirectoryOrSymlink(t, filepath.Join(rtHome, ".claude", "projects"), "claude projects", isEphemeral)
		assertTargetDirectoryOrSymlink(t, filepath.Join(rtHome, ".codex", "sessions"), "codex sessions", isEphemeral)
		assertTargetDirectoryOrSymlink(t, filepath.Join(rtHome, ".local", "share", "muse", "sessions"), "muse sessions", isEphemeral)

		if target == TargetVoice && p.dataDir != "" {
			voiceData := filepath.Join(p.dataDir, "voice-sessions")
			for _, check := range []struct {
				linkPath string
				wantDst  string
			}{
				{filepath.Join(rtHome, ".claude", "projects"), filepath.Join(voiceData, "claude", "projects")},
				{filepath.Join(rtHome, ".codex", "sessions"), filepath.Join(voiceData, "codex", "sessions")},
				{filepath.Join(rtHome, ".local", "share", "muse", "sessions"), filepath.Join(voiceData, "muse", "sessions")},
				{filepath.Join(rtHome, ".gemini", "antigravity-cli", "brain"), filepath.Join(voiceData, ".gemini", "antigravity-cli", "brain")},
				{filepath.Join(rtHome, ".gemini", "antigravity-cli", "conversations"), filepath.Join(voiceData, ".gemini", "antigravity-cli", "conversations")},
			} {
				dst, err := os.Readlink(check.linkPath)
				if err != nil {
					t.Errorf("expected %s to be symlink: %v", check.linkPath, err)
					continue
				}
				if filepath.Clean(dst) != filepath.Clean(check.wantDst) {
					t.Errorf("symlink %s points to %s; want %s", check.linkPath, dst, check.wantDst)
				}
			}
		}
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

func assertSyncRulesProvisionOutput(t *testing.T, tc syncRulesProvisionCase, p *Provisioner) {
	t.Helper()
	rulesDir := filepath.Join(p.homeDir, ".gemini", "rules")
	entries, rErr := os.ReadDir(rulesDir)
	if rErr != nil || len(entries) == 0 {
		t.Fatalf("expected rules in %s: %v", rulesDir, rErr)
	}
	assertPrimaryRulesMD(t, p, tc.wantPrimaryMD, tc.checkIsolation)
	if !tc.noDataDir {
		assertRuntimeRulesMD(t, p)
		if tc.checkIsolation {
			assertRuleEngineIsolation(t, p)
		}
	}
}

func runSyncRulesProvisionCase(t *testing.T, tc syncRulesProvisionCase) {
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
	if err != nil {
		t.Fatalf("SyncRules() error = %v", err)
	}

	assertSyncRulesProvisionOutput(t, tc, p)
}

func TestSyncRules_ProvisioningAndIsolation(t *testing.T) {
	tests := []syncRulesProvisionCase{
		{
			name:      "empty home returns nil",
			emptyHome: true,
		},
		{
			name: "successful sync compiles and provisions all runtimes",
		},
		{
			name:           "successful sync compiles and provisions all runtimes with strict engine isolation",
			checkIsolation: true,
		},
		{
			name:          "stale primary CLAUDE.md removed when runtimes exist",
			staleClaudeMD: true,
		},
		{
			name:          "no dataDir writes primary CLAUDE.md",
			noDataDir:     true,
			wantPrimaryMD: true,
		},
		{
			name:           "no dataDir writes primary CLAUDE.md and AGENTS.md with per-engine content",
			noDataDir:      true,
			checkIsolation: true,
			wantPrimaryMD:  true,
		},
		{
			name:       "concurrent sync execution safe under systemRulesMu",
			concurrent: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSyncRulesProvisionCase(t, tc)
		})
	}
}

type syncRulesFaultCase struct {
	name           string
	zeroRules      bool
	tornAerialRel  string
	homeBlockerRel string
	dataBlockerRel string
	wantErr        bool
}

func runSyncRulesFaultCase(t *testing.T, tc syncRulesFaultCase) {
	t.Helper()
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

	if tc.zeroRules {
		p.SetAerialRulesDir(t.TempDir())
		p.SetConfigRulesDir(t.TempDir())
	}
	if tc.tornAerialRel != "" {
		writeTestFile(t, filepath.Join(p.aerialRulesDir, tc.tornAerialRel), "")
	}
	if tc.homeBlockerRel != "" {
		writeTestFile(t, filepath.Join(p.homeDir, tc.homeBlockerRel), "file blocker")
	}
	if tc.dataBlockerRel != "" {
		writeTestFile(t, filepath.Join(p.dataDir, tc.dataBlockerRel), "file blocker")
	}

	err := p.SyncRules("Custom System Prompt")
	if (err != nil) != tc.wantErr {
		t.Fatalf("SyncRules() error = %v, wantErr %v", err, tc.wantErr)
	}

	if !tc.wantErr {
		rulesDir := filepath.Join(p.homeDir, ".gemini", "rules")
		entries, rErr := os.ReadDir(rulesDir)
		if rErr != nil || len(entries) == 0 {
			t.Fatalf("expected rules in %s: %v", rulesDir, rErr)
		}
		assertPrimaryRulesMD(t, p, false, false)
		assertRuntimeRulesMD(t, p)
	}
}

func TestSyncRules_CompilationAndWriteFaults(t *testing.T) {
	tests := []syncRulesFaultCase{
		{
			name:      "zero rules aborts swap under lobotomy protection",
			zeroRules: true,
			wantErr:   true,
		},
		{
			name:          "voice rules compilation failure returns error",
			tornAerialRel: "voice/torn.md",
			wantErr:       true,
		},
		{
			name:          "ephemeral rules compilation failure returns error",
			tornAerialRel: "ephemeral/torn.md",
			wantErr:       true,
		},
		{
			name:          "claude discord rules compilation failure returns error",
			tornAerialRel: "discord/claude/torn.md",
			wantErr:       true,
		},
		{
			name:          "codex discord rules compilation failure returns error",
			tornAerialRel: "discord/codex/torn.md",
			wantErr:       true,
		},
		{
			name:           "primary rules write failure returns error",
			homeBlockerRel: ".gemini/rules",
			wantErr:        true,
		},
		{
			name:           "config rules write failure logs warning but succeeds",
			homeBlockerRel: ".gemini/config/rules",
			wantErr:        false,
		},
		{
			name:           "runtime rules write failure logs warning and continues",
			dataBlockerRel: "runtimes/discord/.gemini/rules",
			wantErr:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSyncRulesFaultCase(t, tc)
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
