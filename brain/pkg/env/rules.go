package env

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const (
	// MaxRuleFileSizeBytes is the maximum allowed byte length for compiled runtime rules
	// in ~/.gemini/rules/ to prevent silent Antigravity prompt truncation (23 KB hard ceiling).
	MaxRuleFileSizeBytes = 23 * 1024

	// MaxSourceRuleFileSizeBytes is the maximum allowed byte length for source rule files,
	// leaving buffer for YAML frontmatter and header wrapping.
	MaxSourceRuleFileSizeBytes = 23040
)

// RuleTarget specifies the destination execution environment for compiled rules.
type RuleTarget string

const (
	TargetDiscord RuleTarget = "discord"
	TargetVoice   RuleTarget = "voice"
)

var (
	systemRulesMu       sync.Mutex
	renameRulesDir      = os.Rename
	symlinkRuntimeAsset = os.Symlink

	// DefaultAerialRulesPaths specifies standard search roots for aerial system rules in priority order.
	DefaultAerialRulesPaths = []string{
		"/share/aerial/rules",
		"./rules",
		"../rules",
		"/app/rules",
		"../../rules",
		"../../../rules",
	}

	// DefaultConfigRulesPaths specifies standard search roots for aerial-config user rules in priority order.
	DefaultConfigRulesPaths = []string{
		"/share/aerial-config/rules",
		"./aerial-config/rules",
		"../aerial-config/rules",
		"../../aerial-config/rules",
		"../../../aerial-config/rules",
	}

	// DefaultAgentInstructionsSearchPaths specifies standard locations for legacy AGENTS.md (deprecated).
	DefaultAgentInstructionsSearchPaths = []string{
		"/share/aerial-config/AGENTS.local.md",
		"/share/aerial-config/AGENTS.md",
		"/share/aerial/AGENTS.md",
		"/app/AGENTS.md",
		"/data/AGENTS.md",
		"/data/.AGENTS.md.lkgc",
		"./AGENTS.local.md",
		"./AGENTS.md",
	}

	// DefaultSystemInstructionsSearchPaths specifies standard locations for legacy GEMINI.md (deprecated).
	DefaultSystemInstructionsSearchPaths = []string{
		"/share/aerial-config/GEMINI.local.md",
		"/share/aerial-config/GEMINI.md",
		"/share/aerial/GEMINI.md",
		"/app/GEMINI.md",
		"/data/GEMINI.md",
		"/data/.GEMINI.md.lkgc",
		"./GEMINI.local.md",
		"./GEMINI.md",
	}
)

type ruleSourceFile struct {
	path     string
	filename string
	content  string
	prefix   string
}

// resolveDir checks the configured path and fallback candidates for an existing directory.
func resolveDir(configured string, candidates []string) string {
	if configured != "" {
		if fi, err := os.Stat(configured); err == nil && fi.IsDir() {
			return configured
		}
	}
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && fi.IsDir() {
			return c
		}
	}
	return configured
}

// discoverRuleFiles collects .md files from a subfolder (e.g. "common" or target) in lexical order.
func discoverRuleFiles(dir, subfolder, prefix string) ([]ruleSourceFile, error) {
	if dir == "" {
		return nil, nil
	}
	targetDir := filepath.Join(dir, subfolder)
	fi, statErr := os.Stat(targetDir)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read rules directory %s: %w", targetDir, statErr)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("failed to read rules directory %s: not a directory", targetDir)
	}
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read rules directory %s: %w", targetDir, err)
	}

	var files []ruleSourceFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		fullPath := filepath.Join(targetDir, e.Name())
		data, readErr := os.ReadFile(fullPath)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read rule file %s: %w", fullPath, readErr)
		}
		trimmed := bytes.TrimSpace(data)
		if len(trimmed) == 0 {
			// Torn read during git sync
			return nil, fmt.Errorf("source rule file %s is empty (possible git sync torn read)", fullPath)
		}
		if len(data) > MaxSourceRuleFileSizeBytes {
			return nil, fmt.Errorf("rule file %s exceeds %d bytes (%d bytes)", fullPath, MaxSourceRuleFileSizeBytes, len(data))
		}

		content := string(data)
		// Ensure standard YAML frontmatter trigger: always_on is present
		if !strings.HasPrefix(content, "---") {
			desc := strings.TrimSuffix(e.Name(), ".md")
			content = fmt.Sprintf("---\ndescription: %s\ntrigger: always_on\n---\n\n%s\n", desc, content)
		}

		files = append(files, ruleSourceFile{
			path:     fullPath,
			filename: e.Name(),
			content:  content,
			prefix:   prefix,
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].filename < files[j].filename
	})
	return files, nil
}

func (p *Provisioner) compileTargetRules(target RuleTarget, customPrompt string) ([]ruleSourceFile, error) {
	aerialDir := resolveDir(p.aerialRulesDir, DefaultAerialRulesPaths)
	configDir := resolveDir(p.configRulesDir, DefaultConfigRulesPaths)

	// 1. Common rules from aerial: prefix 00_aerial_common_
	aerialCommon, err := discoverRuleFiles(aerialDir, "common", "00_aerial_common_")
	if err != nil {
		return nil, err
	}

	// 2. Common rules from aerial-config: prefix 10_user_common_
	configCommon, err := discoverRuleFiles(configDir, "common", "10_user_common_")
	if err != nil {
		return nil, err
	}

	// Lobotomy Guardrail: if 0 common rule files discovered across both, abort!
	if len(aerialCommon)+len(configCommon) == 0 {
		return nil, fmt.Errorf("rules compilation aborted: common rules directory has 0 files (potential mount or git sync failure)")
	}

	// 3. Target rules from aerial: prefix 20_aerial_<target>_
	aerialTarget, err := discoverRuleFiles(aerialDir, string(target), fmt.Sprintf("20_aerial_%s_", target))
	if err != nil {
		return nil, err
	}

	// 4. Target rules from aerial-config: prefix 30_user_<target>_
	configTarget, err := discoverRuleFiles(configDir, string(target), fmt.Sprintf("30_user_%s_", target))
	if err != nil {
		return nil, err
	}

	var allRules []ruleSourceFile
	allRules = append(allRules, aerialCommon...)
	allRules = append(allRules, configCommon...)
	allRules = append(allRules, aerialTarget...)
	allRules = append(allRules, configTarget...)

	if target == TargetDiscord && strings.TrimSpace(customPrompt) != "" {
		allRules = append(allRules, ruleSourceFile{
			filename: "99_custom_prompt.md",
			prefix:   "",
			content:  fmt.Sprintf("---\ndescription: Environment Prompt Override\ntrigger: always_on\n---\n\n# Environment Prompt Override\n\n%s\n", strings.TrimSpace(customPrompt)),
		})
	}

	return allRules, nil
}

func (p *Provisioner) atomicSwapRulesDir(rules []ruleSourceFile, targetDir string) error {
	parentDir := filepath.Dir(targetDir)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return err
	}

	randSuffix := uuid.New().String()[:8]
	stagingDir := filepath.Join(parentDir, fmt.Sprintf(".rules.tmp.%s", randSuffix))
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return err
	}
	defer func() {
		if rmErr := os.RemoveAll(stagingDir); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("[Env] Warning removing staging rules dir %s: %v", stagingDir, rmErr)
		}
	}()

	for _, r := range rules {
		destName := r.prefix + r.filename
		destPath := filepath.Join(stagingDir, destName)
		if err := os.WriteFile(destPath, []byte(r.content), 0644); err != nil {
			return fmt.Errorf("failed to write rule file %s in staging: %w", destName, err)
		}
	}

	// Verify staging has files
	entries, err := os.ReadDir(stagingDir)
	if err != nil || len(entries) == 0 {
		return fmt.Errorf("staging rules directory is empty, aborting swap")
	}

	// Atomic directory swap
	backupDir := filepath.Join(parentDir, fmt.Sprintf(".rules.backup.%s", randSuffix))
	hasActive := false
	if fi, statErr := os.Stat(targetDir); statErr == nil {
		if !fi.IsDir() {
			return fmt.Errorf("target rules path %s is not a directory", targetDir)
		}
		hasActive = true
		if renErr := renameRulesDir(targetDir, backupDir); renErr != nil {
			return fmt.Errorf("failed to backup existing rules dir: %w", renErr)
		}
	}

	if renErr := renameRulesDir(stagingDir, targetDir); renErr != nil {
		if hasActive {
			if rbErr := renameRulesDir(backupDir, targetDir); rbErr != nil {
				log.Printf("[Env] ERROR: failed to restore backup rules dir %s -> %s: %v", backupDir, targetDir, rbErr)
			}
		}
		return fmt.Errorf("failed to move staging rules to target dir: %w", renErr)
	}

	if hasActive {
		if rmErr := os.RemoveAll(backupDir); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("[Env] Warning removing backup rules dir %s: %v", backupDir, rmErr)
		}
	}
	return nil
}

func (p *Provisioner) syncRulesToDir(rules []ruleSourceFile, geminiDir string) error {
	if err := os.MkdirAll(geminiDir, 0755); err != nil {
		return err
	}

	primaryRulesDir := filepath.Join(geminiDir, "rules")
	configRulesDir := filepath.Join(geminiDir, "config", "rules")

	// Atomically swap primary rules directory
	if err := p.atomicSwapRulesDir(rules, primaryRulesDir); err != nil {
		return fmt.Errorf("failed to swap primary rules: %w", err)
	}

	// Atomically swap config rules directory (for compatibility)
	if err := p.atomicSwapRulesDir(rules, configRulesDir); err != nil {
		log.Printf("[Env] Warning: failed to swap config rules: %v", err)
	}

	return nil
}

func (p *Provisioner) provisionRuntimeSharedAssets(runtimeHome string) {
	runtimeGemini := filepath.Join(runtimeHome, ".gemini")
	primaryGemini := filepath.Join(p.homeDir, ".gemini")

	if err := os.MkdirAll(filepath.Join(runtimeGemini, "antigravity-cli"), 0755); err != nil {
		log.Printf("[Env] Warning: failed to mkdir antigravity-cli: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(runtimeGemini, "config"), 0755); err != nil {
		log.Printf("[Env] Warning: failed to mkdir config: %v", err)
	}

	primarySettings := filepath.Join(primaryGemini, "antigravity-cli", "settings.json")
	targetSettings := filepath.Join(runtimeGemini, "antigravity-cli", "settings.json")
	if _, err := os.Stat(primarySettings); err == nil {
		if data, rErr := os.ReadFile(primarySettings); rErr == nil {
			if err := p.writeAtomic(targetSettings, string(data)); err != nil {
				log.Printf("[Env] Warning: failed to write target settings: %v", err)
			}
		}
	}

	primarySkills := filepath.Join(primaryGemini, "config", "skills")
	targetSkills := filepath.Join(runtimeGemini, "config", "skills")
	if _, err := os.Stat(primarySkills); err == nil {
		if rErr := os.Remove(targetSkills); rErr != nil && !os.IsNotExist(rErr) {
			log.Printf("[Env] Warning: failed to remove target skills symlink: %v", rErr)
		}
		if err := os.Symlink(primarySkills, targetSkills); err != nil {
			log.Printf("[Env] Warning: failed to symlink primary skills: %v", err)
		}
	}

	legacySkills := filepath.Join(primaryGemini, "skills")
	targetLegacySkills := filepath.Join(runtimeGemini, "skills")
	if _, err := os.Stat(legacySkills); err == nil {
		if rErr := os.Remove(targetLegacySkills); rErr != nil && !os.IsNotExist(rErr) {
			log.Printf("[Env] Warning: failed to remove target legacy skills symlink: %v", rErr)
		}
		if err := os.Symlink(legacySkills, targetLegacySkills); err != nil {
			log.Printf("[Env] Warning: failed to symlink legacy skills: %v", err)
		}
	}

	// Share authentication credentials and CLI state across isolated runtimes
	sharedCliFiles := []string{
		"antigravity-oauth-token",
		"mcp_oauth_tokens.json",
		"installation_id",
		"jetski_state.pbtxt",
	}
	for _, fname := range sharedCliFiles {
		primaryFile := filepath.Join(primaryGemini, "antigravity-cli", fname)
		targetFile := filepath.Join(runtimeGemini, "antigravity-cli", fname)
		if _, err := os.Stat(primaryFile); err == nil {
			if rErr := os.Remove(targetFile); rErr != nil && !os.IsNotExist(rErr) {
				log.Printf("[Env] Warning: failed to remove existing %s in runtime before link: %v", fname, rErr)
			}
			if sErr := symlinkRuntimeAsset(primaryFile, targetFile); sErr != nil {
				data, rErr := os.ReadFile(primaryFile)
				if rErr != nil {
					log.Printf("[Env] Warning: failed to read %s for runtime copy: %v", fname, rErr)
				} else if wErr := p.writeAtomic(targetFile, string(data)); wErr != nil {
					log.Printf("[Env] Warning: failed to copy %s to runtime: %v", fname, wErr)
				}
			}
		}
	}

	primaryMcpTokens := filepath.Join(primaryGemini, "mcp_oauth_tokens.json")
	targetMcpTokens := filepath.Join(runtimeGemini, "mcp_oauth_tokens.json")
	if _, err := os.Stat(primaryMcpTokens); err == nil {
		if rErr := os.Remove(targetMcpTokens); rErr != nil && !os.IsNotExist(rErr) {
			log.Printf("[Env] Warning: failed to remove target mcp_oauth_tokens.json before link: %v", rErr)
		}
		if sErr := symlinkRuntimeAsset(primaryMcpTokens, targetMcpTokens); sErr != nil {
			data, rErr := os.ReadFile(primaryMcpTokens)
			if rErr != nil {
				log.Printf("[Env] Warning: failed to read primary mcp_oauth_tokens.json for runtime copy: %v", rErr)
			} else if wErr := p.writeAtomic(targetMcpTokens, string(data)); wErr != nil {
				log.Printf("[Env] Warning: failed to copy mcp_oauth_tokens.json to runtime: %v", wErr)
			}
		}
	}

	// Unify brain transcripts and conversations across runtimes into shared storage
	canonicalBrain := filepath.Join(p.dataDir, "brain")
	canonicalConversations := filepath.Join(p.dataDir, "conversations")
	if p.dataDir == "" {
		canonicalBrain = filepath.Join(primaryGemini, "antigravity-cli", "brain")
		canonicalConversations = filepath.Join(primaryGemini, "antigravity-cli", "conversations")
	}
	if err := os.MkdirAll(canonicalBrain, 0755); err != nil {
		log.Printf("[Env] Warning: failed to ensure canonical brain directory %s: %v", canonicalBrain, err)
	}
	if err := os.MkdirAll(canonicalConversations, 0755); err != nil {
		log.Printf("[Env] Warning: failed to ensure canonical conversations directory %s: %v", canonicalConversations, err)
	}
	p.linkSharedSessionStorage(runtimeGemini, canonicalBrain, canonicalConversations)
}

func (p *Provisioner) linkSharedSessionStorage(targetGemini, canonicalBrain, canonicalConversations string) {
	shared := []struct {
		targetRel string
		canonical string
	}{
		{targetRel: filepath.Join("antigravity-cli", "brain"), canonical: canonicalBrain},
		{targetRel: "brain", canonical: canonicalBrain},
		{targetRel: filepath.Join("antigravity-cli", "conversations"), canonical: canonicalConversations},
		{targetRel: "conversations", canonical: canonicalConversations},
	}

	for _, item := range shared {
		targetDir := filepath.Join(targetGemini, item.targetRel)
		cleanTarget := filepath.Clean(targetDir)
		cleanCanonical := filepath.Clean(item.canonical)
		if cleanTarget == cleanCanonical {
			continue
		}

		if fi, err := os.Lstat(targetDir); err == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				if linkTarget, rErr := os.Readlink(targetDir); rErr == nil && filepath.Clean(linkTarget) == cleanCanonical {
					continue
				}
				if rErr := os.Remove(targetDir); rErr != nil && !os.IsNotExist(rErr) {
					log.Printf("[Env] Warning: failed to remove outdated symlink %s: %v", targetDir, rErr)
				}
			} else if fi.IsDir() {
				// Migrate any existing files or sessions to canonical directory
				if entries, rErr := os.ReadDir(targetDir); rErr == nil {
					for _, entry := range entries {
						src := filepath.Join(targetDir, entry.Name())
						dst := filepath.Join(item.canonical, entry.Name())
						if _, dErr := os.Stat(dst); os.IsNotExist(dErr) {
							if mErr := os.Rename(src, dst); mErr != nil {
								log.Printf("[Env] Warning: failed to migrate %s to canonical dir %s: %v", src, dst, mErr)
							}
						}
					}
				}
				if rmErr := os.RemoveAll(targetDir); rmErr != nil {
					log.Printf("[Env] Warning: failed to remove existing dir %s before symlink: %v", targetDir, rmErr)
				}
			} else {
				if rmErr := os.Remove(targetDir); rmErr != nil && !os.IsNotExist(rmErr) {
					log.Printf("[Env] Warning: failed to remove file %s before symlink: %v", targetDir, rmErr)
				}
			}
		}

		// Ensure parent directory exists (e.g. targetGemini/antigravity-cli)
		if pErr := os.MkdirAll(filepath.Dir(targetDir), 0755); pErr != nil {
			log.Printf("[Env] Warning: failed to create parent dir for %s: %v", targetDir, pErr)
		}

		if sErr := symlinkRuntimeAsset(item.canonical, targetDir); sErr != nil {
			log.Printf("[Env] Warning: failed to link shared storage %s -> %s: %v", targetDir, item.canonical, sErr)
		}
	}
}

// SyncRules compiles the modular rules into target runtime directories and primary home.
func (p *Provisioner) SyncRules(customPrompt string) error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	systemRulesMu.Lock()
	defer systemRulesMu.Unlock()

	// 1. Compile Discord target rules
	discordRules, dErr := p.compileTargetRules(TargetDiscord, customPrompt)
	if dErr != nil {
		log.Printf("[Env] ERROR: Failed to compile Discord rules: %v; retaining existing rules", dErr)
		return dErr
	}

	// 2. Compile Voice target rules
	voiceRules, vErr := p.compileTargetRules(TargetVoice, "")
	if vErr != nil {
		log.Printf("[Env] ERROR: Failed to compile Voice rules: %v; retaining existing rules", vErr)
		return vErr
	}

	// 3. Sync to primary homeDir (.gemini) with Discord target as default
	primaryGemini := filepath.Join(p.homeDir, ".gemini")
	if err := p.syncRulesToDir(discordRules, primaryGemini); err != nil {
		return fmt.Errorf("failed to sync rules to primary home: %w", err)
	}
	log.Printf("[Env] Configured modular rules in %s (target: discord)", primaryGemini)
	if p.dataDir != "" {
		canonicalBrain := filepath.Join(p.dataDir, "brain")
		canonicalConversations := filepath.Join(p.dataDir, "conversations")
		if err := os.MkdirAll(canonicalBrain, 0755); err != nil {
			log.Printf("[Env] Warning: failed to ensure canonical brain directory %s: %v", canonicalBrain, err)
		}
		if err := os.MkdirAll(canonicalConversations, 0755); err != nil {
			log.Printf("[Env] Warning: failed to ensure canonical conversations directory %s: %v", canonicalConversations, err)
		}
		p.linkSharedSessionStorage(primaryGemini, canonicalBrain, canonicalConversations)
	}

	// 4. If dataDir is configured, provision isolated runtimes for discord and voice
	if p.dataDir != "" {
		runtimesRoot := filepath.Join(p.dataDir, "runtimes")

		discordHome := filepath.Join(runtimesRoot, string(TargetDiscord))
		discordGemini := filepath.Join(discordHome, ".gemini")
		if err := p.syncRulesToDir(discordRules, discordGemini); err != nil {
			log.Printf("[Env] Warning: failed to sync rules to discord runtime: %v", err)
		} else {
			p.provisionRuntimeSharedAssets(discordHome)
		}

		voiceHome := filepath.Join(runtimesRoot, string(TargetVoice))
		voiceGemini := filepath.Join(voiceHome, ".gemini")
		if err := p.syncRulesToDir(voiceRules, voiceGemini); err != nil {
			log.Printf("[Env] Warning: failed to sync rules to voice runtime: %v", err)
		} else {
			p.provisionRuntimeSharedAssets(voiceHome)
		}
	}

	return nil
}
