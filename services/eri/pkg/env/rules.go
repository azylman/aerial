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
	TargetDiscord   RuleTarget = "discord"
	TargetVoice     RuleTarget = "voice"
	TargetEphemeral RuleTarget = "ephemeral"
)

var (
	allRuntimeTargets = []RuleTarget{TargetDiscord, TargetVoice, TargetEphemeral}

	systemRulesMu       sync.Mutex
	renameRulesDir      = os.Rename
	symlinkRuntimeAsset = os.Symlink

	// DefaultAerialRulesPaths specifies standard search roots for aerial system rules in priority order.
	DefaultAerialRulesPaths = []string{
		"/share/aerial/rules", "./rules", "../rules", "/app/rules", "../../rules", "../../../rules",
	}

	// DefaultConfigRulesPaths specifies standard search roots for aerial-config user rules in priority order.
	DefaultConfigRulesPaths = []string{
		"/share/aerial-config/rules", "./aerial-config/rules", "../aerial-config/rules", "../../aerial-config/rules", "../../../aerial-config/rules",
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

	// 3. Persona rules from aerial and aerial-config:
	// Loaded for conversational runtimes (Discord, Voice), omitted for Ephemeral to maintain lean execution.
	var aerialPersona, configPersona []ruleSourceFile
	if target != TargetEphemeral {
		aerialPersona, err = discoverRuleFiles(aerialDir, "persona", "05_aerial_persona_")
		if err != nil {
			return nil, err
		}
		configPersona, err = discoverRuleFiles(configDir, "persona", "15_user_persona_")
		if err != nil {
			return nil, err
		}
	} else {
		// Transitional backward compatibility: if legacy persona file still resides in common, omit it for ephemeral
		var filteredConfigCommon []ruleSourceFile
		for _, r := range configCommon {
			if !strings.Contains(r.filename, "persona") {
				filteredConfigCommon = append(filteredConfigCommon, r)
			}
		}
		configCommon = filteredConfigCommon
	}

	// 4. Target rules from aerial: prefix 20_aerial_<target>_
	aerialTarget, err := discoverRuleFiles(aerialDir, string(target), fmt.Sprintf("20_aerial_%s_", target))
	if err != nil {
		return nil, err
	}

	// 5. Target rules from aerial-config: prefix 30_user_<target>_
	configTarget, err := discoverRuleFiles(configDir, string(target), fmt.Sprintf("30_user_%s_", target))
	if err != nil {
		return nil, err
	}

	var allRules []ruleSourceFile
	for _, set := range [][]ruleSourceFile{aerialCommon, aerialPersona, configCommon, configPersona, aerialTarget, configTarget} {
		allRules = append(allRules, set...)
	}

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
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return err
	}

	randSuffix := uuid.New().String()[:8]
	stagingDir := filepath.Join(parentDir, fmt.Sprintf(".rules.tmp.%s", randSuffix))
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return err
	}
	defer func() {
		ignore(os.RemoveAll(stagingDir))
	}()

	for _, r := range rules {
		destName := r.prefix + r.filename
		destPath := filepath.Join(stagingDir, destName)
		if err := os.WriteFile(destPath, []byte(r.content), 0o644); err != nil {
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
		ignore(os.RemoveAll(backupDir))
	}
	return nil
}

func (p *Provisioner) syncRulesToDir(rules []ruleSourceFile, geminiDir string) error {
	if err := os.MkdirAll(geminiDir, 0o755); err != nil {
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

func (p *Provisioner) linkOrCopyAsset(src, dst string) {
	if _, err := os.Stat(src); err == nil {
		if rErr := os.Remove(dst); rErr != nil && !os.IsNotExist(rErr) {
			log.Printf("[Env] Warning: failed to remove %s before link: %v", dst, rErr)
		}
		if sErr := symlinkRuntimeAsset(src, dst); sErr != nil {
			data, rErr := os.ReadFile(src)
			if rErr != nil {
				log.Printf("[Env] Warning: failed to read %s for runtime copy: %v", src, rErr)
			} else if wErr := p.writeAtomic(dst, string(data)); wErr != nil {
				log.Printf("[Env] Warning: failed to copy %s to runtime: %v", src, wErr)
			}
		}
	}
}

func (p *Provisioner) provisionRuntimeSharedAssets(runtimeHome string) {
	runtimeGemini := filepath.Join(runtimeHome, ".gemini")
	primaryGemini := filepath.Join(p.homeDir, ".gemini")

	ignore(os.MkdirAll(filepath.Join(runtimeGemini, "antigravity-cli"), 0o755))
	ignore(os.MkdirAll(filepath.Join(runtimeGemini, "config"), 0o755))

	primarySettings := filepath.Join(primaryGemini, "antigravity-cli", "settings.json")
	targetSettings := filepath.Join(runtimeGemini, "antigravity-cli", "settings.json")
	if _, err := os.Stat(primarySettings); err == nil {
		if data, rErr := os.ReadFile(primarySettings); rErr == nil {
			if err := p.writeAtomic(targetSettings, string(data)); err != nil {
				log.Printf("[Env] Warning: failed to write target settings: %v", err)
			}
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
		p.linkOrCopyAsset(
			filepath.Join(primaryGemini, "antigravity-cli", fname),
			filepath.Join(runtimeGemini, "antigravity-cli", fname),
		)
	}
	p.linkOrCopyAsset(
		filepath.Join(primaryGemini, "mcp_oauth_tokens.json"),
		filepath.Join(runtimeGemini, "mcp_oauth_tokens.json"),
	)

	// Unify brain transcripts and conversations across runtimes into shared storage
	canonicalBrain := filepath.Join(p.dataDir, "brain")
	canonicalConversations := filepath.Join(p.dataDir, "conversations")
	if p.dataDir == "" {
		canonicalBrain = filepath.Join(primaryGemini, "antigravity-cli", "brain")
		canonicalConversations = filepath.Join(primaryGemini, "antigravity-cli", "conversations")
	}
	ignore(os.MkdirAll(canonicalBrain, 0o755))
	ignore(os.MkdirAll(canonicalConversations, 0o755))
	p.linkSharedSessionStorage(runtimeGemini, canonicalBrain, canonicalConversations)
}

func migrateExistingSessionDir(targetDir, canonicalDir string) {
	if entries, rErr := os.ReadDir(targetDir); rErr == nil {
		for _, entry := range entries {
			src := filepath.Join(targetDir, entry.Name())
			dst := filepath.Join(canonicalDir, entry.Name())
			if _, dErr := os.Stat(dst); os.IsNotExist(dErr) {
				ignore(os.Rename(src, dst))
			}
		}
	}
	ignore(os.RemoveAll(targetDir))
}

func prepareSharedTargetDir(targetDir, cleanCanonical string) bool {
	fi, err := os.Lstat(targetDir)
	if err != nil {
		return false
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		if linkTarget, rErr := os.Readlink(targetDir); rErr == nil && filepath.Clean(linkTarget) == cleanCanonical {
			return true
		}
		ignore(os.Remove(targetDir))
		return false
	}
	if fi.IsDir() {
		migrateExistingSessionDir(targetDir, cleanCanonical)
		return false
	}
	ignore(os.Remove(targetDir))
	return false
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

		if prepareSharedTargetDir(targetDir, cleanCanonical) {
			continue
		}

		// Ensure parent directory exists (e.g. targetGemini/antigravity-cli)
		ignore(os.MkdirAll(filepath.Dir(targetDir), 0o755))

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

	// 3. Compile Ephemeral target rules
	ephemeralRules, eErr := p.compileTargetRules(TargetEphemeral, "")
	if eErr != nil {
		log.Printf("[Env] ERROR: Failed to compile Ephemeral rules: %v; retaining existing rules", eErr)
		return eErr
	}

	// 4. Sync to primary homeDir (.gemini) with Discord target as default
	primaryGemini := filepath.Join(p.homeDir, ".gemini")
	if err := p.syncRulesToDir(discordRules, primaryGemini); err != nil {
		return fmt.Errorf("failed to sync rules to primary home: %w", err)
	}
	log.Printf("[Env] Configured modular rules in %s (target: discord)", primaryGemini)
	if p.dataDir != "" {
		canonicalBrain := filepath.Join(p.dataDir, "brain")
		canonicalConversations := filepath.Join(p.dataDir, "conversations")
		ignore(os.MkdirAll(canonicalBrain, 0o755))
		ignore(os.MkdirAll(canonicalConversations, 0o755))
		p.linkSharedSessionStorage(primaryGemini, canonicalBrain, canonicalConversations)
	}

	// 5. If dataDir is configured, provision isolated runtimes for discord, voice, and ephemeral
	if p.dataDir != "" {
		compiled := map[RuleTarget][]ruleSourceFile{
			TargetDiscord:   discordRules,
			TargetVoice:     voiceRules,
			TargetEphemeral: ephemeralRules,
		}
		for _, target := range allRuntimeTargets {
			home := p.runtimeHomeDir(target)
			gemini := filepath.Join(home, ".gemini")
			if err := p.syncRulesToDir(compiled[target], gemini); err != nil {
				log.Printf("[Env] Warning: failed to sync rules to %s runtime: %v", target, err)
			} else {
				p.provisionRuntimeSharedAssets(home)
			}
		}
	}

	return nil
}
