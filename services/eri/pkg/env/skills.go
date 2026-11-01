package env

import (
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	symlinkSkill    = os.Symlink
	renameSkill     = os.Rename
	removeSkill     = os.Remove
	removeAllSkills = os.RemoveAll
)

// SyncSkills symlinks target-partitioned skills into:
// 1. Primary ~/.gemini/config/skills (Discord target for CLI)
// 2. Discord runtime <dataDir>/runtimes/discord/.gemini/config/skills (Discord target)
// 3. Voice runtime <dataDir>/runtimes/voice/.gemini/config/skills (Voice target)
func (p *Provisioner) SyncSkills() error {
	if p == nil || p.homeDir == "" {
		return nil
	}

	// Clean up legacy ~/.gemini/skills directory if present to eliminate redundant skill trees
	legacySkillsDir := filepath.Join(p.homeDir, ".gemini", "skills")
	if fi, err := os.Lstat(legacySkillsDir); err == nil {
		if fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			ignore(removeAllSkills(legacySkillsDir))
		}
	}

	// Clean up legacy ~/.gemini/config/plugins/superpowers directory if present on persistent volumes
	legacyPluginDir := filepath.Join(p.homeDir, ".gemini", "config", "plugins", "superpowers")
	if fi, err := os.Lstat(legacyPluginDir); err == nil {
		if fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			ignore(removeAllSkills(legacyPluginDir))
		}
	}

	// 1. Primary config (Discord compatibility)
	primarySkillDir := filepath.Join(p.homeDir, ".gemini", "config", "skills")
	p.LinkTargetSkills(primarySkillDir, TargetDiscord)

	// 2. Isolated runtimes (if dataDir configured)
	if p.dataDir != "" {
		counts := make(map[RuleTarget]int)
		for _, target := range allRuntimeTargets {
			runtimeDir := filepath.Join(p.runtimeHomeDir(target), ".gemini", "config", "skills")
			counts[target] = p.LinkTargetSkills(runtimeDir, target)
		}
		log.Printf("[Skills] Synchronized skills: %d discord skills, %d voice skills, %d ephemeral skills", counts[TargetDiscord], counts[TargetVoice], counts[TargetEphemeral])
	}

	return nil
}

func collectValidSkills(srcDir string, allowed map[string]string) {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		skillName := entry.Name()
		if _, exists := allowed[skillName]; exists {
			continue
		}
		candidateDir := filepath.Join(srcDir, skillName)
		skillMD := filepath.Join(candidateDir, "SKILL.md")
		fi, err := os.Stat(skillMD)
		if err != nil || fi.IsDir() || fi.Size() == 0 {
			// Skip torn reads (0 bytes) or missing SKILL.md
			continue
		}
		allowed[skillName] = candidateDir
	}
}

func warnRootLevelSkills(srcBase string) {
	entries, err := os.ReadDir(srcBase)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() && entry.Type()&os.ModeSymlink == 0 {
			continue
		}
		name := entry.Name()
		if name == "common" || name == "discord" || name == "voice" || name == "ephemeral" {
			continue
		}
		candidateMD := filepath.Join(srcBase, name, "SKILL.md")
		if fi, err := os.Stat(candidateMD); err == nil && !fi.IsDir() {
			log.Printf("[Skills] Warning: ignoring root-level skill %q in %s; skills must be categorized under 'common', 'discord', 'voice', or 'ephemeral'", name, srcBase)
		}
	}
}

func (p *Provisioner) sourceSkillDirs() []string {
	sourceDirs := []string{p.customSkillsDir, "/share/aerial/.agents/skills"}
	if p.agentsSkillsDir != "" && p.agentsSkillsDir != "/share/aerial/.agents/skills" {
		sourceDirs = append(sourceDirs, p.agentsSkillsDir)
	}
	return sourceDirs
}

// LinkTargetSkills links categorized skills for the specified target (discord, voice, or ephemeral) into targetDir.
// It scans {source}/common/ and {source}/{target}/ (or only {source}/ephemeral/ for TargetEphemeral), rejects flat un-nested skills with a warning,
// validates that SKILL.md is non-empty (>0 bytes), and actively prunes unauthorized symlinks.
func (p *Provisioner) LinkTargetSkills(targetDir string, target RuleTarget) int {
	if p == nil || targetDir == "" {
		return 0
	}

	allowedSkills := make(map[string]string)
	sourceDirs := p.sourceSkillDirs()

	targetSubdirs := []string{"common", string(target)}
	if target == TargetEphemeral {
		targetSubdirs = []string{string(target)}
	}

	for _, srcBase := range sourceDirs {
		if srcBase == "" {
			continue
		}

		// Log warning for un-nested root skills
		warnRootLevelSkills(srcBase)

		// Scan allowed target subdirectories: common and <target>
		for _, sub := range targetSubdirs {
			collectValidSkills(filepath.Join(srcBase, sub), allowedSkills)
		}
	}

	// Superpowers are provisioned exclusively to Discord
	if target == TargetDiscord && p.superpowersDir != "" {
		collectValidSkills(p.superpowersDir, allowedSkills)
	}

	return reconcileTargetSkills(targetDir, allowedSkills)
}

func reconcileTargetSkills(targetDir string, allowedSkills map[string]string) int {
	if fi, err := os.Lstat(targetDir); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		ignore(removeSkill(targetDir))
	}

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		log.Printf("[Skills] Warning: failed to create target skills directory %s: %v", targetDir, err)
		return 0
	}

	// Active reconciliation: prune any symlinks/files in targetDir not in allowedSkills
	if entries, err := os.ReadDir(targetDir); err == nil {
		for _, entry := range entries {
			name := entry.Name()
			if strings.HasSuffix(name, ".tmp") {
				ignore(removeSkill(filepath.Join(targetDir, name)))
				continue
			}
			if _, ok := allowedSkills[name]; !ok {
				entryPath := filepath.Join(targetDir, name)
				ignore(removeSkill(entryPath))
				log.Printf("[Skills] Pruned unauthorized skill %s from %s", name, targetDir)
			}
		}
	}

	installedCount := 0
	for skillName, srcPath := range allowedSkills {
		destPath := filepath.Join(targetDir, skillName)
		tmpDest := destPath + ".tmp"
		ignore(removeSkill(tmpDest))

		if linkSkillTarget(srcPath, destPath, tmpDest) {
			installedCount++
		}
	}

	sweepOrphanedSymlinks([]string{targetDir})
	return installedCount
}

func linkSkillTarget(srcPath, destPath, tmpDest string) bool {
	if err := symlinkSkill(srcPath, tmpDest); err != nil {
		ignore(removeSkill(destPath))
		if err := symlinkSkill(srcPath, destPath); err != nil {
			log.Printf("[Skills] Warning: failed to symlink skill %s -> %s: %v", srcPath, destPath, err)
			return false
		}
		return true
	}

	if err := renameSkill(tmpDest, destPath); err != nil {
		ignore(removeAllSkills(destPath))
		if err := renameSkill(tmpDest, destPath); err != nil {
			ignore(removeSkill(tmpDest))
			log.Printf("[Skills] Warning renaming %s -> %s: %v", tmpDest, destPath, err)
			return false
		}
	}
	return true
}

func sweepOrphanedSymlinks(targetSkillDirs []string) {
	for _, dir := range targetSkillDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			linkPath := filepath.Join(dir, entry.Name())
			fi, err := os.Lstat(linkPath)
			if err != nil {
				continue
			}
			if fi.Mode()&os.ModeSymlink != 0 {
				if _, err := os.Stat(linkPath); os.IsNotExist(err) {
					ignore(removeSkill(linkPath))
				}
			}
		}
	}
}
