package env

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	skillsMu        sync.Mutex
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
			if rmErr := removeAllSkills(legacySkillsDir); rmErr != nil {
				log.Printf("[Skills] Warning removing legacy skills directory %s: %v", legacySkillsDir, rmErr)
			}
		}
	}

	// Clean up legacy ~/.gemini/config/plugins/superpowers directory if present on persistent volumes
	legacyPluginDir := filepath.Join(p.homeDir, ".gemini", "config", "plugins", "superpowers")
	if fi, err := os.Lstat(legacyPluginDir); err == nil {
		if fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			if rmErr := removeAllSkills(legacyPluginDir); rmErr != nil {
				log.Printf("[Skills] Warning removing legacy plugin directory %s: %v", legacyPluginDir, rmErr)
			}
		}
	}

	// 1. Primary config (Discord compatibility)
	primarySkillDir := filepath.Join(p.homeDir, ".gemini", "config", "skills")
	discordCount := p.LinkTargetSkills(primarySkillDir, TargetDiscord)

	// 2. Isolated runtimes (if dataDir configured)
	if p.dataDir != "" {
		discordRuntimeDir := filepath.Join(p.dataDir, "runtimes", "discord", ".gemini", "config", "skills")
		p.LinkTargetSkills(discordRuntimeDir, TargetDiscord)

		voiceRuntimeDir := filepath.Join(p.dataDir, "runtimes", "voice", ".gemini", "config", "skills")
		voiceCount := p.LinkTargetSkills(voiceRuntimeDir, TargetVoice)
		log.Printf("[Skills] Synchronized skills: %d discord skills, %d voice skills", discordCount, voiceCount)
	}

	return nil
}

// LinkTargetSkills links categorized skills for the specified target (discord or voice) into targetDir.
// It scans {source}/common/ and {source}/{target}/, rejects flat un-nested skills with a warning,
// validates that SKILL.md is non-empty (>0 bytes), and actively prunes unauthorized symlinks.
func (p *Provisioner) LinkTargetSkills(targetDir string, target RuleTarget) int {
	if p == nil || targetDir == "" {
		return 0
	}

	allowedSkills := make(map[string]string)

	// Source directories in priority order:
	// 1. Custom user skills
	// 2. Built-in skills
	sourceDirs := []string{
		p.customSkillsDir,
		"/share/aerial/.agents/skills",
		p.agentsSkillsDir,
	}

	targetSubdirs := []string{"common", string(target)}

	for _, srcBase := range sourceDirs {
		if srcBase == "" {
			continue
		}

		// Log warning for un-nested root skills
		if entries, err := os.ReadDir(srcBase); err == nil {
			for _, entry := range entries {
				if !entry.IsDir() && entry.Type()&os.ModeSymlink == 0 {
					continue
				}
				name := entry.Name()
				if name == "common" || name == "discord" || name == "voice" {
					continue
				}
				candidateMD := filepath.Join(srcBase, name, "SKILL.md")
				if fi, err := os.Stat(candidateMD); err == nil && !fi.IsDir() {
					log.Printf("[Skills] Warning: ignoring root-level skill %q in %s; skills must be categorized under 'common', 'discord', or 'voice'", name, srcBase)
				}
			}
		}

		// Scan allowed target subdirectories: common and <target>
		for _, sub := range targetSubdirs {
			subPath := filepath.Join(srcBase, sub)
			entries, err := os.ReadDir(subPath)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				skillName := entry.Name()
				if _, exists := allowedSkills[skillName]; exists {
					continue
				}
				candidateDir := filepath.Join(subPath, skillName)
				skillMD := filepath.Join(candidateDir, "SKILL.md")
				fi, err := os.Stat(skillMD)
				if err != nil || fi.IsDir() || fi.Size() == 0 {
					// Skip torn reads (0 bytes) or missing SKILL.md
					continue
				}
				allowedSkills[skillName] = candidateDir
			}
		}
	}

	// Superpowers are provisioned exclusively to Discord
	if target == TargetDiscord && p.superpowersDir != "" {
		if entries, err := os.ReadDir(p.superpowersDir); err == nil {
			for _, entry := range entries {
				skillName := entry.Name()
				if _, exists := allowedSkills[skillName]; exists {
					continue
				}
				candidateDir := filepath.Join(p.superpowersDir, skillName)
				skillMD := filepath.Join(candidateDir, "SKILL.md")
				fi, err := os.Stat(skillMD)
				if err != nil || fi.IsDir() || fi.Size() == 0 {
					continue
				}
				allowedSkills[skillName] = candidateDir
			}
		}
	}

	return reconcileTargetSkills(targetDir, allowedSkills)
}

func reconcileTargetSkills(targetDir string, allowedSkills map[string]string) int {
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		log.Printf("[Skills] Warning: failed to create target skills directory %s: %v", targetDir, err)
		return 0
	}

	// Active reconciliation: prune any symlinks/files in targetDir not in allowedSkills
	if entries, err := os.ReadDir(targetDir); err == nil {
		for _, entry := range entries {
			name := entry.Name()
			if strings.HasSuffix(name, ".tmp") {
				if rmErr := removeSkill(filepath.Join(targetDir, name)); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
					log.Printf("[Skills] Warning removing stale temp skill %s: %v", filepath.Join(targetDir, name), rmErr)
				}
				continue
			}
			if _, ok := allowedSkills[name]; !ok {
				entryPath := filepath.Join(targetDir, name)
				if err := removeSkill(entryPath); err != nil && !os.IsNotExist(err) {
					log.Printf("[Skills] Warning: failed removing unauthorized skill %s: %v", entryPath, err)
				} else {
					log.Printf("[Skills] Pruned unauthorized skill %s from %s", name, targetDir)
				}
			}
		}
	}

	installedCount := 0
	for skillName, srcPath := range allowedSkills {
		destPath := filepath.Join(targetDir, skillName)
		tmpDest := destPath + ".tmp"
		if rmErr := removeSkill(tmpDest); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			log.Printf("[Skills] Warning removing stale temp skill %s: %v", tmpDest, rmErr)
		}

		if err := symlinkSkill(srcPath, tmpDest); err != nil {
			// Fallback: remove dest and symlink directly
			if err := removeSkill(destPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf("[Skills] Warning removing existing destination skill %s: %v", destPath, err)
			}
			if err := symlinkSkill(srcPath, destPath); err != nil {
				log.Printf("[Skills] Warning: failed to symlink skill %s -> %s: %v", srcPath, destPath, err)
			} else {
				installedCount++
			}
		} else {
			if err := renameSkill(tmpDest, destPath); err != nil {
				if err := removeSkill(destPath); err != nil && !errors.Is(err, os.ErrNotExist) {
					log.Printf("[Skills] Warning removing destination skill %s prior to rename: %v", destPath, err)
				}
				if err := renameSkill(tmpDest, destPath); err != nil {
					log.Printf("[Skills] Warning renaming %s -> %s: %v", tmpDest, destPath, err)
				} else {
					installedCount++
				}
			} else {
				installedCount++
			}
		}
	}

	sweepOrphanedSymlinks([]string{targetDir})
	return installedCount
}

// LinkSkills iterates through sourceDirs in priority order, symlinking non-duplicate skills to targetSkillDirs.
func LinkSkills(targetSkillDirs, sourceDirs []string) int {
	seenSkills := make(map[string]bool)
	installedCount := 0
	for _, srcDir := range sourceDirs {
		entries, err := os.ReadDir(srcDir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			isDir := entry.IsDir()
			if !isDir && entry.Type()&os.ModeSymlink != 0 {
				if fi, err := os.Stat(filepath.Join(srcDir, entry.Name())); err == nil && fi.IsDir() {
					isDir = true
				}
			}
			if !isDir {
				continue
			}

			skillName := entry.Name()
			if seenSkills[skillName] {
				continue
			}
			srcPath := filepath.Join(srcDir, skillName)

			if fi, err := os.Stat(filepath.Join(srcPath, "SKILL.md")); err != nil || fi.IsDir() || fi.Size() == 0 {
				continue
			}

			seenSkills[skillName] = true

			for _, targetDir := range targetSkillDirs {
				destPath := filepath.Join(targetDir, skillName)
				tmpDest := destPath + ".tmp"
				if err := removeSkill(tmpDest); err != nil && !errors.Is(err, os.ErrNotExist) {
					log.Printf("[Skills] Warning removing stale temp skill %s: %v", tmpDest, err)
				}
				if err := symlinkSkill(srcPath, tmpDest); err != nil {
					if err := removeSkill(destPath); err != nil && !errors.Is(err, os.ErrNotExist) {
						log.Printf("[Skills] Warning removing existing destination skill %s: %v", destPath, err)
					}
					if err := symlinkSkill(srcPath, destPath); err != nil {
						log.Printf("Warning: failed to symlink skill %s -> %s: %v", srcPath, destPath, err)
					} else {
						installedCount++
					}
				} else {
					if err := renameSkill(tmpDest, destPath); err != nil {
						if err := removeSkill(destPath); err != nil && !errors.Is(err, os.ErrNotExist) {
							log.Printf("[Skills] Warning removing destination skill %s prior to rename: %v", destPath, err)
						}
						if err := renameSkill(tmpDest, destPath); err != nil {
							log.Printf("[Skills] Warning renaming %s -> %s: %v", tmpDest, destPath, err)
						}
					}
					installedCount++
				}
			}
		}
	}
	return installedCount
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
					if err := removeSkill(linkPath); err != nil && !errors.Is(err, os.ErrNotExist) {
						log.Printf("[Skills] Warning removing orphaned symlink %s: %v", linkPath, err)
					}
				}
			}
		}
	}
}
