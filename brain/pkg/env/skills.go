package env

import (
	"errors"
	"log"
	"os"
	"path/filepath"
)

// DefaultBlockedSkills defines skill names excluded from runtime symlinking to prevent clashes with Aerial invariants.
var DefaultBlockedSkills = map[string]bool{
	"using-git-worktrees":            true,
	"finishing-a-development-branch": true,
	"requesting-code-review":         true,
	"subagent-driven-development":    true,
}

// SyncSkills symlinks custom and built-in skills into ~/.gemini/config/skills.
func (p *Provisioner) SyncSkills() error {
	if p == nil || p.homeDir == "" {
		return nil
	}

	// Clean up legacy ~/.gemini/skills directory if present to eliminate redundant skill trees
	legacySkillsDir := filepath.Join(p.homeDir, ".gemini", "skills")
	if fi, err := os.Lstat(legacySkillsDir); err == nil {
		if fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			if rmErr := os.RemoveAll(legacySkillsDir); rmErr != nil {
				log.Printf("[Skills] Warning removing legacy skills directory %s: %v", legacySkillsDir, rmErr)
			}
		}
	}

	targetSkillDirs := []string{
		filepath.Join(p.homeDir, ".gemini", "config", "skills"),
	}
	for _, dir := range targetSkillDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Warning: failed to create skills directory %s: %v", dir, err)
		}
	}

	blocked := p.blockedSkills
	if blocked == nil {
		blocked = DefaultBlockedSkills
	}

	// Clean up any previously linked blocked skills in target directories
	pruneBlockedSkills(targetSkillDirs, blocked)

	// Orphaned Symlink Sweeper: Clean up dead links before scanning
	sweepOrphanedSymlinks(targetSkillDirs)

	sourceDirs := []string{
		p.customSkillsDir,
		p.superpowersDir,
		"/share/aerial/.agents/skills",
		p.agentsSkillsDir,
	}

	installedCount := LinkSkillsWithFilter(targetSkillDirs, sourceDirs, blocked)

	// Final sweep to remove any remaining broken symlinks and prune any blocked skills
	sweepOrphanedSymlinks(targetSkillDirs)
	pruneBlockedSkills(targetSkillDirs, blocked)

	log.Printf("Skills subsystem initialized: %d skill links verified across target directories", installedCount)
	return nil
}

// LinkSkills iterates through sourceDirs in priority order, symlinking non-duplicate skills to targetSkillDirs,
// filtering out DefaultBlockedSkills.
func LinkSkills(targetSkillDirs, sourceDirs []string) int {
	return LinkSkillsWithFilter(targetSkillDirs, sourceDirs, DefaultBlockedSkills)
}

// LinkSkillsWithFilter iterates through sourceDirs in priority order, symlinking non-duplicate skills
// to targetSkillDirs while filtering out any skills in blockedSkills.
func LinkSkillsWithFilter(targetSkillDirs, sourceDirs []string, blockedSkills map[string]bool) int {
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
			if blockedSkills != nil && blockedSkills[skillName] {
				continue
			}
			if seenSkills[skillName] {
				continue
			}
			srcPath := filepath.Join(srcDir, skillName)

			if _, err := os.Stat(filepath.Join(srcPath, "SKILL.md")); err != nil {
				continue
			}

			seenSkills[skillName] = true

			for _, targetDir := range targetSkillDirs {
				destPath := filepath.Join(targetDir, skillName)
				tmpDest := destPath + ".tmp"
				if err := os.Remove(tmpDest); err != nil && !errors.Is(err, os.ErrNotExist) {
					log.Printf("[Skills] Warning removing stale temp skill %s: %v", tmpDest, err)
				}
				if err := os.Symlink(srcPath, tmpDest); err != nil {
					if err := os.Remove(destPath); err != nil && !errors.Is(err, os.ErrNotExist) {
						log.Printf("[Skills] Warning removing existing destination skill %s: %v", destPath, err)
					}
					if err := os.Symlink(srcPath, destPath); err != nil {
						log.Printf("Warning: failed to symlink skill %s -> %s: %v", srcPath, destPath, err)
					} else {
						installedCount++
					}
				} else {
					if err := os.Rename(tmpDest, destPath); err != nil {
						if err := os.Remove(destPath); err != nil && !errors.Is(err, os.ErrNotExist) {
							log.Printf("[Skills] Warning removing destination skill %s prior to rename: %v", destPath, err)
						}
						if err := os.Rename(tmpDest, destPath); err != nil {
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

func pruneBlockedSkills(targetSkillDirs []string, blockedSkills map[string]bool) {
	if len(blockedSkills) == 0 {
		return
	}
	for _, dir := range targetSkillDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if blockedSkills[entry.Name()] {
				targetPath := filepath.Join(dir, entry.Name())
				if err := os.RemoveAll(targetPath); err != nil && !errors.Is(err, os.ErrNotExist) {
					log.Printf("[Skills] Warning removing blocked skill %s: %v", targetPath, err)
				}
			}
		}
	}
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
					if err := os.Remove(linkPath); err != nil && !errors.Is(err, os.ErrNotExist) {
						log.Printf("[Skills] Warning removing orphaned symlink %s: %v", linkPath, err)
					}
				}
			}
		}
	}
}

