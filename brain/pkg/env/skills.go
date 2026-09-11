package env

import (
	"log"
	"os"
	"path/filepath"
)

// SyncSkills symlinks custom and built-in skills into ~/.gemini/skills directories.
func (p *Provisioner) SyncSkills() error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	targetSkillDirs := []string{
		filepath.Join(p.homeDir, ".gemini", "config", "skills"),
		filepath.Join(p.homeDir, ".gemini", "skills"),
	}
	for _, dir := range targetSkillDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Warning: failed to create skills directory %s: %v", dir, err)
		}
	}

	// Orphaned Symlink Sweeper: Clean up dead links before scanning
	sweepOrphanedSymlinks(targetSkillDirs)

	sourceDirs := []string{
		p.customSkillsDir,
		p.superpowersDir,
		"/share/aerial/.agents/skills",
		p.agentsSkillsDir,
	}

	installedCount := LinkSkills(targetSkillDirs, sourceDirs)

	// Final sweep to remove any remaining broken symlinks
	sweepOrphanedSymlinks(targetSkillDirs)

	log.Printf("Skills subsystem initialized: %d skill links verified across target directories", installedCount)
	return nil
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

			if _, err := os.Stat(filepath.Join(srcPath, "SKILL.md")); err != nil {
				continue
			}

			seenSkills[skillName] = true

			for _, targetDir := range targetSkillDirs {
				destPath := filepath.Join(targetDir, skillName)
				tmpDest := destPath + ".tmp"
				_ = os.Remove(tmpDest)
				if err := os.Symlink(srcPath, tmpDest); err != nil {
					_ = os.Remove(destPath)
					if err := os.Symlink(srcPath, destPath); err != nil {
						log.Printf("Warning: failed to symlink skill %s -> %s: %v", srcPath, destPath, err)
					} else {
						installedCount++
					}
				} else {
					if err := os.Rename(tmpDest, destPath); err != nil {
						_ = os.Remove(destPath)
						_ = os.Rename(tmpDest, destPath)
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
					log.Printf("[Skills] Removing orphaned symlink: %s", linkPath)
					_ = os.Remove(linkPath)
				}
			}
		}
	}
}
