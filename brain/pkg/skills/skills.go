package skills

import (
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

func EnsureSkills() error {
	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		homeDir = "/root"
	}

	pluginDir := filepath.Join(homeDir, ".gemini", "config", "plugins", "superpowers")

	// If superpowers is not present, clone it
	if _, err := os.Stat(filepath.Join(pluginDir, ".git")); err != nil {
		_ = os.MkdirAll(filepath.Dir(pluginDir), 0755)
		log.Printf("Cloning obra/superpowers into %s...", pluginDir)
		cmd := exec.Command("git", "clone", "https://github.com/obra/superpowers.git", pluginDir)
		if out, err := cmd.CombinedOutput(); err != nil {
			log.Printf("Warning: failed to clone superpowers: %v, output: %s", err, string(out))
		} else {
			log.Printf("Successfully cloned obra/superpowers")
		}
	}

	targetSkillDirs := []string{
		filepath.Join(homeDir, ".gemini", "config", "skills"),
		filepath.Join(homeDir, ".gemini", "skills"),
	}
	for _, dir := range targetSkillDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Warning: failed to create skills directory %s: %v", dir, err)
		}
	}

	sourceDirs := []string{
		filepath.Join(pluginDir, "skills"),
		"/share/aerial/.agents/skills",
		"/app/.agents/skills",
	}

	installedCount := 0
	for _, srcDir := range sourceDirs {
		entries, err := os.ReadDir(srcDir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			skillName := entry.Name()
			srcPath := filepath.Join(srcDir, skillName)

			if _, err := os.Stat(filepath.Join(srcPath, "SKILL.md")); err != nil {
				continue
			}

			for _, targetDir := range targetSkillDirs {
				destPath := filepath.Join(targetDir, skillName)
				if _, err := os.Lstat(destPath); err == nil {
					_ = os.Remove(destPath)
				}
				if err := os.Symlink(srcPath, destPath); err != nil {
					log.Printf("Warning: failed to symlink skill %s -> %s: %v", srcPath, destPath, err)
				} else {
					installedCount++
				}
			}
		}
	}

	log.Printf("Skills subsystem initialized: %d skill links verified across target directories", installedCount)
	return nil
}

