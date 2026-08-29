package skills

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureSkills(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	// Create dummy skills in mock plugin and .agents dirs
	mockSkillDir := filepath.Join(tmpDir, ".gemini", "config", "plugins", "superpowers", "skills", "test-skill")
	if err := os.MkdirAll(mockSkillDir, 0755); err != nil {
		t.Fatalf("Failed to create mock skill dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mockSkillDir, "SKILL.md"), []byte("# Test Skill"), 0644); err != nil {
		t.Fatalf("Failed to write mock SKILL.md: %v", err)
	}
	// Create .git directory to avoid attempting clone in test
	if err := os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "plugins", "superpowers", ".git"), 0755); err != nil {
		t.Fatalf("Failed to create mock .git dir: %v", err)
	}

	if err := EnsureSkills(); err != nil {
		t.Errorf("Expected nil error from EnsureSkills, got: %v", err)
	}

	// Verify symlink was created
	linkPath := filepath.Join(tmpDir, ".gemini", "config", "skills", "test-skill")
	if _, err := os.Lstat(linkPath); err != nil {
		t.Errorf("Expected symlink at %s, got error: %v", linkPath, err)
	}
}

func TestEnsureSkills_OrphanedSymlinkSweeper(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	targetSkillsDir := filepath.Join(tmpDir, ".gemini", "config", "skills")
	if err := os.MkdirAll(targetSkillsDir, 0755); err != nil {
		t.Fatalf("Failed to create target skills dir: %v", err)
	}

	// Create a broken symlink pointing to a non-existent target
	brokenLink := filepath.Join(targetSkillsDir, "broken-skill")
	nonExistentTarget := filepath.Join(tmpDir, "non-existent-skill-dir")
	if err := os.Symlink(nonExistentTarget, brokenLink); err != nil {
		t.Fatalf("Failed to create broken symlink: %v", err)
	}

	// Create mock superpowers git dir to avoid cloning
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "plugins", "superpowers", ".git"), 0755)

	if err := EnsureSkills(); err != nil {
		t.Fatalf("EnsureSkills failed: %v", err)
	}

	// Verify the broken symlink was removed
	if _, err := os.Lstat(brokenLink); !os.IsNotExist(err) {
		t.Errorf("Expected broken symlink %s to be removed, but it still exists", brokenLink)
	}
}

func TestEnsureSkills_PriorityOrder(t *testing.T) {
	tmpDir := t.TempDir()

	customSkillsDir := filepath.Join(tmpDir, "share", "aerial-config", "custom-skills")
	pluginSkillsDir := filepath.Join(tmpDir, "superpowers", "skills")
	shareSkillsDir := filepath.Join(tmpDir, "share", "aerial", ".agents", "skills")
	appSkillsDir := filepath.Join(tmpDir, "app", ".agents", "skills")

	targetSkillsDir := filepath.Join(tmpDir, "target-skills")
	_ = os.MkdirAll(targetSkillsDir, 0755)

	// Create duplicate skill "smart-home" in custom-skills (highest) and app-skills (lowest)
	customSmartHome := filepath.Join(customSkillsDir, "smart-home")
	_ = os.MkdirAll(customSmartHome, 0755)
	_ = os.WriteFile(filepath.Join(customSmartHome, "SKILL.md"), []byte("# Custom Smart Home Skill"), 0644)

	appSmartHome := filepath.Join(appSkillsDir, "smart-home")
	_ = os.MkdirAll(appSmartHome, 0755)
	_ = os.WriteFile(filepath.Join(appSmartHome, "SKILL.md"), []byte("# App Default Smart Home Skill"), 0644)

	// Create unique skill in app-skills (lowest)
	appUnique := filepath.Join(appSkillsDir, "app-unique")
	_ = os.MkdirAll(appUnique, 0755)
	_ = os.WriteFile(filepath.Join(appUnique, "SKILL.md"), []byte("# App Unique Skill"), 0644)

	// Create unique skill in superpowers plugin (medium)
	pluginUnique := filepath.Join(pluginSkillsDir, "plugin-unique")
	_ = os.MkdirAll(pluginUnique, 0755)
	_ = os.WriteFile(filepath.Join(pluginUnique, "SKILL.md"), []byte("# Plugin Unique Skill"), 0644)

	sourceDirs := []string{
		customSkillsDir,
		pluginSkillsDir,
		shareSkillsDir,
		appSkillsDir,
	}

	count := LinkSkills([]string{targetSkillsDir}, sourceDirs)
	if count != 3 {
		t.Errorf("Expected 3 skills linked, got %d", count)
	}

	// Verify "smart-home" points to custom-skills, NOT app-skills
	smartHomeLink := filepath.Join(targetSkillsDir, "smart-home", "SKILL.md")
	data, err := os.ReadFile(smartHomeLink)
	if err != nil {
		t.Fatalf("Failed to read linked smart-home SKILL.md: %v", err)
	}
	if string(data) != "# Custom Smart Home Skill" {
		t.Errorf("Expected custom-skills to take precedence, got content: %q", string(data))
	}

	// Verify "app-unique" is linked from app-skills
	appUniqueLink := filepath.Join(targetSkillsDir, "app-unique", "SKILL.md")
	dataUnique, err := os.ReadFile(appUniqueLink)
	if err != nil {
		t.Fatalf("Failed to read linked app-unique SKILL.md: %v", err)
	}
	if string(dataUnique) != "# App Unique Skill" {
		t.Errorf("Expected app-unique content, got: %q", string(dataUnique))
	}
}



