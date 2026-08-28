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

