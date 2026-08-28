package skills

import "testing"

func TestEnsureSkills(t *testing.T) {
	if err := EnsureSkills(); err != nil {
		t.Errorf("Expected nil error from EnsureSkills, got: %v", err)
	}
}
