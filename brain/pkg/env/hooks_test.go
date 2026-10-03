package env

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSyncHooks_NilAndEmptyHome(t *testing.T) {
	t.Parallel()

	var pNil *Provisioner
	if err := pNil.SyncHooks(); err != nil {
		t.Errorf("expected nil error on nil provisioner, got: %v", err)
	}

	pEmpty := New("", "")
	if err := pEmpty.SyncHooks(); err != nil {
		t.Errorf("expected nil error on empty homeDir, got: %v", err)
	}
}

func TestSyncHooks_Default(t *testing.T) {
	t.Parallel()

	tmpHome := t.TempDir()
	tmpData := t.TempDir()

	p := New(tmpHome, tmpData)

	if err := p.SyncHooks(); err != nil {
		t.Fatalf("SyncHooks failed: %v", err)
	}

	// Verify hooks.json in primary home
	primaryHooksJSON := filepath.Join(tmpHome, ".gemini", "config", "hooks.json")
	data, err := os.ReadFile(primaryHooksJSON)
	if err != nil {
		t.Fatalf("failed to read primary hooks.json: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("primary hooks.json is not valid JSON: %v", err)
	}

	if _, ok := parsed["share-guard"]; !ok {
		t.Errorf("expected 'share-guard' in hooks.json")
	}
	if _, ok := parsed["schedule-guard"]; !ok {
		t.Errorf("expected 'schedule-guard' in hooks.json")
	}

	// Verify runtime targets
	runtimes := []string{string(TargetDiscord), string(TargetVoice), string(TargetEphemeral)}
	for _, rt := range runtimes {
		rtHooksJSON := filepath.Join(tmpData, "runtimes", rt, ".gemini", "config", "hooks.json")
		if _, statErr := os.Stat(rtHooksJSON); statErr != nil {
			t.Errorf("expected hooks.json in runtime %s, got err: %v", rt, statErr)
		}
	}
}

func TestDefaultCanonicalHooksJSON(t *testing.T) {
	t.Parallel()

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(defaultCanonicalHooksJSON), &parsed); err != nil {
		t.Fatalf("defaultCanonicalHooksJSON is not valid JSON: %v", err)
	}
	if _, ok := parsed["share-guard"]; !ok {
		t.Errorf("expected 'share-guard' in defaultCanonicalHooksJSON")
	}
	if _, ok := parsed["schedule-guard"]; !ok {
		t.Errorf("expected 'schedule-guard' in defaultCanonicalHooksJSON")
	}
	if _, ok := parsed["commit-guard"]; !ok {
		t.Errorf("expected 'commit-guard' in defaultCanonicalHooksJSON")
	}
}

func TestSyncHooks_WithSourceFiles(t *testing.T) {
	t.Parallel()

	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	sourceDir := t.TempDir()

	// Write custom source hooks.json and test script
	customHooks := `{"custom-guard":{"PreToolUse":[]}}`
	if err := os.WriteFile(filepath.Join(sourceDir, "hooks.json"), []byte(customHooks), 0644); err != nil {
		t.Fatalf("failed to write source hooks.json: %v", err)
	}
	testScript := `#!/usr/bin/env python3
print('{"decision": "allow"}')
`
	if err := os.WriteFile(filepath.Join(sourceDir, "custom_guard.py"), []byte(testScript), 0644); err != nil {
		t.Fatalf("failed to write source custom_guard.py: %v", err)
	}

	p := New(tmpHome, tmpData)
	p.SetHooksDir(sourceDir)
	if err := p.SyncHooks(); err != nil {
		t.Fatalf("SyncHooks failed: %v", err)
	}

	// Verify primary has custom hooks.json
	primaryHooksJSON := filepath.Join(tmpHome, ".gemini", "config", "hooks.json")
	data, err := os.ReadFile(primaryHooksJSON)
	if err != nil {
		t.Fatalf("failed to read primary hooks.json: %v", err)
	}
	if string(data) != customHooks {
		t.Errorf("expected custom hooks content %q, got %q", customHooks, string(data))
	}

	// Verify script was copied to config/hooks/ and is executable
	scriptPath := filepath.Join(tmpHome, ".gemini", "config", "hooks", "custom_guard.py")
	fi, statErr := os.Stat(scriptPath)
	if statErr != nil {
		t.Fatalf("expected script at %s: %v", scriptPath, statErr)
	}
	if fi.Mode().Perm()&0111 == 0 {
		t.Errorf("expected executable script permissions, got: %v", fi.Mode().Perm())
	}

	// Verify idempotency
	if err := p.SyncHooks(); err != nil {
		t.Errorf("second SyncHooks call failed: %v", err)
	}
}

func TestSyncHooks_Branches(t *testing.T) {
	// 1. Candidate match in DefaultHooksSearchPaths
	sourceDir := t.TempDir()
	origPaths := DefaultHooksSearchPaths
	defer func() { DefaultHooksSearchPaths = origPaths }()
	DefaultHooksSearchPaths = []string{sourceDir}

	tmpHome := t.TempDir()
	p := New(tmpHome, "")
	p.SetHooksDir("") // force candidate search

	if err := p.SyncHooks(); err != nil {
		t.Errorf("SyncHooks with candidate path failed: %v", err)
	}

	// 2. Subdirectory and broken symlink in sourceDir
	subDir := filepath.Join(sourceDir, "subfolder")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("failed to create subfolder: %v", err)
	}
	brokenSymlink := filepath.Join(sourceDir, "broken.sh")
	_ = os.Symlink(filepath.Join(sourceDir, "nonexistent"), brokenSymlink)

	p.SetHooksDir(sourceDir)
	if err := p.SyncHooks(); err != nil {
		t.Errorf("SyncHooks with subfolder and broken symlink failed: %v", err)
	}

	// 3. Invalid hooksDir pointing to a plain file
	plainFile := filepath.Join(tmpHome, "file.txt")
	_ = os.WriteFile(plainFile, []byte("not a dir"), 0644)
	p.SetHooksDir(plainFile)
	if err := p.SyncHooks(); err != nil {
		t.Errorf("SyncHooks with file hooksDir failed: %v", err)
	}

	// 4. MkdirAll error on config dir (regular file where directory is expected)
	errHome := t.TempDir()
	geminiRoot := filepath.Join(errHome, ".gemini")
	_ = os.MkdirAll(geminiRoot, 0755)
	_ = os.WriteFile(filepath.Join(geminiRoot, "config"), []byte("file blocker"), 0644)
	pErr := New(errHome, "")
	if err := pErr.SyncHooks(); err != nil {
		t.Errorf("SyncHooks should not fail when config dir creation fails: %v", err)
	}

	// 5. MkdirAll error on hooks scripts dir
	errHome2 := t.TempDir()
	geminiConfig2 := filepath.Join(errHome2, ".gemini", "config")
	_ = os.MkdirAll(geminiConfig2, 0755)
	_ = os.WriteFile(filepath.Join(geminiConfig2, "hooks"), []byte("file blocker"), 0644)
	pErr2 := New(errHome2, "")
	pErr2.SetHooksDir(sourceDir)
	if err := pErr2.SyncHooks(); err != nil {
		t.Errorf("SyncHooks should not fail when hooks scripts dir creation fails: %v", err)
	}
}

