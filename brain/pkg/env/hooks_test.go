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
