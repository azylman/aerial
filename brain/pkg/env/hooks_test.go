package env

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultCanonicalHooksJSON(t *testing.T) {
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(defaultCanonicalHooksJSON), &parsed); err != nil {
		t.Fatalf("defaultCanonicalHooksJSON is not valid JSON: %v", err)
	}
	for _, expected := range []string{"share-guard", "schedule-guard", "commit-guard"} {
		if _, ok := parsed[expected]; !ok {
			t.Errorf("expected %q in defaultCanonicalHooksJSON", expected)
		}
	}
}

func TestSyncHooks(t *testing.T) {
	tests := []struct {
		name             string
		nilProv          bool
		emptyHome        bool
		withDataDir      bool
		withCustomAssets bool
		withCandidates   bool
		hooksDirIsFile   bool
		blockConfig      bool
		blockHooksDir    bool
	}{
		{name: "nil provisioner", nilProv: true},
		{name: "empty home", emptyHome: true},
		{name: "default canonical across runtimes", withDataDir: true},
		{name: "custom hooks and executable scripts", withCustomAssets: true},
		{name: "candidate search with subfolder and broken symlink", withCandidates: true},
		{name: "hooksDir points to plain file", hooksDirIsFile: true},
		{name: "config directory blocked by file", blockConfig: true},
		{name: "hooks scripts directory blocked by file", withCustomAssets: true, blockHooksDir: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nilProv {
				var pNil *Provisioner
				if err := pNil.SyncHooks(); err != nil {
					t.Fatalf("expected nil error on nil provisioner, got: %v", err)
				}
				return
			}
			if tc.emptyHome {
				pEmpty := New("", "")
				if err := pEmpty.SyncHooks(); err != nil {
					t.Fatalf("expected nil error on empty homeDir, got: %v", err)
				}
				return
			}

			tmpHome := t.TempDir()
			var tmpData string
			if tc.withDataDir {
				tmpData = t.TempDir()
			}

			p := New(tmpHome, tmpData)

			if tc.blockConfig {
				geminiRoot := filepath.Join(tmpHome, ".gemini")
				if err := os.MkdirAll(geminiRoot, 0755); err != nil {
					t.Fatalf("mkdir gemini root: %v", err)
				}
				if err := os.WriteFile(filepath.Join(geminiRoot, "config"), []byte("file blocker"), 0644); err != nil {
					t.Fatalf("write blocker: %v", err)
				}
			}

			if tc.blockHooksDir {
				geminiConfig := filepath.Join(tmpHome, ".gemini", "config")
				if err := os.MkdirAll(geminiConfig, 0755); err != nil {
					t.Fatalf("mkdir gemini config: %v", err)
				}
				if err := os.WriteFile(filepath.Join(geminiConfig, "hooks"), []byte("file blocker"), 0644); err != nil {
					t.Fatalf("write blocker: %v", err)
				}
			}

			if tc.withCustomAssets {
				sourceDir := t.TempDir()
				customHooks := `{"custom-guard":{"PreToolUse":[]}}`
				if err := os.WriteFile(filepath.Join(sourceDir, "hooks.json"), []byte(customHooks), 0644); err != nil {
					t.Fatalf("write custom hooks.json: %v", err)
				}
				testScript := "#!/usr/bin/env python3\nprint('ok')\n"
				if err := os.WriteFile(filepath.Join(sourceDir, "custom_guard.py"), []byte(testScript), 0644); err != nil {
					t.Fatalf("write custom script: %v", err)
				}
				p.SetHooksDir(sourceDir)
			}

			if tc.withCandidates {
				sourceDir := t.TempDir()
				origPaths := DefaultHooksSearchPaths
				defer func() { DefaultHooksSearchPaths = origPaths }()
				DefaultHooksSearchPaths = []string{sourceDir}

				if err := os.MkdirAll(filepath.Join(sourceDir, "subfolder"), 0755); err != nil {
					t.Fatalf("mkdir subfolder: %v", err)
				}
				_ = os.Symlink(filepath.Join(sourceDir, "nonexistent"), filepath.Join(sourceDir, "broken.sh"))
				p.SetHooksDir("")
			}

			if tc.hooksDirIsFile {
				plainFile := filepath.Join(tmpHome, "file.txt")
				if err := os.WriteFile(plainFile, []byte("not a dir"), 0644); err != nil {
					t.Fatalf("write plain file: %v", err)
				}
				p.SetHooksDir(plainFile)
			}

			if err := p.SyncHooks(); err != nil {
				t.Fatalf("SyncHooks failed: %v", err)
			}

			if tc.withDataDir {
				for _, target := range allRuntimeTargets {
					rtHooksJSON := filepath.Join(tmpData, "runtimes", string(target), ".gemini", "config", "hooks.json")
					if _, err := os.Stat(rtHooksJSON); err != nil {
						t.Errorf("expected hooks.json in runtime %s: %v", target, err)
					}
				}
			}

			if tc.withCustomAssets && !tc.blockHooksDir && !tc.blockConfig {
				scriptPath := filepath.Join(tmpHome, ".gemini", "config", "hooks", "custom_guard.py")
				fi, err := os.Stat(scriptPath)
				if err != nil {
					t.Fatalf("expected copied script at %s: %v", scriptPath, err)
				}
				if fi.Mode().Perm()&0111 == 0 {
					t.Errorf("expected executable script permissions, got %v", fi.Mode().Perm())
				}
				// Verify idempotency
				if err := p.SyncHooks(); err != nil {
					t.Errorf("second SyncHooks call failed: %v", err)
				}
			}
		})
	}
}
