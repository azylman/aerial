package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigPath(t *testing.T) {
	fallback := "/config/config.yaml"

	t.Run("CONFIG_PATH environment variable takes precedence", func(t *testing.T) {
		t.Setenv("CONFIG_PATH", "/custom/override/config.yaml")
		got := ResolveConfigPath(fallback)
		if got != "/custom/override/config.yaml" {
			t.Errorf("expected /custom/override/config.yaml, got %s", got)
		}
	})

	t.Run("Local config exists and has non-zero size", func(t *testing.T) {
		t.Setenv("CONFIG_PATH", "")
		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "config.yaml")
		if err := os.WriteFile(localFile, []byte("port: '4004'\ntimezone: 'UTC'\n"), 0644); err != nil {
			t.Fatalf("failed to write temp local config: %v", err)
		}

		oldLocal := localConfigPath
		localConfigPath = localFile
		t.Cleanup(func() { localConfigPath = oldLocal })

		got := ResolveConfigPath(fallback)
		if got != localFile {
			t.Errorf("expected %s, got %s", localFile, got)
		}
	})

	t.Run("Local config exists but has zero size falls back", func(t *testing.T) {
		t.Setenv("CONFIG_PATH", "")
		tmpDir := t.TempDir()
		emptyFile := filepath.Join(tmpDir, "empty.yaml")
		if err := os.WriteFile(emptyFile, []byte(""), 0644); err != nil {
			t.Fatalf("failed to write empty local config: %v", err)
		}

		oldLocal := localConfigPath
		localConfigPath = emptyFile
		t.Cleanup(func() { localConfigPath = oldLocal })

		got := ResolveConfigPath(fallback)
		if got != fallback {
			t.Errorf("expected fallback %s, got %s", fallback, got)
		}
	})

	t.Run("Local config does not exist falls back", func(t *testing.T) {
		t.Setenv("CONFIG_PATH", "")
		oldLocal := localConfigPath
		localConfigPath = "/non/existent/config.yaml"
		t.Cleanup(func() { localConfigPath = oldLocal })

		got := ResolveConfigPath(fallback)
		if got != fallback {
			t.Errorf("expected fallback %s, got %s", fallback, got)
		}
	})

	t.Run("Real /local/config.yaml path if accessible", func(t *testing.T) {
		if fi, err := os.Stat("/local"); err == nil && fi.IsDir() {
			realLocal := "/local/config.yaml"
			existed := false
			var originalContent []byte
			if data, err := os.ReadFile(realLocal); err == nil {
				existed = true
				originalContent = data
			}
			t.Cleanup(func() {
				if existed {
					_ = os.WriteFile(realLocal, originalContent, 0644)
				} else {
					_ = os.Remove(realLocal)
				}
			})

			// Test 0-byte file
			if err := os.WriteFile(realLocal, []byte{}, 0644); err == nil {
				t.Setenv("CONFIG_PATH", "")
				oldLocal := localConfigPath
				localConfigPath = realLocal
				t.Cleanup(func() { localConfigPath = oldLocal })

				got := ResolveConfigPath(fallback)
				if got != fallback {
					t.Errorf("expected fallback %s for 0-byte /local/config.yaml, got %s", fallback, got)
				}

				// Test non-empty file
				_ = os.WriteFile(realLocal, []byte("port: '4004'\n"), 0644)
				got = ResolveConfigPath(fallback)
				if got != realLocal {
					t.Errorf("expected %s for non-empty /local/config.yaml, got %s", realLocal, got)
				}
			}
		}
	})
}
