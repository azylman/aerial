// Package bootstrap holds the startup and config-reload plumbing shared by the
// eri gateway and the eri-restate worker.
package bootstrap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// DefaultGeminiHomeDir returns the production default base directory for .gemini files.
func DefaultGeminiHomeDir() string {
	return defaultGeminiHomeDirWithLookup(os.Getenv("HOME"), testing.Testing(), os.UserHomeDir)
}

func defaultGeminiHomeDirWithLookup(home string, isTesting bool, userHomeDirFn func() (string, error)) string {
	if strings.TrimSpace(home) != "" {
		return strings.TrimSpace(home)
	}
	if isTesting {
		return filepath.Join(os.TempDir(), "aerial-test-gemini")
	}
	if userHomeDirFn != nil {
		if h, err := userHomeDirFn(); err == nil && strings.TrimSpace(h) != "" {
			return strings.TrimSpace(h)
		}
	}
	return "/root"
}

// DefaultDataDir returns the production default base directory for persistent data.
func DefaultDataDir() string {
	return defaultDataDir(testing.Testing())
}

func defaultDataDir(isTesting bool) string {
	if isTesting {
		return filepath.Join(os.TempDir(), "aerial-test-data")
	}
	return "/data"
}
