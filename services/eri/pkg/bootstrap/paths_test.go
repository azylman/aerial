package bootstrap

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultGeminiHomeDirWithLookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		home      string
		isTesting bool
		userHome  string
		userErr   error
		noLookup  bool
		want      string
	}{
		{name: "home env wins and is trimmed", home: " /custom/home ", want: "/custom/home"},
		{name: "testing falls back to temp dir", isTesting: true, want: filepath.Join(os.TempDir(), "aerial-test-gemini")},
		{name: "user home dir lookup", userHome: "/home/testuser", want: "/home/testuser"},
		{name: "blank user home dir falls back to root", userHome: "  ", want: "/root"},
		{name: "user home dir error falls back to root", userErr: errors.New("err"), want: "/root"},
		{name: "nil lookup falls back to root", noLookup: true, want: "/root"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lookup := func() (string, error) { return tc.userHome, tc.userErr }
			if tc.noLookup {
				lookup = nil
			}
			if got := defaultGeminiHomeDirWithLookup(tc.home, tc.isTesting, lookup); got != tc.want {
				t.Errorf("defaultGeminiHomeDirWithLookup() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDefaultDataDir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		isTesting bool
		want      string
	}{
		{name: "production", isTesting: false, want: "/data"},
		{name: "testing", isTesting: true, want: filepath.Join(os.TempDir(), "aerial-test-data")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := defaultDataDir(tc.isTesting); got != tc.want {
				t.Errorf("defaultDataDir() = %q, want %q", got, tc.want)
			}
		})
	}
}

// Mutates HOME, so it cannot run in parallel.
func TestDefaultDirsFromEnvironment(t *testing.T) {
	tests := []struct {
		name     string
		home     string
		wantHome string
		wantData string
	}{
		{name: "home set", home: "/custom/home", wantHome: "/custom/home", wantData: filepath.Join(os.TempDir(), "aerial-test-data")},
		{name: "home empty under test", home: "", wantHome: filepath.Join(os.TempDir(), "aerial-test-gemini"), wantData: filepath.Join(os.TempDir(), "aerial-test-data")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", tc.home)
			if got := DefaultGeminiHomeDir(); got != tc.wantHome {
				t.Errorf("DefaultGeminiHomeDir() = %q, want %q", got, tc.wantHome)
			}
			if got := DefaultDataDir(); got != tc.wantData {
				t.Errorf("DefaultDataDir() = %q, want %q", got, tc.wantData)
			}
		})
	}
}
