package env

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureEngineHome_Suite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		dataDir      string
		runtimeHome  string
		blockingFile string
		wantLink     bool
		wantErr      bool
	}{
		{
			name:        "normal discord runtime creates canonical dir and symlink",
			dataDir:     "data",
			runtimeHome: "runtimes/discord",
			wantLink:    true,
			wantErr:     false,
		},
		{
			name:        "empty dataDir is a no-op",
			dataDir:     "",
			runtimeHome: "runtimes/discord",
			wantLink:    false,
			wantErr:     false,
		},
		{
			name:        "empty runtimeHome is a no-op",
			dataDir:     "data",
			runtimeHome: "",
			wantLink:    false,
			wantErr:     false,
		},
		{
			name:        "runtime config matching canonical config returns nil without relinking",
			dataDir:     "data",
			runtimeHome: "data/brain/discord",
			wantLink:    false,
			wantErr:     false,
		},
		{
			name:         "uncreatable persistent canonical dir returns error",
			dataDir:      "data-blocked",
			runtimeHome:  "runtimes/discord",
			blockingFile: "data-blocked",
			wantLink:     false,
			wantErr:      true,
		},
		{
			name:         "uncreatable runtime home returns error",
			dataDir:      "data",
			runtimeHome:  "blocked-home",
			blockingFile: "blocked-home",
			wantLink:     false,
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmp := t.TempDir()

			if tc.blockingFile != "" {
				if err := os.WriteFile(filepath.Join(tmp, tc.blockingFile), []byte("blocker"), 0o644); err != nil {
					t.Fatalf("failed to create blocking file: %v", err)
				}
			}

			var resolvedDataDir string
			if tc.dataDir != "" {
				resolvedDataDir = filepath.Join(tmp, tc.dataDir)
			}
			var resolvedRuntimeHome string
			if tc.runtimeHome != "" {
				resolvedRuntimeHome = filepath.Join(tmp, tc.runtimeHome)
			}

			p := &Provisioner{
				dataDir: resolvedDataDir,
			}

			err := p.EnsureEngineHome(resolvedRuntimeHome)
			if (err != nil) != tc.wantErr {
				t.Fatalf("EnsureEngineHome() error = %v, wantErr = %v", err, tc.wantErr)
			}

			if tc.wantLink {
				assertCanonicalConfigSymlink(t, p, resolvedDataDir, resolvedRuntimeHome)
			}
		})
	}
}

func assertCanonicalConfigSymlink(t *testing.T, p *Provisioner, dataDir, runtimeHome string) {
	t.Helper()
	rt := filepath.Base(runtimeHome)
	expectedCanonical := filepath.Join(dataDir, "brain", rt, ".config")
	if fi, err := os.Stat(expectedCanonical); err != nil || !fi.IsDir() {
		t.Fatalf("expected canonical directory %q to exist and be dir: %v", expectedCanonical, err)
	}

	runtimeConfig := filepath.Join(runtimeHome, ".config")
	linkDest, err := os.Readlink(runtimeConfig)
	if err != nil {
		t.Fatalf("expected %q to be a symlink: %v", runtimeConfig, err)
	}
	if filepath.Clean(linkDest) != filepath.Clean(expectedCanonical) {
		t.Fatalf("symlink target = %q, want %q", linkDest, expectedCanonical)
	}

	// Calling it again should be idempotent
	if idempErr := p.EnsureEngineHome(runtimeHome); idempErr != nil {
		t.Fatalf("idempotent EnsureEngineHome() error = %v", idempErr)
	}
}
