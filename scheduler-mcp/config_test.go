package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigPath(t *testing.T) {
	tmpDir := t.TempDir()
	validLocal, emptyLocal, nonExistent := filepath.Join(tmpDir, "valid.yaml"), filepath.Join(tmpDir, "empty.yaml"), filepath.Join(tmpDir, "missing.yaml")
	_ = os.WriteFile(validLocal, []byte("port: '4004'\n"), 0644)
	_ = os.WriteFile(emptyLocal, []byte{}, 0644)

	fallback := "/config/config.yaml"
	tests := []struct {
		name      string
		envVal    string
		localPath string
		want      string
	}{
		{"env override takes precedence", "/custom/override.yaml", validLocal, "/custom/override.yaml"},
		{"local config non-empty exists", "", validLocal, validLocal},
		{"local config empty falls back", "", emptyLocal, fallback},
		{"local config missing falls back", "", nonExistent, fallback},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", tc.envVal)
			oldLocal := localConfigPath
			localConfigPath = tc.localPath
			defer func() { localConfigPath = oldLocal }()

			if got := ResolveConfigPath(fallback); got != tc.want {
				t.Errorf("ResolveConfigPath(%q) = %q, want %q", fallback, got, tc.want)
			}
		})
	}
}

func TestLoadConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	validPath, noPortPath, noTZPath, malformedPath := filepath.Join(tmpDir, "valid.yaml"), filepath.Join(tmpDir, "no_port.yaml"), filepath.Join(tmpDir, "no_tz.yaml"), filepath.Join(tmpDir, "malformed.yaml")
	_ = os.WriteFile(validPath, []byte("port: '4005'\ntimezone: 'America/Chicago'\n"), 0644)
	_ = os.WriteFile(noPortPath, []byte("timezone: 'America/Chicago'\n"), 0644)
	_ = os.WriteFile(noTZPath, []byte("port: '4005'\n"), 0644)
	_ = os.WriteFile(malformedPath, []byte("port: [unclosed"), 0644)

	tests := []struct {
		name     string
		path     string
		dbURL    string
		wantErr  bool
		wantPort string
		wantTZ   string
	}{
		{"valid config", validPath, "postgres://user:pass@host:5432/db", false, "4005", "America/Chicago"},
		{"missing file", filepath.Join(tmpDir, "missing.yaml"), "postgres://localhost/db", true, "", ""},
		{"malformed yaml", malformedPath, "postgres://localhost/db", true, "", ""},
		{"missing port", noPortPath, "postgres://localhost/db", true, "", ""},
		{"missing timezone", noTZPath, "postgres://localhost/db", true, "", ""},
		{"empty dbURL", validPath, "", true, "", ""},
		{"empty path", "", "postgres://localhost/db", true, "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfigFile(tc.path, tc.dbURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (cfg.Port != tc.wantPort || cfg.Timezone != tc.wantTZ || cfg.DatabaseURL != tc.dbURL) {
				t.Fatalf("unexpected config: %+v", cfg)
			}
		})
	}
}
