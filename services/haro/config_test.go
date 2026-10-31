package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigPath(t *testing.T) {
	tmpDir := t.TempDir()
	validLocal, emptyLocal, nonExistent := filepath.Join(tmpDir, "valid.yaml"), filepath.Join(tmpDir, "empty.yaml"), filepath.Join(tmpDir, "missing.yaml")
	fallbackFile := filepath.Join(tmpDir, "haro.yaml")
	_ = os.WriteFile(validLocal, []byte("port: '4004'\n"), 0o644)
	_ = os.WriteFile(emptyLocal, []byte{}, 0o644)
	_ = os.WriteFile(fallbackFile, []byte("port: '4004'\n"), 0o644)

	fallback := "/config/haro.yaml"
	tests := []struct {
		name         string
		envVal       string
		localPath    string
		fallbackPath string
		want         string
	}{
		{"env override takes precedence", "/custom/override.yaml", validLocal, fallback, "/custom/override.yaml"},
		{"local config non-empty exists", "", validLocal, fallback, validLocal},
		{"local config empty falls back to fallbackFile", "", emptyLocal, fallbackFile, fallbackFile},
		{"local config missing falls back to fallbackFile", "", nonExistent, fallbackFile, fallbackFile},
		{"fallback missing falls back to default", "", nonExistent, fallback, fallback},
		{"fallback empty falls back to defaultConfigPath", "", nonExistent, "", defaultConfigPath},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", tc.envVal)
			oldLocal := localConfigPath
			localConfigPath = tc.localPath
			defer func() {
				localConfigPath = oldLocal
			}()

			if got := ResolveConfigPath(tc.fallbackPath); got != tc.want {
				t.Errorf("ResolveConfigPath(%q) = %q, want %q", tc.fallbackPath, got, tc.want)
			}
		})
	}
}

func TestLoadConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	validPath, noPortPath, noTZPath, malformedPath := filepath.Join(tmpDir, "valid.yaml"), filepath.Join(tmpDir, "no_port.yaml"), filepath.Join(tmpDir, "no_tz.yaml"), filepath.Join(tmpDir, "malformed.yaml")
	extraPath := filepath.Join(tmpDir, "extra.yaml")
	_ = os.WriteFile(validPath, []byte("port: '4005'\ntimezone: 'America/Chicago'\n"), 0o644)
	_ = os.WriteFile(extraPath, []byte("port: '4005'\ntimezone: 'America/Chicago'\nextra_event_sources:\n  - ' Custom_Source '\n  - ''\n  - 'custom_source'\n"), 0o644)
	_ = os.WriteFile(noPortPath, []byte("timezone: 'America/Chicago'\n"), 0o644)
	_ = os.WriteFile(noTZPath, []byte("port: '4005'\n"), 0o644)
	_ = os.WriteFile(malformedPath, []byte("port: [unclosed"), 0o644)

	tests := []struct {
		name        string
		path        string
		dbURL       string
		wantErr     bool
		wantPort    string
		wantTZ      string
		wantSources int
	}{
		{"valid config", validPath, "postgres://user:pass@host:5432/db", false, "4005", "America/Chicago", 0},
		{"valid config with extra_event_sources", extraPath, "postgres://user:pass@host:5432/db", false, "4005", "America/Chicago", 1},
		{"missing file", filepath.Join(tmpDir, "missing.yaml"), "postgres://localhost/db", true, "", "", 0},
		{"malformed yaml", malformedPath, "postgres://localhost/db", true, "", "", 0},
		{"missing port", noPortPath, "postgres://localhost/db", true, "", "", 0},
		{"missing timezone", noTZPath, "postgres://localhost/db", true, "", "", 0},
		{"empty dbURL", validPath, "", true, "", "", 0},
		{"empty path", "", "postgres://localhost/db", true, "", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfigFile(tc.path, tc.dbURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (cfg.Port != tc.wantPort || cfg.Timezone != tc.wantTZ || cfg.DatabaseURL != tc.dbURL || len(cfg.ExtraEventSources) != tc.wantSources) {
				t.Fatalf("unexpected config: %+v", cfg)
			}
		})
	}
}

func TestLoadConfig_BrainURLResolution(t *testing.T) {
	tmpDir := t.TempDir()
	pathWithBrain := filepath.Join(tmpDir, "with_brain.yaml")
	pathWithEri := filepath.Join(tmpDir, "with_eri.yaml")
	pathWithoutBrain := filepath.Join(tmpDir, "without_brain.yaml")
	_ = os.WriteFile(pathWithBrain, []byte("port: '4005'\ntimezone: 'UTC'\nbrain_url: 'http://yaml.brain:8080/'\n"), 0o644)
	_ = os.WriteFile(pathWithEri, []byte("port: '4005'\ntimezone: 'UTC'\neri_url: 'http://yaml.eri:8088/'\n"), 0o644)
	_ = os.WriteFile(pathWithoutBrain, []byte("port: '4005'\ntimezone: 'UTC'\n"), 0o644)

	tests := []struct {
		name             string
		path             string
		envEri           string
		envEriInternal   string
		envBrainInternal string
		envBrain         string
		envDB            string
		wantErr          bool
		wantBrainURL     string
	}{
		{
			name:             "ERI_URL takes top precedence",
			path:             pathWithBrain,
			envEri:           "http://eri.aerial:9001/",
			envEriInternal:   "http://eri-internal.aerial:9000/",
			envBrainInternal: "http://internal.aerial:9000/",
			envBrain:         "http://brain.aerial:8000/",
			envDB:            "postgres://localhost/db",
			wantBrainURL:     "http://eri.aerial:9001",
		},
		{
			name:             "ERI_INTERNAL_URL takes precedence over BRAIN_INTERNAL_URL",
			path:             pathWithBrain,
			envEriInternal:   "http://eri-internal.aerial:9000/",
			envBrainInternal: "http://internal.aerial:9000/",
			envBrain:         "http://brain.aerial:8000/",
			envDB:            "postgres://localhost/db",
			wantBrainURL:     "http://eri-internal.aerial:9000",
		},
		{
			name:             "BRAIN_INTERNAL_URL takes precedence over BRAIN_URL",
			path:             pathWithBrain,
			envBrainInternal: "http://internal.aerial:9000/",
			envBrain:         "http://brain.aerial:8000/",
			envDB:            "postgres://localhost/db",
			wantBrainURL:     "http://internal.aerial:9000",
		},
		{
			name:             "BRAIN_URL takes precedence over yaml",
			path:             pathWithBrain,
			envBrainInternal: "",
			envBrain:         "http://brain.aerial:8000/",
			envDB:            "postgres://localhost/db",
			wantBrainURL:     "http://brain.aerial:8000",
		},
		{
			name:         "yaml eri_url used when env empty",
			path:         pathWithEri,
			envDB:        "postgres://localhost/db",
			wantBrainURL: "http://yaml.eri:8088",
		},
		{
			name:             "yaml brain_url used when env empty",
			path:             pathWithBrain,
			envBrainInternal: "",
			envBrain:         "",
			envDB:            "postgres://localhost/db",
			wantBrainURL:     "http://yaml.brain:8080",
		},
		{
			name:             "default fallback when nothing configured",
			path:             pathWithoutBrain,
			envBrainInternal: "",
			envBrain:         "",
			envDB:            "postgres://localhost/db",
			wantBrainURL:     "http://eri.aerial",
		},
		{
			name:             "missing DB returns error via LoadConfig",
			path:             pathWithoutBrain,
			envBrainInternal: "",
			envBrain:         "",
			envDB:            "",
			wantErr:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ERI_URL", tc.envEri)
			t.Setenv("ERI_INTERNAL_URL", tc.envEriInternal)
			t.Setenv("BRAIN_INTERNAL_URL", tc.envBrainInternal)
			t.Setenv("BRAIN_URL", tc.envBrain)
			t.Setenv("DATABASE_URL", tc.envDB)

			cfg, err := LoadConfig(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfig() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if cfg.BrainURL != tc.wantBrainURL {
					t.Errorf("cfg.BrainURL = %q, want %q", cfg.BrainURL, tc.wantBrainURL)
				}
				if cfg.EriURL != tc.wantBrainURL {
					t.Errorf("cfg.EriURL = %q, want %q", cfg.EriURL, tc.wantBrainURL)
				}
			}
		})
	}
}

func TestLoadConfig_VanadisURLResolution(t *testing.T) {
	tmpDir := t.TempDir()
	pathWithVanadis := filepath.Join(tmpDir, "with_vanadis.yaml")
	pathWithoutVanadis := filepath.Join(tmpDir, "without_vanadis.yaml")
	_ = os.WriteFile(pathWithVanadis, []byte("port: '4005'\ntimezone: 'UTC'\nvanadis_url: 'http://yaml.vanadis:4025/'\n"), 0o644)
	_ = os.WriteFile(pathWithoutVanadis, []byte("port: '4005'\ntimezone: 'UTC'\n"), 0o644)

	tests := []struct {
		name           string
		path           string
		envVanadis     string
		envDB          string
		wantErr        bool
		wantVanadisURL string
	}{
		{
			name:           "VANADIS_URL takes precedence over yaml",
			path:           pathWithVanadis,
			envVanadis:     "http://env.vanadis:4025/",
			envDB:          "postgres://localhost/db",
			wantVanadisURL: "http://env.vanadis:4025",
		},
		{
			name:           "yaml vanadis_url used when env empty",
			path:           pathWithVanadis,
			envVanadis:     "",
			envDB:          "postgres://localhost/db",
			wantVanadisURL: "http://yaml.vanadis:4025",
		},
		{
			name:           "default fallback when nothing configured",
			path:           pathWithoutVanadis,
			envVanadis:     "",
			envDB:          "postgres://localhost/db",
			wantVanadisURL: "http://vanadis.aerial",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("VANADIS_URL", tc.envVanadis)
			t.Setenv("DATABASE_URL", tc.envDB)

			cfg, err := LoadConfig(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfig() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && cfg.VanadisURL != tc.wantVanadisURL {
				t.Errorf("cfg.VanadisURL = %q, want %q", cfg.VanadisURL, tc.wantVanadisURL)
			}
		})
	}
}
