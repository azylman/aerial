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
	_ = os.WriteFile(validPath, []byte("port: '4005'\ntimezone: 'America/Chicago'\neri_url: 'http://eri:8080'\nvanadis_url: 'http://vanadis:8080'\n"), 0o644)
	_ = os.WriteFile(extraPath, []byte("port: '4005'\ntimezone: 'America/Chicago'\neri_url: 'http://eri:8080'\nvanadis_url: 'http://vanadis:8080'\nextra_event_sources:\n  - ' Custom_Source '\n  - ''\n  - 'custom_source'\n"), 0o644)
	_ = os.WriteFile(noPortPath, []byte("timezone: 'America/Chicago'\neri_url: 'http://eri:8080'\nvanadis_url: 'http://vanadis:8080'\n"), 0o644)
	_ = os.WriteFile(noTZPath, []byte("port: '4005'\neri_url: 'http://eri:8080'\nvanadis_url: 'http://vanadis:8080'\n"), 0o644)
	_ = os.WriteFile(malformedPath, []byte("port: [unclosed"), 0o644)

	tests := []struct {
		name        string
		path        string
		dbURL       string
		envPassword string
		wantErr     bool
		wantPort    string
		wantTZ      string
		wantDB      string
		wantSources int
	}{
		{"valid config", validPath, "postgres://user:pass@host:5432/db", "", false, "4005", "America/Chicago", "postgres://user:pass@host:5432/db", 0},
		{"valid config with extra_event_sources", extraPath, "postgres://user:pass@host:5432/db", "", false, "4005", "America/Chicago", "postgres://user:pass@host:5432/db", 1},
		{"empty dbURL falls back to decomposed POSTGRES_PASSWORD", validPath, "", "secretpass", false, "4005", "America/Chicago", "postgres://aerial:secretpass@postgres:5432/aerial?sslmode=disable", 0},
		{"missing file", filepath.Join(tmpDir, "missing.yaml"), "postgres://localhost/db", "", true, "", "", "", 0},
		{"malformed yaml", malformedPath, "postgres://localhost/db", "", true, "", "", "", 0},
		{"missing port", noPortPath, "postgres://localhost/db", "", true, "", "", "", 0},
		{"missing timezone", noTZPath, "postgres://localhost/db", "", true, "", "", "", 0},
		{"empty dbURL", validPath, "", "", true, "", "", "", 0},
		{"empty path", "", "postgres://localhost/db", "", true, "", "", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ERI_URL", "")
			t.Setenv("VANADIS_URL", "")
			t.Setenv("DATABASE_URL", "")
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", tc.envPassword)
			t.Setenv("POSTGRES_DB", "")

			cfg, err := LoadConfigFile(tc.path, tc.dbURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (cfg.Port != tc.wantPort || cfg.Timezone != tc.wantTZ || cfg.DatabaseURL != tc.wantDB || len(cfg.ExtraEventSources) != tc.wantSources) {
				t.Fatalf("unexpected config: %+v", cfg)
			}
		})
	}
}

func TestLoadConfig_BrainURLResolution(t *testing.T) {
	tmpDir := t.TempDir()
	pathWithEri := filepath.Join(tmpDir, "with_eri.yaml")
	pathWithoutBrain := filepath.Join(tmpDir, "without_brain.yaml")
	_ = os.WriteFile(pathWithEri, []byte("port: '4005'\ntimezone: 'UTC'\neri_url: 'http://yaml.eri:8088/'\nvanadis_url: 'http://yaml.vanadis:8080'\n"), 0o644)
	_ = os.WriteFile(pathWithoutBrain, []byte("port: '4005'\ntimezone: 'UTC'\nvanadis_url: 'http://yaml.vanadis:8080'\n"), 0o644)

	tests := []struct {
		name         string
		path         string
		envEri       string
		envDB        string
		envPassword  string
		wantErr      bool
		wantBrainURL string
		wantDB       string
	}{
		{
			name:         "ERI_URL takes top precedence",
			path:         pathWithEri,
			envEri:       "http://eri.aerial:9001/",
			envDB:        "postgres://localhost/db",
			wantBrainURL: "http://eri.aerial:9001",
		},
		{
			name:         "yaml eri_url used when env empty",
			path:         pathWithEri,
			envDB:        "postgres://localhost/db",
			wantBrainURL: "http://yaml.eri:8088",
		},
		{
			name:    "missing eri_url returns error",
			path:    pathWithoutBrain,
			envDB:   "postgres://localhost/db",
			wantErr: true,
		},
		{
			name:         "decomposed POSTGRES_PASSWORD resolution",
			path:         pathWithEri,
			envPassword:  "secretpass",
			wantBrainURL: "http://yaml.eri:8088",
			wantDB:       "postgres://aerial:secretpass@postgres:5432/aerial?sslmode=disable",
		},
		{
			name:    "missing DB returns error via LoadConfig",
			path:    pathWithEri,
			envDB:   "",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("VANADIS_URL", "")
			t.Setenv("ERI_URL", tc.envEri)
			t.Setenv("DATABASE_URL", tc.envDB)
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", tc.envPassword)
			t.Setenv("POSTGRES_DB", "")

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
				if tc.wantDB != "" && cfg.DatabaseURL != tc.wantDB {
					t.Errorf("cfg.DatabaseURL = %q, want %q", cfg.DatabaseURL, tc.wantDB)
				}
			}
		})
	}
}

func TestLoadConfig_VanadisURLResolution(t *testing.T) {
	tmpDir := t.TempDir()
	pathWithVanadis := filepath.Join(tmpDir, "with_vanadis.yaml")
	pathWithoutVanadis := filepath.Join(tmpDir, "without_vanadis.yaml")
	_ = os.WriteFile(pathWithVanadis, []byte("port: '4005'\ntimezone: 'UTC'\neri_url: 'http://yaml.eri:8080'\nvanadis_url: 'http://yaml.vanadis:4025/'\n"), 0o644)
	_ = os.WriteFile(pathWithoutVanadis, []byte("port: '4005'\ntimezone: 'UTC'\neri_url: 'http://yaml.eri:8080'\n"), 0o644)

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
			name:       "missing vanadis_url returns error",
			path:       pathWithoutVanadis,
			envVanadis: "",
			envDB:      "postgres://localhost/db",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ERI_URL", "")
			t.Setenv("VANADIS_URL", tc.envVanadis)
			t.Setenv("DATABASE_URL", tc.envDB)
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", "")
			t.Setenv("POSTGRES_DB", "")

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
