package httpkit

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigPathWithLocal_TableDriven(t *testing.T) {
	// Not t.Parallel() because it modifies environment variables
	tempDir := t.TempDir()
	validLocal := filepath.Join(tempDir, "local_valid.yaml")
	if err := os.WriteFile(validLocal, []byte("key: val\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	emptyLocal := filepath.Join(tempDir, "local_empty.yaml")
	if err := os.WriteFile(emptyLocal, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}

	fallback1 := filepath.Join(tempDir, "fallback1.yaml")
	if err := os.WriteFile(fallback1, []byte("fb1"), 0o644); err != nil {
		t.Fatal(err)
	}

	nonExistent := filepath.Join(tempDir, "does_not_exist.yaml")

	tests := []struct {
		name          string
		envKey        string
		envVal        string
		localPath     string
		fallbackPaths []string
		wantResult    string
	}{
		{name: "env variable explicit key set", envKey: "MY_CONFIG", envVal: "/etc/config/my_app.yaml", wantResult: "/etc/config/my_app.yaml"},
		{name: "default env key CONFIG_PATH set", envVal: "/etc/default/config.yaml", wantResult: "/etc/default/config.yaml"},
		{name: "local path exists with positive size", envKey: "UNSET_ENV", localPath: validLocal, wantResult: validLocal},
		{
			name: "local path empty size falls back to existing fallback", envKey: "UNSET_ENV", localPath: emptyLocal, fallbackPaths: []string{fallback1},
			wantResult: fallback1,
		},
		{name: "none exist returns fallbackPaths[0]", envKey: "UNSET_ENV", localPath: nonExistent, fallbackPaths: []string{nonExistent}, wantResult: nonExistent},
		{name: "none exist and fallbackPaths empty returns empty string", envKey: "UNSET_ENV", localPath: nonExistent, wantResult: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envKey != "" {
				t.Setenv(tc.envKey, tc.envVal)
			} else if tc.envVal != "" {
				t.Setenv("CONFIG_PATH", tc.envVal)
			}

			got := ResolveConfigPathWithLocal(tc.envKey, tc.localPath, tc.fallbackPaths...)
			if got != tc.wantResult {
				t.Errorf("ResolveConfigPathWithLocal() = %q, want %q", got, tc.wantResult)
			}
		})
	}
}

func TestBuildPostgresDSN(t *testing.T) {
	t.Parallel()

	// A nil env map means a nil lookup func; an empty map is a lookup with nothing set.
	tests := []struct {
		name      string
		env       map[string]string
		defaultDB string
		want      string
	}{
		{"nil lookup returns empty", nil, "aerial", ""},
		{"empty env returns empty", map[string]string{}, "aerial", ""},
		{
			"postgres url precedence over database url",
			map[string]string{"POSTGRES_URL": "postgres://pguser:pgpass@pghost:5432/primary_db", "DATABASE_URL": "postgres://dbuser:dbpass@dbhost:5432/ignored_db"},
			"aerial", "postgres://pguser:pgpass@pghost:5432/primary_db",
		},
		{
			"database url with explicit postgres db path override",
			map[string]string{"DATABASE_URL": "postgres://app:secret@db:5432/olddb", "POSTGRES_DB": "overridedb"},
			"aerial", "postgres://app:secret@db:5432/overridedb",
		},
		{
			"postgres password only auto defaults host to postgres",
			map[string]string{"POSTGRES_PASSWORD": "secretpassword"},
			"aerial", "postgres://aerial:secretpassword@postgres:5432/aerial?sslmode=disable",
		},
		{
			"custom host port user and db",
			map[string]string{"POSTGRES_HOST": "custom-pg", "POSTGRES_PORT": "5433", "POSTGRES_USER": "myuser", "POSTGRES_PASSWORD": "mypassword", "POSTGRES_DB": "mydb"},
			"aerial", "postgres://myuser:mypassword@custom-pg:5433/mydb?sslmode=disable",
		},
		{
			"kubernetes service link tcp port normalized",
			map[string]string{"POSTGRES_PASSWORD": "secretpassword", "POSTGRES_PORT": "tcp://192.0.2.188:5432"},
			"aerial", "postgres://aerial:secretpassword@postgres:5432/aerial?sslmode=disable",
		},
		{
			"default db fallback when postgres db unset",
			map[string]string{"POSTGRES_HOST": "postgres", "POSTGRES_PASSWORD": "pass"},
			"custom_default", "postgres://aerial:pass@postgres:5432/custom_default?sslmode=disable",
		},
		{
			"default db fallback to aerial when both unset",
			map[string]string{"POSTGRES_HOST": "postgres"},
			"", "postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable",
		},
		{
			"special character url encoding in passwords",
			map[string]string{"POSTGRES_HOST": "postgres", "POSTGRES_PASSWORD": "p@ss:w/ord"},
			"aerial", "postgres://aerial:p%40ss%3Aw%2Ford@postgres:5432/aerial?sslmode=disable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var lookup func(string) string
			if tc.env != nil {
				lookup = func(k string) string { return tc.env[k] }
			}

			got := BuildPostgresDSN(lookup, tc.defaultDB)
			if got != tc.want {
				t.Errorf("BuildPostgresDSN() = %q, want %q", got, tc.want)
			}
		})
	}
}
