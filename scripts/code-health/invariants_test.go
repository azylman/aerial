package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckNoImperativeDDL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		content     string
		isTestFile  bool
		isDbPkg     bool
		expectCount int
	}{
		{
			name:        "clean_go_code",
			content:     "package main\n\nfunc Run() {}\n",
			expectCount: 0,
		},
		{
			name:        "imperative_create_table",
			content:     "package main\n\nconst q = `CREATE TABLE users (id int)`\n",
			expectCount: 1,
		},
		{
			name:        "imperative_alter_table",
			content:     "package main\n\nfunc m() { db.Exec(\"ALTER TABLE foo ADD col text\") }\n",
			expectCount: 1,
		},
		{
			name:        "commented_ddl",
			content:     "package main\n// CREATE TABLE test (id int)\n",
			expectCount: 0,
		},
		{
			name:        "test_file_exempt",
			content:     "package main\n\nfunc TestX() { db.Exec(\"CREATE TABLE t (id int)\") }\n",
			isTestFile:  true,
			expectCount: 0,
		},
		{
			name:        "db_pkg_exempt",
			content:     "package db\n\nconst create = \"CREATE TABLE schema_migrations (version int)\"\n",
			isDbPkg:     true,
			expectCount: 0,
		},
		{
			name:        "nonexistent_file_skipped",
			content:     "",
			expectCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			rel := "services/eri/internal/server/main.go"
			switch {
			case tc.isTestFile:
				rel = "services/eri/internal/server/main_test.go"
			case tc.isDbPkg:
				rel = "services/eri/pkg/db/migrations/main.go"
			case tc.name == "nonexistent_file_skipped":
				rel = "services/eri/internal/server/nonexistent.go"
			}
			if tc.name != "nonexistent_file_skipped" {
				fullPath := filepath.Join(dir, rel)
				if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(fullPath, []byte(tc.content), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			v := checkNoImperativeDDL(dir, []string{rel})
			if len(v) != tc.expectCount {
				t.Fatalf("expected %d violations, got %d: %v", tc.expectCount, len(v), v)
			}
		})
	}
}

func TestCheckSynapseZeroVanadis(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	pClean := filepath.Join(dir, "services", "synapse", "internal", "clean.go")
	pDirty := filepath.Join(dir, "services", "synapse", "internal", "dirty.go")
	if err := os.MkdirAll(filepath.Dir(pClean), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pClean, []byte("package internal\n\nfunc Clean() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pDirty, []byte("package internal\n\n// calls vanadis\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		targets     []string
		expectCount int
	}{
		{
			name:        "clean_synapse_file",
			targets:     []string{"services/synapse/internal/clean.go"},
			expectCount: 0,
		},
		{
			name:        "vanadis_in_synapse",
			targets:     []string{"services/synapse/internal/dirty.go"},
			expectCount: 1,
		},
		{
			name:        "non_synapse_service_allowed",
			targets:     []string{"services/haro/client.go"},
			expectCount: 0,
		},
		{
			name:        "nonexistent_file_skipped",
			targets:     []string{"services/synapse/internal/missing.go"},
			expectCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v := checkSynapseZeroVanadis(dir, tc.targets)
			if len(v) != tc.expectCount {
				t.Fatalf("expected %d violations, got %d: %v", tc.expectCount, len(v), v)
			}
		})
	}
}

func TestCheckUTF8BOM(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		prefixBOM   bool
		expectCount int
	}{
		{
			name:        "clean_file",
			prefixBOM:   false,
			expectCount: 0,
		},
		{
			name:        "file_with_bom",
			prefixBOM:   true,
			expectCount: 1,
		},
		{
			name:        "nonexistent_file",
			expectCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			fullPath := filepath.Join(dir, "file.go")
			if tc.name != "nonexistent_file" {
				content := []byte("package main\n")
				if tc.prefixBOM {
					content = append([]byte{0xef, 0xbb, 0xbf}, content...)
				}
				if err := os.WriteFile(fullPath, content, 0o644); err != nil {
					t.Fatal(err)
				}
			}

			v := checkUTF8BOM(dir, []string{"file.go"})
			if len(v) != tc.expectCount {
				t.Fatalf("expected %d violations, got %d: %v", tc.expectCount, len(v), v)
			}
		})
	}
}

func TestCheckNoMainInTests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		rel         string
		content     string
		expectCount int
	}{
		{
			name:        "normal_eri_test",
			rel:         "services/eri/eri_test.go",
			content:     "package eri_test\n\nfunc TestX(t *testing.T) {}\n",
			expectCount: 0,
		},
		{
			name:        "direct_main_call_in_eri",
			rel:         "services/eri/internal/server_test.go",
			content:     "package server_test\n\nfunc TestX(t *testing.T) { main() }\n",
			expectCount: 1,
		},
		{
			name:        "non_eri_service_exempt",
			rel:         "services/haro/haro_test.go",
			content:     "package haro_test\n\nfunc TestH(t *testing.T) { main() }\n",
			expectCount: 0,
		},
		{
			name:        "nonexistent_test_skipped",
			rel:         "services/eri/nonexistent_test.go",
			content:     "",
			expectCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if tc.name != "nonexistent_test_skipped" {
				full := filepath.Join(dir, tc.rel)
				if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, []byte(tc.content), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			v := checkNoMainInTests(dir, []string{tc.rel})
			if len(v) != tc.expectCount {
				t.Fatalf("expected %d violations, got %d: %v", tc.expectCount, len(v), v)
			}
		})
	}
}

func TestCheckDependabotManifests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		dependabot  string
		manifests   []string
		expectCount int
	}{
		{
			name: "covered",
			dependabot: `version: 2
updates:
  - directory: "/"
  - directory: "/services/console/ui"
`,
			manifests:   []string{"go.mod", "services/console/ui/package.json"},
			expectCount: 0,
		},
		{
			name: "missing_manifest_dir",
			dependabot: `version: 2
updates:
  - directory: "/"
`,
			manifests:   []string{"go.mod", "services/console/ui/package.json"},
			expectCount: 1,
		},
		{
			name:        "dependabot_file_missing",
			dependabot:  "",
			manifests:   []string{"go.mod"},
			expectCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if tc.name != "dependabot_file_missing" {
				dotGithub := filepath.Join(dir, ".github")
				if err := os.MkdirAll(dotGithub, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dotGithub, "dependabot.yml"), []byte(tc.dependabot), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			for _, m := range tc.manifests {
				mPath := filepath.Join(dir, m)
				if err := os.MkdirAll(filepath.Dir(mPath), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(mPath, []byte("{}"), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			v := checkDependabotCoverage(dir)
			if len(v) != tc.expectCount {
				t.Fatalf("expected %d violations, got %d: %v", tc.expectCount, len(v), v)
			}
		})
	}
}
