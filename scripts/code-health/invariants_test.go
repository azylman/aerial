package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckNoImperativeDDL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		relPath     string
		content     string
		writeFile   bool
		expectCount int
	}{
		{
			name:        "clean_go_code",
			relPath:     "services/eri/internal/server/main.go",
			content:     "package main\n\nfunc Run() {}\n",
			writeFile:   true,
			expectCount: 0,
		},
		{
			name:        "imperative_create_table",
			relPath:     "services/eri/internal/server/main.go",
			content:     "package main\n\nconst q = `CREATE TABLE users (id int)`\n",
			writeFile:   true,
			expectCount: 1,
		},
		{
			name:        "imperative_alter_table",
			relPath:     "services/eri/internal/server/main.go",
			content:     "package main\n\nfunc m() { db.Exec(\"ALTER TABLE foo ADD col text\") }\n",
			writeFile:   true,
			expectCount: 1,
		},
		{
			name:        "commented_ddl",
			relPath:     "services/eri/internal/server/main.go",
			content:     "package main\n// CREATE TABLE test (id int)\n",
			writeFile:   true,
			expectCount: 0,
		},
		{
			name:        "test_file_exempt",
			relPath:     "services/eri/internal/server/main_test.go",
			content:     "package main\n\nfunc TestX() { db.Exec(\"CREATE TABLE t (id int)\") }\n",
			writeFile:   true,
			expectCount: 0,
		},
		{
			name:        "sql_file_exempt",
			relPath:     "services/eri/pkg/db/migrations/001_init.sql",
			content:     "CREATE TABLE schema_migrations (version int);\n",
			writeFile:   true,
			expectCount: 0,
		},
		{
			name:        "nonexistent_file_skipped",
			relPath:     "services/eri/internal/server/nonexistent.go",
			content:     "",
			writeFile:   false,
			expectCount: 0,
		},
		{
			name:        "python_create_table",
			relPath:     "services/foo/main.py",
			content:     "query = 'CREATE TABLE foo (id INT)'\n",
			writeFile:   true,
			expectCount: 1,
		},
		{
			name:        "python_comment_exempt",
			relPath:     "services/foo/main.py",
			content:     "# CREATE TABLE foo (id INT)\n",
			writeFile:   true,
			expectCount: 0,
		},
		{
			name:        "ts_alter_table",
			relPath:     "services/console/ui/db.ts",
			content:     "const q = 'ALTER TABLE bar ADD COLUMN x INT';\n",
			writeFile:   true,
			expectCount: 1,
		},
		{
			name:        "ts_test_exempt",
			relPath:     "services/console/ui/db.test.ts",
			content:     "const q = 'ALTER TABLE bar ADD COLUMN x INT';\n",
			writeFile:   true,
			expectCount: 0,
		},
		{
			name:        "python_test_exempt",
			relPath:     "services/foo/test_bar.py",
			content:     "q = 'CREATE TABLE foo (id INT)'\n",
			writeFile:   true,
			expectCount: 0,
		},
		{
			name:        "scripts_py_exempt",
			relPath:     "scripts/build.py",
			content:     "q = 'CREATE TABLE foo (id INT)'\n",
			writeFile:   true,
			expectCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if tc.writeFile {
				writeTestFile(t, dir, tc.relPath, tc.content)
			}

			if v := checkNoImperativeDDL(dir, []string{tc.relPath}); len(v) != tc.expectCount {
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
		writeFile   bool
		prefixBOM   bool
		expectCount int
	}{
		{
			name:        "clean_file",
			writeFile:   true,
			prefixBOM:   false,
			expectCount: 0,
		},
		{
			name:        "file_with_bom",
			writeFile:   true,
			prefixBOM:   true,
			expectCount: 1,
		},
		{
			name:        "nonexistent_file",
			writeFile:   false,
			prefixBOM:   false,
			expectCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if tc.writeFile {
				content := []byte("package main\n")
				if tc.prefixBOM {
					content = append([]byte{0xef, 0xbb, 0xbf}, content...)
				}
				fullPath := filepath.Join(dir, "file.go")
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

func writeTestFile(t *testing.T, dir, rel, content string) {
	t.Helper()
	full := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCheckNoMainInTests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		rel         string
		content     string
		writeFile   bool
		expectCount int
	}{
		{
			name:        "normal_eri_test",
			rel:         "services/eri/eri_test.go",
			content:     "package eri_test\n// main() in comment\nfunc TestX(t *testing.T) { runMain() }\n",
			writeFile:   true,
			expectCount: 0,
		},
		{
			name:        "direct_main_call_in_eri",
			rel:         "services/eri/internal/server_test.go",
			content:     "package server_test\n\nfunc TestX(t *testing.T) { main() }\n",
			writeFile:   true,
			expectCount: 1,
		},
		{
			name:        "non_eri_service_exempt",
			rel:         "services/haro/haro_test.go",
			content:     "package haro_test\n\nfunc TestH(t *testing.T) { main() }\n",
			writeFile:   true,
			expectCount: 0,
		},
		{
			name:        "nonexistent_test_skipped",
			rel:         "services/eri/nonexistent_test.go",
			content:     "",
			writeFile:   false,
			expectCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if tc.writeFile {
				writeTestFile(t, dir, tc.rel, tc.content)
			}

			if v := checkNoMainInTests(dir, []string{tc.rel}); len(v) != tc.expectCount {
				t.Fatalf("expected %d violations, got %d: %v", tc.expectCount, len(v), v)
			}
		})
	}
}

func TestCheckDependabotManifests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		writeDependabot bool
		dependabot      string
		manifests       []string
		expectCount     int
	}{
		{
			name:            "covered",
			writeDependabot: true,
			dependabot: `version: 2
updates:
  - package-ecosystem: "gomod"
    directory: "/"
    groups:
      v:
        applies-to: version-updates
      s:
        applies-to: security-updates
  - package-ecosystem: "npm"
    directory: "/services/console/ui"
    groups:
      v:
        applies-to: version-updates
      s:
        applies-to: security-updates
`,
			manifests:   []string{"go.mod", "services/console/ui/package.json"},
			expectCount: 0,
		},
		{
			name:            "missing_manifest_dir",
			writeDependabot: true,
			dependabot: `version: 2
updates:
  - package-ecosystem: "gomod"
    directory: "/"
    groups:
      v:
        applies-to: version-updates
      s:
        applies-to: security-updates
  - package-ecosystem: "npm"
    directory: "/services/console/ui-other"
    groups:
      v:
        applies-to: version-updates
      s:
        applies-to: security-updates
`,
			manifests:   []string{"go.mod", "services/console/ui/package.json"},
			expectCount: 1,
		},
		{
			name:            "grouping_mismatch",
			writeDependabot: true,
			dependabot: `version: 2
updates:
  - package-ecosystem: "gomod"
    directory: "/"
`,
			manifests:   []string{"go.mod"},
			expectCount: 1,
		},
		{
			name:            "dependabot_file_missing",
			writeDependabot: false,
			dependabot:      "",
			manifests:       []string{"go.mod"},
			expectCount:     1,
		},
		{
			name:            "no_manifests",
			writeDependabot: true,
			dependabot:      "version: 2\n",
			manifests:       nil,
			expectCount:     0,
		},
		{
			name:            "no_ecosystems",
			writeDependabot: true,
			dependabot:      "version: 2\nupdates:\n  - directory: \"/\"\n",
			manifests:       []string{"go.mod"},
			expectCount:     1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if tc.writeDependabot {
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

func setupParityFiles(t *testing.T, dir, dfContent, mbContent string) {
	t.Helper()
	dfDir := filepath.Join(dir, "services", "eri-restate")
	if err := os.MkdirAll(dfDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dfDir, "Dockerfile"), []byte(dfContent), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "MODULE.bazel"), []byte(mbContent), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCheckCommunitySkillsParity_Validation(t *testing.T) {
	t.Parallel()

	validDockerfile := `ARG ANDROID_SKILLS_REV=rev1
ARG ANDROID_SKILLS_SHA256=sha1
ARG CC_SKILLS_GOLANG_REV=rev2
ARG CC_SKILLS_GOLANG_SHA256=sha2
ARG BEAUTIFUL_RUST_REV=rev3
ARG BEAUTIFUL_RUST_SHA256=sha3
ARG KOTLIN_AGENT_SKILLS_REV=rev4
ARG KOTLIN_AGENT_SKILLS_SHA256=sha4
ARG KOTLIN_FOOTGUNS_REV=rev5
ARG KOTLIN_FOOTGUNS_SHA256=sha5

RUN cp -a wear-compose-m3 wear-widgets jetpack-compose-performance android-intent-security android-permissions-security anr-audit-and-fix testing-setup \
    golang-code-style golang-concurrency golang-context golang-error-handling golang-modernize golang-performance golang-safety golang-structs-interfaces golang-testing \
    rust-best-practices rust-port rust-review \
    kotlin-tooling-agp9-migration kotlin-tooling-kotlin-toolchain \
    compose-screens-footguns footgun-scan remote-api-footguns state-and-background-footguns
`

	validModuleBazel := `http_archive(
    name = "android_skills",
    sha256 = "sha1",
    urls = ["https://github.com/android/skills/archive/rev1.tar.gz"],
    patch_cmds = ["wear-compose-m3 wear-widgets jetpack-compose-performance android-intent-security android-permissions-security anr-audit-and-fix testing-setup"],
)

http_archive(
    name = "cc_skills_golang",
    sha256 = "sha2",
    urls = ["https://github.com/samber/cc-skills-golang/archive/rev2.tar.gz"],
    srcs = ["golang-code-style", "golang-concurrency", "golang-context", "golang-error-handling", "golang-modernize", "golang-performance", "golang-safety", "golang-structs-interfaces", "golang-testing"],
)

http_archive(
    name = "beautiful_rust",
    sha256 = "sha3",
    urls = ["https://github.com/morganlinton/BeautifulRust/archive/rev3.tar.gz"],
    srcs = ["rust-best-practices", "rust-port", "rust-review"],
)

http_archive(
    name = "kotlin_agent_skills",
    sha256 = "sha4",
    urls = ["https://github.com/Kotlin/kotlin-agent-skills/archive/rev4.tar.gz"],
    srcs = ["kotlin-tooling-agp9-migration", "kotlin-tooling-kotlin-toolchain"],
)

http_archive(
    name = "kotlin_footguns",
    sha256 = "sha5",
    urls = ["https://github.com/maxrave-dev/kotlin-footguns/archive/rev5.tar.gz"],
    srcs = ["compose-screens-footguns", "footgun-scan", "remote-api-footguns", "state-and-background-footguns"],
)
`

	tests := []struct {
		name        string
		dockerfile  string
		moduleBazel string
		expectCount int
	}{
		{
			name:        "valid_parity",
			dockerfile:  validDockerfile,
			moduleBazel: validModuleBazel,
			expectCount: 0,
		},
		{
			name:        "revision_mismatch",
			dockerfile:  strings.Replace(validDockerfile, "ARG BEAUTIFUL_RUST_REV=rev3", "ARG BEAUTIFUL_RUST_REV=rev3_drifted", 1),
			moduleBazel: validModuleBazel,
			expectCount: 1,
		},
		{
			name:        "sha_mismatch",
			dockerfile:  strings.Replace(validDockerfile, "ARG CC_SKILLS_GOLANG_SHA256=sha2", "ARG CC_SKILLS_GOLANG_SHA256=sha2_drifted", 1),
			moduleBazel: validModuleBazel,
			expectCount: 1,
		},
		{
			name:        "missing_skill_in_dockerfile",
			dockerfile:  strings.Replace(validDockerfile, "rust-review", "rust-somethingelse", 1),
			moduleBazel: validModuleBazel,
			expectCount: 1,
		},
		{
			name:        "missing_skill_in_bazel",
			dockerfile:  validDockerfile,
			moduleBazel: strings.Replace(validModuleBazel, "rust-review", "rust-somethingelse", 1),
			expectCount: 1,
		},
		{
			name:        "missing_arg_rev",
			dockerfile:  strings.Replace(validDockerfile, "ARG BEAUTIFUL_RUST_REV=rev3", "# omitted", 1),
			moduleBazel: validModuleBazel,
			expectCount: 1,
		},
		{
			name:        "missing_arg_sha",
			dockerfile:  strings.Replace(validDockerfile, "ARG BEAUTIFUL_RUST_SHA256=sha3", "# omitted", 1),
			moduleBazel: validModuleBazel,
			expectCount: 1,
		},
		{
			name:        "missing_bazel_repo",
			dockerfile:  validDockerfile,
			moduleBazel: strings.Replace(validModuleBazel, `name = "beautiful_rust"`, `name = "other"`, 1),
			expectCount: 1,
		},
		{
			name:        "missing_bazel_sha",
			dockerfile:  validDockerfile,
			moduleBazel: strings.Replace(validModuleBazel, `sha256 = "sha3"`, `# omitted`, 1),
			expectCount: 1,
		},
		{
			name:        "missing_bazel_url",
			dockerfile:  validDockerfile,
			moduleBazel: strings.Replace(validModuleBazel, `urls = ["https://github.com/morganlinton/BeautifulRust/archive/rev3.tar.gz"]`, `# omitted`, 1),
			expectCount: 1,
		},
		{
			name:        "missing_archive_header",
			dockerfile:  validDockerfile,
			moduleBazel: strings.Replace(validModuleBazel, "http_archive(\n    name = \"android_skills\"", "name = \"android_skills\"", 1),
			expectCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			setupParityFiles(t, dir, tc.dockerfile, tc.moduleBazel)

			violations := checkCommunitySkillsParity(dir)
			if len(violations) != tc.expectCount {
				t.Fatalf("expected %d violations, got %d: %v", tc.expectCount, len(violations), violations)
			}
		})
	}
}

func TestCheckCommunitySkillsParity_MissingFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	violations := checkCommunitySkillsParity(dir)
	if len(violations) != 0 {
		t.Fatalf("expected 0 violations for missing files, got %d: %v", len(violations), violations)
	}
}

func TestCheckCommunitySkillsParity_ReadErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dfDir := filepath.Join(dir, "services", "eri-restate", "Dockerfile")
	if err := os.MkdirAll(dfDir, 0o755); err != nil {
		t.Fatal(err)
	}
	violations := checkCommunitySkillsParity(dir)
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for directory as file, got %d: %v", len(violations), violations)
	}
}
