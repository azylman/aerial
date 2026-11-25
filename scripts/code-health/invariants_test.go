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

const testValidDockerfile = `ARG ANDROID_SKILLS_REV=rev1
ARG ANDROID_SKILLS_SHA256=sha1
ARG CC_SKILLS_GOLANG_REV=rev2
ARG CC_SKILLS_GOLANG_SHA256=sha2
ARG BEAUTIFUL_RUST_REV=rev3
ARG BEAUTIFUL_RUST_SHA256=sha3
ARG KOTLIN_AGENT_SKILLS_REV=rev4
ARG KOTLIN_AGENT_SKILLS_SHA256=sha4
ARG KOTLIN_FOOTGUNS_REV=rev5
ARG KOTLIN_FOOTGUNS_SHA256=sha5

RUN mkdir -p /tmp/android-skills && \
    cp -a /tmp/android-skills/wear/wear-compose-m3 /opt/skills/ && \
    cp -a /tmp/android-skills/wear/wear-widgets /opt/skills/ && \
    cp -a /tmp/android-skills/jetpack-compose/jetpack-compose-performance /opt/skills/ && \
    cp -a /tmp/android-skills/security/android-intent-security /opt/skills/ && \
    cp -a /tmp/android-skills/security/android-permissions-security /opt/skills/ && \
    cp -a /tmp/android-skills/performance/anr-audit-and-fix /opt/skills/ && \
    cp -a /tmp/android-skills/testing/testing-setup /opt/skills/ && \
    rm -rf /tmp/android-skills && \
    mkdir -p /tmp/golang-skills && \
    for skill in \
        golang-code-style \
        golang-concurrency \
        golang-context \
        golang-error-handling \
        golang-modernize \
        golang-performance \
        golang-safety \
        golang-structs-interfaces \
        golang-testing; do \
        cp -a "/tmp/golang-skills/skills/$skill" /opt/skills/; \
    done && \
    rm -rf /tmp/golang-skills && \
    mkdir -p /tmp/rust-skills && \
    for skill in \
        rust-best-practices \
        rust-port \
        rust-review; do \
        cp -a "/tmp/rust-skills/skills/$skill" /opt/skills/; \
    done && \
    rm -rf /tmp/rust-skills && \
    mkdir -p /tmp/kotlin-agent-skills && \
    for skill in \
        kotlin-tooling-agp9-migration \
        kotlin-tooling-kotlin-toolchain; do \
        cp -a "/tmp/kotlin-agent-skills/skills/$skill" /opt/skills/; \
    done && \
    rm -rf /tmp/kotlin-agent-skills && \
    mkdir -p /tmp/kotlin-footguns && \
    for skill in \
        compose-screens-footguns \
        footgun-scan \
        remote-api-footguns \
        state-and-background-footguns; do \
        cp -a "/tmp/kotlin-footguns/skills/$skill" /opt/skills/; \
    done && \
    rm -rf /tmp/kotlin-footguns
`

const testValidModuleBazel = `http_archive(
    name = "android_skills",
    sha256 = "sha1",
    urls = ["https://github.com/android/skills/archive/rev1.tar.gz"],
    patch_cmds = [
        "mkdir -p skills && mv wear/wear-compose-m3 wear/wear-widgets jetpack-compose/jetpack-compose-performance security/android-intent-security security/android-permissions-security performance/anr-audit-and-fix testing/testing-setup skills/",
    ],
)

http_archive(
    name = "cc_skills_golang",
    sha256 = "sha2",
    urls = ["https://github.com/samber/cc-skills-golang/archive/rev2.tar.gz"],
    build_file_content = """
pkg_tar(
    name = "skills_tar",
    srcs = glob([
        "skills/golang-code-style/**",
        "skills/golang-concurrency/**",
        "skills/golang-context/**",
        "skills/golang-error-handling/**",
        "skills/golang-modernize/**",
        "skills/golang-performance/**",
        "skills/golang-safety/**",
        "skills/golang-structs-interfaces/**",
        "skills/golang-testing/**",
    ]),
)
""",
)

http_archive(
    name = "beautiful_rust",
    sha256 = "sha3",
    urls = ["https://github.com/morganlinton/BeautifulRust/archive/rev3.tar.gz"],
    build_file_content = """
pkg_tar(
    name = "skills_tar",
    srcs = glob([
        "skills/rust-best-practices/**",
        "skills/rust-port/**",
        "skills/rust-review/**",
    ]),
)
""",
)

http_archive(
    name = "kotlin_agent_skills",
    sha256 = "sha4",
    urls = ["https://github.com/Kotlin/kotlin-agent-skills/archive/rev4.tar.gz"],
    build_file_content = """
pkg_tar(
    name = "skills_tar",
    srcs = glob([
        "skills/kotlin-tooling-agp9-migration/**",
        "skills/kotlin-tooling-kotlin-toolchain/**",
    ]),
)
""",
)

http_archive(
    name = "kotlin_footguns",
    sha256 = "sha5",
    urls = ["https://github.com/maxrave-dev/kotlin-footguns/archive/rev5.tar.gz"],
    build_file_content = """
pkg_tar(
    name = "skills_tar",
    srcs = glob([
        "skills/compose-screens-footguns/**",
        "skills/footgun-scan/**",
        "skills/remote-api-footguns/**",
        "skills/state-and-background-footguns/**",
    ]),
)
""",
)
`

func TestCheckCommunitySkillsParity_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		dockerfile  string
		moduleBazel string
		expectCount int
	}{
		{
			name:        "valid_parity",
			dockerfile:  testValidDockerfile,
			moduleBazel: testValidModuleBazel,
			expectCount: 0,
		},
		{
			name:        "revision_mismatch",
			dockerfile:  strings.Replace(testValidDockerfile, "ARG BEAUTIFUL_RUST_REV=rev3", "ARG BEAUTIFUL_RUST_REV=rev3_drifted", 1),
			moduleBazel: testValidModuleBazel,
			expectCount: 1,
		},
		{
			name:        "sha_mismatch",
			dockerfile:  strings.Replace(testValidDockerfile, "ARG CC_SKILLS_GOLANG_SHA256=sha2", "ARG CC_SKILLS_GOLANG_SHA256=sha2_drifted", 1),
			moduleBazel: testValidModuleBazel,
			expectCount: 1,
		},
		{
			name:        "missing_skill_in_dockerfile",
			dockerfile:  strings.Replace(testValidDockerfile, "rust-review;", ";", 1),
			moduleBazel: testValidModuleBazel,
			expectCount: 1,
		},
		{
			name:        "missing_skill_in_bazel",
			dockerfile:  testValidDockerfile,
			moduleBazel: strings.Replace(testValidModuleBazel, "\"skills/rust-review/**\",\n", "", 1),
			expectCount: 1,
		},
		{
			name:        "extra_skill_in_dockerfile",
			dockerfile:  strings.Replace(testValidDockerfile, "rust-review", "rust-review \\\n        rust-extra", 1),
			moduleBazel: testValidModuleBazel,
			expectCount: 1,
		},
		{
			name:        "extra_skill_in_bazel",
			dockerfile:  testValidDockerfile,
			moduleBazel: strings.Replace(testValidModuleBazel, "\"skills/rust-review/**\",\n", "\"skills/rust-review/**\",\n        \"skills/rust-extra/**\",\n", 1),
			expectCount: 1,
		},
		{
			name:        "missing_arg_rev",
			dockerfile:  strings.Replace(testValidDockerfile, "ARG BEAUTIFUL_RUST_REV=rev3", "# omitted", 1),
			moduleBazel: testValidModuleBazel,
			expectCount: 1,
		},
		{
			name:        "missing_arg_sha",
			dockerfile:  strings.Replace(testValidDockerfile, "ARG BEAUTIFUL_RUST_SHA256=sha3", "# omitted", 1),
			moduleBazel: testValidModuleBazel,
			expectCount: 1,
		},
		{
			name:        "missing_bazel_repo",
			dockerfile:  testValidDockerfile,
			moduleBazel: strings.Replace(testValidModuleBazel, `name = "beautiful_rust"`, `name = "other"`, 1),
			expectCount: 1,
		},
		{
			name:        "missing_bazel_sha",
			dockerfile:  testValidDockerfile,
			moduleBazel: strings.Replace(testValidModuleBazel, `sha256 = "sha3"`, `# omitted`, 1),
			expectCount: 1,
		},
		{
			name:        "missing_bazel_url",
			dockerfile:  testValidDockerfile,
			moduleBazel: strings.Replace(testValidModuleBazel, `urls = ["https://github.com/morganlinton/BeautifulRust/archive/rev3.tar.gz"]`, `# omitted`, 1),
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
